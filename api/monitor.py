import datetime
import feedparser
import logging
import os
import sys
from lxml import etree
from nose.tools import set_trace
from StringIO import StringIO

from sqlalchemy import (
    and_,
    or_,
)

from core.monitor import (
    CollectionMonitor,
    EditionSweepMonitor,
    ReaperMonitor,
)
from core.model import (
    Annotation,
    Collection,
    DataSource,
    Edition,
    ExternalIntegration,
    Hold,
    Identifier,
    LicensePool,
    Loan,
)
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    OPDSImporter,
    OPDSXMLParser,
)
from core.util.http import RemoteIntegrationException

from odl import (
    ODLWithConsolidatedCopiesAPI,
    SharedODLAPI,
)


class MetadataWranglerCollectionMonitor(CollectionMonitor):

    """Abstract base CollectionMonitor with helper methods for interactions
    with the Metadata Wrangler.
    """

    def __init__(self, _db, collection, lookup=None):
        super(MetadataWranglerCollectionMonitor, self).__init__(
            _db, collection
        )
        self.lookup = lookup or MetadataWranglerOPDSLookup.from_config(
            self._db, collection=collection
        )
        self.importer = OPDSImporter(
            self._db, self.collection,
            data_source_name=DataSource.METADATA_WRANGLER,
            metadata_client=self.lookup, map_from_collection=True,
        )

    def get_response(self, url=None, **kwargs):
        try:
            if url:
                response = self.lookup._get(url)
            else:
                response = self.endpoint(**kwargs)
            self.lookup.check_content_type(response)
            return response
        except RemoteIntegrationException as e:
            self.log.error(
                "Error getting feed for %r: %s",
                self.collection, e.debug_message
            )
            self.keep_timestamp = False
            return None

    def endpoint(self, *args, **kwargs):
        raise NotImplementedError()


class MWCollectionUpdateMonitor(MetadataWranglerCollectionMonitor):
    """Retrieves updated metadata from the Metadata Wrangler"""

    SERVICE_NAME = "Metadata Wrangler Collection Updates"
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    def endpoint(self, timestamp):
        return self.lookup.updates(timestamp)

    def run_once(self, start, cutoff):
        if not self.lookup.authenticated:
            self.keep_timestamp = False
            return

        queue = [None]
        seen_links = set()

        new_timestamp = None
        while queue:
            url = queue.pop(0)
            if url in seen_links:
                continue
            next_links, editions, possible_new_timestamp = self.import_one_feed(
                start, url
            )
            if not new_timestamp or (
                    possible_new_timestamp
                    and possible_new_timestamp > new_timestamp
            ):
                # We imported an OPDS feed that included an entry
                # with a certain 'last updated' timestamp. We make the
                # assumption here that if there were an OPDS entry with
                # a prior timestamp, the server would have sent it to us
                # earlier. Thus, we can update the timestamp associated
                # with this Monitor so that the next time it runs, it
                # only asks for entries updated after this time.
                new_timestamp = possible_new_timestamp
            seen_links.add(url)

            # If we didn't import any editions, then don't add any of
            # the 'next' links found in this feed to the queue.
            if editions:
                for link in next_links:
                    if link not in seen_links:
                        queue.append(link)
            if new_timestamp:
                self.timestamp().timestamp = new_timestamp
            self._db.commit()
        return new_timestamp or self.timestamp().timestamp

    def import_one_feed(self, timestamp, url):
        response = self.get_response(url=url, timestamp=timestamp)
        if not response:
            return [], [], timestamp

        # Import the metadata
        raw_feed = response.text
        (editions, licensepools,
         works, errors) = self.importer.import_from_feed(raw_feed)

        # TODO: this oughtn't be necessary, because import_from_feed
        # already parsed the feed, but there's no way to access the
        # parsed copy.
        parsed = feedparser.parse(raw_feed)

        # Get last update times to set the timestamp.
        update_dates = self.importer.extract_last_update_dates(parsed)
        update_dates = [d[1] for d in update_dates]
        if update_dates:
            # We know that every entry updated before the earliest
            # date in the OPDS feed has been handled already, or the
            # server would have sent it and we would have an even
            # earlier date.
            timestamp = min(update_dates)

        # Add all links with rel='next' to the queue.
        next_links = self.importer.extract_next_links(parsed)
        return next_links, editions, timestamp


class MWAuxiliaryMetadataMonitor(MetadataWranglerCollectionMonitor):

    """Retrieves and processes requests for needed third-party metadata
    from the Metadata Wrangler.

    The Wrangler will only request metadata if it can't process an
    identifier from its own third-party resources. In these cases (e.g. ISBNs
    from Axis 360 or Bibliotheca), the wrangler will put out a call for metadata
    that it needs to process the identifier. This monitor answers that call.
    """

    SERVICE_NAME = "Metadata Wrangler Auxiliary Metadata Delivery"
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    def __init__(self, _db, collection, lookup=None, provider=None):
        super(MWAuxiliaryMetadataMonitor, self).__init__(
            _db, collection, lookup=lookup
        )
        self.parser = OPDSXMLParser()
        self.provider = provider or MetadataUploadCoverageProvider(
            collection, lookup_client=lookup
        )

    def endpoint(self):
        return self.lookup.metadata_needed()

    def run_once(self, start, cutoff):
        if not self.lookup.authenticated:
            self.keep_timestamp = False
            return

        queue = [None]
        seen_links = set()

        while queue:
            url = queue.pop(0)
            if url in seen_links:
                continue

            identifiers, next_links = self.get_identifiers(url=url)

            # Export metadata for the provided identifiers, but only if
            # they have a presentation-ready work. (This prevents creating
            # CoverageRecords for identifiers that don't actually have metadata
            # to send.)
            identifiers = [i for i in identifiers
                           if i.work and i.work.simple_opds_entry]
            self.provider.bulk_register(identifiers)
            self.provider.run_on_specific_identifiers(identifiers)

            seen_links.add(url)
            if identifiers:
                for link in next_links:
                    if link not in seen_links:
                        queue.append(link)

    def get_identifiers(self, url=None):
        """Pulls mapped identifiers from a feed of SimplifiedOPDSMessages."""
        response = self.get_response(url=url)
        feed = response.text

        etree_feed = etree.parse(StringIO(response.text))
        messages = self.importer.extract_messages(self.parser, etree_feed)

        urns = [m.urn for m in messages]
        identifiers_by_urn, _failures = Identifier.parse_urns(
            self._db, urns, autocreate=False
        )
        urns = identifiers_by_urn.keys()
        identifiers = identifiers_by_urn.values()

        self.importer.build_identifier_mapping(urns)
        mapped_identifiers = list()
        for identifier in identifiers:
            mapped_identifier = self.importer.identifier_mapping.get(
                identifier, identifier
            )
            mapped_identifiers.append(mapped_identifier)

        parsed_feed = feedparser.parse(feed)
        next_links = self.importer.extract_next_links(parsed_feed)
        return mapped_identifiers, next_links

class LoanlikeReaperMonitor(ReaperMonitor):

    SOURCE_OF_TRUTH_PROTOCOLS = [
        ODLWithConsolidatedCopiesAPI.NAME,
        SharedODLAPI.NAME,
        ExternalIntegration.OPDS_FOR_DISTRIBUTORS,
    ]

    @property
    def where_clause(self):
        """We never want to automatically reap loans or holds for situations
        where the circulation manager is the source of truth. If we
        delete something we shouldn't have, we won't be able to get
        the 'real' information back.

        This means loans of open-access content and loans from
        collections based on a protocol found in
        SOURCE_OF_TRUTH_PROTOCOLS.

        Subclasses will append extra clauses to this filter.
        """
        source_of_truth = or_(
            LicensePool.open_access==True,
            ExternalIntegration.protocol.in_(
                self.SOURCE_OF_TRUTH_PROTOCOLS
            )
        )

        source_of_truth_subquery = self._db.query(self.MODEL_CLASS.id).join(
            self.MODEL_CLASS.license_pool).join(
                LicensePool.collection).join(
                    ExternalIntegration, 
                    Collection.external_integration_id==ExternalIntegration.id
                ).filter(
                    source_of_truth
                )
        return ~self.MODEL_CLASS.id.in_(source_of_truth_subquery)


class LoanReaper(LoanlikeReaperMonitor):
    """Remove expired and abandoned loans from the database."""
    MODEL_CLASS = Loan
    MAX_AGE = 90

    @property
    def where_clause(self):
        """Find loans that have either expired, or that were created a long
        time ago and have no definite end date.
        """
        start_field = self.MODEL_CLASS.start
        end_field = self.MODEL_CLASS.end
        superclause = super(LoanReaper, self).where_clause
        now = datetime.datetime.utcnow()
        expired = end_field < now
        very_old_with_no_clear_end_date = and_(
            start_field < self.cutoff,
            end_field == None
        )
        return and_(superclause, or_(expired, very_old_with_no_clear_end_date))
ReaperMonitor.REGISTRY.append(LoanReaper)


class HoldReaper(LoanlikeReaperMonitor):
    """Remove seemingly abandoned holds from the database."""
    MODEL_CLASS = Hold
    MAX_AGE = 365

    @property
    def where_clause(self):
        """Find holds that were created a long time ago and either have
        no end date or have an end date in the past.

        The 'end date' for a hold is just an estimate, but if the estimate
        is in the future it's better to keep the hold around.
        """
        start_field = self.MODEL_CLASS.start
        end_field = self.MODEL_CLASS.end
        superclause = super(HoldReaper, self).where_clause
        end_date_in_past = end_field < datetime.datetime.utcnow()
        probably_abandoned = and_(
            start_field < self.cutoff,
            or_(end_field == None, end_date_in_past)
        )
        return and_(superclause, probably_abandoned)
ReaperMonitor.REGISTRY.append(HoldReaper)


class IdlingAnnotationReaper(ReaperMonitor):
    """Remove idling annotations for inactive loans."""

    MODEL_CLASS = Annotation
    TIMESTAMP_FIELD = 'timestamp'
    MAX_AGE = 60

    @property
    def where_clause(self):
        """The annotation must have motivation=IDLING, must be at least 60
        days old (meaning there has been no attempt to read the book
        for 60 days), and must not be associated with one of the
        patron's active loans or holds.
        """
        superclause = super(IdlingAnnotationReaper, self).where_clause

        restrictions = []
        for t in Loan, Hold:
            active_subquery = self._db.query(
                Annotation.id
            ).join(
                t,
                t.patron_id==Annotation.patron_id
            ).join(
                LicensePool,
                and_(LicensePool.id==t.license_pool_id,
                     LicensePool.identifier_id==Annotation.identifier_id)
            )
            restrictions.append(
                ~Annotation.id.in_(active_subquery)
            )
        return and_(
            superclause,
            Annotation.motivation==Annotation.IDLING,
            *restrictions
        )
ReaperMonitor.REGISTRY.append(IdlingAnnotationReaper)
