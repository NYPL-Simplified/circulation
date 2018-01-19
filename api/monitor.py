import datetime
import feedparser
import logging
import os
import sys
from lxml import etree
from nose.tools import set_trace
from StringIO import StringIO

from sqlalchemy import or_

from core.monitor import (
    CollectionMonitor,
    EditionSweepMonitor,
)
from core.model import (
    DataSource,
    Edition,
    Identifier,
    LicensePool,
)
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    OPDSImporter,
    OPDSXMLParser,
)
from core.util.http import RemoteIntegrationException


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


class MWUpdateMonitor(MetadataWranglerCollectionMonitor):
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
