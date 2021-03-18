# Code relating to the interaction between the circulation manager
# and the metadata wrangler.
import datetime
import feedparser
from io import StringIO
from lxml import etree

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import (
    aliased,
    contains_eager,
)

from .config import CannotLoadConfiguration
from core.coverage import CoverageFailure
from core.metadata_layer import TimestampData
from core.model import (
    CoverageRecord,
    DataSource,
    Identifier,
    LicensePool,
    Session,
    Timestamp,
)
from core.monitor import (
    CollectionMonitor,
)
from core.opds import AcquisitionFeed
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    OPDSImporter,
    OPDSXMLParser,
)

from core.util.http import RemoteIntegrationException

from api.coverage import (
    OPDSImportCoverageProvider,
    RegistrarImporter,
    ReaperImporter,
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
            raise e

    def endpoint(self, *args, **kwargs):
        raise NotImplementedError()

    def assert_authenticated(self):
        """Raise an exception unless the client has authentication
        credentials.

        Raising an exception will keep the Monitor timestamp from
        being updated.
        """
        if not self.lookup.authenticated:
            raise Exception(
                "Cannot get updates from metadata wrangler -- no authentication credentials provided."
            )


class MWCollectionUpdateMonitor(MetadataWranglerCollectionMonitor):
    """Retrieves updated metadata from the Metadata Wrangler"""

    SERVICE_NAME = "Metadata Wrangler Collection Updates"
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    def endpoint(self, timestamp):
        return self.lookup.updates(timestamp)

    def run_once(self, progress):
        """Ask the metadata wrangler about titles that have changed
        since the last time this monitor ran.

        :param progress: A TimestampData representing the span of time
            covered during the previous run of this monitor.

        :return: A modified TimestampData.
        """
        start = progress.finish
        self.assert_authenticated()
        queue = [None]
        seen_links = set()
        total_editions = 0

        new_timestamp = None
        while queue:
            url = queue.pop(0)
            if url in seen_links:
                continue
            next_links, editions, possible_new_timestamp = self.import_one_feed(
                start, url
            )
            total_editions += len(editions)
            achievements = "Editions processed: %s" % total_editions
            if not new_timestamp or (
                    possible_new_timestamp
                    and possible_new_timestamp > new_timestamp
            ):
                # We imported an OPDS feed that included an entry with
                # a certain 'last updated' timestamp (or was empty but
                # included a feed-level 'last updated' timestamp. We
                # make the assumption here that if there were an OPDS
                # entry with a timestamp prior to this one, the server
                # would have sent it to us already. Thus, we can
                # update the timestamp associated with this Monitor so
                # that the next time it runs, it only asks for entries
                # updated after this time.
                new_timestamp = possible_new_timestamp
            seen_links.add(url)

            # If we didn't import any editions, then don't add any of
            # the 'next' links found in this feed to the queue.
            if editions:
                for link in next_links:
                    if link not in seen_links:
                        queue.append(link)

            # Immediately update the timestamps table so that a later
            # crash doesn't mean we have to redo this work.
            if new_timestamp not in (None, Timestamp.CLEAR_VALUE):
                timestamp_obj = self.timestamp()
                timestamp_obj.finish = new_timestamp
                timestamp_obj.achievements = achievements
            self._db.commit()

        # The TimestampData we return is going to be written to the database.
        # Unlike most Monitors, there are times when we just don't
        # want that to happen.
        #
        # If we found an OPDS feed, the latest timestamp in that feed
        # should be used as Timestamp.finish.
        #
        # Otherwise, the existing timestamp.finish should be used. If
        # that value happens to be None, we need to set
        # TimestampData.finish to CLEAR_VALUE to make sure it ends up
        # as None (rather than the current time).
        finish = new_timestamp or self.timestamp().finish or Timestamp.CLEAR_VALUE
        progress.start = start
        progress.finish = finish
        progress.achievements = achievements
        return progress

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
        else:
            # Look for a timestamp on the feed level.
            feed_timestamp = self.importer._datetime(
                parsed['feed'], 'updated_parsed'
            )

            # Subtract one day from the time to reduce the chance of
            # race conditions. Otherwise, work done but not committed
            # to the database might result in a new entry showing up
            # with an earlier timestamp than this.
            if feed_timestamp:
                timestamp = feed_timestamp - datetime.timedelta(days=1)

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

    def run_once(self, progress):
        self.assert_authenticated()

        queue = [None]
        seen_links = set()

        total_identifiers_processed = 0
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
            total_identifiers_processed += len(identifiers)
            self.provider.bulk_register(identifiers)
            self.provider.run_on_specific_identifiers(identifiers)

            seen_links.add(url)
            if identifiers:
                for link in next_links:
                    if link not in seen_links:
                        queue.append(link)
        achievements = "Identifiers processed: %d" % total_identifiers_processed
        return TimestampData(achievements=achievements)

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
        urns = list(identifiers_by_urn.keys())
        identifiers = list(identifiers_by_urn.values())

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


class BaseMetadataWranglerCoverageProvider(OPDSImportCoverageProvider):
    """Makes sure the metadata wrangler knows about all Identifiers
    licensed to a Collection.

    This has two subclasses: MetadataWranglerCollectionRegistrar
    (which adds Identifiers from a circulation manager's catalog to
    the corresponding catalog on the metadata wrangler) and
    MetadataWranglerCollectionReaper (which removes Identifiers from
    the metadata wrangler catalog once they no longer exist in the
    circulation manager's catalog).
    """

    DATA_SOURCE_NAME = DataSource.METADATA_WRANGLER

    # We want to register a given identifier once for every
    # collection it's catalogued under.
    COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False

    INPUT_IDENTIFIER_TYPES = [
        Identifier.OVERDRIVE_ID,
        Identifier.BIBLIOTHECA_ID,
        Identifier.AXIS_360_ID,
        Identifier.RB_DIGITAL_ID,
        Identifier.URI,
    ]

    def __init__(self, collection, lookup_client=None, **kwargs):
        """Since we are processing a specific collection, we must be able to
        get an _authenticated_ metadata wrangler lookup client for the
        collection.
        """
        _db = Session.object_session(collection)
        lookup_client = lookup_client or MetadataWranglerOPDSLookup.from_config(
            _db, collection=collection
        )
        super(BaseMetadataWranglerCoverageProvider, self).__init__(
            collection, lookup_client, **kwargs
        )
        if not self.lookup_client.authenticated:
            raise CannotLoadConfiguration(
                "Authentication for the Library Simplified Metadata Wrangler "
                "is not set up. Without this, there is no way to register "
                "your identifiers with the metadata wrangler."
            )

    def create_identifier_mapping(self, batch):
        """The metadata wrangler can look up ISBNs and Overdrive
        identifiers. All other identifier types need to be mapped to
        ISBNs.
        """
        mapping = dict()
        for identifier in batch:
            if identifier.type in [
                    Identifier.AXIS_360_ID, Identifier.BIBLIOTHECA_ID,
                    Identifier.RB_DIGITAL_ID
            ]:
                for e in identifier.equivalencies:
                    if e.output.type == Identifier.ISBN:
                        mapping[e.output] = identifier
                        break
            else:
                mapping[identifier] = identifier
        return mapping


class MetadataWranglerCollectionRegistrar(BaseMetadataWranglerCoverageProvider):
    """Register all Identifiers licensed to a Collection with the
    metadata wrangler.

    If OPDS metadata is immediately returned, make use of it. Even if
    no metadata is returned for an Identifier, mark it as covered.

    Once it's registered, any future updates to the available metadata
    for a given Identifier will be detected by the
    MWCollectionUpdateMonitor.
    """

    SERVICE_NAME = "Metadata Wrangler Collection Registrar"
    OPERATION = CoverageRecord.IMPORT_OPERATION
    OPDS_IMPORTER_CLASS = RegistrarImporter

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Retrieves items from the Collection that are not registered with
        the Metadata Wrangler.
        """

        # Start with all items in this Collection that have not been
        # registered.
        uncovered = super(MetadataWranglerCollectionRegistrar, self)\
            .items_that_need_coverage(identifiers, **kwargs)
        # Make sure they're actually available through this
        # collection.
        uncovered = uncovered.filter(
            or_(LicensePool.open_access, LicensePool.licenses_owned > 0)
        )

        # Exclude items that have been reaped because we stopped
        # having a license.
        reaper_covered = self._db.query(Identifier)\
            .join(Identifier.coverage_records)\
            .filter(
                CoverageRecord.data_source_id==self.data_source.id,
                CoverageRecord.collection_id==self.collection_id,
                CoverageRecord.operation==CoverageRecord.REAP_OPERATION
            )

        # If any items were reaped earlier but have since been
        # relicensed or otherwise added back to the collection, remove
        # their reaper CoverageRecords. This ensures we get Metadata
        # Wrangler coverage for books that have had their licenses
        # repurchased or extended.
        relicensed = reaper_covered.join(Identifier.licensed_through).filter(
                LicensePool.collection_id==self.collection_id,
                or_(LicensePool.licenses_owned > 0, LicensePool.open_access)
            ).options(contains_eager(Identifier.coverage_records))

        needs_commit = False
        for identifier in relicensed.all():
            for record in identifier.coverage_records:
                if (record.data_source_id==self.data_source.id and
                    record.collection_id==self.collection_id and
                    record.operation==CoverageRecord.REAP_OPERATION):
                    # Delete any reaper CoverageRecord for this Identifier
                    # in this Collection.
                    self._db.delete(record)
                    needs_commit = True
        if needs_commit:
            self._db.commit()

        # We want all items that don't have a IMPORT coverage record, so
        # long as they're also missing a REAP coverage record (uncovered).
        # If they were relicensed, we just removed the REAP coverage
        # record.
        return uncovered.except_(reaper_covered).order_by(Identifier.id)


class MetadataWranglerCollectionReaper(BaseMetadataWranglerCoverageProvider):
    """Removes unlicensed identifiers from the remote Metadata Wrangler
    Collection
    """

    SERVICE_NAME = "Metadata Wrangler Reaper"
    OPERATION = CoverageRecord.REAP_OPERATION
    OPDS_IMPORTER_CLASS = ReaperImporter

    @property
    def api_method(self):
        return self.lookup_client.remove

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Retrieves Identifiers that were imported but are no longer licensed.
        """
        qu = self._db.query(Identifier).select_from(LicensePool).\
            join(LicensePool.identifier).join(CoverageRecord).\
            filter(LicensePool.collection_id==self.collection_id).\
            filter(LicensePool.licenses_owned==0, LicensePool.open_access!=True).\
            filter(CoverageRecord.data_source_id==self.data_source.id).\
            filter(CoverageRecord.operation==CoverageRecord.IMPORT_OPERATION).\
            filter(CoverageRecord.status==CoverageRecord.SUCCESS).\
            filter(CoverageRecord.collection==self.collection)

        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))
        return qu

    def finalize_batch(self):
        """Deletes Metadata Wrangler coverage records of reaped Identifiers

        This allows Identifiers to be added to the collection again via
        MetadataWranglerCoverageProvider lookup if a license is repurchased.
        """
        # Join CoverageRecord against an alias of itself to find all
        # 'import' CoverageRecords that have been obviated by a
        # 'reaper' coverage record for the same Identifier.
        reaper_coverage = aliased(CoverageRecord)
        qu = self._db.query(CoverageRecord).join(
            reaper_coverage,
            CoverageRecord.identifier_id==reaper_coverage.identifier_id

        # The CoverageRecords were selecting are 'import' records.
        ).filter(
            CoverageRecord.data_source_id==self.data_source.id
        ).filter(
            CoverageRecord.operation==CoverageRecord.IMPORT_OPERATION

        # And we're only selecting them if there's also a 'reaper'
        # coverage record.
        ).filter(
            reaper_coverage.data_source_id==self.data_source.id
        ).filter(
            reaper_coverage.operation==CoverageRecord.REAP_OPERATION
        )

        # Delete all 'import' CoverageRecords that have been reaped.
        for record in qu:
            self._db.delete(record)
        super(MetadataWranglerCollectionReaper, self).finalize_batch()


class MetadataUploadCoverageProvider(BaseMetadataWranglerCoverageProvider):
    """Provide coverage for identifiers by uploading OPDS metadata to
    the metadata wrangler.
    """
    DEFAULT_BATCH_SIZE = 25
    SERVICE_NAME = "Metadata Upload Coverage Provider"
    OPERATION = CoverageRecord.METADATA_UPLOAD_OPERATION
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING

    def __init__(self, *args, **kwargs):
        kwargs['registered_only'] = kwargs.get('registered_only', True)
        super(MetadataUploadCoverageProvider, self).__init__(*args, **kwargs)

    def process_batch(self, batch):
        """Create an OPDS feed from a batch and upload it to the metadata client."""
        works = []
        results = []
        for identifier in batch:
            work = self.work(identifier)
            if not isinstance(work, CoverageFailure):
                works.append(work)
                results.append(identifier)
            else:
                results.append(work)
        feed = AcquisitionFeed(self._db, "Metadata Upload Feed", "", works, None)
        self.lookup_client.add_with_metadata(feed)

        # We grant coverage for all identifiers if the upload doesn't raise an exception.
        return results
