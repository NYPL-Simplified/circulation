import logging
from lxml import etree
from nose.tools import set_trace
from config import Configuration
from StringIO import StringIO
from core.coverage import (
    CoverageFailure,
    CollectionCoverageProvider,
    WorkCoverageProvider,
)
from sqlalchemy import and_
from sqlalchemy.orm import contains_eager
from core.model import (
    Collection,
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    WorkCoverageRecord,
)
from core.util.opds_writer import (
    OPDSFeed
)
from core.external_search import (
    ExternalSearchIndex,
)
from core.opds_import import (
    AccessNotAuthenticated,
    SimplifiedOPDSLookup,
    OPDSImporter,
    OPDSXMLParser,
)

from core.util.http import BadResponseException


class OPDSImportCoverageProvider(CollectionCoverageProvider):
    """Provide coverage for identifiers by looking them up, in batches,
    using the Simplified lookup protocol.
    """
    DEFAULT_BATCH_SIZE = 25
    
    def __init__(self, _db, collection, lookup_client, **kwargs):
        """Constructor.

        :param lookup_client: A SimplifiedOPDSLookup object.
        """
        super(OPDSImportCoverageProvider, self).__init__(collection, **kwargs)
        self.lookup_client = lookup_client

    def process_batch(self, batch):
        """Perform a Simplified lookup and import the resulting OPDS feed."""
        imported_editions, pools, works, error_messages_by_id = self.lookup_and_import_batch(
            batch
        )

        results = []
        imported_identifiers = set()
        # We grant coverage if an Edition was created from the operation.
        for edition in imported_editions:
            identifier = edition.primary_identifier
            results.append(identifier)
            imported_identifiers.add(identifier)

        # The operation may also have updated information from a
        # number of LicensePools.
        for pool in pools:
            identifier = pool.identifier
            if identifier in imported_identifiers:
                self.finalize_license_pool(pool)
            else:
                msg = "OPDS import operation imported LicensePool, but no Edition."
                results.append(
                    self.failure(identifier, msg, transient=True)
                )

        # Failures during the OPDS import process are propagated.
        for failure in error_messages_by_id.values():
            results.append(failure)
        return results

    def process_item(self, identifier):
        """Handle an individual item (e.g. through ensure_coverage) as a very
        small batch. Not efficient, but it works.
        """
        [result] = self.process_batch([identifier])
        return result

    def finalize_license_pool(self, pool):
        """An OPDS entry was matched with a LicensePool. Do something special
        to mark the occasion.

        By default, nothing happens.
        """
        pass

    def lookup_and_import_batch(self, batch):
        """Look up a batch of identifiers and parse the resulting OPDS feed.

        This method is overridden by MockOPDSImportCoverageProvider.
        """
        # id_mapping maps our local identifiers to identifiers the
        # foreign data source will reocgnize.
        id_mapping = self.create_identifier_mapping(batch)
        if id_mapping:
            foreign_identifiers = id_mapping.keys()
        else:
            foreign_identifiers = batch

        response = self.lookup_client.lookup(foreign_identifiers)

        # import_feed_response takes id_mapping so it can map the
        # foreign identifiers back to their local counterparts.
        return self.import_feed_response(response, id_mapping)

    def create_identifier_mapping(self, batch):
        """Map the internal identifiers used for books to the corresponding
        identifiers used by the lookup client.

        By default, no identifier mapping is needed.
        """
        return None
    
    def check_content_type(self, response):
        content_type = response.headers.get('content-type')
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise BadResponseException.from_response(
                response.url, 
                "Wrong media type: %s" % content_type,
                response
            )

    def import_feed_response(self, response, id_mapping):
        """Confirms OPDS feed response and imports feed.
        """
        self.check_content_type(response)
        importer = OPDSImporter(self._db, self.collection,
                                identifier_mapping=id_mapping,
                                data_source_name=self.data_source.name)
        return importer.import_from_feed(response.text)


class MetadataWranglerCoverageProvider(OPDSImportCoverageProvider):

    """Make sure that the metadata wrangler has weighed in on all 
    Identifiers licensed to a Collection.
    """

    SERVICE_NAME = "Metadata Wrangler Coverage Provider"
    OPERATION = CoverageRecord.SYNC_OPERATION
    DATA_SOURCE_NAME = DataSource.METADATA_WRANGLER
    INPUT_IDENTIFIER_TYPES = [
        Identifier.OVERDRIVE_ID, 
        Identifier.THREEM_ID,
        Identifier.GUTENBERG_ID, 
        Identifier.AXIS_360_ID,
        Identifier.ONECLICK_ID, 
    ]
    
    def __init__(self, _db, collection, lookup_client, **kwargs):
        super(MetadataWranglerCoverageProvider, self).__init__(
            _db, collection, lookup_client, **kwargs
        )
        if not self.lookup_client.authenticated:
            self.log.warn(
                "Authentication for the Library Simplified Metadata Wrangler "
                "is not set up. You can still use the metadata wrangler, but "
                "it will not know which collection you're asking about."
            )

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Returns items that need to have their metadata looked up.
        """

        # Start with items in this Collection that have never had
        # metadata coverage.
        uncovered = super(
            MetadataWranglerCoverageProvider, self).items_that_need_coverage(
                identifiers, **kwargs
            )

        # We'll be excluding items that have been reaped because we
        # stopped having a license.
        reaper_covered = self._db.query(Identifier).\
                join(Identifier.coverage_records).\
                filter(CoverageRecord.data_source==self.data_source).\
                filter(CoverageRecord.operation==CoverageRecord.REAP_OPERATION)

        # But we'll be _including_ items that were reaped and then we
        # got the license back.
        relicensed = reaper_covered.join(Identifier.licensed_through).\
                filter(LicensePool.licenses_owned > 0).\
                options(contains_eager(Identifier.coverage_records))

        # Remove MetadataWranglerCollectionReaper coverage records from
        # relicensed identifiers. This ensures that we can get Metadata
        # Wrangler coverage for books that have had their licenses repurchased
        # or extended.
        for identifier in relicensed.all():
            [reaper_coverage_record] = [record
                    for record in identifier.coverage_records
                    if (record.data_source==self.data_source and
                        record.operation==CoverageRecord.REAP_OPERATION)]
            self._db.delete(reaper_coverage_record)

        # We want all items that don't have a SYNC coverage record, so
        # long as they're also missing a REAP coverage record. But if
        # we have licenses for them, we want them even if they do have
        # a REAP coverage record.
        return uncovered.except_(reaper_covered).union(relicensed)

    def create_identifier_mapping(self, batch):
        """The metadata wrangler can look up URIs, Gutenberg identifiers,
        and Overdrive identifiers. All other identifier types need to be
        mapped to ISBNs.
        """
        mapping = dict()
        for identifier in batch:
            if identifier.type in [
                    Identifier.AXIS_360_ID, Identifier.THREEM_ID,
                    Identifier.ONECLICK_ID
            ]:
                for e in identifier.equivalencies:
                    if e.output.type == Identifier.ISBN:
                        mapping[e.output] = identifier
                        break
            else:
                mapping[identifier] = identifier
        return mapping


class MetadataWranglerCollectionReaper(MetadataWranglerCoverageProvider):
    """Removes unlicensed identifiers from the Metadata Wrangler collection"""

    SERVICE_NAME = "Metadata Wrangler Reaper"
    OPERATION = CoverageRecord.REAP_OPERATION

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Retrieves Identifiers that were synced but are no longer licensed.
        """
        qu = self._db.query(Identifier).select_from(LicensePool).\
            join(LicensePool.identifier).join(CoverageRecord).\
            filter(LicensePool.licenses_owned==0, LicensePool.open_access!=True).\
            filter(CoverageRecord.data_source==self.data_source).\
            filter(CoverageRecord.operation==CoverageRecord.SYNC_OPERATION).\
            filter(CoverageRecord.status==CoverageRecord.SUCCESS)

        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))
        return qu

    def process_batch(self, batch):
        id_mapping = self.create_identifier_mapping(batch)
        batch = id_mapping.keys()
        response = self.lookup.remove(batch)
        return self.process_feed_response(response, id_mapping)

    def process_feed_response(self, response, id_mapping):
        """Confirms OPDS feed response and extracts messages.
        """        
        self.check_content_type(response)
        importer = OPDSImporter(
            self._db, self.collection, data_source_name=self.data_source.name,
            identifier_mapping=id_mapping
        )
        parser = OPDSXMLParser()
        root = etree.parse(StringIO(response.text))
        return importer.extract_messages(parser, root)

    def finalize_batch(self):
        """Deletes Metadata Wrangler coverage records of reaped Identifiers

        This allows Identifiers to be added to the collection again via
        MetadataWranglerCoverageProvider lookup if a license is repurchased.
        """
        qu = self._db.query(Identifier.id).join(Identifier.coverage_records)
        reaper_covered = qu.filter(
            CoverageRecord.data_source==self.data_source,
            CoverageRecord.operation==CoverageRecord.REAP_OPERATION
        )
        wrangler_covered = qu.filter(
            CoverageRecord.data_source==self.data_source,
            CoverageRecord.operation==CoverageRecord.SYNC_OPERATION
        )
        # Get the db ids of identifiers that have been both synced and reaped.
        subquery = reaper_covered.intersect(wrangler_covered).subquery()

        # Retrieve the outdated syncing coverage record and delete it.
        coverage_records = self._db.query(CoverageRecord).\
                join(CoverageRecord.identifier).\
                join(subquery, Identifier.id.in_(subquery)).\
                filter(
                    CoverageRecord.data_source==self.data_source,
                    CoverageRecord.operation==CoverageRecord.SYNC_OPERATION
                )
        for record in coverage_records.all():
            self._db.delete(record)
        super(MetadataWranglerCollectionReaper, self).finalize_batch()


class ContentServerBibliographicCoverageProvider(OPDSImportCoverageProvider):
    """Make sure our records for open-access books match what the content
    server says.
    """
    SERVICE_NAME = "Open-access content server bibliographic coverage provider"
    DATA_SOURCE_NAME = DataSource.OA_CONTENT_SERVER
    INPUT_IDENTIFIER_TYPES = None
    
    def __init__(self, _db, collection, lookup_client, *args, **kwargs):
        if not lookup_client:
            content_server_url = (
                Configuration.integration_url(
                    Configuration.CONTENT_SERVER_INTEGRATION
                )
            )
            lookup_client = SimplifiedOPDSLookup(content_server_url)
        super(ContentServerBibliographicCoverageProvider, self).__init__(
            _db, collection, lookup_client, *args, **kwargs
        )

    def finalize_license_pool(self, license_pool):
        """Ensure that a LicensePool successfully imported from the content
        server has a presentation-ready Work.
        """
        work, new_work = license_pool.calculate_work(even_if_no_author=True)
        work.set_presentation_ready()
        
    def items_that_need_coverage(self, *args, **kwargs):
        """Only identifiers already associated with an open-access LicensePool
        need coverage.
        """
        qu = super(ContentServerBibliographicCoverageProvider, 
                   self).items_that_need_coverage(*args, **kwargs)
        qu = qu.join(Identifier.licensed_through).filter(
            LicensePool.open_access==True
        )
        return qu


class MockOPDSImportCoverageProvider(OPDSImportCoverageProvider):

    SERVICE_NAME = "Mock Provider"
    DATA_SOURCE_NAME = DataSource.OA_CONTENT_SERVER
    
    def __init__(self, _db, collection, *args, **kwargs):
        super(MockOPDSImportCoverageProvider, self).__init__(
            _db, collection, None, *args, **kwargs
        )
        self.batches = []
        self.finalized = []
        self.import_results = []

    def queue_import_results(self, editions, pools, works, messages_by_id):
        self.import_results.insert(0, (editions, pools, works, messages_by_id))

    def finalize_license_pool(self, license_pool):
        self.finalized.append(license_pool)
        super(MockOPDSImportCoverageProvider, self).finalize_license_pool(
            license_pool
        )

    def lookup_and_import_batch(self, batch):
        self.batches.append(batch)
        return self.import_results.pop()
