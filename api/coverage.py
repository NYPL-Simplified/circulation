import logging
from lxml import etree
from nose.tools import set_trace
from config import (
    Configuration,
    CannotLoadConfiguration,
)
from StringIO import StringIO
from core.coverage import (
    CoverageFailure,
    CollectionCoverageProvider,
    WorkCoverageProvider,
)
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import contains_eager
from core.model import (
    Collection,
    ConfigurationSetting,
    CoverageRecord,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    LicensePool,
    Session,
    WorkCoverageRecord,
)
from core.util.opds_writer import (
    OPDSFeed
)
from core.opds_import import (
    AccessNotAuthenticated,
    MetadataWranglerOPDSLookup,
    OPDSImporter,
    OPDSXMLParser,
    SimplifiedOPDSLookup,
)
from core.opds import AcquisitionFeed
from core.util.http import (
    RemoteIntegrationException,
)


class OPDSImportCoverageProvider(CollectionCoverageProvider):
    """Provide coverage for identifiers by looking them up, in batches,
    using the Simplified lookup protocol.
    """
    DEFAULT_BATCH_SIZE = 25
    
    def __init__(self, collection, lookup_client, **kwargs):
        """Constructor.

        :param lookup_client: A SimplifiedOPDSLookup object.
        """
        super(OPDSImportCoverageProvider, self).__init__(collection, **kwargs)
        self.lookup_client = lookup_client

    @classmethod
    def all(cls, _db, **kwargs):
        if cls.PROTOCOL and cls.DATA_SOURCE_NAME:
            qu = Collection.by_protocol(_db, cls.PROTOCOL)
            qu = qu.join(ExternalIntegration.settings).filter(
                ConfigurationSetting.key == Collection.DATA_SOURCE_NAME_SETTING,
                ConfigurationSetting.value == cls.DATA_SOURCE_NAME
            ).order_by(func.random())
            for collection in qu:
                yield cls(collection, **kwargs)
        else:
            for collection in super(OPDSImportCoverageProvider, cls).all(_db, **kwargs):
                yield collection

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

    def import_feed_response(self, response, id_mapping):
        """Confirms OPDS feed response and imports feed.
        """
        self.lookup_client.check_content_type(response)
        importer = OPDSImporter(self._db, self.collection,
                                identifier_mapping=id_mapping,
                                data_source_name=self.data_source.name)
        return importer.import_from_feed(response.text)


class MetadataWranglerCoverageProvider(OPDSImportCoverageProvider):

    """Make sure that the metadata wrangler has weighed in on all 
    Identifiers licensed to a Collection.
    """

    SERVICE_NAME = "Metadata Wrangler Coverage Provider"
    OPERATION = CoverageRecord.IMPORT_OPERATION
    DATA_SOURCE_NAME = DataSource.METADATA_WRANGLER
    INPUT_IDENTIFIER_TYPES = [
        Identifier.OVERDRIVE_ID,
        Identifier.BIBLIOTHECA_ID,
        Identifier.AXIS_360_ID,
        Identifier.ONECLICK_ID,
        Identifier.URI,
    ]
    
    def __init__(self, collection, lookup_client=None, **kwargs):
        _db = Session.object_session(collection)
        lookup_client = lookup_client or MetadataWranglerOPDSLookup.from_config(
            _db, collection=collection
        )

        super(MetadataWranglerCoverageProvider, self).__init__(
            collection, lookup_client, **kwargs
        )
        if not self.lookup_client.authenticated:
            self.log.warn(
                "Authentication for the Library Simplified Metadata Wrangler "
                "is not set up. You can still use the metadata wrangler, but "
                "it will not know which collection you're asking about."
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
                    Identifier.ONECLICK_ID
            ]:
                for e in identifier.equivalencies:
                    if e.output.type == Identifier.ISBN:
                        mapping[e.output] = identifier
                        break
            else:
                mapping[identifier] = identifier
        return mapping


class MetadataWranglerCollectionManager(MetadataWranglerCoverageProvider):

    def add_coverage_record_for(self, item):
        """Record this CoverageProvider's coverage for the given
        Edition/Identifier in the known Collection
        """
        return CoverageRecord.add_for(
            item, data_source=self.data_source, operation=self.operation,
            collection=self.collection
        )

    def failure(self, identifier, error, transient=True):
        """Create a CoverageFailure object with an associated Collection"""
        return CoverageFailure(
            identifier, error,
            data_source=self.data_source,
            transient=transient,
            collection=self.collection,
        )

    def _process_batch(self, client_method, success_codes, batch):
        results = list()
        id_mapping = self.create_identifier_mapping(batch)
        mapped_batch = id_mapping.keys()

        try:
            response = client_method(mapped_batch)
            self.lookup_client.check_content_type(response)
        except RemoteIntegrationException as e:
            return [self.failure(id_mapping[obj], e.debug_message)
                    for obj in mapped_batch]

        for message in self.process_feed_response(response, id_mapping):
            try:
                identifier, _new = Identifier.parse_urn(self._db, message.urn)
                mapped_batch.remove(identifier)
            except ValueError as e:
                # For some reason this URN can't be parsed. This
                # shouldn't happen.
                continue

            if message.status_code in success_codes:
                result = id_mapping[identifier]
                results.append(result)
            elif message.status_code == 400:
                # The URN couldn't be recognized. (This shouldn't happen,
                # since if we can parse it here, we can parse it on MW, too.)
                exception = "%s: %s" % (message.status_code, message.message)
                failure = self.failure(identifier, exception)
                results.append(failure)
            else:
                exception = "Unknown OPDSMessage status: %s" % message.status_code
                failure = self.failure(identifier, exception)
                results.append(failure)

        return results

    def process_feed_response(self, response, id_mapping):
        """Extracts messages from OPDS feed"""
        importer = OPDSImporter(
            self._db, self.collection, data_source_name=self.data_source.name,
            identifier_mapping=id_mapping
        )
        parser = OPDSXMLParser()
        root = etree.parse(StringIO(response.text))
        return importer.extract_messages(parser, root)


class MetadataWranglerCollectionSync(MetadataWranglerCollectionManager):
    """Adds identifiers from a local Collection to the remote Metadata
    Wrangler Collection
    """

    SERVICE_NAME = "Metadata Wrangler Sync"
    OPERATION = CoverageRecord.SYNC_OPERATION

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Retrieves items from the Collection that need to be synced
        with the Metadata Wrangler.
        """

        # Start with items in this Collection that have not been synced.
        uncovered = super(MetadataWranglerCoverageProvider, self)\
            .items_that_need_coverage(identifiers, **kwargs)

        # Make sure they're licensed by this collection.
        uncovered = uncovered.filter(
            or_(LicensePool.open_access, LicensePool.licenses_owned > 0)
        )

        # We'll be excluding items that have been reaped because we
        # stopped having a license.
        reaper_covered = self._db.query(Identifier)\
            .join(Identifier.coverage_records)\
            .filter(
                CoverageRecord.data_source==self.data_source,
                CoverageRecord.collection_id==self.collection_id,
                CoverageRecord.operation==CoverageRecord.REAP_OPERATION
            )

        # But we'll be _including_ items that were reaped but have since been
        # relicensed or otherwise added back to the collection.
        relicensed = reaper_covered.join(Identifier.licensed_through).filter(
                LicensePool.collection_id==self.collection_id,
                or_(LicensePool.licenses_owned > 0, LicensePool.open_access)
            ).options(contains_eager(Identifier.coverage_records))

        # Remove MetadataWranglerCollectionReaper coverage records from
        # relicensed identifiers. This ensures that we can get Metadata
        # Wrangler coverage for books that have had their licenses repurchased
        # or extended.
        for identifier in relicensed.all():
            for record in identifier.coverage_records:
                if (record.data_source==self.data_source and
                    record.collection_id==self.collection_id and
                    record.operation==CoverageRecord.REAP_OPERATION):
                    # Delete any reaper CoverageRecord for this Identifier
                    # in this Collection.
                    self._db.delete(record)

        # We want all items that don't have a SYNC coverage record, so
        # long as they're also missing a REAP coverage record (uncovered).
        # But if we have licenses for them (relicensed), we want them
        # even if they do have a REAP coverage record.
        return uncovered.except_(reaper_covered).union(relicensed)

    def process_batch(self, batch):
        # Success codes:
            # - 200: It's already in the remote Collection.
            # - 201: It was added successfully.
        return self._process_batch(
            self.lookup_client.add, (200, 201), batch
        )


class MetadataWranglerCollectionReaper(MetadataWranglerCollectionManager):
    """Removes unlicensed identifiers from the remote Metadata Wrangler
    Collection
    """

    SERVICE_NAME = "Metadata Wrangler Reaper"
    OPERATION = CoverageRecord.REAP_OPERATION

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Retrieves Identifiers that were synced but are no longer licensed.
        """
        qu = self._db.query(Identifier).select_from(LicensePool).\
            join(LicensePool.identifier).join(CoverageRecord).\
            filter(LicensePool.collection_id==self.collection_id).\
            filter(LicensePool.licenses_owned==0, LicensePool.open_access!=True).\
            filter(CoverageRecord.data_source==self.data_source).\
            filter(CoverageRecord.operation==CoverageRecord.SYNC_OPERATION).\
            filter(CoverageRecord.status==CoverageRecord.SUCCESS).\
            filter(CoverageRecord.collection==self.collection)

        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))
        return qu

    def process_batch(self, batch):
        # Success codes:
            # - 200: It was removed successfully.
            # - 404: It wasn't found in the remote Collection
        return self._process_batch(
            self.lookup_client.remove, (200, 404), batch
        )

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


class MetadataUploadCoverageProvider(CollectionCoverageProvider):
    """Provide coverage for identifiers by uploading OPDS metadata to
    the metadata wrangler.
    """
    DEFAULT_BATCH_SIZE = 25
    SERVICE_NAME = "Metadata Upload Coverage Provider"
    OPERATION = CoverageRecord.METADATA_UPLOAD_OPERATION
    DATA_SOURCE_NAME = DataSource.INTERNAL_PROCESSING
    
    def __init__(self, collection, upload_client=None, **kwargs):
        _db = Session.object_session(collection)
        self.upload_client = upload_client or MetadataWranglerOPDSLookup.from_config(
            _db, collection=collection
        )

        super(MetadataUploadCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if not self.upload_client.authenticated:
            raise CannotLoadConfiguration(
                "Authentication for the Library Simplified Metadata Wrangler "
                "is not set up. You can't upload metadata without authenticating."
            )
    
    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Find all identifiers lacking coverage from this CoverageProvider.
        Only identifiers that have CoverageRecords in the 'transient
        failure' state will be returned. Unlike with other
        CoverageProviders, Identifiers that have no CoverageRecord at
        all will not be processed.
        """
        qu = super(MetadataUploadCoverageProvider, self).items_that_need_coverage(
            identifiers=identifiers, **kwargs
        )
        qu = qu.filter(CoverageRecord.id != None)
        return qu

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
        self.upload_client.add_with_metadata(feed)
        
        # We grant coverage for all identifiers if the upload doesn't raise an exception.
        return results

    def process_item(self, identifier):
        """Handle an individual item (e.g. through ensure_coverage) as a very
        small batch. Not efficient, but it works.
        """
        [result] = self.process_batch([identifier])
        return result

class ContentServerBibliographicCoverageProvider(OPDSImportCoverageProvider):
    """Make sure our records for open-access books match what the content
    server says.
    """
    SERVICE_NAME = "Open-access content server bibliographic coverage provider"
    DATA_SOURCE_NAME = DataSource.OA_CONTENT_SERVER
    INPUT_IDENTIFIER_TYPES = None

    PROTOCOL = ExternalIntegration.OPDS_IMPORT
    
    def __init__(self, collection, lookup_client, *args, **kwargs):
        if not lookup_client:
            content_server_url = (
                Configuration.integration_url(
                    Configuration.CONTENT_SERVER_INTEGRATION
                )
            )
            lookup_client = SimplifiedOPDSLookup(content_server_url)
        super(ContentServerBibliographicCoverageProvider, self).__init__(
            collection, lookup_client, *args, **kwargs
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
    
    def __init__(self, collection, *args, **kwargs):
        super(MockOPDSImportCoverageProvider, self).__init__(
            collection, None, *args, **kwargs
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
