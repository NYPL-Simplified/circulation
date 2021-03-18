"""Base classes for CoverageProviders.

The CoverageProviders themselves are in the file corresponding to the
service that needs coverage -- overdrive.py, metadata_wrangler.py, and
so on.
"""
import logging
from lxml import etree
from io import StringIO
from core.coverage import (
    CoverageFailure,
    CollectionCoverageProvider,
    WorkCoverageProvider,
)
from core.model import (
    Collection,
    ConfigurationSetting,
    CoverageRecord,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    LicensePool,
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
from core.util.http import (
    RemoteIntegrationException,
)


class RegistrarImporter(OPDSImporter):
    """We are successful whenever the metadata wrangler puts an identifier
    into the catalog, even if no metadata is immediately available.
    """
    SUCCESS_STATUS_CODES = [200, 201, 202]


class ReaperImporter(OPDSImporter):
    """We are successful if the metadata wrangler acknowledges that an
    identifier has been removed, and also if the identifier wasn't in
    the catalog in the first place.
    """
    SUCCESS_STATUS_CODES = [200, 404]


class OPDSImportCoverageProvider(CollectionCoverageProvider):
    """Provide coverage for identifiers by looking them up, in batches,
    using the Simplified lookup protocol.
    """
    DEFAULT_BATCH_SIZE = 25
    OPDS_IMPORTER_CLASS = OPDSImporter

    def __init__(self, collection, lookup_client, **kwargs):
        """Constructor.

        :param lookup_client: A SimplifiedOPDSLookup object.
        """
        super(OPDSImportCoverageProvider, self).__init__(collection, **kwargs)
        self.lookup_client = lookup_client

    def process_batch(self, batch):
        """Perform a Simplified lookup and import the resulting OPDS feed."""
        (imported_editions, pools, works,
         error_messages_by_id) = self.lookup_and_import_batch(batch)

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

        # Anything left over is either a CoverageFailure, or an
        # Identifier that used to be a CoverageFailure, indicating
        # that a simplified:message that a normal OPDSImporter would
        # consider a 'failure' should actually be considered a
        # success.
        for failure_or_identifier in sorted(error_messages_by_id.values()):
            if isinstance(failure_or_identifier, CoverageFailure):
                failure_or_identifier.collection = self.collection_or_not
            results.append(failure_or_identifier)
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

    @property
    def api_method(self):
        """The method to call to fetch an OPDS feed from the remote server.
        """
        return self.lookup_client.lookup

    def lookup_and_import_batch(self, batch):
        """Look up a batch of identifiers and parse the resulting OPDS feed.

        This method is overridden by MockOPDSImportCoverageProvider.
        """
        # id_mapping maps our local identifiers to identifiers the
        # foreign data source will reocgnize.
        id_mapping = self.create_identifier_mapping(batch)
        if id_mapping:
            foreign_identifiers = list(id_mapping.keys())
        else:
            foreign_identifiers = batch

        response = self.api_method(foreign_identifiers)

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
        """Confirms OPDS feed response and imports feed through
        the appropriate OPDSImporter subclass.
        """
        self.lookup_client.check_content_type(response)
        importer = self.OPDS_IMPORTER_CLASS(
            self._db, self.collection,
            identifier_mapping=id_mapping,
            data_source_name=self.data_source.name
        )
        return importer.import_from_feed(response.text)


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
