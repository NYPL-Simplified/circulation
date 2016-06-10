import logging
from nose.tools import set_trace
from config import Configuration
from core.coverage import (
    CoverageFailure,
    CoverageProvider,
    WorkCoverageProvider,
)
from sqlalchemy import and_
from sqlalchemy.orm import contains_eager
from core.model import (
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    WorkCoverageRecord,
)
from core.opds import (
    OPDSFeed
)
from core.external_search import (
    ExternalSearchIndex,
)
from core.opds_import import (
    AccessNotAuthenticated,
    SimplifiedOPDSLookup,
    OPDSImporter,
)

from core.util.http import BadResponseException


class OPDSImportCoverageProvider(CoverageProvider):
    """Provide coverage for identifiers by looking them up, in batches,
    using the Simplified lookup protocol and importing the
    corresponding OPDS feeds.
    """

    def __init__(self, service_name, input_identifier_types, output_source,
                 lookup=None, batch_size=25, expect_license_pool=False,
                 presentation_ready_on_success=False, **kwargs):
        """Basic constructor.

        :param expect_license_pool: Would we expect the import operation to
          create a LicensePool for an identifier where no LicensePool currently exists?

        :param presentation_ready_on_success: If we manage to create a
          Work because of this import, should the Work be considered
          presentation-ready?

        """
        self.lookup = lookup
        self.expect_license_pool=expect_license_pool
        self.presentation_ready_on_success=presentation_ready_on_success
        super(OPDSImportCoverageProvider, self).__init__(
            service_name, input_identifier_types, output_source, batch_size=batch_size, 
            **kwargs
        )

    def create_identifier_mapping(self, batch):
        """By default, no identifier mapping is needed."""
        return None


    def process_batch(self, batch):
        """Perform a Simplified lookup and import the resulting OPDS feed."""
        imported_editions, imported_pools, imported_works, error_messages_by_id, next_links = self.lookup_and_import_batch(
            batch
        )

        results = []
        for edition in imported_editions:
            self.finalize_edition(edition)
            results.append(edition.primary_identifier)

        for failure in self.handle_import_messages(error_messages_by_id):
            results.append(failure)
        return results


    def process_item(self, identifier):
        """Handle an individual item (e.g. through ensure_coverage) as a very
        small batch. Not efficient, but it works.
        """
        [result] = self.process_batch([identifier])
        return result

    def handle_import_messages(self, messages_by_id):
        """Turn OPDS messages from the lookup protocol into CoverageFailure
        objects.
        """
        for identifier, message in messages_by_id.items():
            # If the message indicates success but we didn't actually
            # get the data, treat it as a transient error.
            #
            # If the message does not indicate success, create a
            # CoverageRecord with the error so we stop trying this
            # book.
            if message.success:
                continue
                
            exception = str(message.status_code)
            if message.message:
                exception += ": %s" % message.message
            transient = message.transient
            identifier_obj, ignore = Identifier.parse_urn(self._db, identifier)
            yield CoverageFailure(self, identifier_obj, exception, transient)

    def finalize_edition(self, edition):
        """An OPDS entry has become an Edition. This method may (depending on
        configuration) create a Work for that book and mark it as
        presentation-ready.
        """
        pool = edition.license_pool

        if not pool:
            # Without a LicensePool there can be no Work.
            if self.expect_license_pool:
                self.log.warn(
                    "Expected that OPDS import would create a LicensePool for %r, but it didn't happen.",
                    edition
                )
            return
            
        # With a LicensePool and an Edition, there can be a Work.
        #
        # If the Work already exists, calculate_work() will at least
        # update the presentation.
        work, new_work = pool.calculate_work(
            even_if_no_author=True
        )            

        if work and self.presentation_ready_on_success:
            work.set_presentation_ready()

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

        response = self.lookup.lookup(foreign_identifiers)

        # import_feed_response takes id_mapping so it can map the
        # foreign identifiers back to their local counterparts.
        return self.import_feed_response(
            response, id_mapping
        )


    def import_feed_response(self, response, id_mapping):
        """Confirms OPDS feed response and imports feed.
        """
        
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise BadResponseException.from_response(
                response.url, 
                "Wrong media type: %s" % content_type,
                response
            )

        importer = OPDSImporter(self._db, identifier_mapping=id_mapping)
        return importer.import_from_feed(response.text)


class MockOPDSImportCoverageProvider(OPDSImportCoverageProvider):

    def __init__(self, *args, **kwargs):
        super(MockOPDSImportCoverageProvider, self).__init__(*args, **kwargs)
        self.batches = []
        self.finalized = []
        self.import_results = []

    def queue_import_results(self, editions, pools, works, messages_by_id, next_links=None):
        self.import_results.insert(0, (editions, pools, works, messages_by_id, next_links or []))

    def finalize_edition(self, edition):
        self.finalized.append(edition)
        super(MockOPDSImportCoverageProvider, self).finalize_edition(edition)

    def lookup_and_import_batch(self, batch):
        self.batches.append(batch)
        return self.import_results.pop()


class MetadataWranglerCoverageProvider(OPDSImportCoverageProvider):

    """Make sure that the metadata wrangler has weighed in on all 
    identifiers that might be associated with a LicensePool.
    """

    SERVICE_NAME = "Metadata Wrangler Coverage Provider"
    OPERATION = CoverageRecord.SYNC_OPERATION

    def __init__(self, _db, lookup=None, input_identifier_types=None, 
                 operation=None, **kwargs):
        if not input_identifier_types:
            input_identifier_types = [
                Identifier.OVERDRIVE_ID, 
                Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID, 
                Identifier.AXIS_360_ID,
            ]
        output_source = DataSource.lookup(
            _db, DataSource.METADATA_WRANGLER
        )
        super(MetadataWranglerCoverageProvider, self).__init__(
            lookup = lookup or SimplifiedOPDSLookup.from_config(),
            service_name=self.SERVICE_NAME,
            input_identifier_types=input_identifier_types,
            output_source=output_source,
            operation=operation or self.OPERATION,
            **kwargs
        )

        if not self.lookup.authenticated:
            self.log.warn(
                "Authentication for the Library Simplified Metadata Wrangler "
                "is not set up. You can still use the metadata wrangler, but "
                "it will not know which collection you're asking about."
            )

    def items_that_need_coverage(self, identifiers=None):
        """Returns items that are licensed and have not been covered"""
        uncovered = super(MetadataWranglerCoverageProvider, self).items_that_need_coverage(identifiers)
        reaper_covered = self._db.query(Identifier).\
                join(Identifier.coverage_records).\
                filter(CoverageRecord.data_source==self.output_source).\
                filter(CoverageRecord.operation==CoverageRecord.REAP_OPERATION)
        relicensed = reaper_covered.join(Identifier.licensed_through).\
                filter(LicensePool.licenses_owned > 0).\
                options(contains_eager(Identifier.coverage_records))

        # Remove Wrangler Reaper coverage records from relicensed identifiers
        for identifier in relicensed.all():
            [reaper_coverage_record] = [record
                    for record in identifier.coverage_records
                    if (record.data_source==self.output_source and
                        record.operation==CoverageRecord.REAP_OPERATION)]
            self._db.delete(reaper_coverage_record)
        return uncovered.except_(reaper_covered).union(relicensed)

    def create_identifier_mapping(self, batch):
        """The metadata wrangler can't look up Axis 360 identifiers, so look
        up the corresponding ISBNs instead. All other identifier types
        are fine, so they map to themselves.
        """
        mapping = dict()
        for identifier in batch:
            if identifier.type == Identifier.AXIS_360_ID:
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

    def items_that_need_coverage(self, identifiers=None):
        """Retreives Identifiers that have been synced and are no longer licensed"""

        qu = self._db.query(Identifier).select_from(LicensePool).\
            join(LicensePool.identifier).join(CoverageRecord).\
            filter(LicensePool.licenses_owned==0, LicensePool.open_access!=True).\
            filter(CoverageRecord.data_source==self.output_source).\
            filter(CoverageRecord.operation==CoverageRecord.SYNC_OPERATION)
        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))
        return qu

    def process_batch(self, batch):
        id_mapping = self.create_identifier_mapping(batch)
        batch = id_mapping.keys()
        response = self.lookup.remove(batch)
        removed, messages_by_id, next_links = self.import_feed_response(
            response, id_mapping
        )

        results = []
        for identifier in removed:
            results.append(id_mapping[identifier])
        for failure in self.handle_import_messages(messages_by_id):
            # 404 error indicates that the identifier wasn't in the
            # collection to begin with.
            if failure.message.starts_with("404"):
                results.append(id_mapping[failure.obj])
            else:
                results.append(failure)
        return results

    def finalize_batch(self):
        """Deletes Metadata Wrangler coverage records of reaped Identifiers

        This allows Identifiers to be added to the collection again via
        MetadataWranglerCoverageProvider lookup if a license is repurchased.
        """
        qu = self._db.query(Identifier.id).join(Identifier.coverage_records)
        reaper_covered = qu.filter(
            CoverageRecord.data_source==self.output_source,
            CoverageRecord.operation==CoverageRecord.REAP_OPERATION
        )
        wrangler_covered = qu.filter(
            CoverageRecord.data_source==self.output_source,
            CoverageRecord.operation==CoverageRecord.SYNC_OPERATION
        )
        # Get the db ids of identifiers that have been both synced and reaped.
        subquery = reaper_covered.intersect(wrangler_covered).subquery()

        # Retreive the outdated syncing coverage record and delete it.
        coverage_records = self._db.query(CoverageRecord).\
                join(CoverageRecord.identifier).\
                join(subquery, Identifier.id.in_(subquery)).\
                filter(
                    CoverageRecord.data_source==self.output_source,
                    CoverageRecord.operation==CoverageRecord.SYNC_OPERATION
                )
        for record in coverage_records.all():
            self._db.delete(record)


class ContentServerBibliographicCoverageProvider(OPDSImportCoverageProvider):
    """Make sure our records for open-access books match what the content
    server says.
    """
    DEFAULT_SERVICE_NAME = "Open-access content server bibliographic coverage provider"

    def __init__(self, _db, service_name=None, lookup=None, **kwargs):
        service_name = service_name or self.DEFAULT_SERVICE_NAME
        if not lookup:
            content_server_url = (
                Configuration.integration_url(
                    Configuration.CONTENT_SERVER_INTEGRATION
                )
            )
            lookup = SimplifiedOPDSLookup(content_server_url)
        output_source = DataSource.lookup(
            _db, DataSource.OA_CONTENT_SERVER
        )
        super(ContentServerBibliographicCoverageProvider, self).__init__(
            service_name, input_identifier_types=None,
            output_source=output_source, lookup=lookup,
            expect_license_pool=True, presentation_ready_on_success=True,
            **kwargs
        )

    def items_that_need_coverage(self, identifiers=None):
        """Only identifiers associated with an open-access license
        need coverage.
        """
        qu = super(ContentServerBibliographicCoverageProvider, 
                   self).items_that_need_coverage(identifiers)
        qu = qu.join(Identifier.licensed_through).filter(
            LicensePool.open_access==True
        )
        return qu


class SearchIndexCoverageProvider(WorkCoverageProvider):
    """Make sure the search index is up-to-date for every Work."""

    def __init__(self, _db, index_name, index_client=None, **kwargs):
        if index_client:
            # This would only happen during a test.
            self.search_index_client = index_client
        else:
            self.search_index_client = ExternalSearchIndex(
                works_index=index_name
            )
            
        index_name = self.search_index_client.works_index
        self.operation_name = WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION + '-' + index_name
        super(SearchIndexCoverageProvider, self).__init__(
            _db, 
            service_name="Search index update (%s)" % index_name,
            operation=self.operation_name,
            **kwargs
        )


    def process_item(self, work):
        """Update the search index for one item.

        TODO: It would be more efficient to override process_batch() to do a
        bulk upload.
        """

        # We pass add_coverage_record=False because the CoverageProvider
        # mechanisms will take care of adding the WorkCoverageRecord.
        present_in_index = work.update_external_index(
            self.search_index_client, add_coverage_record=False
        )
        if not present_in_index:
            if not work.presentation_ready:
                error = "Work not indexed because not presentation-ready."
            else:
                error = "Work not indexed"
            return CoverageFailure(self, work, error, transient=True)
        return work
