import logging
from nose.tools import set_trace
from config import Configuration
from core.coverage import (
    CoverageFailure,
    CoverageProvider,
)
from sqlalchemy import and_
from sqlalchemy.orm import contains_eager
from core.model import (
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
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

class HTTPIntegrationException(Exception):
    pass


class OPDSImportCoverageProvider(CoverageProvider):

    def handle_import_messages(self, messages_by_id):
        """Turn import messages from the OPDS importer into CoverageFailure
        objects.
        """
        for identifier, message in messages_by_id.items():
            # If the message indicates success but we didn't actually
            # get the data, treat it as a transient error.
            #
            # If the message does not indicate success, create a
            # CoverageRecord with the error so we stop trying this
            # book.
            if not message.success:
                exception = str(message.status_code)
                if message.message:
                    exception += ": %s" % message.message
                transient = message.transient
                identifier_obj, ignore = Identifier.parse_urn(self._db, identifier)
                yield CoverageFailure(self, identifier_obj, exception, transient)


class MetadataWranglerCoverageProvider(OPDSImportCoverageProvider):

    """Make sure that the metadata wrangler has weighed in on all 
    identifiers that might be associated with a LicensePool.
    """

    service_name = "Metadata Wrangler Coverage Provider"

    def __init__(self, _db, identifier_types=None, metadata_lookup=None,
                 cutoff_time=None):
        self._db = _db
        if not identifier_types:
            identifier_types = [
                Identifier.OVERDRIVE_ID, 
                Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID, 
                Identifier.AXIS_360_ID,
            ]
        self.output_source = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )

        if not metadata_lookup:
            metadata_lookup = SimplifiedOPDSLookup.from_config()
        self.lookup = metadata_lookup

        super(MetadataWranglerCoverageProvider, self).__init__(
            self.service_name,
            identifier_types,
            self.output_source,
            workset_size=20,
            cutoff_time=cutoff_time,
        )

    @property
    def items_that_need_coverage(self):
        reaper_source = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER_COLLECTION
        )
        uncovered = super(MetadataWranglerCoverageProvider, self).items_that_need_coverage
        reaper_covered = self._db.query(Identifier).\
                join(Identifier.coverage_records).\
                filter(CoverageRecord.data_source==reaper_source)
        relicensed = reaper_covered.join(Identifier.licensed_through).\
                filter(LicensePool.licenses_owned > 0).\
                options(contains_eager(Identifier.coverage_records))

        # Remove Wrangler Reaper coverage records from relicensed identifiers
        for identifier in relicensed.all():
            [reaper_coverage_record] = [record
                    for record in identifier.coverage_records
                    if record.data_source==reaper_source]
            identifier.coverage_records.remove(reaper_coverage_record)
        return uncovered.except_(reaper_covered).union(relicensed)

    def create_id_mapping(self, batch):
        mapping = dict()
        for identifier in batch:
            if identifier.type == Identifier.AXIS_360_ID:
                # The metadata wrangler can't look up Axis 360
                # identifiers, so look up the corresponding ISBNs
                # instead.
                for e in identifier.equivalencies:
                    if e.output.type == Identifier.ISBN:
                        mapping[e.output] = identifier
            else:
                mapping[identifier] = identifier
        return mapping

    def import_feed_response(self, response):
        """Confirms OPDS feed response and imports feed"""

        if response.status_code != 200:
            self.log.error("BAD RESPONSE CODE: %s", response.status_code)
            raise HTTPIntegrationException(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise HTTPIntegrationException("Wrong media type: %s" % content_type)

        importer = OPDSImporter(self._db, identifier_mapping=id_mapping)
        return importer.import_from_feed(response.text)

    def process_batch(self, batch):
        if not self.lookup.authenticated:
            self.log.warn(
                "Library Simplified Metadata Wrangler is not configured."
            )

        id_mapping = self.create_id_mapping(batch)
        batch = id_mapping.keys()
        response = self.lookup.lookup(batch)
        imported, messages_by_id, next_links = self.import_feed_response(response)

        results = []
        for edition in imported:
            self.finalize_edition(edition)
            results.append(edition.primary_identifier)

        for failure in self.handle_import_messages(messages_by_id):
            results.append(failure)
        return results

    def process_item(self, identifier):
        [result] = self.process_batch([identifier])
        return result

    def finalize_edition(self, edition):
        """Now that an OPDS entry has been imported into an Edition, make sure
        there's a Work associated with the edition, and mark the Work
        as presentation-ready.
        """
        pool = edition.license_pool
        work = edition.work

        if not pool:
            if work:
                warning = "Edition %r has a work but no associated license pool."
            else:
                warning = "Edition %r has no license pool. Will not create work."
            self.log.warn(warning, edition)
            
        # Make sure there's a Work associated with the edition.
        if pool and not work:
            work, new_work = pool.calculate_work(
                even_if_no_author=True
            )            

        # If the Work wasn't presentation ready before, it
        # certainly is now.
        if pool and work:
            work.set_presentation_ready()


class MetadataWranglerCollectionReaper(MetadataWranglerCoverageProvider):
    """Removes unlicensed identifiers from the Metadata Wrangler collection"""

    service_name = "Metadata Wrangler Reaper"

    def __init__(self, _db):
        super(MetadataWranglerCollectionReaper, self).__init__(_db)
        self.output_source = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER_COLLECTION
        )
        self.collection_source = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )

    @property
    def items_that_need_coverage(self):
        """Retreives Identifiers that are no longer licensed and have
        Metadata Wrangler coverage"""

        return self._db.query(Identifier).select_from(LicensePool).\
            join(LicensePool.identifier).\
            filter(
                LicensePool.licenses_owned==0, LicensePool.open_access!=True
            ).join(CoverageRecord).filter(
                CoverageRecord.data_source==self.collection_source
            )

    def process_batch(self, batch):
        id_mapping = self.create_id_mapping(batch)
        batch = id_mapping.keys()
        response = self.lookup.remove(batch)
        removed, messages_by_id, next_links = self.import_feed_response(response)

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

    def process_item(self, identifier):
        [result] = self.process_batch([identifier])
        return result

    def finalize_batch(self):
        """Deletes Metadata Wrangler coverage records of reaped Identifiers

        This allows Identifiers to be added to the collection again via
        MetadataWranglerCoverageProvider lookup if a license is repurchased.
        """
        # Get the identifiers that have double coverage.
        qu = self._db.query(Identifier.id).join(Identifier.coverage_records)
        reaper_covered = qu.filter(
            CoverageRecord.data_source==self.output_source
        )
        wrangler_covered = qu.filter(
            CoverageRecord.data_source==self.collection_source
        )
        subquery = reaper_covered.intersect(wrangler_covered).subquery()

        # Retreive and the Metadata Wrangler coverage record
        coverage_records = self._db.query(CoverageRecord).\
                join(CoverageRecord.identifier).\
                join(subquery, Identifier.id.in_(subquery)).\
                filter(CoverageRecord.data_source==self.collection_source)

        for record in coverage_records.all():
            self._db.delete(record)


class OpenAccessDownloadURLCoverageProvider(OPDSImportCoverageProvider):

    """Make sure all open-access books have download URLs, or record
    the reason why they can't have one.

    This may not be necessary anymore, but it's useful to have around
    in case there's a problem.
    """
    service_name = "Open Access Download URL Coverage Provider"

    def __init__(self, _db, content_lookup=None, cutoff_time=None):
        self._db = _db
        if not content_lookup:
            content_server_url = (
                Configuration.integration_url(
                    Configuration.CONTENT_SERVER_INTEGRATION)
            )
            content_lookup = SimplifiedOPDSLookup(content_server_url)
        self.content_lookup = content_lookup
        self.coverage_source = DataSource.lookup(
            self._db, DataSource.OA_CONTENT_SERVER
        )
        super(OpenAccessDownloadURLCoverageProvider, self).__init__(
            self.service_name,
            None,
            self.coverage_source,
            workset_size=50,
            cutoff_time=cutoff_time
        )

    @property
    def items_that_need_coverage(self):
        """Returns Editions associated with an open-access LicensePool but
        with no open-access download URL.
        """
        q = Edition.missing_coverage_from(self._db, [], self.coverage_source)
        clause = and_(Edition.data_source_id==LicensePool.data_source_id,
                      Edition.primary_identifier_id==LicensePool.identifier_id)
        q = q.join(LicensePool, clause)
        q = q.filter(LicensePool.open_access == True).filter(
            Edition.open_access_download_url==None
        )
        return q

    def process_batch(self, items):
        identifiers = [x.primary_identifier for x in items]
        response = self.content_lookup.lookup(identifiers)
        importer = OPDSImporter(self._db, DataSource.OA_CONTENT_SERVER)
        imported, messages_by_id, next_links = importer.import_from_feed(
            response.content
        )

        results = []

        # Handle the successes and seeming successes
        for edition in imported:
            if edition.open_access_download_url:
                self.log.info(
                    "Successfully located open access download ID for %r: %s", 
                    edition, edition.open_access_download_url
                )
                results.append(edition.primary_identifier)
            else:
                exception = "Open access content server acknowledged book but gave no open-access download URL."
                failure = CoverageFailure(
                    self, edition, exception=exception, transient=False
                )
                results.append(failure)

        # Handle the outright failures.
        for failure in self.handle_import_messages(messages_by_id):
            results.append(failure)
        return results
