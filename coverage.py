import logging
from nose.tools import set_trace
from config import Configuration
from core.coverage import (
    CoverageProvider,
)
from sqlalchemy import (
    and_,
)
from core.model import (
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
)
from core.external_search import (
    ExternalSearchIndex,
)
from core.opds_import import (
    SimplifiedOPDSLookup,
    OPDSImporter,
)

# TODO: We want to ask the metadata wrangler about ISBNs, but only the
# ISBNs that are the primary identifier of some LicensePool.
#
# This probably demands a separate CoverageProvider.

class MetadataWranglerCoverageProvider(CoverageProvider):

    """Make sure that the metadata wrangler has weighed in on all 
    relevant identifiers.
    """

    service_name = "Metadata Wrangler Coverage Provider"

    def __init__(self, _db, identifier_types=None):
        self._db = _db
        if not identifier_types:
            identifier_types = [
                Identifier.OVERDRIVE_ID, 
                Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID, 
                Identifier.AXIS_360_ID,
            ]
        self.coverage_source = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )
        self.make_presentation_ready = MakePresentationReady(self._db)
        super(MetadataWranglerCoverageProvider, self).__init__(
            self.service_name,
            identifier_types,
            self.coverage_source,
            workset_size=50,
        )

    @property
    def editions_that_need_coverage(self):
        """Returns identifiers (not editions) that need coverage."""
        q = Identifier.missing_coverage_from(
            self._db, self.input_sources, self.coverage_source)
        return q

    def process_batch(self, batch):
        """Look up a batch of Identifiers in the metadata wrangler.

        We may also end up asking the content server about some of
        these.
        """
        imports, messages = self.make_presentation_ready.process_batch(batch)


class OpenAccessDownloadURLCoverageProvider(CoverageProvider):

    """Make sure all open-access books have download URLs, or record
    the reason why they can't have one.

    This may not be necessary anymore, but it's useful to have around
    in case there's a problem.
    """
    service_name = "Open Access Download URL Coverage Provider"

    def __init__(self, _db, content_lookup=None):
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
            workset_size=2,
        )

    @property
    def editions_that_need_coverage(self):
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

    def process_batch(self, editions):
        identifiers = [x.primary_identifier for x in editions]
        response = self.content_lookup.lookup(identifiers)
        importer = OPDSImporter(self._db, DataSource.OA_CONTENT_SERVER)
        imported, messages_by_id, next_links = importer.import_from_feed(
            response.content
        )

        successes = []
        failures = []

        for edition in imported:
            record, ignore = CoverageRecord.add_for(edition, self.output_source)
            if edition.open_access_download_url:
                self.log.info(
                    "Successfully located open access download ID for %r: %s", 
                    edition, edition.open_access_download_url
                )
                successes.append(edition)
            else:
                record.exception = "Open access content server acknowledged book but gave no open-access download URL."
                self.log.warn("%r: %s", edition, record.exception)
                failures.append(edition)

        for identifier, message in messages_by_id.items():
            if message.success:
                # Message indicates success but we didn't actually get
                # the data. Let's just try again later.
                continue
                failures.append(identifier.licensed_through)
            else:
                # This is an error bad enough to justify not trying
                # again later. Create a CoverageRecord with the error
                # so we stop trying this book.
                record, ignore = CoverageRecord.add_for(
                    identifier, self.output_source
                )
                self.log.warn("%r: %s", edition, status.message)
                record.exception = status.message
                failures.append(identifier.licensed_through)

        self._db.commit()
        return successes, failures

class MakePresentationReady(object):
    """A helper class that asks the metadata wrangler about a bunch of
    Identifiers.
    """

    def __init__(self, _db, metadata_wrangler_url=None):
        metadata_wrangler_url = (
            metadata_wrangler_url or Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION
            )
        )

        self._db = _db
        self.lookup = SimplifiedOPDSLookup(metadata_wrangler_url)
        self.search_index = ExternalSearchIndex()
        self.log = logging.getLogger("Circulation - Make Presentation Ready")

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

    def process_batch(self, batch):
        id_mapping = self.create_id_mapping(batch)
        batch = id_mapping.keys()
        self.log.debug("%d batch", len(batch))
        response = self.lookup.lookup(batch)

        if response.status_code != 200:
            self.log.error("BAD RESPONSE CODE: %s", response.status_code)
            raise HTTPIntegrationException(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise HTTPIntegrationException("Wrong media type: %s" % content_type)

        importer = OPDSImporter(self._db, identifier_mapping=id_mapping)
        imported, messages_by_id, next_links = importer.import_from_feed(
            response.text
        )

        # Look up any open-access works for which there is no
        # open-access link. We'll try to get one from the open-access
        # content server.
        needs_open_access_import = []
        for e in imported:
            pool = e.license_pool
            if pool and pool.open_access and not e.best_open_access_link:
                needs_open_access_import.append(e.primary_identifier)

        if needs_open_access_import:
            self.log.info(
                "%d works need open access import.", 
                len(needs_open_access_import)
            )
            response = self.content_lookup.lookup(needs_open_access_import)
            importer = OPDSImporter(self._db, DataSource.OA_CONTENT_SERVER)
            imported, messages_by_id, next_links = importer.import_from_feed(
                response.content
            )
            self.log.info(
                "%d successes, %d failures.",
                len(imported), len(messages_by_id)
            )

            for identifier, status_message in oa_messages_by_id.items():
                self.log.info("%r: %r", identifier, status_message)

        # We need to commit the database to make sure that a newly
        # created Edition will show up as its Work's .primary_edition.
        self._db.commit()
        self.log.info(
            "%d successful imports, %d holds/failures.",
            len(imported), len(messages_by_id)
        )
        for edition in imported:
            # We don't hear about a work until the metadata wrangler
            # is confident it has decent data, so at this point the
            # work is ready.
            pool = edition.license_pool
            dirty = False
            if not edition.work:                
                dirty = True
                pool.calculate_work(
                    even_if_no_author=True,
                    search_index_client=self.search_index
                )

            if pool.work != edition.work:
                # This is a big problem. But hopefully it's limited to
                # old-code situations where works were grouped
                # together if their permanent work IDs were None.
                dirty = True
                if pool.identifier != edition.primary_identifier:
                    # The edition is associated with the wrong
                    # LicensePool.
                    #
                    # Not sure what to do in this case.
                    self.log.error(
                        "Edition %r, with identifier %r, is associated with license pool %r, with mismatched identifier %r",
                        edition, edition.primary_identifier, pool,
                        pool.identifier
                    )
                else:
                    # The Edition is associated with the correct
                    # LicensePool, but the LicensePool is associated
                    # with a Work that doesn't include the Edition.
                    pool.work = None

                pool.calculate_work(
                    even_if_no_author=True,
                    search_index_client=self.search_index
                )
                if pool.work != edition.work:
                    self.log.error(
                        "Pool %r is associated with work %r, but its primary edition, %r, is associated with work %r", pool, pool.work, edition, edition.work
                    )
            if dirty:
                self._db.commit()
            self.log.info("%s READY", edition.work.title)
            edition.work.set_presentation_ready()
        now = datetime.datetime.utcnow()
        failures = []
        for identifier, status_message in messages_by_id.items():
            self.log.info("%r: %r", identifier, status_message)
            work = None
            edition = identifier.edition
            if edition:
                work = edition.work
            if status_message.status_code == 400:
                # The metadata wrangler thinks we made a mistake here,
                # and will probably never give us information about
                # this work. We need to record the problem and work
                # through it manually.
                if work:
                    work.presentation_ready_exception = message
                    work.presentation_ready_attempt = now
            if edition and status_message.status_code % 100 != 2:
                failures.append(edition)
        self._db.commit()
        set_trace()
        return imported, 
