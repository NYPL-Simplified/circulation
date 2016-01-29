import logging
from nose.tools import set_trace
from config import Configuration
from core.coverage import (
    CoverageFailure,
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
from core.opds import (
    OPDSFeed
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
            exception = status.message
            transient = message.success
            yield CoverageFailure(self, identifier, exception, transient)


class MetadataWranglerCoverageProvider(OPDSImportCoverageProvider):

    """Make sure that the metadata wrangler has weighed in on all 
    identifiers that might be associated with a LicensePool.
    """

    service_name = "Metadata Wrangler Coverage Provider"

    def __init__(self, _db, identifier_types=None, metadata_lookup=None):
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

        if not metadata_lookup:
            url = Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION
            )
            metadata_lookup = SimplifiedOPDSLookup(url)
        self.lookup = metadata_lookup
        self.search_index = ExternalSearchIndex()

        super(MetadataWranglerCoverageProvider, self).__init__(
            self.service_name,
            identifier_types,
            self.coverage_source,
            workset_size=2,
        )

    @property
    def editions_that_need_coverage(self):
        """Returns identifiers (not editions) that need coverage."""
        q = Identifier.missing_coverage_from(
            self._db, self.input_sources, self.coverage_source)
        return q

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
        response = self.lookup.lookup(batch)
        results = []

        if response.status_code != 200:
            self.log.error("BAD RESPONSE CODE: %s", response.status_code)
            raise HTTPIntegrationException(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise HTTPIntegrationException("Wrong media type: %s" % content_type)

        # If we got this far, we did get an OPDS feed.
        importer = OPDSImporter(self._db, identifier_mapping=id_mapping)
        imported, messages_by_id, next_links = importer.import_from_feed(
            response.text
        )

        for edition in imported:
            self.finalize_import(edition)
            reults.append(edition)

        for failure in self.handle_import_messages(messages_by_id):
            results.append(failure)
        return results

    def finalize_import(self, edition):
        """Now that an OPDS entry has been imported into an Edition, make sure
        there's a Work associated with the edition, and mark the Work
        as presentation-ready.
        """
        pool = edition.license_pool
        work = edition.work

        # Make sure there's a Work associated with the edition.
        if not work:
            work, new_work = pool.calculate_work(
                even_if_no_author=True,
                search_index_client=self.search_index
            )            

        # If the Work wasn't presentation ready before, it
        # certainly is now.
        work.set_presentation_ready()


class OpenAccessDownloadURLCoverageProvider(OPDSImportCoverageProvider):

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

        results = []

        # Handle the successes and seeming successes
        for edition in imported:
            if edition.open_access_download_url:
                self.log.info(
                    "Successfully located open access download ID for %r: %s", 
                    edition, edition.open_access_download_url
                )
                results.append(edition)
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
