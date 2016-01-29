import datetime
from nose.tools import set_trace
import os
import sys
import csv
from sqlalchemy import or_
import logging
from config import Configuration
from core.monitor import (
    EditionSweepMonitor,
    IdentifierSweepMonitor,
    WorkSweepMonitor,
)
from core.model import (
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Work,
)
from core.opds import OPDSFeed
from core.opds_import import (
    SimplifiedOPDSLookup,
    OPDSImporter,
)
from core.external_search import (
    ExternalSearchIndex,
)

class HTTPIntegrationException(Exception):
    pass


class CirculationPresentationReadyMonitor(WorkSweepMonitor):
    """Make works presentation-ready by asking the metadata wrangler about
    them.
    """

    def __init__(self, _db, metadata_wrangler_url=None,
                 service_name="Presentation ready monitor", batch_size=10,
                 interval_seconds=10*60):
        super(CirculationPresentationReadyMonitor, self).__init__(
            _db, "Presentation ready monitor", batch_size=batch_size, 
            interval_seconds=interval_seconds)
        self.make_presentation_ready = MakePresentationReady(
            self._db, metadata_wrangler_url)
        self.batch_size = batch_size

    def work_query(self):
        one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        try_this_work = or_(Work.presentation_ready_attempt==None,
                            Work.presentation_ready_attempt > one_day_ago)
        q = self._db.query(Work).filter(
            Work.presentation_ready==False).filter(
            try_this_work).filter(Work.primary_edition != None)
        return q

    def process_batch(self, batch):
        batch = [work.primary_edition.primary_identifier for work in batch]
        self.make_presentation_ready.process_batch(batch)


class MetadataRefreshMonitor(CirculationPresentationReadyMonitor):
    """Refresh the metadata of works that are already presentation-ready.
    """

    def __init__(self, _db, batch_size=10,
                 interval_seconds=3600*24*7):
        super(MetadataRefreshMonitor, self).__init__(
            _db, service_name="Metadata refresh monitor",
            batch_size=batch_size, 
            interval_seconds=interval_seconds)

    def work_query(self):
        # Only get works that are already presentation-ready
        q = self._db.query(Work).filter(Work.presentation_ready==True)
        return q


class LicensePoolButNoEditionPresentationReadyMonitor(IdentifierSweepMonitor):
    """Turn LicensePools that have no corresponding Edition into
    presentation-ready works.
    """

    def __init__(self, _db, batch_size=10, interval_seconds=10*60):
        super(LicensePoolButNoEditionPresentationReadyMonitor, self).__init__(
            _db, 
            "Presentation ready monitor - Identifier has LicensePool but no Edition", 
            interval_seconds)
        self.make_presentation_ready = MakePresentationReady(self._db)
        self.batch_size = batch_size

    def cleanup(self):
        LicensePool.consolidate_works(
            self._db, calculate_work_even_if_no_author=True)

    def identifier_query(self):
        """Find identifiers that have a LicensePool but no Edition."""
        q = self._db.query(Identifier).join(
            Identifier.licensed_through).outerjoin(
            Edition, Edition.primary_identifier_id==Identifier.id).filter(
                Edition.id == None)
        q = q.filter(
            Identifier.type.in_(
                [
                    Identifier.GUTENBERG_ID, 
                    Identifier.OVERDRIVE_ID,
                    Identifier.THREEM_ID,
                    Identifier.AXIS_360_ID,
                    ]
                )
            )
        return q

    def process_batch(self, batch):
        self.make_presentation_ready.process_batch(batch)


class LicensePoolButNoWorkPresentationReadyMonitor(IdentifierSweepMonitor):
    """Turn LicensePools that have no corresponding Work into
    presentation-ready works.
    """

    def __init__(self, _db, batch_size=50, interval_seconds=10*60):
        super(LicensePoolButNoWorkPresentationReadyMonitor, self).__init__(
            _db, 
            "Presentation ready monitor - Identifier has LicensePool but no Work", 
            interval_seconds)
        self.make_presentation_ready = MakePresentationReady(self._db)
        self.batch_size = batch_size

    def cleanup(self):
        # Make sure any newly created Editions have Works.
        # TODO: Check if necessary--probably only necessary for 
        # LicensePoolButNoEditionPresentationReadyMonitor.
        #LicensePool.consolidate_works(
        #    self._db, calculate_work_even_if_no_author=True)
        pass

    def identifier_query(self):
        """Find identifiers that have a LicensePool but no Work."""
        # TODO: It's indicative of a problem that the commented-out query
        # tends to pick up many fewer results than the other one.
        #q = self._db.query(Identifier).join(
        #    Identifier.licensed_through).outerjoin(
        #    Work, Work.id==LicensePool.work_id).filter(
        #        Work.id == None)
        q = self._db.query(Identifier).join(
            Identifier.licensed_through).join(LicensePool.edition).outerjoin(
            Work, Work.id==Edition.work_id).filter(
                Work.id == None)
        q = q.filter(
            Identifier.type.in_(
                [
                    Identifier.GUTENBERG_ID, 
                    Identifier.OVERDRIVE_ID,
                    Identifier.THREEM_ID,
                    Identifier.AXIS_360_ID,
                    ]
                )
            )
        return q

    def process_batch(self, batch):
        self.make_presentation_ready.process_batch(batch)

class SearchIndexUpdateMonitor(WorkSweepMonitor):
    """Make sure the search index is up-to-date for every work.
    """

    def __init__(self, _db, batch_size=100, interval_seconds=3600*24):
        super(SearchIndexUpdateMonitor, self).__init__(
            _db, 
            "Index Update Monitor", 
            interval_seconds)
        self.batch_size = batch_size
        self.search_index_client = ExternalSearchIndex(fallback_to_dummy=False)

    def work_query(self):
        return self._db.query(Work).filter(Work.presentation_ready==True)

    def process_batch(self, batch):
        # TODO: Perfect opportunity for a bulk upload.
        highest_id = 0
        for work in batch:
            if work.id > highest_id:
                highest_id = work.id
            work.update_external_index(self.search_index_client)
            if not work.title:
                logging.warn(
                    "Work %d is presentation-ready but has no title?" % work.id
                )
        return highest_id

class MakePresentationReady(object):
    """A helper class that takes a bunch of Identifiers and
    asks the metadata wrangler (and possibly the content server)
    about them.
    """

    def __init__(self, _db, metadata_wrangler_url=None):
        metadata_wrangler_url = (
            metadata_wrangler_url or Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION
            )
        )
        content_server_url = (
            Configuration.integration_url(
                Configuration.CONTENT_SERVER_INTEGRATION)
        )

        self._db = _db
        self.lookup = SimplifiedOPDSLookup(metadata_wrangler_url)
        self.content_lookup = SimplifiedOPDSLookup(content_server_url)
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
            oa_imported, oa_messages_by_id, oa_next_links = importer.import_from_feed(
                response.content
            )
            self.log.info(
                "%d successes, %d failures.",
                len(oa_imported), len(oa_messages_by_id)
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
        for identifier, status_message in messages_by_id.items():
            self.log.info("%r: %r", identifier, status_message)
            if status_message.status_code == 400:
                # The metadata wrangler thinks we made a mistake here,
                # and will probably never give us information about
                # this work. We need to record the problem and work
                # through it manually.
                edition.work.presentation_ready_exception = message
                edition.work.presentation_ready_attempt = now
        self._db.commit()

class UpdateOpenAccessURL(EditionSweepMonitor):
    """Set Edition.open_access_full_url for all Gutenberg works."""

    def __init__(self, _db, batch_size=100, interval_seconds=600):
        super(UpdateOpenAccessURL, self).__init__(
            _db, 
            "Update open access URLs for Gutenberg editions", 
            interval_seconds)
        self.make_presentation_ready = MakePresentationReady(self._db)
        self.batch_size = batch_size
    
    def edition_query(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        return self._db.query(Edition).filter(
            Edition.data_source==gutenberg)

    def process_edition(self, edition):
        edition.set_open_access_link()

