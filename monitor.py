import datetime
from nose.tools import set_trace
import os
from sqlalchemy import or_
from core.monitor import (
    IdentifierSweepMonitor,
    WorkSweepMonitor,
)
from core.model import (
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Work,
)
from core.opds import OPDSFeed
from core.opds_import import (
    SimplifiedOPDSLookup,
    DetailedOPDSImporter,
)
from scripts import ContentOPDSImporter
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

        # TODO: This is a temporary measure to improve the
        # classification of childrens' books.
        from core.classifier import Classifier
        q = q.filter(Work.audience.in_(
            [Classifier.AUDIENCE_CHILDREN,
             Classifier.AUDIENCE_YOUNG_ADULT]))
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
                    Identifier.THREEM_ID
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
                    Identifier.THREEM_ID
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
        self.search_index_client = ExternalSearchIndex()

    def work_query(self):
        return self._db.query(Work).filter(Work.presentation_ready==True)

    def process_batch(self, batch):
        # TODO: Perfect opportunity for a bulk upload.
        highest_id = 0
        for work in batch:
            if work.id > highest_id:
                highest_id = work.id
            work.update_external_index(self.search_index_client)
            if work.title:
                print work.title.encode("utf8")
            else:
                print "WARN: Work %d is presentation-ready but has no title?" % work.id
        return highest_id

class MakePresentationReady(object):
    """A helper class that takes a bunch of Identifiers and
    asks the metadata wrangler (and possibly the content server)
    about them.
    """

    def __init__(self, _db, metadata_wrangler_url=None):
        metadata_wrangler_url = (
            metadata_wrangler_url or os.environ['METADATA_WEB_APP_URL'])
        content_server_url = (os.environ['CONTENT_WEB_APP_URL'])

        self._db = _db
        self.lookup = SimplifiedOPDSLookup(metadata_wrangler_url)
        self.content_lookup = SimplifiedOPDSLookup(content_server_url)
        self.search_index = ExternalSearchIndex()

    def process_batch(self, batch):
        print "%d batch" % len(batch)
        response = self.lookup.lookup(batch)
        print "Response!"

        if response.status_code != 200:
            print "BAD RESPONSE CODE: %s" % response.status_code
            raise HTTPIntegrationException(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise HTTPIntegrationException("Wrong media type: %s" % content_type)

        
        importer = DetailedOPDSImporter(
            self._db, response.text,
            overwrite_rels=[Hyperlink.IMAGE, Hyperlink.DESCRIPTION])
        imported, messages_by_id = importer.import_from_feed()

        # Look up any open-access works for which there is no
        # open-access link. We'll try to get one from the open-access
        # content server.
        needs_open_access_import = []
        for e in imported:
            pool = e.license_pool
            if pool.open_access and not e.best_open_access_link:
                needs_open_access_import.append(e.primary_identifier)

        if needs_open_access_import:
            print "%d works need open access import." % len(needs_open_access_import)
            response = self.content_lookup.lookup(needs_open_access_import)
            importer = ContentOPDSImporter(self._db, response.text)
            oa_imported, oa_messages_by_id = importer.import_from_feed()
            print "%d successes, %d failures." % (len(oa_imported), len(oa_messages_by_id))
            for identifier, (status_code, message) in oa_messages_by_id.items():
                print identifier, status_code, message

        # We need to commit the database to make sure that a newly
        # created Edition will show up as its Work's .primary_edition.
        self._db.commit()
        print "%d successful imports, %d holds/failures." % (
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
                    set_trace()
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
                    set_trace()
            if dirty:
                self._db.commit()
            print "%s READY" % edition.work.title.encode("utf8")
            edition.work.set_presentation_ready()
        now = datetime.datetime.utcnow()
        for identifier, (status_code, message) in messages_by_id.items():
            print identifier, status_code, message
            if status_code == 400:
                # The metadata wrangler thinks we made a mistake here,
                # and will probably never give us information about
                # this work. We need to record the problem and work
                # through it manually.
                edition.work.presentation_ready_exception = message
                edition.work.presentation_ready_attempt = now
        self._db.commit()
