from nose.tools import set_trace
import os
from core.monitor import Monitor
from core.model import (
    LicensePool,
    Work,
)
from core.opds import OPDSFeed
from core.opds_import import (
    SimplifiedOPDSLookup,
    DetailedOPDSImporter,
)

class HTTPIntegrationException(Exception):
    pass

class PresentationReadyMonitor(Monitor):
    """Make works presentation-ready by asking the metadata wrangler about
    them.
    """

    BATCH_SIZE = 100

    def __init__(self, metadata_wrangler_url=None, interval_seconds=10*60):
        metadata_wrangler_url = (
            metadata_wrangler_url or os.environ['METADATA_WRANGLER_URL'])
        self.lookup = SimplifiedOPDSLookup(metadata_wrangler_url)
        super(PresentationReadyMonitor, self).__init__(
            "Presentation ready monitor", interval_seconds)

    def run_once(self, _db, start, cutoff):
        # First make Works out of any Editions that were created since
        # the last time this monitor ran.
        LicensePool.consolidate_works(_db)

        # Now go through the Works that are not presentation ready
        # and ask the metadata wrangler about them.
        batch = []
        q = _db.query(Work).filter(
            Work.presentation_ready==False).filter(
            Work.presentation_ready_exception==None).order_by(
                Work.last_update_time.asc())
        for work in q:
            batch.append(work.primary_edition.primary_identifier)
            if len(batch) >= self.BATCH_SIZE:
                self.process_batch(_db, batch)
                batch = []

        if batch:
            self.process_batch(_db, batch)
        print "All done."

    def process_batch(self, _db, batch):

        print "%d batch" % len(batch)
        response = self.lookup.lookup(batch)
        print "Response!"

        if response.status_code != 200:
            raise HTTPIntegrationException(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise HTTPIntegrationException("Wrong media type: %s" % content_type)

        importer = DetailedOPDSImporter(_db, response.text)
        imported, messages_by_id = importer.import_from_feed()
         for edition in imported:
            # We don't hear about a work until the metadata wrangler
            # is confident it has decent data, so at this point the
            # work is ready.
            edition.work.set_presentation_ready()
        for identifier, (status_code, message) in messages_by_id.items():
            print identifier, status_code, message
            if status_code == 400:
                # The metadata wrangler thinks we made a mistake here,
                # and will probably never give us information about
                # this work. We need to record the problem and work
                # through it manually.
                edition.work.presentation_ready_exception = message
            
