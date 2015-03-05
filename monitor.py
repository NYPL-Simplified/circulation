import datetime
from nose.tools import set_trace
import os
from sqlalchemy import or_
from core.monitor import Monitor
from core.model import (
    Edition,
    Identifier,
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

class CirculationPresentationReadyMonitor(Monitor):
    """Make works presentation-ready by asking the metadata wrangler about
    them.
    """

    def __init__(self, _db, metadata_wrangler_url=None, batch_size=200,
                 interval_seconds=10*60):
        metadata_wrangler_url = (
            metadata_wrangler_url or os.environ['METADATA_WEB_APP_URL'])
        self.lookup = SimplifiedOPDSLookup(metadata_wrangler_url)
        self.batch_size = batch_size
        super(CirculationPresentationReadyMonitor, self).__init__(
            _db, "Presentation ready monitor", interval_seconds)

    def run_once(self, start, cutoff):
        # Fix any Identifiers that have LicensePools but not Editions.
        self.resolve_identifiers()

        # Make sure any newly created Editions have Works.
        #LicensePool.consolidate_works(
        #    self._db, calculate_work_even_if_no_author=True, max=1)

        # Finally, make all Works presentation-ready.
        self.make_works_presentation_ready()

    def resolve_identifiers(self):
        """Look up any Identifiers that have associated LicensePools but no
        associated Editions, or Editions but no Works.
        """
        q1 = self._db.query(Identifier).join(
            Identifier.licensed_through).outerjoin(
            Edition, Edition.primary_identifier_id==Identifier.id).filter(
                Edition.id == None)

        q2 = self._db.query(Identifier).join(
            Identifier.licensed_through).outerjoin(
            Work, Work.id==LicensePool.work_id).filter(
                Work.id == None)

        for q, message in (
            (q1, "LicensePool but no Edition"),
            (q2, "LicensePool but no Work")):
            q = q.filter(
                Identifier.type.in_(
                    [
                        # Identifier.GUTENBERG_ID, 
                        Identifier.OVERDRIVE_ID,
                        Identifier.THREEM_ID
                        ]
                    )
                )
            needy_identifiers = q.count()
            if not needy_identifiers:
                continue
            print "Asking metadata wrangler about %d identifiers which have %s." % (needy_identifiers, message)
            batch = []
            for identifier in q:
                batch.append(identifier)
                if len(batch) >= self.batch_size:
                    self.process_batch(batch)
                    # for i in batch:
                    #     if not i.primarily_identifies:
                    #         set_trace()
                    batch = []
            self.process_batch(batch)

    def make_works_presentation_ready(self):
        # Go through the Works that are not presentation ready and ask
        # the metadata wrangler about them.
        batch = []
        one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        try_this_work = or_(Work.presentation_ready_attempt==None,
            Work.presentation_ready_attempt > one_day_ago)
        q = self._db.query(Work).filter(
            Work.presentation_ready==False).filter(
            try_this_work).order_by(
                Work.last_update_time.asc())
        print "Making %d works presentation ready." % q.count()
        for work in q:
            batch.append(work.primary_edition.primary_identifier)
            if len(batch) >= self.batch_size:
                self.process_batch(batch)
                batch = []

        if batch:
            self.process_batch(batch)

        print "All done."

    def process_batch(self, batch):
        print "%d batch" % len(batch)
        response = self.lookup.lookup(batch)
        print "Response!"

        if response.status_code != 200:
            raise HTTPIntegrationException(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise HTTPIntegrationException("Wrong media type: %s" % content_type)

        importer = DetailedOPDSImporter(self._db, response.text)
        imported, messages_by_id = importer.import_from_feed()
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
                pool.calculate_work(even_if_no_author=True)

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

                pool.calculate_work(even_if_no_author=True)
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
