import os
import sys
from nose.tools import set_trace
from sqlalchemy.sql.functions import func
from model import (
    production_session,
    DataSource,
    Edition,
    LicensePool,
    Work,
    WorkGenre,
)
from opds_import import OPDSImportMonitor

class Script(object):

    @property
    def _db(self):
        return production_session()

    @property
    def data_directory(self):
        return self.required_environment_variable('DATA_DIRECTORY')

    def required_environment_variable(self, name):
        if not name in os.environ:
            print "Missing required environment variable: %s" % name
            sys.exit()
        return os.environ[name]

    def run(self):
        pass

class RunMonitorScript(Script):

    def __init__(self, monitor):
        if callable(monitor):
            monitor = monitor()
        self.monitor = monitor

    def run(self):
        self.monitor.run(self._db)

class WorkProcessingScript(Script):

    def __init__(self, force=False, restrict_to_source=None, 
                 specific_identifier=None, random_order=True,
                 batch_size=10):
        self.db = self._db
        if restrict_to_source:
            # Process works from a certain data source.
            data_source = DataSource.lookup(self.db, restrict_to_source)
            self.restrict_to_source = data_source
        else:
            # Process works from any data source.
            self.restrict_to_source = None
        self.force = force
        self.specific_works = None
        if specific_identifier:
            # Look up the works for this identifier
            q = self.db.query(Work).join(Edition).filter(
                Edition.primary_identifier==specific_identifier)
            self.specific_works = q

        self.random_order=random_order
        self.batch_size = batch_size

    def run(self):
        q = None
        if self.specific_works:
            print "Processing specific works: %r" % self.specific_works.all()
            q = self.specific_works
        elif self.restrict_to_source:
            print "Processing %s works." % self.restrict_to_source.name
        else:
            print "Processing all works."

        if not q:
            q = self.db.query(Work)
            if self.restrict_to_source:
                q = q.join(Edition).filter(
                    Edition.data_source==self.restrict_to_source)
            q = self.query_hook(q)

        if self.random_order:
            q = q.order_by(func.random())
        print "That's %d works." % q.count()

        a = 0
        for work in q:
            self.process_work(work)
            a += 1
            if not a % self.batch_size:
                self.db.commit()
        self.db.commit()

    def query_hook(self, q):
        return q

    def process_work(self, work):
        raise NotImplementedError()      

class WorkConsolidationScript(WorkProcessingScript):

    def run(self):
        work_ids_to_delete = set()
        unset_work_id = dict(work_id=None)

        if self.force:
            self.clear_existing_works()

        print "Consolidating works."
        LicensePool.consolidate_works(self.db)

        print "Deleting works with no editions."
        for i in self.db.query(Work).filter(Work.primary_edition==None):
            self.db.delete(i)            
        self.db.commit()

    def clear_existing_works(self):
        # Locate works we want to consolidate.
        unset_work_id = { Edition.work_id : None }
        work_ids_to_delete = set()
        work_records = self.db.query(Edition)
        if self.identifier_type:
            work_records = work_records.join(
                Identifier).filter(
                    Identifier.type==self.identifier_type)
            for wr in work_records:
                work_ids_to_delete.add(wr.work_id)
            work_records = self.db.query(Edition).filter(
                Edition.work_id.in_(work_ids_to_delete))
        else:
            work_records = work_records.filter(Edition.work_id!=None)

        # Unset the work IDs for any works we want to re-consolidate.
        work_records.update(unset_work_id, synchronize_session='fetch')

        pools = self.db.query(LicensePool)
        if self.identifier_type:
            # Unset the work IDs for those works' LicensePools.
            pools = pools.join(Identifier).filter(
                Identifier.type==self.identifier_type)
            for pool in pools:
                # This should not be necessary--every single work ID we're
                # going to delete should have showed up in the first
                # query--but just in case.
                work_ids_to_delete.add(pool.work_id)
            pools = self.db.query(LicensePool).filter(
                LicensePool.work_id.in_(work_ids_to_delete))
        else:
            pools = pools.filter(LicensePool.work_id!=None)
        pools.update(unset_work_id, synchronize_session='fetch')

        # Delete all work-genre assignments for works that will be
        # reconsolidated.
        if work_ids_to_delete:
            genres = self.db.query(WorkGenre)
            genres = genres.filter(WorkGenre.work_id.in_(work_ids_to_delete))
            print "Deleting %d genre assignments." % genres.count()
            genres.delete(synchronize_session='fetch')
            self.db.flush()

        if work_ids_to_delete:
            works = self.db.query(Work)
            print "Deleting %d works." % len(work_ids_to_delete)
            works = works.filter(Work.id.in_(work_ids_to_delete))
            works.delete(synchronize_session='fetch')
            self.db.commit()


class WorkPresentationScript(WorkProcessingScript):
    """Calculate the presentation for Work objects."""

    def process_work(self, work):
        work.calculate_presentation(
            choose_edition=True, classify=True, choose_summary=True,
            calculate_quality=True)


class OPDSImportScript(Script):
    """Import all books from an OPDS feed."""
    def __init__(self, feed_url, importer_class):
        self.feed_url = feed_url
        self.importer_class = importer_class

    def run(self):
        OPDSImportMonitor(self.feed_url, self.importer_class).run(self._db)
        

class WorkReclassifierScript(WorkProcessingScript):

    def __init__(self, force=False, restrict_to_source=None):
        self.force = force
        self.db = self._db
        if restrict_to_source:
            restrict_to_source = DataSource.lookup(self.db, restrict_to_source)
        self.restrict_to_source = restrict_to_source

    def run(self):
        if self.restrict_to_source:
            which_works = works_from_source.name
        else:
            which_works = "all"

        print "Reclassifying %s works." % (which_works)
        i = 0
        db = self.db
        q = db.query(Work)
        if self.restrict_to_source:
            q = q.join(Edition).filter(Edition.data_source==self.restrict_to_source)
        q = q.order_by(func.random())

        print "That's %d works." % q.count()

        q = q.limit(10)
        while q.count():
            for work in q:
                # old_genres = work.genres
                work.calculate_presentation(
                    choose_edition=False, classify=True,
                    choose_summary=False,
                    calculate_quality=False, debug=True)
                # new_genres = work.genres
                # if new_genres != old_genres:
                #     set_trace()
            db.commit()
        db.commit()

