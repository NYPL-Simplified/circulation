import os
import sys
from nose.tools import set_trace
from model import (
    production_session,
    Edition,
    LicensePool,
    Work,
    WorkGenre,
)

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

class WorkConsolidationScript(Script):

    def __init__(self, data_source_name=None, force=False):
        self.db = self._db
        if data_source_name:
            # Consolidate works from a certain data source.
            data_source = DataSource.lookup(self.db, data_source_name)
            self.identifier_type = data_source.primary_identifier_type
        else:
            # Consolidate works from any data source.
            self.identifier_type = None
        self.force = force

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


class WorkPresentationScript(Script):
    """Calculate the presentation for Work objects."""

    def __init__(self, works_from_source=None, force=False,
                 specific_identifier=None):
        self.session = self._db
        if specific_identifier:
            self.specific_work = specific_identifier.work
        else:
            self.specific_work = None
        self.force = force
        if works_from_source:
            self.works_from_source = DataSource.lookup(
                self.session, works_from_source)
        else:
            self.works_from_source = None

    def run(self):
        if self.specific_work:
            print "Recalculating presentation for %s" % self.specific_work
            self.specific_work.calculate_presentation()
            return

        if self.works_from_source:
            which_works = self.works_from_source.name
        else:
            which_works = "all"

        print "Recalculating presentation for %s works, force=%r" % (
            which_works, self.force)
        i = 0
        q = self.session.query(Work)
        if self.works_from_source:
            q = q.join(Edition).filter(
                Edition.data_source==self.works_from_source)
        if not self.force:
            q = q.filter(Work.fiction==None).filter(Work.audience==None)

        print "That's %d works." % q.count()
        for work in q:
            work.calculate_presentation(
                choose_edition=True, classify=True, choose_summary=True,
                calculate_quality=True)
            i += 1
            if not i % 10:
                self.session.commit()
        self.session.commit()

