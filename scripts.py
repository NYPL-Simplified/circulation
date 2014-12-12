import os
import sys
from nose.tools import set_trace
from model import (
    production_session,
    Edition,
    LicensePool,
    Work,
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

    def __init__(self, data_source_name=None):
        self.db = self._db
        if data_source_name:
            # Consolidate works from a certain data source.
            data_source = DataSource.lookup(self.db, data_source_name)
            self.identifier_type = data_source.primary_identifier_type
        else:
            # Consolidate works from any data source.
            self.identifier_type = None

    def run(self):
        work_ids_to_delete = set()
        unset_work_id = dict(work_id=None)

        # Locate works we want to consolidate.
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
            pools.update(unset_work_id, synchronize_session='fetch')
        else:
            pools = pools.filter(LicensePool.work_id!=None)

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

        print "Consolidating works."
        LicensePool.consolidate_works(self.db)
        self.db.commit()
