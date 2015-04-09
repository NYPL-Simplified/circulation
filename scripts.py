import os
import sys
from nose.tools import set_trace
from sqlalchemy.sql.functions import func
from external_search import (
    ExternalSearchIndex,
)

from model import (
    production_session,
    CustomList,
    DataSource,
    Edition,
    LicensePool,
    Subject,
    Work,
    WorkGenre,
)
from nyt import NYTBestSellerAPI
from opds_import import OPDSImportMonitor
from nyt import NYTBestSellerAPI

class Script(object):

    @property
    def _db(self):
        if not hasattr(self, "_session"):
            self._session = production_session()
        return self._session

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
            monitor = monitor(self._db)
        self.monitor = monitor

    def run(self):
        self.monitor.run()

class RunCoverageProviderScript(Script):

    def __init__(self, provider):
        if callable(provider):
            provider = provider(self._db)
        self.provider = provider

    def run(self):
        self.provider.run()

class WorkProcessingScript(Script):

    def __init__(self, _db=None, force=False, restrict_to_source=None, 
                 specific_identifier=None, random_order=True,
                 batch_size=10):
        self.db = _db or self._db
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

        q = q.order_by(Work.id)
        print "That's %d works." % q.count()

        works = True
        offset = 0
        while works:
            works = q.offset(offset).limit(self.batch_size)
            for work in works:
                self.process_work(work)
            offset += self.batch_size
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
        if getattr(self, 'identifier_type', None):
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
        if getattr(self, 'identifier_type', None):
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

class PermanentWorkIDCalculationScript(Script):
    """Calculate the permanent work ID for all Editions."""

    def run(self):
        counter = 0
        for edition in self._db.query(Edition):
            edition.calculate_permanent_work_id()
            counter += 1
            if not counter % 1000:
                print counter
        self._db.commit()


class OPDSImportScript(Script):
    """Import all books from an OPDS feed."""
    def __init__(self, feed_url, importer_class):
        self.feed_url = feed_url
        self.importer_class = importer_class

    def run(self):
        monitor = OPDSImportMonitor(self._db, self.feed_url, self.importer_class)
        monitor.run()
        

class OPDSRefreshingScript(WorkProcessingScript):
    """Refresh the cached OPDS feeds for Work objects."""

    def process_work(self, work):
        if work.presentation_ready:
            work.calculate_opds_entries()

class IndexRefreshingScript(WorkProcessingScript):
    """Re-index each work under the default external search index."""

    def __init__(self, *args, **kwargs):
        super(IndexRefreshingScript, self).__init__(*args, **kwargs)
        self.client = ExternalSearchIndex()

    def process_work(self, work):
        work.update_external_index(self.client)
            

class WorkReclassifierScript(WorkProcessingScript):

    def __init__(self, force=False, restrict_to_source=None):
        self.force = force
        self.db = self._db
        if restrict_to_source:
            restrict_to_source = DataSource.lookup(self.db, restrict_to_source)
        self.restrict_to_source = restrict_to_source
        self.search_index = ExternalSearchIndex()

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

        #q = q.limit(batch_size)
        #while q.count():
        a = 0
        for work in q:
            # old_genres = work.genres
            work.calculate_presentation(
                choose_edition=False, classify=True,
                choose_summary=False,
                calculate_quality=False, debug=True,
                search_index_client=self.search_index,
            )
                # new_genres = work.genres
                # if new_genres != old_genres:
                #     set_trace()
            a += 1
            if not a % 100:
                db.commit()
        db.commit()

class NYTBestSellerListsScript(Script):

    def __init__(self, include_history=False):
        super(NYTBestSellerListsScript, self).__init__()
        self.include_history = include_history
    
    def run(self):
        self.api = NYTBestSellerAPI(self._db)
        self.data_source = DataSource.lookup(self._db, DataSource.NYT)
        # For every best-seller list...
        names = self.api.list_of_lists()
        for l in sorted(names['results'], key=lambda x: x['list_name_encoded']):

            name = l['list_name_encoded']
            print "Handling list %s" % name
            best = self.api.best_seller_list(l)

            if self.include_history:
                self.api.fill_in_history(best)
            else:
                self.api.update(best)

            # Mirror the list to the database.
            customlist = best.to_customlist(self._db)
            print "Now %s entries in the list." % len(customlist.entries)
            self._db.commit()


class SubjectAssignmentScript(Script):

    def __init__(self, force):
        self.force = force

    def run(self):
        a = 0
        q = self._db.query(Subject)
        if not self.force:
            q = q.filter(Subject.fiction==None)

        print "Fixing up %d subjects." % q.count()
        for s in q:
            self.process(s)
            print s
            a += 1
            if not a % 1000:
                self._db.commit()
        self._db.commit()

    def process(self, subject):
        subject.assign_to_genre()
