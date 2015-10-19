import os
import logging
import sys
from nose.tools import set_trace
from sqlalchemy.sql.functions import func
import time

from config import Configuration
import log # This sets the appropriate log format and level.
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
from external_search import (
    ExternalSearchIndex,
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
    def log(self):
        if not hasattr(self, '_log'):
            logger_name = getattr(self, 'name', None)
            self._log = logging.getLogger(logger_name)
        return self._log        

    @property
    def data_directory(self):
        return Configuration.data_directory()

    def run(self):
        self.load_configuration()
        try:
            self.do_run()
        except Exception, e:
            logging.error(
                "Fatal exception while running script: %s", e,
                exc_info=e
            )
            raise e

    def load_configuration(self):
        if not Configuration.instance:
            Configuration.load()

class RunMonitorScript(Script):

    def __init__(self, monitor):
        if callable(monitor):
            monitor = monitor(self._db)
        self.monitor = monitor
        self.name = self.monitor.service_name

    def do_run(self):
        self.monitor.run()

class RunCoverageProviderScript(Script):

    def __init__(self, provider):
        if callable(provider):
            provider = provider(self._db)
        self.provider = provider
        self.name = self.provider.service_name

    def do_run(self):
        self.provider.run()

class WorkProcessingScript(Script):

    name = "Work processing script"

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

    def do_run(self):
        q = None
        if self.specific_works:
            logging.info(
                "Processing specific works: %r", self.specific_works.all()
            )
            q = self.specific_works
        elif self.restrict_to_source:
            logging.info(
                "Processing %s works.",
                self.restrict_to_source.name,
            )
        else:
            logging.info("Processing all works.")

        if not q:
            q = self.db.query(Work)
            if self.restrict_to_source:
                q = q.join(Edition).filter(
                    Edition.data_source==self.restrict_to_source)
            q = self.query_hook(q)

        q = q.order_by(Work.id)
        logging.info("That's %d works.", q.count())

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

    name = "Work consolidation script"

    def do_run(self):
        work_ids_to_delete = set()
        unset_work_id = dict(work_id=None)

        if self.force:
            self.clear_existing_works()                  

        logging.info("Consolidating works.")
        LicensePool.consolidate_works(self.db)

        logging.info("Deleting works with no editions.")
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
            logging.info(
                "Deleting %d genre assignments.", genres.count()
            )
            genres.delete(synchronize_session='fetch')
            self.db.flush()

        if work_ids_to_delete:
            works = self.db.query(Work)
            logging.info(
                "Deleting %d works.", len(work_ids_to_delete)
            )
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
    def __init__(self, feed_url, importer_class, keep_timestamp=True):
        self.feed_url = feed_url
        self.importer_class = importer_class
        self.keep_timestamp = keep_timestamp

    def do_run(self):
        monitor = OPDSImportMonitor(
            self._db, self.feed_url, self.importer_class,
            keep_timestamp=self.keep_timestamp)
        monitor.run()
        

class NYTBestSellerListsScript(Script):

    def __init__(self, include_history=False):
        super(NYTBestSellerListsScript, self).__init__()
        self.include_history = include_history
    
    def do_run(self):
        self.api = NYTBestSellerAPI(self._db)
        self.data_source = DataSource.lookup(self._db, DataSource.NYT)
        # For every best-seller list...
        names = self.api.list_of_lists()
        for l in sorted(names['results'], key=lambda x: x['list_name_encoded']):

            name = l['list_name_encoded']
            logging.info("Handling list %s" % name)
            best = self.api.best_seller_list(l)

            if self.include_history:
                self.api.fill_in_history(best)
            else:
                self.api.update(best)

            # Mirror the list to the database.
            customlist = best.to_customlist(self._db)
            logging.info(
                "Now %s entries in the list.", len(customlist.entries))
            self._db.commit()


class RefreshMaterializedViewsScript(Script):
    """Refresh all materialized views."""
    
    def do_run(self):
        # Initialize database
        db = self._db
        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        for i in (MaterializedWork, MaterializedWorkWithGenre):
            view_name = i.__table__.name
            a = time.time()
            db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY %s" % view_name)
            b = time.time()
            print "%s refreshed in %.2f sec" % (view_name, b-a)


class Explain(Script):
    """Explain everything known about a given work."""
    def run(self):
        title = sys.argv[1]
        editions = self._db.query(Edition).filter(Edition.title==title)
        for edition in editions:
            self.explain(edition)

    def explain(self, edition):
        print edition.title
        work = edition.work
        lp = edition.license_pool
        for identifier in edition.equivalent_identifiers():
            self.explain_identifier(
                identifier, identifier==edition.primary_identifier
            )
        if work:
            self.explain_work(work)
        if lp:
            self.explain_license_pool(lp)
        print "Metadata URL: http://metadata.alpha.librarysimplified.org/lookup?urn=%s" % edition.primary_identifier.urn

        if work:
            print
            work.calculate_presentation()
            print
            print "After recalculating presentation:"
            self.explain_work(work)
            self._db.commit()

    def explain_identifier(self, identifier, primary=False):
        if primary:
            ident = "Primary identifier"
        else:
            ident = "Identifier"
        print "%s: %s/%s" % (ident, identifier.type, identifier.identifier)

    def explain_license_pool(self, pool):
        print "Licensepool info:"
        print " Delivery mechanisms:"
        if pool.delivery_mechanisms:
            for lpdm in pool.delivery_mechanisms:
                dm = lpdm.delivery_mechanism
                if dm.default_client_can_fulfill:
                    fulfillable = "Fulfillable"
                else:
                    fulfillable = "Unfulfillable"
                    print "  %s %s/%s" % (fulfillable, dm.content_type, dm.drm_scheme)
        else:
            print " No delivery mechanisms."
        print " %s owned, %d available, %d holds, %d reserves" % (
            pool.licenses_owned, pool.licenses_available, pool.patrons_in_hold_queue, pool.licenses_reserved
        )

    def explain_work(self, work):
        print "Work info:"
        print " Fiction: %s" % work.fiction
        print " Audience: %s" % work.audience
        print " Target age: %r" % work.target_age
        print " %s genres." % (len(work.genres))
        for genre in work.genres:
            print " ", genre
