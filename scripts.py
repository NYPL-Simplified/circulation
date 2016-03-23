import os
import logging
import sys
from nose.tools import set_trace
from sqlalchemy import create_engine
from sqlalchemy.sql.functions import func
from sqlalchemy.orm.session import Session
import time

from config import Configuration
import log # This sets the appropriate log format and level.
import random
from model import (
    get_one_or_create,
    production_session,
    CustomList,
    DataSource,
    Edition,
    Identifier,
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

from overdrive import (
    OverdriveBibliographicCoverageProvider,
)

from threem import (
    ThreeMBibliographicCoverageProvider,
)

from axis import Axis360BibliographicCoverageProvider

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

    @classmethod
    def parse_identifier_list(cls, _db, arguments):
        """Turn a list of arguments into a list of identifiers.

        This makes it easy to identify specific identifiers on the
        command line. Examples:

        "Gutenberg ID" 1 2
        
        "Overdrive ID" a b c

        Basic but effective.
        """
        current_identifier_type = None
        if len(arguments) == 0:
            return []
        identifier_type = arguments[0]
        identifiers = []
        for arg in arguments[1:]:
            identifier, ignore = Identifier.for_foreign_id(
                _db, identifier_type, arg, autocreate=False
            )
            if not identifier:
                logging.warn(
                    "Could not load identifier %s/%s", identifier_type, arg
                )
            if identifier:
                identifiers.append(identifier)
        return identifiers

    @classmethod
    def parse_identifier_list_or_data_source(cls, _db, arguments):
        """Try to parse `arguments` as a list of identifiers.
        If that fails, try to interpret it as a data source.
        """
        identifiers = cls.parse_identifier_list(_db, arguments)
        if identifiers:
            return identifiers

        if len(arguments) == 1:
            # Try treating the sole argument as a data source.
            restrict_to_source = arguments[0]
            data_source = DataSource.lookup(_db, restrict_to_source)
            return data_source
        return []

    def run(self):
        self.load_configuration()
        DataSource.well_known_sources(self._db)
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

    def __init__(self, monitor, **kwargs):
        if callable(monitor):
            monitor = monitor(self._db, **kwargs)
        self.monitor = monitor
        self.name = self.monitor.service_name

    def do_run(self):
        self.monitor.run()

class RunCoverageProvidersScript(Script):
    """Alternate between multiple coverage providers."""
    def __init__(self, providers):
        self.providers = []
        for i in providers:
            if callable(i):
                i = i(self._db)
            self.providers.append(i)

    def do_run(self):
        offsets = dict()
        providers = list(self.providers)
        while providers:
            random.shuffle(providers)
            for provider in providers:
                offset = offsets.get(provider, 0)
                self.log.debug(
                    "Running %s with offset %s", provider.service_name, offset
                )
                offset = provider.run_once_and_update_timestamp(offset)
                self.log.debug(
                    "Completed %s, new offset is %s", provider.service_name, offset
                )
                if offset is None:
                    # We're done with this provider for now.
                    if provider in offsets:
                        del offsets[provider]
                    if provider in providers:
                        providers.remove(provider)
                else:
                    offsets[provider] = offset


class IdentifierInputScript(Script):
    """A script that takes identifiers as command line inputs."""

    def parse_identifiers(self):
        potential_identifiers = sys.argv[1:]
        identifiers = self.parse_identifier_list(
            self._db, potential_identifiers
        )
        if potential_identifiers and not identifiers:
            self.log.warn("Could not extract any identifiers from command-line arguments, falling back to default behavior.")
        return identifiers

    def parse_identifiers_or_data_source(self):
        """Try to parse the command-line arguments as a list of identifiers.
        If that fails, try to find a data source.
        """
        return self.parse_identifier_list_or_data_source(
            self._db, sys.argv[1:]
        )

class RunCoverageProviderScript(IdentifierInputScript):
    """Run a single coverage provider."""

    def __init__(self, provider):
        if callable(provider):
            provider = provider(self._db)
        self.provider = provider
        self.name = self.provider.service_name

    def do_run(self):

        identifiers = self.parse_identifiers()
        if identifiers:
            self.provider.process_batch(identifiers)
            self._db.commit()
        else:
            self.provider.run()

class BibliographicRefreshScript(IdentifierInputScript):
    """Refresh the core bibliographic data for Editions direct from the
    license source.
    """
    def do_run(self):
        identifiers = self.parse_identifiers()
        if not identifiers:
            raise Exception(
                "You must specify at least one identifier to refresh."
            )
        for identifier in identifiers:
            self.refresh_metadata(identifier)

    def refresh_metadata(self, identifier):
        provider = None
        if identifier.type==Identifier.THREEM_ID:
            provider = ThreeMBibliographicCoverageProvider
        elif identifier.type==Identifier.OVERDRIVE_ID:
            provider = OverdriveBibliographicCoverageProvider
        elif identifier.type==Identifier.AXIS_360_ID:
            provider = Axis360BibliographicCoverageProvider
        else:
            self.log.warn("Cannot update coverage for %r" % identifier)
        if provider:
            provider(self._db).ensure_coverage(identifier, force=True)


class WorkProcessingScript(IdentifierInputScript):

    name = "Work processing script"

    def __init__(self, force=False, batch_size=10):

        identifiers_or_source = self.parse_identifiers_or_data_source()
        self.batch_size = batch_size
        self.query = self.make_query(identifiers_or_source)
        self.force = force

    def make_query(self, identifiers):
        query = self._db.query(Work)
        if identifiers is None:
            self.log.info(
                "Processing all %d works.", query.count()
            )
        elif isinstance(identifiers, DataSource):
            # Find all works from the given data source.
            query = query.join(Edition).filter(
                Edition.data_source==identifiers
            )
            self.log.info(
                "Processing %d works from %s", query.count(),
                identifiers.name
            )
        else:
            # Find works with specific identifiers.
            query = query.join(Edition).filter(
                    Edition.primary_identifier_id.in_(
                        [x.id for x in identifiers]
                    )
            )
            self.log.info(
                "Processing %d specific works." % query.count()
            )
        return query.order_by(Work.id)

    def do_run(self):
        works = True
        offset = 0
        while works:
            works = self.query.offset(offset).limit(self.batch_size).all()
            for work in works:
                self.process_work(work)
            offset += self.batch_size
            self._db.commit()
        self._db.commit()

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
        LicensePool.consolidate_works(self._db)

        logging.info("Deleting works with no editions.")
        for i in self.db.query(Work).filter(Work.primary_edition==None):
            self._db.delete(i)            
        self._db.commit()

    def clear_existing_works(self):
        # Locate works we want to consolidate.
        unset_work_id = { Edition.work_id : None }
        work_ids_to_delete = set()
        work_records = self._db.query(Edition)
        if getattr(self, 'identifier_type', None):
            work_records = work_records.join(
                Identifier).filter(
                    Identifier.type==self.identifier_type)
            for wr in work_records:
                work_ids_to_delete.add(wr.work_id)
            work_records = self._db.query(Edition).filter(
                Edition.work_id.in_(work_ids_to_delete))
        else:
            work_records = work_records.filter(Edition.work_id!=None)

        # Unset the work IDs for any works we want to re-consolidate.
        work_records.update(unset_work_id, synchronize_session='fetch')

        pools = self._db.query(LicensePool)
        if getattr(self, 'identifier_type', None):
            # Unset the work IDs for those works' LicensePools.
            pools = pools.join(Identifier).filter(
                Identifier.type==self.identifier_type)
            for pool in pools:
                # This should not be necessary--every single work ID we're
                # going to delete should have showed up in the first
                # query--but just in case.
                work_ids_to_delete.add(pool.work_id)
            pools = self._db.query(LicensePool).filter(
                LicensePool.work_id.in_(work_ids_to_delete))
        else:
            pools = pools.filter(LicensePool.work_id!=None)
        pools.update(unset_work_id, synchronize_session='fetch')

        # Delete all work-genre assignments for works that will be
        # reconsolidated.
        if work_ids_to_delete:
            genres = self._db.query(WorkGenre)
            genres = genres.filter(WorkGenre.work_id.in_(work_ids_to_delete))
            logging.info(
                "Deleting %d genre assignments.", genres.count()
            )
            genres.delete(synchronize_session='fetch')
            self._db.flush()

        if work_ids_to_delete:
            works = self._db.query(Work)
            logging.info(
                "Deleting %d works.", len(work_ids_to_delete)
            )
            works = works.filter(Work.id.in_(work_ids_to_delete))
            works.delete(synchronize_session='fetch')
            self._db.commit()


class WorkPresentationScript(WorkProcessingScript):
    """Calculate the presentation for Work objects."""

    choose_edition = True
    classify = True
    choose_summary = True
    calculate_quality = True

    def process_work(self, work):
        work.calculate_presentation(
            choose_edition=self.choose_edition, 
            classify=self.classify,
            choose_summary=self.choose_summary,
            calculate_quality=self.calculate_quality
        )

class WorkClassificationScript(WorkPresentationScript):
    """Recalculate the classification for Work objects.
    Just the classification, not the rest of calculate_presentation.
    """
    choose_edition = False
    classify = True
    choose_summary = False
    calculate_quality = False


class CustomListManagementScript(Script):
    """Maintain a CustomList whose membership is determined by a
    MembershipManager.
    """

    def __init__(self, manager_class,
                 data_source_name, list_identifier, list_name,
                 primary_language, description,
                 **manager_kwargs
             ):
        data_source = DataSource.lookup(self._db, data_source_name)
        self.custom_list, is_new = get_one_or_create(
            self._db, CustomList,
            data_source_id=data_source.id,
            foreign_identifier=list_identifier,
        )
        self.custom_list.primary_language = primary_language
        self.custom_list.description = description
        self.membership_manager = manager_class(
            self.custom_list, **manager_kwargs
        )

    def run(self):
        self.membership_manager.update()
        self._db.commit()


class OPDSImportScript(Script):
    """Import all books from an OPDS feed."""
    def __init__(self, feed_url, default_data_source, importer_class, 
                 keep_timestamp=True, immediately_presentation_ready=False):
        self.feed_url = feed_url
        self.default_data_source = default_data_source
        self.importer_class = importer_class
        self.keep_timestamp = keep_timestamp
        self.immediately_presentation_ready = immediately_presentation_ready

    def do_run(self):
        monitor = OPDSImportMonitor(
            self._db, self.feed_url, self.default_data_source, 
            self.importer_class, keep_timestamp=self.keep_timestamp,
            immediately_presentation_ready = self.immediately_presentation_ready
        )
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
        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        db = self._db
        for i in (MaterializedWork, MaterializedWorkWithGenre):
            view_name = i.__table__.name
            a = time.time()
            db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY %s" % view_name)
            b = time.time()
            print "%s refreshed in %.2f sec." % (view_name, b-a)

        # Close out this session because we're about to create another one.
        db.commit()
        db.close()

        # The normal database connection (which we want almost all the
        # time) wraps everything in a big transaction, but VACUUM
        # can't be executed within a transaction block. So create a
        # separate connection that uses autocommit.
        url = Configuration.database_url()
        engine = create_engine(url, isolation_level="AUTOCOMMIT")
        engine.autocommit = True
        a = time.time()
        engine.execute("VACUUM (VERBOSE, ANALYZE)")
        b = time.time()
        print "Vacuumed in %.2f sec." % (b-a)


class Explain(IdentifierInputScript):
    """Explain everything known about a given work."""
    def run(self):
        identifiers = self.parse_identifiers()
        identifier_ids = [x.id for x in identifiers]
        editions = self._db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids)
        )
        for edition in editions:
            self.explain(self._db, edition)
            print "-" * 80
        #self._db.commit()

    @classmethod
    def explain(cls, _db, edition, calculate_presentation=False):
        if edition.medium != 'Book':
            return
        output = "%s (%s, %s)" % (edition.title, edition.author, edition.medium)
        print output.encode("utf8")
        work = edition.work
        lp = edition.license_pool
        print " Metadata URL: http://metadata.alpha.librarysimplified.org/lookup?urn=%s" % edition.primary_identifier.urn
        seen = set()
        cls.explain_identifier(edition.primary_identifier, True, seen, 1, 0)
        if lp:
            cls.explain_license_pool(lp)
        else:
            print " No associated license pool."
        if work:
            cls.explain_work(work)
        else:
            print " No associated work."

        if work and calculate_presentation:
             print "!!! About to calculate presentation!"
             work.calculate_presentation()
             print "!!! All done!"
             print
             print "After recalculating presentation:"
             cls.explain_work(work)

    @classmethod
    def explain_identifier(cls, identifier, primary, seen, strength, level):
        indent = "  " * level
        if primary:
            ident = "Primary identifier"
        else:
            ident = "Identifier"
        if primary:
            strength = 1
        output = "%s %s: %s/%s (q=%s)" % (indent, ident, identifier.type, identifier.identifier, strength)
        print output.encode("utf8")

        _db = Session.object_session(identifier)
        classifications = Identifier.classifications_for_identifier_ids(
            _db, [identifier.id])
        for classification in classifications:
            subject = classification.subject
            genre = subject.genre
            if genre:
                genre = genre.name
            else:
                genre = "(!genre)"
            #print "%s  %s says: %s/%s %s w=%s" % (
            #    indent, classification.data_source.name,
            #    subject.identifier, subject.name, genre, classification.weight
            #)
        seen.add(identifier)
        for equivalency in identifier.equivalencies:
            if equivalency.id in seen:
                continue
            seen.add(equivalency.id)
            output = equivalency.output
            cls.explain_identifier(output, False, seen,
                                    equivalency.strength, level+1)

    @classmethod
    def explain_license_pool(cls, pool):
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

    @classmethod
    def explain_work(cls, work):
        print "Work info:"
        print " Fiction: %s" % work.fiction
        print " Audience: %s" % work.audience
        print " Target age: %r" % work.target_age
        print " %s genres." % (len(work.genres))
        for genre in work.genres:
            print " ", genre
