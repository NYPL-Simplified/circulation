import argparse
import datetime
import imp
import logging
import os
import re
import requests
import time
from requests.exceptions import (
    ConnectionError, 
    HTTPError,
)
import sys
import traceback
import unicodedata

from collections import defaultdict
import json
from nose.tools import set_trace
from sqlalchemy import create_engine
from sqlalchemy.sql.functions import func
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)
from sqlalchemy.orm.session import Session

from app_server import ComplaintController
from axis import Axis360BibliographicCoverageProvider
from config import Configuration, CannotLoadConfiguration
from metadata_layer import ReplacementPolicy
from model import (
    get_one,
    get_one_or_create,
    production_session,
    Complaint, 
    Contributor, 
    CoverageRecord, 
    CustomList,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Patron,
    PresentationCalculationPolicy,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    WorkGenre,
)
from external_search import ExternalSearchIndex
from monitor import SubjectAssignmentMonitor
from nyt import NYTBestSellerAPI
from opds_import import OPDSImportMonitor
from oneclick import OneClickAPI, MockOneClickAPI
from overdrive import OverdriveBibliographicCoverageProvider
from threem import ThreeMBibliographicCoverageProvider
from util.opds_writer import OPDSFeed
from util.personal_names import (
    contributor_name_match_ratio, 
    display_name_to_sort_name, 
    is_corporate_name
)



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
    def parse_command_line(cls, _db=None, cmd_args=None):
        parser = cls.arg_parser()
        return parser.parse_known_args(cmd_args)[0]

    @classmethod
    def arg_parser(cls):
        raise NotImplementedError()

    @classmethod
    def parse_time(cls, time_string):
        """Try to pass the given string as a time."""
        if not time_string:
            return None
        for format in ('%Y-%m-%d', '%m/%d/%Y', '%Y%m%d'):
            for hours in ('', ' %H:%M:%S'):
                full_format = format + hours
                try:
                    parsed = datetime.datetime.strptime(
                        time_string, full_format
                    )
                    return parsed
                except ValueError, e:
                    continue
        raise ValueError("Could not parse time: %s" % time_string)

    def __init__(self, _db=None):
        """Basic constructor.

        :_db: A database session to be used instead of
        creating a new one. Useful in tests.
        """
        if _db:
            self._session = _db

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

class InputScript(Script):
    @classmethod
    def read_stdin_lines(self, stdin):
        """Read lines from a (possibly mocked, possibly empty) standard input."""
        if stdin is not sys.stdin or not os.isatty(0):
            # A file has been redirected into standard input. Grab its
            # lines.
            lines = [x.strip() for x in stdin.readlines()]
        else:
            lines = []
        return lines
    
                    
class IdentifierInputScript(InputScript):
    """A script that takes identifiers as command line inputs."""

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None, stdin=sys.stdin, 
                           *args, **kwargs):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        stdin = cls.read_stdin_lines(stdin)
        return cls.look_up_identifiers(_db, parsed, stdin, *args, **kwargs)

    @classmethod
    def look_up_identifiers(cls, _db, parsed, stdin_identifier_strings, *args, **kwargs):
        """Turn identifiers as specified on the command line into
        real database Identifier objects.
        """
        if _db and parsed.identifier_type:
            # We can also call parse_identifier_list.
            identifier_strings = parsed.identifier_strings
            if stdin_identifier_strings:
                identifier_strings = (
                    identifier_strings + stdin_identifier_strings
                )
            parsed.identifiers = cls.parse_identifier_list(
                _db, parsed.identifier_type, identifier_strings,
                *args, **kwargs
            )
        else:
            # The script can call parse_identifier_list later if it
            # wants to.
            parsed.identifiers = None
        return parsed

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--identifier-type', 
            help='Process identifiers of this type. If IDENTIFIER is not specified, all identifiers of this type will be processed. If IDENTIFIER is specified, this argument is required.'
        )
        parser.add_argument(
            'identifier_strings',
            help='A specific identifier to process.',
            metavar='IDENTIFIER', nargs='*'
        )
        return parser

    @classmethod
    def parse_identifier_list(
            cls, _db, identifier_type, arguments, autocreate=False
    ):
        """Turn a list of identifiers into a list of Identifier objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        IdentifierInputScript.arg_parser().

        This makes it easy to identify specific identifiers on the
        command line. Examples:

        1 2
        
        a b c
        """
        current_identifier_type = None
        if len(arguments) == 0:
            return []
        if not identifier_type:
            raise ValueError("No identifier type specified!")
        identifiers = []
        for arg in arguments:
            identifier, ignore = Identifier.for_foreign_id(
                _db, identifier_type, arg, autocreate=autocreate
            )
            if not identifier:
                logging.warn(
                    "Could not load identifier %s/%s", identifier_type, arg
                )
            if identifier:
                identifiers.append(identifier)
        return identifiers


class PatronInputScript(InputScript):
    """A script that operates on one or more Patrons."""

    @classmethod
    def parse_command_line(cls, _db=None, cmd_args=None, stdin=sys.stdin, 
                           *args, **kwargs):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        if stdin:
            stdin = cls.read_stdin_lines(stdin)
        return cls.look_up_patrons(_db, parsed, stdin, *args, **kwargs)

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'identifiers',
            help='A specific patron identifier to process.',
            metavar='IDENTIFIER', nargs='*'
        )
        return parser

    @classmethod
    def look_up_patrons(cls, _db, parsed, stdin_patron_strings, *args, **kwargs):
        """Turn patron identifiers as specified on the command line into real
        Patron objects.
        """
        if _db:
            patron_strings = parsed.identifiers
            if stdin_patron_strings:
                patron_strings = (
                    patron_strings + stdin_patron_strings
                )
            parsed.patrons = cls.parse_patron_list(
                _db, patron_strings, *args, **kwargs
            )
        else:
            # Database is not active yet. The script can call
            # parse_patron_list later if it wants to.
            parsed.patrons = None
        return parsed

    @classmethod
    def parse_patron_list(cls, _db, arguments):
        """Turn a list of patron identifiers into a list of Patron objects.

        The list of arguments is probably derived from a command-line
        parser such as the one defined in
        PatronInputScript.arg_parser().
        """
        if len(arguments) == 0:
            return []
        patrons = []
        for arg in arguments:
            if not arg:
                continue
            for field in (Patron.authorization_identifier, Patron.username,
                          Patron.external_identifier):
                try:
                    patron = _db.query(Patron).filter(field==arg).one()
                except NoResultFound:
                    continue
                except MultipleResultsFound:
                    continue
                if patron:
                    patrons.append(patron)
                    break
            else:
                logging.warn(
                    "Could not find patron %s", arg
                )
        return patrons

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        self.process_patrons(parsed.patrons)

    def process_patrons(self, patrons):
        for patron in patrons:
            self.process_patron(patron)

    def process_patron(self, patron):
        raise NotImplementedError()


class SubjectInputScript(Script):
    """A script whose command line filters the set of Subjects.

    :return: a 2-tuple (subject type, subject filter) that can be
    passed into the SubjectSweepMonitor constructor.
    """
    
    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--subject-type', 
            help='Process subjects of this type'
        )
        parser.add_argument(
            '--subject-filter', 
            help='Process subjects whose names or identifiers match this substring'
        )
        return parser


class RunCoverageProviderScript(IdentifierInputScript):
    """Run a single coverage provider."""

    @classmethod
    def arg_parser(cls):
        parser = IdentifierInputScript.arg_parser()
        parser.add_argument(
            '--cutoff-time', 
            help='Update existing coverage records if they were originally created after this time.'
        )
        return parser

    @classmethod
    def parse_command_line(cls, _db, cmd_args=None, stdin=sys.stdin, 
                           *args, **kwargs):
        parser = cls.arg_parser()
        parsed = parser.parse_args(cmd_args)
        stdin = cls.read_stdin_lines(stdin)
        parsed = cls.look_up_identifiers(_db, parsed, stdin, *args, **kwargs)
        if parsed.cutoff_time:
            parsed.cutoff_time = cls.parse_time(parsed.cutoff_time)
        return parsed

    def __init__(self, provider, _db=None, cmd_args=None, **provider_arguments):
        super(RunCoverageProviderScript, self).__init__(_db)
        args = self.parse_command_line(self._db, cmd_args)
        if callable(provider):
            if args.identifier_type:
                self.identifier_type = args.identifier_type
                self.identifier_types = [self.identifier_type]
            else:
                self.identifier_type = None
                self.identifier_types = []
            kwargs = self.extract_additional_command_line_arguments(args)
            kwargs.update(provider_arguments)
            provider = provider(
                self._db, 
                cutoff_time=args.cutoff_time,
                **kwargs
            )
        self.provider = provider
        self.name = self.provider.service_name
        self.identifiers = args.identifiers

    def extract_additional_command_line_arguments(self, args):
        """A hook method for subclasses.
        
        Turns command-line arguments into additional keyword arguments
        to the CoverageProvider constructor.

        By default, pass in a value used only by CoverageProvider
        (as opposed to WorkCoverageProvider).
        """
        return {
            "input_identifier_types" : self.identifier_types, 
        }

    def do_run(self):
        if self.identifiers:
            self.provider.run_on_specific_identifiers(self.identifiers)
        else:
            self.provider.run()


class BibliographicRefreshScript(RunCoverageProviderScript):
    """Refresh the core bibliographic data for Editions direct from the
    license source.

    This covers all known sources of licensed content.
    """
    def __init__(self, **metadata_replacement_args):
        
        self.metadata_replacement_policy = ReplacementPolicy.from_metadata_source(
            **metadata_replacement_args
        )

    def do_run(self):
        args = self.parse_command_line(self._db)
        if args.identifiers:
            # This script is being invoked to fix a problem.
            # Make sure to always recalculate OPDS feeds and reindex the
            # work.
            self.metadata_replacement_policy.presentation_calculation_policy = (
                PresentationCalculationPolicy.recalculate_everything()
            )
            for identifier in args.identifiers:
                self.refresh_metadata(identifier)
        else:
            # This script is being invoked to provide general coverage,
            # so we'll only recalculate OPDS feeds and reindex the work
            # if something actually changes.
            for provider_class in (
                    ThreeMBibliographicCoverageProvider,
                    OverdriveBibliographicCoverageProvider,
                    Axis360BibliographicCoverageProvider
            ):
                try:
                    provider = provider_class(
                        self._db, 
                        cutoff_time=args.cutoff_time
                    )
                except CannotLoadConfiguration, e:
                    self.log.info(
                        'Cannot create provider: "%s" Assuming this is intentional and proceeding.',
                        str(e)
                    )
                    provider = None
                try:
                    if provider:
                        provider.run()
                except Exception, e:
                    self.log.error(
                        "Error in %r, moving on to next source.",
                        provider, exc_info=e
                    )
        self._db.commit()

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
            provider = provider(
                self._db, 
                metadata_replacement_policy=self.metadata_replacement_policy,
            )
            provider.ensure_coverage(identifier, force=True)


class AddClassificationScript(IdentifierInputScript):
    name = "Add a classification to an identifier"

    @classmethod
    def arg_parser(cls):
        parser = IdentifierInputScript.arg_parser()
        parser.add_argument(
            '--subject-type', 
            help='The type of the subject to add to each identifier.',
            required=True
        )
        parser.add_argument(
            '--subject-identifier', 
            help='The identifier of the subject to add to each identifier.'
        )        
        parser.add_argument(
            '--subject-name', 
            help='The name of the subject to add to each identifier.'
        )        
        parser.add_argument(
            '--data-source', 
            help='The data source to use when classifying.',
            default=DataSource.MANUAL
        )     
        parser.add_argument(
            '--weight', 
            help='The weight to use when classifying.',
            type=int,
            default=1000
        )     
        parser.add_argument(
            '--create-subject', 
            help="Add the subject to the database if it doesn't already exist",
            action='store_const',
            const=True
        )     
        return parser
    
    def __init__(self, _db=None, cmd_args=None):
        _db = _db or self._db
        args = self.parse_command_line(_db, cmd_args=cmd_args)
        self.identifier_type = args.identifier_type
        self.identifiers = args.identifiers
        subject_type = args.subject_type
        subject_identifier = args.subject_identifier
        subject_name = args.subject_name
        if not subject_name and not subject_identifier:
            raise ValueError(
                "Either subject-name or subject-identifier must be provided."
            )
        self.data_source = DataSource.lookup(_db, args.data_source)
        self.weight = args.weight
        self.subject, ignore = Subject.lookup(
            _db, subject_type, subject_identifier, subject_name,
            autocreate=args.create_subject
        )
        
    def run(self):
        policy = PresentationCalculationPolicy(
            choose_edition=False, 
            set_edition_metadata=False,
            classify=True,
            choose_summary=False, 
            calculate_quality=False,
            choose_cover=False,
            regenerate_opds_entries=True,
            update_search_index=True,
            verbose=True,
        )
        if self.subject:
            for identifier in self.identifiers:
                identifier.classify(
                    self.data_source, self.subject.type,
                    self.subject.identifier, self.subject.name,
                    self.weight
                )
                pool = identifier.licensed_through
                if pool and pool.work:
                    pool.work.calculate_presentation(policy=policy)
        else:
            self.log.warn("Could not locate subject, doing nothing.")


class WorkProcessingScript(IdentifierInputScript):

    name = "Work processing script"

    def __init__(self, force=False, batch_size=10):
        args = self.parse_command_line(self._db)
        self.identifier_type = args.identifier_type
        self.identifiers = args.identifiers
        self.batch_size = batch_size
        self.query = self.make_query(
            self._db, self.identifier_type, self.identifiers, self.log
        )
        self.force = force

    @classmethod
    def make_query(self, _db, identifier_type, identifiers, log=None):
        query = _db.query(Work)
        if identifiers or identifier_type:
            query = query.join(Work.license_pools).join(
                LicensePool.identifier
            )

        if identifiers:
            if log:
                log.info(
                    'Restricted to %d specific identifiers.' % len(identifiers)
                )
            query = query.filter(
                LicensePool.identifier_id.in_([x.id for x in identifiers])
            )
        if identifier_type:
            if log:
                log.info(
                    'Restricted to identifier type "%s".' % identifier_type
                )
            query = query.filter(Identifier.type==identifier_type)

        if log:
            log.info(
                "Processing %d works.", query.count()
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
    """Given an Identifier, make sure all the LicensePools for that
    Identifier are in Works that follow these rules:

    a) For a given permanent work ID, there may be at most one Work
    containing open-access LicensePools.

    b) Each non-open-access LicensePool has its own individual Work.
    """

    name = "Work consolidation script"

    def make_query(self, _db, identifier_type, identifiers, log=None):
        # We actually process LicensePools, not Works.
        qu = _db.query(LicensePool).join(LicensePool.identifier)
        if identifier_type:
            qu = qu.filter(Identifier.type==identifier_type)
        if identifiers:
            qu = qu.filter(
                Identifier.identifier.in_([x.identifier for x in identifiers])
            )
        return qu

    def process_work(self, work):
        # We call it 'work' for signature compatibility with the superclass,
        # but it's actually a LicensePool.
        licensepool = work
        licensepool.calculate_work()

    def do_run(self):
        super(WorkConsolidationScript, self).do_run()
        qu = self._db.query(Work).outerjoin(Work.license_pools).filter(
            LicensePool.id==None
        )
        self.log.info(
            "Deleting %d Works that have no LicensePools." % qu.count()
        )
        for i in qu:
            self._db.delete(i)
        self._db.commit()


class WorkPresentationScript(WorkProcessingScript):
    """Calculate the presentation for Work objects."""

    # Do a complete recalculation of the presentation.
    policy = PresentationCalculationPolicy()

    def process_work(self, work):
        work.calculate_presentation(policy=self.policy)


class WorkClassificationScript(WorkPresentationScript):
    """Recalculate the classification--and nothing else--for Work objects.
    """
    policy = PresentationCalculationPolicy(
        choose_edition=False,
        set_edition_metadata=False,
        classify=True,
        choose_summary=False,
        calculate_quality=False,
        choose_cover=False,
        regenerate_opds_entries=False, 
        update_search_index=False,
    )


class WorkOPDSScript(WorkPresentationScript):
    """Recalculate the OPDS entries and search index entries for Work objects.

    This is intended to verify that a problem has already been resolved and just
    needs to be propagated to these two 'caches'.
    """
    policy = PresentationCalculationPolicy(
        choose_edition=False,
        set_edition_metadata=False,
        classify=True,
        choose_summary=False,
        calculate_quality=False,
        choose_cover=False,
        regenerate_opds_entries=True, 
        update_search_index=True,
    )


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


class OneClickImportScript(Script):
    """Import all books from a OneClick-subscribed library catalog."""

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--mock', 
            help='If turned on, will use the MockOneClickAPI client.', 
            action='store_true'
        )
        return parser


    def __init__(self, _db=None, cmd_args=None):
        super(OneClickImportScript, self).__init__(_db=_db)

        # get database connection passed in from test or establish a prod one
        if _db:
            db = _db
        else:
            db = self._db

        parsed_args = self.parse_command_line(cmd_args=cmd_args)
        self.mock_mode = parsed_args.mock

        if self.mock_mode:
            self.log.debug(
                "This is mocked run, with metadata coming from test files, rather than live OneClick connection."
            )
            base_path = os.path.split(__file__)[0]
            base_path = os.path.join(base_path, "tests")
            self.api = MockOneClickAPI(_db=db, base_path=base_path)
        else:
            self.api = OneClickAPI.from_config(_db=db)


    def do_run(self):
        print "OneClickImportScript.do_run"
        self.log.info("OneClickImportScript.do_run().")
        items_transmitted, items_created = self.api.populate_all_catalog()
        result_string = "OneClickImportScript: %s items transmitted, %s items saved to DB" % (items_transmitted, items_created)
        print result_string
        self.log.info(result_string)




class OneClickDeltaScript(OneClickImportScript):
    """Import book deletions, additions, and metadata changes for a 
    OneClick-subscribed library catalog.
    """

    def __init__(self, _db=None, cmd_args=None):
        super(OneClickDeltaScript, self).__init__(_db=_db, cmd_args=cmd_args)


    def do_run(self):
        print "OneClickDeltaScript.do_run"
        self.log.info("OneClickDeltaScript.do_run().")
        items_transmitted, items_updated = self.api.populate_delta()



class OPDSImportScript(Script):
    """Import all books from an OPDS feed."""

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--url', 
            help='URL of the OPDS feed to be imported'
        )
        parser.add_argument(
            '--data-source', 
            help='The name of the data source providing the OPDS feed.'
        )
        parser.add_argument(
            '--force', 
            help='Import the feed from scratch, even if it seems like it was already imported.',
            dest='force', action='store_true'
        )
        return parser

    def __init__(self, feed_url, opds_data_source, importer_class, 
                 immediately_presentation_ready=False, cmd_args=None):
        args = self.parse_command_line(cmd_args)
        self.force_reimport = args.force
        self.feed_url = args.url or feed_url
        self.opds_data_source = args.data_source or opds_data_source
        self.importer_class = importer_class
        self.immediately_presentation_ready = immediately_presentation_ready

    def do_run(self):
        monitor = OPDSImportMonitor(
            self._db, self.feed_url, self.opds_data_source, 
            self.importer_class, 
            immediately_presentation_ready = self.immediately_presentation_ready,
            force_reimport=self.force_reimport
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
            self.log.info("Handling list %s" % name)
            best = self.api.best_seller_list(l)

            if self.include_history:
                self.api.fill_in_history(best)
            else:
                self.api.update(best)

            # Mirror the list to the database.
            customlist = best.to_customlist(self._db)
            self.log.info(
                "Now %s entries in the list.", len(customlist.entries))
            self._db.commit()


class RefreshMaterializedViewsScript(Script):
    """Refresh all materialized views."""

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--blocking-refresh', 
            help="Provide this argument if you're on an older version of Postgres and can't refresh materialized views concurrently.",
            action='store_true',
        )
        return parser

    def do_run(self):
        args = self.parse_command_line()
        if args.blocking_refresh:
            concurrently = ''
        else:
            concurrently = 'CONCURRENTLY'
        # Initialize database
        from model import (
            MaterializedWork,
            MaterializedWorkWithGenre,
        )
        db = self._db
        for i in (MaterializedWork, MaterializedWorkWithGenre):
            view_name = i.__table__.name
            a = time.time()
            db.execute("REFRESH MATERIALIZED VIEW %s %s" % (concurrently, view_name))
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


class DatabaseMigrationScript(Script):
    """Runs new migrations"""

    name = "Database Migration"
    MIGRATION_WITH_COUNTER = re.compile("\d{8}-(\d+)-(.)+\.(py|sql)")

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-d', '--last-run-date',
            help=('A date string representing the last migration file '
                  'run against your database, formatted as YYYY-MM-DD')
        )
        parser.add_argument(
            '-c', '--last-run-counter', type=int,
            help=('An optional digit representing the counter of the last '
                  'migration run against your database. Only necessary if '
                  'multiple migrations were created on the same date.')
        )
        return parser

    @classmethod
    def migratable_files(cls, filelist):
        """Filter a list of files for migratable file extensions"""

        migratable = [f for f in filelist
            if (f.endswith('.py') or f.endswith('.sql'))]
        return cls.sort_migrations(migratable)

    @classmethod
    def sort_migrations(self, migrations):
        """Ensures that migrations with a counter digit are sorted after
        migrations without one.
        """

        def compare_migrations(first, second):
            """Compares migrations according to ideal sorting order.

            - Migrations are first ordered by timestamp (asc).
            - If two migrations have the same timestamp, any migrations
              without counters come before migrations with counters.
            - If two migrations with the same timestamp, have counters,
              migrations are sorted by counter (asc).
            """
            first_datestamp = int(first[:8])
            second_datestamp = int(second[:8])
            datestamp_difference = first_datestamp - second_datestamp
            if datestamp_difference != 0:
                return datestamp_difference

            # Both migrations have the same timestamp, so compare using
            # their counters (default to 0 if no counter is included)
            first_count = self.MIGRATION_WITH_COUNTER.search(first) or 0
            second_count = self.MIGRATION_WITH_COUNTER.search(second) or 0
            if not isinstance(first_count, int):
                first_count = int(first_count.groups()[0])
            if not isinstance(second_count, int):
                second_count = int(second_count.groups()[0])
            return first_count - second_count

        return sorted(migrations, cmp=compare_migrations)

    @property
    def directories_by_priority(self):
        """Returns a list containing the migration directory path for core
        and its container server, organized in priority order (core first)
        """
        current_dir = os.path.split(os.path.abspath(__file__))[0]
        core = os.path.join(current_dir, 'migration')
        server = os.path.join(os.path.split(current_dir)[0], 'migration')

        # Core is listed first, since core makes changes to the core database
        # schema. Server migrations generally fix bugs or otherwise update
        # the data itself.
        return [core, server]

    def do_run(self):
        args = self.parse_command_line()
        last_run_date = args.last_run_date
        last_run_counter = args.last_run_counter

        existing_timestamp = get_one(self._db, Timestamp, service=self.name)
        if last_run_date:
            last_run_datetime = self.parse_time(last_run_date)
            if existing_timestamp:
                existing_timestamp.timestamp = last_run_datetime
                if last_run_counter:
                    existing_timestamp.counter = last_run_counter
            else:
                existing_timestamp, ignore = get_one_or_create(
                    self._db, Timestamp,
                    service=self.name,
                    timestamp=last_run_datetime
                )

        if existing_timestamp:
            migrations, migrations_by_dir = self.fetch_migration_files()

            new_migrations = self.get_new_migrations(
                existing_timestamp, migrations
            )
            if new_migrations:
                # Log the new migrations.
                print "%d new migrations found." % len(new_migrations)
                for migration in new_migrations:
                    print "  - %s" % migration

                self.run_migrations(
                    new_migrations, migrations_by_dir, existing_timestamp
                )
            else:
                print "No new migrations found. Your database is up-to-date."
        else:
            print ""
            print (
                "NO TIMESTAMP FOUND. Run script with timestamp that indicates"
                " the last migration run against this database."
            )
            self.arg_parser().print_help()

    def fetch_migration_files(self):
        """Pulls migration files from the expected locations

        Return a list of migration filenames and a dictionary of those
        files separated by their absolute directory location.
        """
        migrations = list()
        migrations_by_dir = defaultdict(list)

        for directory in self.directories_by_priority:
            # In the case of tests, the container server migration directory
            # may not exist.
            if os.path.isdir(directory):
                dir_migrations = self.migratable_files(os.listdir(directory))
                migrations += dir_migrations
                migrations_by_dir[directory] = dir_migrations

        return migrations, migrations_by_dir

    def get_new_migrations(self, timestamp, migrations):
        """Return a list of migration filenames, representing migrations
        created since the timestamp
        """
        last_run = timestamp.timestamp.strftime('%Y%m%d')
        migrations = self.sort_migrations(migrations)
        new_migrations = [migration for migration in migrations
                          if int(migration[:8]) >= int(last_run)]

        # Multiple migrations run on the same day have an additional digit
        # after the date and a dash, eg:
        #
        #     20150826-1-change_target_age_from_int_to_range.sql
        #
        # When that migration is run, the number will be saved to the
        # 'counter' column of Timestamp, so we have to account for that.
        start_found = False
        later_found = False
        index = 0
        while not start_found and not later_found and index < len(new_migrations):
            start_found, later_found = self._is_matching_migration(
                new_migrations[index], timestamp
            )
            index += 1

        if later_found:
            index -= 1
        new_migrations = new_migrations[index:]
        return new_migrations

    def _is_matching_migration(self, migration_file, timestamp):
        """Determine whether a given migration filename matches a given
        timestamp or is after it.
        """
        is_match = False
        is_after_timestamp = False

        timestamp_str = timestamp.timestamp.strftime('%Y%m%d')
        counter = timestamp.counter

        if migration_file[:8]>=timestamp_str:
            if migration_file[:8]>timestamp_str:
                is_after_timestamp = True
            elif counter:
                count = self.MIGRATION_WITH_COUNTER.search(migration_file)
                if count:
                    migration_num = int(count.groups()[0])
                    if migration_num==counter:
                        is_match = True
                    if migration_num > counter:
                        is_after_timestamp = True
            else:
                is_match = True
        return is_match, is_after_timestamp

    def run_migrations(self, migrations, migrations_by_dir, timestamp):
        """Run each migration, first by timestamp and then by directory
        priority.
        """
        previous = None

        migrations = self.sort_migrations(migrations)
        for migration_file in migrations:
            for d in self.directories_by_priority:
                if migration_file in migrations_by_dir[d]:
                    full_migration_path = os.path.join(d, migration_file)
                    try:
                        self._run_migration(full_migration_path, timestamp)
                        self._db.commit()
                        previous = migration_file
                    except Exception:
                        print
                        print "ERROR: Migration has been halted."
                        print "%s must be migrated manually." % full_migration_path
                        print "=" * 50
                        print traceback.print_exc(file=sys.stdout)
                        sys.exit(1)
        else:
            print "All new migrations have been run."

    def _run_migration(self, migration_path, timestamp):
        """Runs a single SQL or Python migration file"""

        migration_filename = os.path.split(migration_path)[1]

        if migration_path.endswith('.sql'):
            with open(migration_path) as clause:
                sql = clause.read()
                self._db.execute(sql)
        if migration_path.endswith('.py'):
            module_name = migration_filename[:-3]
            imp.load_source(module_name, migration_path)

        # Update timestamp for the migration.
        self.update_timestamp(timestamp, migration_filename)

    def update_timestamp(self, timestamp, migration_file):
        """Updates this service's timestamp to match a given migration"""

        last_run_date = self.parse_time(migration_file[0:8])
        timestamp.timestamp = last_run_date

        # When multiple migration files are created on the same date, an
        # additional number is added. This number is held in the 'counter'
        # column of Timestamp.
        # (It's not ideal, but it avoids creating a new database table.)
        match = self.MIGRATION_WITH_COUNTER.search(migration_file)
        if match:
            timestamp.counter = int(match.groups()[0])
        self._db.commit()

        print "New timestamp created at %s for %s" % (
            last_run_date.strftime('%Y-%m-%d'), migration_file
        )


class DatabaseMigrationInitializationScript(DatabaseMigrationScript):

    """Creates a timestamp to kickoff the regular use of
    DatabaseMigrationScript to manage migrations.
    """

    def do_run(self):
        existing_timestamp = get_one(self._db, Timestamp, service=self.name)
        if existing_timestamp:
            raise Exception(
                "Timestamp for Database Migration script already exists"
            )

        migrations = self.fetch_migration_files()[0]
        most_recent_migration = self.sort_migrations(migrations)[-1]

        initial_timestamp = Timestamp.stamp(self._db, self.name)
        self.update_timestamp(initial_timestamp, most_recent_migration)



class CheckContributorNamesInDB(IdentifierInputScript):
    """ Checks that contributor sort_names are display_names in 
    "last name, comma, other names" format.  

    Read contributors edition by edition, so that can, if necessary, 
    restrict db query by passed-in identifiers, and so can find associated 
    license pools to register author complaints to.

    NOTE:  There's also CheckContributorNamesOnWeb in metadata, 
    it's a child of this script.  Use it to check our knowledge against 
    viaf, with the newer better sort_name selection and formatting.

    TODO: make sure don't start at beginning again when interrupt while batch job is running.
    """

    COMPLAINT_SOURCE = "CheckContributorNamesInDB"
    COMPLAINT_TYPE = "http://librarysimplified.org/terms/problem/wrong-author";

    @classmethod
    def make_query(self, _db, identifier_type, identifiers, log=None):
        query = _db.query(Edition)
        if identifiers or identifier_type:
            query = query.join(Edition.primary_identifier)

        # we only want to look at editions with license pools, in case we want to make a Complaint
        query = query.join(Edition.is_presentation_for)

        if identifiers:
            if log:
                log.info(
                    'Restricted to %d specific identifiers.' % len(identifiers)
                )
            query = query.filter(
                Edition.primary_identifier_id.in_([x.id for x in identifiers])
            )
        if identifier_type:
            if log:
                log.info(
                    'Restricted to identifier type "%s".' % identifier_type
                )
            query = query.filter(Identifier.type==identifier_type)

        if log:
            log.info(
                "Processing %d editions.", query.count()
            )
            print "Processing %d editions.", query.count()

        return query.order_by(Edition.id)


    def run(self, batch_size=10):
        param_args = self.parse_command_line(self._db)
        
        if param_args.identifiers:
            # we're asked about a specific set of work contributors
            identifier_ids = [x.id for x in param_args.identifiers]

        self.query = self.make_query(
            self._db, param_args.identifier_type, param_args.identifiers, self.log
        )

        editions = True
        offset = 0
        output = "ContributorID|\tSortName|\tDisplayName|\tComputedSortName|\tResolution|\tComplaintSource"
        print output.encode("utf8")

        while editions:
            my_query = self.query.offset(offset).limit(batch_size)
            editions = my_query.all()

            for edition in editions:
                if edition.contributions:
                    for contribution in edition.contributions:
                        self.process_contribution_local(self._db, contribution, self.log)
            offset += batch_size

            self._db.commit()
        self._db.commit()


    @classmethod
    def process_contribution_local(cls, _db, contribution, log=None):
        if not contribution or not contribution.edition:
            return

        contributor = contribution.contributor

        identifier = contribution.edition.primary_identifier

        if contributor.sort_name and contributor.display_name:
            computed_sort_name_local_new = unicodedata.normalize("NFKD", unicode(display_name_to_sort_name(contributor.display_name)))
            # Did HumanName parser produce a differet result from the plain comma replacement?
            if (contributor.sort_name.strip().lower() != computed_sort_name_local_new.strip().lower()):
                error_message_detail = "Contributor[id=%s].sort_name is oddly different from computed_sort_name, human intervention required." % contributor.id

                # computed names don't match.  by how much?  if it's a matter of a comma or a misplaced 
                # suffix, we can fix without asking for human intervention.  if the names are very different, 
                # there's a chance the sort and display names are different on purpose, s.a. when foreign names 
                # are passed as translated into only one of the fields, or when the author has a popular pseudonym. 
                # best ask a human.

                # if the relative lengths are off than by a stray space or comma, ask a human
                # it probably means that a human metadata professional had added an explanation/expansion to the 
                # sort_name, s.a. "Bob A. Jones" --> "Bob A. (Allan) Jones", and we'd rather not replace this data 
                # with the "Jones, Bob A." that the auto-algorigthm would generate.
                length_difference = len(contributor.sort_name.strip()) - len(computed_sort_name_local_new.strip())
                if abs(length_difference) > 3:
                    return cls.process_local_mismatch(_db=_db, contribution=contribution,  
                        computed_sort_name=computed_sort_name_local_new, error_message_detail=error_message_detail, log=log)

                match_ratio = contributor_name_match_ratio(contributor.sort_name, computed_sort_name_local_new, normalize_names=False)

                if (match_ratio < 40):
                    # ask a human.  this kind of score can happen when the sort_name is a transliteration of the display_name, 
                    # and is non-trivial to fix.  
                    cls.process_local_mismatch(_db=_db, contribution=contribution, 
                        computed_sort_name=computed_sort_name_local_new, error_message_detail=error_message_detail, log=log)
                else:
                    # we can fix it!
                    output = "%s|\t%s|\t%s|\t%s|\tlocal_fix" % (contributor.id, contributor.sort_name, contributor.display_name, computed_sort_name_local_new)
                    print output.encode("utf8")
                    cls.set_contributor_sort_name(computed_sort_name_local_new, contribution)


    @classmethod
    def set_contributor_sort_name(cls, sort_name, contribution):
        """ Sets the contributor.sort_name and associated edition.author_name to the passed-in value. """
        contribution.contributor.sort_name = sort_name

        # also change edition.sort_author, if the author was primary
        # Note: I considered using contribution.edition.author_contributors, but 
        # found that it's not impossible to have a messy dataset that doesn't work on.  
        # For our purpose here, the following logic is cleaner-acting:
        # If this author appears as Primary Author anywhere on the edition, then change edition.sort_author.
        edition_contributions = contribution.edition.contributions
        for edition_contribution in edition_contributions:
            if ((edition_contribution.role == Contributor.PRIMARY_AUTHOR_ROLE) and 
                (edition_contribution.contributor.display_name == contribution.contributor.display_name)):
                contribution.edition.sort_author = sort_name


    @classmethod
    def process_local_mismatch(cls, _db, contribution, computed_sort_name, error_message_detail, log=None):
        """
        Determines if a problem is to be investigated further or recorded as a Complaint, 
        to be solved by a human.  In this class, it's always a complaint.  In the overridden 
        method in the child class in metadata_wrangler code, we sometimes go do a web query.
        """ 
        cls.register_problem(source=cls.COMPLAINT_SOURCE, contribution=contribution, 
            computed_sort_name=computed_sort_name, error_message_detail=error_message_detail, log=log)


    @classmethod
    def register_problem(cls, source, contribution, computed_sort_name, error_message_detail, log=None):
        """
        Make a Complaint in the database, so a human can take a look at this Contributor's name
        and resolve whatever the complex issue that got us here.
        """
        success = True
        contributor = contribution.contributor

        pool = contribution.edition.is_presentation_for
        try:
            complaint, is_new = Complaint.register(pool, cls.COMPLAINT_TYPE, source, error_message_detail)
            output = "%s|\t%s|\t%s|\t%s|\tcomplain|\t%s" % (contributor.id, contributor.sort_name, contributor.display_name, computed_sort_name, source)
            print output.encode("utf8")
        except ValueError, e:
            # log and move on, don't stop run
            log.error("Error registering complaint: %r", contributor, exc_info=e)
            print("Error registering complaint: %r", contributor)
            success = False

        return success






class Explain(IdentifierInputScript):
    """Explain everything known about a given work."""
    def run(self):
        param_args = self.parse_command_line(self._db)
        identifier_ids = [x.id for x in param_args.identifiers]
        editions = self._db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids)
        )
        #policy = PresentationCalculationPolicy.recalculate_everything()
        policy = None
        for edition in editions:
            self.explain(self._db, edition, policy)
            print "-" * 80
        #self._db.commit()

    @classmethod
    def explain(cls, _db, edition, presentation_calculation_policy=None):
        if edition.medium not in ('Book', 'Audio'):
            # we haven't yet decided what to display for you
            return

        # Tell about the Edition record.
        output = "%s (%s, %s) according to %s" % (edition.title, edition.author, edition.medium, edition.data_source.name)
        print output.encode("utf8")
        print " Permanent work ID: %s" % edition.permanent_work_id
        print " Metadata URL: http://metadata.alpha.librarysimplified.org/lookup?urn=%s" % edition.primary_identifier.urn

        seen = set()
        cls.explain_identifier(edition.primary_identifier, True, seen, 1, 0)

        # Find all contributions, and tell about the contributors.
        if edition.contributions:
            for contribution in edition.contributions:
                cls.explain_contribution(contribution)

        # Tell about the LicensePool.
        lp = edition.license_pool
        if lp:
            cls.explain_license_pool(lp)
        else:
            print " No associated license pool."

        # Tell about the Work.
        work = edition.work
        if work:
            cls.explain_work(work)
        else:
            print " No associated work."

        # Note:  Can change DB state.
        if work and presentation_calculation_policy is not None:
             print "!!! About to calculate presentation!"
             work.calculate_presentation(policy=presentation_calculation_policy)
             print "!!! All done!"
             print
             print "After recalculating presentation:"
             cls.explain_work(work)


    @classmethod
    def explain_contribution(cls, contribution):
        contributor_id = contribution.contributor.id
        contributor_sort_name = contribution.contributor.sort_name
        contributor_display_name = contribution.contributor.display_name
        output = " Contributor[%s]: contributor_sort_name=%s, contributor_display_name=%s, " % (contributor_id, contributor_sort_name, contributor_display_name)
        print output.encode("utf8")


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
        if work.presentation_edition:
            print " Identifier of presentation edition: %r" % work.presentation_edition.primary_identifier
        else:
            print " No presentation edition."
        print " Fiction: %s" % work.fiction
        print " Audience: %s" % work.audience
        print " Target age: %r" % work.target_age
        print " %s genres." % (len(work.genres))
        for genre in work.genres:
            print " ", genre
        print " License pools:"
        for pool in work.license_pools:
            active = "SUPERCEDED"
            if not pool.superceded:
                active = "ACTIVE"
            print "  %s: %r" % (active, pool.identifier)



class SubjectAssignmentScript(SubjectInputScript):

    def run(self):
        args = self.parse_command_line(self._db)
        monitor = SubjectAssignmentMonitor(
            self._db, args.subject_type, args.subject_filter
        )
        monitor.run()


class MockStdin(object):
    """Mock a list of identifiers passed in on standard input."""
    def __init__(self, *lines):
        self.lines = lines

    def readlines(self):
        lines = self.lines
        self.lines = []
        return lines
