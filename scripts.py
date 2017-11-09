# encoding: utf-8
from cStringIO import StringIO
from datetime import (
    datetime,
    timedelta,
)
from nose.tools import set_trace
import csv
import json
import os
import sys
import time
import urlparse
import logging
import argparse

from sqlalchemy import (
    or_,
    func,
)
from sqlalchemy.orm import (
    contains_eager, 
    defer
)
from psycopg2.extras import NumericRange

from core import log
from core.lane import Lane
from core.classifier import Classifier
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    Contribution,
    Credential,
    CustomList,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    get_one,
    Hold,
    Hyperlink,
    Identifier,
    Library,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    Representation,
    Subject,
    Timestamp,
    Work,
)
from core.scripts import (
    Script as CoreScript,
    DatabaseMigrationInitializationScript,
    RunCoverageProvidersScript,
    RunCoverageProviderScript,
    IdentifierInputScript,
    LibraryInputScript,
    PatronInputScript,
    RunMonitorScript,
)
from core.lane import (
    Pagination,
    Facets,
)
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    OPDSImporter,
)
from core.opds import (
    AcquisitionFeed,
)
from core.util.opds_writer import (    
     OPDSFeed,
)
from core.external_list import CustomListFromCSV
from core.external_search import ExternalSearchIndex
from core.util import LanguageCodes

from api.config import (
    CannotLoadConfiguration,
    Configuration,
)
from api.adobe_vendor_id import (
    AdobeVendorIDModel,
    AuthdataUtility,
)
from api.lanes import create_default_lanes
from api.controller import CirculationManager
from api.overdrive import OverdriveAPI
from api.circulation import CirculationAPI
from api.opds import CirculationManagerAnnotator
from api.overdrive import (
    OverdriveAPI,
    OverdriveBibliographicCoverageProvider,
)
from api.bibliotheca import (
    BibliothecaBibliographicCoverageProvider,
    BibliothecaCirculationSweep
)
from api.axis import (
    Axis360API,
)
from api.nyt import NYTBestSellerAPI
from core.axis import Axis360BibliographicCoverageProvider
from api.opds_for_distributors import (
    OPDSForDistributorsImporter,
    OPDSForDistributorsImportMonitor,
    OPDSForDistributorsReaperMonitor,
)
from api.odl import (
    ODLBibliographicImporter,
    ODLBibliographicImportMonitor,
)
from core.scripts import OPDSImportScript

class Script(CoreScript):
    def load_config(self):
        if not Configuration.instance:
            Configuration.load(self._db)

class CreateWorksForIdentifiersScript(Script):

    """Do the bare minimum to associate each Identifier with an Edition
    with title and author, so that we can calculate a permanent work
    ID.
    """
    to_check = [Identifier.OVERDRIVE_ID, Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID]
    BATCH_SIZE = 100
    name = "Create works for identifiers"

    def __init__(self, metadata_web_app_url=None):
        if metadata_web_app_url:
            self.lookup = MetadataWranglerOPDSLookup(metadata_web_app_url)
        else:
            self.lookup = MetadataWranglerOPDSLookup.from_config(_db)

    def run(self):

        # We will try to fill in Editions that are missing
        # title/author and as such have no permanent work ID.
        #
        # We will also try to create Editions for Identifiers that
        # have no Edition.

        either_title_or_author_missing = or_(
            Edition.title == None,
            Edition.sort_author == None,
        )
        edition_missing_title_or_author = self._db.query(Identifier).join(
            Identifier.primarily_identifies).filter(
                either_title_or_author_missing)

        no_edition = self._db.query(Identifier).filter(
            Identifier.primarily_identifies==None).filter(
                Identifier.type.in_(self.to_check))

        for q, descr in (
                (edition_missing_title_or_author,
                 "identifiers whose edition is missing title or author"),
                (no_edition, "identifiers with no edition")):
            batch = []
            self.log.debug("Trying to fix %d %s", q.count(), descr)
            for i in q:
                batch.append(i)
                if len(batch) >= self.BATCH_SIZE:
                    self.process_batch(batch)
                    batch = []

    def process_batch(self, batch):
        response = self.lookup.lookup(batch)

        if response.status_code != 200:
            raise Exception(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise Exception("Wrong media type: %s" % content_type)

        importer = OPDSImporter(
            self._db, response.text,
            overwrite_rels=[Hyperlink.DESCRIPTION, Hyperlink.IMAGE])
        imported, messages_by_id = importer.import_from_feed()
        self.log.info("%d successes, %d failures.", 
                      len(imported), len(messages_by_id))
        self._db.commit()

class MetadataCalculationScript(Script):

    """Force calculate_presentation() to be called on some set of Editions.

    This assumes that the metadata is in already in the database and
    will fall into place if we just call
    Edition.calculate_presentation() and Edition.calculate_work() and
    Work.calculate_presentation().

    Most of these will be data repair scripts that do not need to be run
    regularly.

    """

    name = "Metadata calculation script"

    def q(self):
        raise NotImplementedError()

    def run(self):
        q = self.q()
        search_index_client = ExternalSearchIndex(self._db)
        self.log.info("Attempting to repair metadata for %d works" % q.count())

        success = 0
        failure = 0
        also_created_work = 0

        def checkpoint():
            self._db.commit()
            self.log.info("%d successes, %d failures, %d new works.",
                          success, failure, also_created_work)

        i = 0
        for edition in q:
            edition.calculate_presentation()
            if edition.sort_author:
                success += 1
                work, is_new = edition.license_pool.calculate_work(
                    search_index_client=search_index_client)
                if work:
                    work.calculate_presentation()
                    if is_new:
                        also_created_work += 1
            else:
                failure += 1
            i += 1
            if not i % 1000:
                checkpoint()
        checkpoint()

class FillInAuthorScript(MetadataCalculationScript):
    """Fill in Edition.sort_author for Editions that have a list of
    Contributors, but no .sort_author.

    This is a data repair script that should not need to be run
    regularly.
    """

    name = "Fill in missing authors"

    def q(self):
        return self._db.query(Edition).join(
            Edition.contributions).join(Contribution.contributor).filter(
                Edition.sort_author==None)

class UpdateStaffPicksScript(Script):

    DEFAULT_URL_TEMPLATE = "https://docs.google.com/spreadsheets/d/%s/export?format=csv"

    def run(self):
        inp = self.open()
        tag_fields = {
            'tags': Subject.NYPL_APPEAL,
        }

        integ = Configuration.integration(Configuration.STAFF_PICKS_INTEGRATION)
        fields = integ.get(Configuration.LIST_FIELDS, {})

        importer = CustomListFromCSV(
            DataSource.LIBRARY_STAFF, CustomList.STAFF_PICKS_NAME,
            **fields
        )
        reader = csv.DictReader(inp, dialect='excel-tab')
        importer.to_customlist(self._db, reader)
        self._db.commit()

    def open(self):
        if len(sys.argv) > 1:
            return open(sys.argv[1])

        url = Configuration.integration_url(
            Configuration.STAFF_PICKS_INTEGRATION, True
        )
        if not url.startswith('https://') or url.startswith('http://'):
            url = self.DEFAULT_URL_TEMPLATE % url
        self.log.info("Retrieving %s", url)
        representation, cached = Representation.get(
            self._db, url, do_get=Representation.browser_http_get,
            accept="text/csv", max_age=timedelta(days=1))
        if representation.status_code != 200:
            raise ValueError("Unexpected status code %s" % 
                             representation.status_code)
        if not representation.media_type.startswith("text/csv"):
            raise ValueError("Unexpected media type %s" % 
                             representation.media_type)
        return StringIO(representation.content)


class LaneSweeperScript(LibraryInputScript):
    """Do something to each lane in a library."""

    def __init__(self, _db=None, testing=False):
        _db = _db or self._db
        super(LaneSweeperScript, self).__init__(_db)
        from api.app import app
        app.manager = CirculationManager(_db, testing=testing)
        self.app = app
        self.base_url = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY).value

    def process_library(self, library):
        begin = time.time()
        client = self.app.test_client()
        ctx = self.app.test_request_context(base_url=self.base_url)
        ctx.push()
        queue = [self.app.manager.top_level_lanes[library.id]]
        while queue:
            new_queue = []
            self.log.debug("Beginning of loop: %d lanes to process", len(queue))
            for l in queue:
                if self.should_process_lane(l):
                    self.process_lane(l)
                    self._db.commit()
                for sublane in l.sublanes:
                    new_queue.append(sublane)
            queue = new_queue
        ctx.pop()
        end = time.time()
        self.log.info("Entire process took %.2fsec", (end-begin))

    def should_process_lane(self, lane):
        return True

    def process_lane(self, lane):
        pass


class CacheRepresentationPerLane(LaneSweeperScript):

    name = "Cache one representation per lane"

    @classmethod
    def arg_parser(cls, _db):
        parser = LaneSweeperScript.arg_parser(_db)
        parser.add_argument(
            '--language', 
            help='Process only lanes that include books in this language.',
            action='append'
        )
        parser.add_argument(
            '--max-depth', 
            help='Stop processing lanes once you reach this depth.',
            type=int,
            default=None
        )
        parser.add_argument(
            '--min-depth', 
            help='Start processing lanes once you reach this depth.',
            type=int,
            default=1
        )
        return parser

    def __init__(self, _db=None, cmd_args=None, *args, **kwargs):
        super(CacheRepresentationPerLane, self).__init__(_db, *args, **kwargs)
        self.parse_args(cmd_args)
        
    def parse_args(self, cmd_args=None):
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        self.languages = []
        if parsed.language:
            for language in parsed.language:
                alpha = LanguageCodes.string_to_alpha_3(language)
                if alpha:
                    self.languages.append(alpha)
                else:
                    self.log.warn("Ignored unrecognized language code %s", alpha)
        self.max_depth = parsed.max_depth
        self.min_depth = parsed.min_depth

        # Return the parsed arguments in case a subclass needs to
        # process more args.
        return parsed
    
    def should_process_lane(self, lane):
        if not isinstance(lane, Lane):
            return False

        language_ok = False
        if not self.languages:
            # We are considering lanes for every single language.
            language_ok = True
        
        if not lane.languages:
            # The lane has no language restrictions.
            language_ok = True
        
        for language in self.languages:
            if language in lane.languages:
                language_ok = True
                break
        if not language_ok:
            return False
        
        if self.max_depth is not None and lane.depth > self.max_depth:
            return False
        if self.min_depth is not None and lane.depth < self.min_depth:
            return False
        
        return True

    def cache_url(self, annotator, lane, languages):
        raise NotImplementedError()

    def generate_representation(self, *args, **kwargs):
        raise NotImplementedError()

    # The generated document will probably be an OPDS acquisition
    # feed.
    ACCEPT_HEADER = OPDSFeed.ACQUISITION_FEED_TYPE

    cache_url_method = None

    def process_lane(self, lane):
        annotator = self.app.manager.annotator(lane)
        a = time.time()
        self.log.info(
            "Generating feed(s) for %s", lane.full_identifier
        )
        cached_feeds = list(self.do_generate(lane))
        b = time.time()
        total_size = sum(len(x.content) for x in cached_feeds if x)
        self.log.info(
            "Generated %d feed(s) for %s. Took %.2fsec to make %d bytes.",
            len(cached_feeds), lane.full_identifier, (b-a), total_size
        )
        return cached_feeds
        
class CacheFacetListsPerLane(CacheRepresentationPerLane):
    """Cache the first two pages of every relevant facet list for this lane."""

    name = "Cache OPDS feeds"
    
    @classmethod
    def arg_parser(cls, _db):
        parser = CacheRepresentationPerLane.arg_parser(_db)
        available = Facets.DEFAULT_ENABLED_FACETS[Facets.ORDER_FACET_GROUP_NAME]
        order_help = 'Generate feeds for this ordering. Possible values: %s.' % (
            ", ".join(available)
        )
        parser.add_argument(
            '--order',
            help=order_help,
            action='append',
            default=[],
        )

        available = Facets.DEFAULT_ENABLED_FACETS[Facets.AVAILABILITY_FACET_GROUP_NAME]
        availability_help = 'Generate feeds for this availability setting. Possible values: %s.' % (
            ", ".join(available)
        )
        parser.add_argument(
            '--availability',
            help=availability_help,
            action='append',
            default=[],
        )

        available = Facets.DEFAULT_ENABLED_FACETS[Facets.COLLECTION_FACET_GROUP_NAME]
        collection_help = 'Generate feeds for this collection within each lane. Possible values: %s.' % (
            ", ".join(available)
        )
        parser.add_argument(
            '--collection',
            help=collection_help,
            action='append',
            default=[],
        )
        
        default_pages = 2
        parser.add_argument(
            '--pages',
            help="Number of pages to cache for each facet. Default: %d" % default_pages,
            type=int,
            default=default_pages
        )
        return parser
    
    def parse_args(self, cmd_args=None):
        parsed = super(CacheFacetListsPerLane, self).parse_args(cmd_args)
        self.orders = parsed.order
        self.availabilities = parsed.availability
        self.collections = parsed.collection
        self.pages = parsed.pages
        return parsed

    def do_generate(self, lane):
        feeds = []
        annotator = self.app.manager.annotator(lane)
        if isinstance(lane, Lane) and lane.parent:
            languages = lane.language_key
            lane_name = lane.name
        else:
            languages = None
            lane_name = None

        library = lane.library
        url = self.app.manager.cdn_url_for(
            "feed", languages=languages, lane_name=lane_name, library_short_name=library.short_name
        )

        default_order = library.default_facet(Facets.ORDER_FACET_GROUP_NAME)
        allowed_orders = library.enabled_facets(Facets.ORDER_FACET_GROUP_NAME)
        chosen_orders = self.orders or [default_order]
        
        default_availability = library.default_facet(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        allowed_availabilities = library.enabled_facets(
            Facets.AVAILABILITY_FACET_GROUP_NAME
        )
        chosen_availabilities = self.availabilities or [default_availability]
        
        default_collection = library.default_facet(
            Facets.COLLECTION_FACET_GROUP_NAME
        )
        allowed_collections = library.enabled_facets(
            Facets.COLLECTION_FACET_GROUP_NAME
        )        
        chosen_collections = self.collections or [default_collection]
        
        for order in chosen_orders:
            if order not in allowed_orders:
                logging.warn("Ignoring unsupported ordering %s" % order)
                continue
            for availability in chosen_availabilities:
                if availability not in allowed_availabilities:
                    logging.warn("Ignoring unsupported availability %s" % availability)
                    continue
                for collection in chosen_collections:
                    if collection not in allowed_collections:
                        logging.warn("Ignoring unsupported collection %s" % collection)
                        continue
                    pagination = Pagination.default()
                    facets = Facets(
                        library=library, collection=collection,
                        availability=availability,
                        order=order, order_ascending=True
                    )
                    title = lane.display_name
                    for pagenum in range(0, self.pages):
                        yield AcquisitionFeed.page(
                            self._db, title, url, lane, annotator, 
                            facets=facets, pagination=pagination,
                            force_refresh=True
                        )
                        pagination = pagination.next_page


class CacheOPDSGroupFeedPerLane(CacheRepresentationPerLane):

    name = "Cache OPDS group feed for each lane"

    def should_process_lane(self, lane):
        # OPDS group feeds are only generated for lanes that have sublanes.
        if not lane.sublanes:
            return False
        if self.max_depth and lane.depth > self.max_depth:
            return False
        return True

    def do_generate(self, lane):
        feeds = []
        annotator = self.app.manager.annotator(lane)
        title = lane.display_name
        if isinstance(lane, Lane) and lane.parent:
            languages = lane.language_key
            lane_name = lane.name
        else:
            languages = None
            lane_name = None
        library = lane.library
        url = self.app.manager.cdn_url_for(
            "acquisition_groups", languages=languages, lane_name=lane_name, library_short_name=library.short_name
        )
        yield AcquisitionFeed.groups(
            self._db, title, url, lane, annotator,
            force_refresh=True
        )


class AdobeAccountIDResetScript(PatronInputScript):

    @classmethod
    def arg_parser(cls):
        parser = PatronInputScript.arg_parser()
        parser.add_argument(
            '--delete',
            help="Actually delete credentials as opposed to showing what would happen.",
            action='store_true'
        )
        return parser
    
    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        patrons = parsed.patrons
        self.delete = parsed.delete
        if not self.delete:
            self.log.info(
                "This is a dry run. Nothing will actually change in the database."
            )
            self.log.info(
                "Run with --delete to change the database."
            )

        if patrons and self.delete:
            self.log.warn(
                """This is not a drill.
Running this script will permanently disconnect %d patron(s) from their Adobe account IDs.
They will be unable to fulfill any existing loans that involve Adobe-encrypted files.
Sleeping for five seconds to give you a chance to back out.
You'll get another chance to back out before the database session is committed.""",
                len(patrons)
            )
            time.sleep(5)
        self.process_patrons(patrons)
        if self.delete:
            self.log.warn("All done. Sleeping for five seconds before committing.")
            time.sleep(5)
            self._db.commit()
        
    def process_patron(self, patron):
        """Delete all of a patron's Credentials that contain an Adobe account
        ID _or_ connect the patron to a DelegatedPatronIdentifier that
        contains an Adobe account ID.
        """
        self.log.info(
            'Processing patron "%s"',
            patron.authorization_identifier or patron.username
            or patron.external_identifier
        )
        types = (AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE,
                 AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER)
        credentials = self._db.query(
            Credential).filter(Credential.patron==patron).filter(
                Credential.type.in_(types)
            )
        for credential in credentials:
            self.log.info(
                ' Deleting "%s" credential "%s"',
                credential.type, credential.credential
            )
            if self.delete:
                self._db.delete(credential)


class BibliographicCoverageProvidersScript(RunCoverageProvidersScript):
    """Alternate between running bibliographic coverage providers for
    all registered book sources.
    """

    def __init__(self):

        providers = []
        if Configuration.integration('3M'):
            providers.append(BibliothecaBibliographicCoverageProvider)
        if Configuration.integration('Overdrive'):
            providers.append(OverdriveBibliographicCoverageProvider)
        if Configuration.integration('Axis 360'):
            providers.append(Axis360BibliographicCoverageProvider)

        if not providers:
            raise Exception("No licensed book sources configured, nothing to get coverage from!")
        super(BibliographicCoverageProvidersScript, self).__init__(providers)


class AvailabilityRefreshScript(IdentifierInputScript):
    """Refresh the availability information for a LicensePool, direct from the
    license source.
    """
    def do_run(self):
        args = self.parse_command_line(self._db)
        if not args.identifiers:
            raise Exception(
                "You must specify at least one identifier to refresh."
            )
        
        # We don't know exactly how big to make these batches, but 10 is
        # always safe.
        start = 0
        size = 10
        while start < len(args.identifiers):
            batch = args.identifiers[start:start+size]
            self.refresh_availability(batch)
            self._db.commit()
            start += size

    def refresh_availability(self, identifiers):
        provider = None
        identifier = identifiers[0]
        if identifier.type==Identifier.THREEM_ID:
            sweeper = BibliothecaCirculationSweep(self._db)
            sweeper.process_batch(identifiers)
        elif identifier.type==Identifier.OVERDRIVE_ID:
            api = OverdriveAPI(self._db)
            for identifier in identifiers:
                api.update_licensepool(identifier.identifier)
        elif identifier.type==Identifier.AXIS_360_ID:
            provider = Axis360BibliographicCoverageProvider(self._db)
            provider.process_batch(identifiers)
        else:
            self.log.warn("Cannot update coverage for %r" % identifier.type)

    
class LanguageListScript(LibraryInputScript):
    """List all the languages with at least one non-open access work
    in the collection.
    """

    def process_library(self, library):
        print library.short_name
        for item in self.languages(library):
            print item

    def languages(self, library):
        ":yield: A list of output lines, one per language."
        for abbreviation, count in library.estimated_holdings_by_language(
            include_open_access=False
        ).most_common():
            display_name = LanguageCodes.name_for_languageset(abbreviation)
            yield "%s %i (%s)" % (abbreviation, count, display_name)


class CompileTranslationsScript(Script):
    """A script to combine translation files for circulation, core
    and the admin interface, and compile the result to be used by the
    app. The combination step is necessary because Flask-Babel does not
    support multiple domains yet.
    """

    def run(self):
        languages = Configuration.localization_languages()
        for language in languages:
            base_path = "translations/%s/LC_MESSAGES" % language
            if not os.path.exists(base_path):
                logging.warn("No translations for configured language %s" % language)
                continue

            os.system("rm %(path)s/messages.po" % dict(path=base_path))
            os.system("cat %(path)s/*.po > %(path)s/messages.po" % dict(path=base_path))
        
        os.system("pybabel compile -f -d translations")


class InstanceInitializationScript(Script):
    """An idempotent script to initialize an instance of the Circulation Manager.

    This script is intended for use in servers, Docker containers, etc,
    when the Circulation Manager app is being installed. It initializes
    the database and sets an appropriate alias on the ElasticSearch index.

    Because it's currently run every time a container is started, it must
    remain idempotent.
    """

    def do_run(self, ignore_search=False):
        # Creates a "-current" alias on the Elasticsearch client.
        if not ignore_search:
            try:
                search_client = ExternalSearchIndex(self._db)
            except CannotLoadConfiguration as e:
                # Elasticsearch isn't configured, so do nothing.
                pass

        # Set a timestamp that represents the new database's version.
        db_init_script = DatabaseMigrationInitializationScript(_db=self._db)
        existing = get_one(self._db, Timestamp, service=db_init_script.name)
        if existing:
            # No need to run the script. We already have a timestamp.
            return
        db_init_script.run()

        # Create a secret key if one doesn't already exist.
        ConfigurationSetting.sitewide_secret(self._db, Configuration.SECRET_KEY)


class LoanReaperScript(Script):
    """Remove expired loans and holds whose owners have not yet synced
    with the loan providers.

    This stops the library from keeping a record of the final loans and
    holds of a patron who stopped using the circulation manager.

    If a loan or (more likely) hold is removed incorrectly, it will be
    restored the next time the patron syncs their loans feed.
    """
    def do_run(self):
        now = datetime.utcnow()

        # Reap loans and holds that we know have expired.
        for obj, what in ((Loan, 'loans'), (Hold, 'holds')):
            qu = self._db.query(obj).filter(obj.end < now)
            self._reap(qu, "expired %s" % what)

        for obj, what, max_age in (
                (Loan, 'loans', timedelta(days=90)),
                (Hold, 'holds', timedelta(days=365)),
        ):
            # Reap loans and holds which have no end date and are very
            # old. It's very likely these loans and holds have expired
            # and we simply don't have the information.
            older_than = now - max_age
            qu = self._db.query(obj).join(obj.license_pool).filter(
                obj.end == None).filter(
                    obj.start < older_than).filter(
                        LicensePool.open_access == False
                    )
            explain = "%s older than %s" % (
                what, older_than.strftime("%Y-%m-%d")
            )
            self._reap(qu, explain)

    def _reap(self, qu, what):
        """Delete every database object that matches the given query.

        :param qu: The query that yields objects to delete.
        :param what: A human-readable explanation of what's being
                     deleted.
        """
        counter = 0
        print "Reaping %d %s." % (qu.count(), what)
        for o in qu:
            self._db.delete(o)
            counter += 1
            if not counter % 100:
                print counter
                self._db.commit()
        self._db.commit()


class DisappearingBookReportScript(Script):

    """Print a TSV-format report on books that used to be in the
    collection, or should be in the collection, but aren't.
    """
    
    def do_run(self):
        qu = self._db.query(LicensePool).filter(
            LicensePool.open_access==False).filter(
                LicensePool.suppressed==False).filter(
                    LicensePool.licenses_owned<=0).order_by(
                        LicensePool.availability_time.desc())
        first_row = ["Identifier",
                     "Title",
                     "Author",
                     "First seen",
                     "Last seen (best guess)",
                     "Current licenses owned",
                     "Current licenses available",
                     "Changes in number of licenses",
                     "Changes in title availability",
        ]
        print "\t".join(first_row)

        for pool in qu:
            self.explain(pool)

    def investigate(self, licensepool):
        """Find when the given LicensePool might have disappeared from the
        collection.

        :param licensepool: A LicensePool.

        :return: a 3-tuple (last_seen, title_removal_events,
        license_removal_events).

        `last_seen` is the latest point at which we knew the book was
        circulating. If we never knew the book to be circulating, this
        is the first time we ever saw the LicensePool.

        `title_removal_events` is a query that returns CirculationEvents
        in which this LicensePool was removed from the remote collection.

        `license_removal_events` is a query that returns
        CirculationEvents in which LicensePool.licenses_owned went
        from having a positive number to being zero or a negative
        number.
        """
        first_activity = None
        most_recent_activity = None

        # If we have absolutely no information about the book ever
        # circulating, we act like we lost track of the book
        # immediately after seeing it for the first time.
        last_seen = licensepool.availability_time

        # If there's a recorded loan or hold on the book, that can
        # push up the last time the book was known to be circulating.
        for l in (licensepool.loans, licensepool.holds):
            for item in l:
                if not last_seen or item.start > last_seen:
                    last_seen = item.start
                    
        # Now we look for relevant circulation events. First, an event
        # where the title was explicitly removed is pretty clearly
        # a 'last seen'.
        base_query = self._db.query(CirculationEvent).filter(
            CirculationEvent.license_pool==licensepool).order_by(
                CirculationEvent.start.desc()
            )
        title_removal_events = base_query.filter(
            CirculationEvent.type==CirculationEvent.DISTRIBUTOR_TITLE_REMOVE
        )
        if title_removal_events.count():
            candidate = title_removal_events[-1].start
            if not last_seen or candidate > last_seen:
                last_seen = candidate
        
        # Also look for an event where the title went from a nonzero
        # number of licenses to a zero number of licenses. That's a
        # good 'last seen'.
        license_removal_events = base_query.filter(
            CirculationEvent.type==CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE,
        ).filter(
            CirculationEvent.old_value>0).filter(
                CirculationEvent.new_value<=0
            )
        if license_removal_events.count():
            candidate = license_removal_events[-1].start
            if not last_seen or candidate > last_seen:
                last_seen = candidate
        
        return last_seen, title_removal_events, license_removal_events

    format = "%Y-%m-%d"

    def explain(self, licensepool):
        edition = licensepool.presentation_edition
        identifier = licensepool.identifier
        last_seen, title_removal_events, license_removal_events = self.investigate(
            licensepool
        )

        data = ["%s %s" % (identifier.type, identifier.identifier)]
        if edition:
            data.extend([edition.title, edition.author])
        if licensepool.availability_time:
            first_seen = licensepool.availability_time.strftime(self.format)
        else:
            first_seen = ''
        data.append(first_seen)
        if last_seen:
            last_seen = last_seen.strftime(self.format)
        else:
            last_seen = ''
        data.append(last_seen)
        data.append(licensepool.licenses_owned)
        data.append(licensepool.licenses_available)

        license_removals = []
        for event in license_removal_events:
            description =u"%s: %s→%s" % (
                    event.start.strftime(self.format), event.old_value,
                event.new_value
            )
            license_removals.append(description)
        data.append(", ".join(license_removals))

        title_removals = [event.start.strftime(self.format)
                          for event in title_removal_events]
        data.append(", ".join(title_removals))
        
        print "\t".join([unicode(x).encode("utf8") for x in data])


class NYTBestSellerListsScript(Script):

    def __init__(self, include_history=False):
        super(NYTBestSellerListsScript, self).__init__()
        self.include_history = include_history

    def do_run(self):
        self.api = NYTBestSellerAPI.from_config(self._db)
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

class OPDSForDistributorsImportScript(OPDSImportScript):
    """Import all books from the OPDS feed associated with a collection
    that requires authentication."""

    IMPORTER_CLASS = OPDSForDistributorsImporter
    MONITOR_CLASS = OPDSForDistributorsImportMonitor
    PROTOCOL = OPDSForDistributorsImporter.NAME

class OPDSForDistributorsReaperScript(OPDSImportScript):
    """Get all books from the OPDS feed associated with a collection
    to find out if any have been removed."""

    IMPORTER_CLASS = OPDSForDistributorsImporter
    MONITOR_CLASS = OPDSForDistributorsReaperMonitor
    PROTOCOL = OPDSForDistributorsImporter.NAME

class LaneResetScript(LibraryInputScript):
    """Reset a library's lanes based on language configuration or estimates
    of the library's current collection."""

    @classmethod
    def arg_parser(cls, _db):
        parser = LibraryInputScript.arg_parser(_db)
        parser.add_argument(
            '--reset',
            help="Actually reset the lanes as opposed to showing what would happen.",
            action='store_true'
        )
        return parser

    def do_run(self, output=sys.stdout, **kwargs):
        parsed = self.parse_command_line(self._db, **kwargs)
        libraries = parsed.libraries
        self.reset = parsed.reset
        if not self.reset:
            self.log.info(
                "This is a dry run. Nothing will actually change in the database."
            )
            self.log.info(
                "Run with --reset to change the database."
            )

        if libraries and self.reset:
            self.log.warn(
                """This is not a drill.
Running this script will permanently reset the lanes for %d libraries. Any lanes created from
custom lists will be deleted (though the lists themselves will be preserved).
Sleeping for five seconds to give you a chance to back out.
You'll get another chance to back out before the database session is committed.""",
                len(libraries)
            )
            time.sleep(5)
        self.process_libraries(libraries)

        new_lane_output = "New Lane Configuration:"
        for library in libraries:
            new_lane_output += "\n\nLibrary '%s':\n" % library.name

            def print_lanes_for_parent(parent):
                lanes = self._db.query(Lane).filter(Lane.library==library).filter(Lane.parent==parent).order_by(Lane.priority)
                lane_output = ""
                for lane in lanes:
                    lane_output += "  " + ("  " * len(list(lane.parentage)))  + lane.display_name + "\n"
                    lane_output += print_lanes_for_parent(lane)
                return lane_output

            new_lane_output += print_lanes_for_parent(None)

        output.write(new_lane_output)

        if self.reset:
            self.log.warn("All done. Sleeping for five seconds before committing.")
            time.sleep(5)
            self._db.commit()

    def process_library(self, library):
        create_default_lanes(self._db, library)

class ODLBibliographicImportScript(OPDSImportScript):
    """Import bibliographic information from the feed associated
    with an ODL collection."""

    IMPORTER_CLASS = ODLBibliographicImporter
    MONITOR_CLASS = ODLBibliographicImportMonitor
    PROTOCOL = ODLBibliographicImporter.NAME
