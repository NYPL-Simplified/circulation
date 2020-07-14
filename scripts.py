# encoding: utf-8
import argparse
import csv
import logging
import os
import sys
import time
from cStringIO import StringIO
from datetime import (
    datetime,
    timedelta,
)

from enum import Enum
from sqlalchemy import (
    or_,
)

from api.adobe_vendor_id import (
    AuthdataUtility,
)
from api.bibliotheca import (
    BibliothecaCirculationSweep
)
from api.config import (
    CannotLoadConfiguration,
    Configuration,
)
from api.controller import CirculationManager
from api.lanes import create_default_lanes
from api.local_analytics_exporter import LocalAnalyticsExporter
from api.marc import LibraryAnnotator as MARCLibraryAnnotator
from api.novelist import (
    NoveListAPI
)
from api.nyt import NYTBestSellerAPI
from api.odl import (
    ODLImporter,
    ODLImportMonitor,
    SharedODLImporter,
    SharedODLImportMonitor,
)
from api.onix import ONIXExtractor
from api.opds_for_distributors import (
    OPDSForDistributorsImporter,
    OPDSForDistributorsImportMonitor,
    OPDSForDistributorsReaperMonitor,
)
from api.overdrive import (
    OverdriveAPI,
)
from core.entrypoint import EntryPoint
from core.external_list import CustomListFromCSV
from core.external_search import ExternalSearchIndex
from core.lane import Lane
from core.lane import (
    Pagination,
    Facets,
    FeaturedFacets,
)
from core.marc import MARCExporter
from core.metadata_layer import (
    CirculationData,
    FormatData,
    ReplacementPolicy,
    LinkData,
)
from core.metadata_layer import MARCExtractor
from core.mirror import MirrorUploader
from core.model import (
    CachedMARCFile,
    CirculationEvent,
    Collection,
    ConfigurationSetting,
    Contribution,
    CustomList,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    get_one,
    Hold,
    Hyperlink,
    Identifier,
    LicensePool,
    Loan,
    Representation,
    RightsStatus,
    SessionManager,
    Subject,
    Timestamp,
    Work,
)
from core.model.configuration import ExternalIntegrationLink
from core.opds import (
    AcquisitionFeed,
)
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    OPDSImporter,
)
from core.scripts import OPDSImportScript
from core.scripts import (
    Script as CoreScript,
    DatabaseMigrationInitializationScript,
    IdentifierInputScript,
    LaneSweeperScript,
    LibraryInputScript,
    PatronInputScript,
    TimestampScript,
)
from core.util import LanguageCodes
from core.util.opds_writer import (
    OPDSFeed,
)


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


class CacheRepresentationPerLane(TimestampScript, LaneSweeperScript):

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

    def __init__(self, _db=None, cmd_args=None, testing=False, manager=None,
                 *args, **kwargs):
        """Constructor.
        :param _db: A database connection.
        :param cmd_args: A mock set of command-line arguments, to use instead
           of looking at the actual command line.
        :param testing: If this method creates a CirculationManager object,
           this value will be passed in to its constructor as its value for
           `testing`.
        :param manager: A mock CirculationManager object, to use instead
           of creating a new one (creating a CirculationManager object is
           very time-consuming).
        :param *args: Positional arguments to pass to the superconstructor.
        :param **kwargs: Keyword arguments to pass to the superconstructor.
        """

        super(CacheRepresentationPerLane, self).__init__(_db, *args, **kwargs)
        self.parse_args(cmd_args)
        if not manager:
            manager = CirculationManager(self._db, testing=testing)
        from api.app import app
        app.manager = manager
        self.app = app
        self.base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY).value

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

    def process_library(self, library):
        begin = time.time()
        client = self.app.test_client()
        ctx = self.app.test_request_context(base_url=self.base_url)
        ctx.push()
        super(CacheRepresentationPerLane, self).process_library(library)
        ctx.pop()
        end = time.time()
        self.log.info(
            "Processed library %s in %.2fsec", library.short_name, end-begin
        )

    def process_lane(self, lane):
        """Generate a number of feeds for this lane.
        One feed will be generated for each combination of Facets and
        Pagination objects returned by facets() and pagination().
        """
        cached_feeds = []
        for facets in self.facets(lane):
            for pagination in self.pagination(lane):
                extra_description = ""
                if facets:
                    extra_description += " Facets: %s." % facets.query_string
                if pagination:
                    extra_description += " Pagination: %s." % pagination.query_string
                self.log.info(
                    "Generating feed for %s.%s", lane.full_identifier,
                    extra_description
                )
                a = time.time()
                feed = self.do_generate(lane, facets, pagination)
                b = time.time()
                if feed:
                    cached_feeds.append(feed)
                    self.log.info(
                        "Took %.2f sec to make %d bytes.", (b-a),
                        len(feed.data)
                    )
        total_size = sum(len(x.data) for x in cached_feeds)
        return cached_feeds

    def facets(self, lane):
        """Yield a Facets object for each set of facets this
        script is expected to handle.
        :param lane: The lane under consideration. (Different lanes may have
        different available facets.)
        :yield: A sequence of Facets objects.
        """
        yield None

    def pagination(self, lane):
        """Yield a Pagination object for each page of a feed this
        script is expected to handle.
        :param lane: The lane under consideration. (Different lanes may have
        different pagination rules.)
        :yield: A sequence of Pagination objects.
        """
        yield None


class CacheFacetListsPerLane(CacheRepresentationPerLane):
    """Cache the first two pages of every relevant facet list for this lane."""

    name = "Cache paginated OPDS feed for each lane"

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

        available = [x.INTERNAL_NAME for x in EntryPoint.ENTRY_POINTS]
        entrypoint_help = 'Generate feeds for this entry point within each lane. Possible values: %s.' % (
            ", ".join(available)
        )
        parser.add_argument(
            '--entrypoint',
            help=entrypoint_help,
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
        self.entrypoints = parsed.entrypoint
        self.pages = parsed.pages
        return parsed

    def facets(self, lane):
        """This script covers a user-specified combination of facets, but it
        defaults to using every combination of available facets for
        the given lane with a certain sort order.
        This means every combination of availability, collection, and
        entry point.
        That's a whole lot of feeds, which is why this script isn't
        actually used -- by the time we generate all of then, they've
        expired.
        """
        library = lane.get_library(self._db)
        default_order = library.default_facet(Facets.ORDER_FACET_GROUP_NAME)
        allowed_orders = library.enabled_facets(Facets.ORDER_FACET_GROUP_NAME)
        chosen_orders = self.orders or [default_order]

        allowed_entrypoint_names = [
            x.INTERNAL_NAME for x in library.entrypoints
        ]
        default_entrypoint_name = None
        if allowed_entrypoint_names:
            default_entrypoint_name = allowed_entrypoint_names[0]

        chosen_entrypoints = self.entrypoints or allowed_entrypoint_names

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

        top_level = (lane.parent is None)
        for entrypoint_name in chosen_entrypoints:
            entrypoint = EntryPoint.BY_INTERNAL_NAME.get(entrypoint_name)
            if not entrypoint:
                logging.warn("Ignoring unknown entry point %s" % entrypoint_name)
                continue
            if not entrypoint_name in allowed_entrypoint_names:
                logging.warn("Ignoring disabled entry point %s" % entrypoint_name)
                continue
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
                        facets = Facets(
                            library=library, collection=collection,
                            availability=availability,
                            entrypoint=entrypoint,
                            entrypoint_is_default=(
                                top_level and
                                entrypoint.INTERNAL_NAME == default_entrypoint_name
                            ),
                            order=order, order_ascending=True
                        )
                        yield facets

    def pagination(self, lane):
        """This script covers a user-specified number of pages."""
        page = Pagination.default()
        for pagenum in range(0, self.pages):
            yield page
            page = page.next_page
            if not page:
                # There aren't enough books to fill `self.pages`
                # pages. Stop working.
                break

    def do_generate(self, lane, facets, pagination, feed_class=None):
        feeds = []
        title = lane.display_name
        library = lane.get_library(self._db)
        annotator = self.app.manager.annotator(lane, facets=facets)
        url = annotator.feed_url(lane, facets=facets, pagination=pagination)
        feed_class = feed_class or AcquisitionFeed
        return feed_class.page(
            _db=self._db, title=title, url=url, worklist=lane,
            annotator=annotator, facets=facets, pagination=pagination,
            max_age=0
        )


class CacheOPDSGroupFeedPerLane(CacheRepresentationPerLane):

    name = "Cache OPDS grouped feed for each lane"

    def should_process_lane(self, lane):
        # OPDS grouped feeds are only generated for lanes that have sublanes.
        if not lane.children:
            return False
        if self.max_depth is not None and lane.depth > self.max_depth:
            return False
        return True

    def do_generate(self, lane, facets, pagination, feed_class=None):
        title = lane.display_name
        annotator = self.app.manager.annotator(lane, facets=facets)
        url = annotator.groups_url(lane, facets)
        feed_class = feed_class or AcquisitionFeed

        # Since grouped feeds are only cached for lanes that have sublanes,
        # there's no need to consider the case of a lane with no sublanes,
        # unlike the corresponding code in OPDSFeedController.groups()
        return feed_class.groups(
            _db=self._db, title=title, url=url, worklist=lane,
            annotator=annotator, max_age=0, facets=facets
        )

    def facets(self, lane):
        """Generate a Facets object for each of the library's enabled
        entrypoints.
        This is the only way grouped feeds are ever generated, so there is
        no way to override this.
        """
        top_level = (lane.parent is None)
        library = lane.get_library(self._db)

        # If the WorkList has explicitly defined EntryPoints, we want to
        # create a grouped feed for each EntryPoint. Otherwise, we want
        # to create a single grouped feed with no particular EntryPoint.
        #
        # We use library.entrypoints instead of lane.entrypoints
        # because WorkList.entrypoints controls which entry points you
        # can *switch to* from a given WorkList. We're handling the
        # case where you switched further up the hierarchy and now
        # you're navigating downwards.
        entrypoints = list(library.entrypoints) or [None]
        default_entrypoint = entrypoints[0]
        for entrypoint in entrypoints:
            facets = FeaturedFacets(
                minimum_featured_quality=library.minimum_featured_quality,
                uses_customlists=lane.uses_customlists,
                entrypoint=entrypoint,
                entrypoint_is_default=(
                    top_level and entrypoint is default_entrypoint
                )
            )
            yield facets

class CacheMARCFiles(LaneSweeperScript):
    """Generate and cache MARC files for each input library."""

    name = "Cache MARC files"

    @classmethod
    def arg_parser(cls, _db):
        parser = LaneSweeperScript.arg_parser(_db)
        parser.add_argument(
            '--max-depth',
            help='Stop processing lanes once you reach this depth.',
            type=int,
            default=0,
        )
        parser.add_argument(
            '--force',
            help="Generate new MARC files even if MARC files have already been generated recently enough",
            dest='force', action='store_true',
        )
        return parser

    def __init__(self, _db=None, cmd_args=None, *args, **kwargs):
        super(CacheMARCFiles, self).__init__(_db, *args, **kwargs)
        self.parse_args(cmd_args)

    def parse_args(self, cmd_args=None):
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        self.max_depth = parsed.max_depth
        self.force = parsed.force
        return parsed

    def should_process_library(self, library):
        integration = ExternalIntegration.lookup(
            self._db, ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL, library)
        return (integration is not None)

    def process_library(self, library):
        if self.should_process_library(library):
            super(CacheMARCFiles, self).process_library(library)
            self.log.info("Processed library %s" % library.name)

    def should_process_lane(self, lane):
        if isinstance(lane, Lane):
            if self.max_depth is not None and lane.depth > self.max_depth:
                return False
            if lane.size == 0:
                return False
        return True

    def process_lane(self, lane, exporter=None):
        # Generate a MARC file for this lane, if one has not been generated recently enough.
        if isinstance(lane, Lane):
            library = lane.library
        else:
            library = lane.get_library(self._db)

        annotator = MARCLibraryAnnotator(library)
        exporter = exporter or MARCExporter.from_config(library)

        update_frequency = ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.UPDATE_FREQUENCY, library, exporter.integration
        ).int_value
        if update_frequency is None:
            update_frequency = MARCExporter.DEFAULT_UPDATE_FREQUENCY

        last_update = None
        files_q = self._db.query(CachedMARCFile).filter(
            CachedMARCFile.library==library
        ).filter(
            CachedMARCFile.lane==(lane if isinstance(lane, Lane) else None),
        ).order_by(CachedMARCFile.end_time.desc())

        if files_q.count() > 0:
            last_update = files_q.first().end_time
        if not self.force and last_update and (last_update > datetime.utcnow() - timedelta(days=update_frequency)):
            self.log.info("Skipping lane %s because last update was less than %d days ago" % (lane.display_name, update_frequency))
            return

        # To find the storage integration for the exporter, first find the
        # external integration link associated with the exporter's external
        # integration.
        integration_link = get_one(
            self._db, ExternalIntegrationLink,
            external_integration_id=exporter.integration.id,
            purpose=ExternalIntegrationLink.MARC
        )
        # Then use the "other" integration value to find the storage integration.
        storage_integration = get_one(self._db, ExternalIntegration,
            id=integration_link.other_integration_id
        )

        if not storage_integration:
            self.log.info("No storage External Integration was found.")
            return

        # First update the file with ALL the records.
        records = exporter.records(
            lane, annotator, storage_integration
        )

        # Then create a new file with changes since the last update.
        start_time = None
        if last_update:
            # Allow one day of overlap to ensure we don't miss anything due to script timing.
            start_time = last_update - timedelta(days=1)

            records = exporter.records(
                lane, annotator, storage_integration, start_time=start_time
            )


class AdobeAccountIDResetScript(PatronInputScript):

    @classmethod
    def arg_parser(cls, _db):
        parser = super(AdobeAccountIDResetScript, cls).arg_parser(_db)
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
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self.log.info(
                ' Deleting "%s" credential "%s"',
                credential.type, credential.credential
            )
            if self.delete:
                self._db.delete(credential)

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


class InstanceInitializationScript(TimestampScript):
    """An idempotent script to initialize an instance of the Circulation Manager.

    This script is intended for use in servers, Docker containers, etc,
    when the Circulation Manager app is being installed. It initializes
    the database and sets an appropriate alias on the ElasticSearch index.

    Because it's currently run every time a container is started, it must
    remain idempotent.
    """

    name = "Instance initialization"

    TEST_SQL = "select * from timestamps limit 1"

    def run(self, *args, **kwargs):
        # Create a special database session that doesn't initialize
        # the ORM -- this could be fatal if there are migration
        # scripts that haven't run yet.
        #
        # In fact, we don't even initialize the database schema,
        # because that's the thing we're trying to check for.
        url = Configuration.database_url()
        _db = SessionManager.session(
            url, initialize_data=False, initialize_schema=False
        )

        results = None
        try:
            # We need to check for the existence of a known table --
            # this will demonstrate that this script has been run before --
            # but we don't need to actually look at what we get from the
            # database.
            #
            # Basically, if this succeeds, we can bail out and not run
            # the rest of the script.
            results = list(_db.execute(self.TEST_SQL))
        except Exception, e:
            # This did _not_ succeed, so the schema is probably not
            # initialized and we do need to run this script.. This
            # database session is useless now, but we'll create a new
            # one during the super() call, and use that one to do the
            # work.
            _db.close()

        if results is None:
            super(InstanceInitializationScript, self).run(*args, **kwargs)
        else:
            self.log.error("I think this site has already been initialized; doing nothing.")

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
        existing = get_one(
            self._db, Timestamp, service=db_init_script.name,
            service_type=Timestamp.SCRIPT_TYPE
        )
        if existing:
            # No need to run the script. We already have a timestamp.
            return
        db_init_script.run()

        # Create a secret key if one doesn't already exist.
        ConfigurationSetting.sitewide_secret(self._db, Configuration.SECRET_KEY)


class LoanReaperScript(TimestampScript):
    """Remove expired loans and holds whose owners have not yet synced
    with the loan providers.

    This stops the library from keeping a record of the final loans and
    holds of a patron who stopped using the circulation manager.

    If a loan or (more likely) hold is removed incorrectly, it will be
    restored the next time the patron syncs their loans feed.
    """

    name = "Remove expired loans and holds from local database"

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


class NYTBestSellerListsScript(TimestampScript):

    name = "Update New York Times best-seller lists"

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


class DirectoryImportScript(TimestampScript):
    """Import some books into a collection, based on a file containing
    metadata and directories containing ebook and cover files.
    """

    class CollectionType(Enum):
        OPEN_ACCESS = 'OPEN_ACCESS'
        PROTECTED_ACCESS = 'PROTECTED_ACCESS'

        def __str__(self):
            return self.name

    name = "Import new titles from a directory on disk"

    @classmethod
    def arg_parser(cls, _db):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--collection-name',
            help=u'Titles will be imported into a collection with this name. The collection will be created if it does not already exist.',
            required=True
        )
        parser.add_argument(
            '--collection-type',
            help=u'Collection type. Valid values are: OPEN_ACCESS (default), PROTECTED_ACCESS.',
            type=DirectoryImportScript.CollectionType,
            choices=list(DirectoryImportScript.CollectionType),
            default=DirectoryImportScript.CollectionType.OPEN_ACCESS
        )
        parser.add_argument(
            '--data-source-name',
            help=u'All data associated with this import activity will be recorded as originating with this data source. The data source will be created if it does not already exist.',
            required=True
        )
        parser.add_argument(
            '--metadata-file',
            help=u'Path to a file containing MARC or ONIX 3.0 metadata for every title in the collection',
            required=True
        )
        parser.add_argument(
            '--metadata-format',
            help=u'Format of the metadata file ("marc" or "onix")',
            default='marc',
        )
        parser.add_argument(
            '--cover-directory',
            help=u'Directory containing a full-size cover image for every title in the collection.',
        )
        parser.add_argument(
            '--ebook-directory',
            help=u'Directory containing an EPUB or PDF file for every title in the collection.',
            required=True
        )
        RS = RightsStatus
        rights_uris = ", ".join(RS.OPEN_ACCESS)
        parser.add_argument(
            '--rights-uri',
            help=u"A URI explaining the rights status of the works being uploaded. Acceptable values: %s" % rights_uris,
            required=True
        )
        parser.add_argument(
            '--dry-run',
            help=u"Show what would be imported, but don't actually do the import.",
            action='store_true',
        )
        return parser

    def do_run(self, cmd_args=None):
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        collection_name = parsed.collection_name
        collection_type = parsed.collection_type
        data_source_name = parsed.data_source_name
        metadata_file = parsed.metadata_file
        metadata_format = parsed.metadata_format
        cover_directory = parsed.cover_directory
        ebook_directory = parsed.ebook_directory
        rights_uri = parsed.rights_uri
        dry_run = parsed.dry_run
        return self.run_with_arguments(
            collection_name, collection_type, data_source_name,
            metadata_file, metadata_format, cover_directory,
            ebook_directory, rights_uri, dry_run
        )

    def run_with_arguments(
            self, collection_name, collection_type, data_source_name, metadata_file,
            metadata_format, cover_directory, ebook_directory, rights_uri,
            dry_run
    ):
        if dry_run:
            self.log.warn(
                "This is a dry run. No files will be uploaded and nothing will change in the database."
            )

        collection, mirrors = self.load_collection(collection_name, collection_type, data_source_name)

        if not collection or not mirrors:
            return

        self.timestamp_collection = collection

        if dry_run:
            mirrors = None

        replacement_policy = ReplacementPolicy.from_license_source(self._db)
        replacement_policy.mirrors = mirrors
        metadata_records = self.load_metadata(metadata_file, metadata_format, data_source_name)
        for metadata in metadata_records:
            self.work_from_metadata(
                collection,
                collection_type,
                metadata,
                replacement_policy,
                cover_directory,
                ebook_directory,
                rights_uri
            )

            for licensepool in collection.licensepools:
                licensepool.self_hosted = True

            if not dry_run:
                self._db.commit()

    def load_collection(self, collection_name, collection_type, data_source_name):
        """Locate a Collection with the given name.

        If the collection is found, it will be associated
        with the given data source and configured with existing
        covers and books mirror configurations.

        :param collection_name: Name of the Collection.
        :type collection_name: string

        :param collection_type: Type of the collection: open access/proteceted access.
        :type collection_name: DirectoryImportScript.CollectionType

        :param data_source_name: Associate this data source with
            the Collection if it does not already have a data source.
            A DataSource object will be created if necessary.
        :type data_source_name: string

        :return: A 2-tuple (Collection, list of MirrorUploader instances)
        :rtype: Tuple[Collection, List[MirrorUploader]]
        """
        collection, is_new = Collection.by_name_and_protocol(
            self._db, collection_name, ExternalIntegration.MANUAL
        )

        if is_new:
            self.log.error(
                "An existing collection must be used and should be set up before running this script."
            )
            return None, None

        mirrors = dict(covers_mirror=None, books_mirror=None)

        types = [
            ExternalIntegrationLink.COVERS,
            ExternalIntegrationLink.OPEN_ACCESS_BOOKS
            if collection_type == DirectoryImportScript.CollectionType.OPEN_ACCESS
            else ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS
        ]
        for type in types:
            mirror_for_type = MirrorUploader.for_collection(collection, type)
            if not mirror_for_type:
                self.log.error(
                    "An existing %s mirror integration should be assigned to the collection before running the script." % type
                )
                return None, None
            mirrors[type] = mirror_for_type

        data_source = DataSource.lookup(
            self._db, data_source_name, autocreate=True,
            offers_licenses=True
        )
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, data_source.name
        )

        return collection, mirrors

    def load_metadata(self, metadata_file, metadata_format, data_source_name):
        """Read a metadata file and convert the data into Metadata records."""
        metadata_records = []

        if metadata_format == 'marc':
            extractor = MARCExtractor()
        elif metadata_format == 'onix':
            extractor = ONIXExtractor()

        with open(metadata_file) as f:
            metadata_records.extend(extractor.parse(f, data_source_name))
        return metadata_records

    def work_from_metadata(self, collection, collection_type, metadata, policy, *args, **kwargs):
        """Creates a Work instance from metadata

        :param collection: Target collection
        :type collection: Collection

        :param collection_type: Collection's type: open access/protected access
        :type collection_type: DirectoryImportScript.CollectionType

        :param metadata: Book's metadata
        :type metadata: Metadata

        :param policy: Replacement policy
        :type policy: ReplacementPolicy

        :return: Work object
        :rtype: Work
        """
        self.annotate_metadata(collection_type, metadata, policy, *args, **kwargs)

        if not metadata.circulation:
            # We cannot actually provide access to the book so there
            # is no point in proceeding with the import.
            return

        edition, new = metadata.edition(self._db)
        metadata.apply(edition, collection, replace=policy)
        data_source = metadata.data_source(self._db)
        [pool] = [x for x in edition.license_pools
                  if x.data_source == data_source]
        if new:
            self.log.info("Created new edition for %s", edition.title)
        else:
            self.log.info("Updating existing edition for %s", edition.title)

        work, ignore = pool.calculate_work()
        if work:
            work.set_presentation_ready()
            self.log.info(
                "FINALIZED %s/%s/%s" % (work.title, work.author, work.sort_author)
            )
        return work

    def annotate_metadata(
            self,
            collection_type,
            metadata,
            policy,
            cover_directory,
            ebook_directory,
            rights_uri):
        """Add a CirculationData and possibly an extra LinkData to `metadata`

        :param collection_type: Collection's type: open access/protected access
        :type collection_type: DirectoryImportScript.CollectionType

        :param metadata: Book's metadata
        :type metadata: Metadata

        :param policy: Replacement policy
        :type policy: ReplacementPolicy

        :param cover_directory: Directory containing book covers
        :type cover_directory: string

        :param ebook_directory: Directory containing books
        :type ebook_directory: string

        :param rights_uri: URI explaining the rights status of the works being uploaded
        :type rights_uri: string
        """
        identifier, ignore = metadata.primary_identifier.load(self._db)
        data_source = metadata.data_source(self._db)
        mirrors = policy.mirrors

        circulation_data = self.load_circulation_data(
            collection_type,
            identifier,
            data_source,
            ebook_directory,
            mirrors,
            metadata.title,
            rights_uri
        )
        if not circulation_data:
            # There is no point in contining.
            return
        metadata.circulation = circulation_data

        # If a cover image is available, add it to the Metadata
        # as a link.
        cover_link = None
        if cover_directory:
            cover_link = self.load_cover_link(
                identifier, data_source, cover_directory, mirrors
            )
        if cover_link:
            metadata.links.append(cover_link)
        else:
            logging.info(
                "Proceeding with import even though %r has no cover.",
                identifier
            )

    def load_circulation_data(
            self,
            collection_type,
            identifier,
            data_source,
            ebook_directory,
            mirrors,
            title,
            rights_uri):
        """Loads an actual copy of a book from disk

        :param collection_type: Collection's type: open access/protected access
        :type collection_type: DirectoryImportScript.CollectionType

        :param identifier: Book's identifier
        :type identifier: Identifier

        :param data_source: DataSource object
        :type data_source: DataSource

        :param ebook_directory: Directory containing books
        :type ebook_directory: string

        :param mirrors: Dictionary containing mirrors for books and their covers
        :type mirrors: Dict[string, MirrorUploader]

        :param title: Book's title
        :type title: string

        :param rights_uri: URI explaining the rights status of the works being uploaded
        :type rights_uri: string

        :return: A CirculationData that contains the book as an open-access
            download, or None if no such book can be found
        :rtype: CirculationData
        """
        ignore, book_media_type, book_content = self._locate_file(
            identifier.identifier, ebook_directory,
            Representation.COMMON_EBOOK_EXTENSIONS,
            "ebook file",
        )
        if not book_content:
            # We couldn't find an actual copy of the book, so there is
            # no point in proceeding.
            return

        book_mirror = mirrors[
            ExternalIntegrationLink.OPEN_ACCESS_BOOKS
            if collection_type == DirectoryImportScript.CollectionType.OPEN_ACCESS
            else ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS
        ] if mirrors else None

        # Use the S3 storage for books.
        if book_mirror:
            book_url = book_mirror.book_url(
                identifier,
                '.' + Representation.FILE_EXTENSIONS[book_media_type],
                open_access=collection_type == DirectoryImportScript.CollectionType.OPEN_ACCESS,
                data_source=data_source,
                title=title
            )
        else:
            # This is a dry run and we won't be mirroring anything.
            book_url = identifier.identifier + "." + Representation.FILE_EXTENSIONS[book_media_type]

        book_link_rel = \
            Hyperlink.OPEN_ACCESS_DOWNLOAD \
            if collection_type == DirectoryImportScript.CollectionType.OPEN_ACCESS \
            else Hyperlink.GENERIC_OPDS_ACQUISITION
        book_link = LinkData(
            rel=book_link_rel,
            href=book_url,
            media_type=book_media_type,
            content=book_content
        )
        formats = [
            FormatData(
                content_type=book_media_type,
                drm_scheme=DeliveryMechanism.NO_DRM,
                link=book_link,
            )
        ]
        circulation_data = CirculationData(
            data_source=data_source.name,
            primary_identifier=identifier,
            links=[book_link],
            formats=formats,
            default_rights_uri=rights_uri,
        )
        return circulation_data

    def load_cover_link(self, identifier, data_source, cover_directory, mirrors):
        """Load an actual book cover from disk.
        
        :return: A LinkData containing a cover of the book, or None
            if no book cover can be found.
        """
        cover_filename, cover_media_type, cover_content = self._locate_file(
            identifier.identifier, cover_directory,
            Representation.COMMON_IMAGE_EXTENSIONS, "cover image"
        )

        if not cover_content:
            return None
        cover_filename = (
            identifier.identifier
            + '.' + Representation.FILE_EXTENSIONS[cover_media_type]
        )

        # Use an S3 storage mirror for specifically for covers.
        if mirrors and mirrors[ExternalIntegrationLink.COVERS]:
            cover_url = mirrors[ExternalIntegrationLink.COVERS].cover_image_url(
                data_source, identifier, cover_filename
            )
        else:
            # This is a dry run and we won't be mirroring anything.
            cover_url = cover_filename

        cover_link = LinkData(
            rel=Hyperlink.IMAGE,
            href=cover_url,
            media_type=cover_media_type,
            content=cover_content,
        )
        return cover_link

    @classmethod
    def _locate_file(cls, base_filename, directory, extensions,
                     file_type="file", mock_filesystem_operations=None):
        """Find an acceptable file in the given directory.

        :param base_filename: A string to be used as the base of the filename.

        :param directory: Look for a file in this directory.

        :param extensions: Any of these extensions for the file is
        acceptable.

        :param file_type: Human-readable description of the type of
            file we're looking for. This is used only in a log warning if
            no file can be found.

        :param mock_filesystem_operations: A test may pass in a
            2-tuple of functions to replace os.path.exists and the 'open'
            function.

        :return: A 3-tuple. (None, None, None) if no file can be
            found; otherwise (filename, media_type, contents).
        """
        if mock_filesystem_operations:
            exists_f, open_f = mock_filesystem_operations
        else:
            exists_f = os.path.exists
            open_f = open

        success_path = None
        media_type = None
        attempts = []
        for extension in extensions:
            for ext in (extension, extension.upper()):
                if not ext.startswith('.'):
                    ext = '.' + ext
                filename = base_filename + ext
                path = os.path.join(directory, filename)
                attempts.append(path)
                if exists_f(path):
                    media_type = Representation.MEDIA_TYPE_FOR_EXTENSION.get(
                        ext.lower()
                    )
                    content = None
                    with open_f(path) as fh:
                        content = fh.read()
                    return filename, media_type, content

        # If we went through that whole loop without returning,
        # we have failed.
        logging.warn(
            "Could not find %s for %s. Looked in: %s",
            file_type, base_filename, ", ".join(attempts)
        )
        return None, None, None


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

class NovelistSnapshotScript(TimestampScript, LibraryInputScript):

    def do_run(self, output=sys.stdout, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            try:
                api = NoveListAPI.from_config(library)
            except CannotLoadConfiguration as e:
                self.log.info(e.message)
                continue
            if (api):
                response = api.put_items_novelist(library)

                if (response):
                    result = "NoveList API Response\n"
                    result += str(response)

                    output.write(result)

class ODLImportScript(OPDSImportScript):
    """Import information from the feed associated
    with an ODL collection."""

    IMPORTER_CLASS = ODLImporter
    MONITOR_CLASS = ODLImportMonitor
    PROTOCOL = ODLImporter.NAME

class SharedODLImportScript(OPDSImportScript):
    IMPORTER_CLASS = SharedODLImporter
    MONITOR_CLASS = SharedODLImportMonitor
    PROTOCOL = SharedODLImporter.NAME

class LocalAnalyticsExportScript(Script):
    """Export circulation events for a date range to a CSV file."""

    @classmethod
    def arg_parser(cls, _db):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--start',
            help="Include circulation events that happened at or after this time.",
            required=True,
        )
        parser.add_argument(
            '--end',
            help="Include circulation events that happened before this time.",
            required=True,
        )
        return parser

    def do_run(self, output=sys.stdout, cmd_args=None, exporter=None):
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        start = parsed.start
        end = parsed.end

        exporter = exporter or LocalAnalyticsExporter()
        output.write(exporter.export(self._db, start, end))
