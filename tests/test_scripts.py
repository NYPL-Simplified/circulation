import pytest
import contextlib
import datetime
import flask
import json
from io import StringIO

from api.adobe_vendor_id import (
    AdobeVendorIDModel,
    AuthdataUtility,
    ShortClientTokenLibraryConfigurationScript,
)

from api.config import (
    temp_config,
    Configuration,
)

from api.novelist import (
    NoveListAPI
)

from core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EntryPoint,
)

from core.external_search import (
    MockExternalSearchIndex,
    mock_search_index,
)

from core.lane import (
    Lane,
    Facets,
    FeaturedFacets,
    Pagination,
    WorkList,
)

from core.metadata_layer import (
    CirculationData,
    IdentifierData,
    LinkData,
    Metadata,
    ReplacementPolicy,
)

from core.model import (
    CachedFeed,
    CachedMARCFile,
    ConfigurationSetting,
    create,
    Credential,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    get_one,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    RightsStatus,
    Timestamp,
    EditionConstants)
from core.model.configuration import ExternalIntegrationLink

from core.opds import AcquisitionFeed

from core.s3 import MockS3Uploader

from core.mirror import MirrorUploader

from core.marc import MARCExporter
from core.scripts import CollectionType

from core.util.flask_util import (
    Response,
    OPDSFeedResponse
)

from api.marc import LibraryAnnotator as  MARCLibraryAnnotator

from core.testing import (
    DatabaseTest,
)

from scripts import (
    AdobeAccountIDResetScript,
    CacheRepresentationPerLane,
    CacheFacetListsPerLane,
    CacheOPDSGroupFeedPerLane,
    CacheMARCFiles,
    DirectoryImportScript,
    InstanceInitializationScript,
    LanguageListScript,
    NovelistSnapshotScript,
    LocalAnalyticsExportScript,
)
from core.util.datetime_helpers import utc_now

class TestAdobeAccountIDResetScript(DatabaseTest):

    def test_process_patron(self):
        patron = self._patron()

        # This patron has old-style and new-style Credentials that link
        # them to Adobe account IDs (hopefully the same ID, though that
        # doesn't matter here.
        def set_value(credential):
            credential.value = "a credential"

        # Data source doesn't matter -- even if it's incorrect, a Credential
        # of the appropriate type will be deleted.
        data_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # Create two Credentials that will be deleted and one that will be
        # left alone.
        for type in (AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE,
                     AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
                     "Some other type"
        ):

            credential = Credential.lookup(
                self._db, data_source, type, patron,
                set_value, True
            )

        assert 3 == len(patron.credentials)

        # Run the patron through the script.
        script = AdobeAccountIDResetScript(self._db)

        # A dry run does nothing.
        script.delete = False
        script.process_patron(patron)
        self._db.commit()
        assert 3 == len(patron.credentials)

        # Now try it for real.
        script.delete = True
        script.process_patron(patron)
        self._db.commit()

        # The two Adobe-related credentials are gone. The other one remains.
        [credential] = patron.credentials
        assert "Some other type" == credential.type


class TestLaneScript(DatabaseTest):

    def setup_method(self):
        super(TestLaneScript, self).setup_method()
        base_url_setting = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY)
        base_url_setting.value = 'http://test-circulation-manager/'
        for k, v in [
                (Configuration.LARGE_COLLECTION_LANGUAGES, []),
                (Configuration.SMALL_COLLECTION_LANGUAGES, []),
                (Configuration.TINY_COLLECTION_LANGUAGES, ['eng', 'fre'])
        ]:
            ConfigurationSetting.for_library(
                k, self._default_library).value = json.dumps(v)


class TestCacheRepresentationPerLane(TestLaneScript):

    def test_should_process_lane(self):

        # Test that should_process_lane respects any specified
        # language restrictions.
        script = CacheRepresentationPerLane(
            self._db, ["--language=fre", "--language=English", "--language=none", "--min-depth=0"],
            manager=object()
        )
        assert ['fre', 'eng'] == script.languages

        english_lane = self._lane(languages=['eng'])
        assert True == script.should_process_lane(english_lane)

        no_english_lane = self._lane(languages=['spa','fre'])
        assert True == script.should_process_lane(no_english_lane)

        no_english_or_french_lane = self._lane(languages=['spa'])
        assert False == script.should_process_lane(no_english_or_french_lane)

        # Test that should_process_lane respects maximum depth
        # restrictions.
        script = CacheRepresentationPerLane(
            self._db, ["--max-depth=0", "--min-depth=0"],
            manager=object()
        )
        assert 0 == script.max_depth

        child = self._lane(display_name="sublane")
        parent = self._lane(display_name="parent")
        parent.sublanes=[child]
        assert True == script.should_process_lane(parent)
        assert False == script.should_process_lane(child)

        script = CacheRepresentationPerLane(
            self._db, ["--min-depth=1"], testing=True
        )
        assert 1 == script.min_depth
        assert False == script.should_process_lane(parent)
        assert True == script.should_process_lane(child)

    def test_process_lane(self):
        # process_lane() calls do_generate() once for every
        # combination of items yielded by facets() and pagination().

        class MockFacets(object):

            def __init__(self, query):
                self.query = query

            @property
            def query_string(self):
                return self.query

        facets1 = MockFacets("facets1")
        facets2 = MockFacets("facets2")
        page1 = Pagination.default()
        page2 = page1.next_page

        class Mock(CacheRepresentationPerLane):
            generated = []
            def do_generate(self, lane, facets, pagination):
                value = (lane, facets, pagination)
                response = Response("mock response")
                response.value = value
                self.generated.append(response)
                return response

            def facets(self, lane):
                yield facets1
                yield facets2

            def pagination(self, lane):
                yield page1
                yield page2

        lane = self._lane()
        script = Mock(self._db, manager=object(), cmd_args=[])
        generated = script.process_lane(lane)
        assert generated == script.generated

        c1, c2, c3, c4 = [x.value for x in script.generated]
        assert (lane, facets1, page1) == c1
        assert (lane, facets1, page2) == c2
        assert (lane, facets2, page1) == c3
        assert (lane, facets2, page2) == c4

    def test_default_facets(self):
        # By default, do_generate will only be called once, with facets=None.
        script = CacheRepresentationPerLane(
            self._db, manager=object(), cmd_args=[]
        )
        assert [None] == list(script.facets(object()))

    def test_default_pagination(self):
        # By default, do_generate will only be called once, with pagination=None.
        script = CacheRepresentationPerLane(
            self._db, manager=object(), cmd_args=[]
        )
        assert [None] == list(script.pagination(object()))


class TestCacheFacetListsPerLane(TestLaneScript):

    def test_arguments(self):
        # Verify that command-line arguments become attributes of
        # the CacheFacetListsPerLane object.
        script = CacheFacetListsPerLane(
            self._db, ["--order=title", "--order=added"],
            manager=object()
        )
        assert ['title', 'added'] == script.orders
        script = CacheFacetListsPerLane(
            self._db, ["--availability=all", "--availability=always"],
            manager=object()
        )
        assert ['all', 'always'] == script.availabilities

        script = CacheFacetListsPerLane(
            self._db, ["--collection=main", "--collection=full"],
            manager=object()
        )
        assert ['main', 'full'] == script.collections

        script = CacheFacetListsPerLane(
            self._db, ["--entrypoint=Audio", "--entrypoint=Book"],
            manager=object()
        )
        assert ['Audio', 'Book'] == script.entrypoints

        script = CacheFacetListsPerLane(
            self._db, ['--pages=1'], manager=object()
        )
        assert 1 == script.pages

    def test_facets(self):
        # Verify that CacheFacetListsPerLane.facets combines the items
        # found in the attributes created by command-line parsing.
        script = CacheFacetListsPerLane(self._db, manager=object(), cmd_args=[])
        script.orders = [Facets.ORDER_TITLE, Facets.ORDER_AUTHOR, "nonsense"]
        script.entrypoints = [
            AudiobooksEntryPoint.INTERNAL_NAME, "nonsense",
            EbooksEntryPoint.INTERNAL_NAME
        ]
        script.availabilities = [Facets.AVAILABLE_NOW, "nonsense"]
        script.collections = [Facets.COLLECTION_FULL, "nonsense"]

        # EbooksEntryPoint is normally a valid entry point, but we're
        # going to disable it for this library.
        setting = self._default_library.setting(EntryPoint.ENABLED_SETTING)
        setting.value = json.dumps([AudiobooksEntryPoint.INTERNAL_NAME])

        lane = self._lane()

        # We get one Facets object for every valid combination
        # of parameters. Here there are 2*1*1*1 combinations.
        f1, f2 = script.facets(lane)

        # The facets differ only in their .order.
        assert Facets.ORDER_TITLE == f1.order
        assert Facets.ORDER_AUTHOR == f2.order

        # All other fields are tied to the only acceptable values
        # given in the script attributes. The first (and only)
        # enabled entry point is treated as the default.
        for f in f1, f2:
            assert AudiobooksEntryPoint == f.entrypoint
            assert True == f.entrypoint_is_default
            assert Facets.AVAILABLE_NOW == f.availability
            assert Facets.COLLECTION_FULL == f.collection

        # The first entry point is treated as the default only for WorkLists
        # that have no parent. When the WorkList has a parent, the selected
        # entry point is treated as an explicit choice -- navigating downward
        # in the lane hierarchy ratifies the default value.
        sublane = self._lane(parent=lane)
        f1, f2 = script.facets(sublane)
        for f in f1, f2:
            assert False == f.entrypoint_is_default

    def test_pagination(self):
        script = CacheFacetListsPerLane(self._db, manager=object(), cmd_args=[])
        script.pages = 3
        lane = self._lane()
        p1, p2, p3 = script.pagination(lane)
        pagination = Pagination.default()
        assert pagination.query_string == p1.query_string
        assert pagination.next_page.query_string == p2.query_string
        assert pagination.next_page.next_page.query_string == p3.query_string

    def test_do_generate(self):
        # When it's time to generate a feed, AcquisitionFeed.page
        # is called with the right arguments.
        class MockAcquisitionFeed(object):
            called_with = None
            @classmethod
            def page(cls, **kwargs):
                cls.called_with = kwargs
                return "here's your feed"

        # Test our ability to generate a single feed.
        script = CacheFacetListsPerLane(self._db, testing=True, cmd_args=[])
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()

        with script.app.test_request_context("/"):
            lane = self._lane()
            result = script.do_generate(
                lane, facets, pagination, feed_class=MockAcquisitionFeed
            )
            assert "here's your feed" == result

            args = MockAcquisitionFeed.called_with
            assert self._db == args['_db']
            assert lane == args['worklist']
            assert lane.display_name == args['title']
            assert 0 == args['max_age']

            # The Pagination object was passed into
            # MockAcquisitionFeed.page, and it was also used to make the
            # feed URL (see below).
            assert pagination == args['pagination']

            # The Facets object was passed into
            # MockAcquisitionFeed.page, and it was also used to make
            # the feed URL and to create the feed annotator.
            assert facets == args['facets']
            annotator = args['annotator']
            assert facets == annotator.facets
            assert (
                args['url'] ==
                annotator.feed_url(lane, facets=facets, pagination=pagination))

            # Try again without mocking AcquisitionFeed, to verify that
            # we get a Flask Response containing an OPDS feed.
            response = script.do_generate(lane, facets, pagination)
            assert isinstance(response, OPDSFeedResponse)
            assert AcquisitionFeed.ACQUISITION_FEED_TYPE == response.content_type
            assert response.get_data(as_text=True).startswith('<feed')


class TestCacheOPDSGroupFeedPerLane(TestLaneScript):

    def test_should_process_lane(self):
        parent = self._lane()
        child = self._lane(parent=parent)
        grandchild = self._lane(parent=child)

        # Only WorkLists which have children are processed.
        script = CacheOPDSGroupFeedPerLane(
            self._db, manager=object(), cmd_args=[]
        )
        script.max_depth = 10
        assert True == script.should_process_lane(parent)
        assert True == script.should_process_lane(child)
        assert False == script.should_process_lane(grandchild)

        # If a WorkList is deeper in the hierarchy than max_depth,
        # it's not processed, even if it has children.
        script.max_depth = 0
        assert True == script.should_process_lane(parent)
        assert False == script.should_process_lane(child)

    def test_do_generate(self):
        # When it's time to generate a feed, AcquisitionFeed.groups
        # is called with the right arguments.

        class MockAcquisitionFeed(object):
            called_with = None
            @classmethod
            def groups(cls, **kwargs):
                cls.called_with = kwargs
                return "here's your feed"

        # Test our ability to generate a single feed.
        script = CacheOPDSGroupFeedPerLane(self._db, testing=True, cmd_args=[])
        facets = FeaturedFacets(0.1, entrypoint=AudiobooksEntryPoint)
        pagination = None

        with script.app.test_request_context("/"):
            lane = self._lane()
            result = script.do_generate(
                lane, facets, pagination, feed_class=MockAcquisitionFeed
            )
            assert "here's your feed" == result

            args = MockAcquisitionFeed.called_with
            assert self._db == args['_db']
            assert lane == args['worklist']
            assert lane.display_name == args['title']
            assert 0 == args['max_age']
            assert pagination == None

            # The Facets object was passed into
            # MockAcquisitionFeed.page, and it was also used to make
            # the feed URL and to create the feed annotator.
            assert facets == args['facets']
            annotator = args['annotator']
            assert facets == annotator.facets
            assert args['url'] == annotator.groups_url(lane, facets)

            # Try again without mocking AcquisitionFeed to verify that
            # we get a Flask response.
            response = script.do_generate(lane, facets, pagination)
            assert AcquisitionFeed.ACQUISITION_FEED_TYPE == response.content_type
            assert response.get_data(as_text=True).startswith('<feed')

    def test_facets(self):
        # Normally we yield one FeaturedFacets object for each of the
        # library's enabled entry points.
        library = self._default_library
        script = CacheOPDSGroupFeedPerLane(
            self._db, manager=object(), cmd_args=[]
        )
        setting = library.setting(EntryPoint.ENABLED_SETTING)
        setting.value = json.dumps(
            [AudiobooksEntryPoint.INTERNAL_NAME,
             EbooksEntryPoint.INTERNAL_NAME]
        )

        lane = self._lane()
        audio_facets, ebook_facets = script.facets(lane)
        assert AudiobooksEntryPoint == audio_facets.entrypoint
        assert EbooksEntryPoint == ebook_facets.entrypoint

        # The first entry point in the library's list of enabled entry
        # points is treated as the default.
        assert True == audio_facets.entrypoint_is_default
        assert audio_facets.entrypoint == list(library.entrypoints)[0]
        assert False == ebook_facets.entrypoint_is_default

        for facets in (audio_facets, ebook_facets):
            # The FeaturedFacets objects knows to feature works at the
            # library's minimum quality level.
            assert (library.minimum_featured_quality ==
                facets.minimum_featured_quality)

        # The first entry point is treated as the default only for WorkLists
        # that have no parent. When the WorkList has a parent, the selected
        # entry point is treated as an explicit choice  -- navigating downward
        # in the lane hierarchy ratifies the default value.
        sublane = self._lane(parent=lane)
        f1, f2 = script.facets(sublane)
        for f in f1, f2:
            assert False == f.entrypoint_is_default

        # Make it look like the lane uses custom lists.
        lane.list_datasource = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # If the library has no enabled entry points, we yield one
        # FeaturedFacets object with no particular entry point.
        setting.value = json.dumps([])
        no_entry_point, = script.facets(lane)
        assert None == no_entry_point.entrypoint

    def test_do_run(self):

        work = self._work(fiction=True, with_license_pool=True,
                          genre="Science Fiction")
        work.quality = 1
        lane = self._lane(display_name="Fantastic Fiction", fiction=True)
        sublane = self._lane(
            parent=lane, display_name="Science Fiction", fiction=True,
            genres=["Science Fiction"]
        )
        search_engine = MockExternalSearchIndex()
        search_engine.bulk_update([work])
        with mock_search_index(search_engine):
            script = CacheOPDSGroupFeedPerLane(self._db, cmd_args=[])
            script.do_run(cmd_args=[])

        # The Lane object was disconnected from its database session
        # when the app server was initialized. Reconnect it.
        lane = self._db.merge(lane)
        [feed] = lane.cachedfeeds

        assert "Fantastic Fiction" in feed.content
        assert "Science Fiction" in feed.content
        assert work.title in feed.content

class TestCacheMARCFiles(TestLaneScript):

    def test_should_process_library(self):
        script = CacheMARCFiles(self._db, cmd_args=[])
        assert False == script.should_process_library(self._default_library)
        integration = self._external_integration(
            ExternalIntegration.MARC_EXPORT, ExternalIntegration.CATALOG_GOAL,
            libraries=[self._default_library])
        assert True == script.should_process_library(self._default_library)

    def test_should_process_lane(self):
        parent = self._lane()
        parent.size = 100
        child = self._lane(parent=parent)
        child.size = 10
        grandchild = self._lane(parent=child)
        grandchild.size = 1
        wl = WorkList()
        empty = self._lane(fiction=False)
        empty.size = 0

        script = CacheMARCFiles(self._db, cmd_args=[])
        script.max_depth = 1
        assert True == script.should_process_lane(parent)
        assert True == script.should_process_lane(child)
        assert False == script.should_process_lane(grandchild)
        assert True == script.should_process_lane(wl)
        assert False == script.should_process_lane(empty)

        script.max_depth = 0
        assert True == script.should_process_lane(parent)
        assert False == script.should_process_lane(child)
        assert False == script.should_process_lane(grandchild)
        assert True == script.should_process_lane(wl)
        assert False == script.should_process_lane(empty)

    def test_process_lane(self):
        lane = self._lane(genres=["Science Fiction"])
        integration = self._external_integration(
            ExternalIntegration.MARC_EXPORT, ExternalIntegration.CATALOG_GOAL)

        class MockMARCExporter(MARCExporter):
            called_with = []

            def records(self, lane, annotator, mirror_integration, start_time=None):
                self.called_with += [(lane, annotator, mirror_integration, start_time)]

        exporter = MockMARCExporter(None, None, integration)

        # This just needs to be an ExternalIntegration, but a storage integration
        # makes the most sense in this context.
        the_linked_integration, ignore = create(
            self._db, ExternalIntegration,
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
        )

        integration_link = self._external_integration_link(
            integration=integration,
            other_integration=the_linked_integration,
            purpose=ExternalIntegrationLink.MARC
        )

        script = CacheMARCFiles(self._db, cmd_args=[])
        script.process_lane(lane, exporter)

        # If the script has never been run before, it runs the exporter once
        # to create a file with all records.
        assert 1 == len(exporter.called_with)

        assert lane == exporter.called_with[0][0]
        assert isinstance(exporter.called_with[0][1], MARCLibraryAnnotator)
        assert the_linked_integration == exporter.called_with[0][2]
        assert None == exporter.called_with[0][3]

        # If we have a cached file already, and it's old enough, the script will
        # run the exporter twice, first to update that file and second to create
        # a file with changes since that first file was originally created.
        exporter.called_with = []
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(days=7)
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.UPDATE_FREQUENCY, self._default_library,
            integration).value = 3
        representation, ignore = self._representation()
        cached, ignore = create(
            self._db, CachedMARCFile, library=self._default_library,
            lane=lane, representation=representation, end_time=last_week)

        script.process_lane(lane, exporter)

        assert 2 == len(exporter.called_with)

        assert lane == exporter.called_with[0][0]
        assert isinstance(exporter.called_with[0][1], MARCLibraryAnnotator)
        assert the_linked_integration == exporter.called_with[0][2]
        assert None == exporter.called_with[0][3]

        assert lane == exporter.called_with[1][0]
        assert isinstance(exporter.called_with[1][1], MARCLibraryAnnotator)
        assert the_linked_integration == exporter.called_with[1][2]
        assert exporter.called_with[1][3] < last_week

        # If we already have a recent cached file, the script won't do anything.
        cached.end_time = yesterday
        exporter.called_with = []
        script.process_lane(lane, exporter)
        assert [] == exporter.called_with

        # But we can force it to run anyway.
        script = CacheMARCFiles(self._db, cmd_args=["--force"])
        script.process_lane(lane, exporter)

        assert 2 == len(exporter.called_with)

        assert lane == exporter.called_with[0][0]
        assert isinstance(exporter.called_with[0][1], MARCLibraryAnnotator)
        assert the_linked_integration == exporter.called_with[0][2]
        assert None == exporter.called_with[0][3]

        assert lane == exporter.called_with[1][0]
        assert isinstance(exporter.called_with[1][1], MARCLibraryAnnotator)
        assert the_linked_integration == exporter.called_with[1][2]
        assert exporter.called_with[1][3] < yesterday
        assert exporter.called_with[1][3] > last_week

        # The update frequency can also be 0, in which case it will always run.
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.UPDATE_FREQUENCY, self._default_library,
            integration).value = 0
        exporter.called_with = []
        script = CacheMARCFiles(self._db, cmd_args=[])
        script.process_lane(lane, exporter)

        assert 2 == len(exporter.called_with)

        assert lane == exporter.called_with[0][0]
        assert isinstance(exporter.called_with[0][1], MARCLibraryAnnotator)
        assert the_linked_integration == exporter.called_with[0][2]
        assert None == exporter.called_with[0][3]

        assert lane == exporter.called_with[1][0]
        assert isinstance(exporter.called_with[1][1], MARCLibraryAnnotator)
        assert the_linked_integration == exporter.called_with[1][2]
        assert exporter.called_with[1][3] < yesterday
        assert exporter.called_with[1][3] > last_week



class TestInstanceInitializationScript(DatabaseTest):

    def test_run(self):

        # If the database has been initialized -- which happened
        # during the test suite setup -- run() will bail out and never
        # call do_run().
        class Mock(InstanceInitializationScript):
            def do_run(self):
                raise Exception("I'll never be called.")
        Mock().run()

        # If the database has not been initialized, run() will detect
        # this and call do_run().

        # Simulate an uninitialized database by changing the test SQL
        # to refer to a nonexistent table. Since this 'known' table
        # doesn't exist, we must not have initialized the site,
        # and do_run() will be called.
        class Mock(InstanceInitializationScript):
            TEST_SQL = "select * from nosuchtable"
            def do_run(self, *args, **kwargs):
                self.was_run = True

        script = Mock()
        script.run()
        assert True == script.was_run


    def test_do_run(self):
        # Normally, do_run is only called by run() if the database has
        # not yet meen initialized. But we can test it by calling it
        # directly.
        timestamp = get_one(
            self._db, Timestamp, service="Database Migration",
            service_type=Timestamp.SCRIPT_TYPE
        )
        assert None == timestamp

        # Remove all secret keys, should they exist, before running the
        # script.
        secret_keys = self._db.query(ConfigurationSetting).filter(
            ConfigurationSetting.key==Configuration.SECRET_KEY)
        [self._db.delete(secret_key) for secret_key in secret_keys]

        script = InstanceInitializationScript(_db=self._db)
        script.do_run(ignore_search=True)

        # It initializes the database.
        timestamp = get_one(
            self._db, Timestamp, service="Database Migration",
            service_type=Timestamp.SCRIPT_TYPE
        )
        assert timestamp

        # It creates a secret key.
        assert 1 == secret_keys.count()
        assert (
            secret_keys.one().value ==
            ConfigurationSetting.sitewide_secret(self._db, Configuration.SECRET_KEY))


class TestLanguageListScript(DatabaseTest):

    def test_languages(self):
        """Test the method that gives this script the bulk of its output."""
        english = self._work(language='eng', with_open_access_download=True)
        tagalog = self._work(language="tgl", with_license_pool=True)
        [pool] = tagalog.license_pools
        self._add_generic_delivery_mechanism(pool)
        script = LanguageListScript(self._db)
        output = list(script.languages(self._default_library))

        # English is ignored because all its works are open-access.
        # Tagalog shows up with the correct estimate.
        assert ["tgl 1 (Tagalog)"] == output


class TestShortClientTokenLibraryConfigurationScript(DatabaseTest):

    def setup_method(self):
        super(TestShortClientTokenLibraryConfigurationScript, self).setup_method()
        self._default_library.setting(
            Configuration.WEBSITE_URL
        ).value = "http://foo/"
        self.script = ShortClientTokenLibraryConfigurationScript(self._db)

    def test_identify_library_by_url(self):
        with pytest.raises(Exception) as excinfo:
            self.script.set_secret(self._db, "http://bar/", "vendorid", "libraryname", "secret", None)
        assert "Could not locate library with URL http://bar/. Available URLs: http://foo/" in str(excinfo.value)

    def test_set_secret(self):
        assert [] == self._default_library.integrations

        output = StringIO()
        self.script.set_secret(
            self._db, "http://foo/", "vendorid", "libraryname", "secret",
            output
        )
        assert (
            'Current Short Client Token configuration for http://foo/:\n Vendor ID: vendorid\n Library name: libraryname\n Shared secret: secret\n' ==
            output.getvalue())
        [integration] = self._default_library.integrations
        assert (
            [('password', 'secret'), ('username', 'libraryname'),
             ('vendor_id', 'vendorid')] ==
            sorted((x.key, x.value) for x in integration.settings))

        # We can modify an existing configuration.
        output = StringIO()
        self.script.set_secret(
            self._db, "http://foo/", "newid", "newname", "newsecret",
            output
        )
        expect = 'Current Short Client Token configuration for http://foo/:\n Vendor ID: newid\n Library name: newname\n Shared secret: newsecret\n'
        assert expect == output.getvalue()
        expect_settings = [
            ('password', 'newsecret'), ('username', 'newname'),
             ('vendor_id', 'newid')
        ]
        assert (expect_settings ==
            sorted((x.key, x.value) for x in integration.settings))

        # We can also just check on the existing configuration without
        # changing anything.
        output = StringIO()
        self.script.set_secret(
            self._db, "http://foo/", None, None, None, output
        )
        assert expect == output.getvalue()
        assert (expect_settings ==
            sorted((x.key, x.value) for x in integration.settings))


class MockDirectoryImportScript(DirectoryImportScript):
    """Mock a filesystem to make it easier to test DirectoryInputScript."""

    def __init__(self, _db, mock_filesystem={}):
        super(MockDirectoryImportScript, self).__init__(_db)
        self.mock_filesystem = mock_filesystem
        self._locate_file_args = None

    def _locate_file(self, identifier, directory, extensions, file_type):
        self._locate_file_args = (identifier, directory, extensions, file_type)
        return self.mock_filesystem.get(
            directory, (None, None, None)
        )


class TestDirectoryImportScript(DatabaseTest):

    def test_do_run(self):
        # Calling do_run with command-line arguments parses the
        # arguments and calls run_with_arguments.

        class Mock(DirectoryImportScript):
            def run_with_arguments(self, *args, **kwargs):
                self.ran_with = kwargs

        script = Mock(self._db)
        script.do_run(
            cmd_args=[
                "--collection-name=coll1",
                "--data-source-name=ds1",
                "--metadata-file=metadata",
                "--metadata-format=marc",
                "--cover-directory=covers",
                "--ebook-directory=ebooks",
                "--rights-uri=rights",
                "--dry-run",
                "--default-medium-type={0}".format(EditionConstants.AUDIO_MEDIUM)
            ]
        )
        assert (
            {
                'collection_name': 'coll1',
                'collection_type': CollectionType.OPEN_ACCESS,
                'data_source_name': 'ds1',
                'metadata_file': 'metadata',
                'metadata_format': 'marc',
                'cover_directory': 'covers',
                'ebook_directory': 'ebooks',
                'rights_uri': 'rights',
                'dry_run': True,
                'default_medium_type': EditionConstants.AUDIO_MEDIUM
            } ==
            script.ran_with)

    def test_run_with_arguments(self):

        metadata1 = object()
        metadata2 = object()
        collection = self._default_collection
        mirrors = object()
        work = object()
        licensepool = LicensePool()

        class Mock(DirectoryImportScript):
            """Mock the methods called by run_with_arguments."""
            def __init__(self, _db):
                super(DirectoryImportScript, self).__init__(_db)
                self.load_collection_calls = []
                self.load_metadata_calls = []
                self.work_from_metadata_calls = []

            def load_collection(self, *args):
                self.load_collection_calls.append(args)
                return collection, mirrors

            def load_metadata(self, *args, **kwargs):
                self.load_metadata_calls.append(args)
                return [metadata1, metadata2]

            def work_from_metadata(self, *args):
                self.work_from_metadata_calls.append(args)
                return work, licensepool

        # First, try a dry run.

        # Make a change to a model object so we can track when the
        # session is committed.
        self._default_collection.name = 'changed'

        script = Mock(self._db)
        basic_args = [
            "collection name",
            CollectionType.OPEN_ACCESS,
            "data source name",
            "metadata file",
            "marc",
            "cover directory",
            "ebook directory",
            "rights URI"
        ]
        script.run_with_arguments(*(basic_args + [True] + [EditionConstants.BOOK_MEDIUM]))

        # load_collection was called with the collection and data source names.
        assert (
            [('collection name', CollectionType.OPEN_ACCESS, 'data source name')] ==
            script.load_collection_calls)

        # load_metadata was called with the metadata file and data source name.
        assert [('metadata file', 'marc', 'data source name', EditionConstants.BOOK_MEDIUM)] == script.load_metadata_calls

        # work_from_metadata was called twice, once on each metadata
        # object.
        [(coll1, t1, o1, policy1, c1, e1, r1),
         (coll2, t2, o2, policy2, c2, e2, r2)] = script.work_from_metadata_calls

        assert coll1 == self._default_collection
        assert coll1 == coll2

        assert o1 == metadata1
        assert o2 == metadata2

        assert c1 == 'cover directory'
        assert c1 == c2

        assert e1 == 'ebook directory'
        assert e1 == e2

        assert "rights URI" == r1
        assert r1 == r2

        # Since this is a dry run, the ReplacementPolicy has no mirror
        # set.
        for policy in (policy1, policy2):
            assert None == policy.mirrors
            assert True == policy.links
            assert True == policy.formats
            assert True == policy.contributions
            assert True == policy.rights

        # Now try it not as a dry run.
        script = Mock(self._db)
        script.run_with_arguments(*(basic_args + [False]))

        # This time, the ReplacementPolicy has a mirror set
        # appropriately.
        [(coll1, t1, o1, policy1, c1, e1, r1),
         (coll1, t2, o2, policy2, c2, e2, r2)] = script.work_from_metadata_calls
        for policy in policy1, policy2:
            assert mirrors == policy.mirrors

        # timestamp_collection has been set to the Collection that will be
        # used when a Timestamp is created for this script.
        assert self._default_collection == script.timestamp_collection

    def test_load_collection_setting_mirrors(self):
        # Calling load_collection does not create a new collection.
        script = DirectoryImportScript(self._db)
        collection, mirrors = script.load_collection(
            "New collection", CollectionType.OPEN_ACCESS, "data source name")
        assert None == collection
        assert None == mirrors

        existing_collection = self._collection(
            name="some collection", protocol=ExternalIntegration.MANUAL
        )

        collection, mirrors = script.load_collection(
            "some collection", CollectionType.OPEN_ACCESS, "data source name")

        # No covers or books mirrors were created beforehand for this collection
        # so nothing is returned.
        assert None == collection
        assert None == mirrors

        # Both mirrors need to set up or else nothing is returned.
        storage1 = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            username="name", password="password"
        )
        external_integration_link = self._external_integration_link(
            integration=existing_collection.external_integration,
            other_integration=storage1,
            purpose=ExternalIntegrationLink.COVERS
        )

        collection, mirrors = script.load_collection(
            "some collection", CollectionType.OPEN_ACCESS, "data source name")
        assert None == collection
        assert None == mirrors

        # Create another storage and assign it for the books mirror
        storage2 = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            username="name", password="password"
        )
        external_integration_link = self._external_integration_link(
            integration=existing_collection.external_integration,
            other_integration=storage2,
            purpose=ExternalIntegrationLink.OPEN_ACCESS_BOOKS
        )

        collection, mirrors = script.load_collection(
            "some collection", CollectionType.OPEN_ACCESS, "data source name")
        assert collection == existing_collection
        assert isinstance(mirrors[ExternalIntegrationLink.COVERS], MirrorUploader)
        assert isinstance(mirrors[ExternalIntegrationLink.OPEN_ACCESS_BOOKS], MirrorUploader)

    def test_work_from_metadata(self):
        # Validate the ability to create a new Work from appropriate metadata.

        class Mock(MockDirectoryImportScript):
            """In this test we need to verify that annotate_metadata
            was called but did nothing.
            """
            def annotate_metadata(self, collection_type, metadata, *args, **kwargs):
                metadata.annotated = True
                return super(Mock, self).annotate_metadata(
                    collection_type, metadata, *args, **kwargs
                )

        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1003")
        identifier_obj, ignore = identifier.load(self._db)
        metadata = Metadata(
            DataSource.GUTENBERG,
            primary_identifier=identifier,
            title="A book"
        )
        metadata.annotated = False
        datasource = DataSource.lookup(self._db, DataSource.GUTENBERG)
        policy = ReplacementPolicy.from_license_source(self._db)
        mirrors = dict(books_mirror=MockS3Uploader(),covers_mirror=MockS3Uploader())
        mirror_type_books = ExternalIntegrationLink.OPEN_ACCESS_BOOKS
        mirror_type_covers = ExternalIntegrationLink.COVERS
        policy.mirrors = mirrors

        # Here, work_from_metadata calls annotate_metadata, but does
        # not actually import anything because there are no files 'on
        # disk' and thus no way to actually get the book.
        collection = self._default_collection
        collection_type = CollectionType.OPEN_ACCESS
        shared_args = (collection_type, metadata, policy,
                       "cover directory", "ebook directory", RightsStatus.CC0)
        # args = (collection, *shared_args)
        script = Mock(self._db)
        assert None == script.work_from_metadata(collection, *shared_args)
        assert True == metadata.annotated

        # Now let's try it with some files 'on disk'.
        with open(self.sample_cover_path('test-book-cover.png'), "rb") as fh:
            image = fh.read()
        mock_filesystem = {
            'cover directory' : (
                'cover.jpg', Representation.JPEG_MEDIA_TYPE, image
            ),
            'ebook directory' : (
                'book.epub', Representation.EPUB_MEDIA_TYPE, "I'm an EPUB."
            )
        }
        script = MockDirectoryImportScript(
            self._db, mock_filesystem=mock_filesystem
        )
        work, licensepool_for_work = script.work_from_metadata(collection, *shared_args)

        # Get the edition that was created for this book. It should have
        # already been created by `script.work_from_metadata`.
        edition, is_new_edition = metadata.edition(self._db)
        assert False == is_new_edition

        # We have created a book. It has a cover image, which has a
        # thumbnail.
        assert "A book" == work.title
        assert (
            work.cover_full_url ==
            'https://test-cover-bucket.s3.amazonaws.com/Gutenberg/Gutenberg%20ID/1003/1003.jpg')
        assert (
            work.cover_thumbnail_url ==
            'https://test-cover-bucket.s3.amazonaws.com/scaled/300/Gutenberg/Gutenberg%20ID/1003/1003.png')
        assert 1 == len(work.license_pools)
        assert 1 == len(edition.license_pools)
        assert 1 == len([lp for lp in edition.license_pools if lp.collection == collection])
        [pool] = work.license_pools
        assert licensepool_for_work == pool
        assert (
            pool.open_access_download_url ==
            'https://test-content-bucket.s3.amazonaws.com/Gutenberg/Gutenberg%20ID/1003/A%20book.epub')
        assert (RightsStatus.CC0 ==
            pool.delivery_mechanisms[0].rights_status.uri)

        # The two mock S3Uploaders have records of 'uploading' all these files
        # to S3. The "books" mirror has the epubs and the "covers" mirror
        # contains all the images.
        [epub] = mirrors[mirror_type_books].uploaded
        [full, thumbnail] = mirrors[mirror_type_covers].uploaded
        assert epub.url == pool.open_access_download_url
        assert full.url == work.cover_full_url
        assert thumbnail.url == work.cover_thumbnail_url

        # The EPUB Representation was cleared out after the upload, to
        # save database space.
        assert b"I'm an EPUB." == mirrors[mirror_type_books].content[0]
        assert None == epub.content

        # Now attempt to get a work for a different collection, but with
        # the same metadata.
        # Even though there will be two license pools associated with the
        # work's presentation edition, the call should be successful.
        collection2 = self._collection('second collection')
        work2, licensepool_for_work2 = script.work_from_metadata(collection2, *shared_args)

        # The presentation edition should be the same for both works.
        edition2 = work2.presentation_edition
        assert edition == edition2

        # The licensepool from which the work is calculated should be
        # associated with collection2.
        assert licensepool_for_work2.collection == collection2

        # The work and its presentation edition should both have two licensepools,
        # one for each collection.
        assert 2 == len(work2.license_pools)
        assert 2 == len(edition2.license_pools)
        assert 1 == len([lp for lp in edition2.license_pools if lp.collection == collection2])

    def test_annotate_metadata(self):
        """Verify that annotate_metadata calls load_circulation_data
        and load_cover_link appropriately.
        """

        # First, test an unsuccessful annotation.
        class MockNoCirculationData(DirectoryImportScript):
            """Do nothing when load_circulation_data is called. Explode if
            load_cover_link is called.
            """
            def load_circulation_data(self, *args):
                self.load_circulation_data_args = args
                return None

            def load_cover_link(self, *args):
                raise Exception("Explode!")

        collection_type = CollectionType.OPEN_ACCESS
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "11111")
        identifier_obj, ignore = identifier.load(self._db)
        metadata = Metadata(
            title=self._str,
            data_source=gutenberg,
            primary_identifier=identifier
        )
        mirrors = object()
        policy = ReplacementPolicy(mirrors=mirrors)
        cover_directory = object()
        ebook_directory = object()
        rights_uri = object()

        script = MockNoCirculationData(self._db)
        args = (
            collection_type,
            metadata,
            policy,
            cover_directory,
            ebook_directory,
            rights_uri
        )
        script.annotate_metadata(*args)

        # load_circulation_data was called.
        assert (
            (
                collection_type,
                identifier_obj,
                gutenberg,
                ebook_directory,
                mirrors,
                metadata.title,
                rights_uri
            ) ==
            script.load_circulation_data_args)

        # But because load_circulation_data returned None,
        # metadata.circulation_data was not modified and
        # load_cover_link was not called (which would have raised an
        # exception).
        assert None == metadata.circulation

        # Test a successful annotation with no cover image.
        class MockNoCoverLink(DirectoryImportScript):
            """Return an object when load_circulation_data is called.
            Do nothing when load_cover_link is called.
            """
            def load_circulation_data(self, *args):
                return "Some circulation data"

            def load_cover_link(self, *args):
                self.load_cover_link_args = args
                return None

        script = MockNoCoverLink(self._db)
        script.annotate_metadata(*args)

        # The Metadata object was annotated with the return value of
        # load_circulation_data.
        assert "Some circulation data" == metadata.circulation

        # load_cover_link was called.
        assert (
            (identifier_obj, gutenberg, cover_directory, mirrors) ==
            script.load_cover_link_args)

        # But since it provided no cover link, metadata.links was empty.
        assert [] == metadata.links

        # Finally, test a completely successful annotation.
        class MockWithCoverLink(DirectoryImportScript):
            """Mock success for both load_circulation_data
            and load_cover_link.
            """
            def load_circulation_data(self, *args):
                return "Some circulation data"

            def load_cover_link(self, *args):
                return "A cover link"

        metadata.circulation = None
        script = MockWithCoverLink(self._db)
        script.annotate_metadata(*args)

        assert "Some circulation data" == metadata.circulation
        assert ['A cover link'] == metadata.links

    def test_load_circulation_data(self):
        # Create a directory import script with an empty mock filesystem.
        script = MockDirectoryImportScript(self._db, {})

        identifier = self._identifier(Identifier.GUTENBERG_ID, "2345")
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        mirrors = dict(books_mirror=MockS3Uploader(),covers_mirror=None)
        args = (
            CollectionType.OPEN_ACCESS,
            identifier,
            gutenberg,
            "ebooks",
            mirrors,
            "Name of book",
            "rights URI"
        )

        # There is nothing on the mock filesystem, so in this case
        # load_circulation_data returns None.
        assert None == script.load_circulation_data(*args)

        # But we tried.
        assert (
            ('2345', 'ebooks', Representation.COMMON_EBOOK_EXTENSIONS,
             'ebook file') ==
            script._locate_file_args)

        # Try another script that has a populated mock filesystem.
        mock_filesystem = {
            'ebooks' : (
                'book.epub', Representation.EPUB_MEDIA_TYPE, "I'm an EPUB."
            )
        }
        script = MockDirectoryImportScript(self._db, mock_filesystem)

        # Now _locate_file finds something on the mock filesystem, and
        # load_circulation_data loads it into a fully populated
        # CirculationData object.
        circulation = script.load_circulation_data(*args)
        assert identifier == circulation.primary_identifier(self._db)
        assert gutenberg == circulation.data_source(self._db)
        assert "rights URI" == circulation.default_rights_uri

        # The CirculationData has an open-access link associated with it.
        [link] = circulation.links
        assert Hyperlink.OPEN_ACCESS_DOWNLOAD == link.rel
        assert (
            link.href ==
            'https://test-content-bucket.s3.amazonaws.com/Gutenberg/Gutenberg%20ID/2345/Name%20of%20book.epub')
        assert Representation.EPUB_MEDIA_TYPE == link.media_type
        assert "I'm an EPUB." == link.content

        # This open-access link will be made available through a
        # delivery mechanism described by this FormatData.
        [format] = circulation.formats
        assert link == format.link
        assert link.media_type == format.content_type
        assert DeliveryMechanism.NO_DRM == format.drm_scheme

    def test_load_cover_link(self):
        # Create a directory import script with an empty mock filesystem.
        script = MockDirectoryImportScript(self._db, {})

        identifier = self._identifier(Identifier.GUTENBERG_ID, "2345")
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        mirrors = dict(covers_mirror=MockS3Uploader(),books_mirror=None)
        args = (identifier, gutenberg, "covers", mirrors)

        # There is nothing on the mock filesystem, so in this case
        # load_cover_link returns None.
        assert None == script.load_cover_link(*args)

        # But we tried.
        assert (
            ('2345', 'covers', Representation.COMMON_IMAGE_EXTENSIONS,
             'cover image') ==
            script._locate_file_args)

        # Try another script that has a populated mock filesystem.
        mock_filesystem = {
            'covers' : (
                'acover.jpeg', Representation.JPEG_MEDIA_TYPE, "I'm an image."
            )
        }
        script = MockDirectoryImportScript(self._db, mock_filesystem)
        link = script.load_cover_link(*args)
        assert Hyperlink.IMAGE == link.rel
        assert (
            link.href ==
            'https://test-cover-bucket.s3.amazonaws.com/Gutenberg/Gutenberg%20ID/2345/2345.jpg')
        assert Representation.JPEG_MEDIA_TYPE == link.media_type
        assert "I'm an image." == link.content

    def test_locate_file(self):
        """Test the ability of DirectoryImportScript._locate_file
        to find files on a mock filesystem.
        """
        # Create a mock filesystem with a single file.
        mock_filesystem = {
            "directory/thefile.JPEG" : "The contents"
        }
        def mock_exists(path):
            return path in mock_filesystem

        @contextlib.contextmanager
        def mock_open(path):
            yield StringIO(mock_filesystem[path])
        mock_filesystem_operations = mock_exists, mock_open

        def assert_not_found(base_filename, directory, extensions):
            """Verify that the given set of arguments to
            _locate_file() does not find anything.
            """
            result = DirectoryImportScript._locate_file(
                base_filename, directory, extensions, file_type="some file",
                mock_filesystem_operations=mock_filesystem_operations
            )
            assert (None, None, None) == result

        def assert_found(base_filename, directory, extensions):
            """Verify that the given set of arguments to _locate_file()
            finds and loads the single file on the mock filesystem..
            """
            result = DirectoryImportScript._locate_file(
                base_filename, directory, extensions, file_type="some file",
                mock_filesystem_operations=mock_filesystem_operations
            )
            assert (
                ("thefile.JPEG", Representation.JPEG_MEDIA_TYPE,
                 "The contents") ==
                result)

        # As long as the file and directory match we have some flexibility
        # regarding the extensions we look for.
        assert_found('thefile', 'directory', ['.jpeg'])
        assert_found('thefile', 'directory', ['.JPEG'])
        assert_found('thefile', 'directory', ['jpeg'])
        assert_found('thefile', 'directory', ['JPEG'])
        assert_found('thefile', 'directory', ['.another-extension', '.jpeg'])

        # But file, directory, and (flexible) extension must all match.
        assert_not_found('anotherfile', 'directory', ['.jpeg'])
        assert_not_found('thefile', 'another_directory', ['.jpeg'])
        assert_not_found('thefile', 'directory', ['.another-extension'])
        assert_not_found('thefile', 'directory', [])

class TestNovelistSnapshotScript(DatabaseTest):

    def mockNoveListAPI(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_do_run(self):
        """Test that NovelistSnapshotScript.do_run() calls the NoveList api.
        """

        class MockNovelistSnapshotScript(NovelistSnapshotScript):
            pass

        oldNovelistConfig = NoveListAPI.from_config
        NoveListAPI.from_config = self.mockNoveListAPI

        l1 = self._library()
        cmd_args = [l1.name]
        script = MockNovelistSnapshotScript(self._db)
        script.do_run(cmd_args=cmd_args)

        (params, args) = self.called_with

        assert params[0] == l1

        NoveListAPI.from_config = oldNovelistConfig

class TestLocalAnalyticsExportScript(DatabaseTest):

    def test_do_run(self):

        class MockLocalAnalyticsExporter(object):
            def export(self, _db, start, end):
                self.called_with = [start, end]
                return "test"

        output = StringIO()
        cmd_args = ['--start=20190820', '--end=20190827']
        exporter = MockLocalAnalyticsExporter()
        script = LocalAnalyticsExportScript()
        script.do_run(
            output=output, cmd_args=cmd_args,
            exporter=exporter)
        assert "test" == output.getvalue()
        assert ['20190820', '20190827'] == exporter.called_with
