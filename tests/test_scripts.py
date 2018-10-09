from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)

import contextlib
import datetime
import flask
import json
from StringIO import StringIO

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
    ConfigurationSetting,
    create,
    Credential,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    get_one,
    Representation,
    RightsStatus,
    Timestamp,
)

from core.opds import AcquisitionFeed

from core.s3 import MockS3Uploader

from core.mirror import MirrorUploader

from . import (
    DatabaseTest,
)

from scripts import (
    AdobeAccountIDResetScript,
    CacheRepresentationPerLane,
    CacheFacetListsPerLane,
    CacheOPDSGroupFeedPerLane,
    DirectoryImportScript,
    InstanceInitializationScript,
    LanguageListScript,
    NovelistSnapshotScript,
)

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

        eq_(3, len(patron.credentials))

        # Run the patron through the script.
        script = AdobeAccountIDResetScript(self._db)

        # A dry run does nothing.
        script.delete = False
        script.process_patron(patron)
        self._db.commit()
        eq_(3, len(patron.credentials))

        # Now try it for real.
        script.delete = True
        script.process_patron(patron)
        self._db.commit()

        # The two Adobe-related credentials are gone. The other one remains.
        [credential] = patron.credentials
        eq_("Some other type", credential.type)


class TestLaneScript(DatabaseTest):

    def setup(self):
        super(TestLaneScript, self).setup()
        base_url_setting = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY)
        base_url_setting.value = u'http://test-circulation-manager/'
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
        eq_(['fre', 'eng'], script.languages)

        english_lane = self._lane(languages=['eng'])
        eq_(True, script.should_process_lane(english_lane))

        no_english_lane = self._lane(languages=['spa','fre'])
        eq_(True, script.should_process_lane(no_english_lane))

        no_english_or_french_lane = self._lane(languages=['spa'])
        eq_(False, script.should_process_lane(no_english_or_french_lane))

        # Test that should_process_lane respects maximum depth
        # restrictions.
        script = CacheRepresentationPerLane(
            self._db, ["--max-depth=0", "--min-depth=0"],
            manager=object()
        )
        eq_(0, script.max_depth)

        child = self._lane(display_name="sublane")
        parent = self._lane(display_name="parent")
        parent.sublanes=[child]
        eq_(True, script.should_process_lane(parent))
        eq_(False, script.should_process_lane(child))

        script = CacheRepresentationPerLane(
            self._db, ["--min-depth=1"], testing=True
        )
        eq_(1, script.min_depth)
        eq_(False, script.should_process_lane(parent))
        eq_(True, script.should_process_lane(child))

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
                self.generated.append(value)
                return value

            def facets(self, lane):
                yield facets1
                yield facets2

            def pagination(self, lane):
                yield page1
                yield page2

        lane = self._lane()
        script = Mock(self._db, manager=object(), cmd_args=[])
        generated = script.process_lane(lane)
        eq_(generated, script.generated)

        c1, c2, c3, c4 = script.generated
        eq_((lane, facets1, page1), c1)
        eq_((lane, facets1, page2), c2)
        eq_((lane, facets2, page1), c3)
        eq_((lane, facets2, page2), c4)

    def test_default_facets(self):
        # By default, do_generate will only be called once, with facets=None.
        script = CacheRepresentationPerLane(
            self._db, manager=object(), cmd_args=[]
        )
        eq_([None], list(script.facets(object())))

    def test_default_pagination(self):
        # By default, do_generate will only be called once, with pagination=None.
        script = CacheRepresentationPerLane(
            self._db, manager=object(), cmd_args=[]
        )
        eq_([None], list(script.pagination(object())))


class TestCacheFacetListsPerLane(TestLaneScript):

    def test_arguments(self):
        # Verify that command-line arguments become attributes of
        # the CacheFacetListsPerLane object.
        script = CacheFacetListsPerLane(
            self._db, ["--order=title", "--order=added"],
            manager=object()
        )
        eq_(['title', 'added'], script.orders)
        script = CacheFacetListsPerLane(
            self._db, ["--availability=all", "--availability=always"],
            manager=object()
        )
        eq_(['all', 'always'], script.availabilities)

        script = CacheFacetListsPerLane(
            self._db, ["--collection=main", "--collection=full"],
            manager=object()
        )
        eq_(['main', 'full'], script.collections)

        script = CacheFacetListsPerLane(
            self._db, ["--entrypoint=Audio", "--entrypoint=Book"],
            manager=object()
        )
        eq_(['Audio', 'Book'], script.entrypoints)

        script = CacheFacetListsPerLane(
            self._db, ['--pages=1'], manager=object()
        )
        eq_(1, script.pages)

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
        eq_(Facets.ORDER_TITLE, f1.order)
        eq_(Facets.ORDER_AUTHOR, f2.order)

        # All other fields are tied to the only acceptable values
        # given in the script attributes. The first (and only)
        # enabled entry point is treated as the default.
        for f in f1, f2:
            eq_(AudiobooksEntryPoint, f.entrypoint)
            eq_(True, f.entrypoint_is_default)
            eq_(Facets.AVAILABLE_NOW, f.availability)
            eq_(Facets.COLLECTION_FULL, f.collection)

    def test_pagination(self):
        script = CacheFacetListsPerLane(self._db, manager=object(), cmd_args=[])
        script.pages = 3
        lane = self._lane()
        p1, p2, p3 = script.pagination(lane)
        pagination = Pagination.default()
        eq_(pagination.query_string, p1.query_string)
        eq_(pagination.next_page.query_string, p2.query_string)
        eq_(pagination.next_page.next_page.query_string, p3.query_string)

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
            eq_("here's your feed", result)

            args = MockAcquisitionFeed.called_with
            eq_(self._db, args['_db'])
            eq_(lane, args['lane'])
            eq_(lane.display_name, args['title'])
            eq_(True, args['force_refresh'])

            # The Pagination object was passed into
            # MockAcquisitionFeed.page, and it was also used to make the
            # feed URL (see below).
            eq_(pagination, args['pagination'])

            # The Facets object was passed into
            # MockAcquisitionFeed.page, and it was also used to make
            # the feed URL and to create the feed annotator.
            eq_(facets, args['facets'])
            annotator = args['annotator']
            eq_(facets, annotator.facets)
            eq_(
                args['url'],
                annotator.feed_url(lane, facets=facets, pagination=pagination)
            )

            # Try again without mocking AcquisitionFeed to verify that
            # we get something that looks like an OPDS feed.
            result = script.do_generate(lane, facets, pagination)
            assert result.startswith('<feed')


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
        eq_(True, script.should_process_lane(parent))
        eq_(True, script.should_process_lane(child))
        eq_(False, script.should_process_lane(grandchild))

        # If a WorkList is deeper in the hierarchy than max_depth,
        # it's not processed, even if it has children.
        script.max_depth = 0
        eq_(True, script.should_process_lane(parent))
        eq_(False, script.should_process_lane(child))

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
            eq_("here's your feed", result)

            args = MockAcquisitionFeed.called_with
            eq_(self._db, args['_db'])
            eq_(lane, args['lane'])
            eq_(lane.display_name, args['title'])
            eq_(True, args['force_refresh'])
            eq_(pagination, None)

            # The Facets object was passed into
            # MockAcquisitionFeed.page, and it was also used to make
            # the feed URL and to create the feed annotator.
            eq_(facets, args['facets'])
            annotator = args['annotator']
            eq_(facets, annotator.facets)
            eq_(args['url'], annotator.groups_url(lane, facets))

            # Try again without mocking AcquisitionFeed to verify that
            # we get something that looks like an OPDS feed.
            result = script.do_generate(lane, facets, pagination)
            assert result.startswith('<feed')

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
        eq_(AudiobooksEntryPoint, audio_facets.entrypoint)
        eq_(EbooksEntryPoint, ebook_facets.entrypoint)

        # The first entry point in the library's list of enabled entry
        # points is treated as the default.
        eq_(True, audio_facets.entrypoint_is_default)
        eq_(audio_facets.entrypoint, list(library.entrypoints)[0])
        eq_(False, ebook_facets.entrypoint_is_default)

        for facets in (audio_facets, ebook_facets):
            # The FeaturedFacets objects knows to feature works at the
            # library's minimum quality level.
            eq_(library.minimum_featured_quality,
                facets.minimum_featured_quality)
            # The FeaturedFacets object knows that custom lists are
            # not in play.
            eq_(False, facets.uses_customlists)

        # Make it look like the lane uses custom lists.
        lane.list_datasource = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # If the library has no enabled entry points, we yield one
        # FeaturedFacets object with no particular entry point.
        setting.value = json.dumps([])
        no_entry_point, = script.facets(lane)
        eq_(None, no_entry_point.entrypoint)

        # The FeaturedFacets object knows that custom lists are in
        # play.
        eq_(True, no_entry_point.uses_customlists)

    def test_do_run(self):

        work = self._work(fiction=True, with_license_pool=True,
                          genre="Science Fiction")
        work.quality = 1
        lane = self._lane(display_name="Fantastic Fiction", fiction=True)
        sublane = self._lane(
            parent=lane, display_name="Science Fiction", fiction=True,
            genres=["Science Fiction"]
        )
        self.add_to_materialized_view([work], true_opds=True)
        script = CacheOPDSGroupFeedPerLane(self._db, cmd_args=[])
        script.do_run(cmd_args=[])

        # The Lane object was disconnected from its database session
        # when the app server was initialized. Reconnect it.
        lane = self._db.merge(lane)
        [feed] = lane.cachedfeeds

        assert "Fantastic Fiction" in feed.content
        assert "Science Fiction" in feed.content
        assert work.title in feed.content


class TestInstanceInitializationScript(DatabaseTest):

    def test_run(self):
        timestamp = get_one(self._db, Timestamp, service=u"Database Migration")
        eq_(None, timestamp)

        # Remove all secret keys, should they exist, before running the
        # script.
        secret_keys = self._db.query(ConfigurationSetting).filter(
            ConfigurationSetting.key==Configuration.SECRET_KEY)
        [self._db.delete(secret_key) for secret_key in secret_keys]

        script = InstanceInitializationScript(_db=self._db)
        script.do_run(ignore_search=True)

        # It initializes the database.
        timestamp = get_one(self._db, Timestamp, service=u"Database Migration")
        assert timestamp

        # It creates a secret key.
        eq_(1, secret_keys.count())
        eq_(
            secret_keys.one().value,
            ConfigurationSetting.sitewide_secret(self._db, Configuration.SECRET_KEY)
        )


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
        eq_(["tgl 1 (Tagalog)"], output)


class TestShortClientTokenLibraryConfigurationScript(DatabaseTest):

    def setup(self):
        super(TestShortClientTokenLibraryConfigurationScript, self).setup()
        self._default_library.setting(
            Configuration.WEBSITE_URL
        ).value = "http://foo/"
        self.script = ShortClientTokenLibraryConfigurationScript(self._db)

    def test_identify_library_by_url(self):
        assert_raises_regexp(
            Exception,
            "Could not locate library with URL http://bar/. Available URLs: http://foo/",
            self.script.set_secret,
            self._db, "http://bar/", "vendorid", "libraryname", "secret", None
        )

    def test_set_secret(self):
        eq_([], self._default_library.integrations)

        output = StringIO()
        self.script.set_secret(
            self._db, "http://foo/", "vendorid", "libraryname", "secret",
            output
        )
        eq_(
            u'Current Short Client Token configuration for http://foo/:\n Vendor ID: vendorid\n Library name: libraryname\n Shared secret: secret\n',
            output.getvalue()
        )
        [integration] = self._default_library.integrations
        eq_(
            [('password', 'secret'), ('username', 'libraryname'),
             ('vendor_id', 'vendorid')],
            sorted((x.key, x.value) for x in integration.settings)
        )

        # We can modify an existing configuration.
        output = StringIO()
        self.script.set_secret(
            self._db, "http://foo/", "newid", "newname", "newsecret",
            output
        )
        expect = u'Current Short Client Token configuration for http://foo/:\n Vendor ID: newid\n Library name: newname\n Shared secret: newsecret\n'
        eq_(expect, output.getvalue())
        expect_settings = [
            ('password', 'newsecret'), ('username', 'newname'),
             ('vendor_id', 'newid')
        ]
        eq_(expect_settings,
            sorted((x.key, x.value) for x in integration.settings)
        )

        # We can also just check on the existing configuration without
        # changing anything.
        output = StringIO()
        self.script.set_secret(
            self._db, "http://foo/", None, None, None, output
        )
        eq_(expect, output.getvalue())
        eq_(expect_settings,
            sorted((x.key, x.value) for x in integration.settings)
        )


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
        """Calling do_run with command-line arguments parses the
        arguments and calls run_with_arguments.
        """
        class Mock(DirectoryImportScript):
            def run_with_arguments(self, *args):
                self.ran_with = args

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
                "--dry-run"
            ]
        )
        eq_(('coll1', 'ds1', 'metadata', 'marc', 'covers', 'ebooks', 'rights', True),
            script.ran_with)

    def test_run_with_arguments(self):

        metadata1 = object()
        metadata2 = object()
        collection = self._default_collection
        mirror = object()

        class Mock(DirectoryImportScript):
            """Mock the methods called by run_with_arguments."""
            def __init__(self, _db):
                super(DirectoryImportScript, self).__init__(_db)
                self.load_collection_calls = []
                self.load_metadata_calls = []
                self.work_from_metadata_calls = []

            def load_collection(self, *args):
                self.load_collection_calls.append(args)
                return collection, mirror

            def load_metadata(self, *args, **kwargs):
                self.load_metadata_calls.append(args)
                return [metadata1, metadata2]

            def work_from_metadata(self, *args):
                self.work_from_metadata_calls.append(args)

        # First, try a dry run.

        # Make a change to a model object so we can track when the
        # session is committed.
        self._default_collection.name = 'changed'

        script = Mock(self._db)
        basic_args = ["collection name", "data source name", "metadata file", "marc",
                      "cover directory", "ebook directory", "rights URI"]
        script.run_with_arguments(*(basic_args + [True]))

        # load_collection was called with the collection and data source names.
        eq_([('collection name', 'data source name')],
            script.load_collection_calls)

        # load_metadata was called with the metadata file and data source name.
        eq_([('metadata file', 'marc', 'data source name')], script.load_metadata_calls)

        # work_from_metadata was called twice, once on each metadata
        # object.
        [(coll1, o1, policy1, c1, e1, r1),
         (coll2, o2, policy2, c2, e2, r2)] = script.work_from_metadata_calls

        eq_(coll1, self._default_collection)
        eq_(coll1, coll2)

        eq_(o1, metadata1)
        eq_(o2, metadata2)

        eq_(c1, 'cover directory')
        eq_(c1, c2)

        eq_(e1, 'ebook directory')
        eq_(e1, e2)

        eq_("rights URI", r1)
        eq_(r1, r2)

        # Since this is a dry run, the ReplacementPolicy has no mirror
        # set.
        for policy in (policy1, policy2):
            eq_(None, policy.mirror)
            eq_(True, policy.links)
            eq_(True, policy.formats)
            eq_(True, policy.contributions)
            eq_(True, policy.rights)

        # Now try it not as a dry run.
        script = Mock(self._db)
        script.run_with_arguments(*(basic_args + [False]))

        # This time, the ReplacementPolicy has a mirror set
        # appropriately.
        [(coll1, o1, policy1, c1, e1, r1),
         (coll1, o2, policy2, c2, e2, r2)] = script.work_from_metadata_calls
        for policy in policy1, policy2:
            eq_(mirror, policy.mirror)

    def test_load_collection_no_site_wide_mirror(self):
        # Calling load_collection creates a new collection with
        # the given data source.
        script = DirectoryImportScript(self._db)
        collection, mirror = script.load_collection(
            "A collection", "A data source"
        )
        eq_("A collection", collection.name)
        eq_("A data source", collection.data_source.name)
        eq_(True, collection.data_source.offers_licenses)

        integration = collection.external_integration
        eq_(ExternalIntegration.LICENSE_GOAL, integration.goal)
        eq_(ExternalIntegration.MANUAL,
            integration.protocol)

        # The Collection has no mirror integration because there is no
        # sitewide storage integration to use.
        eq_(None, collection.mirror_integration)
        eq_(None, mirror)

    def test_load_collection_installs_site_wide_mirror(self):
        # We have a sitewide storage integration.
        integration = self._external_integration("my uploader")
        integration.goal = ExternalIntegration.STORAGE_GOAL

        # Calling load_collection creates a Collection and installs
        # the sitewide storage integration as its mirror integration.
        script = DirectoryImportScript(self._db)
        collection, mirror = script.load_collection(
            "A collection", "A data source"
        )
        eq_(integration, collection.mirror_integration)
        assert isinstance(mirror, MirrorUploader)

        # Calling create_collection again with the same arguments does
        # nothing.
        collection2, mirror2 = script.load_collection(
            "A collection", "A data source"
        )
        eq_(collection2, collection)

    def test_work_from_metadata(self):
        """Validate the ability to create a new Work from appropriate metadata.
        """

        class Mock(MockDirectoryImportScript):
            """In this test we need to verify that annotate_metadata
            was called but did nothing.
            """
            def annotate_metadata(self, metadata, *args, **kwargs):
                metadata.annotated = True
                return super(Mock, self).annotate_metadata(
                    metadata, *args, **kwargs
                )

        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1003")
        identifier_obj, ignore = identifier.load(self._db)
        metadata = Metadata(
            DataSource.GUTENBERG,
            primary_identifier=identifier,
            title=u"A book"
        )
        metadata.annotated = False
        datasource = DataSource.lookup(self._db, DataSource.GUTENBERG)
        policy = ReplacementPolicy.from_license_source(self._db)
        mirror = MockS3Uploader()
        policy.mirror = mirror

        # Here, work_from_metadata calls annotate_metadata, but does
        # not actually import anything because there are no files 'on
        # disk' and thus no way to actually get the book.
        collection = self._default_collection
        args = (collection, metadata, policy, "cover directory",
                "ebook directory", RightsStatus.CC0)
        script = Mock(self._db)
        eq_(None, script.work_from_metadata(*args))
        eq_(True, metadata.annotated)

        # Now let's try it with some files 'on disk'.
        with open(self.sample_cover_path('test-book-cover.png')) as fh:
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
        work = script.work_from_metadata(*args)

        # We have created a book. It has a cover image, which has a
        # thumbnail.
        eq_("A book", work.title)
        assert work.cover_full_url.endswith(
            '/test.cover.bucket/Gutenberg/Gutenberg+ID/1003/1003.jpg'
        )
        assert work.cover_thumbnail_url.endswith(
            '/test.cover.bucket/scaled/300/Gutenberg/Gutenberg+ID/1003/1003.png'
        )
        [pool] = work.license_pools
        assert pool.open_access_download_url.endswith(
            '/test.content.bucket/Gutenberg/Gutenberg+ID/1003/A+book.epub'
        )

        eq_(RightsStatus.CC0,
            pool.delivery_mechanisms[0].rights_status.uri)

        # The mock S3Uploader has a record of 'uploading' all these files
        # to S3.
        epub, full, thumbnail = mirror.uploaded
        eq_(epub.url, pool.open_access_download_url)
        eq_(full.url, work.cover_full_url)
        eq_(thumbnail.url, work.cover_thumbnail_url)

        # The EPUB Representation was cleared out after the upload, to
        # save database space.
        eq_("I'm an EPUB.", mirror.content[0])
        eq_(None, epub.content)

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

        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "11111")
        identifier_obj, ignore = identifier.load(self._db)
        metadata = Metadata(
            title=self._str,
            data_source=gutenberg,
            primary_identifier=identifier
        )
        mirror = object()
        policy = ReplacementPolicy(mirror=mirror)
        cover_directory = object()
        ebook_directory = object()
        rights_uri = object()

        script = MockNoCirculationData(self._db)
        args = (metadata, policy, cover_directory, ebook_directory, rights_uri)
        script.annotate_metadata(*args)

        # load_circulation_data was called.
        eq_(
            (identifier_obj, gutenberg, ebook_directory, mirror,
             metadata.title, rights_uri),
            script.load_circulation_data_args
        )

        # But because load_circulation_data returned None,
        # metadata.circulation_data was not modified and
        # load_cover_link was not called (which would have raised an
        # exception).
        eq_(None, metadata.circulation)

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
        eq_("Some circulation data", metadata.circulation)

        # load_cover_link was called.
        eq_(
            (identifier_obj, gutenberg, cover_directory, mirror),
            script.load_cover_link_args
        )

        # But since it provided no cover link, metadata.links was empty.
        eq_([], metadata.links)

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

        eq_("Some circulation data", metadata.circulation)
        eq_(['A cover link'], metadata.links)

    def test_load_circulation_data(self):
        # Create a directory import script with an empty mock filesystem.
        script = MockDirectoryImportScript(self._db, {})

        identifier = self._identifier(Identifier.GUTENBERG_ID, "2345")
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        mirror = MockS3Uploader()
        args = (identifier, gutenberg, "ebooks", mirror, "Name of book",
                "rights URI")

        # There is nothing on the mock filesystem, so in this case
        # load_circulation_data returns None.
        eq_(None, script.load_circulation_data(*args))

        # But we tried.
        eq_(
            ('2345', 'ebooks', Representation.COMMON_EBOOK_EXTENSIONS,
             'ebook file'),
            script._locate_file_args
        )

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
        eq_(identifier, circulation.primary_identifier(self._db))
        eq_(gutenberg, circulation.data_source(self._db))
        eq_("rights URI", circulation.default_rights_uri)

        # The CirculationData has an open-access link associated with it.
        [link] = circulation.links
        eq_(Hyperlink.OPEN_ACCESS_DOWNLOAD, link.rel)
        assert link.href.endswith(
            '/test.content.bucket/Gutenberg/Gutenberg+ID/2345/Name+of+book.epub'
        )
        eq_(Representation.EPUB_MEDIA_TYPE, link.media_type)
        eq_("I'm an EPUB.", link.content)

        # This open-access link will be made available through a
        # delivery mechanism described by this FormatData.
        [format] = circulation.formats
        eq_(link, format.link)
        eq_(link.media_type, format.content_type)
        eq_(DeliveryMechanism.NO_DRM, format.drm_scheme)

    def test_load_cover_link(self):
        # Create a directory import script with an empty mock filesystem.
        script = MockDirectoryImportScript(self._db, {})

        identifier = self._identifier(Identifier.GUTENBERG_ID, "2345")
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        mirror = MockS3Uploader()
        args = (identifier, gutenberg, "covers", mirror)

        # There is nothing on the mock filesystem, so in this case
        # load_cover_link returns None.
        eq_(None, script.load_cover_link(*args))

        # But we tried.
        eq_(
            ('2345', 'covers', Representation.COMMON_IMAGE_EXTENSIONS,
             'cover image'),
            script._locate_file_args
        )

        # Try another script that has a populated mock filesystem.
        mock_filesystem = {
            'covers' : (
                'acover.jpeg', Representation.JPEG_MEDIA_TYPE, "I'm an image."
            )
        }
        script = MockDirectoryImportScript(self._db, mock_filesystem)
        link = script.load_cover_link(*args)
        eq_(Hyperlink.IMAGE, link.rel)
        assert link.href.endswith(
            '/test.cover.bucket/Gutenberg/Gutenberg+ID/2345/2345.jpg'
        )
        eq_(Representation.JPEG_MEDIA_TYPE, link.media_type)
        eq_("I'm an image.", link.content)

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
            eq_((None, None, None), result)

        def assert_found(base_filename, directory, extensions):
            """Verify that the given set of arguments to _locate_file()
            finds and loads the single file on the mock filesystem..
            """
            result = DirectoryImportScript._locate_file(
                base_filename, directory, extensions, file_type="some file",
                mock_filesystem_operations=mock_filesystem_operations
            )
            eq_(
                ("thefile.JPEG", Representation.JPEG_MEDIA_TYPE,
                 "The contents"),
                result
            )

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

        eq_(params[0], l1)

        NoveListAPI.from_config = oldNovelistConfig
