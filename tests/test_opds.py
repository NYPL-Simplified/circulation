import base64
from collections import defaultdict
import contextlib
import datetime
import os
import re
import json
from lxml import etree
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)
import feedparser
from . import DatabaseTest

from core.analytics import Analytics
from core.lane import (
    FacetsWithEntryPoint,
    Lane,
    WorkList,
)
from core.model import (
    create,
    get_one_or_create,
    CirculationEvent,
    ConfigurationSetting,
    Contributor,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Library,
    PresentationCalculationPolicy,
    Representation,
    RightsStatus,
    SessionManager,
    Work,
)

from core.classifier import (
    Classifier,
    Fantasy,
    Urban_Fantasy
)

from core.entrypoint import (
    AudiobooksEntryPoint,
    EverythingEntryPoint,
)
from core.external_search import (
    MockExternalSearchIndex,
    WorkSearchResult,
)

from core.util.opds_writer import (
    AtomFeed,
    OPDSFeed,
)

from core.opds import (
    AcquisitionFeed,
    UnfulfillableWork,
)

from core.opds_import import (
    OPDSXMLParser
)

from api.circulation import (
    BaseCirculationAPI,
    CirculationAPI,
    FulfillmentInfo,
)
from api.config import (
    Configuration,
    temp_config,
)
from api.opds import (
    CirculationManagerAnnotator,
    LibraryAnnotator,
    SharedCollectionAnnotator,
    LibraryLoanAndHoldAnnotator,
    SharedCollectionLoanAndHoldAnnotator,
)

from api.testing import VendorIDTest
from api.adobe_vendor_id import AuthdataUtility
from api.novelist import NoveListAPI
from api.lanes import ContributorLane, CrawlableCustomListBasedLane
import jwt

_strftime = AtomFeed._strftime


class TestCirculationManagerAnnotator(DatabaseTest):
    def setup(self):
        super(TestCirculationManagerAnnotator, self).setup()
        self.work = self._work(with_open_access_download=True)
        self.lane = self._lane(display_name="Fantasy")
        self.annotator = CirculationManagerAnnotator(
            self.lane, test_mode=True,
        )

    def test_open_access_link(self):
        # The resource URL associated with a LicensePoolDeliveryMechanism
        # becomes the `href` of an open-access `link` tag.
        pool = self.work.license_pools[0]
        [lpdm] = pool.delivery_mechanisms

        # Temporarily disconnect the Resource's Representation so we
        # can verify that this works even if there is no
        # Representation.
        representation = lpdm.resource.representation
        lpdm.resource.representation = None
        lpdm.resource.url = "http://foo.com/thefile.epub"
        link_tag = self.annotator.open_access_link(pool, lpdm)
        eq_(lpdm.resource.url, link_tag.get('href'))

        # The dcterms:rights attribute may provide a more detailed
        # explanation of the book's copyright status.
        rights = link_tag.attrib['{http://purl.org/dc/terms/}rights']
        eq_(lpdm.rights_status.uri, rights)

        # If we have a CDN set up for open-access links, the CDN hostname
        # replaces the original hostname.
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][ExternalIntegration.CDN] = {
                'foo.com' : 'https://cdn.com/'
            }
            link_tag = self.annotator.open_access_link(pool, lpdm)

        link_url = link_tag.get('href')
        eq_("https://cdn.com/thefile.epub", link_url)

        # If the Resource has a Representation, the public URL is used
        # instead of the original Resource URL.
        lpdm.resource.representation = representation
        link_tag = self.annotator.open_access_link(pool, lpdm)
        eq_(representation.public_url, link_tag.get('href'))

        # If there is no Representation, the Resource's original URL is used.
        lpdm.resource.representation = None
        link_tag = self.annotator.open_access_link(pool, lpdm)
        eq_(lpdm.resource.url, link_tag.get('href'))

    def test_default_lane_url(self):
        default_lane_url = self.annotator.default_lane_url()
        assert "feed" in default_lane_url
        assert str(self.lane.id) not in default_lane_url

    def test_feed_url(self):
        feed_url_fantasy = self.annotator.feed_url(self.lane, dict(), dict())
        assert "feed" in feed_url_fantasy
        assert str(self.lane.id) in feed_url_fantasy
        assert self._default_library.name not in feed_url_fantasy

    def test_navigation_url(self):
        navigation_url_fantasy = self.annotator.navigation_url(self.lane)
        assert "navigation" in navigation_url_fantasy
        assert str(self.lane.id) in navigation_url_fantasy

    def test_visible_delivery_mechanisms(self):

        # By default, all delivery mechanisms are visible
        [pool] = self.work.license_pools
        [epub] = list(self.annotator.visible_delivery_mechanisms(pool))
        eq_("application/epub+zip", epub.delivery_mechanism.content_type)

        # Create an annotator that hides PDFs.
        no_pdf = CirculationManagerAnnotator(
            self.lane, hidden_content_types=["application/pdf"],
            test_mode=True,
        )

        # This has no effect on the EPUB.
        [epub2] = list(no_pdf.visible_delivery_mechanisms(pool))
        eq_(epub, epub2)

        # Create an annotator that hides EPUBs.
        no_epub = CirculationManagerAnnotator(
            self.lane, hidden_content_types=["application/epub+zip"],
            test_mode=True,
        )

        # The EPUB is hidden, and this license pool has no delivery
        # mechanisms.
        eq_([], list(no_epub.visible_delivery_mechanisms(pool)))


    def test_rights_attributes(self):
        m = self.annotator.rights_attributes

        # Given a LicensePoolDeliveryMechanism with a RightsStatus,
        # rights_attributes creates a dictionary mapping the dcterms:rights
        # attribute to the URI associated with the RightsStatus.
        lp = self._licensepool(None)
        [lpdm] = lp.delivery_mechanisms
        eq_({"{http://purl.org/dc/terms/}rights":lpdm.rights_status.uri},
            m(lpdm))

        # If any link in the chain is broken, rights_attributes returns
        # an empty dictionary.
        old_uri = lpdm.rights_status.uri
        lpdm.rights_status.uri = None
        eq_({}, m(lpdm))
        lpdm.rights_status.uri = old_uri

        lpdm.rights_status = None
        eq_({}, m(lpdm))

        eq_({}, m(None))

    def test_work_entry_includes_updated(self):

        # By default, the 'updated' date is the value of
        # Work.last_update_time.
        work = self._work(with_open_access_download=True)
        # This date is later, but we don't check it.
        work.license_pools[0].availability_time = datetime.datetime(2019, 1, 1)
        work.last_update_time = datetime.datetime(2018, 2, 4)

        def entry_for(work):
            worklist = WorkList()
            worklist.initialize(None)
            annotator = CirculationManagerAnnotator(worklist, test_mode=True)
            feed = AcquisitionFeed(self._db, "test", "url", [work], annotator)
            feed = feedparser.parse(unicode(feed))
            [entry] = feed.entries
            return entry

        entry = entry_for(work)
        assert '2018-02-04' in entry.get("updated")

        # If the work passed in is a WorkSearchResult that indicates
        # the search index found a later 'update time', then the later
        # time is used. This value isn't always present -- it's only
        # calculated when the list is being _ordered_ by 'update time'.
        # Otherwise it's too slow to bother.
        class MockHit(object):
            def __init__(self, last_update):
                # Store the time the way we get it from ElasticSearch --
                # as a single-element list containing seconds since epoch.
                self.last_update = [
                    (last_update-datetime.datetime(1970, 1, 1)).total_seconds()
                ]
        hit = MockHit(datetime.datetime(2018, 2, 5))
        result = WorkSearchResult(work, hit)
        entry = entry_for(result)
        assert '2018-02-05' in entry.get("updated")

        # Any 'update time' provided by ElasticSearch is used even if
        # it's clearly earlier than Work.last_update_time.
        hit = MockHit(datetime.datetime(2017, 1, 1))
        result._hit = hit
        entry = entry_for(result)
        assert '2017-01-01' in entry.get("updated")


class TestLibraryAnnotator(VendorIDTest):
    def setup(self):
        super(TestLibraryAnnotator, self).setup()
        self.work = self._work(with_open_access_download=True)

        parent = self._lane(
            display_name="Fiction", languages=["eng"], fiction=True
        )
        self.lane = self._lane(display_name="Fantasy", languages=["eng"])
        self.lane.add_genre(Fantasy.name)
        self.lane.parent = parent
        self.annotator = LibraryAnnotator(
            None, self.lane, self._default_library, test_mode=True, top_level_title="Test Top Level Title"
        )

        # Initialize library with Adobe Vendor ID details
        self._default_library.library_registry_short_name = "FAKE"
        self._default_library.library_registry_shared_secret = "s3cr3t5"

        # A ContributorLane to test code that handles it differently.
        self.contributor, ignore = self._contributor("Someone")
        self.contributor_lane = ContributorLane(
            self._default_library, self.contributor, languages=["eng"],
            audiences=None
        )

    def test__hidden_content_types(self):

        def f(value):
            """Set the default library's HIDDEN_CONTENT_TYPES setting
            to a specific value and see what _hidden_content_types
            says.
            """
            library = self._default_library
            library.setting(Configuration.HIDDEN_CONTENT_TYPES).value = value
            return LibraryAnnotator._hidden_content_types(library)

        # When the value is not set at all, no content types are hidden.
        eq_(
            [],
            list(LibraryAnnotator._hidden_content_types(self._default_library))
        )

        # Now set various values and see what happens.
        eq_([], f(None))
        eq_([], f(""))
        eq_([], f(json.dumps([])))
        eq_(["text/html"], f("text/html"))
        eq_(["text/html"], f(json.dumps("text/html")))
        eq_(["text/html"], f(json.dumps({"text/html" : "some value"})))
        eq_(
            ["text/html", "text/plain"],
            f(json.dumps(["text/html", "text/plain"]))
        )

    def test_add_configuration_links(self):
        mock_feed = []
        link_config = {
            LibraryAnnotator.TERMS_OF_SERVICE: "http://terms/",
            LibraryAnnotator.PRIVACY_POLICY: "http://privacy/",
            LibraryAnnotator.COPYRIGHT: "http://copyright/",
            LibraryAnnotator.ABOUT: "http://about/",
            LibraryAnnotator.LICENSE: "http://license/",
            Configuration.HELP_EMAIL : "help@me",
            Configuration.HELP_WEB : "http://help/",
            Configuration.HELP_URI : "uri:help",
        }

        # Set up configuration settings for links.
        for rel, value in link_config.iteritems():
            ConfigurationSetting.for_library(rel, self._default_library).value = value

        # Set up settings for navigation links.
        ConfigurationSetting.for_library(
            Configuration.WEB_HEADER_LINKS, self._default_library
        ).value = json.dumps(["http://example.com/1", "http://example.com/2"])
        ConfigurationSetting.for_library(
            Configuration.WEB_HEADER_LABELS, self._default_library
        ).value = json.dumps(["one", "two"])

        self.annotator.add_configuration_links(mock_feed)

        # Ten links were added to the "feed"
        eq_(10, len(mock_feed))

        # They are the links we'd expect.
        links = {}
        for link in mock_feed:
            rel = link.attrib['rel']
            href = link.attrib['href']
            if rel == 'help' or rel == 'related':
                continue # Tested below
            # Check that the configuration value made it into the link.
            eq_(href, link_config[rel])
            eq_("text/html", link.attrib['type'])
                
        # There are three help links using different protocols.
        help_links = [x.attrib['href'] for x in mock_feed
                      if x.attrib['rel'] == 'help']
        eq_(set(["mailto:help@me", "http://help/", "uri:help"]),
            set(help_links))

        # There are two navigation links.
        navigation_links = [x for x in mock_feed if x.attrib['rel'] == 'related']
        eq_(set(["navigation"]), set([x.attrib["role"] for x in navigation_links]))
        eq_(set(["http://example.com/1", "http://example.com/2"]),
            set([x.attrib["href"] for x in navigation_links]))
        eq_(set(["one", "two"]), set([x.attrib["title"] for x in navigation_links]))

    def test_top_level_title(self):
        eq_("Test Top Level Title", self.annotator.top_level_title())

    def test_group_uri_with_flattened_lane(self):
        spanish_lane = self._lane(display_name="Spanish", languages=["spa"])
        flat_spanish_lane = dict({
            "lane": spanish_lane,
            "label": "All Spanish",
            "link_to_list_feed": True
        })
        spanish_work = self._work(
            title="Spanish Book",
            with_license_pool=True,
            language="spa"
        )
        lp = spanish_work.license_pools[0]
        self.annotator.lanes_by_work[spanish_work].append(flat_spanish_lane)

        feed_url = self.annotator.feed_url(spanish_lane)
        group_uri = self.annotator.group_uri(spanish_work, lp, lp.identifier)
        eq_((feed_url, "All Spanish"), group_uri)

    def test_lane_url(self):
        fantasy_lane_with_sublanes = self._lane(
            display_name="Fantasy with sublanes", languages=["eng"]
        )
        fantasy_lane_with_sublanes.add_genre(Fantasy.name)

        urban_fantasy_lane = self._lane(display_name="Urban Fantasy")
        urban_fantasy_lane.add_genre(Urban_Fantasy.name)
        fantasy_lane_with_sublanes.sublanes.append(urban_fantasy_lane)

        fantasy_lane_without_sublanes = self._lane(
            display_name="Fantasy without sublanes", languages=["eng"]
        )
        fantasy_lane_without_sublanes.add_genre(Fantasy.name)

        default_lane_url = self.annotator.lane_url(None)
        eq_(default_lane_url, self.annotator.default_lane_url())

        facets = dict(entrypoint="Book")
        default_lane_url = self.annotator.lane_url(None, facets=facets)
        eq_(default_lane_url, self.annotator.default_lane_url(facets=facets))

        groups_url = self.annotator.lane_url(fantasy_lane_with_sublanes)
        eq_(groups_url, self.annotator.groups_url(fantasy_lane_with_sublanes))

        groups_url = self.annotator.lane_url(fantasy_lane_with_sublanes, facets=facets)
        eq_(groups_url, self.annotator.groups_url(fantasy_lane_with_sublanes, facets=facets))

        feed_url = self.annotator.lane_url(fantasy_lane_without_sublanes)
        eq_(feed_url, self.annotator.feed_url(fantasy_lane_without_sublanes))

        feed_url = self.annotator.lane_url(fantasy_lane_without_sublanes, facets=facets)
        eq_(feed_url, self.annotator.feed_url(fantasy_lane_without_sublanes, facets=facets))

    def test_fulfill_link_issues_only_open_access_links_when_library_does_not_identify_patrons(self):

        # This library doesn't identify patrons.
        self.annotator.identifies_patrons = False

        # Because of this, normal fulfillment links are not generated.
        [pool] = self.work.license_pools
        [lpdm] = pool.delivery_mechanisms
        eq_(None,
            self.annotator.fulfill_link(pool, None, lpdm)
        )

        # However, fulfillment links _can_ be generated with the
        # 'open-access' link relation.
        link = self.annotator.fulfill_link(
            pool, None, lpdm, OPDSFeed.OPEN_ACCESS_REL
        )
        eq_(OPDSFeed.OPEN_ACCESS_REL, link.attrib['rel'])

    def test_fulfill_link_includes_device_registration_tags(self):
        """Verify that when Adobe Vendor ID delegation is included, the
        fulfill link for an Adobe delivery mechanism includes instructions
        on how to get a Vendor ID.
        """
        self.initialize_adobe(self._default_library)
        [pool] = self.work.license_pools
        identifier = pool.identifier
        patron = self._patron()
        old_credentials = list(patron.credentials)

        loan, ignore = pool.loan_to(patron, start=datetime.datetime.utcnow())
        adobe_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            self._db, "text/html", DeliveryMechanism.ADOBE_DRM
        )
        other_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            self._db, "text/html", DeliveryMechanism.OVERDRIVE_DRM
        )

        # The fulfill link for non-Adobe DRM does not
        # include the drm:licensor tag.
        link = self.annotator.fulfill_link(
            pool, loan, other_delivery_mechanism
       )
        for child in link.getchildren():
            assert child.tag != "{http://librarysimplified.org/terms/drm}licensor"

        # No new Credential has been associated with the patron.
        eq_(old_credentials, patron.credentials)

        # The fulfill link for Adobe DRM includes information
        # on how to get an Adobe ID in the drm:licensor tag.
        link = self.annotator.fulfill_link(
            pool, loan, adobe_delivery_mechanism
        )
        licensor = link.getchildren()[-1]
        eq_("{http://librarysimplified.org/terms/drm}licensor",
            licensor.tag)

        # An Adobe ID-specific identifier has been created for the patron.
        [adobe_id_identifier] = [x for x in patron.credentials
                                 if x not in old_credentials]
        eq_(AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            adobe_id_identifier.type)
        eq_(DataSource.INTERNAL_PROCESSING,
            adobe_id_identifier.data_source.name)
        eq_(None, adobe_id_identifier.expires)

        # The drm:licensor tag is the one we get by calling
        # adobe_id_tags() on that identifier.
        [expect] = self.annotator.adobe_id_tags(adobe_id_identifier.credential)
        eq_(etree.tostring(expect), etree.tostring(licensor))

    def test_no_adobe_id_tags_when_vendor_id_not_configured(self):
        """When vendor ID delegation is not configured, adobe_id_tags()
        returns an empty list.
        """
        eq_([], self.annotator.adobe_id_tags("patron identifier"))

    def test_adobe_id_tags_when_vendor_id_configured(self):
        """When vendor ID delegation is configured, adobe_id_tags()
        returns a list containing a single tag. The tag contains
        the information necessary to get an Adobe ID and a link to the local
        DRM Device Management Protocol endpoint.
        """
        library = self._default_library
        self.initialize_adobe(library)
        patron_identifier = "patron identifier"
        [element] = self.annotator.adobe_id_tags(patron_identifier)
        eq_('{http://librarysimplified.org/terms/drm}licensor', element.tag)

        key = '{http://librarysimplified.org/terms/drm}vendor'
        eq_(self.adobe_vendor_id.username, element.attrib[key])

        [token, device_management_link] = element.getchildren()

        eq_('{http://librarysimplified.org/terms/drm}clientToken', token.tag)
        # token.text is a token which we can decode, since we know
        # the secret.
        token = token.text
        authdata = AuthdataUtility.from_config(library)
        decoded = authdata.decode_short_client_token(token)
        expected_url = ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library
        ).value
        eq_((expected_url, patron_identifier), decoded)

        eq_("link", device_management_link.tag)
        eq_("http://librarysimplified.org/terms/drm/rel/devices",
            device_management_link.attrib['rel'])
        expect_url = self.annotator.url_for(
            'adobe_drm_devices', library_short_name=library.short_name,
            _external=True
        )
        eq_(expect_url, device_management_link.attrib['href'])

        # If we call adobe_id_tags again we'll get a distinct tag
        # object that renders to the same XML.
        [same_tag] = self.annotator.adobe_id_tags(patron_identifier)
        assert same_tag is not element
        eq_(etree.tostring(element), etree.tostring(same_tag))

        # If the Adobe Vendor ID configuration is present but
        # incomplete, adobe_id_tags does nothing.

        # Delete one setting from the existing integration to check
        # this.
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, ExternalIntegration.USERNAME, library,
            self.registry
        )
        self._db.delete(setting)
        eq_([], self.annotator.adobe_id_tags("new identifier"))

    def test_default_lane_url(self):
        default_lane_url = self.annotator.default_lane_url()
        assert "groups" in default_lane_url
        assert str(self.lane.id) not in default_lane_url

        facets = dict(entrypoint="Book")
        default_lane_url = self.annotator.default_lane_url(facets=facets)
        assert "entrypoint=Book" in default_lane_url

    def test_groups_url(self):
        groups_url_no_lane = self.annotator.groups_url(None)
        assert "groups" in groups_url_no_lane
        assert str(self.lane.id) not in groups_url_no_lane

        groups_url_fantasy = self.annotator.groups_url(self.lane)
        assert "groups" in groups_url_fantasy
        assert str(self.lane.id) in groups_url_fantasy

        facets = dict(arg="value")
        groups_url_facets = self.annotator.groups_url(None, facets=facets)
        assert "arg=value" in groups_url_facets

    def test_feed_url(self):
        # A regular Lane.
        feed_url_fantasy = self.annotator.feed_url(
            self.lane, dict(facet="value"), dict()
        )
        assert "feed" in feed_url_fantasy
        assert "facet=value" in feed_url_fantasy
        assert str(self.lane.id) in feed_url_fantasy
        assert self._default_library.name in feed_url_fantasy

        # A QueryGeneratedLane.
        self.annotator.lane = self.contributor_lane
        feed_url_contributor = self.annotator.feed_url(self.contributor_lane, dict(), dict())
        assert self.contributor_lane.ROUTE in feed_url_contributor
        assert self.contributor_lane.contributor_key in feed_url_contributor
        assert self._default_library.name in feed_url_contributor

    def test_search_url(self):
        search_url = self.annotator.search_url(
            self.lane, "query", dict(), dict(facet="value")
        )
        assert "search" in search_url
        assert "query" in search_url
        assert "facet=value" in search_url
        assert str(self.lane.id) in search_url

    def test_facet_url(self):
        # A regular Lane.
        facets = dict(collection="main")
        facet_url = self.annotator.facet_url(facets)
        assert "collection=main" in facet_url
        assert str(self.lane.id) in facet_url

        # A QueryGeneratedLane.
        self.annotator.lane = self.contributor_lane

        facet_url_contributor = self.annotator.facet_url(facets)
        assert "collection=main" in facet_url_contributor
        assert self.contributor_lane.ROUTE in facet_url_contributor
        assert self.contributor_lane.contributor_key in facet_url_contributor

    def test_alternate_link_is_permalink(self):
        work = self._work(with_open_access_download=True)
        works = self._db.query(Work)
        annotator = LibraryAnnotator(None, self.lane, self._default_library, test_mode=True)
        pool = annotator.active_licensepool_for(work)

        feed = self.get_parsed_feed([work])
        [entry] = feed['entries']
        eq_(entry['id'], pool.identifier.urn)

        [(alternate, type)] = [(x['href'], x['type']) for x in entry['links'] if x['rel'] == 'alternate']
        permalink, permalink_type = self.annotator.permalink_for(
            work, pool, pool.identifier
        )
        eq_(alternate, permalink)
        eq_(OPDSFeed.ENTRY_TYPE, type)
        eq_(permalink_type, type)

        # Make sure we are using the 'permalink' controller -- we were using
        # 'work' and that was wrong.
        assert '/host/permalink' in permalink

    def test_annotate_work_entry(self):
        lane = self._lane()

        # Create a Work.
        work = self._work(with_license_pool=True)
        [pool] = work.license_pools
        identifier = pool.identifier
        edition = pool.presentation_edition

        # Try building an entry for this Work with and without
        # patron authentication turned on -- each setting is valid
        # but will result in different links being available.
        linksets = []
        for auth in (True, False):
            annotator = LibraryAnnotator(
                None, lane, self._default_library, test_mode=True,
                library_identifies_patrons=auth
            )
            feed = AcquisitionFeed(self._db, "test", "url", [], annotator)
            entry = feed._make_entry_xml(work, edition)
            annotator.annotate_work_entry(
                work, pool, edition, identifier, feed, entry
            )
            parsed = feedparser.parse(etree.tostring(entry))
            [entry_parsed] = parsed['entries']
            linksets.append(set([x['rel'] for x in entry_parsed['links']]))

        with_auth, no_auth = linksets

        # Some links are present no matter what.
        for expect in [u'alternate', u'issues', u'related']:
            assert expect in with_auth
            assert expect in no_auth

        # A library with patron authentication offers some additional
        # links -- one to borrow the book and one to annotate the
        # book.
        for expect in [
                u'http://www.w3.org/ns/oa#annotationservice',
                u'http://opds-spec.org/acquisition/borrow'
        ]:
            assert expect in with_auth
            assert expect not in no_auth

        # We can also build an entry for a work with no license pool,
        # but it will have no borrow link.
        work = self._work(with_license_pool=False)
        edition = work.presentation_edition
        identifier = edition.primary_identifier

        annotator = LibraryAnnotator(
            None, lane, self._default_library, test_mode=True,
            library_identifies_patrons=True
        )
        feed = AcquisitionFeed(self._db, "test", "url", [], annotator)
        entry = feed._make_entry_xml(work, edition)
        annotator.annotate_work_entry(
            work, None, edition, identifier, feed, entry
        )
        parsed = feedparser.parse(etree.tostring(entry))
        [entry_parsed] = parsed['entries']
        links = set([x['rel'] for x in entry_parsed['links']])

        # These links are still present.
        for expect in [u'alternate', u'issues', u'related', u'http://www.w3.org/ns/oa#annotationservice']:
            assert expect in links

        # But the borrow link is gone.
        assert u'http://opds-spec.org/acquisition/borrow' not in links

        # There are no links to create analytics events for this title,
        # because the library has no analytics configured.
        open_book_rel = 'http://librarysimplified.org/terms/rel/analytics/open-book'
        assert open_book_rel not in links

        # If analytics are configured, a link is added to
        # create an 'open_book' analytics event for this title.
        Analytics.GLOBAL_ENABLED = True
        entry = feed._make_entry_xml(work, edition)
        annotator.annotate_work_entry(
            work, None, edition, identifier, feed, entry
        )
        parsed = feedparser.parse(etree.tostring(entry))
        [entry_parsed] = parsed['entries']
        [analytics_link] = [x['href'] for x in entry_parsed['links']
                            if x['rel'] == open_book_rel]
        expect = annotator.url_for(
            'track_analytics_event',
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            event_type=CirculationEvent.OPEN_BOOK,
            library_short_name=self._default_library.short_name,
            _external=True
        )
        eq_(expect, analytics_link)

    def test_annotate_feed(self):
        lane = self._lane()
        linksets = []
        for auth in (True, False):
            annotator = LibraryAnnotator(
                None, lane, self._default_library, test_mode=True,
                library_identifies_patrons=auth
            )
            feed = AcquisitionFeed(self._db, "test", "url", [], annotator)
            annotator.annotate_feed(feed, lane)
            parsed = feedparser.parse(unicode(feed))
            linksets.append([x['rel'] for x in parsed['feed']['links']])

        with_auth, without_auth = linksets

        # There's always a self link, a search link, and an auth
        # document link.
        for rel in (
                'self', 'search', u'http://opds-spec.org/auth/document'
        ):
            assert rel in with_auth
            assert rel in without_auth

        # But there's only a bookshelf link and an annotation link
        # when patron authentication is enabled.
        for rel in (
                u'http://opds-spec.org/shelf',
                u'http://www.w3.org/ns/oa#annotationservice'
        ):
            assert rel in with_auth
            assert rel not in without_auth


    def get_parsed_feed(self, works, lane=None, **kwargs):
        if not lane:
            lane = self._lane(display_name="Main Lane")
        feed = AcquisitionFeed(
            self._db, "test", "url", works,
            LibraryAnnotator(None, lane, self._default_library, test_mode=True,
                             **kwargs)
        )
        return feedparser.parse(unicode(feed))

    def assert_link_on_entry(self, entry, link_type=None, rels=None,
                             partials_by_rel=None
    ):
        """Asserts that a link with a certain 'rel' value exists on a
        given feed or entry, as well as its link 'type' value and parts
        of its 'href' value.
        """
        def get_link_by_rel(rel):
            try:
                [link] = [x for x in entry['links'] if x['rel']==rel]
            except ValueError as e:
                raise AssertionError
            if link_type:
                eq_(link_type, link.type)
            return link

        if rels:
            [get_link_by_rel(rel) for rel in rels]

        partials_by_rel = partials_by_rel or dict()
        for rel, uri_partials in partials_by_rel.items():
            link = get_link_by_rel(rel)
            if not isinstance(uri_partials, list):
                uri_partials = [uri_partials]
            for part in uri_partials:
                assert part in link.href

    def test_work_entry_includes_problem_reporting_link(self):
        work = self._work(with_open_access_download=True)
        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        expected_rel_and_partial = { 'issues' : '/report' }
        self.assert_link_on_entry(entry, partials_by_rel=expected_rel_and_partial)

    def test_work_entry_includes_open_access_or_borrow_link(self):
        open_access_work = self._work(with_open_access_download=True)
        licensed_work = self._work(with_license_pool=True)
        licensed_work.license_pools[0].open_access = False

        feed = self.get_parsed_feed([open_access_work, licensed_work])
        [open_access_entry, licensed_entry] = feed.entries

        self.assert_link_on_entry(open_access_entry, rels=[OPDSFeed.BORROW_REL])
        self.assert_link_on_entry(licensed_entry, rels=[OPDSFeed.BORROW_REL])

    def test_language_and_audience_key_from_work(self):
        work = self._work(language='eng', audience=Classifier.AUDIENCE_CHILDREN)
        result = self.annotator.language_and_audience_key_from_work(work)
        eq_(('eng', 'Children'), result)

        work = self._work(language='fre', audience=Classifier.AUDIENCE_YOUNG_ADULT)
        result = self.annotator.language_and_audience_key_from_work(work)
        eq_(('fre', 'All+Ages,Children,Young+Adult'), result)

        work = self._work(language='spa', audience=Classifier.AUDIENCE_ADULT)
        result = self.annotator.language_and_audience_key_from_work(work)
        eq_(('spa', 'Adult,Adults+Only,All+Ages,Children,Young+Adult'), result)

        work = self._work(audience=Classifier.AUDIENCE_ADULTS_ONLY)
        result = self.annotator.language_and_audience_key_from_work(work)
        eq_(('eng', 'Adult,Adults+Only,All+Ages,Children,Young+Adult'), result)

        work = self._work(audience=Classifier.AUDIENCE_RESEARCH)
        result = self.annotator.language_and_audience_key_from_work(work)
        eq_(('eng', 'Adult,Adults+Only,All+Ages,Children,Research,Young+Adult'), result)

        work = self._work(audience=Classifier.AUDIENCE_ALL_AGES)
        result = self.annotator.language_and_audience_key_from_work(work)
        eq_(('eng', 'All+Ages,Children'), result)


    def test_work_entry_includes_contributor_links(self):
        """ContributorLane links are added to works with contributors"""
        work = self._work(with_open_access_download=True)
        contributor1 = work.presentation_edition.author_contributors[0]
        feed = self.get_parsed_feed([work])
        [entry] = feed.entries

        expected_rel_and_partial = dict(contributor='/contributor')
        self.assert_link_on_entry(
            entry, link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial,
        )

        # When there are two authors, they each get a contributor link.
        work.presentation_edition.add_contributor(u'Oprah', Contributor.AUTHOR_ROLE)
        work.calculate_presentation(
            PresentationCalculationPolicy(regenerate_opds_entries=True),
            MockExternalSearchIndex()
        )
        [entry] = self.get_parsed_feed([work]).entries
        contributor_links = [l for l in entry.links if l.rel == 'contributor']
        eq_(2, len(contributor_links))
        contributor_links.sort(key=lambda l: l.href)
        for l in contributor_links:
            assert l.type == OPDSFeed.ACQUISITION_FEED_TYPE
            assert '/contributor' in l.href
        assert contributor1.sort_name in contributor_links[0].href
        assert 'Oprah' in contributor_links[1].href

        # When there's no author, there's no contributor link.
        self._db.delete(work.presentation_edition.contributions[0])
        self._db.delete(work.presentation_edition.contributions[1])
        self._db.commit()
        work.calculate_presentation(
            PresentationCalculationPolicy(regenerate_opds_entries=True),
            MockExternalSearchIndex()
        )
        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        eq_([], filter(lambda l: l.rel=='contributor', entry.links))

    def test_work_entry_includes_series_link(self):
        """A series lane link is added to the work entry when its in a series
        """
        work = self._work(
            with_open_access_download=True, series='Serious Cereals Series'
        )
        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        expected_rel_and_partial = dict(series='/series')
        self.assert_link_on_entry(
            entry, link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial
        )

        # When there's no series, there's no series link.
        work = self._work(with_open_access_download=True)
        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        eq_([], filter(lambda l: l.rel=='series', entry.links))

    def test_work_entry_includes_recommendations_link(self):
        work = self._work(with_open_access_download=True)

        # If NoveList Select isn't configured, there's no recommendations link.
        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        eq_([], filter(lambda l: l.rel=='recommendations', entry.links))

        # There's a recommendation link when configuration is found, though!
        NoveListAPI.IS_CONFIGURED = None
        self._external_integration(
            ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL, username=u'library',
            password=u'sure', libraries=[self._default_library],
        )

        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        expected_rel_and_partial = dict(recommendations='/recommendations')
        self.assert_link_on_entry(
            entry, link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial)

    def test_work_entry_includes_annotations_link(self):
        work = self._work(with_open_access_download=True)
        identifier_str = work.license_pools[0].identifier.identifier
        uri_parts = ['/annotations', identifier_str]
        annotation_rel = 'http://www.w3.org/ns/oa#annotationservice'
        rel_with_partials = { annotation_rel : uri_parts }

        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        self.assert_link_on_entry(entry, partials_by_rel=rel_with_partials)

        # If the library does not authenticate patrons, no link to the
        # annotation service is provided.
        feed = self.get_parsed_feed(
            [work], library_identifies_patrons=False
        )
        [entry] = feed.entries
        assert annotation_rel not in [x['rel'] for x in entry['links']]

    def test_active_loan_feed(self):
        self.initialize_adobe(self._default_library)
        patron = self._patron()
        cls = LibraryLoanAndHoldAnnotator
        raw = cls.active_loans_for(None, patron, test_mode=True)
        # No entries in the feed...
        raw = unicode(raw)
        feed = feedparser.parse(raw)
        eq_(0, len(feed['entries']))

        # ... but we have a link to the User Profile Management
        # Protocol endpoint...
        links = feed['feed']['links']
        [upmp_link] = [
            x for x in links
            if x['rel'] == 'http://librarysimplified.org/terms/rel/user-profile'
        ]
        annotator = cls(None, None, library=patron.library, patron=patron, test_mode=True)
        expect_url = annotator.url_for(
            'patron_profile', library_short_name=patron.library.short_name, _external=True
        )
        eq_(expect_url, upmp_link['href'])

        # ... and we have DRM licensing information.
        tree = etree.fromstring(raw)
        parser = OPDSXMLParser()
        licensor = parser._xpath1(tree, "//atom:feed/drm:licensor")

        adobe_patron_identifier = AuthdataUtility._adobe_patron_identifier(patron)

        # The DRM licensing information includes the Adobe vendor ID
        # and the patron's patron identifier for Adobe purposes.
        eq_(self.adobe_vendor_id.username,
            licensor.attrib['{http://librarysimplified.org/terms/drm}vendor'])
        [client_token, device_management_link] = licensor.getchildren()
        expected = ConfigurationSetting.for_library_and_externalintegration(
            self._db, ExternalIntegration.USERNAME, self._default_library, self.registry
        ).value.upper()
        assert client_token.text.startswith(expected)
        assert adobe_patron_identifier in client_token.text
        eq_("{http://www.w3.org/2005/Atom}link",
            device_management_link.tag)
        eq_("http://librarysimplified.org/terms/drm/rel/devices",
            device_management_link.attrib['rel'])

        # Unlike other places this tag shows up, we use the
        # 'scheme' attribute to explicitly state that this
        # <drm:licensor> tag is talking about an ACS licensing
        # scheme. Since we're in a <feed> and not a <link> to a
        # specific book, that context would otherwise be lost.
        eq_('http://librarysimplified.org/terms/drm/scheme/ACS',
            licensor.attrib['{http://librarysimplified.org/terms/drm}scheme'])

        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)

        # A loan of an open-access book is open-ended.
        work1 = self._work(language="eng", with_open_access_download=True)
        loan1 = work1.license_pools[0].loan_to(patron, start=now)

        # A loan of some other kind of book
        work2 = self._work(language="eng", with_license_pool=True)
        loan2 = work2.license_pools[0].loan_to(patron, start=now, end=tomorrow)
        unused = self._work(language="eng", with_open_access_download=True)

        # Get the feed.
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)
        feed = feedparser.parse(raw)

        # The only entries in the feed is the work currently out on loan
        # to this patron.
        eq_(2, len(feed['entries']))
        e1, e2 = sorted(feed['entries'], key=lambda x: x['title'])
        eq_(work1.title, e1['title'])
        eq_(work2.title, e2['title'])

        # Make sure that the start and end dates from the loan are present
        # in an <opds:availability> child of the acquisition link.
        tree = etree.fromstring(raw)
        parser = OPDSXMLParser()
        acquisitions = parser._xpath(
            tree, "//atom:entry/atom:link[@rel='http://opds-spec.org/acquisition']"
        )
        eq_(2, len(acquisitions))

        now_s = _strftime(now)
        tomorrow_s = _strftime(tomorrow)
        availabilities = [
            parser._xpath1(x, "opds:availability") for x in acquisitions
        ]

        # One of these availability tags has 'since' but not 'until'.
        # The other one has both.
        [no_until] = [x for x in availabilities if 'until' not in x.attrib]
        eq_(now_s, no_until.attrib['since'])

        [has_until] = [x for x in availabilities if 'until' in x.attrib]
        eq_(now_s, has_until.attrib['since'])
        eq_(tomorrow_s, has_until.attrib['until'])

    def test_loan_feed_includes_patron(self):
        patron = self._patron()

        patron.username = u'bellhooks'
        patron.authorization_identifier = u'987654321'
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)
        feed_details = feedparser.parse(raw)['feed']

        assert "simplified:authorizationIdentifier" in raw
        assert "simplified:username" in raw
        eq_(patron.username, feed_details['simplified_patron']['simplified:username'])
        eq_(u'987654321', feed_details['simplified_patron']['simplified:authorizationidentifier'])

    def test_loans_feed_includes_annotations_link(self):
        patron = self._patron()
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)
        feed = feedparser.parse(raw)['feed']
        links = feed['links']

        [annotations_link] = [x for x in links if x['rel'].lower() == "http://www.w3.org/ns/oa#annotationService".lower()]
        assert '/annotations' in annotations_link['href']

    def test_active_loan_feed_ignores_inconsistent_local_data(self):
        patron = self._patron()

        work1 = self._work(language="eng", with_license_pool=True)
        loan, ignore = work1.license_pools[0].loan_to(patron)
        work2 = self._work(language="eng", with_license_pool=True)
        hold, ignore = work2.license_pools[0].on_hold_to(patron)

        # Uh-oh, our local loan data is bad.
        loan.license_pool.identifier = None

        # Our local hold data is also bad.
        hold.license_pool = None

        # We can still get a feed...
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)

        # ...but it's empty.
        assert '<entry>' not in unicode(feed_obj)

    def test_acquisition_feed_includes_license_information(self):
        work = self._work(with_open_access_download=True)
        pool = work.license_pools[0]

        # These numbers are impossible, but it doesn't matter for
        # purposes of this test.
        pool.open_access = False
        pool.licenses_owned = 100
        pool.licenses_available = 50
        pool.patrons_in_hold_queue = 25

        feed = AcquisitionFeed(
            self._db, "title", "url", [work], self.annotator
        )
        u = unicode(feed)
        holds_re = re.compile('<opds:holds\W+total="25"\W*/>', re.S)
        assert holds_re.search(u) is not None

        copies_re = re.compile('<opds:copies[^>]+available="50"', re.S)
        assert copies_re.search(u) is not None

        copies_re = re.compile('<opds:copies[^>]+total="100"', re.S)
        assert copies_re.search(u) is not None

    def test_loans_feed_includes_fulfill_links(self):
        patron = self._patron()

        work = self._work(with_license_pool=True, with_open_access_download=False)
        pool = work.license_pools[0]
        pool.open_access = False
        mech1 = pool.delivery_mechanisms[0]
        mech2 = pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT, None
        )
        streaming_mech = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM,
            RightsStatus.IN_COPYRIGHT, None
        )

        now = datetime.datetime.utcnow()
        loan, ignore = pool.loan_to(patron, start=now)

        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)

        entries = feedparser.parse(raw)['entries']
        eq_(1, len(entries))

        links = entries[0]['links']

        # Before we fulfill the loan, there are fulfill links for all three mechanisms.
        fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
        eq_(3, len(fulfill_links))

        eq_(set([mech1.delivery_mechanism.drm_scheme_media_type, mech2.delivery_mechanism.drm_scheme_media_type,
                 OPDSFeed.ENTRY_TYPE]),
            set([link['type'] for link in fulfill_links]))

        # If one of the content types is hidden, the corresponding
        # delivery mechanism does not have a link.
        setting = self._default_library.setting(
            Configuration.HIDDEN_CONTENT_TYPES
        )
        setting.value = json.dumps([mech1.delivery_mechanism.content_type])
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )
        eq_(set([mech2.delivery_mechanism.drm_scheme_media_type,
                 OPDSFeed.ENTRY_TYPE]),
            set([link['type'] for link in fulfill_links]))
        setting.value = None

        # When the loan is fulfilled, there are only fulfill links for that mechanism
        # and the streaming mechanism.
        loan.fulfillment = mech1

        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)

        entries = feedparser.parse(raw)['entries']
        eq_(1, len(entries))

        links = entries[0]['links']

        fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
        eq_(2, len(fulfill_links))

        eq_(set([mech1.delivery_mechanism.drm_scheme_media_type,
                 OPDSFeed.ENTRY_TYPE]),
            set([link['type'] for link in fulfill_links]))

    def test_fulfill_feed(self):
        patron = self._patron()

        work = self._work(with_license_pool=True, with_open_access_download=False)
        pool = work.license_pools[0]
        pool.open_access = False
        streaming_mech = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM,
            RightsStatus.IN_COPYRIGHT, None
        )

        now = datetime.datetime.utcnow()
        loan, ignore = pool.loan_to(patron, start=now)
        fulfillment = FulfillmentInfo(
            pool.collection, pool.data_source.name,
            pool.identifier.type, pool.identifier.identifier,
            "http://streaming_link",
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
            None, None)

        response = LibraryLoanAndHoldAnnotator.single_item_feed(
            None, loan, fulfillment, test_mode=True
        )
        raw = response.data

        entries = feedparser.parse(raw)['entries']
        eq_(1, len(entries))

        links = entries[0]['links']

        # The feed for a single fulfillment only includes one fulfill link.
        fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
        eq_(1, len(fulfill_links))

        eq_(Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
            fulfill_links[0]['type'])
        eq_("http://streaming_link", fulfill_links[0]['href'])


    def test_drm_device_registration_feed_tags(self):
        """Check that drm_device_registration_feed_tags returns
        a generic drm:licensor tag, except with the drm:scheme attribute
        set.
        """
        self.initialize_adobe(self._default_library)
        annotator = LibraryLoanAndHoldAnnotator(None, None, self._default_library, test_mode=True)
        patron = self._patron()
        [feed_tag] = annotator.drm_device_registration_feed_tags(patron)
        [generic_tag] = annotator.adobe_id_tags(patron)

        # The feed-level tag has the drm:scheme attribute set.
        key = '{http://librarysimplified.org/terms/drm}scheme'
        eq_("http://librarysimplified.org/terms/drm/scheme/ACS",
            feed_tag.attrib[key])

        # If we remove that attribute, the feed-level tag is the same as the
        # generic tag.
        del feed_tag.attrib[key]
        eq_(etree.tostring(feed_tag), etree.tostring(generic_tag))

    def test_borrow_link_raises_unfulfillable_work(self):
        edition, pool = self._edition(with_license_pool=True)
        kindle_mechanism = pool.set_delivery_mechanism(
            DeliveryMechanism.KINDLE_CONTENT_TYPE, DeliveryMechanism.KINDLE_DRM,
            RightsStatus.IN_COPYRIGHT, None)
        epub_mechanism = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT, None)
        data_source_name = pool.data_source.name
        identifier = pool.identifier

        annotator = LibraryLoanAndHoldAnnotator(None, None, self._default_library, test_mode=True)

        # If there's no way to fulfill the book, borrow_link raises
        # UnfulfillableWork.
        assert_raises(
            UnfulfillableWork,
            annotator.borrow_link,
            pool, None, [])

        assert_raises(
            UnfulfillableWork,
            annotator.borrow_link,
            pool, None, [kindle_mechanism])

        # If there's a fulfillable mechanism, everything's fine.
        link = annotator.borrow_link(pool, None, [epub_mechanism])
        assert link != None

        link = annotator.borrow_link(
            pool, None, [epub_mechanism, kindle_mechanism]
        )
        assert link != None

    def test_feed_includes_lane_links(self):
        def annotated_links(lane, annotator):
            # Create an AcquisitionFeed is using the given Annotator.
            # extract its links and return a dictionary that maps link
            # relations to URLs.
            feed = AcquisitionFeed(self._db, "test", "url", [], annotator)
            annotator.annotate_feed(feed, lane)
            raw = unicode(feed)
            parsed = feedparser.parse(raw)['feed']
            links = parsed['links']

            d = defaultdict(list)
            for link in links:
                d[link['rel'].lower()].append(link['href'])
            return d

        # When an EntryPoint is explicitly selected, it shows up in the
        # link to the search controller.
        facets = FacetsWithEntryPoint(entrypoint=AudiobooksEntryPoint)
        lane = self._lane()
        annotator = LibraryAnnotator(
            None, lane, self._default_library, test_mode=True, facets=facets
        )
        [url] = annotated_links(lane, annotator)['search']
        assert '/lane_search' in url
        assert 'entrypoint=%s' % AudiobooksEntryPoint.INTERNAL_NAME in url
        assert str(lane.id) in url

        # When the selected EntryPoint is a default, it's not used --
        # instead, we search everything.
        annotator.facets.entrypoint_is_default = True
        links = annotated_links(lane, annotator)
        [url] = links['search']
        assert 'entrypoint=%s' % EverythingEntryPoint.INTERNAL_NAME in url

        # This lane isn't based on a custom list, so there's no crawlable link.
        eq_([], links["http://opds-spec.org/crawlable"])

        # It's also not crawlable if it's based on multiple lists.
        list1, ignore = self._customlist()
        list2, ignore = self._customlist()
        lane.customlists = [list1, list2]
        links = annotated_links(lane, annotator)
        eq_([], links["http://opds-spec.org/crawlable"])

        # A lane based on a single list gets a crawlable link.
        lane.customlists = [list1]
        links = annotated_links(lane, annotator)
        [crawlable] = links['http://opds-spec.org/crawlable']
        assert '/crawlable_list_feed' in crawlable
        assert str(list1.name) in crawlable

    def test_acquisition_links(self):
        annotator = LibraryLoanAndHoldAnnotator(None, None, self._default_library, test_mode=True)
        feed = AcquisitionFeed(
            self._db, "test", "url", [], annotator
        )

        patron = self._patron()

        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)

        # Loan of an open-access book.
        work1 = self._work(with_open_access_download=True)
        loan1, ignore = work1.license_pools[0].loan_to(patron, start=now)

        # Loan of a licensed book.
        work2 = self._work(with_license_pool=True)
        loan2, ignore = work2.license_pools[0].loan_to(patron, start=now, end=tomorrow)

        # Hold on a licensed book.
        work3 = self._work(with_license_pool=True)
        hold, ignore = work3.license_pools[0].on_hold_to(patron, start=now, end=tomorrow)

        # Book with no loans or holds yet.
        work4 = self._work(with_license_pool=True)

        loan1_links = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, feed, loan1.license_pool.identifier)
        # Fulfill, open access, and revoke.
        [revoke, fulfill, open_access] = sorted(loan1_links, key=lambda x: x.attrib.get("rel"))
        assert 'revoke_loan_or_hold' in revoke.attrib.get("href")
        eq_('http://librarysimplified.org/terms/rel/revoke', revoke.attrib.get("rel"))
        assert "fulfill" in fulfill.attrib.get("href")
        eq_('http://opds-spec.org/acquisition', fulfill.attrib.get("rel"))
        assert 'fulfill' in open_access.attrib.get("href")
        eq_('http://opds-spec.org/acquisition/open-access', open_access.attrib.get("rel"))

        loan2_links = annotator.acquisition_links(
            loan2.license_pool, loan2, None, None, feed,
            loan2.license_pool.identifier
        )
        # Fulfill and revoke.
        [revoke, fulfill] = sorted(loan2_links, key=lambda x: x.attrib.get("rel"))
        assert 'revoke_loan_or_hold' in revoke.attrib.get("href")
        eq_('http://librarysimplified.org/terms/rel/revoke', revoke.attrib.get("rel"))
        assert "fulfill" in fulfill.attrib.get("href")
        eq_('http://opds-spec.org/acquisition', fulfill.attrib.get("rel"))

        # If a book is ready to be fulfilled, but the library has
        # hidden all of its available content types, the fulfill link does
        # not show up -- only the revoke link.
        hidden = self._default_library.setting(
            Configuration.HIDDEN_CONTENT_TYPES
        )
        available_types = [
            lpdm.delivery_mechanism.content_type
            for lpdm in loan2.license_pool.delivery_mechanisms
        ]
        hidden.value = json.dumps(available_types)

        # The list of hidden content types is stored in the Annotator
        # constructor, so this particular test needs a fresh Annotator.
        annotator_with_hidden_types = LibraryLoanAndHoldAnnotator(
            None, None, self._default_library, test_mode=True
        )
        loan2_links = annotator_with_hidden_types.acquisition_links(
            loan2.license_pool, loan2, None, None, feed,
            loan2.license_pool.identifier
        )
        [revoke] = loan2_links
        eq_('http://librarysimplified.org/terms/rel/revoke',
            revoke.attrib.get("rel"))
        # Un-hide the content types so the test can continue.
        hidden.value = None

        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, feed, hold.license_pool.identifier)
        # Borrow and revoke.
        [revoke, borrow] = sorted(hold_links, key=lambda x: x.attrib.get("rel"))
        assert 'revoke_loan_or_hold' in revoke.attrib.get("href")
        eq_('http://librarysimplified.org/terms/rel/revoke', revoke.attrib.get("rel"))
        assert "borrow" in borrow.attrib.get("href")
        eq_('http://opds-spec.org/acquisition/borrow', borrow.attrib.get("rel"))

        work4_links = annotator.acquisition_links(
            work4.license_pools[0], None, None, None, feed, work4.license_pools[0].identifier)
        # Borrow only.
        [borrow] = work4_links
        assert "borrow" in borrow.attrib.get("href")
        eq_('http://opds-spec.org/acquisition/borrow', borrow.attrib.get("rel"))

        # If patron authentication is turned off for the library, then
        # only open-access links are displayed.
        annotator.identifies_patrons = False

        [open_access] = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, feed, loan1.license_pool.identifier)
        eq_('http://opds-spec.org/acquisition/open-access', open_access.attrib.get("rel"))

        # This may include links with the open-access relation for
        # non-open-access works that are available without
        # authentication.  To get such link, you pass in a list of
        # LicensePoolDeliveryMechanisms as
        # `direct_fufillment_delivery_mechanisms`.
        [lp4] = work4.license_pools
        [lpdm4] = lp4.delivery_mechanisms
        lpdm4.set_rights_status(RightsStatus.IN_COPYRIGHT)
        [not_open_access] = annotator.acquisition_links(
            lp4, None, None, None, feed, lp4.identifier,
            direct_fulfillment_delivery_mechanisms=[lpdm4]
        )

        # The link relation is OPDS 'open-access', which just means the
        # book can be downloaded with no hassle.
        eq_('http://opds-spec.org/acquisition/open-access', not_open_access.attrib.get("rel"))

        # The dcterms:rights attribute provides a more detailed
        # explanation of the book's copyright status -- note that it's
        # not "open access" in the typical sense.
        rights = not_open_access.attrib['{http://purl.org/dc/terms/}rights']
        eq_(RightsStatus.IN_COPYRIGHT, rights)

        # Hold links are absent even when there are active holds in the
        # database -- there is no way to distinguish one patron from
        # another so the concept of a 'hold' is meaningless.
        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, feed, hold.license_pool.identifier)
        eq_([], hold_links)

    def test_acquisition_links_multiple_links(self):
        annotator = LibraryLoanAndHoldAnnotator(None, None, self._default_library, test_mode=True)
        feed = AcquisitionFeed(
            self._db, "test", "url", [], annotator
        )

        # This book has two delivery mechanisms
        work = self._work(with_license_pool=True)
        [pool] = work.license_pools
        [mech1] = pool.delivery_mechanisms
        mech2 = pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.IN_COPYRIGHT, None
        )

        # The vendor API for LicensePools of this type requires that a
        # delivery mechanism be chosen at the point of borrowing.
        class MockAPI(object):
            SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

        # This means that two different acquisition links will be
        # generated -- one for each delivery mechanism.
        links = annotator.acquisition_links(
            pool, None, None, None, feed, pool.identifier,
            mock_api=MockAPI()
        )
        eq_(2, len(links))
        indirects = []

        # This sorts the links so that the first link corresponds
        # to the delivery mechanism for the first link.
        links = sorted(links, key=lambda x: x.attrib['href'], reverse=True)
        for link in links:
            # Both links should have the same subtags.
            [availability, copies, holds, indirect] = sorted(
                link, key=lambda x: x.tag
            )
            assert availability.tag.endswith('availability')
            assert copies.tag.endswith('copies')
            assert holds.tag.endswith('holds')
            assert indirect.tag.endswith('indirectAcquisition')
            indirects.append(indirect)

        # The target of the top-level link is different.
        [href1, href2] = [x.attrib['href'] for x in links]
        assert "mechanism_id=%s" % mech1.delivery_mechanism.id in href1
        assert "mechanism_id=%s" % mech2.delivery_mechanism.id in href2

        # So is the media type seen in the indirectAcquisition subtag.
        [i1, i2] = indirects

        # The first delivery mechanism (created when the Work was created)
        # uses Adobe DRM, so that shows up as the first indirect acquisition
        # type.
        eq_(mech1.delivery_mechanism.drm_scheme, i1.attrib['type'])

        # The second delivery mechanism doesn't use DRM, so the content
        # type shows up as the first (and only) indirect acquisition type.
        eq_(mech2.delivery_mechanism.content_type, i2.attrib['type'])

        # If we configure the library to hide one of the content types,
        # we end up with only one link -- the one for the delivery
        # mechanism that's not hidden.
        self._default_library.setting(
            Configuration.HIDDEN_CONTENT_TYPES
        ).value = json.dumps([mech1.delivery_mechanism.content_type])
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, self._default_library, test_mode=True
        )
        [link] = annotator.acquisition_links(
            pool, None, None, None, feed, pool.identifier,
            mock_api=MockAPI()
        )
        [availability, copies, holds, indirect] = sorted(
            link, key=lambda x: x.tag
        )
        eq_(mech2.delivery_mechanism.content_type, indirect.attrib['type'])


class TestSharedCollectionAnnotator(DatabaseTest):
    def setup(self):
        super(TestSharedCollectionAnnotator, self).setup()
        self.work = self._work(with_open_access_download=True)
        self.collection = self._collection()
        self.lane = self._lane(display_name="Fantasy")
        self.annotator = SharedCollectionAnnotator(
            self.collection, self.lane, test_mode=True,
        )

    def test_top_level_title(self):
        eq_(self.collection.name, self.annotator.top_level_title())

    def test_feed_url(self):
        feed_url_fantasy = self.annotator.feed_url(self.lane, dict(), dict())
        assert "feed" in feed_url_fantasy
        assert str(self.lane.id) in feed_url_fantasy
        assert self.collection.name in feed_url_fantasy

    def get_parsed_feed(self, works, lane=None):
        if not lane:
            lane = self._lane(display_name="Main Lane")
        feed = AcquisitionFeed(
            self._db, "test", "url", works,
            SharedCollectionAnnotator(self.collection, lane, test_mode=True)
        )
        return feedparser.parse(unicode(feed))

    def assert_link_on_entry(self, entry, link_type=None, rels=None,
                             partials_by_rel=None
    ):
        """Asserts that a link with a certain 'rel' value exists on a
        given feed or entry, as well as its link 'type' value and parts
        of its 'href' value.
        """
        def get_link_by_rel(rel, should_exist=True):
            try:
                [link] = [x for x in entry['links'] if x['rel']==rel]
            except ValueError as e:
                raise AssertionError
            if link_type:
                eq_(link_type, link.type)
            return link

        if rels:
            [get_link_by_rel(rel) for rel in rels]

        partials_by_rel = partials_by_rel or dict()
        for rel, uri_partials in partials_by_rel.items():
            link = get_link_by_rel(rel)
            if not isinstance(uri_partials, list):
                uri_partials = [uri_partials]
            for part in uri_partials:
                assert part in link.href

    def test_work_entry_includes_updated(self):
        work = self._work(with_open_access_download=True)
        work.license_pools[0].availability_time = datetime.datetime(2018, 1, 1, 0, 0, 0)
        work.last_update_time = datetime.datetime(2018, 2, 4, 0, 0, 0)

        feed = self.get_parsed_feed([work])
        [entry] = feed.entries
        assert '2018-02-04' in entry.get("updated")

    def test_work_entry_includes_open_access_or_borrow_link(self):
        open_access_work = self._work(with_open_access_download=True)
        licensed_work = self._work(with_license_pool=True)
        licensed_work.license_pools[0].open_access = False

        feed = self.get_parsed_feed([open_access_work, licensed_work])
        [open_access_entry, licensed_entry] = feed.entries

        self.assert_link_on_entry(open_access_entry, rels=[Hyperlink.OPEN_ACCESS_DOWNLOAD])
        self.assert_link_on_entry(licensed_entry, rels=[OPDSFeed.BORROW_REL])

        # The open access entry shouldn't have a borrow link, and the licensed entry
        # shouldn't have an open access link.
        links = [x for x in open_access_entry['links'] if x['rel']==OPDSFeed.BORROW_REL]
        eq_(0, len(links))
        links = [x for x in licensed_entry['links'] if x['rel']==Hyperlink.OPEN_ACCESS_DOWNLOAD]
        eq_(0, len(links))

    def test_borrow_link_raises_unfulfillable_work(self):
        edition, pool = self._edition(with_license_pool=True)
        kindle_mechanism = pool.set_delivery_mechanism(
            DeliveryMechanism.KINDLE_CONTENT_TYPE, DeliveryMechanism.KINDLE_DRM,
            RightsStatus.IN_COPYRIGHT, None)
        epub_mechanism = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT, None)
        data_source_name = pool.data_source.name
        identifier = pool.identifier

        annotator = SharedCollectionLoanAndHoldAnnotator(self.collection, None, test_mode=True)

        # If there's no way to fulfill the book, borrow_link raises
        # UnfulfillableWork.
        assert_raises(
            UnfulfillableWork,
            annotator.borrow_link,
            pool, None, [])

        assert_raises(
            UnfulfillableWork,
            annotator.borrow_link,
            pool, None, [kindle_mechanism])

        # If there's a fulfillable mechanism, everything's fine.
        link = annotator.borrow_link(pool, None, [epub_mechanism])
        assert link != None

        link = annotator.borrow_link(
            pool, None, [epub_mechanism, kindle_mechanism]
        )
        assert link != None

    def test_acquisition_links(self):
        annotator = SharedCollectionLoanAndHoldAnnotator(self.collection, None, test_mode=True)
        feed = AcquisitionFeed(
            self._db, "test", "url", [], annotator
        )

        client = self._integration_client()

        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)

        # Loan of an open-access book.
        work1 = self._work(with_open_access_download=True)
        loan1, ignore = work1.license_pools[0].loan_to(client, start=now)

        # Loan of a licensed book.
        work2 = self._work(with_license_pool=True)
        loan2, ignore = work2.license_pools[0].loan_to(client, start=now, end=tomorrow)

        # Hold on a licensed book.
        work3 = self._work(with_license_pool=True)
        hold, ignore = work3.license_pools[0].on_hold_to(client, start=now, end=tomorrow)

        # Book with no loans or holds yet.
        work4 = self._work(with_license_pool=True)

        loan1_links = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, feed, loan1.license_pool.identifier)
        # Fulfill, open access, revoke, and loan info.
        [revoke, fulfill, open_access, info] = sorted(loan1_links, key=lambda x: x.attrib.get("rel"))
        assert 'shared_collection_revoke_loan' in revoke.attrib.get("href")
        eq_('http://librarysimplified.org/terms/rel/revoke', revoke.attrib.get("rel"))
        assert "shared_collection_fulfill" in fulfill.attrib.get("href")
        eq_('http://opds-spec.org/acquisition', fulfill.attrib.get("rel"))
        eq_(work1.license_pools[0].delivery_mechanisms[0].resource.representation.mirror_url, open_access.attrib.get("href"))
        eq_('http://opds-spec.org/acquisition/open-access', open_access.attrib.get("rel"))
        assert 'shared_collection_loan_info' in info.attrib.get("href")
        eq_("self", info.attrib.get("rel"))

        loan2_links = annotator.acquisition_links(
            loan2.license_pool, loan2, None, None, feed, loan2.license_pool.identifier)
        # Fulfill, revoke, and loan info.
        [revoke, fulfill, info] = sorted(loan2_links, key=lambda x: x.attrib.get("rel"))
        assert 'shared_collection_revoke_loan' in revoke.attrib.get("href")
        eq_('http://librarysimplified.org/terms/rel/revoke', revoke.attrib.get("rel"))
        assert "shared_collection_fulfill" in fulfill.attrib.get("href")
        eq_('http://opds-spec.org/acquisition', fulfill.attrib.get("rel"))
        assert 'shared_collection_loan_info' in info.attrib.get("href")
        eq_("self", info.attrib.get("rel"))

        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, feed, hold.license_pool.identifier)
        # Borrow, revoke, and hold info.
        [revoke, borrow, info] = sorted(hold_links, key=lambda x: x.attrib.get("rel"))
        assert 'shared_collection_revoke_hold' in revoke.attrib.get("href")
        eq_('http://librarysimplified.org/terms/rel/revoke', revoke.attrib.get("rel"))
        assert "shared_collection_borrow" in borrow.attrib.get("href")
        eq_('http://opds-spec.org/acquisition/borrow', borrow.attrib.get("rel"))
        assert 'shared_collection_hold_info' in info.attrib.get("href")
        eq_("self", info.attrib.get("rel"))

        work4_links = annotator.acquisition_links(
            work4.license_pools[0], None, None, None, feed, work4.license_pools[0].identifier)
        # Borrow only.
        [borrow] = work4_links
        assert "shared_collection_borrow" in borrow.attrib.get("href")
        eq_('http://opds-spec.org/acquisition/borrow', borrow.attrib.get("rel"))
