import base64
import contextlib
import datetime
import os
import re
from lxml import etree
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)
import feedparser
from . import DatabaseTest

from core.lane import (
    LaneList,
    Lane,
)
from core.model import (
    DataSource,
    Library,
    Work,
    Representation,
    DeliveryMechanism,
    RightsStatus,
)

from core.classifier import (
    Classifier,
    Fantasy,
    Urban_Fantasy
)

from core.util.opds_writer import (
    AtomFeed, 
    OPDSFeed,
)

from core.opds_import import (
    OPDSXMLParser
)

from api.circulation import (
    CirculationAPI,
    FulfillmentInfo,
)
from api.config import (
    Configuration, 
    temp_config,
)

from api.opds import (
    CirculationManagerAnnotator,
    CirculationManagerLoanAndHoldAnnotator,
)
from core.opds import (
    AcquisitionFeed,
    UnfulfillableWork,
)
from api.adobe_vendor_id import AuthdataUtility

from core.util.cdn import cdnify
from api.novelist import NoveListAPI
from api.lanes import ContributorLane
import jwt

_strftime = AtomFeed._strftime


class WithVendorIDTest(DatabaseTest):

    @contextlib.contextmanager
    def temp_config(self):
        """Configure a basic Vendor ID Service setup."""
        with temp_config() as config:
            library_uri = "http://a-library/"
            secret = "a-secret"
            vendor_id = "Some Vendor"
            short_name = "a library"
            config[Configuration.INTEGRATIONS][Configuration.ADOBE_VENDOR_ID_INTEGRATION] = {
                Configuration.ADOBE_VENDOR_ID : vendor_id,
                AuthdataUtility.LIBRARY_URI_KEY : library_uri,
            }
            library = Library.instance(self._db)
            library.library_registry_short_name = short_name
            library.library_registry_shared_secret = secret

            yield config

    
class TestCirculationManagerAnnotator(WithVendorIDTest):

    def setup(self):
        super(TestCirculationManagerAnnotator, self).setup()
        self.work = self._work(with_open_access_download=True)
        self.annotator = CirculationManagerAnnotator(
            None, Fantasy, test_mode=True, top_level_title="Test Top Level Title"
        )
            
    def test_add_configuration_links(self):
        mock_feed = []
        link_config = {
            Configuration.TERMS_OF_SERVICE: "http://terms/",
            Configuration.PRIVACY_POLICY: "http://privacy/",
            Configuration.COPYRIGHT: "http://copyright/",
            Configuration.ABOUT: "http://about/",
            Configuration.LICENSE: "http://license/",
        }
        with temp_config() as config:
            config['links'] = link_config
            CirculationManagerAnnotator.add_configuration_links(mock_feed)

        # Five links were added to the "feed"
        eq_(5, len(mock_feed))

        # They are the links we'd expect.
        links = {}
        for link in mock_feed:
            rel = link.attrib['rel']
            href = link.attrib['href']
            type = link.attrib['type']

            eq_("text/html", type)

            # Convert the link relation into a key to the configuration.
            config_value = rel.replace('-', '_')

            # Check that the configuration value made it into the link.
            eq_(href, link_config[config_value])
            
    def test_open_access_link(self):

        # The resource URL associated with a LicensePoolDeliveryMechanism
        # becomes the `href` of an open-access `link` tag.
        [lpdm] = self.work.license_pools[0].delivery_mechanisms
        lpdm.resource.url = "http://foo.com/thefile.epub"
        link_tag = self.annotator.open_access_link(lpdm)
        eq_(lpdm.resource.url, link_tag.get('href'))

        # If we have a CDN set up for open-access links, the CDN hostname
        # replaces the original hostname.
        with temp_config() as config:
            cdn_host = "https://cdn.com/"
            cdns = {
                "foo.com" : cdn_host
            }
            config[Configuration.INTEGRATIONS] = {
                Configuration.CDN_INTEGRATION : cdns
            }
            link_tag = self.annotator.open_access_link(lpdm)
            link_url = link_tag.get('href')
            eq_("https://cdn.com/thefile.epub", link_url)

    def test_top_level_title(self):
        eq_("Test Top Level Title", self.annotator.top_level_title())

    def test_group_uri_with_flattened_lane(self):
        spanish_lane = Lane(
            self._db, "Spanish", languages="spa"
        )
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
        everything_lane = Lane(
            self._db, "Everything", fiction=Lane.BOTH_FICTION_AND_NONFICTION)

        fantasy_lane_with_sublanes = Lane(
            self._db, "Fantasy", genres=[Fantasy], languages="eng", 
            subgenre_behavior=Lane.IN_SAME_LANE,
            sublanes=[Urban_Fantasy],
            parent=everything_lane)

        fantasy_lane_without_sublanes = Lane(
            self._db, "Fantasy", genres=[Fantasy], languages="eng", 
            subgenre_behavior=Lane.IN_SAME_LANE,
            parent=everything_lane)

        default_lane_url = self.annotator.lane_url(everything_lane)
        eq_(default_lane_url, self.annotator.default_lane_url())

        groups_url = self.annotator.lane_url(fantasy_lane_with_sublanes)
        eq_(groups_url, self.annotator.groups_url(fantasy_lane_with_sublanes))

        feed_url = self.annotator.lane_url(fantasy_lane_without_sublanes)
        eq_(feed_url, self.annotator.feed_url(fantasy_lane_without_sublanes))

    def test_single_entry_no_active_license_pool(self):
        work = self._work(with_open_access_download=True)
        pool = work.license_pools[0]

        # Create an <entry> tag for this work and its LicensePool.
        feed1 = AcquisitionFeed.single_entry(
            self._db, work, self.annotator, pool
        )

        # If we don't pass in the license pool, it makes a guess to
        # figure out which license pool we're talking about.
        feed2 = AcquisitionFeed.single_entry(
            self._db, work, self.annotator, None
        )

        # Both entries are identical.
        eq_(etree.tostring(feed1), etree.tostring(feed2))

    def test_fulfill_link_includes_device_registration_tags(self):
        """Verify that when Adobe Vendor ID delegation is included, the
        fulfill link for an Adobe delivery mechanism includes instructions
        on how to get a Vendor ID.
        """
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

        with self.temp_config() as config:
            # The fulfill link for non-Adobe DRM does not
            # include the drm:licensor tag.
            link = self.annotator.fulfill_link(
                pool.data_source.name, pool.identifier, pool, loan,
                other_delivery_mechanism 
           )
            for child in link.getchildren():
                assert child.tag != "{http://librarysimplified.org/terms/drm}licensor"

            # No new Credential has been associated with the patron.
            eq_(old_credentials, patron.credentials)
                
            # The fulfill link for Adobe DRM includes information
            # on how to get an Adobe ID in the drm:licensor tag.
            link = self.annotator.fulfill_link(
                pool.data_source.name, pool.identifier, pool, loan,
                adobe_delivery_mechanism
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
            [expect] = self.annotator.adobe_id_tags(
                self._db, adobe_id_identifier.credential
            )
            eq_(etree.tostring(expect), etree.tostring(licensor))
            
    def test_no_adobe_id_tags_when_vendor_id_not_configured(self):

        with temp_config() as config:
            """When vendor ID delegation is not configured, adobe_id_tags()
            returns an empty list.
            """
            config[Configuration.INTEGRATIONS][Configuration.ADOBE_VENDOR_ID_INTEGRATION] = {}
            eq_([], self.annotator.adobe_id_tags(
                self._db, "patron identifier")
            )

    def test_adobe_id_tags_when_vendor_id_configured(self):
        """When vendor ID delegation is configured, adobe_id_tags()
        returns a list containing a single tag. The tag contains
        the information necessary to get an Adobe ID and a link to the local
        DRM Device Management Protocol endpoint.
        """
        with self.temp_config() as config:
            patron_identifier = "patron identifier"
            [element] = self.annotator.adobe_id_tags(
                self._db, patron_identifier
            )
            eq_('{http://librarysimplified.org/terms/drm}licensor', element.tag)

            key = '{http://librarysimplified.org/terms/drm}vendor'
            eq_("Some Vendor", element.attrib[key])
            
            [token, device_management_link] = element.getchildren()
            
            eq_('{http://librarysimplified.org/terms/drm}clientToken', token.tag)
            # token.text is a token which we can decode, since we know
            # the secret.
            token = token.text
            authdata = AuthdataUtility.from_config(self._db)
            decoded = authdata.decode_short_client_token(token)
            eq_(("http://a-library/", patron_identifier), decoded)

            eq_("link", device_management_link.tag)
            eq_("http://librarysimplified.org/terms/drm/rel/devices",
                device_management_link.attrib['rel'])
            expect_url = self.annotator.url_for(
                'adobe_drm_devices', _external=True
            )
            eq_(expect_url, device_management_link.attrib['href'])
            
            # If we call adobe_id_tags again we'll get a distinct tag
            # object that renders to the same XML.
            [same_tag] = self.annotator.adobe_id_tags(
                self._db, patron_identifier
            )
            assert same_tag is not element
            eq_(etree.tostring(element), etree.tostring(same_tag))

            
class TestOPDS(WithVendorIDTest):

    def setup(self):
        super(TestOPDS, self).setup()
        parent = Lane(self._db, "Fiction", languages=["eng"], fiction=True)
        fantasy_lane = Lane(self._db, "Fantasy", languages=["eng"], genres=[Fantasy], parent=parent)
        self.lane = fantasy_lane

        # A QueryGeneratedLane to test code that handles it differently.
        self.contributor_lane = ContributorLane(self._db, "Someone", languages=["eng"], audiences=None)

    def test_default_lane_url(self):
        annotator = CirculationManagerAnnotator(None, self.lane, test_mode=True)

        default_lane_url = annotator.default_lane_url()

        assert "groups" in default_lane_url
        assert "Fantasy" not in default_lane_url

    def test_groups_url(self):
        annotator = CirculationManagerAnnotator(None, self.lane, test_mode=True)

        groups_url_no_lane = annotator.groups_url(None)

        assert "groups" in groups_url_no_lane
        assert "Fantasy" not in groups_url_no_lane

        groups_url_fantasy = annotator.groups_url(self.lane)
        assert "groups" in groups_url_fantasy
        assert "Fantasy" in groups_url_fantasy

    def test_feed_url(self):
        # A regular Lane.
        annotator = CirculationManagerAnnotator(None, self.lane, test_mode=True)

        feed_url_fantasy = annotator.feed_url(self.lane, dict(), dict())
        assert "feed" in feed_url_fantasy
        assert "Fantasy" in feed_url_fantasy

        # A QueryGeneratedLane.
        annotator = CirculationManagerAnnotator(None, self.contributor_lane, test_mode=True)

        feed_url_contributor = annotator.feed_url(self.contributor_lane, dict(), dict())
        assert self.contributor_lane.ROUTE in feed_url_contributor
        assert self.contributor_lane.contributor_name in feed_url_contributor

    def test_search_url(self):
        annotator = CirculationManagerAnnotator(None, self.lane, test_mode=True)

        search_url = annotator.search_url(self.lane, "query", dict())
        assert "search" in search_url
        assert "query" in search_url
        assert "Fantasy" in search_url

    def test_facet_url(self):
        # A regular Lane.
        facets = dict(collection="main")
        annotator = CirculationManagerAnnotator(None, self.lane, test_mode=True)

        facet_url = annotator.facet_url(facets)
        assert "collection=main" in facet_url
        assert "Fantasy" in facet_url

        # A QueryGeneratedLane.
        annotator = CirculationManagerAnnotator(None, self.contributor_lane, test_mode=True)

        facet_url_contributor = annotator.facet_url(facets)
        assert "collection=main" in facet_url_contributor
        assert self.contributor_lane.ROUTE in facet_url_contributor
        assert self.contributor_lane.contributor_name in facet_url_contributor


    def test_alternate_link_is_permalink(self):
        w1 = self._work(with_open_access_download=True)
        self._db.commit()

        works = self._db.query(Work)
        annotator = CirculationManagerAnnotator(None, Fantasy, test_mode=True)
        pool = annotator.active_licensepool_for(w1)

        feed = AcquisitionFeed(self._db, "test", "url", works, annotator)
        feed = feedparser.parse(unicode(feed))
        [entry] = feed['entries']
        eq_(entry['id'], pool.identifier.urn)


        [(alternate, type)] = [(x['href'], x['type']) for x in entry['links'] if x['rel'] == 'alternate']
        permalink = annotator.permalink_for(w1, pool, pool.identifier)
        eq_(alternate, permalink)
        eq_(OPDSFeed.ENTRY_TYPE, type)

        # Make sure we are using the 'permalink' controller -- we were using
        # 'work' and that was wrong.
        assert '/host/permalink' in permalink

    def test_acquisition_feed_includes_problem_reporting_link(self):
        w1 = self._work(with_open_access_download=True)
        self._db.commit()
        feed = AcquisitionFeed(
            self._db, "test", "url", [w1], CirculationManagerAnnotator(
                None, Fantasy, test_mode=True))
        feed = feedparser.parse(unicode(feed))
        [entry] = feed['entries']
        [issues_link] = [x for x in entry['links'] if x['rel'] == 'issues']
        assert '/report' in issues_link['href']

    def test_acquisition_feed_includes_open_access_or_borrow_link(self):
        w1 = self._work(with_open_access_download=True)
        w2 = self._work(with_open_access_download=True)
        w2.license_pools[0].open_access = False
        w2.license_pools[0].licenses_owned = 1
        self._db.commit()

        works = self._db.query(Work)
        feed = AcquisitionFeed(
            self._db, "test", "url", works, CirculationManagerAnnotator(
                None, Fantasy, test_mode=True))

        feed = feedparser.parse(unicode(feed))
        entries = sorted(feed['entries'], key = lambda x: int(x['title']))

        open_access_links, borrow_links = [x['links'] for x in entries]
        open_access_rels = [x['rel'] for x in open_access_links]
        assert OPDSFeed.BORROW_REL in open_access_rels

        borrow_rels = [x['rel'] for x in borrow_links]
        assert OPDSFeed.BORROW_REL in borrow_rels

    def test_acquisition_feed_includes_related_books_link(self):

        work = self._work(with_license_pool=True, with_open_access_download=True)
        def confirm_related_books_link():
            """Tests the presence of a /related_books link in a feed."""
            feed = AcquisitionFeed(
                self._db, "test", "url", [work],
                CirculationManagerAnnotator(None, Fantasy, test_mode=True)
            )
            feed = feedparser.parse(unicode(feed))
            [entry] = feed['entries']
            [recommendations_link] = [x for x in entry['links']
                                      if x['rel'] == 'related']
            eq_(OPDSFeed.ACQUISITION_FEED_TYPE, recommendations_link['type'])
            assert '/related_books' in recommendations_link['href']


        # If there is a contributor, there's a related books link.
        with temp_config() as config:
            NoveListAPI.IS_CONFIGURED = None
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}
            confirm_related_books_link()

        # If there is no possibility of related works,
        # there's no related books link.
        with temp_config() as config:
            # Remove contributors.
            self._db.delete(work.license_pools[0].presentation_edition.contributions[0])
            self._db.commit()

            # Turn off NoveList.
            NoveListAPI.IS_CONFIGURED = None
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}
            feed = AcquisitionFeed(
                self._db, "test", "url", [work],
                CirculationManagerAnnotator(None, Fantasy, test_mode=True)
            )
        feed = feedparser.parse(unicode(feed))
        [entry] = feed['entries']
        recommendations_links = [x for x in entry['links'] if x['rel'] == 'related']
        eq_([], recommendations_links)

        # If NoveList is configured (and thus recommendations are available),
        # there's is a related books link.
        with temp_config() as config:
            NoveListAPI.IS_CONFIGURED = None
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library",
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            confirm_related_books_link()

        # If the book is in a series, there's is a related books link.
        with temp_config() as config:
            NoveListAPI.IS_CONFIGURED = None
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}
            work.license_pools[0].presentation_edition.series = "Serious Cereal Series"
            confirm_related_books_link()

    def test_acquisition_feed_includes_annotations_link(self):
        w1 = self._work(with_open_access_download=True)
        self._db.commit()
        feed = AcquisitionFeed(
            self._db, "test", "url", [w1], CirculationManagerAnnotator(
                None, Fantasy, test_mode=True))
        feed = feedparser.parse(unicode(feed))
        [entry] = feed['entries']
        [annotations_link] = [x for x in entry['links'] if x['rel'] == 'http://www.w3.org/ns/oa#annotationservice']
        assert '/annotations' in annotations_link['href']
        identifier = w1.license_pools[0].identifier
        assert identifier.identifier in annotations_link['href']

    def test_active_loan_feed(self):
        with self.temp_config() as config:
            patron = self._patron()
            cls = CirculationManagerLoanAndHoldAnnotator
            raw = cls.active_loans_for(None, patron, test_mode=True)
            # No entries in the feed...
            raw = unicode(raw)
            feed = feedparser.parse(raw)
            eq_(0, len(feed['entries']))

            # ... but we do have DRM licensing information.
            tree = etree.fromstring(raw)
            parser = OPDSXMLParser()
            licensor = parser._xpath1(tree, "//atom:feed/drm:licensor")

            adobe_patron_identifier = cls._adobe_patron_identifier(
                patron
            )

            # The DRM licensing information includes the Adobe vendor ID
            # and the patron's patron identifier for Adobe purposes.
            eq_('Some Vendor',
                licensor.attrib['{http://librarysimplified.org/terms/drm}vendor'])
            [client_token, device_management_link] = licensor.getchildren()
            assert client_token.text.startswith('A LIBRARY')
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
        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
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
        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)
        feed_details = feedparser.parse(raw)['feed']

        assert "simplified:authorizationIdentifier" in raw
        assert "simplified:username" in raw
        eq_(patron.username, feed_details['simplified_patron']['simplified:username'])
        eq_(u'987654321', feed_details['simplified_patron']['simplified:authorizationidentifier'])

    def test_loans_feed_includes_preload_link(self):
        patron = self._patron()
        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True)
        raw = unicode(feed_obj)
        feed = feedparser.parse(raw)['feed']
        links = feed['links']

        [preload_link] = [x for x in links if x['rel'] == 'http://librarysimplified.org/terms/rel/preload']
        assert '/preload' in preload_link['href']
        
    def test_loans_feed_includes_annotations_link(self):
        patron = self._patron()
        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
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
        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
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
        self._db.commit()

        works = self._db.query(Work)
        feed = AcquisitionFeed(
            self._db, "test", "url", works,
            CirculationManagerAnnotator(None, Fantasy, test_mode=True)
        )
        u = unicode(feed)
        holds_re = re.compile('<opds:holds\W+total="25"\W*/>', re.S)
        assert holds_re.search(u) is not None
        
        copies_re = re.compile('<opds:copies[^>]+available="50"', re.S)
        assert copies_re.search(u) is not None

        copies_re = re.compile('<opds:copies[^>]+total="100"', re.S)
        assert copies_re.search(u) is not None

    def test_loans_feed_includes_fulfill_links_for_streaming(self):
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
        
        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
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

        # When the loan is fulfilled, there are only fulfill links for that mechanism
        # and the streaming mechanism.
        loan.fulfillment = mech1

        feed_obj = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
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
            pool.identifier.type, pool.identifier.identifier,
            "http://streaming_link",
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
            None, None)

        feed_obj = CirculationManagerLoanAndHoldAnnotator.single_fulfillment_feed(
            None, loan, fulfillment, test_mode=True)
        raw = etree.tostring(feed_obj)

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
        annotator = CirculationManagerLoanAndHoldAnnotator(None, None, test_mode=True)
        patron = self._patron()
        with self.temp_config() as config:
            [feed_tag] = annotator.drm_device_registration_feed_tags(patron)
            [generic_tag] = annotator.adobe_id_tags(self._db, patron)

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

        annotator = CirculationManagerLoanAndHoldAnnotator(None, None, test_mode=True)
        
        # If there's no way to fulfill the book, borrow_link raises
        # UnfulfillableWork.
        assert_raises(
            UnfulfillableWork,
            annotator.borrow_link,
            data_source_name, identifier,
            None, [])

        assert_raises(
            UnfulfillableWork,
            annotator.borrow_link,
            data_source_name, identifier,
            None, [kindle_mechanism])

        # If there's a fulfillable mechanism, everything's fine.
        link = annotator.borrow_link(
            data_source_name, identifier,
            None, [epub_mechanism])
        assert link != None

        link = annotator.borrow_link(
            data_source_name, identifier,
            None, [epub_mechanism, kindle_mechanism])
        assert link != None
