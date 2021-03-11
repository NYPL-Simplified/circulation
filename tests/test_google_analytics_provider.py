from nose.tools import (
    eq_,
    set_trace,
    assert_raises_regexp,
)
from api.config import (
    CannotLoadConfiguration,
)
from core.analytics import Analytics
from api.google_analytics_provider import GoogleAnalyticsProvider
from core.testing import DatabaseTest
from core.model import (
    get_one_or_create,
    create,
    CirculationEvent,
    ConfigurationSetting,
    DataSource,
    EditionConstants,
    ExternalIntegration,
    LicensePool
)
import unicodedata
import urlparse
import datetime
from psycopg2.extras import NumericRange

class MockGoogleAnalyticsProvider(GoogleAnalyticsProvider):

    def post(self, url, params):
        self.count = self.count + 1 if hasattr(self, "count") else 1
        self.url = url
        self.params = params

class TestGoogleAnalyticsProvider(DatabaseTest):

    def test_init(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Google Analytics can't be configured without a library.",
            GoogleAnalyticsProvider, integration
        )

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Missing tracking id for library %s" % self._default_library.short_name,
            GoogleAnalyticsProvider, integration, self._default_library
        )

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, GoogleAnalyticsProvider.TRACKING_ID, self._default_library, integration
        ).value = "faketrackingid"
        ga = GoogleAnalyticsProvider(integration, self._default_library)
        eq_(GoogleAnalyticsProvider.DEFAULT_URL, ga.url)
        eq_("faketrackingid", ga.tracking_id)

        integration.url = self._str
        ga = GoogleAnalyticsProvider(integration, self._default_library)
        eq_(integration.url, ga.url)
        eq_("faketrackingid", ga.tracking_id)

    def test_collect_event_with_work(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )
        integration.url = self._str
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, GoogleAnalyticsProvider.TRACKING_ID, self._default_library, integration
        ).value = "faketrackingid"
        ga = MockGoogleAnalyticsProvider(integration, self._default_library)

        work = self._work(
            title=u"pi\u00F1ata", authors=u"chlo\u00E9", fiction=True,
            audience="audience", language="lang",
            with_license_pool=True, genre="Folklore",
            with_open_access_download=True
        )
        work.presentation_edition.publisher = "publisher"
        work.target_age = NumericRange(10, 15)
        [lp] = work.license_pools
        now = datetime.datetime.utcnow()
        ga.collect_event(
            self._default_library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, now,
            neighborhood="Neighborhood will not be sent"
        )

        # Neighborhood information is not being sent -- that's for
        # local consumption only.
        assert 'Neighborhood' not in ga.params

        # Let's take a look at what _is_ being sent.
        params = urlparse.parse_qs(ga.params)

        eq_(1, ga.count)
        eq_(integration.url, ga.url)
        eq_("faketrackingid", params['tid'][0])
        eq_("event", params['t'][0])
        eq_("circulation", params['ec'][0])
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, params['ea'][0])
        eq_(str(now), params['cd1'][0])
        eq_(lp.identifier.identifier, params['cd2'][0])
        eq_(lp.identifier.type, params['cd3'][0])
        eq_(unicodedata.normalize("NFKD", work.title).encode('utf8'),
            params['cd4'][0])
        eq_(unicodedata.normalize("NFKD", work.author).encode('utf8'),
            params['cd5'][0])
        eq_("fiction", params['cd6'][0])
        eq_("audience", params['cd7'][0])
        eq_(work.target_age_string, params['cd8'][0])
        eq_("publisher", params['cd9'][0])
        eq_("lang", params['cd10'][0])
        eq_("Folklore", params['cd11'][0])
        eq_("true", params['cd12'][0])
        eq_(DataSource.GUTENBERG, params['cd13'][0])
        eq_(EditionConstants.BOOK_MEDIUM, params['cd14'][0])
        eq_(self._default_library.short_name, params['cd15'][0])

    def test_collect_event_without_work(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )
        integration.url = self._str
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, GoogleAnalyticsProvider.TRACKING_ID, self._default_library, integration
        ).value = "faketrackingid"
        ga = MockGoogleAnalyticsProvider(integration, self._default_library)

        identifier = self._identifier()
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        pool, is_new = get_one_or_create(
            self._db, LicensePool,
            identifier=identifier, data_source=source,
            collection=self._default_collection
        )

        now = datetime.datetime.utcnow()
        ga.collect_event(self._default_library, pool, CirculationEvent.DISTRIBUTOR_CHECKIN, now)
        params = urlparse.parse_qs(ga.params)

        eq_(1, ga.count)
        eq_(integration.url, ga.url)
        eq_("faketrackingid", params['tid'][0])
        eq_("event", params['t'][0])
        eq_("circulation", params['ec'][0])
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, params['ea'][0])
        eq_(str(now), params['cd1'][0])
        eq_(pool.identifier.identifier, params['cd2'][0])
        eq_(pool.identifier.type, params['cd3'][0])
        eq_(None, params.get('cd4'))
        eq_(None, params.get('cd5'))
        eq_(None, params.get('cd6'))
        eq_(None, params.get('cd7'))
        eq_(None, params.get('cd8'))
        eq_(None, params.get('cd9'))
        eq_(None, params.get('cd10'))
        eq_(None, params.get('cd11'))
        eq_(None, params.get('cd12'))
        eq_([source.name], params.get('cd13'))
        eq_(None, params.get('cd14'))
        eq_([self._default_library.short_name], params.get('cd15'))

    def test_collect_event_without_license_pool(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )
        integration.url = self._str
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, GoogleAnalyticsProvider.TRACKING_ID, self._default_library, integration
        ).value = "faketrackingid"
        ga = MockGoogleAnalyticsProvider(integration, self._default_library)

        now = datetime.datetime.utcnow()
        ga.collect_event(self._default_library, None, CirculationEvent.NEW_PATRON, now)
        params = urlparse.parse_qs(ga.params)

        eq_(1, ga.count)
        eq_(integration.url, ga.url)
        eq_("faketrackingid", params['tid'][0])
        eq_("event", params['t'][0])
        eq_("circulation", params['ec'][0])
        eq_(CirculationEvent.NEW_PATRON, params['ea'][0])
        eq_(str(now), params['cd1'][0])
        eq_(None, params.get('cd2'))
        eq_(None, params.get('cd3'))
        eq_(None, params.get('cd4'))
        eq_(None, params.get('cd5'))
        eq_(None, params.get('cd6'))
        eq_(None, params.get('cd7'))
        eq_(None, params.get('cd8'))
        eq_(None, params.get('cd9'))
        eq_(None, params.get('cd10'))
        eq_(None, params.get('cd11'))
        eq_(None, params.get('cd12'))
        eq_(None, params.get('cd13'))
        eq_(None, params.get('cd14'))
        eq_([self._default_library.short_name], params.get('cd15'))
