import pytest
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
import urllib.parse
import datetime
from psycopg2.extras import NumericRange
from core.util.datetime_helpers import utc_now

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

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            GoogleAnalyticsProvider(integration)
        assert "Google Analytics can't be configured without a library." in str(excinfo.value)

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            GoogleAnalyticsProvider(integration, self._default_library)
        assert "Missing tracking id for library %s" % self._default_library.short_name in str(excinfo.value)

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, GoogleAnalyticsProvider.TRACKING_ID, self._default_library, integration
        ).value = "faketrackingid"
        ga = GoogleAnalyticsProvider(integration, self._default_library)
        assert GoogleAnalyticsProvider.DEFAULT_URL == ga.url
        assert "faketrackingid" == ga.tracking_id

        integration.url = self._str
        ga = GoogleAnalyticsProvider(integration, self._default_library)
        assert integration.url == ga.url
        assert "faketrackingid" == ga.tracking_id

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
            title="pi\u00F1ata", authors="chlo\u00E9", fiction=True,
            audience="audience", language="lang",
            with_license_pool=True, genre="Folklore",
            with_open_access_download=True
        )
        work.presentation_edition.publisher = "publisher"
        work.target_age = NumericRange(10, 15)
        [lp] = work.license_pools
        now = utc_now()
        ga.collect_event(
            self._default_library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, now,
            neighborhood="Neighborhood will not be sent"
        )

        # Neighborhood information is not being sent -- that's for
        # local consumption only.
        assert 'Neighborhood' not in ga.params

        # Let's take a look at what _is_ being sent.
        params = urllib.parse.parse_qs(ga.params)

        assert 1 == ga.count
        assert integration.url == ga.url
        assert "faketrackingid" == params['tid'][0]
        assert "event" == params['t'][0]
        assert "circulation" == params['ec'][0]
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == params['ea'][0]
        assert str(now) == params['cd1'][0]
        assert lp.identifier.identifier == params['cd2'][0]
        assert lp.identifier.type == params['cd3'][0]
        assert unicodedata.normalize("NFKD", work.title) == params['cd4'][0]
        assert unicodedata.normalize("NFKD", work.author) == params['cd5'][0]
        assert "fiction" == params['cd6'][0]
        assert "audience" == params['cd7'][0]
        assert work.target_age_string == params['cd8'][0]
        assert "publisher" == params['cd9'][0]
        assert "lang" == params['cd10'][0]
        assert "Folklore" == params['cd11'][0]
        assert "true" == params['cd12'][0]
        assert DataSource.GUTENBERG == params['cd13'][0]
        assert EditionConstants.BOOK_MEDIUM == params['cd14'][0]
        assert self._default_library.short_name == params['cd15'][0]

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

        now = utc_now()
        ga.collect_event(self._default_library, pool, CirculationEvent.DISTRIBUTOR_CHECKIN, now)
        params = urllib.parse.parse_qs(ga.params)

        assert 1 == ga.count
        assert integration.url == ga.url
        assert "faketrackingid" == params['tid'][0]
        assert "event" == params['t'][0]
        assert "circulation" == params['ec'][0]
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == params['ea'][0]
        assert str(now) == params['cd1'][0]
        assert pool.identifier.identifier == params['cd2'][0]
        assert pool.identifier.type == params['cd3'][0]
        assert None == params.get('cd4')
        assert None == params.get('cd5')
        assert None == params.get('cd6')
        assert None == params.get('cd7')
        assert None == params.get('cd8')
        assert None == params.get('cd9')
        assert None == params.get('cd10')
        assert None == params.get('cd11')
        assert None == params.get('cd12')
        assert [source.name] == params.get('cd13')
        assert None == params.get('cd14')
        assert [self._default_library.short_name] == params.get('cd15')

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

        now = utc_now()
        ga.collect_event(self._default_library, None, CirculationEvent.NEW_PATRON, now)
        params = urllib.parse.parse_qs(ga.params)

        assert 1 == ga.count
        assert integration.url == ga.url
        assert "faketrackingid" == params['tid'][0]
        assert "event" == params['t'][0]
        assert "circulation" == params['ec'][0]
        assert CirculationEvent.NEW_PATRON == params['ea'][0]
        assert str(now) == params['cd1'][0]
        assert None == params.get('cd2')
        assert None == params.get('cd3')
        assert None == params.get('cd4')
        assert None == params.get('cd5')
        assert None == params.get('cd6')
        assert None == params.get('cd7')
        assert None == params.get('cd8')
        assert None == params.get('cd9')
        assert None == params.get('cd10')
        assert None == params.get('cd11')
        assert None == params.get('cd12')
        assert None == params.get('cd13')
        assert None == params.get('cd14')
        assert [self._default_library.short_name] == params.get('cd15')
