from nose.tools import (
    eq_,
)
from api.config import (
    Configuration,
    temp_config,
)
from api.analytics import Analytics
from api.google_analytics import GoogleAnalytics
from api.local_analytics import LocalAnalytics
from . import DatabaseTest
from core.model import CirculationEvent
from api.testing import MockAnalytics

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        # supports multiple analytics providers
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.ANALYTICS_POLICY: [
                    'api.google_analytics',
                    'api.local_analytics'
                ]
            }
            config[Configuration.INTEGRATIONS] = {
                GoogleAnalytics.NAME: {
                    "tracking_id": "faketrackingid"
                }
            }
            analytics = Analytics.initialize()

            assert isinstance(analytics.providers[0], GoogleAnalytics)
            assert isinstance(analytics.providers[1], LocalAnalytics)
            eq_(2, len(analytics.providers))
            eq_("faketrackingid", analytics.providers[0].tracking_id)

        # analytics providers not required
        with temp_config() as config:
            config[Configuration.POLICIES] = {}
            work = self._work("title", with_license_pool=True)     
            [lp] = work.license_pools
            analytics = Analytics.initialize()
            eq_(0, len(analytics.providers))
            analytics.collect(self._db, lp, CirculationEvent.CHECKIN, None)

    def test_collect(self):
        mock = MockAnalytics()
        analytics = Analytics.initialize()
        analytics.providers = [mock]   
        work = self._work("title", with_license_pool=True)     
        [lp] = work.license_pools
        analytics.collect(self._db, lp, CirculationEvent.CHECKIN, None)

        eq_(1, mock.count)