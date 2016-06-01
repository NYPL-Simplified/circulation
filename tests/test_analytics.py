from nose.tools import (
    eq_,
)
from api.config import (
    Configuration,
    temp_config,
)
from api.analytics import Analytics
from api.google_analytics import GoogleAnalytics
from . import DatabaseTest
from core.model import CirculationEvent
from api.testing import MockAnalytics

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.ANALYTICS_POLICY: 'api.google_analytics'
            }
            config[Configuration.INTEGRATIONS] = {
                GoogleAnalytics.NAME: {
                    "tracking_id": "faketrackingid"
                }
            }
            analytics = Analytics.initialize()

            assert isinstance(analytics.providers[0], GoogleAnalytics)
            eq_(1, len(analytics.providers))
            eq_("faketrackingid", analytics.providers[0].tracking_id)

    def test_collect_event(self):
        dummy = DummyAnalytics()
        analytics = Analytics.initialize()
        analytics.providers = [dummy]   
        work = self._work("title", with_license_pool=True)     
        [lp] = work.license_pools
        event, is_new = CirculationEvent.log(
            self._db, lp, CirculationEvent.CHECKIN, None, None)        
        analytics.collect_event(event)

        eq_(1, dummy.count)