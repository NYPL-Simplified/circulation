from nose.tools import (
    eq_,
)
from api.config import (
    Configuration,
    temp_config,
)
from api.google_analytics import GoogleAnalytics
from . import DatabaseTest
from core.model import CirculationEvent
import urlparse
import datetime

class MockGoogleAnalytics(GoogleAnalytics):

    def post(self, url, params):
        self.count = self.count + 1 if hasattr(self, "count") else 1
        self.url = url
        self.params = params

class TestGoogleAnalytics(DatabaseTest):

    def test_from_config(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.ANALYTICS_POLICY: 'api.google_analytics'
            }
            config[Configuration.INTEGRATIONS] = {
                GoogleAnalytics.NAME: {
                    "tracking_id": "faketrackingid"
                }
            }
            ga = GoogleAnalytics.from_config()
            eq_("faketrackingid", ga.tracking_id)

    def test_collect(self):
        ga = MockGoogleAnalytics("faketrackingid")
        work = self._work(
            title="title", authors="author", fiction=True,
            audience="audience", language="lang", 
            with_license_pool=True
        )     
        [lp] = work.license_pools
        now = datetime.datetime.utcnow()
        ga.collect(self._db, lp, CirculationEvent.CHECKIN, now)
        params = urlparse.parse_qs(ga.params)

        eq_(1, ga.count)
        eq_("http://www.google-analytics.com/collect", ga.url)
        eq_("faketrackingid", params['tid'][0])
        eq_("event", params['t'][0])
        eq_("circulation", params['ec'][0])
        eq_(CirculationEvent.CHECKIN, params['ea'][0])
        eq_(lp.identifier.identifier, params['cd1'][0])
        eq_("title", params['cd2'][0])
        eq_("author", params['cd3'][0])
        eq_("fiction", params['cd4'][0])
        eq_("audience", params['cd5'][0])
        eq_("lang", params['cd7'][0])
        eq_(str(now), params['cd9'][0])
