from nose.tools import (
    eq_,
)
from config import (
    Configuration,
    temp_config,
)
from analytics import Analytics
from mock_analytics_provider import MockAnalyticsProvider
from local_analytics_provider import LocalAnalyticsProvider
from . import DatabaseTest
from model import CirculationEvent
import json

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        # supports multiple analytics providers
        config = { "option": "value" }
        analytics = Analytics.initialize(["mock_analytics_provider"], config)
        assert isinstance(analytics.providers[0], MockAnalyticsProvider)
        eq_("value", analytics.providers[0].option)

    def test_collect_event(self):
        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["mock_analytics_provider"]
            },
            "option": "value"
        }
        with temp_config(config) as config:
            work = self._work(title="title", with_license_pool=True)
            [lp] = work.license_pools
            Analytics.collect_event(self._db, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, None)
            mock = Analytics.instance().providers[0]
            eq_(1, mock.count)
            
    def test_load_providers_from_config(self):
        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["mock_analytics_provider"]
            },
            "option": "value"
        }
        providers = Analytics.load_providers_from_config(config)
        eq_("mock_analytics_provider", providers[0])

    def test_load_providers_from_config_without_analytics(self):
        providers = Analytics.load_providers_from_config({})
        eq_("local_analytics_provider", providers[0])
