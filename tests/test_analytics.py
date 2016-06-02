from nose.tools import (
    eq_,
)
from core.config import (
    Configuration,
    temp_config,
)
from core.analytics import Analytics
from core.mock_analytics_provider import MockAnalyticsProvider
from . import DatabaseTest
from core.model import CirculationEvent
import json

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        # supports multiple analytics providers
        config = { "option": "value" }
        analytics = Analytics.initialize(["core.mock_analytics_provider"], config)
        assert isinstance(analytics.providers[0], MockAnalyticsProvider)
        eq_("value", analytics.providers[0].option)

    def test_collect_event(self):
        with temp_config() as config:
            mock = MockAnalyticsProvider()
            analytics = Analytics([mock])
            work = self._work(title="title", with_license_pool=True)
            [lp] = work.license_pools
            analytics.collect_event(self._db, lp, CirculationEvent.CHECKIN, None)
            eq_(1, mock.count)
            
    def test_load_analytics_configuration(self):
        config = {
            Configuration.POLICIES: {
                Configuration.ANALYTICS_POLICY: ["core.mock_analytics_provider"]
            },
            "option": "value"
        }
        loaded_config = Configuration._load(json.dumps(config))
        providers = loaded_config[Configuration.POLICIES][Configuration.ANALYTICS_POLICY].providers
        assert isinstance(providers[0], MockAnalyticsProvider)

    def test_load_configuration_without_analytics(self):
        loaded_config = Configuration._load(json.dumps({}))
        providers = loaded_config[Configuration.POLICIES][Configuration.ANALYTICS_POLICY].providers
        eq_([], providers)