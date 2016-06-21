from nose.tools import (
    eq_,
)
from config import (
    Configuration,
    temp_config,
)
from analytics import Analytics, format_range
from mock_analytics_provider import MockAnalyticsProvider
from . import DatabaseTest
from model import CirculationEvent
from psycopg2.extras import NumericRange
import json

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        # supports multiple analytics providers
        config = { "option": "value" }
        analytics = Analytics.initialize(["mock_analytics_provider"], config)
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
                Configuration.ANALYTICS_POLICY: ["mock_analytics_provider"]
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

    def test_format_range(self):
        lower_only = NumericRange(18)
        eq_("18", format_range(lower_only))

        lower_and_upper = NumericRange(14, 17, "[)")
        eq_("14,15,16", format_range(lower_and_upper))

        lower_and_upper_inc = NumericRange(14, 17, "[]")
        eq_("14,15,16,17", format_range(lower_and_upper_inc))