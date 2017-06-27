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
from model import (
    CirculationEvent,
    ExternalIntegration,
    create,
)
import json

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        # supports multiple analytics providers
        mock_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="mock_analytics_provider"
        )
        mock_integration.url = self._str
        local_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="local_analytics_provider"
        )
        missing_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="missing_provider"
        )

        analytics = Analytics(self._db)
        eq_(2, len(analytics.providers))
        assert isinstance(analytics.providers[0], MockAnalyticsProvider)
        eq_(mock_integration.url, analytics.providers[0].url)
        assert isinstance(analytics.providers[1], LocalAnalyticsProvider)
        assert missing_integration.id in analytics.initialization_exceptions

    def test_collect_event(self):
        mock_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="mock_analytics_provider"
        )

        work = self._work(title="title", with_license_pool=True)
        [lp] = work.license_pools
        analytics = Analytics(self._db)
        analytics.collect_event(self._default_library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, None)
        mock = analytics.providers[0]
        eq_(1, mock.count)

