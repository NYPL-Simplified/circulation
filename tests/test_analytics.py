from nose.tools import (
    eq_,
    set_trace,
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
    Library,
    create,
)
import json

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        # supports multiple analytics providers, site-wide or with libraries

        # Two site-wide integrations
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

        # A broken integration
        missing_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="missing_provider"
        )

        # Two library-specific integrations
        l1, ignore = create(self._db, Library, short_name="L1")
        l2, ignore = create(self._db, Library, short_name="L2")

        library_integration1, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="mock_analytics_provider"
         )
        library_integration1.libraries += [l1, l2]

        library_integration2, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="mock_analytics_provider"
         )
        library_integration2.libraries += [l2]

        analytics = Analytics(self._db)
        eq_(2, len(analytics.sitewide_providers))
        assert isinstance(analytics.sitewide_providers[0], MockAnalyticsProvider)
        eq_(mock_integration.url, analytics.sitewide_providers[0].url)
        assert isinstance(analytics.sitewide_providers[1], LocalAnalyticsProvider)
        assert missing_integration.id in analytics.initialization_exceptions

        eq_(1, len(analytics.library_providers[l1.id]))
        assert isinstance(analytics.library_providers[l1.id][0], MockAnalyticsProvider)

        eq_(2, len(analytics.library_providers[l2.id]))
        for provider in analytics.library_providers[l2.id]:
            assert isinstance(provider, MockAnalyticsProvider)

    def test_collect_event(self):
        sitewide_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="mock_analytics_provider"
        )

        library, ignore = create(self._db, Library, short_name="library")
        library_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="mock_analytics_provider",
        )
        library_integration.libraries += [library]

        work = self._work(title="title", with_license_pool=True)
        [lp] = work.license_pools
        analytics = Analytics(self._db)
        sitewide_provider = analytics.sitewide_providers[0]
        library_provider = analytics.library_providers[library.id][0]

        analytics.collect_event(self._default_library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, None)

        # The sitewide provider was called.
        eq_(1, sitewide_provider.count)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, sitewide_provider.event_type)

        # The library provider wasn't called, since the event was for a different library.
        eq_(0, library_provider.count)

        analytics.collect_event(library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, None)

        # Now both providers were called, since the event was for the library provider's library.
        eq_(2, sitewide_provider.count)
        eq_(1, library_provider.count)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, library_provider.event_type)

        # Here's an event that we couldn't associate with any
        # particular library.
        analytics.collect_event(None, lp, CirculationEvent.DISTRIBUTOR_CHECKOUT, None)

        # It's counted as a sitewide event, but not as a library event.
        eq_(3, sitewide_provider.count)
        eq_(1, library_provider.count)
