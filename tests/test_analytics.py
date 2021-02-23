from ..config import (
    Configuration,
    temp_config,
)
from ..analytics import Analytics
from ..mock_analytics_provider import MockAnalyticsProvider
from ..local_analytics_provider import LocalAnalyticsProvider
from . import DatabaseTest
from ..model import (
    CirculationEvent,
    ExternalIntegration,
    Library,
    create,
    get_one
)
import json

# We can't import mock_analytics_provider from within a test,
# and we can't tell Analytics to do so either. We need to tell
# it to perform an import relative to the module the Analytics
# class is in.
MOCK_PROTOCOL = "..mock_analytics_provider"

class TestAnalytics(DatabaseTest):

    def test_initialize(self):
        # supports multiple analytics providers, site-wide or with libraries

        # Two site-wide integrations
        mock_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL
        )
        mock_integration.url = self._str
        local_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="..local_analytics_provider"
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
            protocol=MOCK_PROTOCOL
         )
        library_integration1.libraries += [l1, l2]

        library_integration2, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL
         )
        library_integration2.libraries += [l2]

        analytics = Analytics(self._db)
        assert 2 == len(analytics.sitewide_providers)
        assert isinstance(analytics.sitewide_providers[0], MockAnalyticsProvider)
        assert mock_integration.url == analytics.sitewide_providers[0].url
        assert isinstance(analytics.sitewide_providers[1], LocalAnalyticsProvider)
        assert missing_integration.id in analytics.initialization_exceptions

        assert 1 == len(analytics.library_providers[l1.id])
        assert isinstance(analytics.library_providers[l1.id][0], MockAnalyticsProvider)

        assert 2 == len(analytics.library_providers[l2.id])
        for provider in analytics.library_providers[l2.id]:
            assert isinstance(provider, MockAnalyticsProvider)

        # Instantiating an Analytics object initializes class
        # variables with the current state of site analytics.

        # We have global analytics enabled.
        assert True == Analytics.GLOBAL_ENABLED

        # We also have analytics enabled for two of the three libraries.
        assert set([l1.id, l2.id]) == Analytics.LIBRARY_ENABLED

        # If the analytics situation changes, instantiating an
        # Analytics object will change the class variables.
        self._db.delete(mock_integration)
        self._db.delete(local_integration)
        self._db.delete(library_integration1)

        # There are no longer any global analytics providers, and only
        # one of the libraries has a library-specific provider.
        analytics = Analytics(self._db)
        assert False == Analytics.GLOBAL_ENABLED
        assert set([l2.id]) == Analytics.LIBRARY_ENABLED

    def test_is_configured(self):
        # If the Analytics constructor has not been called, then
        # is_configured() calls it so that the values are populated.
        Analytics.GLOBAL_ENABLED = None
        Analytics.LIBRARY_ENABLED = object()
        library = self._default_library
        assert False == Analytics.is_configured(library)
        assert False == Analytics.GLOBAL_ENABLED
        assert set() == Analytics.LIBRARY_ENABLED

        # If analytics are enabled globally, they are enabled for any
        # library.
        Analytics.GLOBAL_ENABLED = True
        assert True == Analytics.is_configured(object())

        # If not, they are enabled only for libraries whose IDs are
        # in LIBRARY_ENABLED.
        Analytics.GLOBAL_ENABLED = False
        assert False == Analytics.is_configured(library)
        Analytics.LIBRARY_ENABLED.add(library.id)
        assert True == Analytics.is_configured(library)

    def test_collect_event(self):
        sitewide_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL
        )

        library, ignore = create(self._db, Library, short_name="library")
        library_integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL,
        )
        library_integration.libraries += [library]

        work = self._work(title="title", with_license_pool=True)
        [lp] = work.license_pools
        analytics = Analytics(self._db)
        sitewide_provider = analytics.sitewide_providers[0]
        library_provider = analytics.library_providers[library.id][0]

        analytics.collect_event(self._default_library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, None)

        # The sitewide provider was called.
        assert 1 == sitewide_provider.count
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == sitewide_provider.event_type

        # The library provider wasn't called, since the event was for a different library.
        assert 0 == library_provider.count

        analytics.collect_event(library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, None)

        # Now both providers were called, since the event was for the library provider's library.
        assert 2 == sitewide_provider.count
        assert 1 == library_provider.count
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == library_provider.event_type

        # Here's an event that we couldn't associate with any
        # particular library.
        analytics.collect_event(None, lp, CirculationEvent.DISTRIBUTOR_CHECKOUT, None)

        # It's counted as a sitewide event, but not as a library event.
        assert 3 == sitewide_provider.count
        assert 1 == library_provider.count

    def test_initialize(self):

        local_analytics = get_one(
            self._db, ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL
        )

        # There shouldn't exist a local analytics service.
        assert None == local_analytics

        # So when the Local Analytics provider is initialized, it will
        # create one with the default name of "Local Analytics".
        local_analytics = LocalAnalyticsProvider.initialize(self._db)

        assert isinstance(local_analytics, ExternalIntegration)
        assert local_analytics.name == LocalAnalyticsProvider.NAME

        # When an analytics provider is initialized, retrieving a
        # local analytics service should return the same one.
        local_analytics = LocalAnalyticsProvider.initialize(self._db)

        local_analytics_2 = get_one(
            self._db, ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL
        )

        assert local_analytics_2.id == local_analytics.id
        assert local_analytics_2.name == local_analytics.name