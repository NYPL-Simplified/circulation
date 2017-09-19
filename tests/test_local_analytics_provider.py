from nose.tools import (
    eq_,
)
from local_analytics_provider import LocalAnalyticsProvider
from . import DatabaseTest
from model import (
    CirculationEvent,
    ExternalIntegration,
    create,
)
import datetime

class TestLocalAnalyticsProvider(DatabaseTest):

    def test_collect_event(self):
        library2 = self._library()

        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider")
        la = LocalAnalyticsProvider(integration, self._default_library)
        work = self._work(
            title="title", authors="author", fiction=True,
            audience="audience", language="lang",
            with_license_pool=True
        )
        [lp] = work.license_pools
        now = datetime.datetime.utcnow()
        la.collect_event(
            self._default_library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, now,
            old_value=None, new_value=None)

        qu = self._db.query(CirculationEvent).filter(
            CirculationEvent.type == CirculationEvent.DISTRIBUTOR_CHECKIN
        )
        eq_(1, qu.count())
        [event] = qu.all()

        eq_(lp, event.license_pool)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, event.type)
        eq_(now, event.start)

        # The LocalAnalyticsProvider will not handle an event intended
        # for a different library.
        now = datetime.datetime.now()
        la.collect_event(
            library2, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, now,
            old_value=None, new_value=None)
        eq_(1, qu.count())

        # It's possible to instantiate the LocalAnalyticsProvider
        # without a library.
        la = LocalAnalyticsProvider(integration)

        # In that case, it will process events for any library.
        for library in [self._default_library, library2]:
            now = datetime.datetime.now()
            la.collect_event(library, lp, 
                             CirculationEvent.DISTRIBUTOR_CHECKIN, now,
                             old_value=None, new_value=None
            )
        eq_(3, qu.count())
