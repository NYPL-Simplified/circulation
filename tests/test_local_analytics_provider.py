from nose.tools import (
    eq_,
)
from local_analytics_provider import LocalAnalyticsProvider
from . import DatabaseTest
from model import CirculationEvent
import datetime

class TestLocalAnalyticsProvider(DatabaseTest):

    def test_collect_event(self):
        la = LocalAnalyticsProvider()
        work = self._work(
            title="title", authors="author", fiction=True,
            audience="audience", language="lang",
            with_license_pool=True
        )
        [lp] = work.license_pools
        now = datetime.datetime.utcnow()
        la.collect_event(
            self._db, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, now,
            old_value=None, new_value=None)
        [event] = self._db \
            .query(CirculationEvent) \
            .filter(CirculationEvent.type == CirculationEvent.DISTRIBUTOR_CHECKIN) \
            .all()

        eq_(lp, event.license_pool)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, event.type)
        eq_(now, event.start)
