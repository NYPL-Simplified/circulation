from nose.tools import (
    assert_raises_regexp,
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

    def setup(self):
        super(TestLocalAnalyticsProvider, self).setup()
        self.integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider")
        self.la = LocalAnalyticsProvider(
            self.integration, self._default_library
        )

    def test_collect_event(self):
        library2 = self._library()

        work = self._work(
            title="title", authors="author", fiction=True,
            audience="audience", language="lang",
            with_license_pool=True
        )
        [lp] = work.license_pools
        now = datetime.datetime.utcnow()
        self.la.collect_event(
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
        self.la.collect_event(
            library2, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, now,
            old_value=None, new_value=None)
        eq_(1, qu.count())

        # It's possible to instantiate the LocalAnalyticsProvider
        # without a library.
        la = LocalAnalyticsProvider(self.integration)

        # In that case, it will process events for any library.
        for library in [self._default_library, library2]:
            now = datetime.datetime.now()
            la.collect_event(library, lp,
                             CirculationEvent.DISTRIBUTOR_CHECKIN, now,
                             old_value=None, new_value=None
            )
        eq_(3, qu.count())

    def test_collect_with_missing_information(self):
        """A circulation event may be collected with either the
        library or the license pool missing, but not both.
        """
        now = datetime.datetime.now()
        self.la.collect_event(self._default_library, None, "event", now)

        pool = self._licensepool(None)
        self.la.collect_event(None, pool, "event", now)

        assert_raises_regexp(
            ValueError,
            "Either library or license_pool must be provided.",
            self.la.collect_event, None, None, "event", now
        )
