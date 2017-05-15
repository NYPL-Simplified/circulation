from nose.tools import (
    set_trace,
    eq_,
)

from . import (
    DatabaseTest,
)

from api.monitor import SearchIndexMonitor

from core.external_search import DummyExternalSearchIndex


class TestSearchIndexMonitor(DatabaseTest):

    def test_process_batch(self):
        index = DummyExternalSearchIndex()

        # Here's a work.
        work = self._work()
        work.presentation_ready = True

        # Here's a Monitor that can index it.
        monitor = SearchIndexMonitor(self._db, None, "works-index", index)
        eq_("Search index update (works)", monitor.service_name)

        # The first time we call process_batch we handle the one and
        # only work in the database. The ID of that work is returned for
        # next time.
        eq_(work.id, monitor.process_batch(0))

        # The work was added to the search index.
        eq_([('works', 'work-type', work.id)], index.docs.keys())

        # The next time we call process_batch, no work is done and the
        # result is 0, meaning we're done with every work in the system.
        eq_(0, monitor.process_batch(work.id))
