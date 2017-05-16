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

    def test_run(self):
        index = DummyExternalSearchIndex()

        # Here's a work.
        work = self._work()
        work.presentation_ready = True

        # Here's a Monitor that can index it.
        monitor = SearchIndexMonitor(self._db, "works-index", index)

        # Let's run the monitor.
        monitor.run()

        # The work was added to the search index.
        eq_([('works', 'work-type', work.id)], index.docs.keys())
