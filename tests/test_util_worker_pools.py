import threading
from contextlib import contextmanager
from nose.tools import (
    eq_,
    set_trace,
)

from util.worker_pools import (
    DatabaseWorker,
    Job,
    Pool,
    Queue,
    Worker,
)

from . import DatabaseTest


class TestWorker(object):

    def test_factory(self):
        mock_queue = object()
        result = Worker.factory(mock_queue)
        assert isinstance(result, Worker)
        eq_(mock_queue, result.jobs)
        eq_(True, result.daemon)

    def test_works_on_callable_job(self):
        results = list()

        def task():
            results.append('werk')

        try:
            q = Queue()
            for i in range(6):
                q.put(task)
            rihanna = Worker(q)
            rihanna.start()
        finally:
            q.join()

        eq_(['werk', 'werk', 'werk', 'werk', 'werk', 'werk'], results)

    def test_works_on_job_object(self):
        results = list()

        original = ['Who Can I * To', '* To You', 'Water *s Dry', '* The World']
        class MockJob(object):
            def __init__(self, idx):
                self.idx = idx

            def run(self):
                results.append(original[self.idx])

        try:
            q = Queue()
            for i in range(len(original)):
                q.put(MockJob(i))
            rb = Worker(q)
            rb.start()
        finally:
            q.join()

        eq_(sorted(original), sorted(results))
