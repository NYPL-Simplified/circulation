import threading
from contextlib import contextmanager

from nose.tools import (
    eq_,
    set_trace,
)

from model import Identifier
from util.worker_pools import (
    DatabaseWorker,
    Job,
    Pool,
    Queue,
    Worker,
)

from . import DatabaseTest


class TestPool(object):

    def test_initializes_with_active_workers(self):
        original_thread_count = threading.active_count()
        with Pool(3) as pool:
            pool_thread_count = threading.active_count() - original_thread_count
            eq_(3, pool_thread_count)
            eq_(3, pool.size)
            eq_(3, len(pool.workers))

    def test_put_tracks_total_job_count(self):
        def task():
            return "T'Challa"

        with Pool(2) as pool:
            eq_(0, pool.job_total)
            for i in range(4):
                pool.put(task)
            eq_(4, pool.job_total)

    def test_pool_tracks_error_count(self):
        def broken_task():
            raise RuntimeError

        pool = Pool(2)
        try:
            # The pool instantiates with 0 errors.
            eq_(0, pool.error_count)

            for i in range(3):
                pool.put(broken_task)
        finally:
            pool.join()

        # The pool maintains a count of its errors.
        eq_(3, pool.error_count)

    def test_success_rate(self):
        def task():
            return "Shuri"

        def broken_task():
            raise RuntimeError

        pool = Pool(2)
        try:
            # When there are no tasks, the success rate is 1.0.
            eq_(1.0, pool.success_rate)

            pool.put(task)
            pool.put(task)
            # When there are no errors, the success rate is 1.0.
            pool.join()
            eq_(1.0, pool.success_rate)

            # When a job fails, it impacts the success rate.
            pool.put(broken_task)
        finally:
            pool.join()
        eq_(1/3.0, pool.success_rate)


class MockQueue(Queue):
    error_count = 0

    def inc_error(self):
        self.error_count += 1


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
            q = MockQueue()
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
            q = MockQueue()
            for i in range(len(original)):
                q.put(MockJob(i))
            rb = Worker(q)
            rb.start()
        finally:
            q.join()

        eq_(sorted(original), sorted(results))


class TestDatabaseWorker(DatabaseTest):

    def test_scoped_session(self):

        # Create a mock database object to keep track of methods that
        # are called.
        class MockDatabase():
            def __init__(self, _db):
                self._expired = 0
                self._committed = 0
                self._db = _db

            def expire_all(self):
                self._expired += 1

            def commit(self):
                self._committed += 1

            def add(self, item):
                self._db.add(item)

        # A job that works.
        def task(_db):
            identifier = Identifier(type='Keep It', identifier='100')
            _db.add(identifier)

        # A job that breaks.
        def broken_task(_db):
            identifier = Identifier(type='You Can', identifier='Keep It')
            _db.add(identifier)
            raise RuntimeError

        try:
            q = MockQueue()
            q.put(task)
            q.put(broken_task)

            mock_db = MockDatabase(self._db)
            dbw = DatabaseWorker(q, mock_db)
            dbw.start()
        finally:
            q.join()

        # The database was expired for each job, then called one more
        # time before finding out the queue was empty.
        eq_(3, mock_db._expired)

        # It was only committed for the working job.
        eq_(1, mock_db._committed)

        # The DatabaseWorker doesn't rollback the database. It
        # trusts the task to manage that.
        [i1, i2] = self._db.query(Identifier).order_by(Identifier.id).all()
        eq_(('Keep It', '100'), (i1.type, i1.identifier))
        eq_(('You Can', 'Keep It'), (i2.type, i2.identifier))

        # The error count has been incremented.
        eq_(1, q.error_count)
