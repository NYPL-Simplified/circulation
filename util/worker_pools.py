import logging
from contextlib import contextmanager
from nose.tools import set_trace
from threading import (
    RLock,
    Thread,
    settrace,
)
from Queue import Queue

# Much of the work in this file is based on
# https://github.com/shazow/workerpool, with
# great appreciation.

# TODO: Consider supporting multiprocessing as well as
# (or instead of) multithreading.


class Worker(Thread):

    """A Thread that finishes jobs"""

    @classmethod
    def factory(cls, worker_pool):
        return cls(worker_pool)

    def __init__(self, jobs):
        super(Worker, self).__init__()
        self.daemon = True
        self.jobs = jobs

    def run(self):
        while True:
            job = self.jobs.get()
            job.run()
            job.task_done()


class DatabaseWorker(Worker):

    """A worker Thread that provides jobs with a db scoped_session"""

    def __init__(self, jobs, _db):
        super(DatabaseWorker, self).__init__(jobs)

        # A scoped_session to run tasks against.
        self._db = _db
        self._log = logging.getLogger(self.name)

    @property
    def log(self):
        return self._log

    @contextmanager
    def scoped_session(self, _db):
        _db.expire_all()
        try:
            yield
            _db.commit()
        except Exception as e:
            self.jobs.inc_error()
            self.log.error("Job raised error: %r", e)

    def run(self):
        while True:
            job = self.jobs.get()
            with self.scoped_session(self._db):
                try:
                    job.run(self._db)
                finally:
                    self.jobs.task_done()


class Pool(object):

    """A pool of Worker threads and a job queue to keep them busy."""

    log = logging.getLogger(__name__)

    def __init__(self, size, worker_factory=None):
        self.jobs = Queue()
        self.size = size
        self.job_total = 0
        self.error_count = 0

        # Use Worker for pool by default.
        worker_factory = worker_factory or Worker.factory
        for i in range(size):
            w = worker_factory(self)
            w.start()

    def inc_error(self):
        self.error_count += 1

    @property
    def success_rate(self):
        if self.job_total <= 0 or self.error_count <= 0:
            return float(1)
        return self.error_count / float(self.job_total)

    def get(self):
        return self.jobs.get()

    def put(self, job):
        self.job_total += 1
        return self.jobs.put(job)

    def task_done(self):
        return self.jobs.task_done()

    def join(self):
        self.jobs.join()
        self.log.info(
            "%d job errors occurred. %.2f\% success rate.",
            self.error_count, self.success_rate
        )


class Job(object):

    """Abstract parent class for a bit o' work that can be run in a Thread.
    For use with Worker."""

    def run(self):
        raise NotImplementedError()


class DatabaseJob(object):

    """Abstrct parent class for a bit o' work that can be run in a Thread and
    uses a database session. For use with DatabaseWorker.
    """

    def run(self, _db):
        raise NotImplementedError()
