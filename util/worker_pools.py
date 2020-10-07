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
    """A Thread that performs jobs"""

    @classmethod
    def factory(cls, worker_pool):
        return cls(worker_pool)

    def __init__(self, jobs):
        super(Worker, self).__init__()
        self.daemon = True
        self.jobs = jobs
        self._log = logging.getLogger(self.name)

    @property
    def log(self):
        return self._log

    def run(self):
        while True:
            try:
                self.do_job()
            except Exception as e:
                self.jobs.inc_error()
                self.log.error("Job raised error: %r", e, exc_info=e)
            finally:
                self.jobs.task_done()

    def do_job(self, *args, **kwargs):
        job = self.jobs.get()
        if callable(job):
            job(*args, **kwargs)
            return

        # This is a Job object. Do any setup and finalization, as well as
        # running the task.
        job.run(*args, **kwargs)


class DatabaseWorker(Worker):
    """A worker Thread that performs jobs with a database session"""

    @classmethod
    def factory(cls, worker_pool, _db):
        return cls(worker_pool, _db)

    def __init__(self, jobs, _db):
        super(DatabaseWorker, self).__init__(jobs)
        self._db = _db

    def do_job(self):
        super(DatabaseWorker, self).do_job(self._db)


class Pool(object):
    """A pool of Worker threads and a job queue to keep them busy."""

    log = logging.getLogger(__name__)

    def __init__(self, size, worker_factory=None):
        self.jobs = Queue()

        self.size = size
        self.workers = list()

        self.job_total = 0
        self.error_count = 0

        # Use Worker for pool by default.
        self.worker_factory = worker_factory or Worker.factory
        for i in range(self.size):
            w = self.create_worker()
            self.workers.append(w)
            w.start()

    @property
    def success_rate(self):
        if self.job_total <= 0 or self.error_count <= 0:
            return float(1)
        return self.error_count / float(self.job_total)

    def create_worker(self):
        return self.worker_factory(self)

    def inc_error(self):
        self.error_count += 1

    def restart(self):
        for w in self.workers:
            if not w.is_alive():
                w.start()
        return self

    __enter__ = restart

    def __exit__(self, type, value, traceback):
        self.join()
        if type:
            self.log.error('Error with %r: %r', self, value, exc_info=traceback)
            raise value
        return

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
            "%d/%d job errors occurred. %.2f%% success rate.",
            self.error_count, self.job_total, self.success_rate*100
        )


class DatabasePool(Pool):
    """A pool of DatabaseWorker threads and a job queue to keep them busy."""
    def __init__(self, size, session_factory, worker_factory=None):
        self.session_factory = session_factory

        self.worker_factory = worker_factory or DatabaseWorker.factory
        super(DatabasePool, self).__init__(
            size, worker_factory=self.worker_factory
        )

    def create_worker(self):
        worker_session = self.session_factory()
        return self.worker_factory(self, worker_session)


class Job(object):
    """Abstract parent class for a bit o' work that can be run in a Thread.
    For use with Worker.
    """

    def rollback(self, *args, **kwargs):
        """Cleans up the task if it errors"""
        pass

    def finalize(self, *args, **kwargs):
        """Finalizes the task if it is successful"""
        pass

    def do_run(self):
        """Does the work"""
        raise NotImplementedError()

    def run(self, *args, **kwargs):
        try:
            self.do_run(*args, **kwargs)
        except Exception:
            self.rollback(*args, **kwargs)
            raise
        else:
            self.finalize(*args, **kwargs)


class DatabaseJob(Job):

    def rollback(self, _db):
        _db.rollback()

    def finalize(self, _db):
        _db.commit()

    def do_run(self):
        raise NotImplementedError()
