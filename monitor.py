from nose.tools import set_trace
import datetime
import time

from model import (
    get_one_or_create,
    LicensePool,
    Timestamp,
    Work,
)

class Monitor(object):

    ONE_MINUTE_AGO = datetime.timedelta(seconds=60)

    def __init__(
            self, _db, name, interval_seconds=1*60,
            default_start_time=None):
        self._db = _db
        self.service_name = name
        self.interval_seconds = interval_seconds
        self.stop_running = False
        if not default_start_time:
             default_start_time = (
                 datetime.datetime.utcnow() - self.ONE_MINUTE_AGO)
        self.default_start_time = default_start_time

    def run(self):        
        self.timestamp, new = get_one_or_create(
            self._db, Timestamp,
            service=self.service_name,
            create_method_kwargs=dict(
                timestamp=self.default_start_time
            )
        )
        start = self.timestamp.timestamp or self.default_start_time

        while not self.stop_running:
            cutoff = datetime.datetime.utcnow()
            new_timestamp = self.run_once(start, cutoff) or cutoff
            duration = datetime.datetime.utcnow() - cutoff
            to_sleep = self.interval_seconds-duration.seconds-1
            self.cleanup()
            self.timestamp.timestamp = new_timestamp
            self._db.commit()
            if to_sleep > 0:
                print "Sleeping for %.1f" % to_sleep
                time.sleep(to_sleep)
            start = new_timestamp

    def run_once(self, start, cutoff):
        raise NotImplementedError()

    def cleanup(self):
        pass
        

class PresentationReadyMonitor(Monitor):
    """A monitor that makes works presentation ready.

    By default this works by passing the work's active edition into
    ensure_coverage() for each of a list of CoverageProviders. If all
    the ensure_coverage() calls succeed, presentation of the work is
    calculated and the work is marked presentation ready.
    """
    def __init__(self, _db, coverage_providers):
        super(PresentationReadyMonitor, self).__init__(
            _db, "Make Works Presentation Ready")
        self.coverage_providers = coverage_providers

    def run_once(self, start, cutoff):
        # Consolidate works.
        LicensePool.consolidate_works(self._db)

        unready_works = self._db.query(Work).filter(
            Work.presentation_ready==False).filter(
                Work.presentation_ready_exception==None).order_by(
                    Work.last_update_time.desc()).limit(10)
        # Work in batches of 10 works. This lets us consolidate and
        # parallelize IO-bound activities like uploading assets to S3.
        while unready_works.count():
            self.make_batch_presentation_ready(unready_works.all())

    def make_batch_presentation_ready(self, batch):
        for work in batch:
            failures = None
            exception = None
            try:
                failures = self.prepare(work)
            except Exception, e:
                exception = str(e)
            if failures and failures not in (None, True):
                if isinstance(failures, list):
                    # This is a list of providers that failed.
                    if len(failures):
                        provider_names = ", ".join(
                            [x.service_name for x in failures])
                        exception = "Provider(s) failed: %s" % provider_names
                    else:
                        # Just kidding, the list is empty, there were
                        # no failures.
                        pass
                else:
                    exception = str(failures)
            if exception:
                work.presentation_ready_exception = exception
            else:
                work.calculate_presentation(choose_edition=False)
                work.set_presentation_ready()                    
        self.finalize_batch()

    def prepare(self, work):
        edition = work.primary_edition
        identifier = edition.primary_identifier
        overall_success = True
        failures = []
        for provider in self.coverage_providers:
            if edition.data_source in provider.input_sources:
                coverage_record = provider.ensure_coverage(edition)
                if not coverage_record:
                    failures.append(provider)
        return failures

    def finalize_batch(self):
        self._db.commit()
