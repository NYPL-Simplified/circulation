from nose.tools import set_trace
import datetime
import os
import logging
import time
import traceback
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import (
    or_,
)

import log # This sets the appropriate log format and level.
from config import Configuration
from coverage import CoverageFailure
from model import (
    get_one_or_create,
    Collection,
    CoverageRecord,
    Edition,
    CustomListEntry,
    Identifier,
    LicensePool,
    PresentationCalculationPolicy,
    Subject,
    Timestamp,
    Work,
)

class Monitor(object):
    """A Monitor is responsible for running some piece of code on a regular
    basis. 

    Running a Monitor will update a Timestamp object with the last time the
    Monitor was run.

    Although any Monitor may be associated with a Collection, it's
    most useful to subclass CollectionMonitor if you're writing code
    that needs to be run on every Collection of a certain type.
    """    

    # The Monitor code will not run more than once every this number of seconds.
    #
    # It's possible to override this by passing in `interval_seconds`
    # into the constructor, but generally nobody will bother doing
    # so. It's more common to change this behavior by redefining
    # DEFAULT_INTERVAL_SECONDS in a subclass.
    DEFAULT_INTERVAL_SECONDS = 60

    ONE_MINUTE_AGO = datetime.timedelta(seconds=60)
    ONE_YEAR_AGO = datetime.timedelta(seconds=60*60*24*365)
    NEVER = object()
    
    def __init__(
            self, _db, name, collection=None, interval_seconds=None,
            default_start_time=None, keep_timestamp=True):
        if interval_seconds is None:
            interval_seconds = self.DEFAULT_INTERVAL_SECONDS
        self._db = _db
        self.service_name = name
        self.interval_seconds = interval_seconds
        self.collection = collection
        self.stop_running = False
        self.keep_timestamp = keep_timestamp

        if not default_start_time:
             default_start_time = (
                 datetime.datetime.utcnow() - self.ONE_MINUTE_AGO)
        if default_start_time is self.NEVER:
            default_start_time = None
        self.default_start_time = default_start_time
        
    @property
    def log(self):
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(self.service_name)
        return self._log        

    def run(self):        
        if self.keep_timestamp:
            self.timestamp, new = get_one_or_create(
                self._db, Timestamp,
                service=self.service_name,
                collection=self.collection,
                create_method_kwargs=dict(
                    timestamp=self.default_start_time
                )
            )
            start = self.timestamp.timestamp or self.default_start_time
        else:
            start = self.default_start_time
            self.timestamp = None

        while not self.stop_running:
            cutoff = datetime.datetime.utcnow()           
            new_timestamp = self.run_once(start, cutoff) or cutoff
            duration = datetime.datetime.utcnow() - cutoff
            to_sleep = self.interval_seconds-duration.seconds-1
            self.cleanup()
            if self.keep_timestamp:
                self.timestamp.timestamp = new_timestamp
            self._db.commit()

            # TODO: This could be a little nicer, but basically we now
            # want monitors to run through once and then stop.
            if True:
                self.stop_running = True
            elif to_sleep > 0:
                self.log.debug("Sleeping for %.1f", to_sleep)
                time.sleep(to_sleep)
            start = new_timestamp

    def run_once(self, start, cutoff):
        raise NotImplementedError()

    def cleanup(self):
        pass


class CollectionMonitor(Monitor):
    """A Monitor that does something for all Collections that implement
    a certain protocol.

    This class is designed to be subclassed rather than instantiated
    directly. Subclasses should define SERVICE_NAME and PROTOCOL.
    """

    # In your subclass, set this to the name of the service,
    # e.g. "Overdrive Circulation Monitor". All instances of your
    # subclass will give this as their service name and track their
    # Timestamps under this name.
    SERVICE_NAME = None

    # Set this to the name of the protocol managed by this Monitor.
    PROTOCOL = None

    def __init__(self, _db, **kwargs):
        super(CollectionMonitor, self).__init__(
            _db, name=self.SERVICE_NAME, **kwargs
        )
        
    @classmethod
    def all(cls, _db, **kwargs):
        """Yield a sequence of CollectionMonitor objects: one for every
        Collection that implements cls.PROTOCOL.

        Monitors that have no Timestamp will be yielded first. After that,
        Monitors with older Timestamps will be yielded before Monitors with
        newer timestamps.
        """
        service_match = or_(Timestamp.service==cls.SERVICE_NAME,
                            Timestamp.service==None)
        collections = _db.query(Collection).outerjoin(
            Collection.timestamps).filter(
                Collection.protocol==cls.PROTOCOL).filter(
                    service_match).order_by(
                    Timestamp.timestamp.asc().nullsfirst()
                )
        for collection in collections:
            yield cls(_db=_db, collection=collection, **kwargs)
    
    
class IdentifierSweepMonitor(Monitor):

    # The completion of each individual item should be logged at
    # this log level.
    COMPLETION_LOG_LEVEL = logging.INFO

    def __init__(self, _db, name, interval_seconds=3600,
                 default_counter=0, batch_size=100):
        super(IdentifierSweepMonitor, self).__init__(
            _db, name, interval_seconds=interval_seconds)
        self.default_counter = default_counter
        self.batch_size = batch_size

    def run(self):        
        self.timestamp, new = get_one_or_create(
            self._db, Timestamp,
            service=self.service_name,
            create_method_kwargs=dict(
                counter=self.default_counter
            )
        )
        offset = self.timestamp.counter or self.default_counter

        started_at = datetime.datetime.utcnow()
        while not self.stop_running:
            a = time.time()
            old_offset = offset
            try:
                new_offset = self.run_once(offset)
            except Exception, e:
                self.log.error("Error during run: %s", e, exc_info=e)
                break
            to_sleep = 0
            if new_offset == 0:
                # We completed a sweep. We're done.
                self.stop_running = True
                self.cleanup()
            self.counter = new_offset
            self.timestamp.counter = self.counter
            self._db.commit()
            if old_offset != new_offset:
                self.log.debug("Old offset: %s" % offset)
                self.log.debug("New offset: %s", new_offset)
                b = time.time()
                self.log.debug("Elapsed: %.2f sec" % (b-a))
            if to_sleep > 0:
                if old_offset != new_offset:
                    self.log.debug("Sleeping for %.1f", to_sleep)
                time.sleep(to_sleep)
            offset = new_offset

    def run_once(self, offset):
        q = self.identifier_query().filter(
            Identifier.id > offset).order_by(
            Identifier.id).limit(self.batch_size)
        identifiers = q.all()
        if identifiers:
            self.process_batch(identifiers)
            return identifiers[-1].id
        else:
            return 0

    def identifier_query(self):
        return self._db.query(Identifier)

    def process_batch(self, identifiers):
        for i in identifiers:
            self.process_identifier(i)

    def process_identifier(self, identifier):
        raise NotImplementedError()

class SubjectSweepMonitor(IdentifierSweepMonitor):

    def __init__(self, _db, name, subject_type=None, filter_string=None,
                 batch_size=500):
        super(SubjectSweepMonitor, self).__init__(
            _db, name, batch_size=batch_size
        )
        self.subject_type = subject_type
        self.filter_string = filter_string

    def run_once(self, offset):
        q = self.subject_query().filter(
            Subject.id > offset).order_by(
            Subject.id).limit(self.batch_size)
        subjects = q.all()
        if subjects:
            self.process_batch(subjects)
            return subjects[-1].id
        else:
            return 0

    def subject_query(self):
        qu = self._db.query(Subject)
        if self.subject_type:
            qu = qu.filter(Subject.type==self.subject_type)
        if self.filter_string:
            filter_string = '%' + self.filter_string + '%'
            or_clause = or_(
                Subject.identifier.ilike(filter_string),
                Subject.name.ilike(filter_string)
            )
            qu = qu.filter(or_clause)
        return qu

class CustomListEntrySweepMonitor(IdentifierSweepMonitor):

    def run_once(self, offset):
        q = self.custom_list_entry_query().filter(
            CustomListEntry.id > offset).order_by(
            CustomListEntry.id).limit(self.batch_size)
        entries = q.all()
        if entries:
            self.process_batch(entries)
            return entries[-1].id
        else:
            return 0

    def process_batch(self, entries):
        for entry in entries:
            self.process_entry(entry)

    def custom_list_entry_query(self):
        return self._db.query(CustomListEntry)


class EditionSweepMonitor(IdentifierSweepMonitor):

    def run_once(self, offset):
        if offset is None:
            offset = 0
        q = self.edition_query().filter(
            Edition.id > offset).order_by(
            Edition.id).limit(self.batch_size)
        editions = q.all()
        if editions:
            self.process_batch(editions)
            return editions[-1].id
        else:
            return 0

    def edition_query(self):
        return self._db.query(Edition)

    def process_batch(self, batch):
        for edition in batch:
            self.process_edition(edition)
            self.log.log(self.COMPLETION_LOG_LEVEL, "Completed %r", edition)

    def process_edition(self, work):
        raise NotImplementedError()


class WorkSweepMonitor(IdentifierSweepMonitor):

    def run_once(self, offset):
        if offset is None:
            offset = 0
        q = self.work_query().filter(
            Work.id > offset).order_by(
            Work.id).limit(self.batch_size)
        works = q.all()
        if works:
            self.process_batch(works)
            return works[-1].id
        else:
            return 0

    def work_query(self):
        return self._db.query(Work)

    def process_batch(self, batch):
        for work in batch:
            self.process_work(work)
            self.log.log(self.COMPLETION_LOG_LEVEL, "Completed %r", work)

    def process_work(self, work):
        raise NotImplementedError()

class PresentationReadyWorkSweepMonitor(WorkSweepMonitor):

    def work_query(self):
        return self._db.query(Work).filter(Work.presentation_ready==True)


class OPDSEntryCacheMonitor(PresentationReadyWorkSweepMonitor):

    def __init__(self, _db, interval_seconds=None,
                 include_verbose_entry=True):
        super(OPDSEntryCacheMonitor, self).__init__(
            _db, "ODPS Entry Cache Monitor", interval_seconds=interval_seconds)
        self.include_verbose_entry=include_verbose_entry

    def process_work(self, work):
        work.calculate_opds_entries(verbose=self.include_verbose_entry)

class SimpleOPDSEntryCacheMonitor(OPDSEntryCacheMonitor):
    def __init__(self, _db, interval_seconds=None):
        super(SimpleOPDSEntryCacheMonitor, self).__init__(
            _db, interval_seconds=interval_seconds, keep_timestamp=False)

class SubjectAssignmentMonitor(SubjectSweepMonitor):

    def __init__(self, _db, subject_type=None, filter_string=None,
                 interval_seconds=None):
        super(SubjectAssignmentMonitor, self).__init__(
            _db, "Subject assignment monitor", subject_type, filter_string,
            interval_seconds
        )

    def process_batch(self, subjects):
        highest_id = 0
        for subject in subjects:
            if subject.id > highest_id:
                highest_id = subject.id
            subject.assign_to_genre()
        self.log.log(self.COMPLETION_LOG_LEVEL, "Completed %r", subject)
        return highest_id

class PermanentWorkIDRefreshMonitor(EditionSweepMonitor):
    """Recalculate the permanent work ID for every edition."""

    def __init__(self, _db, interval_seconds=None):
        super(PermanentWorkIDRefreshMonitor, self).__init__(
            _db, "Permanent Work ID refresh", interval_seconds=interval_seconds)

    def process_edition(self, edition):
        edition.calculate_permanent_work_id()

class PresentationReadyMonitor(WorkSweepMonitor):
    """A monitor that makes works presentation ready.

    By default this works by passing the work's active edition into
    ensure_coverage() for each of a list of CoverageProviders. If all
    the ensure_coverage() calls succeed, presentation of the work is
    calculated and the work is marked presentation ready.
    """
    def __init__(self, _db, coverage_providers,
                 calculate_work_even_if_no_author=False):
        super(PresentationReadyMonitor, self).__init__(
            _db, "Make Works Presentation Ready")
        self.coverage_providers = coverage_providers
        self.calculate_work_even_if_no_author = calculate_work_even_if_no_author

    def work_query(self):
        not_presentation_ready = or_(
            Work.presentation_ready==False,
            Work.presentation_ready==None)
        return self._db.query(Work).filter(not_presentation_ready)

    def run_once(self, offset):
        # Consolidate works.
        LicensePool.consolidate_works(
            self._db,
            calculate_work_even_if_no_author=self.calculate_work_even_if_no_author)

        return super(PresentationReadyMonitor, self).run_once(offset)

    def process_batch(self, batch):
        max_id = 0
        one_success = False
        for work in batch:
            failures = None
            exception = None
            if work.id > max_id:
                max_id = work.id
            try:
                failures = self.prepare(work)
            except Exception, e:
                self.log.error(
                    "Exception processing work %r", work, exc_info=e
                )
                failures = e
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
                policy = PresentationCalculationPolicy(
                    choose_edition=False
                )
                work.calculate_presentation(policy)
                work.set_presentation_ready()                    
                one_success = True
        self.finalize_batch()
        return max_id

    def prepare(self, work):
        edition = work.presentation_edition
        if not edition:
            work = work.calculate_presentation()
        identifier = edition.primary_identifier
        overall_success = True
        failures = []
        for provider in self.coverage_providers:
            if identifier.type in provider.input_identifier_types:
                coverage_record = provider.ensure_coverage(identifier)
                if (not isinstance(coverage_record, CoverageRecord) 
                    or coverage_record.exception is not None):
                    failures.append(provider)
        return failures

    def finalize_batch(self):
        self._db.commit()


class WorkRandomnessUpdateMonitor(WorkSweepMonitor):

    def __init__(self, _db, interval_seconds=3600*24,
                 default_counter=0, batch_size=1000):
        super(WorkRandomnessUpdateMonitor, self).__init__(
            _db, "Work Randomness Updater", interval_seconds=interval_seconds,
            default_counter=default_counter, batch_size=batch_size)

    def run_once(self, offset):
        new_offset = offset + self.batch_size
        text = "update works set random=random() where id >= :offset and id < :new_offset;"
        self._db.execute(text, dict(offset=offset, new_offset=new_offset))
        [[self.max_work_id]] = self._db.execute('select max(id) from works')
        if self.max_work_id < new_offset:
            return 0
        return new_offset


class CustomListEntryLicensePoolUpdateMonitor(CustomListEntrySweepMonitor):

    def __init__(self, _db, interval_seconds=3600*24,
                 default_counter=0, batch_size=100):
        super(CustomListEntryLicensePoolUpdateMonitor, self).__init__(
            _db, "Custom List Entry License Pool Update Monitor",
            interval_seconds=interval_seconds,
            default_counter=default_counter, batch_size=batch_size
        )

    def process_entry(self, entry):
        entry.set_license_pool()

