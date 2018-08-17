from nose.tools import set_trace
import datetime
import os
import logging
import time
import traceback
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import (
    or_,
    and_,
)

import log # This sets the appropriate log format and level.
from config import Configuration
from coverage import CoverageFailure
from model import (
    get_one,
    get_one_or_create,
    CachedFeed,
    Collection,
    CollectionMissing,
    CoverageRecord,
    Credential,
    Edition,
    ExternalIntegration,
    CustomListEntry,
    Identifier,
    LicensePool,
    PresentationCalculationPolicy,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
)


class Monitor(object):
    """A Monitor is responsible for running some piece of code as a
    job. When invoked, a Monitor will decide whether to do some work
    based on how long it's been since the last invocation. If a
    Monitor does work, it will update a Timestamp object to track the
    last time the work was done.

    A Monitor will run once and then stop. To repeatedly run a
    Monitor, you'll need to repeatedly invoke the Monitor from some
    external source such as a cron job.

    This class is designed to be subclassed rather than instantiated
    directly. Subclasses must define SERVICE_NAME. Subclasses may
    define replacement values for KEEP_TIMESTAMP, INTERVAL_SECONDS,
    and DEFAULT_START_TIME.

    Although any Monitor may be associated with a Collection, it's
    most useful to subclass CollectionMonitor if you're writing code
    that needs to be run on every Collection of a certain type.
    """
    # In your subclass, set this to the name of the service,
    # e.g. "Overdrive Circulation Monitor". All instances of your
    # subclass will give this as their service name and track their
    # Timestamps under this name.
    SERVICE_NAME = None

    # If this is set to False, this Monitor does not keep a timestamp.
    # When it runs, the work will be done but no Timestamp will be
    # created, and any existing timestamp will not be updated.
    KEEP_TIMESTAMP = True

    # The Monitor code will not run more than once every this number
    # of seconds. If the Monitor is invoked and its Timestamp is not
    # this old, the Monitor will do no work. If this is set to 0, the
    # Monitor code will run every time it's invoked.
    INTERVAL_SECONDS = 60

    # Some useful relative constants for DEFAULT_START_TIME (below).
    ONE_MINUTE_AGO = datetime.timedelta(seconds=60)
    ONE_YEAR_AGO = datetime.timedelta(seconds=60*60*24*365)
    NEVER = object()

    # If there is no Timestamp for this Monitor, this time will be
    # passed into `run_once()` as the `start_time` parameter.
    DEFAULT_START_TIME = ONE_MINUTE_AGO

    # When the Timestamp for this Monitor is created, this value will
    # be set for `Timestamp.counter`.
    #
    # This is only used by the SweepMonitor subclass.
    DEFAULT_COUNTER = None

    def __init__(self, _db, collection=None):
        self._db = _db
        cls = self.__class__
        if not self.SERVICE_NAME and not cls.SERVICE_NAME:
            raise ValueError("%s must define SERVICE_NAME." % cls.__name__)
        self.service_name = self.SERVICE_NAME
        self.interval_seconds = cls.INTERVAL_SECONDS
        self.keep_timestamp = cls.KEEP_TIMESTAMP
        default_start_time = cls.DEFAULT_START_TIME
        if isinstance(default_start_time, datetime.timedelta):
            default_start_time = (
                datetime.datetime.utcnow() - default_start_time
            )
        self.default_start_time = default_start_time
        self.default_counter = cls.DEFAULT_COUNTER

        # We store the collection ID rather than the Collection to
        # avoid breakage in case an app server with a scoped session
        # ever uses a Monitor.
        self.collection_id = None
        if collection:
            self.collection_id = collection.id

    @property
    def log(self):
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(self.service_name)
        return self._log

    @property
    def collection(self):
        """Retrieve the Collection object associated with this
        Monitor.
        """
        if not self.collection_id:
            return None
        return get_one(self._db, Collection, id=self.collection_id)

    def timestamp(self):
        """Find or create the Timestamp for this Monitor."""
        if self.default_start_time is self.NEVER:
            initial_timestamp = None
        elif not self.default_start_time:
            initial_timestamp = datetime.datetime.utcnow()
        else:
            initial_timestamp = self.default_start_time
        timestamp, new = get_one_or_create(
            self._db, Timestamp,
            service=self.service_name,
            collection=self.collection,
            create_method_kwargs=dict(
                timestamp=initial_timestamp,
                counter=self.default_counter,
            )
        )
        return timestamp

    def run(self):
        """Do the Monitor's work, assuming it's not too soon since
        the last time.
        """
        if self.keep_timestamp:
            timestamp = self.timestamp()
            start = timestamp.timestamp or self.default_start_time
        else:
            timestamp = None
            start = self.default_start_time

        if start == self.NEVER:
            start = None

        cutoff = datetime.datetime.utcnow()
        new_timestamp_value = self.run_once(start, cutoff) or cutoff
        duration = datetime.datetime.utcnow() - cutoff
        self.cleanup()
        self.log.info(
            "Ran %s monitor in %.2f sec.", self.service_name,
            duration.total_seconds()
        )
        if self.keep_timestamp:
            # Update the Timestamp value.
            timestamp.timestamp = new_timestamp_value
        self._db.commit()

    def run_once(self, start, cutoff):
        """Do the actual work of the Monitor.

        :param start: The last time the Monitor was run.

        :param cutoff: It's not necessary to do work for anything that
            happened after this time. Usually, this is the current time.
        """
        raise NotImplementedError()

    def cleanup(self):
        """Do any work that needs to be done at the end, once the main work
        has completed successfully.
        """
        pass


class CollectionMonitor(Monitor):
    """A Monitor that does something for all Collections that come
    from a certain provider.

    This class is designed to be subclassed rather than instantiated
    directly. Subclasses must define SERVICE_NAME and
    PROTOCOL. Subclasses may define replacement values for
    KEEP_TIMESTAMP, INTERVAL_SECONDS, and DEFAULT_START_TIME.
    """

    # Set this to the name of the license provider managed by this
    # Monitor. If this value is set, the CollectionMonitor can only be
    # instantiated with Collections that get their licenses from this
    # provider. If this is unset, the CollectionMonitor can be
    # instantiated with any Collection, or with no Collection at all.
    PROTOCOL = None

    def __init__(self, _db, collection):
        cls = self.__class__
        self.protocol = cls.PROTOCOL
        if self.protocol:
            if collection is None:
                raise CollectionMissing()
        if self.protocol and collection.protocol != self.protocol:
            raise ValueError(
                "Collection protocol (%s) does not match Monitor protocol (%s)" % (
                    collection.protocol, cls.PROTOCOL
                )
            )

        super(CollectionMonitor, self).__init__(_db, collection)

    @classmethod
    def all(cls, _db, **constructor_kwargs):
        """Yield a sequence of CollectionMonitor objects: one for every
        Collection associated with cls.PROTOCOL.

        Monitors that have no Timestamp will be yielded first. After that,
        Monitors with older Timestamps will be yielded before Monitors with
        newer timestamps.

        :param constructor_kwargs: These keyword arguments will be passed
        into the CollectionMonitor constructor.
        """
        service_match = or_(Timestamp.service==cls.SERVICE_NAME,
                            Timestamp.service==None)
        collections = Collection.by_protocol(_db, cls.PROTOCOL).outerjoin(
            Timestamp,
            and_(
                Timestamp.collection_id==Collection.id,
                service_match,
            )
        )
        collections = collections.order_by(
            Timestamp.timestamp.asc().nullsfirst()
        )
        for collection in collections:
            yield cls(_db=_db, collection=collection, **constructor_kwargs)


class SweepMonitor(CollectionMonitor):
    """A monitor that does some work for every item in a database table,
    then stops.

    Progress through the table is stored in the Timestamp, so that if
    the Monitor crashes, the next time the Monitor is run, it starts
    at the item that caused the crash, rather than starting from the
    beginning of the table.
    """

    # The completion of each individual item should be logged at
    # this log level.
    COMPLETION_LOG_LEVEL = logging.INFO

    # Items will be processed in batches of this size.
    DEFAULT_BATCH_SIZE = 100

    INTERVAL_SECONDS = 3600

    DEFAULT_COUNTER = 0

    # The model class corresponding to the database table that this
    # Monitor sweeps over. This class must keep its primary key in the
    # `id` field.
    MODEL_CLASS = None

    def __init__(self, _db, collection=None, batch_size=None):
        cls = self.__class__
        if not batch_size or batch_size < 0:
            batch_size = cls.DEFAULT_BATCH_SIZE
        self.batch_size = batch_size
        if not cls.MODEL_CLASS:
            raise ValueError("%s must define MODEL_CLASS" % cls.__name__)
        self.model_class = cls.MODEL_CLASS
        super(SweepMonitor, self).__init__(_db, collection=collection)

    def run(self):
        timestamp = self.timestamp()
        offset = timestamp.counter

        started_at = datetime.datetime.utcnow()
        while True:
            start_time = time.time()
            old_offset = offset
            try:
                new_offset = self.process_batch(offset)
            except Exception, e:
                self.log.error("Error during run: %s", e, exc_info=e)
                break

            # We completed one batch of work. Update the Timestamp so
            # we don't do the same work again.
            timestamp.counter = new_offset
            self._db.commit()

            if old_offset != new_offset:
                end_time = time.time()
                self.log.debug(
                    "%s monitor went from offset %s to %s in %.2f sec",
                    self.service_name, offset, new_offset,
                    (end_time-start_time)
                )
            offset = new_offset
            if offset == 0:
                # We completed a sweep. We're done.
                self.cleanup()
                break

    def process_batch(self, offset):
        """Process one batch of work."""
        offset = offset or 0
        items = self.fetch_batch(offset).all()
        if items:
            self.process_items(items)
            # We've completed a batch. Return the ID of the last item
            # in the batch so we don't do this work again.
            return items[-1].id
        else:
            # There are no more items in this database table, so we
            # are done with the sweep. Reset the counter.
            return 0

    def process_items(self, items):
        """Process a list of items."""
        for item in items:
            self.process_item(item)
            self.log.log(self.COMPLETION_LOG_LEVEL, "Completed %r", item)

    def fetch_batch(self, offset):
        """Retrieve one batch of work from the database."""
        q = self.item_query().filter(self.model_class.id > offset).order_by(
            self.model_class.id).limit(self.batch_size)
        return q

    def item_query(self):
        """Find the items that need to be processed in the sweep.

        :return: A query object.
        """
        # Start by getting everything in the table.
        qu = self._db.query(self.model_class)
        if self.collection:
            # Restrict to only those items associated with self.collection
            # somehow.
            qu = self.scope_to_collection(qu, self.collection)
        qu = qu.order_by(self.model_class.id)
        return qu

    def scope_to_collection(self, qu, collection):
        """Restrict the given query so that it only finds items
        associated with the given collection.

        :param qu: A query object.
        :param collection: A Collection object, presumed to not be None.
        """
        raise NotImplementedError()

    def process_item(self, item):
        """Do the work that needs to be done for a given item."""
        raise NotImplementedError()


class IdentifierSweepMonitor(SweepMonitor):
    """A Monitor that does some work for every Identifier."""
    MODEL_CLASS = Identifier

    def scope_to_collection(self, qu, collection):
        """Only find Identifiers licensed through the given Collection."""
        return qu.join(Identifier.licensed_through).filter(
            LicensePool.collection==collection
        )


class SubjectSweepMonitor(SweepMonitor):
    """A Monitor that does some work for every Subject."""
    MODEL_CLASS = Subject

    # It's usually easy to process a Subject, so make the batch size
    # large.
    DEFAULT_BATCH_SIZE = 500

    def __init__(self, _db, subject_type=None, filter_string=None):
        """Constructor.
        :param subject_type: Only process Subjects of this type.
        :param filter_string: Only process Subjects whose .identifier
           or .name contain this string.
        """
        super(SubjectSweepMonitor, self).__init__(_db, None)
        self.subject_type = subject_type
        self.filter_string = filter_string

    def item_query(self):
        """Find only Subjects that match the given filters."""
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

    def scope_to_collection(self, qu, collection):
        """Refuse to scope this query to a Collection."""
        return qu


class CustomListEntrySweepMonitor(SweepMonitor):
    """A Monitor that does something to every CustomListEntry."""
    MODEL_CLASS = CustomListEntry

    def scope_to_collection(self, qu, collection):
        """Restrict the query to only find CustomListEntries whose
        Work is in the given Collection.
        """
        return qu.join(CustomListEntry.work).join(Work.license_pools).filter(
            LicensePool.collection==collection
        )


class EditionSweepMonitor(SweepMonitor):
    """A Monitor that does something to every Edition."""
    MODEL_CLASS = Edition

    def scope_to_collection(self, qu, collection):
        """Restrict the query to only find Editions whose
        primary Identifier is licensed to the given Collection.
        """
        return qu.join(Edition.primary_identifier).join(
            Identifier.licensed_through).filter(
                LicensePool.collection==collection
            )


class WorkSweepMonitor(SweepMonitor):
    """A Monitor that does something to every Work."""
    MODEL_CLASS = Work

    def scope_to_collection(self, qu, collection):
        """Restrict the query to only find Works found in the given
        Collection.
        """
        return qu.join(Work.license_pools).filter(
            LicensePool.collection==collection
        )


class PresentationReadyWorkSweepMonitor(WorkSweepMonitor):
    """A Monitor that does something to every presentation-ready Work."""

    def item_query(self):
        return super(
            PresentationReadyWorkSweepMonitor, self).item_query().filter(
                Work.presentation_ready==True
            )

class NotPresentationReadyWorkSweepMonitor(WorkSweepMonitor):
    """A Monitor that does something to every Work that is not
    presentation-ready.
    """

    def item_query(self):
        not_presentation_ready = or_(
            Work.presentation_ready==False,
            Work.presentation_ready==None
        )
        return super(
            NotPresentationReadyWorkSweepMonitor, self).item_query().filter(
                not_presentation_ready
            )


# SweepMonitors that do something specific.

class OPDSEntryCacheMonitor(PresentationReadyWorkSweepMonitor):
    """A Monitor that recalculates the OPDS entries for every
    presentation-ready Work.

    This is different from the OPDSEntryWorkCoverageProvider,
    which only processes works that are missing a WorkCoverageRecord
    with the 'generate-opds' operation.
    """
    SERVICE_NAME = "ODPS Entry Cache Monitor"

    def process_item(self, work):
        work.calculate_opds_entries()


class PermanentWorkIDRefreshMonitor(EditionSweepMonitor):
    """A monitor that calculates or recalculates the permanent work ID for
    every edition.
    """
    SERVICE_NAME = "Permanent work ID refresh"

    def process_item(self, edition):
        edition.calculate_permanent_work_id()


class MakePresentationReadyMonitor(NotPresentationReadyWorkSweepMonitor):
    """A monitor that makes works presentation ready.

    By default this works by passing the work's active edition into
    ensure_coverage() for each of a list of CoverageProviders. If all
    the ensure_coverage() calls succeed, presentation of the work is
    calculated and the work is marked presentation ready.
    """
    SERVICE_NAME = "Make Works Presentation Ready"

    def __init__(self, _db, coverage_providers, collection=None,
                 calculate_work_even_if_no_author=False):
        super(MakePresentationReadyMonitor, self).__init__(_db, collection)
        self.coverage_providers = coverage_providers
        self.calculate_work_even_if_no_author = calculate_work_even_if_no_author
        self.policy = PresentationCalculationPolicy(
            choose_edition=False
        )

    def run(self):
        """Before doing anything, consolidate works."""
        LicensePool.consolidate_works(
            self._db,
            calculate_work_even_if_no_author=self.calculate_work_even_if_no_author
        )
        return super(MakePresentationReadyMonitor, self).run()

    def process_item(self, work):
        """Do the work necessary to make one Work presentation-ready,
        and handle exceptions.
        """
        exception = None

        try:
            self.prepare(work)
        except CoverageProvidersFailed, e:
            exception = "Provider(s) failed: %s" % e
        except Exception, e:
            self.log.error(
                "Exception processing work %r", work, exc_info=e
            )
            exception = str(e)

        if exception:
            # Unlike with most Monitors, an exception is not a good
            # reason to stop doing our job. Note it inside the Work
            # and keep going.
            work.presentation_ready_exception = exception
        else:
            # Success!
            work.calculate_presentation(self.policy)
            work.set_presentation_ready()

    def prepare(self, work):
        """Try to make a single Work presentation-ready.

        :raise CoverageProvidersFailed: If we can't make a Work
            presentation-ready because one or more CoverageProviders
            failed.
        """
        edition = work.presentation_edition
        if not edition:
            work = work.calculate_presentation()
        identifier = edition.primary_identifier
        overall_success = True
        failures = []
        for provider in self.coverage_providers:
            covered_types = provider.input_identifier_types
            if covered_types and identifier.type in covered_types:
                coverage_record = provider.ensure_coverage(identifier)
                if (not isinstance(coverage_record, CoverageRecord)
                    or coverage_record.status != CoverageRecord.SUCCESS
                    or coverage_record.exception is not None):
                    # This provider has failed.
                    failures.append(provider)
        if failures:
            raise CoverageProvidersFailed(failures)
        return failures


class CoverageProvidersFailed(Exception):
    """We tried to run CoverageProviders on a Work's identifier,
    but some of the providers failed.
    """
    def __init__(self, failed_providers):
        self.failed_providers = failed_providers
        super(CoverageProvidersFailed, self).__init__(
            ", ".join([x.service_name for x in failed_providers])
        )


class WorkRandomnessUpdateMonitor(WorkSweepMonitor):
    """Update the random value associated with each work.

    (This value is used when randomly choosing books to feature.)
    """

    SERVICE_NAME = "Work Randomness Updater"
    INTERVAL_SECONDS = 3600 * 24
    DEFAULT_BATCH_SIZE = 1000

    def process_batch(self, offset):
        """Unlike other Monitors, this one leaves process_item() undefined
        because it works on a large number of Works at once using raw
        SQL.
        """
        new_offset = offset + self.batch_size
        text = "update works set random=random() where id >= :offset and id < :new_offset;"
        self._db.execute(text, dict(offset=offset, new_offset=new_offset))
        [[self.max_work_id]] = self._db.execute('select max(id) from works')
        if self.max_work_id < new_offset:
            # We're all done.
            return 0
        return new_offset


class CustomListEntryWorkUpdateMonitor(CustomListEntrySweepMonitor):

    """Set or reset the Work associated with each custom list entry."""
    SERVICE_NAME = "Update Works for custom list entries"
    INTERVAL_SECONDS = 3600 * 24
    DEFAULT_BATCH_SIZE = 100

    def process_item(self, item):
        item.set_work()


class ReaperMonitor(Monitor):
    """A Monitor that deletes database rows that have expired but
    have no other process to delete them.

    A subclass of ReaperMonitor MUST define values for the following
    constants:
    MODEL_CLASS - The model class this monitor is reaping, e.g. Credential.
    TIMESTAMP_FIELD - Within the model class, the DateTime field to be
       used when deciding which rows to deleting,
       e.g. 'expires'. The reaper will be more efficient if there's
       an index on this field.
    MAX_AGE - A datetime.timedelta or number of days representing
        the time that must pass before an item can be safely deleted.

    """
    MODEL_CLASS = None
    TIMESTAMP_FIELD = None
    MAX_AGE = None

    REGISTRY = []

    def __init__(self, *args, **kwargs):
        self.SERVICE_NAME = "Reaper for %s.%s" % (
            self.MODEL_CLASS.__name__,
            self.TIMESTAMP_FIELD
        )
        super(ReaperMonitor, self).__init__(*args, **kwargs)

    @property
    def cutoff(self):
        """Items with a timestamp earlier than this time will be reaped.
        """
        if isinstance(self.MAX_AGE, datetime.timedelta):
            max_age = self.MAX_AGE
        else:
            max_age = datetime.timedelta(days=self.MAX_AGE)
        return datetime.datetime.utcnow() - max_age

    @property
    def timestamp_field(self):
        return getattr(self.MODEL_CLASS, self.TIMESTAMP_FIELD)

    @property
    def where_clause(self):
        """A SQLAlchemy clause that identifies the database rows to be reaped.
        """
        return self.timestamp_field < self.cutoff

    def run_once(self, *args, **kwargs):
        rows_deleted = self.query().delete(synchronize_session=False)
        self.log.info("Deleted %d row(s)", rows_deleted)

    def query(self):
        return self._db.query(self.MODEL_CLASS).filter(self.where_clause)

# ReaperMonitors that do something specific.

class CachedFeedReaper(ReaperMonitor):
    """Removed cached feeds older than thirty days."""
    MODEL_CLASS = CachedFeed
    TIMESTAMP_FIELD = 'timestamp'
    MAX_AGE = 30
ReaperMonitor.REGISTRY.append(CachedFeedReaper)


class CredentialReaper(ReaperMonitor):
    """Remove Credentials that expired more than a day ago."""
    MODEL_CLASS = Credential
    TIMESTAMP_FIELD = 'expires'
    MAX_AGE = 1
ReaperMonitor.REGISTRY.append(CredentialReaper)
