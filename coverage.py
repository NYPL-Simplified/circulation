from nose.tools import set_trace
import datetime
import logging

from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func

from model import (
    get_one,
    get_one_or_create,
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Timestamp,
)
from metadata_layer import (
    ReplacementPolicy
)

import log # This sets the appropriate log format.

class CoverageFailure(object):
    """Object representing the failure to provide coverage."""

    def __init__(self, provider, obj, exception, transient=True):
        self.obj = obj
        self.output_source = provider.output_source
        self.exception = exception
        self.transient = transient

    def to_coverage_record(self, operation=None):
        """Convert this failure into a CoverageRecord."""
        if not self.transient:
            # This is a persistent error. Turn it into a CoverageRecord
            # so we don't keep trying to provide coverage that isn't
            # gonna happen.
            record, ignore = CoverageRecord.add_for(
                self.obj, self.output_source, operation=operation
            )
            record.exception = self.exception
            return record

    def to_work_coverage_record(self, operation):
        """Convert this failure into a WorkCoverageRecord."""
        if not self.transient:
            # This is a persistent error. Turn it into a CoverageRecord
            # so we don't keep trying to provide coverage that isn't
            # gonna happen.
            record, ignore = WorkCoverageRecord.add_for(
                self.obj, operation=operation
            )
            record.exception = self.exception
            return record

class BaseCoverageProvider(object):

    """Run certain objects through an algorithm. If the algorithm returns
    success, add a coverage record for that object, so the object
    doesn't need to be processed again. If the algorithm returns a
    CoverageFailure, that failure may itself be memorialized as a
    coverage record.

    In CoverageProvider the 'objects' are Identifier objects and the
    coverage records are CoverageRecord objects. In
    WorkCoverageProvider the 'objects' are Work objects and the
    coverage records are WorkCoverageRecord objects.
    """

    def __init__(self, service_name, operation, batch_size=100, 
                 cutoff_time=None):
        """Constructor.

        :param service_name: The name of the coverage provider. Used in
        log messages and Timestamp objects.

        :batch_size: The maximum number of objects that will be processed
        at once.

        :param cutoff_time: Coverage records created before this time
        will be treated as though they did not exist.
        """
        self.service_name = service_name
        self.operation = operation
        self.batch_size = batch_size
        self.cutoff_time = cutoff_time

    @property
    def log(self):
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(self.service_name)
        return self._log        

    def run(self):
        self.log.info("%d items need coverage.", (
            self.items_that_need_coverage().count())
        )
        offset = 0
        while offset is not None:
            offset = self.run_once_and_update_timestamp(offset)

    def run_once_and_update_timestamp(self, offset):
        offset = self.run_once(offset)
        Timestamp.stamp(self._db, self.service_name)
        self._db.commit()
        return offset

    def run_on_specific_identifiers(self, identifiers):
        """Split a specific set of Identifiers into batches and process one
        batch at a time.

        This is for use by IdentifierInputScript.

        :return: The same (counts, records) 2-tuple as
            process_batch_and_handle_results.
        """
        index = 0
        successes = 0
        transient_failures = 0
        persistent_failures = 0
        records = []

        # Of all the items that need coverage, find the intersection
        # with the given list of items.
        need_coverage = self.items_that_need_coverage(identifiers).all()

        # Treat any items with up-to-date coverage records as
        # automatic successes.
        #
        # NOTE: We won't actually be returning those coverage records
        # in `records`, since items_that_need_coverage() filters them
        # out, but nobody who calls this method really needs those
        # records.
        automatic_successes = len(identifiers) - len(need_coverage)
        successes += automatic_successes
        self.log.info("%d automatic successes.", successes)

        # Iterate over any items that were not automatic
        # successes.
        while index < len(need_coverage):
            batch = need_coverage[index:index+self.batch_size]
            (s, t, p), r = self.process_batch_and_handle_results(batch)
            successes += s
            transient_failures += t
            persistent_failures += p
            records += r
            self._db.commit()
            index += self.batch_size
        return (successes, transient_failures, persistent_failures), records

    def run_once(self, offset):
        batch = self.items_that_need_coverage().limit(
            self.batch_size).offset(offset)

        if not batch.count():
            # The batch is empty. We're done.
            return None
        (successes, transient_failures, persistent_failures), results = (
            self.process_batch_and_handle_results(batch)
        )

        # Ignore transient failures.
        return offset + transient_failures

    def process_batch_and_handle_results(self, batch):
        """:return: A 2-tuple (counts, records). 

        `counts` is a 3-tuple (successes, transient failures,
        persistent_failures).

        `records` is a mixed list of CoverageRecord objects (for
        successes and persistent failures) and CoverageFailure objects
        (for transient failures).
        """

        offset_increment = 0
        results = self.process_batch(batch)
        successes = 0
        transient_failures = 0
        persistent_failures = 0
        records = []
        for item in results:
            if isinstance(item, CoverageFailure):
                if item.transient:
                    # Ignore this error for now, but come back to it
                    # on the next run.
                    self.log.warn(
                        "Transient failure covering %r: %s", 
                        item, item.exception
                    )
                    transient_failures += 1
                    record = item
                else:
                    # Create a CoverageRecord memorializing this
                    # failure. It won't show up anymore, on this 
                    # run or subsequent runs.
                    persistent_failures += 1
                    record = self.record_failure_as_coverage_record(item)
            else:
                # Count this as a success and add a CoverageRecord for
                # it. It won't show up anymore, on this run or
                # subsequent runs.
                successes += 1
                record, ignore = self.add_coverage_record_for(item)
            records.append(record)

        # Perhaps some records were ignored--they neither succeeded nor
        # failed. Ignore them on this run and try them again later.
        if isinstance(batch, list):
            batch_size = len(batch)
        else:
            batch_size = batch.count()
        num_ignored = max(0, batch_size - len(results))

        self.log.info(
            "Batch processed with %d successes, %d transient failures, %d persistent failures, %d ignored.",
            successes, transient_failures, persistent_failures, num_ignored
        )

        # Finalize this batch before moving on to the next one.
        self.finalize_batch()

        # For all purposes outside this method, treat an ignored identifier
        # as a transient failure.
        transient_failures += num_ignored
        return (successes, transient_failures, persistent_failures), records

    def process_batch(self, batch):
        """Do what it takes to give CoverageRecords to a batch of
        items.

        :return: A mixed list of CoverageRecords and CoverageFailures.
        """
        results = []
        for item in batch:
            result = self.process_item(item)
            if result:
                results.append(result)
        return results

    def should_update(self, coverage_record):
        """Should we do the work to update the given CoverageRecord?"""
        if coverage_record is None:
            # An easy decision -- there is no existing CoverageRecord,
            # so we need to do the work.
            return True

        if self.cutoff_time is None:
            # An easy decision -- without a cutoff_time, once we
            # create a CoverageRecord we never update it.
            return False

        # We update a CoverageRecord if it was last updated before
        # cutoff_time.
        return coverage_record.timestamp < self.cutoff_time

    def finalize_batch(self):
        """Do whatever is necessary to complete this batch before moving on to
        the next one.
        
        e.g. uploading a bunch of assets to S3.
        """
        pass

    #
    # Subclasses must implement these virtual methods.
    #

    def items_that_need_coverage(self, identifiers):
        """Create a database query returning only those items that
        need coverage.

        :param subset: A list of Identifier objects. If present, return
        only items that need coverage *and* are associated with one
        of these identifiers.

        Implemented in CoverageProvider and WorkCoverageProvider.
        """
        raise NotImplementedError()

    def add_coverage_record_for(self, item):
        """Add a coverage record for the given item.

        Implemented in CoverageProvider and WorkCoverageProvider.
        """
        raise NotImplementedError()
        
    def record_failure_as_coverage_record(self, failure):
        """Convert the given CoverageFailure to a coverage record.

        Implemented in CoverageProvider and WorkCoverageProvider.
        """
        raise NotImplementedError()

    def process_item(self, item):
        """Do the work necessary to give coverage to one specific item.

        Since this is where the actual work happens, this is not
        implemented in CoverageProvider or WorkCoverageProvider, and
        must be handled in a subclass.
        """
        raise NotImplementedError()


class CoverageProvider(BaseCoverageProvider):

    """Run Identifiers of certain types (the input_identifier_types)
    through code associated with a DataSource (the
    `output_source`). If the code returns success, add a
    CoverageRecord for the Edition and the output DataSource, so that
    the record doesn't get processed next time.
    """

    # Does this CoverageProvider get its data from a source that also
    # provides licenses for books?
    CAN_CREATE_LICENSE_POOLS = False

    def __init__(self, service_name, input_identifier_types, output_source,
                 batch_size=100, cutoff_time=None, operation=None):
        super(CoverageProvider, self).__init__(
            service_name, operation, batch_size, cutoff_time
        )
        self._db = Session.object_session(output_source)
        if input_identifier_types and not isinstance(input_identifier_types, list):
            input_identifier_types = [input_identifier_types]
        self.input_identifier_types = input_identifier_types
        self.output_source_name = output_source.name

    def ensure_coverage(self, item, force=False):
        """Ensure coverage for one specific item.

        TODO: Could potentially be moved into BaseCoverageProvider.

        :param force: Run the coverage code even if an existing
           covreage record for this item was created after
           `self.cutoff_time`.

        :return: Either a coverage record or a CoverageFailure.

        """
        if isinstance(item, Identifier):
            identifier = item
        else:
            identifier = item.primary_identifier
        coverage_record = get_one(
            self._db, CoverageRecord,
            identifier=identifier,
            data_source=self.output_source,
            operation=self.operation,
            on_multiple='interchangeable',
        )
        if not force and not self.should_update(coverage_record):
            return coverage_record

        counts, records = self.process_batch_and_handle_results(
            [identifier]
        )
        if records:
            coverage_record = records[0]
        else:
            coverage_record = None
        return coverage_record

    @property
    def output_source(self):
        """Look up the DataSource object corresponding to the
        service we're running this data through.

        Out of an excess of caution, we look up the DataSource every
        time, rather than storing it, in case a CoverageProvider is
        ever used in an environment where the database session is
        scoped (e.g. the circulation manager).
        """
        return DataSource.lookup(self._db, self.output_source_name)

    def license_pool(self, identifier):
        """Finds or creates the LicensePool for a given Identifier."""
        license_pool = identifier.licensed_through
        if not license_pool:
            if self.CAN_CREATE_LICENSE_POOLS:
                # The source of this data also provides license
                # pools, so it's okay to automatically create
                # a license pool for this book.
                license_pool, ignore = LicensePool.for_foreign_id(
                    self._db, self.output_source, identifier.type, 
                    identifier.identifier
                )
            else:
                return None
        return license_pool

    def edition(self, identifier):
        """Finds or creates the Edition for a given Identifier."""
        license_pool = self.license_pool(identifier)
        if not license_pool:
            e = "No license pool available"
            return CoverageFailure(self, identifier, e, transient=True)

        edition, ignore = Edition.for_foreign_id(
            self._db, license_pool.data_source, identifier.type,
            identifier.identifier
        )
        return edition

    def work(self, identifier):
        """Finds or creates the Work for a given Identifier.
        
        :return: The Work (if it could be found) or an appropriate
        CoverageFailure (if not).
        """
        license_pool = self.license_pool(identifier)
        if not license_pool:
            e = "No license pool available"
            return CoverageFailure(self, identifier, e, transient=True)
        work, created = license_pool.calculate_work(even_if_no_author=True)
        if not work:
            e = "Work could not be calculated"
            return CoverageFailure(self, identifier, e, transient=True)
        return work


    def set_metadata(self, identifier, metadata, 
                     metadata_replacement_policy=None):
        return self.set_metadata_and_circulation_data(
            identifier, metadata, None, metadata_replacement_policy,
        )

    def set_metadata_and_circulation_data(self, identifier, metadata, circulationdata, 
        metadata_replacement_policy=None, 
        circulationdata_replacement_policy=None, 
    ):
        """
        Performs the function of the old set_metadata.  Finds or creates the Edition 
        and the LicensePool for the passed-in Identifier, updates them, 
        then finds or creates a Work for them.

        TODO:  Makes assumption of one license pool per identifier.  In a 
        later branch, this will change.
        TODO:  Update doc string removing reference to past function.

        :return: The Identifier (if successful) or an appropriate
        CoverageFailure (if not).
        """

        if not metadata and not circulationdata:
            e = "Received neither metadata nor circulation data from input source"
            return CoverageFailure(self, identifier, e, transient=True)


        if metadata:
            result = self._set_metadata(identifier, metadata, metadata_replacement_policy)
            if isinstance(result, CoverageFailure):
                return result

        if circulationdata:
            result = self._set_circulationdata(identifier, circulationdata, circulationdata_replacement_policy)
            if isinstance(result, CoverageFailure):
                return result

        # now that made sure that have an edition and a pool on the identifier, 
        # can try to make work
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work

        return identifier


    def _set_metadata(self, identifier, metadata, 
                     metadata_replacement_policy=None
    ):
        """Finds or creates the Edition for an Identifier, updates it
        with the given metadata.

        :return: The Identifier (if successful) or an appropriate
        CoverageFailure (if not).
        """
        metadata_replacement_policy = metadata_replacement_policy or (
            ReplacementPolicy.from_metadata_source()
        )

        edition = self.edition(identifier)
        if isinstance(edition, CoverageFailure):
            return edition

        if not metadata:
            e = "Did not receive metadata from input source"
            return CoverageFailure(self, identifier, e, transient=True)

        try:
            metadata.apply(
                edition, replace=metadata_replacement_policy,
            )
        except Exception as e:
            self.log.warn(
                "Error applying metadata to edition %d: %s",
                edition.id, e, exc_info=e
            )
            return CoverageFailure(self, identifier, repr(e), transient=True)

        return identifier


    def _set_circulationdata(self, identifier, circulationdata, 
                     circulationdata_replacement_policy=None
    ):
        """Finds or creates the LicensePool for an Identifier, updates it
        with the given circulationdata, then creates a Work for the book.

        TODO:  Makes assumption of one license pool per identifier.  In a 
        later branch, this will change.

        :return: The Identifier (if successful) or an appropriate
        CoverageFailure (if not).
        """
        circulationdata_replacement_policy = circulationdata_replacement_policy or (
            ReplacementPolicy.from_license_source()
        )

        pool = self.license_pool(identifier)
        if isinstance(pool, CoverageFailure):
            return pool

        if not circulationdata:
            e = "Did not receive circulationdata from input source"
            return CoverageFailure(self, identifier, e, transient=True)

        try:
            circulationdata.apply(
                pool, replace=circulationdata_replacement_policy,
            )
        except Exception as e:
            self.log.warn(
                "Error applying circulationdata to pool %d: %s",
                pool.id, e, exc_info=e
            )
            return CoverageFailure(self, identifier, repr(e), transient=True)

        return identifier


    def set_presentation_ready(self, identifier):
        """Set a Work presentation-ready."""
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work
        work.set_presentation_ready()
        return identifier

    #
    # Implementation of BaseCoverageProvider virtual methods.
    #

    def items_that_need_coverage(self, identifiers=None):
        """Find all items lacking coverage from this CoverageProvider.

        Items should be Identifiers, though Editions should also work.

        By default, all identifiers of the `input_identifier_types` which
        don't already have coverage are chosen.
        """
        qu = Identifier.missing_coverage_from(
            self._db, self.input_identifier_types, self.output_source,
            count_as_missing_before=self.cutoff_time, operation=self.operation
        )
        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))
        return qu

    def add_coverage_record_for(self, item):
        return CoverageRecord.add_for(
            item, data_source=self.output_source, operation=self.operation
        )

    def record_failure_as_coverage_record(self, failure):
        return failure.to_coverage_record(operation=self.operation)

class WorkCoverageProvider(BaseCoverageProvider):

    #
    # Implementation of BaseCoverageProvider virtual methods.
    #

    def items_that_need_coverage(self, identifiers=None):
        """Find all Works lacking coverage from this CoverageProvider.

        By default, all Works which don't already have coverage are
        chosen.

        :param: Only Works connected with one of the given identifiers
        are chosen.
        """
        qu = Work.missing_coverage_from(
            self._db, operation=self.operation, 
            count_as_missing_before=self.cutoff_time
        )
        if identifiers:
            ids = [x.id for x in identifiers]
            qu = qu.join(Work.license_pools).filter(
                LicensePool.identifier_id.in_(ids)
            )
        return qu


    def add_coverage_record_for(self, work):
        return WorkCoverageRecord.add_for(
            work, operation=self.operation
        )

    def record_failure_as_coverage_record(self, failure):
        return failure.to_work_coverage_record(operation=self.operation)


class BibliographicCoverageProvider(CoverageProvider):
    """Fill in bibliographic metadata for records.

    Ensures that a given DataSource provides coverage for all
    identifiers of the type primarily used to identify books from that
    DataSource.

    e.g. ensures that we get Overdrive coverage for all Overdrive IDs.
    """

    CAN_CREATE_LICENSE_POOLS = True

    def __init__(self, _db, api, datasource, batch_size=10,
                 metadata_replacement_policy=None, circulationdata_replacement_policy=None, 
                 cutoff_time=None
    ):
        self._db = _db
        self.api = api
        output_source = DataSource.lookup(_db, datasource)
        input_identifier_types = [output_source.primary_identifier_type]
        service_name = "%s Bibliographic Monitor" % datasource
        metadata_replacement_policy = (
            metadata_replacement_policy or ReplacementPolicy.from_metadata_source()
        )
        circulationdata_replacement_policy = (
            circulationdata_replacement_policy or ReplacementPolicy.from_license_source()
        )
        self.metadata_replacement_policy = metadata_replacement_policy
        self.circulationdata_replacement_policy = circulationdata_replacement_policy
        super(BibliographicCoverageProvider, self).__init__(
            service_name,
            input_identifier_types, output_source,
            batch_size=batch_size,
            cutoff_time=cutoff_time
        )

    def process_batch(self, identifiers):
        """Returns a list of successful identifiers and CoverageFailures"""
        results = []
        for identifier in identifiers:
            result = self.process_item(identifier)
            if not isinstance(result, CoverageFailure):
                self.handle_success(identifier)
            results.append(result)
        return results

    def handle_success(self, identifier):
        self.set_presentation_ready(identifier)
