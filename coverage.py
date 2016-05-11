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
        if not self.transient:
            # This is a permanent error. Turn it into a CoverageRecord
            # so we don't keep trying to provide coverage that isn't
            # gonna happen.
            record, ignore = CoverageRecord.add_for(
                self.obj, self.output_source, operation=operation
            )
            record.exception = self.exception
            return record


class CoverageProvider(object):

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
                 workset_size=100, cutoff_time=None, operation=None):
        self._db = Session.object_session(output_source)
        self.service_name = service_name

        if not isinstance(input_identifier_types, list):
            input_identifier_types = [input_identifier_types]
        self.input_identifier_types = input_identifier_types
        self.output_source = output_source
        self.workset_size = workset_size
        self.cutoff_time = cutoff_time
        self.operation = operation

    @property
    def log(self):
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(self.service_name)
        return self._log        

    @property
    def items_that_need_coverage(self):
        """Find all items lacking coverage from this CoverageProvider.

        Items should be Identifiers, though Editions should also work.

        By default, all identifiers of the `input_identifier_types` which
        don't already have coverage are chosen.

        Items are selected randomly to reduce the effect of
        persistent errors.
        """
        return Identifier.missing_coverage_from(
            self._db, self.input_identifier_types, self.output_source,
            count_as_missing_before=self.cutoff_time
        )

    def run(self):
        self.log.info("%d items need coverage.", (
            self.items_that_need_coverage.count())
        )
        offset = 0
        while offset is not None:
            offset = self.run_once_and_update_timestamp(offset)

    def run_once_and_update_timestamp(self, offset):
        offset = self.run_once(offset)
        Timestamp.stamp(self._db, self.service_name)
        self._db.commit()
        return offset

    def run_on_identifiers(self, identifiers):
        """Split a specific set of identifiers into batches and
        process one batch at a time.

        :return: The same (counts, records) 2-tuple as
            process_batch_and_handle_results.
        """
        index = 0
        successes = 0
        transient_failures = 0
        persistent_failures = 0
        records = []
        while index < len(identifiers):
            batch = identifiers[index:index+self.workset_size]
            (s, t, p), r = self.process_batch_and_handle_results(batch)
            successes += s
            transient_failures += t
            persistent_failures += p
            records += r
            self._db.commit()
            index += self.workset_size
        return (successes, transient_failures, persistent_failures), records

    def run_once(self, offset):
        batch = self.items_that_need_coverage.limit(
            self.workset_size).offset(offset)

        if not batch.count():
            # The batch is empty. We're done.
            return None
        (successes, transient_failures, permanent_failures), results = (
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
        permanent_failures = 0
        records = []
        for item in results:
            if isinstance(item, CoverageFailure):
                if item.transient:
                    # Ignore this error for now, but come back to it
                    # on the next run.
                    transient_failures += 1
                    record = item
                else:
                    # Create a CoverageRecord memorializing this
                    # failure. It won't show up anymore, on this 
                    # run or subsequent runs.
                    permanent_failures += 1
                    record = item.to_coverage_record(operation=self.operation)
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
            "Batch processed with %d successes, %d transient failures, %d ignored, %d permanent failures.",
            successes, transient_failures, permanent_failures, num_ignored
        )

        # Finalize this batch before moving on to the next one.
        self.finalize_batch()

        # For all purposes outside this method, treat an ignored identifier
        # as a transient failure.
        transient_failures += num_ignored
        return (successes, transient_failures, permanent_failures), records

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

    def ensure_coverage(self, item, force=False):
        """Ensure coverage for one specific item.

        :return: The same (counts, records) 2-tuple as
            process_batch_and_handle_results. `records` will either be
            empty (indicating that coverage was already present) or it
            will contain a single item (either a CoverageRecord or a
            CoverageFailure).
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
        if force or coverage_record is None:
            return self.process_batch_and_handle_results([identifier])
        return ((0, 0, 0), [])

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
        work, created = license_pool.calculate_work(
            even_if_no_author=True, known_edition=self.edition(identifier)
        )
        if not work:
            e = "Work could not be calculated"
            return CoverageFailure(self, identifier, e, transient=True)
        return work

    def set_metadata(self, identifier, metadata, 
                     metadata_replacement_policy=None
    ):
        """Finds or creates the Edition for an Identifier, updates it
        with the given metadata, then creates a Work for the book.

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

        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work

        return identifier

    def set_presentation_ready(self, identifier):
        """Set a Work presentation-ready."""
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work
        work.set_presentation_ready()
        return identifier

    def add_coverage_record_for(self, identifier):
        return CoverageRecord.add_for(
            identifier, self.output_source, operation=self.operation
        )

    def process_item(self, identifier):
        raise NotImplementedError()

    def finalize_batch(self):
        """Do whatever is necessary to complete this batch before moving on to
        the next one.
        
        e.g. uploading a bunch of assets to S3.
        """
        pass


class BibliographicCoverageProvider(CoverageProvider):
    """Fill in bibliographic metadata for records.

    Ensures that a given DataSource provides coverage for all
    identifiers of the type primarily used to identify books from that
    DataSource.

    e.g. ensures that we get Overdrive coverage for all Overdrive IDs.
    """

    CAN_CREATE_LICENSE_POOLS = True

    def __init__(self, _db, api, datasource, workset_size=10,
                 metadata_replacement_policy=None
    ):
        self._db = _db
        self.api = api
        output_source = DataSource.lookup(_db, datasource)
        input_identifier_types = [output_source.primary_identifier_type]
        service_name = "%s Bibliographic Monitor" % datasource
        metadata_replacement_policy = (
            metadata_replacement_policy or ReplacementPolicy.from_metadata_source()
        )
        self.metadata_replacement_policy = metadata_replacement_policy
        super(BibliographicCoverageProvider, self).__init__(
            service_name,
            input_identifier_types, output_source,
            workset_size=workset_size,
        )

    def process_batch(self):
        """Returns a list of successful identifiers and CoverageFailures"""
        raise NotImplementedError
