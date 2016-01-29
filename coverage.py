from nose.tools import set_trace
import datetime
import logging

from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func

from model import (
    get_one,
    get_one_or_create,
    CoverageRecord,
    Edition,
    Identifier,
    Timestamp,
)
import log # This sets the appropriate log format.

class CoverageFailure(object):
    """Object representing the failure to provide coverage."""

    def __init__(self, provider, obj, exception, transient=True):
        self.obj = obj
        self.output_source = provider.output_source
        self.exception = exception
        self.transient = transient

    def to_coverage_record(self):
        if not self.transient:
            # This is a permanent error. Turn it into a CoverageRecord
            # so we don't keep trying to provide coverage that isn't
            # gonna happen.
            record, ignore = CoverageRecord.add_for(self.obj, self.output_source)
            record.exception = self.exception
            return record


class CoverageProvider(object):

    """Run Editions from one DataSource (the input DataSource) through
    code associated with another DataSource (the output
    DataSource). If the code returns success, add a CoverageRecord for
    the Edition and the output DataSource, so that the record
    doesn't get processed next time.
    """

    def __init__(self, service_name, input_sources, output_source,
                 workset_size=100):
        self._db = Session.object_session(output_source)
        self.service_name = service_name
        if not isinstance(input_sources, list):
            input_sources = [input_sources]
        self.input_sources = input_sources
        self.output_source = output_source
        self.workset_size = workset_size

    @property
    def log(self):
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(self.service_name)
        return self._log        

    @property
    def editions_that_need_coverage(self):
        """Find all Editions lacking coverage from this CoverageProvider.

        Editions are selected randomly to reduce the effect of
        persistent errors.
        """
        return Edition.missing_coverage_from(
            self._db, self.input_sources, self.output_source).order_by(
                func.random())

    def run(self):
        self.log.info("%d records need coverage.", (
            self.editions_that_need_coverage.count())
        )
        offset = 0
        while offset is not None:
            offset = self.run_once(offset)
            Timestamp.stamp(self._db, self.service_name)
            self._db.commit()

    def run_once(self, offset):
        batch = self.editions_that_need_coverage.limit(
            self.workset_size).offset(offset)

        if not batch:
            # The batch is empty. We're done.
            return None

        results = self.process_batch(batch)
        successes = 0
        failures = 0

        for item in results:
            if isinstance(item, CoverageFailure):
                failures += 1
                if item.transient:
                    # Ignore this error for now, but come back to it
                    # on the next run.
                    offset += 1
                else:
                    # Create a CoverageRecord memorializing this
                    # failure. It won't show up anymore, on this 
                    # run or subsequent runs.
                    item.to_coverage_record()
            else:
                # Count this as a success and add a CoverageRecord for
                # it. It won't show up anymore, on this run or
                # subsequent runs.
                successes += 1
                self.add_coverage_record_for(item)

        # Perhaps some records were ignored--they neither succeeded nor
        # failed. Ignore them on this run and try them again later.
        num_ignored = max(0, batch.count() - len(results))
        offset += num_ignored

        # Finalize this batch before moving on to the next one.
        self.finalize_batch()

        self.log.info(
            "Batch processed with %d successes, %d failures, %d ignored.",
            successes, failures, num_ignored
        )
        return offset

    def process_batch(self, batch):
        """Do what it takes to give CoverageRecords to a batch of
        editions.
        """
        results = []
        for edition in batch:
            result = self.process_edition(edition)
            if result:
                results.append(result)
        return results

    def ensure_coverage(self, edition, force=False):
        """Ensure coverage for one specific Edition.

        :return: A CoverageRecord if one was created, None if
        the attempt failed.
        """
        if isinstance(edition, Identifier):
            identifier = edition
            # NOTE: This assumes that this particular coverage provider
            # handles identifiers all the way through rather than editions.
        else:
            identifier = edition.primary_identifier
        coverage_record = get_one(
            self._db, CoverageRecord,
            identifier=identifier,
            data_source=self.output_source,
            on_multiple='interchangeable',
        )
        if force or coverage_record is None:
            result = self.process_edition(edition)
            if isinstance(result, CoverageFailure):
                return result.to_coverage_record()
            else:
                coverage_record, ignore = self.add_coverage_record_for(
                    identifier)
                return coverage_record

    def add_coverage_record_for(self, edition):
        return CoverageRecord.add_for(edition, self.output_source)

    def process_edition(self, edition):
        raise NotImplementedError()

    def finalize_batch(self):
        """Do whatever is necessary to complete this batch before moving on to
        the next one.
        
        e.g. uploading a bunch of assets to S3.
        """
        pass


class IdentifierBasedCoverageProvider(CoverageProvider):
    """Run Identifiers from one DataSource (the input DataSource) through
    code associated with another DataSource (the output
    DataSource). If the code returns success, add a CoverageRecord for
    the Identifier and the output DataSource, so that the record
    doesn't get processed next time.
    """

    @property
    def editions_that_need_coverage(self):
        """Find all Identifiers lacking coverage from this CoverageProvider.

        Identifiers are selected randomly to reduce the effect of
        persistent errors.
        """
        return Identifier.missing_coverage_from(
            self._db, self.input_sources, self.output_source).order_by(
                func.random())
