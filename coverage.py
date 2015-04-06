from nose.tools import set_trace
import datetime
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
    def editions_that_need_coverage(self):
        """Find all Editions lacking coverage from this CoverageProvider.

        Editions are selected randomly to reduce the effect of
        persistent errors.
        """
        return Edition.missing_coverage_from(
            self._db, self.input_sources, self.output_source).order_by(
                func.random())

    def run(self):
        remaining = True
        failures = set([])
        print "%d records need coverage." % (
            self.editions_that_need_coverage.count())
        while remaining:
            successes = 0
            if len(failures) >= self.workset_size:
                raise Exception(
                    "Number of failures equals workset size, cannot continue.")
            batch = self.editions_that_need_coverage.limit(
                self.workset_size)
            batch = [x for x in batch if x not in failures]
            successes, new_failures = self.process_batch(batch)
            if len(successes) == 0 and len(new_failures) == 0:
                # We did not see any new records.
                print "No new records seen."
                break
            failures.update(new_failures)
            for success in successes:
                self.add_coverage_record_for(success)
            # Finalize this batch before moving on to the next one.
            self.finalize_batch()
            print "Batch processed with %d successes, %d failures." % (
                len(successes), len(new_failures))
            # Now that we're done, update the timestamp and commit the DB.
            Timestamp.stamp(self._db, self.service_name)
            self._db.commit()

    def process_batch(self, batch):
        """Do what it takes to give CoverageRecords to a batch of
        editions.
        """
        successes = []
        failures = []
        for edition in batch:
            if self.process_edition(edition):
                # Success! Now there's coverage!
                successes.append(edition)
            else:
                # No coverage.
                failures.append(edition)
        return successes, failures

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
            if self.process_edition(edition):
                coverage_record, ignore = self.add_coverage_record_for(
                    identifier)
            else:
                return None
        return coverage_record

    def add_coverage_record_for(self, edition):
        if isinstance(edition, Identifier):
            identifier = edition
        elif isinstance(edition, Edition):
            identifier = edition.primary_identifier
        else:
            raise ValueError(
                "Cannot create a coverage record for %r." % edition) 
        now = datetime.datetime.utcnow()
        coverage_record, is_new = get_one_or_create(
            self._db, CoverageRecord,
            identifier=identifier,
            data_source=self.output_source,
            on_multiple='interchangeable'
        )
        coverage_record.date = now
        return coverage_record, is_new

    def process_edition(self, edition):
        raise NotImplementedError()

    def finalize_batch(self):
        """Do whatever is necessary to complete this batch before moving on to
        the next one.
        
        e.g. uploading a bunch of assets to S3.
        """
        pass
