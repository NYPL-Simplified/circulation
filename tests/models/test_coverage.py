# encoding: utf-8
from nose.tools import (
    eq_,
    set_trace,
)
import datetime
from .. import DatabaseTest
from ...model.coverage import (
    BaseCoverageRecord,
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from ...model.datasource import DataSource
from ...model.identifier import Identifier

class TestTimestamp(DatabaseTest):

    def test_lookup(self):

        c1 = self._default_collection
        c2 = self._collection()

        # Create a timestamp.
        timestamp = Timestamp.stamp(
            self._db, "service", Timestamp.SCRIPT_TYPE, c1
        )

        # Look it up.
        eq_(
            timestamp,
            Timestamp.lookup(self._db, "service", Timestamp.SCRIPT_TYPE, c1)
        )

        # There are a number of ways to _fail_ to look up this timestamp.
        eq_(
            None,
            Timestamp.lookup(
                self._db, "other service", Timestamp.SCRIPT_TYPE, c1
            )
        )
        eq_(
            None,
            Timestamp.lookup(self._db, "service", Timestamp.MONITOR_TYPE, c1)
        )
        eq_(
            None,
            Timestamp.lookup(self._db, "service", Timestamp.SCRIPT_TYPE, c2)
        )

        # value() works the same way as lookup() but returns the actual
        # timestamp.finish value.
        eq_(timestamp.finish,
            Timestamp.value(self._db, "service", Timestamp.SCRIPT_TYPE, c1))
        eq_(
            None,
            Timestamp.value(self._db, "service", Timestamp.SCRIPT_TYPE, c2)
        )

    def test_stamp(self):
        service = "service"
        type = Timestamp.SCRIPT_TYPE

        # If no date is specified, the value of the timestamp is the time
        # stamp() was called.
        stamp = Timestamp.stamp(self._db, service, type)
        now = datetime.datetime.utcnow()
        assert (now - stamp.finish).total_seconds() < 2
        eq_(stamp.start, stamp.finish)
        eq_(service, stamp.service)
        eq_(type, stamp.service_type)
        eq_(None, stamp.collection)
        eq_(None, stamp.achievements)
        eq_(None, stamp.counter)
        eq_(None, stamp.exception)

        # Calling stamp() again will update the Timestamp.
        stamp2 = Timestamp.stamp(
            self._db, service, type, achievements="yay",
            counter=100, exception="boo"
        )
        eq_(stamp, stamp2)
        now = datetime.datetime.utcnow()
        assert (now - stamp.finish).total_seconds() < 2
        eq_(stamp.start, stamp.finish)
        eq_(service, stamp.service)
        eq_(type, stamp.service_type)
        eq_(None, stamp.collection)
        eq_('yay', stamp.achievements)
        eq_(100, stamp.counter)
        eq_('boo', stamp.exception)

        # Passing in a different collection will create a new Timestamp.
        stamp3 = Timestamp.stamp(
            self._db, service, type, collection=self._default_collection
        )
        assert stamp3 != stamp
        eq_(self._default_collection, stamp3.collection)

        # Passing in CLEAR_VALUE for start, end, or exception will
        # clear an existing Timestamp.
        stamp4 = Timestamp.stamp(
            self._db, service, type,
            start=Timestamp.CLEAR_VALUE, finish=Timestamp.CLEAR_VALUE,
            exception=Timestamp.CLEAR_VALUE
        )
        eq_(stamp4, stamp)
        eq_(None, stamp4.start)
        eq_(None, stamp4.finish)
        eq_(None, stamp4.exception)

    def test_update(self):
        # update() can modify the fields of a Timestamp that aren't
        # used to identify it.
        stamp = Timestamp.stamp(self._db, "service", Timestamp.SCRIPT_TYPE)
        start = datetime.datetime(2010, 1, 2)
        finish = datetime.datetime(2018, 3, 4)
        achievements = self._str
        counter = self._id
        exception = self._str
        stamp.update(start, finish, achievements, counter, exception)

        eq_(start, stamp.start)
        eq_(finish, stamp.finish)
        eq_(achievements, stamp.achievements)
        eq_(counter, stamp.counter)
        eq_(exception, stamp.exception)

        # .exception is the only field update() will set to a value of
        # None. For all other fields, None means "don't update the existing
        # value".
        stamp.update()
        eq_(start, stamp.start)
        eq_(finish, stamp.finish)
        eq_(achievements, stamp.achievements)
        eq_(counter, stamp.counter)
        eq_(None, stamp.exception)

    def to_data(self):
        stamp = Timestamp.stamp(
            self._db, "service", Timestamp.SCRIPT_TYPE,
            collection=self._default_collection, counter=10, achivements="a"
        )
        data = stamp.to_data()
        assert isinstance(data, TimestampData)

        # The TimestampData is not finalized.
        eq_(None, data.service)
        eq_(None, data.service_type)
        eq_(None, data.collection_id)

        # But all the other information is there.
        eq_(stamp.start, data.start)
        eq_(stamp.finish, data.finish)
        eq_(stamp.achievements, data.achievements)
        eq_(stamp.counter, data.counter)


class TestBaseCoverageRecord(DatabaseTest):

    def test_not_covered(self):
        source = DataSource.lookup(self._db, DataSource.OCLC)

        # Here are four identifiers with four relationships to a
        # certain coverage provider: no coverage at all, successful
        # coverage, a transient failure and a permanent failure.

        no_coverage = self._identifier()

        success = self._identifier()
        success_record = self._coverage_record(success, source)
        success_record.timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(seconds=3600)
        )
        eq_(CoverageRecord.SUCCESS, success_record.status)

        transient = self._identifier()
        transient_record = self._coverage_record(
            transient, source, status=CoverageRecord.TRANSIENT_FAILURE
        )
        eq_(CoverageRecord.TRANSIENT_FAILURE, transient_record.status)

        persistent = self._identifier()
        persistent_record = self._coverage_record(
            persistent, source, status = BaseCoverageRecord.PERSISTENT_FAILURE
        )
        eq_(CoverageRecord.PERSISTENT_FAILURE, persistent_record.status)

        # Here's a query that finds all four.
        qu = self._db.query(Identifier).outerjoin(CoverageRecord)
        eq_(4, qu.count())

        def check_not_covered(expect, **kwargs):
            missing = CoverageRecord.not_covered(**kwargs)
            eq_(sorted(expect), sorted(qu.filter(missing).all()))

        # By default, not_covered() only finds the identifier with no
        # coverage and the one with a transient failure.
        check_not_covered([no_coverage, transient])

        # If we pass in different values for covered_status, we change what
        # counts as 'coverage'. In this case, we allow transient failures
        # to count as 'coverage'.
        check_not_covered(
            [no_coverage],
            count_as_covered=[CoverageRecord.PERSISTENT_FAILURE,
                              CoverageRecord.TRANSIENT_FAILURE,
                              CoverageRecord.SUCCESS]
        )

        # Here, only success counts as 'coverage'.
        check_not_covered(
            [no_coverage, transient, persistent],
            count_as_covered=CoverageRecord.SUCCESS
        )

        # We can also say that coverage doesn't count if it was achieved before
        # a certain time. Here, we'll show that passing in the timestamp
        # of the 'success' record means that record still counts as covered.
        check_not_covered(
            [no_coverage, transient],
            count_as_not_covered_if_covered_before=success_record.timestamp
        )

        # But if we pass in a time one second later, the 'success'
        # record no longer counts as covered.
        one_second_after = (
            success_record.timestamp + datetime.timedelta(seconds=1)
        )
        check_not_covered(
            [success, no_coverage, transient],
            count_as_not_covered_if_covered_before=one_second_after
        )

class TestCoverageRecord(DatabaseTest):

    def test_lookup(self):
        source = DataSource.lookup(self._db, DataSource.OCLC)
        edition = self._edition()
        operation = 'foo'
        collection = self._default_collection
        record = self._coverage_record(edition, source, operation,
                                       collection=collection)


        # To find the CoverageRecord, edition, source, operation,
        # and collection must all match.
        result = CoverageRecord.lookup(edition, source, operation,
                                       collection=collection)
        eq_(record, result)

        # You can substitute the Edition's primary identifier for the
        # Edition iteslf.
        lookup = CoverageRecord.lookup(
            edition.primary_identifier, source, operation,
            collection=self._default_collection
        )
        eq_(lookup, record)


        # Omit the collection, and you find nothing.
        result = CoverageRecord.lookup(edition, source, operation)
        eq_(None, result)

        # Same for operation.
        result = CoverageRecord.lookup(edition, source, collection=collection)
        eq_(None, result)

        result = CoverageRecord.lookup(edition, source, "other operation",
                                       collection=collection)
        eq_(None, result)

        # Same for data source.
        other_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        result = CoverageRecord.lookup(edition, other_source, operation,
                                       collection=collection)
        eq_(None, result)

    def test_add_for(self):
        source = DataSource.lookup(self._db, DataSource.OCLC)
        edition = self._edition()
        operation = 'foo'
        record, is_new = CoverageRecord.add_for(edition, source, operation)
        eq_(True, is_new)

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        record2, is_new = CoverageRecord.add_for(
            edition, source, operation, a_week_ago
        )
        eq_(record, record2)
        eq_(False, is_new)
        eq_(a_week_ago, record2.timestamp)

        # If we don't specify an operation we get a totally different
        # record.
        record3, ignore = CoverageRecord.add_for(edition, source)
        assert record3 != record
        eq_(None, record3.operation)
        seconds = (datetime.datetime.utcnow() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = CoverageRecord.lookup(edition.primary_identifier, source)
        eq_(record3, record4)

        # We can change the status.
        record5, is_new = CoverageRecord.add_for(
            edition, source, operation,
            status=CoverageRecord.PERSISTENT_FAILURE
        )
        eq_(record5, record)
        eq_(CoverageRecord.PERSISTENT_FAILURE, record.status)

    def test_bulk_add(self):
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        operation = 'testing'

        # An untouched identifier.
        i1 = self._identifier()

        # An identifier that already has failing coverage.
        covered = self._identifier()
        existing = self._coverage_record(
            covered, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
            exception='Uh oh'
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation
        )

        # A new coverage record is created for the uncovered identifier.
        eq_(i1.coverage_records, resulting_records)
        [new_record] = resulting_records
        eq_(source, new_record.data_source)
        eq_(operation, new_record.operation)
        eq_(CoverageRecord.SUCCESS, new_record.status)
        eq_(None, new_record.exception)

        # The existing coverage record is untouched.
        eq_([covered], ignored_identifiers)
        eq_([existing], covered.coverage_records)
        eq_(CoverageRecord.TRANSIENT_FAILURE, existing.status)
        eq_(original_timestamp, existing.timestamp)
        eq_('Uh oh', existing.exception)

        # Newly untouched identifier.
        i2 = self._identifier()

        # Force bulk add.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i2, covered], source, operation=operation, force=True
        )

        # The new identifier has the expected coverage.
        [new_record] = i2.coverage_records
        assert new_record in resulting_records

        # The existing record has been updated.
        assert existing in resulting_records
        assert covered not in ignored_identifiers
        eq_(CoverageRecord.SUCCESS, existing.status)
        assert existing.timestamp > original_timestamp
        eq_(None, existing.exception)

        # If no records are created or updated, no records are returned.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i2, covered], source, operation=operation
        )

        eq_([], resulting_records)
        eq_(sorted([i2, covered]), sorted(ignored_identifiers))

    def test_bulk_add_with_collection(self):
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        operation = 'testing'

        c1 = self._collection()
        c2 = self._collection()

        # An untouched identifier.
        i1 = self._identifier()

        # An identifier with coverage for a different collection.
        covered = self._identifier()
        existing = self._coverage_record(
            covered, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE, collection=c1,
            exception='Danger, Will Robinson'
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation, collection=c1,
            force=True
        )

        eq_(2, len(resulting_records))
        eq_([], ignored_identifiers)

        # A new record is created for the new identifier.
        [new_record] = i1.coverage_records
        assert new_record in resulting_records
        eq_(source, new_record.data_source)
        eq_(operation, new_record.operation)
        eq_(CoverageRecord.SUCCESS, new_record.status)
        eq_(c1, new_record.collection)

        # The existing record has been updated.
        assert existing in resulting_records
        eq_(CoverageRecord.SUCCESS, existing.status)
        assert existing.timestamp > original_timestamp
        eq_(None, existing.exception)

        # Bulk add for a different collection.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [covered], source, operation=operation, collection=c2,
            status=CoverageRecord.TRANSIENT_FAILURE, exception='Oh no',
        )

        # A new record has been added to the identifier.
        assert existing not in resulting_records
        [new_record] = resulting_records
        eq_(covered, new_record.identifier)
        eq_(CoverageRecord.TRANSIENT_FAILURE, new_record.status)
        eq_(source, new_record.data_source)
        eq_(operation, new_record.operation)
        eq_('Oh no', new_record.exception)

class TestWorkCoverageRecord(DatabaseTest):

    def test_lookup(self):
        work = self._work()
        operation = 'foo'

        lookup = WorkCoverageRecord.lookup(work, operation)
        eq_(None, lookup)

        record = self._work_coverage_record(work, operation)

        lookup = WorkCoverageRecord.lookup(work, operation)
        eq_(lookup, record)

        eq_(None, WorkCoverageRecord.lookup(work, "another operation"))

    def test_add_for(self):
        work = self._work()
        operation = 'foo'
        record, is_new = WorkCoverageRecord.add_for(work, operation)
        eq_(True, is_new)

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        record2, is_new = WorkCoverageRecord.add_for(
            work, operation, a_week_ago
        )
        eq_(record, record2)
        eq_(False, is_new)
        eq_(a_week_ago, record2.timestamp)

        # If we don't specify an operation we get a totally different
        # record.
        record3, ignore = WorkCoverageRecord.add_for(work, None)
        assert record3 != record
        eq_(None, record3.operation)
        seconds = (datetime.datetime.utcnow() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = WorkCoverageRecord.lookup(work, None)
        eq_(record3, record4)

        # We can change the status.
        record5, is_new = WorkCoverageRecord.add_for(
            work, operation, status=WorkCoverageRecord.PERSISTENT_FAILURE
        )
        eq_(record5, record)
        eq_(WorkCoverageRecord.PERSISTENT_FAILURE, record.status)

    def test_bulk_add(self):

        operation = "relevant"
        irrelevant_operation = "irrelevant"

        # This Work will get a new WorkCoverageRecord for the relevant
        # operation, even though it already has a WorkCoverageRecord
        # for an irrelevant operation.
        not_already_covered = self._work()
        irrelevant_record, ignore = WorkCoverageRecord.add_for(
            not_already_covered, irrelevant_operation,
            status=WorkCoverageRecord.SUCCESS
        )

        # This Work will have its existing, relevant CoverageRecord
        # updated.
        already_covered = self._work()
        previously_failed, ignore = WorkCoverageRecord.add_for(
            already_covered, operation,
            status=WorkCoverageRecord.TRANSIENT_FAILURE,
        )
        previously_failed.exception="Some exception"

        # This work will not have a record created for it, because
        # we're not passing it in to the method.
        not_affected = self._work()
        WorkCoverageRecord.add_for(
            not_affected, irrelevant_operation,
            status=WorkCoverageRecord.SUCCESS
        )

        # This work will not have its existing record updated, because
        # we're not passing it in to the method.
        not_affected_2 = self._work()
        not_modified, ignore = WorkCoverageRecord.add_for(
            not_affected_2, operation, status=WorkCoverageRecord.SUCCESS
        )

        # Tell bulk_add to update or create WorkCoverageRecords for
        # not_already_covered and already_covered, but not not_affected.
        new_timestamp = datetime.datetime.utcnow()
        new_status = WorkCoverageRecord.REGISTERED
        WorkCoverageRecord.bulk_add(
            [not_already_covered, already_covered],
            operation, new_timestamp, status=new_status
        )
        self._db.commit()
        def relevant_records(work):
            return [x for x in work.coverage_records
                    if x.operation == operation]

        # No coverage records were added or modified for works not
        # passed in to the method.
        eq_([], relevant_records(not_affected))
        assert not_modified.timestamp < new_timestamp

        # The record associated with already_covered has been updated,
        # and its exception removed.
        [record] = relevant_records(already_covered)
        eq_(new_timestamp, record.timestamp)
        eq_(new_status, record.status)
        eq_(None, previously_failed.exception)

        # A new record has been associated with not_already_covered
        [record] = relevant_records(not_already_covered)
        eq_(new_timestamp, record.timestamp)
        eq_(new_status, record.status)

        # The irrelevant WorkCoverageRecord is not affected by the update,
        # even though its Work was affected, because it's a record for
        # a different operation.
        eq_(WorkCoverageRecord.SUCCESS, irrelevant_record.status)
        assert irrelevant_record.timestamp < new_timestamp
