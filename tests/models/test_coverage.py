# encoding: utf-8
import datetime

from ...metadata_layer import TimestampData
from ...model.coverage import (
    BaseCoverageRecord,
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from ...model.datasource import DataSource
from ...model.identifier import Identifier
from ...util.datetime_helpers import datetime_utc, utc_now


class TestTimestamp:

    def test_lookup(self, db_session, create_collection):
        """
        GIVEN: A Timeestamp
        WHEN:  Looking up the Timestamp through Timestamp.Lookup
        THEN:  None or Timestamp is returned
        """

        c1 = create_collection(db_session)
        c2 = create_collection(db_session)

        # Create a timestamp.
        timestamp = Timestamp.stamp(
            db_session, "service", Timestamp.SCRIPT_TYPE, c1
        )

        # Look it up.
        assert (
            timestamp ==
            Timestamp.lookup(db_session, "service", Timestamp.SCRIPT_TYPE, c1))

        # There are a number of ways to _fail_ to look up this timestamp.
        assert (
            None is
            Timestamp.lookup(
                db_session, "other service", Timestamp.SCRIPT_TYPE, c1
            ))
        assert (
            None is
            Timestamp.lookup(db_session, "service", Timestamp.MONITOR_TYPE, c1))
        assert (
            None is
            Timestamp.lookup(db_session, "service", Timestamp.SCRIPT_TYPE, c2))

        # value() works the same way as lookup() but returns the actual
        # timestamp.finish value.
        assert (
            timestamp.finish ==
            Timestamp.value(db_session, "service", Timestamp.SCRIPT_TYPE, c1))
        assert (
            None is
            Timestamp.value(db_session, "service", Timestamp.SCRIPT_TYPE, c2))

    def test_stamp(self, db_session, create_collection):
        """
        GIVEN: A Timestamp
        WHEN:  Stamping a Timestamp
        THEN:  Timestamp is either set, creating if necessary
        """
        service = "service"
        type = Timestamp.SCRIPT_TYPE
        collection = create_collection(db_session)

        # If no date is specified, the value of the timestamp is the time
        # stamp() was called.
        stamp = Timestamp.stamp(db_session, service, type)
        now = utc_now()
        assert (now - stamp.finish).total_seconds() < 2
        assert stamp.start == stamp.finish
        assert service == stamp.service
        assert type == stamp.service_type
        assert stamp.collection is None
        assert stamp.achievements is None
        assert stamp.counter is None
        assert stamp.exception is None

        # Calling stamp() again will update the Timestamp.
        stamp2 = Timestamp.stamp(
            db_session, service, type, achievements="yay",
            counter=100, exception="boo"
        )
        assert stamp == stamp2
        now = utc_now()
        assert (now - stamp.finish).total_seconds() < 2
        assert stamp.start == stamp.finish
        assert service == stamp.service
        assert type == stamp.service_type
        assert stamp.collection is None
        assert 'yay' == stamp.achievements
        assert 100 == stamp.counter
        assert 'boo' == stamp.exception

        # Passing in a different collection will create a new Timestamp.
        stamp3 = Timestamp.stamp(
            db_session, service, type, collection=collection
        )
        assert stamp3 != stamp
        assert collection == stamp3.collection

        # Passing in CLEAR_VALUE for start, end, or exception will
        # clear an existing Timestamp.
        stamp4 = Timestamp.stamp(
            db_session, service, type,
            start=Timestamp.CLEAR_VALUE, finish=Timestamp.CLEAR_VALUE,
            exception=Timestamp.CLEAR_VALUE
        )
        assert stamp4 == stamp
        assert stamp4.start is None
        assert stamp4.finish is None
        assert stamp4.exception is None

    def test_update(self, db_session):
        """
        GIVEN: A Timestamp
        WHEN:  Updating the Timestamp with new values
        THEN:  The Timestamp has the new values
        """
        # update() can modify the fields of a Timestamp that aren't
        # used to identify it.
        stamp = Timestamp.stamp(db_session, "service", Timestamp.SCRIPT_TYPE)
        start = datetime_utc(2010, 1, 2)
        finish = datetime_utc(2018, 3, 4)
        achievements = "achievements"
        counter = 42
        exception = "exception"
        stamp.update(start, finish, achievements, counter, exception)

        assert start == stamp.start
        assert finish == stamp.finish
        assert achievements == stamp.achievements
        assert counter == stamp.counter
        assert exception == stamp.exception

        # .exception is the only field update() will set to a value of
        # None. For all other fields, None means "don't update the existing
        # value".
        stamp.update()
        assert start == stamp.start
        assert finish == stamp.finish
        assert achievements == stamp.achievements
        assert counter == stamp.counter
        assert stamp.exception is None

    def test_to_data(self, db_session, create_collection):
        """
        GIVEN: A Timestamp for a Collection
        WHEN:  Converting the Timestamp to a TimestampData object
        THEN:  An unfinalized TimestampData object is returned
        """
        collection = create_collection(db_session)
        stamp = Timestamp.stamp(
            db_session, "service", Timestamp.SCRIPT_TYPE,
            collection=collection, counter=10, achievements="a"
        )
        data = stamp.to_data()
        assert isinstance(data, TimestampData)

        # The TimestampData is not finalized.
        assert data.service is None
        assert data.service_type is None
        assert data.collection_id is None

        # But all the other information is there.
        assert stamp.start == data.start
        assert stamp.finish == data.finish
        assert stamp.achievements == data.achievements
        assert stamp.counter == data.counter


class TestBaseCoverageRecord:

    def test_not_covered(self, db_session, create_coverage_record, create_identifier):
        """
        GIVEN: Four Identifiers with four relationships to a certain coverage provider
        WHEN:  Checking the coverage record
        THEN:  Correct status is returned
        """
        source = DataSource.lookup(db_session, DataSource.OCLC)

        # Here are four identifiers with four relationships to a
        # certain coverage provider: no coverage at all, successful
        # coverage, a transient failure and a permanent failure.

        no_coverage = create_identifier(db_session)

        success = create_identifier(db_session)
        success_record = create_coverage_record(db_session, success, source)
        success_record.timestamp = (
            utc_now() - datetime.timedelta(seconds=3600)
        )
        assert CoverageRecord.SUCCESS == success_record.status

        transient = create_identifier(db_session)
        transient_record = create_coverage_record(
            db_session, transient, source, status=CoverageRecord.TRANSIENT_FAILURE
        )
        assert CoverageRecord.TRANSIENT_FAILURE == transient_record.status

        persistent = create_identifier(db_session)
        persistent_record = create_coverage_record(
            db_session, persistent, source, status=BaseCoverageRecord.PERSISTENT_FAILURE
        )
        assert CoverageRecord.PERSISTENT_FAILURE == persistent_record.status

        # Here's a query that finds all four.
        qu = db_session.query(Identifier).outerjoin(CoverageRecord)
        assert 4 == qu.count()

        def check_not_covered(expect, **kwargs):
            missing = CoverageRecord.not_covered(**kwargs)
            assert sorted(expect) == sorted(qu.filter(missing).all())

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


class TestCoverageRecord:

    def test_lookup(self, db_session, create_collection, create_coverage_record, create_edition):
        """
        GIVEN: A CoverageRecord asssociated with an Edition, DataSource, operation, and Collection
        WHEN:  Looking up the CoverageRecord
        THEN:  Record is returned unless it is missing a key variable then None is returned
        """
        source = DataSource.lookup(db_session, DataSource.OCLC)
        edition = create_edition(db_session)
        operation = 'foo'
        collection = create_collection(db_session)
        record = create_coverage_record(
            db_session, edition, source, operation, collection=collection
        )

        # To find the CoverageRecord, edition, source, operation, and collection must all match.
        result = CoverageRecord.lookup(edition, source, operation, collection=collection)
        assert record == result

        # You can substitute the Edition's primary identifier for the Edition iteslf.
        lookup = CoverageRecord.lookup(edition.primary_identifier, source, operation, collection=collection)
        assert lookup == record

        # Omit the collection, and you find nothing.
        result = CoverageRecord.lookup(edition, source, operation)
        assert result is None

        # Same for operation.
        result = CoverageRecord.lookup(edition, source, collection=collection)
        assert result is None

        result = CoverageRecord.lookup(edition, source, "other operation", collection=collection)
        assert result is None

        # Same for data source.
        other_source = DataSource.lookup(db_session, DataSource.OVERDRIVE)
        result = CoverageRecord.lookup(edition, other_source, operation, collection=collection)
        assert result is None

    def test_add_for(self, db_session, create_edition):
        """
        GIVEN: An Edition
        WHEN:  Adding a CoverageRecord for the Edition with a DataSource and operation
        THEN:  CoverageRecord is retrieved or created
        """
        source = DataSource.lookup(db_session, DataSource.OCLC)
        edition = create_edition(db_session)
        operation = 'foo'
        record, is_new = CoverageRecord.add_for(edition, source, operation)
        assert is_new is True

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = utc_now() - datetime.timedelta(days=7)
        record2, is_new = CoverageRecord.add_for(
            edition, source, operation, a_week_ago
        )
        assert record == record2
        assert is_new is False
        assert a_week_ago == record2.timestamp

        # If we don't specify an operation we get a totally different
        # record.
        record3, _ = CoverageRecord.add_for(edition, source)
        assert record3 != record
        assert record3.operation is None
        seconds = (utc_now() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = CoverageRecord.lookup(edition.primary_identifier, source)
        assert record3 == record4

        # We can change the status.
        record5, is_new = CoverageRecord.add_for(
            edition, source, operation,
            status=CoverageRecord.PERSISTENT_FAILURE
        )
        assert record5 == record
        assert CoverageRecord.PERSISTENT_FAILURE == record.status

    def test_bulk_add(self, db_session, create_coverage_record, create_identifier):
        """
        GIVEN: Multiple Identifiers
        WHEN:  Bulk adding Identifiers for CoverageRecord
        THEN:  CoverageRecord is created for uncovered Identifiers
               and updated for existing ones if force=True
        """
        source = DataSource.lookup(db_session, DataSource.GUTENBERG)
        operation = 'testing'

        # An untouched identifier.
        i1 = create_identifier(db_session)

        # An identifier that already has failing coverage.
        covered = create_identifier(db_session)
        existing = create_coverage_record(
            db_session,
            covered, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
            exception='Uh oh'
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation
        )

        # A new coverage record is created for the uncovered identifier.
        assert i1.coverage_records == resulting_records
        [new_record] = resulting_records
        assert source == new_record.data_source
        assert operation == new_record.operation
        assert CoverageRecord.SUCCESS == new_record.status
        assert new_record.exception is None

        # The existing coverage record is untouched.
        assert [covered] == ignored_identifiers
        assert [existing] == covered.coverage_records
        assert CoverageRecord.TRANSIENT_FAILURE == existing.status
        assert original_timestamp == existing.timestamp
        assert 'Uh oh' == existing.exception

        # Newly untouched identifier.
        i2 = create_identifier(db_session)

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
        assert CoverageRecord.SUCCESS == existing.status
        assert existing.timestamp > original_timestamp
        assert existing.exception is None

        # If no records are created or updated, no records are returned.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i2, covered], source, operation=operation
        )

        assert [] == resulting_records
        assert sorted([i2, covered]) == sorted(ignored_identifiers)

    def test_bulk_add_with_collection(self, db_session, create_collection, create_identifier, create_coverage_record):
        """
        GIVEN: Multiple Collections
        WHEN:  Bulk adding Identifiers for a CoverageRecord associated with a Collection
        THEN:  CoverageRecord is created for uncovered Identifiers
               and updated for existing ones if force=True
        """
        source = DataSource.lookup(db_session, DataSource.GUTENBERG)
        operation = 'testing'

        c1 = create_collection(db_session)
        c2 = create_collection(db_session)

        # An untouched identifier.
        i1 = create_identifier(db_session)

        # An identifier with coverage for a different collection.
        covered = create_identifier(db_session)
        existing = create_coverage_record(
            db_session,
            covered, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE, collection=c1,
            exception='Danger, Will Robinson'
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation, collection=c1,
            force=True
        )

        assert 2 == len(resulting_records)
        assert [] == ignored_identifiers

        # A new record is created for the new identifier.
        [new_record] = i1.coverage_records
        assert new_record in resulting_records
        assert source == new_record.data_source
        assert operation == new_record.operation
        assert CoverageRecord.SUCCESS == new_record.status
        assert c1 == new_record.collection

        # The existing record has been updated.
        assert existing in resulting_records
        assert CoverageRecord.SUCCESS == existing.status
        assert existing.timestamp > original_timestamp
        assert existing.exception is None

        # Bulk add for a different collection.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [covered], source, operation=operation, collection=c2,
            status=CoverageRecord.TRANSIENT_FAILURE, exception='Oh no',
        )

        # A new record has been added to the identifier.
        assert existing not in resulting_records
        [new_record] = resulting_records
        assert covered == new_record.identifier
        assert CoverageRecord.TRANSIENT_FAILURE == new_record.status
        assert source == new_record.data_source
        assert operation == new_record.operation
        assert 'Oh no' == new_record.exception


class TestWorkCoverageRecord:

    def test_lookup(self, db_session, create_work, create_work_coverage_record):
        """
        GIVEN: A Work associated with a WorkCoverageRecord
        WHEN:  Looking up the WorkCoverageRecord with the Work
        THEN:  WorkCoverageRecord is returned if found otherwise None is returned
        """
        work = create_work(db_session)
        operation = 'foo'

        lookup = WorkCoverageRecord.lookup(work, operation)
        assert lookup is None

        record = create_work_coverage_record(db_session, work, operation)

        lookup = WorkCoverageRecord.lookup(work, operation)
        assert lookup == record

        assert WorkCoverageRecord.lookup(work, "another operation") is None

    def test_add_for(self, db_session, create_work):
        """
        GIVEN: A Work
        WHEN:  Adding the Work alongside an operation to WorkCoverageRecord
        THEN:  A WorkCoverageRecord is retrieved or created
        """
        work = create_work(db_session)
        operation = 'foo'
        record, is_new = WorkCoverageRecord.add_for(work, operation)
        assert is_new is True

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = utc_now() - datetime.timedelta(days=7)
        record2, is_new = WorkCoverageRecord.add_for(
            work, operation, a_week_ago
        )
        assert record == record2
        assert is_new is False
        assert a_week_ago == record2.timestamp

        # If we don't specify an operation we get a totally different
        # record.
        record3, _ = WorkCoverageRecord.add_for(work, None)
        assert record3 != record
        assert record3.operation is None
        seconds = (utc_now() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = WorkCoverageRecord.lookup(work, None)
        assert record3 == record4

        # We can change the status.
        record5, is_new = WorkCoverageRecord.add_for(
            work, operation, status=WorkCoverageRecord.PERSISTENT_FAILURE
        )
        assert record5 == record
        assert WorkCoverageRecord.PERSISTENT_FAILURE == record.status

    def test_bulk_add(self, db_session, create_work):
        """
        GIVEN: Multiple Works
        WHEN:  Bulk adding Works to WorkCoverageRecord
        THEN:  A WorkCoverageRecord is created or updated
        """
        operation = "relevant"
        irrelevant_operation = "irrelevant"

        # This Work will get a new WorkCoverageRecord for the relevant
        # operation, even though it already has a WorkCoverageRecord
        # for an irrelevant operation.
        not_already_covered = create_work(db_session)
        irrelevant_record, _ = WorkCoverageRecord.add_for(
            not_already_covered, irrelevant_operation,
            status=WorkCoverageRecord.SUCCESS
        )

        # This Work will have its existing, relevant CoverageRecord
        # updated.
        already_covered = create_work(db_session)
        previously_failed, _ = WorkCoverageRecord.add_for(
            already_covered, operation,
            status=WorkCoverageRecord.TRANSIENT_FAILURE,
        )
        previously_failed.exception = "Some exception"

        # This work will not have a record created for it, because
        # we're not passing it in to the method.
        not_affected = create_work(db_session)
        WorkCoverageRecord.add_for(
            not_affected, irrelevant_operation,
            status=WorkCoverageRecord.SUCCESS
        )

        # This work will not have its existing record updated, because
        # we're not passing it in to the method.
        not_affected_2 = create_work(db_session)
        not_modified, _ = WorkCoverageRecord.add_for(
            not_affected_2, operation, status=WorkCoverageRecord.SUCCESS
        )

        # Tell bulk_add to update or create WorkCoverageRecords for
        # not_already_covered and already_covered, but not not_affected.
        new_timestamp = utc_now()
        new_status = WorkCoverageRecord.REGISTERED
        WorkCoverageRecord.bulk_add(
            [not_already_covered, already_covered],
            operation, new_timestamp, status=new_status
        )
        db_session.commit()

        def relevant_records(work):
            return [x for x in work.coverage_records
                    if x.operation == operation]

        # No coverage records were added or modified for works not
        # passed in to the method.
        assert [] == relevant_records(not_affected)
        assert not_modified.timestamp < new_timestamp

        # The record associated with already_covered has been updated,
        # and its exception removed.
        [record] = relevant_records(already_covered)
        assert new_timestamp == record.timestamp
        assert new_status == record.status
        assert previously_failed.exception is None

        # A new record has been associated with not_already_covered
        [record] = relevant_records(not_already_covered)
        assert new_timestamp == record.timestamp
        assert new_status == record.status

        # The irrelevant WorkCoverageRecord is not affected by the update,
        # even though its Work was affected, because it's a record for
        # a different operation.
        assert WorkCoverageRecord.SUCCESS == irrelevant_record.status
        assert irrelevant_record.timestamp < new_timestamp
