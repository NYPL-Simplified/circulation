# encoding: utf-8
import pytest

from ...model import get_one_or_create
from ...model.coverage import WorkCoverageRecord
from ...model.customlist import (
    CustomList,
    CustomListEntry,
)
from ...model.datasource import DataSource
from ...util.datetime_helpers import utc_now


class TestCustomList:

    def test_find(self, db_session, create_customlist, create_library, init_datasource_and_genres):
        """
        GIVEN: A CustomList
        WHEN:  Finding a foreign list in the database
        THEN:  The correct CustomList is returneds
        """
        library = create_library(db_session)
        source = DataSource.lookup(db_session, DataSource.NYT)

        # When there's no CustomList to find, nothing is returned.
        result = CustomList.find(db_session, 'my-list', source)
        assert None is result

        custom_list = create_customlist(
            db_session,
            foreign_identifier='a-list', name='My List', num_entries=0
        )[0]
        # A CustomList can be found by its foreign_identifier.
        result = CustomList.find(db_session, 'a-list', source)
        assert custom_list == result

        # Or its name.
        result = CustomList.find(db_session, 'My List', source.name)
        assert custom_list == result

        # The list can also be found by name without a data source.
        result = CustomList.find(db_session, 'My List')
        assert custom_list == result

        # By default, we only find lists with no associated Library.
        # If we look for a list from a library, there isn't one.
        result = CustomList.find(db_session, 'My List', source, library=library)
        assert None is result

        # If we add the Library to the list, it's returned.
        custom_list.library = library
        result = CustomList.find(db_session, 'My List', source, library=library)
        assert custom_list == result

    def assert_reindexing_scheduled(self, work):
        """Assert that the given work has exactly one WorkCoverageRecord, which
        indicates that it needs to have its search index updated.
        """
        [needs_reindex] = work.coverage_records
        assert WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION == needs_reindex.operation
        assert WorkCoverageRecord.REGISTERED == needs_reindex.status

    def test_add_entry(self, db_session, create_customlist, create_edition, create_work):
        """
        GIVEN: A CustomList
        WHEN:  Adding entries (Editions or Works) to the CustomList
        THEN:  Entry appears in the CustomList
        """
        custom_list = create_customlist(db_session, num_entries=0)[0]
        now = utc_now()

        # An edition without a work can create an entry.
        workless_edition = create_edition(db_session)
        workless_entry, is_new = custom_list.add_entry(workless_edition)
        assert True == is_new
        assert True == isinstance(workless_entry, CustomListEntry)
        assert workless_edition == workless_entry.edition
        assert True == (workless_entry.first_appearance > now)
        assert None == workless_entry.work
        # And the CustomList will be seen as updated.
        assert True == (custom_list.updated > now)
        assert 1 == custom_list.size

        # An edition with a work can create an entry.
        work = create_work(db_session)
        work.coverage_records = []
        worked_entry, is_new = custom_list.add_entry(work.presentation_edition)
        assert True == is_new
        assert work == worked_entry.work
        assert work.presentation_edition == worked_entry.edition
        assert True == (worked_entry.first_appearance > now)
        assert 2 == custom_list.size

        # When this happens, the work is scheduled for reindexing.
        self.assert_reindexing_scheduled(work)

        # A work can create an entry.
        work = create_work(db_session, with_open_access_download=True)
        work.coverage_records = []
        work_entry, is_new = custom_list.add_entry(work)
        assert True == is_new
        assert work.presentation_edition == work_entry.edition
        assert work == work_entry.work
        assert True == (work_entry.first_appearance > now)
        assert 3 == custom_list.size

        # When this happens, the work is scheduled for reindexing.
        self.assert_reindexing_scheduled(work)

        # Annotations can be passed to the entry.
        annotated_edition = create_edition(db_session)
        annotated_entry = custom_list.add_entry(
            annotated_edition, annotation="Sure, this is a good book."
        )[0]
        assert "Sure, this is a good book." == annotated_entry.annotation
        assert 4 == custom_list.size

        # A first_appearance time can be passed to an entry.
        timed_edition = create_edition(db_session)
        timed_entry = custom_list.add_entry(timed_edition, first_appearance=now)[0]
        assert now == timed_entry.first_appearance
        assert now == timed_entry.most_recent_appearance
        assert 5 == custom_list.size

        # If the entry already exists, the most_recent_appearance is updated.
        previous_list_update_time = custom_list.updated
        new_timed_entry, is_new = custom_list.add_entry(timed_edition)
        assert False == is_new
        assert timed_entry == new_timed_entry
        assert True == (timed_entry.most_recent_appearance > now)
        # But the CustomList update time and size are not.
        assert previous_list_update_time == custom_list.updated
        assert 5 == custom_list.size

        # If the entry already exists, the most_recent_appearance can be
        # updated by passing in a later first_appearance.
        later = utc_now()
        new_timed_entry = custom_list.add_entry(timed_edition, first_appearance=later)[0]
        assert timed_entry == new_timed_entry
        assert now == new_timed_entry.first_appearance
        assert later == new_timed_entry.most_recent_appearance
        assert 5 == custom_list.size

        # For existing entries, earlier first_appearance datetimes are ignored.
        entry = custom_list.add_entry(annotated_edition, first_appearance=now)[0]
        assert True == (entry.first_appearance != now)
        assert True == (entry.first_appearance >= now)
        assert True == (entry.most_recent_appearance != now)
        assert True == (entry.most_recent_appearance >= now)
        assert 5 == custom_list.size

    def test_add_entry_edition_duplicate_check(self, db_session, create_edition, create_customlist, create_work):
        """
        GIVEN: A CustomList with entries
        WHEN:  Adding a duplicate entry
        THEN:  A duplicate check is performed so the duplicate does not end up in the CustomList
        """
        # When adding an Edition to a CustomList, a duplicate check is run
        # so we don't end up adding the same book to the list twice.

        # This edition has no Work.
        workless_edition = create_edition(db_session)

        # This edition is equivalent to the first one, and it has an
        # associated Work.
        work = create_work(db_session, with_open_access_download=True)
        equivalent_edition = work.presentation_edition
        workless_edition.primary_identifier.equivalent_to(
            equivalent_edition.data_source, equivalent_edition.primary_identifier, 1
        )

        custom_list, _ = create_customlist(db_session, num_entries=0)

        # Add the edition with no associated Work.
        e1, is_new = custom_list.add_entry(workless_edition)
        assert is_new is True

        previous_list_update_time = custom_list.updated

        # Add the equivalent edition, the one with a Work.
        e2, is_new = custom_list.add_entry(equivalent_edition)

        # Instead of a new CustomListEntry being created, the original
        # CustomListEntry was returned.
        assert e1 == e2
        assert is_new is False

        # The list's updated time has not changed; nor has its size.
        equivalent_entry, is_new = custom_list.add_entry(equivalent_edition)
        assert 1 == custom_list.size

        # But the previously existing CustomListEntry has been updated
        # to take into account the most recently seen Edition and
        # Work.
        assert equivalent_edition == e1.edition
        assert equivalent_edition.work == e1.work

        # The duplicate check also handles the case where a Work has multiple Editions, and both Editions
        # get added to the same list.
        not_equivalent, lp = create_edition(db_session, with_open_access_download=True)
        not_equivalent.work = equivalent_edition.work
        not_equivalent_entry, is_new = custom_list.add_entry(not_equivalent)
        assert not_equivalent_entry == e1
        assert is_new is False
        assert 1 == custom_list.size

        # Again, the .edition has been updated.
        assert e1.edition == not_equivalent

        # The .work has stayed the same because both Editions have the same Work.
        assert work == e1.work

        # Finally, test the case where the duplicate check passes,
        # because a totally different Edition is being added to the
        # list.
        workless_edition_2 = create_edition(db_session)
        e2, is_new = custom_list.add_entry(workless_edition_2)

        # A brand new CustomListEntry is created.
        assert is_new is True
        assert workless_edition_2 == e2.edition
        assert e2.work is None

        # .updated and .size have been updated.
        assert custom_list.updated > previous_list_update_time
        assert 2 == custom_list.size

    def test_add_entry_work_same_presentation_edition(self, db_session, create_work, create_customlist):
        """
        GIVEN: Two Works that have the same presentation edition
        WHEN:  Adding a duplicate Work entry to a CustomList
        THEN:  The duplicate Work does not create a new entry
        """
        # Verify that two Works can be added to a CustomList even if they have the
        # same presentation edition.
        w1 = create_work(db_session)
        w2 = create_work(db_session, presentation_edition=w1.presentation_edition)
        assert w1.presentation_edition == w2.presentation_edition

        custom_list, _ = create_customlist(db_session, num_entries=0)
        entry1, is_new1 = custom_list.add_entry(w1)
        assert is_new1 is True
        assert w1 == entry1.work
        assert w1.presentation_edition == entry1.edition

        entry2, is_new2 = custom_list.add_entry(w2)
        assert is_new2 is True
        assert w2 == entry2.work
        assert w2.presentation_edition == entry2.edition

        assert entry1 != entry2
        assert set([entry1, entry2]) == set(custom_list.entries)

        # Adding the exact same work again won't result in a third entry.
        entry3, is_new3 = custom_list.add_entry(w1)
        assert entry3 == entry1
        assert is_new3 is False

    def test_add_entry_work_equivalent_identifier(self, db_session, create_work, create_customlist,
                                                  init_datasource_and_genres):
        """
        GIVEN: Two Works with equivalent identifiers
        WHEN:  Adding a duplicate Work entry to a CustomList
        THEN:  The duplicate Work does not create a new entry
        """
        # Verify that two Works can be added to a CustomList even if their identifiers
        # are exact equivalents.
        w1 = create_work(db_session)
        w2 = create_work(db_session)
        w1.presentation_edition.primary_identifier.equivalent_to(
            w1.presentation_edition.data_source,
            w2.presentation_edition.primary_identifier, 1
        )

        custom_list, _ = create_customlist(db_session, num_entries=0)
        entry1, is_new1 = custom_list.add_entry(w1)
        assert is_new1 is True
        assert w1 == entry1.work
        assert w1.presentation_edition == entry1.edition

        entry2, is_new2 = custom_list.add_entry(w2)
        assert is_new2 is True
        assert w2 == entry2.work
        assert w2.presentation_edition == entry2.edition

        assert entry1 != entry2
        assert set([entry1, entry2]) == set(custom_list.entries)

        # Adding the exact same work again won't result in a third entry.
        entry3, is_new3 = custom_list.add_entry(w1)
        assert entry3 == entry1
        assert is_new3 is False

    def test_remove_entry(self, db_session, create_customlist, create_edition, init_datasource_and_genres):
        """
        GIVEN: A CustomList with 3 entries
        WHEN:  Removing an entry
        THEN:  The entry is removed
        """
        custom_list, editions = create_customlist(db_session, num_entries=3)
        [first, second, third] = editions
        now = utc_now()

        # An entry is removed if its edition is passed in.
        first.work.coverage_records = []
        custom_list.remove_entry(first)
        assert 2 == len(custom_list.entries)
        assert set([second, third]) == set([entry.edition for entry in custom_list.entries])
        # And CustomList.updated and size are changed.
        assert True == (custom_list.updated > now)
        assert 2 == custom_list.size

        # The editon's work has been scheduled for reindexing.
        self.assert_reindexing_scheduled(first.work)
        first.work.coverage_records = []

        # An entry is also removed if any of its equivalent editions
        # are passed in.
        previous_list_update_time = custom_list.updated
        equivalent, _ = create_edition(db_session, with_open_access_download=True)
        second.primary_identifier.equivalent_to(
            equivalent.data_source, equivalent.primary_identifier, 1
        )
        custom_list.remove_entry(second)
        assert 1 == len(custom_list.entries)
        assert third == custom_list.entries[0].edition
        assert True == (custom_list.updated > previous_list_update_time)
        assert 1 == custom_list.size

        # An entry is also removed if its work is passed in.
        previous_list_update_time = custom_list.updated
        custom_list.remove_entry(third.work)
        assert [] == custom_list.entries
        assert True == (custom_list.updated > previous_list_update_time)
        assert 0 == custom_list.size

        # An edition that's not on the list doesn't cause any problems.
        custom_list.add_entry(second)
        previous_list_update_time = custom_list.updated
        custom_list.remove_entry(first)
        assert 1 == len(custom_list.entries)
        assert previous_list_update_time == custom_list.updated
        assert 1 == custom_list.size

        # The 'removed' edition's work does not need to be reindexed
        # because it wasn't on the list to begin with.
        assert [] == first.work.coverage_records

    def test_entries_for_work(self, db_session, create_customlist, create_edition):
        """
        GIVEN: A CustomList with 2 entries
        WHEN:  Searching for an entry
        THEN:  Correct entry is retrieved
        """
        custom_list, editions = create_customlist(db_session, num_entries=2)
        edition = editions[0]
        [entry] = [e for e in custom_list.entries if e.edition == edition]

        # The entry is returned when you search by Edition.
        assert [entry] == list(custom_list.entries_for_work(edition))

        # It's also returned when you search by Work.
        assert [entry] == list(custom_list.entries_for_work(edition.work))

        # Or when you search with an equivalent Edition
        equivalent = create_edition(db_session)
        edition.primary_identifier.equivalent_to(
            equivalent.data_source, equivalent.primary_identifier, 1
        )
        assert [entry] == list(custom_list.entries_for_work(equivalent))

        # Multiple equivalent entries may be returned, too, if they
        # were added manually or before the editions were set as
        # equivalent.
        not_yet_equivalent = create_edition(db_session)
        other_entry = custom_list.add_entry(not_yet_equivalent)[0]
        edition.primary_identifier.equivalent_to(
            not_yet_equivalent.data_source,
            not_yet_equivalent.primary_identifier, 1
        )
        assert (
            set([entry, other_entry]) ==
            set(custom_list.entries_for_work(not_yet_equivalent)))

    def test_update_size(self, db_session, create_customlist):
        """
        GIVEN: A CustomList with 4 entries
        WHEN:  Calling update_size() on the CustomList
        THEN:  The correct number of entries is returned
        """
        list, _ = create_customlist(db_session, num_entries=4)
        # This list has an incorrect cached size.
        list.size = 44
        list.update_size()
        assert 4 == list.size


class TestCustomListEntry:

    def test_set_work(self, db_session, create_customlist, create_edition,
                      init_datasource_and_genres, init_delivery_mechanism):
        """
        GIVEN: A CustomList with an entry
        WHEN:  Adding a Work with a LicensePool through entry.set_work()
        THEN:  The Work is set as the entry's work.
        """
        # Start with a custom list with no entries
        list, _ = create_customlist(db_session, num_entries=0)

        # Now create an entry with an edition but no license pool.
        edition = create_edition(db_session)
        entry, _ = get_one_or_create(
            db_session, CustomListEntry,
            list_id=list.id, edition_id=edition.id,
        )

        assert edition == entry.edition
        assert None is entry.work

        # Here's another edition, with a license pool.
        other_edition, lp = create_edition(db_session, with_open_access_download=True)

        # And its identifier is equivalent to the entry's edition's identifier.
        data_source = DataSource.lookup(db_session, DataSource.OCLC)
        lp.identifier.equivalent_to(data_source, edition.primary_identifier, 1)

        # If we call set_work, it does nothing, because there is no work
        # associated with either edition.
        entry.set_work()

        # But if we assign a Work with the LicensePool, and try again...
        work, _ = lp.calculate_work()
        entry.set_work()
        assert work == other_edition.work

        # set_work() traces the line from the CustomListEntry to its
        # Edition to the equivalent Edition to its Work, and associates
        # that Work with the CustomListEntry.
        assert work == entry.work

        # Even though the CustomListEntry's edition is not directly
        # associated with the Work.
        assert None is edition.work

    def test_update(self, db_session, create_customlist, create_edition, create_work, init_delivery_mechanism):
        """
        GIVEN: A CustomList
        WHEN:  Updating an entry
        THEN:  Entry is updated or a ValueError is raised
        """
        custom_list, [edition] = create_customlist(db_session, entries_exist_as_works=False)
        identifier = edition.primary_identifier
        [entry] = custom_list.entries
        entry_attributes = list(vars(entry).values())
        created = entry.first_appearance

        # Running update without entries or forcing doesn't change the entry.
        entry.update(db_session)
        assert entry_attributes == list(vars(entry).values())

        # Trying to update an entry with entries from a different
        # CustomList is a no-go.
        other_custom_list = create_customlist(db_session)[0]
        [external_entry] = other_custom_list.entries
        pytest.raises(
            ValueError, entry.update, db_session,
            equivalent_entries=[external_entry]
        )

        # So is attempting to update an entry with other entries that
        # don't represent the same work.
        external_work = create_work(db_session, with_license_pool=True)
        external_work_edition = external_work.presentation_edition
        external_work_entry = custom_list.add_entry(external_work_edition)[0]
        pytest.raises(
            ValueError, entry.update, db_session,
            equivalent_entries=[external_work_entry]
        )

        # Okay, but with an actual equivalent entry...
        work = create_work(db_session, with_open_access_download=True)
        equivalent = work.presentation_edition
        equivalent_entry = custom_list.add_entry(
            equivalent, annotation="Whoo, go books!"
        )[0]
        identifier.equivalent_to(
            equivalent.data_source, equivalent.primary_identifier, 1
        )

        # ...updating changes the original entry as expected.
        entry.update(db_session, equivalent_entries=[equivalent_entry])
        # The first_appearance hasn't changed because the entry was created first.
        assert created == entry.first_appearance
        # But the most recent appearance is of the entry created last.
        assert equivalent_entry.most_recent_appearance == entry.most_recent_appearance
        # Annotations are shared.
        assert "Whoo, go books!" == entry.annotation
        # The Edition and LicensePool are updated to have a Work.
        assert entry.edition == work.presentation_edition
        assert entry.work == equivalent.work
        # The equivalent entry has been deleted.
        assert ([] == db_session.query(CustomListEntry).
                filter(CustomListEntry.id == equivalent_entry.id).all())

        # The entry with the longest annotation wins the annotation awards.
        long_annotation = "Wow books are so great especially when they're annotated."
        longwinded = create_edition(db_session)
        longwinded_entry = custom_list.add_entry(
            longwinded, annotation=long_annotation)[0]

        identifier.equivalent_to(
            longwinded.data_source, longwinded.primary_identifier, 1)
        entry.update(db_session, equivalent_entries=[longwinded_entry])
        assert long_annotation == entry.annotation
        assert longwinded_entry.most_recent_appearance == entry.most_recent_appearance
