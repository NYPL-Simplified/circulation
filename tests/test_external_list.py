# encoding: utf-8
import datetime
import pytz
from ..testing import (
    DatabaseTest,
    DummyMetadataClient,
)

from ..model import (
    DataSource,
    Edition,
    Identifier,
    Subject,
)
from ..external_list import (
    CustomListFromCSV,
    MembershipManager,
    ClassificationBasedMembershipManager,
)

class TestCustomListFromCSV(DatabaseTest):

    def setup_method(self):
        super(TestCustomListFromCSV, self).setup_method()
        self.data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        self.metadata = DummyMetadataClient()
        self.metadata.lookups['Octavia Butler'] = 'Butler, Octavia'
        self.l = CustomListFromCSV(self.data_source.name, "Test list",
                                   metadata_client = self.metadata,
                                   display_author_field='author',
                                   identifier_fields={Identifier.ISBN: "isbn"})
        self.custom_list, ignore = self._customlist(
            data_source_name=self.data_source.name, num_entries=0)
        self.now = datetime.datetime.now(tz=pytz.UTC)

    DATE_FORMAT = "%Y/%m/%d %H:%M:%S"

    def create_row(self, display_author=None, sort_author=None):
        """Create a dummy row for this tests's custom list."""
        l = self.l
        row = dict()
        for scalarkey in (l.title_field, l.annotation_field,
                          l.annotation_author_name_field,
                          l.annotation_author_affiliation_field):
            row[scalarkey] = self._str

        display_author = display_author or self._str
        fn = l.sort_author_field
        if isinstance(fn, list):
            fn = fn[0]
        row[fn] = sort_author
        row['isbn'] = self._isbn

        for key in list(l.subject_fields.keys()):
            row[key] = ", ".join([self._str, self._str])

        for timekey in (l.first_appearance_field,
                        l.published_field):
            if isinstance(timekey, list):
                timekey = timekey[0]
            row[timekey] = self._time.strftime(self.DATE_FORMAT)
        row[self.l.display_author_field] = display_author
        return row

    def test_annotation_citation(self):
        m = self.l.annotation_citation
        row = dict()
        assert None == m(row)
        row[self.l.annotation_author_name_field] = "Alice"
        assert " —Alice" == m(row)
        row[self.l.annotation_author_affiliation_field] = "2nd Street Branch"
        assert " —Alice, 2nd Street Branch" == m(row)
        del row[self.l.annotation_author_name_field]
        assert None == m(row)

    def test_row_to_metadata_complete_success(self):

        row = self.create_row()
        metadata = self.l.row_to_metadata(row)
        assert row[self.l.title_field] == metadata.title
        assert row['author'] == metadata.contributors[0].display_name
        assert row['isbn'] == metadata.identifiers[0].identifier

        expect_pub = datetime.datetime.strptime(
            row['published'], self.DATE_FORMAT).replace(tzinfo=pytz.UTC)
        assert expect_pub == metadata.published
        assert self.l.default_language == metadata.language


    def test_metadata_to_list_entry_complete_success(self):
        row = self.create_row(display_author="Octavia Butler")
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, metadata)
        e = list_entry.edition

        assert row[self.l.title_field] == e.title
        assert "Octavia Butler" == e.author
        assert "Butler, Octavia" == e.sort_author

        i = e.primary_identifier
        assert Identifier.ISBN == i.type
        assert row['isbn'] == i.identifier

        # There should be one description.
        expect = row[self.l.annotation_field] + self.l.annotation_citation(row)
        assert expect == list_entry.annotation

        classifications = i.classifications
        # There should be six classifications, two of type 'tag', two
        # of type 'schema:audience', and two of type
        # 'schema:typicalAgeRange'
        assert 6 == len(classifications)

        tags = [x for x in classifications if x.subject.type==Subject.TAG]
        assert 2 == len(tags)

        audiences = [x for x in classifications
                     if x.subject.type==Subject.FREEFORM_AUDIENCE]
        assert 2 == len(audiences)

        age_ranges = [x for x in classifications
                     if x.subject.type==Subject.AGE_RANGE]
        assert 2 == len(age_ranges)

        expect_first = datetime.datetime.strptime(
            row[self.l.first_appearance_field], self.DATE_FORMAT).replace(tzinfo=pytz.UTC)
        assert expect_first == list_entry.first_appearance
        assert self.now == list_entry.most_recent_appearance


    def test_row_to_item_matching_work_found(self):
        row = self.create_row(display_author="Octavia Butler")
        work = self._work(title=row[self.l.title_field],
                          authors=['Butler, Octavia'])
        self._db.commit()
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, metadata)

        e = list_entry.edition
        assert row[self.l.title_field] == e.title
        assert "Octavia Butler" == e.author
        assert "Butler, Octavia" == e.sort_author

    def test_non_default_language(self):
        row = self.create_row()
        row[self.l.language_field] = 'Spanish'
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, metadata)
        assert 'spa' == list_entry.edition.language

    def test_non_default_language(self):
        row = self.create_row()
        row[self.l.language_field] = 'Spanish'
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, metadata)
        assert 'spa' == list_entry.edition.language

    def test_overwrite_old_data(self):
        self.l.overwrite_old_data = True
        row1 = self.create_row()
        row2 = self.create_row()
        row3 = self.create_row()
        for f in self.l.title_field, self.l.sort_author_field, self.l.display_author_field, 'isbn':
            row2[f] = row1[f]
            row3[f] = row1[f]

        metadata = self.l.row_to_metadata(row1)
        list_entry_1 = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, metadata)

        # Import from the second row, and (e.g.) the new annotation
        # will overwrite the old annotation.

        metadata2 = self.l.row_to_metadata(row2)
        list_entry_2 = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, metadata2)

        assert list_entry_1 == list_entry_2

        assert list_entry_1.annotation == list_entry_2.annotation

        # There are still six classifications.
        i = list_entry_1.edition.primary_identifier
        assert 6 == len(i.classifications)

        # Now import from the third row, but with
        # overwrite_old_data set to False.
        self.l.overwrite_old_data = False

        metadata3 = self.l.row_to_metadata(row3)
        list_entry_3 = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, metadata3)
        assert list_entry_3 == list_entry_1

        # Now there are 12 classifications.
        assert 12 == len(i.classifications)


class BooksInSeries(MembershipManager):
    """A sample implementation of MembershipManager that makes a CustomList
    out of all books that are in some series.
    """

    @property
    def new_membership(self):
        """Only books that are part of a series should be in this list."""
        return self._db.query(Edition).filter(Edition.series != None)


class TestMembershipManager(DatabaseTest):

    def test_update(self):
        # Create two books that are part of series, and one book that
        # is not.
        series1 = self._edition()
        series1.series = "Series 1"

        series2 = self._edition()
        series2.series = "Series Two"

        no_series = self._edition()
        assert None == no_series.series

        update_time = datetime.datetime(2015, 1, 1, tzinfo=pytz.UTC)


        # To create necessary mocked objects,
        # _customlist calls _work
        #    which calls _edition, which makes an edition and a pool (through _licensepool)
        #    then makes work through get_one_or_create
        custom_list, ignore = self._customlist()
        manager = BooksInSeries(custom_list)
        manager.update(update_time)

        [entry1] = [x for x in custom_list.entries if x.edition.series == "Series 1"]
        [entry2] = [x for x in custom_list.entries if x.edition.series == "Series Two"]

        assert update_time == entry1.first_appearance
        assert update_time == entry1.most_recent_appearance

        # In a shocking twist, one of the entries turns out not to
        # have a series, while the entry previously thought not to
        # have a series actually does.
        series2.series = None
        no_series.series = "Actually I do have a series."
        self._db.commit()

        new_update_time = datetime.datetime(2016, 1, 1, tzinfo=pytz.UTC)

        manager.update(new_update_time)

        # Entry #2 has been removed from the list, and a new entry added.
        [old_entry] = [x for x in custom_list.entries if x.edition.series == "Series 1"]
        [new_entry] = [x for x in custom_list.entries if x.edition.series == "Actually I do have a series."]
        assert update_time == old_entry.first_appearance
        assert new_update_time == old_entry.most_recent_appearance
        assert new_update_time == new_entry.first_appearance
        assert new_update_time == new_entry.most_recent_appearance

    def test_classification_based_membership_manager(self):
        e1 = self._edition()
        e2 = self._edition()
        e3 = self._edition()
        source = e1.data_source
        e1.primary_identifier.classify(source, Subject.TAG, "GOOD FOOD")
        e2.primary_identifier.classify(source, Subject.TAG, "barflies")
        e3.primary_identifier.classify(source, Subject.TAG, "irrelevant")

        custom_list, ignore = self._customlist()
        fragments = ["foo", "bar"]
        manager = ClassificationBasedMembershipManager(custom_list, fragments)
        members = list(manager.new_membership)
        assert 2 == len(members)

        # e1 is a member of the list because its primary identifier is
        # classified under a subject that matches %foo%.
        #
        # e2 is a member of the list because its primary identifier is
        # classified under a subject that matches %bar%.
        #
        # e3 is not a member of the list.
        assert e1 in members
        assert e2 in members

