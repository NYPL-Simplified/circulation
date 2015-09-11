# encoding: utf-8
import datetime 
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
    DummyMetadataClient,
)

from model import (
    DataSource,
    Identifier,
    Subject,
)
from external_list import (
    CustomListFromCSV,
)

class TestCustomListFromCSV(DatabaseTest):

    def setup(self):
        super(TestCustomListFromCSV, self).setup()
        self.data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        self.metadata = DummyMetadataClient()
        self.metadata.lookups['Octavia Butler'] = 'Butler, Octavia'
        self.l = CustomListFromCSV(self.data_source.name, "Test list",
                                   metadata_client = self.metadata,
                                   identifier_fields={Identifier.ISBN: "isbn"})
        self.custom_list, ignore = self._customlist(
            data_source_name=self.data_source.name, num_entries=0)
        self.now = datetime.datetime.utcnow()

    DATE_FORMAT = "%Y/%m/%d %H:%M:%S"

    def create_row(self, display_author=None):
        """Create a dummy row for this tests's custom list."""
        l = self.l
        row = dict()
        for scalarkey in (l.title_field, l.annotation_field,
                          l.annotation_author_name_field,
                          l.annotation_author_affiliation_field):
            row[scalarkey] = self._str

        display_author = display_author or self._str
        fn = l.display_author_field
        if isinstance(fn, list):
            fn = fn[0]
        row[fn] = display_author
        row['isbn'] = self._isbn

        for key in l.subject_fields.keys():
            row[key] = ", ".join([self._str, self._str])

        for timekey in (l.first_appearance_field, 
                        l.published_field):
            if isinstance(timekey, list):
                timekey = timekey[0]
            row[timekey] = self._time.strftime(self.DATE_FORMAT)
        return row

    def test_annotation_citation(self):
        m = self.l.annotation_citation
        row = dict()
        eq_(None, m(row))
        row[self.l.annotation_author_name_field] = "Alice"
        eq_(u" —Alice", m(row))
        row[self.l.annotation_author_affiliation_field] = "2nd Street Branch"
        eq_(u" —Alice, 2nd Street Branch", m(row))
        del row[self.l.annotation_author_name_field]
        eq_(None, m(row))

    def test_row_to_title_complete_success(self):

        row = self.create_row()
        title = self.l.row_to_title(self.now, row)
        eq_(row[self.l.title_field], title.metadata.title)
        eq_(row['author'], title.metadata.contributors[0].display_name)
        eq_(row['isbn'], title.metadata.identifiers[0].identifier)

        expect_pub = datetime.datetime.strptime(
            row['published'], self.DATE_FORMAT)
        expect_first = datetime.datetime.strptime(
            row[self.l.first_appearance_field], self.DATE_FORMAT)

        eq_(expect_pub, title.metadata.published)
        eq_(expect_first, title.first_appearance)
        eq_(self.now, title.most_recent_appearance)
        eq_(self.l.default_language, title.metadata.language)


    def test_metadata_to_list_entry_complete_success(self):
        row = self.create_row("Octavia Butler")
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, row)
        e = list_entry.edition

        eq_(row[self.l.title_field], e.title)
        eq_("Octavia Butler", e.author)
        eq_("Butler, Octavia", e.sort_author)

        i = e.primary_identifier
        eq_(Identifier.ISBN, i.type)
        eq_(row['isbn'], i.identifier)

        # There should be one description.
        [link] = i.links
        expect = row[self.l.annotation_field] + self.l.annotation_citation(row)
        eq_(expect.encode("utf8"), link.resource.representation.content)

        classifications = i.classifications
        # There should be six classifications, two of type 'tag' and
        # four of type 'schema:audience'.
        eq_(6, len(classifications))

        tags = [x for x in classifications if x.subject.type==Subject.TAG]
        eq_(2, len(tags))

        audiences = [x for x in classifications
                     if x.subject.type==Subject.FREEFORM_AUDIENCE]
        eq_(4, len(audiences))

    def test_row_to_item_matching_work_found(self):
        row = self.create_row("Octavia Butler")
        work = self._work(title=row[self.l.title_field],
                          authors=['Butler, Octavia'])
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, row)
        # TODO: this needs an assertion
        set_trace()

    def test_non_default_language(self):
        row = self.create_row()
        row[self.l.language_field] = 'Spanish'
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, row)
        eq_('spa', list_entry.edition.language)

    def test_non_default_language(self):
        row = self.create_row()
        row[self.l.language_field] = 'Spanish'
        metadata = self.l.row_to_metadata(row)
        list_entry = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, row)
        eq_('spa', list_entry.edition.language)

    def test_overwrite_old_data(self):
        self.l.overwrite_old_data = True
        row1 = self.create_row()
        row2 = self.create_row()
        row3 = self.create_row()
        for f in self.l.title_field, self.l.sort_author_field, 'isbn':
            row2[f] = row1[f]
            row3[f] = row1[f]

        metadata = self.l.row_to_metadata(row)
        list_entry_1 = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, row)

        # Import from the second row, and (e.g.) the new description
        # will overwrite the old description.

        metadata = self.l.row_to_metadata(row)
        list_entry_2 = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, row2)

        eq_(list_entry_1, list_entry_2)

        i = list_entry_1.edition.primary_identifier
        [link] = i.links
        content = link.resource.representation.content
        assert content.decode("utf8").startswith(row2[self.l.annotation_field])

        # There are six classifications instead of 12.
        descriptions = i.classifications
        eq_(6, len(descriptions))

        # Now import from the third row, but with
        # overwrite_old_data set to False.
        self.l.overwrite_old_data = False

        metadata = self.l.row_to_metadata(row)
        list_entry_3 = self.l.metadata_to_list_entry(
            self.custom_list, self.data_source, self.now, row3)
        eq_(list_entry_3, list_entry_1)

        # Now there are 12 classifications and 2 descriptions.
        eq_(2, len(i.links))
        eq_(12, len(i.classifications))

