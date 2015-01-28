# encoding: utf-8
import os
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)
import datetime
import json

from . import DatabaseTest
from ..bibliocommons import (
    BibliocommonsAPI,
    BibliocommonsListItem,
    BibliocommonsTitle
)
from ..core.model import (
    Contributor,
    Edition,
    Identifier,
)

class DummyBibliocommonsAPI(BibliocommonsAPI):

    def sample_json(self, filename):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "bibliocommons")
        path = os.path.join(resource_path, filename)
        data = open(path).read()
        return json.loads(data)

    def list_pages_for_user(self, user_id, max_age=None):
        for pagenum in range(1,4):
            yield self.sample_json(
                "list_of_user_lists_page%d.json" % pagenum)

    def get_list(self, list_id):
        data = self.sample_json("list_%s.json" % list_id)
        return self._make_list(data)

    def get_title(self, title_id):
        data = self.sample_json("title_detail_%s.json" % title_id)
        return self._make_title(data)

class TestBibliocommonsAPI(DatabaseTest):
    
    def setup(self):
        super(TestBibliocommonsAPI, self).setup()
        self.api = DummyBibliocommonsAPI(self._db)

    def test_list_data_for_user(self):

        all_lists = list(self.api.list_data_for_user("any user"))
        eq_(28, len(all_lists))
        first_list = all_lists[0]

        # Basic list data is present.
        eq_('331352747', first_list['id'])

        # Updated and created dates have been converted to datetimes.
        eq_(datetime.datetime(2014, 9, 30, 20, 55, 13), first_list['updated'])
        eq_(datetime.datetime(2014, 9, 30, 20, 30, 25), first_list['created'])

    def test_list_with_non_titles(self):
        # Two of the items in this list are URLs, not titles.
        list_1 = self.api.get_list("358549907")
        non_titles = [
            x for x in list_1 if x.type != BibliocommonsListItem.TITLE_TYPE]
        assert all(['url' in x.item for x in non_titles])
        assert all(['title' not in x.item for x in non_titles])

    def test_list_with_titles(self):
        l = self.api.get_list("371050767")
        
        # Make sure all the list items were converted to
        # BibliocommonsTitle objects.
        assert all([isinstance(x.item, BibliocommonsTitle) for x in l])

        # Make sure all the annotations got picked up.
        annotations = sorted([x.annotation for x in l])
        eq_([u'', u'Out in February', u'Out in Februrary', u'Out in January',
             u'Out in January', u'Out in January', u'Out in January', 
             u'Out in January', u'Out in January'], annotations)

    def test_title_info(self):
        info = self.api.get_title("20172591052907")
        eq_("Snow", info['title'])
        eq_("20172591052907", info['id'])

    def test_title_to_edition(self):
        title = self.api.get_title("20172591052907")
        edition = title.to_edition(self._db)

        eq_("Snow", edition.title)
        eq_(Edition.BOOK_MEDIUM, edition.medium)
        eq_("eng", edition.language)

        eq_(datetime.datetime(2012, 1, 1, 0, 0), edition.published)

        [cont] = edition.contributions
        eq_("Shulevitz, Uri", cont.contributor.name)
        eq_(Contributor.PRIMARY_AUTHOR_ROLE, cont.role)

        eq_("Shulevitz, Uri", edition.author)
        eq_("Shulevitz, Uri", edition.sort_author)

        # We were given an ISBN-10 and an equivalent ISBN-13 for this
        # book. Only the ISBN-13 was recorded.
        [isbn] = [x.identifier
                 for x in edition.equivalent_identifiers()
                 if x.type == Identifier.ISBN]
        eq_("9780374370930", isbn)

    def test_list_to_customlist(self):
        bib_list = self.api.get_list("371050767")
        custom_list = bib_list.to_customlist(self._db)

        eq_(bib_list.name, custom_list.name)
        eq_(bib_list.description, custom_list.description)
        eq_(bib_list.created, custom_list.created)
        eq_(bib_list.updated, custom_list.updated)

        initial_entry_list = list(custom_list.entries)

        bib_titles = sorted([x.item['title'] for x in bib_list])
        custom_titles = sorted([x.edition.title for x in initial_entry_list])
        eq_(bib_titles, custom_titles)

        bib_annotations = sorted([x.annotation for x in bib_list])
        custom_annotations = sorted([x.annotation for x in initial_entry_list])
        eq_(bib_annotations, custom_annotations)
        eq_(True, all([x.added == custom_list.updated for x in initial_entry_list]))
        eq_(True, all([x.removed is None for x in initial_entry_list]))

        # Now replace this list's entries with the entries from a
        # different list. We wouldn't do this in real life, but it's
        # a convenient way to change the contents of a list.
        other_bibliocommons_list = self.api.get_list("379257178")
        other_bibliocommons_list.update_custom_list(custom_list)

        # The CustomList now contains elements from both Bibliocommons lists.
        new_entries = list(custom_list.entries)
        assert (len(new_entries) == len(initial_entry_list)
                + len(other_bibliocommons_list.items))

        # But all the old entries have had their 'removed' dates set
        # to the date the other list was updated.
        eq_(True, all([x.removed == other_bibliocommons_list.updated
                       for x in initial_entry_list]))

