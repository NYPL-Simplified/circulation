# encoding: utf-8
import os
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)
import datetime
import json

from . import DatabaseTest
from nyt import (
    NYTBestSellerAPI,
    NYTBestSellerList,
    NYTBestSellerListTitle,
)
from model import (
    Contributor,
    Edition,
    Identifier,
    Resource,
    CustomListEntry,
)

class DummyNYTBestSellerAPI(NYTBestSellerAPI):

    def sample_json(self, filename):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "nyt")
        path = os.path.join(resource_path, filename)
        data = open(path).read()
        return json.loads(data)

    def list_of_lists(self):
        return self.sample_json("bestseller_list_list.json")

    def best_seller_list(self, list_info):
        if isinstance(list_info, basestring):
            list_info = self.list_info(list_info)
        name = list_info['list_name_encoded']
        list_data = self.sample_json("list_%s.json" % name)
        return self._make_list(list_info, list_data)


class NYTBestSellerAPITest(DatabaseTest):

    def setup(self):
        super(NYTBestSellerAPITest, self).setup()
        self.api = DummyNYTBestSellerAPI(self._db)


class TestNYTBestSellerAPI(NYTBestSellerAPITest):
    
    """Test the API calls."""

    def test_list_of_lists(self):
        all_lists = self.api.list_of_lists()
        eq_([u'copyright', u'num_results', u'results', u'status'],
            sorted(all_lists.keys()))
        eq_(47, len(all_lists['results']))

    def test_list_info(self):
        list_info = self.api.list_info("combined-print-and-e-book-fiction")
        eq_("Combined Print & E-Book Fiction", list_info['display_name'])

class TestNYTBestSellerList(NYTBestSellerAPITest):

    """Test the NYTBestSellerList object and its ability to be turned
    into a CustomList.
    """

    def test_creation(self):
        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        eq_(True, isinstance(l, NYTBestSellerList))
        eq_(20, len(l))
        eq_(True, all([isinstance(x, NYTBestSellerListTitle) for x in l]))
        eq_(datetime.datetime(2011, 2, 13), l.created)
        eq_(datetime.datetime(2015, 2, 1), l.updated)
        eq_(list_name, l.foreign_identifier)

        # Let's do a spot check on the list items.
        title = [x for x in l if x.title=='THE GIRL ON THE TRAIN'][0]
        eq_("9780698185395", title.primary_isbn13)
        eq_("0698185390", title.primary_isbn10)
        eq_(["9780698185395", "9781594633669"], sorted(title.isbns))

        eq_("Paula Hawkins", title.display_author)
        eq_("Riverhead", title.publisher)
        eq_("A psychological thriller set in London is full of complications and betrayals.", 
            title.description)
        eq_(datetime.datetime(2015, 1, 17), title.bestsellers_date)
        eq_(datetime.datetime(2015, 2, 01), title.published_date)

    def test_to_customlist(self):
        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        custom = l.to_customlist(self._db)
        eq_(custom.created, l.created)
        eq_(custom.updated, l.updated)
        eq_(custom.name, l.name)
        eq_(len(l), len(custom.entries))
        eq_(True, all([isinstance(x, CustomListEntry) 
                       for x in custom.entries]))

        eq_(20, len(custom.entries))
        january_17 = datetime.datetime(2015, 1, 17)
        eq_(True,
            all([x.first_appearance == january_17 for x in custom.entries]))
        eq_(True,
            all([x.most_recent_appearance == january_17 for x in custom.entries]))

        # Now replace this list's entries with the entries from a
        # different list. We wouldn't do this in real life, but it's
        # a convenient way to change the contents of a list.
        other_nyt_list = l = self.api.best_seller_list('hardcover-fiction')
        other_nyt_list.update_custom_list(custom)

        # The CustomList now contains elements from both NYT lists.
        eq_(40, len(custom.entries))


class TestNYTBestSellerListTitle(NYTBestSellerAPITest):

    one_list_title = json.loads("""{"list_name":"Combined Print and E-Book Fiction","display_name":"Combined Print & E-Book Fiction","bestsellers_date":"2015-01-17","published_date":"2015-02-01","rank":1,"rank_last_week":0,"weeks_on_list":1,"asterisk":0,"dagger":0,"amazon_product_url":"http:\/\/www.amazon.com\/The-Girl-Train-A-Novel-ebook\/dp\/B00L9B7IKE?tag=thenewyorktim-20","isbns":[{"isbn10":"1594633665","isbn13":"9781594633669"},{"isbn10":"0698185390","isbn13":"9780698185395"}],"book_details":[{"title":"THE GIRL ON THE TRAIN","description":"A psychological thriller set in London is full of complications and betrayals.","contributor":"by Paula Hawkins","author":"Paula Hawkins","contributor_note":"","price":0,"age_group":"","publisher":"Riverhead","primary_isbn13":"9780698185395","primary_isbn10":"0698185390"}],"reviews":[{"book_review_link":"","first_chapter_link":"","sunday_review_link":"","article_chapter_link":""}]}""")

    def test_creation(self):
        title = NYTBestSellerListTitle(self.one_list_title)

        edition = title.to_edition(self._db)
        eq_("9780698185395", edition.primary_identifier.identifier)

        equivalent_identifiers = [
            (x.type, x.identifier) for x in edition.equivalent_identifiers()]
        eq_([("ISBN", "9780698185395"),
             ("ISBN", "9781594633669"),
         ], sorted(equivalent_identifiers))

        eq_(datetime.datetime(2015, 2, 01), edition.published)
        eq_("Paula Hawkins", edition.author)
        # Note that this is None; the next test shows when it gets set.
        eq_(None, edition.sort_author)
        eq_(None, edition.permanent_work_id)
        eq_("Riverhead", edition.publisher)

        [description] = self._db.query(Resource).filter(
            Resource.data_source==edition.data_source).filter(
                Resource.identifier==edition.primary_identifier).filter(
                    Resource.rel==Resource.DESCRIPTION)
        eq_("A psychological thriller set in London is full of complications and betrayals.", description.content)
        eq_("text/plain", description.media_type)
        
    def test_to_edition_sets_sort_author_name_if_obvious(self):
        [contributor], ignore = Contributor.lookup(
            self._db, "Hawkins, Paula")
        contributor.display_name = "Paula Hawkins"

        title = NYTBestSellerListTitle(self.one_list_title)
        edition = title.to_edition(self._db)
        eq_(contributor.name, edition.sort_author)
        assert edition.permanent_work_id is not None
