# encoding: utf-8
import os
from nose.tools import (
    set_trace, eq_,
    assert_raises,
    assert_raises_regexp,
)
import datetime
import json

from . import (
    DatabaseTest,
)
from core.testing import DummyMetadataClient
from core.config import CannotLoadConfiguration
from api.nyt import (
    NYTBestSellerAPI,
    NYTBestSellerList,
    NYTBestSellerListTitle,
)
from core.model import (
    Contributor,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Resource,
    CustomListEntry,
)
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    MockMetadataWranglerOPDSLookup
)
from core.util.http import IntegrationException


class DummyNYTBestSellerAPI(NYTBestSellerAPI):

    def __init__(self, _db):
        self._db = _db
        self.metadata_client = DummyMetadataClient()

    def sample_json(self, filename):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "nyt")
        path = os.path.join(resource_path, filename)
        data = open(path).read()
        return json.loads(data)

    def list_of_lists(self):
        return self.sample_json("bestseller_list_list.json")

    def update(self, list, date=None, max_age=None):
        if date:
            filename = "list_%s_%s.json" % (list.foreign_identifier, self.date_string(date))
        else:
            filename = "list_%s.json" % list.foreign_identifier
        list.update(self.sample_json(filename))

class NYTBestSellerAPITest(DatabaseTest):

    def setup(self):
        super(NYTBestSellerAPITest, self).setup()
        self.api = DummyNYTBestSellerAPI(self._db)
        self.metadata_client = DummyMetadataClient()

class TestNYTBestSellerAPI(NYTBestSellerAPITest):

    """Test the API calls."""

    def test_from_config(self):
        # You have to have an ExternalIntegration for the NYT.
        assert_raises_regexp(
            CannotLoadConfiguration,
            "No ExternalIntegration found for the NYT.",
            NYTBestSellerAPI.from_config, self._db
        )
        integration = self._external_integration(
            protocol=ExternalIntegration.NYT,
            goal=ExternalIntegration.METADATA_GOAL
        )

        # It has to have the api key in its 'password' setting.
        assert_raises_regexp(
            CannotLoadConfiguration,
            "No NYT API key is specified",
            NYTBestSellerAPI.from_config, self._db
        )

        integration.password = "api key"

        # It's okay if you don't have a Metadata Wrangler configuration
        # configured.
        api = NYTBestSellerAPI.from_config(self._db)
        eq_("api key", api.api_key)
        eq_(None, api.metadata_client)

        # But if you do, it's picked up.
        mw = self._external_integration(
            protocol=ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL
        )
        mw.url = self._url

        api = NYTBestSellerAPI.from_config(self._db)
        assert isinstance(api.metadata_client, MetadataWranglerOPDSLookup)
        assert api.metadata_client.base_url.startswith(mw.url)

        # external_integration() finds the integration used to create
        # the API object.
        eq_(integration, api.external_integration(self._db))

    def test_run_self_tests(self):
        class Mock(NYTBestSellerAPI):
            def __init__(self):
                pass
            def list_of_lists(self):
                return "some lists"

        [list_test] = Mock()._run_self_tests(object())
        eq_("Getting list of best-seller lists", list_test.name)
        eq_(True, list_test.success)
        eq_("some lists", list_test.result)

    def test_list_of_lists(self):
        all_lists = self.api.list_of_lists()
        eq_(['copyright', 'num_results', 'results', 'status'],
            sorted(all_lists.keys()))
        eq_(47, len(all_lists['results']))

    def test_list_info(self):
        list_info = self.api.list_info("combined-print-and-e-book-fiction")
        eq_("Combined Print & E-Book Fiction", list_info['display_name'])

    def test_request_failure(self):
        """Verify that certain unexpected HTTP results are turned into
        IntegrationExceptions.
        """
        self.api.api_key = "some key"
        def result_403(*args, **kwargs):
            return 403, None, None
        self.api.do_get = result_403
        assert_raises_regexp(
            IntegrationException, "API authentication failed",
            self.api.request, "some path"
        )

        def result_500(*args, **kwargs):
            return 500, {}, "bad value"
        self.api.do_get = result_500
        try:
            self.api.request("some path")
            raise Exception("Expected an IntegrationException!")
        except IntegrationException as e:
            eq_("Unknown API error (status 500)", e.message)
            assert e.debug_message.startswith("Response from")
            assert e.debug_message.endswith("was: 'bad value'")

class TestNYTBestSellerList(NYTBestSellerAPITest):

    """Test the NYTBestSellerList object and its ability to be turned
    into a CustomList.
    """

    def test_creation(self):
        """Just creating a list doesn't add any items to it."""
        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        eq_(True, isinstance(l, NYTBestSellerList))
        eq_(0, len(l))

    def test_medium(self):
        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        eq_("Combined Print & E-Book Fiction", l.name)
        eq_(Edition.BOOK_MEDIUM, l.medium)

        l.name = "Audio Nonfiction"
        eq_(Edition.AUDIO_MEDIUM, l.medium)

    def test_update(self):
        list_name = "combined-print-and-e-book-fiction"
        self.metadata_client.lookups['Paula Hawkins'] = 'Hawkins, Paula'
        l = self.api.best_seller_list(list_name)
        self.api.update(l)

        eq_(20, len(l))
        eq_(True, all([isinstance(x, NYTBestSellerListTitle) for x in l]))
        eq_(datetime.datetime(2011, 2, 13), l.created)
        eq_(datetime.datetime(2015, 2, 1), l.updated)
        eq_(list_name, l.foreign_identifier)

        # Let's do a spot check on the list items.
        title = [x for x in l if x.metadata.title=='THE GIRL ON THE TRAIN'][0]
        [isbn] = title.metadata.identifiers
        eq_("ISBN", isbn.type)
        eq_("9780698185395", isbn.identifier)

        # The list's medium is propagated to its Editions.
        eq_(l.medium, title.metadata.medium)

        [contributor] = title.metadata.contributors
        eq_("Paula Hawkins", contributor.display_name)
        eq_("Riverhead", title.metadata.publisher)
        eq_("A psychological thriller set in London is full of complications and betrayals.",
            title.annotation)
        eq_(datetime.datetime(2015, 1, 17), title.first_appearance)
        eq_(datetime.datetime(2015, 2, 1), title.most_recent_appearance)

    def test_historical_dates(self):
        """This list was published 208 times since the start of the API,
        and we can figure out when.
        """
        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        dates = list(l.all_dates)
        eq_(208, len(dates))
        eq_(l.updated, dates[0])
        eq_(l.created, dates[-1])

    def test_to_customlist(self):
        list_name = "combined-print-and-e-book-fiction"
        self.metadata_client.lookups['Paula Hawkins'] = 'Hawkins, Paula'
        l = self.api.best_seller_list(list_name)
        self.api.update(l)
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

        feb_1 = datetime.datetime(2015, 2, 1)
        eq_(True,
            all([x.most_recent_appearance == feb_1 for x in custom.entries]))

        # Now replace this list's entries with the entries from a
        # different list. We wouldn't do this in real life, but it's
        # a convenient way to change the contents of a list.
        other_nyt_list = self.api.best_seller_list('hardcover-fiction')
        self.api.update(other_nyt_list)
        other_nyt_list.update_custom_list(custom)

        # The CustomList now contains elements from both NYT lists.
        eq_(40, len(custom.entries))

    def test_fill_in_history(self):
        list_name = "espionage"
        l = self.api.best_seller_list(list_name)
        self.api.fill_in_history(l)

        # Each 'espionage' best-seller list contains 15 items. Since
        # we picked two, from consecutive months, there's quite a bit
        # of overlap, and we end up with 20.
        eq_(20, len(l))


class TestNYTBestSellerListTitle(NYTBestSellerAPITest):

    one_list_title = json.loads("""{"list_name":"Combined Print and E-Book Fiction","display_name":"Combined Print & E-Book Fiction","bestsellers_date":"2015-01-17","published_date":"2015-02-01","rank":1,"rank_last_week":0,"weeks_on_list":1,"asterisk":0,"dagger":0,"amazon_product_url":"http:\/\/www.amazon.com\/The-Girl-Train-A-Novel-ebook\/dp\/B00L9B7IKE?tag=thenewyorktim-20","isbns":[{"isbn10":"1594633665","isbn13":"9781594633669"},{"isbn10":"0698185390","isbn13":"9780698185395"}],"book_details":[{"title":"THE GIRL ON THE TRAIN","description":"A psychological thriller set in London is full of complications and betrayals.","contributor":"by Paula Hawkins","author":"Paula Hawkins","contributor_note":"","price":0,"age_group":"","publisher":"Riverhead","isbns":[{"isbn10":"1594633665","isbn13":"9781594633669"},{"isbn10":"0698185390","isbn13":"9780698185395"}],"primary_isbn13":"9780698185395","primary_isbn10":"0698185390"}],"reviews":[{"book_review_link":"","first_chapter_link":"","sunday_review_link":"","article_chapter_link":""}]}""")

    def test_creation(self):
        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)

        edition = title.to_edition(self._db, self.metadata_client)
        eq_("9780698185395", edition.primary_identifier.identifier)

        # The alternate ISBN is marked as equivalent to the primary identifier,
        # but at a greatly reduced strength.
        [equivalency] = [x for x in edition.primary_identifier.equivalencies]
        eq_("9781594633669", equivalency.output.identifier)
        eq_(0.5, equivalency.strength)
        # That strength is not enough to make the alternate ISBN an equivalent
        # identifier for the edition.
        equivalent_identifiers = [
            (x.type, x.identifier) for x in edition.equivalent_identifiers()
        ]
        eq_([("ISBN", "9780698185395")], sorted(equivalent_identifiers))

        eq_(datetime.datetime(2015, 2, 1, 0, 0), edition.published)
        eq_("Paula Hawkins", edition.author)
        eq_("Hawkins, Paula", edition.sort_author)
        eq_("Riverhead", edition.publisher)

    def test_to_edition_sets_sort_author_name_if_obvious(self):
        [contributor], ignore = Contributor.lookup(
            self._db, "Hawkins, Paula")
        contributor.display_name = "Paula Hawkins"

        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)
        edition = title.to_edition(self._db, self.metadata_client)
        eq_(contributor.sort_name, edition.sort_author)
        eq_(contributor.display_name, edition.author)
        assert edition.permanent_work_id is not None

    def test_to_edition_sets_sort_author_name_if_metadata_client_provides_it(self):

        # Set the metadata client up for success.
        self.metadata_client.lookups["Paula Hawkins"] = "Hawkins, Paula Z."

        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)
        edition = title.to_edition(self._db, self.metadata_client)
        eq_("Hawkins, Paula Z.", edition.sort_author)
        assert edition.permanent_work_id is not None
