# encoding: utf-8
import os
from pdb import set_trace
import pytest
import datetime
import dateutil
import json

from core.testing import (
    DatabaseTest,
)
from core.testing import DummyMetadataClient
from core.config import CannotLoadConfiguration
from api.nyt import (
    NYTAPI,
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

    def setup_method(self):
        super(NYTBestSellerAPITest, self).setup_method()
        self.api = DummyNYTBestSellerAPI(self._db)
        self.metadata_client = DummyMetadataClient()

    def _midnight(self, *args):
        """Create a datetime representing midnight Eastern time (the time we
        take NYT best-seller lists to be published) on a certain date.
        """
        return datetime.datetime(*args, tzinfo=NYTAPI.TIME_ZONE)


class TestNYTBestSellerAPI(NYTBestSellerAPITest):

    """Test the API calls."""

    def test_from_config(self):
        # You have to have an ExternalIntegration for the NYT.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            NYTBestSellerAPI.from_config(self._db)
        assert "No ExternalIntegration found for the NYT." in str(excinfo.value)
        integration = self._external_integration(
            protocol=ExternalIntegration.NYT,
            goal=ExternalIntegration.METADATA_GOAL
        )

        # It has to have the api key in its 'password' setting.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            NYTBestSellerAPI.from_config(self._db)
        assert "No NYT API key is specified" in str(excinfo.value)

        integration.password = "api key"

        # It's okay if you don't have a Metadata Wrangler configuration
        # configured.
        api = NYTBestSellerAPI.from_config(self._db)
        assert "api key" == api.api_key
        assert None == api.metadata_client

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
        assert integration == api.external_integration(self._db)

    def test_run_self_tests(self):
        class Mock(NYTBestSellerAPI):
            def __init__(self):
                pass
            def list_of_lists(self):
                return "some lists"

        [list_test] = Mock()._run_self_tests(object())
        assert "Getting list of best-seller lists" == list_test.name
        assert True == list_test.success
        assert "some lists" == list_test.result

    def test_list_of_lists(self):
        all_lists = self.api.list_of_lists()
        assert (['copyright', 'num_results', 'results', 'status'] ==
            sorted(all_lists.keys()))
        assert 47 == len(all_lists['results'])

    def test_list_info(self):
        list_info = self.api.list_info("combined-print-and-e-book-fiction")
        assert "Combined Print & E-Book Fiction" == list_info['display_name']

    def test_request_failure(self):
        # Verify that certain unexpected HTTP results are turned into
        # IntegrationExceptions.

        self.api.api_key = "some key"
        def result_403(*args, **kwargs):
            return 403, None, None
        self.api.do_get = result_403
        with pytest.raises(IntegrationException) as excinfo:
            self.api.request("some path")
        assert "API authentication failed" in str(excinfo.value)

        def result_500(*args, **kwargs):
            return 500, {}, "bad value"
        self.api.do_get = result_500
        try:
            self.api.request("some path")
            raise Exception("Expected an IntegrationException!")
        except IntegrationException as e:
            assert "Unknown API error (status 500)" == str(e)
            assert e.debug_message.startswith("Response from")
            assert e.debug_message.endswith("was: 'bad value'")

class TestNYTBestSellerList(NYTBestSellerAPITest):

    """Test the NYTBestSellerList object and its ability to be turned
    into a CustomList.
    """

    def test_creation(self):
        # Just creating a list doesn't add any items to it.
        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        assert True == isinstance(l, NYTBestSellerList)
        assert 0 == len(l)

    def test_medium(self):
        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        assert "Combined Print & E-Book Fiction" == l.name
        assert Edition.BOOK_MEDIUM == l.medium

        l.name = "Audio Nonfiction"
        assert Edition.AUDIO_MEDIUM == l.medium

    def test_update(self):
        list_name = "combined-print-and-e-book-fiction"
        self.metadata_client.lookups['Paula Hawkins'] = 'Hawkins, Paula'
        l = self.api.best_seller_list(list_name)
        self.api.update(l)

        assert 20 == len(l)
        assert True == all([isinstance(x, NYTBestSellerListTitle) for x in l])
        assert self._midnight(2011, 2, 13) == l.created
        assert self._midnight(2015, 2, 1) == l.updated
        assert list_name == l.foreign_identifier

        # Let's do a spot check on the list items.
        title = [x for x in l if x.metadata.title=='THE GIRL ON THE TRAIN'][0]
        [isbn] = title.metadata.identifiers
        assert "ISBN" == isbn.type
        assert "9780698185395" == isbn.identifier

        # The list's medium is propagated to its Editions.
        assert l.medium == title.metadata.medium

        [contributor] = title.metadata.contributors
        assert "Paula Hawkins" == contributor.display_name
        assert "Riverhead" == title.metadata.publisher
        assert ("A psychological thriller set in London is full of complications and betrayals." ==
            title.annotation)
        assert self._midnight(2015, 1, 17) == title.first_appearance
        assert self._midnight(2015, 2, 1) == title.most_recent_appearance

    def test_historical_dates(self):
        # This list was published 208 times since the start of the API,
        # and we can figure out when.

        list_name = "combined-print-and-e-book-fiction"
        l = self.api.best_seller_list(list_name)
        dates = list(l.all_dates)
        assert 208 == len(dates)
        assert l.updated == dates[0]
        assert l.created == dates[-1]

    def test_to_customlist(self):
        list_name = "combined-print-and-e-book-fiction"
        self.metadata_client.lookups['Paula Hawkins'] = 'Hawkins, Paula'
        l = self.api.best_seller_list(list_name)
        self.api.update(l)
        custom = l.to_customlist(self._db)
        assert custom.created == l.created
        assert custom.updated == l.updated
        assert custom.name == l.name
        assert len(l) == len(custom.entries)
        assert True == all([isinstance(x, CustomListEntry)
                       for x in custom.entries])

        assert 20 == len(custom.entries)

        # The publication of a NYT best-seller list is treated as
        # midnight Eastern time on the publication date.
        jan_17 = self._midnight(2015, 1, 17)
        assert all([x.first_appearance.timestamp() == jan_17.timestamp() for x in custom.entries]) is True

        feb_1 = self._midnight(2015, 2, 1)
        assert all([x.most_recent_appearance.timestamp() == feb_1.timestamp() for x in custom.entries]) is True

        # Now replace this list's entries with the entries from a
        # different list. We wouldn't do this in real life, but it's
        # a convenient way to change the contents of a list.
        other_nyt_list = self.api.best_seller_list('hardcover-fiction')
        self.api.update(other_nyt_list)
        other_nyt_list.update_custom_list(custom)

        # The CustomList now contains elements from both NYT lists.
        assert 40 == len(custom.entries)

    def test_fill_in_history(self):
        list_name = "espionage"
        l = self.api.best_seller_list(list_name)
        self.api.fill_in_history(l)

        # Each 'espionage' best-seller list contains 15 items. Since
        # we picked two, from consecutive months, there's quite a bit
        # of overlap, and we end up with 20.
        assert 20 == len(l)


class TestNYTBestSellerListTitle(NYTBestSellerAPITest):

    one_list_title = json.loads("""{"list_name":"Combined Print and E-Book Fiction","display_name":"Combined Print & E-Book Fiction","bestsellers_date":"2015-01-17","published_date":"2015-02-01","rank":1,"rank_last_week":0,"weeks_on_list":1,"asterisk":0,"dagger":0,"amazon_product_url":"http:\/\/www.amazon.com\/The-Girl-Train-A-Novel-ebook\/dp\/B00L9B7IKE?tag=thenewyorktim-20","isbns":[{"isbn10":"1594633665","isbn13":"9781594633669"},{"isbn10":"0698185390","isbn13":"9780698185395"}],"book_details":[{"title":"THE GIRL ON THE TRAIN","description":"A psychological thriller set in London is full of complications and betrayals.","contributor":"by Paula Hawkins","author":"Paula Hawkins","contributor_note":"","price":0,"age_group":"","publisher":"Riverhead","isbns":[{"isbn10":"1594633665","isbn13":"9781594633669"},{"isbn10":"0698185390","isbn13":"9780698185395"}],"primary_isbn13":"9780698185395","primary_isbn10":"0698185390"}],"reviews":[{"book_review_link":"","first_chapter_link":"","sunday_review_link":"","article_chapter_link":""}]}""")

    def test_creation(self):
        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)

        edition = title.to_edition(self._db, self.metadata_client)
        assert "9780698185395" == edition.primary_identifier.identifier

        # The alternate ISBN is marked as equivalent to the primary identifier,
        # but at a greatly reduced strength.
        [equivalency] = [x for x in edition.primary_identifier.equivalencies]
        assert "9781594633669" == equivalency.output.identifier
        assert 0.5 == equivalency.strength
        # That strength is not enough to make the alternate ISBN an equivalent
        # identifier for the edition.
        equivalent_identifiers = [
            (x.type, x.identifier) for x in edition.equivalent_identifiers()
        ]
        assert [("ISBN", "9780698185395")] == sorted(equivalent_identifiers)

        assert datetime.date(2015, 2, 1) == edition.published
        assert "Paula Hawkins" == edition.author
        assert "Hawkins, Paula" == edition.sort_author
        assert "Riverhead" == edition.publisher

    def test_to_edition_sets_sort_author_name_if_obvious(self):
        [contributor], ignore = Contributor.lookup(
            self._db, "Hawkins, Paula")
        contributor.display_name = "Paula Hawkins"

        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)
        edition = title.to_edition(self._db, self.metadata_client)
        assert contributor.sort_name == edition.sort_author
        assert contributor.display_name == edition.author
        assert edition.permanent_work_id is not None

    def test_to_edition_sets_sort_author_name_if_metadata_client_provides_it(self):

        # Set the metadata client up for success.
        self.metadata_client.lookups["Paula Hawkins"] = "Hawkins, Paula Z."

        title = NYTBestSellerListTitle(self.one_list_title, Edition.BOOK_MEDIUM)
        edition = title.to_edition(self._db, self.metadata_client)
        assert "Hawkins, Paula Z." == edition.sort_author
        assert edition.permanent_work_id is not None
