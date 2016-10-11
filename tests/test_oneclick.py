from nose.tools import (
    assert_raises_regexp,
    eq_, 
    set_trace,
)

import os

from oneclick import (
    OneClickAPI,
    MockOneClickAPI,
)

from util.http import (
    RemoteIntegrationException,
    HTTP,
)

from . import DatabaseTest
from scripts import RunCoverageProviderScript
from testing import MockRequestsResponse


class OneClickTest(DatabaseTest):

    def get_data(self, filename):
        path = os.path.join(
            os.path.split(__file__)[0], "files/oneclick/", filename)
        return open(path).read()


class TestOneClickAPI(OneClickTest):

    def test_create_identifier_strings(self):
        identifier = self._identifier()
        values = OneClickAPI.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)


    def test_availability_exception(self):
        api = MockOneClickAPI(self._db)
        api.queue_response(500)
        assert_raises_regexp(
            RemoteIntegrationException, "Bad response from www.oneclickapi.testv1/libraries/library_id_123/search: Got status code 500 from external server, cannot continue.", 
            api.get_all_available_through_search
        )

    def test_search(self):
        api = MockOneClickAPI(self._db)
        data = self.get_data("response_search_one_item_1.json")
        api.queue_response(status_code=200, content=data)

        response = api.search(mediatype='ebook', author="Alexander Mccall Smith", title="Tea Time for the Traditionally Built")
        response_dictionary = response.json()
        eq_(1, response_dictionary['pageCount'])
        eq_(u'Tea Time for the Traditionally Built', response_dictionary['items'][0]['item']['title'])

    def test_get_all_available_through_search(self):
        api = MockOneClickAPI(self._db)
        data = self.get_data("response_search_five_items_1.json")
        api.queue_response(status_code=200, content=data)

        response_dictionary = api.get_all_available_through_search()
        eq_(1, response_dictionary['pageCount'])
        eq_(5, response_dictionary['resultSetCount'])
        eq_(5, len(response_dictionary['items']))
        returned_titles = [iteminterest['item']['title'] for iteminterest in response_dictionary['items']]
        assert (u'Unusual Uses for Olive Oil' in returned_titles)

    def test_get_ebook_availability_info(self):
        api = MockOneClickAPI(self._db)
        data = self.get_data("response_availability_ebook_1.json")
        api.queue_response(status_code=200, content=data)
        response_list = api.get_ebook_availability_info()
        eq_(u'9781420128567', response_list[0]['isbn'])
        eq_(False, response_list[0]['availability'])



