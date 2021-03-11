
import requests
from requests.auth import HTTPBasicAuth
import feedparser
import os

from . import CirculationIntegrationTest

class TestBorrow(CirculationIntegrationTest):

    def test_borrow(self):
        if 'TEST_IDENTIFIER' in os.environ:
            overdrive_id = os.environ['TEST_IDENTIFIER']
        else:
            # Fifty Shades of Grey has a large number of copies available
            overdrive_id = "82cdd641-857a-45ca-8775-34eede35b238"
        borrow_url = "%sworks/Overdrive/%s/borrow" % (self.url, overdrive_id)
        borrow_response = requests.get(borrow_url, auth=HTTPBasicAuth(self.test_username, self.test_password))

        # it's possible we already have the book borrowed, if a previous test didn't revoke it
        assert borrow_response.status_code in [200, 201]
        feed = feedparser.parse(borrow_response.text)
        entries = feed['entries']
        eq_(1, len(entries))
        entry = entries[0]

        links = entry['links']
        fulfill_links = [link for link in links if link.rel == "http://opds-spec.org/acquisition"]
        assert len(fulfill_links) > 0
        fulfill_url = fulfill_links[0].href
        fulfill_response = requests.get(fulfill_url, auth=HTTPBasicAuth(self.test_username, self.test_password))
        eq_(200, fulfill_response.status_code)


        revoke_links = [link for link in links if link.rel == "http://librarysimplified.org/terms/rel/revoke"]
        eq_(1, len(revoke_links))
        revoke_url = revoke_links[0].href

        revoke_response = requests.get(revoke_url, auth=HTTPBasicAuth(self.test_username, self.test_password))
        eq_(200, revoke_response.status_code)
