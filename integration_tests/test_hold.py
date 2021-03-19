
import requests
from requests.auth import HTTPBasicAuth
import feedparser
import os

from . import CirculationIntegrationTest

class TestHold(CirculationIntegrationTest):

    def test_hold(self):
        if 'TEST_IDENTIFIER' in os.environ:
            overdrive_id = os.environ['TEST_IDENTIFIER']
        else:
            # Yes Please has a large hold queue
            overdrive_id = "0abe1ed3-f117-4b7c-a6b0-857a2e7d227b"

        borrow_url = "%sworks/Overdrive/%s/borrow" % (self.url, overdrive_id)
        borrow_response = requests.get(borrow_url, auth=HTTPBasicAuth(self.test_username, self.test_password))
        # it's possible we already have the book on hold, if a previous test didn't revoke it
        assert borrow_response.status_code in [200, 201]
        feed = feedparser.parse(borrow_response.text)
        entries = feed['entries']
        eq_(1, len(entries))
        entry = entries[0]

        availability = entry['opds_availability']
        eq_("reserved", availability['status'])

        links = entry['links']
        fulfill_links = [link for link in links if link.rel == "http://opds-spec.org/acquisition"]
        eq_(0, len(fulfill_links))

        revoke_links = [link for link in links if link.rel == "http://librarysimplified.org/terms/rel/revoke"]
        eq_(1, len(revoke_links))
        revoke_url = revoke_links[0].href

        revoke_response = requests.get(revoke_url, auth=HTTPBasicAuth(self.test_username, self.test_password))
        eq_(200, revoke_response.status_code)


