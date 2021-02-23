# encoding: utf-8
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
import datetime
from .. import DatabaseTest
from ...model.integrationclient import IntegrationClient

class TestIntegrationClient(DatabaseTest):

    def setup_method(self):
        super(TestIntegrationClient, self).setup_method()
        self.client = self._integration_client()

    def test_for_url(self):
        now = datetime.datetime.utcnow()
        url = self._url
        client, is_new = IntegrationClient.for_url(self._db, url)

        # A new IntegrationClient has been created.
        eq_(True, is_new)

        # Its .url is a normalized version of the provided URL.
        eq_(client.url, IntegrationClient.normalize_url(url))

        # It has timestamps for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        eq_(True, isinstance(client.created, datetime.datetime))
        eq_(client.created, client.last_accessed)

        # It does not have a shared secret.
        eq_(None, client.shared_secret)

        # Calling it again on the same URL gives the same object.
        client2, is_new = IntegrationClient.for_url(self._db, url)
        eq_(client, client2)

    def test_register(self):
        now = datetime.datetime.utcnow()
        client, is_new = IntegrationClient.register(self._db, self._url)

        # It creates a shared_secret.
        assert client.shared_secret
        # And sets a timestamp for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        eq_(True, isinstance(client.created, datetime.datetime))
        eq_(client.created, client.last_accessed)

        # It raises an error if the url is already registered and the
        # submitted shared_secret is inaccurate.
        assert_raises(ValueError, IntegrationClient.register, self._db, client.url)
        assert_raises(ValueError, IntegrationClient.register, self._db, client.url, 'wrong')

    def test_authenticate(self):

        result = IntegrationClient.authenticate(self._db, u"secret")
        eq_(self.client, result)

        result = IntegrationClient.authenticate(self._db, u"wrong_secret")
        eq_(None, result)

    def test_normalize_url(self):
        # http/https protocol is removed.
        url = 'https://fake.com'
        eq_('fake.com', IntegrationClient.normalize_url(url))

        url = 'http://really-fake.com'
        eq_('really-fake.com', IntegrationClient.normalize_url(url))

        # www is removed if it exists, along with any trailing /
        url = 'https://www.also-fake.net/'
        eq_('also-fake.net', IntegrationClient.normalize_url(url))

        # Subdomains and paths are retained.
        url = 'https://www.super.fake.org/wow/'
        eq_('super.fake.org/wow', IntegrationClient.normalize_url(url))

        # URL is lowercased.
        url = 'http://OMG.soVeryFake.gov'
        eq_('omg.soveryfake.gov', IntegrationClient.normalize_url(url))
