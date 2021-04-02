# encoding: utf-8
import datetime
import pytz
import pytest

from ...testing import DatabaseTest
from ...model.integrationclient import IntegrationClient

class TestIntegrationClient(DatabaseTest):

    def setup_method(self):
        super(TestIntegrationClient, self).setup_method()
        self.client = self._integration_client()

    def test_for_url(self):
        now = datetime.datetime.now(tz=pytz.UTC)
        url = self._url
        client, is_new = IntegrationClient.for_url(self._db, url)

        # A new IntegrationClient has been created.
        assert True == is_new

        # Its .url is a normalized version of the provided URL.
        assert client.url == IntegrationClient.normalize_url(url)

        # It has timestamps for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        assert True == isinstance(client.created, datetime.datetime)
        assert client.created == client.last_accessed

        # It does not have a shared secret.
        assert None == client.shared_secret

        # Calling it again on the same URL gives the same object.
        client2, is_new = IntegrationClient.for_url(self._db, url)
        assert client == client2

    def test_register(self):
        now = datetime.datetime.now(tz=pytz.UTC)
        client, is_new = IntegrationClient.register(self._db, self._url)

        # It creates a shared_secret.
        assert client.shared_secret
        # And sets a timestamp for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        assert True == isinstance(client.created, datetime.datetime)
        assert client.created == client.last_accessed

        # It raises an error if the url is already registered and the
        # submitted shared_secret is inaccurate.
        pytest.raises(ValueError, IntegrationClient.register, self._db, client.url)
        pytest.raises(ValueError, IntegrationClient.register, self._db, client.url, 'wrong')

    def test_authenticate(self):

        result = IntegrationClient.authenticate(self._db, "secret")
        assert self.client == result

        result = IntegrationClient.authenticate(self._db, "wrong_secret")
        assert None == result

    def test_normalize_url(self):
        # http/https protocol is removed.
        url = 'https://fake.com'
        assert 'fake.com' == IntegrationClient.normalize_url(url)

        url = 'http://really-fake.com'
        assert 'really-fake.com' == IntegrationClient.normalize_url(url)

        # www is removed if it exists, along with any trailing /
        url = 'https://www.also-fake.net/'
        assert 'also-fake.net' == IntegrationClient.normalize_url(url)

        # Subdomains and paths are retained.
        url = 'https://www.super.fake.org/wow/'
        assert 'super.fake.org/wow' == IntegrationClient.normalize_url(url)

        # URL is lowercased.
        url = 'http://OMG.soVeryFake.gov'
        assert 'omg.soveryfake.gov' == IntegrationClient.normalize_url(url)
