# encoding: utf-8
import datetime
import pytest

from ...model.integrationclient import IntegrationClient
from ...util.datetime_helpers import utc_now

class TestIntegrationClient:

    @pytest.fixture(autouse=True)
    def setup_method(self, db_session, create_integration_client):
        self.client = create_integration_client(db_session)

    def test_for_url(self, db_session):
        """
        GIVEN: A URL
        WHEN:  Finding an IntegrationClient for a given URL
        THEN:  One is either created or retrieved
        """
        now = utc_now()
        url = self.client.url
        client, is_new = IntegrationClient.for_url(db_session, url)

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
        client2, is_new = IntegrationClient.for_url(db_session, url)
        assert client == client2

    def test_register(self, db_session):
        """
        GIVEN: An IntegrationClient
        WHEN:  Creating a new server with client details
        THEN:  Correct details are set
        """
        now = utc_now()
        client, _ = IntegrationClient.register(db_session, self.client.url)

        # It creates a shared_secret.
        assert client.shared_secret
        # And sets a timestamp for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        assert True == isinstance(client.created, datetime.datetime)
        assert client.created == client.last_accessed

        # It raises an error if the url is already registered and the
        # submitted shared_secret is inaccurate.
        pytest.raises(ValueError, IntegrationClient.register, db_session, client.url)
        pytest.raises(ValueError, IntegrationClient.register, db_session, client.url, 'wrong')

    def test_authenticate(self, db_session):
        """
        GIVEN: An IntegrationClient
        WHEN:  Authenticating
        THEN:  Authentication is contingent based on the secret
        """

        result = IntegrationClient.authenticate(db_session, "secret")
        assert self.client == result

        result = IntegrationClient.authenticate(db_session, "wrong_secret")
        assert None == result

    def test_normalize_url(self, db_session):
        """
        GIVEN: A URL
        WHEN:  Normalizing a URL
        THEN:  URL is normalized
        """
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
