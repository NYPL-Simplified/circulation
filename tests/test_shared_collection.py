import pytest
import json
import flask
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

from api.circulation_exceptions import *
from api.shared_collection import (
    SharedCollectionAPI,
    BaseSharedCollectionAPI,
)
from core.config import CannotLoadConfiguration
from api.odl import ODLAPI
from core.model import (
    ConfigurationSetting,
    Hold,
    IntegrationClient,
    Loan,
    create,
    get_one,
)
from core.util.string_helpers import base64
from api.circulation import FulfillmentInfo

from core.testing import DatabaseTest
from core.testing import MockRequestsResponse

class MockAPI(BaseSharedCollectionAPI):
    def __init__(self, _db, collection):
        self.checkouts = []
        self.returns = []
        self.fulfills = []
        self.holds = []
        self.released_holds = []
        self.fulfillment = None

    def checkout_to_external_library(self, client, pool, hold=None):
        self.checkouts.append((client, pool))

    def checkin_from_external_library(self, client, loan):
        self.returns.append((client, loan))

    def fulfill_for_external_library(self, client, loan, mechanism):
        self.fulfills.append((client, loan, mechanism))
        return self.fulfillment

    def release_hold_from_external_library(self, client, hold):
        self.released_holds.append((client, hold))

class TestSharedCollectionAPI(DatabaseTest):

    def setup_method(self):
        super(TestSharedCollectionAPI, self).setup_method()
        self.collection = self._collection(protocol="Mock")
        self.shared_collection = SharedCollectionAPI(
            self._db, api_map = {
                "Mock" : MockAPI
            }
        )
        self.api = self.shared_collection.api(self.collection)
        ConfigurationSetting.for_externalintegration(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, self.collection.external_integration
        ).value = json.dumps(["http://library.org"])
        self.client, ignore = IntegrationClient.register(self._db, "http://library.org")
        edition, self.pool = self._edition(
            with_license_pool=True, collection=self.collection
        )
        [self.delivery_mechanism] = self.pool.delivery_mechanisms

    def test_initialization_exception(self):
        class MisconfiguredAPI(object):
            def __init__(self, _db, collection):
                raise CannotLoadConfiguration("doomed!")

        api_map = { self._default_collection.protocol: MisconfiguredAPI }
        shared_collection = SharedCollectionAPI(
            self._db, api_map=api_map
        )
        # Although the SharedCollectionAPI was created, it has no functioning
        # APIs.
        assert {} == shared_collection.api_for_collection

        # Instead, the CannotLoadConfiguration exception raised by the
        # constructor has been stored in initialization_exceptions.
        e = shared_collection.initialization_exceptions[self._default_collection.id]
        assert isinstance(e, CannotLoadConfiguration)
        assert "doomed!" == str(e)

    def test_api_for_licensepool(self):
        collection = self._collection(protocol=ODLAPI.NAME)
        edition, pool = self._edition(with_license_pool=True, collection=collection)
        shared_collection = SharedCollectionAPI(self._db)
        assert isinstance(shared_collection.api_for_licensepool(pool), ODLAPI)

    def test_api_for_collection(self):
        collection = self._collection()
        shared_collection = SharedCollectionAPI(self._db)
        # The collection isn't a shared collection, so looking up its API
        # raises an exception.
        pytest.raises(CirculationException, shared_collection.api, collection)

        collection.protocol = ODLAPI.NAME
        shared_collection = SharedCollectionAPI(self._db)
        assert isinstance(shared_collection.api(collection), ODLAPI)

    def test_register(self):
        # An auth document URL is required to register.
        pytest.raises(InvalidInputException, self.shared_collection.register,
                      self.collection, None)

        # If the url doesn't return a valid auth document, there's an exception.
        auth_response = "not json"
        def do_get(*args, **kwargs):
            return MockRequestsResponse(200, content=auth_response)
        pytest.raises(RemoteInitiatedServerError, self.shared_collection.register,
                      self.collection, "http://library.org/auth", do_get=do_get)

        # The auth document also must have a link to the library's catalog.
        auth_response = json.dumps({"links": []})
        pytest.raises(RemoteInitiatedServerError, self.shared_collection.register,
                      self.collection, "http://library.org/auth", do_get=do_get)

        # If no external library URLs are configured, no one can register.
        auth_response = json.dumps({"links": [{"href": "http://library.org", "rel": "start"}]})
        ConfigurationSetting.for_externalintegration(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, self.collection.external_integration
        ).value = None
        pytest.raises(AuthorizationFailedException, self.shared_collection.register,
                      self.collection, "http://library.org/auth", do_get=do_get)

        # If the library's URL isn't in the configuration, it can't register.
        auth_response = json.dumps({"links": [{"href": "http://differentlibrary.org", "rel": "start"}]})
        ConfigurationSetting.for_externalintegration(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, self.collection.external_integration
        ).value = json.dumps(["http://library.org"])
        pytest.raises(AuthorizationFailedException, self.shared_collection.register,
                      self.collection, "http://differentlibrary.org/auth", do_get=do_get)

        # Or if the public key is missing from the auth document.
        auth_response = json.dumps({"links": [{"href": "http://library.org", "rel": "start"}]})
        pytest.raises(RemoteInitiatedServerError, self.shared_collection.register,
                      self.collection, "http://library.org/auth", do_get=do_get)

        auth_response = json.dumps({"public_key": { "type": "not RSA", "value": "123" },
                                    "links": [{"href": "http://library.org", "rel": "start"}]})
        pytest.raises(RemoteInitiatedServerError, self.shared_collection.register,
                      self.collection, "http://library.org/auth", do_get=do_get)

        auth_response = json.dumps({"public_key": { "type": "RSA" },
                                    "links": [{"href": "http://library.org", "rel": "start"}]})
        pytest.raises(RemoteInitiatedServerError, self.shared_collection.register,
                      self.collection, "http://library.org/auth", do_get=do_get)


        # Here's an auth document with a valid key.
        key = RSA.generate(2048)
        public_key = key.publickey().exportKey()
        encryptor = PKCS1_OAEP.new(key)
        auth_response = json.dumps({"public_key": { "type": "RSA", "value": public_key },
                                    "links": [{"href": "http://library.org", "rel": "start"}]})
        response = self.shared_collection.register(self.collection, "http://library.org/auth", do_get=do_get)

        # An IntegrationClient has been created.
        client = get_one(self._db, IntegrationClient, url=IntegrationClient.normalize_url("http://library.org/"))
        decrypted_secret = encryptor.decrypt(base64.b64decode(response.get("metadata", {}).get("shared_secret")))
        assert client.shared_secret == decrypted_secret

    def test_borrow(self):
        # This client is registered, but isn't one of the allowed URLs for the collection
        # (maybe it was registered for a different shared collection).
        other_client, ignore = IntegrationClient.register(self._db, "http://other_library.org")

        # Trying to borrow raises an exception.
        pytest.raises(AuthorizationFailedException, self.shared_collection.borrow,
                      self.collection, other_client, self.pool)

        # A client that's registered with the collection can borrow.
        self.shared_collection.borrow(self.collection, self.client, self.pool)
        assert [(self.client, self.pool)] == self.api.checkouts

        # If the client's checking out an existing hold, the hold must be for that client.
        hold, ignore = create(self._db, Hold, integration_client=other_client, license_pool=self.pool)
        pytest.raises(CannotLoan, self.shared_collection.borrow,
                      self.collection, self.client, self.pool, hold=hold)

        hold.integration_client = self.client
        self.shared_collection.borrow(self.collection, self.client, self.pool, hold=hold)
        assert [(self.client, self.pool)] == self.api.checkouts[1:]

    def test_revoke_loan(self):
        other_client, ignore = IntegrationClient.register(self._db, "http://other_library.org")
        loan, ignore = create(self._db, Loan, integration_client=other_client, license_pool=self.pool)
        pytest.raises(NotCheckedOut, self.shared_collection.revoke_loan,
                      self.collection, self.client, loan)

        loan.integration_client = self.client
        self.shared_collection.revoke_loan(self.collection, self.client, loan)
        assert [(self.client, loan)] == self.api.returns

    def test_fulfill(self):
        other_client, ignore = IntegrationClient.register(self._db, "http://other_library.org")
        loan, ignore = create(self._db, Loan, integration_client=other_client, license_pool=self.pool)
        pytest.raises(CannotFulfill, self.shared_collection.fulfill,
                      self.collection, self.client, loan, self.delivery_mechanism)

        loan.integration_client = self.client

        # If the API does not return content or a content link, the loan can't be fulfilled.
        pytest.raises(CannotFulfill, self.shared_collection.fulfill,
                      self.collection, self.client, loan, self.delivery_mechanism)
        assert [(self.client, loan, self.delivery_mechanism)] == self.api.fulfills

        self.api.fulfillment = FulfillmentInfo(
            self.collection,
            self.pool.data_source.name,
            self.pool.identifier.type,
            self.pool.identifier.identifier,
            "http://content",
            "text/html",
            None,
            None,
        )
        fulfillment = self.shared_collection.fulfill(self.collection, self.client, loan, self.delivery_mechanism)
        assert [(self.client, loan, self.delivery_mechanism)] == self.api.fulfills[1:]
        assert self.delivery_mechanism == loan.fulfillment

    def test_revoke_hold(self):
        other_client, ignore = IntegrationClient.register(self._db, "http://other_library.org")
        hold, ignore = create(self._db, Hold, integration_client=other_client, license_pool=self.pool)

        pytest.raises(CannotReleaseHold, self.shared_collection.revoke_hold,
                      self.collection, self.client, hold)

        hold.integration_client = self.client
        self.shared_collection.revoke_hold(self.collection, self.client, hold)
        assert [(self.client, hold)] == self.api.released_holds
