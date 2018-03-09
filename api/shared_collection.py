from nose.tools import set_trace
import logging
import flask
import base64
import json
from flask_babel import lazy_gettext as _

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

from core.model import (
    Collection,
    ConfigurationSetting,
    IntegrationClient,
    Library,
    get_one,
)
from circulation_exceptions import *
from core.config import CannotLoadConfiguration
from core.util.http import HTTP

class SharedCollectionAPI(object):
    """Logic for circulating books to patrons of libraries on other 
    circulation managers. This can be used for something like ODL where the 
    circulation manager is responsible for managing loans and holds rather
    than the distributor, or potentially for inter-library loans for other
    collection types.
    """

    def __init__(self, _db, library, api_map=None):
        """Constructor.

        :param _db: A database session (probably a scoped session, which is
            why we can't derive it from `library`).

        :param library: A Library object representing the library
          whose circulation we're concerned with.

        :param api_map: A dictionary mapping Collection protocols to
           API classes that should be instantiated to deal with these
           protocols. The default map will work fine unless you're a
           unit test.

           Since instantiating these API classes may result in API
           calls, we only instantiate one CirculationAPI per library,
           and keep them around as long as possible.
        """
        # TODO: Should there be an analytics events for external libraries?
        self._db = _db
        self.library_id = library.id
        api_map = api_map or self.default_api_map

        self.api_for_collection = {}
        self.initialization_exceptions = {}

        self.log = logging.getLogger("Shared Collection API")
        for collection in library.collections:
            if collection.protocol in api_map:
                api = None
                try:
                    api = api_map[collection.protocol](_db, collection)
                except CannotLoadConfiguration, e:
                    self.log.error(
                        "Error loading configuration for %s: %s",
                        collection.name, e.message
                    )
                    self.initialization_exceptions[collection.id] = e
                if api:
                    self.api_for_collection[collection.id] = api

    @property
    def library(self):
        return Library.by_id(self._db, self.library_id)

    @property
    def default_api_map(self):
        """When you see a Collection that implements protocol X, instantiate
        API class Y to handle that collection.
        """
        from odl import ODLWithConsolidatedCopiesAPI
        return {
            ODLWithConsolidatedCopiesAPI.NAME: ODLWithConsolidatedCopiesAPI,
        }

    def api_for_licensepool(self, pool):
        """Find the API to use for the given license pool."""
        return self.api_for_collection.get(pool.collection.id)

    def api(self, collection):
        """Find the API to use for the given collection, and raise an exception
        if there isn't one."""
        api = self.api_for_collection.get(collection.id)
        if not api:
            raise CirculationException(
                _("Collection %(collection)s is not a shared collection for library %(library)s",
                  collection=collection.name, library=self.library.name))
        return api

    def register(self, collection, auth_document_url, do_get=HTTP.get_with_timeout):
        """Register a library on an external circulation manager for access to this
        collection. The library's auth document url must be whitelisted in the
        collection's settings."""
        if not auth_document_url:
            raise InvalidInputException(
                _("An authentication document URL is required to register a library."))

        external_library_urls = ConfigurationSetting.for_externalintegration(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, collection.external_integration
        ).json_value

        if not external_library_urls or auth_document_url not in external_library_urls:
            raise AuthorizationFailedException(
                _("Your library's URL is not one of the allowed URLs for this collection. Ask the collection administrator to add %(auth_document_url)s to the list of allowed URLs.",
                  auth_document_url=auth_document_url))

        auth_response = do_get(auth_document_url, allowed_response_codes=["2xx", "3xx"])
        try:
            auth_document = json.loads(auth_response.content)
        except ValueError, e:
            raise RemoteInitiatedServerError(
                _("Authentication document at %(auth_document_url)s was not valid JSON.",
                  auth_document_url=auth_document_url),
                _("Remote authentication document"))
        public_key = auth_document.get("public_key")
        if not public_key or not public_key.get("type") == "RSA" or not public_key.get("value"):
            raise RemoteInitiatedServerError(
                _("Authentication document at %(auth_document_url)s did not contain an RSA public key.",
                  auth_document_url=auth_document_url),
                _("Remote authentication document"))

        public_key = public_key.get("value")
        public_key = RSA.importKey(public_key)
        encryptor = PKCS1_OAEP.new(public_key)

        normalized_url = IntegrationClient.normalize_url(auth_document_url)
        client = get_one(self._db, IntegrationClient, url=normalized_url)
        if not client:
            client, ignore = IntegrationClient.register(self._db, auth_document_url)

        shared_secret = client.shared_secret
        encrypted_secret = encryptor.encrypt(str(shared_secret))
        return dict(metadata=dict(shared_secret=base64.b64encode(encrypted_secret)))

    def check_client_authorization(self, collection, client):
        """Verify that an IntegrationClient is whitelisted for access to the collection."""
        external_library_urls = ConfigurationSetting.for_externalintegration(
            BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, collection.external_integration
        ).json_value
        if client.url not in [IntegrationClient.normalize_url(url) for url in external_library_urls]:
            raise AuthorizationFailedException()

    def borrow(self, collection, client, pool, hold=None):
        api = self.api(collection)
        self.check_client_authorization(collection, client)
        if hold and hold.integration_client != client:
            raise CannotLoan(_("This hold belongs to a different library."))
        return api.checkout_to_external_library(client, pool, hold=hold)

    def revoke_loan(self, collection, client, loan):
        api = self.api(collection)
        self.check_client_authorization(collection, client)
        if loan.integration_client != client:
            raise NotCheckedOut(_("This loan belongs to a different library."))
        return api.checkin_from_external_library(client, loan)

    def fulfill(self, collection, client, loan, mechanism):
        api = self.api(collection)
        self.check_client_authorization(collection, client)

        if loan.integration_client != client:
            raise CannotFulfill(_("This loan belongs to a different library."))

        fulfillment = api.fulfill_for_external_library(client, loan, mechanism)
        if not fulfillment or not (fulfillment.content_link or fulfillment.content):
            raise CannotFulfill()
        
        if loan.fulfillment is None and not mechanism.delivery_mechanism.is_streaming:
            __transaction = self._db.begin_nested()
            loan.fulfillment = mechanism
            __transaction.commit()
        return fulfillment

    def place_hold(self, collection, client, pool):
        api = self.api(collection)
        self.check_client_authorization(collection, client)
        return api.place_hold_for_external_library(client, pool)

    def revoke_hold(self, collection, client, hold):
        api = self.api(collection)
        self.check_client_authorization(collection, client)
        if hold and hold.integration_client != client:
            raise CannotReleaseHold(_("This hold belongs to a different library."))
        return api.release_hold_from_external_library(client, hold)


class BaseSharedCollectionAPI(object):
    """APIs that permit external circulation managers to access their collections
    should extend this class."""

    EXTERNAL_LIBRARY_URLS = "external_library_urls"

    SETTINGS = [
        {
            "key": EXTERNAL_LIBRARY_URLS,
            "label": _("URLs for libraries on other circulation managers that use this collection"),
            "type": "list",
            "optional": True,
        },
        {
            "key": Collection.EBOOK_LOAN_DURATION_KEY,
            "label": _("Ebook Loan Duration for libraries on other circulation managers (in Days)"),
            "default": Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            "description": _("When a patron uses SimplyE to borrow an ebook from this collection, SimplyE will ask for a loan that lasts this number of days. This must be equal to or less than the maximum loan duration negotiated with the distributor."),
            "optional": True,
        }
    ]

    def checkout_to_external_library(self, client, pool, hold=None):
        raise NotImplementedError()

    def checkin_from_external_library(self, client, loan):
        raise NotImplementedError()

    def fulfill_for_external_library(self, client, loan, mechanism):
        raise NotImplementedError()
        
    def place_hold_for_external_library(self, client, pool):
        raise NotImplementedError()

    def release_hold_from_external_library(self, client, hold):
        raise NotImplementedError()
