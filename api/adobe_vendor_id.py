import argparse
from nose.tools import set_trace
import json
import logging
import uuid
import os
import datetime
import jwt
from jwt.algorithms import HMACAlgorithm
import sys

import flask
from flask import Response
from flask_babel import lazy_gettext as _
from .config import (
    CannotLoadConfiguration,
    Configuration,
)
from api.base_controller import BaseCirculationManagerController
from .problem_details import *
from sqlalchemy.orm.session import Session
from core.util.xmlparser import XMLParser
from core.util.problem_detail import ProblemDetail
from core.app_server import url_for
from core.model import (
    create,
    get_one,
    ConfigurationSetting,
    Credential,
    DataSource,
    DelegatedPatronIdentifier,
    ExternalIntegration,
    Library,
    Patron,
)
from core.scripts import Script
from core.util.string_helpers import base64

class AdobeVendorIDController(object):

    """Flask controllers that implement the Account Service and
    Authorization Service portions of the Adobe Vendor ID protocol.
    """
    def __init__(self, _db, library, vendor_id, node_value, authenticator):
        self._db = _db
        self.library = library
        self.request_handler = AdobeVendorIDRequestHandler(vendor_id)
        self.model = AdobeVendorIDModel(_db, library, authenticator, node_value)

    def create_authdata_handler(self, patron):
        """Create an authdata token for the given patron.

        This controller method exists only for backwards compatibility
        with older client applications. Newer applications are
        expected to understand the DRM Extensions for OPDS.
        """
        __transaction = self._db.begin_nested()
        credential = self.model.create_authdata(patron)
        __transaction.commit()
        return Response(credential.credential, 200, {"Content-Type": "text/plain"})

    def signin_handler(self):
        """Process an incoming signInRequest document."""
        __transaction = self._db.begin_nested()
        output = self.request_handler.handle_signin_request(
            flask.request.data, self.model.standard_lookup,
            self.model.authdata_lookup)
        __transaction.commit()
        return Response(output, 200, {"Content-Type": "application/xml"})

    def userinfo_handler(self):
        """Process an incoming userInfoRequest document."""
        output = self.request_handler.handle_accountinfo_request(
            flask.request.data, self.model.urn_to_label)
        return Response(output, 200, {"Content-Type": "application/xml"})

    def status_handler(self):
        return Response("UP", 200, {"Content-Type": "text/plain"})


class DeviceManagementProtocolController(BaseCirculationManagerController):
    """Implementation of the DRM Device ID Management Protocol.

    The code that does the actual work is in DeviceManagementRequestHandler.
    """
    DEVICE_ID_LIST_MEDIA_TYPE = "vnd.librarysimplified/drm-device-id-list"
    PLAIN_TEXT_HEADERS = {"Content-Type" : "text/plain"}

    @property
    def link_template_header(self):
        """Generate the Link Template that explains how to deregister
        a specific DRM device ID.
        """
        library = flask.request.library
        url = url_for("adobe_drm_device", library_short_name=library.short_name, device_id="{id}", _external=True)
        # The curly brackets in {id} were escaped. Un-escape them to
        # get a Link Template.
        url = url.replace("%7Bid%7D", "{id}")
        return {"Link-Template": '<%s>; rel="item"' % url}

    def _request_handler(self, patron):
        """Create a DeviceManagementRequestHandler for the appropriate
        Credential of the given Patron.

        :return: A DeviceManagementRequestHandler
        """
        if not patron:
            return INVALID_CREDENTIALS.detailed(_("No authenticated patron"))

        credential = AdobeVendorIDModel.get_or_create_patron_identifier_credential(
            patron
        )
        return DeviceManagementRequestHandler(credential)

    def device_id_list_handler(self):
        """Manage the list of device IDs associated with an Adobe ID."""
        handler = self._request_handler(flask.request.patron)
        if isinstance(handler, ProblemDetail):
            return handler

        device_ids = self.DEVICE_ID_LIST_MEDIA_TYPE
        if flask.request.method=='GET':
            # Serve a list of device IDs.
            output = handler.device_list()
            if isinstance(output, ProblemDetail):
                return output
            headers = self.link_template_header
            headers['Content-Type'] = device_ids
            return Response(output, 200, headers)
        elif flask.request.method=='POST':
            # Add a device ID to the list.
            incoming_media_type = flask.request.headers.get('Content-Type')
            if incoming_media_type != device_ids:
                return UNSUPPORTED_MEDIA_TYPE.detailed(
                    _("Expected %(media_type)s document.",
                      media_type=device_ids)
                )
            output = handler.register_device(flask.request.data)
            if isinstance(output, ProblemDetail):
                return output
            return Response(output, 200, self.PLAIN_TEXT_HEADERS)
        return METHOD_NOT_ALLOWED.detailed(
            _("Only GET and POST are supported.")
        )

    def device_id_handler(self, device_id):
        """Manage one of the device IDs associated with an Adobe ID."""
        handler = self._request_handler(getattr(flask.request, 'patron', None))
        if isinstance(handler, ProblemDetail):
            return handler

        if flask.request.method != 'DELETE':
            return METHOD_NOT_ALLOWED.detailed(_("Only DELETE is supported."))

        # Delete the specified device ID.
        output = handler.deregister_device(device_id)
        if isinstance(output, ProblemDetail):
            return output
        return Response(output, 200, self.PLAIN_TEXT_HEADERS)


class AdobeVendorIDRequestHandler(object):

    """Standalone class that can be tested without bringing in Flask or
    the database schema.
    """

    SIGN_IN_RESPONSE_TEMPLATE = """<signInResponse xmlns="http://ns.adobe.com/adept">
<user>%(user)s</user>
<label>%(label)s</label>
</signInResponse>"""

    ACCOUNT_INFO_RESPONSE_TEMPLATE = """<accountInfoResponse xmlns="http://ns.adobe.com/adept">
<label>%(label)s</label>
</accountInfoResponse>"""

    AUTH_ERROR_TYPE = "AUTH"
    ACCOUNT_INFO_ERROR_TYPE = "ACCOUNT_INFO"

    ERROR_RESPONSE_TEMPLATE = '<error xmlns="http://ns.adobe.com/adept" data="E_%(vendor_id)s_%(type)s %(message)s"/>'

    TOKEN_FAILURE = 'Incorrect token.'
    AUTHENTICATION_FAILURE = 'Incorrect barcode or PIN.'
    URN_LOOKUP_FAILURE = "Could not identify patron from '%s'."

    def __init__(self, vendor_id):
        self.vendor_id = vendor_id

    def handle_signin_request(self, data, standard_lookup, authdata_lookup):
        parser = AdobeSignInRequestParser()
        try:
            data = parser.process(data)
        except Exception as e:
            logging.error("Error processing %s", data, exc_info=e)
            return self.error_document(self.AUTH_ERROR_TYPE, str(e))
        user_id = label = None
        if not data:
            return self.error_document(
                self.AUTH_ERROR_TYPE, "Request document in wrong format.")
        if not 'method' in data:
            return self.error_document(
                self.AUTH_ERROR_TYPE, "No method specified")
        if data['method'] == parser.STANDARD:
            user_id, label = standard_lookup(data)
            failure = self.AUTHENTICATION_FAILURE
        elif data['method'] == parser.AUTH_DATA:
            authdata = data[parser.AUTH_DATA]
            user_id, label = authdata_lookup(authdata)
            failure = self.TOKEN_FAILURE
        if user_id is None:
            return self.error_document(self.AUTH_ERROR_TYPE, failure)
        else:
            return self.SIGN_IN_RESPONSE_TEMPLATE % dict(
                user=user_id, label=label)

    def handle_accountinfo_request(self, data, urn_to_label):
        parser = AdobeAccountInfoRequestParser()
        label = None
        try:
            data = parser.process(data)
            if not data:
                return self.error_document(
                    self.ACCOUNT_INFO_ERROR_TYPE,
                    "Request document in wrong format.")
            if not 'user' in data:
                return self.error_document(
                    self.ACCOUNT_INFO_ERROR_TYPE,
                    "Could not find user identifer in request document.")
            label = urn_to_label(data['user'])
        except Exception as e:
            return self.error_document(
                self.ACCOUNT_INFO_ERROR_TYPE, str(e))

        if label:
            return self.ACCOUNT_INFO_RESPONSE_TEMPLATE % dict(label=label)
        else:
            return self.error_document(
                self.ACCOUNT_INFO_ERROR_TYPE,
                self.URN_LOOKUP_FAILURE % data['user']
            )

    def error_document(self, type, message):
        return self.ERROR_RESPONSE_TEMPLATE % dict(
            vendor_id=self.vendor_id, type=type, message=message)


class DeviceManagementRequestHandler(object):
    """Handle incoming requests for the DRM Device Management Protocol."""

    def __init__(self, credential):
        self.credential = credential

    def device_list(self):
        return "\n".join(
            sorted(
                x.device_identifier
                for x in self.credential.drm_device_identifiers
            )
        )

    def register_device(self, data):
        device_ids = data.split("\n")
        if len(device_ids) > 1:
            return PAYLOAD_TOO_LARGE.detailed(
                _("You may only register one device ID at a time.")
            )
        for device_id in device_ids:
            if device_id:
                self.credential.register_drm_device_identifier(device_id)
        return 'Success'

    def deregister_device(self, device_id):
        self.credential.deregister_drm_device_identifier(device_id)
        return 'Success'


class AdobeRequestParser(XMLParser):

    NAMESPACES = { "adept" : "http://ns.adobe.com/adept" }

    def process(self, data):
        requests = list(self.process_all(
            data, self.REQUEST_XPATH, self.NAMESPACES))
        if not requests:
            return None
        # There should only be one request tag, but if there's more than
        # one, only return the first one.
        return requests[0]

    def _add(self, d, tag, key, namespaces, transform=None):
        v = self._xpath1(tag, 'adept:' + key, namespaces)
        if v is not None:
            v = v.text
            if v is not None:
                v = v.strip()
                if transform is not None:
                    v = transform(v)
        d[key] = v

class AdobeSignInRequestParser(AdobeRequestParser):

    REQUEST_XPATH = "/adept:signInRequest"

    STANDARD = 'standard'
    AUTH_DATA = 'authData'

    def process_one(self, tag, namespaces):
        method = tag.attrib.get('method')

        if not method:
            raise ValueError("No signin method specified")
        data = dict(method=method)
        if method == self.STANDARD:
            self._add(data, tag, 'username', namespaces)
            self._add(data, tag, 'password', namespaces)
        elif method == self.AUTH_DATA:
            self._add(data, tag, self.AUTH_DATA, namespaces, base64.b64decode)
        else:
            raise ValueError("Unknown signin method: %s" % method)
        return data

class AdobeAccountInfoRequestParser(AdobeRequestParser):

    REQUEST_XPATH = "/adept:accountInfoRequest"

    def process_one(self, tag, namespaces):
        method = tag.attrib.get('method')
        data = dict(method=method)
        self._add(data, tag, 'user', namespaces)
        return data


class AdobeVendorIDModel(object):

    """Implement Adobe Vendor ID within the Simplified database
    model.
    """

    AUTHDATA_TOKEN_TYPE = "Authdata for Adobe Vendor ID"
    VENDOR_ID_UUID_TOKEN_TYPE = "Vendor ID UUID"

    def __init__(self, _db, library, authenticator, node_value,
                 temporary_token_duration=None):
        self.library = library
        self._db = _db
        self.authenticator = authenticator
        self.temporary_token_duration = (
            temporary_token_duration or datetime.timedelta(minutes=10))
        if isinstance(node_value, str):
            node_value = int(node_value, 16)
        self.node_value = node_value

    @property
    def data_source(self):
        return DataSource.lookup(self._db, DataSource.ADOBE)

    def uuid_and_label(self, patron):
        """Create or retrieve a Vendor ID UUID and human-readable Vendor ID
        label for the given patron.

        This code is semi-deprecated, which accounts for the varying
        paths and the code that tries to migrate patrons to the new
        system. In the future everyone will send JWTs as authdata and
        we will always go from the JWT to a DelegatedPatronIdentifier.
        This code always ends up at a DelegatedPatronIdentifier, but
        it might pick up the final value from somewhere else along the way.

        The _reason_ this code is semi-deprecated is that it only
        works for a library that has its own Adobe Vendor ID.
        """
        if not patron:
            return None, None

        # First, find or create a Credential containing the patron's
        # anonymized key into the DelegatedPatronIdentifier database.
        adobe_account_id_patron_identifier_credential = self.get_or_create_patron_identifier_credential(
            patron
        )

        # Look up a Credential containing the patron's Adobe account
        # ID created under the old system. We don't use
        # Credential.lookup because we don't want to create a
        # Credential if it doesn't exist.
        old_style_adobe_account_id_credential = get_one(
            self._db, Credential, patron=patron, data_source=self.data_source,
            type=self.VENDOR_ID_UUID_TOKEN_TYPE
        )

        if old_style_adobe_account_id_credential:
            # The value of the old-style credential will become the
            # default value of the DelegatedPatronIdentifier, assuming
            # we have to create one.
            def new_value():
                return old_style_adobe_account_id_credential.credential
        else:
            # There is no old-style credential. If we have to create a
            # new DelegatedPatronIdentifier we will give it a value
            # using the default mechanism.
            new_value = None

        # Look up or create a DelegatedPatronIdentifier using the
        # anonymized patron identifier we just looked up or created.
        utility = AuthdataUtility.from_config(patron.library, self._db)
        return self.to_delegated_patron_identifier_uuid(
            utility.library_uri, adobe_account_id_patron_identifier_credential.credential,
            value_generator=new_value
        )

    def create_authdata(self, patron):
        credential, is_new = Credential.persistent_token_create(
            self._db, self.data_source, self.AUTHDATA_TOKEN_TYPE,
            patron
        )
        return credential

    def standard_lookup(self, authorization_data):
        """Look up a patron by authorization header. Return their Vendor ID
        UUID and their human-readable label, creating a Credential
        object to hold the UUID if necessary.
        """
        username = authorization_data.get('username')
        password = authorization_data.get('password')
        if username and not password:
            # The absence of a password indicates the username might
            # be a persistent authdata token smuggled to get around a
            # broken Adobe client-side API. Try treating the
            # 'username' as a token.
            possible_authdata_token = authorization_data['username']
            return self.authdata_lookup(possible_authdata_token)

        if username and password:
            # Try to look up the username and password as a short
            # client token. This is currently the best way to do
            # authentication.
            uuid, label = self.short_client_token_lookup(username, password)
            if uuid and label:
                return uuid, label

        # Last ditch effort: try a normal username/password lookup.
        # This should almost never be used.
        patron = self.authenticator.authenticated_patron(
            self._db, authorization_data
        )
        return self.uuid_and_label(patron)

    def authdata_lookup(self, authdata):
        """Turn an authdata string into a Vendor ID UUID and a human-readable
        label.

        Generally we do this by decoding the authdata as a JWT and
        looking up or creating an appropriate
        DelegatedPatronIdentifier.

        However, for backwards compatibility purposes, if the authdata
        cannot be decoded as a JWT, we will try the old method of
        treating it as a Credential that identifies a Patron, and
        finding the DelegatedPatronIdentifier that way.
        """
        if not authdata:
            return None, None

        library_uri = foreign_patron_identifier = None
        utility = AuthdataUtility.from_config(self.library, self._db)
        if utility:
            # Hopefully this is an authdata JWT generated by another
            # library's circulation manager.
            try:
                library_uri, foreign_patron_identifier = utility.decode(
                    authdata
                )
            except Exception as e:
                # Not a problem -- we'll try the old system.
                pass

        if library_uri and foreign_patron_identifier:
            # We successfully decoded the authdata as a JWT. We know
            # which library the patron is from and which (hopefully
            # anonymized) ID identifies this patron within that
            # library. Keep their Adobe account ID in a
            # DelegatedPatronIdentifier.
            uuid_and_label = self.to_delegated_patron_identifier_uuid(
                library_uri, foreign_patron_identifier
            )
        else:
            # Maybe this is an old-style authdata, stored as a
            # Credential associated with a specific patron.
            patron = self.patron_from_authdata_lookup(authdata)
            if patron:
                # Yes, that's what's going on.
                uuid_and_label = self.uuid_and_label(patron)
            else:
                # This alleged authdata doesn't fit into either
                # category. Stop trying to turn it into an Adobe account ID.
                uuid_and_label = (None, None)
        return uuid_and_label

    def short_client_token_lookup(self, token, signature):
        """Validate a short client token that came in as username/password."""
        utility = AuthdataUtility.from_config(self.library, self._db)
        library_uri = foreign_patron_identifier = None
        if utility:
            # Hopefully this is a short client token generated by
            # another library's circulation manager.
            try:
                library_uri, foreign_patron_identifier = utility.decode_two_part_short_client_token(token, signature)
            except Exception as e:
                # This didn't work--either the incoming data was wrong
                # or this technique wasn't the right one to use.
                pass

        if library_uri and foreign_patron_identifier:
            # We successfully decoded the authdata as a short client
            # token. We know which library the patron is from and
            # which (hopefully anonymized) ID identifies this patron
            # within that library. Keep their Adobe account ID in a
            # DelegatedPatronIdentifier.
            uuid_and_label = self.to_delegated_patron_identifier_uuid(
                library_uri, foreign_patron_identifier
            )
        else:
            # We were not able to decode the authdata as a short client
            # token.
            uuid_and_label = (None, None)
        return uuid_and_label

    def to_delegated_patron_identifier_uuid(
            self, library_uri, foreign_patron_identifier, value_generator=None
    ):
        """Create or lookup a DelegatedPatronIdentifier containing an Adobe
        account ID for the given library and foreign patron ID.

        :return: A 2-tuple (UUID, label)
        """
        if not library_uri or not foreign_patron_identifier:
            return None, None
        value_generator = value_generator or self.uuid
        identifier, is_new = DelegatedPatronIdentifier.get_one_or_create(
            self._db, library_uri, foreign_patron_identifier,
            DelegatedPatronIdentifier.ADOBE_ACCOUNT_ID, value_generator
        )

        if identifier is None:
            return None, None
        return (identifier.delegated_identifier,
                self.urn_to_label(identifier.delegated_identifier))

    def patron_from_authdata_lookup(self, authdata):
        """Look up a patron by their persistent authdata token."""
        credential = Credential.lookup_by_token(
            self._db, self.data_source, self.AUTHDATA_TOKEN_TYPE,
            authdata, allow_persistent_token=True
        )
        if not credential:
            return None
        return credential.patron

    def urn_to_label(self, urn):
        """We have no information about patrons, so labels are sparse."""
        return "Delegated account ID %s" % urn

    def uuid(self):
        """Create a new UUID URN compatible with the Vendor ID system."""
        u = str(uuid.uuid1(self.node_value))
        # This chop is required by the spec. I have no idea why, but
        # since the first part of the UUID is the least significant,
        # it doesn't do much damage.
        value = "urn:uuid:0" + u[1:]
        return value

    @classmethod
    def get_or_create_patron_identifier_credential(cls, patron):
        _db = Session.object_session(patron)
        def refresh(credential):
            credential.credential = str(uuid.uuid1())
        data_source = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)
        patron_identifier_credential = Credential.lookup(
            _db, data_source,
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            patron, refresher_method=refresh, allow_persistent_token=True
        )
        return patron_identifier_credential


class AuthdataUtility(object):

    """Generate authdata JWTs as per the Vendor ID Service spec:
    https://docs.google.com/document/d/1j8nWPVmy95pJ_iU4UTC-QgHK2QhDUSdQ0OQTFR2NE_0

    Capable of encoding JWTs (for this library), and decoding them
    (from this library and potentially others).

    Also generates and decodes JWT-like strings used to get around
    Adobe's lack of support for authdata in deactivation.
    """

    # The type of the Credential created to identify a patron to the
    # Vendor ID Service. Using this as an alias keeps the Vendor ID
    # Service from knowing anything about the patron's true
    # identity. This Credential is permanent (unlike a patron's
    # username or authorization identifier), but can be revoked (if
    # the patron needs to reset their Adobe account ID) with no
    # consequences other than losing their currently checked-in books.
    ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER = "Identifier for Adobe account ID purposes"

    ALGORITHM = 'HS256'

    def __init__(self, vendor_id, library_uri, library_short_name, secret,
                 other_libraries={}):
        """Basic constructor.

        :param vendor_id: The Adobe Vendor ID that should accompany authdata
        generated by this utility.

        If this library has its own Adobe Vendor ID, it should go
        here. If this library is delegating authdata control to some
        other library, that library's Vendor ID should go here.

        :param library_uri: A URI identifying this library. This is
        used when generating JWTs.

        :param short_name: A short string identifying this
        library. This is used when generating short client tokens,
        which must be as short as possible (thus the name).

        :param secret: A secret used to sign this library's authdata.

        :param other_libraries: A dictionary mapping other libraries'
        canonical URIs to their (short name, secret) 2-tuples. An
        instance of this class will be able to decode an authdata from
        any library in this dictionary (plus the library it was
        initialized for).
        """
        self.vendor_id = vendor_id

        # This is used to _encode_ JWTs and send them to the
        # delegation authority.
        self.library_uri = library_uri

        # This is used to _encode_ short client tokens.
        self.short_name = library_short_name.upper()

        # This is used to encode both JWTs and short client tokens.
        self.secret = secret

        # This is used by the delegation authority to _decode_ JWTs.
        self.secrets_by_library_uri = {}
        self.secrets_by_library_uri[self.library_uri] = secret

        # This is used by the delegation authority to _decode_ short
        # client tokens.
        self.library_uris_by_short_name = {}
        self.library_uris_by_short_name[self.short_name] = self.library_uri

        # Fill in secrets_by_library_uri and library_uris_by_short_name
        # for other libraries.
        for uri, v in list(other_libraries.items()):
            short_name, secret = v
            short_name = short_name.upper()
            if short_name in self.library_uris_by_short_name:
                # This can happen if the same library is in the list
                # twice, capitalized differently.
                raise ValueError(
                    "Duplicate short name: %s" % short_name
                )
            self.library_uris_by_short_name[short_name] = uri
            self.secrets_by_library_uri[uri] = secret

        self.log = logging.getLogger("Adobe authdata utility")

        self.short_token_signer = HMACAlgorithm(HMACAlgorithm.SHA256)
        self.short_token_signing_key = self.short_token_signer.prepare_key(
            self.secret
        )

    VENDOR_ID_KEY = 'vendor_id'
    OTHER_LIBRARIES_KEY = 'other_libraries'

    @classmethod
    def from_config(cls, library, _db=None):
        """Initialize an AuthdataUtility from site configuration.

        :return: An AuthdataUtility if one is configured; otherwise None.

        :raise CannotLoadConfiguration: If an AuthdataUtility is
            incompletely configured.
        """
        _db = _db or Session.object_session(library)
        if not _db:
            raise ValueError(
                "No database connection provided and could not derive one from Library object!"
            )
        # Use a version of the library
        library = _db.merge(library, load=False)

        # Try to find an external integration with a configured Vendor ID.
        integrations = _db.query(
            ExternalIntegration
        ).outerjoin(
            ExternalIntegration.libraries
        ).filter(
            ExternalIntegration.protocol==ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.goal==ExternalIntegration.DISCOVERY_GOAL,
            Library.id==library.id
        )

        integration = None
        for possible_integration in integrations:
            vendor_id = ConfigurationSetting.for_externalintegration(
                cls.VENDOR_ID_KEY, possible_integration).value
            if vendor_id:
                integration = possible_integration
                break

        library_uri = ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library).value

        if not integration:
            return None

        vendor_id = integration.setting(cls.VENDOR_ID_KEY).value
        library_short_name = ConfigurationSetting.for_library_and_externalintegration(
            _db, ExternalIntegration.USERNAME, library, integration
        ).value
        secret = ConfigurationSetting.for_library_and_externalintegration(
            _db, ExternalIntegration.PASSWORD, library, integration
        ).value

        other_libraries = None
        adobe_integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.ADOBE_VENDOR_ID,
            ExternalIntegration.DRM_GOAL, library=library
        )
        if adobe_integration:
            other_libraries = adobe_integration.setting(cls.OTHER_LIBRARIES_KEY).json_value
        other_libraries = other_libraries or dict()

        if (not vendor_id or not library_uri
            or not library_short_name or not secret
        ):
            raise CannotLoadConfiguration(
                "Short Client Token configuration is incomplete. "
                "vendor_id (%s), username (%s), password (%s) and "
                "Library website_url (%s) must all be defined." % (
                    vendor_id, library_uri, library_short_name, secret
                )
            )
        if '|' in library_short_name:
            raise CannotLoadConfiguration(
                "Library short name cannot contain the pipe character."
            )
        return cls(vendor_id, library_uri, library_short_name, secret,
                   other_libraries)

    @classmethod
    def adobe_relevant_credentials(self, patron):
        """Find all Adobe-relevant Credential objects for the given
        patron.

        This includes the patron's identifier for Adobe ID purposes,
        and (less likely) any Adobe IDs directly associated with the
        Patron.

        :return: A SQLAlchemy query
        """
        _db = Session.object_session(patron)
        types = (AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE,
                 AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER)
        return _db.query(
            Credential).filter(Credential.patron==patron).filter(
                Credential.type.in_(types)
            )

    def encode(self, patron_identifier):
        """Generate an authdata JWT suitable for putting in an OPDS feed, where
        it can be picked up by a client and sent to the delegation
        authority to look up an Adobe ID.

        :return: A 2-tuple (vendor ID, authdata)
        """
        if not patron_identifier:
            raise ValueError("No patron identifier specified")
        now = datetime.datetime.utcnow()
        expires = now + datetime.timedelta(minutes=60)
        authdata = self._encode(
            self.library_uri, patron_identifier, now, expires
        )
        return self.vendor_id, authdata

    def _encode(self, iss=None, sub=None, iat=None, exp=None):
        """Helper method split out separately for use in tests."""
        payload = dict(iss=iss)                    # Issuer
        if sub:
            payload['sub'] = sub                   # Subject
        if iat:
            payload['iat'] = self.numericdate(iat) # Issued At
        if exp:
            payload['exp'] = self.numericdate(exp) # Expiration Time
        return base64.encodestring(
            jwt.encode(payload, self.secret, algorithm=self.ALGORITHM)
        )

    @classmethod
    def adobe_base64_encode(cls, str):
        """A modified base64 encoding that avoids triggering an Adobe bug.

        The bug seems to happen when the 'password' portion of a
        username/password pair contains a + character. So we replace +
        with :. We also replace / (another "suspicious" character)
        with ;. and strip newlines.
        """
        encoded = base64.encodestring(str)
        return encoded.replace("+", ":").replace("/", ";").replace("=", "@").strip()

    @classmethod
    def adobe_base64_decode(cls, str):
        """Undoes adobe_base64_encode."""
        encoded = str.replace(":", "+").replace(";", "/").replace("@", "=")
        return base64.decodestring(encoded)

    def decode(self, authdata):
        """Decode and verify an authdata JWT from one of the libraries managed
        by `secrets_by_library`.

        :return: a 2-tuple (library_uri, patron_identifier)

        :raise jwt.exceptions.DecodeError: When the JWT is not valid
            for any reason.
        """

        self.log.info("Authdata.decode() received authdata %s", authdata)
        # We are going to try to verify the authdata as is (in case
        # Adobe secretly decoded it en route), but we're also going to
        # try to decode it ourselves and verify it that way.
        potential_tokens = [authdata]
        try:
            decoded = base64.decodestring(authdata)
            potential_tokens.append(decoded)
        except Exception as e:
            # Do nothing -- the authdata was not encoded to begin with.
            pass

        exceptions = []
        library_uri = subject = None
        for authdata in potential_tokens:
            try:
                return self._decode(authdata)
            except Exception as e:
                self.log.error("Error decoding %s", authdata, exc_info=e)
                exceptions.append(e)

        # If we got to this point there is at least one exception
        # in the list.
        raise exceptions[-1]

    def _decode(self, authdata):
        # First, decode the authdata without checking the signature.
        decoded = jwt.decode(
            authdata, algorithm=self.ALGORITHM,
            options=dict(verify_signature=False)
        )

        # This lets us get the library URI, which lets us get the secret.
        library_uri = decoded.get('iss')
        if not library_uri in self.secrets_by_library_uri:
            # The request came in without a library specified
            # or with an unknown library specified.
            raise jwt.exceptions.DecodeError(
                "Unknown library: %s" % library_uri
            )

        # We know the secret for this library, so we can re-decode the
        # secret and require signature valudation this time.
        secret = self.secrets_by_library_uri[library_uri]
        decoded = jwt.decode(authdata, secret, algorithm=self.ALGORITHM)
        if not 'sub' in decoded:
            raise jwt.exceptions.DecodeError("No subject specified.")
        return library_uri, decoded['sub']

    @classmethod
    def _adobe_patron_identifier(self, patron):
        """Take patron object and return identifier for Adobe ID purposes"""
        _db = Session.object_session(patron)
        internal = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)

        def refresh(credential):
            credential.credential = str(uuid.uuid1())
        patron_identifier = Credential.lookup(
            _db, internal, AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER, patron,
            refresher_method=refresh, allow_persistent_token=True
        )
        return patron_identifier.credential


    def short_client_token_for_patron(self, patron_information):
        """Generate short client token for patron, or for a patron's identifier
         for Adobe ID purposes"""

        if isinstance(patron_information, Patron):
            # Find the patron's identifier for Adobe ID purposes.
            patron_identifier = self._adobe_patron_identifier(
                patron_information
            )
        else:
            patron_identifier = patron_information

        vendor_id, token = self.encode_short_client_token(patron_identifier)
        return vendor_id, token

    def encode_short_client_token(self, patron_identifier):
        """Generate a short client token suitable for putting in an OPDS feed,
        where it can be picked up by a client and sent to the
        delegation authority to look up an Adobe ID.

        :return: A 2-tuple (vendor ID, token)
        """
        if not patron_identifier:
            raise ValueError("No patron identifier specified")
        now = datetime.datetime.utcnow()
        expires = int(self.numericdate(now + datetime.timedelta(minutes=60)))
        authdata = self._encode_short_client_token(
            self.short_name, patron_identifier, expires
        )
        return self.vendor_id, authdata

    def _encode_short_client_token(self, library_short_name,
                                   patron_identifier, expires):
        base = library_short_name + "|" + str(expires) + "|" + patron_identifier
        signature = self.short_token_signer.sign(
            base, self.short_token_signing_key
        )
        signature = self.adobe_base64_encode(signature)
        if len(base) > 80:
            self.log.error(
                "Username portion of short client token exceeds 80 characters; Adobe will probably truncate it."
            )
        if len(signature) > 76:
            self.log.error(
                "Password portion of short client token exceeds 76 characters; Adobe will probably truncate it."
            )
        return base + "|" + signature

    def decode_short_client_token(self, token):
        """Attempt to interpret a 'username' and 'password' as a short
        client token identifying a patron of a specific library.

        :return: a 2-tuple (library_uri, patron_identifier)

        :raise ValueError: When the token is not valid for any reason.
        """
        if not '|' in token:
            raise ValueError(
                'Supposed client token "%s" does not contain a pipe.' % token
            )

        username, password = token.rsplit('|', 1)
        return self.decode_two_part_short_client_token(username, password)

    def decode_two_part_short_client_token(self, username, password):
        """Decode a short client token that has already been split into
        two parts.
        """
        signature = self.adobe_base64_decode(password)
        return self._decode_short_client_token(username, signature)

    def _decode_short_client_token(self, token, supposed_signature):
        """Make sure a client token is properly formatted, correctly signed,
        and not expired.
        """
        if token.count('|') < 2:
            raise ValueError("Invalid client token: %s" % token)
        library_short_name, expiration, patron_identifier = token.split("|", 2)

        library_short_name = library_short_name.upper()
        try:
            expiration = float(expiration)
        except ValueError:
            raise ValueError('Expiration time "%s" is not numeric.' % expiration)

        # We don't police the content of the patron identifier but there
        # has to be _something_ there.
        if not patron_identifier:
            raise ValueError(
                "Token %s has empty patron identifier" % token
            )

        if not library_short_name in self.library_uris_by_short_name:
            raise ValueError(
                "I don't know how to handle tokens from library \"%s\"" % library_short_name
            )
        library_uri = self.library_uris_by_short_name[library_short_name]
        if not library_uri in self.secrets_by_library_uri:
            raise ValueError(
                "I don't know the secret for library %s" % library_uri
            )
        secret = self.secrets_by_library_uri[library_uri]

        # Don't bother checking an expired token.
        now = datetime.datetime.utcnow()
        expiration = self.EPOCH + datetime.timedelta(seconds=expiration)
        if expiration < now:
            raise ValueError(
                "Token %s expired at %s (now is %s)." % (
                    token, expiration, now
                )
            )

        # Sign the token and check against the provided signature.
        key = self.short_token_signer.prepare_key(secret)
        actual_signature = self.short_token_signer.sign(token, key)

        if actual_signature != supposed_signature:
            raise ValueError(
                "Invalid signature for %s." % token
            )

        return library_uri, patron_identifier

    EPOCH = datetime.datetime(1970, 1, 1)

    @classmethod
    def numericdate(cls, d):
        """Turn a datetime object into a NumericDate as per RFC 7519."""
        return (d-cls.EPOCH).total_seconds()

    def migrate_adobe_id(self, patron):
        """If the given patron has an Adobe ID stored as a Credential, also
        store it as a DelegatedPatronIdentifier.

        This method and its test should be removed once all instances have
        run the migration script
        20161102-adobe-id-is-delegated-patron-identifier.py.
        """
        import uuid

        _db = Session.object_session(patron)
        credential = get_one(
            _db, Credential,
            patron=patron, type=AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE
        )
        if not credential:
            # This patron has no Adobe ID. Do nothing.
            return None, None
        adobe_id = credential.credential

        # Create a new Credential containing an anonymized patron ID.
        patron_identifier_credential = AdobeVendorIDModel.get_or_create_patron_identifier_credential(
            patron
        )

        # Then create a DelegatedPatronIdentifier mapping that
        # anonymized patron ID to the patron's Adobe ID.
        def create_function():
            """This will be called as the DelegatedPatronIdentifier
            is created. We already know the patron's Adobe ID and just
            want to store it in the DPI.
            """
            return adobe_id
        delegated_identifier, is_new = DelegatedPatronIdentifier.get_one_or_create(
            _db, self.library_uri, patron_identifier_credential.credential,
            DelegatedPatronIdentifier.ADOBE_ACCOUNT_ID, create_function
        )
        return patron_identifier_credential, delegated_identifier


class VendorIDLibraryConfigurationScript(Script):

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--website-url',
            help="The URL to this library's patron-facing website (not their circulation manager), e.g. \"https://nypl.org/\". This is used to uniquely identify a library."
        )
        parser.add_argument(
            '--short-name',
            help="The short name the library will use in Short Client Tokens, e.g. \"NYNYPL\"."
        )
        parser.add_argument(
            '--secret',
            help="The secret the library will use to sign Short Client Tokens."
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(self._db, cmd_args=cmd_args)

        default_library = Library.default(_db)
        adobe_integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.ADOBE_VENDOR_ID,
            ExternalIntegration.DRM_GOAL, library=default_library
        )
        if not adobe_integration:
            output.write(
                "Could not find an Adobe Vendor ID integration for default library %s.\n" %
                default_library.short_name
            )
            return

        setting = adobe_integration.setting(
            AuthdataUtility.OTHER_LIBRARIES_KEY
        )
        other_libraries = setting.json_value

        chosen_website = args.website_url
        if not chosen_website:
            for website in list(other_libraries.keys()):
                self.explain(output, other_libraries, website)
            return

        if (not args.short_name and not args.secret):
            self.explain(output, other_libraries, chosen_website)
            return

        if not args.short_name or not args.secret:
            output.write("To configure a library you must provide both --short_name and --secret.\n")
            return

        # All three arguments are specified. Set or modify the library's
        # SCT configuration.
        if chosen_website in other_libraries:
            what = "change"
        else:
            what = "set"
        output.write(
            "About to %s the Short Client Token configuration for %s.\n" % (
                what, chosen_website
            )
        )
        if chosen_website in other_libraries:
            output.write("Old configuration:\n")
            short_name, secret = other_libraries[chosen_website]
            self.explain(output, other_libraries, chosen_website)
        other_libraries[chosen_website] = [args.short_name, args.secret]

        output.write("New configuration:\n")
        self.explain(output, other_libraries, chosen_website)
        setting.value = json.dumps(other_libraries)
        self._db.commit()

    def explain(self, output, libraries, website):
        if not website in libraries:
            raise ValueError("Library not configured: %s" % website)
        short_name, secret = libraries[website]
        output.write("Website: %s\n" % website)
        output.write(" Short name: %s\n" % short_name)
        output.write(" Short Client Token secret: %s\n" % secret)


class ShortClientTokenLibraryConfigurationScript(Script):

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--website-url',
            help="The URL to this library's patron-facing website (not their circulation manager), e.g. \"https://nypl.org/\". This is used to uniquely identify a library.",
            required=True,
        )
        parser.add_argument(
            '--vendor-id',
            help="The name of the vendor ID the library will use. The default of 'NYPL' is probably what you want.",
            default='NYPL'
        )
        parser.add_argument(
            '--short-name',
            help="The short name the library will use in Short Client Tokens, e.g. \"NYBPL\".",
        )
        parser.add_argument(
            '--secret',
            help="The secret the library will use to sign Short Client Tokens.",
        )
        return parser

    def do_run(self, _db=None, cmd_args=None, output=sys.stdout):
        _db = _db or self._db
        args = self.parse_command_line(self._db, cmd_args=cmd_args)

        self.set_secret(
            _db, args.website_url, args.vendor_id, args.short_name,
            args.secret, output
        )
        _db.commit()

    def set_secret(self, _db, website_url, vendor_id, short_name,
                   secret, output):
        # Look up a library by its url setting.
        library_setting = get_one(
            _db, ConfigurationSetting,
            key=Configuration.WEBSITE_URL,
            value=website_url,
        )
        if not library_setting:
            available_urls = _db.query(
                ConfigurationSetting
            ).filter(
                ConfigurationSetting.key==Configuration.WEBSITE_URL
            ).filter(
                ConfigurationSetting.library!=None
            )
            raise Exception(
                "Could not locate library with URL %s. Available URLs: %s" %
                (website_url, ",".join(x.value for x in available_urls))
            )
        library = library_setting.library
        integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL, library=library
        )
        if not integration:
            integration, ignore = create(
                _db, ExternalIntegration,
                protocol=ExternalIntegration.OPDS_REGISTRATION,
                goal=ExternalIntegration.DISCOVERY_GOAL
            )
            library.integrations.append(integration)

        vendor_id_s = integration.setting(AuthdataUtility.VENDOR_ID_KEY)
        username_s = ConfigurationSetting.for_library_and_externalintegration(
            _db, ExternalIntegration.USERNAME, library, integration
        )
        password_s = ConfigurationSetting.for_library_and_externalintegration(
            _db, ExternalIntegration.PASSWORD, library, integration
        )

        if vendor_id and short_name and secret:
            vendor_id_s.value = vendor_id
            username_s.value = short_name
            password_s.value = secret

        output.write(
            "Current Short Client Token configuration for %s:\n"
            % website_url
        )
        output.write(" Vendor ID: %s\n" % vendor_id_s.value)
        output.write(" Library name: %s\n" % username_s.value)
        output.write(" Shared secret: %s\n" % password_s.value)
