import argparse
import json
import logging
import uuid
import base64
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
from core.util.datetime_helpers import (
    datetime_utc,
    utc_now,
)
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

    def get_or_create_patron_identifier_credential(self, patron):
        _db = Session.object_session(patron)

        def refresh(credential):
            credential.credential = str(uuid.uuid1())

        data_source = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)
        patron_identifier_credential = Credential.lookup(
            _db, data_source,
            ShortClientTokenUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            patron, refresher_method=refresh, allow_persistent_token=True
        )
        return patron_identifier_credential

    def _request_handler(self, patron):
        """Create a DeviceManagementRequestHandler for the appropriate
        Credential of the given Patron.

        :return: A DeviceManagementRequestHandler
        """
        if not patron:
            return INVALID_CREDENTIALS.detailed(_("No authenticated patron"))

        credential = self.get_or_create_patron_identifier_credential(patron)
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
            output = handler.register_device(flask.request.get_data(as_text=True))
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


class ShortClientTokenUtility(object):

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
    VENDOR_ID_UUID_TOKEN_TYPE = "Vendor ID UUID"

    ALGORITHM = 'HS256'

    def __init__(self, vendor_id, library_uri, library_short_name, secret):
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

        self.log = logging.getLogger("Adobe authdata utility")

        self.short_token_signer = HMACAlgorithm(HMACAlgorithm.SHA256)
        self.short_token_signing_key = self.short_token_signer.prepare_key(
            self.secret
        )

    VENDOR_ID_KEY = 'vendor_id'
    OTHER_LIBRARIES_KEY = 'other_libraries'

    @classmethod
    def from_config(cls, library, _db=None):
        """Initialize an ShortClientTokenUtility from site configuration.

        :return: An ShortClientTokenUtility if one is configured; otherwise None.

        :raise CannotLoadConfiguration: If an ShortClientTokenUtility is
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

        if (not vendor_id or not library_uri or not library_short_name or not secret):
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
        return cls(vendor_id, library_uri, library_short_name, secret)

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
        types = (
            ShortClientTokenUtility.VENDOR_ID_UUID_TOKEN_TYPE,
            ShortClientTokenUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER
        )
        return _db.query(
            Credential).filter(Credential.patron==patron).filter(
                Credential.type.in_(types)
            )

    @classmethod
    def adobe_base64_encode(cls, str_to_encode):
        """A modified base64 encoding that avoids triggering an Adobe bug.

        The bug seems to happen when the 'password' portion of a
        username/password pair contains a + character. So we replace +
        with :. We also replace / (another "suspicious" character)
        with ;. and strip newlines.
        """
        if isinstance(str_to_encode, str):
            str_to_encode = str_to_encode.encode("utf-8")
        encoded = base64.encodebytes(str_to_encode).decode("utf-8").strip()
        return encoded.replace("+", ":").replace("/", ";").replace("=", "@")

    @classmethod
    def adobe_base64_decode(cls, str):
        """Undoes adobe_base64_encode."""
        encoded = str.replace(":", "+").replace(";", "/").replace("@", "=")
        return base64.decodebytes(encoded.encode("utf-8"))

    @classmethod
    def _adobe_patron_identifier(cls, patron):
        """Take patron object and return identifier for Adobe ID purposes"""
        _db = Session.object_session(patron)
        internal = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)

        def refresh(credential):
            credential.credential = str(uuid.uuid1())
        patron_identifier = Credential.lookup(
            _db, internal, ShortClientTokenUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER, patron,
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
        now = utc_now()
        expires = int(self.numericdate(now + datetime.timedelta(minutes=60)))
        authdata = self._encode_short_client_token(
            self.short_name, patron_identifier, expires
        )
        return self.vendor_id, authdata

    def _encode_short_client_token(self, library_short_name,
                                   patron_identifier, expires):
        base = library_short_name + "|" + str(expires) + "|" + patron_identifier
        signature = self.short_token_signer.sign(
            base.encode("utf-8"), self.short_token_signing_key
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
        now = utc_now()
        expiration = self.EPOCH + datetime.timedelta(seconds=expiration)
        if expiration < now:
            raise ValueError(
                "Token %s expired at %s (now is %s)." % (
                    token, expiration, now
                )
            )

        # Sign the token and check against the provided signature.
        key = self.short_token_signer.prepare_key(secret)
        actual_signature = self.short_token_signer.sign(token.encode("utf-8"), key)

        if actual_signature != supposed_signature:
            raise ValueError(
                "Invalid signature for %s." % token
            )

        return library_uri, patron_identifier

    EPOCH = datetime_utc(1970, 1, 1)

    @classmethod
    def numericdate(cls, d):
        """Turn a datetime object into a NumericDate as per RFC 7519."""
        return (d-cls.EPOCH).total_seconds()
