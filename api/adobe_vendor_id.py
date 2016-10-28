from nose.tools import set_trace
import uuid
import base64
import os
import datetime
import jwt

import flask
from flask import Response
from core.util.xmlparser import XMLParser
from core.model import (
    Credential,
    DataSource,
)

class AdobeVendorIDController(object):

    """Flask controllers that implement the Account Service and
    Authorization Service portions of the Adobe Vendor ID protocol.
    """
    def __init__(self, _db, vendor_id, node_value, authenticator):
        self._db = _db
        self.request_handler = AdobeVendorIDRequestHandler(vendor_id)
        self.model = AdobeVendorIDModel(self._db, authenticator, node_value)

    def create_authdata_handler(self, patron):
        """Create an authdata token for the given patron."""
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
        except Exception, e:
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
        except Exception, e:
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

    def __init__(self, _db, authenticator, node_value,
                 temporary_token_duration=None):
        self._db = _db
        self.authenticator = authenticator
        self.temporary_token_duration = (
            temporary_token_duration or datetime.timedelta(minutes=10))
        if isinstance(node_value, basestring):
            node_value = int(node_value, 16)
        self.node_value = node_value

    @property
    def data_source(self):
        return DataSource.lookup(self._db, DataSource.ADOBE)

    def uuid_and_label(self, patron):
        """Create or retrieve a Vendor ID UUID and human-readable Vendor ID
        label for the given patron.
        """
        if not patron:
            return None, None

        def generate_uuid(credential):
            # This is the first time a credential has ever been
            # created for this patron. Set the value of the 
            # credential to a new UUID.
            print "GENERATING NEW UUID"
            credential.credential = self.uuid()

        credential = Credential.lookup(
            self._db, self.data_source, self.VENDOR_ID_UUID_TOKEN_TYPE,
            patron, generate_uuid, True)

        identifier = patron.authorization_identifier          
        if not identifier:
            # Maybe this should be an error, but even though the lack
            # of an authorization identifier is a problem, the problem
            # should manifest when the patron tries to actually use
            # their credential.
            return "Unknown card number.", "Unknown card number"
        return credential.credential, "Card number " + identifier

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
        patron = None
        if (authorization_data.get('username') 
            and not authorization_data.get('password')):
            # The absence of a password indicates the username might
            # be a persistent authdata token smuggled to get around a
            # broken Adobe client-side API. Try treating the
            # 'username' as a token.
            possible_authdata_token = authorization_data['username']
            patron = self.patron_from_authdata_lookup(possible_authdata_token)
        if not patron:
            # Either a password was provided or the authdata token
            # lookup failed. Try a normal username/password lookup.
            patron = self.authenticator.authenticated_patron(
                self._db, authorization_data
            )
        return self.uuid_and_label(patron)

    def patron_from_authdata_lookup(self, authdata):
        """Look up a patron by their persistent authdata token."""
        credential = Credential.lookup_by_token(
            self._db, self.data_source, self.AUTHDATA_TOKEN_TYPE, 
            authdata, allow_persistent_token=True
        )
        if not credential:
            return None
        return credential.patron

    def authdata_lookup(self, authdata):
        """Look up a patron by a persistent authdata token. Return their
        Vendor ID UUID and their human-readable label.
        """
        patron = self.patron_from_authdata_lookup(authdata)
        if not patron:
            return None, None
        return self.uuid_and_label(patron)

    def urn_to_label(self, urn):
        credential = Credential.lookup_by_token(
            self._db, self.data_source, self.VENDOR_ID_UUID_TOKEN_TYPE, 
            urn, allow_persistent_token=True
        )
        if not credential:
            return None
        patron = credential.patron
        uuid, label = self.uuid_and_label(credential.patron)
        return label

    def uuid(self):
        """Create a new UUID URN compatible with the Vendor ID system."""
        u = str(uuid.uuid1(self.node_value))
        # This chop is required by the spec. I have no idea why, but
        # since the first part of the UUID is the least significant,
        # it doesn't do much damage.
        value = "urn:uuid:0" + u[1:]
        return value

class AuthdataUtility(object):

    """Generate authdata JWTs as per the Vendor ID Service spec:
    https://docs.google.com/document/d/1j8nWPVmy95pJ_iU4UTC-QgHK2QhDUSdQ0OQTFR2NE_0    

    Capable of encoding JWTs (for this library), and decoding them
    (from this library and potentially others).
    """

    ALGORITHM = 'HS256'
    
    def __init__(self, vendor_id, library_uri, secret, other_libraries={}):
        """Basic constructor.

        :param vendor_id: The Adobe Vendor ID that should accompany authdata
        generated by this utility.

        If this library has its own Adobe Vendor ID, it should go
        here. If this library is delegating authdata control to some
        other library, that library's Vendor ID should go here.

        :param library_uri: A URI identifying this library.

        :param secret: A secret used to sign this library's authdata.

        :param other_libraries: A dictionary mapping other libraries'
        URIs to their secrets. An instance of this class will be able
        to decode an authdata from any library in this dictionary
        (plus the library it was initialized for).
        """
        self.vendor_id = vendor_id

        # This is used to _encode_ JWTs and send them to the
        # delegation authority.
        self.library_uri = library_uri
        self.secret = secret

        # This is used by the delegation authority to _decode_ JWTs.
        self.secrets_by_library = dict(other_libraries)
        self.secrets_by_library[library_uri] = secret
       
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
        return jwt.encode(payload, self.secret, algorithm=self.ALGORITHM)

    def decode(self, authdata):
        """Decode and verify an authdata JWT from one of the libraries managed
        by `secrets_by_library`.

        :return: a 2-tuple (library_uri, patron_identifier)

        :raise jwt.exceptions.DecodeError: When the JWT is not valid
        for any reason.
        """
        # First, decode the authdata without checking the signature.
        decoded = jwt.decode(
            authdata, algorithm='HS256', options=dict(verify_signature=False)
        )

        # This lets us get the library URI, which lets us get the secret.
        library_uri = decoded.get('iss')
        if not library_uri in self.secrets_by_library:
            # The request came in without a library specified
            # or with an unknown library specified.
            raise jwt.exceptions.DecodeError(
                "Unknown library: %s" % library_uri
            )

        # We know the secret for this library, so we can re-decode the
        # secret and require signature valudation this time.
        secret = self.secrets_by_library[library_uri]
        decoded = jwt.decode(authdata, secret, algorithm=self.ALGORITHM)
        if not 'sub' in decoded:
            raise jwt.exceptions.DecodeError("No subject specified.")
        return library_uri, decoded['sub']
        
    EPOCH = datetime.datetime(1970, 1, 1)

    @classmethod
    def numericdate(cls, d):
        """Turn a datetime object into a NumericDate as per RFC 7519."""
        return (d-cls.EPOCH).total_seconds()
