from nose.tools import set_trace
import uuid
import base64
import os
import datetime

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
            username = data['username']
            password = data['password']
            user_id, label = standard_lookup(username, password)
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

    TEMPORARY_TOKEN_TYPE = "Temporary token for Adobe Vendor ID"
    VENDOR_ID_UUID_TOKEN_TYPE = "Vendor ID UUID"

    def __init__(self, _db, authenticator, node_value,
                 temporary_token_duration=None):
        self._db = _db
        self.authenticator = authenticator
        self.data_source = DataSource.lookup(_db, DataSource.ADOBE)
        self.temporary_token_duration = (
            temporary_token_duration or datetime.timedelta(minutes=10))
        if isinstance(node_value, basestring):
            node_value = int(node_value, 16)
        self.node_value = node_value

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
        credential, is_new = Credential.temporary_token_create(
            self._db, self.data_source, self.TEMPORARY_TOKEN_TYPE,
            patron, self.temporary_token_duration)
        return credential

    def standard_lookup(self, username, password):       
        """Look up a patron by username and password. Return their Vendor ID
        UUID and their human-readable label, creating a Credential
        object to hold the UUID if necessary.
        """
        patron = self.authenticator.authenticated_patron(
            self._db, username, password)
        return self.uuid_and_label(patron)

    def authdata_lookup(self, authdata):
        """Look up a patron by a temporary Adobe Vendor ID token. Return their
        Vendor ID UUID and their human-readable label.
        """
        credential = Credential.lookup_by_temporary_token(
            self._db, self.data_source, self.TEMPORARY_TOKEN_TYPE, 
            authdata)
        if not credential:
            return None, None
        return self.uuid_and_label(credential.patron)

    def urn_to_label(self, urn):
        credential = Credential.lookup_by_token(
            self._db, self.data_source, self.VENDOR_ID_UUID_TOKEN_TYPE, 
            urn, True)
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
