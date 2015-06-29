from nose.tools import set_trace
import uuid
import base64

import flask
from flask import Response
from core.util.xmlparser import XMLParser

class AdobeVendorIDHandler(object):
    """Implement the Account Service and Authorization Service
    portions of the Adobe Vendor ID protocol.
    """

    def __init__(self, vendor_id, node_value):
        self.vendor_id = vendor_id
        self.node_value = node_value

    def signin_handler(self):
        """Process an incoming signInRequest document."""
        pass

    def account_info_handler(self):
        """Process an incoming accountInfoRequest document."""
        pass

    def status_handler(self):
        return Response("UP", 200, {"Content-Type": "text/plain"})

    def error_response(self, message):
        return self.ERORR_RESPONSE_TEMPLATE % dict(
            vendor_id=self.vendor_id, message=message)


class AdobeVendorIDUtility(object):

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

    def uuid(self):
        uuid = str(uuid.uuid1(self.node_value))
        # This chop is required by the spec. I have no idea why, but
        # since the first part of the UUID is the least significant,
        # it doesn't do much damage.
        return "urn:uuid:0" + uuid[:-1]

    def error_document(self, type, message):
        return self.ERORR_RESPONSE_TEMPLATE % dict(
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
            v = v.text.strip()
            if transform is not None:
                v = transform(v)
        d[key] = v

class AdobeSignInRequestParser(AdobeRequestParser):

    REQUEST_XPATH = "/adept:signInRequest"

    def process_one(self, tag, namespaces):
        method = tag.attrib.get('method')

        if not method:
            raise ValueError("No signin method specified")
        data = dict(method=method)
        if method == 'standard':
            self._add(data, tag, 'username', namespaces)
            self._add(data, tag, 'password', namespaces)

        elif method == 'authData':
            self._add(data, tag, 'authData', namespaces, base64.b64decode)
        return data

class AdobeAccountInfoRequestParser(AdobeRequestParser):

    REQUEST_XPATH = "/adept:accountInfoRequest"

    def process_one(self, tag, namespaces):
        method = tag.attrib.get('method')
        data = dict(method=method)
        self._add(data, tag, 'user', namespaces)
        return data
