import base64
from nose.tools import set_trace, eq_
import re
import datetime

from ..adobe_vendor_id import (
    AdobeSignInRequestParser,
    AdobeAccountInfoRequestParser,
    AdobeVendorIDRequestHandler,
    AdobeVendorIDModel,
)

from . import (
    DatabaseTest,
)

from ..core.model import (
    Credential,
    DataSource,
)

from ..millenium_patron import DummyMilleniumPatronAPI

class TestVendorIDModel(DatabaseTest):

    TEST_NODE_VALUE = 114740953091845

    def setup(self):
        super(TestVendorIDModel, self).setup()
        self.authenticator = DummyMilleniumPatronAPI()
        self.model = AdobeVendorIDModel(self._db, self.authenticator,
                                        self.TEST_NODE_VALUE)
        self.data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        # Normally this test patron doesn't have an authorization identifier.
        # Let's make sure there is one so it'll show up as the label.
        self.bob_patron = self.authenticator.authenticated_patron(
            self._db, "5", "5555")
        self.bob_patron.authorization_identifier = "5"

    def test_uuid(self):
        u = self.model.uuid()
        # All UUIDs need to start with a 0 and end with the same node
        # value.
        assert u.startswith('urn:uuid:0')
        assert u.endswith('685b35c00f0')

    def test_standard_lookup_success(self):
        urn, label = self.model.standard_lookup("5", "5555")

        # There is now a UUID associated with Bob's patron account,
        # and that's the UUID returned by standard_lookup().
        bob_uuid = Credential.lookup(
            self._db, self.data_source, self.model.VENDOR_ID_UUID_TOKEN_TYPE,
            self.bob_patron, None)
        eq_("Card number 5", label)
        eq_(urn, bob_uuid.credential)
        assert urn.startswith("urn:uuid:0")
        assert urn.endswith('685b35c00f0')

    def test_authdata_lookup_success(self):
        now = datetime.datetime.utcnow()
        temp_token = Credential.temporary_token_create(
            self._db, self.data_source, self.model.TEMPORARY_TOKEN_TYPE,
            self.bob_patron, datetime.timedelta(seconds=60))
        old_expires = temp_token.expires
        assert temp_token.expires > now
        urn, label = self.model.authdata_lookup(temp_token.credential)

        # There is now a UUID associated with Bob's patron account,
        # and that's the UUID returned by standard_lookup().
        bob_uuid = Credential.lookup(
            self._db, self.data_source, self.model.VENDOR_ID_UUID_TOKEN_TYPE,
            self.bob_patron, None)
        eq_(urn, bob_uuid.credential)
        eq_("Card number 5", label)

        # Having been used once, the temporary token has been expired.
        assert temp_token.expires < now

    def test_authdata_lookup_failure_no_token(self):
        urn, label = self.model.authdata_lookup("nosuchauthdata")
        eq_(None, urn)
        eq_(None, label)

    def test_authdata_lookup_failure_wrong_token(self):
        temp_token = Credential.temporary_token_create(
            self._db, self.data_source, self.model.TEMPORARY_TOKEN_TYPE,
            self.bob_patron, datetime.timedelta(seconds=60))
        urn, label = self.model.authdata_lookup("nosuchauthdata")
        eq_(None, urn)
        eq_(None, label)

    def test_urn_to_label_success(self):
        urn, label = self.model.standard_lookup("5", "5555")
        eq_("Card number 5", label)

    def test_urn_to_label_failure_no_active_credential(self):
        label = self.model.urn_to_label("bad urn")
        eq_(None, label)

    def test_urn_to_label_failure_incorrect_urn(self):
        urn, label = self.model.standard_lookup("5", "5555")
        label = self.model.urn_to_label("bad urn")
        eq_(None, label)


class TestVendorIDRequestParsers(object):

    username_sign_in_request = """<signInRequest method="standard" xmlns="http://ns.adobe.com/adept">
<username>Vendor username</username>
<password>Vendor password</password>
</signInRequest>"""

    authdata_sign_in_request = """<signInRequest method="authData" xmlns="http://ns.adobe.com/adept">
<authData> dGhpcyBkYXRhIHdhcyBiYXNlNjQgZW5jb2RlZA== </authData>
</signInRequest>"""

    accountinfo_request = """<accountInfoRequest method="standard" xmlns="http://ns.adobe.com/adept">
<user>urn:uuid:0xxxxxxx-xxxx-1xxx-xxxx-yyyyyyyyyyyy</user>
</accountInfoRequest >"""

    def test_username_sign_in_request(self):
        parser = AdobeSignInRequestParser()
        data = parser.process(self.username_sign_in_request)
        eq_({'username': 'Vendor username',
             'password': 'Vendor password', 'method': 'standard'}, data)

    def test_authdata_sign_in_request(self):
        parser = AdobeSignInRequestParser()
        data = parser.process(self.authdata_sign_in_request)
        eq_({'authData': 'this data was base64 encoded', 'method': 'authData'},
            data)

    def test_accountinfo_request(self):
        parser = AdobeAccountInfoRequestParser()
        data = parser.process(self.accountinfo_request)
        eq_({'method': 'standard', 
             'user': 'urn:uuid:0xxxxxxx-xxxx-1xxx-xxxx-yyyyyyyyyyyy'},
            data)

class TestVendorIDRequestHandler(object):

    username_sign_in_request = """<signInRequest method="standard" xmlns="http://ns.adobe.com/adept">
<username>%(username)s</username>
<password>%(password)s</password>
</signInRequest>"""

    authdata_sign_in_request = """<signInRequest method="authData" xmlns="http://ns.adobe.com/adept">
<authData>%(authdata)s</authData>
</signInRequest>"""

    accountinfo_request = """<accountInfoRequest method="standard" xmlns="http://ns.adobe.com/adept">
<user>%(uuid)s</user>
</accountInfoRequest >"""

    TEST_VENDOR_ID = "1045"

    user1_uuid = "test-uuid"
    user1_label = "Human-readable label for user1"
    username_password_lookup = {
        ("user1", "pass1") : (user1_uuid, user1_label)
    }

    authdata_lookup = {
        "The secret token" : (user1_uuid, user1_label)
    }

    userinfo_lookup = { user1_uuid : user1_label }

    @property
    def _handler(self):
        return AdobeVendorIDRequestHandler(
            self.TEST_VENDOR_ID)

    @classmethod
    def _standard_login(cls, username, password):
        return cls.username_password_lookup.get(
            (username, password), (None, None))

    @classmethod
    def _authdata_login(cls, authdata):
        return cls.authdata_lookup.get(authdata, (None, None))

    @classmethod
    def _userinfo(cls, uuid):
        return cls.userinfo_lookup.get(uuid)

    def test_error_document(self):
        doc = self._handler.error_document(
            "VENDORID", "Some random error")
        eq_('<error xmlns="http://ns.adobe.com/adept" data="E_1045_VENDORID Some random error"/>', doc)

    def test_handle_username_sign_in_request_success(self):
        doc = self.username_sign_in_request % dict(
            username="user1", password="pass1")
        result = self._handler.handle_signin_request(
            doc, self._standard_login, self._authdata_login)
        assert result.startswith('<signInResponse xmlns="http://ns.adobe.com/adept">\n<user>test-uuid</user>\n<label>Human-readable label for user1</label>\n</signInResponse>')

    def test_handle_username_sign_in_request_failure(self):
        doc = self.username_sign_in_request % dict(
            username="user1", password="wrongpass")
        result = self._handler.handle_signin_request(
            doc, self._standard_login, self._authdata_login)
        eq_('<error xmlns="http://ns.adobe.com/adept" data="E_1045_AUTH Incorrect barcode or PIN."/>', result)

    def test_handle_username_authdata_request_success(self):
        doc = self.authdata_sign_in_request % dict(
            authdata=base64.b64encode("The secret token"))
        result = self._handler.handle_signin_request(
            doc, self._standard_login, self._authdata_login)
        assert result.startswith('<signInResponse xmlns="http://ns.adobe.com/adept">\n<user>test-uuid</user>\n<label>Human-readable label for user1</label>\n</signInResponse>')

    def test_handle_username_authdata_request_invalid(self):
        doc = self.authdata_sign_in_request % dict(
            authdata="incorrect")
        result = self._handler.handle_signin_request(
            doc, self._standard_login, self._authdata_login)
        assert result.startswith('<error xmlns="http://ns.adobe.com/adept" data="E_1045_AUTH')

    def test_handle_username_authdata_request_failure(self):
        doc = self.authdata_sign_in_request % dict(
            authdata=base64.b64encode("incorrect"))
        result = self._handler.handle_signin_request(
            doc, self._standard_login, self._authdata_login)
        eq_('<error xmlns="http://ns.adobe.com/adept" data="E_1045_AUTH Incorrect token."/>', result)

    def test_handle_accountinfo_success(self):
        doc = self.accountinfo_request % dict(
            uuid=self.user1_uuid)
        result = self._handler.handle_accountinfo_request(
            doc, self._userinfo)
        eq_('<accountInfoResponse xmlns="http://ns.adobe.com/adept">\n<label>Human-readable label for user1</label>\n</accountInfoResponse>', result)

    def test_handle_accountinfo_failure(self):
        doc = self.accountinfo_request % dict(
            uuid="not the uuid")
        result = self._handler.handle_accountinfo_request(
            doc, self._userinfo)
        eq_('<error xmlns="http://ns.adobe.com/adept" data="E_1045_ACCOUNT_INFO Could not identify patron."/>', result)
