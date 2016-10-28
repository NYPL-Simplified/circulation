import base64
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
    assert_raises_regexp
)
import jwt
from jwt.exceptions import (
    DecodeError,
    ExpiredSignatureError,
    InvalidIssuedAtError
)
import re
import datetime

from api.adobe_vendor_id import (
    AdobeSignInRequestParser,
    AdobeAccountInfoRequestParser,
    AdobeVendorIDRequestHandler,
    AdobeVendorIDModel,
    AuthdataUtility,
)

from . import (
    DatabaseTest,
)

from core.model import (
    Credential,
    DataSource,
)

from api.mock_authentication import MockAuthenticationProvider       

class TestVendorIDModel(DatabaseTest):

    TEST_NODE_VALUE = 114740953091845

    credentials = dict(username="validpatron", password="password")
    
    def setup(self):
        super(TestVendorIDModel, self).setup()
        self.authenticator = MockAuthenticationProvider(
            patrons={"validpatron" : "password" }
        )
        self.model = AdobeVendorIDModel(self._db, self.authenticator,
                                        self.TEST_NODE_VALUE)
        self.data_source = DataSource.lookup(self._db, DataSource.ADOBE)

        self.bob_patron = self.authenticator.authenticated_patron(
            self._db, dict(username="validpatron", password="password"))
        
    def test_uuid(self):
        u = self.model.uuid()
        # All UUIDs need to start with a 0 and end with the same node
        # value.
        assert u.startswith('urn:uuid:0')
        assert u.endswith('685b35c00f05')

    def test_uuid_and_label_respects_existing_id(self):
        uuid, label = self.model.uuid_and_label(self.bob_patron)
        uuid2, label2 = self.model.uuid_and_label(self.bob_patron)
        eq_(uuid, uuid2)
        eq_(label, label2)

    def test_create_authdata(self):
        credential = self.model.create_authdata(self.bob_patron)

        # There's now a persistent token associated with Bob's
        # patron account, and that's the token returned by create_authdata()
        bob_authdata = Credential.lookup(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron, None)
        eq_(credential.credential, bob_authdata.credential)

    def test_standard_lookup_success(self):
        urn, label = self.model.standard_lookup(self.credentials)

        # There is now a UUID associated with Bob's patron account,
        # and that's the UUID returned by standard_lookup().
        bob_uuid = Credential.lookup(
            self._db, self.data_source, self.model.VENDOR_ID_UUID_TOKEN_TYPE,
            self.bob_patron, None)
        eq_("Card number validpatron", label)
        eq_(urn, bob_uuid.credential)
        assert urn.startswith("urn:uuid:0")
        assert urn.endswith('685b35c00f05')

    def test_authdata_lookup_success(self):
        
        # Create an authdata token for Bob.
        now = datetime.datetime.utcnow()
        token, ignore = Credential.persistent_token_create(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron
        )

        # The token is persistent.
        eq_(None, token.expires)

        # Use that token to perform a lookup of Bob's Adobe Vendor ID
        # UUID.
        urn, label = self.model.authdata_lookup(token.credential)

        # There is now a UUID associated with Bob's patron account,
        # and that's the UUID returned by standard_lookup().
        bob_uuid = Credential.lookup(
            self._db, self.data_source, self.model.VENDOR_ID_UUID_TOKEN_TYPE,
            self.bob_patron, None)
        eq_(urn, bob_uuid.credential)
        eq_("Card number validpatron", label)

        # The token is persistent and does not expire.
        eq_(None, token.expires)

    def test_smuggled_authdata_success(self):
        # Bob's client has created a persistent token to authenticate him.
        now = datetime.datetime.utcnow()
        token, ignore = Credential.persistent_token_create(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron
        )

        # But Bob's client can't trigger the operation that will cause
        # Adobe to authenticate him via that token, so it passes in
        # the token credential as the 'username' and leaves the
        # password blank.
        urn, label = self.model.standard_lookup(
            dict(username=token.credential)
        )

        # There is now a UUID associated with Bob's patron account,
        # and that's the UUID returned by standard_lookup().
        bob_uuid = Credential.lookup(
            self._db, self.data_source, self.model.VENDOR_ID_UUID_TOKEN_TYPE,
            self.bob_patron, None)
        eq_(urn, bob_uuid.credential)

        # The token is persistent and will not expire or be consumed.
        eq_(None, token.expires)

        # A future attempt to authenticate with the token will succeed.
        urn, label = self.model.standard_lookup(
            dict(username=token.credential)
        )
        eq_(urn, bob_uuid.credential)

    def test_authdata_lookup_failure_no_token(self):
        urn, label = self.model.authdata_lookup("nosuchauthdata")
        eq_(None, urn)
        eq_(None, label)

    def test_authdata_lookup_failure_wrong_token(self):
        # Bob has an authdata token.
        token, ignore = Credential.persistent_token_create(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron
        )

        # But we look up a different token and get nothing.
        urn, label = self.model.authdata_lookup("nosuchauthdata")
        eq_(None, urn)
        eq_(None, label)

    def test_urn_to_label_success(self):
        urn, label = self.model.standard_lookup(self.credentials)
        label2 = self.model.urn_to_label(urn)
        eq_(label, label2)
        eq_("Card number validpatron", label)

    def test_urn_to_label_failure_no_active_credential(self):
        label = self.model.urn_to_label("bad urn")
        eq_(None, label)

    def test_urn_to_label_failure_incorrect_urn(self):
        urn, label = self.model.standard_lookup(self.credentials)
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
    def _standard_login(cls, data):
        return cls.username_password_lookup.get(
            (data.get('username'), data.get('password')), (None, None))

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

    def test_failure_send_login_request_to_accountinfo(self):
        doc = self.authdata_sign_in_request % dict(
            authdata=base64.b64encode("incorrect"))
        result = self._handler.handle_accountinfo_request(
            doc, self._userinfo)
        eq_('<error xmlns="http://ns.adobe.com/adept" data="E_1045_ACCOUNT_INFO Request document in wrong format."/>', result)

    def test_failure_send_accountinfo_request_to_login(self):
        doc = self.accountinfo_request % dict(
            uuid=self.user1_uuid)
        result = self._handler.handle_signin_request(
            doc, self._standard_login, self._authdata_login)
        eq_('<error xmlns="http://ns.adobe.com/adept" data="E_1045_AUTH Request document in wrong format."/>', result)

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
        eq_('<error xmlns="http://ns.adobe.com/adept" data="E_1045_ACCOUNT_INFO Could not identify patron from \'not the uuid\'."/>', result)


class TestAuthdataUtility(object):

    def setup(self):
        self.authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://my-library.org/",
            secret = "My library secret",
            other_libraries = {
                "http://your-library.org/": "Your library secret"
            }
        )
           
    def test_decode_round_trip(self):        
        patron_identifier = "Patron identifier"
        vendor_id, authdata = self.authdata.encode(patron_identifier)
        eq_("The Vendor ID", vendor_id)
        
        # We can decode the authdata with our secret.
        decoded = self.authdata.decode(authdata)
        eq_(("http://my-library.org/", "Patron identifier"), decoded)

    def test_encode(self):
        """Test that _encode gives a known value with known input."""
        patron_identifier = "Patron identifier"
        now = datetime.datetime(2016, 1, 1, 12, 0, 0)
        expires = datetime.datetime(2018, 1, 1, 12, 0, 0)
        authdata = self.authdata._encode(
            self.authdata.library_uri, patron_identifier, now, expires
        )
        eq_('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vbXktbGlicmFyeS5vcmcvIiwiaWF0IjoxNDUxNjQ5NjAwLjAsInN1YiI6IlBhdHJvbiBpZGVudGlmaWVyIiwiZXhwIjoxNTE0ODA4MDAwLjB9.n7VRVv3gIyLmNxTzNRTEfCdjoky0T0a1Jhehcag1oQw', authdata)

    def test_decode_from_another_library(self):        

        # Here's the AuthdataUtility used by another library.
        foreign_authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://your-library.org/",
            secret = "Your library secret",
        )
        
        patron_identifier = "Patron identifier"
        vendor_id, authdata = foreign_authdata.encode(patron_identifier)

        # Because we know the other library's secret, we're able to
        # decode the authdata.
        decoded = self.authdata.decode(authdata)
        eq_(("http://your-library.org/", "Patron identifier"), decoded)

        # If our secret doesn't match the other library's secret,
        # we can't decode the authdata
        foreign_authdata.secret = 'A new secret'
        vendor_id, authdata = foreign_authdata.encode(patron_identifier)
        assert_raises_regexp(
            DecodeError, "Signature verification failed",
            self.authdata.decode, authdata
        )
        
    def test_decode_from_unknown_library_fails(self):

        # Here's the AuthdataUtility used by a library we don't know
        # about.
        foreign_authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://some-other-library.org/",
            secret = "Some other library secret",
        )
        vendor_id, authdata = foreign_authdata.encode("A patron")

        # They can encode, but we cna't decode.
        assert_raises_regexp(
            DecodeError, "Unknown library: http://some-other-library.org/",
            self.authdata.decode, authdata
        )

    def test_cannot_decode_token_from_future(self):
        future = datetime.datetime.utcnow() + datetime.timedelta(days=365)
        authdata = self.authdata._encode(
            "Patron identifier", iat=future
        )        
        assert_raises(
            InvalidIssuedAtError, self.authdata.decode, authdata
        )
        
    def test_cannot_decode_expired_token(self):
        expires = datetime.datetime(2016, 1, 1, 12, 0, 0)
        authdata = self.authdata._encode(
            "Patron identifier", exp=expires
        )
        assert_raises(
            ExpiredSignatureError, self.authdata.decode, authdata
        )
        
    def test_cannot_encode_null_patron_identifier(self):
        assert_raises_regexp(
            ValueError, "No patron identifier specified",
            self.authdata.encode, None
        )
        
    def test_cannot_decode_null_patron_identifier(self):

        authdata = self.authdata._encode(
            self.authdata.library_uri, None, 
        )
        assert_raises_regexp(
            DecodeError, "No subject specified",
            self.authdata.decode, authdata
        )
