import base64
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
    assert_raises_regexp
)
import contextlib
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

from api.opds import CirculationManagerAnnotator

from . import (
    DatabaseTest,
)

from core.model import (
    Credential,
    DataSource,
    DelegatedPatronIdentifier,
)

from api.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from api.mock_authentication import MockAuthenticationProvider       

class VendorIDTest(DatabaseTest):

    TEST_VENDOR_ID = "vendor id"
    TEST_LIBRARY_URI = "http://me/"
    TEST_SECRET = "some secret"
    TEST_OTHERS  = {"http://you/": "secret2"}
        
    @contextlib.contextmanager
    def temp_config(self):
        """Configure a basic Vendor ID Service setup."""
        name = Configuration.ADOBE_VENDOR_ID_INTEGRATION
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][name] = {
                Configuration.ADOBE_VENDOR_ID: self.TEST_VENDOR_ID,
                AuthdataUtility.LIBRARY_URI_KEY: self.TEST_LIBRARY_URI,
                AuthdataUtility.AUTHDATA_SECRET_KEY: self.TEST_SECRET,
                AuthdataUtility.OTHER_LIBRARIES_KEY: self.TEST_OTHERS
            }
            yield config

class TestVendorIDModel(VendorIDTest):

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
        with self.temp_config():
            uuid, label = self.model.uuid_and_label(self.bob_patron)
            uuid2, label2 = self.model.uuid_and_label(self.bob_patron)
        eq_(uuid, uuid2)
        eq_(label, label2)

    def test_uuid_and_label_creates_delegatedpatronid_from_credential(self):
       
        # This patron once used the old system to create an Adobe
        # account ID which was stored in a Credential. For whatever
        # reason, the migration script did not give them a
        # DelegatedPatronIdentifier.
        adobe = self.data_source
        def set_value(credential):
            credential.credential = "A dummy value"
        old_style_credential = Credential.lookup(
            self._db, adobe, self.model.VENDOR_ID_UUID_TOKEN_TYPE,
            self.bob_patron, set_value, True
        )

        # Now uuid_and_label works.
        with self.temp_config():
            uuid, label = self.model.uuid_and_label(self.bob_patron)
        eq_("A dummy value", uuid)
        eq_("Delegated account ID A dummy value", label)

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            CirculationManagerAnnotator.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            self.bob_patron, None
        )

        # That anonymized identifier is associated with a
        # DelegatedPatronIdentifier whose delegated_identifier is
        # taken from the old-style Credential.
        [bob_delegated_patron_identifier] = self._db.query(
            DelegatedPatronIdentifier).filter(
                DelegatedPatronIdentifier.patron_identifier
                ==bob_anonymized_identifier.credential
            ).all()
        eq_("A dummy value",
            bob_delegated_patron_identifier.delegated_identifier)

        # If the DelegatedPatronIdentifier and the Credential
        # have different values, the DelegatedPatronIdentifier wins.
        old_style_credential.credential = "A different value."
        with self.temp_config():
            uuid, label = self.model.uuid_and_label(self.bob_patron)
        eq_("A dummy value", uuid)
        
        # We can even delete the old-style Credential, and
        # uuid_and_label will still give the value that was stored in
        # it.
        self._db.delete(old_style_credential)
        self._db.commit()
        with self.temp_config():
            uuid, label = self.model.uuid_and_label(self.bob_patron)
        eq_("A dummy value", uuid)

        
    def test_create_authdata(self):
        credential = self.model.create_authdata(self.bob_patron)

        # There's now a persistent token associated with Bob's
        # patron account, and that's the token returned by create_authdata()
        bob_authdata = Credential.lookup(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron, None)
        eq_(credential.credential, bob_authdata.credential)      
        
    def test_to_delegated_patron_identifier_uuid(self):
        
        foreign_uri = "http://your-library/"
        foreign_identifier = "foreign ID"

        # Pass in nothing and you get nothing.
        eq_((None, None),
            self.model.to_delegated_patron_identifier_uuid(foreign_uri, None)
        )
        eq_((None, None),
            self.model.to_delegated_patron_identifier_uuid(
                None, foreign_identifier
            )
        )

        # Pass in a URI and identifier and you get a UUID and a label.
        with self.temp_config() as config:
            uuid, label = self.model.to_delegated_patron_identifier_uuid(
                foreign_uri, foreign_identifier
            )

        # We can't test a specific value for the UUID but we can test the label.
        eq_("Delegated account ID " + uuid, label)

        # And we can verify that a DelegatedPatronIdentifier was
        # created for the URI+identifier, and that it contains the
        # UUID.
        [dpi] = self._db.query(DelegatedPatronIdentifier).filter(
            DelegatedPatronIdentifier.library_uri==foreign_uri).filter(
            DelegatedPatronIdentifier.patron_identifier==foreign_identifier
        ).all()
        eq_(uuid, dpi.delegated_identifier)

    def test_authdata_lookup_delegated_patron_identifier_success(self):
        """Test that one library can perform an authdata lookup on a JWT
        generated by a different library.
        """
        # Here's a library that delegates to another library's vendor
        # ID. It can't issue Adobe IDs, but it can generate a JWT for
        # one of its patrons.
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][Configuration.ADOBE_VENDOR_ID_INTEGRATION] = {
                Configuration.ADOBE_VENDOR_ID: self.TEST_VENDOR_ID,
                AuthdataUtility.LIBRARY_URI_KEY: "http://you/",
                AuthdataUtility.AUTHDATA_SECRET_KEY: "secret2",
            }
            utility = AuthdataUtility.from_config()
            vendor_id, jwt = utility.encode("Foreign patron")

        # Here's another library that issues Adobe IDs for that
        # first library.
        with self.temp_config():
            utility = AuthdataUtility.from_config()
            eq_("secret2", utility.secrets_by_library["http://you/"])

            # Because this library shares the other library's secret,
            # it can decode a JWT issued by the other library, and
            # issue an Adobe ID (UUID).
            uuid, label = self.model.authdata_lookup(jwt)

            # We get the same result if we smuggle the JWT into
            # a username/password lookup as the username.
            uuid2, label2 = self.model.standard_lookup(dict(username=jwt))
            eq_(uuid2, uuid)
            eq_(label2, label)
            
        # The UUID corresponds to a DelegatedPatronIdentifier,
        # associated with the foreign library and the patron
        # identifier that library encoded in its JWT.
        [dpi] = self._db.query(DelegatedPatronIdentifier).filter(
            DelegatedPatronIdentifier.library_uri=="http://you/").filter(
                DelegatedPatronIdentifier.patron_identifier=="Foreign patron"
            ).all()
        eq_(uuid, dpi.delegated_identifier)
        eq_("Delegated account ID %s" % uuid, label)
        
    def test_username_password_lookup_success(self):
        with self.temp_config():
            urn, label = self.model.standard_lookup(self.credentials)

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            CirculationManagerAnnotator.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            self.bob_patron, None
        )

        # That anonymized identifier is associated with a
        # DelegatedPatronIdentifier whose delegated_identifier is a
        # UUID.
        [bob_delegated_patron_identifier] = self._db.query(
            DelegatedPatronIdentifier).filter(
                DelegatedPatronIdentifier.patron_identifier
                ==bob_anonymized_identifier.credential
            ).all()

        eq_("Delegated account ID %s" % urn, label)
        eq_(urn, bob_delegated_patron_identifier.delegated_identifier)
        assert urn.startswith("urn:uuid:0")
        assert urn.endswith('685b35c00f05')

    def test_authdata_token_credential_lookup_success(self):
        
        # Create an authdata token Credential for Bob.
        now = datetime.datetime.utcnow()
        token, ignore = Credential.persistent_token_create(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron
        )

        # The token is persistent.
        eq_(None, token.expires)

        # Use that token to perform a lookup of Bob's Adobe Vendor ID
        # UUID.
        with self.temp_config():
            urn, label = self.model.authdata_lookup(token.credential)

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            CirculationManagerAnnotator.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            self.bob_patron, None
        )

        # That anonymized identifier is associated with a
        # DelegatedPatronIdentifier whose delegated_identifier is a
        # UUID.
        [bob_delegated_patron_identifier] = self._db.query(
            DelegatedPatronIdentifier).filter(
                DelegatedPatronIdentifier.patron_identifier
                ==bob_anonymized_identifier.credential
            ).all()

        # That UUID is the one returned by authdata_lookup.
        eq_(urn, bob_delegated_patron_identifier.delegated_identifier)

    def test_smuggled_authdata_credential_success(self):
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
        with self.temp_config():
            urn, label = self.model.standard_lookup(
                dict(username=token.credential)
            )

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            CirculationManagerAnnotator.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            self.bob_patron, None
        )

        # That anonymized identifier is associated with a
        # DelegatedPatronIdentifier whose delegated_identifier is a
        # UUID.
        [bob_delegated_patron_identifier] = self._db.query(
            DelegatedPatronIdentifier).filter(
                DelegatedPatronIdentifier.patron_identifier
                ==bob_anonymized_identifier.credential
            ).all()

        # That UUID is the one returned by standard_lookup.
        eq_(urn, bob_delegated_patron_identifier.delegated_identifier)

        # A future attempt to authenticate with the token will succeed.
        urn, label = self.model.standard_lookup(
            dict(username=token.credential)
        )
        eq_(urn, bob_delegated_patron_identifier.delegated_identifier)

    def test_authdata_lookup_failure_no_token(self):
        with self.temp_config():
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
        with self.temp_config():
            urn, label = self.model.authdata_lookup("nosuchauthdata")
        eq_(None, urn)
        eq_(None, label)

    def test_urn_to_label_success(self):
        with self.temp_config():
            urn, label = self.model.standard_lookup(self.credentials)
        label2 = self.model.urn_to_label(urn)
        eq_(label, label2)
        eq_("Delegated account ID %s" % urn, label)


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


class TestAuthdataUtility(VendorIDTest):

    def setup(self):
        super(TestAuthdataUtility, self).setup()
        self.authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://my-library.org/",
            secret = "My library secret",
            other_libraries = {
                "http://your-library.org/": "Your library secret"
            }
        )

    def test_from_config(self):
        name = Configuration.ADOBE_VENDOR_ID_INTEGRATION

        # If there is no Adobe Vendor ID integration set up,
        # from_config() returns None.
        with temp_config() as config:
            config[Configuration.INTEGRATIONS] = {}
            eq_(None, AuthdataUtility.from_config())
            
        with self.temp_config() as config:
            # Test success
            utility = AuthdataUtility.from_config()
            eq_(self.TEST_VENDOR_ID, utility.vendor_id)
            eq_(self.TEST_LIBRARY_URI, utility.library_uri)
            eq_(self.TEST_SECRET, utility.secret)
            expect = dict(self.TEST_OTHERS)
            expect[self.TEST_LIBRARY_URI] = self.TEST_SECRET
            eq_(expect, utility.secrets_by_library)

            # If an integration is set up but incomplete, from_config
            # raises CannotLoadConfiguration.
            integration = config[Configuration.INTEGRATIONS][name]
            del integration[Configuration.ADOBE_VENDOR_ID]
            assert_raises(
                CannotLoadConfiguration, AuthdataUtility.from_config
            )
            integration[Configuration.ADOBE_VENDOR_ID] = self.TEST_VENDOR_ID

            del integration[AuthdataUtility.LIBRARY_URI_KEY]
            assert_raises(
                CannotLoadConfiguration, AuthdataUtility.from_config
            )
            integration[AuthdataUtility.LIBRARY_URI_KEY] = self.TEST_LIBRARY_URI

            del integration[AuthdataUtility.AUTHDATA_SECRET_KEY]
            assert_raises(
                CannotLoadConfiguration, AuthdataUtility.from_config
            )
            integration[AuthdataUtility.AUTHDATA_SECRET_KEY] = self.TEST_SECRET

            # If other libraries are not configured, that's fine.
            del integration[AuthdataUtility.OTHER_LIBRARIES_KEY]
            authdata = AuthdataUtility.from_config()
            eq_({self.TEST_LIBRARY_URI : self.TEST_SECRET}, authdata.secrets_by_library)

            
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

    # Tests of code that is used only in a migration script.  This can
    # be deleted once
    # 20161102-adobe-id-is-delegated-patron-identifier.py is run on
    # all affected instances.
    def test_migrate_adobe_id_noop(self):
        patron = self._patron()
        self.authdata.migrate_adobe_id(patron)

        # Since the patron has no adobe ID, nothing happens.
        eq_([], patron.credentials)
        eq_([], self._db.query(DelegatedPatronIdentifier).all())

    def test_migrate_adobe_id_success(self):
        from api.opds import CirculationManagerAnnotator
        patron = self._patron()

        # This patron has a Credential containing their Adobe ID
        data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        adobe_id = Credential(
            patron=patron, data_source=data_source,
            type=AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE,
            credential="My Adobe ID"
        )

        # Run the migration.
        new_credential, delegated_identifier = self.authdata.migrate_adobe_id(patron)
        
        # The patron now has _two_ Credentials -- the old one
        # containing the Adobe ID, and a new one.
        eq_(set([new_credential, adobe_id]), set(patron.credentials))

        # The new credential contains an anonymized patron identifier
        # used solely to connect the patron to their Adobe ID.
        eq_(CirculationManagerAnnotator.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            new_credential.type)

        # We can use that identifier to look up a DelegatedPatronIdentifier
        # 
        def explode():
            # This method won't be called because the
            # DelegatedPatronIdentifier already exists.
            raise Exception()
        identifier, is_new = DelegatedPatronIdentifier.get_one_or_create(
            self._db, self.authdata.library_uri, new_credential.credential,
            DelegatedPatronIdentifier.ADOBE_ACCOUNT_ID, explode
        )
        eq_(delegated_identifier, identifier)
        eq_(False, is_new)
        eq_("My Adobe ID", identifier.delegated_identifier)

        # An integration-level test:
        # AdobeVendorIDModel.to_delegated_patron_identifier_uuid works
        # now.
        model = AdobeVendorIDModel(self._db, None, None)
        uuid, label = model.to_delegated_patron_identifier_uuid(
            self.authdata.library_uri, new_credential.credential
        )
        eq_("My Adobe ID", uuid)
        eq_('Delegated account ID My Adobe ID', label)
        
        # If we run the migration again, nothing new happens.
        new_credential_2, delegated_identifier_2 = self.authdata.migrate_adobe_id(patron)
        eq_(new_credential, new_credential_2)
        eq_(delegated_identifier, delegated_identifier_2)
        eq_(2, len(patron.credentials))
        uuid, label = model.to_delegated_patron_identifier_uuid(
            self.authdata.library_uri, new_credential.credential
        )
        eq_("My Adobe ID", uuid)
        eq_('Delegated account ID My Adobe ID', label)
