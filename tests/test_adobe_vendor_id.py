import base64
import json

import pytest

import jwt
from jwt.exceptions import (
    DecodeError,
    ExpiredSignatureError,
    InvalidIssuedAtError
)
import re
import datetime

from api.problem_details import *
from api.adobe_vendor_id import (
    AdobeSignInRequestParser,
    AdobeAccountInfoRequestParser,
    AdobeVendorIDController,
    AdobeVendorIDRequestHandler,
    AdobeVendorIDModel,
    AuthdataUtility,
    DeviceManagementRequestHandler,
)

from api.opds import CirculationManagerAnnotator
from api.testing import VendorIDTest

from core.model import (
    ConfigurationSetting,
    Credential,
    DataSource,
    DelegatedPatronIdentifier,
    ExternalIntegration,
    Library,
)
from core.util.problem_detail import ProblemDetail

from api.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from api.simple_authentication import SimpleAuthenticationProvider


class TestVendorIDModel(VendorIDTest):

    credentials = dict(username="validpatron", password="password")

    def setup_method(self):
        super(TestVendorIDModel, self).setup_method()

        # This library is going to act as the Vendor ID server.
        self.vendor_id_library = self._default_library
        # This library can create Short Client Tokens that the Vendor
        # ID server will recognize.
        self.short_client_token_library = self._library(
            short_name="shortclienttoken"
        )

        # Initialize the Adobe-specific ExternalIntegrations for both
        # libraries.
        self.initialize_adobe(
            self.vendor_id_library, [self.short_client_token_library]
        )

        # Set up a simple authentication provider that validates
        # one specific patron.
        integration = self._external_integration(self._str)
        provider = SimpleAuthenticationProvider
        integration.setting(provider.TEST_IDENTIFIER).value = "validpatron"
        integration.setting(provider.TEST_PASSWORD).value = "password"
        self.authenticator = SimpleAuthenticationProvider(
            self._default_library, integration
        )

        self.model = AdobeVendorIDModel(
            self._db, self._default_library, self.authenticator,
            self.TEST_NODE_VALUE
        )
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
        assert uuid == uuid2
        assert label == label2

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
        uuid, label = self.model.uuid_and_label(self.bob_patron)
        assert "A dummy value" == uuid
        assert "Delegated account ID A dummy value" == label

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
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
        assert ("A dummy value" ==
            bob_delegated_patron_identifier.delegated_identifier)

        # If the DelegatedPatronIdentifier and the Credential
        # have different values, the DelegatedPatronIdentifier wins.
        old_style_credential.credential = "A different value."
        uuid, label = self.model.uuid_and_label(self.bob_patron)
        assert "A dummy value" == uuid

        # We can even delete the old-style Credential, and
        # uuid_and_label will still give the value that was stored in
        # it.
        self._db.delete(old_style_credential)
        self._db.commit()
        uuid, label = self.model.uuid_and_label(self.bob_patron)
        assert "A dummy value" == uuid


    def test_create_authdata(self):
        credential = self.model.create_authdata(self.bob_patron)

        # There's now a persistent token associated with Bob's
        # patron account, and that's the token returned by create_authdata()
        bob_authdata = Credential.lookup(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron, None)
        assert credential.credential == bob_authdata.credential

    def test_to_delegated_patron_identifier_uuid(self):

        foreign_uri = "http://your-library/"
        foreign_identifier = "foreign ID"

        # Pass in nothing and you get nothing.
        assert ((None, None) ==
            self.model.to_delegated_patron_identifier_uuid(foreign_uri, None))
        assert ((None, None) ==
            self.model.to_delegated_patron_identifier_uuid(
                None, foreign_identifier
            ))

        # Pass in a URI and identifier and you get a UUID and a label.
        uuid, label = self.model.to_delegated_patron_identifier_uuid(
            foreign_uri, foreign_identifier
        )

        # We can't test a specific value for the UUID but we can test the label.
        assert "Delegated account ID " + uuid == label

        # And we can verify that a DelegatedPatronIdentifier was
        # created for the URI+identifier, and that it contains the
        # UUID.
        [dpi] = self._db.query(DelegatedPatronIdentifier).filter(
            DelegatedPatronIdentifier.library_uri==foreign_uri).filter(
            DelegatedPatronIdentifier.patron_identifier==foreign_identifier
        ).all()
        assert uuid == dpi.delegated_identifier

    def test_authdata_lookup_delegated_patron_identifier_success(self):
        """Test that one library can perform an authdata lookup on a JWT
        generated by a different library.
        """
        # Here's a library that's not a Vendor ID server, but which
        # can generate a JWT for one of its patrons.
        sct_library = self.short_client_token_library
        utility = AuthdataUtility.from_config(sct_library)
        vendor_id, jwt = utility.encode("Foreign patron")

        # Here's an AuthdataUtility for the library that _is_
        # a Vendor ID server.
        vendor_id_utility = AuthdataUtility.from_config(self.vendor_id_library)

        # The Vendor ID library knows the secret it shares with the
        # other library -- initialize_adobe() took care of that.
        sct_library_uri = sct_library.setting(Configuration.WEBSITE_URL).value
        assert ("%s token secret" % sct_library.short_name ==
            vendor_id_utility.secrets_by_library_uri[sct_library_uri])

        # Because this library shares the other library's secret,
        # it can decode a JWT issued by the other library, and
        # issue an Adobe ID (UUID).
        uuid, label = self.model.authdata_lookup(jwt)

        # We get the same result if we smuggle the JWT into
        # a username/password lookup as the username.
        uuid2, label2 = self.model.standard_lookup(dict(username=jwt))
        assert uuid2 == uuid
        assert label2 == label

        # The UUID corresponds to a DelegatedPatronIdentifier,
        # associated with the foreign library and the patron
        # identifier that library encoded in its JWT.
        [dpi] = self._db.query(DelegatedPatronIdentifier).filter(
            DelegatedPatronIdentifier.library_uri==sct_library_uri).filter(
                DelegatedPatronIdentifier.patron_identifier=="Foreign patron"
            ).all()
        assert uuid == dpi.delegated_identifier
        assert "Delegated account ID %s" % uuid == label

    def test_short_client_token_lookup_delegated_patron_identifier_success(self):
        """Test that one library can perform an authdata lookup on a short
        client token generated by a different library.
        """
        # Here's a library that's not a Vendor ID server, but which can
        # generate a Short Client Token for one of its patrons.
        sct_library = self.short_client_token_library
        utility = AuthdataUtility.from_config(sct_library)
        vendor_id, short_client_token = utility.encode_short_client_token(
            "Foreign patron"
        )

        # Here's an AuthdataUtility for the library that _is_
        # a Vendor ID server.
        vendor_id_utility = AuthdataUtility.from_config(self.vendor_id_library)

        # The Vendor ID library knows the secret it shares with the
        # other library -- initialize_adobe() took care of that.
        sct_library_url = sct_library.setting(Configuration.WEBSITE_URL).value
        assert ("%s token secret" % sct_library.short_name ==
            vendor_id_utility.secrets_by_library_uri[sct_library_url])

        # Because the Vendor ID library shares the Short Client Token
        # library's secret, it can decode a short client token issued
        # by that library, and issue an Adobe ID (UUID).
        token, signature = short_client_token.rsplit("|", 1)
        uuid, label = self.model.short_client_token_lookup(
            token, signature
        )

        # The UUID corresponds to a DelegatedPatronIdentifier,
        # associated with the foreign library and the patron
        # identifier that library encoded in its JWT.
        [dpi] = self._db.query(DelegatedPatronIdentifier).filter(
            DelegatedPatronIdentifier.library_uri==sct_library_url).filter(
                DelegatedPatronIdentifier.patron_identifier=="Foreign patron"
            ).all()
        assert uuid == dpi.delegated_identifier
        assert "Delegated account ID %s" % uuid == label

        # We get the same UUID and label by passing the token and
        # signature to standard_lookup as username and password.
        # (That's because standard_lookup calls short_client_token_lookup
        # behind the scenes.)
        credentials = dict(username=token, password=signature)
        new_uuid, new_label = self.model.standard_lookup(credentials)
        assert new_uuid == uuid
        assert new_label == label

    def test_short_client_token_lookup_delegated_patron_identifier_failure(self):
        uuid, label = self.model.short_client_token_lookup(
            "bad token", "bad signature"
        )
        assert None == uuid
        assert None == label

    def test_username_password_lookup_success(self):
        urn, label = self.model.standard_lookup(self.credentials)

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
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

        assert "Delegated account ID %s" % urn == label
        assert urn == bob_delegated_patron_identifier.delegated_identifier
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
        assert None == token.expires

        # Use that token to perform a lookup of Bob's Adobe Vendor ID
        # UUID.
        urn, label = self.model.authdata_lookup(token.credential)

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
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
        assert urn == bob_delegated_patron_identifier.delegated_identifier

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
        urn, label = self.model.standard_lookup(
            dict(username=token.credential)
        )

        # There is now an anonymized identifier associated with Bob's
        # patron account.
        internal = DataSource.lookup(self._db, DataSource.INTERNAL_PROCESSING)
        bob_anonymized_identifier = Credential.lookup(
            self._db, internal,
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
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
        assert urn == bob_delegated_patron_identifier.delegated_identifier

        # A future attempt to authenticate with the token will succeed.
        urn, label = self.model.standard_lookup(
            dict(username=token.credential)
        )
        assert urn == bob_delegated_patron_identifier.delegated_identifier

    def test_authdata_lookup_failure_no_token(self):
        urn, label = self.model.authdata_lookup("nosuchauthdata")
        assert None == urn
        assert None == label

    def test_authdata_lookup_failure_wrong_token(self):
        # Bob has an authdata token.
        token, ignore = Credential.persistent_token_create(
            self._db, self.data_source, self.model.AUTHDATA_TOKEN_TYPE,
            self.bob_patron
        )

        # But we look up a different token and get nothing.
        urn, label = self.model.authdata_lookup("nosuchauthdata")
        assert None == urn
        assert None == label

    def test_urn_to_label_success(self):
        urn, label = self.model.standard_lookup(self.credentials)
        label2 = self.model.urn_to_label(urn)
        assert label == label2
        assert "Delegated account ID %s" % urn == label


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
        assert {'username': 'Vendor username',
             'password': 'Vendor password', 'method': 'standard'} == data

    def test_authdata_sign_in_request(self):
        parser = AdobeSignInRequestParser()
        data = parser.process(self.authdata_sign_in_request)
        assert ({'authData': 'this data was base64 encoded', 'method': 'authData'} ==
            data)

    def test_accountinfo_request(self):
        parser = AdobeAccountInfoRequestParser()
        data = parser.process(self.accountinfo_request)
        assert ({'method': 'standard',
             'user': 'urn:uuid:0xxxxxxx-xxxx-1xxx-xxxx-yyyyyyyyyyyy'} ==
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
        assert '<error xmlns="http://ns.adobe.com/adept" data="E_1045_VENDORID Some random error"/>' == doc

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
        assert '<error xmlns="http://ns.adobe.com/adept" data="E_1045_AUTH Incorrect barcode or PIN."/>' == result

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
        assert '<error xmlns="http://ns.adobe.com/adept" data="E_1045_AUTH Incorrect token."/>' == result

    def test_failure_send_login_request_to_accountinfo(self):
        doc = self.authdata_sign_in_request % dict(
            authdata=base64.b64encode("incorrect"))
        result = self._handler.handle_accountinfo_request(
            doc, self._userinfo)
        assert '<error xmlns="http://ns.adobe.com/adept" data="E_1045_ACCOUNT_INFO Request document in wrong format."/>' == result

    def test_failure_send_accountinfo_request_to_login(self):
        doc = self.accountinfo_request % dict(
            uuid=self.user1_uuid)
        result = self._handler.handle_signin_request(
            doc, self._standard_login, self._authdata_login)
        assert '<error xmlns="http://ns.adobe.com/adept" data="E_1045_AUTH Request document in wrong format."/>' == result

    def test_handle_accountinfo_success(self):
        doc = self.accountinfo_request % dict(
            uuid=self.user1_uuid)
        result = self._handler.handle_accountinfo_request(
            doc, self._userinfo)
        assert '<accountInfoResponse xmlns="http://ns.adobe.com/adept">\n<label>Human-readable label for user1</label>\n</accountInfoResponse>' == result

    def test_handle_accountinfo_failure(self):
        doc = self.accountinfo_request % dict(
            uuid="not the uuid")
        result = self._handler.handle_accountinfo_request(
            doc, self._userinfo)
        assert '<error xmlns="http://ns.adobe.com/adept" data="E_1045_ACCOUNT_INFO Could not identify patron from \'not the uuid\'."/>' == result


class TestAuthdataUtility(VendorIDTest):

    def setup_method(self):
        super(TestAuthdataUtility, self).setup_method()
        self.authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://my-library.org/",
            library_short_name = "MyLibrary",
            secret = "My library secret",
            other_libraries = {
                "http://your-library.org/": ("you", "Your library secret")
            },
        )

    def test_from_config(self):
        library = self._default_library
        library2 = self._library()
        self.initialize_adobe(library, [library2])
        library_url = library.setting(Configuration.WEBSITE_URL).value
        library2_url = library2.setting(Configuration.WEBSITE_URL).value

        utility = AuthdataUtility.from_config(library)

        registry = ExternalIntegration.lookup(
            self._db, ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL, library=library
        )
        assert (library.short_name + "token" ==
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.USERNAME, library, registry).value)
        assert (library.short_name + " token secret" ==
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.PASSWORD, library, registry).value)

        assert self.TEST_VENDOR_ID == utility.vendor_id
        assert library_url == utility.library_uri
        assert (
            {library2_url : "%s token secret" % library2.short_name,
             library_url : "%s token secret" % library.short_name} ==
            utility.secrets_by_library_uri)

        assert (
            {"%sTOKEN" % library.short_name.upper() : library_url,
             "%sTOKEN" % library2.short_name.upper() : library2_url } ==
            utility.library_uris_by_short_name)

        # If the Library object is disconnected from its database
        # session, as may happen in production...
        self._db.expunge(library)

        # Then an attempt to use it to get an AuthdataUtility
        # will fail...
        with pytest.raises(ValueError) as excinfo:
            AuthdataUtility.from_config(library)
        assert "No database connection provided and could not derive one from Library object!" in str(excinfo.value)

        # ...unless a database session is provided in the constructor.
        authdata = AuthdataUtility.from_config(library, self._db)
        assert (
            {"%sTOKEN" % library.short_name.upper() : library_url,
             "%sTOKEN" % library2.short_name.upper() : library2_url } ==
            authdata.library_uris_by_short_name)
        library = self._db.merge(library)
        self._db.commit()

        # If an integration is set up but incomplete, from_config
        # raises CannotLoadConfiguration.
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, ExternalIntegration.USERNAME, library, registry)
        old_short_name = setting.value
        setting.value = None
        pytest.raises(
            CannotLoadConfiguration, AuthdataUtility.from_config,
            library
        )
        setting.value = old_short_name

        setting = library.setting(Configuration.WEBSITE_URL)
        old_value = setting.value
        setting.value = None
        pytest.raises(
            CannotLoadConfiguration, AuthdataUtility.from_config, library
        )
        setting.value = old_value

        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, ExternalIntegration.PASSWORD, library, registry)
        old_secret = setting.value
        setting.value = None
        pytest.raises(
            CannotLoadConfiguration, AuthdataUtility.from_config, library
        )
        setting.value = old_secret

        # If other libraries are not configured, that's fine. We'll
        # only have a configuration for ourselves.
        self.adobe_vendor_id.set_setting(
            AuthdataUtility.OTHER_LIBRARIES_KEY, None
        )
        authdata = AuthdataUtility.from_config(library)
        assert ({library_url : "%s token secret" % library.short_name} ==
            authdata.secrets_by_library_uri)
        assert ({"%sTOKEN" % library.short_name.upper(): library_url} ==
            authdata.library_uris_by_short_name)

        # Short library names are case-insensitive. If the
        # configuration has the same library short name twice, you
        # can't create an AuthdataUtility.
        self.adobe_vendor_id.set_setting(
            AuthdataUtility.OTHER_LIBRARIES_KEY,
            json.dumps({
                "http://a/" : ("a", "secret1"),
                "http://b/" : ("A", "secret2"),
            })
        )
        pytest.raises(ValueError, AuthdataUtility.from_config, library)

        # If there is no Adobe Vendor ID integration set up,
        # from_config() returns None.
        self._db.delete(registry)
        assert None == AuthdataUtility.from_config(library)

    def test_short_client_token_for_patron(self):
        class MockAuthdataUtility(AuthdataUtility):
            def __init__(self):
                pass
            def encode_short_client_token(self, patron_identifier):
                self.encode_sct_called_with = patron_identifier
                return "a", "b"
            def _adobe_patron_identifier(self, patron_information):
                self.patron_identifier_called_with = patron_information
                return "patron identifier"
        # A patron is passed in; we get their identifier for Adobe ID purposes,
        # and generate a short client token based on it
        patron = self._patron()
        authdata = MockAuthdataUtility()
        sct = authdata.short_client_token_for_patron(patron)
        assert patron == authdata.patron_identifier_called_with
        assert authdata.encode_sct_called_with == "patron identifier"
        assert sct == ("a", "b")
        # The identifier for Adobe ID purposes is passed in, and we use it directly.
        authdata.short_client_token_for_patron("identifier for Adobe ID purposes")
        assert sct == ("a", "b")
        assert authdata.encode_sct_called_with == "identifier for Adobe ID purposes"

    def test_decode_round_trip(self):
        patron_identifier = "Patron identifier"
        vendor_id, authdata = self.authdata.encode(patron_identifier)
        assert "The Vendor ID" == vendor_id

        # We can decode the authdata with our secret.
        decoded = self.authdata.decode(authdata)
        assert ("http://my-library.org/", "Patron identifier") == decoded

    def test_decode_round_trip_with_intermediate_mischief(self):
        patron_identifier = "Patron identifier"
        vendor_id, authdata = self.authdata.encode(patron_identifier)
        assert "The Vendor ID" == vendor_id

        # A mischievious party in the middle decodes our authdata
        # without telling us.
        authdata = base64.decodestring(authdata)

        # But it still works.
        decoded = self.authdata.decode(authdata)
        assert ("http://my-library.org/", "Patron identifier") == decoded

    def test_encode(self):
        # Test that _encode gives a known value with known input.
        patron_identifier = "Patron identifier"
        now = datetime.datetime(2016, 1, 1, 12, 0, 0)
        expires = datetime.datetime(2018, 1, 1, 12, 0, 0)
        authdata = self.authdata._encode(
            self.authdata.library_uri, patron_identifier, now, expires
        )
        assert (
            base64.encodestring('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vbXktbGlicmFyeS5vcmcvIiwiaWF0IjoxNDUxNjQ5NjAwLjAsInN1YiI6IlBhdHJvbiBpZGVudGlmaWVyIiwiZXhwIjoxNTE0ODA4MDAwLjB9.n7VRVv3gIyLmNxTzNRTEfCdjoky0T0a1Jhehcag1oQw') ==
            authdata)

    def test_decode_from_another_library(self):

        # Here's the AuthdataUtility used by another library.
        foreign_authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://your-library.org/",
            library_short_name = "you",
            secret = "Your library secret",
        )

        patron_identifier = "Patron identifier"
        vendor_id, authdata = foreign_authdata.encode(patron_identifier)

        # Because we know the other library's secret, we're able to
        # decode the authdata.
        decoded = self.authdata.decode(authdata)
        assert ("http://your-library.org/", "Patron identifier") == decoded

        # If our secret doesn't match the other library's secret,
        # we can't decode the authdata
        foreign_authdata.secret = 'A new secret'
        vendor_id, authdata = foreign_authdata.encode(patron_identifier)
        with pytest.raises(DecodeError) as excinfo:
            self.authdata.decode(authdata)
        assert "Signature verification failed" in str(excinfo.value)

    def test_decode_from_unknown_library_fails(self):

        # Here's the AuthdataUtility used by a library we don't know
        # about.
        foreign_authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://some-other-library.org/",
            library_short_name = "SomeOther",
            secret = "Some other library secret",
        )
        vendor_id, authdata = foreign_authdata.encode("A patron")
        # They can encode, but we cna't decode.
        with pytest.raises(DecodeError) as excinfo:
            self.authdata.decode(authdata)
        assert "Unknown library: http://some-other-library.org/" in str(excinfo.value)

    def test_cannot_decode_token_from_future(self):
        future = datetime.datetime.utcnow() + datetime.timedelta(days=365)
        authdata = self.authdata._encode(
            "Patron identifier", iat=future
        )
        pytest.raises(
            InvalidIssuedAtError, self.authdata.decode, authdata
        )

    def test_cannot_decode_expired_token(self):
        expires = datetime.datetime(2016, 1, 1, 12, 0, 0)
        authdata = self.authdata._encode(
            "Patron identifier", exp=expires
        )
        pytest.raises(
            ExpiredSignatureError, self.authdata.decode, authdata
        )

    def test_cannot_encode_null_patron_identifier(self):
        with pytest.raises(ValueError) as excinfo:
            self.authdata.encode(None)
        assert "No patron identifier specified" in str(excinfo.value)

    def test_cannot_decode_null_patron_identifier(self):

        authdata = self.authdata._encode(
            self.authdata.library_uri, None,
        )
        with pytest.raises(DecodeError) as excinfo:
            self.authdata.decode(authdata)
        assert "No subject specified" in str(excinfo.value)

    def test_short_client_token_round_trip(self):
        # Encoding a token and immediately decoding it gives the expected
        # result.
        vendor_id, token = self.authdata.encode_short_client_token("a patron")
        assert self.authdata.vendor_id == vendor_id

        library_uri, patron = self.authdata.decode_short_client_token(token)
        assert self.authdata.library_uri == library_uri
        assert "a patron" == patron

    def test_short_client_token_encode_known_value(self):
        # Verify that the encoding algorithm gives a known value on known
        # input.
        value = self.authdata._encode_short_client_token(
            "a library", "a patron identifier", 1234.5
        )

        # Note the colon characters that replaced the plus signs in
        # what would otherwise be normal base64 text. Similarly for
        # the semicolon which replaced the slash, and the at sign which
        # replaced the equals sign.
        assert ('a library|1234.5|a patron identifier|YoNGn7f38mF531KSWJ;o1H0Z3chbC:uTE:t7pAwqYxM@' ==
            value)

        # Dissect the known value to show how it works.
        token, signature = value.rsplit("|", 1)

        # Signature is base64-encoded in a custom way that avoids
        # triggering an Adobe bug ; token is not.
        signature = AuthdataUtility.adobe_base64_decode(signature)

        # The token comes from the library name, the patron identifier,
        # and the time of creation.
        assert "a library|1234.5|a patron identifier" == token

        # The signature comes from signing the token with the
        # secret associated with this library.
        expect_signature = self.authdata.short_token_signer.sign(
            token, self.authdata.short_token_signing_key
        )
        assert expect_signature == signature

    def test_decode_short_client_token_from_another_library(self):
        # Here's the AuthdataUtility used by another library.
        foreign_authdata = AuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://your-library.org/",
            library_short_name = "you",
            secret = "Your library secret",
        )

        patron_identifier = "Patron identifier"
        vendor_id, token = foreign_authdata.encode_short_client_token(
            patron_identifier
        )

        # Because we know the other library's secret, we're able to
        # decode the authdata.
        decoded = self.authdata.decode_short_client_token(token)
        assert ("http://your-library.org/", "Patron identifier") == decoded

        # If our secret for a library doesn't match the other
        # library's short token signing key, we can't decode the
        # authdata.
        foreign_authdata.short_token_signing_key = 'A new secret'
        vendor_id, token = foreign_authdata.encode_short_client_token(
            patron_identifier
        )
        with pytest.raises(ValueError) as excinfo:
            self.authdata.decode_short_client_token(token)
        assert "Invalid signature for" in str(excinfo.value)

    def test_decode_client_token_errors(self):
        # Test various token errors
        m = self.authdata._decode_short_client_token

        # A token has to contain at least two pipe characters.
        with pytest.raises(ValueError) as excinfo:
            m("foo|", "signature")
        assert "Invalid client token" in str(excinfo.value)

        # The expiration time must be numeric.
        with pytest.raises(ValueError) as excinfo:
            m("library|a time|patron", "signature")
        assert 'Expiration time "a time" is not numeric' in str(excinfo.value)

        # The patron identifier must not be blank.
        with pytest.raises(ValueError) as excinfo:
            m("library|1234|", "signature")
        assert 'Token library|1234| has empty patron identifier' in str(excinfo.value)

        # The library must be a known one.
        with pytest.raises(ValueError) as excinfo:
            m("library|1234|patron", "signature")
        assert 'I don\'t know how to handle tokens from library "LIBRARY"' in str(excinfo.value)

        # We must have the shared secret for the given library.
        self.authdata.library_uris_by_short_name['LIBRARY'] = 'http://a-library.com/'
        with pytest.raises(ValueError) as excinfo:
            m("library|1234|patron", "signature")
        assert 'I don\'t know the secret for library http://a-library.com/' in str(excinfo.value)

        # The token must not have expired.
        with pytest.raises(ValueError) as excinfo:
            m("mylibrary|1234|patron", "signature")
        assert 'Token mylibrary|1234|patron expired at 1970-01-01 00:20:34' in str(excinfo.value)

        # Finally, the signature must be valid.
        with pytest.raises(ValueError) as excinfo:
            m("mylibrary|99999999999|patron", "signature")
        assert 'Invalid signature for' in str(excinfo.value)

    def test_adobe_base64_encode_decode(self):
        # Test our special variant of base64 encoding designed to avoid
        # triggering an Adobe bug.
        value = "!\tFN6~'Es52?X!#)Z*_S"

        encoded = AuthdataUtility.adobe_base64_encode(value)
        assert 'IQlGTjZ:J0VzNTI;WCEjKVoqX1M@' == encoded

        # This is like normal base64 encoding, but with a colon
        # replacing the plus character, a semicolon replacing the
        # slash, an at sign replacing the equal sign and the final
        # newline stripped.
        assert (
            encoded.replace(":", "+").replace(";", "/").replace("@", "=") + "\n" ==
            base64.encodestring(value))

        # We can reverse the encoding to get the original value.
        assert value == AuthdataUtility.adobe_base64_decode(encoded)

    def test__encode_short_client_token_uses_adobe_base64_encoding(self):
        class MockSigner(object):
            def sign(self, value, key):
                """Always return the same signature, crafted to contain a
                plus sign, a slash and an equal sign when base64-encoded.
                """
                return "!\tFN6~'Es52?X!#)Z*_S"
        self.authdata.short_token_signer = MockSigner()
        token = self.authdata._encode_short_client_token("lib", "1234", 0)

        # The signature part of the token has been encoded with our
        # custom encoding, not vanilla base64.
        assert 'lib|0|1234|IQlGTjZ:J0VzNTI;WCEjKVoqX1M@' == token

    def test_decode_two_part_short_client_token_uses_adobe_base64_encoding(self):

        # The base64 encoding of this signature has a plus sign in it.
        signature = 'LbU}66%\\-4zt>R>_)\n2Q'
        encoded_signature = AuthdataUtility.adobe_base64_encode(signature)

        # We replace the plus sign with a colon.
        assert ':' in encoded_signature
        assert '+' not in encoded_signature

        # Make sure that decode_two_part_short_client_token properly
        # reverses that change when decoding the 'password'.
        class MockAuthdataUtility(AuthdataUtility):
            def _decode_short_client_token(self, token, supposed_signature):
                assert supposed_signature == signature
                self.test_code_ran = True

        utility =  MockAuthdataUtility(
            vendor_id = "The Vendor ID",
            library_uri = "http://your-library.org/",
            library_short_name = "you",
            secret = "Your library secret",
        )
        utility.test_code_ran = False
        utility.decode_two_part_short_client_token(
            "username", encoded_signature
        )

        # The code in _decode_short_client_token ran. Since there was no
        # test failure, it ran successfully.
        assert True == utility.test_code_ran


    # Tests of code that is used only in a migration script.  This can
    # be deleted once
    # 20161102-adobe-id-is-delegated-patron-identifier.py is run on
    # all affected instances.
    def test_migrate_adobe_id_noop(self):
        patron = self._patron()
        self.authdata.migrate_adobe_id(patron)

        # Since the patron has no adobe ID, nothing happens.
        assert [] == patron.credentials
        assert [] == self._db.query(DelegatedPatronIdentifier).all()

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
        assert set([new_credential, adobe_id]) == set(patron.credentials)

        # The new credential contains an anonymized patron identifier
        # used solely to connect the patron to their Adobe ID.
        assert (AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER ==
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
        assert delegated_identifier == identifier
        assert False == is_new
        assert "My Adobe ID" == identifier.delegated_identifier

        # An integration-level test:
        # AdobeVendorIDModel.to_delegated_patron_identifier_uuid works
        # now.
        model = AdobeVendorIDModel(self._db, self._default_library, None, None)
        uuid, label = model.to_delegated_patron_identifier_uuid(
            self.authdata.library_uri, new_credential.credential
        )
        assert "My Adobe ID" == uuid
        assert 'Delegated account ID My Adobe ID' == label

        # If we run the migration again, nothing new happens.
        new_credential_2, delegated_identifier_2 = self.authdata.migrate_adobe_id(patron)
        assert new_credential == new_credential_2
        assert delegated_identifier == delegated_identifier_2
        assert 2 == len(patron.credentials)
        uuid, label = model.to_delegated_patron_identifier_uuid(
            self.authdata.library_uri, new_credential.credential
        )
        assert "My Adobe ID" == uuid
        assert 'Delegated account ID My Adobe ID' == label


class TestDeviceManagementRequestHandler(VendorIDTest):

    def test_register_drm_device_identifier(self):
        credential = self._credential()
        handler = DeviceManagementRequestHandler(credential)
        handler.register_device("device1")
        assert (
            ['device1'] ==
            [x.device_identifier for x in credential.drm_device_identifiers])

    def test_register_drm_device_identifier_does_nothing_on_no_input(self):
        credential = self._credential()
        handler = DeviceManagementRequestHandler(credential)
        handler.register_device("")
        assert [] == credential.drm_device_identifiers

    def test_register_drm_device_identifier_failure(self):
        """You can only register one device in a single call."""
        credential = self._credential()
        handler = DeviceManagementRequestHandler(credential)
        result = handler.register_device("device1\ndevice2")
        assert isinstance(result, ProblemDetail)
        assert PAYLOAD_TOO_LARGE.uri == result.uri
        assert [] == credential.drm_device_identifiers

    def test_deregister_drm_device_identifier(self):
        credential = self._credential()
        credential.register_drm_device_identifier("foo")
        handler = DeviceManagementRequestHandler(credential)

        result = handler.deregister_device("foo")
        assert "Success" == result
        assert [] == credential.drm_device_identifiers

        # Deregistration is idempotent.
        result = handler.deregister_device("foo")
        assert "Success" == result
        assert [] == credential.drm_device_identifiers

    def test_device_list(self):
        credential = self._credential()
        credential.register_drm_device_identifier("foo")
        credential.register_drm_device_identifier("bar")
        handler = DeviceManagementRequestHandler(credential)
        # Device IDs are sorted alphabetically.
        assert "bar\nfoo" == handler.device_list()


class TestAdobeVendorIDController(VendorIDTest):

    def test_create_authdata_handler(self):

        controller = AdobeVendorIDController(
            self._db, self._default_library, self.TEST_VENDOR_ID,
            self.TEST_NODE_VALUE, object()
        )
        patron = self._patron()
        response = controller.create_authdata_handler(patron)

        # An authdata was created.
        assert 200 == response.status_code

        # The authdata returned is the one stored as a Credential
        # for the Patron.
        [credential] = patron.credentials
        assert credential.credential == response.get_data(as_text=True)
