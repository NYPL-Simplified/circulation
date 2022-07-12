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
from api.util.short_client_token import ShortClientTokenUtility

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
from core.util.datetime_helpers import (
    datetime_utc,
    utc_now,
)
from core.util.problem_detail import ProblemDetail
import base64

from api.config import (
    CannotLoadConfiguration,
    Configuration,
    temp_config,
)

from api.simple_authentication import SimpleAuthenticationProvider



class TestShortClientTokenUtility(VendorIDTest):

    def setup_method(self):
        super(TestShortClientTokenUtility, self).setup_method()
        self.authdata = ShortClientTokenUtility(
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

        utility = ShortClientTokenUtility.from_config(library)

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

        # Then an attempt to use it to get an ShortClientTokenUtility
        # will fail...
        with pytest.raises(ValueError) as excinfo:
            ShortClientTokenUtility.from_config(library)
        assert "No database connection provided and could not derive one from Library object!" in str(excinfo.value)

        # ...unless a database session is provided in the constructor.
        authdata = ShortClientTokenUtility.from_config(library, self._db)
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
            CannotLoadConfiguration, ShortClientTokenUtility.from_config,
            library
        )
        setting.value = old_short_name

        setting = library.setting(Configuration.WEBSITE_URL)
        old_value = setting.value
        setting.value = None
        pytest.raises(
            CannotLoadConfiguration, ShortClientTokenUtility.from_config, library
        )
        setting.value = old_value

        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, ExternalIntegration.PASSWORD, library, registry)
        old_secret = setting.value
        setting.value = None
        pytest.raises(
            CannotLoadConfiguration, ShortClientTokenUtility.from_config, library
        )
        setting.value = old_secret

        # If other libraries are not configured, that's fine. We'll
        # only have a configuration for ourselves.
        self.adobe_vendor_id.set_setting(
            ShortClientTokenUtility.OTHER_LIBRARIES_KEY, None
        )
        authdata = ShortClientTokenUtility.from_config(library)
        assert ({library_url : "%s token secret" % library.short_name} ==
            authdata.secrets_by_library_uri)
        assert ({"%sTOKEN" % library.short_name.upper(): library_url} ==
            authdata.library_uris_by_short_name)

        # Short library names are case-insensitive. If the
        # configuration has the same library short name twice, you
        # can't create a ShortClientTokenUtility.
        self.adobe_vendor_id.set_setting(
            ShortClientTokenUtility.OTHER_LIBRARIES_KEY,
            json.dumps({
                "http://a/" : ("a", "secret1"),
                "http://b/" : ("A", "secret2"),
            })
        )
        pytest.raises(ValueError, ShortClientTokenUtility.from_config, library)

        # If there is no Adobe Vendor ID integration set up,
        # from_config() returns None.
        self._db.delete(registry)
        assert None == ShortClientTokenUtility.from_config(library)

    def test_short_client_token_for_patron(self):
        class MockShortClientTokenUtility(ShortClientTokenUtility):
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
        authdata = MockShortClientTokenUtility()
        sct = authdata.short_client_token_for_patron(patron)
        assert patron == authdata.patron_identifier_called_with
        assert authdata.encode_sct_called_with == "patron identifier"
        assert sct == ("a", "b")
        # The identifier for Adobe ID purposes is passed in, and we use it directly.
        authdata.short_client_token_for_patron("identifier for Adobe ID purposes")
        assert sct == ("a", "b")
        assert authdata.encode_sct_called_with == "identifier for Adobe ID purposes"

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
        signature = ShortClientTokenUtility.adobe_base64_decode(signature)

        # The token comes from the library name, the patron identifier,
        # and the time of creation.
        assert "a library|1234.5|a patron identifier" == token

        # The signature comes from signing the token with the
        # secret associated with this library.
        expect_signature = self.authdata.short_token_signer.sign(
            token.encode("utf-8"), self.authdata.short_token_signing_key
        )
        assert expect_signature == signature

    def test_decode_short_client_token_from_another_library(self):
        # Here's the ShortClientTokenUtility used by another library.
        foreign_authdata = ShortClientTokenUtility(
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
        foreign_authdata.short_token_signing_key = b'A new secret'
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

        encoded = ShortClientTokenUtility.adobe_base64_encode(value)
        assert 'IQlGTjZ:J0VzNTI;WCEjKVoqX1M@' == encoded

        # This is like normal base64 encoding, but with a colon
        # replacing the plus character, a semicolon replacing the
        # slash, an at sign replacing the equal sign and the final
        # newline stripped.
        assert (
            encoded.replace(":", "+").replace(";", "/").replace("@", "=") + "\n" ==
            base64.encodebytes(value.encode("utf-8")).decode("utf-8"))

        # We can reverse the encoding to get the original value.
        assert value == ShortClientTokenUtility.adobe_base64_decode(encoded).decode("utf-8")

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
        encoded_signature = ShortClientTokenUtility.adobe_base64_encode(signature)

        # We replace the plus sign with a colon.
        assert ':' in encoded_signature
        assert '+' not in encoded_signature

        # Make sure that decode_two_part_short_client_token properly
        # reverses that change when decoding the 'password'.
        class MockShortClientTokenUtility(ShortClientTokenUtility):
            def _decode_short_client_token(self, token, supposed_signature):
                assert supposed_signature.decode("utf-8") == signature
                self.test_code_ran = True

        utility =  MockShortClientTokenUtility(
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
