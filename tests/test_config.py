from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

from collections import Counter
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
import json

from core.config import Configuration as CoreConfiguration
from core.model import (
    ConfigurationSetting
)
from . import DatabaseTest
from api.config import Configuration

class TestConfiguration(DatabaseTest):

    def test_key_pair(self):
        # Test the ability to create, replace, or look up a
        # public/private key pair in a ConfigurationSetting.
        setting = ConfigurationSetting.sitewide(
            self._db, Configuration.KEY_PAIR
        )
        setting.value = "nonsense"

        # If you pass in a ConfigurationSetting that is missing its
        # value, or whose value is not a public key pair, a new key
        # pair is created.
        public_key, private_key = Configuration.key_pair(setting)
        assert 'BEGIN PUBLIC KEY' in public_key
        assert 'BEGIN RSA PRIVATE KEY' in private_key
        eq_([public_key, private_key], setting.json_value)

        setting.value = None
        public_key, private_key = Configuration.key_pair(setting)
        assert 'BEGIN PUBLIC KEY' in public_key
        assert 'BEGIN RSA PRIVATE KEY' in private_key
        eq_([public_key, private_key], setting.json_value)

        # If the setting has a good value already, the key pair is
        # returned as is.
        new_public, new_private = Configuration.key_pair(setting)
        eq_(new_public, public_key)
        eq_(new_private, private_key)

    def test_cipher(self):
        """Test the cipher() helper method."""

        # Generate a public/private key pair.
        key = RSA.generate(2048)
        cipher = PKCS1_OAEP.new(key)
        public = key.publickey().exportKey()
        private = key.exportKey()

        # Pass the public key into cipher() to get something that can
        # encrypt.
        encryptor = Configuration.cipher(public)
        encrypted = encryptor.encrypt("some text")

        # Pass the private key into cipher() to get something that can
        # decrypt.
        decryptor = Configuration.cipher(private)
        decrypted = decryptor.decrypt(encrypted)
        eq_("some text", decrypted)

    def test_collection_language_method_performs_estimate(self):
        C = Configuration
        library = self._default_library

        # We haven't set any of these values.
        for key in [C.LARGE_COLLECTION_LANGUAGES,
                    C.SMALL_COLLECTION_LANGUAGES,
                    C.TINY_COLLECTION_LANGUAGES]:
            eq_(None, ConfigurationSetting.for_library(key, library).value)

        # So how does this happen?
        eq_(["eng"], C.large_collection_languages(library))
        eq_([], C.small_collection_languages(library))
        eq_([], C.tiny_collection_languages(library))

        # It happens because the first time we call one of those
        # *_collection_languages, it estimates values for all three
        # configuration settings, based on the library's current
        # holdings.
        large_setting = ConfigurationSetting.for_library(
            C.LARGE_COLLECTION_LANGUAGES, library
        )
        eq_(["eng"], large_setting.json_value)
        eq_([], ConfigurationSetting.for_library(
            C.SMALL_COLLECTION_LANGUAGES, library).json_value)
        eq_([], ConfigurationSetting.for_library(
            C.TINY_COLLECTION_LANGUAGES, library).json_value)

        # We can change these values.
        large_setting.value = json.dumps(["spa", "jpn"])
        eq_(["spa", "jpn"], C.large_collection_languages(library))

        # If we enter an invalid value, or a value that's not a list,
        # the estimate is re-calculated the next time we look.
        large_setting.value = "this isn't json"
        eq_(["eng"], C.large_collection_languages(library))

        large_setting.value = '"this is json but it\'s not a list"'
        eq_(["eng"], C.large_collection_languages(library))

    def test_estimate_language_collection_for_library(self):

        library = self._default_library

        # We thought we'd have big collections.
        old_settings = {
            Configuration.LARGE_COLLECTION_LANGUAGES : ["spa", "fre"],
            Configuration.SMALL_COLLECTION_LANGUAGES : ["chi"],
            Configuration.TINY_COLLECTION_LANGUAGES : ["rus"],
        }

        for key, value in list(old_settings.items()):
            ConfigurationSetting.for_library(
                key, library).value = json.dumps(value)

        # But there's nothing in our database, so when we call
        # Configuration.estimate_language_collections_for_library...
        Configuration.estimate_language_collections_for_library(library)

        # ...it gets reset to the default.
        eq_(["eng"], ConfigurationSetting.for_library(
            Configuration.LARGE_COLLECTION_LANGUAGES, library
        ).json_value)

        eq_([], ConfigurationSetting.for_library(
            Configuration.SMALL_COLLECTION_LANGUAGES, library
        ).json_value)

        eq_([], ConfigurationSetting.for_library(
            Configuration.TINY_COLLECTION_LANGUAGES, library
        ).json_value)

    def test_classify_holdings(self):

        m = Configuration.classify_holdings

        # If there are no titles in the collection at all, we assume
        # there will eventually be a large English collection.
        eq_([["eng"], [], []], m(Counter()))

        # The largest collection is given the 'large collection' treatment,
        # even if it's very small.
        very_small = Counter(rus=2, pol=1)
        eq_([["rus"], [], ["pol"]], m(very_small))

        # Otherwise, the classification of a collection depends on the
        # sheer number of items in that collection. Within a
        # classification, languages are ordered by holding size.
        different_sizes = Counter(jpn=16000, fre=20000, spa=8000,
                                  nav=6, ukr=4000, ira=1500)
        eq_([['fre', 'jpn'], ['spa', 'ukr', 'ira'], ['nav']],
            m(different_sizes))

    def test_max_outstanding_fines(self):
        m = Configuration.max_outstanding_fines

        # By default, fines are not enforced.
        eq_(None, m(self._default_library))

        # The maximum fine value is determined by this
        # ConfigurationSetting.
        setting = ConfigurationSetting.for_library(
            Configuration.MAX_OUTSTANDING_FINES,
            self._default_library
        )

        # Any amount of fines is too much.
        setting.value = "$0"
        max_fines = m(self._default_library)
        eq_(0, max_fines.amount)

        # A more lenient approach.
        setting.value = "100"
        max_fines = m(self._default_library)
        eq_(100, max_fines.amount)

    def test_default_opds_format(self):
        # Initializing the Configuration object modifies the corresponding
        # object in core, so that core code will behave appropriately.
        eq_(Configuration.DEFAULT_OPDS_FORMAT, CoreConfiguration.DEFAULT_OPDS_FORMAT)
