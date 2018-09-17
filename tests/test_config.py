from collections import Counter
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
import json

from core.model import (
    ConfigurationSetting
)
from . import DatabaseTest
from api.config import Configuration

class TestConfiguration(DatabaseTest):

    def test_key_pair(self):
        public = ConfigurationSetting.sitewide(
            self._db, Configuration.PUBLIC_KEY
        )
        public.value = "old value"
        private = ConfigurationSetting.sitewide(
            self._db, Configuration.PRIVATE_KEY
        )
        irrelevant = ConfigurationSetting.sitewide(
            self._db, "irrelevant"
        )
        irrelevant.value = "blue"

        # If you pass in a public-private key pair, and at least one of the
        # key pair is missing, a new key pair is created.
        public_key, private_key = Configuration.key_pair(public, private)
        assert 'BEGIN PUBLIC KEY' in public_key
        assert 'BEGIN RSA PRIVATE KEY' in private_key
        eq_(public_key, public.value)
        eq_(private_key, private.value)

        # If the key pair is intact, it is returned as is.
        public.value = "public 1"
        private.value = "private 2"
        eq_(("public 1", "private 2"), Configuration.key_pair(public, private))

        # If you mistake public for private, or pass in a clearly
        # irrelevant setting, you get a ValueError.
        assert_raises_regexp(
            ValueError,
            'Incorrect ConfigurationSetting "public-key" passed in as private key!',
            Configuration.key_pair, public, public
        )

        assert_raises_regexp(
            ValueError,
            '"private-key" passed in as public key!',
            Configuration.key_pair, private, private
        )

        assert_raises_regexp(
            ValueError,
            '"irrelevant" passed in as private key!',
            Configuration.key_pair, public, irrelevant
        )


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

        for key, value in old_settings.items():
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

