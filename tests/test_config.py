from collections import Counter
from nose.tools import (
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
        eq_(["eng"], ConfigurationSetting.for_library(
            C.LARGE_COLLECTION_LANGUAGES, library).json_value)
        eq_([], ConfigurationSetting.for_library(
            C.SMALL_COLLECTION_LANGUAGES, library).json_value)
        eq_([], ConfigurationSetting.for_library(
            C.TINY_COLLECTION_LANGUAGES, library).json_value)
        
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
        
