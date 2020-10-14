# encoding: utf-8
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)
from .. import DatabaseTest
from ...model.configuration import ConfigurationSetting
from ...model.hasfulltablecache import HasFullTableCache
from ...model.library import Library

class TestLibrary(DatabaseTest):

    def test_library_registry_short_name(self):
        library = self._default_library

        # Short name is always uppercased.
        library.library_registry_short_name = "foo"
        eq_("FOO", library.library_registry_short_name)

        # Short name cannot contain a pipe character.
        def set_to_pipe():
            library.library_registry_short_name = "foo|bar"
        assert_raises(ValueError, set_to_pipe)

        # You can set the short name to None. This isn't
        # recommended, but it's not an error.
        library.library_registry_short_name = None

    def test_lookup(self):
        library = self._default_library
        name = library.short_name
        eq_(name, library.cache_key())

        # Cache is empty.
        eq_(HasFullTableCache.RESET, Library._cache)

        eq_(library, Library.lookup(self._db, name))

        # Cache is populated.
        eq_(library, Library._cache[name])

    def test_default(self):
        # We start off with no libraries.
        eq_(None, Library.default(self._db))

        # Let's make a couple libraries.
        l1 = self._default_library
        l2 = self._library()

        # None of them are the default according to the database.
        eq_(False, l1.is_default)
        eq_(False, l2.is_default)

        # If we call Library.default, the library with the lowest database
        # ID is made the default.
        eq_(l1, Library.default(self._db))
        eq_(True, l1.is_default)
        eq_(False, l2.is_default)

        # We can set is_default to change the default library.
        l2.is_default = True
        eq_(False, l1.is_default)
        eq_(True, l2.is_default)

        # If ever there are multiple default libraries, calling default()
        # will set the one with the lowest database ID to the default.
        l1._is_default = True
        l2._is_default = True
        eq_(l1, Library.default(self._db))
        eq_(True, l1.is_default)
        eq_(False, l2.is_default)

        def assign_false():
            l1.is_default = False
        assert_raises_regexp(
            ValueError,
            "You cannot stop a library from being the default library; you must designate a different library as the default.",
            assign_false
        )

    def test_has_root_lanes(self):
        # A library has root lanes if any of its lanes are the root for any
        # patron type(s).
        library = self._default_library
        lane = self._lane()
        eq_(False, library.has_root_lanes)

        # If a library goes back and forth between 'has root lanes'
        # and 'doesn't have root lanes', has_root_lanes continues to
        # give the correct result so long as there was a database
        # flush in between.
        #
        # (This is because there's a listener that resets
        # Library._has_default_lane_cache whenever lane configuration
        # changes.)
        lane.root_for_patron_type = ["1","2"]
        self._db.flush()
        eq_(True, library.has_root_lanes)

        lane.root_for_patron_type = None
        self._db.flush()
        eq_(False, library.has_root_lanes)

    def test_all_collections(self):
        library = self._default_library

        parent = self._collection()
        self._default_collection.parent_id = parent.id

        eq_([self._default_collection], library.collections)
        eq_(set([self._default_collection, parent]),
            set(library.all_collections))

    def test_estimated_holdings_by_language(self):
        library = self._default_library

        # Here's an open-access English book.
        english = self._work(language="eng", with_open_access_download=True)

        # Here's a non-open-access Tagalog book with a delivery mechanism.
        tagalog = self._work(language="tgl", with_license_pool=True)
        [pool] = tagalog.license_pools
        self._add_generic_delivery_mechanism(pool)

        # Here's an open-access book that improperly has no language set.
        no_language = self._work(with_open_access_download=True)
        no_language.presentation_edition.language = None

        # estimated_holdings_by_language counts the English and the
        # Tagalog works. The work with no language is ignored.
        estimate = library.estimated_holdings_by_language()
        eq_(dict(eng=1, tgl=1), estimate)

        # If we disqualify open-access works, it only counts the Tagalog.
        estimate = library.estimated_holdings_by_language(
            include_open_access=False)
        eq_(dict(tgl=1), estimate)

        # If we remove the default collection from the default library,
        # it loses all its works.
        self._default_library.collections = []
        estimate = library.estimated_holdings_by_language(
            include_open_access=False)
        eq_(dict(), estimate)

    def test_explain(self):
        """Test that Library.explain gives all relevant information
        about a Library.
        """
        library = self._default_library
        library.uuid = "uuid"
        library.name = "The Library"
        library.short_name = "Short"
        library.library_registry_short_name = "SHORT"
        library.library_registry_shared_secret = "secret"

        integration = self._external_integration(
            "protocol", "goal"
        )
        integration.url = "http://url/"
        integration.username = "someuser"
        integration.password = "somepass"
        integration.setting("somesetting").value = "somevalue"

        # Different libraries specialize this integration differently.
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-specific", library, integration
        ).value = "value for library1"

        library2 = self._library()
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-specific", library2, integration
        ).value = "value for library2"

        library.integrations.append(integration)

        expect = """Library UUID: "uuid"
Name: "The Library"
Short name: "Short"
Short name (for library registry): "SHORT"

External integrations:
----------------------
ID: %s
Protocol/Goal: protocol/goal
library-specific='value for library1' (applies only to The Library)
somesetting='somevalue'
url='http://url/'
username='someuser'
""" % integration.id
        actual = library.explain()
        eq_(expect, "\n".join(actual))

        with_secrets = library.explain(True)
        assert 'Shared secret (for library registry): "secret"' in with_secrets
        assert "password='somepass'" in with_secrets
