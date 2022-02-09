# encoding: utf-8
import pytest
from ...model.configuration import ConfigurationSetting
from ...model.hasfulltablecache import HasFullTableCache
from ...model.library import Library


def test_library_registry_short_name(db_session, create_library):
    # GIVEN: A Library
    # WHEN:  The short name is set to "foo"
    # THEN:  The short name should be set to "FOO"
    library = create_library(db_session)

    # Short name is always uppercased.
    library.library_registry_short_name = "foo"
    assert "FOO" == library.library_registry_short_name

    # Short name cannot contain a pipe character.
    with pytest.raises(ValueError):
        library.library_registry_short_name = "foo|bar"

    # You can set the short name to None. This isn't
    # recommended, but it's not an error.
    library.library_registry_short_name = None
    assert None == library.library_registry_short_name # Do we need this assert?

def test_library_lookup(db_session, create_library):
    # GIVEN: A Library
    # WHEN:  Checking the Library's cache
    # THEN:  Ensure the cache is populated after looking up the Library by name
    library = create_library(db_session)
    name = library.short_name
    assert name == library.cache_key()

    # Cache is empty.
    assert HasFullTableCache.RESET == Library._cache

    assert library == Library.lookup(db_session, name)

    # Cache is populated.
    assert library == Library._cache[name]

def test_library_default(db_session, create_library):
    # GIVEN: Two Libraries
    # WHEN:  Changing the default status
    # THEN:  Ensure the correct Library has the correct default status

    # Start off with no libraries
    assert None == Library.default(db_session)

    # Let's make a couple libraries.
    library1 = create_library(db_session, short_name="First", name="First")
    library2 = create_library(db_session, short_name="Second", name="Second")

    # None of them are the default according to the database.
    assert False == library1.is_default
    assert False == library2.is_default

    # If we call Library.default, the library with the lowest database
    # ID is made the defualt.
    assert library1 == Library.default(db_session)
    assert True == library1.is_default
    assert False == library2.is_default

    # We can set is_default to change the default library.
    library2.is_default = True
    assert False == library1.is_default
    assert True == library2.is_default

    # If ever there are multiple default libraries, calling default()
    # will set the one with the lowest database ID to the default.
    library1._is_default = True
    library2._is_default = True
    assert library1 == Library.default(db_session)
    assert True == library1.is_default
    assert False == library2.is_default
    with pytest.raises(ValueError) as excinfo:
        library1.is_default = False
    assert "You cannot stop a library from being the default library; you must designate a different library as the default." \
        in str(excinfo.value)

def test_has_root_lanes(db_session, create_lane, create_library):
    # GIVEN: A Library and Lane
    # WHEN:  Checking for the Library's root lane
    # THEN:  Ensure the Library has root lanes if it has root_for_patron_type
    library = create_library(db_session)
    lane = create_lane(db_session, library=library)
    assert False == library.has_root_lanes

    # If a library goes back and forth between 'has root lanes'
    # and 'doesn't have root lanes', has_root_lanes continues to
    # give the correct result so long as there was a database
    # flush in between.
    #
    # (This is because there's a listener that resets
    # Library._has_default_lane_cache whenever lane configuration
    # changes.)
    lane.root_for_patron_type = ["1","2"]
    db_session.flush()
    assert True == library.has_root_lanes

    lane.root_for_patron_type = None
    db_session.flush()
    assert False == library.has_root_lanes

def test_all_collections(db_session, create_library, create_collection):
    # GIVEN: A Library with a Collection
    # WHEN:  Adding a child collection to a parent collection
    # THEN:  Ensure the correct collections are associated with the Library
    library = create_library(db_session)
    default_collection = create_collection(db_session, name="defaut")
    library.collections.append(default_collection)

    parent = create_collection(db_session, name="parent")
    default_collection.parent_id = parent.id

    assert [default_collection] == library.collections
    assert (set([default_collection, parent]) ==
        set(library.all_collections))

def test_estimated_holdings_by_language(db_session, create_collection, create_licensepooldeliverymechanism, create_library, create_work):
    # GIVEN: Works with a variety of languages
    # WHEN:  Estimating holdings by language
    # THEN:  Ensure the correct count of holdings by language
    library = create_library(db_session)
    collection = create_collection(db_session)
    library.collections.append(collection)

    # Here's an open-access English book.
    english = create_work(db_session, language="eng", with_open_access_download=True, collection=collection)

    # Here's a non-open-access Tagalog book with a delivery mechanism.
    tagalog = create_work(db_session, language="tgl", with_license_pool=True, collection=collection)
    [pool] = tagalog.license_pools
    create_licensepooldeliverymechanism(pool)

    # Here's an open-access book that improperly has no language set.
    no_language = create_work(db_session, with_open_access_download=True, collection=collection)
    no_language.presentation_edition.language = None

    # estimated_holdings_by_language counts the English and the
    # Tagalog works. The work with no language is ignored.
    estimate = library.estimated_holdings_by_language()
    assert dict(eng=1, tgl=1) == estimate

    # If we disqualify open-access works, it only counts the Tagalog.
    estimate = library.estimated_holdings_by_language(include_open_access=False)
    assert dict(tgl=1) == estimate

    # If we remove the default collection from the default library,
    # it loses all its works.
    library.collections = []
    estimate = library.estimated_holdings_by_language(include_open_access=False)
    assert dict() == estimate

def test_explain(db_session, create_library, create_externalintegration):
    # GIVEN: Two Libraries and an External Integration
    # WHEN:  Setting a ConfigurationSetting for a library and external integration
    # THEN:  Ensure the Library's settings are correctly configured
    library = create_library(db_session)
    library.uuid = "uuid"
    library.name = "The Library"
    library.short_name = "Short"
    library.library_registry_short_name = "SHORT"
    library.library_registry_shared_secret = "secret"
    
    integration = create_externalintegration(db_session, protocol="protocol", goal="goal")
    integration.url = "http://url/"
    integration.username = "someuser"
    integration.password = "somepass"
    integration.setting("somesetting").value = "somevalue"

    # Different libraries specialize this integration differently.
    ConfigurationSetting.for_library_and_externalintegration(
        db_session, "library-specific", library, integration
    ).value = "value for library1"

    library2 = create_library(db_session, name="library2")
    ConfigurationSetting.for_library_and_externalintegration(
        db_session, "library-specific", library2, integration
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
    assert expect == "\n".join(actual)

    with_secrets = library.explain(True)
    assert 'Shared secret (for library registry): "secret"' in with_secrets
    assert "password='somepass'" in with_secrets
