# encoding: utf-8
import pytest
from sqlalchemy.orm.exc import NoResultFound
from .. import DatabaseTest
from ...model.datasource import DataSource
from ...model.hasfulltablecache import HasFullTableCache
from ...model.identifier import Identifier

class TestDataSource(DatabaseTest):

    def test_lookup(self):
        key = DataSource.GUTENBERG

        # Unlike with most of these tests, this cache doesn't start
        # out empty. It's populated with all known values at the start
        # of the test. Let's reset the cache.
        DataSource.reset_cache()

        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        assert key == gutenberg.name
        assert True == gutenberg.offers_licenses
        assert key == gutenberg.cache_key()

        # Object has been loaded into cache.
        assert (gutenberg, False) == DataSource.by_cache_key(self._db, key, None)

        # Now try creating a new data source.
        key = "New data source"

        # It's not in the cache.
        assert (None, False) == DataSource.by_cache_key(self._db, key, None)

        new_source = DataSource.lookup(
            self._db, key, autocreate=True, offers_licenses=True
        )

        # A new data source has been created.
        assert key == new_source.name
        assert True == new_source.offers_licenses

        # The cache was reset when the data source was created.
        assert HasFullTableCache.RESET == DataSource._cache

        assert (new_source, False) == DataSource.by_cache_key(self._db, key, None)

    def test_lookup_by_deprecated_name(self):
        threem = DataSource.lookup(self._db, "3M")
        assert DataSource.BIBLIOTHECA == threem.name
        assert DataSource.BIBLIOTHECA != "3M"

    def test_lookup_returns_none_for_nonexistent_source(self):
        assert None == DataSource.lookup(
            self._db, "No such data source " + self._str)

    def test_lookup_with_autocreate(self):
        name = "Brand new data source " + self._str
        new_source = DataSource.lookup(self._db, name, autocreate=True)
        assert name == new_source.name
        assert False == new_source.offers_licenses

        name = "New data source with licenses" + self._str
        new_source = DataSource.lookup(
            self._db, name, autocreate=True, offers_licenses=True
        )
        assert True == new_source.offers_licenses

    def test_metadata_sources_for(self):
        content_cafe = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)
        isbn_metadata_sources = DataSource.metadata_sources_for(
            self._db, Identifier.ISBN
        )

        assert 1 == len(isbn_metadata_sources)
        assert [content_cafe] == isbn_metadata_sources

    def test_license_source_for(self):
        identifier = self._identifier(Identifier.OVERDRIVE_ID)
        source = DataSource.license_source_for(self._db, identifier)
        assert DataSource.OVERDRIVE == source.name

    def test_license_source_for_string(self):
        source = DataSource.license_source_for(
            self._db, Identifier.THREEM_ID)
        assert DataSource.THREEM == source.name

    def test_license_source_fails_if_identifier_type_does_not_provide_licenses(self):
        identifier = self._identifier(DataSource.MANUAL)
        pytest.raises(
            NoResultFound, DataSource.license_source_for, self._db, identifier)
