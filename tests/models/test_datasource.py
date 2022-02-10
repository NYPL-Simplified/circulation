# encoding: utf-8
import pytest
from sqlalchemy.orm.exc import NoResultFound
from ...model.datasource import DataSource
from ...model.hasfulltablecache import HasFullTableCache
from ...model.identifier import Identifier


class TestDataSource:

    def test_lookup(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        key = DataSource.GUTENBERG

        gutenberg = DataSource.lookup(db_session, key)
        assert key == gutenberg.name
        assert True == gutenberg.offers_licenses
        assert key == gutenberg.cache_key()

        # Object has been loaded into cache
        assert (gutenberg, False) == DataSource.by_cache_key(db_session, key, None)

        # Now try creating a new data source.
        key = "New data source"

        # It's not in the cache.
        assert (None, False) == DataSource.by_cache_key(db_session, key, None)

        new_source = DataSource.lookup(
            db_session, key, autocreate=True, offers_licenses=True
        )

        # A new data source has been created
        assert key == new_source.name
        assert True == new_source.offers_licenses

        # The cache was reset when the data source was created
        assert HasFullTableCache.RESET == DataSource._cache

        assert (new_source, False) == DataSource.by_cache_key(db_session, key, None)

    def test_lookup_by_deprecated_name(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        threem = DataSource.lookup(db_session, "3M")
        assert DataSource.BIBLIOTHECA == threem.name
        assert DataSource.BIBLIOTHECA != "3M"

    def test_lookup_returns_none_for_nonexistent_source(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        assert None == DataSource.lookup(db_session, "No such data source")

    def test_lookup_with_autocreate(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN: 
        THEN:
        """
        name = "Brand new data source"
        new_source = DataSource.lookup(db_session, name, autocreate=True)
        assert name == new_source.name
        assert False == new_source.offers_licenses

        name = "New data source with licenses"
        new_source = DataSource.lookup(
            db_session, name, autocreate=True, offers_licenses=True
        )
        assert True == new_source.offers_licenses

    def test_metadata_sources_for(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        content_cafe = DataSource.lookup(db_session, DataSource.CONTENT_CAFE)
        isbn_metadata_sources = DataSource.metadata_sources_for(db_session, Identifier.ISBN)

        assert 1 == len(isbn_metadata_sources)
        assert [content_cafe] == isbn_metadata_sources

    def test_license_source_for(self, db_session, create_identifier, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        identifier = create_identifier(identifier_type=Identifier.OVERDRIVE_ID)
        source = DataSource.license_source_for(db_session, identifier)
        assert DataSource.OVERDRIVE == source.name
        
    def test_license_source_for_string(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        source = DataSource.license_source_for(db_session, Identifier.THREEM_ID)
        assert DataSource.THREEM == source.name

    def test_license_source_fails_if_identifier_type_does_not_provide_licenses(self, db_session, create_identifier, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        identifier = create_identifier(DataSource.MANUAL)
        pytest.raises(
            NoResultFound, DataSource.license_source_for, db_session, identifier)
