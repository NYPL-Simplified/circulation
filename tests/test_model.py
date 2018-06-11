# encoding: utf-8
from StringIO import StringIO
import base64
import datetime
import feedparser
import os
import sys
import site
import random
import re
import tempfile

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    assert_not_equal,
    eq_,
    set_trace,
)

from psycopg2.extras import NumericRange

from sqlalchemy import not_

from sqlalchemy.exc import (
    IntegrityError,
)

from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound
)
from sqlalchemy.orm.session import Session

from lxml import etree

from config import (
    CannotLoadConfiguration,
    Configuration, 
    temp_config,
)

from entrypoint import AudiobooksEntryPoint

import lane
from lane import (
    Facets,
    Pagination,
    WorkList,
)
import model
from model import (
    Admin,
    AdminRole,
    Annotation,
    BaseCoverageRecord,
    CachedFeed,
    CirculationEvent,
    Classification,
    Collection,
    CollectionMissing,
    Complaint,
    ConfigurationSetting,
    Contributor,
    CoverageRecord,
    Credential,
    CustomList,
    CustomListEntry,
    DataSource,
    DelegatedPatronIdentifier,
    DeliveryMechanism,
    DRMDeviceIdentifier,
    ExternalIntegration,
    Genre,
    HasFullTableCache,
    Hold,
    Hyperlink,
    IntegrationClient,
    Library,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Measurement,
    Patron,
    PatronProfileStorage,
    PolicyException,
    Representation,
    Resource,
    RightsStatus,
    SessionManager,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    WorkGenre,
    Identifier,
    Edition,
    create,
    get_one,
    get_one_or_create,
    site_configuration_has_changed,
    tuple_to_numericrange,
)
from external_search import (
    DummyExternalSearchIndex,
)

import classifier
from classifier import (
    Classifier,
    Fantasy,
    Romance,
    Science_Fiction,
    Drama,
)

from . import (
    DatabaseTest,
    DummyHTTPClient,
)

from testing import MockRequestsResponse

from mock_analytics_provider import MockAnalyticsProvider

class TestSessionManager(DatabaseTest):

    def test_refresh_materialized_views(self):
        work = self._work(fiction=True, with_license_pool=True, 
                          genre="Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")
        work.genres.append(romance)
        fiction = self._lane(display_name="Fiction", fiction=True)
        nonfiction = self._lane(display_name="Nonfiction", fiction=False)

        from model import MaterializedWorkWithGenre as mwg

        # There are no items in the materialized views.
        eq_([], self._db.query(mwg).all())

        # The lane sizes are wrong.
        fiction.size = 100
        nonfiction.size = 100

        SessionManager.refresh_materialized_views(self._db)

        # The work has been added to the materialized view. (It was
        # added twice because it's filed under two genres.)
        eq_([work.id, work.id], [x.works_id for x in self._db.query(mwg)])

        # Both lanes have had .size set to the correct value.
        eq_(1, fiction.size)
        eq_(0, nonfiction.size)


class TestDatabaseInterface(DatabaseTest):

    def test_get_one(self):

        # When a matching object isn't found, None is returned.
        result = get_one(self._db, Edition)
        eq_(None, result)

        # When a single item is found, it is returned.
        edition = self._edition()
        result = get_one(self._db, Edition)
        eq_(edition, result)

        # When multiple items are found, an error is raised.
        other_edition = self._edition()
        assert_raises(MultipleResultsFound, get_one, self._db, Edition)

        # Unless they're interchangeable.
        result = get_one(self._db, Edition, on_multiple='interchangeable')
        assert result in self._db.query(Edition)

        # Or specific attributes are passed that limit the results to one.
        result = get_one(
            self._db, Edition,
            title=other_edition.title,
            author=other_edition.author)
        eq_(other_edition, result)

        # A particular constraint clause can also be passed in.
        titles = [ed.title for ed in (edition, other_edition)]
        constraint = not_(Edition.title.in_(titles))
        result = get_one(self._db, Edition, constraint=constraint)
        eq_(None, result)

    def test_initialize_data_does_not_reset_timestamp(self):
        # initialize_data() has already been called, so the database is
        # initialized and the 'site configuration changed' Timestamp has
        # been set. Calling initialize_data() again won't change the 
        # date on the timestamp.
        timestamp = get_one(self._db, Timestamp,
                            collection=None, 
                            service=Configuration.SITE_CONFIGURATION_CHANGED)
        old_timestamp = timestamp.timestamp
        SessionManager.initialize_data(self._db)
        eq_(old_timestamp, timestamp.timestamp)


class TestDataSource(DatabaseTest):

    def test_lookup(self):
        key = DataSource.GUTENBERG

        # Unlike with most of these tests, this cache doesn't start
        # out empty. It's populated with all known values at the start
        # of the test. Let's reset the cache.
        DataSource.reset_cache()
        
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        eq_(key, gutenberg.name)
        eq_(True, gutenberg.offers_licenses)
        eq_(key, gutenberg.cache_key())

        # Object has been loaded into cache.
        eq_((gutenberg, False), DataSource.by_cache_key(self._db, key, None))

        # Now try creating a new data source.
        key = "New data source"

        # It's not in the cache.
        eq_((None, False), DataSource.by_cache_key(self._db, key, None))

        new_source = DataSource.lookup(
            self._db, key, autocreate=True, offers_licenses=True
        )

        # A new data source has been created.
        eq_(key, new_source.name)
        eq_(True, new_source.offers_licenses)

        # The cache was reset when the data source was created.
        eq_(HasFullTableCache.RESET, DataSource._cache)

        eq_((new_source, False), DataSource.by_cache_key(self._db, key, None))
        
    def test_lookup_by_deprecated_name(self):
        threem = DataSource.lookup(self._db, "3M")
        eq_(DataSource.BIBLIOTHECA, threem.name)
        assert DataSource.BIBLIOTHECA != "3M"
        
    def test_lookup_returns_none_for_nonexistent_source(self):
        eq_(None, DataSource.lookup(
            self._db, "No such data source " + self._str))

    def test_lookup_with_autocreate(self):
        name = "Brand new data source " + self._str
        new_source = DataSource.lookup(self._db, name, autocreate=True)
        eq_(name, new_source.name)
        eq_(False, new_source.offers_licenses)
        
        name = "New data source with licenses" + self._str
        new_source = DataSource.lookup(
            self._db, name, autocreate=True, offers_licenses=True
        )
        eq_(True, new_source.offers_licenses)
        
    def test_metadata_sources_for(self):
        content_cafe = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)
        isbn_metadata_sources = DataSource.metadata_sources_for(
            self._db, Identifier.ISBN
        )

        eq_(1, len(isbn_metadata_sources))
        eq_([content_cafe], isbn_metadata_sources)

    def test_license_source_for(self):
        identifier = self._identifier(Identifier.OVERDRIVE_ID)
        source = DataSource.license_source_for(self._db, identifier)
        eq_(DataSource.OVERDRIVE, source.name)

    def test_license_source_for_string(self):
        source = DataSource.license_source_for(
            self._db, Identifier.THREEM_ID)
        eq_(DataSource.THREEM, source.name)

    def test_license_source_fails_if_identifier_type_does_not_provide_licenses(self):
        identifier = self._identifier(DataSource.MANUAL)
        assert_raises(
            NoResultFound, DataSource.license_source_for, self._db, identifier)
            

class TestIdentifier(DatabaseTest):

    def test_for_foreign_id(self):
        identifier_type = Identifier.ISBN
        isbn = "3293000061"

        # Getting the data automatically creates a database record.
        identifier, was_new = Identifier.for_foreign_id(
            self._db, identifier_type, isbn)
        eq_(Identifier.ISBN, identifier.type)
        eq_(isbn, identifier.identifier)
        eq_(True, was_new)

        # If we get it again we get the same data, but it's no longer new.
        identifier2, was_new = Identifier.for_foreign_id(
            self._db, identifier_type, isbn)
        eq_(identifier, identifier2)
        eq_(False, was_new)

        # If we pass in no data we get nothing back.
        eq_(None, Identifier.for_foreign_id(self._db, None, None))

    def test_for_foreign_id_by_deprecated_type(self):
        threem_id, is_new = Identifier.for_foreign_id(
            self._db, "3M ID", self._str
        )
        eq_(Identifier.BIBLIOTHECA_ID, threem_id.type)
        assert Identifier.BIBLIOTHECA_ID != "3M ID"

    def test_for_foreign_id_rejects_invalid_identifiers(self):
        assert_raises_regexp(
            ValueError,
            '"foo/bar" is not a valid Bibliotheca ID.',
            Identifier.for_foreign_id,
            self._db, Identifier.BIBLIOTHECA_ID, "foo/bar"
        )

    def test_valid_as_foreign_identifier(self):
        m = Identifier.valid_as_foreign_identifier

        eq_(True, m(Identifier.BIBLIOTHECA_ID, "bhhot389"))
        eq_(False, m(Identifier.BIBLIOTHECA_ID, "bhhot389/open_book"))
        eq_(False, m(Identifier.BIBLIOTHECA_ID, "bhhot389,bhhot389"))

        eq_(True, m(Identifier.BIBLIOTHECA_ID, "0015142259"))
        eq_(False, m(Identifier.BIBLIOTHECA_ID, "0015142259,0015187940"))
            
    def test_for_foreign_id_without_autocreate(self):
        identifier_type = Identifier.ISBN
        isbn = self._str

        # We don't want to auto-create a database record, so we set
        # autocreate=False
        identifier, was_new = Identifier.for_foreign_id(
            self._db, identifier_type, isbn, autocreate=False)
        eq_(None, identifier)
        eq_(False, was_new)

    def test_from_asin(self):
        isbn10 = '1449358063'
        isbn13 = '9781449358068'
        asin = 'B0088IYM3C'
        isbn13_with_dashes = '978-144-935-8068'        

        i_isbn10, new1 = Identifier.from_asin(self._db, isbn10)
        i_isbn13, new2 = Identifier.from_asin(self._db, isbn13)
        i_asin, new3 = Identifier.from_asin(self._db, asin)
        i_isbn13_2, new4 = Identifier.from_asin(self._db, isbn13_with_dashes)

        # The three ISBNs are equivalent, so they got turned into the same
        # Identifier, using the ISBN13.
        eq_(i_isbn10, i_isbn13)
        eq_(i_isbn13_2, i_isbn13)
        eq_(Identifier.ISBN, i_isbn10.type)
        eq_(isbn13, i_isbn10.identifier)
        eq_(True, new1)
        eq_(False, new2)
        eq_(False, new4)

        eq_(Identifier.ASIN, i_asin.type)
        eq_(asin, i_asin.identifier)

    def test_urn(self):
        # ISBN identifiers use the ISBN URN scheme.
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "9781449358068")
        eq_("urn:isbn:9781449358068", identifier.urn)

        # URI identifiers don't need a URN scheme.
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.URI, "http://example.com/")
        eq_(identifier.identifier, identifier.urn)

        # Gutenberg identifiers use Gutenberg's URL-based sceheme
        identifier = self._identifier(Identifier.GUTENBERG_ID)
        eq_(Identifier.GUTENBERG_URN_SCHEME_PREFIX + identifier.identifier,
            identifier.urn)

        # All other identifiers use our custom URN scheme.
        identifier = self._identifier(Identifier.OVERDRIVE_ID)
        assert identifier.urn.startswith(Identifier.URN_SCHEME_PREFIX)

    def test_parse_urns(self):
        identifier = self._identifier()
        fake_urn = "what_even_is_this"
        new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/nosuchidentifier"
        # Also create a different URN that would result in the same identifier.
        same_new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/NOSUCHidentifier"
        urns = [identifier.urn, fake_urn, new_urn, same_new_urn]

        results = Identifier.parse_urns(self._db, urns, autocreate=False)
        identifiers_by_urn, failures = results

        # By default, no new identifiers are created. All URNs for identifiers
        # that aren't in the db are included in the list of failures.
        eq_(sorted([fake_urn, new_urn, same_new_urn]), sorted(failures))

        # Only the existing identifier is included in the results.
        eq_(1, len(identifiers_by_urn))
        eq_({identifier.urn : identifier}, identifiers_by_urn)

        # By default, new identifiers are created, too.
        results = Identifier.parse_urns(self._db, urns)
        identifiers_by_urn, failures = results

        # Only the fake URN is returned as a failure.
        eq_([fake_urn], failures)

        # Only two additional identifiers have been created.
        eq_(2, len(identifiers_by_urn))

        # One is the existing identifier.
        eq_(identifier, identifiers_by_urn[identifier.urn])

        # And the new identifier has been created.
        new_identifier = identifiers_by_urn[new_urn]
        assert isinstance(new_identifier, Identifier)
        assert new_identifier in self._db
        eq_(Identifier.OVERDRIVE_ID, new_identifier.type)
        eq_("nosuchidentifier", new_identifier.identifier)

        # By passing in a list of allowed_types we can stop certain
        # types of Identifiers from being looked up, even if they
        # already exist.
        isbn_urn = "urn:isbn:9781453219539"
        urns = [new_urn, isbn_urn]
        only_overdrive = [Identifier.OVERDRIVE_ID]
        only_isbn = [Identifier.OVERDRIVE_ID]
        everything = []

        success, failure = Identifier.parse_urns(
            self._db, urns, allowed_types=[Identifier.OVERDRIVE_ID]
        )
        assert new_urn in success
        assert isbn_urn in failure

        success, failure = Identifier.parse_urns(
            self._db, urns, allowed_types=[
                Identifier.OVERDRIVE_ID, Identifier.ISBN
            ]
        )
        assert new_urn in success
        assert isbn_urn in success
        eq_([], failure)

        # If the allowed_types is empty, no URNs can be looked up
        # -- this is most likely the caller's mistake.
        success, failure = Identifier.parse_urns(
            self._db, urns, allowed_types=[]
        )
        assert new_urn in failure
        assert isbn_urn in failure

    def test_parse_urn(self):

        # We can parse our custom URNs back into identifiers.
        identifier = self._identifier()
        self._db.commit()
        new_identifier, ignore = Identifier.parse_urn(self._db, identifier.urn)
        eq_(identifier, new_identifier)

        # We can parse urn:isbn URNs into ISBN identifiers. ISBN-10s are
        # converted to ISBN-13s.
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "9781449358068")
        isbn_urn = "urn:isbn:1449358063"
        isbn_identifier, ignore = Identifier.parse_urn(self._db, isbn_urn)
        eq_(Identifier.ISBN, isbn_identifier.type)
        eq_("9781449358068", isbn_identifier.identifier)

        isbn_urn = "urn:isbn:9781449358068"
        isbn_identifier2, ignore = Identifier.parse_urn(self._db, isbn_urn)
        eq_(isbn_identifier2, isbn_identifier)

        # We can parse ordinary http: or https: URLs into URI
        # identifiers.
        http_identifier, ignore = Identifier.parse_urn(
            self._db, "http://example.com")
        eq_(Identifier.URI, http_identifier.type)
        eq_("http://example.com", http_identifier.identifier)

        https_identifier, ignore = Identifier.parse_urn(
            self._db, "https://example.com")
        eq_(Identifier.URI, https_identifier.type)
        eq_("https://example.com", https_identifier.identifier)

        # We can parse UUIDs.
        uuid_identifier, ignore = Identifier.parse_urn(
            self._db, "urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952")
        eq_(Identifier.URI, uuid_identifier.type)
        eq_("urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952", uuid_identifier.identifier)

        # A URN we can't handle raises an exception.
        ftp_urn = "ftp://example.com"
        assert_raises(ValueError, Identifier.parse_urn, self._db, ftp_urn)

        # An invalid ISBN raises an exception.
        assert_raises(ValueError, Identifier.parse_urn, self._db, "urn:isbn:notanisbn")

        # Pass in None and you get None.
        eq_(None, Identifier.parse_urn(self._db, None))
        
    def parse_urn_must_support_license_pools(self):
        # We have no way of associating ISBNs with license pools.
        # If we try to parse an ISBN URN in a context that only accepts
        # URNs that can have associated license pools, we get an exception.
        isbn_urn = "urn:isbn:1449358063"
        assert_raises(
            Identifier.UnresolvableIdentifierException, 
            Identifier.parse_urn, self._db, isbn_urn, 
            must_support_license_pools=True)

    def test_recursively_equivalent_identifier_ids(self):
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.MANUAL)

        strong_equivalent = self._identifier()
        identifier.equivalent_to(data_source, strong_equivalent, 0.9)

        weak_equivalent = self._identifier()
        identifier.equivalent_to(data_source, weak_equivalent, 0.2)

        level_2_equivalent = self._identifier()
        strong_equivalent.equivalent_to(data_source, level_2_equivalent, 0.5)

        level_3_equivalent = self._identifier()
        level_2_equivalent.equivalent_to(data_source, level_3_equivalent, 0.9)

        level_4_equivalent = self._identifier()
        level_3_equivalent.equivalent_to(data_source, level_4_equivalent, 0.6)

        unrelated = self._identifier()

        # With a low threshold and enough levels, we find all the identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=5, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[identifier.id]))

        # If we only look at one level, we don't find the level 2, 3, or 4 identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=1, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id]),
            set(equivs[identifier.id]))

        # If we raise the threshold, we don't find the weak identifier.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=1, threshold=0.4)
        eq_(set([identifier.id,
                 strong_equivalent.id]),
            set(equivs[identifier.id]))

        # For deeper levels, the strength is the product of the strengths
        # of all the equivalencies in between the two identifiers.

        # In this example:
        # identifier - level_2_equivalent = 0.9 * 0.5 = 0.45
        # identifier - level_3_equivalent = 0.9 * 0.5 * 0.9 = 0.405
        # identifier - level_4_equivalent = 0.9 * 0.5 * 0.9 * 0.6 = 0.243

        # With a threshold of 0.5, level 2 and all subsequent levels are too weak.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=5, threshold=0.5)
        eq_(set([identifier.id,
                 strong_equivalent.id]),
            set(equivs[identifier.id]))

        # With a threshold of 0.25, level 2 is strong enough, but level
        # 4 is too weak.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id], levels=5, threshold=0.25)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id]),
            set(equivs[identifier.id]))

        # It also works if we start from other identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [strong_equivalent.id], levels=5, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[strong_equivalent.id]))

        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [level_4_equivalent.id], levels=5, threshold=0.1)
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[level_4_equivalent.id]))
        
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [level_4_equivalent.id], levels=5, threshold=0.5)
        eq_(set([level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivs[level_4_equivalent.id]))
        
        # A chain of very strong equivalents can keep a high strength
        # even at deep levels. This wouldn't work if we changed the strength
        # threshold by level instead of accumulating a strength product.
        another_identifier = self._identifier()
        l2 = self._identifier()
        l3 = self._identifier()
        l4 = self._identifier()
        l2.equivalent_to(data_source, another_identifier, 1)
        l3.equivalent_to(data_source, l2, 1)
        l4.equivalent_to(data_source, l3, 0.9)
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [another_identifier.id], levels=5, threshold=0.89)
        eq_(set([another_identifier.id,
                 l2.id,
                 l3.id,
                 l4.id]),
            set(equivs[another_identifier.id]))

        # We can look for multiple identifiers at once.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            self._db, [identifier.id, level_3_equivalent.id], levels=2, threshold=0.8)
        eq_(set([identifier.id,
                 strong_equivalent.id]),
            set(equivs[identifier.id]))
        eq_(set([level_2_equivalent.id,
                 level_3_equivalent.id]),
            set(equivs[level_3_equivalent.id]))

        # The query uses the same db function, but returns equivalents
        # for all identifiers together so it can be used as a subquery.
        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, levels=5, threshold=0.1)
        query = query.where(Identifier.id==identifier.id)
        results = self._db.execute(query)
        equivalent_ids = [r[0] for r in results]
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 weak_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id,
                 level_4_equivalent.id]),
            set(equivalent_ids))

        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, levels=2, threshold=0.8)
        query = query.where(Identifier.id.in_([identifier.id, level_3_equivalent.id]))
        results = self._db.execute(query)
        equivalent_ids = [r[0] for r in results]
        eq_(set([identifier.id,
                 strong_equivalent.id,
                 level_2_equivalent.id,
                 level_3_equivalent.id]),
            set(equivalent_ids))

    def test_licensed_through_collection(self):
        c1 = self._default_collection
        c2 = self._collection()
        c3 = self._collection()

        edition, lp1 = self._edition(collection=c1, with_license_pool=True)
        lp2 = self._licensepool(collection=c2, edition=edition)

        identifier = lp1.identifier
        eq_(lp2.identifier, identifier)

        eq_(lp1, identifier.licensed_through_collection(c1))
        eq_(lp2, identifier.licensed_through_collection(c2))
        eq_(None, identifier.licensed_through_collection(c3))

    def test_missing_coverage_from(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "1")

        g2, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        c1 = self._coverage_record(g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        c2 = self._coverage_record(g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            self._db, web, Identifier.URI, "http://www.foo.com/")

        # If we run missing_coverage_from we pick up the Gutenberg
        # record with no generic OCLC coverage. It doesn't pick up the
        # other Gutenberg record, it doesn't pick up the web record,
        # and it doesn't pick up the OCLC coverage for a specific
        # operation.
        [in_gutenberg_but_not_in_oclc] = Identifier.missing_coverage_from(
            self._db, [Identifier.GUTENBERG_ID], oclc).all()

        eq_(g2.primary_identifier, in_gutenberg_but_not_in_oclc)

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.

        [has_generic_coverage_only] = Identifier.missing_coverage_from(
            self._db, [Identifier.GUTENBERG_ID], oclc, "some operation").all()
        eq_(g1.primary_identifier, has_generic_coverage_only)

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Identifier.missing_coverage_from(
            self._db, [Identifier.URI], oclc).all()
        eq_(w.primary_identifier, in_web_but_not_in_oclc)

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        eq_([g1.primary_identifier.id, g2.primary_identifier.id], sorted(
            [x.id for x in Identifier.missing_coverage_from(
                self._db, [Identifier.GUTENBERG_ID], web)])
        )

    def test_missing_coverage_from_with_collection(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = self._identifier()
        collection1 = self._default_collection
        collection2 = self._collection()
        self._coverage_record(identifier, gutenberg, collection=collection1)

        # The Identifier has coverage in collection 1.
        eq_([], 
            Identifier.missing_coverage_from(
                self._db, [identifier.type], gutenberg, collection=collection1
            ).all()
        )

        # It is missing coverage in collection 2.
        eq_(
            [identifier], Identifier.missing_coverage_from(
                self._db, [identifier.type], gutenberg, collection=collection2
            ).all()
        )

        # If no collection is specified, we look for a CoverageRecord
        # that also has no collection specified, and the Identifier is
        # not treated as covered.
        eq_([identifier], 
            Identifier.missing_coverage_from(
                self._db, [identifier.type], gutenberg
            ).all()
        )


    def test_missing_coverage_from_with_cutoff_date(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here's an Edition with a coverage record from OCLC classify.
        gutenberg, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "1")
        identifier = gutenberg.primary_identifier
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        coverage = self._coverage_record(gutenberg, oclc)

        # The CoverageRecord knows when the coverage was provided.
        timestamp = coverage.timestamp
        
        # If we ask for Identifiers that are missing coverage records
        # as of that time, we see nothing.
        eq_(
            [], 
            Identifier.missing_coverage_from(
                self._db, [identifier.type], oclc, 
                count_as_missing_before=timestamp
            ).all()
        )

        # But if we give a time one second later, the Identifier is
        # missing coverage.
        eq_(
            [identifier], 
            Identifier.missing_coverage_from(
                self._db, [identifier.type], oclc, 
                count_as_missing_before=timestamp+datetime.timedelta(seconds=1)
            ).all()
        )

    def test_opds_entry(self):
        identifier = self._identifier()
        source = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)

        summary = identifier.add_link(
            Hyperlink.DESCRIPTION, 'http://description', source,
            media_type=Representation.TEXT_PLAIN, content='a book'
        )[0]
        cover = identifier.add_link(
            Hyperlink.IMAGE, 'http://cover', source,
            media_type=Representation.JPEG_MEDIA_TYPE
        )[0]

        def get_entry_dict(entry):
            return feedparser.parse(unicode(etree.tostring(entry))).entries[0]

        def format_timestamp(timestamp):
            return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

        # The entry includes the urn, description, and cover link.
        entry = get_entry_dict(identifier.opds_entry())
        eq_(identifier.urn, entry.id)
        eq_('a book', entry.summary)
        [cover_link] = entry.links
        eq_('http://cover', cover_link.href)

        # The 'updated' time is set to the latest timestamp associated
        # with the Identifier.
        eq_([], identifier.coverage_records)

        # This may be the time the cover image was mirrored.
        cover.resource.representation.set_as_mirrored(self._url)
        now = datetime.datetime.utcnow()
        cover.resource.representation.mirrored_at = now
        entry = get_entry_dict(identifier.opds_entry())
        eq_(format_timestamp(now), entry.updated)

        # Or it may be a timestamp on a coverage record associated
        # with the Identifier.

        # For whatever reason, this coverage record is missing its
        # timestamp. This indicates an error elsewhere, but it
        # doesn't crash the method we're testing.
        no_timestamp = self._coverage_record(
            identifier, source, operation="bad operation"
        )
        no_timestamp.timestamp = None

        # If a coverage record is dated after the cover image's mirror
        # time, That becomes the new updated time.
        record = self._coverage_record(identifier, source)
        the_future = now + datetime.timedelta(minutes=60)
        record.timestamp = the_future
        identifier.opds_entry()
        entry = get_entry_dict(identifier.opds_entry())
        eq_(format_timestamp(record.timestamp), entry.updated)

        # Basically the latest date is taken from either a coverage record
        # or a representation.
        even_later = now + datetime.timedelta(minutes=120)
        thumbnail = identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, 'http://thumb', source,
            media_type=Representation.JPEG_MEDIA_TYPE
        )[0]
        thumb_rep = thumbnail.resource.representation
        cover_rep = cover.resource.representation
        thumbnail.resource.representation.thumbnail_of_id = cover_rep.id
        cover_rep.thumbnails.append(thumb_rep)
        thumbnail.resource.representation.mirrored_at = even_later

        entry = get_entry_dict(identifier.opds_entry())
        # The thumbnail has been added to the links.
        eq_(2, len(entry.links))
        assert any(filter(lambda l: l.href=='http://thumb', entry.links))
        # And the updated time has been changed accordingly.
        expected = thumbnail.resource.representation.mirrored_at
        eq_(format_timestamp(even_later), entry.updated)


class TestGenre(DatabaseTest):

    def test_full_table_cache(self):
        """We use Genre as a convenient way of testing
        HasFullTableCache.populate_cache, which requires a real
        SQLAlchemy ORM class to operate on.
        """

        # We start with an unusable object as the cache.
        eq_(Genre.RESET, Genre._cache)
        eq_(Genre.RESET, Genre._id_cache)

        # When we call populate_cache()...
        Genre.populate_cache(self._db)

        # Every Genre in the database is copied to the cache.
        dont_call_this = object
        drama, is_new = Genre.by_cache_key(self._db, "Drama", dont_call_this)
        eq_("Drama", drama.name)
        eq_(False, is_new)

        # The ID of every genre is copied to the ID cache.
        eq_(drama, Genre._id_cache[drama.id])
        drama2 = Genre.by_id(self._db, drama.id)
        eq_(drama2, drama)

    def test_by_id(self):

        # Get a genre to test with.
        drama = get_one(self._db, Genre, name="Drama")
        
        # Since we went right to the database, that didn't change the
        # fact that the ID cache is uninitialized.
        eq_(Genre.RESET, Genre._id_cache)

        # Look up the same genre using by_id...
        eq_(drama, Genre.by_id(self._db, drama.id))

        # ... and the ID cache is fully initialized.
        eq_(drama, Genre._id_cache[drama.id])
        assert len(Genre._id_cache) > 1
        
    def test_by_cache_key_miss_triggers_create_function(self):
        _db = self._db
        class Factory(object):

            def __init__(self):
                self.called = False

            def call_me(self):
                self.called = True
                genre, is_new = get_one_or_create(_db, Genre, name="Drama")
                return genre, is_new
                
        factory = Factory()
        Genre._cache = {}
        Genre._id_cache = {}
        genre, is_new = Genre.by_cache_key(self._db, "Drama", factory.call_me)
        eq_("Drama", genre.name)
        eq_(False, is_new)
        eq_(True, factory.called)

        # The Genre object created in call_me has been associated with the
        # Genre's cache key in the table-wide cache.
        eq_(genre, Genre._cache[genre.cache_key()])

        # The cache by ID has been similarly populated.
        eq_(genre, Genre._id_cache[genre.id])

    def test_by_cache_key_miss_when_cache_is_reset_populates_cache(self):
        # The cache is not in a state to be used.
        eq_(Genre._cache, Genre.RESET)

        # Call Genreby_cache_key...
        drama, is_new = Genre.by_cache_key(
            self._db, "Drama",
            lambda: get_one_or_create(self._db, Genre, name="Drama")
        )
        eq_("Drama", drama.name)
        eq_(False, is_new)

        # ... and the cache is repopulated
        assert drama.cache_key() in Genre._cache
        assert drama.id in Genre._id_cache

    def test_by_cache_key_hit_returns_cached_object(self):

        # If the object we ask for is not already in the cache, this
        # function will be called and raise an exception.
        def exploding_create_hook():
            raise Exception("Kaboom")
        drama, ignore = get_one_or_create(self._db, Genre, name="Drama")
        Genre._cache = { "Drama": drama }
        drama2, is_new = Genre.by_cache_key(
            self._db, "Drama", exploding_create_hook
        )

        # The object was already in the cache, so we just looked it up.
        # No exception.
        eq_(drama, drama2)
        eq_(False, is_new)

    def test_name_is_unique(self):
        g1, ignore = Genre.lookup(self._db, "A Genre", autocreate=True)
        g2, ignore = Genre.lookup(self._db, "A Genre", autocreate=True)
        eq_(g1, g2)

        assert_raises(IntegrityError, create, self._db, Genre, name="A Genre")

    def test_default_fiction(self):
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        nonfiction, ignore = Genre.lookup(self._db, "History")
        eq_(True, sf.default_fiction)
        eq_(False, nonfiction.default_fiction)

        # Create a previously unknown genre.
        genre, ignore = Genre.lookup(
            self._db, "Some Weird Genre", autocreate=True
        )

        # We don't know its default fiction status.
        eq_(None, genre.default_fiction)

        
class TestSubject(DatabaseTest):

    def test_lookup_errors(self):
        """Subject.lookup will complain if you don't give it
        enough information to find a Subject.
        """
        assert_raises_regexp(
            ValueError, "Cannot look up Subject with no type.",
            Subject.lookup, self._db, None, "identifier", "name"
        )
        assert_raises_regexp(
            ValueError,
            "Cannot look up Subject when neither identifier nor name is provided.",
            Subject.lookup, self._db, Subject.TAG, None, None
        )

    def test_lookup_autocreate(self):
        # By default, Subject.lookup creates a Subject that doesn't exist.
        identifier = self._str
        name = self._str
        subject, was_new = Subject.lookup(
            self._db, Subject.TAG, identifier, name
        )
        eq_(True, was_new)
        eq_(identifier, subject.identifier)
        eq_(name, subject.name)

        # But you can tell it not to autocreate.
        identifier2 = self._str
        subject, was_new = Subject.lookup(
            self._db, Subject.TAG, identifier2, None, autocreate=False
        )
        eq_(False, was_new)
        eq_(None, subject)

    def test_lookup_by_name(self):
        """We can look up a subject by its name, without providing an
        identifier."""
        s1 = self._subject(Subject.TAG, "i1")
        s1.name = "A tag"
        eq_((s1, False), Subject.lookup(self._db, Subject.TAG, None, "A tag"))

        # If we somehow get into a state where there are two Subjects
        # with the same name, Subject.lookup treats them as interchangeable.
        s2 = self._subject(Subject.TAG, "i2")
        s2.name = "A tag"

        subject, is_new = Subject.lookup(self._db, Subject.TAG, None, "A tag")
        assert subject in [s1, s2]
        eq_(False, is_new)
        
    def test_assign_to_genre_can_remove_genre(self):
        # Here's a Subject that identifies children's books.
        subject, was_new = Subject.lookup(self._db, Subject.TAG, "Children's books", None)

        # The genre and audience data for this Subject is totally wrong.
        subject.audience = Classifier.AUDIENCE_ADULT
        subject.target_age = NumericRange(1,10)
        subject.fiction = False
        sf, ignore = Genre.lookup(self._db, "Science Fiction")
        subject.genre = sf

        # But calling assign_to_genre() will fix it.
        subject.assign_to_genre()
        eq_(Classifier.AUDIENCE_CHILDREN, subject.audience)
        eq_(NumericRange(None, None, '[]'), subject.target_age)
        eq_(None, subject.genre)
        eq_(None, subject.fiction)


class TestContributor(DatabaseTest):

    def test_marc_code_for_every_role_constant(self):
        """We have determined the MARC Role Code for every role
        that's important enough we gave it a constant in the Contributor
        class.
        """
        for constant, value in Contributor.__dict__.items():
            if not constant.endswith('_ROLE'):
                # Not a constant.
                continue
            assert value in Contributor.MARC_ROLE_CODES

    def test_lookup_by_viaf(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name="Bob", viaf="foo")
        bob2, new = Contributor.lookup(self._db, sort_name="Bob", viaf="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, viaf="foo"))

    def test_lookup_by_lc(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name="Bob", lc="foo")
        bob2, new = Contributor.lookup(self._db, sort_name="Bob", lc="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, lc="foo"))

    def test_lookup_by_viaf_interchangeable(self):
        # Two contributors with the same lc. This shouldn't happen, but 
        # the reason it shouldn't happen is these two people are the same
        # person, so lookup() should just pick one and go with it.
        bob1, new = self._contributor(sort_name="Bob", lc="foo")
        bob2, new = self._contributor()
        bob2.sort_name = "Bob"
        bob2.lc = "foo"
        self._db.commit()
        assert bob1 != bob2
        [some_bob], new = Contributor.lookup(
            self._db, sort_name="Bob", lc="foo"
        )
        eq_(False, new)
        assert some_bob in (bob1, bob2)

    def test_lookup_by_name(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name=u"Bob", lc=u"foo")
        bob2, new = Contributor.lookup(self._db, sort_name=u"Bob", lc=u"bar")

        # Lookup by name finds both of them.
        bobs, new = Contributor.lookup(self._db, sort_name=u"Bob")
        eq_(False, new)
        eq_(["Bob", "Bob"], [x.sort_name for x in bobs])

    def test_create_by_lookup(self):
        [bob1], new = Contributor.lookup(self._db, sort_name=u"Bob")
        eq_("Bob", bob1.sort_name)
        eq_(True, new)

        [bob2], new = Contributor.lookup(self._db, sort_name=u"Bob")
        eq_(bob1, bob2)
        eq_(False, new)

    def test_merge(self):

        # Here's Robert.
        [robert], ignore = Contributor.lookup(self._db, sort_name=u"Robert")
        
        # Here's Bob.
        [bob], ignore = Contributor.lookup(self._db, sort_name=u"Jones, Bob")
        bob.extra[u'foo'] = u'bar'
        bob.aliases = [u'Bobby']
        bob.viaf = u'viaf'
        bob.lc = u'lc'
        bob.display_name = u"Bob Jones"
        bob.family_name = u"Bobb"
        bob.wikipedia_name = u"Bob_(Person)"

        # Each is a contributor to a Edition.
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        roberts_book, ignore = Edition.for_foreign_id(
            self._db, data_source, Identifier.GUTENBERG_ID, u"1")
        roberts_book.add_contributor(robert, Contributor.AUTHOR_ROLE)

        bobs_book, ignore = Edition.for_foreign_id(
            self._db, data_source, Identifier.GUTENBERG_ID, u"10")
        bobs_book.add_contributor(bob, Contributor.AUTHOR_ROLE)

        # In a shocking turn of events, it transpires that "Bob" and
        # "Robert" are the same person. We merge "Bob" into Robert
        # thusly:
        bob.merge_into(robert)

        # 'Bob' is now listed as an alias for Robert, as is Bob's
        # alias.
        eq_([u'Jones, Bob', u'Bobby'], robert.aliases)

        # The extra information associated with Bob is now associated
        # with Robert.
        eq_(u'bar', robert.extra['foo'])

        eq_(u"viaf", robert.viaf)
        eq_(u"lc", robert.lc)
        eq_(u"Bobb", robert.family_name)
        eq_(u"Bob Jones", robert.display_name)
        eq_(u"Robert", robert.sort_name)
        eq_(u"Bob_(Person)", robert.wikipedia_name)

        # The standalone 'Bob' record has been removed from the database.
        eq_(
            [], 
            self._db.query(Contributor).filter(Contributor.sort_name=="Bob").all())

        # Bob's book is now associated with 'Robert', not the standalone
        # 'Bob' record.
        eq_([robert], bobs_book.author_contributors)

        # confirm the sort_name is propagated, if not already set in the destination contributor
        robert.sort_name = None
        [bob], ignore = Contributor.lookup(self._db, sort_name=u"Jones, Bob")
        bob.merge_into(robert)
        eq_(u"Jones, Bob", robert.sort_name)



    def _names(self, in_name, out_family, out_display,
               default_display_name=None):
        f, d = Contributor._default_names(in_name, default_display_name)
        eq_(f, out_family)
        eq_(d, out_display)

    def test_default_names(self):

        # Pass in a default display name and it will always be used.
        self._names("Jones, Bob", "Jones", "Sally Smith",
                    default_display_name="Sally Smith")

        # Corporate names are untouched and get no family name.
        self._names("Bob's Books.", None, "Bob's Books.")
        self._names("Bob's Books, Inc.", None, "Bob's Books, Inc.")
        self._names("Little, Brown &amp; Co.", None, "Little, Brown & Co.")
        self._names("Philadelphia Broad Street Church (Philadelphia, Pa.)",
                    None, "Philadelphia Broad Street Church")

        # Dates and other gibberish after a name is removed.
        self._names("Twain, Mark, 1855-1910", "Twain", "Mark Twain")
        self._names("Twain, Mark, ???-1910", "Twain", "Mark Twain")
        self._names("Twain, Mark, circ. 1900", "Twain", "Mark Twain")
        self._names("Twain, Mark, !@#!@", "Twain", "Mark Twain")
        self._names(
            "Coolbrith, Ina D. 1842?-1928", "Coolbrith", "Ina D. Coolbrith")
        self._names("Caesar, Julius, 1st cent.", "Caesar", "Julius Caesar")
        self._names("Arrian, 2nd cent.", "Arrian", "Arrian")
        self._names("Hafiz, 14th cent.", "Hafiz", "Hafiz")
        self._names("Hormel, Bob 1950?-", "Hormel", "Bob Hormel")
        self._names("Holland, Henry 1583-1650? Monumenta sepulchraria Sancti Pauli",
                    "Holland", "Henry Holland")
        

        # Suffixes stay on the end, except for "Mrs.", which goes
        # to the front.
        self._names("Twain, Mark, Jr.", "Twain", "Mark Twain, Jr.")
        self._names("House, Gregory, M.D.", "House", "Gregory House, M.D.")
        self._names("Twain, Mark, Mrs.", "Twain", "Mrs. Mark Twain")
        self._names("Twain, Mark, Mrs", "Twain", "Mrs Mark Twain")

        # The easy case.
        self._names("Twain, Mark", "Twain", "Mark Twain")
        self._names("Geering, R. G.", "Geering", "R. G. Geering")


    def test_sort_name(self):
        bob, new = get_one_or_create(self._db, Contributor, sort_name=None)
        eq_(None, bob.sort_name)

        bob, ignore = self._contributor(sort_name="Bob Bitshifter")
        bob.sort_name = None
        eq_(None, bob.sort_name)

        bob, ignore = self._contributor(sort_name="Bob Bitshifter")
        eq_("Bitshifter, Bob", bob.sort_name)

        bob, ignore = self._contributor(sort_name="Bitshifter, Bob")
        eq_("Bitshifter, Bob", bob.sort_name)

        # test that human name parser doesn't die badly on foreign names
        bob, ignore = self._contributor(sort_name=u"Боб  Битшифтер")
        eq_(u"Битшифтер, Боб", bob.sort_name)



class TestEdition(DatabaseTest):

    def test_license_pools(self):
        # Here are two collections that provide access to the same book.
        c1 = self._collection()
        c2 = self._collection()
        
        edition, lp1 = self._edition(with_license_pool=True)
        lp2 = self._licensepool(edition=edition, collection=c2)

        # Two LicensePools for the same work.
        eq_(lp1.identifier, lp2.identifier)
        
        # Edition.license_pools contains both.
        eq_(set([lp1, lp2]), set(edition.license_pools))

    def test_author_contributors(self):
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = self._str
        type = Identifier.GUTENBERG_ID

        edition, was_new = Edition.for_foreign_id(
            self._db, data_source, type, id
        )

        # We've listed the same person as primary author and author.
        [alice], ignore = Contributor.lookup(self._db, "Adder, Alice")
        edition.add_contributor(
            alice, [Contributor.AUTHOR_ROLE, Contributor.PRIMARY_AUTHOR_ROLE]
        )

        # We've listed a different person as illustrator.
        [bob], ignore = Contributor.lookup(self._db, "Bitshifter, Bob")
        edition.add_contributor(bob, [Contributor.ILLUSTRATOR_ROLE])

        # Both contributors show up in .contributors.
        eq_(set([alice, bob]), edition.contributors)

        # Only the author shows up in .author_contributors, and she
        # only shows up once.
        eq_([alice], edition.author_contributors)

    def test_for_foreign_id(self):
        """Verify we can get a data source's view of a foreign id."""
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = "549"
        type = Identifier.GUTENBERG_ID

        record, was_new = Edition.for_foreign_id(
            self._db, data_source, type, id)
        eq_(data_source, record.data_source)
        identifier = record.primary_identifier
        eq_(id, identifier.identifier)
        eq_(type, identifier.type)
        eq_(True, was_new)
        eq_([identifier], record.equivalent_identifiers())

        # We can get the same work record by providing only the name
        # of the data source.
        record, was_new = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, type, id)
        eq_(data_source, record.data_source)
        eq_(identifier, record.primary_identifier)
        eq_(False, was_new)

    def test_missing_coverage_from(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "1")

        g2, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        c1 = self._coverage_record(g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        c2 = self._coverage_record(g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            self._db, web, Identifier.URI, "http://www.foo.com/")

        # missing_coverage_from picks up the Gutenberg record with no
        # coverage from OCLC. It doesn't pick up the other
        # Gutenberg record, and it doesn't pick up the web record.
        [in_gutenberg_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, gutenberg, oclc).all()

        eq_(g2, in_gutenberg_but_not_in_oclc)

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.
        [has_generic_coverage_only] = Edition.missing_coverage_from(
            self._db, gutenberg, oclc, "some operation").all()
        eq_(g1, has_generic_coverage_only)

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, web, oclc).all()
        eq_(w, in_web_but_not_in_oclc)

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        eq_([g1.id, g2.id], sorted([x.id for x in Edition.missing_coverage_from(
            self._db, gutenberg, web)]))

    def test_sort_by_priority(self):
        edition_admin = self._edition(data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        edition_od, pool = self._edition(data_source_name=DataSource.OVERDRIVE, with_license_pool=True)
        edition_mw = self._edition(data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)

        # Unset edition_no_data_source's data source
        edition_no_data_source = self._edition(with_license_pool=False)
        edition_no_data_source.data_source = None

        editions_correct = (edition_no_data_source, edition_od, edition_mw, edition_admin)

        # Give all the editions the same identifier and sort them.
        identifier = pool.identifier
        for edition in editions_correct:
            edition.primary_identifier = identifier
        editions_contender = Edition.sort_by_priority(identifier.primarily_identifies)

        eq_(len(editions_correct), len(editions_contender))
        for index, edition in enumerate(editions_correct):
            eq_(editions_contender[index].title, edition.title)

    def test_equivalent_identifiers(self):

        edition = self._edition()
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.OCLC)

        identifier.equivalent_to(data_source, edition.primary_identifier, 0.6)

        eq_(set([identifier, edition.primary_identifier]),
            set(edition.equivalent_identifiers(threshold=0.5)))

        eq_(set([edition.primary_identifier]),
            set(edition.equivalent_identifiers(threshold=0.7)))

    def test_recursive_edition_equivalence(self):

        # Here's a Edition for a Project Gutenberg text.
        gutenberg, gutenberg_pool = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Original Gutenberg text")

        # Here's a Edition for an Open Library text.
        open_library, open_library_pool = self._edition(
            data_source_name=DataSource.OPEN_LIBRARY,
            identifier_type=Identifier.OPEN_LIBRARY_ID,
            identifier_id="W1111",
            with_open_access_download=True,
            title="Open Library record")

        # We've learned from OCLC Classify that the Gutenberg text is
        # equivalent to a certain OCLC Number. We've learned from OCLC
        # Linked Data that the Open Library text is equivalent to the
        # same OCLC Number.
        oclc_classify = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_linked_data = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)

        oclc_number, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "22")
        gutenberg.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        open_library.primary_identifier.equivalent_to(
            oclc_linked_data, oclc_number, 1)
       
        # Here's a Edition for a Recovering the Classics cover.
        web_source = DataSource.lookup(self._db, DataSource.WEB)
        recovering, ignore = Edition.for_foreign_id(
            self._db, web_source, Identifier.URI, 
            "http://recoveringtheclassics.com/pride-and-prejudice.jpg")
        recovering.title = "Recovering the Classics cover"

        # We've manually associated that Edition's URI directly
        # with the Project Gutenberg text.
        manual = DataSource.lookup(self._db, DataSource.MANUAL)
        gutenberg.primary_identifier.equivalent_to(
            manual, recovering.primary_identifier, 1)

        # Finally, here's a completely unrelated Edition, which
        # will not be showing up.
        gutenberg2, gutenberg2_pool = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="2",
            with_open_access_download=True,
            title="Unrelated Gutenberg record.")

        # When we call equivalent_editions on the Project Gutenberg
        # Edition, we get three Editions: the Gutenberg record
        # itself, the Open Library record, and the Recovering the
        # Classics record.
        #
        # We get the Open Library record because it's associated with
        # the same OCLC Number as the Gutenberg record. We get the
        # Recovering the Classics record because it's associated
        # directly with the Gutenberg record.
        results = list(gutenberg.equivalent_editions())
        eq_(3, len(results))
        assert gutenberg in results
        assert open_library in results
        assert recovering in results

        # Here's a Work that incorporates one of the Gutenberg records.
        work = self._work()
        work.license_pools.extend([gutenberg2_pool])

        # Its set-of-all-editions contains only one record.
        eq_(1, work.all_editions().count())

        # If we add the other Gutenberg record to it, then its
        # set-of-all-editions is extended by that record, *plus*
        # all the Editions equivalent to that record.
        work.license_pools.extend([gutenberg_pool])
        eq_(4, work.all_editions().count())

    def test_calculate_presentation_title(self):
        wr = self._edition(title="The Foo")
        wr.calculate_presentation()
        eq_("Foo, The", wr.sort_title)

        wr = self._edition(title="A Foo")
        wr.calculate_presentation()
        eq_("Foo, A", wr.sort_title)

    def test_calculate_presentation_missing_author(self):
        wr = self._edition()
        self._db.delete(wr.contributions[0])
        self._db.commit()
        wr.calculate_presentation()
        eq_(u"[Unknown]", wr.sort_author)
        eq_(u"[Unknown]", wr.author)

    def test_calculate_presentation_author(self):
        bob, ignore = self._contributor(sort_name="Bitshifter, Bob")
        wr = self._edition(authors=bob.sort_name)
        wr.calculate_presentation()
        eq_("Bob Bitshifter", wr.author)
        eq_("Bitshifter, Bob", wr.sort_author)

        bob.display_name="Bob A. Bitshifter"
        wr.calculate_presentation()
        eq_("Bob A. Bitshifter", wr.author)
        eq_("Bitshifter, Bob", wr.sort_author)

        kelly, ignore = self._contributor(sort_name="Accumulator, Kelly")
        wr.add_contributor(kelly, Contributor.AUTHOR_ROLE)
        wr.calculate_presentation()
        eq_("Kelly Accumulator, Bob A. Bitshifter", wr.author)
        eq_("Accumulator, Kelly ; Bitshifter, Bob", wr.sort_author)

    def test_set_summary(self):
        e, pool = self._edition(with_license_pool=True)
        work = self._work(presentation_edition=e)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # Set the work's summmary.
        l1, new = pool.add_link(Hyperlink.DESCRIPTION, None, overdrive, "text/plain",
                      "F")
        work.set_summary(l1.resource)

        eq_(l1.resource, work.summary)
        eq_("F", work.summary_text)

        # Remove the summary.
        work.set_summary(None)
        
        eq_(None, work.summary)
        eq_("", work.summary_text)

    def test_calculate_evaluate_summary_quality_with_privileged_data_sources(self):
        e, pool = self._edition(with_license_pool=True)
        oclc = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # There's a perfunctory description from Overdrive.
        l1, new = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, overdrive, "text/plain",
                      "F")

        overdrive_resource = l1.resource

        # There's a much better description from OCLC Linked Data.
        l2, new = pool.add_link(Hyperlink.DESCRIPTION, None, oclc, "text/plain",
                      """Nothing about working with his former high school crush, Stephanie Stephens, is ideal. Still, if Aaron Caruthers intends to save his grandmother's bakery, he must. Good thing he has a lot of ideas he can't wait to implement. He never imagines Stephanie would have her own ideas for the business. Or that they would clash with his!""")
        oclc_resource = l2.resource

        # In a head-to-head evaluation, the OCLC Linked Data description wins.
        ids = [e.primary_identifier.id]
        champ1, resources = Identifier.evaluate_summary_quality(self._db, ids)

        eq_(set([overdrive_resource, oclc_resource]), set(resources))
        eq_(oclc_resource, champ1)

        # But if we say that Overdrive is the privileged data source, it wins
        # automatically. The other resource isn't even considered.
        champ2, resources2 = Identifier.evaluate_summary_quality(
            self._db, ids, [overdrive])
        eq_(overdrive_resource, champ2)
        eq_([overdrive_resource], resources2)

        # If we say that some other data source is privileged, and
        # there are no descriptions from that data source, a
        # head-to-head evaluation is performed, and OCLC Linked Data
        # wins.
        threem = DataSource.lookup(self._db, DataSource.THREEM)
        champ3, resources3 = Identifier.evaluate_summary_quality(
            self._db, ids, [threem])
        eq_(set([overdrive_resource, oclc_resource]), set(resources3))
        eq_(oclc_resource, champ3)

        # If there are two privileged data sources and there's no
        # description from the first, the second is used.
        champ4, resources4 = Identifier.evaluate_summary_quality(
            self._db, ids, [threem, overdrive])
        eq_([overdrive_resource], resources4)
        eq_(overdrive_resource, champ4)

        # Even an empty string wins if it's from the most privileged data source.
        # This is not a silly example.  The librarian may choose to set the description 
        # to an empty string in the admin inteface, to override a bad overdrive/etc. description.
        staff = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        l3, new = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, staff, "text/plain", "")
        staff_resource = l3.resource

        champ5, resources5 = Identifier.evaluate_summary_quality(
            self._db, ids, [staff, overdrive])
        eq_([staff_resource], resources5)
        eq_(staff_resource, champ5)

    def test_calculate_presentation_cover(self):
        # Here's a cover image with a thumbnail.
        representation, ignore = get_one_or_create(self._db, Representation, url="http://cover")
        representation.media_type = Representation.JPEG_MEDIA_TYPE
        representation.mirrored_at = datetime.datetime.now()
        representation.mirror_url = "http://mirror/cover"
        thumb, ignore = get_one_or_create(self._db, Representation, url="http://thumb")
        thumb.media_type = Representation.JPEG_MEDIA_TYPE
        thumb.mirrored_at = datetime.datetime.now()
        thumb.mirror_url = "http://mirror/thumb"
        thumb.thumbnail_of_id = representation.id

        # Verify that a cover for the edition's primary identifier is used.
        e, pool = self._edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://cover", e.data_source)
        link.resource.representation = representation
        e.calculate_presentation()
        eq_("http://mirror/cover", e.cover_full_url)
        eq_("http://mirror/thumb", e.cover_thumbnail_url)

        # Verify that a cover will be used even if it's some
        # distance away along the identifier-equivalence line.
        e, pool = self._edition(with_license_pool=True)
        oclc_classify = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_number, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "22")
        e.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        link, ignore = oclc_number.add_link(Hyperlink.IMAGE, "http://cover", oclc_classify)
        link.resource.representation = representation
        e.calculate_presentation()
        eq_("http://mirror/cover", e.cover_full_url)
        eq_("http://mirror/thumb", e.cover_thumbnail_url)

        # Verify that a nearby cover takes precedence over a
        # faraway cover.
        link, ignore = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://nearby-cover", e.data_source)
        nearby, ignore = get_one_or_create(self._db, Representation, url=link.resource.url)
        nearby.media_type = Representation.JPEG_MEDIA_TYPE
        nearby.mirrored_at = datetime.datetime.now()
        nearby.mirror_url = "http://mirror/nearby-cover"
        link.resource.representation = nearby
        nearby_thumb, ignore = get_one_or_create(self._db, Representation, url="http://nearby-thumb")
        nearby_thumb.media_type = Representation.JPEG_MEDIA_TYPE
        nearby_thumb.mirrored_at = datetime.datetime.now()
        nearby_thumb.mirror_url = "http://mirror/nearby-thumb"
        nearby_thumb.thumbnail_of_id = nearby.id
        e.calculate_presentation()
        eq_("http://mirror/nearby-cover", e.cover_full_url)
        eq_("http://mirror/nearby-thumb", e.cover_thumbnail_url)

        # Verify that a thumbnail is used even if there's
        # no full-sized cover.
        e, pool = self._edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(Hyperlink.THUMBNAIL_IMAGE, "http://thumb", e.data_source)
        link.resource.representation = thumb
        e.calculate_presentation()
        eq_(None, e.cover_full_url)
        eq_("http://mirror/thumb", e.cover_thumbnail_url)


    def test_calculate_presentation_registers_coverage_records(self):
        edition = self._edition()
        identifier = edition.primary_identifier

        # This Identifier has no CoverageRecords.
        eq_([], identifier.coverage_records)

        # But once we calculate the Edition's presentation...
        edition.calculate_presentation()

        # Two CoverageRecords have been associated with this Identifier.
        records = identifier.coverage_records

        # One for setting the Edition metadata and one for choosing
        # the Edition's cover.
        expect = set([
            CoverageRecord.SET_EDITION_METADATA_OPERATION,
            CoverageRecord.CHOOSE_COVER_OPERATION]
        )
        eq_(expect, set([x.operation for x in records]))

        # We know the records are associated with this specific
        # Edition, not just the Identifier, because each
        # CoverageRecord's DataSource is set to this Edition's
        # DataSource.
        eq_(
            [edition.data_source, edition.data_source], 
            [x.data_source for x in records]
        )

    def test_no_permanent_work_id_for_edition_with_no_title(self):
        """An edition with no title is not assigned a permanent work ID."""
        edition = self._edition()
        edition.title = ''
        eq_(None, edition.permanent_work_id)
        edition.calculate_permanent_work_id()
        eq_(None, edition.permanent_work_id)
        edition.title = u'something'
        edition.calculate_permanent_work_id()
        assert_not_equal(None, edition.permanent_work_id)

    def test_choose_cover_can_choose_full_image_and_thumbnail_separately(self):
        edition = self._edition()

        # This edition has a full-sized image and a thumbnail image,
        # but there is no evidence that they are the _same_ image.
        main_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.IMAGE, "http://main/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumbnail/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )

        # Nonetheless, Edition.choose_cover() will assign the
        # potentially unrelated images to the Edition, because there
        # is no better option.
        edition.choose_cover()
        eq_(main_image.resource.url, edition.cover_full_url)
        eq_(thumbnail_image.resource.url, edition.cover_thumbnail_url)

        # If there is a clear indication that one of the thumbnails
        # associated with the identifier is a thumbnail _of_ the
        # full-sized image...
        thumbnail_2, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumbnail2/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_2.resource.representation.thumbnail_of = main_image.resource.representation
        edition.choose_cover()
        
        # ...That thumbnail will be chosen in preference to the
        # possibly unrelated thumbnail.
        eq_(main_image.resource.url, edition.cover_full_url)
        eq_(thumbnail_2.resource.url, edition.cover_thumbnail_url)


class TestLicensePool(DatabaseTest):

    def test_for_foreign_id(self):
        """Verify we can get a LicensePool for a data source, an 
        appropriate work identifier, and a Collection."""
        now = datetime.datetime.utcnow()
        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=self._collection()
        )
        assert (pool.availability_time - now).total_seconds() < 2
        eq_(True, was_new)
        eq_(DataSource.GUTENBERG, pool.data_source.name)
        eq_(Identifier.GUTENBERG_ID, pool.identifier.type)
        eq_("541", pool.identifier.identifier)        
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

    def test_for_foreign_id_fails_when_no_collection_provided(self):
        """We cannot create a LicensePool that is not associated
        with some Collection.
        """
        assert_raises(
            CollectionMissing,
            LicensePool.for_foreign_id,
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=None
        )

    def test_no_license_pool_for_non_primary_identifier(self):
        """Overdrive offers licenses, but to get an Overdrive license pool for
        a book you must identify the book by Overdrive's primary
        identifier, not some other kind of identifier.
        """
        collection = self._collection()
        assert_raises_regexp(
            ValueError, 
            "License pools for data source 'Overdrive' are keyed to identifier type 'Overdrive ID' \(not 'ISBN', which was provided\)",
            LicensePool.for_foreign_id,
            self._db, DataSource.OVERDRIVE, Identifier.ISBN, "{1-2-3}",
            collection=collection
        )

    def test_licensepools_for_same_identifier_have_same_presentation_edition(self):
        """Two LicensePools for the same Identifier will get the same
        presentation edition.
        """
        identifier = self._identifier()
        edition1, pool1 = self._edition(
            with_license_pool=True, data_source_name=DataSource.GUTENBERG,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        edition2, pool2 = self._edition(
            with_license_pool=True, data_source_name=DataSource.UNGLUE_IT,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        pool1.set_presentation_edition()
        pool2.set_presentation_edition()
        eq_(pool1.presentation_edition, pool2.presentation_edition)
        
    def test_collection_datasource_identifier_must_be_unique(self):
        """You can't have two LicensePools with the same Collection,
        DataSource, and Identifier.
        """
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = self._identifier()
        collection = self._default_collection
        pool = create(
            self._db,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )

        assert_raises(
            IntegrityError,
            create,
            self._db,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )        
        
    def test_with_no_work(self):
        p1, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=self._default_collection
        )

        p2, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, "2",
            collection=self._default_collection
        )

        work = self._work(title="Foo")
        p1.work = work
        
        assert p1 in work.license_pools

        eq_([p2], LicensePool.with_no_work(self._db))

    def test_update_availability(self):
        work = self._work(with_license_pool=True)
        work.last_update_time = None

        [pool] = work.license_pools
        pool.update_availability(30, 20, 2, 0)
        eq_(30, pool.licenses_owned)
        eq_(20, pool.licenses_available)
        eq_(2, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

    def test_update_availability_triggers_analytics(self):
        work = self._work(with_license_pool=True)
        [pool] = work.license_pools
        provider = MockAnalyticsProvider()
        pool.update_availability(30, 20, 2, 0, analytics=provider)
        count = provider.count
        pool.update_availability(30, 21, 2, 0, analytics=provider)
        eq_(count + 1, provider.count)
        eq_(CirculationEvent.DISTRIBUTOR_CHECKIN, provider.event_type)
        pool.update_availability(30, 21, 2, 1, analytics=provider)
        eq_(count + 2, provider.count)
        eq_(CirculationEvent.DISTRIBUTOR_HOLD_PLACE, provider.event_type)

    def test_update_availability_does_nothing_if_given_no_data(self):
        """Passing an empty set of data into update_availability is
        a no-op.
        """

        # Set up a Work.
        work = self._work(with_license_pool=True)
        work.last_update_time = None

        # Set up a LicensePool.
        [pool] = work.license_pools
        pool.last_checked = None
        pool.licenses_owned = 10
        pool.licenses_available = 20
        pool.licenses_reserved = 30
        pool.patrons_in_hold_queue = 40

        # Pass empty values into update_availability.
        pool.update_availability(None, None, None, None)

        # The LicensePool's circulation data is what it was before.
        eq_(10, pool.licenses_owned)
        eq_(20, pool.licenses_available)
        eq_(30, pool.licenses_reserved)
        eq_(40, pool.patrons_in_hold_queue)

        # Work.update_time and LicensePool.last_checked are unaffected.
        eq_(None, work.last_update_time)
        eq_(None, pool.last_checked)

        # If we pass a mix of good and null values...
        pool.update_availability(5, None, None, None)

        # Only the good values are changed.
        eq_(5, pool.licenses_owned)
        eq_(20, pool.licenses_available)
        eq_(30, pool.licenses_reserved)
        eq_(40, pool.patrons_in_hold_queue)


    def test_open_access_links(self):
        edition, pool = self._edition(with_open_access_download=True)
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        [oa1] = list(pool.open_access_links)

        # We have one open-access download, let's
        # add another.
        url = self._url
        media_type = Representation.EPUB_MEDIA_TYPE
        link2, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url,
            source, media_type
        )
        oa2 = link2.resource

        # And let's add a link that's not an open-access download.
        url = self._url
        image, new = pool.identifier.add_link(
            Hyperlink.IMAGE, url, source, Representation.JPEG_MEDIA_TYPE
        )
        self._db.commit()

        # Only the two open-access download links show up.
        eq_(set([oa1, oa2]), set(pool.open_access_links))

    def test_better_open_access_pool_than(self):

        gutenberg_1 = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )

        gutenberg_2 = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )
        
        assert int(gutenberg_1.identifier.identifier) < int(gutenberg_2.identifier.identifier)

        standard_ebooks = self._licensepool(
            None, open_access=True, data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=True
        )

        # Make sure Feedbooks data source exists -- it's not created
        # by default.
        feedbooks_data_source = DataSource.lookup(
            self._db, DataSource.FEEDBOOKS, autocreate=True
        )
        feedbooks = self._licensepool(
            None, open_access=True, data_source_name=DataSource.FEEDBOOKS,
            with_open_access_download=True
        )
        
        overdrive = self._licensepool(
            None, open_access=False, data_source_name=DataSource.OVERDRIVE
        )

        suppressed = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG
        )
        suppressed.suppressed = True

        def better(x,y):
            return x.better_open_access_pool_than(y)

        # We would rather have nothing at all than a suppressed
        # LicensePool.
        eq_(False, better(suppressed, None))

        # A non-open-access LicensePool is not considered at all.
        eq_(False, better(overdrive, None))

        # Something is better than nothing.
        eq_(True, better(gutenberg_1, None))

        # An open access book from a high-quality source beats one
        # from a low-quality source.
        eq_(True, better(standard_ebooks, gutenberg_1))
        eq_(True, better(feedbooks, gutenberg_1))
        eq_(False, better(gutenberg_1, standard_ebooks))
        
        # A high Gutenberg number beats a low Gutenberg number.
        eq_(True, better(gutenberg_2, gutenberg_1))
        eq_(False, better(gutenberg_1, gutenberg_2))

        # If a supposedly open-access LicensePool doesn't have an
        # open-access download resource, it will only be considered if
        # there is no other alternative.
        no_resource = self._licensepool(
            None, open_access=True, 
            data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=False,
        )
        no_resource.open_access = True
        eq_(True, better(no_resource, None))
        eq_(False, better(no_resource, gutenberg_1))

    def test_with_complaint(self):
        library = self._default_library
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)
        type3 = next(type)

        work1 = self._work(
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        lp1 = work1.license_pools[0]
        lp1_complaint1 = self._complaint(
            lp1,
            type1,
            "lp1 complaint1 source",
            "lp1 complaint1 detail")
        lp1_complaint2 = self._complaint(
            lp1,
            type1,
            "lp1 complaint2 source",
            "lp1 complaint2 detail")
        lp1_complaint3 = self._complaint(
            lp1,
            type2,
            "work1 complaint3 source",
            "work1 complaint3 detail")
        lp1_resolved_complaint = self._complaint(
            lp1,
            type3,
            "work3 resolved complaint source",
            "work3 resolved complaint detail",
            datetime.datetime.now())

        work2 = self._work(
            "nonfiction work with complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)
        lp2 = work2.license_pools[0]
        lp2_complaint1 = self._complaint(
            lp2,
            type2,
            "work2 complaint1 source",
            "work2 complaint1 detail")
        lp2_resolved_complaint = self._complaint(
            lp2,
            type2,
            "work2 resolved complaint source",
            "work2 resolved complaint detail",
            datetime.datetime.now())
        
        work3 = self._work(
            "fiction work without complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        lp3 = work3.license_pools[0]
        lp3_resolved_complaint = self._complaint(
            lp3,
            type3,
            "work3 resolved complaint source",
            "work3 resolved complaint detail",
            datetime.datetime.now())

        work4 = self._work(
            "nonfiction work without complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)

        # excludes resolved complaints by default
        results = LicensePool.with_complaint(library).all()

        eq_(2, len(results))
        eq_(lp1.id, results[0][0].id)
        eq_(3, results[0][1])
        eq_(lp2.id, results[1][0].id)
        eq_(1, results[1][1])

        # include resolved complaints this time
        more_results = LicensePool.with_complaint(library, resolved=None).all()

        eq_(3, len(more_results))
        eq_(lp1.id, more_results[0][0].id)
        eq_(4, more_results[0][1])
        eq_(lp2.id, more_results[1][0].id)
        eq_(2, more_results[1][1])
        eq_(lp3.id, more_results[2][0].id)
        eq_(1, more_results[2][1])

        # show only resolved complaints
        resolved_results = LicensePool.with_complaint(
            library, resolved=True).all()
        lp_ids = set([result[0].id for result in resolved_results])
        counts = set([result[1] for result in resolved_results])
        
        eq_(3, len(resolved_results))
        eq_(lp_ids, set([lp1.id, lp2.id, lp3.id]))
        eq_(counts, set([1]))

        # This library has none of the license pools that have complaints,
        # so passing it in to with_complaint() gives no results.
        library2 = self._library()
        eq_(0, LicensePool.with_complaint(library2).count())

        # If we add the default library's collection to this new library,
        # we start getting the same results.
        library2.collections.extend(library.collections)
        eq_(3, LicensePool.with_complaint(library2, resolved=None).count())

    def test_set_presentation_edition(self):
        """
        Make sure composite edition creation makes good choices when combining 
        field data from provider, metadata wrangler, admin interface, etc. editions.
        """
        # create different types of editions, all with the same identifier
        edition_admin = self._edition(data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        edition_mw = self._edition(data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)
        edition_od, pool = self._edition(data_source_name=DataSource.OVERDRIVE, with_license_pool=True)

        edition_mw.primary_identifier = pool.identifier
        edition_admin.primary_identifier = pool.identifier

        # set overlapping fields on editions
        edition_od.title = u"OverdriveTitle1"

        edition_mw.title = u"MetadataWranglerTitle1"
        edition_mw.subtitle = u"MetadataWranglerSubTitle1"

        edition_admin.title = u"AdminInterfaceTitle1"

        pool.set_presentation_edition()

        edition_composite = pool.presentation_edition

        assert_not_equal(edition_mw, edition_od)
        assert_not_equal(edition_od, edition_admin)
        assert_not_equal(edition_admin, edition_composite)
        assert_not_equal(edition_od, edition_composite)

        # make sure admin pool data had precedence
        eq_(edition_composite.title, u"AdminInterfaceTitle1")
        eq_(edition_admin.contributors, edition_composite.contributors)

        # make sure data not present in the higher-precedence editions didn't overwrite the lower-precedented editions' fields
        eq_(edition_composite.subtitle, u"MetadataWranglerSubTitle1")
        [license_pool] = edition_composite.is_presentation_for
        eq_(license_pool, pool)

        # Change the admin interface's opinion about who the author
        # is.
        for c in edition_admin.contributions:
            self._db.delete(c)
        self._db.commit()
        [jane], ignore = Contributor.lookup(self._db, u"Doe, Jane")
        jane.family_name, jane.display_name = jane.default_names()
        edition_admin.add_contributor(jane, Contributor.AUTHOR_ROLE)
        pool.set_presentation_edition()

        # The old contributor has been removed from the composite
        # edition, and the new contributor added.
        eq_(set([jane]), edition_composite.contributors)

    def test_circulation_changelog(self):
        
        edition, pool = self._edition(with_license_pool=True)
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 8
        pool.patrons_in_hold_queue = 7

        msg, args = pool.circulation_changelog(1, 2, 3, 4)

        # Since all four circulation values changed, the message is as
        # long as it could possibly get.
        eq_(
            'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s %s: %s=>%s %s: %s=>%s %s: %s=>%s',
            msg
        )
        eq_(
            args,
            (edition.medium, edition.title, edition.author, 
             pool.identifier.type, pool.identifier.identifier,
             'OWN', 1, 10, 'AVAIL', 2, 9, 'RSRV', 3, 8, 'HOLD', 4, 7)
        )

        # If only one circulation value changes, the message is a lot shorter.
        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        eq_(
            'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s',
            msg
        )
        eq_(
            args,
            (edition.medium, edition.title, edition.author, 
             pool.identifier.type, pool.identifier.identifier,
             'HOLD', 15, 7)
        )

        # This works even if, for whatever reason, the edition's
        # bibliographic data is missing.
        edition.title = None
        edition.author = None
        
        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        eq_("[NO TITLE]", args[1])
        eq_("[NO AUTHOR]", args[2])

    def test_update_availability_from_delta(self):
        """A LicensePool may have its availability information updated based
        on a single observed change.
        """

        edition, pool = self._edition(with_license_pool=True)
        eq_(None, pool.last_checked)
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)

        add = CirculationEvent.DISTRIBUTOR_LICENSE_ADD
        checkout = CirculationEvent.DISTRIBUTOR_CHECKOUT
        analytics = MockAnalyticsProvider()
        eq_(0, analytics.count)

        # This observation has no timestamp, but the pool has no
        # history, so we process it.
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        eq_(None, pool.last_checked)
        eq_(2, pool.licenses_owned)
        eq_(2, pool.licenses_available)

        # Processing triggered two analytics events -- one for creating
        # the license pool and one for making it available.
        eq_(2, analytics.count)

        # Now the pool has a history, and we can't fit an undated
        # observation into that history, so undated observations
        # have no effect on circulation data.
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        pool.last_checked = yesterday
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        eq_(2, pool.licenses_owned)
        eq_(yesterday, pool.last_checked)

        # However, outdated events are passed on to analytics so that
        # we record the fact that they happened... at some point.
        eq_(3, analytics.count)

        # This observation is more recent than the last time the pool
        # was checked, so it's processed and the last check time is
        # updated.
        pool.update_availability_from_delta(checkout, now, 1, analytics)
        eq_(2, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(now, pool.last_checked)
        eq_(4, analytics.count)

        # This event is less recent than the last time the pool was
        # checked, so it's ignored. Processing it is likely to do more
        # harm than good.
        pool.update_availability_from_delta(add, yesterday, 1, analytics)
        eq_(2, pool.licenses_owned)
        eq_(now, pool.last_checked)

        # It's still logged to analytics, though.
        eq_(5, analytics.count)

        # This event is new but does not actually cause the
        # circulation to change at all.
        pool.update_availability_from_delta(add, now, 0, analytics)
        eq_(2, pool.licenses_owned)
        eq_(now, pool.last_checked)

        # We still send the analytics event.
        eq_(6, analytics.count)

    def test_calculate_change_from_one_event(self):
        """Test the internal method called by update_availability_from_delta."""
        CE = CirculationEvent

        # Create a LicensePool with a large number of available licenses.
        edition, pool = self._edition(with_license_pool=True)
        pool.licenses_owned = 5
        pool.licenses_available = 4
        pool.licenses_reserved = 0
        pool.patrons_in_hold_queue = 0

        # Calibrate _calculate_change_from_one_event by sending it an
        # event that makes no difference. This lets us see what a
        # 'status quo' response from the method would look like.
        calc = pool._calculate_change_from_one_event
        eq_((5,4,0,0), calc(CE.DISTRIBUTOR_CHECKIN, 0))

        # If there ever appear to be more licenses available than
        # owned, the number of owned licenses is left alone. It's
        # possible that we have more licenses than we thought, but
        # it's more likely that a license has expired or otherwise
        # been removed.
        eq_((5,5,0,0), calc(CE.DISTRIBUTOR_CHECKIN, 3))

        # But we don't bump up the number of available licenses just
        # because one becomes available.
        eq_((5,5,0,0), calc(CE.DISTRIBUTOR_CHECKIN, 1))

        # When you signal a hold on a book that's available, we assume
        # that the book has stopped being available.
        eq_((5,0,0,3), calc(CE.DISTRIBUTOR_HOLD_PLACE, 3))

        # If a license stops being owned, it implicitly stops being
        # available. (But we don't know if the license that became
        # unavailable is one of the ones currently checked out to
        # someone, or one of the other ones.)
        eq_((3,3,0,0), calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 2))

        # If a license stops being available, it doesn't stop
        # being owned.
        eq_((5,3,0,0), calc(CE.DISTRIBUTOR_CHECKOUT, 1))

        # None of these numbers will go below zero.
        eq_((0,0,0,0), calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 100))

        # Newly added licenses start out available if there are no
        # patrons in the hold queue.
        eq_((6,5,0,0), calc(CE.DISTRIBUTOR_LICENSE_ADD, 1))

        # Now let's run some tests with a LicensePool that has a large holds
        # queue.
        pool.licenses_owned = 5
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        pool.patrons_in_hold_queue = 3        
        eq_((5,0,1,3), calc(CE.DISTRIBUTOR_HOLD_PLACE, 0))

        # When you signal a hold on a book that already has holds, it
        # does nothing but increase the number of patrons in the hold
        # queue.
        eq_((5,0,1,6), calc(CE.DISTRIBUTOR_HOLD_PLACE, 3))

        # A checkin event has no effect...
        eq_((5,0,1,3), calc(CE.DISTRIBUTOR_CHECKIN, 1))

        # ...because it's presumed that it will be followed by an
        # availability notification event, which takes a patron off
        # the hold queue and adds them to the reserved list.
        eq_((5,0,2,2), calc(CE.DISTRIBUTOR_AVAILABILITY_NOTIFY, 1))

        # The only exception is if the checkin event wipes out the
        # entire holds queue, in which case the number of available
        # licenses increases.  (But nothing else changes -- we're
        # still waiting for the availability notification events.)
        eq_((5,3,1,3), calc(CE.DISTRIBUTOR_CHECKIN, 6))

        # Again, note that even though six copies were checked in,
        # we're not assuming we own more licenses than we
        # thought. It's more likely that the sixth license expired and
        # we weren't notified.

        # When there are no licenses available, a checkout event
        # draws from the pool of licenses reserved instead.
        eq_((5,0,0,3), calc(CE.DISTRIBUTOR_CHECKOUT, 2))

        # Newly added licenses do not start out available if there are
        # patrons in the hold queue.
        eq_((6,0,1,3), calc(CE.DISTRIBUTOR_LICENSE_ADD, 1))


class TestLicensePoolDeliveryMechanism(DatabaseTest):

    def test_lpdm_change_may_change_open_access_status(self):
        # Here's a book that's not open access.
        edition, pool = self._edition(with_license_pool=True)
        eq_(False, pool.open_access)

        # We're going to use LicensePoolDeliveryMechanism.set to
        # to give it a non-open-access LPDM.
        data_source = pool.data_source
        identifier = pool.identifier
        content_type = Representation.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.IN_COPYRIGHT
        )

        # Now there's a way to get the book, but it's not open access.
        eq_(False, pool.open_access)

        # Now give it an open-access LPDM.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
            data_source, content_type
        )
        oa_lpdm = LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.GENERIC_OPEN_ACCESS, link.resource
        )
        
        # Now it's open access.
        eq_(True, pool.open_access)

        # Delete the open-access LPDM, and it stops being open access.
        oa_lpdm.delete()
        eq_(False, pool.open_access)
        
    def test_set_rights_status(self):
        # Here's a non-open-access book.
        edition, pool = self._edition(with_license_pool=True)
        pool.open_access = False
        [lpdm] = pool.delivery_mechanisms

        # We set its rights status to 'in copyright', and nothing changes.
        uri = RightsStatus.IN_COPYRIGHT
        status = lpdm.set_rights_status(uri)
        eq_(status, lpdm.rights_status)
        eq_(uri, status.uri)
        eq_(RightsStatus.NAMES.get(uri), status.name)
        eq_(False, pool.open_access)

        # Setting it again won't change anything.
        status2 = lpdm.set_rights_status(uri)
        eq_(status, status2)

        # Set the rights status to a different URL, we change to a different
        # RightsStatus object.
        uri2 = "http://unknown"
        status3 = lpdm.set_rights_status(uri2)
        assert status != status3
        eq_(RightsStatus.UNKNOWN, status3.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.UNKNOWN), status3.name)

        # Set the rights status to a URL that implies open access,
        # and the status of the LicensePool is changed.
        open_access_uri = RightsStatus.GENERIC_OPEN_ACCESS
        open_access_status = lpdm.set_rights_status(open_access_uri)
        eq_(open_access_uri, open_access_status.uri)
        eq_(RightsStatus.NAMES.get(open_access_uri), open_access_status.name)
        eq_(True, pool.open_access)

        # Set it back to a URL that does not imply open access, and
        # the status of the LicensePool is changed back.
        non_open_access_status = lpdm.set_rights_status(uri)
        eq_(False, pool.open_access)

        # Now add a second delivery mechanism, so the pool has one
        # open-access and one commercial delivery mechanism.
        lpdm2 = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY, None)
        eq_(2, len(pool.delivery_mechanisms))

        # Now the pool is open access again
        eq_(True, pool.open_access)

        # But if we change the new delivery mechanism to non-open
        # access, the pool won't be open access anymore either.
        lpdm2.set_rights_status(uri)
        eq_(False, pool.open_access)

    def test_uniqueness_constraint(self):
        # with_open_access_download will create a LPDM
        # for the open-access download.
        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        [lpdm] = pool.delivery_mechanisms
        
        # We can create a second LPDM with the same data type and DRM status,
        # so long as the resource is different.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
            pool.data_source, "text/html"
        )
        lpdm2 = pool.set_delivery_mechanism(
            lpdm.delivery_mechanism.content_type,
            lpdm.delivery_mechanism.drm_scheme,
            lpdm.rights_status.uri,
            link.resource,
        )
        eq_(lpdm2.delivery_mechanism, lpdm.delivery_mechanism)
        assert lpdm2.resource != lpdm.resource

    def test_compatible_with(self):
        """Test the rules about which LicensePoolDeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """

        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        [mech] = pool.delivery_mechanisms

        # Test the simple cases.
        eq_(False, mech.compatible_with(None))
        eq_(False, mech.compatible_with("Not a LicensePoolDeliveryMechanism"))
        eq_(True, mech.compatible_with(mech))

        # Now let's set up a scenario that works and then see how it fails.
        self._add_generic_delivery_mechanism(pool)

        # This book has two different LicensePoolDeliveryMechanisms
        # with the same underlying DeliveryMechanism. They're
        # compatible.
        [mech1, mech2] = pool.delivery_mechanisms
        assert mech1.id != mech2.id
        eq_(mech1.delivery_mechanism, mech2.delivery_mechanism)
        eq_(True, mech1.compatible_with(mech2))

        # The LicensePoolDeliveryMechanisms must identify the same
        # book from the same data source.
        mech1.data_source_id = self._id
        eq_(False, mech1.compatible_with(mech2))

        mech1.data_source_id = mech2.data_source_id
        mech1.identifier_id = self._id
        eq_(False, mech1.compatible_with(mech2))
        mech1.identifier_id = mech2.identifier_id

        # The underlying delivery mechanisms don't have to be exactly
        # the same, but they must be compatible.
        pdf_adobe, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )
        mech1.delivery_mechanism = pdf_adobe
        self._db.commit()
        eq_(False, mech1.compatible_with(mech2))

        streaming, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )
        mech1.delivery_mechanism = streaming
        self._db.commit()
        eq_(True, mech1.compatible_with(mech2))

    def test_compatible_with_calls_compatible_with_on_deliverymechanism(self):
        # Create two LicensePoolDeliveryMechanisms with different
        # media types.
        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        self._add_generic_delivery_mechanism(pool)
        [mech1, mech2] = pool.delivery_mechanisms
        mech2.delivery_mechanism, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        self._db.commit()

        eq_(True, mech1.is_open_access)
        eq_(False, mech2.is_open_access)

        # Determining whether the mechanisms are compatible requires
        # calling compatible_with on the first mechanism's
        # DeliveryMechanism, passing in the second DeliveryMechanism
        # plus the answer to 'are both LicensePoolDeliveryMechanisms
        # open-access?'
        class Mock(object):
            called_with = None
            @classmethod
            def compatible_with(cls, other, open_access):
                cls.called_with = (other, open_access)
                return True
        mech1.delivery_mechanism.compatible_with = Mock.compatible_with

        # Call compatible_with, and the mock method is called with the
        # second DeliveryMechanism and (since one of the
        # LicensePoolDeliveryMechanisms is not open-access) the value
        # False.
        mech1.compatible_with(mech2)
        eq_(
            (mech2.delivery_mechanism, False),
            Mock.called_with
        )

        # If both LicensePoolDeliveryMechanisms are open-access,
        # True is passed in instead, so that
        # DeliveryMechanism.compatible_with() applies the less strict
        # compatibility rules for open-access fulfillment.
        mech2.set_rights_status(RightsStatus.GENERIC_OPEN_ACCESS)
        mech1.compatible_with(mech2)
        eq_(
            (mech2.delivery_mechanism, True),
            Mock.called_with
        )

class TestWork(DatabaseTest):

    def test_complaints(self):
        work = self._work(with_license_pool=True)

        [lp1] = work.license_pools
        lp2 = self._licensepool(
            edition=work.presentation_edition,
            data_source_name=DataSource.OVERDRIVE
        )
        lp2.work = work

        complaint_type = random.choice(list(Complaint.VALID_TYPES))
        complaint1, ignore = Complaint.register(
            lp1, complaint_type, "blah", "blah"
        )
        complaint2, ignore = Complaint.register(
            lp2, complaint_type, "blah", "blah"
        )

        # Create a complaint with no association with the work.
        _edition, lp3 = self._edition(with_license_pool=True)
        complaint3, ignore = Complaint.register(
            lp3, complaint_type, "blah", "blah"
        )

        # Only the first two complaints show up in work.complaints.
        eq_(sorted([complaint1.id, complaint2.id]),
            sorted([x.id for x in work.complaints]))

    def test_all_identifier_ids(self):
        work = self._work(with_license_pool=True)
        lp = work.license_pools[0]
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.OCLC)
        identifier.equivalent_to(data_source, lp.identifier, 1)

        # Make sure there aren't duplicates in the list, if an
        # identifier's equivalent to two of the primary identifiers.
        lp2 = self._licensepool(None)
        work.license_pools.append(lp2)
        identifier.equivalent_to(data_source, lp2.identifier, 1)

        all_identifier_ids = work.all_identifier_ids()
        eq_(3, len(all_identifier_ids))
        eq_(set([lp.identifier.id, lp2.identifier.id, identifier.id]),
            set(all_identifier_ids))

    def test_from_identifiers(self):
        # Prep a work to be identified and a work to be ignored.
        work = self._work(with_license_pool=True, with_open_access_download=True)
        lp = work.license_pools[0]
        ignored_work = self._work(with_license_pool=True, with_open_access_download=True)

        # No identifiers returns None.
        result = Work.from_identifiers(self._db, [])
        eq_(None, result)

        # A work can be found according to its identifier.
        identifiers = [lp.identifier]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # When the work has an equivalent identifier.
        isbn = self._identifier(Identifier.ISBN)
        source = lp.data_source
        lp.identifier.equivalent_to(source, isbn, 1)

        # It can be found according to that equivalency.
        identifiers = [isbn]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # Unless the strength is too low.
        lp.identifier.equivalencies[0].strength = 0.8
        identifiers = [isbn]

        result = Work.from_identifiers(self._db, identifiers).all()
        eq_([], result)

        # Two+ of the same or equivalent identifiers lead to one result.
        identifiers = [lp.identifier, isbn, lp.identifier]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # It accepts a base query.
        qu = self._db.query(Work).join(LicensePool).join(Identifier).\
            filter(LicensePool.suppressed)
        identifiers = [lp.identifier]
        result = Work.from_identifiers(self._db, identifiers, base_query=qu).all()
        # Because the work's license_pool isn't suppressed, it isn't returned.
        eq_([], result)

        # It's possible to filter a field other than Identifier.id.
        # Here, we filter based on the value of
        # mv_works_for_lanes.identifier_id.
        from model import MaterializedWorkWithGenre as mw
        qu = self._db.query(mw)
        m = lambda: Work.from_identifiers(
            self._db, [lp.identifier], base_query=qu,
            identifier_id_field=mw.identifier_id
        ).all()
        eq_([], m())
        self.add_to_materialized_view([work, ignored_work])
        eq_([work.id], [x.works_id for x in m()])

    def test_calculate_presentation(self):
        # Test that:
        # - work coverage records are made on work creation and primary edition selection.
        # - work's presentation information (author, title, etc. fields) does a proper job 
        #   of combining fields from underlying editions.
        # - work's presentation information keeps in sync with work's presentation edition.
        # - there can be only one edition that thinks it's the presentation edition for this work.
        # - time stamps are stamped.
        # - higher-standard sources (library staff) can replace, but not delete, authors.
        
        gutenberg_source = DataSource.GUTENBERG
        gitenberg_source = DataSource.PROJECT_GITENBERG

        [bob], ignore = Contributor.lookup(self._db, u"Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()

        edition1, pool1 = self._edition(gitenberg_source, Identifier.GUTENBERG_ID, 
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition1.title = u"The 1st Title"
        edition1.subtitle = u"The 1st Subtitle"
        edition1.add_contributor(bob, Contributor.AUTHOR_ROLE)

        edition2, pool2 = self._edition(gitenberg_source, Identifier.GUTENBERG_ID, 
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition2.title = u"The 2nd Title"
        edition2.subtitle = u"The 2nd Subtitle"
        edition2.add_contributor(bob, Contributor.AUTHOR_ROLE)
        [alice], ignore = Contributor.lookup(self._db, u"Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()
        edition2.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition3, pool3 = self._edition(gutenberg_source, Identifier.GUTENBERG_ID, 
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition3.title = u"The 2nd Title"
        edition3.subtitle = u"The 2nd Subtitle"
        edition3.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition3.add_contributor(alice, Contributor.AUTHOR_ROLE)

        work = self._slow_work(presentation_edition=edition2)
        # add in 3, 2, 1 order to make sure the selection of edition1 as presentation
        # in the second half of the test is based on business logic, not list order.
        for p in pool3, pool1:
            work.license_pools.append(p)

        # The author of the Work is the author of its primary work record.
        eq_("Alice Adder, Bob Bitshifter", work.author)

        # This Work starts out with a single CoverageRecord reflecting the
        # work done to generate its initial OPDS entry, and then it adds choose-edition 
        # as a primary edition is set.
        [choose_edition, generate_opds] = sorted(work.coverage_records, key=lambda x: x.operation)
        assert (generate_opds.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION)
        assert (choose_edition.operation == WorkCoverageRecord.CHOOSE_EDITION_OPERATION)

        # pools aren't yet aware of each other
        eq_(pool1.superceded, False)
        eq_(pool2.superceded, False)
        eq_(pool3.superceded, False)

        work.last_update_time = None
        work.presentation_ready = True
        index = DummyExternalSearchIndex()

        work.calculate_presentation(search_index_client=index)

        # The author of the Work has not changed.
        eq_("Alice Adder, Bob Bitshifter", work.author)

        # one and only one license pool should be un-superceded
        eq_(pool1.superceded, True)
        eq_(pool2.superceded, False)
        eq_(pool3.superceded, True)

        # sanity check
        eq_(work.presentation_edition, pool2.presentation_edition)
        eq_(work.presentation_edition, edition2)

        # editions that aren't the presentation edition have no work
        eq_(edition1.work, None)
        eq_(edition2.work, work)
        eq_(edition3.work, None)

        # The title of the Work is the title of its primary work record.
        eq_("The 2nd Title", work.title)
        eq_("The 2nd Subtitle", work.subtitle)

        # The author of the Work is the author of its primary work record.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

        # The index has not been updated.
        eq_([], index.docs.items())

        # The Work now has a complete set of WorkCoverageRecords
        # associated with it, reflecting all the operations that
        # occured as part of calculate_presentation().
        #
        # All the work has actually been done, except for the work of
        # updating the search index, which has been registered and
        # will be done later.
        records = work.coverage_records

        wcr = WorkCoverageRecord
        success = wcr.SUCCESS
        expect = set([
            (wcr.CHOOSE_EDITION_OPERATION, success),
            (wcr.CLASSIFY_OPERATION, success),
            (wcr.SUMMARY_OPERATION, success),
            (wcr.QUALITY_OPERATION, success),
            (wcr.GENERATE_OPDS_OPERATION, success),
            (wcr.UPDATE_SEARCH_INDEX_OPERATION, wcr.REGISTERED),
        ])
        eq_(expect, set([(x.operation, x.status) for x in records]))

        # Now mark the pool with the presentation edition as suppressed.
        # work.calculate_presentation() will call work.mark_licensepools_as_superceded(), 
        # which will mark the suppressed pool as superceded and take its edition out of the running.
        # Make sure that work's presentation edition and work's author, etc. 
        # fields are updated accordingly, and that the superceded pool's edition 
        # knows it's no longer the champ.
        pool2.suppressed = True
        
        work.calculate_presentation(search_index_client=index)

        # The title of the Work is the title of its new primary work record.
        eq_("The 1st Title", work.title)
        eq_("The 1st Subtitle", work.subtitle)

        # author of composite edition is now just Bob
        eq_("Bob Bitshifter", work.author)
        eq_("Bitshifter, Bob", work.sort_author)

        # sanity check
        eq_(work.presentation_edition, pool1.presentation_edition)
        eq_(work.presentation_edition, edition1)

        # editions that aren't the presentation edition have no work
        eq_(edition1.work, work)
        eq_(edition2.work, None)
        eq_(edition3.work, None)

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

        # make a staff (admin interface) edition.  its fields should supercede all others below it
        # except when it has no contributors, and they do.
        pool2.suppressed = False

        staff_edition = self._edition(data_source_name=DataSource.LIBRARY_STAFF, 
            with_license_pool=False, authors=[])
        staff_edition.title = u"The Staff Title"
        staff_edition.primary_identifier = pool2.identifier
        # set edition's authorship to "nope", and make sure the lower-priority 
        # editions' authors don't get clobbered
        staff_edition.contributions = []
        staff_edition.author = Edition.UNKNOWN_AUTHOR
        staff_edition.sort_author = Edition.UNKNOWN_AUTHOR

        work.calculate_presentation(search_index_client=index)

        # The title of the Work got superceded.
        eq_("The Staff Title", work.title)

        # The author of the Work is still the author of edition2 and was not clobbered.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

    def test_set_presentation_ready(self):

        work = self._work(with_license_pool=True)

        search = DummyExternalSearchIndex()
        # This is how the work will be represented in the dummy search
        # index.
        index_key = (search.works_index, 
                     DummyExternalSearchIndex.work_document_type,
                     work.id)

        presentation = work.presentation_edition
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

        # The work has not been added to the search index.
        eq_([], search.docs.keys())

        # But the work of adding it to the search engine has been
        # registered.
        [record] = [
            x for x in work.coverage_records 
            if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        ]
        eq_(WorkCoverageRecord.REGISTERED, record.status)
        
        # This work is presentation ready because it has a title
        # and a fiction status.

        # Remove the title, and the work stops being presentation
        # ready.
        presentation.title = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(False, work.presentation_ready)        

        # The work has been removed from the search index.
        eq_([], search.docs.keys())

        # Restore the title, and everything is fixed.
        presentation.title = u"foo"
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)        

        # Remove the fiction status, and the work stops being
        # presentation ready.
        work.fiction = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(False, work.presentation_ready)        

        # Restore the fiction status, and everything is fixed.
        work.fiction = False
        work.set_presentation_ready_based_on_content(search_index_client=search)

        eq_(True, work.presentation_ready)

    def test_assign_genres_from_weights(self):
        work = self._work()

        # This work was once classified under Fantasy and Romance.        
        work.assign_genres_from_weights({Romance : 1000, Fantasy : 1000})
        self._db.commit()
        before = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        eq_([(u'Fantasy', 0.5), (u'Romance', 0.5)], before)

        # But now it's classified under Science Fiction and Romance.
        work.assign_genres_from_weights({Romance : 100, Science_Fiction : 300})
        self._db.commit()
        after = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        eq_([(u'Romance', 0.25), (u'Science Fiction', 0.75)], after)

    def test_classifications_with_genre(self):
        work = self._work(with_open_access_download=True)
        identifier = work.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.genre = genres[0]
        subject2 = self._subject(type="type2", identifier="subject2")
        subject2.genre = genres[1]
        subject3 = self._subject(type="type2", identifier="subject3")
        subject3.genre = None
        source = DataSource.lookup(self._db, DataSource.AXIS_360)        
        classification1 = self._classification(
            identifier=identifier, subject=subject1, 
            data_source=source, weight=1)
        classification2 = self._classification(
            identifier=identifier, subject=subject2, 
            data_source=source, weight=2)
        classification3 = self._classification(
            identifier=identifier, subject=subject3, 
            data_source=source, weight=2)

        results = work.classifications_with_genre().all()
        
        eq_([classification2, classification1], results)

    def test_mark_licensepools_as_superceded(self):
        # A commercial LP that somehow got superceded will be
        # un-superceded.
        commercial = self._licensepool(
            None, data_source_name=DataSource.OVERDRIVE
        )
        work, is_new = commercial.calculate_work()
        commercial.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(False, commercial.superceded)

        # An open-access LP that was superceded will be un-superceded if
        # chosen.
        gutenberg = self._licensepool(
            None, data_source_name=DataSource.GUTENBERG,
            open_access=True, with_open_access_download=True
        )
        work, is_new = gutenberg.calculate_work()
        gutenberg.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(False, gutenberg.superceded)

        # Of two open-access LPs, the one from the higher-quality data
        # source will be un-superceded, and the one from the
        # lower-quality data source will be superceded.
        standard_ebooks = self._licensepool(
            None, data_source_name=DataSource.STANDARD_EBOOKS,
            open_access=True, with_open_access_download=True
        )
        work.license_pools.append(standard_ebooks)
        gutenberg.superceded = False
        standard_ebooks.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(True, gutenberg.superceded)
        eq_(False, standard_ebooks.superceded)

        # Of three open-access pools, 1 and only 1 will be chosen as non-superceded.
        gitenberg1 = self._licensepool(edition=None, open_access=True, 
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gitenberg2 = self._licensepool(edition=None, open_access=True, 
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gutenberg1 = self._licensepool(edition=None, open_access=True, 
            data_source_name=DataSource.GUTENBERG, with_open_access_download=True
        )

        work_multipool = self._work(presentation_edition=None)
        work_multipool.license_pools.append(gutenberg1)
        work_multipool.license_pools.append(gitenberg2)
        work_multipool.license_pools.append(gitenberg1)

        # pools aren't yet aware of each other
        eq_(gutenberg1.superceded, False)
        eq_(gitenberg1.superceded, False)
        eq_(gitenberg2.superceded, False)

        # make pools figure out who's best
        work_multipool.mark_licensepools_as_superceded()

        eq_(gutenberg1.superceded, True)
        # There's no way to choose between the two gitenberg pools, 
        # so making sure only one has been chosen is enough. 
        chosen_count = 0
        for chosen_pool in gutenberg1, gitenberg1, gitenberg2:
            if chosen_pool.superceded is False:
                chosen_count += 1;
        eq_(chosen_count, 1)

        # throw wrench in
        gitenberg1.suppressed = True

        # recalculate bests
        work_multipool.mark_licensepools_as_superceded()
        eq_(gutenberg1.superceded, True)
        eq_(gitenberg1.superceded, True)
        eq_(gitenberg2.superceded, False)

    def test_work_remains_viable_on_pools_suppressed(self):
        """ If a work has all of its pools suppressed, the work's author, title, 
        and subtitle still have the last best-known info in them.
        """
        (work, pool_std_ebooks, pool_git, pool_gut, 
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = self._sample_ecosystem()

        # make sure the setup is what we expect
        eq_(pool_std_ebooks.suppressed, False)
        eq_(pool_git.suppressed, False)
        eq_(pool_gut.suppressed, False)

        # sanity check - we like standard ebooks and it got determined to be the best
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is the title of its presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is the author of its presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

        # now suppress all of the license pools
        pool_std_ebooks.suppressed = True
        pool_git.suppressed = True
        pool_gut.suppressed = True

        # and let work know
        work.calculate_presentation()

        # standard ebooks was last viable pool, and it stayed as work's choice
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is still the title of its last viable presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is still the author of its last viable presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

    def test_work_updates_info_on_pool_suppressed(self):
        """ If the provider of the work's presentation edition gets suppressed, 
        the work will choose another child license pool's presentation edition as 
        its presentation edition.
        """
        (work, pool_std_ebooks, pool_git, pool_gut, 
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = self._sample_ecosystem()

        # make sure the setup is what we expect
        eq_(pool_std_ebooks.suppressed, False)
        eq_(pool_git.suppressed, False)
        eq_(pool_gut.suppressed, False)

        # sanity check - we like standard ebooks and it got determined to be the best
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is the title of its presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is the author of its presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

        # now suppress the primary license pool
        pool_std_ebooks.suppressed = True

        # and let work know
        work.calculate_presentation()

        # gitenberg is next best and it got determined to be the best
        eq_(work.presentation_edition, pool_git.presentation_edition)
        eq_(work.presentation_edition, edition_git)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, None)
        eq_(edition_git.work, work)
        eq_(edition_gut.work, None)

        # The title of the Work is still the title of its last viable presentation edition.
        eq_("The GItenberg Title", work.title)
        eq_("The GItenberg Subtitle", work.subtitle)

        # The author of the Work is still the author of its last viable presentation edition.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

    def test_different_language_means_different_work(self):
        """There are two open-access LicensePools for the same book in
        different languages. The author and title information is the
        same, so the books have the same permanent work ID, but since
        they are in different languages they become separate works.
        """
        title = 'Siddhartha'
        author = ['Herman Hesse']
        edition1, lp1 = self._edition(
            title=title, authors=author, language='eng', with_license_pool=True,
            with_open_access_download=True
        )
        w1 = lp1.calculate_work()
        edition2, lp2 = self._edition(
            title=title, authors=author, language='ger', with_license_pool=True,
            with_open_access_download=True
        )
        w2 = lp2.calculate_work()
        for l in (lp1, lp2):
            eq_(False, l.superceded)
        assert w1 != w2

    def test_reject_covers(self):
        edition, lp = self._edition(with_open_access_download=True)

        # Create a cover and thumbnail for the edition.
        base_path = os.path.split(__file__)[0]
        sample_cover_path = base_path + '/files/covers/test-book-cover.png'
        cover_href = 'http://cover.png'
        cover_link = lp.add_link(
            Hyperlink.IMAGE, cover_href, lp.data_source,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path).read()
        )[0]

        thumbnail_href = 'http://thumbnail.png'
        thumbnail_rep = self._representation(
            url=thumbnail_href,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path).read(),
            mirrored=True
        )[0]

        cover_rep = cover_link.resource.representation
        cover_rep.mirror_url = cover_href
        cover_rep.mirrored_at = datetime.datetime.utcnow()
        cover_rep.thumbnails.append(thumbnail_rep)

        edition.set_cover(cover_link.resource)
        full_url = cover_link.resource.url
        thumbnail_url = thumbnail_rep.mirror_url

        # A Work created from this edition has cover details.
        work = self._work(presentation_edition=edition)
        assert work.cover_full_url and work.cover_thumbnail_url

        # A couple helper methods to make these tests more readable.
        def has_no_cover(work_or_edition):
            """Determines whether a Work or an Edition has a cover."""
            eq_(None, work_or_edition.cover_full_url)
            eq_(None, work_or_edition.cover_thumbnail_url)
            eq_(True, cover_link.resource.voted_quality < 0)
            eq_(True, cover_link.resource.votes_for_quality > 0)

            if isinstance(work_or_edition, Work):
                # It also removes the link from the cached OPDS entries.
                for url in [full_url, thumbnail_url]:
                    assert url not in work.simple_opds_entry
                    assert url not in work.verbose_opds_entry

            return True

        def reset_cover():
            """Makes the cover visible again for the main work object
            and confirms its visibility.
            """
            r = cover_link.resource
            r.votes_for_quality = r.voted_quality = 0
            r.update_quality()
            work.calculate_presentation(search_index_client=index)
            eq_(full_url, work.cover_full_url)
            eq_(thumbnail_url, work.cover_thumbnail_url)
            for url in [full_url, thumbnail_url]:
                assert url in work.simple_opds_entry
                assert url in work.verbose_opds_entry

        # Suppressing the cover removes the cover from the work.
        index = DummyExternalSearchIndex()
        Work.reject_covers(self._db, [work], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # It also works with Identifiers.
        identifier = work.license_pools[0].identifier
        Work.reject_covers(self._db, [identifier], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # When other Works or Editions share a cover, they are also
        # updated during the suppression process.
        other_edition = self._edition()
        other_edition.set_cover(cover_link.resource)
        other_work_ed = self._edition()
        other_work_ed.set_cover(cover_link.resource)
        other_work = self._work(presentation_edition=other_work_ed)

        Work.reject_covers(self._db, [work], search_index_client=index)
        assert has_no_cover(other_edition)
        assert has_no_cover(other_work)

    def test_missing_coverage_from(self):
        operation = 'the_operation'

        # Here's a work with a coverage record.
        work = self._work(with_license_pool=True)

        # It needs coverage.
        eq_([work], Work.missing_coverage_from(self._db, operation).all())

        # Let's give it coverage.
        record = self._work_coverage_record(work, operation)

        # It no longer needs coverage!
        eq_([], Work.missing_coverage_from(self._db, operation).all())

        # But if we disqualify coverage records created before a 
        # certain time, it might need coverage again.
        cutoff = record.timestamp + datetime.timedelta(seconds=1)

        eq_(
            [work], Work.missing_coverage_from(
                self._db, operation, count_as_missing_before=cutoff
            ).all()
        )

    def test_top_genre(self):
        work = self._work()
        identifier = work.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        source = DataSource.lookup(self._db, DataSource.AXIS_360)

        # returns None when work has no genres
        eq_(None, work.top_genre())

        # returns only genre
        wg1, is_new = get_one_or_create(
            self._db, WorkGenre, work=work, genre=genres[0], affinity=1
        )
        eq_(genres[0].name, work.top_genre())

        # returns top genre
        wg1.affinity = 0.2
        wg2, is_new = get_one_or_create(
            self._db, WorkGenre, work=work, genre=genres[1], affinity=0.8
        )
        eq_(genres[1].name, work.top_genre())

    def test_to_search_document(self):
        # Set up an edition and work.
        edition, pool = self._edition(authors=[self._str, self._str], with_license_pool=True)
        work = self._work(presentation_edition=edition)

        # Create a second Collection that has a different LicensePool
        # for the same Work.
        collection1 = self._default_collection
        collection2 = self._collection()
        self._default_library.collections.append(collection2)
        pool2 = self._licensepool(edition=edition, collection=collection2)
        pool2.work_id = work.id

        # Create a third Collection that's just hanging around, not
        # doing anything.
        collection3 = self._collection()

        # These are the edition's authors.
        [contributor1] = [c.contributor for c in edition.contributions if c.role == Contributor.PRIMARY_AUTHOR_ROLE]
        contributor1.family_name = self._str
        [contributor2] = [c.contributor for c in edition.contributions if c.role == Contributor.AUTHOR_ROLE]

        data_source = DataSource.lookup(self._db, DataSource.THREEM)
        
        # This identifier is strongly equivalent to the edition's.
        identifier = self._identifier()
        identifier.equivalent_to(data_source, edition.primary_identifier, 0.9)

        # This identifier is equivalent to the other identifier, but the strength
        # is too weak for it to be used.
        identifier2 = self._identifier()
        identifier.equivalent_to(data_source, identifier, 0.1)

        # Add some classifications.

        # This classification has no subject name, so the search document will use the subject identifier.
        edition.primary_identifier.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 6)

        # This one has the same subject type and identifier, so their weights will be combined.
        identifier.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 1)

        # Here's another classification with a different subject type.
        edition.primary_identifier.classify(data_source, Subject.OVERDRIVE, "Romance", None, 2)

        # This classification has a subject name, so the search document will use that instead of the identifier.
        identifier.classify(data_source, Subject.FAST, self._str, "Sea Stories", 7)

        # This classification will be left out because its subject type isn't useful for search.
        identifier.classify(data_source, Subject.DDC, self._str, None)

        # This classification will be left out because its identifier isn't sufficiently equivalent to the edition's.
        identifier2.classify(data_source, Subject.FAST, self._str, None)

        # Add some genres.
        genre1, ignore = Genre.lookup(self._db, "Science Fiction")
        genre2, ignore = Genre.lookup(self._db, "Romance")
        work.genres = [genre1, genre2]
        work.work_genres[0].affinity = 1

        # Add the other fields used in the search document.
        work.target_age = NumericRange(7, 8, '[]')
        edition.subtitle = self._str
        edition.series = self._str
        edition.publisher = self._str
        edition.imprint = self._str
        work.fiction = False
        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        work.summary_text = self._str
        work.rating = 5
        work.popularity = 4

        # Make sure all of this will show up in a database query.
        self._db.flush()


        search_doc = work.to_search_document()
        eq_(work.id, search_doc['_id'])
        eq_(work.title, search_doc['title'])
        eq_(edition.subtitle, search_doc['subtitle'])
        eq_(edition.series, search_doc['series'])
        eq_(edition.language, search_doc['language'])
        eq_(work.sort_title, search_doc['sort_title'])
        eq_(work.author, search_doc['author'])
        eq_(work.sort_author, search_doc['sort_author'])
        eq_(edition.medium, search_doc['medium'])
        eq_(edition.publisher, search_doc['publisher'])
        eq_(edition.imprint, search_doc['imprint'])
        eq_(edition.permanent_work_id, search_doc['permanent_work_id'])
        eq_("Nonfiction", search_doc['fiction'])
        eq_("YoungAdult", search_doc['audience'])
        eq_(work.summary_text, search_doc['summary'])
        eq_(work.quality, search_doc['quality'])
        eq_(work.rating, search_doc['rating'])
        eq_(work.popularity, search_doc['popularity'])

        # Each collection in which the Work is found is listed in
        # the 'collections' section.
        collections = search_doc['collections']
        eq_(2, len(collections))
        for collection in self._default_library.collections:
            assert dict(collection_id=collection.id) in collections

        contributors = search_doc['contributors']
        eq_(2, len(contributors))
        [contributor1_doc] = [c for c in contributors if c['sort_name'] == contributor1.sort_name]
        [contributor2_doc] = [c for c in contributors if c['sort_name'] == contributor2.sort_name]
        eq_(contributor1.family_name, contributor1_doc['family_name'])
        eq_(None, contributor2_doc['family_name'])
        eq_(Contributor.PRIMARY_AUTHOR_ROLE, contributor1_doc['role'])
        eq_(Contributor.AUTHOR_ROLE, contributor2_doc['role'])

        classifications = search_doc['classifications']
        eq_(3, len(classifications))
        [classification1_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.BISAC]]
        [classification2_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.OVERDRIVE]]
        [classification3_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.FAST]]
        eq_("FICTION Science Fiction Time Travel", classification1_doc['term'])
        eq_(float(6 + 1)/(6 + 1 + 2 + 7), classification1_doc['weight'])
        eq_("Romance", classification2_doc['term'])
        eq_(float(2)/(6 + 1 + 2 + 7), classification2_doc['weight'])
        eq_("Sea Stories", classification3_doc['term'])
        eq_(float(7)/(6 + 1 + 2 + 7), classification3_doc['weight'])
        
        genres = search_doc['genres']
        eq_(2, len(genres))
        [genre1_doc] = [g for g in genres if g['name'] == genre1.name]
        [genre2_doc] = [g for g in genres if g['name'] == genre2.name]
        eq_(Subject.SIMPLIFIED_GENRE, genre1_doc['scheme'])
        eq_(genre1.id, genre1_doc['term'])
        eq_(1, genre1_doc['weight'])
        eq_(Subject.SIMPLIFIED_GENRE, genre2_doc['scheme'])
        eq_(genre2.id, genre2_doc['term'])
        eq_(0, genre2_doc['weight'])

        target_age_doc = search_doc['target_age']
        eq_(work.target_age.lower, target_age_doc['lower'])
        eq_(work.target_age.upper, target_age_doc['upper'])

        # Each collection in which the Work is found is listed in
        # the 'collections' section.
        collections = search_doc['collections']
        eq_(2, len(collections))
        for collection in self._default_library.collections:
            assert dict(collection_id=collection.id) in collections

        # If the book stops being available through a collection
        # (because its LicensePool loses all its licenses or stops
        # being open access), that collection will not be listed
        # in the search document.
        [pool] = collection1.licensepools
        pool.licenses_owned = 0
        self._db.commit()
        search_doc = work.to_search_document()
        eq_([dict(collection_id=collection2.id)], search_doc['collections'])

        # If the book becomes available again, the collection will
        # start showing up again.
        pool.open_access = True
        self._db.commit()
        search_doc = work.to_search_document()
        eq_(2, len(search_doc['collections']))

    def test_target_age_string(self):
        work = self._work()
        work.target_age = NumericRange(7, 8, '[]')
        eq_("7-8", work.target_age_string)

        work.target_age = NumericRange(0, 8, '[]')
        eq_("0-8", work.target_age_string)

        work.target_age = NumericRange(8, None, '[]')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(None, 8, '[]')
        eq_("8", work.target_age_string)

    def test_reindex_on_availability_change(self):
        """A change in a LicensePool's availability creates a 
        WorkCoverageRecord indicating that the work needs to be
        re-indexed.
        """
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        def find_record(work):
            """Find the Work's 'update search index operation' 
            WorkCoverageRecord.
            """
            records = [
                x for x in work.coverage_records 
                if x.operation.startswith(
                        WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
                )
            ]
            if records:
                return records[0]
            return None
        registered = WorkCoverageRecord.REGISTERED
        success = WorkCoverageRecord.SUCCESS

        # The work starts off with no relevant WorkCoverageRecord.
        eq_(None, find_record(work))

        # If it stops being open-access, it needs to be reindexed.
        pool.open_access = False
        record = find_record(work)
        eq_(registered, record.status)

        # If its licenses_owned goes from zero to nonzero, it needs to
        # be reindexed.
        record.status = success
        pool.licenses_owned = 10
        pool.licenses_available = 10
        eq_(registered, record.status)

        # If its licenses_owned changes, but not to zero, nothing happens.
        record.status = success
        pool.licenses_owned = 1
        eq_(success, record.status)

        # If its licenses_available changes, nothing happens
        pool.licenses_available = 0
        eq_(success, record.status)

        # If its licenses_owned goes from nonzero to zero, it needs to
        # be reindexed.
        pool.licenses_owned = 0
        eq_(registered, record.status)

        # If it becomes open-access again, it needs to be reindexed.
        record.status = success
        pool.open_access = True
        eq_(registered, record.status)

        # If its collection changes (which shouldn't happen), it needs
        # to be reindexed.
        record.status = success
        collection2 = self._collection()
        pool.collection_id = collection2.id
        eq_(registered, record.status)

        # If a LicensePool is deleted (which also shouldn't happen),
        # its former Work needs to be reindexed.
        record.status = success
        self._db.delete(pool)
        work = self._db.query(Work).one()
        record = find_record(work)
        eq_(registered, record.status)


    def test_update_external_index(self):
        """Test the deprecated update_external_index method."""
        work = self._work()
        work.presentation_ready = True
        records = [
            x for x in work.coverage_records
            if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        ]
        index = DummyExternalSearchIndex()
        work.update_external_index(index)

        # A WorkCoverageRecord was created to register the work that
        # needs to be done.
        [record] = [
            x for x in work.coverage_records
            if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        ]
        eq_(WorkCoverageRecord.REGISTERED, record.status)

        # The work was not added to the search index -- that happens
        # later, when the WorkCoverageRecord is processed.
        eq_([], index.docs.values())
      

    def test_for_unchecked_subjects(self):

        w1 = self._work(with_license_pool=True)
        w2 = self._work()
        identifier = w1.license_pools[0].identifier

        # Neither of these works is associated with any subjects, so
        # they're not associated with any unchecked subjects.
        qu = Work.for_unchecked_subjects(self._db)
        eq_([], qu.all())

        # These Subjects haven't been checked, so the Work associated with
        # them shows up.
        ds = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        classification = identifier.classify(ds, Subject.TAG, "some tag")
        classification2 = identifier.classify(ds, Subject.TAG, "another tag")
        eq_([w1], qu.all())

        # If one of them is checked, the Work still shows up.
        classification.subject.checked = True
        eq_([w1], qu.all())

        # Only when all Subjects are checked does the work stop showing up.
        classification2.subject.checked = True
        eq_([], qu.all())

    def test_calculate_opds_entries(self):
        """Verify that calculate_opds_entries sets both simple and verbose
        entries.
        """
        work = self._work()
        work.simple_opds_entry = None
        work.verbose_opds_entry = None

        work.calculate_opds_entries(verbose=False)
        simple_entry = work.simple_opds_entry
        assert simple_entry.startswith('<entry')
        eq_(None, work.verbose_opds_entry)

        work.calculate_opds_entries(verbose=True)
        # The simple OPDS entry is the same length as before.
        # It's not necessarily _exactly_ the same because the
        # <updated> timestamp may be different.
        eq_(len(simple_entry), len(work.simple_opds_entry))

        # The verbose OPDS entry is longer than the simple one.
        assert work.verbose_opds_entry.startswith('<entry')
        assert len(work.verbose_opds_entry) > len(simple_entry)


class TestCirculationEvent(DatabaseTest):

    def _event_data(self, **kwargs):
        for k, default in (
                ("source", DataSource.OVERDRIVE),
                ("id_type", Identifier.OVERDRIVE_ID),
                ("start", datetime.datetime.utcnow()),
                ("type", CirculationEvent.DISTRIBUTOR_LICENSE_ADD),
        ):
            kwargs.setdefault(k, default)
        if 'old_value' in kwargs and 'new_value' in kwargs:
            kwargs['delta'] = kwargs['new_value'] - kwargs['old_value']
        return kwargs

    def _get_datetime(self, data, key):
        date = data.get(key, None)
        if not date:
            return None
        elif isinstance(date, datetime.date):
            return date
        else:
            return datetime.datetime.strptime(date, CirculationEvent.TIME_FORMAT)

    def _get_int(self, data, key):
        value = data.get(key, None)
        if not value:
            return value
        else:
            return int(value)

    def from_dict(self, data):
        _db = self._db

        # Identify the source of the event.
        source_name = data['source']
        source = DataSource.lookup(_db, source_name)

        # Identify which LicensePool the event is talking about.
        foreign_id = data['id']
        identifier_type = source.primary_identifier_type
        collection = data['collection']
        
        license_pool, was_new = LicensePool.for_foreign_id(
            _db, source, identifier_type, foreign_id, collection=collection
        )

        # Finally, gather some information about the event itself.
        type = data.get("type")
        start = self._get_datetime(data, 'start')
        end = self._get_datetime(data, 'end')
        old_value = self._get_int(data, 'old_value')
        new_value = self._get_int(data, 'new_value')
        delta = self._get_int(data, 'delta')
        foreign_patron_id = data.get("foreign_patron_id")
        event, was_new = get_one_or_create(
            _db, CirculationEvent, license_pool=license_pool,
            type=type, start=start, foreign_patron_id=foreign_patron_id,
            create_method_kwargs=dict(
                old_value=old_value,
                new_value=new_value,
                delta=delta,
                end=end)
            )
        return event, was_new

    def test_new_title(self):

        # Here's a new title.
        collection = self._collection()
        data = self._event_data(
            source=DataSource.OVERDRIVE,
            id="{1-2-3}",
            type=CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
            collection=collection,
            old_value=0,
            delta=2,
            new_value=2,
        )
        
        # Turn it into an event and see what happens.
        event, ignore = self.from_dict(data)

        # The event is associated with the correct data source.
        eq_(DataSource.OVERDRIVE, event.license_pool.data_source.name)

        # The event identifies a work by its ID plus the data source's
        # primary identifier and its collection.
        eq_(Identifier.OVERDRIVE_ID, event.license_pool.identifier.type)
        eq_("{1-2-3}", event.license_pool.identifier.identifier)
        eq_(collection, event.license_pool.collection)

        # The number of licenses has not been set to the new value.
        # The creator of a circulation event is responsible for also
        # updating the dataset.
        eq_(0, event.license_pool.licenses_owned)


# class TestWorkQuality(DatabaseTest):

#     def test_better_known_work_gets_higher_rating(self):

#         gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

#         edition1_1, pool1 = self._edition(with_license_pool=True)
#         edition1_2 = self._edition(with_license_pool=False)

#         edition2_1, pool2 = self._edition(with_license_pool=True)

#         wrs = []
#         pools = []
#         for i in range(10):
#             wr, pool = self._edition(with_license_pool=True)
#             wrs.append(wr)
#             pools.append(pool)

#         work1 = Work()
#         work1.editions.extend([edition1_1, edition1_2] + wrs)
#         work1.license_pools.extend(pools + [pool1])

#         work2 = Work()
#         work2.editions.append(edition2_1)
#         work2.license_pools.append(pool2)

#         work1.calculate_presentation()
#         work2.calculate_presentation()

#         assert work1.quality > work2.quality

#     def test_more_license_pools_gets_higher_rating(self):

#         gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

#         edition1_1, pool1 = self._edition(with_license_pool=True)
#         edition1_2, pool2 = self._edition(with_license_pool=True)

#         edition2_1, pool3 = self._edition(with_license_pool=True)
#         edition2_2 = self._edition(with_license_pool=False)

#         wrs = []
#         pools = []
#         for i in range(10):
#             wr, pool = self._edition(with_license_pool=True)
#             wrs.append(wr)
#             pools.append(pool)

#         work1 = Work()
#         work1.editions.extend([edition1_1, edition1_2] + wrs)
#         work1.license_pools.extend([pool1, pool2] + pools)

#         work2 = Work()
#         work2.editions.extend([edition2_1, edition2_2])
#         work2.license_pools.extend([pool3])

#         work1.calculate_presentation()
#         work2.calculate_presentation()

#         assert work1.quality > work2.quality


class TestWorkConsolidation(DatabaseTest):

    def test_calculate_work_success(self):
        e, p = self._edition(with_license_pool=True)
        work, new = p.calculate_work(even_if_no_author=True)
        eq_(p.presentation_edition, work.presentation_edition)
        eq_(True, new)

    def test_calculate_work_bails_out_if_no_title(self):
        e, p = self._edition(with_license_pool=True)
        e.title=None
        work, new = p.calculate_work(even_if_no_author=True)
        eq_(None, work)
        eq_(False, new)

        # even_if_no_title means we don't need a title.
        work, new = p.calculate_work(
            even_if_no_author=True, even_if_no_title=True
        )
        assert isinstance(work, Work)
        eq_(True, new)
        eq_(None, work.title)
        eq_(None, work.presentation_edition.permanent_work_id)

    def test_calculate_work_bails_out_if_no_author(self):
        e, p = self._edition(with_license_pool=True, authors=[])
        work, new = p.calculate_work(even_if_no_author=False)
        eq_(None, work)
        eq_(False, new)

        # If we know that there simply is no author for this work,
        # we can pass in even_if_no_author=True
        work, new = p.calculate_work(even_if_no_author=True)
        eq_(p.presentation_edition, work.presentation_edition)
        eq_(True, new)


    def test_calculate_work_matches_based_on_permanent_work_id(self):
        # Here are two Editions with the same permanent work ID, 
        # since they have the same title/author.
        edition1, ignore = self._edition(with_license_pool=True)
        edition2, ignore = self._edition(
            title=edition1.title, authors=edition1.author,
            with_license_pool=True
        )

        # For purposes of this test, let's pretend all these books are
        # open-access.
        for e in [edition1, edition2]:
            for license_pool in e.license_pools:
                license_pool.open_access = True
        
        # Calling calculate_work() on the first edition creates a Work.
        work1, created = edition1.license_pools[0].calculate_work()
        eq_(created, True)

        # Calling calculate_work() on the second edition associated
        # the second edition's pool with the first work.
        work2, created = edition2.license_pools[0].calculate_work()
        eq_(created, False)

        eq_(work1, work2)

        expect = edition1.license_pools + edition2.license_pools
        eq_(set(expect), set(work1.license_pools))


    def test_calculate_work_for_licensepool_creates_new_work(self):
        edition1, ignore = self._edition(data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID, 
            title=self._str, authors=[self._str], with_license_pool=True)

        # This edition is unique to the existing work.
        preexisting_work = Work()
        preexisting_work.set_presentation_edition(edition1)

        # This edition is unique to the new LicensePool
        edition2, pool = self._edition(data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID, 
            title=self._str, authors=[self._str], with_license_pool=True)

        # Call calculate_work(), and a new Work is created.
        work, created = pool.calculate_work()
        eq_(True, created)
        assert work != preexisting_work



    def test_calculate_work_does_nothing_unless_edition_has_title_and_author(self):
        collection=self._collection()
        edition, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
        )
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=collection
        )
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.title = u"foo"
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.add_contributor(u"bar", Contributor.PRIMARY_AUTHOR_ROLE)
        edition.calculate_presentation()
        work, created = pool.calculate_work()
        eq_(True, created)

        # The edition is the work's presentation edition.
        eq_(work, edition.work)
        eq_(edition, work.presentation_edition)
        eq_(u"foo", work.title)
        eq_(u"bar", work.author)

    def test_calculate_work_can_be_forced_to_work_with_no_author(self):
        collection = self._collection()
        edition, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
        )
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=collection
        )
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.title = u"foo"
        work, created = pool.calculate_work(even_if_no_author=True)
        eq_(True, created)
        self._db.commit()
        eq_(edition, work.presentation_edition)
        eq_(u"foo", work.title)
        eq_(Edition.UNKNOWN_AUTHOR, work.author)

    def test_calculate_work_fails_when_presentation_edition_identifier_does_not_match_license_pool(self):

        # Here's a LicensePool with an Edition.
        edition1, pool = self._edition(
            data_source_name=DataSource.GUTENBERG, with_license_pool=True
        )
        
        # Here's a second Edition that's talking about a different Identifier
        # altogether, and has no LicensePool.
        edition2 = self._edition()
        assert edition1.primary_identifier != edition2.primary_identifier

        # Here's a third Edition that's tied to a totally different
        # LicensePool.
        edition3, pool2 = self._edition(with_license_pool=True)
        assert edition1.primary_identifier != edition3.primary_identifier
        
        # When we calculate a Work for a LicensePool, we can pass in
        # any Edition as the presentation edition, so long as that
        # Edition's primary identifier matches the LicensePool's
        # identifier.
        work, is_new = pool.calculate_work(known_edition=edition1)

        # But we can't pass in an Edition that's the presentation
        # edition for a LicensePool with a totally different Identifier.
        for edition in (edition2, edition3):
            assert_raises_regexp(
                ValueError,
                "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!",
                pool.calculate_work,
                known_edition=edition
            )
        
    def test_open_access_pools_grouped_together(self):

        # We have four editions with exactly the same title and author.
        # Two of them are open-access, two are not.
        title = "The Only Title"
        author = "Single Author"
        ed1, open1 = self._edition(title=title, authors=author, with_license_pool=True)
        ed2, open2 = self._edition(title=title, authors=author, with_license_pool=True)
        open1.open_access = True
        open2.open_access = True
        ed3, restricted3 = self._edition(
            title=title, authors=author, data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)
        ed4, restricted4 = self._edition(
            title=title, authors=author, data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)

        restricted3.open_access = False
        restricted4.open_access = False

        # Every identifier is equivalent to every other identifier.
        s = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        ed1.primary_identifier.equivalent_to(s, ed2.primary_identifier, 1)
        ed1.primary_identifier.equivalent_to(s, ed3.primary_identifier, 1)
        ed1.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)
        ed2.primary_identifier.equivalent_to(s, ed3.primary_identifier, 1)
        ed2.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)
        ed3.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)

        open1.calculate_work()
        open2.calculate_work()
        restricted3.calculate_work()
        restricted4.calculate_work()

        assert open1.work != None
        assert open2.work != None
        assert restricted3.work != None
        assert restricted4.work != None

        # The two open-access pools are grouped together.
        eq_(open1.work, open2.work)

        # Each restricted-access pool is completely isolated.
        assert restricted3.work != restricted4.work
        assert restricted3.work != open1.work
       
    def test_all_licensepools_with_same_identifier_get_same_work(self):

        # Here are two LicensePools for the same Identifier and
        # DataSource, but different Collections.
        edition1, pool1 = self._edition(with_license_pool=True)
        identifier = pool1.identifier
        collection2 = self._collection()

        edition2, pool2 = self._edition(
            with_license_pool=True,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
            collection=collection2
        )

        eq_(pool1.identifier, pool2.identifier)
        eq_(pool1.data_source, pool2.data_source)
        eq_(self._default_collection, pool1.collection)
        eq_(collection2, pool2.collection)

        # The two LicensePools have the same Edition (since a given
        # DataSource has only one opinion about an Identifier's
        # bibliographic information).
        eq_(edition1, edition2)

        # Because the two LicensePools have the same Identifier, they
        # have the same Work.
        work1, is_new_1 = pool1.calculate_work()
        work2, is_new_2 = pool2.calculate_work()
        eq_(work1, work2)
        eq_(True, is_new_1)
        eq_(False, is_new_2)
        eq_(edition1, work1.presentation_edition)
        
    def test_calculate_work_fixes_work_in_invalid_state(self):
        # Here's a Work with a commercial edition of "abcd".
        work = self._work(with_license_pool=True)
        [abcd_commercial] = work.license_pools
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains a _second_
        # commercial edition of "abcd"...
        edition, abcd_commercial_2 = self._edition(with_license_pool=True)
        abcd_commercial_2.open_access = False
        abcd_commercial_2.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(abcd_commercial_2)

        # ...as well as an open-access edition of "abcd".
        edition, abcd_open_access = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        abcd_open_access.open_access = True
        abcd_open_access.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(abcd_open_access)

        # calculate_work() recalculates the permanent work ID of a
        # LicensePool's presentation edition, and obviously the real
        # value isn't "abcd" for any of these Editions. Mocking
        # calculate_permanent_work_id ensures that we run the code
        # under the assumption that all these Editions have the same
        # permanent work ID.
        def mock_pwid(debug=False):
            return "abcd"
        for lp in [abcd_commercial, abcd_commercial_2, abcd_open_access]:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # Anyway, we can fix the whole problem by calling
        # calculate_work() on one of the LicensePools.
        work_after, is_new = abcd_commercial.calculate_work()
        eq_(work_after, work)
        eq_(False, is_new)

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other two have been kicked out and
        # given their own works.
        assert abcd_commercial_2.work != work
        assert abcd_open_access.work != work

        # The commercial LicensePool has been given a Work of its own.
        eq_([abcd_commercial_2], abcd_commercial_2.work.license_pools)

        # The open-access work has been given the Work that will be
        # used for all open-access LicensePools for that book going
        # forward.

        expect_open_access_work, open_access_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        eq_(expect_open_access_work, abcd_open_access.work)

        # Now we're going to restore the bad configuration, where all
        # three books have the same Work. This time we're going to
        # call calculate_work() on the open-access LicensePool, and
        # verify that we get similar results as when we call
        # calculate_work() on one of the commercial LicensePools.
        abcd_commercial_2.work = work
        abcd_open_access.work = work

        work_after, is_new = abcd_open_access.calculate_work()
        # Since we called calculate_work() on the open-access work, it
        # maintained control of the Work, and both commercial books
        # got assigned new Works.
        eq_(work, work_after)
        eq_(False, is_new)

        assert abcd_commercial.work != work
        assert abcd_commercial.work != None
        assert abcd_commercial_2.work != work
        assert abcd_commercial_2.work != None
        assert abcd_commercial.work != abcd_commercial_2.work

        # Finally, let's test that nothing happens if you call
        # calculate_work() on a self-consistent situation.
        open_access_work = abcd_open_access.work
        eq_((open_access_work, False), abcd_open_access.calculate_work())

        commercial_work = abcd_commercial.work
        eq_((commercial_work, False), abcd_commercial.calculate_work())

    def test_calculate_work_fixes_incorrectly_grouped_books(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.open_access = True
        book.presentation_edition.permanent_work_id = "abcd"
        
        # Due to a earlier error, the Work also contains an
        # open-access _audiobook_ of "abcd".
        edition, audiobook = self._edition(with_license_pool=True)
        audiobook.open_access = True
        audiobook.presentation_edition.medium=Edition.AUDIO_MEDIUM
        audiobook.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(audiobook)

        # And the Work _also_ contains an open-access book of "abcd"
        # in a different language.
        edition, spanish = self._edition(with_license_pool=True)
        spanish.open_access = True
        spanish.presentation_edition.language='spa'
        spanish.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(spanish)

        def mock_pwid(debug=False):
            return "abcd"
        for lp in [book, audiobook, spanish]:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We can fix this by calling calculate_work() on one of the
        # LicensePools.
        work_after, is_new = book.calculate_work()
        eq_(work_after, work)
        eq_(False, is_new)

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other one has been kicked out and
        # given its own work.
        eq_(book.work, work)
        assert audiobook.work != work

        # The audiobook LicensePool has been given a Work of its own.
        eq_([audiobook], audiobook.work.license_pools)

        # The book has been given the Work that will be used for all
        # book-type LicensePools for that title going forward.
        expect_book_work, book_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        eq_(expect_book_work, book.work)

        # The audiobook has been given the Work that will be used for
        # all audiobook-type LicensePools for that title going
        # forward.
        expect_audiobook_work, audiobook_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.AUDIO_MEDIUM, 'eng'
            )
        )
        eq_(expect_audiobook_work, audiobook.work)

        # The Spanish book has been given the Work that will be used
        # for all Spanish LicensePools for that title going forward.
        expect_spanish_work, spanish_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'spa'
            )
        )
        eq_(expect_spanish_work, spanish.work)
        eq_('spa', expect_spanish_work.language)


    def test_calculate_work_detaches_licensepool_with_no_title(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # But the LicensePool's presentation edition has lost its
        # title.
        book.presentation_edition.title = None

        # Calling calculate_work() on the LicensePool will detach the
        # book from its work, since a book with no title cannot have
        # an associated Work.
        work_after, is_new = book.calculate_work()
        eq_(None, work_after)
        eq_([], work.license_pools)

    def test_calculate_work_detaches_licensepool_with_no_pwid(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains an edition
        # with no title or author, and thus no permanent work ID.
        edition, no_title = self._edition(with_license_pool=True)

        no_title.presentation_edition.title=None
        no_title.presentation_edition.author=None
        no_title.presentation_edition.permanent_work_id = None
        work.license_pools.append(no_title)

        # Calling calculate_work() on the functional LicensePool will
        # split off the bad one.
        work_after, is_new = book.calculate_work()
        eq_([book], work.license_pools)
        eq_(None, no_title.work)
        eq_(None, no_title.presentation_edition.work)

        # calculate_work() on the bad LicensePool will split it off from
        # the good one.
        work.license_pools.append(no_title)
        work_after_2, is_new = no_title.calculate_work()
        eq_(None, work_after_2)
        eq_([book], work.license_pools)

        # The same thing happens if the bad LicensePool has no
        # presentation edition at all.
        work.license_pools.append(no_title)
        no_title.presentation_edition = None
        work_after, is_new = book.calculate_work()
        eq_([book], work.license_pools)

        work.license_pools.append(no_title)
        work_after, is_new = no_title.calculate_work()
        eq_([book], work.license_pools)


    def test_pwids(self):
        """Test the property that finds all permanent work IDs
        associated with a Work.
        """
        # Create a (bad) situation in which LicensePools associated
        # with two different PWIDs are associated with the same work.
        work = self._work(with_license_pool=True)
        [lp1] = work.license_pools
        eq_(set([lp1.presentation_edition.permanent_work_id]),
            work.pwids)
        edition, lp2 = self._edition(with_license_pool=True)
        work.license_pools.append(lp2)

        # Work.pwids finds both PWIDs.
        eq_(set([lp1.presentation_edition.permanent_work_id,
                 lp2.presentation_edition.permanent_work_id]),
            work.pwids)

    def test_open_access_for_permanent_work_id_no_licensepools(self):
        # There are no LicensePools, which short-circuilts
        # open_access_for_permanent_work_id.
        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "No such permanent work ID", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        # Now it works.
        w = self._work(
            language="eng", with_license_pool=True,
            with_open_access_download=True
        )
        w.presentation_edition.permanent_work_id = "permid"
        eq_(
            (w, False), Work.open_access_for_permanent_work_id(
                self._db, "permid", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        # But the language, medium, and permanent ID must all match.
        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "permid", Edition.BOOK_MEDIUM,
                "spa"
            )
        )

        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "differentid", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "differentid", Edition.AUDIO_MEDIUM,
                "eng"
            )
        )

    def test_open_access_for_permanent_work_id(self):
        # Two different works full of open-access license pools.
        w1 = self._work(with_license_pool=True, with_open_access_download=True)

        w2 = self._work(with_license_pool=True, with_open_access_download=True)

        [lp1] = w1.license_pools 
        [lp2] = w2.license_pools 

        # Work #2 has two different license pools grouped
        # together. Work #1 only has one.
        edition, lp3 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        w2.license_pools.append(lp3)

        # Due to an error, it turns out both Works are providing the
        # exact same book.
        def mock_pwid(debug=False):
            return "abcd"
        for lp in [lp1, lp2, lp3]:
            lp.presentation_edition.permanent_work_id="abcd"
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We've also got Work #3, which provides a commercial license
        # for that book.
        w3 = self._work(with_license_pool=True)
        w3_pool = w3.license_pools[0]
        w3_pool.presentation_edition.permanent_work_id="abcd"
        w3_pool.open_access = False

        # Work.open_access_for_permanent_work_id can resolve this problem.
        work, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )

        # Work #3 still exists and its license pool was not affected.
        eq_([w3], self._db.query(Work).filter(Work.id==w3.id).all())
        eq_(w3, w3_pool.work)

        # But the other three license pools now have the same work.
        eq_(work, lp1.work)
        eq_(work, lp2.work)
        eq_(work, lp3.work)
        
        # Because work #2 had two license pools, and work #1 only had
        # one, work #1 was merged into work #2, rather than the other
        # way around.
        eq_(w2, work)
        eq_(False, is_new)

        # Work #1 no longer exists.
        eq_([], self._db.query(Work).filter(Work.id==w1.id).all())

        # Calling Work.open_access_for_permanent_work_id again returns the same
        # result.
        _db = self._db
        Work.open_access_for_permanent_work_id(_db, "abcd", Edition.BOOK_MEDIUM, "eng")
        eq_((w2, False), Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        ))

    def test_open_access_for_permanent_work_id_can_create_work(self):

        # Here's a LicensePool with no corresponding Work.
        edition, lp = self._edition(with_license_pool=True)
        lp.open_access = True
        edition.permanent_work_id="abcd"

        # open_access_for_permanent_work_id creates the Work.
        work, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, edition.language
        )
        eq_([lp], work.license_pools)
        eq_(True, is_new)

    def test_potential_open_access_works_for_permanent_work_id(self):
        """Test of the _potential_open_access_works_for_permanent_work_id
        helper method.
        """

        # Here are two editions of the same book with the same PWID.
        title = 'Siddhartha'
        author = ['Herman Hesse']
        e1, lp1 = self._edition(
            data_source_name=DataSource.STANDARD_EBOOKS,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e1.permanent_work_id = "pwid"

        e2, lp2 = self._edition(
            data_source_name=DataSource.GUTENBERG,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e2.permanent_work_id = "pwid"

        w1 = Work()
        for lp in [lp1, lp2]:
            w1.license_pools.append(lp)
            lp.open_access = True

        def m():
            return Work._potential_open_access_works_for_permanent_work_id(
                self._db, "pwid", Edition.BOOK_MEDIUM, "eng"
            )
        pools, counts = m()

        # Both LicensePools show up in the list of LicensePools that
        # should be grouped together, and both LicensePools are
        # associated with the same Work.
        poolset = set([lp1, lp2])
        eq_(poolset, pools)
        eq_({w1 : 2}, counts)

        # Since the work was just created, it has no presentation
        # edition and thus no language. If the presentation edition
        # were set, the result would be the same.
        w1.presentation_edition = e1
        pools, counts = m()
        eq_(poolset, pools)
        eq_({w1 : 2}, counts)

        # If the Work's presentation edition has information that
        # _conflicts_ with the information passed in to
        # _potential_open_access_works_for_permanent_work_id, the Work
        # does not show up in `counts`, indicating that a new Work
        # should to be created to hold those books.
        bad_pe = self._edition()
        bad_pe.permanent_work_id='pwid'
        w1.presentation_edition = bad_pe

        bad_pe.language = 'fin'
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.language = 'eng'

        bad_pe.medium = Edition.AUDIO_MEDIUM
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.medium = Edition.BOOK_MEDIUM

        bad_pe.permanent_work_id = "Some other ID"
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.permanent_work_id = "pwid"

        w1.presentation_edition = None

        # Now let's see what changes to a LicensePool will cause it
        # not to be eligible in the first place.
        def assert_lp1_missing():
            # A LicensePool that is not eligible will not show up in
            # the set and will not be counted towards the total of eligible
            # LicensePools for its Work.
            pools, counts = m()
            eq_(set([lp2]), pools)
            eq_({w1 : 1}, counts)

        # It has to be open-access.
        lp1.open_access = False
        assert_lp1_missing()
        lp1.open_access = True

        # The presentation edition's permanent work ID must match
        # what's passed into the helper method.
        e1.permanent_work_id = "another pwid"
        assert_lp1_missing()
        e1.permanent_work_id = "pwid"

        # The medium must also match.
        e1.medium = Edition.AUDIO_MEDIUM
        assert_lp1_missing()
        e1.medium = Edition.BOOK_MEDIUM

        # The language must also match.
        e1.language = "another language"
        assert_lp1_missing()
        e1.language = 'eng'

        # Finally, let's see what happens when there are two Works where
        # there should be one.
        w2 = Work()
        w2.license_pools.append(lp2)
        pools, counts = m()

        # This work is irrelevant and will not show up at all.
        w3 = Work()

        # Both Works have one associated LicensePool, so they have
        # equal claim to being 'the' Work for this work
        # ID/language/medium. The calling code will have to sort it
        # out.
        eq_(poolset, pools)
        eq_({w1: 1, w2: 1}, counts)

    def test_make_exclusive_open_access_for_permanent_work_id(self):
        # Here's a work containing an open-access LicensePool for
        # literary work "abcd".
        work1 = self._work(with_license_pool=True, 
                          with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id="abcd"

        # Unfortunately, a commercial LicensePool for the literary
        # work "abcd" has gotten associated with the same work.
        edition, abcd_commercial = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id="abcd"
        abcd_commercial.work = work1

        # Here's another Work containing an open-access LicensePool
        # for literary work "efgh".
        work2 = self._work(with_license_pool=True, 
                          with_open_access_download=True)
        [efgh_1] = work2.license_pools
        efgh_1.presentation_edition.permanent_work_id="efgh"

        # Unfortunately, there's another open-access LicensePool for
        # "efgh", and it's incorrectly associated with the "abcd"
        # work.
        edition, efgh_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        efgh_2.presentation_edition.permanent_work_id = "efgh"
        efgh_2.work = work1

        # Let's fix these problems.
        work1.make_exclusive_open_access_for_permanent_work_id(
            "abcd", Edition.BOOK_MEDIUM, "eng",
        )

        # The open-access "abcd" book is now the only LicensePool
        # associated with work1.
        eq_([abcd_oa], work1.license_pools)

        # Both open-access "efgh" books are now associated with work2.
        eq_(set([efgh_1, efgh_2]), set(work2.license_pools))

        # A third work has been created for the commercial edition of "abcd".
        assert abcd_commercial.work not in (work1, work2)

    def test_make_exclusive_open_access_for_null_permanent_work_id(self):
        # Here's a LicensePool that, due to a previous error, has
        # a null PWID in its presentation edition.
        work = self._work(with_license_pool=True, 
                          with_open_access_download=True)
        [null1] = work.license_pools
        null1.presentation_edition.title = None
        null1.presentation_edition.sort_author = None
        null1.presentation_edition.permanent_work_id = None
        
        # Here's another LicensePool associated with the same work and
        # with the same problem.
        edition, null2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(null2)

        for pool in work.license_pools:
            pool.presentation_edition.title = None
            pool.presentation_edition.sort_author = None
            pool.presentation_edition.permanent_work_id = None

        work.make_exclusive_open_access_for_permanent_work_id(
            None, Edition.BOOK_MEDIUM, edition.language
        )

        # Since a LicensePool with no PWID cannot have an associated Work,
        # this Work now have no LicensePools at all.
        eq_([], work.license_pools)

        eq_(None, null1.work)
        eq_(None, null2.work)

    def test_merge_into_success(self):
        # Here's a work with an open-access LicensePool.
        work1 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id="abcd"

        # Let's give it a WorkGenre and a WorkCoverageRecord.
        genre, ignore = Genre.lookup(self._db, "Fantasy")
        wg, wg_is_new = get_one_or_create(
            self._db, WorkGenre, work=work1, genre=genre
        )
        wcr, wcr_is_new = WorkCoverageRecord.add_for(work1, "test")

        # Here's another work with an open-access LicensePool for the
        # same book.
        work2 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.presentation_edition.permanent_work_id="abcd"

        # Let's merge the first work into the second.
        work1.merge_into(work2)

        # The first work has been deleted, as have its WorkGenre and
        # WorkCoverageRecord.
        eq_([], self._db.query(Work).filter(Work.id==work1.id).all())
        eq_([], self._db.query(WorkGenre).all())
        eq_([], self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.work_id==work1.id).all()
        )

    def test_open_access_for_permanent_work_id_fixes_mismatched_works_incidentally(self):

        # Here's a work with two open-access LicensePools for the book "abcd".
        work1 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [abcd_1] = work1.license_pools
        edition, abcd_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work1.license_pools.append(abcd_2)

        # Unfortunately, due to an earlier error, that work also
        # contains a _third_ open-access LicensePool, and this one
        # belongs to a totally separate book, "efgh".
        edition, efgh = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work1.license_pools.append(efgh)

        # Here's another work with an open-access LicensePool for the
        # book "abcd".
        work2 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [abcd_3] = work2.license_pools

        # Unfortunately, this work also contains an open-access Licensepool 
        # for the totally separate book, 'ijkl".
        edition, ijkl = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work2.license_pools.append(ijkl)

        # Mock the permanent work IDs for all the presentation
        # editions in play.
        def mock_pwid_abcd(debug=False):
            return "abcd"

        def mock_pwid_efgh(debug=False):
            return "efgh"

        def mock_pwid_ijkl(debug=False):
            return "ijkl"

        for lp in abcd_1, abcd_2, abcd_3:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_abcd
            lp.presentation_edition.permanent_work_id = 'abcd'

        efgh.presentation_edition.calculate_permanent_work_id = mock_pwid_efgh
        efgh.presentation_edition.permanent_work_id = 'efgh'

        ijkl.presentation_edition.calculate_permanent_work_id = mock_pwid_ijkl
        ijkl.presentation_edition.permanent_work_id = 'ijkl'

        # Calling Work.open_access_for_permanent_work_id()
        # automatically kicks the 'efgh' and 'ijkl' LicensePools into
        # their own works, and merges the second 'abcd' work with the
        # first one. (The first work is chosen because it represents
        # two LicensePools for 'abcd', not just one.)
        abcd_work, abcd_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )
        efgh_work, efgh_new = Work.open_access_for_permanent_work_id(
            self._db, "efgh", Edition.BOOK_MEDIUM, "eng"
        )
        ijkl_work, ijkl_new = Work.open_access_for_permanent_work_id(
            self._db, "ijkl", Edition.BOOK_MEDIUM, "eng"
        )

        # We've got three different works here. The 'abcd' work is the
        # old 'abcd' work that had three LicensePools--the other work
        # was merged into it.
        eq_(abcd_1.work, abcd_work)
        assert efgh_work != abcd_work
        assert ijkl_work != abcd_work
        assert ijkl_work != efgh_work

        # The two 'new' works (for efgh and ijkl) are not counted as
        # new because they were created during the first call to
        # Work.open_access_for_permanent_work_id, when those
        # LicensePools were split out of Works where they didn't
        # belong.
        eq_(False, efgh_new)
        eq_(False, ijkl_new)

        eq_([ijkl], ijkl_work.license_pools)
        eq_([efgh], efgh_work.license_pools)
        eq_(3, len(abcd_work.license_pools))

    def test_open_access_for_permanent_work_untangles_tangled_works(self):

        # Here are three works for the books "abcd", "efgh", and "ijkl".
        abcd_work = self._work(with_license_pool=True, 
                               with_open_access_download=True)
        [abcd_1] = abcd_work.license_pools

        efgh_work = self._work(with_license_pool=True, 
                               with_open_access_download=True)
        [efgh_1] = efgh_work.license_pools

        # Unfortunately, due to an earlier error, the 'abcd' work
        # contains a LicensePool for 'efgh', and the 'efgh' work contains
        # a LicensePool for 'abcd'.
        #
        # (This is pretty much impossible, but bear with me...)

        abcd_edition, abcd_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        efgh_work.license_pools.append(abcd_2)

        efgh_edition, efgh_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        abcd_work.license_pools.append(efgh_2)

        # Both Works have a presentation edition that indicates the
        # permanent work ID is 'abcd'.
        abcd_work.presentation_edition = efgh_edition
        efgh_work.presentation_edition = efgh_edition

        def mock_pwid_abcd(debug=False):
            return "abcd"

        for lp in abcd_1, abcd_2:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_abcd
            lp.presentation_edition.permanent_work_id = 'abcd'

        def mock_pwid_efgh(debug=False):
            return "efgh"

        for lp in efgh_1, efgh_2:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_efgh
            lp.presentation_edition.permanent_work_id = 'efgh'

        # Calling Work.open_access_for_permanent_work_id() creates a
        # new work that contains both 'abcd' LicensePools.
        abcd_new, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )
        eq_(True, is_new)
        eq_(set([abcd_1, abcd_2]), set(abcd_new.license_pools))

        # The old abcd_work now contains only the 'efgh' LicensePool
        # that didn't fit.
        eq_([efgh_2], abcd_work.license_pools)

        # We now have two works with 'efgh' LicensePools: abcd_work
        # and efgh_work. Calling
        # Work.open_access_for_permanent_work_id on 'efgh' will
        # consolidate the two LicensePools into one of the Works
        # (which one is nondeterministic).
        efgh_new, is_new = Work.open_access_for_permanent_work_id(
            self._db, "efgh", Edition.BOOK_MEDIUM, "eng"
        )
        eq_(False, is_new)
        eq_(set([efgh_1, efgh_2]), set(efgh_new.license_pools))
        assert efgh_new in (abcd_work, efgh_work)

        # The Work that was not chosen for consolidation now has no
        # LicensePools.
        if efgh_new is abcd_work:
            other = efgh_work
        else:
            other = abcd_work
        eq_([], other.license_pools)

    def test_merge_into_raises_exception_if_grouping_rules_violated(self):
        # Here's a work with an open-access LicensePool.
        work1 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id="abcd"

        # Here's another work with a commercial LicensePool for the
        # same book.
        work2 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.open_access = False
        lp2.presentation_edition.permanent_work_id="abcd"

        # The works cannot be merged.
        assert_raises_regexp(
            ValueError, 
            "Refusing to merge .* into .* because it would put an open-access LicensePool into the same work as a non-open-access LicensePool.",
            work1.merge_into, work2,
        )

    def test_merge_into_raises_exception_if_pwids_differ(self):
        work1 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id="abcd"

        work2 = self._work(with_license_pool=True, 
                           with_open_access_download=True)
        [efgh_oa] = work2.license_pools
        efgh_oa.presentation_edition.permanent_work_id="efgh"

        assert_raises_regexp(
            ValueError,
            "Refusing to merge .* into .* because permanent work IDs don't match: abcd vs. efgh",
            work1.merge_into, 
            work2
        )

    def test_licensepool_without_identifier_gets_no_work(self):
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools
        lp.identifier = None

        # Even if the LicensePool had a work before, it gets removed.
        eq_((None, False), lp.calculate_work())
        eq_(None, lp.work)

    def test_licensepool_without_presentation_edition_gets_no_work(self):
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools

        # This LicensePool has no presentation edition and no way of 
        # getting one.
        lp.presentation_edition = None
        lp.identifier.primarily_identifies = []

        # Even if the LicensePool had a work before, it gets removed.
        eq_((None, False), lp.calculate_work())
        eq_(None, lp.work)

class TestLoans(DatabaseTest):

    def test_open_access_loan(self):
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]
        pool.is_open_access = True

        # The patron has no active loans.
        eq_([], patron.loans)

        # Loan them the book
        fulfillment = pool.delivery_mechanisms[0]
        loan, was_new = pool.loan_to(patron, fulfillment=fulfillment)

        # Now they have a loan!
        eq_([loan], patron.loans)
        eq_(loan.patron, patron)
        eq_(loan.license_pool, pool)
        eq_(fulfillment, loan.fulfillment)
        assert (datetime.datetime.utcnow() - loan.start) < datetime.timedelta(seconds=1)

        # TODO: At some future point it may be relevant that loan.end
        # is None here, but before that happens the loan process will
        # become more complicated, so there's no point in writing
        # a bunch of test code now.

        # Try getting another loan for this book.
        loan2, was_new = pool.loan_to(patron)

        # They're the same!
        eq_(loan, loan2)
        eq_(False, was_new)

        # Make sure we can also loan this book to an IntegrationClient.
        client = self._integration_client()
        loan, was_new = pool.loan_to(client)
        eq_(True, was_new)
        eq_(client, loan.integration_client)
        eq_(pool, loan.license_pool)

        # Loaning the book to the same IntegrationClient twice creates two loans,
        # since these loans could be on behalf of different patrons on the client.
        loan2, was_new = pool.loan_to(client)
        eq_(True, was_new)
        eq_(client, loan2.integration_client)
        eq_(pool, loan2.license_pool)
        assert loan != loan2

    def test_work(self):
        """Test the attribute that finds the Work for a Loan or Hold."""
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]

        # The easy cases.
        loan, is_new = pool.loan_to(patron)
        eq_(work, loan.work)

        loan.license_pool = None
        eq_(None, loan.work)

        # If pool.work is None but pool.edition.work is valid, we use that.
        loan.license_pool = pool
        pool.work = None
        # Presentation_edition is not representing a lendable object, 
        # but it is on a license pool, and a pool has lending capacity.  
        eq_(pool.presentation_edition.work, loan.work)

        # If that's also None, we're helpless.
        pool.presentation_edition.work = None
        eq_(None, loan.work)

    def test_library(self):
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]

        loan, is_new = pool.loan_to(patron)
        eq_(self._default_library, loan.library)

        loan.patron = None
        client = self._integration_client()
        loan.integration_client = client
        eq_(None, loan.library)

        loan.integration_client = None
        eq_(None, loan.library)

        patron.library = self._library()
        loan.patron = patron
        eq_(patron.library, loan.library)


class TestHold(DatabaseTest):

    def test_on_hold_to(self):
        now = datetime.datetime.utcnow()
        later = now + datetime.timedelta(days=1)
        patron = self._patron()
        edition = self._edition()
        pool = self._licensepool(edition)
        self._default_library.setting(Library.ALLOW_HOLDS).value = True
        hold, is_new = pool.on_hold_to(patron, now, later, 4)
        eq_(True, is_new)
        eq_(now, hold.start)
        eq_(later, hold.end)
        eq_(4, hold.position)

        # Now update the position to 0. It's the patron's turn
        # to check out the book.
        hold, is_new = pool.on_hold_to(patron, now, later, 0)
        eq_(False, is_new)
        eq_(now, hold.start)
        # The patron has until `hold.end` to actually check out the book.
        eq_(later, hold.end)
        eq_(0, hold.position)

        # Make sure we can also hold this book for an IntegrationClient.
        client = self._integration_client()
        hold, was_new = pool.on_hold_to(client)
        eq_(True, was_new)
        eq_(client, hold.integration_client)
        eq_(pool, hold.license_pool)

        # Holding the book twice for the same IntegrationClient creates two holds,
        # since they might be for different patrons on the client.
        hold2, was_new = pool.on_hold_to(client)
        eq_(True, was_new)
        eq_(client, hold2.integration_client)
        eq_(pool, hold2.license_pool)
        assert hold != hold2

    def test_holds_not_allowed(self):
        patron = self._patron()
        edition = self._edition()
        pool = self._licensepool(edition)

        self._default_library.setting(Library.ALLOW_HOLDS).value = False
        assert_raises_regexp(
            PolicyException,
            "Holds are disabled for this library.",
            pool.on_hold_to, patron, datetime.datetime.now(), 4
        )
        
    def test_work(self):
        # We don't need to test the functionality--that's tested in
        # Loan--just that Hold also has access to .work.
        patron = self._patron()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]
        hold, is_new = pool.on_hold_to(patron)
        eq_(work, hold.work)

    def test_calculate_until(self):
        start = datetime.datetime(2010, 1, 1)

        # The cycle time is one week.
        default_loan = datetime.timedelta(days=6)
        default_reservation = datetime.timedelta(days=1)
        
        # I'm 20th in line for 4 books.
        #
        # After 6 days, four copies are released and I am 16th in line.
        # After 13 days, those copies are released and I am 12th in line.
        # After 20 days, those copies are released and I am 8th in line.
        # After 27 days, those copies are released and I am 4th in line.
        # After 34 days, those copies are released and get my notification.
        a = Hold._calculate_until(
            start, 20, 4, default_loan, default_reservation)
        eq_(a, start + datetime.timedelta(days=(7*5)-1))

        # If I am 21st in line, I need to wait six weeks.
        b = Hold._calculate_until(
            start, 21, 4, default_loan, default_reservation)
        eq_(b, start + datetime.timedelta(days=(7*6)-1))

        # If I am 3rd in line, I only need to wait six days--that's when
        # I'll get the notification message.
        b = Hold._calculate_until(
            start, 3, 4, default_loan, default_reservation)
        eq_(b, start + datetime.timedelta(days=6))

        # A new person gets the book every week. Someone has the book now
        # and there are 3 people ahead of me in the queue. I will get
        # the book in 6 days + 3 weeks
        c = Hold._calculate_until(
            start, 3, 1, default_loan, default_reservation)
        eq_(c, start + datetime.timedelta(days=(7*4)-1))

        # The book is reserved to me. I need to hurry up and check it out.
        d = Hold._calculate_until(
            start, 0, 1, default_loan, default_reservation)
        eq_(d, start + datetime.timedelta(days=1))

        # If there are no licenses, I will never get the book.
        e = Hold._calculate_until(
            start, 10, 0, default_loan, default_reservation)
        eq_(e, None)

    def test_vendor_hold_end_value_takes_precedence_over_calculated_value(self):
        """If the vendor has provided an estimated availability time,
        that is used in preference to the availability time we
        calculate.
        """
        now = datetime.datetime.utcnow()
        tomorrow = now + datetime.timedelta(days=1)

        patron = self._patron()
        pool = self._licensepool(edition=None)
        hold, is_new = pool.on_hold_to(patron)
        hold.position = 1
        hold.end = tomorrow
        
        default_loan = datetime.timedelta(days=1)
        default_reservation = datetime.timedelta(days=2)
        eq_(tomorrow, hold.until(default_loan, default_reservation))

        calculated_value = hold._calculate_until(
            now, hold.position, pool.licenses_available,
            default_loan, default_reservation
        )

        # If the vendor value is not in the future, it's ignored
        # and the calculated value is used instead.
        def assert_calculated_value_used():
            result = hold.until(default_loan, default_reservation)
            assert (result-calculated_value).seconds < 5
        hold.end = now
        assert_calculated_value_used()

        # The calculated value is also used there is no
        # vendor-provided value.
        hold.end = None
        assert_calculated_value_used()

class TestAnnotation(DatabaseTest):
    def test_set_inactive(self):
        pool = self._licensepool(None)
        annotation, ignore = create(
            self._db, Annotation,
            patron=self._patron(),
            identifier=pool.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        annotation.timestamp = yesterday

        annotation.set_inactive()
        eq_(False, annotation.active)
        eq_(None, annotation.content)
        assert annotation.timestamp > yesterday

    def test_patron_annotations_are_descending(self):
        pool1 = self._licensepool(None)
        pool2 = self._licensepool(None)
        patron = self._patron()
        annotation1, ignore = create(
            self._db, Annotation,
            patron=patron,
            identifier=pool2.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        annotation2, ignore = create(
            self._db, Annotation,
            patron=patron,
            identifier=pool2.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )

        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        today = datetime.datetime.now()
        annotation1.timestamp = yesterday
        annotation2.timestamp = today

        eq_(2, len(patron.annotations))
        eq_(annotation2, patron.annotations[0])
        eq_(annotation1, patron.annotations[1])
    

class TestHyperlink(DatabaseTest):

    def test_add_link(self):
        edition, pool = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        data_source = pool.data_source
        original, ignore = create(self._db, Resource, url="http://bar.com")
        hyperlink, is_new = pool.add_link(
            Hyperlink.DESCRIPTION, "http://foo.com/", data_source, 
            "text/plain", "The content", None, RightsStatus.CC_BY,
            "The rights explanation", original,
            transformation_settings=dict(setting="a setting"))
        eq_(True, is_new)
        rep = hyperlink.resource.representation
        eq_("text/plain", rep.media_type)
        eq_("The content", rep.content)
        eq_(Hyperlink.DESCRIPTION, hyperlink.rel)
        eq_(identifier, hyperlink.identifier)
        eq_(RightsStatus.CC_BY, hyperlink.resource.rights_status.uri)
        eq_("The rights explanation", hyperlink.resource.rights_explanation)
        transformation = hyperlink.resource.derived_through
        eq_(hyperlink.resource, transformation.derivative)
        eq_(original, transformation.original)
        eq_("a setting", transformation.settings.get("setting"))
        eq_([transformation], original.transformations)

    def test_default_filename(self):
        m = Hyperlink._default_filename
        eq_("content", m(Hyperlink.OPEN_ACCESS_DOWNLOAD))
        eq_("cover", m(Hyperlink.IMAGE))
        eq_("cover-thumbnail", m(Hyperlink.THUMBNAIL_IMAGE))

    def test_unmirrored(self):

        ds = DataSource.lookup(self._db, DataSource.GUTENBERG)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        c1 = self._default_collection
        c1.data_source = ds

        # Here's an Identifier associated with a collection.
        work = self._work(with_license_pool=True, collection=c1)
        [pool] = work.license_pools
        i1 = pool.identifier

        # This is a random identifier not associated with the collection.
        i2 = self._identifier()

        def m():
            return Hyperlink.unmirrored(c1).all()

        # Identifier is not in the collection.
        not_in_collection, ignore = i2.add_link(Hyperlink.IMAGE, self._url, ds)
        eq_([], m())

        # Hyperlink rel is not mirrorable.
        wrong_type, ignore = i1.add_link(
            "not mirrorable", self._url, ds, "text/plain"
        )
        eq_([], m())

        # Hyperlink has no associated representation -- it needs to be
        # mirrored, which will create one!
        hyperlink, ignore = i1.add_link(
            Hyperlink.IMAGE, self._url, ds, "image/png"
        )
        eq_([hyperlink], m())

        # Representation is already mirrored, so does not show up
        # in the unmirrored list.
        representation = hyperlink.resource.representation
        representation.set_as_mirrored(self._url)
        eq_([], m())

        # Representation exists in database but is not mirrored -- it needs
        # to be mirrored!
        representation.mirror_url = None
        eq_([hyperlink], m())

        # Hyperlink is associated with a data source other than the
        # data source of the collection. It ought to be mirrored, but
        # this collection isn't responsible for mirroring it.
        hyperlink.data_source = overdrive
        eq_([], m())


class TestResource(DatabaseTest):

    def test_as_delivery_mechanism_for(self):

        # Calling as_delivery_mechanism_for on a Resource that is used
        # to deliver a specific LicensePool returns the appropriate
        # LicensePoolDeliveryMechanism.
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        [lpdm] = pool.delivery_mechanisms
        eq_(lpdm, lpdm.resource.as_delivery_mechanism_for(pool))

        # If there's no relationship between the Resource and 
        # the LicensePoolDeliveryMechanism, as_delivery_mechanism_for
        # returns None.
        w2 = self._work(with_license_pool=True)
        [unrelated] = w2.license_pools
        eq_(None, lpdm.resource.as_delivery_mechanism_for(unrelated))


class TestRepresentation(DatabaseTest):

    def test_normalized_content_path(self):
        eq_("baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/foo/bar"))

        eq_("baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/foo/bar/"))

        eq_("/foo/bar/baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/blah/blah/"))

    def test_best_media_type(self):
        """Test our ability to determine whether the Content-Type
        header should override a presumed media type.
        """
        m = Representation._best_media_type

        # If there are no headers or no content-type header, the
        # presumed media type takes precedence.
        eq_("text/plain", m("http://text/all.about.jpeg", None, "text/plain"))
        eq_("text/plain", m(None, {}, "text/plain"))

        # Most of the time, the content-type header takes precedence over
        # the presumed media type.
        eq_("image/gif", m(None, {"content-type": "image/gif"}, "text/plain"))

        # Except when the content-type header is so generic as to be uselses.
        eq_("text/plain", m(
            None,
            {"content-type": "application/octet-stream;profile=foo"}, 
            "text/plain")
        )

        # If no default media type is specified, but one can be derived from
        # the URL, that one is used as the default.
        eq_("image/jpeg", m(
            "http://images-galore/cover.jpeg",
            {"content-type": "application/octet-stream;profile=foo"},
            None)
        )

        # But a default media type doesn't override a specific
        # Content-Type from the server, even if it superficially makes
        # more sense.
        eq_("image/png", m(
            "http://images-galore/cover.jpeg",
            {"content-type": "image/png"},
            None)
        )


    def test_mirrorable_media_type(self):
        representation, ignore = self._representation(self._url)

        # Ebook formats and image formats get mirrored.
        representation.media_type = Representation.EPUB_MEDIA_TYPE
        eq_(True, representation.mirrorable_media_type)
        representation.media_type = Representation.MOBI_MEDIA_TYPE
        eq_(True, representation.mirrorable_media_type)
        representation.media_type = Representation.JPEG_MEDIA_TYPE
        eq_(True, representation.mirrorable_media_type)

        # Other media types don't get mirrored
        representation.media_type = "text/plain"
        eq_(False, representation.mirrorable_media_type)

    def test_guess_media_type(self):
        m = Representation.guess_media_type

        eq_(Representation.JPEG_MEDIA_TYPE, m("file.jpg"))
        eq_(Representation.ZIP_MEDIA_TYPE, m("file.ZIP"))

        for extension, media_type in Representation.MEDIA_TYPE_FOR_EXTENSION.items():
            filename = "file" + extension
            eq_(media_type, m(filename))

        eq_(None, m(None))
        eq_(None, m("file"))
        eq_(None, m("file.unknown-extension"))

    def test_external_media_type_and_extension(self):
        """Test the various transformations that might happen to media type
        and extension when we mirror a representation.
        """

        # An unknown file at /foo
        representation, ignore = self._representation(self._url, "text/unknown")
        eq_("text/unknown", representation.external_media_type)
        eq_('', representation.extension())

        # A text file at /foo
        representation, ignore = self._representation(self._url, "text/plain")
        eq_("text/plain", representation.external_media_type)
        eq_('.txt', representation.extension())

        # A JPEG at /foo.jpg
        representation, ignore = self._representation(
            self._url + ".jpg", "image/jpeg"
        )
        eq_("image/jpeg", representation.external_media_type)
        eq_(".jpg", representation.extension())

        # A JPEG at /foo
        representation, ignore = self._representation(self._url, "image/jpeg")
        eq_("image/jpeg", representation.external_media_type)
        eq_(".jpg", representation.extension())

        # A PNG at /foo
        representation, ignore = self._representation(self._url, "image/png")
        eq_("image/png", representation.external_media_type)
        eq_(".png", representation.extension())

        # An EPUB at /foo.epub.images -- information present in the URL
        # is preserved.
        representation, ignore = self._representation(
            self._url + '.epub.images', Representation.EPUB_MEDIA_TYPE
        )
        eq_(Representation.EPUB_MEDIA_TYPE, representation.external_media_type)
        eq_(".epub.images", representation.extension())
        

        # SVG representations are always converted to PNG on the way out.
        # This affects the media type.
        representation, ignore = self._representation(self._url + ".svg", "image/svg+xml")
        eq_("image/png", representation.external_media_type)
        eq_(".png", representation.extension())

    def test_set_fetched_content(self):
        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content("some text")
        eq_("some text", representation.content_fh().read())

    def test_set_fetched_content_file_on_disk(self):
        filename = "set_fetched_content_file_on_disk.txt"
        path = os.path.join(self.tmp_data_dir, filename)
        open(path, "w").write("some text")

        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(None, filename)
        fh = representation.content_fh()
        eq_("some text", fh.read())

    def test_unicode_content_utf8_default(self):
        unicode_content = u"It’s complicated."

        utf8_content = unicode_content.encode("utf8")

        # This bytestring can be decoded as Windows-1252, but that
        # would be the wrong answer.
        bad_windows_1252 = utf8_content.decode("windows-1252")
        eq_(u"Itâ€™s complicated.", bad_windows_1252)

        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(unicode_content, None)
        eq_(utf8_content, representation.content)

        # By trying to interpret the content as UTF-8 before falling back to 
        # Windows-1252, we get the right answer.
        eq_(unicode_content, representation.unicode_content)

    def test_unicode_content_windows_1252(self):
        unicode_content = u"A “love” story"
        windows_1252_content = unicode_content.encode("windows-1252")

        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(windows_1252_content)
        eq_(windows_1252_content, representation.content)
        eq_(unicode_content, representation.unicode_content)

    def test_unicode_content_is_none_when_decoding_is_impossible(self):
        byte_content = b"\x81\x02\x03"
        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(byte_content)
        eq_(byte_content, representation.content)
        eq_(None, representation.unicode_content)

    def test_presumed_media_type(self):
        h = DummyHTTPClient()

        # In the absence of a content-type header, the presumed_media_type
        # takes over.
        h.queue_response(200, None, content='content')
        representation, cached = Representation.get(
            self._db, 'http://url', do_get=h.do_get, max_age=0,
            presumed_media_type="text/xml"
        )
        eq_('text/xml', representation.media_type)

        # In the presence of a generic content-type header, the
        # presumed_media_type takes over.
        h.queue_response(200, 'application/octet-stream',
                         content='content')
        representation, cached = Representation.get(
            self._db, 'http://url', do_get=h.do_get, max_age=0,
            presumed_media_type="text/xml"
        )
        eq_('text/xml', representation.media_type)

        # A non-generic content-type header takes precedence over
        # presumed_media_type.
        h.queue_response(200, 'text/plain', content='content')
        representation, cached = Representation.get(
            self._db, 'http://url', do_get=h.do_get, max_age=0,
            presumed_media_type="text/xml"
        )
        eq_('text/plain', representation.media_type)


    def test_404_creates_cachable_representation(self):
        h = DummyHTTPClient()
        h.queue_response(404)

        url = self._url
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

        representation2, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(True, cached)
        eq_(representation, representation2)

    def test_302_creates_cachable_representation(self):
        h = DummyHTTPClient()
        h.queue_response(302)

        url = self._url
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

        representation2, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(True, cached)
        eq_(representation, representation2)

    def test_500_creates_uncachable_representation(self):
        h = DummyHTTPClient()
        h.queue_response(500)
        url = self._url
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

        h.queue_response(500)
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

    def test_response_reviewer_impacts_representation(self):
        h = DummyHTTPClient()
        h.queue_response(200, media_type='text/html')

        def reviewer(response):
            status, headers, content = response
            if 'html' in headers['content-type']:
                raise Exception("No. Just no.")

        representation, cached = Representation.get(
            self._db, self._url, do_get=h.do_get, response_reviewer=reviewer
        )
        assert "No. Just no." in representation.fetch_exception
        eq_(False, cached)

    def test_exception_handler(self):
        def oops(*args, **kwargs):
            raise Exception("oops!")

        # By default exceptions raised during get() are 
        # recorded along with the (empty) Representation objects
        representation, cached = Representation.get(
            self._db, self._url, do_get=oops,
        )
        assert representation.fetch_exception.strip().endswith(
            "Exception: oops!"
        )
        eq_(None, representation.content)
        eq_(None, representation.status_code)

        # But we can ask that exceptions simply be re-raised instead of
        # being handled.
        assert_raises_regexp(
            Exception, "oops!", Representation.get,
            self._db, self._url, do_get=oops,
            exception_handler=Representation.reraise_exception
        )

    def test_url_extension(self):
        epub, ignore = self._representation("test.epub")
        eq_(".epub", epub.url_extension)

        epub3, ignore = self._representation("test.epub3")
        eq_(".epub3", epub3.url_extension)

        noimages, ignore = self._representation("test.epub.noimages")
        eq_(".epub.noimages", noimages.url_extension)

        unknown, ignore = self._representation("test.1234.abcd")
        eq_(".abcd", unknown.url_extension)

        no_extension, ignore = self._representation("test")
        eq_(None, no_extension.url_extension)

        no_filename, ignore = self._representation("foo.com/")
        eq_(None, no_filename.url_extension)

        query_param, ignore = self._representation("test.epub?version=3")
        eq_(".epub", query_param.url_extension)

    def test_clean_media_type(self):
        m = Representation._clean_media_type
        eq_("image/jpeg", m("image/jpeg"))
        eq_("application/atom+xml",
            m("application/atom+xml;profile=opds-catalog;kind=acquisition")
        )

    def test_extension(self):
        m = Representation._extension
        eq_(".jpg", m("image/jpeg"))
        eq_(".mobi", m("application/x-mobipocket-ebook"))
        eq_("", m("no/such-media-type"))

    def test_default_filename(self):

        # Here's a common sort of URL.
        url = "http://example.com/foo/bar/baz.txt"
        representation, ignore = self._representation(url)

        # Here's the filename we would give it if we were to mirror
        # it.
        filename = representation.default_filename()
        eq_("baz.txt", filename)

        # File extension is always set based on media type.
        filename = representation.default_filename(destination_type="image/png")
        eq_("baz.png", filename)

        # The original file extension is not treated as reliable and
        # need not be present.
        url = "http://example.com/1"
        representation, ignore = self._representation(url, "text/plain")
        filename = representation.default_filename()
        eq_("1.txt", filename)

        # Again, file extension is always set based on media type.
        filename = representation.default_filename(destination_type="image/png")
        eq_("1.png", filename)

        # In this case, we don't have an extension registered for
        # text/unknown, so the extension is omitted.
        filename = representation.default_filename(destination_type="text/unknown")
        eq_("1", filename)

        # This URL has no path component, so we can't even come up with a
        # decent default filename. We have to go with 'resource'.
        representation, ignore = self._representation("http://example.com/", "text/unknown")
        eq_('resource', representation.default_filename())
        eq_('resource.png', representation.default_filename(destination_type="image/png"))

        # But if we know what type of thing we're linking to, we can
        # do a little better.
        link = Hyperlink(rel=Hyperlink.IMAGE)
        filename = representation.default_filename(link=link)
        eq_('cover', filename)
        filename = representation.default_filename(link=link, destination_type="image/png")
        eq_('cover.png', filename)

    def test_automatic_conversion_svg_to_png(self):
        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

<svg xmlns="http://www.w3.org/2000/svg" width="100" height="50">
    <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
</svg>"""
        edition = self._edition()
        pool = self._licensepool(edition)
        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, None, source, Representation.SVG_MEDIA_TYPE,
            content=svg)
        representation = hyperlink.resource.representation

        eq_(Representation.SVG_MEDIA_TYPE, representation.media_type)
        eq_(Representation.PNG_MEDIA_TYPE, representation.external_media_type)

        # If we get the Representation as a PIL image, it's automatically
        # converted to PNG.
        image = representation.as_image()
        eq_("PNG", image.format)
        expect = StringIO()
        image.save(expect, format='PNG')

        # When we prepare to mirror the Representation to an external
        # file store, it's automatically converted to PNG.
        external_fh = representation.external_content()
        eq_(expect.getvalue(), external_fh.read())
        eq_(Representation.PNG_MEDIA_TYPE, representation.external_media_type)
        
        # Verify that the conversion happened correctly.

        # Even though the SVG image is smaller than the thumbnail
        # size, thumbnailing it will create a separate PNG-format
        # Representation, because we want all the thumbnails to be
        # bitmaps.
        thumbnail, is_new = representation.scale(
            Edition.MAX_THUMBNAIL_HEIGHT, Edition.MAX_THUMBNAIL_WIDTH,
            self._url, Representation.PNG_MEDIA_TYPE
        )
        eq_(True, is_new)
        assert thumbnail != hyperlink.resource.representation
        eq_(Representation.PNG_MEDIA_TYPE, thumbnail.media_type)

    def test_cautious_http_get(self):

        h = DummyHTTPClient()
        h.queue_response(200, content="yay")

        # If the domain is obviously safe, the GET request goes through,
        # with no HEAD request being made.
        m = Representation.cautious_http_get
        status, headers, content = m(
            "http://safe.org/", {}, do_not_access=['unsafe.org'],
            do_get=h.do_get, cautious_head_client=object()
        )
        eq_(200, status)
        eq_("yay", content)

        # If the domain is obviously unsafe, no GET request or HEAD
        # request is made.
        status, headers, content = m(
            "http://unsafe.org/", {}, do_not_access=['unsafe.org'],
            do_get=object(), cautious_head_client=object()
        )
        eq_(417, status)
        eq_("Cautiously decided not to make a GET request to http://unsafe.org/",
            content)

        # If the domain is potentially unsafe, a HEAD request is made,
        # and the answer depends on its outcome.

        # Here, the HEAD request redirects to a prohibited site.
        def mock_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(location="http://unsafe.org/")
            )
        status, headers, content = m(
            "http://caution.org/", {},
            do_not_access=['unsafe.org'],
            check_for_redirect=['caution.org'],
            do_get=object(), cautious_head_client=mock_redirect
        )
        eq_(417, status)
        eq_("application/vnd.librarysimplified-did-not-make-request",
            headers['content-type'])
        eq_("Cautiously decided not to make a GET request to http://caution.org/",
            content)

        # Here, the HEAD request redirects to an allowed site.
        h.queue_response(200, content="good content")
        def mock_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(location="http://safe.org/")
            )
        status, headers, content = m(
            "http://caution.org/", {},
            do_not_access=['unsafe.org'],
            check_for_redirect=['caution.org'],
            do_get=h.do_get, cautious_head_client=mock_redirect
        )
        eq_(200, status)
        eq_("good content", content)

    def test_get_would_be_useful(self):
        """Test the method that determines whether a GET request will go (or
        redirect) to a site we don't to make requests to.
        """
        safe = Representation.get_would_be_useful

        # If get_would_be_useful tries to use this object to make a HEAD
        # request, the test will blow up.
        fake_head = object()

        # Most sites are safe with no HEAD request necessary.
        eq_(True, safe("http://www.safe-site.org/book.epub", {},
                       head_client=fake_head))

        # gutenberg.org is problematic, no HEAD request necessary.
        eq_(False, safe("http://www.gutenberg.org/book.epub", {},
                        head_client=fake_head))

        # do_not_access controls which domains should always be
        # considered unsafe.
        eq_(
            False, safe(
                "http://www.safe-site.org/book.epub", {},
                do_not_access=['safe-site.org'], head_client=fake_head
            )
        )
        eq_(
            True, safe(
                "http://www.gutenberg.org/book.epub", {},
                do_not_access=['safe-site.org'], head_client=fake_head
            )
        )

        # Domain match is based on a subdomain match, not a substring
        # match.
        eq_(True, safe("http://www.not-unsafe-site.org/book.epub", {},
                       do_not_access=['unsafe-site.org'],
                       head_client=fake_head))

        # Some domains (unglue.it) are known to make surprise
        # redirects to unsafe domains. For these, we must make a HEAD
        # request to check.

        def bad_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(
                    location="http://www.gutenberg.org/a-book.html"
                )
            )
        eq_(False, safe("http://www.unglue.it/book", {},
                        head_client=bad_redirect))

        def good_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301,
                dict(location="http://www.some-other-site.org/a-book.epub")
            )
        eq_(
            True,
            safe("http://www.unglue.it/book", {}, head_client=good_redirect)
        )

        def not_a_redirect(*args, **kwargs):
            return MockRequestsResponse(200)
        eq_(True, safe("http://www.unglue.it/book", {},
                       head_client=not_a_redirect))

        # The `check_for_redirect` argument controls which domains are
        # checked using HEAD requests. Here, we customise it to check
        # a site other than unglue.it.
        eq_(False, safe("http://www.questionable-site.org/book.epub", {},
                        check_for_redirect=['questionable-site.org'],
                        head_client=bad_redirect))

    def test_best_thumbnail(self):
        # This Representation has no thumbnails.
        representation, ignore = self._representation()
        eq_(None, representation.best_thumbnail)

        # Now it has two thumbnails, neither of which is mirrored.
        t1, ignore = self._representation()
        t2, ignore = self._representation()
        for i in t1, t2:
            representation.thumbnails.append(i)
        
        # There's no distinction between the thumbnails, so the first one
        # is selected as 'best'.
        eq_(t1, representation.best_thumbnail)
        
        # If one of the thumbnails is mirrored, it becomes the 'best'
        # thumbnail.
        t2.set_as_mirrored(self._url)
        eq_(t2, representation.best_thumbnail)

class TestCoverResource(DatabaseTest):

    def test_set_cover(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        thumbnail_mirror = self._url
        sample_cover_path = self.sample_cover_path("test-book-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            content=open(sample_cover_path).read())
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(None, edition.cover_thumbnail_url)

        # Now scale the cover.
        thumbnail, ignore = self._representation()
        thumbnail.thumbnail_of = full_rep
        thumbnail.set_as_mirrored(thumbnail_mirror)
        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(thumbnail_mirror, edition.cover_thumbnail_url)

    def test_set_cover_for_very_small_image(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        sample_cover_path = self.sample_cover_path("tiny-image-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path).read())
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(mirror, edition.cover_thumbnail_url)

    def test_set_cover_for_smallish_image_uses_full_sized_image_as_thumbnail(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        sample_cover_path = self.sample_cover_path("tiny-image-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path).read())
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        # For purposes of this test, pretend that the full-sized image is 
        # larger than a thumbnail, but not terribly large.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(mirror, edition.cover_thumbnail_url)

        # If the full-sized image had been slightly larger, we would have
        # decided not to use a thumbnail at all.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT + 1
        edition.cover_thumbnail_url = None
        edition.set_cover(hyperlink.resource)
        eq_(None, edition.cover_thumbnail_url)


    def test_attempt_to_scale_non_image_sets_scale_exception(self):
        rep, ignore = self._representation(media_type="text/plain", content="foo")
        scaled, ignore = rep.scale(300, 600, self._url, "image/png")
        expect = "ValueError: Cannot load non-image representation as image: type text/plain"
        assert scaled == rep
        assert expect in rep.scale_exception
        
    def test_cannot_scale_to_non_image(self):
        rep, ignore = self._representation(media_type="image/png", content="foo")
        assert_raises_regexp(
            ValueError, 
            "Unsupported destination media type: text/plain",
            rep.scale, 300, 600, self._url, "text/plain")

    def test_success(self):
        cover = self.sample_cover_representation("test-book-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        eq_(True, is_new)
        eq_(url, thumbnail.url)
        eq_(None, thumbnail.mirror_url)
        eq_(None, thumbnail.mirrored_at)
        eq_(cover, thumbnail.thumbnail_of)
        eq_("image/png", thumbnail.media_type)
        eq_(300, thumbnail.image_height)
        eq_(200, thumbnail.image_width)

        # Try to scale the image to the same URL, and nothing will
        # happen, even though the proposed image size is
        # different.
        thumbnail2, is_new = cover.scale(400, 700, url, "image/png")
        eq_(thumbnail2, thumbnail)
        eq_(False, is_new)

        # Let's say the thumbnail has been mirrored.
        thumbnail.set_as_mirrored(self._url)

        old_content = thumbnail.content
        # With the force argument we can forcibly re-scale an image,
        # changing its size.
        eq_([thumbnail], cover.thumbnails)
        thumbnail2, is_new = cover.scale(
            400, 700, url, "image/png", force=True)
        eq_(True, is_new)
        eq_([thumbnail2], cover.thumbnails)
        eq_(cover, thumbnail2.thumbnail_of)

        # The same Representation, but now its data is different.
        eq_(thumbnail, thumbnail2)
        assert thumbnail2.content != old_content
        eq_(400, thumbnail.image_height)
        eq_(266, thumbnail.image_width)

        # The thumbnail has been regenerated, so it needs to be mirrored again.
        eq_(None, thumbnail.mirrored_at)

    def test_book_with_odd_aspect_ratio(self):
        # This book is 1200x600.
        cover = self.sample_cover_representation("childrens-book-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 400, url, "image/png")
        eq_(True, is_new)
        eq_(url, thumbnail.url)
        eq_(cover, thumbnail.thumbnail_of)
        # The width was reduced to max_width, a reduction of a factor of three
        eq_(400, thumbnail.image_width)
        # The height was also reduced by a factory of three, even
        # though this takes it below max_height.
        eq_(200, thumbnail.image_height)

    def test_book_smaller_than_thumbnail_size(self):
        # This book is 200x200. No thumbnail will be created.
        cover = self.sample_cover_representation("tiny-image-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        eq_(False, is_new)
        eq_(thumbnail, cover)
        eq_([], cover.thumbnails)
        eq_(None, thumbnail.thumbnail_of)
        assert thumbnail.url != url

    def test_image_type_priority(self):
        """Test the image_type_priority method.

        All else being equal, we prefer some image types over
        others. Better image types get lower numbers.
        """
        m = Resource.image_type_priority
        eq_(None, m(None))
        eq_(None, m(Representation.EPUB_MEDIA_TYPE))

        png = m(Representation.PNG_MEDIA_TYPE)
        jpeg = m(Representation.JPEG_MEDIA_TYPE)
        gif = m(Representation.GIF_MEDIA_TYPE)
        svg = m(Representation.SVG_MEDIA_TYPE)

        assert png < jpeg
        assert jpeg < gif
        assert gif < svg
        
    def test_best_covers_among(self):
        # Here's a book with a thumbnail image.
        edition, pool = self._edition(with_license_pool=True)

        link1, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_no_representation = link1.resource

        # A resource with no representation is not considered even if
        # it's the only option.
        eq_([], Resource.best_covers_among([resource_with_no_representation]))

        # Here's an abysmally bad cover.
        lousy_cover = self.sample_cover_representation("tiny-image-cover.png")
        lousy_cover.image_height=1
        lousy_cover.image_width=10000 
        link2, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_lousy_cover = link2.resource
        resource_with_lousy_cover.representation = lousy_cover

        # This cover is so bad that it's not even considered if it's
        # the only option.
        eq_([], Resource.best_covers_among([resource_with_lousy_cover]))

        # Here's a decent cover.
        decent_cover = self.sample_cover_representation("test-book-cover.png")
        link3, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_decent_cover = link3.resource
        resource_with_decent_cover.representation = decent_cover

        # This cover is at least good enough to pass muster if there
        # is no other option.
        eq_(
            [resource_with_decent_cover], 
            Resource.best_covers_among([resource_with_decent_cover])
        )

        # Let's create another cover image with identical
        # characteristics.
        link4, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        decent_cover_2 = self.sample_cover_representation("test-book-cover.png")
        resource_with_decent_cover_2 = link4.resource
        resource_with_decent_cover_2.representation = decent_cover_2

        l = [resource_with_decent_cover, resource_with_decent_cover_2]

        # best_covers_among() can't decide between the two -- they have
        # the same score.
        eq_(set(l), set(Resource.best_covers_among(l)))

        # All else being equal, if one cover is an PNG and the other
        # is a JPEG, we prefer the PNG.
        resource_with_decent_cover.representation.media_type = Representation.JPEG_MEDIA_TYPE
        eq_([resource_with_decent_cover_2], Resource.best_covers_among(l))
        
        # But if the metadata wrangler said to use the JPEG, we use the JPEG.
        metadata_wrangler = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )
        resource_with_decent_cover.data_source = metadata_wrangler

        # ...the decision becomes easy.
        eq_([resource_with_decent_cover], Resource.best_covers_among(l))

    def test_rejection_and_approval(self):
        # Create a Resource.
        edition, pool = self._edition(with_open_access_download=True)
        link = pool.add_link(Hyperlink.IMAGE, self._url, pool.data_source)[0]
        cover = link.resource

        # Give it all the right covers.
        cover_rep = self.sample_cover_representation("test-book-cover.png")
        thumbnail_rep = self.sample_cover_representation("test-book-cover.png")
        cover.representation = cover_rep
        cover_rep.thumbnails.append(thumbnail_rep)

        # Set its quality.
        cover.quality_as_thumbnail_image
        original_quality = cover.quality
        eq_(True, original_quality > 0)

        # Rejecting it sets the voted_quality and quality below zero.
        cover.reject()
        eq_(True, cover.voted_quality < 0)
        eq_(True, cover.quality < 0)

        # If the quality is already below zero, rejecting it doesn't
        # change the value.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        eq_(True, last_votes_for_quality > 0)
        cover.reject()
        eq_(last_voted_quality, cover.voted_quality)
        eq_(last_votes_for_quality, cover.votes_for_quality)
        eq_(last_quality, cover.quality)

        # If the quality is approved, the votes are updated as expected.
        cover.approve()
        eq_(0, cover.voted_quality)
        eq_(2, cover.votes_for_quality)
        # Because the number of human votes have gone up in contention,
        # the overall quality is lower than it was originally.
        eq_(True, cover.quality < original_quality)
        # But it's still above zero.
        eq_(True, cover.quality > 0)

        # Approving the cover again improves its quality further.
        last_quality = cover.quality
        cover.approve()
        eq_(True, cover.voted_quality > 0)
        eq_(3, cover.votes_for_quality)
        eq_(True, cover.quality > last_quality)

        # Rejecting the cover again will make the existing value negative.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        cover.reject()
        eq_(-last_voted_quality, cover.voted_quality)
        eq_(True, cover.quality < 0)

        eq_(last_votes_for_quality+1, cover.votes_for_quality)

    def test_quality_as_thumbnail_image(self):

        # Get some data sources ready, since a big part of image
        # quality comes from data source.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        gutenberg_cover_generator = DataSource.lookup(
            self._db, DataSource.GUTENBERG_COVER_GENERATOR
        )
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        metadata_wrangler = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )

        # Here's a book with a thumbnail image.
        edition, pool = self._edition(with_license_pool=True)
        hyperlink, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, overdrive
        )
        resource = hyperlink.resource
        
        # Without a representation, the thumbnail image is useless.
        eq_(0, resource.quality_as_thumbnail_image)

        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        cover = self.sample_cover_representation("tiny-image-cover.png")
        resource.representation = cover
        eq_(1.0, resource.quality_as_thumbnail_image)

        # Changing the image aspect ratio affects the quality as per
        # thumbnail_size_quality_penalty.
        cover.image_height = ideal_height * 2
        cover.image_width = ideal_width
        eq_(0.5, resource.quality_as_thumbnail_image)
        
        # Changing the data source also affects the quality. Gutenberg
        # covers are penalized heavily...
        cover.image_height = ideal_height
        cover.image_width = ideal_width
        resource.data_source = gutenberg
        eq_(0.5, resource.quality_as_thumbnail_image)

        # The Gutenberg cover generator is penalized less heavily.
        resource.data_source = gutenberg_cover_generator
        eq_(0.6, resource.quality_as_thumbnail_image)

        # The metadata wrangler actually gets a _bonus_, to encourage the
        # use of its covers over those provided by license sources.
        resource.data_source = metadata_wrangler
        eq_(2, resource.quality_as_thumbnail_image)

    def test_thumbnail_size_quality_penalty(self):
        """Verify that Representation._cover_size_quality_penalty penalizes
        images that are the wrong aspect ratio, or too small.
        """

        ideal_ratio = Identifier.IDEAL_COVER_ASPECT_RATIO
        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        def f(width, height):
            return Representation._thumbnail_size_quality_penalty(width, height)

        # In the absence of any size information we assume
        # everything's fine.
        eq_(1, f(None, None))

        # The perfect image has no penalty.
        eq_(1, f(ideal_width, ideal_height))

        # An image that is the perfect aspect ratio, but too large,
        # has no penalty.
        eq_(1, f(ideal_width*2, ideal_height*2))
        
        # An image that is the perfect aspect ratio, but is too small,
        # is penalised.
        eq_(1/4.0, f(ideal_width*0.5, ideal_height*0.5))
        eq_(1/16.0, f(ideal_width*0.25, ideal_height*0.25))

        # An image that deviates from the perfect aspect ratio is
        # penalized in proportion.
        eq_(1/2.0, f(ideal_width*2, ideal_height))
        eq_(1/2.0, f(ideal_width, ideal_height*2))
        eq_(1/4.0, f(ideal_width*4, ideal_height))
        eq_(1/4.0, f(ideal_width, ideal_height*4))


class TestDeliveryMechanism(DatabaseTest):

    def setup(self):
        super(TestDeliveryMechanism, self).setup()
        self.epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        self.epub_adobe_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        self.overdrive_streaming_text, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM)

    def test_implicit_medium(self):
        eq_(Edition.BOOK_MEDIUM, self.epub_no_drm.implicit_medium)
        eq_(Edition.BOOK_MEDIUM, self.epub_adobe_drm.implicit_medium)
        eq_(Edition.BOOK_MEDIUM, self.overdrive_streaming_text.implicit_medium)

    def test_is_media_type(self):
        eq_(False, DeliveryMechanism.is_media_type(None))
        eq_(True, DeliveryMechanism.is_media_type(Representation.EPUB_MEDIA_TYPE))
        eq_(False, DeliveryMechanism.is_media_type(DeliveryMechanism.KINDLE_CONTENT_TYPE))
        eq_(False, DeliveryMechanism.is_media_type(DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE))

    def test_is_streaming(self):
        eq_(False, self.epub_no_drm.is_streaming)
        eq_(False, self.epub_adobe_drm.is_streaming)
        eq_(True, self.overdrive_streaming_text.is_streaming)

    def test_drm_scheme_media_type(self):
        eq_(None, self.epub_no_drm.drm_scheme_media_type)
        eq_(DeliveryMechanism.ADOBE_DRM, self.epub_adobe_drm.drm_scheme_media_type)
        eq_(None, self.overdrive_streaming_text.drm_scheme_media_type)

    def test_content_type_media_type(self):
        eq_(Representation.EPUB_MEDIA_TYPE, self.epub_no_drm.content_type_media_type)
        eq_(Representation.EPUB_MEDIA_TYPE, self.epub_adobe_drm.content_type_media_type)
        eq_(Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
            self.overdrive_streaming_text.content_type_media_type)

    def test_default_fulfillable(self):
        mechanism, is_new = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, 
            DeliveryMechanism.ADOBE_DRM
        )
        eq_(False, is_new)
        eq_(True, mechanism.default_client_can_fulfill)

        mechanism, is_new = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE, 
            DeliveryMechanism.STREAMING_DRM
        )
        eq_(True, is_new)
        eq_(False, mechanism.default_client_can_fulfill)

    def test_association_with_licensepool(self):
        ignore, with_download = self._edition(with_open_access_download=True)
        [lpmech] = with_download.delivery_mechanisms
        eq_("Dummy content", lpmech.resource.representation.content)
        mech = lpmech.delivery_mechanism
        eq_(Representation.EPUB_MEDIA_TYPE, mech.content_type)
        eq_(mech.NO_DRM, mech.drm_scheme)

    def test_compatible_with(self):
        """Test the rules about which DeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """
        epub_adobe, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        pdf_adobe, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        pdf_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        streaming, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )

        # A non-streaming DeliveryMechanism is compatible only with
        # itself or a streaming mechanism.
        eq_(False, epub_adobe.compatible_with(None))
        eq_(False, epub_adobe.compatible_with("Not a DeliveryMechanism"))
        eq_(False, epub_adobe.compatible_with(epub_no_drm))
        eq_(False, epub_adobe.compatible_with(pdf_adobe))
        eq_(False, epub_no_drm.compatible_with(pdf_no_drm))
        eq_(True, epub_adobe.compatible_with(epub_adobe))
        eq_(True, epub_adobe.compatible_with(streaming))

        # A streaming mechanism is compatible with anything.
        eq_(True, streaming.compatible_with(epub_adobe))
        eq_(True, streaming.compatible_with(pdf_adobe))
        eq_(True, streaming.compatible_with(epub_no_drm))

        # Rules are slightly different for open-access books: books
        # in any format are compatible so long as they have no DRM.
        eq_(True, epub_no_drm.compatible_with(pdf_no_drm, True))
        eq_(False, epub_no_drm.compatible_with(pdf_adobe, True))


class TestRightsStatus(DatabaseTest):

    def test_lookup(self):
        status = RightsStatus.lookup(self._db, RightsStatus.IN_COPYRIGHT)
        eq_(RightsStatus.IN_COPYRIGHT, status.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.IN_COPYRIGHT), status.name)
        
        status = RightsStatus.lookup(self._db, RightsStatus.CC0)
        eq_(RightsStatus.CC0, status.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.CC0), status.name)
        
        status = RightsStatus.lookup(self._db, "not a known rights uri")
        eq_(RightsStatus.UNKNOWN, status.uri)
        eq_(RightsStatus.NAMES.get(RightsStatus.UNKNOWN), status.name)

    def test_unique_uri_constraint(self):
        # We already have this RightsStatus.
        status = RightsStatus.lookup(self._db, RightsStatus.IN_COPYRIGHT)

        # Let's try to create another one with the same URI.
        dupe = RightsStatus(uri=RightsStatus.IN_COPYRIGHT)
        self._db.add(dupe)

        # Nope.
        assert_raises(IntegrityError, self._db.commit)


class TestCredentials(DatabaseTest):
    
    def test_temporary_token(self):

        # Create a temporary token good for one hour.
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        patron = self._patron()
        now = datetime.datetime.utcnow() 
        expect_expires = now + duration
        token, is_new = Credential.temporary_token_create(
            self._db, data_source, "some random type", patron, duration)
        eq_(data_source, token.data_source)
        eq_("some random type", token.type)
        eq_(patron, token.patron)
        expires_difference = abs((token.expires-expect_expires).seconds)
        assert expires_difference < 2

        # Now try to look up the credential based solely on the UUID.
        new_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential)
        eq_(new_token, token)

        # When we call lookup_and_expire_temporary_token, the token is automatically
        # expired and we cannot use it anymore.
        new_token = Credential.lookup_and_expire_temporary_token(
            self._db, data_source, token.type, token.credential)
        eq_(new_token, token)        
        assert new_token.expires < now

        new_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential)
        eq_(None, new_token)

        new_token = Credential.lookup_and_expire_temporary_token(
            self._db, data_source, token.type, token.credential)
        eq_(None, new_token)
 
        # A token with no expiration date is treated as expired...
        token.expires = None
        self._db.commit()
        no_expiration_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential)
        eq_(None, no_expiration_token)

        # ...unless we specifically say we're looking for a persistent token.
        no_expiration_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential, 
            allow_persistent_token=True
        )
        eq_(token, no_expiration_token)

    def test_specify_value_of_temporary_token(self):
        """By default, a temporary token has a randomly generated value, but
        you can give a specific value to represent a temporary token you got
        from somewhere else.
        """
        patron = self._patron()
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        token, is_new = Credential.temporary_token_create(
            self._db, data_source, "some random type", patron, duration,
            "Some random value"
        )
        eq_("Some random value", token.credential)
        
    def test_temporary_token_overwrites_old_token(self):
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        patron = self._patron()
        old_token, is_new = Credential.temporary_token_create(
            self._db, data_source, "some random type", patron, duration)
        eq_(True, is_new)
        old_credential = old_token.credential

        # Creating a second temporary token overwrites the first.
        token, is_new = Credential.temporary_token_create(
            self._db, data_source, "some random type", patron, duration)
        eq_(False, is_new)
        eq_(token.id, old_token.id)
        assert old_credential != token.credential

    def test_persistent_token(self):

        # Create a persistent token.
        data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        patron = self._patron()
        token, is_new = Credential.persistent_token_create(
            self._db, data_source, "some random type", patron
        )
        eq_(data_source, token.data_source)
        eq_("some random type", token.type)
        eq_(patron, token.patron)

        # Now try to look up the credential based solely on the UUID.
        new_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential, 
            allow_persistent_token=True
        )
        eq_(new_token, token)
        credential = new_token.credential

        # We can keep calling lookup_by_token and getting the same
        # Credential object with the same .credential -- it doesn't
        # expire.
        again_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential, 
            allow_persistent_token=True
        )
        eq_(again_token, new_token)
        eq_(again_token.credential, credential)

    def test_cannot_look_up_nonexistent_token(self):
        data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        new_token = Credential.lookup_by_token(
            self._db, data_source, "no such type", "no such credential")
        eq_(None, new_token)


class TestDelegatedPatronIdentifier(DatabaseTest):

    def test_get_one_or_create(self):
        library_uri = self._url
        patron_identifier = self._str
        identifier_type = DelegatedPatronIdentifier.ADOBE_ACCOUNT_ID
        def make_id():
            return "id1"
        identifier, is_new = DelegatedPatronIdentifier.get_one_or_create(
            self._db, library_uri, patron_identifier, identifier_type,
            make_id
        )
        eq_(True, is_new)
        eq_(library_uri, identifier.library_uri)
        eq_(patron_identifier, identifier.patron_identifier)
        # id_1() was called.
        eq_("id1", identifier.delegated_identifier)

        # Try the same thing again but provide a different create_function
        # that raises an exception if called.
        def explode():
            raise Exception("I should never be called.")
        identifier2, is_new = DelegatedPatronIdentifier.get_one_or_create(
            self._db, library_uri, patron_identifier, identifier_type, explode
        )
        # The existing identifier was looked up.
        eq_(False, is_new)
        eq_(identifier2.id, identifier.id)
        # id_2() was not called.
        eq_("id1", identifier2.delegated_identifier)


class TestDRMDeviceIdentifier(DatabaseTest):

    def setup(self):
        super(TestDRMDeviceIdentifier, self).setup()
        self.data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        self.patron = self._patron()
        self.credential, ignore = Credential.persistent_token_create(
            self._db, self.data_source, "Some Credential", self.patron)
        
    def test_devices_for_credential(self):
        device_id_1, new = self.credential.register_drm_device_identifier("foo")
        eq_("foo", device_id_1.device_identifier)
        eq_(self.credential, device_id_1.credential)
        eq_(True, new)

        device_id_2, new = self.credential.register_drm_device_identifier("foo")
        eq_(device_id_1, device_id_2)
        eq_(False, new)
        
        device_id_3, new = self.credential.register_drm_device_identifier("bar")

        eq_(set([device_id_1, device_id_3]), set(self.credential.drm_device_identifiers))

    def test_deregister(self):
        device, new = self.credential.register_drm_device_identifier("foo")
        self.credential.deregister_drm_device_identifier("foo")
        eq_([], self.credential.drm_device_identifiers)
        eq_([], self._db.query(DRMDeviceIdentifier).all())
        
class TestPatron(DatabaseTest):

    def test_identifier_to_remote_service(self):

        # Here's a patron.
        patron = self._patron()

        # Get identifiers to use when identifying that patron on two
        # different remote services.
        axis = DataSource.AXIS_360
        axis_identifier = patron.identifier_to_remote_service(axis)

        rb_digital = DataSource.lookup(self._db, DataSource.RB_DIGITAL)
        rb_identifier = patron.identifier_to_remote_service(rb_digital)

        # The identifiers are different.
        assert axis_identifier != rb_identifier

        # But they're both 36-character UUIDs.
        eq_(36, len(axis_identifier))
        eq_(36, len(rb_identifier))

        # They're persistent.
        eq_(rb_identifier, patron.identifier_to_remote_service(rb_digital))
        eq_(axis_identifier, patron.identifier_to_remote_service(axis))

        # You can customize the function used to generate the
        # identifier, in case the data source won't accept a UUID as a
        # patron identifier.
        def fake_generator():
            return "fake string"
        bib = DataSource.BIBLIOTHECA
        eq_("fake string", 
            patron.identifier_to_remote_service(bib, fake_generator)
        )

        # Once the identifier is created, specifying a different generator
        # does nothing.
        eq_("fake string", 
            patron.identifier_to_remote_service(bib)
        )
        eq_(
            axis_identifier, 
            patron.identifier_to_remote_service(axis, fake_generator)
        )

    def test_set_synchronize_annotations(self):
        # Two patrons.
        p1 = self._patron()
        p2 = self._patron()
        
        identifier = self._identifier()
        
        for patron in [p1, p2]:
            # Each patron decides they want to synchronize annotations
            # to a library server.
            eq_(None, patron.synchronize_annotations)
            patron.synchronize_annotations = True

            # Each patron gets one annotation.
            annotation, ignore = Annotation.get_one_or_create(
                self._db,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )
            annotation.content="The content for %s" % patron.id,

            eq_(1, len(patron.annotations))
            
        # Patron #1 decides they don't want their annotations stored
        # on a library server after all. This deletes their
        # annotation.
        p1.synchronize_annotations = False
        self._db.commit()
        eq_(0, len(p1.annotations))

        # Patron #1 can no longer use Annotation.get_one_or_create.
        assert_raises(
            ValueError, Annotation.get_one_or_create,
            self._db, patron=p1, identifier=identifier,
            motivation=Annotation.IDLING,
        )
        
        # Patron #2's annotation is unaffected.
        eq_(1, len(p2.annotations))

        # But patron #2 can use Annotation.get_one_or_create.
        i2, is_new = Annotation.get_one_or_create(
            self._db, patron=p2, identifier=self._identifier(),
            motivation=Annotation.IDLING,
        )
        eq_(True, is_new)

        # Once you make a decision, you can change your mind, but you
        # can't go back to not having made the decision.
        def try_to_set_none(patron):
            patron.synchronize_annotations = None
        assert_raises(ValueError, try_to_set_none, p2)


class TestPatronProfileStorage(DatabaseTest):

    def setup(self):
        super(TestPatronProfileStorage, self).setup()
        self.patron = self._patron()
        self.store = PatronProfileStorage(self.patron)
        
    def test_writable_setting_names(self):
        """Only one setting is currently writable."""
        eq_(set([self.store.SYNCHRONIZE_ANNOTATIONS]),
            self.store.writable_setting_names)

    def test_profile_document(self):
        # synchronize_annotations always shows up as settable, even if
        # the current value is None.
        eq_(None, self.patron.synchronize_annotations)
        rep = self.store.profile_document
        eq_({'settings': {'simplified:synchronize_annotations': None}},
            rep)

        self.patron.synchronize_annotations = True
        self.patron.authorization_expires = datetime.datetime(
            2016, 1, 1, 10, 20, 30
        )
        rep = self.store.profile_document
        eq_({'simplified:authorization_expires': '2016-01-01T10:20:30Z',
             'settings': {'simplified:synchronize_annotations': True}},
            rep
        )

    def test_update(self):
        # This is a no-op.
        self.store.update({}, {})
        eq_(None, self.patron.synchronize_annotations)

        # This is not.
        self.store.update({self.store.SYNCHRONIZE_ANNOTATIONS : True}, {})
        eq_(True, self.patron.synchronize_annotations)

        
class TestBaseCoverageRecord(DatabaseTest):

    def test_not_covered(self):
        source = DataSource.lookup(self._db, DataSource.OCLC)

        # Here are four identifiers with four relationships to a
        # certain coverage provider: no coverage at all, successful
        # coverage, a transient failure and a permanent failure.

        no_coverage = self._identifier()

        success = self._identifier()
        success_record = self._coverage_record(success, source)
        success_record.timestamp = (
            datetime.datetime.now() - datetime.timedelta(seconds=3600)
        )
        eq_(CoverageRecord.SUCCESS, success_record.status)

        transient = self._identifier()
        transient_record = self._coverage_record(
            transient, source, status=CoverageRecord.TRANSIENT_FAILURE
        )
        eq_(CoverageRecord.TRANSIENT_FAILURE, transient_record.status)

        persistent = self._identifier()
        persistent_record = self._coverage_record(
            persistent, source, status = BaseCoverageRecord.PERSISTENT_FAILURE
        )
        eq_(CoverageRecord.PERSISTENT_FAILURE, persistent_record.status)
        
        # Here's a query that finds all four.
        qu = self._db.query(Identifier).outerjoin(CoverageRecord)
        eq_(4, qu.count())

        def check_not_covered(expect, **kwargs):
            missing = CoverageRecord.not_covered(**kwargs)
            eq_(sorted(expect), sorted(qu.filter(missing).all()))

        # By default, not_covered() only finds the identifier with no
        # coverage and the one with a transient failure.
        check_not_covered([no_coverage, transient])

        # If we pass in different values for covered_status, we change what
        # counts as 'coverage'. In this case, we allow transient failures
        # to count as 'coverage'.
        check_not_covered(
            [no_coverage],
            count_as_covered=[CoverageRecord.PERSISTENT_FAILURE, 
                              CoverageRecord.TRANSIENT_FAILURE,
                              CoverageRecord.SUCCESS]
        )

        # Here, only success counts as 'coverage'.
        check_not_covered(
            [no_coverage, transient, persistent],
            count_as_covered=CoverageRecord.SUCCESS
        )

        # We can also say that coverage doesn't count if it was achieved before
        # a certain time. Here, we'll show that passing in the timestamp
        # of the 'success' record means that record still counts as covered.
        check_not_covered(
            [no_coverage, transient],
            count_as_not_covered_if_covered_before=success_record.timestamp
        )

        # But if we pass in a time one second later, the 'success'
        # record no longer counts as covered.
        one_second_after = (
            success_record.timestamp + datetime.timedelta(seconds=1)
        )
        check_not_covered(
            [success, no_coverage, transient],
            count_as_not_covered_if_covered_before=one_second_after
        )        


class TestCoverageRecord(DatabaseTest):

    def test_lookup(self):
        source = DataSource.lookup(self._db, DataSource.OCLC)
        edition = self._edition()
        operation = 'foo'
        collection = self._default_collection
        record = self._coverage_record(edition, source, operation, 
                                       collection=collection)


        # To find the CoverageRecord, edition, source, operation,
        # and collection must all match.
        result = CoverageRecord.lookup(edition, source, operation, 
                                       collection=collection)
        eq_(record, result)

        # You can substitute the Edition's primary identifier for the
        # Edition iteslf.
        lookup = CoverageRecord.lookup(
            edition.primary_identifier, source, operation, 
            collection=self._default_collection
        )
        eq_(lookup, record)


        # Omit the collection, and you find nothing.
        result = CoverageRecord.lookup(edition, source, operation)
        eq_(None, result)

        # Same for operation.
        result = CoverageRecord.lookup(edition, source, collection=collection)
        eq_(None, result)

        result = CoverageRecord.lookup(edition, source, "other operation",
                                       collection=collection)
        eq_(None, result)

        # Same for data source.
        other_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        result = CoverageRecord.lookup(edition, other_source, operation,
                                       collection=collection)
        eq_(None, result)

    def test_add_for(self):
        source = DataSource.lookup(self._db, DataSource.OCLC)
        edition = self._edition()
        operation = 'foo'
        record, is_new = CoverageRecord.add_for(edition, source, operation)
        eq_(True, is_new)

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        record2, is_new = CoverageRecord.add_for(
            edition, source, operation, a_week_ago
        )
        eq_(record, record2)
        eq_(False, is_new)
        eq_(a_week_ago, record2.timestamp)

        # If we don't specify an operation we get a totally different
        # record.
        record3, ignore = CoverageRecord.add_for(edition, source)
        assert record3 != record
        eq_(None, record3.operation)
        seconds = (datetime.datetime.utcnow() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = CoverageRecord.lookup(edition.primary_identifier, source)
        eq_(record3, record4)

        # We can change the status.
        record5, is_new = CoverageRecord.add_for(
            edition, source, operation, 
            status=CoverageRecord.PERSISTENT_FAILURE
        )
        eq_(record5, record)
        eq_(CoverageRecord.PERSISTENT_FAILURE, record.status)

    def test_bulk_add(self):
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        operation = u'testing'

        # An untouched identifier.
        i1 = self._identifier()

        # An identifier that already has failing coverage.
        covered = self._identifier()
        existing = self._coverage_record(
            covered, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
            exception=u'Uh oh'
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation
        )

        # A new coverage record is created for the uncovered identifier.
        eq_(i1.coverage_records, resulting_records)
        [new_record] = resulting_records
        eq_(source, new_record.data_source)
        eq_(operation, new_record.operation)
        eq_(CoverageRecord.SUCCESS, new_record.status)
        eq_(None, new_record.exception)

        # The existing coverage record is untouched.
        eq_([covered], ignored_identifiers)
        eq_([existing], covered.coverage_records)
        eq_(CoverageRecord.TRANSIENT_FAILURE, existing.status)
        eq_(original_timestamp, existing.timestamp)
        eq_('Uh oh', existing.exception)

        # Newly untouched identifier.
        i2 = self._identifier()

        # Force bulk add.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i2, covered], source, operation=operation, force=True
        )

        # The new identifier has the expected coverage.
        [new_record] = i2.coverage_records
        assert new_record in resulting_records

        # The existing record has been updated.
        assert existing in resulting_records
        assert covered not in ignored_identifiers
        eq_(CoverageRecord.SUCCESS, existing.status)
        assert existing.timestamp > original_timestamp
        eq_(None, existing.exception)

        # If no records are created or updated, no records are returned.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i2, covered], source, operation=operation
        )

        eq_([], resulting_records)
        eq_(sorted([i2, covered]), sorted(ignored_identifiers))

    def test_bulk_add_with_collection(self):
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        operation = u'testing'

        c1 = self._collection()
        c2 = self._collection()

        # An untouched identifier.
        i1 = self._identifier()

        # An identifier with coverage for a different collection.
        covered = self._identifier()
        existing = self._coverage_record(
            covered, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE, collection=c1,
            exception=u'Danger, Will Robinson'
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation, collection=c1,
            force=True
        )

        eq_(2, len(resulting_records))
        eq_([], ignored_identifiers)

        # A new record is created for the new identifier.
        [new_record] = i1.coverage_records
        assert new_record in resulting_records
        eq_(source, new_record.data_source)
        eq_(operation, new_record.operation)
        eq_(CoverageRecord.SUCCESS, new_record.status)
        eq_(c1, new_record.collection)

        # The existing record has been updated.
        assert existing in resulting_records
        eq_(CoverageRecord.SUCCESS, existing.status)
        assert existing.timestamp > original_timestamp
        eq_(None, existing.exception)

        # Bulk add for a different collection.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [covered], source, operation=operation, collection=c2,
            status=CoverageRecord.TRANSIENT_FAILURE, exception=u'Oh no',
        )

        # A new record has been added to the identifier.
        assert existing not in resulting_records
        [new_record] = resulting_records
        eq_(covered, new_record.identifier)
        eq_(CoverageRecord.TRANSIENT_FAILURE, new_record.status)
        eq_(source, new_record.data_source)
        eq_(operation, new_record.operation)
        eq_(u'Oh no', new_record.exception)


class TestWorkCoverageRecord(DatabaseTest):

    def test_lookup(self):
        work = self._work()
        operation = 'foo'

        lookup = WorkCoverageRecord.lookup(work, operation)
        eq_(None, lookup)

        record = self._work_coverage_record(work, operation)

        lookup = WorkCoverageRecord.lookup(work, operation)
        eq_(lookup, record)

        eq_(None, WorkCoverageRecord.lookup(work, "another operation"))

    def test_add_for(self):
        work = self._work()
        operation = 'foo'
        record, is_new = WorkCoverageRecord.add_for(work, operation)
        eq_(True, is_new)

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        record2, is_new = WorkCoverageRecord.add_for(
            work, operation, a_week_ago
        )
        eq_(record, record2)
        eq_(False, is_new)
        eq_(a_week_ago, record2.timestamp)

        # If we don't specify an operation we get a totally different
        # record.
        record3, ignore = WorkCoverageRecord.add_for(work, None)
        assert record3 != record
        eq_(None, record3.operation)
        seconds = (datetime.datetime.utcnow() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = WorkCoverageRecord.lookup(work, None)
        eq_(record3, record4)

        # We can change the status.
        record5, is_new = WorkCoverageRecord.add_for(
            work, operation, status=WorkCoverageRecord.PERSISTENT_FAILURE
        )
        eq_(record5, record)
        eq_(WorkCoverageRecord.PERSISTENT_FAILURE, record.status)

    def test_bulk_add(self):

        operation = "relevant"
        irrelevant_operation = "irrelevant"

        # This Work will get a new WorkCoverageRecord for the relevant
        # operation, even though it already has a WorkCoverageRecord
        # for an irrelevant operation.
        not_already_covered = self._work()
        irrelevant_record, ignore = WorkCoverageRecord.add_for(
            not_already_covered, irrelevant_operation, 
            status=WorkCoverageRecord.SUCCESS
        )

        # This Work will have its existing, relevant CoverageRecord
        # updated.
        already_covered = self._work()
        previously_failed, ignore = WorkCoverageRecord.add_for(
            already_covered, operation, 
            status=WorkCoverageRecord.TRANSIENT_FAILURE,
        )
        previously_failed.exception="Some exception"

        # This work will not have a record created for it, because
        # we're not passing it in to the method.
        not_affected = self._work()
        WorkCoverageRecord.add_for(
            not_affected, irrelevant_operation, 
            status=WorkCoverageRecord.SUCCESS
        )

        # This work will not have its existing record updated, because
        # we're not passing it in to the method.
        not_affected_2 = self._work()
        not_modified, ignore = WorkCoverageRecord.add_for(
            not_affected_2, operation, status=WorkCoverageRecord.SUCCESS
        )

        # Tell bulk_add to update or create WorkCoverageRecords for
        # not_already_covered and already_covered, but not not_affected.
        new_timestamp = datetime.datetime.utcnow()
        new_status = WorkCoverageRecord.REGISTERED
        WorkCoverageRecord.bulk_add(
            [not_already_covered, already_covered],
            operation, new_timestamp, status=new_status
        )
        self._db.commit()
        def relevant_records(work):
            return [x for x in work.coverage_records
                    if x.operation == operation]

        # No coverage records were added or modified for works not
        # passed in to the method.
        eq_([], relevant_records(not_affected))
        assert not_modified.timestamp < new_timestamp

        # The record associated with already_covered has been updated,
        # and its exception removed.
        [record] = relevant_records(already_covered)
        eq_(new_timestamp, record.timestamp)
        eq_(new_status, record.status)
        eq_(None, previously_failed.exception)

        # A new record has been associated with not_already_covered
        [record] = relevant_records(not_already_covered)
        eq_(new_timestamp, record.timestamp)
        eq_(new_status, record.status)

        # The irrelevant WorkCoverageRecord is not affected by the update,
        # even though its Work was affected, because it's a record for
        # a different operation.
        eq_(WorkCoverageRecord.SUCCESS, irrelevant_record.status)
        assert irrelevant_record.timestamp < new_timestamp


class TestComplaint(DatabaseTest):

    def setup(self):
        super(TestComplaint, self).setup()
        self.edition, self.pool = self._edition(with_license_pool=True)
        self.type = "http://librarysimplified.org/terms/problem/wrong-genre"

    def test_for_license_pool(self):
        work_complaint, is_new = Complaint.register(
            self.pool, self.type, "yes", "okay"
        )

        lp_type = self.type.replace('wrong-genre', 'cannot-render')
        lp_complaint, is_new = Complaint.register(
            self.pool, lp_type, "yes", "okay")

        eq_(False, work_complaint.for_license_pool)
        eq_(True, lp_complaint.for_license_pool)

    def test_success(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)
        assert abs(datetime.datetime.utcnow() -complaint.timestamp).seconds < 3

        # A second complaint from the same source is folded into the
        # original complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        eq_(False, is_new)
        eq_(complaint.id, complaint2.id)
        eq_("baz", complaint.detail)

        eq_(1, len(self.pool.complaints))

    def test_success_no_source(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_(None, complaint.source)

        # A second identical complaint from no source is treated as a
        # separate complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        eq_(True, is_new)
        eq_(None, complaint.source)
        assert complaint2.id != complaint.id

        eq_(2, len(self.pool.complaints))

    def test_failure_no_licensepool(self):
        assert_raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )

    def test_unrecognized_type(self):
        type = "http://librarysimplified.org/terms/problem/no-such-error"
        assert_raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )
        
    def test_register_resolved(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar", resolved=datetime.datetime.utcnow()
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)
        assert abs(datetime.datetime.utcnow() -complaint.timestamp).seconds < 3
        assert abs(datetime.datetime.utcnow() -complaint.resolved).seconds < 3

        # A second complaint from the same source is not folded into the same complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        eq_(True, is_new)
        assert complaint2.id != complaint.id
        eq_("baz", complaint2.detail)
        eq_(2, len(self.pool.complaints))

    def test_resolve(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        complaint.resolve()
        assert complaint.resolved != None
        assert abs(datetime.datetime.utcnow() - complaint.resolved).seconds < 3


class TestCustomList(DatabaseTest):

    def test_find(self):
        source = DataSource.lookup(self._db, DataSource.NYT)
        # When there's no CustomList to find, nothing is returned.
        result = CustomList.find(self._db, 'my-list', source)
        eq_(None, result)

        custom_list = self._customlist(
            foreign_identifier='a-list', name='My List', num_entries=0
        )[0]
        # A CustomList can be found by its foreign_identifier.
        result = CustomList.find(self._db, 'a-list', source)
        eq_(custom_list, result)

        # Or its name.
        result = CustomList.find(self._db, 'My List', source.name)
        eq_(custom_list, result)

        # The list can also be found by name without a data source.
        result = CustomList.find(self._db, 'My List')
        eq_(custom_list, result)

        # By default, we only find lists with no associated Library.
        # If we look for a list from a library, there isn't one.
        result = CustomList.find(self._db, 'My List', source, library=self._default_library)
        eq_(None, result)

        # If we add the Library to the list, it's returned.
        custom_list.library = self._default_library
        result = CustomList.find(self._db, 'My List', source, library=self._default_library)
        eq_(custom_list, result)

    def assert_reindexing_scheduled(self, work):
        """Assert that the given work has exactly one WorkCoverageRecord, which
        indicates that it needs to have its search index updated.
        """
        [needs_reindex] = work.coverage_records
        eq_(WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION,
            needs_reindex.operation)
        eq_(WorkCoverageRecord.REGISTERED, needs_reindex.status)

    def test_add_entry(self):
        custom_list = self._customlist(num_entries=0)[0]
        now = datetime.datetime.utcnow()

        # An edition without a work can create an entry.
        workless_edition = self._edition()
        workless_entry, is_new = custom_list.add_entry(workless_edition)
        eq_(True, is_new)
        eq_(True, isinstance(workless_entry, CustomListEntry))
        eq_(workless_edition, workless_entry.edition)
        eq_(True, workless_entry.first_appearance > now)
        eq_(None, workless_entry.work)
        # And the CustomList will be seen as updated.
        eq_(True, custom_list.updated > now)

        # An edition with a work can create an entry.
        work = self._work()
        work.coverage_records = []
        worked_entry, is_new = custom_list.add_entry(work.presentation_edition)
        eq_(True, is_new)
        eq_(work, worked_entry.work)
        eq_(work.presentation_edition, worked_entry.edition)
        eq_(True, worked_entry.first_appearance > now)

        # When this happens, the work is scheduled for reindexing.
        self.assert_reindexing_scheduled(work)

        # A work can create an entry.
        work = self._work(with_open_access_download=True)
        work.coverage_records = []
        work_entry, is_new = custom_list.add_entry(work)
        eq_(True, is_new)
        eq_(work.presentation_edition, work_entry.edition)
        eq_(work, work_entry.work)
        eq_(True, work_entry.first_appearance > now)

        # When this happens, the work is scheduled for reindexing.
        self.assert_reindexing_scheduled(work)

        # Annotations can be passed to the entry.
        annotated_edition = self._edition()
        annotated_entry = custom_list.add_entry(
            annotated_edition, annotation="Sure, this is a good book."
        )[0]
        eq_(u"Sure, this is a good book.", annotated_entry.annotation)

        # A first_appearance time can be passed to an entry.
        timed_edition = self._edition()
        timed_entry = custom_list.add_entry(timed_edition, first_appearance=now)[0]
        eq_(now, timed_entry.first_appearance)
        eq_(now, timed_entry.most_recent_appearance)

        # If the entry already exists, the most_recent_appearance is updated.
        previous_list_update_time = custom_list.updated
        new_timed_entry, is_new = custom_list.add_entry(timed_edition)
        eq_(False, is_new)
        eq_(timed_entry, new_timed_entry)
        eq_(True, timed_entry.most_recent_appearance > now)
        # But the CustomList update time is not.
        eq_(previous_list_update_time, custom_list.updated)

        # If the entry already exists, the most_recent_appearance can be
        # updated by passing in a later first_appearance.
        later = datetime.datetime.utcnow()
        new_timed_entry = custom_list.add_entry(timed_edition, first_appearance=later)[0]
        eq_(timed_entry, new_timed_entry)
        eq_(now, new_timed_entry.first_appearance)
        eq_(later, new_timed_entry.most_recent_appearance)

        # For existing entries, earlier first_appearance datetimes are ignored.
        entry = custom_list.add_entry(annotated_edition, first_appearance=now)[0]
        eq_(True, entry.first_appearance != now)
        eq_(True, entry.first_appearance >= now)
        eq_(True, entry.most_recent_appearance != now)
        eq_(True, entry.most_recent_appearance >= now)

        # Adding an equivalent edition will not create multiple entries.
        equivalent, lp = self._edition(with_open_access_download=True)
        workless_edition.primary_identifier.equivalent_to(
            equivalent.data_source, equivalent.primary_identifier, 1
        )
        equivalent_entry, is_new = custom_list.add_entry(equivalent)
        eq_(False, is_new)
        eq_(workless_entry, equivalent_entry)
        # Or update the CustomList updated time
        eq_(previous_list_update_time, custom_list.updated)
        # But it will change the edition to the one that's requested.
        eq_(equivalent, workless_entry.edition)
        # And/or add a .work if one is newly available.
        eq_(lp.work, equivalent_entry.work)

        # Adding a non-equivalent edition for the same work will not create multiple entries.
        not_equivalent, lp = self._edition(with_open_access_download=True)
        not_equivalent.work = work
        not_equivalent_entry, is_new = custom_list.add_entry(not_equivalent)
        eq_(False, is_new)

    def test_remove_entry(self):
        custom_list, editions = self._customlist(num_entries=3)
        [first, second, third] = editions
        now = datetime.datetime.utcnow()

        # An entry is removed if its edition is passed in.
        first.work.coverage_records = []
        custom_list.remove_entry(first)
        eq_(2, len(custom_list.entries))
        eq_(set([second, third]), set([entry.edition for entry in custom_list.entries]))
        # And CustomList.updated is changed.
        eq_(True, custom_list.updated > now)

        # The editon's work has been scheduled for reindexing.
        self.assert_reindexing_scheduled(first.work)
        first.work.coverage_records = []

        # An entry is also removed if any of its equivalent editions
        # are passed in.
        previous_list_update_time = custom_list.updated
        equivalent = self._edition(with_open_access_download=True)[0]
        second.primary_identifier.equivalent_to(
            equivalent.data_source, equivalent.primary_identifier, 1
        )
        custom_list.remove_entry(second)
        eq_(1, len(custom_list.entries))
        eq_(third, custom_list.entries[0].edition)
        eq_(True, custom_list.updated > previous_list_update_time)

        # An entry is also removed if its work is passed in.
        previous_list_update_time = custom_list.updated
        custom_list.remove_entry(third.work)
        eq_([], custom_list.entries)
        eq_(True, custom_list.updated > previous_list_update_time)

        # An edition that's not on the list doesn't cause any problems.
        custom_list.add_entry(second)
        previous_list_update_time = custom_list.updated
        custom_list.remove_entry(first)
        eq_(1, len(custom_list.entries))
        eq_(previous_list_update_time, custom_list.updated)

        # The 'removed' edition's work does not need to be reindexed
        # because it wasn't on the list to begin with.
        eq_([], first.work.coverage_records)

    def test_entries_for_work(self):
        custom_list, editions = self._customlist(num_entries=2)
        edition = editions[0]
        [entry] = [e for e in custom_list.entries if e.edition==edition]

        # The entry is returned when you search by Edition.
        eq_([entry], list(custom_list.entries_for_work(edition)))

        # It's also returned when you search by Work.
        eq_([entry], list(custom_list.entries_for_work(edition.work)))

        # Or when you search with an equivalent Edition
        equivalent = self._edition()
        edition.primary_identifier.equivalent_to(
            equivalent.data_source, equivalent.primary_identifier, 1
        )
        eq_([entry], list(custom_list.entries_for_work(equivalent)))

        # Multiple equivalent entries may be returned, too, if they
        # were added manually or before the editions were set as
        # equivalent.
        not_yet_equivalent = self._edition()
        other_entry = custom_list.add_entry(not_yet_equivalent)[0]
        edition.primary_identifier.equivalent_to(
            not_yet_equivalent.data_source,
            not_yet_equivalent.primary_identifier, 1
        )
        eq_(
            sorted([entry, other_entry]),
            sorted(list(custom_list.entries_for_work(not_yet_equivalent)))
        )


class TestCustomListEntry(DatabaseTest):

    def test_set_work(self):

        # Start with a custom list with no entries
        list, ignore = self._customlist(num_entries=0)

        # Now create an entry with an edition but no license pool.
        edition = self._edition()

        entry, ignore = get_one_or_create(
            self._db, CustomListEntry,
            list_id=list.id, edition_id=edition.id,
        )

        eq_(edition, entry.edition)
        eq_(None, entry.work)

        # Here's another edition, with a license pool.
        other_edition, lp = self._edition(with_open_access_download=True)
       
        # And its identifier is equivalent to the entry's edition's identifier.
        data_source = DataSource.lookup(self._db, DataSource.OCLC)
        lp.identifier.equivalent_to(data_source, edition.primary_identifier, 1)

        # If we call set_work, it does nothing, because there is no work
        # associated with either edition.
        entry.set_work()

        # But if we assign a Work with the LicensePool, and try again...
        work, ignore = lp.calculate_work()
        entry.set_work()
        eq_(work, other_edition.work)
        
        # set_work() traces the line from the CustomListEntry to its
        # Edition to the equivalent Edition to its Work, and associates
        # that Work with the CustomListEntry.
        eq_(work, entry.work)

        # Even though the CustomListEntry's edition is not directly
        # associated with the Work.
        eq_(None, edition.work)

    def test_update(self):
        custom_list, [edition] = self._customlist(entries_exist_as_works=False)
        identifier = edition.primary_identifier
        [entry] = custom_list.entries
        entry_attributes = vars(entry).values()
        created = entry.first_appearance

        # Running update without entries or forcing doesn't change the entry.
        entry.update(self._db)
        eq_(entry_attributes, vars(entry).values())

        # Trying to update an entry with entries from a different
        # CustomList is a no-go.
        other_custom_list = self._customlist()[0]
        [external_entry] = other_custom_list.entries
        assert_raises(
            ValueError, entry.update, self._db,
            equivalent_entries=[external_entry]
        )

        # So is attempting to update an entry with other entries that
        # don't represent the same work.
        external_work = self._work(with_license_pool=True)
        external_work_edition = external_work.presentation_edition
        external_work_entry = custom_list.add_entry(external_work_edition)[0]
        assert_raises(
            ValueError, entry.update, self._db,
            equivalent_entries=[external_work_entry]
        )

        # Okay, but with an actual equivalent entry...
        work = self._work(with_open_access_download=True)
        equivalent = work.presentation_edition
        equivalent_entry = custom_list.add_entry(
            equivalent, annotation="Whoo, go books!"
        )[0]
        identifier.equivalent_to(
            equivalent.data_source, equivalent.primary_identifier, 1
        )

        # ...updating changes the original entry as expected.
        entry.update(self._db, equivalent_entries=[equivalent_entry])
        # The first_appearance hasn't changed because the entry was created first.
        eq_(created, entry.first_appearance)
        # But the most recent appearance is of the entry created last.
        eq_(equivalent_entry.most_recent_appearance, entry.most_recent_appearance)
        # Annotations are shared.
        eq_(u"Whoo, go books!", entry.annotation)
        # The Edition and LicensePool are updated to have a Work.
        eq_(entry.edition, work.presentation_edition)
        eq_(entry.work, equivalent.work)
        # The equivalent entry has been deleted.
        eq_([], self._db.query(CustomListEntry).\
                filter(CustomListEntry.id==equivalent_entry.id).all())

        # The entry with the longest annotation wins the annotation awards.
        long_annotation = "Wow books are so great especially when they're annotated."
        longwinded = self._edition()
        longwinded_entry = custom_list.add_entry(
            longwinded, annotation=long_annotation)[0]

        identifier.equivalent_to(
            longwinded.data_source, longwinded.primary_identifier, 1)
        entry.update(self._db, equivalent_entries=[longwinded_entry])
        eq_(long_annotation, entry.annotation)
        eq_(longwinded_entry.most_recent_appearance, entry.most_recent_appearance)


class TestCachedFeed(DatabaseTest):

    def test_fetch_page_feeds(self):
        """CachedFeed.fetch retrieves paginated feeds from the database if
        they exist, and prepares them for creation if not.
        """
        m = CachedFeed.fetch
        lane = self._lane()
        page = CachedFeed.PAGE_TYPE
        annotator = object()

        # A page feed for a lane with no facets or pagination.
        feed, fresh = m(self._db, lane, page, None, None, annotator)
        eq_(page, feed.type)

        # The feed is not usable as-is because there's no content.
        eq_(False, fresh)

        # If we set content, we can fetch the same feed and then it
        # becomes usable.
        feed.content = "some content"
        feed.timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(seconds=5)
        )
        feed2, fresh = m(self._db, lane, page, None, None, annotator)
        eq_(feed, feed2)
        eq_(True, fresh)

        # But a feed is not considered fresh if it's older than `max_age`
        # seconds.
        feed, fresh = m(
            self._db, lane, page, None, None, annotator, max_age=0
        )
        eq_(False, fresh)

        # This feed has no unique key because its lane ID and type
        # are enough to uniquely identify it.
        eq_(None, feed.unique_key)
        eq_("", feed.pagination)
        eq_("", feed.facets)

        # Now let's introduce some pagination and facet information.
        facets = Facets.default(self._default_library)
        pagination = Pagination.default()
        feed2, fresh = m(
            self._db, lane, page, facets, pagination, annotator
        )
        assert feed2 != feed
        eq_(pagination.query_string, feed2.pagination)
        eq_(facets.query_string, feed2.facets)

        # There's still no need for a unique key because pagination
        # and facets are taken into account when trying to uniquely
        # identify a feed.
        eq_(None, feed.unique_key)

        # However, a lane based on a WorkList has no lane ID, so a
        # unique key is necessary.
        worklist = WorkList()
        worklist.initialize(
            library=self._default_library, display_name="aworklist",
            languages=["eng", "spa"], audiences=[Classifier.AUDIENCE_CHILDREN]
        )
        feed, fresh = m(
            self._db, worklist, page, None, None, annotator
        )
        # The unique key incorporates the WorkList's display name,
        # its languages, and its audiences.
        eq_("aworklist-eng,spa-Children", feed.unique_key)

    def test_fetch_group_feeds(self):
        # Group feeds don't need to worry about facets or pagination,
        # but they have their own complications.

        m = CachedFeed.fetch
        lane = self._lane()
        groups = CachedFeed.GROUPS_TYPE
        annotator = object()

        # Ask for a groups feed for a lane.
        feed, usable = m(self._db, lane, groups, None, None, annotator)

        # The feed is not usable because there's no content.
        eq_(False, usable)

        # Group-type feeds are too expensive to generate, so when
        # asked to produce one we prepared a page-type feed instead.
        eq_(CachedFeed.PAGE_TYPE, feed.type)
        eq_(lane, feed.lane)
        eq_(None, feed.unique_key)
        eq_("", feed.facets)
        eq_("", feed.pagination)

        # But what if a group feed had been created ahead of time
        # through some other mechanism?
        feed.content = "some content"
        feed.type = groups
        feed.timestamp = datetime.datetime.utcnow()

        # Now fetch() finds the feed, but because there was content
        # and a recent timestamp, it's now usable and there's no need
        # to change the type.
        feed2, usable = m(self._db, lane, groups, None, None, annotator)
        eq_(feed, feed2)
        eq_(True, usable)
        eq_(groups, feed.type)
        eq_("some content", feed.content)

        # If we pass in force_refresh then the feed is always treated as
        # stale.
        feed, usable = m(self._db, lane, groups, None, None, annotator,
                         force_refresh=True)
        eq_(False, usable)


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


class TestExternalIntegration(DatabaseTest):

    def setup(self):
        super(TestExternalIntegration, self).setup()
        self.external_integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )

    def test_for_library_and_goal(self):
        goal = self.external_integration.goal
        qu = ExternalIntegration.for_library_and_goal(
            self._db, self._default_library, goal
        )

        # This matches nothing because the ExternalIntegration is not
        # associated with the Library.
        eq_([], qu.all())
        get_one = ExternalIntegration.one_for_library_and_goal
        eq_(None, get_one(self._db, self._default_library, goal))

        # Associate the library with the ExternalIntegration and
        # the query starts matching it. one_for_library_and_goal
        # also starts returning it.
        self.external_integration.libraries.append(self._default_library)
        eq_([self.external_integration], qu.all())
        eq_(self.external_integration,
            get_one(self._db, self._default_library, goal))

        # Create another, similar ExternalIntegration. By itself, this
        # has no effect.
        integration2, ignore = create(
            self._db, ExternalIntegration, goal=goal, protocol=self._str
        )
        eq_([self.external_integration], qu.all())
        eq_(self.external_integration,
            get_one(self._db, self._default_library, goal))

        # Associate that ExternalIntegration with the library, and
        # the query starts picking it up, and one_for_library_and_goal
        # starts raising an exception.
        integration2.libraries.append(self._default_library)
        eq_(set([self.external_integration, integration2]), set(qu.all()))
        assert_raises_regexp(
            CannotLoadConfiguration,
            "Library .* defines multiple integrations with goal .*",
            get_one, self._db, self._default_library, goal
        )

    def test_data_source(self):
        # For most collections, the protocol determines the
        # data source.
        collection = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        eq_(DataSource.OVERDRIVE, collection.data_source.name)

        # For OPDS Import collections, data source is a setting which
        # might not be present.
        eq_(None, self._default_collection.data_source)

        # data source will be automatically created if necessary.
        self._default_collection.external_integration.setting(
            Collection.DATA_SOURCE_NAME_SETTING
        ).value = "New Data Source"
        eq_("New Data Source", self._default_collection.data_source.name)
        
    def test_set_key_value_pair(self):
        """Test the ability to associate extra key-value pairs with
        an ExternalIntegration.
        """
        eq_([], self.external_integration.settings)

        setting = self.external_integration.set_setting("website_id", "id1")
        eq_("website_id", setting.key)
        eq_("id1", setting.value)

        # Calling set() again updates the key-value pair.
        eq_([setting.id], [x.id for x in self.external_integration.settings])
        setting2 = self.external_integration.set_setting("website_id", "id2")
        eq_(setting.id, setting2.id)
        eq_("id2", setting2.value)

        eq_(setting2, self.external_integration.setting("website_id"))

    def test_explain(self):
        integration = self._external_integration(
            "protocol", "goal"
        )
        integration.name = "The Integration"
        integration.url = "http://url/"
        integration.username = "someuser"
        integration.password = "somepass"
        integration.setting("somesetting").value = "somevalue"

        # Two different libraries have slightly different
        # configurations for this integration.
        self._default_library.name = "First Library"
        self._default_library.integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-specific", self._default_library, integration
        ).value = "value1"
        
        library2 = self._library()
        library2.name = "Second Library"
        library2.integrations.append(integration)
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "library-specific", library2, integration
        ).value = "value2"

        # If we decline to pass in a library, we get information about how
        # each library in the system configures this integration.

        expect = """ID: %s
Name: The Integration
Protocol/Goal: protocol/goal
library-specific='value1' (applies only to First Library)
library-specific='value2' (applies only to Second Library)
somesetting='somevalue'
url='http://url/'
username='someuser'""" % integration.id
        actual = integration.explain()
        eq_(expect, "\n".join(actual))

        # If we pass in a library, we only get information about
        # how that specific library configures the integration.
        for_library_2 = "\n".join(integration.explain(library=library2))
        assert "applies only to First Library" not in for_library_2
        assert "applies only to Second Library" in for_library_2
        
        # If we pass in True for include_secrets, we see the passwords.
        with_secrets = integration.explain(include_secrets=True)
        assert "password='somepass'" in with_secrets
        

class TestConfigurationSetting(DatabaseTest):

    def test_is_secret(self):
        """Some configuration settings are considered secrets, 
        and some are not.
        """
        m = ConfigurationSetting._is_secret
        eq_(True, m('secret'))
        eq_(True, m('password'))
        eq_(True, m('its_a_secret_to_everybody'))
        eq_(True, m('the_password'))
        eq_(True, m('password_for_the_account'))
        eq_(False, m('public_information'))

        eq_(True,
            ConfigurationSetting.sitewide(self._db, "secret_key").is_secret)
        eq_(False,
            ConfigurationSetting.sitewide(self._db, "public_key").is_secret)

    def test_value_or_default(self):
        integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )
        setting = integration.setting("key")
        eq_(None, setting.value)
        
        # If the setting has no value, value_or_default sets the value to
        # the default, and returns the default.
        eq_("default value", setting.value_or_default("default value"))
        eq_("default value", setting.value)

        # Once the value is set, value_or_default returns the value.
        eq_("default value", setting.value_or_default("new default"))
        
        # If the setting has any value at all, even the empty string,
        # it's returned instead of the default.
        setting.value = ""
        eq_("", setting.value_or_default("default"))

    def test_value_inheritance(self):

        key = "SomeKey"

        # Here's a sitewide configuration setting.
        sitewide_conf = ConfigurationSetting.sitewide(self._db, key)

        # Its value is not set.
        eq_(None, sitewide_conf.value)

        # Set it.
        sitewide_conf.value = "Sitewide value"
        eq_("Sitewide value", sitewide_conf.value)
        
        # Here's an integration, let's say the SIP2 authentication mechanism
        sip, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.PATRON_AUTH_GOAL, protocol="SIP2"
        )

        # It happens to a ConfigurationSetting for the same key used
        # in the sitewide configuration.
        sip_conf = ConfigurationSetting.for_externalintegration(key, sip)

        # But because the meaning of a configuration key differ so
        # widely across integrations, the SIP2 integration does not
        # inherit the sitewide value for the key.
        eq_(None, sip_conf.value)
        sip_conf.value = "SIP2 value"
        
        # Here's a library which has a ConfigurationSetting for the same
        # key used in the sitewide configuration.
        library = self._default_library
        library_conf = ConfigurationSetting.for_library(key, library)
        
        # Since all libraries use a given ConfigurationSetting to mean
        # the same thing, a library _does_ inherit the sitewide value
        # for a configuration setting.
        eq_("Sitewide value", library_conf.value)

        # Change the site-wide configuration, and the default also changes.
        sitewide_conf.value = "New site-wide value"
        eq_("New site-wide value", library_conf.value)

        # The per-library value takes precedence over the site-wide
        # value.
        library_conf.value = "Per-library value"
        eq_("Per-library value", library_conf.value)
        
        # Now let's consider a setting like the patron identifier
        # prefix.  This is set on the combination of a library and a
        # SIP2 integration.
        key = "patron_identifier_prefix"
        library_patron_prefix_conf = ConfigurationSetting.for_library_and_externalintegration(
            self._db, key, library, sip
        )
        eq_(None, library_patron_prefix_conf.value)

        # If the SIP2 integration has a value set for this
        # ConfigurationSetting, that value is inherited for every
        # individual library that uses the integration.
        generic_patron_prefix_conf = ConfigurationSetting.for_externalintegration(
            key, sip
        )
        eq_(None, generic_patron_prefix_conf.value)
        generic_patron_prefix_conf.value = "Integration-specific value"
        eq_("Integration-specific value", library_patron_prefix_conf.value)

        # Change the value on the integration, and the default changes
        # for each individual library.
        generic_patron_prefix_conf.value = "New integration-specific value"
        eq_("New integration-specific value", library_patron_prefix_conf.value)

        # The library+integration setting takes precedence over the
        # integration setting.
        library_patron_prefix_conf.value = "Library-specific value"
        eq_("Library-specific value", library_patron_prefix_conf.value)
        
    def test_duplicate(self):
        """You can't have two ConfigurationSettings for the same key,
        library, and external integration.

        (test_relationships shows that you can have two settings for the same
        key as long as library or integration is different.)
        """
        key = self._str
        integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )
        library = self._default_library
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, key, library, integration
        )
        setting2 = ConfigurationSetting.for_library_and_externalintegration(
            self._db, key, library, integration
        )
        eq_(setting.id, setting2.id)
        assert_raises(
            IntegrityError,
            create, self._db, ConfigurationSetting,
            key=key,
            library=library, external_integration=integration
        )
    
    def test_relationships(self):
        integration, ignore = create(
            self._db, ExternalIntegration, goal=self._str, protocol=self._str
        )
        eq_([], integration.settings)
        
        library = self._default_library
        eq_([], library.settings)
        
        # Create four different ConfigurationSettings with the same key.
        cs = ConfigurationSetting
        key = self._str

        for_neither = cs.sitewide(self._db, key)
        eq_(None, for_neither.library)
        eq_(None, for_neither.external_integration)
        
        for_library = cs.for_library(key, library)
        eq_(library, for_library.library)
        eq_(None, for_library.external_integration)

        for_integration = cs.for_externalintegration(key, integration)
        eq_(None, for_integration.library)
        eq_(integration, for_integration.external_integration)

        for_both = cs.for_library_and_externalintegration(
            self._db, key, library, integration
        )
        eq_(library, for_both.library)
        eq_(integration, for_both.external_integration)
        
        # We got four distinct objects with the same key.
        objs = [for_neither, for_library, for_integration, for_both]
        eq_(4, len(set(objs)))
        for o in objs:
            eq_(o.key, key)

        eq_([for_library, for_both], library.settings)
        eq_([for_integration, for_both], integration.settings)
        eq_(library, for_both.library)
        eq_(integration, for_both.external_integration)
        
        # If we delete the integration, all configuration settings
        # associated with it are deleted, even the one that's also
        # associated with the library.
        self._db.delete(integration)
        self._db.commit()
        eq_([for_library.id], [x.id for x in library.settings])

    def test_int_value(self):
        number = ConfigurationSetting.sitewide(self._db, "number")
        eq_(None, number.int_value)
        
        number.value = "1234"
        eq_(1234, number.int_value)

        number.value = "tra la la"
        assert_raises(ValueError, lambda: number.int_value)

    def test_float_value(self):
        number = ConfigurationSetting.sitewide(self._db, "number")
        eq_(None, number.int_value)
        
        number.value = "1234.5"
        eq_(1234.5, number.float_value)

        number.value = "tra la la"
        assert_raises(ValueError, lambda: number.float_value)
        
    def test_json_value(self):
        jsondata = ConfigurationSetting.sitewide(self._db, "json")
        eq_(None, jsondata.int_value)

        jsondata.value = "[1,2]"
        eq_([1,2], jsondata.json_value)

        jsondata.value = "tra la la"
        assert_raises(ValueError, lambda: jsondata.json_value)
        
    def test_explain(self):
        """Test that ConfigurationSetting.explain gives information
        about all site-wide configuration settings.
        """
        ConfigurationSetting.sitewide(self._db, "a_secret").value = "1"
        ConfigurationSetting.sitewide(self._db, "nonsecret_setting").value = "2"

        integration = self._external_integration("a protocol", "a goal")
        
        actual = ConfigurationSetting.explain(self._db, include_secrets=True)
        expect = """Site-wide configuration settings:
---------------------------------
a_secret='1'
nonsecret_setting='2'"""
        eq_(expect, "\n".join(actual))
        
        without_secrets = "\n".join(ConfigurationSetting.explain(
            self._db, include_secrets=False
        ))
        assert 'a_secret' not in without_secrets
        assert 'nonsecret_setting' in without_secrets



        
class TestSiteConfigurationHasChanged(DatabaseTest):

    class MockSiteConfigurationHasChanged(object):
        """Keep track of whether site_configuration_has_changed was
        ever called.
        """
        def __init__(self):
            self.was_called = False

        def run(self, _db):
            self.was_called = True
            site_configuration_has_changed(_db)

        def assert_was_called(self):
            "Assert that `was_called` is True, then reset it for the next assertion."
            assert self.was_called
            self.was_called = False

        def assert_was_not_called(self):
            assert not self.was_called
            
    def setup(self):
        super(TestSiteConfigurationHasChanged, self).setup()

        # Mock model.site_configuration_has_changed
        self.old_site_configuration_has_changed = model.site_configuration_has_changed
        self.mock = self.MockSiteConfigurationHasChanged()
        for module in model, lane:
            module.site_configuration_has_changed = self.mock.run

    def teardown(self):
        super(TestSiteConfigurationHasChanged, self).teardown()
        for module in model, lane:
            module.site_configuration_has_changed = self.old_site_configuration_has_changed
        
    def test_site_configuration_has_changed(self):
        """Test the site_configuration_has_changed() function and its
        effects on the Configuration object.
        """
        # The database configuration timestamp is initialized as part
        # of the default data. In that case, it happened during the
        # package_setup() for this test run.
        last_update = Configuration.site_configuration_last_update(self._db)
        
        timestamp_value = Timestamp.value(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        eq_(timestamp_value, last_update)
        
        # Now let's call site_configuration_has_changed().
        time_of_update = datetime.datetime.utcnow()
        site_configuration_has_changed(self._db, timeout=0)
        
        # The Timestamp has changed in the database.
        new_timestamp_value = Timestamp.value(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        assert new_timestamp_value > timestamp_value
        
        # The locally-stored last update value has been updated.
        new_last_update_time = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert new_last_update_time > last_update
        assert (new_last_update_time - time_of_update).total_seconds() < 1
                
        # Let's be sneaky and update the timestamp directly,
        # without calling site_configuration_has_changed(). This
        # simulates another process on a different machine calling
        # site_configuration_has_changed() -- they will know about the
        # change but we won't be informed.
        timestamp = Timestamp.stamp(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )

        # Calling Configuration.check_for_site_configuration_update
        # doesn't detect the change because by default we only go to
        # the database every ten minutes.
        eq_(new_last_update_time,
            Configuration.site_configuration_last_update(self._db))

        # Passing in a different timeout value forces the method to go
        # to the database and find the correct answer.
        newer_update = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert newer_update > last_update
        
        # It's also possible to change the timeout value through a
        # site-wide ConfigurationSetting
        ConfigurationSetting.sitewide(
            self._db, Configuration.SITE_CONFIGURATION_TIMEOUT
        ).value = 0
        timestamp = Timestamp.stamp(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        even_newer_update = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert even_newer_update > newer_update


        # If ConfigurationSettings are updated twice within the
        # timeout period (default 1 second), the last update time is
        # only set once, to avoid spamming the Timestamp with updates.
        
        # The high site-wide value for 'timeout' saves this code. If we decided
        # that the timeout had expired and tried to check the
        # Timestamp, the code would crash because we're not passing
        # a database connection in.
        site_configuration_has_changed(None, timeout=100)

        # Nothing has changed -- how could it, with no database connection
        # to modify anything?
        eq_(even_newer_update, 
            Configuration.site_configuration_last_update(self._db))

    # We don't test every event listener, but we do test one of each type.
    def test_configuration_relevant_lifecycle_event_updates_configuration(self):
        """When you create or modify a relevant item such as a
        ConfigurationSetting, site_configuration_has_changed is called.
        """                
        ConfigurationSetting.sitewide(self._db, "setting").value = "value"
        self.mock.assert_was_called()
        
        ConfigurationSetting.sitewide(self._db, "setting").value = "value2"
        self.mock.assert_was_called()

    def test_lane_change_updates_configuration(self):
        """Verify that configuration-relevant changes work the same way
        in the lane module as they do in the model module.
        """
        lane = self._lane()
        self.mock.assert_was_called()
        
        lane.add_genre("Science Fiction")
        self.mock.assert_was_called()

    def test_configuration_relevant_collection_change_updates_configuration(self):
        """When you add a relevant item to a SQLAlchemy collection, such as
        adding a Collection to library.collections,
        site_configuration_has_changed is called.
        """                

        # Creating a collection calls the method via an 'after_insert'
        # event on Collection.
        library = self._default_library
        collection = self._collection()
        self._db.commit()
        self.mock.assert_was_called()

        # Adding the collection to the library calls the method via
        # an 'append' event on Collection.libraries.
        library.collections.append(collection)
        self._db.commit()
        self.mock.assert_was_called()

        # Associating a CachedFeed with the library does _not_ call
        # the method, because nothing changed on the Library object and
        # we don't listen for 'append' events on Library.cachedfeeds.
        create(self._db, CachedFeed, type='page', pagination='', 
               facets='', library=library)
        self._db.commit()
        self.mock.assert_was_not_called()
    
class TestCollection(DatabaseTest):

    def setup(self):
        super(TestCollection, self).setup()
        self.collection = self._collection(
            name="test collection", protocol=ExternalIntegration.OVERDRIVE
        )

    def test_by_name_and_protocol(self):
        name = "A name"
        protocol = ExternalIntegration.OVERDRIVE
        key = (name, protocol)
        
        # Cache is empty.
        eq_(HasFullTableCache.RESET, Collection._cache)
        
        collection1, is_new = Collection.by_name_and_protocol(
            self._db, name, ExternalIntegration.OVERDRIVE
        )
        eq_(True, is_new)

        # Cache was populated and then reset because we created a new
        # Collection.
        eq_(HasFullTableCache.RESET, Collection._cache)
        
        collection2, is_new = Collection.by_name_and_protocol(
            self._db, name, ExternalIntegration.OVERDRIVE
        )
        eq_(collection1, collection2)
        eq_(False, is_new)

        # This time the cache was not reset after being populated.
        eq_(collection1, Collection._cache[key])
        
        # You'll get an exception if you look up an existing name
        # but the protocol doesn't match.
        assert_raises_regexp(
            ValueError,
            'Collection "A name" does not use protocol "Bibliotheca".',
            Collection.by_name_and_protocol,
            self._db, name, ExternalIntegration.BIBLIOTHECA
        )

    def test_by_protocol(self):
        """Verify the ability to find all collections that implement
        a certain protocol.
        """
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA
        c1 = self._collection(self._str, protocol=overdrive)
        c1.parent = self.collection
        c2 = self._collection(self._str, protocol=bibliotheca)
        eq_(set([self.collection, c1]),
            set(Collection.by_protocol(self._db, overdrive).all()))
        eq_(([c2]),
            Collection.by_protocol(self._db, bibliotheca).all())
        eq_(set([self.collection, c1, c2]),
            set(Collection.by_protocol(self._db, None).all()))

    def test_by_datasource(self):
        """Collections can be found by their associated DataSource"""
        c1 = self._collection(data_source_name=DataSource.GUTENBERG)
        c2 = self._collection(data_source_name=DataSource.OVERDRIVE)

        # Using the DataSource name
        eq_(set([c1]),
            set(Collection.by_datasource(self._db, DataSource.GUTENBERG).all()))

        # Using the DataSource itself
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        eq_(set([c2]),
            set(Collection.by_datasource(self._db, overdrive).all()))

    def test_parents(self):
        # Collections can return all their parents recursively.
        c1 = self._collection()
        eq_([], list(c1.parents))

        c2 = self._collection()
        c2.parent_id = c1.id
        eq_([c1], list(c2.parents))

        c3 = self._collection()
        c3.parent_id = c2.id
        eq_([c2, c1], list(c3.parents))

    def test_create_external_integration(self):
        # A newly created Collection has no associated ExternalIntegration.
        collection, ignore = get_one_or_create(
            self._db, Collection, name=self._str
        )
        eq_(None, collection.external_integration_id)
        assert_raises_regexp(
            ValueError,
            "No known external integration for collection",
            getattr, collection, 'external_integration'
        )
        
        # We can create one with create_external_integration().
        overdrive = ExternalIntegration.OVERDRIVE
        integration = collection.create_external_integration(protocol=overdrive)
        eq_(integration.id, collection.external_integration_id)
        eq_(overdrive, integration.protocol)

        # If we call create_external_integration() again we get the same
        # ExternalIntegration as before.
        integration2 = collection.create_external_integration(protocol=overdrive)
        eq_(integration, integration2)
        
        
        # If we try to initialize an ExternalIntegration with a different
        # protocol, we get an error.
        assert_raises_regexp(
            ValueError,
            "Located ExternalIntegration, but its protocol \(Overdrive\) does not match desired protocol \(blah\).",
            collection.create_external_integration,
            protocol="blah"
        )
        
    def test_change_protocol(self):
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA

        # Create a parent and a child collection, both with
        # protocol=Overdrive.
        child = self._collection(self._str, protocol=overdrive)
        child.parent = self.collection

        # We can't change the child's protocol to a value that contradicts
        # the parent's protocol.
        child.protocol = overdrive
        def set_child_protocol():
            child.protocol = bibliotheca
        assert_raises_regexp(
            ValueError,
            "Proposed new protocol \(Bibliotheca\) contradicts parent collection's protocol \(Overdrive\).",
            set_child_protocol
        )

        # If we change the parent's protocol, the children are
        # automatically updated.
        self.collection.protocol = bibliotheca
        eq_(bibliotheca, child.protocol)

    def test_data_source(self):
        opds = self._collection()
        bibliotheca = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # The rote data_source is returned for the obvious collection.
        eq_(DataSource.BIBLIOTHECA, bibliotheca.data_source.name)

        # The less obvious OPDS collection doesn't have a DataSource.
        eq_(None, opds.data_source)

        # Trying to change the Bibliotheca collection's data_source does nothing.
        bibliotheca.data_source = DataSource.AXIS_360
        eq_(DataSource.BIBLIOTHECA, bibliotheca.data_source.name)

        # Trying to change the opds collection's data_source is fine.
        opds.data_source = DataSource.PLYMPTON
        eq_(DataSource.PLYMPTON, opds.data_source.name)

        # Resetting it to something else is fine.
        opds.data_source = DataSource.OA_CONTENT_SERVER
        eq_(DataSource.OA_CONTENT_SERVER, opds.data_source.name)

        # Resetting it to None is fine.
        opds.data_source = None
        eq_(None, opds.data_source)

    def test_default_loan_period(self):
        library = self._default_library
        library.collections.append(self.collection)

        ebook = Edition.BOOK_MEDIUM
        audio = Edition.AUDIO_MEDIUM

        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD, 
            self.collection.default_loan_period(library, ebook)
        )

        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD, 
            self.collection.default_loan_period(library, audio)
        )

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(library, ebook).value = 604
        eq_(604, self.collection.default_loan_period(library))
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD, 
            self.collection.default_loan_period(library, audio)
        )

        self.collection.default_loan_period_setting(library, audio).value = 606
        eq_(606, self.collection.default_loan_period(library, audio))

        # Given an integration client rather than a library, use
        # a sitewide integration setting rather than a library-specific
        # setting.
        client = self._integration_client()

        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD, 
            self.collection.default_loan_period(client, ebook)
        )

        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD, 
            self.collection.default_loan_period(client, audio)
        )

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(client, ebook).value = 347
        eq_(347, self.collection.default_loan_period(client))
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD, 
            self.collection.default_loan_period(client, audio)
        )

        self.collection.default_loan_period_setting(client, audio).value = 349
        eq_(349, self.collection.default_loan_period(client, audio))

        # The same value is used for other clients.
        client2 = self._integration_client()
        eq_(347, self.collection.default_loan_period(client))
        eq_(349, self.collection.default_loan_period(client, audio))

    def test_default_reservation_period(self):
        library = self._default_library
        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_RESERVATION_PERIOD, 
            self.collection.default_reservation_period
        )

        # Set a value, and it's used.
        self.collection.default_reservation_period = 601
        eq_(601, self.collection.default_reservation_period)

        # The underlying value is controlled by a ConfigurationSetting.
        self.collection.external_integration.setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY
        ).value = 954
        eq_(954, self.collection.default_reservation_period)

    def test_explain(self):
        """Test that Collection.explain gives all relevant information
        about a Collection.
        """
        library = self._default_library
        library.name="The only library"
        library.short_name = "only one"
        library.collections.append(self.collection)
        
        self.collection.external_account_id = "id"
        self.collection.external_integration.url = "url"
        self.collection.external_integration.username = "username"
        self.collection.external_integration.password = "password"
        setting = self.collection.external_integration.set_setting("setting", "value")

        data = self.collection.explain()
        eq_(['Name: "test collection"',
             'Protocol: "Overdrive"',
             'Used by library: "only one"',
             'External account ID: "id"',
             'Setting "setting": "value"',
             'Setting "url": "url"',
             'Setting "username": "username"',
        ],
            data
        )

        with_password = self.collection.explain(include_secrets=True)
        assert 'Setting "password": "password"' in with_password

        # If the collection is the child of another collection,
        # its parent is mentioned.
        child = Collection(
            name="Child", parent=self.collection, external_account_id="id2"
        )
        child.create_external_integration(
            protocol=ExternalIntegration.OVERDRIVE
        )
        data = child.explain()
        eq_(['Name: "Child"',
             'Parent: test collection',
             'Protocol: "Overdrive"',
             'External account ID: "id2"'],
            data
        )

    def test_metadata_identifier(self):
        # If the collection doesn't have its unique identifier, an error
        # is raised.
        assert_raises(ValueError, getattr, self.collection, 'metadata_identifier')

        def build_expected(protocol, unique_id):
            encoded = [base64.b64encode(unicode(value), '-_')
                       for value in [protocol, unique_id]]
            return base64.b64encode(':'.join(encoded), '-_')

        # With a unique identifier, we get back the expected identifier.
        self.collection.external_account_id = 'id'
        expected = build_expected(ExternalIntegration.OVERDRIVE, 'id')
        eq_(expected, self.collection.metadata_identifier)

        # If there's a parent, its unique id is incorporated into the result.
        child = self._collection(
            name="Child", protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=self._url
        )
        child.parent = self.collection
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, 'id+%s' % child.external_account_id)
        eq_(expected, child.metadata_identifier)

        # If it's an OPDS_IMPORT collection with a url external_account_id,
        # closing '/' marks are removed.
        opds = self._collection(
            name='OPDS', protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=(self._url+'/')
        )
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, opds.external_account_id[:-1])
        eq_(expected, opds.metadata_identifier)

    def test_from_metadata_identifier(self):
        # If a mirrored collection doesn't exist, it is created.
        self.collection.external_account_id = 'id'
        mirror_collection, is_new = Collection.from_metadata_identifier(
            self._db, self.collection.metadata_identifier
        )
        eq_(True, is_new)
        eq_(self.collection.metadata_identifier, mirror_collection.name)
        eq_(self.collection.protocol, mirror_collection.protocol)
        # Because this isn't an OPDS collection, no account details are held.
        eq_(None, mirror_collection.external_account_id)

        # If the mirrored collection already exists, it is returned.
        collection = self._collection(external_account_id=self._url)
        mirror_collection = create(
            self._db, Collection,
            name=collection.metadata_identifier,
        )[0]
        mirror_collection.create_external_integration(collection.protocol)
        # Confirm that there's no external_account_id and no DataSource.
        eq_(None, mirror_collection.external_account_id)
        eq_(None, mirror_collection.data_source)

        source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        result, is_new = Collection.from_metadata_identifier(
            self._db, collection.metadata_identifier, data_source=source
        )
        eq_(False, is_new)
        eq_(mirror_collection, result)
        # The external_account_id and data_source have been set now.
        eq_(collection.external_account_id, mirror_collection.external_account_id)
        eq_(source, mirror_collection.data_source)

    def test_catalog_identifier(self):
        """#catalog_identifier associates an identifier with the catalog"""
        identifier = self._identifier()
        self.collection.catalog_identifier(identifier)

        eq_(1, len(self.collection.catalog))
        eq_(identifier, self.collection.catalog[0])

    def test_catalog_identifiers(self):
        """#catalog_identifier associates multiple identifiers with a catalog"""
        i1 = self._identifier()
        i2 = self._identifier()
        i3 = self._identifier()

        # One of the identifiers is already in the catalog.
        self.collection.catalog_identifier(i3)

        self.collection.catalog_identifiers([i1, i2, i3])

        # Now all three identifiers are in the catalog.
        assert sorted([i1, i2, i3]) == sorted(self.collection.catalog)

    def test_unresolved_catalog(self):
        # A regular schmegular identifier: untouched, pure.
        pure_id = self._identifier()

        # A 'resolved' identifier that doesn't have a work yet.
        # (This isn't supposed to happen, but jic.)
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        operation = 'test-thyself'
        resolved_id = self._identifier()
        self._coverage_record(
            resolved_id, source, operation=operation,
            status=CoverageRecord.SUCCESS
        )

        # An unresolved identifier--we tried to resolve it, but
        # it all fell apart.
        unresolved_id = self._identifier()
        self._coverage_record(
            unresolved_id, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE
        )

        # An identifier with a Work already.
        id_with_work = self._work().presentation_edition.primary_identifier


        self.collection.catalog_identifiers([
            pure_id, resolved_id, unresolved_id, id_with_work
        ])

        result = self.collection.unresolved_catalog(
            self._db, source.name, operation
        )

        # Only the failing identifier is in the query.
        eq_([unresolved_id], result.all())

    def test_works_updated_since(self):
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        w3 = self._work(with_license_pool=True)

        # An empty catalog returns nothing.
        timestamp = datetime.datetime.utcnow()
        eq_([], self.collection.works_updated_since(self._db, timestamp).all())

        self.collection.catalog_identifier(w1.license_pools[0].identifier)
        self.collection.catalog_identifier(w2.license_pools[0].identifier)

        # This Work is catalogued in another catalog and will never show up.
        collection2 = self._collection()
        in_other_catalog = self._work(
            with_license_pool=True, collection=collection2
        )
        collection2.catalog_identifier(
            in_other_catalog.license_pools[0].identifier
        )

        # When no timestamp is passed, all works in the catalog are returned.
        # in order of their WorkCoverageRecord timestamp.
        t1, t2 = self.collection.works_updated_since(self._db, None).all()
        eq_(w1, t1[0])
        eq_(w2, t2[0])

        # The return value is a sequence of 5-tuples, each containing
        # (Work, LicensePool, Identifier, WorkCoverageRecord,
        # CollectionIdentifier). This gives the caller all the information
        # necessary to understand the path by which a given Work belongs to
        # a given Collection.
        _w1, lp1, i1 = t1
        [pool] = w1.license_pools
        eq_(pool, lp1)
        eq_(pool.identifier, i1)

        # When a timestamp is passed, only works that have been updated
        # since then will be returned
        [w1_coverage_record] = [
            c for c in w1.coverage_records
            if c.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION
        ]
        w1_coverage_record.timestamp = datetime.datetime.utcnow()
        eq_([w1], [x[0] for x in self.collection.works_updated_since(self._db, timestamp)])

    def test_isbns_updated_since(self):
        i1 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i2 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i3 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i4 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)

        timestamp = datetime.datetime.utcnow()

        # An empty catalog returns nothing..
        eq_([], self.collection.isbns_updated_since(self._db, None).all())

        # Give the ISBNs some coverage.
        content_cafe = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)
        for isbn in [i2, i3, i1]:
            self._coverage_record(isbn, content_cafe)

        # Give one ISBN more than one coverage record.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        i1_oclc_record = self._coverage_record(i1, oclc)

        def assert_isbns(expected, result_query):
            results = [r[0] for r in result_query]
            eq_(expected, results)

        # When no timestamp is given, all ISBNs in the catalog are returned,
        # in order of their CoverageRecord timestamp.
        self.collection.catalog_identifiers([i1, i2])
        updated_isbns = self.collection.isbns_updated_since(self._db, None).all()
        assert_isbns([i2, i1], updated_isbns)

        # That CoverageRecord timestamp is also returned.
        i1_timestamp = updated_isbns[1][1]
        assert isinstance(i1_timestamp, datetime.datetime)
        eq_(i1_oclc_record.timestamp, i1_timestamp)

        # When a timestamp is passed, only works that have been updated since
        # then will be returned.
        timestamp = datetime.datetime.utcnow()
        i1.coverage_records[0].timestamp = datetime.datetime.utcnow()
        updated_isbns = self.collection.isbns_updated_since(self._db, timestamp)
        assert_isbns([i1], updated_isbns)

        # Prepare an ISBN associated with a Work.
        work = self._work(with_license_pool=True)
        work.license_pools[0].identifier = i2
        i2.coverage_records[0].timestamp = datetime.datetime.utcnow()

        # ISBNs that have a Work will be ignored.
        updated_isbns = self.collection.isbns_updated_since(self._db, timestamp)
        assert_isbns([i1], updated_isbns)

    def test_custom_lists(self):
        # A Collection can be associated with one or more CustomLists.
        list1, ignore = get_one_or_create(self._db, CustomList, name=self._str)
        list2, ignore = get_one_or_create(self._db, CustomList, name=self._str)
        self.collection.customlists = [list1, list2]
        eq_(0, len(list1.entries))
        eq_(0, len(list2.entries))

        # When a new pool is added to the collection and its presentation edition is
        # calculated for the first time, it's automatically added to the lists.
        work = self._work(collection=self.collection, with_license_pool=True)
        eq_(1, len(list1.entries))
        eq_(1, len(list2.entries))
        eq_(work, list1.entries[0].work)
        eq_(work, list2.entries[0].work)

        # Now remove it from one of the lists. If its presentation edition changes
        # again or its pool changes works, it's not added back.
        self._db.delete(list1.entries[0])
        self._db.commit()
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))

        pool = work.license_pools[0]
        identifier = pool.identifier
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        staff_edition, ignore = Edition.for_foreign_id(
            self._db, staff_data_source,
            identifier.type, identifier.identifier)

        staff_edition.title = self._str
        work.calculate_presentation()
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))

        new_work = self._work(collection=self.collection)
        pool.work = new_work
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))


class TestCollectionForMetadataWrangler(DatabaseTest):

    """Tests that requirements to the metadata wrangler's use of Collection
    are being met by continued development on the Collection class.

    If any of these tests are failing, development will be required on the
    metadata wrangler to meet the needs of the new Collection class.
    """

    def test_only_name_is_required(self):
        """Test that only name is a required field on
        the Collection class.
        """
        collection = create(
            self._db, Collection, name='banana'
        )[0]
        eq_(True, isinstance(collection, Collection))


class TestIntegrationClient(DatabaseTest):

    def setup(self):
        super(TestIntegrationClient, self).setup()
        self.client = self._integration_client()

    def test_for_url(self):
        now = datetime.datetime.utcnow()
        url = self._url
        client, is_new = IntegrationClient.for_url(self._db, url)

        # A new IntegrationClient has been created.
        eq_(True, is_new)

        # Its .url is a normalized version of the provided URL.
        eq_(client.url, IntegrationClient.normalize_url(url))

        # It has timestamps for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        eq_(True, isinstance(client.created, datetime.datetime))
        eq_(client.created, client.last_accessed)

        # It does not have a shared secret.
        eq_(None, client.shared_secret)

        # Calling it again on the same URL gives the same object.
        client2, is_new = IntegrationClient.for_url(self._db, url)
        eq_(client, client2)

    def test_register(self):
        now = datetime.datetime.utcnow()
        client, is_new = IntegrationClient.register(self._db, self._url)

        # It creates a shared_secret.
        assert client.shared_secret
        # And sets a timestamp for created & last_accessed.
        assert client.created and client.last_accessed
        assert client.created > now
        eq_(True, isinstance(client.created, datetime.datetime))
        eq_(client.created, client.last_accessed)

        # It raises an error if the url is already registered and the
        # submitted shared_secret is inaccurate.
        assert_raises(ValueError, IntegrationClient.register, self._db, client.url)
        assert_raises(ValueError, IntegrationClient.register, self._db, client.url, 'wrong')

    def test_authenticate(self):

        result = IntegrationClient.authenticate(self._db, u"secret")
        eq_(self.client, result)

        result = IntegrationClient.authenticate(self._db, u"wrong_secret")
        eq_(None, result)

    def test_normalize_url(self):
        # http/https protocol is removed.
        url = 'https://fake.com'
        eq_('fake.com', IntegrationClient.normalize_url(url))

        url = 'http://really-fake.com'
        eq_('really-fake.com', IntegrationClient.normalize_url(url))

        # www is removed if it exists, along with any trailing /
        url = 'https://www.also-fake.net/'
        eq_('also-fake.net', IntegrationClient.normalize_url(url))

        # Subdomains and paths are retained.
        url = 'https://www.super.fake.org/wow/'
        eq_('super.fake.org/wow', IntegrationClient.normalize_url(url))

        # URL is lowercased.
        url = 'http://OMG.soVeryFake.gov'
        eq_('omg.soveryfake.gov', IntegrationClient.normalize_url(url))


class TestMaterializedViews(DatabaseTest):

    def test_license_pool_is_works_preferred_license_pool(self):
        """Verify that the license_pool_id stored in the materialized views
        identifies the LicensePool associated with the Work's
        presentation edition, not some other LicensePool.
        """
        # Create a Work with two LicensePools
        work = self._work(with_license_pool=True)
        [pool1] = work.license_pools
        edition2, pool2 = self._edition(with_license_pool=True)
        work.license_pools.append(pool1)
        eq_([pool1], work.presentation_edition.license_pools)
        work.presentation_ready = True
        work.simple_opds_entry = '<entry>'
        work.assign_genres_from_weights({classifier.Fantasy : 1})

        # Make sure the Work shows up in the materialized view.
        SessionManager.refresh_materialized_views(self._db)

        from model import MaterializedWorkWithGenre as mwgc
        [mwg] = self._db.query(mwgc).all()

        eq_(pool1.id, mwg.license_pool_id)

        # If we change the Work's preferred edition, we change the
        # license_pool_id that gets stored in the materialized views.
        work.set_presentation_edition(edition2)
        SessionManager.refresh_materialized_views(self._db)
        [mwg] = self._db.query(mwgc).all()

        eq_(pool2.id, mwg.license_pool_id)

    def test_license_data_source_is_stored_in_views(self):
        """Verify that the data_source_name stored in the materialized view
        is the DataSource associated with the LicensePool, not the
        DataSource associated with the presentation Edition.
        """

        # Create a Work whose LicensePool has three Editions: one from
        # Gutenberg (created by default), one from the admin interface
        # (created manually), and one generated by the presentation
        # edition generator, which synthesizes the other two.
        work = self._work(with_license_pool=True)

        [pool] = work.license_pools
        gutenberg_edition = pool.presentation_edition

        identifier = pool.identifier
        staff_edition = self._edition(
            data_source_name=DataSource.LIBRARY_STAFF, 
            identifier_type=identifier.type, 
            identifier_id=identifier.identifier
        )
        staff_edition.title = u"staff chose this title"
        staff_edition.sort_title = u"staff chose this title"
        pool.set_presentation_edition()
        work.set_presentation_edition(pool.presentation_edition)

        # The presentation edition has the title taken from the admin
        # interface, but it was created by the presentation edition
        # generator.
        presentation_edition = pool.presentation_edition
        eq_("staff chose this title", presentation_edition.title)
        eq_(DataSource.PRESENTATION_EDITION, 
            presentation_edition.data_source.name
        )

        # Make sure the Work will show up in the materialized view.
        work.presentation_ready = True
        work.simple_opds_entry = '<entry>'
        work.assign_genres_from_weights({classifier.Fantasy : 1})

        SessionManager.refresh_materialized_views(self._db)

        from model import MaterializedWorkWithGenre as mwgc
        [mwg] = self._db.query(mwgc).all()

        # We would expect the data source to be Gutenberg, since
        # that's the edition associated with the LicensePool, and not
        # the data source of the Work's presentation edition.
        eq_(pool.data_source.name, mwg.name)

        # However, we would expect the title of the work to come from
        # the presentation edition.
        eq_("staff chose this title", mwg.sort_title)

        # And since the data_source_id is the ID of the data source
        # associated with the license pool, we would expect it to be
        # the data source ID of the license pool.
        eq_(pool.data_source.id, mwg.data_source_id)

    def test_work_on_same_list_twice(self):
        # Here's the NYT best-seller list.
        cl, ignore = self._customlist(num_entries=0)

        # Here are two Editions containing data from the NYT
        # best-seller list.
        now = datetime.datetime.utcnow()
        earlier = now - datetime.timedelta(seconds=3600)
        edition1 = self._edition()
        entry1, ignore = cl.add_entry(edition1, first_appearance=earlier)

        edition2 = self._edition()
        entry2, ignore = cl.add_entry(edition2, first_appearance=now)

        # In a shocking turn of events, we've determined that the two
        # editions are slight title variants of the same work.
        romance, ignore = Genre.lookup(self._db, "Romance")
        work = self._work(with_license_pool=True, genre=romance)
        entry1.work = work
        entry2.work = work
        self._db.commit()

        # The materialized view can handle this revelation
        # and stores the two list entries in different rows.
        SessionManager.refresh_materialized_views(self._db)
        from model import MaterializedWorkWithGenre as mw
        [o1, o2] = self._db.query(mw).order_by(mw.list_edition_id)

        # Both MaterializedWorkWithGenre objects are on the same
        # list, associated with the same work, the same genre,
        # and the same presentation edition.
        for o in (o1, o2):
            eq_(cl.id, o.list_id)
            eq_(work.id, o.works_id)
            eq_(romance.id, o.genre_id)
            eq_(work.presentation_edition.id, o.editions_id)

        # But they are associated with different list editions.
        eq_(edition1.id, o1.list_edition_id)
        eq_(edition2.id, o2.list_edition_id)


class TestAdmin(DatabaseTest):
    def setup(self):
        super(TestAdmin, self).setup()
        self.admin, ignore = create(self._db, Admin, email=u"admin@nypl.org")
        self.admin.password = u"password"

    def test_password_hashed(self):
        assert_raises(NotImplementedError, lambda: self.admin.password)
        assert self.admin.password_hashed.startswith('$2a$')

    def test_with_password(self):
        self._db.delete(self.admin)
        eq_([], Admin.with_password(self._db).all())

        admin, ignore = create(self._db, Admin, email="admin@nypl.org")
        eq_([], Admin.with_password(self._db).all())

        admin.password = "password"
        eq_([admin], Admin.with_password(self._db).all())

        admin2, ignore = create(self._db, Admin, email="admin2@nypl.org")
        eq_([admin], Admin.with_password(self._db).all())

        admin2.password = "password2"
        eq_(set([admin, admin2]), set(Admin.with_password(self._db).all()))

    def test_has_password(self):
        eq_(True, self.admin.has_password(u"password"))
        eq_(False, self.admin.has_password(u"banana"))

    def test_authenticate(self):
        other_admin, ignore = create(self._db, Admin, email=u"other@nypl.org")
        other_admin.password = u"banana"
        eq_(self.admin, Admin.authenticate(self._db, "admin@nypl.org", "password"))
        eq_(None, Admin.authenticate(self._db, "other@nypl.org", "password"))
        eq_(None, Admin.authenticate(self._db, "example@nypl.org", "password"))

    def test_roles(self):
        # The admin has no roles yet.
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(False, self.admin.is_librarian(self._default_library))

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        eq_(True, self.admin.is_system_admin())
        eq_(True, self.admin.is_sitewide_library_manager())
        eq_(True, self.admin.is_sitewide_librarian())
        eq_(True, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        eq_(False, self.admin.is_system_admin())
        eq_(True, self.admin.is_sitewide_library_manager())
        eq_(True, self.admin.is_sitewide_librarian())
        eq_(True, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(True, self.admin.is_sitewide_librarian())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(False, self.admin.is_sitewide_librarian())
        eq_(True, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(False, self.admin.is_sitewide_librarian())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(False, self.admin.is_sitewide_librarian())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(False, self.admin.is_librarian(self._default_library))

        other_library = self._library()
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, other_library)
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_library_manager(other_library))
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_library_manager(other_library))
        eq_(True, self.admin.is_librarian(self._default_library))
        eq_(True, self.admin.is_librarian(other_library))
        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, other_library)
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(False, self.admin.is_library_manager(other_library))
        eq_(True, self.admin.is_librarian(self._default_library))
        eq_(True, self.admin.is_librarian(other_library))

class TestTupleToNumericrange(object):
    """Test the tuple_to_numericrange helper function."""

    def test_tuple_to_numericrange(self):
        f = tuple_to_numericrange
        eq_(None, f(None))

        one_to_ten = f((1,10))
        assert isinstance(one_to_ten, NumericRange)
        eq_(1, one_to_ten.lower)
        eq_(10, one_to_ten.upper)
        eq_(True, one_to_ten.upper_inc)

        up_to_ten = f((None, 10))
        assert isinstance(up_to_ten, NumericRange)
        eq_(None, up_to_ten.lower)
        eq_(10, up_to_ten.upper)
        eq_(True, up_to_ten.upper_inc)

        ten_and_up = f((10,None))
        assert isinstance(ten_and_up, NumericRange)
        eq_(10, ten_and_up.lower)
        eq_(None, ten_and_up.upper)
        eq_(False, ten_and_up.upper_inc)



class MockHasTableCache(HasFullTableCache):

    """A simple HasFullTableCache that returns the same cache key
    for every object.
    """
    
    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    ID = "the only ID"
    KEY = "the only cache key"

    @property
    def id(self):
        return self.ID
    
    def cache_key(self):
        return self.KEY
    
        
class TestHasFullTableCache(DatabaseTest):

    def setup(self):
        super(TestHasFullTableCache, self).setup()
        self.mock_class = MockHasTableCache
        self.mock = MockHasTableCache()
        self.mock._cache = HasFullTableCache.RESET

    def test_reset_cache(self):
        self.mock_class._cache = object()
        self.mock_class._id_cache = object()
        self.mock_class.reset_cache()
        eq_(HasFullTableCache.RESET, self.mock_class._cache)
        eq_(HasFullTableCache.RESET, self.mock_class._id_cache)

    def test_cache_insert(self):
        temp_cache = {}
        temp_id_cache = {}
        self.mock_class._cache_insert(self.mock, temp_cache, temp_id_cache)
        eq_({MockHasTableCache.KEY: self.mock}, temp_cache)
        eq_({MockHasTableCache.ID: self.mock}, temp_id_cache)

    # populate_cache(), by_cache_key(), and by_id() are tested in
    # TestGenre since those methods must be backed by a real database
    # table.


    
