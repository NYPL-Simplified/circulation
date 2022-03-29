# encoding: utf-8
import pytest
import datetime
import feedparser
from lxml import etree
from mock import PropertyMock, create_autospec

from ...model import PresentationCalculationPolicy
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier
from ...model.resource import Hyperlink, Representation
from ...util.datetime_helpers import utc_now
from ...util.opds_writer import AtomFeed


class TestIdentifier:

    def test_for_foreign_id(self, db_session):
        """
        GIVEN: An identifier type of ISBN and an ISBN foreign ID
        WHEN:  Getting an Identifier for that type and foreign ID
        THEN:  An Identifier is fetched from the database or created
        """
        identifier_type = Identifier.ISBN
        isbn = "3293000061"

        # Getting the data automatically creates a database record.
        identifier, was_new = Identifier.for_foreign_id(db_session, identifier_type, isbn)
        assert Identifier.ISBN == identifier.type
        assert isbn == identifier.identifier
        assert was_new is True

        # If we get it again we get the same data, but it's no longer new.
        identifier2, was_new = Identifier.for_foreign_id(db_session, identifier_type, isbn)
        assert identifier == identifier2
        assert was_new is False

        # If we pass in no data we get nothing back.
        assert Identifier.for_foreign_id(db_session, None, None) is None

    def test_for_foreign_id_by_deprecated_type(self, db_session):
        """
        GIVEN: An deprecated identifier type and foreign ID
        WHEN:  Getting an Identifier for the deprecated type and foreign ID
        THEN:  The returned Identifier is associated with the new type
        """
        threem_id, _ = Identifier.for_foreign_id(db_session, "3M ID", "test")
        assert Identifier.BIBLIOTHECA_ID == threem_id.type
        assert Identifier.BIBLIOTHECA_ID != "3M ID"

    def test_for_foreign_id_rejects_invalid_identifiers(self, db_session):
        """
        GIVEN: An invalid foreign identifier
        WHEN:  Determining if the foreign identifier is valid
        THEN:  A ValueError is raised
        """
        with pytest.raises(ValueError) as excinfo:
            Identifier.for_foreign_id(db_session, Identifier.BIBLIOTHECA_ID, "foo/bar")
        assert '"foo/bar" is not a valid Bibliotheca ID.' in str(excinfo.value)

    @pytest.mark.parametrize(
        'type,id,expected',
        [
            (Identifier.BIBLIOTHECA_ID, 'bhhot389', True),
            (Identifier.BIBLIOTHECA_ID, 'bhhot389/open_book', False),
            (Identifier.BIBLIOTHECA_ID, 'bhhot389,bhhot389', False),
            (Identifier.BIBLIOTHECA_ID, '0015142259', True),
            (Identifier.BIBLIOTHECA_ID, '0015142259,0015187940', False),
            (Identifier.AXIS_360_ID, 'foo/bar', True),
            (Identifier.AXIS_360_ID, 'foo,bar', False)
        ],
    )
    def test_valid_as_foreign_identifier(self, type, id, expected):
        """
        GIVEN: A foreign ID
        WHEN:  Determining if the foreign identifier is valid
        THEN:  The Identifier is valid / invalid
        """

        assert Identifier.valid_as_foreign_identifier(type, id) is expected

    def test_for_foreign_id_without_autocreate(self, db_session):
        """
        GIVEN: A foreign ID
        WHEN:  Turning a foreign ID into an Identifier with autocreate=False
        THEN:  An Identifier was not retrieved or created
        """
        identifier_type = Identifier.ISBN
        isbn = "test"

        # We don't want to auto-create a database record, so we set autocreate=False
        identifier, was_new = Identifier.for_foreign_id(db_session, identifier_type, isbn, autocreate=False)

        assert identifier is None
        assert was_new is False

    def test_from_asin(self, db_session):
        """
        GIVEN: ASIN-like strings
        WHEN:  Turning the ASIN-like string into an Identifier
        THEN:  One ISBN Identifier is created for the equivalent strings
        """
        isbn10 = '1449358063'
        isbn13 = '9781449358068'
        asin = 'B0088IYM3C'
        isbn13_with_dashes = '978-144-935-8068'

        i_isbn10, new1 = Identifier.from_asin(db_session, isbn10)
        i_isbn13, new2 = Identifier.from_asin(db_session, isbn13)
        i_asin, _ = Identifier.from_asin(db_session, asin)
        i_isbn13_2, new4 = Identifier.from_asin(db_session, isbn13_with_dashes)

        # The three ISBNs are equivalent, so they got turned into the same
        # Identifier, using the ISBN13.
        assert i_isbn10 == i_isbn13
        assert i_isbn13_2 == i_isbn13
        assert Identifier.ISBN == i_isbn10.type
        assert isbn13 == i_isbn10.identifier
        assert new1 is True
        assert new2 is False
        assert new4 is False

        assert Identifier.ASIN == i_asin.type
        assert asin == i_asin.identifier

    def test_urn(self, db_session, create_identifier):
        """
        GIVEN: An Identifier
        WHEN:  Turning a foreign ID into an Identifier
        THEN:  The URN is correctly set
        """
        # ISBN identifiers use the ISBN URN scheme.
        identifier, _ = Identifier.for_foreign_id(db_session, Identifier.ISBN, "9781449358068")
        assert "urn:isbn:9781449358068" == identifier.urn

        # URI identifiers don't need a URN scheme.
        identifier, _ = Identifier.for_foreign_id(db_session, Identifier.URI, "http://example.com/")
        assert identifier.identifier == identifier.urn

        # Gutenberg identifiers use Gutenberg's URL-based sceheme
        identifier = create_identifier(db_session, identifier_type=Identifier.GUTENBERG_ID)
        assert (Identifier.GUTENBERG_URN_SCHEME_PREFIX + identifier.identifier == identifier.urn)

        # All other identifiers use our custom URN scheme.
        identifier = create_identifier(db_session, identifier_type=Identifier.OVERDRIVE_ID)
        assert identifier.urn.startswith(Identifier.URN_SCHEME_PREFIX)

    def test_parse_urns(self, db_session, create_identifier):
        """
        GIVEN: Multiple URNs
        WHEN:  Parsing the URNs
        THEN:  URN is turned into an Identifier if it is valid
        """
        identifier = create_identifier(db_session)
        fake_urn = "what_even_is_this"
        new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/nosuchidentifier"
        # Also create a different URN that would result in the same identifier.
        same_new_urn = Identifier.URN_SCHEME_PREFIX + "Overdrive%20ID/NOSUCHidentifier"
        urns = [identifier.urn, fake_urn, new_urn, same_new_urn]

        results = Identifier.parse_urns(db_session, urns, autocreate=False)
        identifiers_by_urn, failures = results

        # By default, no new identifiers are created. All URNs for identifiers
        # that aren't in the db are included in the list of failures.
        assert sorted([fake_urn, new_urn, same_new_urn]) == sorted(failures)

        # Only the existing identifier is included in the results.
        assert 1 == len(identifiers_by_urn)
        assert {identifier.urn: identifier} == identifiers_by_urn

        # By default, new identifiers are created, too.
        results = Identifier.parse_urns(db_session, urns)
        identifiers_by_urn, failures = results

        # Only the fake URN is returned as a failure.
        assert [fake_urn] == failures

        # Only two additional identifiers have been created.
        assert 2 == len(identifiers_by_urn)

        # One is the existing identifier.
        assert identifier == identifiers_by_urn[identifier.urn]

        # And the new identifier has been created.
        new_identifier = identifiers_by_urn[new_urn]
        assert isinstance(new_identifier, Identifier)
        assert new_identifier in db_session
        assert Identifier.OVERDRIVE_ID == new_identifier.type
        assert "nosuchidentifier" == new_identifier.identifier

        # By passing in a list of allowed_types we can stop certain
        # types of Identifiers from being looked up, even if they
        # already exist.
        isbn_urn = "urn:isbn:9781453219539"
        urns = [new_urn, isbn_urn]

        success, failure = Identifier.parse_urns(
            db_session, urns, allowed_types=[Identifier.OVERDRIVE_ID]
        )
        assert new_urn in success
        assert isbn_urn in failure

        success, failure = Identifier.parse_urns(
            db_session, urns, allowed_types=[
                Identifier.OVERDRIVE_ID, Identifier.ISBN
            ]
        )
        assert new_urn in success
        assert isbn_urn in success
        assert [] == failure

        # If the allowed_types is empty, no URNs can be looked up
        # -- this is most likely the caller's mistake.
        success, failure = Identifier.parse_urns(
            db_session, urns, allowed_types=[]
        )
        assert new_urn in failure
        assert isbn_urn in failure

    def test_parse_urn(self, db_session, create_identifier):
        """
        GIVEN: An URN
        WHEN:  Parsing the URN
        THEN:  An Identifier is created from the URN
        """
        # We can parse our custom URNs back into identifiers.
        identifier = create_identifier(db_session)
        new_identifier, _ = Identifier.parse_urn(db_session, identifier.urn)
        assert identifier == new_identifier

        # We can parse urn:isbn URNs into ISBN identifiers. ISBN-10s are
        # converted to ISBN-13s.
        identifier, _ = Identifier.for_foreign_id(db_session, Identifier.ISBN, "9781449358068")
        isbn_urn = "urn:isbn:1449358063"
        isbn_identifier, _ = Identifier.parse_urn(db_session, isbn_urn)
        assert Identifier.ISBN == isbn_identifier.type
        assert "9781449358068" == isbn_identifier.identifier

        isbn_urn = "urn:isbn:9781449358068"
        isbn_identifier2, _ = Identifier.parse_urn(db_session, isbn_urn)
        assert isbn_identifier2 == isbn_identifier

        # We can parse ordinary http: or https: URLs into URI
        # identifiers.
        http_identifier, _ = Identifier.parse_urn(db_session, "http://example.com")
        assert Identifier.URI == http_identifier.type
        assert "http://example.com" == http_identifier.identifier

        https_identifier, _ = Identifier.parse_urn(db_session, "https://example.com")
        assert Identifier.URI == https_identifier.type
        assert "https://example.com" == https_identifier.identifier

        # We can parse UUIDs.
        uuid_identifier, _ = Identifier.parse_urn(db_session, "urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952")
        assert Identifier.URI == uuid_identifier.type
        assert "urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952" == uuid_identifier.identifier

        # A URN we can't handle raises an exception.
        ftp_urn = "ftp://example.com"
        pytest.raises(ValueError, Identifier.parse_urn, db_session, ftp_urn)

        # An invalid ISBN raises an exception.
        pytest.raises(ValueError, Identifier.parse_urn, db_session, "urn:isbn:notanisbn")

        # Pass in None and you get None.
        assert Identifier.parse_urn(db_session, None) is None

    def test_parse_urn_must_support_license_pools(self, db_session):
        """
        GIVEN: An ISBN URN
        WHEN:  Parsing the ISBN URN
        THEN:  An Identifier.UnresolvableIdentifierException is raised
        """
        # We have no way of associating ISBNs with license pools.
        # If we try to parse an ISBN URN in a context that only accepts
        # URNs that can have associated license pools, we get an exception.
        isbn_urn = "urn:isbn:1449358063"

        # Delete all DataSources to force the exception
        rows = db_session.query(DataSource).all()
        for row in rows:
            db_session.delete(row)
        db_session.flush()

        pytest.raises(
            Identifier.UnresolvableIdentifierException,
            Identifier.parse_urn, db_session, isbn_urn,
            must_support_license_pools=True)

    def test_recursively_equivalent_identifier_ids(self, db_session, create_identifier):
        """
        GIVEN: Multiple Identifiers with various levels of equivalencies
        WHEN:  Querying for Identifiers within a threshold
        THEN:  Correct set of Identifiers are returned
        """
        identifier = create_identifier(db_session)
        data_source = DataSource.lookup(db_session, DataSource.MANUAL)

        strong_equivalent = create_identifier(db_session)
        identifier.equivalent_to(data_source, strong_equivalent, 0.9)

        weak_equivalent = create_identifier(db_session)
        identifier.equivalent_to(data_source, weak_equivalent, 0.2)

        level_2_equivalent = create_identifier(db_session)
        strong_equivalent.equivalent_to(data_source, level_2_equivalent, 0.5)

        level_3_equivalent = create_identifier(db_session)
        level_2_equivalent.equivalent_to(data_source, level_3_equivalent, 0.9)

        level_4_equivalent = create_identifier(db_session)
        level_3_equivalent.equivalent_to(data_source, level_4_equivalent, 0.6)

        _ = create_identifier(db_session)

        # With a low threshold and enough levels, we find all the identifiers.
        high_levels_low_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5,
            equivalent_identifier_threshold=0.1
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id], policy=high_levels_low_threshold
        )
        identifiers = set([
            identifier.id,
            strong_equivalent.id,
            weak_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id
        ])
        assert (identifiers == set(equivs[identifier.id]))

        # If we only look at one level, we don't find the level 2, 3, or 4 identifiers.
        one_level = PresentationCalculationPolicy(
            equivalent_identifier_levels=1,
            equivalent_identifier_threshold=0.1
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id], policy=one_level
        )
        identifiers = set([
                identifier.id,
                strong_equivalent.id,
                weak_equivalent.id
        ])
        assert (identifiers == set(equivs[identifier.id]))

        # If we raise the threshold, we don't find the weak identifier.
        one_level_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=1,
            equivalent_identifier_threshold=0.4
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id], policy=one_level_high_threshold
        )
        identifiers = set([
                identifier.id,
                strong_equivalent.id
        ])
        assert (identifiers == set(equivs[identifier.id]))

        # For deeper levels, the strength is the product of the strengths
        # of all the equivalencies in between the two identifiers.

        # In this example:
        # identifier - level_2_equivalent = 0.9 * 0.5 = 0.45
        # identifier - level_3_equivalent = 0.9 * 0.5 * 0.9 = 0.405
        # identifier - level_4_equivalent = 0.9 * 0.5 * 0.9 * 0.6 = 0.243

        # With a threshold of 0.5, level 2 and all subsequent levels are too weak.
        high_levels_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5,
            equivalent_identifier_threshold=0.5
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id], policy=high_levels_high_threshold
        )
        identifiers = set([
                identifier.id,
                strong_equivalent.id
        ])
        assert (identifiers == set(equivs[identifier.id]))

        # With a threshold of 0.25, level 2 is strong enough, but level
        # 4 is too weak.
        high_levels_lower_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5,
            equivalent_identifier_threshold=0.25
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id], policy=high_levels_lower_threshold
        )
        identifiers = set([
                identifier.id,
                strong_equivalent.id,
                level_2_equivalent.id,
                level_3_equivalent.id
        ])
        assert (identifiers == set(equivs[identifier.id]))

        # It also works if we start from other identifiers.
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [strong_equivalent.id], policy=high_levels_low_threshold
        )
        identifiers = set([
            identifier.id,
            strong_equivalent.id,
            weak_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id
        ])
        assert (identifiers == set(equivs[strong_equivalent.id]))

        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [level_4_equivalent.id], policy=high_levels_low_threshold
        )
        identifiers = set([
            identifier.id,
            strong_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id
        ])
        assert (identifiers == set(equivs[level_4_equivalent.id]))

        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [level_4_equivalent.id], policy=high_levels_high_threshold
        )
        identifiers = set([
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id
        ])
        assert (identifiers == set(equivs[level_4_equivalent.id]))

        # A chain of very strong equivalents can keep a high strength
        # even at deep levels. This wouldn't work if we changed the strength
        # threshold by level instead of accumulating a strength product.
        another_identifier = create_identifier(db_session)
        l2 = create_identifier(db_session)
        l3 = create_identifier(db_session)
        l4 = create_identifier(db_session)
        l2.equivalent_to(data_source, another_identifier, 1)
        l3.equivalent_to(data_source, l2, 1)
        l4.equivalent_to(data_source, l3, 0.9)
        high_levels_fairly_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=5,
            equivalent_identifier_threshold=0.89
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [another_identifier.id],
            high_levels_fairly_high_threshold
        )
        identifiers = set([
            another_identifier.id,
            l2.id,
            l3.id,
            l4.id
        ])
        assert (identifiers == set(equivs[another_identifier.id]))

        # We can look for multiple identifiers at once.
        two_levels_high_threshold = PresentationCalculationPolicy(
            equivalent_identifier_levels=2,
            equivalent_identifier_threshold=0.8
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id, level_3_equivalent.id],
            policy=two_levels_high_threshold
        )
        assert (
            set([identifier.id, strong_equivalent.id])
            ==
            set(equivs[identifier.id])
        )
        assert (
            set([level_2_equivalent.id, level_3_equivalent.id])
            ==
            set(equivs[level_3_equivalent.id])
        )

        # By setting a cutoff, you can say to look deep in the tree,
        # but stop looking as soon as you have a certain number of
        # equivalents.
        with_cutoff = PresentationCalculationPolicy(
            equivalent_identifier_levels=5,
            equivalent_identifier_threshold=0.1,
            equivalent_identifier_cutoff=1,
        )
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id], policy=with_cutoff
        )

        # The cutoff was set to 1, but we always go at least one level
        # deep, and that gives us three equivalent identifiers. We
        # don't artificially trim it back down to 1.
        assert 3 == len(equivs[identifier.id])

        # Increase the cutoff, and we get more identifiers.
        with_cutoff.equivalent_identifier_cutoff = 5
        equivs = Identifier.recursively_equivalent_identifier_ids(
            db_session, [identifier.id], policy=with_cutoff
        )
        assert len(equivs[identifier.id]) > 3

        # The query() method uses the same db function, but returns
        # equivalents for all identifiers together so it can be used
        # as a subquery.
        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, policy=high_levels_low_threshold
        )
        query = query.where(Identifier.id == identifier.id)
        results = db_session.execute(query)
        equivalent_ids = [r[0] for r in results]
        identifiers = set([
            identifier.id,
            strong_equivalent.id,
            weak_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id,
            level_4_equivalent.id
        ])
        assert (identifiers == set(equivalent_ids))

        query = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, policy=two_levels_high_threshold
        )
        query = query.where(Identifier.id.in_([identifier.id, level_3_equivalent.id]))
        results = db_session.execute(query)
        equivalent_ids = [r[0] for r in results]
        identifiers = set([
            identifier.id,
            strong_equivalent.id,
            level_2_equivalent.id,
            level_3_equivalent.id
        ])
        assert (identifiers == set(equivalent_ids))

    def test_licensed_through_collection(self, db_session, create_collection, create_edition, create_licensepool):
        """
        GIVEN: A LicensePool with an Edition and Collection
        WHEN:  Finding a LicensePool for an Identifier in a Collection
        THEN:  The correct LicensePool is returned
        """
        c1 = create_collection(db_session)
        c2 = create_collection(db_session)
        c3 = create_collection(db_session)

        edition, lp1 = create_edition(db_session, collection=c1, with_license_pool=True)
        lp2 = create_licensepool(db_session, collection=c2, edition=edition)

        identifier = lp1.identifier
        assert lp2.identifier == identifier

        assert lp1 == identifier.licensed_through_collection(c1)
        assert lp2 == identifier.licensed_through_collection(c2)
        assert None is identifier.licensed_through_collection(c3)

    def test_missing_coverage_from(self, db_session, create_coverage_record, init_datasource_and_genres):
        """
        GIVEN: Identifiers with various data sources
        WHEN:  Finding Identifiers of a given type which have no CoverageRecord from a given data source
        THEN:  Missing Identifiers are correctly found
        """
        gutenberg = DataSource.lookup(db_session, DataSource.GUTENBERG)
        oclc = DataSource.lookup(db_session, DataSource.OCLC)
        web = DataSource.lookup(db_session, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, _ = Edition.for_foreign_id(
            db_session, gutenberg, Identifier.GUTENBERG_ID, "1")

        g2, _ = Edition.for_foreign_id(
            db_session, gutenberg, Identifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        create_coverage_record(db_session, g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        create_coverage_record(db_session, g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, _ = Edition.for_foreign_id(
            db_session, web, Identifier.URI, "http://www.example.com/")

        # If we run missing_coverage_from we pick up the Gutenberg
        # record with no generic OCLC coverage. It doesn't pick up the
        # other Gutenberg record, it doesn't pick up the web record,
        # and it doesn't pick up the OCLC coverage for a specific
        # operation.
        [in_gutenberg_but_not_in_oclc] = Identifier.missing_coverage_from(
            db_session, [Identifier.GUTENBERG_ID], oclc).all()

        assert g2.primary_identifier == in_gutenberg_but_not_in_oclc

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.

        [has_generic_coverage_only] = Identifier.missing_coverage_from(
            db_session, [Identifier.GUTENBERG_ID], oclc, "some operation").all()
        assert g1.primary_identifier == has_generic_coverage_only

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Identifier.missing_coverage_from(
            db_session, [Identifier.URI], oclc).all()
        assert w.primary_identifier == in_web_but_not_in_oclc

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        assert [g1.primary_identifier.id, g2.primary_identifier.id] == sorted(
            [x.id for x in Identifier.missing_coverage_from(
                db_session, [Identifier.GUTENBERG_ID], web)])

    def test_missing_coverage_from_with_collection(
            self, db_session, create_collection, create_coverage_record, create_identifier):
        """
        GIVEN: A CoverageRecord for an Identifier in a Collection
        WHEN:  Finding Identifiers missing a CoverageRecord from a Collection
        THEN:  Identifiers are correctly found
        """
        gutenberg = DataSource.lookup(db_session, DataSource.GUTENBERG)
        identifier = create_identifier(db_session)
        collection1 = create_collection(db_session)
        collection2 = create_collection(db_session)
        create_coverage_record(db_session, identifier, gutenberg, collection=collection1)

        # The Identifier has coverage in collection 1.
        assert ([] ==
                Identifier.missing_coverage_from(
                db_session, [identifier.type], gutenberg, collection=collection1
                ).all())

        # It is missing coverage in collection 2.
        assert (
            [identifier] == Identifier.missing_coverage_from(
                db_session, [identifier.type], gutenberg, collection=collection2
            ).all()
        )

        # If no collection is specified, we look for a CoverageRecord
        # that also has no collection specified, and the Identifier is
        # not treated as covered.
        assert (
            [identifier] ==
            Identifier.missing_coverage_from(
                db_session, [identifier.type], gutenberg
            ).all()
        )

    def test_missing_coverage_from_with_cutoff_date(self, db_session, create_coverage_record):
        """
        GIVEN: An Edition with a primary Identifier
        WHEN:  Finding Identifiers missing a CoveraRecrod with a cutoff time
        THEN:  Identifiers are correctly found
        """
        gutenberg = DataSource.lookup(db_session, DataSource.GUTENBERG)
        oclc = DataSource.lookup(db_session, DataSource.OCLC)

        # Here's an Edition with a coverage record from OCLC classify.
        gutenberg, _ = Edition.for_foreign_id(
            db_session, gutenberg, Identifier.GUTENBERG_ID, "1")
        identifier = gutenberg.primary_identifier
        oclc = DataSource.lookup(db_session, DataSource.OCLC)
        coverage = create_coverage_record(db_session, gutenberg, oclc)

        # The CoverageRecord knows when the coverage was provided.
        timestamp = coverage.timestamp

        # If we ask for Identifiers that are missing coverage records
        # as of that time, we see nothing.
        assert (
            [] ==
            Identifier.missing_coverage_from(
                db_session, [identifier.type], oclc,
                count_as_missing_before=timestamp
            ).all())

        # But if we give a time one second later, the Identifier is
        # missing coverage.
        assert (
            [identifier] ==
            Identifier.missing_coverage_from(
                db_session, [identifier.type], oclc,
                count_as_missing_before=timestamp+datetime.timedelta(seconds=1)
            ).all())

    def test_opds_entry(self, db_session, create_coverage_record, create_identifier, init_datasource_and_genres):
        """
        GIVEN: An Identifier
        WHEN:  Creating an OPDS entry for this Identifier
        THEN:  An OPDS entry is correctly created
        """
        identifier = create_identifier(db_session)
        source = DataSource.lookup(db_session, DataSource.CONTENT_CAFE)

        identifier.add_link(
            Hyperlink.DESCRIPTION, 'http://description', source,
            media_type=Representation.TEXT_PLAIN, content='a book'
        )
        cover = identifier.add_link(
            Hyperlink.IMAGE, 'http://cover', source,
            media_type=Representation.JPEG_MEDIA_TYPE
        )[0]

        def get_entry_dict(entry):
            return feedparser.parse(etree.tostring(entry, encoding="unicode")).entries[0]

        # The entry includes the urn, description, and cover link.
        entry = get_entry_dict(identifier.opds_entry())
        assert identifier.urn == entry.id
        assert 'a book' == entry.summary
        [cover_link] = entry.links
        assert 'http://cover' == cover_link.href

        # The 'updated' time is set to the latest timestamp associated
        # with the Identifier.
        assert [] == identifier.coverage_records

        # This may be the time the cover image was mirrored.
        cover.resource.representation.set_as_mirrored('http://example.com')
        now = utc_now()
        cover.resource.representation.mirrored_at = now
        entry = get_entry_dict(identifier.opds_entry())
        assert AtomFeed._strftime(now) == entry.updated

        # Or it may be a timestamp on a coverage record associated
        # with the Identifier.

        # For whatever reason, this coverage record is missing its
        # timestamp. This indicates an error elsewhere, but it
        # doesn't crash the method we're testing.
        no_timestamp = create_coverage_record(
            db_session, identifier, source, operation="bad operation"
        )
        no_timestamp.timestamp = None

        # If a coverage record is dated after the cover image's mirror
        # time, That becomes the new updated time.
        record = create_coverage_record(db_session, identifier, source)
        the_future = now + datetime.timedelta(minutes=60)
        record.timestamp = the_future
        identifier.opds_entry()
        entry = get_entry_dict(identifier.opds_entry())
        assert AtomFeed._strftime(record.timestamp) == entry.updated

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
        assert 2 == len(entry.links)
        assert any(filter(lambda l: l.href == 'http://thumb', entry.links))
        # And the updated time has been changed accordingly.
        assert AtomFeed._strftime(even_later) == entry.updated

    @pytest.mark.parametrize(
        'identifier_type, identifier, title',
        [
            pytest.param('a', 'a', None, id='ascii_type_ascii_identifier_no_title'),
            pytest.param('a', 'ą', None, id='ascii_type_non_ascii_identifier_no_title'),
            pytest.param('ą', 'a', None, id='non_ascii_type_ascii_identifier_no_title'),
            pytest.param('ą', 'ą', None, id='non_ascii_type_non_ascii_identifier_no_title'),
            pytest.param('a', 'a', 'a', id='ascii_type_ascii_identifier_ascii_title'),
            pytest.param('a', 'ą', 'a', id='ascii_type_non_ascii_identifier_ascii_title'),
            pytest.param('ą', 'a', 'a', id='non_ascii_type_ascii_identifier_ascii_title'),
            pytest.param('ą', 'ą', 'a', id='non_ascii_type_non_ascii_identifier_ascii_title'),
            pytest.param('a', 'a', 'ą', id='ascii_type_ascii_identifier_non_ascii_title'),
            pytest.param('a', 'ą', 'ą', id='ascii_type_non_ascii_identifier_non_ascii_title'),
            pytest.param('ą', 'a', 'ą', id='non_ascii_type_ascii_identifier_non_ascii_title'),
            pytest.param('ą', 'ą', 'ą', id='non_ascii_type_non_ascii_identifier_non_ascii_title'),
        ],
    )
    def test_repr(self, identifier_type, identifier, title):
        """
        GIVEN: An identifier type, identifier, and title
        WHEN:  Calling repr on the Identifier
        THEN:  No exceptions are raised
        """
        # Arrange
        identifier = Identifier(type=identifier_type, identifier=identifier)

        if title:
            edition = create_autospec(spec=Edition)
            edition.title = PropertyMock(return_value=title)

            identifier.primarily_identifies = PropertyMock(return_value=[edition])

        # Act
        # NOTE: we are not interested in the result returned by repr,
        # we just want to make sure that repr doesn't throw any unexpected exceptions
        _ = repr(identifier)
