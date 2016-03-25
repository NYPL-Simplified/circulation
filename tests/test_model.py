import datetime
import os
import sys
import site
import re
import tempfile

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)

from psycopg2.extras import NumericRange
from sqlalchemy.orm.exc import (
    NoResultFound,
)

from config import (
    Configuration, 
    temp_config,
)

from model import (
    CirculationEvent,
    Complaint,
    Contributor,
    CoverageRecord,
    Credential,
    DataSource,
    DeliveryMechanism,
    Genre,
    Hold,
    Hyperlink,
    LicensePool,
    Measurement,
    Patron,
    Representation,
    SessionManager,
    Subject,
    Timestamp,
    UnresolvedIdentifier,
    Work,
    WorkCoverageRecord,
    WorkGenre,
    Identifier,
    Edition,
    get_one_or_create,
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

class TestDataSource(DatabaseTest):

    # Commented out this test because the result kept changing and not
    # providing any value.
    #
    # def test_initial_data_sources(self):
    #     sources = [
    #         (x.name, x.offers_licenses, x.primary_identifier_type)
    #         for x in DataSource.well_known_sources(self._db)
    #     ]

    #     expect = [
    #         (DataSource.GUTENBERG, True, Identifier.GUTENBERG_ID),
    #         (DataSource.PROJECT_GITENBERG, True, Identifier.GUTENBERG_ID),
    #         (DataSource.OVERDRIVE, True, Identifier.OVERDRIVE_ID),
    #         (DataSource.THREEM, True, Identifier.THREEM_ID),
    #         (DataSource.AXIS_360, True, Identifier.AXIS_360_ID),

    #         (DataSource.OCLC, False, Identifier.OCLC_NUMBER),
    #         (DataSource.OCLC_LINKED_DATA, False, Identifier.OCLC_NUMBER),
    #         (DataSource.OPEN_LIBRARY, False, Identifier.OPEN_LIBRARY_ID),
    #         (DataSource.WEB, True, Identifier.URI),
    #         (DataSource.AMAZON, False, Identifier.ASIN),
    #         (DataSource.GUTENBERG_COVER_GENERATOR, False, Identifier.GUTENBERG_ID),
    #         (DataSource.GUTENBERG_EPUB_GENERATOR, False, Identifier.GUTENBERG_ID),
    #         (DataSource.CONTENT_CAFE, True, None),
    #         (DataSource.MANUAL, False, None),
    #         (DataSource.BIBLIOCOMMONS, False, Identifier.BIBLIOCOMMONS_ID),
    #         (DataSource.NYT, False, Identifier.ISBN),
    #         (DataSource.LIBRARY_STAFF, False, Identifier.ISBN),
    #         (DataSource.METADATA_WRANGLER, False, Identifier.URI),
    #         (DataSource.VIAF, False, None),
    #         (DataSource.ADOBE, False, None),
    #     ]
    #     eq_(set(sources), set(expect))

    def test_lookup(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        eq_(DataSource.GUTENBERG, gutenberg.name)
        eq_(True, gutenberg.offers_licenses)

    def test_lookup_returns_none_for_nonexistent_source(self):
        eq_(None, DataSource.lookup(
            self._db, "No such data source " + self._str))

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

        # A URN we can't handle raises an exception.
        ftp_urn = "ftp://example.com"
        assert_raises(ValueError, Identifier.parse_urn, self._db, ftp_urn)

        # An invalid ISBN raises an exception.
        assert_raises(ValueError, Identifier.parse_urn, self._db, "urn:isbn:notanisbn")

    def parse_urn_must_support_license_pools(self):
        # We have no way of associating ISBNs with license pools.
        # If we try to parse an ISBN URN in a context that only accepts
        # URNs that can have associated license pools, we get an exception.
        isbn_urn = "urn:isbn:1449358063"
        assert_raises(
            Identifier.UnresolvableIdentifierException, 
            Identifier.parse_urn, self._db, isbn_urn, 
            must_support_license_pools=True)

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

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            self._db, web, Identifier.URI, "http://www.foo.com/")

        # If we run missing_coverage_from with  picks up the Gutenberg record with no
        # coverage from OCLC. It doesn't pick up the other
        # Gutenberg record, and it doesn't pick up the web record.
        [in_gutenberg_but_not_in_oclc] = Identifier.missing_coverage_from(
            self._db, [Identifier.GUTENBERG_ID], oclc).all()

        eq_(g2.primary_identifier, in_gutenberg_but_not_in_oclc)

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

class TestUnresolvedIdentifier(DatabaseTest):

    def test_successful_register(self):
        identifier = self._identifier()
        unresolved, is_new = UnresolvedIdentifier.register(self._db, identifier)
        eq_(True, is_new)
        eq_(identifier, unresolved.identifier)
        eq_(202, unresolved.status)

    def test_register_fails_for_already_resolved_identifier(self):
        edition, pool = self._edition(with_license_pool=True)
        assert_raises(
            ValueError, UnresolvedIdentifier.register, self._db,
            pool.identifier)
    
    def test_register_fails_for_unresolvable_identifier(self):
        identifier = self._identifier(Identifier.ASIN)
        assert_raises(
            Identifier.UnresolvableIdentifierException,
            UnresolvedIdentifier.register, self._db, identifier)

    def test_set_attempt(self):
        i, ignore = self._unresolved_identifier()
        eq_(None, i.first_attempt)
        eq_(None, i.most_recent_attempt)
        
        now = datetime.datetime.utcnow()
        i.set_attempt()
        eq_(i.first_attempt, i.most_recent_attempt)
        first_attempt = i.first_attempt
        assert abs(first_attempt-now).seconds < 2

        future = now + datetime.timedelta(days=1)
        i.set_attempt(future)
        eq_(first_attempt, i.first_attempt)
        eq_(future, i.most_recent_attempt)

    def test_ready_to_process(self):
        now = datetime.datetime.utcnow()
        never_tried, ignore = self._unresolved_identifier()

        just_tried, ignore = self._unresolved_identifier()
        just_tried.set_attempt(now)
        just_tried.exception = 'Blah'

        tried_a_while_ago, ignore = self._unresolved_identifier()
        tried_a_while_ago.set_attempt(now-datetime.timedelta(days=7))
        tried_a_while_ago.exception = 'Blah'

        tried_a_long_time_ago, ignore = self._unresolved_identifier()
        tried_a_long_time_ago.set_attempt(now-datetime.timedelta(days=365))
        tried_a_long_time_ago.exception = 'Blah'

        # By default we get all UnresolvedIdentifiers that have never been
        # tried, and those tried more than a day ago.
        ready = UnresolvedIdentifier.ready_to_process(self._db).all()
        assert len(ready) == 3
        assert tried_a_while_ago in ready
        assert never_tried in ready
        assert tried_a_long_time_ago in ready

        # But we can customize the "a day ago" part by passing in a
        # custom timedelta.
        thirty_days = datetime.timedelta(days=30)
        ready = UnresolvedIdentifier.ready_to_process(
            self._db, retry_after=thirty_days
        ).all()
        assert len(ready) == 2
        assert never_tried in ready
        assert tried_a_long_time_ago in ready

class TestSubject(DatabaseTest):

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

    def test_lookup_by_viaf(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, name="Bob", viaf="foo")
        bob2, new = Contributor.lookup(self._db, name="Bob", viaf="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, viaf="foo"))

    def test_lookup_by_lc(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, name="Bob", lc="foo")
        bob2, new = Contributor.lookup(self._db, name="Bob", lc="bar")

        assert bob1 != bob2

        eq_((bob1, False), Contributor.lookup(self._db, lc="foo"))

    def test_lookup_by_name(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, name=u"Bob", lc=u"foo")
        bob2, new = Contributor.lookup(self._db, name=u"Bob", lc=u"bar")

        # Lookup by name finds both of them.
        bobs, new = Contributor.lookup(self._db, name=u"Bob")
        eq_(False, new)
        eq_(["Bob", "Bob"], [x.name for x in bobs])

    def test_create_by_lookup(self):
        [bob1], new = Contributor.lookup(self._db, name=u"Bob")
        eq_("Bob", bob1.name)
        eq_(True, new)

        [bob2], new = Contributor.lookup(self._db, name=u"Bob")
        eq_(bob1, bob2)
        eq_(False, new)

    def test_merge(self):

        # Here's Robert.
        [robert], ignore = Contributor.lookup(self._db, name=u"Robert")
        
        # Here's Bob.
        [bob], ignore = Contributor.lookup(self._db, name=u"Bob")
        bob.extra[u'foo'] = u'bar'
        bob.aliases = [u'Bobby']
        bob.viaf = u'viaf'
        bob.lc = u'lc'
        bob.display_name = u"Bob's display name"
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
        eq_([u'Bob', u'Bobby'], robert.aliases)

        # The extra information associated with Bob is now associated
        # with Robert.
        eq_(u'bar', robert.extra['foo'])

        eq_(u"viaf", robert.viaf)
        eq_(u"lc", robert.lc)
        eq_(u"Bobb", robert.family_name)
        eq_(u"Bob's display name", robert.display_name)
        eq_(u"Bob_(Person)", robert.wikipedia_name)

        # The standalone 'Bob' record has been removed from the database.
        eq_(
            [], 
            self._db.query(Contributor).filter(Contributor.name=="Bob").all())

        # Bob's book is now associated with 'Robert', not the standalone
        # 'Bob' record.
        eq_([robert], bobs_book.author_contributors)

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

class TestEdition(DatabaseTest):

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
        eq_(set([identifier.id]), record.equivalent_identifier_ids())

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

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            self._db, web, Identifier.URI, "http://www.foo.com/")

        # missing_coverage_from picks up the Gutenberg record with no
        # coverage from OCLC. It doesn't pick up the other
        # Gutenberg record, and it doesn't pick up the web record.
        [in_gutenberg_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, gutenberg, oclc).all()

        eq_(g2, in_gutenberg_but_not_in_oclc)

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, web, oclc).all()
        eq_(w, in_web_but_not_in_oclc)

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        eq_([g1.id, g2.id], sorted([x.id for x in Edition.missing_coverage_from(
            self._db, gutenberg, web)]))

    def test_recursive_edition_equivalence(self):

        gutenberg_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        open_library_source = DataSource.lookup(self._db, DataSource.OPEN_LIBRARY)
        web_source = DataSource.lookup(self._db, DataSource.WEB)

        # Here's a Edition for a Project Gutenberg text.
        gutenberg, ignore = Edition.for_foreign_id(
            self._db, gutenberg_source, Identifier.GUTENBERG_ID, "1")
        gutenberg.title = "Original Gutenberg text"

        # Here's a Edition for an Open Library text.
        open_library, ignore = Edition.for_foreign_id(
            self._db, open_library_source, Identifier.OPEN_LIBRARY_ID,
            "W1111")
        open_library.title = "Open Library record"

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
        gutenberg2, ignore = Edition.for_foreign_id(
            self._db, gutenberg_source, Identifier.GUTENBERG_ID, "2")
        gutenberg2.title = "Unrelated Gutenberg record."

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
        work = Work()
        work.editions.extend([gutenberg2])

        # Its set-of-all-editions contains only one record.
        eq_(1, work.all_editions().count())

        # If we add the other Gutenberg record to it, then its
        # set-of-all-editions is extended by that record, *plus*
        # all the Editions equivalent to that record.
        work.editions.extend([gutenberg])
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
        bob, ignore = self._contributor(name="Bitshifter, Bob")
        wr = self._edition(authors=bob.name)
        wr.calculate_presentation()
        eq_("Bob Bitshifter", wr.author)
        eq_("Bitshifter, Bob", wr.sort_author)

        bob.display_name="Bob A. Bitshifter"
        wr.calculate_presentation()
        eq_("Bob A. Bitshifter", wr.author)
        eq_("Bitshifter, Bob", wr.sort_author)

        kelly, ignore = self._contributor(name="Accumulator, Kelly")
        wr.add_contributor(kelly, Contributor.AUTHOR_ROLE)
        wr.calculate_presentation()
        eq_("Kelly Accumulator, Bob A. Bitshifter", wr.author)
        eq_("Accumulator, Kelly ; Bitshifter, Bob", wr.sort_author)

    def test_calculate_evaluate_summary_quality_with_privileged_data_source(self):
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
            self._db, ids, overdrive)
        eq_(overdrive_resource, champ2)
        eq_([overdrive_resource], resources2)

        # If we say that some other data source is privileged, and
        # there are no descriptions from that data source, a
        # head-to-head evaluation is performed, and OCLC Linked Data
        # wins.
        threem = DataSource.lookup(self._db, DataSource.THREEM)
        champ3, resources3 = Identifier.evaluate_summary_quality(
            self._db, ids, threem)
        eq_(set([overdrive_resource, oclc_resource]), set(resources3))
        eq_(oclc_resource, champ3)
        

    def test_calculate_presentation_cover(self):
        # TODO: Verify that a cover will be used even if it's some
        # distance away along the identifier-equivalence line.

        # TODO: Verify that a nearby cover takes precedence over a
        # faraway cover.
        pass

    def test_better_primary_edition_than(self):

        generic, p1 = self._edition(with_license_pool=True)
        generic2, p2 = self._edition(with_license_pool=True)

        def better(x,y):
            return x.better_primary_edition_than(y)

        # Something is better than nothing.
        eq_(True, better(generic, None))

        # A license pool beats no license pool.
        no_license_pool = self._edition(with_license_pool=False)
        eq_(True, better(generic, no_license_pool))
        eq_(False, better(no_license_pool, generic))

        # An open access pool beats non-open access.
        open_access, ignore = self._edition(with_open_access_download=True)
        eq_(True, better(open_access, generic))

        # TODO: An open access book from a high-quality source beats one
        # from a low-quality source.

        # A high Gutenberg number beats a low Gutenberg number.
        open_access2, ignore = self._edition(with_open_access_download=True)
        eq_(True, better(open_access2, open_access))

        # More licenses beats fewer licenses
        p1.open_access = False
        p2.open_access = False
        p1.licenses_owned = 1
        p2.licenses_owned = 2
        eq_(True, better(generic2, generic))

        p1.licenses_owned = 3
        eq_(False, better(generic2, generic))

        # More licenses available beats fewer
        p1.licenses_owned = p2.licenses_owned = 5
        p1.licenses_available = 2
        p2.licenses_available = 1
        eq_(True, better(generic, generic2))
        p2.licenses_available = 3
        eq_(False, better(generic, generic2))

        # Fewer people on hold beats more
        p1.licenses_available = 0
        p1.patrons_in_hold_queue = 1
        p2.licenses_available = 0
        p2.patrons_in_hold_queue = 2
        eq_(True, better(generic, generic2))
        p1.patrons_in_hold_queue = 3
        eq_(False, better(generic, generic2))

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

class TestLicensePool(DatabaseTest):

    def test_for_foreign_id(self):
        """Verify we can get a LicensePool for a data source and an 
        appropriate work identifier."""
        now = datetime.datetime.utcnow()
        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541")
        assert (pool.availability_time - now).total_seconds() < 2
        eq_(True, was_new)
        eq_(DataSource.GUTENBERG, pool.data_source.name)
        eq_(Identifier.GUTENBERG_ID, pool.identifier.type)
        eq_("541", pool.identifier.identifier)        

    def test_no_license_pool_for_data_source_that_offers_no_licenses(self):
        """OCLC doesn't offer licenses. It only provides metadata. We can get
        a Edition for OCLC's view of a book, but we cannot get a
        LicensePool for OCLC's view of a book.
        """
        assert_raises_regexp(
            ValueError, 
            'Data source "OCLC Classify" does not offer licenses',
            LicensePool.for_foreign_id,
            self._db, DataSource.OCLC, "1015", 
            Identifier.OCLC_WORK)

    def test_no_license_pool_for_non_primary_identifier(self):
        """Overdrive offers licenses, but to get an Overdrive license pool for
        a book you must identify the book by Overdrive's primary
        identifier, not some other kind of identifier.
        """
        assert_raises_regexp(
            ValueError, 
            "License pools for data source 'Overdrive' are keyed to identifier type 'Overdrive ID' \(not 'ISBN', which was provided\)",
            LicensePool.for_foreign_id,
            self._db, DataSource.OVERDRIVE, Identifier.ISBN, "{1-2-3}")

    def test_with_no_work(self):
        p1, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1")

        p2, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, "2")

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

    def test_set_rights_status(self):
        edition, pool = self._edition(with_license_pool=True)
        uri = "http://foo"
        name = "bar"
        status = pool.set_rights_status(uri, name)
        eq_(status, pool.rights_status)
        eq_(uri, status.uri)
        eq_(name, status.name)

        status2 = pool.set_rights_status(uri)
        eq_(status, status2)

        uri2 = "http://baz"
        status3 = pool.set_rights_status(uri2)
        assert status != status3
        eq_(uri2, status3.uri)
        eq_(None, status3.name)

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
            source, pool
        )
        oa2 = link2.resource

        # And let's add a link that's not an open-access download.
        url = self._url
        image, new = pool.identifier.add_link(
            Hyperlink.IMAGE, url, source, pool
        )
        self._db.commit()

        # Only the two open-access download links show up.
        eq_(set([oa1, oa2]), set(pool.open_access_links))

class TestWork(DatabaseTest):

    def test_calculate_presentation(self):

        gutenberg_source = DataSource.GUTENBERG

        [bob], ignore = Contributor.lookup(self._db, u"Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()

        edition1, pool1 = self._edition(
            gutenberg_source, Identifier.GUTENBERG_ID, True, authors=[])
        edition1.title = u"The 1st Title"
        edition1.title = u"The 1st Subtitle"
        edition1.add_contributor(bob, Contributor.AUTHOR_ROLE)

        edition2, pool2 = self._edition(
            gutenberg_source, Identifier.GUTENBERG_ID, True, authors=[])
        edition2.title = u"The 2nd Title"
        edition2.subtitle = u"The 2nd Subtitle"
        edition2.add_contributor(bob, Contributor.AUTHOR_ROLE)
        [alice], ignore = Contributor.lookup(self._db, u"Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()
        edition2.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition3, pool3 = self._edition(
            gutenberg_source, Identifier.GUTENBERG_ID, True, authors=[])
        edition3.title = u"The 2nd Title"
        edition3.subtitle = u"The 2nd Subtitle"
        edition3.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition3.add_contributor(alice, Contributor.AUTHOR_ROLE)

        work = self._work(primary_edition=edition2)
        for i in edition1, edition3:
            work.editions.append(i)
        for p in pool1, pool2, pool3:
            work.license_pools.append(p)

        # This Work starts out with a single CoverageRecord reflecting the
        # work done to generate its initial OPDS entry.
        [record] = work.coverage_records
        eq_(WorkCoverageRecord.GENERATE_OPDS_OPERATION, record.operation)

        work.last_update_time = None
        work.presentation_ready = True
        index = DummyExternalSearchIndex()
        work.calculate_presentation(search_index_client=index)

        # The title of the Work is the title of its primary work
        # record.
        eq_("The 2nd Title", work.title)
        eq_("The 2nd Subtitle", work.subtitle)

        # The author of the Work is the author of its primary work
        # record.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

        # The index has been updated with a document.
        [[args, doc]] = index.docs.items()
        eq_(doc, work.to_search_document())

        # The Work now has a complete set of WorkCoverageRecords
        # associated with it, reflecting all the operations that
        # occured as part of calculate_presentation().
        records = work.coverage_records
        expect = set([
            WorkCoverageRecord.CHOOSE_EDITION_OPERATION,
            WorkCoverageRecord.CLASSIFY_OPERATION,
            WorkCoverageRecord.SUMMARY_OPERATION,
            WorkCoverageRecord.QUALITY_OPERATION,
            WorkCoverageRecord.GENERATE_OPDS_OPERATION,
            WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION,
        ])
        eq_(expect, set([x.operation for x in records]))

    def test_set_presentation_ready(self):
        work = self._work(with_license_pool=True)
        primary = work.primary_edition
        work.set_presentation_ready_based_on_content()
        eq_(False, work.presentation_ready)
        
        # This work is not presentation ready because it has no
        # cover. If we record the fact that we tried and failed to
        # find a cover, it will be considered presentation ready.
        work.primary_edition.no_known_cover = True
        work.set_presentation_ready_based_on_content()
        eq_(True, work.presentation_ready)

        # It would also work to add a cover, of course.
        work.primary_edition.cover_thumbnail_url = "http://example.com/"
        work.primary_edition.no_known_cover = False
        work.set_presentation_ready_based_on_content()
        eq_(True, work.presentation_ready)

        # Remove the title, and the work stops being presentation
        # ready.
        primary.title = None
        work.set_presentation_ready_based_on_content()
        eq_(False, work.presentation_ready)        
        primary.title = u"foo"
        work.set_presentation_ready_based_on_content()
        eq_(True, work.presentation_ready)        

        # Remove the fiction setting, and the work stops being
        # presentation ready.
        work.fiction = None
        work.set_presentation_ready_based_on_content()
        eq_(False, work.presentation_ready)        

        work.fiction = False
        work.set_presentation_ready_based_on_content()
        eq_(True, work.presentation_ready)        

        # Remove the author's presentation string, and the work stops
        # being presentation ready.
        primary.author = None
        work.set_presentation_ready_based_on_content()
        eq_(False, work.presentation_ready)        
        primary.author = u"foo"
        work.set_presentation_ready_based_on_content()
        eq_(True, work.presentation_ready)        

        # TODO: there are some other things you can do to stop a work
        # being presentation ready, and they should all be tested.

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

    def test_with_complaint(self):
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)

        work1 = self._work(
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        work1_complaint1 = self._complaint(
            work1.license_pools[0],
            type1,
            "work1 complaint1 source",
            "work1 complaint1 detail")
        work1_complaint2 = self._complaint(
            work1.license_pools[0],
            type1,
            "work1 complaint2 source",
            "work1 complaint2 detail")
        work1_complaint3 = self._complaint(
            work1.license_pools[0],
            type2,
            "work1 complaint3 source",
            "work1 complaint3 detail")
        work2 = self._work(
            "nonfiction work with complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)
        work2_complaint1 = self._complaint(
            work2.license_pools[0],
            type2,
            "work2 complaint1 source",
            "work2 complaint1 detail")
        work3 = self._work(
            "fiction work without complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        work4 = self._work(
            "nonfiction work without complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)

        results = Work.with_complaint(self._db).all()
        (works, types, counts) = zip(*results)

        eq_(3, len(results))
        eq_(work1.id, results[0][0].id)
        eq_(type1, results[0][1])
        eq_(2, results[0][2])
        eq_(work1.id, results[1][0].id)
        eq_(type2, results[1][1])
        eq_(1, results[1][2])
        eq_(work2.id, results[2][0].id)
        eq_(type2, results[2][1])
        eq_(1, results[2][2])
        eq_((type1, type2, type2), types)


class TestCirculationEvent(DatabaseTest):

    def _event_data(self, **kwargs):
        for k, default in (
                ("source", DataSource.OVERDRIVE),
                ("id_type", Identifier.OVERDRIVE_ID),
                ("start", datetime.datetime.utcnow()),
                ("type", CirculationEvent.LICENSE_ADD),
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

        license_pool, was_new = LicensePool.for_foreign_id(
            _db, source, identifier_type, foreign_id)

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
        data = self._event_data(
            source=DataSource.OVERDRIVE,
            id="{1-2-3}",
            type=CirculationEvent.LICENSE_ADD,
            old_value=0,
            delta=2,
            new_value=2,
        )
        
        # Turn it into an event and see what happens.
        event, ignore = self.from_dict(data)

        # The event is associated with the correct data source.
        eq_(DataSource.OVERDRIVE, event.license_pool.data_source.name)

        # The event identifies a work by its ID plus the data source's
        # primary identifier.
        eq_(Identifier.OVERDRIVE_ID, event.license_pool.identifier.type)
        eq_("{1-2-3}", event.license_pool.identifier.identifier)

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

class TestWorkSimilarity(DatabaseTest):

    def test_work_is_similar_to_itself(self):
        wr = self._edition()
        eq_(1, wr.similarity_to(wr))

class TestWorkConsolidation(DatabaseTest):

    # Versions of Work and Edition instrumented to bypass the
    # normal similarity comparison process.

    def setup(self):
        super(TestWorkConsolidation, self).setup()
        # Replace the complex implementations of similarity_to with 
        # much simpler versions that let us simply say which objects 
        # are to be considered similar.
        def similarity_to(self, other):
            if other in getattr(self, 'similar', []):
                return 1
            return 0
        self.old_w = Work.similarity_to
        self.old_wr = Edition.similarity_to
        Work.similarity_to = similarity_to
        Edition.similarity_to = similarity_to

    def teardown(self):
        Work.similarity_to = self.old_w
        Edition.similarity_to = self.old_wr
        super(TestWorkConsolidation, self).teardown()

    def test_calculate_work_matches_based_on_permanent_work_id(self):
        # Here are two Editions with the same permanent work ID, 
        # since they have the same title/author.
        edition1, ignore = self._edition(with_license_pool=True)
        edition2, ignore = self._edition(
            title=edition1.title, authors=edition1.author,
            with_license_pool=True)

        # Calling calculate_work() on the first edition creates a Work.
        work1, created = edition1.license_pool.calculate_work()
        eq_(created, True)

        # Calling calculate_work() on the second edition associated
        # the second edition with the first work.
        work2, created = edition2.license_pool.calculate_work()
        eq_(created, False)

        eq_(work1, work2)

        eq_(set([edition1, edition2]), set(work1.editions))

        # Note that this works even though the Edition somehow got a
        # Work without having a title or author.

    def test_calculate_work_for_licensepool_creates_new_work(self):

        # This work record is unique to the existing work.
        edition1, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1")
        edition1.title = self._str
        edition1.author = self._str
        preexisting_work = Work()
        preexisting_work.editions = [edition1]

        # This work record is unique to the new LicensePool
        edition2, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "3")
        edition2.title = self._str
        edition2.author = self._str
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "3")

        # Call calculate_work(), and a new Work is created.
        work, created = pool.calculate_work()
        eq_(True, created)
        assert work != preexisting_work
        eq_(edition2, pool.edition)

    def test_calculate_work_does_nothing_unless_edition_has_title_and_author(self):
        edition, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1")
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1")
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.title = u"foo"
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.add_contributor(u"bar", Contributor.PRIMARY_AUTHOR_ROLE)
        edition.calculate_presentation()
        work, created = pool.calculate_work()
        eq_(True, created)

        # Even before the forthcoming commit, the edition is clearly
        # the primary for the work.
        eq_(work, edition.work)
        eq_(True, edition.is_primary_for_work)
        eq_(edition, work.primary_edition)

        # But without this commit, the join for the .primary_edition
        # won't succeed and work.title won't work.
        self._db.commit()

        # Ta-da!
        eq_(edition, work.primary_edition)
        eq_(u"foo", work.title)
        eq_(u"bar", work.author)

    def test_calculate_work_can_be_forced_to_work_with_no_author(self):
        edition, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1")
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1")
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.title = u"foo"
        work, created = pool.calculate_work(even_if_no_author=True)
        eq_(True, created)
        self._db.commit()
        eq_(edition, work.primary_edition)
        eq_(u"foo", work.title)
        eq_(Edition.UNKNOWN_AUTHOR, work.author)

    def test_calculate_work_for_new_work(self):
        # TODO: This test doesn't actually test
        # anything. calculate_work() is too complicated and needs to
        # be refactored.

        # This work record is unique to the existing work.
        edition1, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1")

        # This work record is shared by the existing work and the new
        # LicensePool.
        edition2, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "2")

        # These work records are unique to the new LicensePool.

        edition3, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "3")

        edition4, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "4")

        # Make edition4's primary identifier equivalent to edition3's and edition1's
        # primaries.
        data_source = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        for make_equivalent in edition3, edition1:
            edition4.primary_identifier.equivalent_to(
                data_source, make_equivalent.primary_identifier, 1)
        preexisting_work = self._work(primary_edition=edition1)
        preexisting_work.editions.append(edition2)

        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "4")
        self._db.commit()

        pool.calculate_work()

    def test_merge_into(self):

        # Here's a work with a license pool and two work records.
        edition_1a, pool_1a = self._edition(
            DataSource.OCLC, Identifier.OCLC_WORK, True)
        edition_1b, ignore = Edition.for_foreign_id(
            self._db, DataSource.OCLC, Identifier.OCLC_WORK, "W2")

        work1 = Work()
        work1.license_pools = [pool_1a]
        work1.editions = [edition_1a, edition_1b]
        work1.set_primary_edition()

        # Here's a work with two license pools and one work record
        edition_2a, pool_2a = self._edition(
            DataSource.GUTENBERG, Identifier.GUTENBERG_ID, True)
        edition_2a.title = "The only title in this whole test."
        pool_2b = self._licensepool(edition_2a, 
                                    data_source_name=DataSource.OCLC)

        work2 = Work()
        work2.license_pools = [pool_2a, pool_2b]
        work2.editions = [edition_2a]
        work2.set_primary_edition()

        self._db.commit()

        # This attempt to merge the two work records will fail because
        # they don't meet the similarity threshold.
        work2.merge_into(work1, similarity_threshold=1)
        eq_(None, work2.was_merged_into)

        # This attempt will succeed because we lower the similarity
        # threshold.
        work2.merge_into(work1, similarity_threshold=0)
        eq_(work1, work2.was_merged_into)

        # The merged Work no longer has any work records or license
        # pools.
        eq_([], work2.editions)
        eq_([], work2.license_pools)

        # The remaining Work has all three license pools.
        for p in pool_1a, pool_2a, pool_2b:
            assert p in work1.license_pools

        # It has all three work records.
        for w in edition_1a, edition_1b, edition_2a:
            assert w in work1.editions

    def test_open_access_pools_grouped_together(self):

        # We have four editions with exactly the same title and author.
        # Two of them are open-access, two are not.
        title = "The Only Title"
        author = "Single Author"
        ed1, open1 = self._edition(title=title, authors=author, with_license_pool=True)
        ed2, open2 = self._edition(title=title, authors=author, with_license_pool=True)
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

        # If pool.work is None but pool.edition.work is valid, we 
        # use that.
        loan.license_pool = pool
        pool.work = None
        eq_(pool.edition.work, loan.work)

        # If that's also None, we're helpless.
        pool.edition.work = None
        eq_(None, loan.work)


class TestHold(DatabaseTest):

    def test_on_hold_to(self):
        now = datetime.datetime.utcnow()
        later = now + datetime.timedelta(days=1)
        patron = self._patron()
        edition = self._edition()
        pool = self._licensepool(edition)

        with temp_config() as config:
            config['policies'] = {
                Configuration.HOLD_POLICY : Configuration.HOLD_POLICY_ALLOW
            }
            hold, is_new = pool.on_hold_to(patron, now, later, 4)
            eq_(True, is_new)
            eq_(now, hold.start)
            eq_(None, hold.end)
            eq_(4, hold.position)

            # Now update the position to 0. It's the patron's turn
            # to check out the book.
            hold, is_new = pool.on_hold_to(patron, now, later, 0)
            eq_(False, is_new)
            eq_(now, hold.start)
            # The patron has until `hold.end` to actually check out the book.
            eq_(later, hold.end)
            eq_(0, hold.position)

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


class TestHyperlink(DatabaseTest):

    def test_add_link(self):
        edition, pool = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        data_source = pool.data_source
        hyperlink, is_new = pool.add_link(
            Hyperlink.DESCRIPTION, "http://foo.com/", data_source, 
            "text/plain", "The content")
        eq_(True, is_new)
        rep = hyperlink.resource.representation
        eq_("text/plain", rep.media_type)
        eq_("The content", rep.content)
        eq_(Hyperlink.DESCRIPTION, hyperlink.rel)
        eq_(pool, hyperlink.license_pool)
        eq_(identifier, hyperlink.identifier)

    def test_add_link_fails_if_license_pool_and_identifier_dont_match(self):
        edition, pool = self._edition(with_license_pool=True)
        data_source = pool.data_source
        identifier = self._identifier()
        assert_raises_regexp(
            ValueError, re.compile("License pool is associated with .*, not .*!"),
            identifier.add_link,
            Hyperlink.DESCRIPTION, "http://foo.com/", data_source, 
            pool, "text/plain", "The content")
        

class TestRepresentation(DatabaseTest):

    def test_normalized_content_path(self):
        eq_("baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/foo/bar"))

        eq_("baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/foo/bar/"))

        eq_("/foo/bar/baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/blah/blah/"))

    def test_set_fetched_content(self):
        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content("some text")
        eq_("some text", representation.content_fh().read())

    def test_set_fetched_content_file_on_disk(self):
        filename = "set_fetched_content_file_on_disk.txt"
        path = os.path.join(self.DBInfo.tmp_data_dir, filename)
        open(path, "w").write("some text")

        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(None, filename)
        fh = representation.content_fh()
        eq_("some text", fh.read())

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

class TestScaleRepresentation(DatabaseTest):

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
        full_rep.mirror_url = mirror
        full_rep.set_as_mirrored()

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(None, edition.cover_thumbnail_url)

        # Now scale the cover.
        thumbnail, ignore = self._representation()
        thumbnail.thumbnail_of = full_rep
        thumbnail.mirror_url = thumbnail_mirror
        thumbnail.set_as_mirrored()
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
        full_rep.mirror_url = mirror
        full_rep.set_as_mirrored()

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(mirror, edition.cover_thumbnail_url)

    def sample_cover_path(self, name):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "covers")
        sample_cover_path = os.path.join(resource_path, name)
        return sample_cover_path

    def sample_cover_representation(self, name):
        sample_cover_path = self.sample_cover_path(name)
        return self._representation(
            media_type="image/png", content=open(sample_cover_path).read())[0]

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
        eq_(url, thumbnail.mirror_url)
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
        thumbnail.mirrored_at = datetime.datetime.utcnow()

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


class TestDeliveryMechanism(DatabaseTest):

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

        # When we call lookup_by_temporary_token, the token is automatically
        # expired and we cannot use it anymore.
        new_token = Credential.lookup_by_temporary_token(
            self._db, data_source, token.type, token.credential)
        eq_(new_token, token)        
        assert new_token.expires < now

        new_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential)
        eq_(None, new_token)

        new_token = Credential.lookup_by_temporary_token(
            self._db, data_source, token.type, token.credential)
        eq_(None, new_token)
 
        # A token with no expiration date is treated as expired.
        token.expires = None
        self._db.commit()
        no_expiration_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential)
        eq_(None, no_expiration_token)

        no_expiration_token = Credential.lookup_by_token(
            self._db, data_source, token.type, token.credential, True)
        eq_(token, no_expiration_token)

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

    def test_cannot_look_up_nonexistent_token(self):
        data_source = DataSource.lookup(self._db, DataSource.ADOBE)
        new_token = Credential.lookup_by_token(
            self._db, data_source, "no such type", "no such credential")
        eq_(None, new_token)

class TestPatron(DatabaseTest):

    def test_external_type_regular_expression(self):
        patron = self._patron("234")
        patron.authorization_identifier = "A123"
        key = Patron.EXTERNAL_TYPE_REGULAR_EXPRESSION
        with temp_config() as config:

            config[Configuration.POLICIES] = {}

            config[Configuration.POLICIES][key] = None
            eq_(None, patron.external_type)

            config[Configuration.POLICIES][key] = "([A-Z])"
            eq_("A", patron.external_type)
            patron._external_type = None

            config[Configuration.POLICIES][key] = "([0-9]$)"
            eq_("3", patron.external_type)
            patron._external_type = None

            config[Configuration.POLICIES][key] = "A"
            eq_(None, patron.external_type)
            patron._external_type = None

            config[Configuration.POLICIES][key] = "(not a valid regexp"
            assert_raises(TypeError, lambda x: patron.external_type)
            patron._external_type = None


class TestCoverageRecord(DatabaseTest):

    def test_lookup(self):
        source = DataSource.lookup(self._db, DataSource.OCLC)
        edition = self._edition()
        operation = 'foo'
        record = self._coverage_record(edition, source, operation)

        lookup = CoverageRecord.lookup(edition, source, operation)
        eq_(lookup, record)

        lookup = CoverageRecord.lookup(edition, source)
        eq_(None, lookup)

        lookup = CoverageRecord.lookup(edition.primary_identifier, source, operation)
        eq_(lookup, record)

        lookup = CoverageRecord.lookup(edition.primary_identifier, source)
        eq_(None, lookup)

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


class TestComplaint(DatabaseTest):

    def setup(self):
        super(TestComplaint, self).setup()
        self.edition, self.pool = self._edition(with_license_pool=True)
        self.type = "http://librarysimplified.org/terms/problem/wrong-genre"

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
