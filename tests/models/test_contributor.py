# encoding: utf-8
import pytest
from ...model.contributor import Contributor
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier


class TestContributor:
    def test_marc_code_for_every_role_constant(self):
        """We have determined the MARC Role Code for every role
        that's important enough we gave it a constant in the Contributor
        class.
        """
        # GIVEN:
        # WHEN:
        # THEN:
        for constant, value in list(Contributor.__dict__.items()):
            if not constant.endswith("_ROLE"):
                # Not a constant
                continue
            assert value in Contributor.MARC_ROLE_CODES

    def test_lookup_by_viaf(self, db_session):
        """
        GIVEN:
        WHEN:
        THEN:
        """

        # Two contributors named Bob.
        bob1, _ = Contributor.lookup(db_session, sort_name="Bob", viaf="foo")
        bob2, _ = Contributor.lookup(db_session, sort_name="Bob", viaf="bar")

        assert bob1 != bob2

        assert (bob1, False) == Contributor.lookup(db_session, viaf="foo")

    def test_lookup_by_lc(self, db_session):
        """
        GIVEN:
        WHEN:
        THEN:
        """

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(db_session, sort_name="Bob", lc="foo")
        bob2, new = Contributor.lookup(db_session, sort_name="Bob", lc="bar")

        assert bob1 != bob2

        assert (bob1, False) == Contributor.lookup(db_session, lc="foo")

    def test_lookup_by_viaf_interchangeable(self, db_session, create_contributor):
        """
        GIVEN:
        WHEN:
        THEN:
        """

        # Two contributors with the same lc. This shouldn't happen, but
        # the reason it shouldn't happen is these two people are the same
        # person, so lookup() should just pick one and go with it.
        bob1 = create_contributor(db_session, sort_name="Bob", lc="foo")
        bob2 = create_contributor(db_session)
        bob2.sort_name = "Bob"
        bob2.lc = "foo"
        #db_session.commit()
        assert bob1 != bob2
        [some_bob], new = Contributor.lookup(db_session, sort_name="Bob", lc="foo")

        assert False == new
        assert some_bob in (bob1, bob2)

    def test_create_by_lookup(self, db_session):
        """
        GIVEN:
        WHEN:
        THEN:
        """

        [bob1], new = Contributor.lookup(db_session, sort_name="Bob")
        assert "Bob" == bob1.sort_name
        assert True == new

        [bob2], new = Contributor.lookup(db_session, sort_name="Bob")
        assert bob1 == bob2
        assert False == new

    def test_merge(self, db_session):
        """
        GIVEN:
        WHEN:
        THEN:
        """

        # Here's Robert.
        [robert], _ = Contributor.lookup(db_session, sort_name="Robert")

        # Here's Bob.
        [bob], _ = Contributor.lookup(db_session, sort_name="Jones, Bob")
        bob.extra['foo'] = 'bar'
        bob.aliases = ['Bobby']
        bob.viaf = 'viaf'
        bob.lc = 'lc'
        bob.display_name = "Bob Jones"
        bob.family_name = "Bobb"
        bob.wikipedia_name = "Bob_(Person)"

        # Each is a contributor to a Edition.
        data_source = DataSource.lookup(db_session, DataSource.GUTENBERG)

        roberts_book, ignore = Edition.for_foreign_id(
            db_session, data_source, Identifier.GUTENBERG_ID, "1")
        roberts_book.add_contributor(robert, Contributor.AUTHOR_ROLE)

        bobs_book, ignore = Edition.for_foreign_id(
            db_session, data_source, Identifier.GUTENBERG_ID, "10")
        bobs_book.add_contributor(bob, Contributor.AUTHOR_ROLE)

        # In a shocking turn of events, it transpires that "Bob" and
        # "Robert" are the same person. We merge "Bob" into Robert
        # thusly:
        bob.merge_into(robert)

        # 'Bob' is now listed as an alias for Robert, as is Bob's
        # alias.
        assert ['Jones, Bob', 'Bobby'] == robert.aliases

        # The extra information associated with Bob is now associated
        # with Robert.
        assert 'bar' == robert.extra['foo']

        assert "viaf" == robert.viaf
        assert "lc" == robert.lc
        assert "Bobb" == robert.family_name
        assert "Bob Jones" == robert.display_name
        assert "Robert" == robert.sort_name
        assert "Bob_(Person)" == robert.wikipedia_name

        # The standalone 'Bob' record has been removed from the database.
        assert (
            [] ==
            db_session.query(Contributor).filter(Contributor.sort_name=="Bob").all())

        # Bob's book is now associated with 'Robert', not the standalone
        # 'Bob' record.
        assert [robert] == bobs_book.author_contributors

        # confirm the sort_name is propagated, if not already set in the destination contributor
        robert.sort_name = None
        [bob], _ = Contributor.lookup(db_session, sort_name="Jones, Bob")
        bob.merge_into(robert)
        assert "Jones, Bob" == robert.sort_name

    @pytest.mark.parametrize(
        'in_name,out_family,out_display,default_display_name',
        [
            # Pass in a default display name and it will always be used.
            ("Jones, Bob", "Jones", "Sally Smith", "Sally Smith"),
            # Corporate names are untouched and get no family name.
            ("Bob's Books", None, "Bob's Books", None),
            ("Bob's Books, Inc.", None, "Bob's Books, Inc.", None),
            ("Little, Brown &amp; Co.", None, "Little, Brown & Co.", None),
            ("Philadelphia Broad Street Church (Philadelphia, Pa.)", None, "Philadelphia Broad Street Church", None),
            # Dates and other gibberish after a name is removed.
            ("Twain, Mark, 1855-1910", "Twain", "Mark Twain", None),
            ("Twain, Mark, ???-1910", "Twain", "Mark Twain", None),
            ("Twain, Mark, circ. 1900", "Twain", "Mark Twain", None),
            ("Twain, Mark, !@#!@", "Twain", "Mark Twain", None),
            ("Coolbrith, Ina D. 1842?-1928", "Coolbrith", "Ina D. Coolbrith", None),
            ("Caesar, Julius, 1st cent.", "Caesar", "Julius Caesar", None),
            ("Arrian, 2nd cent.", "Arrian", "Arrian", None),
            ("Hafiz, 14th cent.", "Hafiz", "Hafiz", None),
            ("Hormel, Bob 1950?-", "Hormel", "Bob Hormel", None),
            ("Holland, Henry 1583-1650? Monumenta sepulchraria Sancti Pauli", "Holland", "Henry Holland", None),
            # Suffixes stay on the end, except for "Mrs.", which goes to the front.
            ("Twain, Mark, Jr.", "Twain", "Mark Twain, Jr.", None),
            ("House, Gregory, M.D.", "House", "Gregory House, M.D.", None),
            ("Twain, Mark, Mrs.", "Twain", "Mrs. Mark Twain", None),
            ("Twain, Mark, Mrs", "Twain", "Mrs Mark Twain", None),
            # The easy case.
            ("Twain, Mark", "Twain", "Mark Twain", None),
            ("Geering, R. G.", "Geering", "R. G. Geering", None),

        ],
    )
    def test_default_names(self, in_name, out_family, out_display, default_display_name):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        f, d = Contributor._default_names(in_name, default_display_name)
        assert f == out_family
        assert d == out_display

    def test_sort_name(self, db_session, create_contributor):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        bob = create_contributor(db_session, sort_name=None)
        assert None == bob.sort_name

        bob = create_contributor(db_session, sort_name="Bob Bitshifter")
        bob.sort_name = None
        assert None == bob.sort_name

        bob = create_contributor(db_session, sort_name="Bob Bitshifter")
        assert "Bitshifter, Bob" == bob.sort_name

        # test that human name parser doesn't die badly on foreign names
        bob= create_contributor(db_session, sort_name="Боб  Битшифтер")
        assert "Битшифтер, Боб" == bob.sort_name
