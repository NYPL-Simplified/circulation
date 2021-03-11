# encoding: utf-8
from ...testing import DatabaseTest
from ...model import get_one_or_create
from ...model.contributor import Contributor
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier

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

        assert (bob1, False) == Contributor.lookup(self._db, viaf="foo")

    def test_lookup_by_lc(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name="Bob", lc="foo")
        bob2, new = Contributor.lookup(self._db, sort_name="Bob", lc="bar")

        assert bob1 != bob2

        assert (bob1, False) == Contributor.lookup(self._db, lc="foo")

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
        assert False == new
        assert some_bob in (bob1, bob2)

    def test_lookup_by_name(self):

        # Two contributors named Bob.
        bob1, new = Contributor.lookup(self._db, sort_name=u"Bob", lc=u"foo")
        bob2, new = Contributor.lookup(self._db, sort_name=u"Bob", lc=u"bar")

        # Lookup by name finds both of them.
        bobs, new = Contributor.lookup(self._db, sort_name=u"Bob")
        assert False == new
        assert ["Bob", "Bob"] == [x.sort_name for x in bobs]

    def test_create_by_lookup(self):
        [bob1], new = Contributor.lookup(self._db, sort_name=u"Bob")
        assert "Bob" == bob1.sort_name
        assert True == new

        [bob2], new = Contributor.lookup(self._db, sort_name=u"Bob")
        assert bob1 == bob2
        assert False == new

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
        assert [u'Jones, Bob', u'Bobby'] == robert.aliases

        # The extra information associated with Bob is now associated
        # with Robert.
        assert u'bar' == robert.extra['foo']

        assert u"viaf" == robert.viaf
        assert u"lc" == robert.lc
        assert u"Bobb" == robert.family_name
        assert u"Bob Jones" == robert.display_name
        assert u"Robert" == robert.sort_name
        assert u"Bob_(Person)" == robert.wikipedia_name

        # The standalone 'Bob' record has been removed from the database.
        assert (
            [] ==
            self._db.query(Contributor).filter(Contributor.sort_name=="Bob").all())

        # Bob's book is now associated with 'Robert', not the standalone
        # 'Bob' record.
        assert [robert] == bobs_book.author_contributors

        # confirm the sort_name is propagated, if not already set in the destination contributor
        robert.sort_name = None
        [bob], ignore = Contributor.lookup(self._db, sort_name=u"Jones, Bob")
        bob.merge_into(robert)
        assert u"Jones, Bob" == robert.sort_name



    def _names(self, in_name, out_family, out_display,
               default_display_name=None):
        f, d = Contributor._default_names(in_name, default_display_name)
        assert f == out_family
        assert d == out_display

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
        assert None == bob.sort_name

        bob, ignore = self._contributor(sort_name="Bob Bitshifter")
        bob.sort_name = None
        assert None == bob.sort_name

        bob, ignore = self._contributor(sort_name="Bob Bitshifter")
        assert "Bitshifter, Bob" == bob.sort_name

        bob, ignore = self._contributor(sort_name="Bitshifter, Bob")
        assert "Bitshifter, Bob" == bob.sort_name

        # test that human name parser doesn't die badly on foreign names
        bob, ignore = self._contributor(sort_name=u"Боб  Битшифтер")
        assert u"Битшифтер, Боб" == bob.sort_name
