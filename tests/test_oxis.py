from nose.tools import eq_, set_trace

import datetime
import pkgutil

from model import (
    Edition,
    Identifier,
    Subject,
    Contributor,
    LicensePool,
)

from axis import (
    BibliographicParser,
)

class TestParsers(object):

    def test_bibliographic_parser(self):
        """Make sure the bibliographic information gets properly
        collated in preparation for creating Edition objects.
        """

        data = pkgutil.get_data("tests", "files/axis/tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            False, True).process_all(data)

        # We didn't ask for availability information, so none was provided.
        eq_(None, av1)
        eq_(None, av2)

        eq_(u'Faith of My Fathers : A Family Memoir', bib1[Edition.title])
        eq_('eng', bib1[Edition.language])
        eq_(datetime.datetime(2000, 3, 7, 0, 0), bib1[Edition.published])

        eq_(u'Simon & Schuster', bib2[Edition.publisher])
        eq_(u'Pocket Books', bib2[Edition.imprint])

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        eq_(None, bib2[Edition.series])

        # Book #1 has a primary author and another author.
        cont1 = bib1[Contributor]
        eq_(["McCain, John"], cont1[Contributor.PRIMARY_AUTHOR_ROLE])
        eq_(["Salter, Mark"], cont1[Contributor.AUTHOR_ROLE])

        # Book #2 only has a primary author.
        cont2 = bib2[Contributor]
        eq_(["Pollero, Rhonda"], cont2[Contributor.PRIMARY_AUTHOR_ROLE])
        eq_([], cont2[Contributor.AUTHOR_ROLE])

        axis_id, isbn = sorted(
            bib1[Identifier], key = lambda x: x[Identifier.type])
        eq_(u'0003642860', axis_id[Identifier.identifier])
        eq_(u'9780375504587', isbn[Identifier.identifier])

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2[Subject], key = lambda x: x[Subject.identifier])
        eq_([Subject.BISAC, Subject.BISAC, Subject.BISAC, 
             Subject.AXIS_360_AUDIENCE], [x[Subject.type] for x in subjects])
        general_fiction, women_sleuths, romantic_suspense, adult = [
            x[Subject.identifier] for x in subjects]
        eq_(u'FICTION / General', general_fiction)
        eq_(u'FICTION / Mystery & Detective / Women Sleuths', women_sleuths)
        eq_(u'FICTION / Romance / Suspense', romantic_suspense)
        eq_(u'General Adult', adult)

    def test_availability_parser(self):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = pkgutil.get_data("tests", "files/axis/tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            True, False).process_all(data)

        # We didn't ask for bibliographic information, so none was provided.
        eq_(None, bib1)
        eq_(None, bib2)

        eq_(datetime.datetime(2015, 5, 20, 2, 9, 8),
            av1[LicensePool.last_checked])
        eq_(9, av1[LicensePool.licenses_owned])
        eq_(9, av1[LicensePool.licenses_available])
        eq_(0, av1[LicensePool.patrons_in_hold_queue])
