from nose.tools import eq_, set_trace

import datetime
import os

from model import (
    Edition,
    Identifier,
    Subject,
    Contributor,
    LicensePool,
    Representation,
    DeliveryMechanism,
)

from axis import (
    Axis360API,
    BibliographicParser,
)

from . import DatabaseTest

class TestAxis360API(DatabaseTest):

    def test_create_identifier_strings(self):
        identifier = self._identifier()
        values = Axis360API.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)

class TestParsers(object):

    def get_data(self, filename):
        path = os.path.join(
            os.path.split(__file__)[0], "files/axis/", filename)
        return open(path).read()

    def test_bibliographic_parser(self):
        """Make sure the bibliographic information gets properly
        collated in preparation for creating Edition objects.
        """
        data = self.get_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            False, True).process_all(data)

        # We didn't ask for availability information, so none was provided.
        eq_(None, av1)
        eq_(None, av2)

        eq_(u'Faith of My Fathers : A Family Memoir', bib1.title)
        eq_('eng', bib1.language)
        eq_(datetime.datetime(2000, 3, 7, 0, 0), bib1.published)

        eq_(u'Simon & Schuster', bib2.publisher)
        eq_(u'Pocket Books', bib2.imprint)

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        eq_(None, bib2.series)

        # Book #1 has a primary author and another author.
        [cont1, cont2] = bib1.contributors
        eq_("McCain, John", cont1.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], cont1.roles)

        eq_("Salter, Mark", cont2.sort_name)
        eq_([Contributor.AUTHOR_ROLE], cont2.roles)

        # Book #2 only has a primary author.
        [cont] = bib2.contributors
        eq_("Pollero, Rhonda", cont.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], cont.roles)

        axis_id, isbn = sorted(bib1.identifiers, key=lambda x: x.identifier)
        eq_(u'0003642860', axis_id.identifier)
        eq_(u'9780375504587', isbn.identifier)

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2.subjects, key = lambda x: x.identifier)
        eq_([Subject.BISAC, Subject.BISAC, Subject.BISAC, 
             Subject.AXIS_360_AUDIENCE], [x.type for x in subjects])
        general_fiction, women_sleuths, romantic_suspense, adult = [
            x.identifier for x in subjects]
        eq_(u'FICTION / General', general_fiction)
        eq_(u'FICTION / Mystery & Detective / Women Sleuths', women_sleuths)
        eq_(u'FICTION / Romance / Suspense', romantic_suspense)
        eq_(u'General Adult', adult)

        [format] = bib1.formats
        eq_(Representation.EPUB_MEDIA_TYPE, format.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, format.drm_scheme)

        # The second book is only available in 'Blio' format, which
        # we can't use.
        eq_([], bib2.formats)

    def test_parse_author_role(self):
        """Suffixes on author names are turned into roles."""
        author = "Dyssegaard, Elisabeth Kallick (TRN)"
        parse = BibliographicParser.parse_contributor
        c = parse(author)
        eq_("Dyssegaard, Elisabeth Kallick", c.sort_name)
        eq_([Contributor.TRANSLATOR_ROLE], c.roles)

        # A corporate author is given a normal author role.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False)
        eq_("Bob, Inc.", c.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], c.roles)

        c = parse(author, primary_author_found=True)
        eq_("Bob, Inc.", c.sort_name)
        eq_([Contributor.AUTHOR_ROLE], c.roles)

        # An unknown author type is given an unknown role
        author = "Eve, Mallory (ZZZ)"
        c = parse(author, primary_author_found=False)
        eq_("Eve, Mallory", c.sort_name)
        eq_([Contributor.UNKNOWN_ROLE], c.roles)

    def test_availability_parser(self):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = self.get_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            True, False).process_all(data)

        # We didn't ask for bibliographic information, so none was provided.
        eq_(None, bib1)
        eq_(None, bib2)

        eq_(datetime.datetime(2015, 5, 20, 14, 9, 8),
            av1.last_checked)
        eq_(9, av1.licenses_owned)
        eq_(9, av1.licenses_available)
        eq_(0, av1.patrons_in_hold_queue)
