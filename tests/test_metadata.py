from StringIO import StringIO
from nose.tools import (
    eq_,
    set_trace,
)
import pkgutil
import csv

from metadata_layer import (
    CSVFormatError,
    CSVMetadataImporter,
)

import os
from model import (
    DataSource,
    Identifier,
)

class TestMetadataImporter(object):

    def test_parse(self):
        base_path = os.path.split(__file__)[0]
        path = os.path.join(
            base_path, "files/csv/staff_picks_small.csv")
        reader = csv.DictReader(open(path))
        importer = CSVMetadataImporter(
            DataSource.LIBRARY_STAFF,
        )
        generator = importer.to_metadata(reader)
        m1, m2, m3 = list(generator)

        eq_(u"Horrorst\xf6r", m1.title)
        eq_("Grady Hendrix", m1.contributors[0].display_name)
        eq_("Martin Jensen", m2.contributors[0].display_name)

        # Let's check out the identifiers we found.

        # The first book has an Overdrive ID
        [overdrive] = m1.identifiers
        eq_(Identifier.OVERDRIVE_ID, overdrive.type)
        eq_('504BA8F6-FF4E-4B57-896E-F1A50CFFCA0C', overdrive.identifier)
        eq_(1, overdrive.weight)

        # The second book has no ID at all.
        eq_([], m2.identifiers)

        # The third book has both a 3M ID and an Overdrive ID.
        overdrive, threem = sorted(m3.identifiers, key=lambda x: x.identifier)

        eq_(Identifier.OVERDRIVE_ID, overdrive.type)
        eq_('eae60d41-e0b8-4f9d-90b5-cbc43d433c2f', overdrive.identifier)
        eq_(1, overdrive.weight)

        eq_(Identifier.THREEM_ID, threem.type)
        eq_('eswhyz9', threem.identifier)
        eq_(1, threem.weight)

        # Now let's check out subjects.
        eq_(
            [
                ('schema:typicalAgeRange', u'Adult', 100),
                ('tag', u'Character Driven', 100),
                ('tag', u'Historical', 100), 
                ('tag', u'Nail-Biters', 100),
                ('tag', u'Setting Driven', 100)
            ],
            [(x.type, x.identifier, x.weight) 
             for x in sorted(m2.subjects, key=lambda x: x.identifier)]
        )
