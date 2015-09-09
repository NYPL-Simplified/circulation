from StringIO import StringIO
from nose.tools import (
    eq_,
    set_trace,
)
import pkgutil
import csv

from metadata import (
    CSVFormatError,
    CSVMetadataImporter,
)

import os
from model import (
    DataSource,
)

class TestMetadataImporter(object):

    def test_parse(self):
        path = os.path.join(
            os.path.split(__file__)[0], "files/csv/staff_picks.csv")
        reader = csv.DictReader(open(path))
        importer = CSVMetadataImporter(
            DataSource.LIBRARY_STAFF,
        )
        generator = importer.to_metadata(reader)
        metadatas = list(generator)

