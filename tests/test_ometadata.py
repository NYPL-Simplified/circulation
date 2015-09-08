from StringIO import StringIO
from nose.tools import (
    eq_,
    set_trace,
)
import pkgutil
import csv

from ..metadata import (
    CSVFormatError,
    CSVMetadataImporter,
)

from ..model import (
    DataSource,
)

class TestMetadataImporter(object):

    def test_parse(self):
        data = StringIO(
            pkgutil.get_data("tests", "files/csv/staff_picks.csv")
        )
        reader = csv.DictReader(data)
        importer = CSVMetadataImporter(
            DataSource.LIBRARY_STAFF,
        )
        generator = importer.to_metadata(reader)
        metadatas = list(generator)
        set_trace()
