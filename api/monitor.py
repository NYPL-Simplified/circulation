import datetime
from nose.tools import set_trace
import os
import sys
import csv
from sqlalchemy import or_
import logging
from config import Configuration
from core.monitor import (
    EditionSweepMonitor,
    IdentifierSweepMonitor,
    WorkSweepMonitor,
)
from core.model import (
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Work,
)
from core.opds import OPDSFeed
from core.opds_import import (
    SimplifiedOPDSLookup,
    OPDSImporter,
)
from core.external_search import (
    ExternalSearchIndex,
)

class SearchIndexUpdateMonitor(WorkSweepMonitor):
    """Make sure the search index is up-to-date for every work.
    """

    def __init__(self, _db, batch_size=100, interval_seconds=3600*24, works_index=None):
        super(SearchIndexUpdateMonitor, self).__init__(
            _db, 
            "Index Update Monitor %s" % works_index, 
            interval_seconds)
        self.batch_size = batch_size
        self.search_index_client = ExternalSearchIndex(works_index=works_index)

    def work_query(self):
        return self._db.query(Work).filter(Work.presentation_ready==True)

    def process_batch(self, batch):
        # TODO: Perfect opportunity for a bulk upload.
        highest_id = 0
        for work in batch:
            if work.id > highest_id:
                highest_id = work.id
            work.update_external_index(self.search_index_client)
            if not work.title:
                logging.warn(
                    "Work %d is presentation-ready but has no title?" % work.id
                )
        return highest_id


class UpdateOpenAccessURL(EditionSweepMonitor):
    """Set Edition.open_access_full_url for all Gutenberg works."""

    def __init__(self, _db, batch_size=100, interval_seconds=600):
        super(UpdateOpenAccessURL, self).__init__(
            _db, 
            "Update open access URLs for Gutenberg editions", 
            interval_seconds)
        self.batch_size = batch_size
    
    def edition_query(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        return self._db.query(Edition).filter(
            Edition.data_source==gutenberg)

    def process_edition(self, edition):
        edition.set_open_access_link()

