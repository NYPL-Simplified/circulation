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
)
from core.model import (
    DataSource,
    Edition,
    LicensePool,
)

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

