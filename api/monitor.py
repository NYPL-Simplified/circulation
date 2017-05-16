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
    WorkSweepMonitor,
)
from core.model import (
    DataSource,
    Edition,
    LicensePool,
)
from core.external_search import ExternalSearchIndex


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

class SearchIndexMonitor(WorkSweepMonitor):
    """Make sure the search index is up-to-date for every work."""

    def __init__(self, _db, index_name=None, index_client=None, batch_size=500, **kwargs):
        if index_client:
            # This would only happen during a test.
            self.search_index_client = index_client
        else:
            self.search_index_client = ExternalSearchIndex(
                works_index=index_name
            )

        index_name = self.search_index_client.works_index
        super(SearchIndexMonitor, self).__init__(
            _db,
            "Search index update (%s)" % index_name,
            batch_size=batch_size,
            **kwargs
        )

    def process_batch(self, batch):
        """Update the search ndex for a set of Works."""

        successes, failures = self.search_index_client.bulk_update(batch)

        for work, message in failures:
            self.log.error("Failed to update search index for %s: %s" % (work, message))

