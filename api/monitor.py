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


class SearchIndexMonitor(WorkSweepMonitor):
    """Make sure the search index is up-to-date for every work."""
    SERVICE_NAME = "Search index update"
    DEFAULT_BATCH_SIZE = 500
    
    def __init__(self, _db, collection, index_name=None, index_client=None,
                 **kwargs):
        super(SearchIndexMonitor, self).__init__(_db, collection, **kwargs)
        
        if index_client:
            # This would only happen during a test.
            self.search_index_client = index_client
        else:
            self.search_index_client = ExternalSearchIndex(
                works_index=index_name
            )

        index_name = self.search_index_client.works_index
        # We got a generic service name. Replace it with a more
        # specific one.
        self.service_name = "Search index update (%s)" % index_name

    def process_batch(self, offset):
        """Update the search index for a set of Works."""
        batch = self.fetch_batch(offset).all()
        if batch:
            successes, failures = self.search_index_client.bulk_update(batch)

            for work, message in failures:
                self.log.error(
                    "Failed to update search index for %s: %s", work, message
                )
            # Start work on the next batch.
            return batch[-1].id
        else:
            # We're done.
            return 0
