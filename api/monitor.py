import datetime
from nose.tools import set_trace
import os
import sys
import csv
from sqlalchemy import or_
import logging
from config import Configuration

from core.monitor import (
    CollectionMonitor,
    EditionSweepMonitor,
    WorkSweepMonitor,
)
from core.model import (
    DataSource,
    Edition,
    LicensePool,
    WorkCoverageRecord,
)
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    OPDSImporter,
)
from core.external_search import ExternalSearchIndex
from core.util.http import RemoteIntegrationException

from coverage import MetadataWranglerCoverageProvider

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
                _db, works_index=index_name
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
            for work in successes:
                WorkCoverageRecord.add_for(
                    work, WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
                )
            # Start work on the next batch.
            return batch[-1].id
        else:
            # We're done.
            return 0


class MetadataWranglerCollectionUpdateMonitor(CollectionMonitor):
    """Retrieves updated metadata from the Metadata Wrangler"""

    SERVICE_NAME = "Metadata Wrangler Collection Updates"
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    def __init__(self, _db, collection, lookup=None):
        super(MetadataWranglerCollectionUpdateMonitor, self).__init__(
            _db, collection
        )
        self.lookup = lookup or MetadataWranglerOPDSLookup.from_config(
            self._db, collection=collection
        )

    def run_once(self, start, cutoff):
        if not self.lookup.authenticated:
            self.keep_timestamp = False
            return

        try:
            response = self.lookup.updates(start)
            self.lookup.check_content_type(response)
        except RemoteIntegrationException as e:
            self.log.error(
                "Error getting updates for %r: %s",
                self.collection, e.debug_message
            )
            self.keep_timestamp = False
            return

        importer = OPDSImporter(
            self._db, self.collection,
            data_source_name=DataSource.METADATA_WRANGLER,
            metadata_client=self.lookup,
            map_from_collection=True,
        )
        importer.import_from_feed(response.text)
