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
)
from core.model import (
    DataSource,
    Edition,
    LicensePool,
)
from core.opds_import import (
    MetadataWranglerOPDSLookup,
    OPDSImporter,
)
from core.util.http import RemoteIntegrationException

from coverage import MetadataWranglerCoverageProvider


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
