import csv
import datetime
import feedparser
import logging
import os
import sys
from nose.tools import set_trace

from sqlalchemy import or_

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

from config import Configuration
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
        self.importer = OPDSImporter(
            self._db, self.collection,
            data_source_name=DataSource.METADATA_WRANGLER,
            metadata_client=self.lookup, map_from_collection=True,
        )

    def run_once(self, start, cutoff):
        if not self.lookup.authenticated:
            self.keep_timestamp = False
            return

        initial = True
        entries = list()
        next_links = list()
        timestamp = start

        while initial or (entries or next_links):
            url = None
            if next_links:
                url = next_links[0]
            response = self.get_response(timestamp, url=url)
            if not response:
                break

            # Import the metadata
            raw_feed = response.text
            self.importer.import_from_feed(raw_feed)

            # Get last update times to set the timestamp.
            update_dates = self.importer.extract_last_update_dates(raw_feed)
            update_dates = [d[1] for d in update_dates]
            if timestamp:
                # Including the existing timestamp, in case it's the latest.
                update_dates.append(timestamp)
            timestamp = max(update_dates)

            next_links = self.importer.extract_next_links(raw_feed)
            entries = feedparser.parse(raw_feed).entries
            if initial:
                initial = False

        return timestamp

    def get_response(self, timestamp, url=None):
        try:
            if not url:
                response = self.lookup.updates(timestamp)
            else:
                response = self.lookup._get(url)
            self.lookup.check_content_type(response)
            return response
        except RemoteIntegrationException as e:
            self.log.error(
                "Error getting updates for %r: %s",
                self.collection, e.debug_message
            )
            self.keep_timestamp = False
            return None
