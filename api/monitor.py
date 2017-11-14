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

        entries = list()
        queue = [None]
        seen_links = set()

        new_timestamp = None
        while queue:
            url = queue.pop()
            if url in seen_links:
                continue
            next_links, editions, possible_new_timestamp = self.import_one_feed(
                start, url
            )
            if not new_timestamp or (
                    possible_new_timestamp 
                    and possible_new_timestamp > new_timestamp
            ):
                new_timestamp = possible_new_timestamp
            seen_links.add(url)

            # If we didn't import any editions, then don't add any of
            # the 'next' links found in this feed to the queue.
            if editions:
                for link in next_links:
                    if link not in seen_links:
                        queue.append(link)
        return new_timestamp

    def import_one_feed(self, timestamp, url):
        response = self.get_response(timestamp, url=url)
        if not response:
            return [], [], timestamp

        # TODO: We parse the feed three different times here.

        # Import the metadata
        raw_feed = response.text
        (editions, licensepools,
         works, errors) = self.importer.import_from_feed(raw_feed)

        # Get last update times to set the timestamp.
        update_dates = self.importer.extract_last_update_dates(raw_feed)
        update_dates = [d[1] for d in update_dates]
        if update_dates:
            # We know that every entry updated before the earliest
            # date in the OPDS feed has been handled already, or the
            # server would have sent it and we would have an even
            # earlier date.
            timestamp = min(update_dates)

        # Add all links with rel='next' to the queue.
        next_links = self.importer.extract_next_links(raw_feed)
        return next_links, editions, timestamp

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
