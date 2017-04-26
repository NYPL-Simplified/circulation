#!/usr/bin/env python
"""Find CachedFeeds that are associated with a LicensePool and
associate them with the overarching Work instead.
"""
import os
import sys
import logging
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    CachedFeed,
    production_session,
)

log = logging.getLogger('Migrate CachedFeeds license_pools')
_db = production_session()

feeds = _db.query(CachedFeed).join(CachedFeed.license_pool)\
        .filter(CachedFeed.license_pool_id.isnot(None)).all()
set_trace()
log.info("%d affected CachedFeeds found", len(feeds))
for feed in feeds:
    feed.work = feed.license_pool.work
_db.commit()

remaining_feeds = _db.query(CachedFeed).filter(
    CachedFeed.license_pool_id.isnot(None),
    CachedFeed.work_id.is_(None)).all()
set_trace()
if remaining_feeds:
    logging.error(
        "ERROR: %d affected CachedFeeds remaining without associated Work",
        len(remaining_feeds)
    )
else:
    logging.info("All affected CachedFeeds given associated Work")
