#!/usr/bin/env python
"""Move integration details from the Configuration file into the
database as ExternalIntegrations
"""
import os
import sys
import logging
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from config import Configuration
from external_search import ExternalSearchIndex
from model import (
    ExternalIntegration as EI,
    Library,
    get_one_or_create,
    production_session,
)

log = logging.getLogger(name="Core configuration import")

def log_import(integration_or_setting, is_new):
    if is_new:
        log.info("CREATED: %r" % integration_or_setting)
    else:
        log.info("%r already exists." % integration_or_setting)

try:
    Configuration.load()
    _db = production_session()
    LIBRARIES = _db.query(Library).all()

    # Import CDN configuration.
    cdn_conf = Configuration.integration(u'CDN')
    if cdn_conf:

        cdn_goals = {
            'book_covers' : EI.BOOK_COVERS_GOAL,
            'open_access_books' : EI.OA_CONTENT_GOAL,
            'opds' : EI.OPDS_FEED_GOAL
        }

        for k, v in cdn_conf.items():
            if not k in cdn_goals:
                log.warn('No ExternalIntegration goal for "%s" CDN' % k)
                continue

            goal = cdn_goals.get(k)
            cdn, is_new = get_one_or_create(
                _db, EI, protocol=EI.CDN, goal=goal,
                url=unicode(v)
            )
            log_import(cdn, is_new)

    # Import Elasticsearch configuration.
    elasticsearch_conf = Configuration.integration(u'Elasticsearch')
    if elasticsearch_conf:
        url = elasticsearch_conf.get('url')
        works_index = elasticsearch_conf.get(ExternalSearchIndex.WORKS_INDEX_KEY)

        integration, is_new = get_one_or_create(
            _db, EI, protocol=EI.ELASTICSEARCH, goal=EI.SEARCH_GOAL
        )

        if url:
            integration.url = unicode(url)
        if works_index:
            integration.set_setting(
                ExternalSearchIndex.WORKS_INDEX_KEY, works_index
            )

        log_import(integration, is_new)

    # Import S3 configuration.
    s3_conf = Configuration.integration('S3')
    if s3_conf:
        username = s3_conf.get('access_key')
        password = s3_conf.get('secret_key')
        del s3_conf['access_key']
        del s3_conf['secret_key']

        s3_goals = {
            'book_covers_bucket' : EI.BOOK_COVERS_GOAL,
            'open_access_content_bucket' : EI.OA_CONTENT_GOAL,
            'static_feed_bucket' : EI.OPDS_FEED_GOAL,
        }

        for k, v in s3_conf.items():
            if not k in s3_goals:
                log.warn('No ExternalIntegration goal for "%s" S3 bucket' % k)
                continue

            goal = s3_goals.get(k)
            integration, is_new = get_one_or_create(
                _db, EI, protocol=EI.S3, goal=goal,
                username=unicode(username),
                password=unicode(password),
                url=unicode(v)
            )
            log_import(integration, is_new)

finally:
    _db.commit()
    _db.close()
