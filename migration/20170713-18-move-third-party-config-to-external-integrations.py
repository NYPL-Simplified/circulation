#!/usr/bin/env python
"""Move integration details from the Configuration file into the
database as ExternalIntegrations
"""
import os
import sys
import logging


bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from config import Configuration
from external_search import ExternalSearchIndex
from model import (
    ExternalIntegration as EI,
    production_session,
)

from s3 import S3Uploader

log = logging.getLogger(name="Core configuration import")

def log_import(integration_or_setting):
    log.info("CREATED: %r" % integration_or_setting)

try:
    Configuration.load()
    _db = production_session()

    # Import CDN configuration.
    cdn_conf = Configuration.integration('CDN')

    if cdn_conf and isinstance(cdn_conf, dict):
        for k, v in list(cdn_conf.items()):
            cdn = EI(protocol=EI.CDN, goal=EI.CDN_GOAL)
            _db.add(cdn)
            cdn.url = str(v)
            cdn.setting(Configuration.CDN_MIRRORED_DOMAIN_KEY).value = str(k)
            log_import(cdn)

    # Import Elasticsearch configuration.
    elasticsearch_conf = Configuration.integration('Elasticsearch')
    if elasticsearch_conf:
        url = elasticsearch_conf.get('url')
        works_index = elasticsearch_conf.get(ExternalSearchIndex.WORKS_INDEX_KEY)

        integration = EI(protocol=EI.ELASTICSEARCH, goal=EI.SEARCH_GOAL)
        _db.add(integration)

        if url:
            integration.url = str(url)
        if works_index:
            integration.set_setting(
                ExternalSearchIndex.WORKS_INDEX_KEY, works_index
            )

        log_import(integration)

    # Import S3 configuration.
    s3_conf = Configuration.integration('S3')
    if s3_conf:
        username = s3_conf.get('access_key')
        password = s3_conf.get('secret_key')
        del s3_conf['access_key']
        del s3_conf['secret_key']

        integration = EI(protocol=EI.S3, goal=EI.STORAGE_GOAL)
        _db.add(integration)
        integration.username = username
        integration.password = password

        S3_SETTINGS = [
            S3Uploader.BOOK_COVERS_BUCKET_KEY,
            S3Uploader.OA_CONTENT_BUCKET_KEY,
        ]
        for k, v in list(s3_conf.items()):
            if not k in S3_SETTINGS:
                log.warn('No ExternalIntegration goal for "%s" S3 bucket' % k)
                continue
            integration.setting(str(k)).value = str(v)

        log_import(integration)

finally:
    _db.commit()
    _db.close()
