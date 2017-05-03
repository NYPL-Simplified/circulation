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
from model import (
    ExternalIntegration,
    get_one_or_create,
    production_session,
)

log = logging.getLogger(name="Configuration import")

def log_import(service_name):
    log.info("Importing configuration for %s" % service_name)

try:
    Configuration.load()
    _db = production_session()

    # Import CDN configuration.
    cdn_conf = Configuration.integration(Configuration.CDN_INTEGRATION)
    if cdn_conf:
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.CDN
        )
        [service.set_setting(k, v) for k, v in cdn_conf.items()]

    # Import Circulation Manager configuration.
    circulation_manager_conf = Configuration.integration(
        Configuration.CIRCULATION_MANAGER_INTEGRATION
    )
    if circulation_manager_conf:
        url = circulation_manager_conf.get(Configuration.URL)

        log_import(ExternalIntegration.CIRCULATION_MANAGER)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.CIRCULATION_MANAGER
        )
        service.url = unicode(url)

    # Import Content Server configuration.
    content_server_conf = Configuration.integration(
        Configuration.CONTENT_SERVER_INTEGRATION
    )

    if content_server_conf:
        url = content_server_conf.get(Configuration.URL)

        log_import(ExternalIntegration.CONTENT_SERVER)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.CONTENT_SERVER
        )
        service.url = unicode(url)

    # Import Elasticsearch configuration.
    elasticsearch_conf = Configuration.integration(
        Configuration.ELASTICSEARCH_INTEGRATION
    )
    if elasticsearch_conf:
        url = elasticsearch_conf.get(Configuration.URL)
        works_index = elasticsearch_conf.get(Configuration.ELASTICSEARCH_INDEX_KEY)

        log_import(ExternalIntegration.ELASTICSEARCH)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.ELASTICSEARCH
        )
        service.url = unicode(url)
        service.set_setting(u'works_index', works_index)

    # Import Metadata Wrangler configuration.
    metadata_wrangler_conf = Configuration.integration(
        Configuration.METADATA_WRANGLER_INTEGRATION
    )

    if metadata_wrangler_conf:
        url = metadata_wrangler_conf.get(Configuration.URL)
        username = metadata_wrangler_conf.get(Configuration.METADATA_WRANGLER_CLIENT_ID)
        password = metadata_wrangler_conf.get(Configuration.METADATA_WRANGLER_CLIENT_SECRET)

        log_import(ExternalIntegration.METADATA_WRANGLER)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.METADATA_WRANGLER
        )

        service.url = unicode(url)
        service.username = username
        service.password = password

    # Import NoveList Select configuration.
    novelist = Configuration.integration(Configuration.NOVELIST_INTEGRATION)
    if novelist:
        username = novelist.get(Configuration.NOVELIST_PROFILE)
        password = novelist.get(Configuration.NOVELIST_PASSWORD)

        log_import(ExternalIntegration.NOVELIST)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.NOVELIST
        )
        service.username = username
        service.password = password


    # Import NYT configuration.
    nyt_conf = Configuration.integration(Configuration.NYT_INTEGRATION)
    if nyt_conf:
        password = nyt_conf.get(Configuration.NYT_BEST_SELLERS_API_KEY)

        log_import(ExternalIntegration.NYT)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.NYT
        )
        service.password = unicode(password)

    # Import S3 configuration.
    s3_conf = Configuration.integration(Configuration.S3_INTEGRATION)
    if s3_conf:
        username = s3_conf.get(Configuration.S3_ACCESS_KEY)
        password = s3_conf.get(Configuration.S3_SECRET_KEY)

        log_import(ExternalIntegration.AMAZON_S3)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.AMAZON_S3
        )
        service.username = unicode(username)
        service.password = unicode(password)

        del s3_conf[Configuration.S3_ACCESS_KEY]
        del s3_conf[Configuration.S3_SECRET_KEY]
        [service.set_setting(k, v) for k, v in s3_conf.items()]
finally:
    _db.commit()
    _db.close()
