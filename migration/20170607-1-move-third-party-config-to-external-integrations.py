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
    ConfigurationSetting,
    ExternalIntegration,
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

EI = ExternalIntegration

try:
    Configuration.load()
    _db = production_session()
    LIBRARIES = _db.query(Library).all()

    # Import CDN configuration.
    cdn_conf = Configuration.integration(u'CDN')
    if cdn_conf:
        cdn_goals = [EI.BOOK_COVERS_GOAL, EI.OPDS_GOAL, EI.OA_BOOKS_GOAL]
        cdns = set([])
        for k, v in cdn_conf.items():
            if k in cdn_goals:
                cdn, is_new = get_one_or_create(
                    _db, EI, protocol=EI.CDN, goal=unicode(k),
                    url=unicode(v)
                )
                log_import(cdn, is_new)
                cdns.add(cdn)
            else:
                raise ValueError('No ExternalIntegration goal for %s' % k)

        for cdn in cdns:
            cdn.libraries = LIBRARIES

    # Import Circulation Manager configuration.
finally:
    _db.commit()
    _db.close()
