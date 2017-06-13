#!/usr/bin/env python
"""Move integration details from the Configuration file into the
database as ExternalIntegrations
"""
import os
import sys
import json
import logging
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    ConfigurationSetting,
    ExternalIntegration as EI,
    Library,
    get_one_or_create,
    production_session,
)

from api.config import Configuration

log = logging.getLogger(name="Circulation manager configuration import")

def log_import(integration_or_setting, is_new):
    if is_new:
        log.info("CREATED: %r" % integration_or_setting)
    else:
        log.info("%r already exists." % integration_or_setting)

try:
    Configuration.load()
    _db = production_session()
    LIBRARIES = _db.query(Library).all()

    # Import Circulation Manager base url.
    circ_manager_conf = Configuration.integration('Circulation Manager')
    if circ_manager_conf:
        url = circ_manager_conf.get('url')
        if url:
            setting = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY)
            is_new = setting.value
            setting.value = unicode(url)
            log_import(setting, is_new)

    # Import Metadata Wrangler configuration.
    metadata_wrangler_conf = Configuration.integration('Metadata Wrangler')

    if metadata_wrangler_conf:
        url = metadata_wrangler_conf.get('url')
        username = metadata_wrangler_conf.get('client_id')
        password = metadata_wrangler_conf.get('client_secret')

        integration, is_new = get_one_or_create(
            _db, EI, protocol=EI.METADATA_WRANGLER, goal=EI.METADATA_GOAL,
            url=url, username=username, password=password
        )
        log_import(integration, is_new)

    # Import Adobe Vendor ID configuration.
    adobe_conf = Configuration.integration('Adobe Vendor ID')
    if adobe_conf:
        integration, is_new = get_one_or_create(
            _db, EI, protocol=EI.ADOBE_VENDOR_ID, goal=EI.DRM_GOAL
        )

        integration.username = adobe_conf.get('vendor_id')
        integration.password = adobe_conf.get('node_value')

        other_libraries = adobe_conf.get('other_libraries')
        if other_libraries:
            other_libraries = unicode(json.dumps(other_libraries))
        integration.set_setting(u'other_libraries', other_libraries)
        integration.libraries.extend(LIBRARIES)

    # Import Google OAuth configuration.
    google_oauth_conf = AdminConfiguration.integration(AdminConfiguration.GOOGLE_OAUTH_INTEGRATION)
    if google_oauth_conf:
        log_import(AdminConfiguration.GOOGLE_OAUTH_INTEGRATION)
        admin_auth_service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.GOOGLE_OAUTH
        )

        admin_auth_service.url = google_oauth_conf.get("web", {}).get("auth_uri")
        admin_auth_service.username = google_oauth_conf.get("web", {}).get("client_id")
        admin_auth_service.password = google_oauth_conf.get("web", {}).get("client_secret")

        auth_domain = Configuration.policy(AdminConfiguration.ADMIN_AUTH_DOMAIN)
        admin_auth_service.type = ExternalIntegration.ADMIN_AUTH_TYPE
        if auth_domain:
            admin_auth_service.set_setting("domains", json.dumps([auth_domain]))

    # Import Patron Web Client configuration.
    patron_web_client_conf = Configuration.integration(Configuration.PATRON_WEB_CLIENT_INTEGRATION)
    if patron_web_client_conf:
        log_import(Configuration.PATRON_WEB_CLIENT_INTEGRATION)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.PATRON_WEB_CLIENT
        )

        service.url = patron_web_client_conf.get(Configuration.URL)

    # Import Staff Picks configuration.
    staff_picks_conf = Configuration.integration(Configuration.STAFF_PICKS_INTEGRATION)
    if staff_picks_conf:
        log_import(Configuration.STAFF_PICKS_INTEGRATION)
        service, ignore = get_one_or_create(
            _db, ExternalIntegration, provider=ExternalIntegration.STAFF_PICKS
        )
        service.url = staff_picks_conf.get(Configuration.URL)
        del staff_picks_conf[Configuration.URL]
        [service.set_setting(k, v) for k, v in staff_picks_conf.items()]
finally:
    _db.commit()
    _db.close()
