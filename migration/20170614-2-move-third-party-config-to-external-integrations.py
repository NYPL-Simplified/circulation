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

from api.adobe_vendor_id import AuthdataUtility
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
    LIBRARIES = _db.query(Library).all() or [Library.instance(_db)]

    # Import Circulation Manager base url.
    circ_manager_conf = Configuration.integration('Circulation Manager')
    if circ_manager_conf:
        url = circ_manager_conf.get('url')
        if url:
            setting = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY)
            is_new = setting.value == None
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

    # Import NoveList Select configuration.
    novelist = Configuration.integration('NoveList Select')
    if novelist:
        username = novelist.get('profile')
        password = novelist.get('password')

        integration, is_new = get_one_or_create(
            _db, EI, protocol=EI.NOVELIST, goal=EI.METADATA_GOAL,
            username=username, password=password
        )
        integration.libraries.extend(LIBRARIES)
        log_import(integration, is_new)

    # Import NYT configuration.
    nyt_conf = Configuration.integration(u'New York Times')
    if nyt_conf:
        password = nyt_conf.get('best_sellers_api_key')

        integration, is_new = get_one_or_create(
            _db, EI, protocol=EI.NYT, goal=EI.METADATA_GOAL,
            password=password
        )
        log_import(integration, is_new)

    # Import Adobe Vendor ID configuration.
    adobe_conf = Configuration.integration('Adobe Vendor ID')
    if adobe_conf:
        vendor_id = adobe_conf.get('vendor_id')
        node_value = adobe_conf.get('node_value')
        other_libraries = adobe_conf.get('other_libraries')

        if node_value:
            node_libraries = LIBRARIES
            if len(node_libraries) > 1:
                # There's more than one library on this server.
                # Get the one that isn't listed as an "other" in the
                # JSON config.
                other_lib_names = [v[0].upper() for k, v in other_libraries.items()]
                node_libraries = filter(
                    lambda l: l.library_registry_short_name not in other_lib_names,
                    LIBRARIES
                )

                if len(node_libraries) > 1:
                    # There's still more than one library that claims to be
                    # an Adobe Vendor ID. As if.
                    raise ValueError(
                        "It's unclear which Library has access to the"
                        "Adobe Vendor ID")

            node_library = node_libraries[0]
            integration, is_new = get_one_or_create(
                _db, EI, protocol=EI.ADOBE_VENDOR_ID, goal=EI.DRM_GOAL,
            )
            integration.username = vendor_id
            integration.password = node_value

            if other_libraries:
                other_libraries = unicode(json.dumps(other_libraries))
                integration.set_setting(u'other_libraries', other_libraries)
            integration.libraries.append(node_library)
            log_import(integration, is_new)

        for library in LIBRARIES:
            short_name = library.library_registry_short_name
            short_name = short_name or adobe_conf.get('library_short_name')
            if short_name:
                short_name = short_name.upper()

            shared_secret = library.library_registry_shared_secret
            shared_secret = shared_secret or adobe_conf.get('authdata_secret')

            library_url = adobe_conf.get('library_uri')
            ConfigurationSetting.for_library(
                Library.WEBSITE_KEY, library).value = library_url

            integration, is_new = get_one_or_create(
                _db, EI, protocol=EI.LIBRARY_REGISTRY,
                goal=EI.DRM_GOAL, username=short_name,
                password=shared_secret
            )

            integration.set_setting(
                AuthdataUtility.VENDOR_ID_KEY, vendor_id
            )

            integration.libraries.append(library)

    # Import Google OAuth configuration.
    google_oauth_conf = Configuration.integration('Google OAuth')
    if google_oauth_conf:
        integration, is_new = get_one_or_create(
            _db, EI, protocol=EI.GOOGLE_OAUTH, goal=EI.ADMIN_AUTH_GOAL,
        )

        integration.url = google_oauth_conf.get("web", {}).get("auth_uri")
        integration.username = google_oauth_conf.get("web", {}).get("client_id")
        integration.password = google_oauth_conf.get("web", {}).get("client_secret")

        auth_domain = Configuration.policy('admin_authentication_domain')
        if auth_domain:
            integration.set_setting(u'domains', json.dumps([auth_domain]))

        log_import(integration, is_new)

    # Import Patron Web Client configuration.
    patron_web_client_conf = Configuration.integration(u'Patron Web Client', {})
    patron_web_client_url = patron_web_client_conf.get('url')
    if patron_web_client_url:
        setting = ConfigurationSetting.sitewide(
            _db, Configuration.PATRON_WEB_CLIENT_URL)
        is_new = setting.value == None
        setting.value = patron_web_client_url
        log_import(setting, is_new)
finally:
    _db.commit()
    _db.close()
