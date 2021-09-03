#!/usr/bin/env python3
"""
Move integration details from the Configuration file into the
database as ExternalIntegrations
"""
import os
import sys
import json
import logging

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (        # noqa: E402,F401
    ConfigurationSetting,
    ExternalIntegration as EI,
    Library,
    get_one_or_create,
    production_session,
)

from api.adobe_vendor_id import AuthdataUtility     # noqa: E402
from api.config import Configuration                # noqa: E402

log = logging.getLogger(name="Circulation manager configuration import")


def log_import(integration_or_setting):
    log.info("CREATED: %r" % integration_or_setting)


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
            setting.value = str(url)
            log_import(setting)

    # Import Metadata Wrangler configuration.
    metadata_wrangler_conf = Configuration.integration('Metadata Wrangler')

    if metadata_wrangler_conf:
        integration = EI(protocol=EI.METADATA_WRANGLER, goal=EI.METADATA_GOAL)
        _db.add(integration)

        integration.url = metadata_wrangler_conf.get('url')
        integration.username = metadata_wrangler_conf.get('client_id')
        integration.password = metadata_wrangler_conf.get('client_secret')

        log_import(integration)

    # Import NoveList Select configuration.
    novelist = Configuration.integration('NoveList Select')
    if novelist:
        integration = EI(protocol=EI.NOVELIST, goal=EI.METADATA_GOAL)
        _db.add(integration)

        integration.username = novelist.get('profile')
        integration.password = novelist.get('password')

        integration.libraries.extend(LIBRARIES)
        log_import(integration)

    # Import NYT configuration.
    nyt_conf = Configuration.integration('New York Times')
    if nyt_conf:
        integration = EI(protocol=EI.NYT, goal=EI.METADATA_GOAL)
        _db.add(integration)

        integration.password = nyt_conf.get('best_sellers_api_key')

        log_import(integration)

    # Import Adobe Vendor ID configuration.
    adobe_conf = Configuration.integration('Adobe Vendor ID')
    if adobe_conf:
        vendor_id = adobe_conf.get('vendor_id')
        node_value = adobe_conf.get('node_value')
        other_libraries = adobe_conf.get('other_libraries')

        if node_value:
            node_library = Library.default(_db)
            integration = EI(protocol=EI.ADOBE_VENDOR_ID, goal=EI.DRM_GOAL)
            _db.add(integration)

            integration.username = vendor_id
            integration.password = node_value

            if other_libraries:
                other_libraries = str(json.dumps(other_libraries))
                integration.set_setting('other_libraries', other_libraries)
            integration.libraries.append(node_library)
            log_import(integration)

        # Import short client token configuration.
        integration = EI(protocol='Short Client Token', goal=EI.DRM_GOAL)
        _db.add(integration)
        integration.set_setting(
            AuthdataUtility.VENDOR_ID_KEY, vendor_id
        )

        for library in LIBRARIES:
            short_name = library.library_registry_short_name
            short_name = short_name or adobe_conf.get('library_short_name')
            if short_name:
                ConfigurationSetting.for_library_and_externalintegration(
                    _db, EI.USERNAME, library, integration
                ).value = short_name

            shared_secret = library.library_registry_shared_secret
            shared_secret = shared_secret or adobe_conf.get('authdata_secret')
            ConfigurationSetting.for_library_and_externalintegration(
                _db, EI.PASSWORD, library, integration
            ).value = shared_secret

            library_url = adobe_conf.get('library_uri')
            ConfigurationSetting.for_library(
                Configuration.WEBSITE_URL, library).value = library_url

            integration.libraries.append(library)

    # Import Google OAuth configuration.
    google_oauth_conf = Configuration.integration('Google OAuth')
    if google_oauth_conf:
        integration = EI(protocol=EI.GOOGLE_OAUTH, goal=EI.ADMIN_AUTH_GOAL)
        _db.add(integration)

        integration.url = google_oauth_conf.get("web", {}).get("auth_uri")
        integration.username = google_oauth_conf.get("web", {}).get("client_id")
        integration.password = google_oauth_conf.get("web", {}).get("client_secret")

        auth_domain = Configuration.policy('admin_authentication_domain')
        if auth_domain:
            integration.set_setting('domains', json.dumps([auth_domain]))

        log_import(integration)

    # Import Patron Web Client configuration.
    patron_web_client_conf = Configuration.integration('Patron Web Client', {})
    patron_web_client_url = patron_web_client_conf.get('url')
    if patron_web_client_url:
        setting = ConfigurationSetting.sitewide(
            _db, Configuration.PATRON_WEB_CLIENT_URL)
        setting.value = patron_web_client_url
        log_import(setting)

    # Import analytics configuration.
    policies = Configuration.get("policies", {})
    analytics_modules = policies.get("analytics", ["core.local_analytics_provider"])

    if "api.google_analytics_provider" in analytics_modules:
        google_analytics_conf = Configuration.integration("Google Analytics Provider", {})
        tracking_id = google_analytics_conf.get("tracking_id")

        integration = EI(protocol="api.google_analytics_provider", goal=EI.ANALYTICS_GOAL)
        _db.add(integration)
        integration.url = "http://www.google-analytics.com/collect"

        for library in LIBRARIES:
            ConfigurationSetting.for_library_and_externalintegration(
                _db, "tracking_id", library, integration).value = tracking_id
            library.integrations += [integration]

    if "core.local_analytics_provider" in analytics_modules:
        integration = EI(protocol="core.local_analytics_provider", goal=EI.ANALYTICS_GOAL)
        _db.add(integration)

finally:
    _db.commit()
    _db.close()
