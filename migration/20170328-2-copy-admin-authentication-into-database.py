#!/usr/bin/env python
"""Copy the admin authentication configuration information from the JSON
configuration into AdminAuthenticationService objects.
"""

import os
import sys
from pdb import set_trace
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, u"..")
sys.path.append(os.path.abspath(package_dir))
import json
from api.admin.config import Configuration
from core.model import (
    get_one_or_create,
    production_session,
    AdminAuthenticationService,
    ExternalIntegration,
)

_db = production_session()

auth_domain = Configuration.policy("admin_authentication_domain")
google_oauth_config = Configuration.integration("Google OAuth")

if not google_oauth_config:
    print "Google OAuth is not configured, not creating an AdminAuthenticationService for it"
else:
    admin_auth_service, ignore = get_one_or_create(
        _db, AdminAuthenticationService,
        name="Google OAuth (%s)" % auth_domain,
        provider=AdminAuthenticationService.GOOGLE_OAUTH,
    )

    admin_auth_service.external_integration.set_setting(
        "domains", json.dumps([auth_domain])
    )
    admin_auth_service.external_integration.set_setting(
        "config", json.dumps(google_oauth_config)
    )

    _db.commit()

