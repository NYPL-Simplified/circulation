#!/usr/bin/env python3
"""Delete outdated ConfigurationSettings for the metadata wrangler."""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from model import (                 # noqa: E402
    production_session,
    ExternalIntegration as EI,
)

_db = production_session()

try:
    integration = EI.lookup(_db, EI.METADATA_WRANGLER, EI.METADATA_GOAL)

    if integration:
        for setting in integration.settings:
            if setting.key == 'username':
                # A username (or client_id) is no longer required.
                _db.delete(setting)
            if setting.key == 'password':
                # The password (previously client_secret) must be reset to
                # register for a shared_secret.
                setting.value = None
        _db.commit()
    _db.close()
except Exception:
    _db.close()
    raise
