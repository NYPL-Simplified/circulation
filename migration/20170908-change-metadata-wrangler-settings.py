#!/usr/bin/env python
"""Delete outdated ConfigurationSettings for the metadata wrangler."""
import os
import sys
import logging
from nose.tools import set_trace

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from model import (
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
                # All passwords (previous client_secrets) must be reset as
                # shared_secrets.
                setting.value = None
        _db.commit()
    _db.close()
except Exception as e:
    db.close()
    raise e
