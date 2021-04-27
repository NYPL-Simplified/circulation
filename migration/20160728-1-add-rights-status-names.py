#!/usr/bin/env python
"""Add names to rightsstatus table."""

import os
import sys
import logging
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..", "..")
sys.path.append(os.path.abspath(package_dir))


from core.model import (
    production_session,
    RightsStatus,
)

_db = production_session()

for uri in list(RightsStatus.NAMES.keys()):
    status = RightsStatus.lookup(_db, uri)
    status.name = RightsStatus.NAMES.get(uri)

_db.commit()
