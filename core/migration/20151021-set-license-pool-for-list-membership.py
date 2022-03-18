#!/usr/bin/env python3
"""Make sure every CustomListEntry has a LicensePool set."""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (production_session, CustomListEntry)    # noqa: E402

_db = production_session()

qu = _db.query(CustomListEntry).filter(CustomListEntry.license_pool==None)      # noqa: E225,E711
print("Fixing %d custom list entries with no licensepool." % qu.count())

for cle in qu:
    cle.set_license_pool()
