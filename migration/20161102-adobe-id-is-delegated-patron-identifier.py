#!/usr/bin/env python3
"""
For every patron with a credential containing an Adobe ID, make
sure they also get a DelegatedPatronIdentifier containing the same
Adobe ID. This makes sure that they don't suddenly change Adobe IDs
when they start using a client that employs the new JWT-based authdata
system.
"""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (        # noqa: E402
    production_session,
    Patron
)
from api.util.short_client_token import ShortClientTokenUtility     # noqa: E402

_db = production_session()
authdata = ShortClientTokenUtility.from_config()
if not authdata:
    print("Adobe IDs not configured, doing nothing.")

count = 0
qu = _db.query(Patron)
print("Processing %d patrons." % qu.count())

for patron in qu:
    credential, delegated_identifier = authdata.migrate_adobe_id(patron)
    count += 1
    if not (count % 100):
        print(count)
        _db.commit()

    if credential is None or delegated_identifier is None:
        # This patron did not have an Adobe ID in the first place.
        # Do nothing.
        continue

    output = "%s -> %s -> %s" % (
        patron.authorization_identifier,
        credential.credential,
        delegated_identifier.delegated_identifier
    )
    print(output)

_db.commit()
