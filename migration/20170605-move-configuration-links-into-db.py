#!/usr/bin/env python
"""Move links from the Configuration file into the database as ConfigurationSettings
for the default Library.
"""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    ConfigurationSetting,
    Library,
    get_one_or_create,
    production_session,
)
from api.config import Configuration

Configuration.load()
_db = production_session()
library = Library.instance(_db)

for rel, value in (
        ("terms-of-service", Configuration.terms_of_service_url()),
        ("privacy-policy", Configuration.privacy_policy_url()),
        ("copyright", Configuration.acknowledgements_url()),
        ("about", Configuration.about_url()),
        ("license", Configuration.license_url()),
):
    if value:
        ConfigurationSetting.for_library(_db, rel, library).value = value

_db.commit()
_db.close()

