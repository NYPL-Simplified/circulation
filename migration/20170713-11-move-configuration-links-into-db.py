#!/usr/bin/env python3
"""
Move links from the Configuration file into the database as ConfigurationSettings for the default Library.
"""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (                # noqa: E402,F401
    ConfigurationSetting,
    Library,
    get_one_or_create,
    production_session,
)
from api.config import Configuration    # noqa: E402

Configuration.load()
_db = production_session()
library = Library.default(_db)

for rel, value in (
        ("terms-of-service", Configuration.get('links', {}).get('terms_of_service', None)),
        ("privacy-policy", Configuration.get('links', {}).get('privacy_policy', None)),
        ("copyright", Configuration.get('links', {}).get('copyright', None)),
        ("about", Configuration.get('links', {}).get('about', None)),
        ("license", Configuration.get('links', {}).get('license', None)),
):
    if value:
        ConfigurationSetting.for_library(rel, library).value = value

_db.commit()
_db.close()
