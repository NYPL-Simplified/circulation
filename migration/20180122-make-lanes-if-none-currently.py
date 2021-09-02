#!/usr/bin/env python3
"""Make sure every library has some lanes."""

import os
import sys
import logging

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (                    # noqa: E402
    Library,
    production_session,
)
from api.lanes import create_default_lanes  # noqa: E402


_db = production_session()

for library in _db.query(Library):
    num_lanes = len(library.lanes)
    if num_lanes:
        logging.info("%s has %d lanes, not doing anything.", library.name, num_lanes)
    else:
        logging.warn("%s has no lanes, creating some.", library.name)
        try:
            create_default_lanes(_db, library)
        except Exception as e:
            logging.error(
                "Could not create default lanes; suggest you try resetting them manually.",
                exc_info=e
            )
