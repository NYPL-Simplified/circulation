#!/usr/bin/env python3
"""
Recalculate the display information about all contributors mistakenly given Wikidata IDs as 'names'.
"""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (production_session, Contributor)    # noqa: E402

_db = production_session()

from sqlalchemy.sql import text                             # noqa: E402

contributors = _db.query(Contributor).filter(
    text("contributors.display_name ~ '^Q[0-9]'")
).order_by(Contributor.id)

print(contributors.count())

for contributor in contributors:
    display_name, family_name = contributor.default_names()
    print("%s/%s: %s => %s, %s => %s" % (
        contributor.id,
        contributor.name,
        contributor.display_name, display_name,
        contributor.family_name, family_name
    ))
    contributor.display_name = display_name
    contributor.wikipedia_name = None
    contributor.family_name = family_name
    for contribution in contributor.contributions:
        edition = contribution.edition
        if edition.work:
            edition.work.calculate_presentation()
        else:
            edition.calculate_presentation()
    _db.commit()
