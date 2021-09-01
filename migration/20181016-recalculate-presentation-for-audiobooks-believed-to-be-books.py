#!/usr/bin/env python3

import os
import sys
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import (
    join,
    and_,
)

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (            # noqa: E402,F401
    dump_query,
    production_session,
    LicensePool,
    DataSource,
    Edition,
    PresentationCalculationPolicy,
)

# Find all books where the edition associated with the LicensePool has a
# different medium from the presentation edition.
_db = production_session()

# Find all the LicensePools that aren't books.
subq = select([LicensePool.id]).select_from(
    join(LicensePool, Edition,
         and_(LicensePool.data_source_id==Edition.data_source_id,               # noqa: E225
              LicensePool.identifier_id==Edition.primary_identifier_id))        # noqa: E225
).where(Edition.medium != Edition.BOOK_MEDIUM)

# Of those LicensePools, find every LicensePool whose presentation
# edition says it _is_ a book.
qu = _db.query(LicensePool).join(
    Edition, LicensePool.presentation_edition_id==Edition.id                    # noqa: E225
).filter(LicensePool.id.in_(subq)).filter(Edition.medium == Edition.BOOK_MEDIUM)

print("Recalculating presentation edition for %d LicensePools." % qu.count())

for lp in qu:
    # Recalculate that LicensePool's presentation edition, and then its
    # work presentation.
    lp.set_presentation_edition()
    policy = PresentationCalculationPolicy(
        regenerate_opds_entries=True, update_search_index=True
    )
    work, is_new = lp.calculate_work()
    work.calculate_presentation(policy)
    print("New medium: %s" % lp.presentation_edition.medium)
    _db.commit()
