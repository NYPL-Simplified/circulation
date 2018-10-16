from nose.tools import set_trace
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import (
    join,
    and_,
)
from core.model import (
    dump_query,
    production_session,
    LicensePool,
    DataSource,
    Edition,
)

# Find all books where the edition associated with the LicensePool has a
# different medium from the presentation edition.
_db = production_session()

# Find all the LicensePools that aren't books.
subq = select([LicensePool.id]).select_from(
    join(LicensePool, Edition,
         and_(LicensePool.data_source_id==Edition.data_source_id,
              LicensePool.identifier_id==Edition.primary_identifier_id)
    )
).where(Edition.medium != Edition.BOOK_MEDIUM)

# Of those LicensePools, find every LicensePool whose presentation
# edition says it _is_ a book.
qu = _db.query(LicensePool).join(
    Edition, LicensePool.presentation_edition_id==Edition.id
).filter(LicensePool.id.in_(subq)).filter(Edition.medium == Edition.BOOK_MEDIUM)

print "Recalculating presentation edition for %d LicensePools." % qu.count()

for lp in qu:
    # Recalculate that LicensePool's presentation edition.
    lp.set_presentation_edition()
    print "New medium: %s" % lp.presentation_edition.medium
    _db.commit()
