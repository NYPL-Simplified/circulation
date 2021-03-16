#!/usr/bin/env python
"""Find LicensePools that have a work but no permanent work ID and
call calculate_work() on them. This will fix the problem, either by
calculating the appropriate permanent work ID or kicking them out of
their Works.
"""

import os
import sys
import logging
from pdb import set_trace
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))


from core.model import (
    production_session,
    Edition,
    LicensePool,
    Work,
)

_db = production_session()


def fix(_db, description, qu):
    a = 0
    print("%s: %s" % (description, qu.count()))
    for lp in qu:
        lp.calculate_work()
        a += 1
        if not a % 10:
            print("Committing")
            _db.commit()

no_presentation_edition = _db.query(LicensePool).outerjoin(
    LicensePool.presentation_edition).filter(Edition.id==None).filter(
        LicensePool.work_id != None
    )

no_permanent_work_id = _db.query(LicensePool).join(
    LicensePool.presentation_edition).filter(
        Edition.permanent_work_id==None
    ).filter(
        LicensePool.work_id != None
    )

no_title = _db.query(LicensePool).join(
    LicensePool.presentation_edition).filter(
        Edition.title==None
    ).filter(
        LicensePool.work_id != None
    )

licensepools_in_same_work_as_another_licensepool_with_different_pwid = _db.execute("select lp1.id from licensepools lp1 join works w on lp1.work_id=w.id join editions e1 on lp1.presentation_edition_id=e1.id join licensepools lp2 on lp2.work_id=w.id join editions e2 on e2.id=lp2.presentation_edition_id and e2.permanent_work_id != e1.permanent_work_id;")
ids = [x[0] for x in licensepools_in_same_work_as_another_licensepool_with_different_pwid]
in_same_work = _db.query(LicensePool).filter(LicensePool.id.in_(ids))

fix(_db, "Pools in the same work as another pool with a different pwid", in_same_work)
_db.commit()
fix(_db, "Pools with work but no presentation edition", no_presentation_edition)
_db.commit()
fix(_db, "Pools with work but no permanent work ID", no_permanent_work_id)
_db.commit()
fix(_db, "Pools with work but no title", no_title)
_db.commit()

qu = _db.query(Work).filter(~Work.license_pools.any())
print("Deleting %d works with no license pools." % qu.count())
