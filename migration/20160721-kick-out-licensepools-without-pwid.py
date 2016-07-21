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

from nose.tools import set_trace
from core.model import (
    production_session,
    Edition,
    LicensePool,
    Work,
)

_db = production_session()


def fix(_db, description, qu):
    print "%s: %s" % (description, qu.count())
    for lp in qu:
        lp.calculate_work()

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

fix(_db, "Pools with work but no presentation edition", no_presentation_edition)
fix(_db, "Pools with work but no permanent work ID", no_permanent_work_id)
_db.commit()

qu = _db.query(Work).filter(~Work.license_pools.any())
print "Deleting %d works with no license pools." % qu.count()
