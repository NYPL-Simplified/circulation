import os
import sys
import logging
from pdb import set_trace
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))

from nose.tools import set_trace
from core.model import (
    Edition,
    production_session,
    LicensePool,
    Work,
)

_db = production_session()

works = _db.query(Work).filter(Work.fiction == None).order_by(Work.id)
for work in works:
    work.set_presentation_ready_based_on_content()

_db.commit()


license_pools = _db.query(LicensePool)
                    .join(LicensePool.presentation_edition)
                    .filter(LicensePool.work == None)
                    .filter(Edition.author == "[Unknown]")
                    .order_by(LicensePool.id)

for license_pool in license_pools:
    license_pool.calculate_work()
    _db.commit()
