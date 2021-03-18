#!/usr/bin/env python
import os
import sys
import logging
from pdb import set_trace
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    Edition,
    production_session,
    LicensePool,
    Work,
)

_db = production_session()

works = _db.query(Work).filter(Work.fiction == None).order_by(Work.id)
logging.info("Processing %d works with no fiction status.", works.count())
a = 0
for work in works:
    logging.info("Processing %s", work.title)
    work.set_presentation_ready_based_on_content()
    if not a % 10:
        _db.commit()
_db.commit()


license_pools = _db.query(LicensePool).join(LicensePool.presentation_edition).filter(LicensePool.work == None).filter(Edition.title != None).filter(Edition.author == "[Unknown]").order_by(LicensePool.id)
logging.info("Processing %d license pools with no work and no known author.", license_pools.count())
for license_pool in license_pools:
    logging.info("Processing %s", license_pool.presentation_edition.title)
    try:
        license_pool.calculate_work()
    except Exception, e:
        logging.error("That didn't work.", exc_info=e)
    _db.commit()
