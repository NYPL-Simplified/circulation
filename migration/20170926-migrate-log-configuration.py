#!/usr/bin/env python
"""Move log details from the Configuration file into the
database as ExternalIntegrations
"""

import os
import sys
import logging


bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from config import Configuration
from model import (
    ExternalIntegration as EI,
    production_session,
)
_db = production_session()
log = logging.getLogger(name="Log configuration import")
loggly_conf = Configuration.integration(u'loggly')

if loggly_conf:
    integration = EI(goal=EI.LOGGING_GOAL, protocol=EI.LOGGLY)
    _db.add(integration)
    integration.url = loggly_conf.get(
        'url', 'https://logs-01.loggly.com/inputs/%(token)s/tag/python/'
    )
    integration.password = loggly_conf.get('token')
_db.commit()

