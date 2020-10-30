#!/usr/bin/env python
"""Update the circulation manager server with new books from OPDS 2.0 import collections."""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from api.proquest.importer import ProQuestOPDS2Importer, ProQuestOPDS2ImportMonitor
from api.proquest.scripts import ProQuestOPDS2ImportScript
from core.model import ExternalIntegration

import_script = ProQuestOPDS2ImportScript(
    importer_class=ProQuestOPDS2Importer,
    monitor_class=ProQuestOPDS2ImportMonitor,
    protocol=ExternalIntegration.PROQUEST
)

import_script.run()
