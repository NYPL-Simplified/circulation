#!/usr/bin/env python
"""Import ProQuest OPDS 2.0 feeds into Circulation Manager."""

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
