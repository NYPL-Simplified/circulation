#!/usr/bin/env python
"""Remove miscellaneous expired things (Credentials, CachedFeeds, Loans, etc.)
from the database.
"""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from api.saml.monitor import SAMLMetadataMonitor
from core.scripts import RunMonitorScript
RunMonitorScript(SAMLMetadataMonitor).run()

