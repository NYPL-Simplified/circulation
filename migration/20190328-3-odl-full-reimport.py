#!/usr/bin/env python
"""Reimport ODL collections to get individual license data.
"""
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from scripts import ODLImportScript
ODLImportScript().run()
