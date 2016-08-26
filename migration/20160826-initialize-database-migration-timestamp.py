#!/usr/bin/env python
"""Create Database Migration Script timestamp for the most recent
migration run (this one) so the DatabaseMigrationScript at
bin/migrate_database can be run from here on out.
"""

import os
import sys
import logging
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from nose.tools import set_trace
from scripts import DatabaseMigrationInitializationScript

DatabaseMigrationInitializationScript().run()

