#!/usr/bin/env python3
"""
Running the search index updater script will create the new
circulation-works-v3 index and change the circulation-works-current
alias to point to it.
"""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from scripts import UpdateSearchIndexScript     # noqa: E402

UpdateSearchIndexScript().run()
