#!/usr/bin/env python
"""Set Edition.open_access_download_url for all Project Gutenberg books."""
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))
from core.monitor import EditionSweepMonitor
from core.model import (
    production_session,
    DataSource,
    Edition,
)
from core.scripts import RunMonitorScript

class OpenAccessDownloadSetMonitor(EditionSweepMonitor):
    """Recalculate the permanent work ID for every Project Gutenberg edition."""

    def __init__(self, _db, interval_seconds=None):
        super(OpenAccessDownloadSetMonitor, self).__init__(
            _db, "Open Access Download link set", interval_seconds)

    def edition_query(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        return self._db.query(Edition).filter(Edition.data_source==gutenberg)

    def process_edition(self, edition):
        edition.set_open_access_link()
        if edition.best_open_access_link:
            print edition.id, edition.title, edition.best_open_access_link.url
        else:
            print edition.id, edition.title, "[no link]"
        return True
    
RunMonitorScript(OpenAccessDownloadSetMonitor).run()
