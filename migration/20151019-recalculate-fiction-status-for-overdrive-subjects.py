#!/usr/bin/env python
"""Recalculate the age range for all subjects whose audience is Children or Young Adult."""
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))
from core.monitor import SubjectSweepMonitor
from core.classifier import Classifier
from core.model import (
    production_session,
    DataSource,
    Edition,
    Subject,
)
from core.scripts import RunMonitorScript

class RecalculateFictionStatusMonitor(SubjectSweepMonitor):
    """Recalculate the age range for every young adult or children's subject."""

    def __init__(self, _db, interval_seconds=None):
        super(RecalculateFictionStatusMonitor, self).__init__(
            _db, "20150825 migration - Recalculate age range for children's books",
            interval_seconds, batch_size=1000)

    def subject_query(self):
        return self._db.query(Subject).filter(Subject.type==Subject.OVERDRIVE)

    def process_identifier(self, subject):
        old_fiction = subject.fiction
        old_audience = subject.audience
        subject.assign_to_genre()
        print("%s %s %s" % (subject.identifier, subject.fiction, subject.audience))

RunMonitorScript(RecalculateFictionStatusMonitor).run()
