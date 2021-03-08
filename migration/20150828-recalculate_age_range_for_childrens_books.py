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

class RecalculateAgeRangeMonitor(SubjectSweepMonitor):
    """Recalculate the age range for every young adult or children's subject."""

    def __init__(self, _db, interval_seconds=None):
        super(RecalculateAgeRangeMonitor, self).__init__(
            _db, "20150825 migration - Recalculate age range for children's books",
            interval_seconds, batch_size=1000)

    def subject_query(self):
        audiences = [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN]
        return self._db.query(Subject).filter(Subject.audience.in_(audiences))

    def process_identifier(self, subject):
        old_target_age = subject.target_age
        subject.assign_to_genre()
        if subject.target_age != old_target_age and subject.target_age.lower != None:
            print("%r: %r->%r" % (subject, old_target_age, subject.target_age))

RunMonitorScript(RecalculateAgeRangeMonitor).run()
