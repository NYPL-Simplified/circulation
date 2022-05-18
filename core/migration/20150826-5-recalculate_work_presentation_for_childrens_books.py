#!/usr/bin/env python3
"""Recalculate the age range for all subjects whose audience is Children or Young Adult."""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))

from core.monitor import WorkSweepMonitor           # noqa: E402
from core.classifier import Classifier              # noqa: E402
from core.model import (                            # noqa: E402,F401
    production_session,
    DataSource,
    Edition,
    Subject,
    Work,
    Identifier,
)
from core.scripts import RunMonitorScript           # noqa: E402
from psycopg2.extras import NumericRange            # noqa: E402


class RecalculateAgeRangeMonitor(WorkSweepMonitor):
    """Recalculate the age range for every young adult or children's book."""

    def __init__(self, _db, interval_seconds=None):
        super(RecalculateAgeRangeMonitor, self).__init__(
            _db, "20150825 migration - Recalculate age range for children's books (Works)",
            interval_seconds, batch_size=10)

    def work_query(self):
        audiences = [Classifier.AUDIENCE_YOUNG_ADULT, Classifier.AUDIENCE_CHILDREN]
        return self._db.query(Work).filter(Work.audience.in_(audiences))

    def process_work(self, work):
        primary_identifier_ids = [
            x.primary_identifier.id for x in work.editions]
        data = Identifier.recursively_equivalent_identifier_ids(
            self._db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = Identifier.flatten_identifier_ids(data)
        workgenres, work.fiction, work.audience, target_age = work.assign_genres(flattened_data)
        old_target_age = work.target_age
        work.target_age = NumericRange(*target_age)
        if work.target_age != old_target_age and work.target_age.lower is not None:
            print("%r: %r->%r" % (work.title, old_target_age, work.target_age))


RunMonitorScript(RecalculateAgeRangeMonitor).run()
