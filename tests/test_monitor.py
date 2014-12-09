from nose.tools import eq_, set_trace
import datetime

from testing import (
    DatabaseTest,
)

from model import Timestamp

from monitor import Monitor

class DummyMonitor(Monitor):

    def __init__(self):
        super(DummyMonitor, self).__init__("Dummy monitor for test", 0.1)
        self.run_records = []
        self.cleanup_records = []

    def run_once(self, _db, start, cutoff):
        self.original_timestamp = start
        self.run_records.append(True)
        self.stop_running = True

    def cleanup(self):
        self.cleanup_records.append(True)

class TestMonitor(DatabaseTest):

    def test_monitor_lifecycle(self):
        monitor = DummyMonitor()

        # There is no timestamp for this monitor.
        eq_([], self._db.query(Timestamp).filter(
            Timestamp.service==monitor.service_name).all())

        # Run the monitor.
        monitor.run(self._db)

        # The monitor ran once and then stopped.
        eq_([True], monitor.run_records)

        # cleanup() was called once.
        eq_([True], monitor.cleanup_records)

        # A timestamp was put into the database when we ran the
        # monitor.
        timestamp = self._db.query(Timestamp).filter(
            Timestamp.service==monitor.service_name).one()

        # The current value of the timestamp is different from the
        # original value, because it was updated after run_once() was
        # called.
        assert timestamp.timestamp > monitor.original_timestamp
