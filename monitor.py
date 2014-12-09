from nose.tools import set_trace
import datetime
import time

from model import (
    get_one_or_create,
    Timestamp,
)

class Monitor(object):

    def __init__(self, name, interval_seconds=1*60, default_start_time=None):
        self.service_name = name
        self.interval_seconds = interval_seconds
        self.stop_running = False
        if not default_start_time:
             default_start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=60)
        self.default_start_time = default_start_time

    def run(self, _db):
        
        self.timestamp, new = get_one_or_create(
            _db, Timestamp,
            service=self.service_name,
            create_method_kwargs=dict(
                timestamp=self.default_start_time
            )
        )
        start = self.timestamp.timestamp or self.default_start_time

        while not self.stop_running:
            cutoff = datetime.datetime.utcnow()
            new_timestamp = self.run_once(_db, start, cutoff) or cutoff
            duration = datetime.datetime.utcnow() - cutoff
            to_sleep = self.interval_seconds-duration.seconds-1
            self.cleanup()
            self.timestamp.timestamp = new_timestamp
            _db.commit()
            if to_sleep > 0:
                time.sleep(to_sleep)
            start = new_timestamp

    def run_once(self, _db, start, cutoff):
        raise NotImplementedError()

    def cleanup(self):
        pass
