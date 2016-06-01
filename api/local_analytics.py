from core.model import CirculationEvent

class LocalAnalytics(object):
    @classmethod
    def from_config(cls):
        return cls()

    def collect(self, _db, license_pool, event_type, time, **kwargs):
        CirculationEvent.log(
          _db, license_pool, event_type, kwargs['old_value'], 
          kwargs['new_value'], start=time)

Collector = LocalAnalytics