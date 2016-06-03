class LocalAnalyticsProvider(object):
    @classmethod
    def from_config(cls, config):
        return cls()

    def collect_event(self, _db, license_pool, event_type, time, 
        old_value=None, new_value=None, **kwargs):
        from core.model import CirculationEvent
        CirculationEvent.log(
          _db, license_pool, event_type, old_value, new_value, start=time)

Provider = LocalAnalyticsProvider