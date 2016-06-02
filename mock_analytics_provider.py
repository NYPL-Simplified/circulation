class MockAnalyticsProvider(object):
    """A mock analytics provider that keeps track of how many times it's called."""

    @classmethod
    def from_config(cls, config):        
        return cls(config.get('option'))

    def __init__(self, option=None):
        self.option = option
        self.count = 0
        self.event = None

    def collect_event(self, _db, lp, event_type, time, **kwargs):
        self.count = self.count + 1
        self.event_type = event_type
        self.time = time

Provider = MockAnalyticsProvider