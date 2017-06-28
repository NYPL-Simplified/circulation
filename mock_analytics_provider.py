class MockAnalyticsProvider(object):
    """A mock analytics provider that keeps track of how many times it's called."""

    def __init__(self, integration=None, library=None):
        self.count = 0
        self.event = None
        if integration:
            self.url = integration.url

    def collect_event(self, library, lp, event_type, time=None, **kwargs):
        self.count = self.count + 1
        self.event_type = event_type
        self.time = time

Provider = MockAnalyticsProvider
