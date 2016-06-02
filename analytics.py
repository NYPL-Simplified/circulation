import importlib
import datetime

class Analytics(object):
    @classmethod
    def initialize(cls, providers, config):
        if not providers:
            return cls()
        if isinstance(providers, basestring):
            providers = [providers]
        analytics_providers = []
        for provider_string in providers:
            provider_module = importlib.import_module(provider_string)
            provider_class = getattr(provider_module, "Provider")
            analytics_providers.append(provider_class.from_config(config))
        return cls(analytics_providers)

    def __init__(self, providers=[]):
        self.providers = providers

    def collect_event(self, _db, license_pool, event_type, time=None, **kwargs):
        if not time:
            time = datetime.datetime.utcnow()
        for provider in self.providers:
            provider.collect_event(_db, license_pool, event_type, time, **kwargs)