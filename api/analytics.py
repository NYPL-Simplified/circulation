from config import (
    Configuration,
    CannotLoadConfiguration,
)
import importlib

class Analytics(object):
    @classmethod
    def initialize(cls):
        providers = Configuration.policy("analytics")
        if not providers:
            return cls()
        if isinstance(providers, basestring):
            providers = [providers]
        analytics_providers = []
        for provider_string in providers:
            provider_module = importlib.import_module(provider_string)
            provider_class = getattr(provider_module, "Collector")
            analytics_providers.append(provider_class.from_config())
        return cls(analytics_providers)

    def __init__(self, providers=[]):
        self.providers = providers

    def collect_event(self, event):
        for provider in self.providers:
            provider.collect_event(event)