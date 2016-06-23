import importlib
import datetime
from config import Configuration

class Analytics(object):

    __instance = None

    DEFAULT_PROVIDERS = ["core.local_analytics_provider"]

    @classmethod
    def instance(cls):
        if not cls.__instance:
            config = Configuration.instance
            providers = cls.load_providers_from_config(config)
            cls.initialize(providers, config)
        return cls.__instance

    @classmethod
    def initialize(cls, providers, config):
        if not providers:
            cls.__instance = cls()
            return cls.__instance
        if isinstance(providers, basestring):
            providers = [providers]
        analytics_providers = []
        for provider_string in providers:
            provider_module = importlib.import_module(provider_string)
            provider_class = getattr(provider_module, "Provider")
            analytics_providers.append(provider_class.from_config(config))
        cls.__instance = cls(analytics_providers)
        return cls.__instance

    def __init__(self, providers=[]):
        self.providers = providers

    @classmethod
    def collect_event(cls, _db, license_pool, event_type, time=None, **kwargs):
        if not time:
            time = datetime.datetime.utcnow()
        for provider in cls.instance().providers:
            provider.collect_event(_db, license_pool, event_type, time, **kwargs)

    @classmethod
    def load_providers_from_config(cls, config):
        policies = config.get(Configuration.POLICIES, {})
        return policies.get(Configuration.ANALYTICS_POLICY, cls.DEFAULT_PROVIDERS)
