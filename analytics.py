import importlib
import contextlib
import datetime
from config import Configuration
import logging

class Analytics(object):

    __instance = None

    if '.' in __module__:
        # We are operating in an application that imports this product
        # as a package (probably called 'core'). The module name of
        # the analytics provider should be scoped to the name of the
        # package, i.e.  'core.local_analytics_provider'.
        package_name = __module__[:__module__.rfind('.')+1]
    else:
        # This application is not imported as a package, probably
        # because we're running its unit tests.
        package_name = ''

    DEFAULT_PROVIDERS = [package_name + "local_analytics_provider"]

    @classmethod
    def instance(cls):
        if not cls.__instance:
            config = Configuration.instance
            providers = cls.load_providers_from_config(config)
            logging.info("Analytics providers from config: %s", providers)
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
        logging.info("Analytics initializing with providers: %s", analytics_providers)
        cls.__instance = cls(analytics_providers)
        return cls.__instance

    def __init__(self, providers=[]):
        self.providers = providers
        logging.info("Analytics instance created with providers: %s", self.providers)

    @classmethod
    def collect_event(cls, _db, license_pool, event_type, time=None, **kwargs):
        if not time:
            time = datetime.datetime.utcnow()
        for provider in cls.instance().providers:
            logging.info("Analytics collecting event for %s", provider)
            provider.collect_event(_db, license_pool, event_type, time, **kwargs)

    @classmethod
    def load_providers_from_config(cls, config):
        policies = config.get(Configuration.POLICIES, {})
        logging.info("Analytics loaded policies: %s", policies)
        return policies.get(Configuration.ANALYTICS_POLICY, cls.DEFAULT_PROVIDERS)


@contextlib.contextmanager
def temp_analytics(providers, config):
    """A context manager to temporarily replace the analytics providers
    used by a test.
    """
    old_instance = Analytics._Analytics__instance
    Analytics.initialize(providers, config)
    yield
    Analytics._Analytics__instance = old_instance

