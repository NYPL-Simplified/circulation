from nose.tools import set_trace
import importlib
import contextlib
import datetime
import os
from collections import defaultdict
from model import ExternalIntegration
from config import CannotLoadConfiguration
from sqlalchemy.orm.session import Session

class Analytics(object):

    GLOBAL_ENABLED = None
    LIBRARY_ENABLED = set()

    def __init__(self, _db):
        self.sitewide_providers = []
        self.library_providers = defaultdict(list)
        self.initialization_exceptions = {}
        Analytics.GLOBAL_ENABLED = False
        Analytics.LIBRARY_ENABLED = set()
        # Find a list of all the ExternalIntegrations set up with a
        # goal of analytics.
        integrations = _db.query(ExternalIntegration).filter(ExternalIntegration.goal==ExternalIntegration.ANALYTICS_GOAL)
        # Turn each integration into an analytics provider.
        for integration in integrations:
            kwargs = {}
            module = integration.protocol
            if module.startswith('.'):
                # This is a relative import. Import it relative to
                # this module. This should only happen during tests.
                kwargs['package'] =__name__
            else:
                # This is an absolute import. Trust sys.path to find it.
                pass
            try:
                provider_module = importlib.import_module(module, **kwargs)
                provider_class = getattr(provider_module, "Provider", None)
                if provider_class:
                    if not integration.libraries:
                        provider = provider_class(integration)
                        self.sitewide_providers.append(provider)
                        Analytics.GLOBAL_ENABLED = True
                    else:
                        for library in integration.libraries:
                            provider = provider_class(integration, library)
                            self.library_providers[library.id].append(provider)
                            Analytics.LIBRARY_ENABLED.add(library.id)
                else:
                    self.initialization_exceptions[integration.id] = "Module %s does not have Provider defined." % module
            except (ImportError, CannotLoadConfiguration), e:
                self.initialization_exceptions[integration.id] = e

    def collect_event(self, library, license_pool, event_type, time=None, **kwargs):
        return
        if not time:
            time = datetime.datetime.utcnow()
        providers = list(self.sitewide_providers)
        if library:
            providers.extend(self.library_providers[library.id])
        for provider in providers:
            provider.collect_event(library, license_pool, event_type, time, **kwargs)

    @classmethod
    def is_configured(cls, library):
        if cls.GLOBAL_ENABLED is None:
            Analytics(Session.object_session(library))
        if cls.GLOBAL_ENABLED:
            return True
        else:
            return library.id in cls.LIBRARY_ENABLED
