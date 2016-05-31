from nose.tools import set_trace

from api.config import (
    Configuration as CirculationManagerConfiguration,
    CannotLoadConfiguration,
    temp_config as circulation_temp_config,
)
import contextlib

class Configuration(CirculationManagerConfiguration):

    ADMIN_AUTH_DOMAIN = "admin_authentication_domain"
    SECRET_KEY = "secret_key"
    GOOGLE_OAUTH_INTEGRATION = "Google OAuth"

@contextlib.contextmanager
def temp_config(new_config=None):
    with circulation_temp_config(new_config, [Configuration]) as i:
        yield i
