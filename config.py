import contextlib
from core.config import (
    Configuration as CoreConfiguration,
    CannotLoadConfiguration,
    empty_config as core_empty_config,
    temp_config as core_temp_config,
)

class Configuration(CoreConfiguration):

    LENDING_POLICY = "lending"
    AUTHENTICATION_POLICY = "authentication"
    LANGUAGE_POLICY = "languages"
    PRIMARY_LANGUAGE_COLLECTIONS = "primary"
    OTHER_LANGUAGE_COLLECTIONS = "other"

    LANES_POLICY = "lanes"

    ROOT_LANE_POLICY = "root_lane"

    ADOBE_VENDOR_ID_INTEGRATION = "Adobe Vendor ID"
    ADOBE_VENDOR_ID = "vendor_id"
    ADOBE_VENDOR_ID_NODE_VALUE = "node_value"

    AUTHENTICATION = "authentication"
    AUTHENTICATION_TEST_USERNAME = "test_username"
    AUTHENTICATION_TEST_PASSWORD = "test_password"

    MILLENIUM_INTEGRATION = "Millenium"
    STAFF_PICKS_INTEGRATION = "Staff Picks"
   
    DEFAULT_NOTIFICATION_EMAIL_ADDRESS = "default_notification_email_address"

    @classmethod
    def lending_policy(cls):
        return cls.policy(cls.LENDING_POLICY)

    @classmethod
    def root_lane_policy(cls):
        return cls.policy(cls.ROOT_LANE_POLICY)

    @classmethod
    def language_policy(cls):
        return cls.policy(cls.LANGUAGE_POLICY, required=True)

    @classmethod
    def default_notification_email_address(cls):
        return cls.required(cls.DEFAULT_NOTIFICATION_EMAIL_ADDRESS)

    @classmethod
    def authentication_policy(cls):
        # Find the name and configuration of the integration to be used
        # when doing authentication
        name = cls.policy(cls.AUTHENTICATION)
        if not name:
            # Authentication does not happen locally in this system.
            return {}
        integration = cls.integration(name, required=True)
        integration = dict(integration)
        integration[cls.NAME] = name
        return integration

    @classmethod
    def load(cls):
        CoreConfiguration.load()
        cls.instance = CoreConfiguration.instance

@contextlib.contextmanager
def empty_config():
    with core_empty_config({}, [CoreConfiguration, Configuration]) as i:
        yield i

@contextlib.contextmanager
def temp_config(new_config=None):
    with core_temp_config(new_config, [CoreConfiguration, Configuration]) as i:
        yield i
