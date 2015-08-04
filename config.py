import os
import json
import logging

class CannotLoadConfigurationFile(Exception):
    pass

class ConfigurationFile(object):

    log = logging.getLogger("Configuration file loader")

    @classmethod
    def load(cls, root_directory):
        cfv = 'CONFIGURATION_FILE'
        if cfv in os.environ:
            # If there some indication that a configuration file
            # should be present, any failure to load it is treated
            # as a very serious error error.
            config_path = os.environ[cfv]
            if not config_path.startswith('/'):
                # Path is relative to the application root.
                config_path = os.path.join(root_directory, config_path)
            if not os.path.exists(config_path):
                raise CannotLoadConfigurationFile(
                    "Could not locate configuration file %s" % config_path
                )
            try:
                configuration = json.load(open(config_path))
            except Exception, e:
                raise CannotLoadConfigurationFile(
                    "Error loading configuration file %s: %s", 
                    config_path, e,
                )
        else:
            # If you don't define a configuration file, it's fine--it
            # just means you're using the default settings for
            # everything.
            cls.log.warn("No configuration file defined in %s." % cfv)
            configuration = {}

        # If any environment variables are mentioned in the configuration,
        # mirror them to the environment.
        for k, v in configuration.items():
            if k.upper() == k:
                # This is an environment variable. We will make sure
                # it's mirrored into the environment.
                if k in os.environ and os.environ[k] == v:
                    # Configuration value is the same as environment
                    # value. This is a no-op.
                    msg = None
                elif k in os.environ:
                    msg = "Configuration file overwrote environment variable %s"
                else:
                    msg = "Configuration file set environment variable %s"
                if msg:
                    cls.log.info(msg, k)
                os.environ[k] = v

        return configuration

