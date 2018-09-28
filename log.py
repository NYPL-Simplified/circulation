from nose.tools import set_trace
import datetime
import logging
import json
import os
import socket
from flask_babel import lazy_gettext as _
from config import Configuration
from StringIO import StringIO
from loggly.handlers import HTTPSHandler as LogglyHandler


class JSONFormatter(logging.Formatter):
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    if len(fqdn) > len(hostname):
        hostname = fqdn

    def __init__(self, app_name):
        super(JSONFormatter, self).__init__()
        self.app_name = app_name or LogConfiguration.DEFAULT_APP_NAME

    def format(self, record):
        message = record.msg
        if record.args:
            try:
                message = record.msg % record.args
            except TypeError, e:
                raise e
        data = dict(
            host=self.hostname,
            app=self.app_name,
            name=record.name,
            level=record.levelname,
            filename=record.filename,
            message=message,
            timestamp=datetime.datetime.utcnow().isoformat()
        )
        if record.exc_info:
            data['traceback'] = self.formatException(record.exc_info)
        return json.dumps(data)

class UTF8Formatter(logging.Formatter):
    """Encode all Unicode output to UTF-8 to prevent encoding errors."""
    def format(self, record):
        try:
            data = super(UTF8Formatter, self).format(record)
        except Exception, e:
            data = super(UTF8Formatter, self).format(record)
        if isinstance(data, unicode):
            data = data.encode("utf8")
        return data

class Logger(object):

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

    DEFAULT_APP_NAME = 'simplified'

class SysLogger(Logger):

    NAME = 'sysLog'

    JSON_LOG_FORMAT = 'json'
    TEXT_LOG_FORMAT = 'text'

    DEFAULT_MESSAGE_TEMPLATE = "%(asctime)s:%(name)s:%(levelname)s:%(filename)s:%(message)s"
    # Settings for the integration with protocol=INTERNAL_LOGGING
    LOG_FORMAT = 'log_format'
    LOG_MESSAGE_TEMPLATE = 'message_template'

    SETTINGS = [
        {
            "key": LOG_FORMAT, "label": _("Log Format"), "type": "select",
            "options": [
                { "key": JSON_LOG_FORMAT, "label": _("json") },
                { "key": TEXT_LOG_FORMAT, "label": _("text") }
            ]
        },
        {
            "key": LOG_MESSAGE_TEMPLATE, "label": _("template"),
            "default": DEFAULT_MESSAGE_TEMPLATE
        }
    ]

    SITEWIDE = True

    @classmethod
    def _defaults(cls, testing=False):
        """Return default log configuration values."""
        if testing:
            internal_log_format = cls.TEXT_LOG_FORMAT
        else:
            internal_log_format = cls.JSON_LOG_FORMAT
        message_template = cls.DEFAULT_MESSAGE_TEMPLATE
        internal_log_level = cls.INFO
        database_log_level = cls.WARN
        return (internal_log_level, internal_log_format, database_log_level,
            message_template)

    @classmethod
    def from_configuration(cls, _db, testing=False):
        from model import (ExternalIntegration, ConfigurationSetting)
        (internal_log_level, internal_log_format, database_log_level,
            message_template) = cls._defaults(testing)
        app_name = cls.DEFAULT_APP_NAME

        if _db and not testing:
            goal = ExternalIntegration.LOGGING_GOAL
            internal = ExternalIntegration.lookup(
                _db, ExternalIntegration.INTERNAL_LOGGING, goal
            )

            if internal:
                internal_log_format = (
                    internal.setting(cls.LOG_FORMAT).value
                    or internal_log_format
                )
                message_template = (
                    internal.setting(cls.LOG_MESSAGE_TEMPLATE).value
                    or message_template
                )
                internal_log_level = (
                    ConfigurationSetting.sitewide(_db, Configuration.LOG_LEVEL).value
                    or internal_log_level
                )
                database_log_level = (
                    ConfigurationSetting.sitewide(_db, Configuration.DATABASE_LOG_LEVEL).value
                    or database_log_level
                )
                app_name = ConfigurationSetting.sitewide(_db, Configuration.LOG_APP_NAME).value or app_name

        handler = logging.StreamHandler()
        cls.set_formatter(handler, internal_log_format, message_template, app_name)

        return (handler, internal_log_level, database_log_level)

    @classmethod
    def set_formatter(cls, handler, log_format, message_template, app_name):
        """Tell the given `handler` to format its log messages in a
        certain way.
        """
        if (log_format == cls.JSON_LOG_FORMAT):
            formatter = JSONFormatter(app_name)
        else:
            formatter = UTF8Formatter(message_template)
        handler.setFormatter(formatter)

class Loggly(Logger):

    NAME = "Loggly"
    DEFAULT_LOGGLY_URL = "https://logs-01.loggly.com/inputs/%(token)s/tag/python/"

    USER = 'user'
    PASSWORD = 'password'
    URL = 'url'

    SETTINGS = [
        { "key": USER, "label": _("Username") },
        { "key": PASSWORD, "label": _("Password") },
        { "key": URL, "label": _("URL") },
    ]

    SITEWIDE = True

    @classmethod
    def from_configuration(cls, _db, testing=False):
        loggly = None
        from model import (ExternalIntegration, ConfigurationSetting)

        app_name = cls.DEFAULT_APP_NAME
        if _db and not testing:
            goal = ExternalIntegration.LOGGING_GOAL
            loggly = ExternalIntegration.lookup(
                _db, ExternalIntegration.LOGGLY, goal
            )
            app_name = ConfigurationSetting.sitewide(_db, Configuration.LOG_APP_NAME).value or app_name

        if loggly:
            loggly = Loggly.loggly_handler(loggly)
            cls.set_formatter(loggly, app_name)

        return loggly

    @classmethod
    def loggly_handler(cls, externalintegration):
        """Turn a Loggly ExternalIntegration into a log handler.
        """
        token = externalintegration.password
        url = externalintegration.url or cls.DEFAULT_LOGGLY_URL
        if not url:
            raise CannotLoadConfiguration(
                "Loggly integration configured but no URL provided."
            )
        try:
            url = cls._interpolate_loggly_url(url, token)
        except (TypeError, KeyError), e:
            raise CannotLoadConfiguration(
                "Cannot interpolate token %s into loggly URL %s" % (
                    token, url,
                )
            )
        return LogglyHandler(url)

    @classmethod
    def _interpolate_loggly_url(cls, url, token):
        if '%s' in url:
            return url % token
        if '%(' in url:
            return url % dict(token=token)

        # Assume the token is already in the URL.
        return url

    @classmethod
    def set_formatter(cls, handler, app_name):
        """Tell the given `handler` to format its log messages in a
        certain way.
        """
        formatter = JSONFormatter(app_name)
        handler.setFormatter(formatter)

class LogConfiguration(object):
    """Configures the active Python logging handlers based on logging
    configuration from the database.
    """

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

    # The default value to put into the 'app' field of JSON-format logs,
    # unless LOG_APP_NAME overrides it.
    DEFAULT_APP_NAME = 'simplified'
    LOG_APP_NAME = 'log_app'

    # Settings for the integration with protocol=INTERNAL_LOGGING
    LOG_LEVEL = 'log_level'
    DATABASE_LOG_LEVEL = 'database_log_level'
    LOG_LEVEL_UI = [
        { "key": DEBUG, "value": _("Debug") },
        { "key": INFO, "value": _("Info") },
        { "key": WARN, "value": _("Warn") },
        { "key": ERROR, "value": _("Error") },
    ]

    SITEWIDE_SETTINGS = [
        { "key": LOG_LEVEL, "label": _("Log Level"), "type": "select",
          "options": LOG_LEVEL_UI, "default": INFO,
        },
        { "key": LOG_APP_NAME, "label": _("Log Application name"),
          "description": _("Log messages originating from this application will be tagged with this name. If you run multiple instances, giving each one a different application name will help you determine which instance is having problems."),
          "default": DEFAULT_APP_NAME,
        },
        { "key": DATABASE_LOG_LEVEL, "label": _("Database Log Level"),
          "type": "select", "options": LOG_LEVEL_UI,
          "description": _("Database logs are extremely verbose, so unless you're diagnosing a database-related problem, it's a good idea to set a higher log level for database messages."),
          "default": WARN,
        },
    ]

    @classmethod
    def initialize(cls, _db, testing=False):
        """Make the logging handlers reflect the current logging rules
        as configured in the database.

        :param _db: A database connection. If this is None, the default logging
        configuration will be used.

        :param testing: True if unit tests are currently running; otherwise
        False.
        """
        log_level, database_log_level, new_handlers = (
            cls.from_configuration(_db, testing)
        )

        # Replace the set of handlers associated with the root logger.
        logger = logging.getLogger()
        logger.setLevel(log_level)
        old_handlers = list(logger.handlers)
        for handler in new_handlers:
            logger.addHandler(handler)
            handler.setLevel(log_level)
        for handler in old_handlers:
            logger.removeHandler(handler)

        # Set the loggers for various verbose libraries to the database
        # log level, which is probably higher than the normal log level.
        for logger in (
                'sqlalchemy.engine', 'elasticsearch',
                'requests.packages.urllib3.connectionpool',
        ):
            logging.getLogger(logger).setLevel(database_log_level)

        # These loggers can cause infinite loops if they're set to
        # DEBUG, because their log is triggered during the process of
        # logging something to Loggly. These loggers will never have their
        # log level set lower than WARN.
        if database_log_level == cls.ERROR:
            loop_prevention_log_level = cls.ERROR
        else:
            loop_prevention_log_level = cls.WARN
        for logger in ['urllib3.connectionpool']:
            logging.getLogger(logger).setLevel(loop_prevention_log_level)
        return log_level

    @classmethod
    def from_configuration(cls, _db, testing=False):
        """Return the logging policy as configured in the database.

        :param _db: A database connection. If None, the default
        logging policy will be used.

        :param testing: A boolean indicating whether a unit test is
        happening right now. If True, the database configuration will
        be ignored in favor of a known test-friendly policy. (It's
        okay to pass in False during a test *of this method*.)

        :return: A 3-tuple (internal_log_level, database_log_level,
        handlers). `internal_log_level` is the log level to be used
        for most log messages. `database_log_level` is the log level
        to be applied to the loggers for the database connector and
        other verbose third-party libraries. `handlers` is a list of
        Handler objects that will be associated with the top-level
        logger.
        """

        handlers = []

        (sysLogglerHandler, internal_log_level, database_log_level) = SysLogger.from_configuration(_db, testing)
        handlers.append(sysLogglerHandler)
        loggly = Loggly.from_configuration(_db, testing)
        if loggly:
            handlers.append(loggly)

        return internal_log_level, database_log_level, handlers
