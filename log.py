from nose.tools import set_trace
import datetime
import logging
import json
import os
import socket
from flask_babel import lazy_gettext as _
from .config import Configuration
from io import StringIO
from loggly.handlers import HTTPSHandler as LogglyHandler
from watchtower import CloudWatchLogHandler
from boto3.session import Session as AwsSession
from .config import CannotLoadConfiguration
from .model import ExternalIntegration, ConfigurationSetting

class JSONFormatter(logging.Formatter):
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    if len(fqdn) > len(hostname):
        hostname = fqdn

    def __init__(self, app_name):
        super(JSONFormatter, self).__init__()
        self.app_name = app_name or LogConfiguration.DEFAULT_APP_NAME

    def format(self, record):
        def only_native_strings(s):
            """Convert any kind of string-like object to the native string
            implementation in this version of Python. Leave everything
            else alone.

            We've already converted the message to a native string, and
            we don't want to try to interpolate an incompatible type; it
            could lead to a UnicodeDecodeError.
            """
            if isinstance(s, bytes):
                s = s.decode("utf-8")
            return s
        message = only_native_strings(record.msg)

        if record.args:
            record_args = tuple(
                [only_native_strings(arg) for arg in record.args]
            )
            try:
                message = message % record_args
            except Exception as e:
                # There was a problem formatting the log message,
                # which points to a bug. A problem with the logging
                # code shouldn't break the code that actually does the
                # work, but we can't just let this slide -- we need to
                # report the problem so it can be fixed.
                message = "Log message could not be formatted. Exception: %r. Original message: message=%r args=%r" % (
                    e, message, record_args
                )
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


class StringFormatter(logging.Formatter):
    """Encode all output as a string.
    """
    def format(self, record):
        data = super(StringFormatter, self).format(record)
        return str(data)


class Logger(object):
    """Abstract base class for logging"""

    DEFAULT_APP_NAME = 'simplified'

    JSON_LOG_FORMAT = 'json'
    TEXT_LOG_FORMAT = 'text'
    DEFAULT_MESSAGE_TEMPLATE = "%(asctime)s:%(name)s:%(levelname)s:%(filename)s:%(message)s"

    @classmethod
    def set_formatter(cls, handler, app_name=None, log_format=None, message_template=None):
        """Tell the given `handler` to format its log messages in a
        certain way.
        """
        # Initialize defaults
        if log_format is None:
            log_format = cls.JSON_LOG_FORMAT
        if message_template is None:
            message_template = cls.DEFAULT_MESSAGE_TEMPLATE

        if log_format == cls.JSON_LOG_FORMAT:
            formatter = JSONFormatter(app_name)
        else:
            formatter = StringFormatter(message_template)
        handler.setFormatter(formatter)

    @classmethod
    def from_configuration(cls, _db, testing=False):
        """Should be implemented in each logging class."""
        raise NotImplementedError()

class SysLogger(Logger):

    NAME = 'sysLog'

    # Settings for the integration with protocol=INTERNAL_LOGGING
    LOG_FORMAT = 'log_format'
    LOG_MESSAGE_TEMPLATE = 'message_template'

    SETTINGS = [
        {
            "key": LOG_FORMAT, "label": _("Log Format"), "type": "select",
            "options": [
                { "key": Logger.JSON_LOG_FORMAT, "label": _("json") },
                { "key": Logger.TEXT_LOG_FORMAT, "label": _("text") }
            ]
        },
        {
            "key": LOG_MESSAGE_TEMPLATE, "label": _("template"),
            "default": Logger.DEFAULT_MESSAGE_TEMPLATE,
            "required": True,
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
        return internal_log_format, message_template

    @classmethod
    def from_configuration(cls, _db, testing=False):
        (internal_log_format, message_template) = cls._defaults(testing)
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
                app_name = ConfigurationSetting.sitewide(_db, Configuration.LOG_APP_NAME).value or app_name

        handler = logging.StreamHandler()
        cls.set_formatter(handler, log_format=internal_log_format, message_template=message_template, app_name=app_name)
        return handler

class Loggly(Logger):

    NAME = "Loggly"
    DEFAULT_LOGGLY_URL = "https://logs-01.loggly.com/inputs/%(token)s/tag/python/"

    USER = 'user'
    PASSWORD = 'password'
    URL = 'url'

    SETTINGS = [
        { "key": USER, "label": _("Username"), "required": True },
        { "key": PASSWORD, "label": _("Password"), "required": True },
        { "key": URL, "label": _("URL"), "required": True, "format": "url" },
    ]

    SITEWIDE = True

    @classmethod
    def from_configuration(cls, _db, testing=False):
        loggly = None
        from .model import (ExternalIntegration, ConfigurationSetting)

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
        except (TypeError, KeyError) as e:
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

class CloudwatchLogs(Logger):

    NAME = "AWS Cloudwatch Logs"
    GROUP = 'group'
    STREAM = 'stream'
    INTERVAL = 'interval'
    CREATE_GROUP = 'create_group'
    REGION = 'region'
    DEFAULT_REGION = 'us-west-2'
    DEFAULT_INTERVAL = 60
    DEFAULT_CREATE_GROUP = "TRUE"

    # https://docs.aws.amazon.com/general/latest/gr/rande.html#cwl_region
    REGIONS = [
        {"key": "us-east-2",      "label": _("US East (Ohio)")},
        {"key": "us-east-1",      "label": _("US East (N. Virginia)")},
        {"key": "us-west-1",      "label": _("US West (N. California)")},
        {"key": "us-west-2",      "label": _("US West (Oregon)")},
        {"key": "ap-south-1",     "label": _("Asia Pacific (Mumbai)")},
        {"key": "ap-northeast-3", "label": _("Asia Pacific (Osaka-Local)")},
        {"key": "ap-northeast-2", "label": _("Asia Pacific (Seoul)")},
        {"key": "ap-southeast-1", "label": _("Asia Pacific (Singapore)")},
        {"key": "ap-southeast-2", "label": _("Asia Pacific (Sydney)")},
        {"key": "ap-northeast-1", "label": _("Asia Pacific (Tokyo)")},
        {"key": "ca-central-1",   "label": _("Canada (Central)")},
        {"key": "cn-north-1",     "label": _("China (Beijing)")},
        {"key": "cn-northwest-1", "label": _("China (Ningxia)")},
        {"key": "eu-central-1",   "label": _("EU (Frankfurt)")},
        {"key": "eu-west-1",      "label": _("EU (Ireland)")},
        {"key": "eu-west-2",      "label": _("EU (London)")},
        {"key": "eu-west-3",      "label": _("EU (Paris)")},
        {"key": "sa-east-1",      "label": _("South America (Sao Paulo)")},
    ]

    SETTINGS = [
        {
            "key": GROUP,
            "label": _("Log Group"),
            "default": Logger.DEFAULT_APP_NAME,
            "required": True,
        },
        {
            "key": STREAM,
            "label": _("Log Stream"),
            "default": Logger.DEFAULT_APP_NAME,
            "required": True,
        },
        {
            "key": INTERVAL,
            "label": _("Update Interval Seconds"),
            "default": DEFAULT_INTERVAL,
            "required": True,
        },
        {
            "key": REGION,
            "label": _("AWS Region"),
            "type": "select",
            "options": REGIONS,
            "default": DEFAULT_REGION,
            "required": True,
        },
        {
            "key": CREATE_GROUP,
            "label": _("Automatically Create Log Group"),
            "type": "select",
            "options": [
                { "key": "TRUE", "label": _("Yes") },
                { "key": "FALSE", "label": _("No") },
            ],
            "default": True,
            "required": True,
        },
    ]

    SITEWIDE = True

    @classmethod
    def from_configuration(cls, _db, testing=False):
        settings = None
        cloudwatch = None

        app_name = cls.DEFAULT_APP_NAME
        if _db and not testing:
            goal = ExternalIntegration.LOGGING_GOAL
            settings = ExternalIntegration.lookup(
                _db, ExternalIntegration.CLOUDWATCH, goal
            )
            app_name = ConfigurationSetting.sitewide(_db, Configuration.LOG_APP_NAME).value or app_name

        if settings:
            cloudwatch = cls.get_handler(settings, testing)
            cls.set_formatter(cloudwatch, app_name)

        return cloudwatch

    @classmethod
    def get_handler(cls, settings, testing=False):
        """Turn ExternalIntegration into a log handler.
        """
        group = settings.setting(cls.GROUP).value or cls.DEFAULT_APP_NAME
        stream = settings.setting(cls.STREAM).value or cls.DEFAULT_APP_NAME
        interval = settings.setting(cls.INTERVAL).value or cls.DEFAULT_INTERVAL
        region = settings.setting(cls.REGION).value or cls.DEFAULT_REGION
        create_group = settings.setting(cls.CREATE_GROUP).value or cls.DEFAULT_CREATE_GROUP

        try:
            interval = int(interval)
            if interval <= 0:
                raise CannotLoadConfiguration(
                    "AWS Cloudwatch Logs interval must be a positive integer."
                )
        except ValueError:
            raise CannotLoadConfiguration(
                "AWS Cloudwatch Logs interval configuration must be an integer."
            )
        session = AwsSession(region_name=region)
        handler = CloudWatchLogHandler(
            log_group=group,
            stream_name=stream,
            send_interval=interval,
            boto3_session=session,
            create_log_group=create_group == "TRUE"
        )
        # Add a filter that makes sure no messages from botocore are processed by
        # the cloudwatch logs integration, as these messages can lead to an infinite loop.
        class BotoFilter(logging.Filter):
            def filter(self, record):
                return not record.name.startswith('botocore')
        handler.addFilter(BotoFilter())
        return handler

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

    DEFAULT_LOG_LEVEL = INFO
    DEFAULT_DATABASE_LOG_LEVEL = WARN

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

        :param testing: True if unit tests are currently running; otherwise False.
        """
        log_level, database_log_level, new_handlers, errors = (
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
                'botocore'
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

        # If we had an error creating any log handlers report it
        for error in errors:
            logging.getLogger().error(error)

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
        log_level = cls.DEFAULT_LOG_LEVEL
        database_log_level = cls.DEFAULT_DATABASE_LOG_LEVEL

        if _db and not testing:
            log_level = (
                ConfigurationSetting.sitewide(_db, Configuration.LOG_LEVEL).value
                or log_level
            )
            database_log_level = (
                ConfigurationSetting.sitewide(_db, Configuration.DATABASE_LOG_LEVEL).value
                or database_log_level
            )

        loggers = [SysLogger, Loggly, CloudwatchLogs]
        handlers = []
        errors = []

        for logger in loggers:
            try:
                handler = logger.from_configuration(_db, testing)
                if handler:
                    handlers.append(handler)
            except Exception as e:
                errors.append(
                    "Error creating logger %s %s" % (logger.NAME, str(e))
                )

        return log_level, database_log_level, handlers, errors
