from nose.tools import set_trace
import datetime
import logging
import json
import os
import socket
from config import Configuration
from StringIO import StringIO
from loggly.handlers import HTTPSHandler as LogglyHandler

if not Configuration.instance:
    Configuration.load()

class JSONFormatter(logging.Formatter):
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    if len(fqdn) > len(hostname):
        hostname = fqdn
    def format(self, record):
        message = record.msg
        if record.args:
            try:
                message = record.msg % record.args
            except TypeError, e:
                raise e
        data = dict(
            host=self.hostname,
            app="simplified",
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

       
class LogConfiguration(object):
    """Configures the active Python logging handlers based on logging
    configuration from the database.
    """

    DEFAULT_MESSAGE_TEMPLATE = "%(asctime)s:%(name)s:%(levelname)s:%(filename)s:%(message)s"
    DEFAULT_LOGGLY_URL = "https://logs-01.loggly.com/inputs/%(token)s/tag/python/"

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"

    JSON_LOG_FORMAT = 'json'
    TEXT_LOG_FORMAT = 'text'

    # Settings for the integration with protocol=INTERNAL_LOGGING
    LOG_LEVEL = 'log_level'
    LOG_FORMAT = 'log_format'
    DATABASE_LOG_LEVEL = 'database_log_level'
    LOG_MESSAGE_TEMPLATE = 'message_template'

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
        for handler in old_handlers:
            logger.removeHandler(handler)

        # Set the loggers for various verbose libraries to the database
        # log level, which is probably higher than the normal log level.
        for logger in (
                'sqlalchemy.engine', 'elasticsearch', 
                'requests.packages.urllib3.connectionpool',
                'urllib3.connectionpool',
        ):
            logging.getLogger(logger).setLevel(database_log_level)
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

        # Establish defaults, in case the database is not initialized or
        # it is initialized but logging is not configured.
        (internal_log_level, internal_log_format, database_log_level, 
         message_template) = cls._defaults(testing)

        handlers = []
        from model import ExternalIntegration
        if _db and not testing:
            goal = ExternalIntegration.LOGGING_GOAL
            internal = ExternalIntegration.lookup(
                _db, ExternalIntegration.INTERNAL_LOGGING, goal
            )
            loggly = ExternalIntegration.lookup(
                _db, ExternalIntegration.LOGGLY, goal
            )
            if internal:
                internal_log_level = (
                    internal.setting(cls.LOG_LEVEL).value 
                    or internal_log_level
                )
                internal_log_format = (
                    internal.setting(cls.LOG_FORMAT).value 
                    or internal_log_format
                )
                database_log_level = (
                    internal.setting(cls.DATABASE_LOG_LEVEL).value
                    or database_log_level
                )
                message_template = (
                    internal.setting(cls.LOG_MESSAGE_TEMPLATE).value
                    or message_template
                )

            if loggly:
                handlers.append(cls.loggly_handler(loggly))

        # handlers is either empty or it contains a loggly handler.
        # Let's also add a handler that logs to standard error.
        handlers.append(logging.StreamHandler())

        for handler in handlers:
            cls.set_formatter(
                handler, internal_log_format, message_template
            )

        return internal_log_level, database_log_level, handlers

    @classmethod
    def _defaults(cls, testing=False):
        """Return default log configuration values."""
        if testing:
            internal_log_level = 'DEBUG'
            internal_log_format = cls.TEXT_LOG_FORMAT
        else:
            internal_log_level = 'INFO'
            internal_log_format = cls.JSON_LOG_FORMAT
        database_log_level = 'WARN'
        message_template = cls.DEFAULT_MESSAGE_TEMPLATE
        return (internal_log_level, internal_log_format, database_log_level,
                message_template)

    @classmethod
    def set_formatter(cls, handler, log_format, message_template):
        """Tell the given `handler` to format its log messages in a
        certain way.
        """
        if (log_format==cls.JSON_LOG_FORMAT
            or isinstance(handler, LogglyHandler)):
            formatter = JSONFormatter()
        else:
            formatter = UTF8Formatter(message_template)
        handler.setFormatter(formatter)

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
            raise CannotLoadConfiguraiton(
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

