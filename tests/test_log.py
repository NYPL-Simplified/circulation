import json
import logging
import sys
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace
)

from . import DatabaseTest
from log import (
    UTF8Formatter,
    JSONFormatter,
    LogglyHandler,
    LogConfiguration,
    SysLogger,
    Loggly,
    Logger
)
from model import (
    ExternalIntegration,
    ConfigurationSetting
)
from config import Configuration

class TestJSONFormatter(object):

    def test_format(self):
        formatter = JSONFormatter("some app")
        eq_("some app", formatter.app_name)

        # Cause an exception so we can capture its exc_info()
        try:
            raise ValueError("fake exception")
        except ValueError, e:
            pass
        exception = sys.exc_info()

        record = logging.LogRecord(
            "some logger", logging.DEBUG, "pathname",
            104, "A message", {}, exception, None
        )
        data = json.loads(formatter.format(record))
        eq_("some logger", data['name'])
        eq_("some app", data['app'])
        eq_("DEBUG", data['level'])
        eq_("A message", data['message'])
        eq_("pathname", data['filename'])
        assert 'ValueError: fake exception' in data['traceback']


class TestLogConfiguration(DatabaseTest):

    def test_configuration(self):
        """Loggly.NAME must equal ExternalIntegration.LOGGLY.
        Enforcing this with code would create an import loop,
        but we can enforce it with a test.
        """
        eq_(Loggly.NAME, ExternalIntegration.LOGGLY)

    def loggly_integration(self):
        """Create an ExternalIntegration for a Loggly account."""
        integration = self._external_integration(
            protocol=ExternalIntegration.LOGGLY,
            goal=ExternalIntegration.LOGGING_GOAL
        )
        integration.url = "http://example.com/%s/"
        integration.password = "a_token"
        return integration

    def test_from_configuration(self):
        cls = LogConfiguration
        config = Configuration
        m = cls.from_configuration

        # When logging is configured on initial startup, with no
        # database connection, these are the defaults.
        internal_log_level, database_log_level, [handler] = m(
            None, testing=False
        )
        eq_(cls.INFO, internal_log_level)
        eq_(cls.WARN, database_log_level)
        assert isinstance(handler.formatter, JSONFormatter)

        # The same defaults hold when there is a database connection
        # but nothing is actually configured.
        internal_log_level, database_log_level, [handler] = m(
            self._db, testing=False
        )
        eq_(cls.INFO, internal_log_level)
        eq_(cls.WARN, database_log_level)
        assert isinstance(handler.formatter, JSONFormatter)

        # Let's set up a Loggly integration and change the defaults.
        loggly = self.loggly_integration()
        internal = self._external_integration(
            protocol=ExternalIntegration.INTERNAL_LOGGING,
            goal=ExternalIntegration.LOGGING_GOAL
        )
        ConfigurationSetting.sitewide(self._db, config.LOG_LEVEL).value = config.ERROR
        internal.setting(SysLogger.LOG_FORMAT).value = SysLogger.TEXT_LOG_FORMAT
        ConfigurationSetting.sitewide(self._db, config.DATABASE_LOG_LEVEL).value = config.DEBUG
        ConfigurationSetting.sitewide(self._db, config.LOG_APP_NAME).value = "test app"
        template = "%(filename)s:%(message)s"
        internal.setting(SysLogger.LOG_MESSAGE_TEMPLATE).value = template
        internal_log_level, database_log_level, handlers = m(
            self._db, testing=False
        )
        eq_(cls.ERROR, internal_log_level)
        eq_(cls.DEBUG, database_log_level)
        [loggly_handler] = [x for x in handlers if isinstance(x, LogglyHandler)]
        eq_("http://example.com/a_token/", loggly_handler.url)
        eq_("test app", loggly_handler.formatter.app_name)

        [stream_handler] = [x for x in handlers
                            if isinstance(x, logging.StreamHandler)]
        assert isinstance(stream_handler.formatter, UTF8Formatter)
        eq_(template, stream_handler.formatter._fmt)

        # If testing=True, then the database configuration is ignored,
        # and the log setup is one that's appropriate for display
        # alongside unit test output.
        internal_log_level, database_log_level, [handler] = m(
            self._db, testing=True
        )
        eq_(cls.INFO, internal_log_level)
        eq_(cls.WARN, database_log_level)
        eq_(SysLogger.DEFAULT_MESSAGE_TEMPLATE, handler.formatter._fmt)

    def test_defaults(self):
        cls = SysLogger
        template = SysLogger.DEFAULT_MESSAGE_TEMPLATE

        # Normally the default log level is INFO and log messages are
        # emitted in JSON format.
        eq_(
            (cls.INFO, SysLogger.JSON_LOG_FORMAT, cls.WARN,
             SysLogger.DEFAULT_MESSAGE_TEMPLATE),
            cls._defaults(testing=False)
        )

        # When we're running unit tests, the default log level is INFO
        # and log messages are emitted in text format.
        eq_(
            (cls.INFO, SysLogger.TEXT_LOG_FORMAT, cls.WARN,
             SysLogger.DEFAULT_MESSAGE_TEMPLATE),
            cls._defaults(testing=True)
        )

    def test_set_formatter(self):
        # Create a generic handler.
        handler = logging.StreamHandler()

        # Configure it for text output.
        template = '%(filename)s:%(message)s'
        SysLogger.set_formatter(
            handler, SysLogger.TEXT_LOG_FORMAT, template,
            "some app"
        )
        formatter = handler.formatter
        assert isinstance(formatter, UTF8Formatter)
        eq_(template, formatter._fmt)

        # Configure a similar handler for JSON output.
        handler = logging.StreamHandler()
        SysLogger.set_formatter(
            handler, SysLogger.JSON_LOG_FORMAT, template, None
        )
        formatter = handler.formatter
        assert isinstance(formatter, JSONFormatter)
        eq_(LogConfiguration.DEFAULT_APP_NAME, formatter.app_name)

        # In this case the template is irrelevant. The JSONFormatter
        # uses the default format template, but it doesn't matter,
        # because JSONFormatter overrides the format() method.
        eq_('%(message)s', formatter._fmt)

        # Configure a handler for output to Loggly. In this case
        # the format and template are irrelevant.
        handler = LogglyHandler("no-such-url")
        Loggly.set_formatter(handler, "some app")
        formatter = handler.formatter
        assert isinstance(formatter, JSONFormatter)
        eq_("some app", formatter.app_name)

    def test_loggly_handler(self):
        """Turn an appropriate ExternalIntegration into a LogglyHandler."""

        integration = self.loggly_integration()
        handler = Loggly.loggly_handler(integration)
        assert isinstance(handler, LogglyHandler)
        eq_("http://example.com/a_token/", handler.url)

        # Remove the loggly handler's .url, and the default URL will
        # be used.
        integration.url = None
        handler = Loggly.loggly_handler(integration)
        eq_(Loggly.DEFAULT_LOGGLY_URL % dict(token="a_token"),
            handler.url)

    def test_interpolate_loggly_url(self):
        m = Loggly._interpolate_loggly_url

        # We support two string interpolation techniques for combining
        # a token with a URL.
        eq_("http://foo/token/bar/", m("http://foo/%s/bar/", "token"))
        eq_("http://foo/token/bar/", m("http://foo/%(token)s/bar/", "token"))

        # If the URL contains no string interpolation, we assume the token's
        # already in there.
        eq_("http://foo/othertoken/bar/",
            m("http://foo/othertoken/bar/", "token"))

        # Anything that doesn't fall under one of these cases will raise an
        # exception.
        assert_raises(TypeError, m, "http://%s/%s", "token")
        assert_raises(KeyError, m, "http://%(atoken)s/", "token")
