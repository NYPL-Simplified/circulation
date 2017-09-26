import logging
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
)
from model import (
    ExternalIntegration,
)

class TestLogConfiguration(DatabaseTest):

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
        internal.setting(cls.LOG_LEVEL).value = cls.ERROR
        internal.setting(cls.LOG_FORMAT).value = cls.TEXT_LOG_FORMAT
        internal.setting(cls.DATABASE_LOG_LEVEL).value = cls.DEBUG
        template = "%(filename)s:%(message)s"
        internal.setting(cls.LOG_MESSAGE_TEMPLATE).value = template
        internal_log_level, database_log_level, handlers = m(
            self._db, testing=False
        )
        eq_(cls.ERROR, internal_log_level)
        eq_(cls.DEBUG, database_log_level)
        [loggly_handler] = [x for x in handlers if isinstance(x, LogglyHandler)]
        eq_("http://example.com/a_token/", loggly_handler.url)

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
        eq_(cls.DEBUG, internal_log_level)
        eq_(cls.WARN, database_log_level)
        eq_(cls.DEFAULT_MESSAGE_TEMPLATE, handler.formatter._fmt)

    def test_defaults(self):
        cls = LogConfiguration
        template = cls.DEFAULT_MESSAGE_TEMPLATE

        # Normally the default log level is INFO and log messages are
        # emitted in JSON format.
        eq_(
            (cls.INFO, cls.JSON_LOG_FORMAT, cls.WARN, 
             cls.DEFAULT_MESSAGE_TEMPLATE), 
            cls._defaults(testing=False)
        )

        # When we're running unit tests, the default log level is DEBUG
        # and log messages are emitted in text format.
        eq_(
            (cls.DEBUG, cls.TEXT_LOG_FORMAT, cls.WARN,
             cls.DEFAULT_MESSAGE_TEMPLATE), 
            cls._defaults(testing=True)
        )

    def test_set_formatter(self):
        # Create a generic handler.
        handler = logging.StreamHandler()

        # Configure it for text output.
        template = '%(filename)s:%(message)s'
        LogConfiguration.set_formatter(
            handler, LogConfiguration.TEXT_LOG_FORMAT, template
        )
        formatter = handler.formatter
        assert isinstance(formatter, UTF8Formatter)
        eq_(template, formatter._fmt)

        # Configure a similar handler for JSON output.
        handler = logging.StreamHandler()
        LogConfiguration.set_formatter(
            handler, LogConfiguration.JSON_LOG_FORMAT, template
        )
        formatter = handler.formatter
        assert isinstance(formatter, JSONFormatter)

        # In this case the template is irrelevant. The JSONFormatter
        # uses the default format template, but it doesn't matter,
        # because JSONFormatter overrides the format() method.
        eq_('%(message)s', formatter._fmt)

        # Configure a handler for output to Loggly. In this case
        # the format and template are irrelevant.
        handler = LogglyHandler("no-such-url")
        LogConfiguration.set_formatter(handler, None, None)
        assert isinstance(formatter, JSONFormatter)

    def test_loggly_handler(self):
        """Turn an appropriate ExternalIntegration into a LogglyHandler."""

        integration = self.loggly_integration()
        handler = LogConfiguration.loggly_handler(integration)
        assert isinstance(handler, LogglyHandler)
        eq_("http://example.com/a_token/", handler.url)

        # Remove the loggly handler's .url, and the default URL will
        # be used.
        integration.url = None
        handler = LogConfiguration.loggly_handler(integration)
        eq_(LogConfiguration.DEFAULT_LOGGLY_URL % dict(token="a_token"),
            handler.url)

    def test_interpolate_loggly_url(self):
        m = LogConfiguration._interpolate_loggly_url

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

