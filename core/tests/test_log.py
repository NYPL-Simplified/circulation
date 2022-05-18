# encoding: utf-8
import json
import logging
import sys

import pytest

from ..testing import DatabaseTest
from ..log import (
    StringFormatter,
    JSONFormatter,
    LogglyHandler,
    CloudWatchLogHandler,
    LogConfiguration,
    SysLogger,
    Loggly,
    CloudwatchLogs,
    Logger,
    CannotLoadConfiguration,
)
from ..model import (
    ExternalIntegration,
    ConfigurationSetting
)
from ..config import Configuration

class TestJSONFormatter(object):

    def test_format(self):
        formatter = JSONFormatter("some app")
        assert "some app" == formatter.app_name

        exc_info = None
        # Cause an exception so we can capture its exc_info()
        try:
            raise ValueError("fake exception")
        except ValueError as e:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            "some logger", logging.DEBUG, "pathname",
            104, "A message", {}, exc_info, None
        )
        data = json.loads(formatter.format(record))
        assert "some logger" == data['name']
        assert "some app" == data['app']
        assert "DEBUG" == data['level']
        assert "A message" == data['message']
        assert "pathname" == data['filename']
        assert 'ValueError: fake exception' in data['traceback']

    def test_format_with_different_types_of_strings(self):
        # As long as all data is either Unicode or UTF-8, any combination
        # of Unicode and bytestrings can be combined in log messages.

        unicode_message = "An important snowman: %s"
        byte_message = unicode_message.encode("utf8")

        unicode_snowman = "☃"
        utf8_snowman = unicode_snowman.encode("utf8")

        # Test every combination of Unicode and bytestring message and
        # argument.
        formatter = JSONFormatter("some app")
        for msg, args in (
            (unicode_message, utf8_snowman),
            (unicode_message, unicode_snowman),
            (byte_message, utf8_snowman),
            (byte_message, unicode_snowman),
        ):
            record = logging.LogRecord(
                "some logger", logging.DEBUG, "pathname",
                104, msg, (args,), None, None
            )
            data = json.loads(formatter.format(record))
            # The resulting data is always a Unicode string.
            assert "An important snowman: ☃" == data['message']


class TestLogConfiguration(DatabaseTest):

    def test_configuration(self):
        """Loggly.NAME must equal ExternalIntegration.LOGGLY.
        Enforcing this with code would create an import loop,
        but we can enforce it with a test.
        """
        assert Loggly.NAME == ExternalIntegration.LOGGLY

    def loggly_integration(self):
        """Create an ExternalIntegration for a Loggly account."""
        integration = self._external_integration(
            protocol=ExternalIntegration.LOGGLY,
            goal=ExternalIntegration.LOGGING_GOAL
        )
        integration.url = "http://example.com/%s/"
        integration.password = "a_token"
        return integration

    def cloudwatch_integration(self):
        """Create an ExternalIntegration for a Cloudwatch account."""
        integration = self._external_integration(
            protocol=ExternalIntegration.CLOUDWATCH,
            goal=ExternalIntegration.LOGGING_GOAL
        )

        integration.set_setting(CloudwatchLogs.CREATE_GROUP, "FALSE")
        return integration

    def test_from_configuration(self):
        cls = LogConfiguration
        config = Configuration
        m = cls.from_configuration

        # When logging is configured on initial startup, with no
        # database connection, these are the defaults.
        internal_log_level, database_log_level, [handler], errors = m(
            None, testing=False
        )
        assert cls.INFO == internal_log_level
        assert cls.WARN == database_log_level
        assert [] == errors
        assert isinstance(handler.formatter, JSONFormatter)

        # The same defaults hold when there is a database connection
        # but nothing is actually configured.
        internal_log_level, database_log_level, [handler], errors = m(
            self._db, testing=False
        )
        assert cls.INFO == internal_log_level
        assert cls.WARN == database_log_level
        assert [] == errors
        assert isinstance(handler.formatter, JSONFormatter)

        # Let's set up a integrations and change the defaults.
        self.loggly_integration()
        self.cloudwatch_integration()
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
        internal_log_level, database_log_level, handlers, errors = m(
            self._db, testing=False
        )
        assert cls.ERROR == internal_log_level
        assert cls.DEBUG == database_log_level
        [loggly_handler] = [x for x in handlers if isinstance(x, LogglyHandler)]
        assert "http://example.com/a_token/" == loggly_handler.url
        assert "test app" == loggly_handler.formatter.app_name

        [cloudwatch_handler] = [x for x in handlers if isinstance(x, CloudWatchLogHandler)]
        assert "simplified" == cloudwatch_handler.stream_name
        assert "simplified" == cloudwatch_handler.log_group
        assert 60 == cloudwatch_handler.send_interval

        [stream_handler] = [x for x in handlers
                            if isinstance(x, logging.StreamHandler)]
        assert isinstance(stream_handler.formatter, StringFormatter)
        assert template == stream_handler.formatter._fmt

        # If testing=True, then the database configuration is ignored,
        # and the log setup is one that's appropriate for display
        # alongside unit test output.
        internal_log_level, database_log_level, [handler], errors = m(
            self._db, testing=True
        )
        assert cls.INFO == internal_log_level
        assert cls.WARN == database_log_level
        assert SysLogger.DEFAULT_MESSAGE_TEMPLATE == handler.formatter._fmt

    def test_syslog_defaults(self):
        cls = SysLogger

        # Normally log messages are emitted in JSON format.
        assert (
            (SysLogger.JSON_LOG_FORMAT, SysLogger.DEFAULT_MESSAGE_TEMPLATE) ==
            cls._defaults(testing=False))

        # When we're running unit tests, log messages are emitted in text format.
        assert (
            (SysLogger.TEXT_LOG_FORMAT, SysLogger.DEFAULT_MESSAGE_TEMPLATE) ==
            cls._defaults(testing=True))

    def test_set_formatter(self):
        # Create a generic handler.
        handler = logging.StreamHandler()

        # Configure it for text output.
        template = '%(filename)s:%(message)s'
        SysLogger.set_formatter(
            handler,
            log_format=SysLogger.TEXT_LOG_FORMAT,
            message_template=template,
            app_name="some app"
        )
        formatter = handler.formatter
        assert isinstance(formatter, StringFormatter)
        assert template == formatter._fmt

        # Configure a similar handler for JSON output.
        handler = logging.StreamHandler()
        SysLogger.set_formatter(
            handler, log_format=SysLogger.JSON_LOG_FORMAT, message_template=template
        )
        formatter = handler.formatter
        assert isinstance(formatter, JSONFormatter)
        assert LogConfiguration.DEFAULT_APP_NAME == formatter.app_name

        # In this case the template is irrelevant. The JSONFormatter
        # uses the default format template, but it doesn't matter,
        # because JSONFormatter overrides the format() method.
        assert '%(message)s' == formatter._fmt

        # Configure a handler for output to Loggly. In this case
        # the format and template are irrelevant.
        handler = LogglyHandler("no-such-url")
        Loggly.set_formatter(handler, "some app")
        formatter = handler.formatter
        assert isinstance(formatter, JSONFormatter)
        assert "some app" == formatter.app_name

    def test_loggly_handler(self):
        """Turn an appropriate ExternalIntegration into a LogglyHandler."""

        integration = self.loggly_integration()
        handler = Loggly.loggly_handler(integration)
        assert isinstance(handler, LogglyHandler)
        assert "http://example.com/a_token/" == handler.url

        # Remove the loggly handler's .url, and the default URL will
        # be used.
        integration.url = None
        handler = Loggly.loggly_handler(integration)
        assert (Loggly.DEFAULT_LOGGLY_URL % dict(token="a_token") ==
            handler.url)

    def test_cloudwatch_handler(self):
        """Turn an appropriate ExternalIntegration into a CloudWatchLogHandler."""

        integration = self.cloudwatch_integration()
        integration.set_setting(CloudwatchLogs.GROUP, "test_group")
        integration.set_setting(CloudwatchLogs.STREAM, "test_stream")
        integration.set_setting(CloudwatchLogs.INTERVAL, 120)
        integration.set_setting(CloudwatchLogs.REGION, 'us-east-2')
        handler = CloudwatchLogs.get_handler(integration, testing=True)
        assert isinstance(handler, CloudWatchLogHandler)
        assert "test_stream" == handler.stream_name
        assert "test_group" == handler.log_group
        assert 120 == handler.send_interval

        integration.setting(CloudwatchLogs.INTERVAL).value = -10
        pytest.raises(CannotLoadConfiguration, CloudwatchLogs.get_handler, integration, True)
        integration.setting(CloudwatchLogs.INTERVAL).value = "a string"
        pytest.raises(CannotLoadConfiguration, CloudwatchLogs.get_handler, integration, True)

    def test_interpolate_loggly_url(self):
        m = Loggly._interpolate_loggly_url

        # We support two string interpolation techniques for combining
        # a token with a URL.
        assert "http://foo/token/bar/" == m("http://foo/%s/bar/", "token")
        assert "http://foo/token/bar/" == m("http://foo/%(token)s/bar/", "token")

        # If the URL contains no string interpolation, we assume the token's
        # already in there.
        assert ("http://foo/othertoken/bar/" ==
            m("http://foo/othertoken/bar/", "token"))

        # Anything that doesn't fall under one of these cases will raise an
        # exception.
        pytest.raises(TypeError, m, "http://%s/%s", "token")
        pytest.raises(KeyError, m, "http://%(atoken)s/", "token")

    def test_cloudwatch_initialization_exception(self):
        # Make sure if an exception is thrown during initalization its caught.

        integration = self.cloudwatch_integration()
        integration.set_setting(CloudwatchLogs.CREATE_GROUP, "TRUE")
        internal_log_level, database_log_level, [handler], [error] = LogConfiguration.from_configuration(
            self._db, testing=False
        )
        assert isinstance(handler, logging.StreamHandler)
        assert 'Error creating logger AWS Cloudwatch Logs Unable to locate credentials' == error
