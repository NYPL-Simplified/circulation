"""Define the interfaces used by ExternalIntegration self-tests.
"""
from nose.tools import set_trace
from util.http import IntegrationException
import datetime


class SelfTestResult(object):
    """The result of running a single self-test.

    HasSelfTest.run_self_tests() returns a list of these
    """

    def __init__(self, name):
        # Name of the test.
        self.name = name

        # Set to True when the test runs without raising an exception.
        self.success = False

        # The exception raised, if any.
        self.exception = None

        # The return value of the test method, assuming it ran to
        # completion.
        self.result = None

        # Start time of the test.
        self.start = datetime.datetime.utcnow()

        # End time of the test.
        self.end = None

    def __repr__(self):
        if self.exception:
            if (isinstance(self.exception, IntegrationException)
                and self.exception.debug_message):
                exception = " exception=%r debug=%r" % (
                    self.exception.message, self.exception.debug_message
                )
            else:
                exception = " exception=%r" % self.exception
        else:
            exception = ""
        return "<SelfTestResult: name=%r timing=%.2fsec success=%r%s result=%r>" % (
            self.name, (self.end-self.start).total_seconds(), self.success,
            exception, self.result
        )


class HasSelfTests(object):
    """An object capable of verifying its own setup by running a
    series of self-tests.
    """

    def run_self_tests(self, _db):
        """Run a series of self-tests.

        :return: A list of SelfTestResult objects.
        """
        raise NotImplementedError()

    def run_test(self, name, method, *args, **kwargs):
        """Run a test method, record any exception that happens, and keep
        track of how long the test takes to run.

        :param name: The name of the test to be run.
        :param method: A method to call to run the test.
        :param args: Positional arguments to `method`.
        :param kwargs: Keyword arguments to `method`.

        :return: A filled-in SelfTestResult.
        """
        result = SelfTestResult(name)
        try:
            return_value = method(*args, **kwargs)
            result.success = True
            result.result = return_value
        except Exception, e:
            result.exception = e
            result.success = False
            result.result = None
        finally:
            if not result.end:
                result.end = datetime.datetime.utcnow()
        return result

    def test_failure(self, name, message, debug_message=None):
        """Create a SelfTestResult for a known failure."""
        result = SelfTestResult(name)
        result.end = result.start
        result.success = False
        if isinstance(message, Exception):
            exception = message
        else:
            exception = IntegrationException(message, debug_message)
        result.exception = exception
        return result
