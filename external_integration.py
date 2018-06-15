"""A module to support the classes that implement the code
configured by various ExternalIntegrations.
"""
from nose.tools import set_trace
from config import IntegrationException
import datetime


class SelfTestResult(object):

    def __init__(self, name):
        # Name of the test.
        self.name = name

        # The test ran without raising an exception.
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
                and self.exception.diagnostic):
                exception = " exception=%r diagnostic=%r" % (
                    self.exception.message, self.exception.diagnostic
                )
            else:
                exception = " exception=%r" % self.exception
        else:
            exception = ""
        return "<SelfTestResult: name=%r timing=%.2fsec success=%r%s>" % (
            self.name, (self.end-self.start).total_seconds(), self.success,
            exception
        )


class HasSelfTest(object):
    """An object capable of verifying its own configuration by running a
    self-test.
    """

    def self_test(self, _db):
        """Verify that this integration is properly configured and working.

        :return: A list of SelfTestResult objects.
        """
        raise NotImplementedError()

    def run_test(self, name, method, *args, **kwargs):
        """Run a test method, record any exception that happens, and keep
        track of how long the test takes to run.

        :param name: The name of the test to be run.
        :param method: A method to call to run the test.
        :param args: Position arguments to `method`.
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
