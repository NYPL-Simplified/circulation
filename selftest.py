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

    @classmethod
    def run_self_tests(cls, _db, constructor_method=None, *args, **kwargs):
        """Instantiate this class and call _run_self_tests on it.

        :param _db: A database connection. Will be passed into
        _run_self_tests as well as the first argument of the constructor
        method.

        :param constructor_method: Method to use to instantiate the
        class, if different from the default constructor.

        :return: An iterator of SelfTestResult objects, starting with
        the attempt to instantiate the test class in the first place.
        """
        constructor_method = constructor_method or cls
        result = SelfTestResult("Initial setup.")
        result.start = datetime.datetime.utcnow()
        instance = None
        try:
            instance = constructor_method(_db, *args, **kwargs)
            result.success = True
            result.result = instance
        except Exception, e:
            result.exception = e
            result.success = False
        finally:
            result.end = datetime.datetime.utcnow()
        yield result

        if instance:
            for result in instance._run_self_tests(_db):
                yield result

    def _run_self_tests(self, _db):
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

    @classmethod
    def test_failure(cls, name, message, debug_message=None):
        """Create a SelfTestResult for a known failure.

        This is useful when you can't even get the data necessary to
        run a test method.
        """
        result = SelfTestResult(name)
        result.end = result.start
        result.success = False
        if isinstance(message, Exception):
            exception = message
        else:
            exception = IntegrationException(message, debug_message)
        result.exception = exception
        return result
