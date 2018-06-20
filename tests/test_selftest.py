"""Test the self-test functionality.

Self-tests are not unit tests -- they are executed at runtime on a
specific installation. They verify that that installation is properly
configured, not that the code is correct.
"""
from nose.tools import (
    eq_,
    set_trace,
)

import datetime

from selftest import (
    SelfTestResult,
    HasSelfTests,
)

from util.http import IntegrationException

class TestSelfTestResult(object):

    now = datetime.datetime.utcnow()
    future = now + datetime.timedelta(seconds=5)

    def test_repr_success(self):
        """Show the string representation of a successful test result."""
        # A successful result
        result = SelfTestResult("success1")
        result.start = self.now
        result.end = self.future
        result.result = "The result"
        result.success = True
        eq_(
            "<SelfTestResult: name='success1' timing=5.00sec success=False result='The result'>",
            repr(result)
        )

    def test_repr_success(self):
        """Show the string representation of a successful test result."""

        exception = IntegrationException("basic info", "debug info")

        result = SelfTestResult("failure1")
        result.start = self.now
        result.end = self.future
        result.exception = exception
        result.result = "The result"
        eq_(
            "<SelfTestResult: name='failure1' timing=5.00sec success=False exception='basic info' debug='debug info' result='The result'>",
            repr(result)
        )


class TestHasSelfTests(object):

    def test_run_self_tests(self):
        """See what might happen when run_self_tests tries to instantiate an
        object and run its self-tests.
        """
        class Tester(HasSelfTests):
            def __init__(self, extra_arg=None):
                """This constructor works."""
                self.invoked_with = (extra_arg)

            @classmethod
            def good_alternate_constructor(self, another_extra_arg=None):
                """This alternate constructor works."""
                tester = Tester()
                tester.another_extra_arg = another_extra_arg
                return tester

            @classmethod
            def bad_alternate_constructor(self):
                """This constructor doesn't work."""
                raise Exception("I don't work!")

            def _run_self_tests(self, _db):
                self._run_self_tests_called_with = _db
                return ["a result"]
        mock_db = object()

        # By default, the default constructor is instantiated and its
        # _run_self_tests method is called.
        [setup, test] = list(
            Tester.run_self_tests(mock_db, extra_arg="a value")
        )
        eq_(mock_db, setup.result._run_self_tests_called_with)

        # There are two results -- one from the initial setup
        # and one from the _run_self_tests call.
        eq_("Initial setup.", setup.name)
        eq_(True, setup.success)
        eq_("a value", setup.result.invoked_with)

        eq_("a result", test)

        # You can specify a different class method to use as the
        # constructor. Once the object is instantiated, the same basic
        # code runs.
        [setup, test] = Tester.run_self_tests(
            mock_db, Tester.good_alternate_constructor,
            another_extra_arg="another value"
        )
        eq_("Initial setup.", setup.name)
        eq_(True, setup.success)
        eq_(None, setup.result.invoked_with)
        eq_("another value", setup.result.another_extra_arg)
        eq_("a result", test)

        # If there's an exception in the constructor, the result is a
        # single SelfTestResult describing that failure. Since there is
        # no instance, _run_self_tests can't be called.
        [result] = Tester.run_self_tests(
            mock_db, Tester.bad_alternate_constructor,
        )
        assert isinstance(result, SelfTestResult)
        eq_(False, result.success)
        eq_("I don't work!", result.exception.message)

    def test_run_test_success(self):
        o = HasSelfTests()
        # This self-test method will succeed.
        def successful_test(arg, kwarg):
            return arg, kwarg
        result = o.run_test(
            "A successful test", successful_test, "arg1", kwarg="arg2"
        )
        eq_(True, result.success)
        eq_("A successful test", result.name)
        eq_(("arg1", "arg2"), result.result)
        assert (result.end-result.start).total_seconds() < 1

    def test_run_test_failure(self):
        o = HasSelfTests()
        # This self-test method will fail.
        def unsuccessful_test(arg, kwarg):
            raise IntegrationException(arg, kwarg)
        result = o.run_test(
            "An unsuccessful test", unsuccessful_test, "arg1", kwarg="arg2"
        )
        eq_(False, result.success)
        eq_("An unsuccessful test", result.name)
        eq_(None, result.result)
        eq_("arg1", result.exception.message)
        eq_("arg2", result.exception.debug_message)
        assert (result.end-result.start).total_seconds() < 1

    def test_test_failure(self):
        o = HasSelfTests()

        # You can pass in an Exception...
        exception = Exception("argh")
        now = datetime.datetime.utcnow()
        result = o.test_failure("a failure", exception)
        eq_("a failure", result.name)
        eq_(exception, result.exception)
        assert (result.start-now).total_seconds() < 1

        # ... or you can pass in arguments to an IntegrationException
        result = o.test_failure("another failure", "message", "debug")
        assert isinstance(result.exception, IntegrationException)
        eq_("message", result.exception.message)
        eq_("debug", result.exception.debug_message)

        # Since no test code actually ran, the end time is the
        # same as the start time.
        eq_(result.start, result.end)
