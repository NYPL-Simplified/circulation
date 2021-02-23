"""Test the self-test functionality.

Self-tests are not unit tests -- they are executed at runtime on a
specific installation. They verify that that installation is properly
configured, not that the code is correct.
"""

import datetime

from . import DatabaseTest

from ..selftest import (
    SelfTestResult,
    HasSelfTests,
)

from ..util.http import IntegrationException

class TestSelfTestResult(DatabaseTest):

    now = datetime.datetime.utcnow()
    future = now + datetime.timedelta(seconds=5)

    def test_success_representation(self):
        """Show the string and dictionary representations of a successful
        test result.
        """
        # A successful result
        result = SelfTestResult("success1")
        result.start = self.now
        result.end = self.future
        result.result = "The result"
        result.success = True
        assert (
            "<SelfTestResult: name='success1' duration=5.00sec success=True result='The result'>" ==
            repr(result))

        # A SelfTestResult may have an associated Collection.
        self._default_collection.name = "CollectionA"
        result.collection = self._default_collection
        assert (
            "<SelfTestResult: name='success1' collection='CollectionA' duration=5.00sec success=True result='The result'>" ==
            repr(result))

        d = result.to_dict
        assert "success1" == d['name']
        assert "The result" == d['result']
        assert 5.0 == d['duration']
        assert True == d['success']
        assert None == d['exception']
        assert 'CollectionA' == d['collection']

        # A test result can be either a string (which will be displayed
        # in a fixed-width font) or a list of strings (which will be hidden
        # behind an expandable toggle).
        list_result = ["list", "of", "strings"]
        result.result = list_result
        d = result.to_dict
        assert list_result == d['result']

        # Other .result values don't make it into the dictionary because
        # it's not defined how to display them.
        result.result = {"a": "dictionary"}
        d = result.to_dict
        assert None == d['result']

    def test_repr_failure(self):
        """Show the string representation of a failed test result."""

        exception = IntegrationException("basic info", "debug info")

        result = SelfTestResult("failure1")
        result.start = self.now
        result.end = self.future
        result.exception = exception
        result.result = "The result"
        assert (
            "<SelfTestResult: name='failure1' duration=5.00sec success=False exception='basic info' debug='debug info' result='The result'>" ==
            repr(result))

        d = result.to_dict
        assert "failure1" == d['name']
        assert "The result" == d['result']
        assert 5.0 == d['duration']
        assert False == d['success']
        assert 'IntegrationException' == d['exception']['class']
        assert 'basic info' == d['exception']['message']
        assert 'debug info' == d['exception']['debug_message']


class TestHasSelfTests(DatabaseTest):

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

            def external_integration(self, _db):
                """This integration will be used to store the test results."""
                return self.integration

            def _run_self_tests(self, _db):
                self._run_self_tests_called_with = _db
                return [SelfTestResult("a test result")]
        mock_db = object()

        # This integration will be used to store the test results.
        integration = self._external_integration(self._str)
        Tester.integration = integration

        # By default, the default constructor is instantiated and its
        # _run_self_tests method is called.
        data, [setup, test] = Tester.run_self_tests(
            mock_db, extra_arg="a value"
        )
        assert mock_db == setup.result._run_self_tests_called_with

        # There are two results -- `setup` from the initial setup
        # and `test` from the _run_self_tests call.
        assert "Initial setup." == setup.name
        assert True == setup.success
        assert "a value" == setup.result.invoked_with
        assert "a test result" == test.name

        # The `data` variable contains a dictionary describing the test
        # suite as a whole.
        assert data['duration'] < 1
        for key in 'start', 'end':
            assert key in data

        # `data['results']` contains dictionary versions of the self-tests
        # that were returned separately.
        r1, r2 = data['results']
        assert r1 == setup.to_dict
        assert r2 == test.to_dict

        # A JSON version of `data` is stored in the
        # ExternalIntegration returned by the external_integration()
        # method.
        [result_setting] = integration.settings
        assert HasSelfTests.SELF_TEST_RESULTS_SETTING == result_setting.key
        assert data == result_setting.json_value

        # Remove the testing integration to show what happens when
        # HasSelfTests doesn't support the storage of test results.
        Tester.integration = None
        result_setting.value = "this value will not be changed"

        # You can specify a different class method to use as the
        # constructor. Once the object is instantiated, the same basic
        # code runs.
        data, [setup, test] = Tester.run_self_tests(
            mock_db, Tester.good_alternate_constructor,
            another_extra_arg="another value"
        )
        assert "Initial setup." == setup.name
        assert True == setup.success
        assert None == setup.result.invoked_with
        assert "another value" == setup.result.another_extra_arg
        assert "a test result" == test.name

        # Since the HasSelfTests object no longer has an associated
        # ExternalIntegration, the test results are not persisted
        # anywhere.
        assert "this value will not be changed" == result_setting.value

        # If there's an exception in the constructor, the result is a
        # single SelfTestResult describing that failure. Since there is
        # no instance, _run_self_tests can't be called.
        data, [result] = Tester.run_self_tests(
            mock_db, Tester.bad_alternate_constructor,
        )
        assert isinstance(result, SelfTestResult)
        assert False == result.success
        assert "I don't work!" == unicode(result.exception)

    def test_exception_in_has_self_tests(self):
        """An exception raised in has_self_tests itself is converted into a
        test failure.
        """
        class Tester(HasSelfTests):
            def _run_self_tests(self, _db):
                yield SelfTestResult("everything's ok so far")
                raise Exception("oh no")
                yield SelfTestResult("i'll never be called.")

        status, [init, success, failure] = Tester.run_self_tests(object())
        assert "Initial setup." == init.name
        assert "everything's ok so far" == success.name

        assert "Uncaught exception in the self-test method itself." == failure.name
        assert False == failure.success
        # The Exception was turned into an IntegrationException so that
        # its traceback could be included as debug_message.
        assert isinstance(failure.exception, IntegrationException)
        assert "oh no" == unicode(failure.exception)
        assert failure.exception.debug_message.startswith("Traceback")

    def test_run_test_success(self):
        o = HasSelfTests()
        # This self-test method will succeed.
        def successful_test(arg, kwarg):
            return arg, kwarg
        result = o.run_test(
            "A successful test", successful_test, "arg1", kwarg="arg2"
        )
        assert True == result.success
        assert "A successful test" == result.name
        assert ("arg1", "arg2") == result.result
        assert (result.end-result.start).total_seconds() < 1

    def test_run_test_failure(self):
        o = HasSelfTests()
        # This self-test method will fail.
        def unsuccessful_test(arg, kwarg):
            raise IntegrationException(arg, kwarg)
        result = o.run_test(
            "An unsuccessful test", unsuccessful_test, "arg1", kwarg="arg2"
        )
        assert False == result.success
        assert "An unsuccessful test" == result.name
        assert None == result.result
        assert "arg1" == unicode(result.exception)
        assert "arg2" == result.exception.debug_message
        assert (result.end-result.start).total_seconds() < 1

    def test_test_failure(self):
        o = HasSelfTests()

        # You can pass in an Exception...
        exception = Exception("argh")
        now = datetime.datetime.utcnow()
        result = o.test_failure("a failure", exception)

        # ...which will be turned into an IntegrationException.
        assert "a failure" == result.name
        assert isinstance(result.exception, IntegrationException)
        assert "argh" == unicode(result.exception)
        assert (result.start-now).total_seconds() < 1

        # ... or you can pass in arguments to an IntegrationException
        result = o.test_failure("another failure", "message", "debug")
        assert isinstance(result.exception, IntegrationException)
        assert "message" == unicode(result.exception)
        assert "debug" == result.exception.debug_message

        # Since no test code actually ran, the end time is the
        # same as the start time.
        assert result.start == result.end
