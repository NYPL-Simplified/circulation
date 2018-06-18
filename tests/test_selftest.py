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

