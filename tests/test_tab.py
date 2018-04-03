from testing import DatabaseTest
from nose.tools import (
    assert_raises_regexp,
    eq_,
)
from tab import Tab

class TestTab(DatabaseTest):

    def test_register(self):

        class Mock(object):
            pass

        assert_raises_regexp(
            ValueError, "must define EXTERNAL_NAME", Tab.register, Mock
        )

        # Test success.
        Mock.EXTERNAL_NAME = "a name"
        Tab.register(Mock)

        set_trace()
        

    
