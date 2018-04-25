from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from api.custom_index import (
    CustomIndexView,
    COPPAGate,
)

from . import DatabaseTest

class TestCustomIndexView(DatabaseTest):

    def test_register(self):
        c = CustomIndexView
        old_registry = c.BY_PROTOCOL
        c.BY_PROTOCOL = {}

        class Mock1(object):
            PROTOCOL = "A protocol"

        class Mock2(object):
            PROTOCOL = "A protocol"
        
        c.register(Mock1)
        eq_(Mock1, c.BY_PROTOCOL[Mock1.PROTOCOL])

        assert_raises_regexp(ValueError,
                             "Duplicate index view for protocol: A protocol",
                             c.register, Mock2)
        c.BY_PROTOCOL = old_registry
