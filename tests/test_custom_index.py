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

    def test_default_registry(self):
        """Verify the default contents of the registry."""
        eq_(
            {COPPAGate.PROTOCOL : COPPAGate},
            CustomIndexView.BY_PROTOCOL
        )


    def test_for_library(self):
        m = CustomIndexView.for_library

        # Set up a mock CustomView so we can watch it being
        # instantiated.
        class MockCustomIndexView(object):
            PROTOCOL = self._str
            def __init__(self, library, integration):
                self.instantiated_with = (library, integration)
        CustomIndexView.register(MockCustomIndexView)

        # By default, a library has no CustomIndexView.
        eq_(None, m(self._default_library))

        # But if a library has an ExternalIntegration that corresponds
        # to a registered CustomIndexView...
        integration = self._external_integration(
            MockCustomIndexView.PROTOCOL, CustomIndexView.GOAL,
            libraries=[self._default_library]
        )

        # A CustomIndexView of the appropriate class is instantiated
        # and returned.
        view = m(self._default_library)
        assert isinstance(view, MockCustomIndexView)
        eq_((self._default_library, integration), view.instantiated_with)

