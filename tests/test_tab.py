from testing import DatabaseTest
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
from tab import (
    Tab,
    EbooksTab,
    AudiobooksTab,
)

class TestTab(DatabaseTest):

    def test_defaults(self):
        ebooks, audiobooks = Tab.TABS
        eq_(EbooksTab, ebooks)
        eq_(AudiobooksTab, audiobooks)

        display = Tab.DISPLAY_TITLES
        eq_("Books", display[ebooks])
        eq_("Audiobooks", display[audiobooks])

    def test_register(self):

        class Mock(object):
            pass

        args = [Mock, "Mock!"]

        assert_raises_regexp(
            ValueError, "must define INTERNAL_NAME", Tab.register, *args
        )

        # Test successful registration.
        Mock.INTERNAL_NAME = "a name"
        Tab.register(*args)
        assert Mock in Tab.TABS
        eq_("Mock!", Tab.DISPLAY_TITLES[Mock])
        assert Mock not in Tab.DEFAULT_ENABLED

        # Can't register twice.
        assert_raises_regexp(
            ValueError, "Duplicate tab internal name: a name",
            Tab.register, *args
        )
        
        Tab.unregister(Mock)

        # Test successful registration as a default-enabled tab.
        Tab.register(*args, default_enabled=True)
        assert Mock in Tab.DEFAULT_ENABLED
