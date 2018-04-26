from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from flask import Response

from core.model import ConfigurationSetting

from core.util.opds_writer import OPDSFeed

from api.config import CannotLoadConfiguration
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


class TestCOPPAGate(DatabaseTest):

    def setup(self):
        super(TestCOPPAGate, self).setup()
        # Configure a COPPAGate for the default library.
        self.integration = self._external_integration(
            COPPAGate.PROTOCOL, CustomIndexView.GOAL,
            libraries=[self._default_library]
        )
        self.lane1 = self._lane()
        self.lane2 = self._lane()
        m = ConfigurationSetting.for_library_and_externalintegration
        m(
            self._db, COPPAGate.REQUIREMENT_MET_LANE, self._default_library, 
            self.integration
        ).value = self.lane1.id
        m(
            self._db, COPPAGate.REQUIREMENT_NOT_MET_LANE, self._default_library,
            self.integration
        ).value = self.lane2.id
        self.gate = COPPAGate(self._default_library, self.integration)

        self.url_for_calls = []

    def mock_url_for(self, controller, library_short_name, **kwargs):
        """Create a real-looking URL for any random controller."""
        self.url_for_calls.append((controller, library_short_name, kwargs))
        query = "&".join(
            ["%s=%s" % (k,v) for k, v in sorted(kwargs.items())]
        )
        return "http://%s/%s?%s" % (library_short_name, controller, query)

    def test_lane_loading(self):
        # The default setup loads lane IDs properly.
        eq_(self.lane1.id, self.gate.yes_lane_id)
        eq_(self.lane2.id, self.gate.no_lane_id)

        # If a lane isn't associated with the right library, the
        # COPPAGate is misconfigured and cannot be instantiated.
        library = self._library()
        self.lane1.library = library
        self._db.commit()
        assert_raises_regexp(
            CannotLoadConfiguration,
            "Lane .* is for the wrong library",
            COPPAGate,
            self._default_library, self.integration
        )
        self.lane1.library_id = self._default_library.id

        # If the lane ID doesn't correspond to a real lane, the
        # COPPAGate cannot be instantiated.
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, COPPAGate.REQUIREMENT_MET_LANE, self._default_library,
            self.integration
        ).value = -100
        assert_raises_regexp(
            CannotLoadConfiguration, "No lane with ID: -100",
            COPPAGate, self._default_library, self.integration
        )

    def test_invocation(self):
        """Test the ability of a COPPAGate to act as a view."""

        class MockCOPPAGate(COPPAGate):
            def _navigation_feed(self, *args, **kwargs):
                return "fake feed"
        gate = MockCOPPAGate(self._default_library, self.integration)

        # Calling a COPPAGate creates a Response.
        response = gate(self._default_library, object(), url_for=object())
        assert isinstance(response, Response)

        # The entity-body is the result of calling _navigation_feed,
        # which has been cached as .navigation_feed.
        eq_("200 OK", response.status)
        eq_(OPDSFeed.NAVIGATION_FEED_TYPE, response.headers['Content-Type'])
        eq_("fake feed", response.data)
        eq_(response.data, gate.navigation_feed)


    def test_other(self):
        class MockAnnotator(object):
            def annotate_feed(self, feed, lane):
                self.called_with = (feed, lane)

        annotator = MockAnnotator()

        # The feed was passed to our mock Annotator, which decided to do
        # nothing to it.
        eq_((self.gate.navigation_feed, None), annotator.called_with)
