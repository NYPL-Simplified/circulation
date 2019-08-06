from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
from lxml import etree

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

    def test_lane_loading(self):
        # The default setup loads lane IDs properly.
        gate = COPPAGate(self._default_library, self.integration)
        eq_(self.lane1.id, gate.yes_lane_id)
        eq_(self.lane2.id, gate.no_lane_id)

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

    def test__navigation_feed(self):
        """Test the code that builds an OPDS navigation feed."""

        class MockAnnotator(object):
            """This annotator will have its chance to annotate
            the feed before it's finalized.
            """
            def annotate_feed(self, feed, lane):
                self.called_with = (feed, lane)
        annotator = MockAnnotator()

        url_for_calls = []
        def mock_url_for(controller, library_short_name, **kwargs):
            """Create a real-looking URL for any random controller."""
            url_for_calls.append((controller, library_short_name, kwargs))
            query = "&".join(
                ["%s=%s" % (k,v) for k, v in sorted(kwargs.items())]
            )
            return "http://%s/%s?%s" % (library_short_name, controller, query)

        navigation_entry_calls = []
        gate_tag_calls = []
        class MockCOPPAGate(COPPAGate):
            def navigation_entry(self, url, title, content):
                navigation_entry_calls.append((url, title, content))
                return OPDSFeed.E.entry()

            @classmethod
            def gate_tag(cls, restriction, met_uri, not_met_uri):
                gate_tag_calls.append((restriction, met_uri, not_met_uri))
                return OPDSFeed.E.gate()

        self._default_library.name = "The Library"
        self._default_library.short_name = "LIBR"
        gate = MockCOPPAGate(self._default_library, self.integration)
        feed = gate._navigation_feed(
            self._default_library, annotator, mock_url_for
        )

        # The feed was passed to our mock Annotator, which decided to do
        # nothing to it.
        eq_((feed, None), annotator.called_with)

        # navigation_entry was called twice, once for the 'old enough'
        # entry and once for the 'not old enough' entry.
        older, younger = navigation_entry_calls

        lane_url, title, content = older
        yes_url = mock_url_for(
            "acquisition_groups", self._default_library.short_name,
            lane_identifier=gate.yes_lane_id
        )
        eq_(lane_url, yes_url)
        eq_(title, gate.YES_TITLE)
        eq_(content, gate.YES_CONTENT)

        lane_url, title, content = younger
        no_url = mock_url_for(
            "acquisition_groups", self._default_library.short_name,
            lane_identifier=gate.no_lane_id
        )
        eq_(lane_url, no_url)
        eq_(title, gate.NO_TITLE)
        eq_(content, gate.NO_CONTENT)

        # gate_tag was called once.
        [(restriction, met_url, not_met_url)] = gate_tag_calls
        eq_(gate.URI, restriction)
        eq_(yes_url, met_url)
        eq_(no_url, not_met_url)

        # The feed as a whole incorporates the return values of
        # the methods that were called.
        feed = str(feed)
        assert "<gate/>" in feed
        eq_(2, feed.count("<entry/>"))

        # There's also a self link, a title, an ID, and an updated
        # time, which were inserted by the OPDSFeed constructor.
        index = mock_url_for("index", self._default_library.short_name)
        assert ('<link href="%s" rel="self"/>' % index) in feed
        assert ("<title>%s</title>" % self._default_library.name) in feed
        assert ('<id>%s</id>' % index) in feed
        assert '<updated>' in feed

    def test_navigation_entry(self):
        """navigation_entry creates an OPDS entry with a subsection link."""
        entry = etree.tostring(
            COPPAGate.navigation_entry(
                "some href", "some title", "some content"
            )
        )
        assert entry.startswith('<entry ')
        for expect in (
                '<id>some href</id>',
                '<title>some title</title>',
                '<content type="text">some content</content>',
                '<link href="some href" type="application/atom+xml;profile=opds-catalog;kind=acquisition" rel="subsection"/>',
                '<updated',
        ):
            assert expect in entry

    def test_gate_tag(self):
        """gate_tag creates a simplified:gate tag."""
        gate = COPPAGate.gate_tag(
            "restriction", "http://met/", "http://not-met/"
        )
        simplified_ns = '{%s}' % OPDSFeed.SIMPLIFIED_NS
        eq_(simplified_ns + "gate", gate.tag)

        # The tag contains the URI for the restriction, and the
        # destination URLs designating where clients should go if they
        # do (or don't) meet the restriction.
        eq_("restriction", gate.attrib["restriction"])
        eq_("http://met/", gate.attrib["restriction-met"])
        eq_("http://not-met/", gate.attrib["restriction-not-met"])

