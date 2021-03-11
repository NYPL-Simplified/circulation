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
from api.custom_patron_catalog import (
    COPPAGate,
    CustomPatronCatalog,
    CustomRootLane,
)

from core.testing import DatabaseTest

class TestCustomPatronCatalog(DatabaseTest):

    def test_register(self):
        c = CustomPatronCatalog
        old_registry = c.BY_PROTOCOL
        c.BY_PROTOCOL = {}

        class Mock1(object):
            PROTOCOL = "A protocol"

        class Mock2(object):
            PROTOCOL = "A protocol"

        c.register(Mock1)
        assert Mock1 == c.BY_PROTOCOL[Mock1.PROTOCOL]

        assert_raises_regexp(
            ValueError,
            "Duplicate patron catalog for protocol: A protocol",
            c.register, Mock2
        )
        c.BY_PROTOCOL = old_registry

    def test_default_registry(self):
        """Verify the default contents of the registry."""
        assert (
            {
                COPPAGate.PROTOCOL : COPPAGate,
                CustomRootLane.PROTOCOL : CustomRootLane,
            } ==
            CustomPatronCatalog.BY_PROTOCOL)

    def test_for_library(self):
        m = CustomPatronCatalog.for_library

        # Set up a mock CustomPatronCatalog so we can watch it being
        # instantiated.
        class MockCustomPatronCatalog(object):
            PROTOCOL = self._str
            def __init__(self, library, integration):
                self.instantiated_with = (library, integration)
        CustomPatronCatalog.register(MockCustomPatronCatalog)

        # By default, a library has no CustomPatronCatalog.
        assert None == m(self._default_library)

        # But if a library has an ExternalIntegration that corresponds
        # to a registered CustomPatronCatalog...
        integration = self._external_integration(
            MockCustomPatronCatalog.PROTOCOL, CustomPatronCatalog.GOAL,
            libraries=[self._default_library]
        )

        # A CustomPatronCatalog of the appropriate class is instantiated
        # and returned.
        view = m(self._default_library)
        assert isinstance(view, MockCustomPatronCatalog)
        assert (self._default_library, integration) == view.instantiated_with

    def test__load_lane(self):
        """Test the _load_lane helper method."""
        library1 = self._library()
        library2 = self._library()
        lane = self._lane(library=library1)
        m = CustomPatronCatalog._load_lane

        assert lane == m(library1, lane.id)

        assert_raises_regexp(
            CannotLoadConfiguration,
            "No lane with ID", m, library1, -2
        )

        assert_raises_regexp(
            CannotLoadConfiguration,
            "is for the wrong library", m, library2, lane.id
        )

    def test_replace_link(self):
        """Test the replace_link helper method."""
        links = [
            dict(rel="replace-me", href="link1"),
            dict(rel="leave-me-alone", href="link2"),
            dict(rel="replace-me", href="link3"),
        ]
        doc = dict(ignoreme=True, links=links)

        CustomPatronCatalog.replace_link(
            doc, "replace-me", href="link4", type="text/html"
        )

        # Both replace-me links have been removed, and a a new link
        # with the same relation has been added.
        links = [
            dict(rel="leave-me-alone", href="link2"),
            dict(rel="replace-me", href="link4", type="text/html"),
        ]
        assert doc['links'] == links


class TestCustomRootLane(DatabaseTest):
    """Test a CustomPatronCatalog which modifies the 'start' URL."""

    def setup_method(self):
        super(TestCustomRootLane, self).setup_method()
        # Configure a CustomRootLane for the default library.
        self.integration = self._external_integration(
            CustomRootLane.PROTOCOL, CustomPatronCatalog.GOAL,
            libraries=[self._default_library]
        )
        self.lane = self._lane()
        m = ConfigurationSetting.for_library_and_externalintegration
        m(
            self._db, CustomRootLane.LANE, self._default_library,
            self.integration
        ).value = self.lane.id

    def test_annotate_authentication_document(self):

        class MockCustomRootLane(CustomRootLane):
            def replace_link(self, doc, rel, **kwargs):
                self.replace_link_called_with = (doc, rel, kwargs)
                doc['modified'] = True

            def url_for(self, view, library_short_name, lane_identifier, _external):
                self.url_for_called_with = (
                    view, library_short_name, lane_identifier, _external
                )
                return "new-root"

        library = self._default_library
        custom_root = MockCustomRootLane(library, self.integration)

        doc = dict()
        new_doc = custom_root.annotate_authentication_document(
            library, doc, custom_root.url_for
        )

        # The authentication document was modified in place.
        assert doc == new_doc
        assert dict(modified=True) == doc

        # url_for was called with the expected arguments, and it
        # returned 'new-root', seen above.
        assert (("acquisition_groups", library.short_name, custom_root.lane_id, True) ==
            custom_root.url_for_called_with)

        # replace_link was called with the result of calling url_for.
        assert (
            (doc, 'start',
             dict(href="new-root", type=OPDSFeed.ACQUISITION_FEED_TYPE)
            ) ==
            custom_root.replace_link_called_with)


class TestCOPPAGate(DatabaseTest):

    def setup_method(self):
        super(TestCOPPAGate, self).setup_method()
        # Configure a COPPAGate for the default library.
        self.integration = self._external_integration(
            COPPAGate.PROTOCOL, CustomPatronCatalog.GOAL,
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

    def test_annotate_authentication_document(self):
        """Test the ability of a COPPAGate to modify an Authentication
        For OPDS document.
        """
        class MockCOPPAGate(COPPAGate):
            url_for_called_with = []
            def replace_link(self, doc, rel, **kwargs):
                self.replace_link_called_with = (doc, rel, kwargs)

            def url_for(self, view, library_short_name, lane_identifier,
                        _external):
                self.url_for_called_with.append(
                    (view, library_short_name, lane_identifier, _external)
                )
                return view + "/" + str(lane_identifier)
        library = self._default_library
        gate = MockCOPPAGate(library, self.integration)

        doc = {}
        library = self._default_library
        modified = gate.annotate_authentication_document(
            library, doc, gate.url_for
        )

        # url_for was called twice, to make the lane links for
        # the adults' section and the kids' section.
        [yes_call, no_call] = gate.url_for_called_with
        assert (
            ("acquisition_groups", library.name, gate.yes_lane_id, True) ==
            yes_call)
        assert (
            ("acquisition_groups", library.name, gate.no_lane_id, True) ==
            no_call)

        # These are the possible return values of our mocked url_for.
        yes_url = "acquisition_groups/%s" % gate.yes_lane_id
        no_url = "acquisition_groups/%s" % gate.no_lane_id

        # The document was modified in place.
        assert doc == modified

        # An authentication mechanism was added to the document.
        [authentication] = doc.pop('authentication')

        # No other changes were made to the document.
        assert {} == doc

        # The authentication mechanism is a COPPA age gate,
        assert gate.AUTHENTICATION_TYPE == authentication['type']

        # Each one was added as a link to the authentication mechanism.
        yes_link, no_link = authentication['links']
        for link in (yes_link, no_link):
            assert OPDSFeed.ACQUISITION_FEED_TYPE == link['type']

        assert gate.AUTHENTICATION_YES_REL == yes_link['rel']
        assert yes_url == yes_link['href']

        assert gate.AUTHENTICATION_NO_REL == no_link['rel']
        assert no_url == no_link['href']

        # replace_link was called to replace the rel='start' link,
        # with the link to the kids' section. Because that method was
        # mocked, it didn't actually modify the document.
        assert (
            (doc, 'start',
             dict(href=no_url, type=OPDSFeed.ACQUISITION_FEED_TYPE)
            ) ==
            gate.replace_link_called_with)
        pass
