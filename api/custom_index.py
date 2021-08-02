"""A custom index view customizes a library's 'front page' to serve
something other than the default.

This code is DEPRECATED; you probably want a CustomPatronCatalog instead.
We're keeping it around because existing iOS versions of SimplyE need the
OPDS navigation feed it generates.
"""

from flask import Response
from flask_babel import lazy_gettext as _

from sqlalchemy.orm.session import Session

from .config import CannotLoadConfiguration
from core.app_server import cdn_url_for
from core.model import (
    get_one,
)
from core.lane import Lane
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
)
from core.util.datetime_helpers import utc_now
from core.util.opds_writer import OPDSFeed

class CustomIndexView(object):
    """A custom view that replaces the default OPDS view for a
    library.

    Any subclass of this class must define PROTOCOL and must be
    passed into a CustomIndexView.register() call after the class
    definition is complete.

    Subclasses of this class are loaded into the CirculationManager, so they
    should not store any objects obtained from the database without
    disconnecting them from their session.
    """
    BY_PROTOCOL = {}

    GOAL = "custom_index"

    @classmethod
    def register(self, view_class):
        protocol = view_class.PROTOCOL
        if protocol in self.BY_PROTOCOL:
            raise ValueError("Duplicate index view for protocol: %s" % protocol)
        self.BY_PROTOCOL[protocol] = view_class

    @classmethod
    def unregister(self, view_class):
        """Remove a CustomIndexView from consideration.
        Only used in tests.
        """
        del self.BY_PROTOCOL[view_class.PROTOCOL]

    @classmethod
    def for_library(cls, library):
        """Find the appropriate CustomIndexView for the given library."""
        _db = Session.object_session(library)
        integration = ExternalIntegration.one_for_library_and_goal(
            _db, library, cls.GOAL
        )
        if not integration:
            return None
        protocol = integration.protocol
        if not protocol in cls.BY_PROTOCOL:
            raise CannotLoadConfiguration(
                "Unregistered custom index protocol: %s" % protocol
            )
        view_class = cls.BY_PROTOCOL[protocol]
        return view_class(library, integration)

    def __init__(self, library, integration):
        raise NotImplementedError()

    def __call__(self, library, annotator):
        """Render the custom index view.

        :return: A Response or ProblemDetail
        :param library: A Library
        :param annoatator: An Annotator for annotating OPDS feeds.
        """
        raise NotImplementedError()


class COPPAGate(CustomIndexView):

    PROTOCOL = "COPPA Age Gate"

    URI = "http://librarysimplified.org/terms/restrictions/coppa"
    NO_TITLE = _("I'm Under 13")
    NO_CONTENT = _("Read children's books")
    YES_TITLE = _("I'm 13 or Older")
    YES_CONTENT = _("See the full collection")

    REQUIREMENT_MET_LANE = "requirement_met_lane"
    REQUIREMENT_NOT_MET_LANE = "requirement_not_met_lane"

    SETTINGS = [
        { "key": REQUIREMENT_MET_LANE,
          "label": _("ID of lane for patrons who are 13 or older"),
        },
        { "key": REQUIREMENT_NOT_MET_LANE,
          "label": _("ID of lane for patrons who are under 13"),
        },
    ]

    def __init__(self, library, integration):
        _db = Session.object_session(library)
        m = ConfigurationSetting.for_library_and_externalintegration
        yes_lane_id = m(_db, self.REQUIREMENT_MET_LANE, library, integration)
        no_lane_id = m(_db, self.REQUIREMENT_NOT_MET_LANE, library, integration)

        # We don't want to store the Lane objects long-term, but we do need
        # to make sure the lane IDs correspond to real lanes for the
        # right library.
        self.yes_lane_id = yes_lane_id.int_value
        self.no_lane_id = no_lane_id.int_value
        yes_lane = self._load_lane(library, self.yes_lane_id)
        no_lane = self._load_lane(library, self.no_lane_id)

    def _load_lane(self, library, lane_id):
        """Make sure the Lane with the given ID actually exists and is
        associated with the given Library.
        """
        _db = Session.object_session(library)
        lane = get_one(_db, Lane, id=lane_id)
        if not lane:
            raise CannotLoadConfiguration("No lane with ID: %s" % lane_id)
        if lane.library != library:
            raise CannotLoadConfiguration(
                "Lane %d is for the wrong library (%s, I need %s)" %
                (lane.id, lane.library.name, library.name)
            )
        return lane

    def __call__(self, library, annotator, url_for=None):
        """Render an OPDS navigation feed that lets the patron choose a root
        lane on their own, without providing any credentials.
        """
        if not hasattr(self, 'navigation_feed'):
            self.navigation_feed = self._navigation_feed(
                library, annotator, url_for
            )
        headers = { "Content-Type": OPDSFeed.NAVIGATION_FEED_TYPE }
        return Response(str(self.navigation_feed), 200, headers)

    def _navigation_feed(self, library, annotator, url_for=None):
        """Generate an OPDS feed for navigating the COPPA age gate."""
        url_for = url_for or cdn_url_for
        base_url = url_for('index', library_short_name=library.short_name)

        # An entry for grown-ups.
        feed = OPDSFeed(title=library.name, url=base_url)
        opds = feed.feed

        yes_url = url_for(
            'acquisition_groups',
            library_short_name=library.short_name,
            lane_identifier=self.yes_lane_id
        )
        opds.append(
            self.navigation_entry(yes_url, self.YES_TITLE, self.YES_CONTENT)
        )

        # An entry for children.
        no_url = url_for(
            'acquisition_groups',
            library_short_name=library.short_name,
            lane_identifier=self.no_lane_id
        )
        opds.append(
            self.navigation_entry(no_url, self.NO_TITLE, self.NO_CONTENT)
        )

        # The gate tag is the thing that the SimplyE client actually uses.
        opds.append(self.gate_tag(self.URI, yes_url, no_url))

        # Add any other links associated with this library, notably
        # the link to its authentication document.
        if annotator:
            annotator.annotate_feed(feed, None)

        now = utc_now()
        opds.append(OPDSFeed.E.updated(OPDSFeed._strftime(now)))
        return feed

    @classmethod
    def navigation_entry(cls, href, title, content):
        """Create an <entry> that serves as navigation."""
        E = OPDSFeed.E
        content_tag = E.content(type="text")
        content_tag.text = str(content)
        now = utc_now()
        entry = E.entry(
            E.id(href),
            E.title(str(title)),
            content_tag,
            E.updated(OPDSFeed._strftime(now))
        )
        OPDSFeed.add_link_to_entry(
            entry, href=href, rel="subsection",
            type=OPDSFeed.ACQUISITION_FEED_TYPE
        )
        return entry

    @classmethod
    def gate_tag(cls, restriction, met_url, not_met_url):
        """Create a simplified:gate tag explaining the boolean option
        the client is faced with.
        """
        tag = OPDSFeed.SIMPLIFIED.gate()
        tag.attrib['restriction-met'] = met_url
        tag.attrib['restriction-not-met'] = not_met_url
        tag.attrib['restriction'] = restriction
        return tag

CustomIndexView.register(COPPAGate)
