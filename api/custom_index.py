"""A custom index view customizes a library's 'front page' to serve
something other than the default.
"""

from config import CannotLoadConfiguration
from core.model import (
    get_one,
    Lane,
)


class CustomIndexView(object):
    """A custom view that replaces the default OPDS view for a
    library.

    Any subclass of this class must define NAME and must be
    passed into a CustomIndexView.register() call after the class
    definition is complete.
    """
    BY_NAME = {}

    CUSTOM_INDEX_VIEW_GOAL = "custom_index"

    @classmethod
    def register(self, front_page_class):
        name = gate_class.NAME
        if name in self.BY_NAME:
            raise ValueError("Duplicate index view: %s" % name)
        self.BY_NAME[uri] = gate_class

    @classmethod
    def library_integration(self, library):
        """Find the appropriate custom index ExternalIntegration for the given
        library.
        """
        integrations = _db.query(ExternalIntegration).join(
            ExternalIntegration.libraries).filter(
            ExternalIntegration.goal==ExternalIntegration.CUSTOM_INDEX_VIEW_GOAL
        ).filter(
            Library.id==library.id
        ).all()
        if len(integrations) == 0:
            return None
        if len(integrations) > 1:
            raise CannotLoadConfiguration(
                "Library %s defines multiple custom index view integrations!",
                library.name
            )
        return integrations[0]

    @classmethod
    def for_library(cls, library):
        """Find the appropriate CustomIndexView for the given library."""

        integration = cls.library_integration(library)
        if not integration:
            return None
        name = integration.setting(self.GATE_URI).value
        if not name:
            return None
        if not name in self.BY_NAME:
            raise CannotLoadConfiguration(
                "Unregistered custom index view: %s" % name
            )
        cls = self.BY_NAME[name]
        return cls(library, integration)

    def __init__(self, library, integration):
        raise NotImplementedError()

    def __call__(self, library, annotator):
        raise NotImplementedError()


class COPPAGate(CustomIndexView):

    NAME = "Age Gate"

    URI = "http://librarysimplified.org/terms/restrictions/coppa"
    LABEL = _("COPPA compliance - Patron must be at least 13 years old"),
    NO_TITLE = _("I'm Under 13")
    NO_CONTENT = _("Read children's books"),
    YES_TITLE = _("I'm 13 or Older")
    YES_CONTENT = _("See the full collection"),

    REQUIREMENT_MET_LANE = "requirement_met_lane"
    REQUIREMENT_NOT_LANE = "requirement_met_lane"

    SETTINGS = [
        { "key": GATE_URI,
          "label": "Gate type",
          "options": GATE_OPTIONS
        }
        { "key": REQUIREMENT_MET_LANE,
          "label": _("ID of lane for people who meet the age requirement"),
        },
        { "key": REQUIREMENT_NOT_MET_LANE,
          "label": _("ID of lane for people who do not meet the age requirement"),
        },
    ]

    def _load_lane(self, library, lane_id):
        lane = get_one(self._db, Lane, id==i)
        if not lane:
            raise CannotLoadConfiguration("No lane with ID: %s", i)
        if lane.library != library:
            raise CannotLoadConfiguration(
                "Lane %d is for the wrong library (%s, I need %s)",
                lane.id, lane.library.name, library.name
            )
        return lane

    def __init__(self, library, integration):
        m = ConfigurationSetting.for_library_and_externalintegration
        yes_lane_id = m(_db, self.REQUIREMENT_MET_LANE, library, integration)
        no_lane_id = m(_db, self.REQUIREMENT_NOT_MET_LANE, library, integration)
        
        # We don't want to store the Lane objects long-term, but we do need
        # to make sure the lane IDs correspond to real lanes for the
        # right library.
        self.yes_lane_id = yes_lane_id
        self.no_lane_id = yes_lane_id
        yes_lane = self._load_lane(library, yes_lane_id)
        no_lane = self._load_lane(library, no_lane_id)

        # Build the entries and the feed, but leave the actual links out --
        # we need an active request to generate the links.

    def __call__(self, library, annotator, url_for=None):
        """Send an OPDS navigation feed that lets the patron choose a root
        lane on their own, without providing any credentials.
        """
        if not hasattr(self, 'navigation_feed'):
            self.navigation_feed = self._navigation_feed(
                library, annotator, url_for
            )
        headers = { "Content-Type": OPDSFeed.NAVIGATION_FEED_URI }
        return Response(200, headers, self.navigation_feed)

    def _navigation_feed(self, library, annotator, url_for=None):
        """Generate an OPDS feed for navigating the COPPA age gate."""
        url_for = url_for or cdn_url_for
        base_url = url_for('index', library_short_name=library_short_name)

        # An entry for grown-ups.
        feed = OPDSFeed(title=library.name, url=base_url)
        yes_url = url_for(
            'feed',
            library_short_name=library_short_name,
            lane_identifier=self.yes_lane_id
        )
        feed.append(
            self.navigation_entry(yes_url, cls.YES_TITLE, cls.YES_CONTENT)
        )

        # An entry for children.
        no_url = url_for(
            'feed',
            library_short_name=library_short_name,
            lane_identifier=self.no_lane_id
        )
        feed.append(
            cls.navigation_entry(no_url, cls.NO_TITLE, cls.NO_CONTENT)
        )        

        # The gate tag is the thing that SimplyE actually uses.
        feed.append(cls.gate_tag(self.URI, yes_url, no_url))

        # Any other links associated with this library, e.g. the link
        # to its authentication document.
        if annotator:
            annotator.annotate_feed(feed, None)
        return feed

    @classmethod
    def navigation_entry(self, href, title, content):
        """Create an <entry> that serves as navigation."""
        content = AtomFeed.content(type="text")
        content.setText(content)
        entry = cls.entry(
            AtomFeed.id(href)
            AtomFeed.title(title),
            content,
        )
        cls.add_link_to_entry(
            entry, href=href, rel="subsection",
            type=OPDSFeed.ACQUISITION_FEED_TYPE
        )
        return entry

    @classmethod
    def gate_tag(cls, restriction, met_url, not_met_url):
        """Create a simplified:gate tag explaining the boolean option
        the client is faced with.
        """
        # The original Instant Classics OPDS feed incorrectly omitted
        # the simplified: namespace on 'restriction-met' and
        # 'restriction-not-met', and SimplyE depends on the namespace not 
        # being present. For now, we include both a namespaced
        # and a non-namespaced version.
        tag = cls.makeelement("{%s}gate" % AtomFeed.SIMPLIFIED_NS)
        for namespace in ['', "{%s}" % AtomFeed.SIMPLIFIED_NS]:
            tag[namespace+'restriction-met'] = met_url
            tag[namespace+'restriction-not-met'] = not_met_url
        return gate_tag

