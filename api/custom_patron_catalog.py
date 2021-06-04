"""A custom patron catalog annotates a library's authentication
document to describe an unusual setup.
"""

from flask import Response
from flask_babel import lazy_gettext as _

from sqlalchemy.orm.session import Session

from .config import CannotLoadConfiguration
from core.model import (
    get_one,
)
from core.lane import Lane
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
)
from core.util.opds_writer import OPDSFeed

class CustomPatronCatalog(object):
    """An annotator for a library's authentication document.

    Any subclass of this class must define PROTOCOL and must be
    passed into a CustomPatronCatalog.register() call after the class
    definition is complete.

    A subclass of this class will be stored in the
    LibraryAuthenticator. CustomPatronCatalogs should not store
    any objects obtained from the database without disconnecting them
    from their session.
    """
    BY_PROTOCOL = {}

    GOAL = "custom_patron_catalog"

    @classmethod
    def register(self, view_class):
        protocol = view_class.PROTOCOL
        if protocol in self.BY_PROTOCOL:
            raise ValueError("Duplicate patron catalog for protocol: %s" % protocol)
        self.BY_PROTOCOL[protocol] = view_class

    @classmethod
    def unregister(self, view_class):
        """Remove a CustomPatronCatalog from consideration.
        Only used in tests.
        """
        del self.BY_PROTOCOL[view_class.PROTOCOL]

    @classmethod
    def for_library(cls, library):
        """Find the appropriate CustomPatronCatalog for the given library."""
        _db = Session.object_session(library)
        integration = ExternalIntegration.one_for_library_and_goal(
            _db, library, cls.GOAL
        )
        if not integration:
            return None
        protocol = integration.protocol
        if not protocol in cls.BY_PROTOCOL:
            raise CannotLoadConfiguration(
                "Unregistered custom patron catalog protocol: %s" % protocol
            )
        view_class = cls.BY_PROTOCOL[protocol]
        return view_class(library, integration)

    def __init__(self, library, integration):
        raise NotImplementedError()

    def annotate_authentication_document(self, library, doc, url_for):
        """Modify the library's authentication document.

        :param library: A Library
        :param doc: A dictionary representing the library's
            default authentication document.
        :param url_for: An implementation of Flask url_for,
            used to generate URLs.
        :return: A dictionary representing the library's
            default authentication document. It's okay to modify
            `doc` and return the modified version.
        """
        raise NotImplementedError()

    @classmethod
    def _load_lane(cls, library, lane_id):
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

    @classmethod
    def replace_link(cls, doc, rel, **kwargs):
        """Remove all links with the given relation and replace them
        with the given link.

        :param doc: An authentication document. Will be modified in place.
        :param rel: Remove links with this relation.
        :param kwargs: Add a new link with these attributes.
        :return: A modified authentication document.
        """
        links = [x for x in doc['links'] if x['rel'] != rel]
        links.append(dict(rel=rel, **kwargs))
        doc['links'] = links
        return doc


class CustomRootLane(CustomPatronCatalog):
    """Send library patrons to a lane other than the root lane."""
    PROTOCOL = "Custom Root Lane"

    LANE = "lane"

    SETTINGS = [
        { "key": LANE,
          "label": _("Send patrons to the lane with this ID."),
        },
    ]

    def __init__(self, library, integration):
        _db = Session.object_session(library)
        m = ConfigurationSetting.for_library_and_externalintegration
        lane_id = m(_db, self.LANE, library, integration)

        # We don't want to store the Lane objects long-term, but we do need
        # to make sure the lane ID corresponds to a real lane for the
        # right library.
        self.lane_id = lane_id.int_value
        lane = self._load_lane(library, self.lane_id)

    def annotate_authentication_document(self, library, doc, url_for):
        """Replace the 'start' link with a link to the configured Lane."""
        root_url = url_for(
            "acquisition_groups", library_short_name=library.short_name,
            lane_identifier=self.lane_id, _external=True
        )
        self.replace_link(
            doc, 'start', href=root_url, type=OPDSFeed.ACQUISITION_FEED_TYPE
        )
        return doc
CustomPatronCatalog.register(CustomRootLane)


class COPPAGate(CustomPatronCatalog):

    PROTOCOL = "COPPA Age Gate"

    AUTHENTICATION_TYPE = "http://librarysimplified.org/terms/authentication/gate/coppa"
    AUTHENTICATION_YES_REL = "http://librarysimplified.org/terms/rel/authentication/restriction-met"
    AUTHENTICATION_NO_REL = "http://librarysimplified.org/terms/rel/authentication/restriction-not-met"

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

    def annotate_authentication_document(self, library, doc, url_for):
        """Replace the 'start' link and add a custom authentication
        mechanism.
        """

        # A lane for grown-ups.
        yes_url = url_for(
            'acquisition_groups', library_short_name=library.short_name,
            lane_identifier=self.yes_lane_id, _external=True
        )

        # A lane for children.
        no_url = url_for(
            'acquisition_groups', library_short_name=library.short_name,
            lane_identifier=self.no_lane_id, _external=True
        )

        # Replace the 'start' link with the childrens link. Any client
        # that doesn't understand the extensions will be safe from
        # grown-up content.
        feed = OPDSFeed.ACQUISITION_FEED_TYPE
        self.replace_link(doc, 'start', href=no_url, type=feed)

        # Add a custom authentication technique that
        # explains the COPPA gate.
        links = [
            dict(rel=self.AUTHENTICATION_YES_REL, href=yes_url,
                 type=feed),
            dict(rel=self.AUTHENTICATION_NO_REL, href=no_url,
                 type=feed),
        ]

        authentication = dict(
            type=self.AUTHENTICATION_TYPE,
            links=links
        )

        # It's an academic question whether this is replacing the existing
        # auth mechanisms or just adding another one, but for the moment
        # let's go with "adding another one".
        doc.setdefault('authentication', []).append(authentication)
        return doc
CustomPatronCatalog.register(COPPAGate)

