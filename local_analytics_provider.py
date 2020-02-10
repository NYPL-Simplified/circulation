from flask_babel import lazy_gettext as _
from model import (
    Session,
    CirculationEvent,
    ExternalIntegration,
    get_one,
    create
)

class LocalAnalyticsProvider(object):
    NAME = _("Local Analytics")

    DESCRIPTION = _("Store analytics events in the 'circulationevents' database table.")

    # A given site can only have one analytics provider.
    CARDINALITY = 1

    # Where to get the 'location' of an analytics event.
    LOCATION_SOURCE = "location_source"

    # The 'location' of an analytics event is the 'neighborhood' of
    # the request's authenticated patron.
    LOCATION_SOURCE_NEIGHBORHOOD = "neighborhood"

    # Analytics events have no 'location'.
    LOCATION_SOURCE_DISABLED = ""

    SETTINGS = [
        {
            "key": LOCATION_SOURCE,
            "label": _("Geographic location of events"),
            "description": _("Local analytics events may have a geographic location associated with them. How should the location be determined?<p>Note: to use the patron's neighborhood as the event location, you must also tell your patron authentication mechanism how to <i>gather</i> a patron's neighborhood information."),
            "default": LOCATION_SOURCE_DISABLED,
            "type": "select",
            "options": [
                { "key": LOCATION_SOURCE_DISABLED, "label": _("Disable this feature.") },
                { "key": LOCATION_SOURCE_NEIGHBORHOOD, "label": _("Use the patron's neighborhood as the event location.") },
            ],
        },
    ]

    def __init__(self, integration, library=None):
        self.integration_id = integration.id
        self.location_source = integration.setting(
            self.LOCATION_SOURCE
        ).value or self.LOCATION_SOURCE_DISABLED
        if library:
            self.library_id = library.id
        else:
            self.library_id = None

    def collect_event(self, library, license_pool, event_type, time,
        old_value=None, new_value=None, **kwargs):
        if not library and not license_pool:
            raise ValueError("Either library or license_pool must be provided.")
        if library:
            _db = Session.object_session(library)
        else:
            _db = Session.object_session(license_pool)
        if library and self.library_id and library.id != self.library_id:
            return

        neighborhood = None
        if self.location_source == self.LOCATION_SOURCE_NEIGHBORHOOD:
            neighborhood = kwargs.pop("neighborhood", None)

        return CirculationEvent.log(
            _db, license_pool, event_type, old_value, new_value, start=time,
            library=library, location=neighborhood
        )

    @classmethod
    def initialize(cls, _db):
        """Find or create a local analytics service.
        """

        # If a local analytics service already exists, return it.
        local_analytics = get_one(
            _db, ExternalIntegration,
            protocol=cls.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL
        )

        # If a local analytics service already exists, don't create a
        # default one. Otherwise, create it with default name of
        # "Local Analytics".
        if not local_analytics:
            local_analytics, ignore = create(
                _db, ExternalIntegration,
                protocol=cls.__module__,
                goal=ExternalIntegration.ANALYTICS_GOAL,
                name=str(cls.NAME)
            )
        return local_analytics

Provider = LocalAnalyticsProvider
