from flask_babel import lazy_gettext as _
from model import Session, CirculationEvent

class LocalAnalyticsProvider(object):
    NAME = _("Local Analytics")

    DESCRIPTION = _("Store analytics events in the 'circulationevents' database table.")

    # A given site can only have one analytics provider.
    CARDINALITY = 1

    def __init__(self, integration, library=None):
        self.integration_id = integration.id
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

        CirculationEvent.log(
          _db, license_pool, event_type, old_value, new_value, start=time)

Provider = LocalAnalyticsProvider
