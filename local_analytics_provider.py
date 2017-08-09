from flask_babel import lazy_gettext as _
from model import Session, CirculationEvent

class LocalAnalyticsProvider(object):
    NAME = _("Local Analytics")

    DESCRIPTION = _("Store analytics events in the 'circulationevents' database table.")

    def __init__(self, integration):
        self.integration_id = integration.id

    def collect_event(self, library, license_pool, event_type, time, 
        old_value=None, new_value=None, **kwargs):
        _db = Session.object_session(library)
        
        CirculationEvent.log(
          _db, license_pool, event_type, old_value, new_value, start=time)

Provider = LocalAnalyticsProvider
