from config import Configuration
import uuid
import unicodedata
import urllib
import re
from flask.ext.babel import lazy_gettext as _
from core.util.http import HTTP
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
    Session,
    get_one,
)

class GoogleAnalyticsProvider(object):
    NAME = _("Google Analytics")

    TRACKING_ID = "tracking_id"
    DEFAULT_URL = "http://www.google-analytics.com/collect"

    SETTINGS = [
        { "key": ExternalIntegration.URL, "label": _("URL"), "default": DEFAULT_URL },
    ]

    LIBRARY_SETTINGS = [
        { "key": TRACKING_ID, "label": _("Tracking ID") },
    ]
    
    def __init__(self, integration):
        url_setting = ConfigurationSetting.for_externalintegration(ExternalIntegration.URL, integration)
        self.url = url_setting.value or self.DEFAULT_URL
        self.integration_id = integration.id

    def collect_event(self, library, license_pool, event_type, time, **kwargs):
        _db = Session.object_session(library)
        integration = get_one(_db, ExternalIntegration, id=self.integration_id)
        tracking_id = ConfigurationSetting.for_library_and_externalintegration(
            _db, self.TRACKING_ID, library, integration,
        ).value

        client_id = uuid.uuid4()
        fields = {
            'v': 1,
            'tid': tracking_id,
            'cid': client_id,
            'aip': 1, # anonymize IP
            'ds': "Circulation Manager",
            't': 'event',
            'ec': 'circulation',
            'ea': event_type,
            'cd1': time,
        }

        if license_pool:
            fields.update({
                'cd2': license_pool.identifier.identifier,
                'cd3': license_pool.identifier.type
            })

            work = license_pool.work
            edition = license_pool.presentation_edition
            if work and edition:
                fields.update({
                    'cd4': edition.title,
                    'cd5': edition.author,
                    'cd6': "fiction" if work.fiction else "nonfiction",
                    'cd7': work.audience,
                    'cd8': work.target_age_string,
                    'cd9': edition.publisher,
                    'cd10': edition.language,
                    'cd11': work.top_genre(),
                    'cd12': "true" if license_pool.open_access else "false",
                })
        # urlencode doesn't like unicode strings so we convert them to utf8
        fields = {k: unicodedata.normalize("NFKD", unicode(v)).encode("utf8") for k, v in fields.iteritems()}
        
        params = re.sub(r"=None(&?)", r"=\1", urllib.urlencode(fields))
        self.post(self.url, params)

    def post(self, url, params):
        response = HTTP.post_with_timeout(url, params)

        
Provider = GoogleAnalyticsProvider
