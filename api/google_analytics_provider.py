from config import Configuration
import uuid
import urllib
import re
from core.util.http import HTTP
import logging

class GoogleAnalyticsProvider(object):
    INTEGRATION_NAME = "Google Analytics"
    
    @classmethod
    def from_config(cls, config):
        tracking_id = config[Configuration.INTEGRATIONS][cls.INTEGRATION_NAME]['tracking_id']
        return cls(tracking_id)

    def __init__(self, tracking_id):
        self.tracking_id = tracking_id

    def collect_event(self, _db, license_pool, event_type, time, **kwargs):
        client_id = uuid.uuid4()
        work = license_pool.work
        edition = license_pool.presentation_edition
        fields = {
            'v': 1,
            'tid': self.tracking_id,
            'cid': client_id,
            'aip': 1, # anonymize IP
            'ds': "Circulation Manager",
            't': 'event',
            'ec': 'circulation',
            'ea': event_type,
            'cd1': time,
            'cd2': license_pool.identifier.identifier,
            'cd3': license_pool.identifier.type
        }
        if work and edition:
            fields.update({
                'cd4': edition.title,
                'cd5': edition.author,
                'cd6': "fiction" if work.fiction else "nonfiction",
                'cd7': work.audience,
                'cd8': work.target_age_string,
                'cd9': edition.publisher,
                'cd10': edition.language,
                'cd11': work.top_genre()
            })
        params = re.sub(r"=None(&?)", r"=\1", urllib.urlencode(fields))
        self.post("http://www.google-analytics.com/collect", params)

    def post(self, url, params):
        response = HTTP.post_with_timeout(url, params)
        logging.info(
            "Posted %s to %s and received (%d)",
            params, url, response.status_code
        )

        
Provider = GoogleAnalyticsProvider