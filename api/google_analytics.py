from config import Configuration
import uuid
import urllib
from core.util.http import HTTP

class GoogleAnalytics(object):
    NAME = "Google Analytics"
    
    @classmethod
    def from_config(cls):
        tracking_id = Configuration.integration(cls.NAME, required=True)['tracking_id']
        return cls(tracking_id)

    def __init__(self, tracking_id):
        self.tracking_id = tracking_id

    def format_range(self, r):
        if not r or not r.lower:
            return None
        min = r.lower if r.lower_inc else r.lower + 1
        if r.upper:
            max = r.upper + 1 if r.upper_inc else r.upper
            ",".join(range(min, max))
        else:
            return str(min)

    def collect_event(self, event):
        client_id = uuid.uuid4()
        work = event.license_pool.work
        edition = event.license_pool.presentation_edition
        params = urllib.urlencode({
            'v': 1,
            'tid': self.tracking_id,
            'cid': client_id,
            't': 'event',
            'ec': 'circulation',
            'ea': event.type,
            'cd1': event.license_pool.identifier.identifier,
            'cd2': edition.title,
            'cd3': edition.author,
            'cd4': "fiction" if work.fiction else "nonfiction",
            'cd5': work.audience,
            'cd6': edition.publisher,
            'cd7': edition.language,
            'cd8': self.format_range(work.target_age),
            'cd9': event.start
        })
        self.post("http://www.google-analytics.com/collect", params)

    def post(self, url, params):
        response = HTTP.post_with_timeout(url, params)
        
Collector = GoogleAnalytics