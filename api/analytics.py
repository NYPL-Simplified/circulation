import httplib
import urllib
import uuid
from config import Configuration

def format_range(r):
    if not r.lower:
        return None
    min = r.lower if r.lower_inc else r.lower + 1
    if r.upper:
        max = r.upper + 1 if r.upper_inc else r.upper
        ",".join(range(min, max))
    else:
        return str(min)

def collect_analytics_event(event):
  tracking_id = Configuration.integration("Google Analytics")['tracking_id']
  client_id = uuid.uuid4()
  work = event.license_pool.work
  edition = event.license_pool.presentation_edition
  params = urllib.urlencode({
      'v': 1,
      'tid': tracking_id,
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
      'cd8': format_range(work.target_age),
      'cd9': event.start
  })
  connection = httplib.HTTPConnection('www.google-analytics.com')
  result = connection.request('POST', '/collect', params)