from pdb import set_trace
from core.model import (
    production_session,
    DataSource,
    Edition,
    )

from sqlalchemy.orm import (
    aliased,
)

import json

_db = production_session()
nyt = DataSource.lookup(_db, DataSource.NYT)
overdrive = DataSource.lookup(_db, DataSource.OVERDRIVE)
threem = DataSource.lookup(_db, DataSource.THREEM)
q = _db.query(Edition).filter(Edition.data_source==nyt)
print "%d entries on best seller lists." % q.count()

title = q.filter(Edition.title != None).filter(Edition.author != None)
print "%d best sellers have title and author." % title.count()

sort_author = q.filter(Edition.title != None).filter(Edition.sort_author != None)
print "%d best sellers have title and sort author." % sort_author.count()

pwid = q.filter(Edition.permanent_work_id != None)
print "%d best sellers have permanent work ID." % pwid.count()

best_sellers = []
best_seller_pwids = set()
for x in q:
    data = dict(title=x.title, display_author=x.author, 
                sort_author=x.sort_author, pwid=x.permanent_work_id,
                id=x.id)
    best_sellers.append(data)
    best_seller_pwids.add(data['pwid'])
print "%d best-sellers." % len(best_sellers)
json.dump(best_sellers, open("nyt_pwid.json", "w"))

licensed_sources = [overdrive.id, threem.id]
licensed_editions = _db.query(Edition).filter(Edition.data_source_id.in_(licensed_sources))


licensed_pwids = []
for x in licensed_editions:
    pwid = x.permanent_work_id
    data = dict(id=x.id, title=x.title, display_author=x.author,
                sort_author=x.sort_author,
                pwid=pwid)
    licensed_pwids.append(data)
print "%d Overdrive/3M best-sellers." % len(licensed_pwids)
json.dump(licensed_pwids, open("licensed_pwid.json", "w"))

matching_pwid = _db.query(Edition).filter(Edition.data_source != nyt).filter(Edition.permanent_work_id.in_(best_seller_pwids))
print "%d Simplified editions share a permanent work ID with a best-seller." % matching_pwid.count()
