#!/usr/bin/env python
"""Associate LicensePools to their Collections"""
import os
import sys
import logging

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    Collection,
    DataSource,
    LicensePool,
    production_session,
)
from core.util import fast_query_count

_db = production_session()
log = logging.getLogger('Migration [20170714]')

def log_change(count, collection):
    log.info('UPDATED: %d LicensePools given Collection %r' % (
        int(count), collection))

try:
    collections = _db.query(Collection).all()
    collections_by_data_source = dict([(collection.data_source, collection) for collection in collections])

    base_query = _db.query(LicensePool).filter(LicensePool.collection_id==None)
    for data_source, collection in collections_by_data_source.items():
        # Find LicensePools with the matching DataSource.
        qu = base_query.filter(LicensePool.data_source==data_source)
        qu.update({LicensePool.collection_id : collection.id})
        log_change(fast_query_count(qu), collection)
        _db.commit()

    # Some LicensePools may be associated with the a duplicate or
    # outdated Bibliotheca DataSource. Find them.
    bibliotheca = DataSource.lookup(_db, DataSource.BIBLIOTHECA)
    old_sources = _db.query(DataSource.id).filter(
        DataSource.name.in_(['3M', 'Bibliotecha'])).subquery()
    threem_qu = base_query.filter(LicensePool.data_source_id.in_(old_sources))

    # Associate these LicensePools with the Bibliotheca Collection.
    bibliotheca_collection = collections_by_data_source.get(bibliotheca)
    if bibliotheca_collection:
        result = threem_qu.update(
            {LicensePool.collection_id : bibliotheca_collection.id},
            synchronize_session='fetch'
        )

        # If something changed, log it.
        threem_count = fast_query_count(threem_qu)
        if threem_count:
            log_change(threem_count, bibliotheca_collection)

    remaining = fast_query_count(base_query)
    if remaining > 0:
        log.warning('No Collection found for %d LicensePools', remaining)

        source_ids = _db.query(LicensePool.data_source_id)\
                        .filter(LicensePool.collection_id==None).subquery()
        sources = _db.query(DataSource).filter(DataSource.id.in_(source_ids))
        names = ', '.join(["%s" % source.name for source in sources])

        log.warning('Remaining LicensePools have DataSources: %s', names)
except Exception as e:
    _db.close()
    raise e
finally:
    _db.commit()
    _db.close()
