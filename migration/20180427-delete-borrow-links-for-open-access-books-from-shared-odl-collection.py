#!/usr/bin/env python
"""Due to a bug in version 2.2.0, borrow links for open-access books in a
shared ODL collection were imported. This migration delete the links and
their associated resources and representations."""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from sqlalchemy.orm import aliased
from sqlalchemy import and_

from core.model import (
    Collection,
    Hyperlink,
    LicensePool,
    Representation,
    Resource,
    production_session,
)
from api.odl import SharedODLAPI

try:
    _db = production_session()
    for collection in Collection.by_protocol(_db, SharedODLAPI.NAME):
        borrow_link = aliased(Hyperlink)
        open_link = aliased(Hyperlink)

        pools = _db.query(
            LicensePool
        ).join(
            borrow_link,
            LicensePool.identifier_id==borrow_link.identifier_id,
        ).join(
            open_link,
            LicensePool.identifier_id==open_link.identifier_id,
        ).join(
            Resource,
            borrow_link.resource_id==Resource.id,
        ).join(
            Representation,
            Resource.representation_id==Representation.id,
        ).filter(
            and_(
                LicensePool.collection_id==collection.id,
                borrow_link.rel==Hyperlink.BORROW,
                open_link.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD,
                Representation.media_type=='application/atom+xml;type=entry;profile=opds-catalog',
            )
        )

        print "Deleting hyperlinks for %i license pools" % pools.count()
        for pool in pools:
            for link in pool.identifier.links:
                if link.rel == Hyperlink.BORROW:
                    resource = link.resource
                    representation = resource.representation

                    _db.delete(representation)
                    _db.delete(link)
                    _db.delete(resource)

finally:
    _db.commit()
    _db.close()
