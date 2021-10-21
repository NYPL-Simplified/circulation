#!/usr/bin/env python3
"""
Find open-access LicensePools that do not have a Hyperlink
with an open access rel. If they have a delivery mechanism with
a resource, create a Hyperlink for the resource and identifier.
"""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (        # noqa: E402,F401
    production_session,
    Hyperlink,
    LicensePool,
    Resource,
    get_one_or_create,
)

_db = production_session()

open_access_pools = _db.query(LicensePool).filter(LicensePool.open_access==True)    # noqa: E712,E225

pools_with_open_access_links = _db.query(LicensePool).join(
        Hyperlink,
        Hyperlink.identifier_id==LicensePool.identifier_id      # noqa: E225
    ).filter(
        Hyperlink.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD           # noqa: E225
    ).filter(
        LicensePool.open_access==True                           # noqa: E225,E712
    )

pool_ids_with_open_access_links = [pool.id for pool in pools_with_open_access_links]

open_access_pools_without_open_access_links = open_access_pools.filter(~LicensePool.id.in_(pool_ids_with_open_access_links))    # noqa: E501

print("Found %d open access pools without open access links" % open_access_pools_without_open_access_links.count())

fixed = 0
no_identifier = 0
no_resource = 0

for pool in open_access_pools_without_open_access_links:

    if not pool.identifier:
        no_identifier += 1
        continue

    # Do we have a resource for this pool?
    if pool.delivery_mechanisms and pool.delivery_mechanisms[0].resource:
        resource = pool.delivery_mechanisms[0].resource
        identifier = pool.identifier

        link, is_new = get_one_or_create(
            _db, Hyperlink, identifier=identifier,
            resource=resource, license_pool=pool,
            data_source=pool.data_source, rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
        )

        if not is_new:
            print("Expected to create a new open access link for pool %s but one already existed" % pool)
        else:
            fixed += 1
            pool.presentation_edition.set_open_access_link()

            if not fixed % 20:
                _db.commit()
    else:
        no_resource += 1

_db.commit()
print("Fixed %d pools" % fixed)
print("%d pools with no resource were not fixed" % no_resource)
print("%d pools with no identifier were not fixed" % no_identifier)
