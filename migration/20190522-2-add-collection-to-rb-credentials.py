#!/usr/bin/env python3

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (                    # noqa: E402
    Credential,
    Collection,
    ExternalIntegration,
    DataSource,
    production_session
)
from api.rbdigital import RBDigitalAPI      # noqa: E402
from sqlalchemy import update, and_         # noqa: E402

_db = production_session()


def check_patron_id(api, patron):
    url = "%s/libraries/%s/patrons/%s" % (api.base_url, api.library_id, patron)
    response = api.request(url)
    return response.status_code == 200


# See if we have multiple RB Digital collections
rb_digital_collections = _db.query(Collection.id)\
    .select_from(ExternalIntegration)\
    .join(ExternalIntegration.collections)\
    .filter(ExternalIntegration.protocol == ExternalIntegration.RB_DIGITAL)\
    .all()

# We don't have to do any validation just update the credentials table
if len(rb_digital_collections) == 1:
    rb_collection_id = rb_digital_collections[0][0]
    source = DataSource.lookup(_db, DataSource.RB_DIGITAL)
    update_statement = update(Credential)\
        .where(and_(
            Credential.data_source_id == source.id,
            Credential.type == Credential.IDENTIFIER_FROM_REMOTE_SERVICE
        ))\
        .values(collection_id=rb_collection_id)
    _db.execute(update_statement)

# We have multiple RBDigital integration and we don't know which credential
# belongs to each one. Have to check each credential against RBDigital API.
else:
    rb_api = []
    for collection_id in rb_digital_collections:
        collection = Collection.by_id(_db, collection_id[0])
        rb_api.append(RBDigitalAPI(_db, collection))
    source = DataSource.lookup(_db, DataSource.RB_DIGITAL)
    credentials = _db.query(Credential)\
        .filter(
            Credential.data_source_id == source.id,
            Credential.type == Credential.IDENTIFIER_FROM_REMOTE_SERVICE,
            Credential.credential != None               # noqa: E711
        )
    for credential in credentials:
        for api in rb_api:
            if check_patron_id(api, credential.credential):
                credential.collection = api.collection
                break

# Remove credentials stored with none as the credential
source = DataSource.lookup(_db, DataSource.RB_DIGITAL)
credential = _db.query(Credential)\
    .filter(
        Credential.data_source_id == source.id,
        Credential.type == Credential.IDENTIFIER_FROM_REMOTE_SERVICE,
        Credential.credential == None                   # noqa: E711
    ).first()
if credential is not None:
    _db.delete(credential)

_db.commit()
_db.close()
