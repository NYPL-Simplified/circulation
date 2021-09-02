#!/usr/bin/env python3
"""
Copy the collection configuration information from the JSON configuration
into Collection objects.
"""

import os
import sys
import uuid

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from api.config import Configuration        # noqa: E402
from core.model import (                    # noqa: E402
    get_one_or_create,
    production_session,
    DataSource,
    Library,
    Collection,
)

# We're going directly against the configuration object, rather than
# using shortcuts like OverdriveAPI.from_environment, because this
# script may be running against a branch where the implementation of
# those shortcuts goes against the database.

_db = production_session()


def copy_library_registry_information(_db, library):
    config = Configuration.integration("Adobe Vendor ID")
    if not config:
        print("No Adobe Vendor ID configuration, not setting short name or secret.")
        return
    library.short_name = config.get("library_short_name")
    library.library_registry_short_name = config.get("library_short_name")
    library.library_registry_shared_secret = config.get("authdata_secret")


def convert_overdrive(_db, library):
    config = Configuration.integration('Overdrive')
    if not config:
        print("No Overdrive configuration, not creating a Collection for it.")
        return
    print("Creating Collection object for Overdrive collection.")
    username = config.get('client_key')
    password = config.get('client_secret')
    library_id = config.get('library_id')
    website_id = config.get('website_id')

    collection, ignore = get_one_or_create(
        _db, Collection,
        protocol=Collection.OVERDRIVE,
        name="Overdrive"
    )
    library.collections.append(collection)
    collection.external_integration.username = username
    collection.external_integration.password = password
    collection.external_account_id = library_id
    collection.external_integration.set_setting("website_id", website_id)


def convert_bibliotheca(_db, library):
    config = Configuration.integration('3M')
    if not config:
        print("No Bibliotheca configuration, not creating a Collection for it.")
        return
    print("Creating Collection object for Bibliotheca collection.")
    username = config.get('account_id')
    password = config.get('account_key')
    library_id = config.get('library_id')
    collection, ignore = get_one_or_create(
        _db, Collection,
        protocol=Collection.BIBLIOTHECA,
        name="Bibliotheca"
    )
    library.collections.append(collection)
    collection.external_integration.username = username
    collection.external_integration.password = password
    collection.external_account_id = library_id


def convert_axis(_db, library):
    config = Configuration.integration('Axis 360')
    if not config:
        print("No Axis 360 configuration, not creating a Collection for it.")
        return
    print("Creating Collection object for Axis 360 collection.")
    username = config.get('username')
    password = config.get('password')
    library_id = config.get('library_id')
    # This is not technically a URL, it's "production" or "staging",
    # but it's converted into a URL internally.
    url = config.get('server')
    collection, ignore = get_one_or_create(
        _db, Collection,
        protocol=Collection.AXIS_360,
        name="Axis 360"
    )
    library.collections.append(collection)
    collection.external_integration.username = username
    collection.external_integration.password = password
    collection.external_account_id = library_id
    collection.external_integration.url = url


def convert_one_click(_db, library):
    config = Configuration.integration('OneClick')
    if not config:
        print("No OneClick configuration, not creating a Collection for it.")
        return
    print("Creating Collection object for OneClick collection.")
    basic_token = config.get('basic_token')
    library_id = config.get('library_id')
    url = config.get('url')
    ebook_loan_length = config.get('ebook_loan_length')
    eaudio_loan_length = config.get('eaudio_loan_length')

    collection, ignore = get_one_or_create(
        _db, Collection,
        protocol=Collection.ONECLICK,
        name="OneClick"
    )
    library.collections.append(collection)
    collection.external_integration.password = basic_token
    collection.external_account_id = library_id
    collection.external_integration.url = url
    collection.external_integration.set_setting("ebook_loan_length", ebook_loan_length)
    collection.external_integration.set_setting("eaudio_loan_length", eaudio_loan_length)


def convert_content_server(_db, library):
    config = Configuration.integration("Content Server")
    if not config:
        print("No content server configuration, not creating a Collection for it.")
        return
    url = config.get('url')                     # noqa: F841
    collection, ignore = get_one_or_create(
        _db, Collection,
        protocol=Collection.OPDS_IMPORT,
        name="Open Access Content Server"
    )
    collection.external_integration.setting("data_source").value = DataSource.OA_CONTENT_SERVER
    library.collections.append(collection)


# This is the point in the migration where we first create a Library
# for this system.
library = get_one_or_create(
    _db, Library,
    create_method_kwargs=dict(
        name="Default Library",
        short_name="default",
        uuid=str(uuid.uuid4())
    )
)

copy_library_registry_information(_db, library)
convert_overdrive(_db, library)
convert_bibliotheca(_db, library)
convert_axis(_db, library)
convert_one_click(_db, library)
convert_content_server(_db, library)
_db.commit()
