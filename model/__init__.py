# encoding: utf-8
from nose.tools import set_trace

import log # Make sure logging is set up properly.
import logging
from psycopg2.extensions import adapt as sqlescape
from sqlalchemy import event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import compiler

Base = declarative_base()

from datasource_constants import DataSourceConstants
from edition_constants import EditionConstants
from link_relations import LinkRelations
from media_type_constants import MediaTypes

from has_full_table_cache import HasFullTableCache
from helper_methods import (
    create,
    flush,
    get_one,
    get_one_or_create,
    numericrange_to_string,
    numericrange_to_tuple,
    tuple_to_numericrange,
)
from session_manager import (
    BaseMaterializedWork,
    production_session,
    SessionManager,
)

class PresentationCalculationPolicy(object):
    """Which parts of the Work or Edition's presentation
    are we actually looking to update?
    """
    def __init__(self,
                 choose_edition=True,
                 set_edition_metadata=True,
                 classify=True,
                 choose_summary=True,
                 calculate_quality=True,
                 choose_cover=True,
                 regenerate_opds_entries=False,
                 update_search_index=False,
                 verbose=True,
    ):
        self.choose_edition = choose_edition
        self.set_edition_metadata = set_edition_metadata
        self.classify = classify
        self.choose_summary=choose_summary
        self.calculate_quality=calculate_quality
        self.choose_cover = choose_cover

        # We will regenerate OPDS entries if any of the metadata
        # changes, but if regenerate_opds_entries is True we will
        # _always_ do so. This is so we can regenerate _all_ the OPDS
        # entries if the OPDS presentation algorithm changes.
        self.regenerate_opds_entries = regenerate_opds_entries

        # Similarly for update_search_index.
        self.update_search_index = update_search_index

        self.verbose = verbose

    @classmethod
    def recalculate_everything(cls):
        """A PresentationCalculationPolicy that always recalculates
        everything, even when it doesn't seem necessary.
        """
        return PresentationCalculationPolicy(
            regenerate_opds_entries=True,
            update_search_index=True,
        )

    @classmethod
    def reset_cover(cls):
        """A PresentationCalculationPolicy that only resets covers
        (including updating cached entries, if necessary) without
        impacting any other metadata.
        """
        return cls(
            choose_cover=True,
            choose_edition=False,
            set_edition_metadata=False,
            classify=False,
            choose_summary=False,
            calculate_quality=False
        )

# When a pool gets a work and a presentation edition for the first time,
# the work should be added to any custom lists associated with the pool's
# collection.
# In some cases, the work may be generated before the presentation edition.
# Then we need to add it when the work gets a presentation edition.
from works import (
    WorkGenre,
    Work,
)
from licensing import (
    PolicyException,
    LicensePool,
    LicensePoolDeliveryMechanism,
    DeliveryMechanism,
    RightsStatus,
)

# from sqlalchemy.sql import compiler
# from psycopg2.extensions import adapt as sqlescape

def dump_query(query):
    dialect = query.session.bind.dialect
    statement = query.statement
    comp = compiler.SQLCompiler(dialect, statement)
    comp.compile()
    enc = dialect.encoding
    params = {}
    for k,v in comp.params.iteritems():
        if isinstance(v, unicode):
            v = v.encode(enc)
        params[k] = sqlescape(v)
    return (comp.string.encode(enc) % params).decode(enc)

# Certain ORM events, however they occur, indicate that a work's
# external index needs updating.

@event.listens_for(LicensePool, 'after_delete')
def licensepool_deleted(mapper, connection, target):
    """A LicensePool should never be deleted, but if it is, we need to
    keep the search index up to date.
    """
    work = target.work
    if work:
        record = work.external_index_needs_updating()

@event.listens_for(LicensePool.collection_id, 'set')
def licensepool_collection_change(target, value, oldvalue, initiator):
    """A LicensePool should never change collections, but if it is,
    we need to keep the search index up to date.
    """
    work = target.work
    if not work:
        return
    if value == oldvalue:
        return
    work.external_index_needs_updating()


@event.listens_for(LicensePool.licenses_owned, 'set')
def licenses_owned_change(target, value, oldvalue, initiator):
    """A Work may need to have its search document re-indexed if one of
    its LicensePools changes the number of licenses_owned to or from zero.
    """
    work = target.work
    if not work:
        return
    if target.open_access:
        # For open-access works, the licenses_owned value doesn't
        # matter.
        return
    if (value == oldvalue) or (value > 0 and oldvalue > 0):
        # The availability of this LicensePool has not changed. No need
        # to reindex anything.
        return
    work.external_index_needs_updating()

@event.listens_for(LicensePool.open_access, 'set')
def licensepool_open_access_change(target, value, oldvalue, initiator):
    """A Work may need to have its search document re-indexed if one of
    its LicensePools changes its open-access status.

    This shouldn't ever happen.
    """
    work = target.work
    if not work:
        return
    if value == oldvalue:
        return
    work.external_index_needs_updating()

from listeners import (
    site_configuration_has_changed,
    directly_modified,
)

from background import (
    BaseCoverageRecord,
    Timestamp,
    CoverageRecord,
    WorkCoverageRecord,
)
from bibliographic_metadata import (
    DataSource,
    Identifier,
    Equivalency,
    Edition,
)
from cached_feed import (
    WillNotGenerateExpensiveFeed,
    CachedFeed,
)
from circulation_event import CirculationEvent
from classification import (
    Subject,
    Classification,
    Genre,
)
from collection import (
    Collection,
    CollectionIdentifier,
    CollectionMissing,
)
from configuration import (
    ExternalIntegration,
    ConfigurationSetting,
    Admin,
    AdminRole
)
from complaint import Complaint
from contributions import (
    Contribution,
    Contributor,
    WorkContribution,
)
from credentials import (
    Credential,
    DelegatedPatronIdentifier,
    DRMDeviceIdentifier,
)
from custom_lists import (
    CustomList,
    CustomListEntry,
)
from integration_client import IntegrationClient
from library import Library
from measurement import Measurement
from patrons import (
    LoanAndHoldMixin,
    Patron,
    Loan,
    Hold,
    Annotation,
    PatronProfileStorage,
)
import listeners
from listeners import *
from resources import (
    Resource,
    ResourceTransformation,
    Hyperlink,
    Representation,
)
