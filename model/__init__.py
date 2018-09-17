# encoding: utf-8
from nose.tools import set_trace

from psycopg2.extensions import adapt as sqlescape
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import compiler

Base = declarative_base()

from constants import (
    DataSourceConstants,
    EditionConstants,
    IdentifierConstants,
    LinkRelations,
    MediaTypes,
)
from helper_methods import (
    create,
    flush,
    get_one,
    get_one_or_create,
    numericrange_to_string,
    numericrange_to_tuple,
    tuple_to_numericrange,
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

from admins import (
    Admin,
    AdminRole,
)
from background import (
    BaseCoverageRecord,
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from bibliographic_metadata import (
    Equivalency,
    Identifier,
)
from cached_feed import (
    CachedFeed,
    WillNotGenerateExpensiveFeed,
)
from circulation_event import CirculationEvent
from classification import (
    Classification,
    Genre,
    Subject,
)
from collection import (
    Collection,
    CollectionIdentifier,
    CollectionMissing,
)
from configuration import (
    ConfigurationSetting,
    ExternalIntegration,
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
from datasource import DataSource
from edition import Edition
from has_full_table_cache import HasFullTableCache
from integration_client import IntegrationClient
from library import Library
from licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    PolicyException,
    RightsStatus,
)
from measurement import Measurement
from patrons import (
    Annotation,
    Hold,
    Loan,
    LoanAndHoldMixin,
    Patron,
    PatronProfileStorage,
)
import listeners
from listeners import *
from resources import (
    Hyperlink,
    Representation,
    Resource,
    ResourceTransformation,
)
from session_manager import (
    BaseMaterializedWork,
    production_session,
    SessionManager,
)
from works import (
    Work,
    WorkGenre,
)
