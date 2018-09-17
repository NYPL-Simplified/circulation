# encoding: utf-8
from nose.tools import set_trace

from psycopg2.extensions import adapt as sqlescape
from psycopg2.extras import NumericRange
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)
from sqlalchemy.sql import compiler

Base = declarative_base()

from constants import (
    DataSourceConstants,
    EditionConstants,
    IdentifierConstants,
    LinkRelations,
    MediaTypes,
)


def flush(db):
    """Flush the database connection unless it's known to already be flushing."""
    is_flushing = False
    if hasattr(db, '_flushing'):
        # This is a regular database session.
        is_flushing = db._flushing
    elif hasattr(db, 'registry'):
        # This is a flask_scoped_session scoped session.
        is_flushing = db.registry()._flushing
    else:
        logging.error("Unknown database connection type: %r", db)
    if not is_flushing:
        db.flush()

def create(db, model, create_method='',
           create_method_kwargs=None,
           **kwargs):
    kwargs.update(create_method_kwargs or {})
    created = getattr(model, create_method, model)(**kwargs)
    db.add(created)
    flush(db)
    return created, True

def get_one(db, model, on_multiple='error', constraint=None, **kwargs):
    """Gets an object from the database based on its attributes.

    :param constraint: A single clause that can be passed into
        `sqlalchemy.Query.filter` to limit the object that is returned.
    :return: object or None
    """
    constraint = constraint
    if 'constraint' in kwargs:
        constraint = kwargs['constraint']
        del kwargs['constraint']

    q = db.query(model).filter_by(**kwargs)
    if constraint is not None:
        q = q.filter(constraint)

    try:
        return q.one()
    except MultipleResultsFound, e:
        if on_multiple == 'error':
            raise e
        elif on_multiple == 'interchangeable':
            # These records are interchangeable so we can use
            # whichever one we want.
            #
            # This may be a sign of a problem somewhere else. A
            # database-level constraint might be useful.
            q = q.limit(1)
            return q.one()
    except NoResultFound:
        return None

def get_one_or_create(db, model, create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    one = get_one(db, model, **kwargs)
    if one:
        return one, False
    else:
        __transaction = db.begin_nested()
        try:
            # These kwargs are supported by get_one() but not by create().
            get_one_keys = ['on_multiple', 'constraint']
            for key in get_one_keys:
                if key in kwargs:
                    del kwargs[key]
            obj = create(db, model, create_method, create_method_kwargs, **kwargs)
            __transaction.commit()
            return obj
        except IntegrityError, e:
            logging.info(
                "INTEGRITY ERROR on %r %r, %r: %r", model, create_method_kwargs,
                kwargs, e)
            __transaction.rollback()
            return db.query(model).filter_by(**kwargs).one(), False

def numericrange_to_string(r):
    """Helper method to convert a NumericRange to a human-readable string."""
    if not r:
        return ""
    lower = r.lower
    upper = r.upper
    if upper is None and lower is None:
        return ""
    if lower and upper is None:
        return str(lower)
    if upper and lower is None:
        return str(upper)
    if not r.upper_inc:
        upper -= 1
    if not r.lower_inc:
        lower += 1
    if upper == lower:
        return str(lower)
    return "%s-%s" % (lower,upper)

def numericrange_to_tuple(r):
    """Helper method to normalize NumericRange into a tuple."""
    if r is None:
        return (None, None)
    lower = r.lower
    upper = r.upper
    if lower and not r.lower_inc:
        lower -= 1
    if upper and not r.upper_inc:
        upper -= 1
    return lower, upper

def tuple_to_numericrange(t):
    """Helper method to convert a tuple to an inclusive NumericRange."""
    if not t:
        return None
    return NumericRange(t[0], t[1], '[]')

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

from admin import (
    Admin,
    AdminRole,
)
from background import (
    BaseCoverageRecord,
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from cachedfeed import (
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
from contributor import (
    Contribution,
    Contributor,
    WorkContribution,
)
from credential import (
    Credential,
    DelegatedPatronIdentifier,
    DRMDeviceIdentifier,
)
from customlist import (
    CustomList,
    CustomListEntry,
)
from datasource import DataSource
from edition import Edition
from has_full_table_cache import HasFullTableCache
from identifier import (
    Equivalency,
    Identifier,
)
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
from patron import (
    Annotation,
    Hold,
    Loan,
    LoanAndHoldMixin,
    Patron,
    PatronProfileStorage,
)
import listeners
from listeners import *
from resource import (
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
from work import (
    Work,
    WorkGenre,
)
