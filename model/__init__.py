# encoding: utf-8


import logging
import os
import warnings
from psycopg2.extensions import adapt as sqlescape
from psycopg2.extras import NumericRange
from sqlalchemy import (
    Column,
    create_engine,
    ForeignKey,
    Integer,
    Table,
    text,
)
from sqlalchemy.exc import (
    IntegrityError,
    SAWarning,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    relationship,
    sessionmaker,
)
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)
from sqlalchemy.sql import (
    compiler,
    select,
)
from sqlalchemy.sql.expression import (
    literal_column,
    table,
)

Base = declarative_base()

from .constants import (
    DataSourceConstants,
    EditionConstants,
    IdentifierConstants,
    LinkRelations,
    MediaTypes,
)
from .. import classifier

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
    except MultipleResultsFound:
        if on_multiple == 'error':
            raise
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
        except IntegrityError as e:
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
        lower += 1
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

    DEFAULT_LEVELS = 3
    DEFAULT_THRESHOLD = 0.5
    DEFAULT_CUTOFF = 1000

    def __init__(self,
                 choose_edition=True,
                 set_edition_metadata=True,
                 classify=True,
                 choose_summary=True,
                 calculate_quality=True,
                 choose_cover=True,
                 regenerate_opds_entries=False,
                 regenerate_marc_record=False,
                 update_search_index=False,
                 verbose=True,
                 equivalent_identifier_levels=DEFAULT_LEVELS,
                 equivalent_identifier_threshold=DEFAULT_THRESHOLD,
                 equivalent_identifier_cutoff=DEFAULT_CUTOFF,
    ):
        """Constructor.

        :param choose_edition: Should a new presentation edition be
           chosen/created, or should we assume the old one is fine?
        :param set_edition_metadata: Should we set new values for
           basic metadata such as title?
        :param classify: Should we reconsider which Genres under which
           a Work should be filed?
        :param choose_summary: Should we reconsider which of the
           available summaries is the best?
        :param calculate_quality: Should we recalculate the overall
           quality of the Work?
        :param choose_cover: Should we reconsider which of the
           available cover images is the best?
        :param regenerate_opds_entries: Should we recreate the OPDS entries
           for this Work?
        :param regenerate_marc_record: Should we regenerate the MARC record
           for this Work?
        :param update_search_index: Should we reindex this Work's
           entry in the search index?
        :param verbose: Should we print out information about the work we're
           doing?
        :param equivalent_identifier_levels: When determining which
           identifiers refer to this Work (used when gathering
           classifications, cover images, etc.), how many levels of
           equivalency should we go down? E.g. for one level of
           equivalency we will go from a proprietary vendor ID to the
           equivalent ISBN.
        :param equivalent_identifier_threshold: When determining which
           identifiers refer to this Work, what is the probability
           threshold for 'equivalency'? E.g. a value of 1 means that
           we will not count two identifiers as equivalent unless we
           are absolutely certain.
        :param equivalent_identifier_cutoff: When determining which
           identifiers refer to this work, how many Identifiers are
           enough? Gathering _all_ the identifiers that identify an
           extremely popular work can take an extraordinarily long time
           for very little payoff, so it's useful to have a cutoff.

           The cutoff is applied _per level_, so the total maximum
           number of equivalent identifiers is
           equivalent_identifier_cutoff * equivalent_identifier_levels.
        """
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
        # The same is true for the MARC records, except that they will
        # never be generated unless a MARC organization code is set
        # in a sitewide configuration setting.
        self.regenerate_opds_entries = regenerate_opds_entries
        self.regenerate_marc_record = regenerate_marc_record

        # Similarly for update_search_index.
        self.update_search_index = update_search_index

        self.verbose = verbose

        self.equivalent_identifier_levels = equivalent_identifier_levels
        self.equivalent_identifier_threshold = equivalent_identifier_threshold
        self.equivalent_identifier_cutoff = equivalent_identifier_cutoff


    @classmethod
    def recalculate_everything(cls):
        """A PresentationCalculationPolicy that always recalculates
        everything, even when it doesn't seem necessary.
        """
        return PresentationCalculationPolicy(
            regenerate_opds_entries=True,
            regenerate_marc_record=True,
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
    for k,v in list(comp.params.items()):
        if isinstance(v, str):
            v = v.encode(enc)
        params[k] = sqlescape(v)
    return (comp.string.encode(enc) % params).decode(enc)

DEBUG = False

class SessionManager(object):

    # A function that calculates recursively equivalent identifiers
    # is also defined in SQL.
    RECURSIVE_EQUIVALENTS_FUNCTION = 'recursive_equivalents.sql'

    engine_for_url = {}

    @classmethod
    def engine(cls, url=None):
        url = url or Configuration.database_url()
        return create_engine(url, echo=DEBUG)

    @classmethod
    def sessionmaker(cls, url=None, session=None):
        if not (url or session):
            url = Configuration.database_url()
        if url:
            bind_obj = cls.engine(url)
        elif session:
            bind_obj = session.get_bind()
            if not os.environ.get('TESTING'):
                # If a factory is being created from a session in test mode,
                # use the same Connection for all of the tests so objects can
                # be accessed. Otherwise, bind against an Engine object.
                bind_obj = bind_obj.engine
        return sessionmaker(bind=bind_obj)

    @classmethod
    def resource_directory(cls):
        """The directory containing SQL files used in database setup."""
        base_path = os.path.split(__file__)[0]
        return os.path.join(base_path, "files")

    @classmethod
    def initialize(cls, url, initialize_data=True, initialize_schema=True):
        """Initialize the database.

        This includes the schema, the custom functions, and the
        initial content.
        """
        if url in cls.engine_for_url:
            engine = cls.engine_for_url[url]
            return engine, engine.connect()

        engine = cls.engine(url)
        if initialize_schema:
            cls.initialize_schema(engine)
        connection = engine.connect()

        # Check if the recursive equivalents function exists already.
        query = select(
            [literal_column('proname')]
        ).select_from(
            table('pg_proc')
        ).where(
            literal_column('proname')=='fn_recursive_equivalents'
        )
        result = connection.execute(query)
        result = list(result)

        # If it doesn't, create it.
        if not result and initialize_data:
            resource_file = os.path.join(
                cls.resource_directory(), cls.RECURSIVE_EQUIVALENTS_FUNCTION
            )
            if not os.path.exists(resource_file):
                raise IOError("Could not load recursive equivalents function from %s: file does not exist." % resource_file)
            sql = open(resource_file).read()
            connection.execute(sql)

        if initialize_data:
            session = Session(connection)
            cls.initialize_data(session)

        if connection:
            connection.close()

        if initialize_schema and initialize_data:
            # Only cache the engine if all initialization has been performed.
            #
            # Some pieces of code (e.g. the script that runs
            # migrations) have a legitimate need to bypass some of the
            # initialization, but normal operation of the site
            # requires that everything be initialized.
            #
            # Until someone tells this method to initialize
            # everything, we can't short-circuit this method with a
            # cache.
            cls.engine_for_url[url] = engine
        return engine, engine.connect()

    @classmethod
    def initialize_schema(cls, engine):
        """Initialize the database schema."""
        # Use SQLAlchemy to create all the tables.
        to_create = [
            table_obj for name, table_obj in list(Base.metadata.tables.items())
            if not name.startswith('mv_')
        ]
        Base.metadata.create_all(engine, tables=to_create)

    @classmethod
    def session(cls, url, initialize_data=True, initialize_schema=True):
        engine = connection = 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=SAWarning)
            engine, connection = cls.initialize(
                url, initialize_data=initialize_data,
                initialize_schema=initialize_schema
            )
        session = Session(connection)
        if initialize_data:
            session = cls.initialize_data(session)
        return session

    @classmethod
    def initialize_data(cls, session, set_site_configuration=True):
        # Create initial content.
        from .datasource import DataSource
        from .classification import Genre
        from .licensing import DeliveryMechanism
        list(DataSource.well_known_sources(session))

        # Load all existing Genre objects.
        Genre.populate_cache(session)

        # Create any genres not in the database.
        for g in list(classifier.genres.values()):
            # TODO: On the very first startup this is rather expensive
            # because the cache is invalidated every time a Genre is
            # created, then populated the next time a Genre is looked
            # up. This wouldn't be a big problem, but this also happens
            # on setup for the unit tests.
            Genre.lookup(session, g, autocreate=True)

        # Make sure that the mechanisms fulfillable by the default
        # client are marked as such.
        for content_type, drm_scheme in DeliveryMechanism.default_client_can_fulfill_lookup:
            mechanism, is_new = DeliveryMechanism.lookup(
                session, content_type, drm_scheme
            )
            mechanism.default_client_can_fulfill = True

        # If there is currently no 'site configuration change'
        # Timestamp in the database, create one.
        timestamp, is_new = get_one_or_create(
            session, Timestamp, collection=None,
            service=Configuration.SITE_CONFIGURATION_CHANGED,
            create_method_kwargs=dict(finish=datetime.datetime.utcnow())
        )
        if is_new:
            site_configuration_has_changed(session)
        session.commit()

        # Return a potentially-new Session object in case
        # it was updated by cls.update_timestamps_table
        return session

def production_session(initialize_data=True):
    url = Configuration.database_url()
    if url.startswith('"'):
        url = url[1:]
    logging.debug("Database url: %s", url)
    _db = SessionManager.session(url, initialize_data=initialize_data)

    # The first thing to do after getting a database connection is to
    # set up the logging configuration.
    #
    # If called during a unit test, this will configure logging
    # incorrectly, but 1) this method isn't normally called during
    # unit tests, and 2) package_setup() will call initialize() again
    # with the right arguments.
    from ..log import LogConfiguration
    LogConfiguration.initialize(_db)
    return _db

from .admin import (
    Admin,
    AdminRole,
)
from .coverage import (
    BaseCoverageRecord,
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from .cachedfeed import (
    CachedFeed,
    WillNotGenerateExpensiveFeed,
    CachedMARCFile,
)
from .circulationevent import CirculationEvent
from .classification import (
    Classification,
    Genre,
    Subject,
)
from .collection import (
    Collection,
    CollectionIdentifier,
    CollectionMissing,
    collections_identifiers,
)
from .configuration import (
    ConfigurationSetting,
    ExternalIntegration,
    ExternalIntegrationLink,
)
from .complaint import Complaint
from .contributor import (
    Contribution,
    Contributor,
)
from .credential import (
    Credential,
    DelegatedPatronIdentifier,
    DRMDeviceIdentifier,
)
from .customlist import (
    CustomList,
    CustomListEntry,
)
from .datasource import DataSource
from .edition import Edition
from .hasfulltablecache import HasFullTableCache
from .identifier import (
    Equivalency,
    Identifier,
)
from .integrationclient import IntegrationClient
from .library import Library
from .licensing import (
    DeliveryMechanism,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
    PolicyException,
    RightsStatus,
)
from .measurement import Measurement
from .patron import (
    Annotation,
    Hold,
    Loan,
    LoanAndHoldMixin,
    Patron,
    PatronProfileStorage,
)
from .listeners import *
from .resource import (
    Hyperlink,
    Representation,
    Resource,
    ResourceTransformation,
)
from .work import (
    Work,
    WorkGenre,
)
