# encoding: utf-8
from cStringIO import StringIO
from collections import (
    Counter,
    defaultdict,
)
from lxml import etree
from nose.tools import set_trace
import base64
import bisect
import datetime
import isbnlib
import json
import logging
import md5
import operator
import os
import random
import re
import requests
from threading import RLock
import time
import traceback
import urllib
import urlparse
import uuid
import warnings
import bcrypt

from PIL import (
    Image,
)

from psycopg2.extras import NumericRange
from sqlalchemy.engine.base import Connection
from sqlalchemy import exc as sa_exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    event,
    exists,
    func,
    MetaData,
    Table,
    text,
)
from sqlalchemy.sql import select
from sqlalchemy.orm import (
    backref,
    contains_eager,
    joinedload,
    lazyload,
    mapper,
    relationship,
    sessionmaker,
    synonym,
)
from sqlalchemy.orm.base import NO_VALUE
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)
from sqlalchemy.ext.mutable import (
    MutableDict,
)
from sqlalchemy.ext.associationproxy import (
    association_proxy,
)
from sqlalchemy.ext.hybrid import (
    hybrid_property,
)
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import (
    cast,
    and_,
    or_,
    select,
    join,
    literal,
    literal_column,
    case,
    table,
)
from sqlalchemy.exc import (
    IntegrityError
)
from sqlalchemy import (
    create_engine,
    func,
    Binary,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    Index,
    Numeric,
    String,
    Table,
    Unicode,
    UniqueConstraint,
)

import log # Make sure logging is set up properly.
from config import (
    Configuration,
    CannotLoadConfiguration,
)
import classifier
from classifier import (
    Classifier,
    Erotica,
    COMICS_AND_GRAPHIC_NOVELS,
    GenreData,
    WorkClassifier,
)
from entrypoint import EntryPoint
from facets import FacetConstants
from user_profile import ProfileStorage
from util import (
    fast_query_count,
    LanguageCodes,
    MetadataSimilarity,
    TitleProcessor,
)
from mirror import MirrorUploader
from util.http import (
    HTTP,
    RemoteIntegrationException,
)
from util.permanent_work_id import WorkIDCalculator
from util.personal_names import display_name_to_sort_name
from util.summary import SummaryEvaluator

from sqlalchemy.orm.session import Session

from sqlalchemy.dialects.postgresql import (
    ARRAY,
    HSTORE,
    JSON,
    INT4RANGE,
)

from media_type_constants import MediaTypes
from datasource_constants import DataSourceConstants
from link_relations import LinkRelations
from edition_constants import EditionConstants
from helper_methods import (
    create,
    flush,
    get_one,
    get_one_or_create,
    numericrange_to_string,
    numericrange_to_tuple,
    tuple_to_numericrange,
)

DEBUG = False

def production_session():
    url = Configuration.database_url()
    if url.startswith('"'):
        url = url[1:]
    logging.debug("Database url: %s", url)
    _db = SessionManager.session(url)

    # The first thing to do after getting a database connection is to
    # set up the logging configuration.
    #
    # If called during a unit test, this will configure logging
    # incorrectly, but 1) this method isn't normally called during
    # unit tests, and 2) package_setup() will call initialize() again
    # with the right arguments.
    from log import LogConfiguration
    LogConfiguration.initialize(_db)
    return _db

class PolicyException(Exception):
    pass


class CollectionMissing(Exception):
    """An operation was attempted that can only happen within the context
    of a Collection, but there was no Collection available.
    """


class BaseMaterializedWork(object):
    """A mixin class for materialized views that incorporate Work and Edition."""
    pass


class HasFullTableCache(object):
    """A mixin class for ORM classes that maintain an in-memory cache of
    (hopefully) every item in the database table for performance reasons.
    """

    RESET = object()

    # You MUST define your own class-specific '_cache' and '_id_cache'
    # variables, like so:
    #
    # _cache = HasFullTableCache.RESET
    # _id_cache = HasFullTableCache.RESET

    @classmethod
    def reset_cache(cls):
        cls._cache = cls.RESET
        cls._id_cache = cls.RESET

    def cache_key(self):
        raise NotImplementedError()

    @classmethod
    def _cache_insert(cls, obj, cache, id_cache):
        """Cache an object for later retrieval, possibly by a different
        database session.
        """
        key = obj.cache_key()
        id = obj.id
        try:
            if cache != cls.RESET:
                cache[key] = obj
            if id_cache != cls.RESET:
                id_cache[id] = obj
        except TypeError, e:
            # The cache was reset in between the time we checked for a
            # reset and the time we tried to put an object in the
            # cache. Stop trying to mess with the cache.
            pass

    @classmethod
    def populate_cache(cls, _db):
        """Populate the in-memory caches from scratch with every single
        object from the database table.
        """
        cache = {}
        id_cache = {}
        for obj in _db.query(cls):
            cls._cache_insert(obj, cache, id_cache)
        cls._cache = cache
        cls._id_cache = id_cache

    @classmethod
    def _cache_lookup(cls, _db, cache, cache_name, cache_key, lookup_hook):
        """Helper method used by both by_id and by_cache_key.

        Looks up `cache_key` in `cache` and calls `lookup_hook`
        to find/create it if it's not in there.
        """
        new = False
        obj = None
        if cache == cls.RESET:
            # The cache has been reset. Populate it with the contents
            # of the table.
            cls.populate_cache(_db)

            # Get the new value of the cache, replacing the value
            # that turned out to be cls.RESET.
            cache = getattr(cls, cache_name)

        if cache != cls.RESET:
            try:
                obj = cache.get(cache_key)
            except TypeError, e:
                # This shouldn't happen. Even if the actual cache was
                # reset just now, we still have a copy of the 'old'
                # cache which passed the 'cache != cls.RESET' test.
                pass

        if not obj:
            # Either this object didn't exist when the cache was
            # populated, or the cache was reset while we were trying
            # to look it up.
            #
            # Give up on the cache and go direct to the database,
            # creating the object if necessary.
            if lookup_hook:
                obj, new = lookup_hook()
            else:
                obj = None
            if not obj:
                # The object doesn't exist and couldn't be created.
                return obj, new

            # Stick the object in the caches, assuming they're not
            # currently in a reset state.
            cls._cache_insert(obj, cls._cache, cls._id_cache)

        if obj and obj not in _db:
            try:
                obj = _db.merge(obj, load=False)
            except Exception, e:
                logging.error(
                    "Unable to merge cached object %r into database session",
                    obj, exc_info=e
                )
                # Try to look up a fresh copy of the object.
                obj, new = lookup_hook()
                if obj and obj in _db:
                    logging.error("Was able to look up a fresh copy of %r", obj)
                    return obj, new

                # That didn't work. Re-raise the original exception.
                logging.error("Unable to look up a fresh copy of %r", obj)
                raise e
        return obj, new

    @classmethod
    def by_id(cls, _db, id):
        """Look up an item by its unique database ID."""
        def lookup_hook():
            return get_one(_db, cls, id=id), False
        obj, is_new = cls._cache_lookup(
            _db, cls._id_cache, '_id_cache', id, lookup_hook
        )
        return obj

    @classmethod
    def by_cache_key(cls, _db, cache_key, lookup_hook):
        return cls._cache_lookup(
            _db, cls._cache, '_cache', cache_key, lookup_hook
        )

class SessionManager(object):

    # Materialized views need to be created and indexed from SQL
    # commands kept in files. This dictionary maps the views to the
    # SQL files.

    MATERIALIZED_VIEW_WORKS = 'mv_works_editions_datasources_identifiers'
    MATERIALIZED_VIEW_WORKS_WORKGENRES = 'mv_works_editions_workgenres_datasources_identifiers'
    MATERIALIZED_VIEW_LANES = 'mv_works_for_lanes'
    MATERIALIZED_VIEWS = {
        #MATERIALIZED_VIEW_WORKS : 'materialized_view_works.sql',
        #MATERIALIZED_VIEW_WORKS_WORKGENRES : 'materialized_view_works_workgenres.sql',
        MATERIALIZED_VIEW_LANES : 'materialized_view_for_lanes.sql',
    }

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
    def initialize(cls, url, create_materialized_work_class=True):
        if url in cls.engine_for_url:
            engine = cls.engine_for_url[url]
            return engine, engine.connect()

        engine = cls.engine(url)
        Base.metadata.create_all(engine)

        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files")

        connection = None
        for view_name, filename in cls.MATERIALIZED_VIEWS.items():
            if engine.has_table(view_name):
                continue
            if not connection:
                connection = engine.connect()
            resource_file = os.path.join(resource_path, filename)
            if not os.path.exists(resource_file):
                raise IOError("Could not load materialized view from %s: file does not exist." % resource_file)
            logging.info(
                "Loading materialized view %s from %s.",
                view_name, resource_file)
            sql = open(resource_file).read()
            connection.execution_options(isolation_level='AUTOCOMMIT')\
                .execute(text(sql))

            # NOTE: This is apparently necessary for the creation of
            # the materialized view to be finalized in all cases. As
            # such, materialized views should be created WITH NO DATA,
            # since they will be refreshed immediately after creation.
            result = connection.execute(
                "REFRESH MATERIALIZED VIEW %s;" % view_name
            )

        if not connection:
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
        if not result:
            resource_file = os.path.join(resource_path, cls.RECURSIVE_EQUIVALENTS_FUNCTION)
            if not os.path.exists(resource_file):
                raise IOError("Could not load recursive equivalents function from %s: file does not exist." % resource_file)
            sql = open(resource_file).read()
            connection.execute(sql)

        if connection:
            connection.close()

        if create_materialized_work_class:
            class MaterializedWorkWithGenre(Base, BaseMaterializedWork):
                __table__ = Table(
                    cls.MATERIALIZED_VIEW_LANES,
                    Base.metadata,
                    Column('works_id', Integer, primary_key=True, index=True),
                    Column('workgenres_id', Integer, primary_key=True, index=True),
                    Column('list_id', Integer, ForeignKey('customlists.id'),
                           primary_key=True, index=True),
                    Column(
                        'list_edition_id', Integer, ForeignKey('editions.id'),
                        primary_key=True, index=True
                    ),
                    Column(
                        'license_pool_id', Integer,
                        ForeignKey('licensepools.id'), primary_key=True,
                        index=True
                    ),
                    autoload=True,
                    autoload_with=engine
                )
                license_pool = relationship(
                    LicensePool,
                    primaryjoin="LicensePool.id==MaterializedWorkWithGenre.license_pool_id",
                    foreign_keys=LicensePool.id, lazy='joined', uselist=False)

            globals()['MaterializedWorkWithGenre'] = MaterializedWorkWithGenre

        cls.engine_for_url[url] = engine
        return engine, engine.connect()

    @classmethod
    def refresh_materialized_views(self, _db):
        for view_name in self.MATERIALIZED_VIEWS.keys():
            _db.execute("refresh materialized view %s;" % view_name)
            _db.commit()
        # Immediately update the number of works associated with each
        # lane.
        from lane import Lane
        for lane in _db.query(Lane):
            lane.update_size(_db)

    @classmethod
    def session(cls, url, initialize_data=True):
        engine = connection = 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            engine, connection = cls.initialize(
                url, create_materialized_work_class=initialize_data
            )
        session = Session(connection)
        if initialize_data:
            session = cls.initialize_data(session)
        return session

    @classmethod
    def initialize_data(cls, session, set_site_configuration=True):
        # Create initial data sources.
        list(DataSource.well_known_sources(session))

        # Load all existing Genre objects.
        Genre.populate_cache(session)

        # Create any genres not in the database.
        for g in classifier.genres.values():
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
            create_method_kwargs=dict(timestamp=datetime.datetime.utcnow())
        )
        if is_new:
            site_configuration_has_changed(session)
        session.commit()

        # Return a potentially-new Session object in case
        # it was updated by cls.update_timestamps_table
        return session

Base = declarative_base()

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

class Complaint(Base):
    """A complaint about a LicensePool (or, potentially, something else)."""

    __tablename__ = 'complaints'

    VALID_TYPES = set([
        u"http://librarysimplified.org/terms/problem/" + x
        for x in [
                'wrong-genre',
                'wrong-audience',
                'wrong-age-range',
                'wrong-title',
                'wrong-medium',
                'wrong-author',
                'bad-cover-image',
                'bad-description',
                'cannot-fulfill-loan',
                'cannot-issue-loan',
                'cannot-render',
                'cannot-return',
              ]
    ])

    LICENSE_POOL_TYPES = [
        'cannot-fulfill-loan',
        'cannot-issue-loan',
        'cannot-render',
        'cannot-return',
    ]

    id = Column(Integer, primary_key=True)

    # One LicensePool can have many complaints lodged against it.
    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True)

    # The type of complaint.
    type = Column(String, nullable=False, index=True)

    # The source of the complaint.
    source = Column(String, nullable=True, index=True)

    # Detailed information about the complaint.
    detail = Column(String, nullable=True)

    timestamp = Column(DateTime, nullable=False)

    # When the complaint was resolved.
    resolved = Column(DateTime, nullable=True)

    @classmethod
    def register(self, license_pool, type, source, detail, resolved=None):
        """Register a problem detail document as a Complaint against the
        given LicensePool.
        """
        if not license_pool:
            raise ValueError("No license pool provided")
        _db = Session.object_session(license_pool)
        if type not in self.VALID_TYPES:
            raise ValueError("Unrecognized complaint type: %s" % type)
        now = datetime.datetime.utcnow()
        if source:
            complaint, is_new = get_one_or_create(
                _db, Complaint,
                license_pool=license_pool,
                source=source, type=type,
                resolved=resolved,
                on_multiple='interchangeable',
                create_method_kwargs = dict(
                    timestamp=now,
                )
            )
            complaint.timestamp = now
            complaint.detail = detail
        else:
            complaint, is_new = create(
                _db,
                Complaint,
                license_pool=license_pool,
                source=source,
                type=type,
                timestamp=now,
                detail=detail,
                resolved=resolved
            )
        return complaint, is_new

    @property
    def for_license_pool(self):
        return any(self.type.endswith(t) for t in self.LICENSE_POOL_TYPES)

    def resolve(self):
        self.resolved = datetime.datetime.utcnow()
        return self.resolved

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
    LicensePool,
    LicensePoolDeliveryMechanism,
    DeliveryMechanism,
    RightsStatus,
)
@event.listens_for(LicensePool.work_id, 'set')
@event.listens_for(Work.presentation_edition_id, 'set')
def add_work_to_customlists_for_collection(pool_or_work, value, oldvalue, initiator):
    if isinstance(pool_or_work, LicensePool):
        work = pool_or_work.work
        pools = [pool_or_work]
    else:
        work = pool_or_work
        pools = work.license_pools

    if (not oldvalue or oldvalue is NO_VALUE) and value and work and work.presentation_edition:
        for pool in pools:
            if not pool.collection:
                # This shouldn't happen, but don't crash if it does --
                # the correct behavior is that the work not be added to
                # any CustomLists.
                continue
            for list in pool.collection.customlists:
                # Since the work was just created, we can assume that
                # there's already a pending registration for updating the
                # work's internal index, and decide not to create a
                # second one.
                list.add_entry(work, featured=True, update_external_index=False)

class IntegrationClient(Base):
    """A client that has authenticated access to this application.

    Currently used to represent circulation managers that have access
    to the metadata wrangler.
    """
    __tablename__ = 'integrationclients'

    id = Column(Integer, primary_key=True)

    # URL (or human readable name) to represent the server.
    url = Column(Unicode, unique=True)

    # Shared secret
    shared_secret = Column(Unicode, unique=True, index=True)

    created = Column(DateTime)
    last_accessed = Column(DateTime)

    loans = relationship('Loan', backref='integration_client')
    holds = relationship('Hold', backref='integration_client')

    def __repr__(self):
        return (u"<IntegrationClient: URL=%s ID=%s>" % (self.url, self.id)).encode('utf8')

    @classmethod
    def for_url(cls, _db, url):
        """Finds the IntegrationClient for the given server URL.

        :return: an IntegrationClient. If it didn't already exist,
        it will be created. If it didn't already have a secret, no
        secret will be set.
        """
        url = cls.normalize_url(url)
        now = datetime.datetime.utcnow()
        client, is_new = get_one_or_create(
            _db, cls, url=url, create_method_kwargs=dict(created=now)
        )
        client.last_accessed = now
        return client, is_new

    @classmethod
    def register(cls, _db, url, submitted_secret=None):
        """Creates a new server with client details."""
        client, is_new = cls.for_url(_db, url)

        if not is_new and (not submitted_secret or submitted_secret != client.shared_secret):
            raise ValueError('Cannot update existing IntegratedClient without valid shared_secret')

        generate_secret = (client.shared_secret is None) or submitted_secret
        if generate_secret:
            client.randomize_secret()

        return client, is_new

    @classmethod
    def normalize_url(cls, url):
        url = re.sub(r'^(http://|https://)', '', url)
        url = re.sub(r'^www\.', '', url)
        if url.endswith('/'):
            url = url[:-1]
        return unicode(url.lower())

    @classmethod
    def authenticate(cls, _db, shared_secret):
        client = get_one(_db, cls, shared_secret=unicode(shared_secret))
        if client:
            client.last_accessed = datetime.datetime.utcnow()
            # Committing immediately reduces the risk of contention.
            _db.commit()
            return client
        return None

    def randomize_secret(self):
        self.shared_secret = unicode(os.urandom(24).encode('hex'))

from sqlalchemy.sql import compiler
from psycopg2.extensions import adapt as sqlescape

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

site_configuration_has_changed_lock = RLock()
def site_configuration_has_changed(_db, timeout=1):
    """Call this whenever you want to indicate that the site configuration
    has changed and needs to be reloaded.

    This is automatically triggered on relevant changes to the data
    model, but you also should call it whenever you change an aspect
    of what you consider "site configuration", just to be safe.

    :param _db: Either a Session or (to save time in a common case) an
    ORM object that can turned into a Session.

    :param timeout: Nothing will happen if it's been fewer than this
    number of seconds since the last site configuration change was
    recorded.
    """
    has_lock = site_configuration_has_changed_lock.acquire(blocking=False)
    if not has_lock:
        # Another thread is updating site configuration right now.
        # There is no need to do anything--the timestamp will still be
        # accurate.
        return

    try:
        _site_configuration_has_changed(_db, timeout)
    finally:
        site_configuration_has_changed_lock.release()

def _site_configuration_has_changed(_db, timeout=1):
    """Actually changes the timestamp on the site configuration."""
    now = datetime.datetime.utcnow()
    last_update = Configuration._site_configuration_last_update()
    if not last_update or (now - last_update).total_seconds() > timeout:
        # The configuration last changed more than `timeout` ago, which
        # means it's time to reset the Timestamp that says when the
        # configuration last changed.

        # Convert something that might not be a Connection object into
        # a Connection object.
        if isinstance(_db, Base):
            _db = Session.object_session(_db)

        # Update the timestamp.
        now = datetime.datetime.utcnow()
        earlier = now-datetime.timedelta(seconds=timeout)
        sql = "UPDATE timestamps SET timestamp=:timestamp WHERE service=:service AND collection_id IS NULL AND timestamp<=:earlier;"
        _db.execute(
            text(sql),
            dict(service=Configuration.SITE_CONFIGURATION_CHANGED,
                 timestamp=now, earlier=earlier)
        )

        # Update the Configuration's record of when the configuration
        # was updated. This will update our local record immediately
        # without requiring a trip to the database.
        Configuration.site_configuration_last_update(
            _db, known_value=now
        )

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

def directly_modified(obj):
    """Return True only if `obj` has itself been modified, as opposed to
    having an object added or removed to one of its associated
    collections.
    """
    return Session.object_session(obj).is_modified(
        obj, include_collections=False
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
)
from configuration import (
    ExternalIntegration,
    ConfigurationSetting,
    Admin,
    AdminRole,
)
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
from resources import (
    Resource,
    ResourceTransformation,
    Hyperlink,
    Representation,
)

# Most of the time, we can know whether a change to the database is
# likely to require that the application reload the portion of the
# configuration it gets from the database. These hooks will call
# site_configuration_has_changed() whenever such a change happens.
#
# This is not supposed to be a comprehensive list of changes that
# should trigger a ConfigurationSetting reload -- that needs to be
# handled on the application level -- but it should be good enough to
# catch most that slip through the cracks.
@event.listens_for(Collection.children, 'append')
@event.listens_for(Collection.children, 'remove')
@event.listens_for(Collection.libraries, 'append')
@event.listens_for(Collection.libraries, 'remove')
@event.listens_for(ExternalIntegration.settings, 'append')
@event.listens_for(ExternalIntegration.settings, 'remove')
@event.listens_for(Library.integrations, 'append')
@event.listens_for(Library.integrations, 'remove')
@event.listens_for(Library.settings, 'append')
@event.listens_for(Library.settings, 'remove')
def configuration_relevant_collection_change(target, value, initiator):
    site_configuration_has_changed(target)

@event.listens_for(Library, 'after_insert')
@event.listens_for(Library, 'after_delete')
@event.listens_for(ExternalIntegration, 'after_insert')
@event.listens_for(ExternalIntegration, 'after_delete')
@event.listens_for(Collection, 'after_insert')
@event.listens_for(Collection, 'after_delete')
@event.listens_for(ConfigurationSetting, 'after_insert')
@event.listens_for(ConfigurationSetting, 'after_delete')
def configuration_relevant_lifecycle_event(mapper, connection, target):
    site_configuration_has_changed(target)

@event.listens_for(Library, 'after_update')
@event.listens_for(ExternalIntegration, 'after_update')
@event.listens_for(Collection, 'after_update')
@event.listens_for(ConfigurationSetting, 'after_update')
def configuration_relevant_update(mapper, connection, target):
    if directly_modified(target):
        site_configuration_has_changed(target)

@event.listens_for(Admin, 'after_insert')
@event.listens_for(Admin, 'after_delete')
@event.listens_for(Admin, 'after_update')
def refresh_admin_cache(mapper, connection, target):
    # The next time someone tries to access an Admin,
    # the cache will be repopulated.
    Admin.reset_cache()

@event.listens_for(AdminRole, 'after_insert')
@event.listens_for(AdminRole, 'after_delete')
@event.listens_for(AdminRole, 'after_update')
def refresh_admin_role_cache(mapper, connection, target):
    # The next time someone tries to access an AdminRole,
    # the cache will be repopulated.
    AdminRole.reset_cache()

@event.listens_for(Collection, 'after_insert')
@event.listens_for(Collection, 'after_delete')
@event.listens_for(Collection, 'after_update')
def refresh_collection_cache(mapper, connection, target):
    # The next time someone tries to access a Collection,
    # the cache will be repopulated.
    Collection.reset_cache()

@event.listens_for(ConfigurationSetting, 'after_insert')
@event.listens_for(ConfigurationSetting, 'after_delete')
@event.listens_for(ConfigurationSetting, 'after_update')
def refresh_configuration_settings(mapper, connection, target):
    # The next time someone tries to access a configuration setting,
    # the cache will be repopulated.
    ConfigurationSetting.reset_cache()

@event.listens_for(DataSource, 'after_insert')
@event.listens_for(DataSource, 'after_delete')
@event.listens_for(DataSource, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access a DataSource,
    # the cache will be repopulated.
    DataSource.reset_cache()

@event.listens_for(DeliveryMechanism, 'after_insert')
@event.listens_for(DeliveryMechanism, 'after_delete')
@event.listens_for(DeliveryMechanism, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access a DeliveryMechanism,
    # the cache will be repopulated.
    DeliveryMechanism.reset_cache()

@event.listens_for(ExternalIntegration, 'after_insert')
@event.listens_for(ExternalIntegration, 'after_delete')
@event.listens_for(ExternalIntegration, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access an ExternalIntegration,
    # the cache will be repopulated.
    ExternalIntegration.reset_cache()

@event.listens_for(Library, 'after_insert')
@event.listens_for(Library, 'after_delete')
@event.listens_for(Library, 'after_update')
def refresh_library_cache(mapper, connection, target):
    # The next time someone tries to access a library,
    # the cache will be repopulated.
    Library.reset_cache()

@event.listens_for(Genre, 'after_insert')
@event.listens_for(Genre, 'after_delete')
@event.listens_for(Genre, 'after_update')
def refresh_genre_cache(mapper, connection, target):
    # The next time someone tries to access a genre,
    # the cache will be repopulated.
    #
    # The only time this should really happen is the very first time a
    # site is brought up, but just in case.
    Genre.reset_cache()
