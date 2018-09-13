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

class DataSource(Base, HasFullTableCache, DataSourceConstants):

    """A source for information about books, and possibly the books themselves."""


    __tablename__ = 'datasources'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)
    offers_licenses = Column(Boolean, default=False)
    primary_identifier_type = Column(String, index=True)
    extra = Column(MutableDict.as_mutable(JSON), default={})

    # One DataSource can have one IntegrationClient.
    integration_client_id = Column(
        Integer, ForeignKey('integrationclients.id'),
        unique=True, index=True, nullable=True)
    integration_client = relationship("IntegrationClient", backref=backref("data_source", uselist=False))

    # One DataSource can generate many Editions.
    editions = relationship("Edition", backref="data_source")

    # One DataSource can generate many CoverageRecords.
    coverage_records = relationship("CoverageRecord", backref="data_source")

    # One DataSource can generate many IDEquivalencies.
    id_equivalencies = relationship("Equivalency", backref="data_source")

    # One DataSource can grant access to many LicensePools.
    license_pools = relationship(
        "LicensePool", backref=backref("data_source", lazy='joined'))

    # One DataSource can provide many Hyperlinks.
    links = relationship("Hyperlink", backref="data_source")

    # One DataSource can provide many Resources.
    resources = relationship("Resource", backref="data_source")

    # One DataSource can generate many Measurements.
    measurements = relationship("Measurement", backref="data_source")

    # One DataSource can provide many Classifications.
    classifications = relationship("Classification", backref="data_source")

    # One DataSource can have many associated Credentials.
    credentials = relationship("Credential", backref="data_source")

    # One DataSource can generate many CustomLists.
    custom_lists = relationship("CustomList", backref="data_source")

    # One DataSource can have provide many LicensePoolDeliveryMechanisms.
    delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism", backref="data_source",
        foreign_keys=lambda: [LicensePoolDeliveryMechanism.data_source_id]
    )

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    def __repr__(self):
        return '<DataSource: name="%s">' % (self.name)

    def cache_key(self):
        return self.name

    @classmethod
    def lookup(cls, _db, name, autocreate=False, offers_licenses=False,
               primary_identifier_type=None):
        # Turn a deprecated name (e.g. "3M" into the current name
        # (e.g. "Bibliotheca").
        name = cls.DEPRECATED_NAMES.get(name, name)

        def lookup_hook():
            """There was no such DataSource in the cache. Look one up or
            create one.
            """
            if autocreate:
                data_source, is_new = get_one_or_create(
                    _db, DataSource, name=name,
                    create_method_kwargs=dict(
                        offers_licenses=offers_licenses,
                        primary_identifier_type=primary_identifier_type
                    )
                )
            else:
                data_source = get_one(_db, DataSource, name=name)
                is_new = False
            return data_source, is_new

        # Look up the DataSource in the full-table cache, falling back
        # to the database if necessary.
        obj, is_new = cls.by_cache_key(_db, name, lookup_hook)
        return obj

    URI_PREFIX = u"http://librarysimplified.org/terms/sources/"

    @classmethod
    def name_from_uri(cls, uri):
        """Turn a data source URI into a name suitable for passing
        into lookup().
        """
        if not uri.startswith(cls.URI_PREFIX):
            return None
        name = uri[len(cls.URI_PREFIX):]
        return urllib.unquote(name)

    @classmethod
    def from_uri(cls, _db, uri):
        return cls.lookup(_db, cls.name_from_uri(uri))

    @property
    def uri(self):
        return self.URI_PREFIX + urllib.quote(self.name)

    @classmethod
    def license_source_for(cls, _db, identifier):
        """Find the one DataSource that provides licenses for books identified
        by the given identifier.

        If there is no such DataSource, or there is more than one,
        raises an exception.
        """
        sources = cls.license_sources_for(_db, identifier)
        return sources.one()

    @classmethod
    def license_sources_for(cls, _db, identifier):
        """A query that locates all DataSources that provide licenses for
        books identified by the given identifier.
        """
        if isinstance(identifier, basestring):
            type = identifier
        else:
            type = identifier.type
        q =_db.query(DataSource).filter(DataSource.offers_licenses==True).filter(
            DataSource.primary_identifier_type==type)
        return q

    @classmethod
    def metadata_sources_for(cls, _db, identifier):
        """Finds the DataSources that provide metadata for books
        identified by the given identifier.
        """
        if isinstance(identifier, basestring):
            type = identifier
        else:
            type = identifier.type

        if not hasattr(cls, 'metadata_lookups_by_identifier_type'):
            # This should only happen during testing.
            list(DataSource.well_known_sources(_db))

        names = cls.metadata_lookups_by_identifier_type[type]
        return _db.query(DataSource).filter(DataSource.name.in_(names)).all()

    @classmethod
    def well_known_sources(cls, _db):
        """Make sure all the well-known sources exist in the database.
        """

        cls.metadata_lookups_by_identifier_type = defaultdict(list)

        for (name, offers_licenses, offers_metadata_lookup, primary_identifier_type, refresh_rate) in (
                (cls.GUTENBERG, True, False, Identifier.GUTENBERG_ID, None),
                (cls.RB_DIGITAL, True, True, Identifier.RB_DIGITAL_ID, None),
                (cls.OVERDRIVE, True, False, Identifier.OVERDRIVE_ID, 0),
                (cls.BIBLIOTHECA, True, False, Identifier.BIBLIOTHECA_ID, 60*60*6),
                (cls.ODILO, True, False, Identifier.ODILO_ID, 0),
                (cls.AXIS_360, True, False, Identifier.AXIS_360_ID, 0),
                (cls.OCLC, False, False, None, None),
                (cls.OCLC_LINKED_DATA, False, False, None, None),
                (cls.AMAZON, False, False, None, None),
                (cls.OPEN_LIBRARY, False, False, Identifier.OPEN_LIBRARY_ID, None),
                (cls.GUTENBERG_COVER_GENERATOR, False, False, Identifier.GUTENBERG_ID, None),
                (cls.GUTENBERG_EPUB_GENERATOR, False, False, Identifier.GUTENBERG_ID, None),
                (cls.WEB, True, False, Identifier.URI, None),
                (cls.VIAF, False, False, None, None),
                (cls.CONTENT_CAFE, True, True, Identifier.ISBN, None),
                (cls.MANUAL, False, False, None, None),
                (cls.NYT, False, False, Identifier.ISBN, None),
                (cls.LIBRARY_STAFF, False, False, None, None),
                (cls.METADATA_WRANGLER, False, False, None, None),
                (cls.PROJECT_GITENBERG, True, False, Identifier.GUTENBERG_ID, None),
                (cls.STANDARD_EBOOKS, True, False, Identifier.URI, None),
                (cls.UNGLUE_IT, True, False, Identifier.URI, None),
                (cls.ADOBE, False, False, None, None),
                (cls.PLYMPTON, True, False, Identifier.ISBN, None),
                (cls.ELIB, True, False, Identifier.ELIB_ID, None),
                (cls.OA_CONTENT_SERVER, True, False, None, None),
                (cls.NOVELIST, False, True, Identifier.NOVELIST_ID, None),
                (cls.PRESENTATION_EDITION, False, False, None, None),
                (cls.INTERNAL_PROCESSING, False, False, None, None),
                (cls.FEEDBOOKS, True, False, Identifier.URI, None),
                (cls.BIBBLIO, False, True, Identifier.BIBBLIO_CONTENT_ITEM_ID, None),
                (cls.ENKI, True, False, Identifier.ENKI_ID, None)
        ):

            obj = DataSource.lookup(
                _db, name, autocreate=True,
                offers_licenses=offers_licenses,
                primary_identifier_type = primary_identifier_type
            )

            if offers_metadata_lookup:
                l = cls.metadata_lookups_by_identifier_type[primary_identifier_type]
                l.append(obj.name)

            yield obj

class Equivalency(Base):
    """An assertion that two Identifiers identify the same work.

    This assertion comes with a 'strength' which represents how confident
    the data source is in the assertion.
    """
    __tablename__ = 'equivalents'

    # 'input' is the ID that was used as input to the datasource.
    # 'output' is the output
    id = Column(Integer, primary_key=True)
    input_id = Column(Integer, ForeignKey('identifiers.id'), index=True)
    input = relationship("Identifier", foreign_keys=input_id)
    output_id = Column(Integer, ForeignKey('identifiers.id'), index=True)
    output = relationship("Identifier", foreign_keys=output_id)

    # Who says?
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    # How many distinct votes went into this assertion? This will let
    # us scale the change to the strength when additional votes come
    # in.
    votes = Column(Integer, default=1)

    # How strong is this assertion (-1..1)? A negative number is an
    # assertion that the two Identifiers do *not* identify the
    # same work.
    strength = Column(Float, index=True)

    def __repr__(self):
        r = u"[%s ->\n %s\n source=%s strength=%.2f votes=%d)]" % (
            repr(self.input).decode("utf8"),
            repr(self.output).decode("utf8"),
            self.data_source.name, self.strength, self.votes
        )
        return r.encode("utf8")

    @classmethod
    def for_identifiers(self, _db, identifiers, exclude_ids=None):
        """Find all Equivalencies for the given Identifiers."""
        if not identifiers:
            return []
        if isinstance(identifiers, list) and isinstance(identifiers[0], Identifier):
            identifiers = [x.id for x in identifiers]
        q = _db.query(Equivalency).distinct().filter(
            or_(Equivalency.input_id.in_(identifiers),
                Equivalency.output_id.in_(identifiers))
        )
        if exclude_ids:
            q = q.filter(~Equivalency.id.in_(exclude_ids))
        return q

class Identifier(Base):
    """A way of uniquely referring to a particular edition.
    """

    # Common types of identifiers.
    OVERDRIVE_ID = u"Overdrive ID"
    ODILO_ID = u"Odilo ID"
    BIBLIOTHECA_ID = u"Bibliotheca ID"
    GUTENBERG_ID = u"Gutenberg ID"
    AXIS_360_ID = u"Axis 360 ID"
    ELIB_ID = u"eLiburutegia ID"
    ASIN = u"ASIN"
    ISBN = u"ISBN"
    NOVELIST_ID = u"NoveList ID"
    OCLC_WORK = u"OCLC Work ID"
    OCLC_NUMBER = u"OCLC Number"
    # RBdigital uses ISBNs for ebooks and eaudio, and its own ids for magazines
    RB_DIGITAL_ID = u"RBdigital ID"
    OPEN_LIBRARY_ID = u"OLID"
    BIBLIOCOMMONS_ID = u"Bibliocommons ID"
    URI = u"URI"
    DOI = u"DOI"
    UPC = u"UPC"
    BIBBLIO_CONTENT_ITEM_ID = u"Bibblio Content Item ID"
    ENKI_ID = u"Enki ID"

    DEPRECATED_NAMES = {
        u"3M ID" : BIBLIOTHECA_ID,
        u"OneClick ID" : RB_DIGITAL_ID,
    }
    THREEM_ID = BIBLIOTHECA_ID
    ONECLICK_ID = RB_DIGITAL_ID

    LICENSE_PROVIDING_IDENTIFIER_TYPES = [
        BIBLIOTHECA_ID, OVERDRIVE_ID, ODILO_ID, AXIS_360_ID,
        GUTENBERG_ID, ELIB_ID
    ]

    URN_SCHEME_PREFIX = "urn:librarysimplified.org/terms/id/"
    ISBN_URN_SCHEME_PREFIX = "urn:isbn:"
    GUTENBERG_URN_SCHEME_PREFIX = "http://www.gutenberg.org/ebooks/"
    GUTENBERG_URN_SCHEME_RE = re.compile(
        GUTENBERG_URN_SCHEME_PREFIX + "([0-9]+)")
    OTHER_URN_SCHEME_PREFIX = "urn:"

    __tablename__ = 'identifiers'
    id = Column(Integer, primary_key=True)
    type = Column(String(64), index=True)
    identifier = Column(String, index=True)

    equivalencies = relationship(
        "Equivalency",
        primaryjoin=("Identifier.id==Equivalency.input_id"),
        backref="input_identifiers", cascade="all, delete-orphan"
    )

    inbound_equivalencies = relationship(
        "Equivalency",
        primaryjoin=("Identifier.id==Equivalency.output_id"),
        backref="output_identifiers", cascade="all, delete-orphan"
    )

    # One Identifier may have many associated CoverageRecords.
    coverage_records = relationship("CoverageRecord", backref="identifier")

    def __repr__(self):
        records = self.primarily_identifies
        if records and records[0].title:
            title = u' prim_ed=%d ("%s")' % (records[0].id, records[0].title)
        else:
            title = ""
        return (u"%s/%s ID=%s%s" % (self.type, self.identifier, self.id,
                                    title)).encode("utf8")

    # One Identifier may serve as the primary identifier for
    # several Editions.
    primarily_identifies = relationship(
        "Edition", backref="primary_identifier"
    )

    # One Identifier may serve as the identifier for many
    # LicensePools, through different Collections.
    licensed_through = relationship(
        "LicensePool", backref="identifier", lazy='joined',
    )

    # One Identifier may have many Links.
    links = relationship(
        "Hyperlink", backref="identifier"
    )

    # One Identifier may be the subject of many Measurements.
    measurements = relationship(
        "Measurement", backref="identifier"
    )

    # One Identifier may participate in many Classifications.
    classifications = relationship(
        "Classification", backref="identifier"
    )

    # One identifier may participate in many Annotations.
    annotations = relationship(
        "Annotation", backref="identifier"
    )

    # One Identifier can have have many LicensePoolDeliveryMechanisms.
    delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism", backref="identifier",
        foreign_keys=lambda: [LicensePoolDeliveryMechanism.identifier_id]
    )

    # Type + identifier is unique.
    __table_args__ = (
        UniqueConstraint('type', 'identifier'),
    )

    @classmethod
    def from_asin(cls, _db, asin, autocreate=True):
        """Turn an ASIN-like string into an Identifier.

        If the string is an ISBN10 or ISBN13, the Identifier will be
        of type ISBN and the value will be the equivalent ISBN13.

        Otherwise the Identifier will be of type ASIN and the value will
        be the value of `asin`.
        """
        asin = asin.strip().replace("-", "")
        if isbnlib.is_isbn10(asin):
            asin = isbnlib.to_isbn13(asin)
        if isbnlib.is_isbn13(asin):
            type = cls.ISBN
        else:
            type = cls.ASIN
        return cls.for_foreign_id(_db, type, asin, autocreate)

    @classmethod
    def for_foreign_id(cls, _db, foreign_identifier_type, foreign_id,
                       autocreate=True):
        """Turn a foreign ID into an Identifier."""
        foreign_identifier_type, foreign_id = cls.prepare_foreign_type_and_identifier(
            foreign_identifier_type, foreign_id
        )
        if not foreign_identifier_type or not foreign_id:
            return None

        if autocreate:
            m = get_one_or_create
        else:
            m = get_one

        result = m(_db, cls, type=foreign_identifier_type,
                   identifier=foreign_id)

        if isinstance(result, tuple):
            return result
        else:
            return result, False

    @classmethod
    def prepare_foreign_type_and_identifier(cls, foreign_type, foreign_identifier):
        if not foreign_type or not foreign_identifier:
            return (None, None)

        # Turn a deprecated identifier type (e.g. "3M ID" into the
        # current type (e.g. "Bibliotheca ID").
        foreign_type = cls.DEPRECATED_NAMES.get(foreign_type, foreign_type)

        if foreign_type in (Identifier.OVERDRIVE_ID, Identifier.BIBLIOTHECA_ID):
            foreign_identifier = foreign_identifier.lower()

        if not cls.valid_as_foreign_identifier(foreign_type, foreign_identifier):
            raise ValueError('"%s" is not a valid %s.' % (
                foreign_identifier, foreign_type
            ))

        return (foreign_type, foreign_identifier)

    @classmethod
    def valid_as_foreign_identifier(cls, type, id):
        """Return True if the given `id` can be an Identifier of the given
        `type`.

        This is not a complete implementation; we will add to it as
        necessary.

        In general we err on the side of allowing IDs that look
        invalid (e.g. all Overdrive IDs look like UUIDs, but we
        currently don't enforce that). We only reject an ID out of
        hand if it will cause problems with a third-party API.
        """
        forbidden_characters = ''
        if type == Identifier.BIBLIOTHECA_ID:
            # IDs are joined with commas and provided as a URL path
            # element.  Embedded commas or slashes will confuse the
            # Bibliotheca API.
            forbidden_characters = ',/'
        elif type == Identifier.AXIS_360_ID:
            # IDs are joined with commas during a lookup. Embedded
            # commas will confuse the Axis 360 API.
            forbidden_characters = ','
        if any(x in id for x in forbidden_characters):
            return False
        return True

    @property
    def urn(self):
        identifier_text = urllib.quote(self.identifier)
        if self.type == Identifier.ISBN:
            return self.ISBN_URN_SCHEME_PREFIX + identifier_text
        elif self.type == Identifier.URI:
            return self.identifier
        elif self.type == Identifier.GUTENBERG_ID:
            return self.GUTENBERG_URN_SCHEME_PREFIX + identifier_text
        else:
            identifier_type = urllib.quote(self.type)
            return self.URN_SCHEME_PREFIX + "%s/%s" % (
                identifier_type, identifier_text)

    @property
    def work(self):
        """Find the Work, if any, associated with this Identifier.

        Although one Identifier may be associated with multiple LicensePools,
        all of them must share a Work.
        """
        for lp in self.licensed_through:
            if lp.work:
                return lp.work

    class UnresolvableIdentifierException(Exception):
        # Raised when an identifier that can't be resolved into a LicensePool
        # is provided in a context that requires a resolvable identifier
        pass

    @classmethod
    def type_and_identifier_for_urn(cls, identifier_string):
        if not identifier_string:
            return None, None
        m = cls.GUTENBERG_URN_SCHEME_RE.match(identifier_string)
        if m:
            type = Identifier.GUTENBERG_ID
            identifier_string = m.groups()[0]
        elif identifier_string.startswith("http:") or identifier_string.startswith("https:"):
            type = Identifier.URI
        elif identifier_string.startswith(Identifier.URN_SCHEME_PREFIX):
            identifier_string = identifier_string[len(Identifier.URN_SCHEME_PREFIX):]
            type, identifier_string = map(
                urllib.unquote, identifier_string.split("/", 1))
        elif identifier_string.startswith(Identifier.ISBN_URN_SCHEME_PREFIX):
            type = Identifier.ISBN
            identifier_string = identifier_string[len(Identifier.ISBN_URN_SCHEME_PREFIX):]
            identifier_string = urllib.unquote(identifier_string)
            # Make sure this is a valid ISBN, and convert it to an ISBN-13.
            if not (isbnlib.is_isbn10(identifier_string) or
                    isbnlib.is_isbn13(identifier_string)):
                raise ValueError("%s is not a valid ISBN." % identifier_string)
            if isbnlib.is_isbn10(identifier_string):
                identifier_string = isbnlib.to_isbn13(identifier_string)
        elif identifier_string.startswith(Identifier.OTHER_URN_SCHEME_PREFIX):
            type = Identifier.URI
        else:
            raise ValueError(
                "Could not turn %s into a recognized identifier." %
                identifier_string)
        return (type, identifier_string)

    @classmethod
    def parse_urns(cls, _db, identifier_strings, autocreate=True,
                   allowed_types=None):
        """Converts a batch of URNs into Identifier objects.

        :param _db: A database connection
        :param identifier_strings: A list of strings, each a URN
           identifying some identifier.

        :param autocreate: Create an Identifier for a URN if none
            presently exists.

        :param allowed_types: If this is a list of Identifier
            types, only identifiers of those types may be looked
            up. All other identifier types will be treated as though
            they did not exist.

        :return: A 2-tuple (identifiers, failures). `identifiers` is a
            list of Identifiers. `failures` is a list of URNs that
            did not become Identifiers.
        """
        if allowed_types is not None:
            allowed_types = set(allowed_types)
        failures = list()
        identifier_details = dict()
        for urn in identifier_strings:
            type = identifier = None
            try:
                (type, identifier) = cls.prepare_foreign_type_and_identifier(
                    *cls.type_and_identifier_for_urn(urn)
                )
                if (type and identifier and
                    (allowed_types is None or type in allowed_types)):
                    identifier_details[urn] = (type, identifier)
                else:
                    failures.append(urn)
            except ValueError as e:
                failures.append(urn)

        identifiers_by_urn = dict()
        def find_existing_identifiers(identifier_details):
            if not identifier_details:
                return
            and_clauses = list()
            for type, identifier in identifier_details:
                and_clauses.append(
                    and_(cls.type==type, cls.identifier==identifier)
                )

            identifiers = _db.query(cls).filter(or_(*and_clauses)).all()
            for identifier in identifiers:
                identifiers_by_urn[identifier.urn] = identifier

        # Find identifiers that are already in the database.
        find_existing_identifiers(identifier_details.values())

        # Remove the existing identifiers from the identifier_details list,
        # regardless of whether the provided URN was accurate.
        existing_details = [(i.type, i.identifier) for i in identifiers_by_urn.values()]
        identifier_details = {
            k: v for k, v in identifier_details.items()
            if v not in existing_details and k not in identifiers_by_urn.keys()
        }

        if not autocreate:
            # Don't make new identifiers. Send back unfound urns as failures.
            failures.extend(identifier_details.keys())
            return identifiers_by_urn, failures

        # Find any identifier details that don't correspond to an existing
        # identifier. Try to create them.
        new_identifiers = list()
        new_identifiers_details = set([])
        for urn, details in identifier_details.items():
            if details in new_identifiers_details:
                # For some reason, this identifier is here twice.
                # Don't try to insert it twice.
                continue
            new_identifiers.append(dict(type=details[0], identifier=details[1]))
            new_identifiers_details.add(details)

        # Insert new identifiers into the database, then add them to the
        # results.
        if new_identifiers:
            _db.bulk_insert_mappings(cls, new_identifiers)
            _db.commit()
        find_existing_identifiers(identifier_details.values())

        return identifiers_by_urn, failures

    @classmethod
    def parse_urn(cls, _db, identifier_string, must_support_license_pools=False):
        type, identifier_string = cls.type_and_identifier_for_urn(identifier_string)
        if must_support_license_pools:
            try:
                ls = DataSource.license_source_for(_db, type)
            except NoResultFound:
                raise Identifier.UnresolvableIdentifierException()
            except MultipleResultsFound:
                 # This is fine.
                pass

        return cls.for_foreign_id(_db, type, identifier_string)

    def equivalent_to(self, data_source, identifier, strength):
        """Make one Identifier equivalent to another.

        `data_source` is the DataSource that believes the two
        identifiers are equivalent.
        """
        _db = Session.object_session(self)
        if self == identifier:
            # That an identifier is equivalent to itself is tautological.
            # Do nothing.
            return None
        eq, new = get_one_or_create(
            _db, Equivalency,
            data_source=data_source,
            input=self,
            output=identifier,
            on_multiple='interchangeable'
        )
        eq.strength=strength
        if new:
            logging.info(
                "Identifier equivalency: %r==%r p=%.2f", self, identifier,
                strength
            )
        return eq

    @classmethod
    def recursively_equivalent_identifier_ids_query(
            cls, identifier_id_column, levels=5, threshold=0.50, cutoff=None):
        """Get a SQL statement that will return all Identifier IDs
        equivalent to a given ID at the given confidence threshold.

        `identifier_id_column` can be a single Identifier ID, or a column
        like `Edition.primary_identifier_id` if the query will be used as
        a subquery.

        This uses the function defined in files/recursive_equivalents.sql.
        """
        return select([func.fn_recursive_equivalents(identifier_id_column, levels, threshold, cutoff)])

    @classmethod
    def recursively_equivalent_identifier_ids(
            cls, _db, identifier_ids, levels=3, threshold=0.50, cutoff=None):
        """All Identifier IDs equivalent to the given set of Identifier
        IDs at the given confidence threshold.

        This uses the function defined in files/recursive_equivalents.sql.

        Four levels is enough to go from a Gutenberg text to an ISBN.
        Gutenberg ID -> OCLC Work IS -> OCLC Number -> ISBN

        Returns a dictionary mapping each ID in the original to a
        list of equivalent IDs.

        :param cutoff: For each recursion level, results will be cut
        off at this many results. (The maximum total number of results
        is levels * cutoff)
        """
        query = select([Identifier.id, func.fn_recursive_equivalents(Identifier.id, levels, threshold, cutoff)],
                       Identifier.id.in_(identifier_ids))
        results = _db.execute(query)
        equivalents = defaultdict(list)
        for r in results:
            original = r[0]
            equivalent = r[1]
            equivalents[original].append(equivalent)
        return equivalents

    def equivalent_identifier_ids(self, levels=5, threshold=0.5):
        _db = Session.object_session(self)
        return Identifier.recursively_equivalent_identifier_ids(
            _db, [self.id], levels, threshold)

    def licensed_through_collection(self, collection):
        """Find the LicensePool, if any, for this Identifier
        in the given Collection.

        :return: At most one LicensePool.
        """
        for lp in self.licensed_through:
            if lp.collection == collection:
                return lp

    def add_link(self, rel, href, data_source, media_type=None, content=None,
                 content_path=None, rights_status_uri=None, rights_explanation=None,
                 original_resource=None, transformation_settings=None):
        """Create a link between this Identifier and a (potentially new)
        Resource.

        TODO: There's some code in metadata_layer for automatically
        fetching, mirroring and scaling Representations as links are
        created. It might be good to move that code into here.
        """
        _db = Session.object_session(self)

        # Find or create the Resource.
        if not href:
            href = Hyperlink.generic_uri(data_source, self, rel, content)
        rights_status = None
        if rights_status_uri:
            rights_status = RightsStatus.lookup(_db, rights_status_uri)
        resource, new_resource = get_one_or_create(
            _db, Resource, url=href,
            create_method_kwargs=dict(data_source=data_source,
                                      rights_status=rights_status,
                                      rights_explanation=rights_explanation)
        )

        # Find or create the Hyperlink.
        link, new_link = get_one_or_create(
            _db, Hyperlink, rel=rel, data_source=data_source,
            identifier=self, resource=resource,
        )

        if content or content_path:
            # We have content for this resource.
            resource.set_fetched_content(media_type, content, content_path)
        elif (media_type and not resource.representation):
            # We know the type of the resource, so make a
            # Representation for it.
            resource.representation, is_new = get_one_or_create(
                _db, Representation, url=resource.url, media_type=media_type
            )

        if original_resource:
            original_resource.add_derivative(link.resource, transformation_settings)

        # TODO: This is where we would mirror the resource if we
        # wanted to.
        return link, new_link

    def add_measurement(self, data_source, quantity_measured, value,
                        weight=1, taken_at=None):
        """Associate a new Measurement with this Identifier."""
        _db = Session.object_session(self)

        logging.debug(
            "MEASUREMENT: %s on %s/%s: %s == %s (wt=%d)",
            data_source.name, self.type, self.identifier,
            quantity_measured, value, weight)

        now = datetime.datetime.utcnow()
        taken_at = taken_at or now
        # Is there an existing most recent measurement?
        most_recent = get_one(
            _db, Measurement, identifier=self,
            data_source=data_source,
            quantity_measured=quantity_measured,
            is_most_recent=True, on_multiple='interchangeable'
        )
        if most_recent and most_recent.value == value and taken_at == now:
            # The value hasn't changed since last time. Just update
            # the timestamp of the existing measurement.
            self.taken_at = taken_at

        if most_recent and most_recent.taken_at < taken_at:
            most_recent.is_most_recent = False

        return create(
            _db, Measurement,
            identifier=self, data_source=data_source,
            quantity_measured=quantity_measured, taken_at=taken_at,
            value=value, weight=weight, is_most_recent=True)[0]

    def classify(self, data_source, subject_type, subject_identifier,
                 subject_name=None, weight=1):
        """Classify this Identifier under a Subject.

        :param type: Classification scheme; one of the constants from Subject.
        :param subject_identifier: Internal ID of the subject according to that classification scheme.

        ``value``: Human-readable description of the subject, if different
                   from the ID.

        ``weight``: How confident the data source is in classifying a
                    book under this subject. The meaning of this
                    number depends entirely on the source of the
                    information.
        """
        _db = Session.object_session(self)
        # Turn the subject type and identifier into a Subject.
        classifications = []
        subject, is_new = Subject.lookup(
            _db, subject_type, subject_identifier, subject_name,
        )

        logging.debug(
            "CLASSIFICATION: %s on %s/%s: %s %s/%s (wt=%d)",
            data_source.name, self.type, self.identifier,
            subject.type, subject.identifier, subject.name,
            weight
        )

        # Use a Classification to connect the Identifier to the
        # Subject.
        try:
            classification, is_new = get_one_or_create(
                _db, Classification,
                identifier=self,
                subject=subject,
                data_source=data_source)
        except MultipleResultsFound, e:
            # TODO: This is a hack.
            all_classifications = _db.query(Classification).filter(
                Classification.identifier==self,
                Classification.subject==subject,
                Classification.data_source==data_source)
            all_classifications = all_classifications.all()
            classification = all_classifications[0]
            for i in all_classifications[1:]:
                _db.delete(i)

        classification.weight = weight
        return classification

    @classmethod
    def resources_for_identifier_ids(self, _db, identifier_ids, rel=None,
                                     data_source=None):
        resources = _db.query(Resource).join(Resource.links).filter(
                Hyperlink.identifier_id.in_(identifier_ids))
        if data_source:
            if isinstance(data_source, DataSource):
                data_source = [data_source]
            resources = resources.filter(Hyperlink.data_source_id.in_([d.id for d in data_source]))
        if rel:
            if isinstance(rel, list):
                resources = resources.filter(Hyperlink.rel.in_(rel))
            else:
                resources = resources.filter(Hyperlink.rel==rel)
        resources = resources.options(joinedload('representation'))
        return resources

    @classmethod
    def classifications_for_identifier_ids(self, _db, identifier_ids):
        classifications = _db.query(Classification).filter(
                Classification.identifier_id.in_(identifier_ids))
        return classifications.options(joinedload('subject'))

    IDEAL_COVER_ASPECT_RATIO = 2.0/3
    IDEAL_IMAGE_HEIGHT = 240
    IDEAL_IMAGE_WIDTH = 160

    @classmethod
    def best_cover_for(cls, _db, identifier_ids, rel=None):
        # Find all image resources associated with any of
        # these identifiers.
        rel = rel or Hyperlink.IMAGE
        images = cls.resources_for_identifier_ids(
            _db, identifier_ids, rel)
        images = images.join(Resource.representation)
        images = images.all()

        champions = Resource.best_covers_among(images)
        if not champions:
            champion = None
        elif len(champions) == 1:
            [champion] = champions
        else:
            champion = random.choice(champions)

        return champion, images

    @classmethod
    def evaluate_summary_quality(cls, _db, identifier_ids,
                                 privileged_data_sources=None):
        """Evaluate the summaries for the given group of Identifier IDs.

        This is an automatic evaluation based solely on the content of
        the summaries. It will be combined with human-entered ratings
        to form an overall quality score.

        We need to evaluate summaries from a set of Identifiers
        (typically those associated with a single work) because we
        need to see which noun phrases are most frequently used to
        describe the underlying work.

        :param privileged_data_sources: If present, a summary from one
        of these data source will be instantly chosen, short-circuiting the
        decision process. Data sources are in order of priority.

        :return: The single highest-rated summary Resource.

        """
        evaluator = SummaryEvaluator()

        if privileged_data_sources and len(privileged_data_sources) > 0:
            privileged_data_source = privileged_data_sources[0]
        else:
            privileged_data_source = None

        # Find all rel="description" resources associated with any of
        # these records.
        rels = [Hyperlink.DESCRIPTION, Hyperlink.SHORT_DESCRIPTION]
        descriptions = cls.resources_for_identifier_ids(
            _db, identifier_ids, rels, privileged_data_source).all()

        champion = None
        # Add each resource's content to the evaluator's corpus.
        for r in descriptions:
            if r.representation and r.representation.content:
                evaluator.add(r.representation.content)
        evaluator.ready()

        # Then have the evaluator rank each resource.
        for r in descriptions:
            if r.representation and r.representation.content:
                content = r.representation.content
                quality = evaluator.score(content)
                r.set_estimated_quality(quality)
            if not champion or r.quality > champion.quality:
                champion = r

        if privileged_data_source and not champion:
            # We could not find any descriptions from the privileged
            # data source. Try relaxing that restriction.
            return cls.evaluate_summary_quality(_db, identifier_ids, privileged_data_sources[1:])
        return champion, descriptions

    @classmethod
    def missing_coverage_from(
            cls, _db, identifier_types, coverage_data_source, operation=None,
            count_as_covered=None, count_as_missing_before=None, identifiers=None,
            collection=None
    ):
        """Find identifiers of the given types which have no CoverageRecord
        from `coverage_data_source`.

        :param count_as_covered: Identifiers will be counted as
        covered if their CoverageRecords have a status in this list.
        :param identifiers: Restrict search to a specific set of identifier objects.
        """
        if collection:
            collection_id = collection.id
        else:
            collection_id = None

        data_source_id = None
        if coverage_data_source:
            data_source_id = coverage_data_source.id

        clause = and_(Identifier.id==CoverageRecord.identifier_id,
                      CoverageRecord.data_source_id==data_source_id,
                      CoverageRecord.operation==operation,
                      CoverageRecord.collection_id==collection_id
        )
        qu = _db.query(Identifier).outerjoin(CoverageRecord, clause)
        if identifier_types:
            qu = qu.filter(Identifier.type.in_(identifier_types))
        missing = CoverageRecord.not_covered(
            count_as_covered, count_as_missing_before
        )
        qu = qu.filter(missing)

        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))

        return qu

    def opds_entry(self):
        """Create an OPDS entry using only resources directly
        associated with this Identifier.

        This makes it possible to create an OPDS entry even when there
        is no Edition.

        Currently the only things in this OPDS entry will be description,
        cover image, and popularity.

        NOTE: The timestamp doesn't take into consideration when the
        description was added. Rather than fixing this it's probably
        better to get rid of this hack and create real Works where we
        would be using this method.
        """
        id = self.urn
        cover_image = None
        description = None
        most_recent_update = None
        timestamps = []
        for link in self.links:
            resource = link.resource
            if link.rel == Hyperlink.IMAGE:
                if not cover_image or (
                        not cover_image.representation.thumbnails and
                        resource.representation.thumbnails):
                    cover_image = resource
                    if cover_image.representation:
                        # This is technically redundant because
                        # minimal_opds_entry will redo this work,
                        # but just to be safe.
                        mirrored_at = cover_image.representation.mirrored_at
                        if mirrored_at:
                            timestamps.append(mirrored_at)
            elif link.rel == Hyperlink.DESCRIPTION:
                if not description or resource.quality > description.quality:
                    description = resource

        if self.coverage_records:
            timestamps.extend([
                c.timestamp for c in self.coverage_records if c.timestamp
            ])
        if timestamps:
            most_recent_update = max(timestamps)

        quality = Measurement.overall_quality(self.measurements)
        from opds import AcquisitionFeed
        return AcquisitionFeed.minimal_opds_entry(
            identifier=self, cover=cover_image, description=description,
            quality=quality, most_recent_update=most_recent_update
        )


class Contributor(Base):

    """Someone (usually human) who contributes to books."""
    __tablename__ = 'contributors'
    id = Column(Integer, primary_key=True)

    # Standard identifiers for this contributor.
    lc = Column(Unicode, index=True)
    viaf = Column(Unicode, index=True)

    # This is the name by which this person is known in the original
    # catalog. It is sortable, e.g. "Twain, Mark".
    _sort_name = Column('sort_name', Unicode, index=True)
    aliases = Column(ARRAY(Unicode), default=[])

    # This is the name we will display publicly. Ideally it will be
    # the name most familiar to readers.
    display_name = Column(Unicode, index=True)

    # This is a short version of the contributor's name, displayed in
    # situations where the full name is too long. For corporate contributors
    # this value will be None.
    family_name = Column(Unicode, index=True)

    # This is the name used for this contributor on Wikipedia. This
    # gives us an entry point to Wikipedia, Wikidata, etc.
    wikipedia_name = Column(Unicode, index=True)

    # This is a short biography for this contributor, probably
    # provided by a publisher.
    biography = Column(Unicode)

    extra = Column(MutableDict.as_mutable(JSON), default={})

    contributions = relationship("Contribution", backref="contributor")
    work_contributions = relationship("WorkContribution", backref="contributor",
                                      )
    # Types of roles
    AUTHOR_ROLE = u"Author"
    PRIMARY_AUTHOR_ROLE = u"Primary Author"
    EDITOR_ROLE = u"Editor"
    ARTIST_ROLE = u"Artist"
    PHOTOGRAPHER_ROLE = u"Photographer"
    TRANSLATOR_ROLE = u"Translator"
    ILLUSTRATOR_ROLE = u"Illustrator"
    INTRODUCTION_ROLE = u"Introduction Author"
    FOREWORD_ROLE = u"Foreword Author"
    AFTERWORD_ROLE = u"Afterword Author"
    COLOPHON_ROLE = u"Colophon Author"
    UNKNOWN_ROLE = u'Unknown'
    DIRECTOR_ROLE = u'Director'
    PRODUCER_ROLE = u'Producer'
    EXECUTIVE_PRODUCER_ROLE = u'Executive Producer'
    ACTOR_ROLE = u'Actor'
    LYRICIST_ROLE = u'Lyricist'
    CONTRIBUTOR_ROLE = u'Contributor'
    COMPOSER_ROLE = u'Composer'
    NARRATOR_ROLE = u'Narrator'
    COMPILER_ROLE = u'Compiler'
    ADAPTER_ROLE = u'Adapter'
    PERFORMER_ROLE = u'Performer'
    MUSICIAN_ROLE = u'Musician'
    ASSOCIATED_ROLE = u'Associated name'
    COLLABORATOR_ROLE = u'Collaborator'
    ENGINEER_ROLE = u'Engineer'
    COPYRIGHT_HOLDER_ROLE = u'Copyright holder'
    TRANSCRIBER_ROLE = u'Transcriber'
    DESIGNER_ROLE = u'Designer'
    AUTHOR_ROLES = set([PRIMARY_AUTHOR_ROLE, AUTHOR_ROLE])

    # Map our recognized roles to MARC relators.
    # https://www.loc.gov/marc/relators/relaterm.html
    #
    # This is used when crediting contributors in OPDS feeds.
    MARC_ROLE_CODES = {
        ACTOR_ROLE : 'act',
        ADAPTER_ROLE : 'adp',
        AFTERWORD_ROLE : 'aft',
        ARTIST_ROLE : 'art',
        ASSOCIATED_ROLE : 'asn',
        AUTHOR_ROLE : 'aut',            # Joint author: USE Author
        COLLABORATOR_ROLE : 'ctb',      # USE Contributor
        COLOPHON_ROLE : 'aft',          # Author of afterword, colophon, etc.
        COMPILER_ROLE : 'com',
        COMPOSER_ROLE : 'cmp',
        CONTRIBUTOR_ROLE : 'ctb',
        COPYRIGHT_HOLDER_ROLE : 'cph',
        DESIGNER_ROLE : 'dsr',
        DIRECTOR_ROLE : 'drt',
        EDITOR_ROLE : 'edt',
        ENGINEER_ROLE : 'eng',
        EXECUTIVE_PRODUCER_ROLE : 'pro',
        FOREWORD_ROLE : 'wpr',          # Writer of preface
        ILLUSTRATOR_ROLE : 'ill',
        INTRODUCTION_ROLE : 'win',
        LYRICIST_ROLE : 'lyr',
        MUSICIAN_ROLE : 'mus',
        NARRATOR_ROLE : 'nrt',
        PERFORMER_ROLE : 'prf',
        PHOTOGRAPHER_ROLE : 'pht',
        PRIMARY_AUTHOR_ROLE : 'aut',
        PRODUCER_ROLE : 'pro',
        TRANSCRIBER_ROLE : 'trc',
        TRANSLATOR_ROLE : 'trl',
        UNKNOWN_ROLE : 'asn',
    }

    # People from these roles can be put into the 'author' slot if no
    # author proper is given.
    AUTHOR_SUBSTITUTE_ROLES = [
        EDITOR_ROLE, COMPILER_ROLE, COMPOSER_ROLE, DIRECTOR_ROLE,
        CONTRIBUTOR_ROLE, TRANSLATOR_ROLE, ADAPTER_ROLE, PHOTOGRAPHER_ROLE,
        ARTIST_ROLE, LYRICIST_ROLE, COPYRIGHT_HOLDER_ROLE
    ]

    PERFORMER_ROLES = [ACTOR_ROLE, PERFORMER_ROLE, NARRATOR_ROLE, MUSICIAN_ROLE]

    # Extra fields
    BIRTH_DATE = 'birthDate'
    DEATH_DATE = 'deathDate'

    def __repr__(self):
        extra = ""
        if self.lc:
            extra += " lc=%s" % self.lc
        if self.viaf:
            extra += " viaf=%s" % self.viaf
        return (u"Contributor %d (%s)" % (self.id, self.sort_name)).encode("utf8")

    @classmethod
    def author_contributor_tiers(cls):
        yield [cls.PRIMARY_AUTHOR_ROLE]
        yield cls.AUTHOR_ROLES
        yield cls.AUTHOR_SUBSTITUTE_ROLES
        yield cls.PERFORMER_ROLES

    @classmethod
    def lookup(cls, _db, sort_name=None, viaf=None, lc=None, aliases=None,
               extra=None, create_new=True, name=None):
        """Find or create a record (or list of records) for the given Contributor.
        :return: A tuple of found Contributor (or None), and a boolean flag
        indicating if new Contributor database object has beed created.
        """

        new = False
        contributors = []

        # TODO: Stop using 'name' attribute, everywhere.
        sort_name = sort_name or name
        extra = extra or dict()

        create_method_kwargs = {
            Contributor.sort_name.name : sort_name,
            Contributor.aliases.name : aliases,
            Contributor.extra.name : extra
        }

        if not sort_name and not lc and not viaf:
            raise ValueError(
                "Cannot look up a Contributor without any identifying "
                "information whatsoever!")

        if sort_name and not lc and not viaf:
            # We will not create a Contributor based solely on a name
            # unless there is no existing Contributor with that name.
            #
            # If there *are* contributors with that name, we will
            # return all of them.
            #
            # We currently do not check aliases when doing name lookups.
            q = _db.query(Contributor).filter(Contributor.sort_name==sort_name)
            contributors = q.all()
            if contributors:
                return contributors, new
            else:
                try:
                    contributor = Contributor(**create_method_kwargs)
                    _db.add(contributor)
                    flush(_db)
                    contributors = [contributor]
                    new = True
                except IntegrityError:
                    _db.rollback()
                    contributors = q.all()
                    new = False
        else:
            # We are perfecly happy to create a Contributor based solely
            # on lc or viaf.
            query = dict()
            if lc:
                query[Contributor.lc.name] = lc
            if viaf:
                query[Contributor.viaf.name] = viaf

            if create_new:
                contributor, new = get_one_or_create(
                    _db, Contributor, create_method_kwargs=create_method_kwargs,
                    on_multiple='interchangeable',
                    **query
                )
                if contributor:
                    contributors = [contributor]
            else:
                contributor = get_one(_db, Contributor, **query)
                if contributor:
                    contributors = [contributor]

        return contributors, new


    @property
    def sort_name(self):
        return self._sort_name

    @sort_name.setter
    def sort_name(self, new_sort_name):
        """ See if the passed-in value is in the prescribed Last, First format.
        If it is, great, set the self._sprt_name to the new value.

        If new value is not in correct format, then
        attempt to re-format the value to look like: "Last, First Middle, Dr./Jr./etc.".

        Note: If for any reason you need to force the sort_name to an improper value,
        set it like so:  contributor._sort_name="Foo Bar", and you'll avoid further processing.

        Note: For now, have decided to not automatically update any edition.sort_author
        that might have contributions by this Contributor.
        """

        if not new_sort_name:
            self._sort_name = None
            return

        # simplistic test of format, but catches the most frequent problem
        # where display-style names are put into sort name metadata by third parties.
        if new_sort_name.find(",") == -1:
            # auto-magically fix syntax
            self._sort_name = display_name_to_sort_name(new_sort_name)
            return

        self._sort_name = new_sort_name

    # tell SQLAlchemy to use the sort_name setter for ort_name, not _sort_name, after all.
    sort_name = synonym('_sort_name', descriptor=sort_name)


    def merge_into(self, destination):
        """Two Contributor records should be the same.

        Merge this one into the other one.

        For now, this should only be used when the exact same record
        comes in through two sources. It should not be used when two
        Contributors turn out to represent different names for the
        same human being, e.g. married names or (especially) pen
        names. Just because we haven't thought that situation through
        well enough.
        """
        if self == destination:
            # They're already the same.
            return
        logging.info(
            u"MERGING %r (%s) into %r (%s)",
            self,
            self.viaf,
            destination,
            destination.viaf
        )

        # make sure we're not losing any names we know for the contributor
        existing_aliases = set(destination.aliases)
        new_aliases = list(destination.aliases)
        for name in [self.sort_name] + self.aliases:
            if name != destination.sort_name and name not in existing_aliases:
                new_aliases.append(name)
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases

        if not destination.family_name:
            destination.family_name = self.family_name
        if not destination.display_name:
            destination.display_name = self.display_name
        # keep sort_name if one of the contributor objects has it.
        if not destination.sort_name:
            destination.sort_name = self.sort_name
        if not destination.wikipedia_name:
            destination.wikipedia_name = self.wikipedia_name

        # merge non-name-related properties
        for k, v in self.extra.items():
            if not k in destination.extra:
                destination.extra[k] = v
        if not destination.lc:
            destination.lc = self.lc
        if not destination.viaf:
            destination.viaf = self.viaf
        if not destination.biography:
            destination.biography = self.biography

        _db = Session.object_session(self)
        for contribution in self.contributions:
            # Is the new contributor already associated with this
            # Edition in the given role (in which case we delete
            # the old contribution) or not (in which case we switch the
            # contributor ID)?
            existing_record = _db.query(Contribution).filter(
                Contribution.contributor_id==destination.id,
                Contribution.edition_id==contribution.edition.id,
                Contribution.role==contribution.role)
            if existing_record.count():
                _db.delete(contribution)
            else:
                contribution.contributor_id = destination.id
        for contribution in self.work_contributions:
            existing_record = _db.query(WorkContribution).filter(
                WorkContribution.contributor_id==destination.id,
                WorkContribution.edition_id==contribution.edition.id,
                WorkContribution.role==contribution.role)
            if existing_record.count():
                _db.delete(contribution)
            else:
                contribution.contributor_id = destination.id
            contribution.contributor_id = destination.id

        _db.commit()
        _db.delete(self)
        _db.commit()

    # Regular expressions used by default_names().
    PARENTHETICAL = re.compile("\([^)]*\)")
    ALPHABETIC = re.compile("[a-zA-z]")
    NUMBERS = re.compile("[0-9]")

    DATE_RES = [re.compile("\(?" + x + "\)?") for x in
                "[0-9?]+-",
                "[0-9]+st cent",
                "[0-9]+nd cent",
                "[0-9]+th cent",
                "\bcirca",
                ]


    def default_names(self, default_display_name=None):
        """Attempt to derive a family name ("Twain") and a display name ("Mark
        Twain") from a catalog name ("Twain, Mark").

        This is full of pitfalls, which is why we prefer to use data
        from VIAF. But when there is no data from VIAF, the output of
        this algorithm is better than the input in pretty much every
        case.
        """
        return self._default_names(self.sort_name, default_display_name)

    @classmethod
    def _default_names(cls, name, default_display_name=None):
        name = name or ""
        original_name = name
        """Split out from default_names to make it easy to test."""
        display_name = default_display_name
        # "Little, Brown &amp; Co." => "Little, Brown & Co."
        name = name.replace("&amp;", "&")

        # "Philadelphia Broad Street Church (Philadelphia, Pa.)"
        #  => "Philadelphia Broad Street Church"
        name = cls.PARENTHETICAL.sub("", name)
        name = name.strip()

        if ', ' in name:
            # This is probably a personal name.
            parts = name.split(", ")
            if len(parts) > 2:
                # The most likely scenario is that the final part
                # of the name is a date or a set of dates. If this
                # seems true, just delete that part.
                if (cls.NUMBERS.search(parts[-1])
                    or not cls.ALPHABETIC.search(parts[-1])):
                    parts = parts[:-1]
            # The final part of the name may have a date or a set
            # of dates at the end. If so, remove it from that string.
            final = parts[-1]
            for date_re in cls.DATE_RES:
                m = date_re.search(final)
                if m:
                    new_part = final[:m.start()].strip()
                    if new_part:
                        parts[-1] = new_part
                    else:
                        del parts[-1]
                    break

            family_name = parts[0]
            p = parts[-1].lower()
            if (p in ('llc', 'inc', 'inc.')
                or p.endswith("company") or p.endswith(" co.")
                or p.endswith(" co")):
                # No, this is a corporate name that contains a comma.
                # It can't be split on the comma, so don't bother.
                family_name = None
                display_name = display_name or name
            if not display_name:
                # The fateful moment. Swap the second string and the
                # first string.
                if len(parts) == 1:
                    display_name = parts[0]
                    family_name = display_name
                else:
                    display_name = parts[1] + " " + parts[0]
                if len(parts) > 2:
                    # There's a leftover bit.
                    if parts[2] in ('Mrs.', 'Mrs', 'Sir'):
                        # "Jones, Bob, Mrs."
                        #  => "Mrs. Bob Jones"
                        display_name = parts[2] + " " + display_name
                    else:
                        # "Jones, Bob, Jr."
                        #  => "Bob Jones, Jr."
                        display_name += ", " + " ".join(parts[2:])
        else:
            # Since there's no comma, this is probably a corporate name.
            family_name = None
            display_name = name

        return family_name, display_name



class Contribution(Base):
    """A contribution made by a Contributor to a Edition."""
    __tablename__ = 'contributions'
    id = Column(Integer, primary_key=True)
    edition_id = Column(Integer, ForeignKey('editions.id'), index=True,
                           nullable=False)
    contributor_id = Column(Integer, ForeignKey('contributors.id'), index=True,
                            nullable=False)
    role = Column(Unicode, index=True, nullable=False)
    __table_args__ = (
        UniqueConstraint('edition_id', 'contributor_id', 'role'),
    )


class WorkContribution(Base):
    """A contribution made by a Contributor to a Work."""
    __tablename__ = 'workcontributions'
    id = Column(Integer, primary_key=True)
    work_id = Column(Integer, ForeignKey('works.id'), index=True,
                     nullable=False)
    contributor_id = Column(Integer, ForeignKey('contributors.id'), index=True,
                            nullable=False)
    role = Column(Unicode, index=True, nullable=False)
    __table_args__ = (
        UniqueConstraint('work_id', 'contributor_id', 'role'),
    )


class Edition(Base):

    """A lightly schematized collection of metadata for a work, or an
    edition of a work, or a book, or whatever. If someone thinks of it
    as a "book" with a "title" it can go in here.
    """

    __tablename__ = 'editions'
    id = Column(Integer, primary_key=True)

    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    MAX_THUMBNAIL_HEIGHT = 300
    MAX_THUMBNAIL_WIDTH = 200

    # A full-sized image no larger than this height can be used as a thumbnail
    # in a pinch.
    MAX_FALLBACK_THUMBNAIL_HEIGHT = 500

    # This Edition is associated with one particular
    # identifier--the one used by its data source to identify
    # it. Through the Equivalency class, it is associated with a
    # (probably huge) number of other identifiers.
    primary_identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)

    # An Edition may be the presentation edition for a single Work. If it's not
    # a presentation edition for a work, work will be None.
    work = relationship("Work", uselist=False, backref="presentation_edition")

    # An Edition may show up in many CustomListEntries.
    custom_list_entries = relationship("CustomListEntry", backref="edition")

    # An Edition may be the presentation edition for many LicensePools.
    is_presentation_for = relationship(
        "LicensePool", backref="presentation_edition"
    )

    title = Column(Unicode, index=True)
    sort_title = Column(Unicode, index=True)
    subtitle = Column(Unicode, index=True)
    series = Column(Unicode, index=True)
    series_position = Column(Integer)

    # This is not a foreign key per se; it's a calculated UUID-like
    # identifier for this work based on its title and author, used to
    # group together different editions of the same work.
    permanent_work_id = Column(String(36), index=True)

    # A string depiction of the authors' names.
    author = Column(Unicode, index=True)
    sort_author = Column(Unicode, index=True)

    contributions = relationship("Contribution", backref="edition")

    language = Column(Unicode, index=True)
    publisher = Column(Unicode, index=True)
    imprint = Column(Unicode, index=True)

    # `issued` is the date the ebook edition was sent to the distributor by the publisher,
    # i.e. the date it became available for librarians to buy for their libraries
    issued = Column(Date)
    # `published is the original publication date of the text.
    # A Project Gutenberg text was likely `published` long before being `issued`.
    published = Column(Date)

    ALL_MEDIUM = object()
    BOOK_MEDIUM = u"Book"
    PERIODICAL_MEDIUM = u"Periodical"
    AUDIO_MEDIUM = u"Audio"
    MUSIC_MEDIUM = u"Music"
    VIDEO_MEDIUM = u"Video"
    IMAGE_MEDIUM = u"Image"
    COURSEWARE_MEDIUM = u"Courseware"

    ELECTRONIC_FORMAT = u"Electronic"
    CODEX_FORMAT = u"Codex"

    # These are the media types currently fulfillable by the default
    # client.
    FULFILLABLE_MEDIA = [BOOK_MEDIUM, AUDIO_MEDIUM]

    medium_to_additional_type = {
        BOOK_MEDIUM : u"http://schema.org/EBook",
        AUDIO_MEDIUM : u"http://bib.schema.org/Audiobook",
        PERIODICAL_MEDIUM : u"http://schema.org/PublicationIssue",
        MUSIC_MEDIUM :  u"http://schema.org/MusicRecording",
        VIDEO_MEDIUM :  u"http://schema.org/VideoObject",
        IMAGE_MEDIUM: u"http://schema.org/ImageObject",
        COURSEWARE_MEDIUM: u"http://schema.org/Course"
    }

    additional_type_to_medium = {}
    for k, v in medium_to_additional_type.items():
        additional_type_to_medium[v] = k

    medium = Column(
        Enum(BOOK_MEDIUM, PERIODICAL_MEDIUM, AUDIO_MEDIUM, MUSIC_MEDIUM, VIDEO_MEDIUM, IMAGE_MEDIUM, COURSEWARE_MEDIUM,
             name="medium"),
        default=BOOK_MEDIUM, index=True
    )

    cover_id = Column(
        Integer, ForeignKey(
            'resources.id', use_alter=True, name='fk_editions_summary_id'),
        index=True)
    # These two let us avoid actually loading up the cover Resource
    # every time.
    cover_full_url = Column(Unicode)
    cover_thumbnail_url = Column(Unicode)

    # An OPDS entry containing all metadata about this entry that
    # would be relevant to display to a library patron.
    simple_opds_entry = Column(Unicode, default=None)

    # Information kept in here probably won't be used.
    extra = Column(MutableDict.as_mutable(JSON), default={})

    def __repr__(self):
        id_repr = repr(self.primary_identifier).decode("utf8")
        a = (u"Edition %s [%r] (%s/%s/%s)" % (
            self.id, id_repr, self.title,
            ", ".join([x.sort_name for x in self.contributors]),
            self.language))
        return a.encode("utf8")

    @property
    def language_code(self):
        return LanguageCodes.three_to_two.get(self.language, self.language)

    @property
    def contributors(self):
        return set([x.contributor for x in self.contributions])

    @property
    def author_contributors(self):
        """All distinct 'author'-type contributors, with the primary author
        first, other authors sorted by sort name.

        Basically, we're trying to figure out what would go on the
        book cover. The primary author should go first, and be
        followed by non-primary authors in alphabetical order. People
        whose role does not rise to the level of "authorship"
        (e.g. author of afterword) do not show up.

        The list as a whole should contain no duplicates. This might
        happen because someone is erroneously listed twice in the same
        role, someone is listed as both primary author and regular
        author, someone is listed as both author and translator,
        etc. However it happens, your name only shows up once on the
        front of the book.
        """
        seen_authors = set()
        primary_author = None
        other_authors = []
        acceptable_substitutes = defaultdict(list)
        if not self.contributions:
            return []

        # If there is one and only one contributor, return them, no
        # matter what their role is.
        if len(self.contributions) == 1:
            return [self.contributions[0].contributor]

        # There is more than one contributor. Try to pick out the ones
        # that rise to the level of being 'authors'.
        for x in self.contributions:
            if not primary_author and x.role == Contributor.PRIMARY_AUTHOR_ROLE:
                primary_author = x.contributor
            elif x.role in Contributor.AUTHOR_ROLES:
                other_authors.append(x.contributor)
            elif x.role.lower().startswith('author and'):
                other_authors.append(x.contributor)
            elif (x.role in Contributor.AUTHOR_SUBSTITUTE_ROLES
                  or x.role in Contributor.PERFORMER_ROLES):
                l = acceptable_substitutes[x.role]
                if x.contributor not in l:
                    l.append(x.contributor)

        def dedupe(l):
            """If an item shows up multiple times in a list,
            keep only the first occurence.
            """
            seen = set()
            deduped = []
            for i in l:
                if i in seen:
                    continue
                deduped.append(i)
                seen.add(i)
            return deduped

        if primary_author:
            return dedupe([primary_author] + sorted(other_authors, key=lambda x: x.sort_name))

        if other_authors:
            return dedupe(other_authors)

        for role in (
                Contributor.AUTHOR_SUBSTITUTE_ROLES
                + Contributor.PERFORMER_ROLES
        ):
            if role in acceptable_substitutes:
                contributors = acceptable_substitutes[role]
                return dedupe(sorted(contributors, key=lambda x: x.sort_name))
        else:
            # There are roles, but they're so random that we can't be
            # sure who's the 'author' or so low on the creativity
            # scale (like 'Executive producer') that we just don't
            # want to put them down as 'author'.
            return []


    @classmethod
    def for_foreign_id(cls, _db, data_source,
                       foreign_id_type, foreign_id,
                       create_if_not_exists=True):
        """Find the Edition representing the given data source's view of
        the work that it primarily identifies by foreign ID.

        e.g. for_foreign_id(_db, DataSourceConstants.OVERDRIVE,
                            Identifier.OVERDRIVE_ID, uuid)

        finds the Edition for Overdrive's view of a book identified
        by Overdrive UUID.

        This:

        for_foreign_id(_db, DataSourceConstants.OVERDRIVE, Identifier.ISBN, isbn)

        will probably return nothing, because although Overdrive knows
        that books have ISBNs, it doesn't use ISBN as a primary
        identifier.
        """
        # Look up the data source if necessary.
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)

        identifier, ignore = Identifier.for_foreign_id(
            _db, foreign_id_type, foreign_id)

        # Combine the two to get/create a Edition.
        if create_if_not_exists:
            f = get_one_or_create
            kwargs = dict()
        else:
            f = get_one
            kwargs = dict()
        r = f(_db, Edition, data_source=data_source,
                 primary_identifier=identifier,
                 **kwargs)
        return r

    @property
    def license_pools(self):
        """The LicensePools that provide access to the book described
        by this Edition.
        """
        _db = Session.object_session(self)
        return _db.query(LicensePool).filter(
            LicensePool.data_source==self.data_source,
            LicensePool.identifier==self.primary_identifier).all()

    def equivalent_identifiers(self, levels=3, threshold=0.5, type=None):
        """All Identifiers equivalent to this
        Edition's primary identifier, at the given level of recursion.
        """
        _db = Session.object_session(self)
        identifier_id_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            self.primary_identifier.id, levels, threshold)
        q = _db.query(Identifier).filter(
            Identifier.id.in_(identifier_id_subquery))
        if type:
            if isinstance(type, list):
                q = q.filter(Identifier.type.in_(type))
            else:
                q = q.filter(Identifier.type==type)
        return q.all()

    def equivalent_editions(self, levels=5, threshold=0.5):
        """All Editions whose primary ID is equivalent to this Edition's
        primary ID, at the given level of recursion.

        Five levels is enough to go from a Gutenberg ID to an Overdrive ID
        (Gutenberg ID -> OCLC Work ID -> OCLC Number -> ISBN -> Overdrive ID)
        """
        _db = Session.object_session(self)
        identifier_id_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            self.primary_identifier.id, levels, threshold)
        return _db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_id_subquery))

    @classmethod
    def missing_coverage_from(
            cls, _db, edition_data_sources, coverage_data_source,
            operation=None
    ):
        """Find Editions from `edition_data_source` whose primary
        identifiers have no CoverageRecord from
        `coverage_data_source`.

        e.g.

         gutenberg = DataSource.lookup(_db, DataSourceConstants.GUTENBERG)
         oclc_classify = DataSource.lookup(_db, DataSourceConstants.OCLC)
         missing_coverage_from(_db, gutenberg, oclc_classify)

        will find Editions that came from Project Gutenberg and
        have never been used as input to the OCLC Classify web
        service.

        """
        if isinstance(edition_data_sources, DataSource):
            edition_data_sources = [edition_data_sources]
        edition_data_source_ids = [x.id for x in edition_data_sources]
        join_clause = (
            (Edition.primary_identifier_id==CoverageRecord.identifier_id) &
            (CoverageRecord.data_source_id==coverage_data_source.id) &
            (CoverageRecord.operation==operation)
        )

        q = _db.query(Edition).outerjoin(
            CoverageRecord, join_clause)
        if edition_data_source_ids:
            q = q.filter(Edition.data_source_id.in_(edition_data_source_ids))
        q2 = q.filter(CoverageRecord.id==None)
        return q2

    @classmethod
    def sort_by_priority(self, editions):
        """Return all Editions that describe the Identifier associated with
        this LicensePool, in the order they should be used to create a
        presentation Edition for the LicensePool.
        """
        def sort_key(edition):
            """Return a numeric ordering of this edition."""
            source = edition.data_source
            if not source:
                # This shouldn't happen. Give this edition the
                # lowest priority.
                return -100

            if source == self.data_source:
                # This Edition contains information from the same data
                # source as the LicensePool itself. Put it below any
                # Edition from one of the data sources in
                # PRESENTATION_EDITION_PRIORITY, but above all other
                # Editions.
                return -1

            if source.name in DataSourceConstants.PRESENTATION_EDITION_PRIORITY:
                id_type = edition.primary_identifier.type
                if (id_type == Identifier.ISBN and
                    source.name == DataSourceConstants.METADATA_WRANGLER):
                    # This ISBN edition was pieced together from OCLC data.
                    # To avoid overwriting better author and title data from
                    # the license source, rank this edition lower.
                    return -1.5
                return DataSourceConstants.PRESENTATION_EDITION_PRIORITY.index(source.name)
            else:
                return -2

        return sorted(editions, key=sort_key)

    @classmethod
    def _content(cls, content, is_html=False):
        """Represent content that might be plain-text or HTML.

        e.g. a book's summary.
        """
        if not content:
            return None
        if is_html:
            type = "html"
        else:
            type = "text"
        return dict(type=type, value=content)

    def set_cover(self, resource):
        old_cover = self.cover
        old_cover_full_url = self.cover_full_url
        self.cover = resource
        self.cover_full_url = resource.representation.public_url

        # TODO: In theory there could be multiple scaled-down
        # versions of this representation and we need some way of
        # choosing between them. Right now we just pick the first one
        # that works.
        if (resource.representation.image_height
            and resource.representation.image_height <= self.MAX_THUMBNAIL_HEIGHT):
            # This image doesn't need a thumbnail.
            self.cover_thumbnail_url = resource.representation.public_url
        else:
            # Use the best available thumbnail for this image.
            best_thumbnail = resource.representation.best_thumbnail
            if best_thumbnail:
                self.cover_thumbnail_url = best_thumbnail.public_url
        if (not self.cover_thumbnail_url and
            resource.representation.image_height
            and resource.representation.image_height <= self.MAX_FALLBACK_THUMBNAIL_HEIGHT):
            # The full-sized image is too large to be a thumbnail, but it's
            # not huge, and there is no other thumbnail, so use it.
            self.cover_thumbnail_url = resource.representation.public_url
        if old_cover != self.cover or old_cover_full_url != self.cover_full_url:
            logging.debug(
                "Setting cover for %s/%s: full=%s thumb=%s",
                self.primary_identifier.type, self.primary_identifier.identifier,
                self.cover_full_url, self.cover_thumbnail_url
            )

    def add_contributor(self, name, roles, aliases=None, lc=None, viaf=None,
                        **kwargs):
        """Assign a contributor to this Edition."""
        _db = Session.object_session(self)
        if isinstance(roles, basestring):
            roles = [roles]

        # First find or create the Contributor.
        if isinstance(name, Contributor):
            contributor = name
        else:
            contributor, was_new = Contributor.lookup(
                _db, name, lc, viaf, aliases)
            if isinstance(contributor, list):
                # Contributor was looked up/created by name,
                # which returns a list.
                contributor = contributor[0]

        # Then add their Contributions.
        for role in roles:
            contribution, was_new = get_one_or_create(
                _db, Contribution, edition=self, contributor=contributor,
                role=role)
        return contributor

    def similarity_to(self, other_record):
        """How likely is it that this record describes the same book as the
        given record?

        1 indicates very strong similarity, 0 indicates no similarity
        at all.

        For now we just compare the sets of words used in the titles
        and the authors' names. This should be good enough for most
        cases given that there is usually some preexisting reason to
        suppose that the two records are related (e.g. OCLC said
        they were).

        Most of the Editions are from OCLC Classify, and we expect
        to get some of them wrong (e.g. when a single OCLC work is a
        compilation of several novels by the same author). That's okay
        because those Editions aren't backed by
        LicensePools. They're purely informative. We will have some
        bad information in our database, but the clear-cut cases
        should outnumber the fuzzy cases, so we we should still group
        the Editions that really matter--the ones backed by
        LicensePools--together correctly.

        TODO: apply much more lenient terms if the two Editions are
        identified by the same ISBN or other unique identifier.
        """
        if other_record == self:
            # A record is always identical to itself.
            return 1

        if other_record.language == self.language:
            # The books are in the same language. Hooray!
            language_factor = 1
        else:
            if other_record.language and self.language:
                # Each record specifies a different set of languages. This
                # is an immediate disqualification.
                return 0
            else:
                # One record specifies a language and one does not. This
                # is a little tricky. We're going to apply a penalty, but
                # since the majority of records we're getting from OCLC are in
                # English, the penalty will be less if one of the
                # languages is English. It's more likely that an unlabeled
                # record is in English than that it's in some other language.
                if self.language == 'eng' or other_record.language == 'eng':
                    language_factor = 0.80
                else:
                    language_factor = 0.50

        title_quotient = MetadataSimilarity.title_similarity(
            self.title, other_record.title)

        author_quotient = MetadataSimilarity.author_similarity(
            self.author_contributors, other_record.author_contributors)
        if author_quotient == 0:
            # The two works have no authors in common. Immediate
            # disqualification.
            return 0

        # We weight title more heavily because it's much more likely
        # that one author wrote two different books than that two
        # books with the same title have different authors.
        return language_factor * (
            (title_quotient * 0.80) + (author_quotient * 0.20))

    def apply_similarity_threshold(self, candidates, threshold=0.5):
        """Yield the Editions from the given list that are similar
        enough to this one.
        """
        for candidate in candidates:
            if self == candidate:
                yield candidate
            else:
                similarity = self.similarity_to(candidate)
                if similarity >= threshold:
                    yield candidate

    def best_cover_within_distance(self, distance, threshold=0.5, rel=None):
        _db = Session.object_session(self)
        identifier_ids = [self.primary_identifier.id]
        if distance > 0:
            identifier_ids_dict = Identifier.recursively_equivalent_identifier_ids(
                _db, identifier_ids, distance, threshold=threshold)
            identifier_ids += identifier_ids_dict[self.primary_identifier.id]

        return Identifier.best_cover_for(_db, identifier_ids, rel=rel)

    @property
    def title_for_permanent_work_id(self):
        title = self.title
        if self.subtitle:
            title += (": " + self.subtitle)
        return title

    @property
    def author_for_permanent_work_id(self):
        authors = self.author_contributors
        if authors:
            # Use the sort name of the primary author.
            author = authors[0].sort_name
        else:
            # This may be an Edition that represents an item on a best-seller list
            # or something like that. In this case it wouldn't have any Contributor
            # objects, just an author string. Use that.
            author = self.sort_author or self.author
        return author

    def calculate_permanent_work_id(self, debug=False):
        title = self.title_for_permanent_work_id
        if not title:
            # If a book has no title, it has no permanent work ID.
            self.permanent_work_id = None
            return

        author = self.author_for_permanent_work_id

        if self.medium == Edition.BOOK_MEDIUM:
            medium = "book"
        elif self.medium == Edition.AUDIO_MEDIUM:
            medium = "book"
        elif self.medium == Edition.MUSIC_MEDIUM:
            medium = "music"
        elif self.medium == Edition.PERIODICAL_MEDIUM:
            medium = "book"
        elif self.medium == Edition.VIDEO_MEDIUM:
            medium = "movie"
        elif self.medium == Edition.IMAGE_MEDIUM:
            medium = "image"
        elif self.medium == Edition.COURSEWARE_MEDIUM:
            medium = "courseware"

        w = WorkIDCalculator
        norm_title = w.normalize_title(title)
        norm_author = w.normalize_author(author)

        old_id = self.permanent_work_id
        self.permanent_work_id = self.calculate_permanent_work_id_for_title_and_author(
            title, author, medium)
        args = (
            "Permanent work ID for %d: %s/%s -> %s/%s/%s -> %s (was %s)",
            self.id, title, author, norm_title, norm_author, medium,
                self.permanent_work_id, old_id
        )
        if debug:
            logging.debug(*args)
        elif old_id != self.permanent_work_id:
            logging.info(*args)

    @classmethod
    def calculate_permanent_work_id_for_title_and_author(
            cls, title, author, medium):
        w = WorkIDCalculator
        norm_title = w.normalize_title(title)
        norm_author = w.normalize_author(author)

        return WorkIDCalculator.permanent_id(
            norm_title, norm_author, medium)

    UNKNOWN_AUTHOR = u"[Unknown]"



    def calculate_presentation(self, policy=None):
        """Make sure the presentation of this Edition is up-to-date."""
        _db = Session.object_session(self)
        changed = False
        if policy is None:
            policy = PresentationCalculationPolicy()

        # Gather information up front that will be used to determine
        # whether this method actually did anything.
        old_author = self.author
        old_sort_author = self.sort_author
        old_sort_title = self.sort_title
        old_work_id = self.permanent_work_id
        old_cover = self.cover
        old_cover_full_url = self.cover_full_url
        old_cover_thumbnail_url = self.cover_thumbnail_url

        if policy.set_edition_metadata:
            self.author, self.sort_author = self.calculate_author()
            self.sort_title = TitleProcessor.sort_title_for(self.title)
            self.calculate_permanent_work_id()
            CoverageRecord.add_for(
                self, data_source=self.data_source,
                operation=CoverageRecord.SET_EDITION_METADATA_OPERATION
            )

        if policy.choose_cover:
            self.choose_cover()

        if (self.author != old_author
            or self.sort_author != old_sort_author
            or self.sort_title != old_sort_title
            or self.permanent_work_id != old_work_id
            or self.cover != old_cover
            or self.cover_full_url != old_cover_full_url
            or self.cover_thumbnail_url != old_cover_thumbnail_url
        ):
            changed = True

        # Now that everything's calculated, log it.
        if policy.verbose:
            if changed:
                changed_status = "changed"
                level = logging.info
            else:
                changed_status = "unchanged"
                level = logging.debug

            msg = u"Presentation %s for Edition %s (by %s, pub=%s, ident=%s/%s, pwid=%s, language=%s, cover=%r)"
            args = [changed_status, self.title, self.author, self.publisher,
                    self.primary_identifier.type, self.primary_identifier.identifier,
                    self.permanent_work_id, self.language
            ]
            if self.cover and self.cover.representation:
                args.append(self.cover.representation.public_url)
            else:
                args.append(None)
            level(msg, *args)
        return changed

    def calculate_author(self):
        """Turn the list of Contributors into string values for .author
        and .sort_author.
        """

        sort_names = []
        display_names = []
        for author in self.author_contributors:
            if author.sort_name and not author.display_name or not author.family_name:
                default_family, default_display = author.default_names()
            display_name = author.display_name or default_display or author.sort_name
            family_name = author.family_name or default_family or author.sort_name
            display_names.append([family_name, display_name])
            sort_names.append(author.sort_name)
        if display_names:
            author = ", ".join([x[1] for x in sorted(display_names)])
        else:
            author = self.UNKNOWN_AUTHOR
        if sort_names:
            sort_author = " ; ".join(sorted(sort_names))
        else:
            sort_author = self.UNKNOWN_AUTHOR
        return author, sort_author

    def choose_cover(self):
        """Try to find a cover that can be used for this Edition."""
        self.cover_full_url = None
        self.cover_thumbnail_url = None
        for distance in (0, 5):
            # If there's a cover directly associated with the
            # Edition's primary ID, use it. Otherwise, find the
            # best cover associated with any related identifier.
            best_cover, covers = self.best_cover_within_distance(distance)

            if best_cover:
                if not best_cover.representation:
                    logging.warn(
                        "Best cover for %r has no representation!",
                        self.primary_identifier,
                    )
                else:
                    rep = best_cover.representation
                    if not rep.thumbnails:
                        logging.warn(
                            "Best cover for %r (%s) was never thumbnailed!",
                            self.primary_identifier,
                            rep.public_url
                        )
                self.set_cover(best_cover)
                break
        else:
            # No cover has been found. If the Edition currently references
            # a cover, it has since been rejected or otherwise removed.
            # Cover details need to be removed.
            cover_info = [self.cover, self.cover_full_url]
            if any(cover_info):
                self.cover = None
                self.cover_full_url = None

        if not self.cover_thumbnail_url:
            # The process we went through above did not result in the
            # setting of a thumbnail cover.
            #
            # It's possible there's a thumbnail even when there's no
            # full-sized cover, or when the full-sized cover and
            # thumbnail are different Resources on the same
            # Identifier. Try to find a thumbnail the same way we'd
            # look for a cover.
            for distance in (0, 5):
                best_thumbnail, thumbnails = self.best_cover_within_distance(distance, rel=Hyperlink.THUMBNAIL_IMAGE)
                if best_thumbnail:
                    if not best_thumbnail.representation:
                        logging.warn(
                            "Best thumbnail for %r has no representation!",
                            self.primary_identifier,
                        )
                    else:
                        rep = best_thumbnail.representation
                        if rep:
                            self.cover_thumbnail_url = rep.public_url
                        break
            else:
                # No thumbnail was found. If the Edition references a thumbnail,
                # it needs to be removed.
                if self.cover_thumbnail_url:
                    self.cover_thumbnail_url = None

        # Whether or not we succeeded in setting the cover,
        # record the fact that we tried.
        CoverageRecord.add_for(
            self, data_source=self.data_source,
            operation=CoverageRecord.CHOOSE_COVER_OPERATION
        )

Index("ix_editions_data_source_id_identifier_id", Edition.data_source_id, Edition.primary_identifier_id, unique=True)

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

class LicensePoolDeliveryMechanism(Base):
    """A mechanism for delivering a specific book from a specific
    distributor.

    It's presumed that all LicensePools for a given DataSource and
    Identifier have the same set of LicensePoolDeliveryMechanisms.

    This is mostly an association class between DataSource, Identifier and
    DeliveryMechanism, but it also may incorporate a specific Resource
    (i.e. a static link to a downloadable file) which explains exactly
    where to go for delivery.
    """
    __tablename__ = 'licensepooldeliveries'

    id = Column(Integer, primary_key=True)

    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True, nullable=False
    )

    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True, nullable=False
    )

    delivery_mechanism_id = Column(
        Integer, ForeignKey('deliverymechanisms.id'),
        index=True,
        nullable=False
    )

    resource_id = Column(Integer, ForeignKey('resources.id'), nullable=True)

    # One LicensePoolDeliveryMechanism may fulfill many Loans.
    fulfills = relationship("Loan", backref="fulfillment")

    # One LicensePoolDeliveryMechanism may be associated with one RightsStatus.
    rightsstatus_id = Column(
        Integer, ForeignKey('rightsstatus.id'), index=True)

    @classmethod
    def set(cls, data_source, identifier, content_type, drm_scheme, rights_uri,
            resource=None, autocommit=True):
        """Register the fact that a distributor makes a title available in a
        certain format.

        :param data_source: A DataSource identifying the distributor.
        :param identifier: An Identifier identifying the title.
        :param content_type: The title is available in this media type.
        :param drm_scheme: Access to the title is confounded by this
            DRM scheme.
        :param rights_uri: A URI representing the public's rights to the
            title.
        :param resource: A Resource representing the book itself in
            a freely redistributable form.
        :param autocommit: Commit the database session immediately if
            anything changes in the database. If you're already inside
            a nested transaction, pass in False here to avoid
            committing prematurely, but understand that if a
            LicensePool's open-access status changes as a result of
            calling this method, the change may not be properly
            reflected in LicensePool.open_access.
        """
        _db = Session.object_session(data_source)
        delivery_mechanism, ignore = DeliveryMechanism.lookup(
            _db, content_type, drm_scheme
        )
        rights_status = RightsStatus.lookup(_db, rights_uri)
        lpdm, dirty = get_one_or_create(
            _db, LicensePoolDeliveryMechanism,
            identifier=identifier,
            data_source=data_source,
            delivery_mechanism=delivery_mechanism,
            resource=resource
        )
        if not lpdm.rights_status or rights_status.uri != RightsStatus.UNKNOWN:
            # We have better information available about the
            # rights status of this delivery mechanism.
            lpdm.rights_status = rights_status
            dirty = True

        if dirty:
            # TODO: We need to explicitly commit here so that
            # LicensePool.delivery_mechanisms gets updated. It would be
            # better if we didn't have to do this, but I haven't been able
            # to get LicensePool.delivery_mechanisms to notice that it's
            # out of date.
            if autocommit:
                _db.commit()

            # Creating or modifying a LPDM might change the open-access status
            # of all LicensePools for that DataSource/Identifier.
            for pool in lpdm.license_pools:
                pool.set_open_access_status()
        return lpdm

    @property
    def is_open_access(self):
        """Is this an open-access delivery mechanism?"""
        return (self.rights_status
                and self.rights_status.uri in RightsStatus.OPEN_ACCESS)

    def compatible_with(self, other):
        """Can a single loan be fulfilled with both this
        LicensePoolDeliveryMechanism and the given one?

        :param other: A LicensePoolDeliveryMechanism.
        """
        if not isinstance(other, LicensePoolDeliveryMechanism):
            return False

        if other.id==self.id:
            # They two LicensePoolDeliveryMechanisms are the same object.
            return True

        # The two LicensePoolDeliveryMechanisms must be different ways
        # of getting the same book from the same source.
        if other.identifier_id != self.identifier_id:
            return False
        if other.data_source_id != self.data_source_id:
            return False

        if other.delivery_mechanism_id == self.delivery_mechanism_id:
            # We have two LicensePoolDeliveryMechanisms for the same
            # underlying delivery mechanism. This can happen when an
            # open-access book gets its content mirrored to two
            # different places.
            return True

        # If the DeliveryMechanisms themselves are compatible, then the
        # LicensePoolDeliveryMechanisms are compatible.
        #
        # In practice, this means that either the two
        # DeliveryMechanisms are the same or that one of them is a
        # streaming mechanism.
        open_access_rules = self.is_open_access and other.is_open_access
        return (
            other.delivery_mechanism
            and self.delivery_mechanism.compatible_with(
                other.delivery_mechanism, open_access_rules
            )
        )

    def delete(self):
        """Delete a LicensePoolDeliveryMechanism."""
        _db = Session.object_session(self)
        pools = list(self.license_pools)
        _db.delete(self)

        # TODO: We need to explicitly commit here so that
        # LicensePool.delivery_mechanisms gets updated. It would be
        # better if we didn't have to do this, but I haven't been able
        # to get LicensePool.delivery_mechanisms to notice that it's
        # out of date.
        _db.commit()

        # The deletion of a LicensePoolDeliveryMechanism might affect
        # the open-access status of its associated LicensePools.
        for pool in pools:
            pool.set_open_access_status()

    def set_rights_status(self, uri):
        _db = Session.object_session(self)
        status = RightsStatus.lookup(_db, uri)
        self.rights_status = status
        # A change to a LicensePoolDeliveryMechanism's rights status
        # might affect the open-access status of its associated
        # LicensePools.
        for pool in self.license_pools:
            pool.set_open_access_status()
        return status

    @property
    def license_pools(self):
        """Find all LicensePools for this LicensePoolDeliveryMechanism.
        """
        _db = Session.object_session(self)
        return _db.query(LicensePool).filter(
            LicensePool.data_source==self.data_source).filter(
                LicensePool.identifier==self.identifier)

    def __repr__(self):
        return "<LicensePoolDeliveryMechanism: data_source=%s, identifier=%r, mechanism=%r>" % (self.data_source, self.identifier, self.delivery_mechanism)

    __table_args__ = (
        UniqueConstraint('data_source_id', 'identifier_id',
                         'delivery_mechanism_id', 'resource_id'),
    )

Index(
    "ix_licensepooldeliveries_datasource_identifier_mechanism",
    LicensePoolDeliveryMechanism.data_source_id,
    LicensePoolDeliveryMechanism.identifier_id,
    LicensePoolDeliveryMechanism.delivery_mechanism_id,
    LicensePoolDeliveryMechanism.resource_id,
)


class Hyperlink(Base):
    """A link between an Identifier and a Resource."""

    __tablename__ = 'hyperlinks'

    # Some common link relations.
    CANONICAL = u"canonical"
    GENERIC_OPDS_ACQUISITION = u"http://opds-spec.org/acquisition"
    OPEN_ACCESS_DOWNLOAD = u"http://opds-spec.org/acquisition/open-access"
    IMAGE = u"http://opds-spec.org/image"
    THUMBNAIL_IMAGE = u"http://opds-spec.org/image/thumbnail"
    SAMPLE = u"http://opds-spec.org/acquisition/sample"
    ILLUSTRATION = u"http://librarysimplified.org/terms/rel/illustration"
    REVIEW = u"http://schema.org/Review"
    DESCRIPTION = u"http://schema.org/description"
    SHORT_DESCRIPTION = u"http://librarysimplified.org/terms/rel/short-description"
    AUTHOR = u"http://schema.org/author"
    ALTERNATE = u"alternate"

    # TODO: Is this the appropriate relation?
    DRM_ENCRYPTED_DOWNLOAD = u"http://opds-spec.org/acquisition/"
    BORROW = u"http://opds-spec.org/acquisition/borrow"

    CIRCULATION_ALLOWED = [OPEN_ACCESS_DOWNLOAD, DRM_ENCRYPTED_DOWNLOAD, BORROW, GENERIC_OPDS_ACQUISITION]
    METADATA_ALLOWED = [CANONICAL, IMAGE, THUMBNAIL_IMAGE, ILLUSTRATION, REVIEW,
        DESCRIPTION, SHORT_DESCRIPTION, AUTHOR, ALTERNATE, SAMPLE]
    MIRRORED = [OPEN_ACCESS_DOWNLOAD, IMAGE, THUMBNAIL_IMAGE]

    id = Column(Integer, primary_key=True)

    # A Hyperlink is always associated with some Identifier.
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True, nullable=False)

    # The DataSource through which this link was discovered.
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True, nullable=False)

    # The link relation between the Identifier and the Resource.
    rel = Column(Unicode, index=True, nullable=False)

    # The Resource on the other end of the link.
    resource_id = Column(
        Integer, ForeignKey('resources.id'), index=True, nullable=False)

    @classmethod
    def unmirrored(cls, collection):
        """Find all Hyperlinks associated with an item in the
        given Collection that could be mirrored but aren't.

        TODO: We don't cover the case where an image was mirrored but no
        thumbnail was created of it. (We do cover the case where the thumbnail
        was created but not mirrored.)
        """
        _db = Session.object_session(collection)
        qu = _db.query(Hyperlink).join(
            Hyperlink.identifier
        ).join(
            Identifier.licensed_through
        ).outerjoin(
            Hyperlink.resource
        ).outerjoin(
            Resource.representation
        )
        qu = qu.filter(LicensePool.collection_id==collection.id)
        qu = qu.filter(Hyperlink.rel.in_(Hyperlink.MIRRORED))
        qu = qu.filter(Hyperlink.data_source==collection.data_source)
        qu = qu.filter(
            or_(
                Representation.id==None,
                Representation.mirror_url==None,
            )
        )
        # Without this ordering, the query does a table scan looking for
        # items that match. With the ordering, they're all at the front.
        qu = qu.order_by(Representation.mirror_url.asc().nullsfirst(),
                         Representation.id.asc().nullsfirst())
        return qu

    @classmethod
    def generic_uri(cls, data_source, identifier, rel, content=None):
        """Create a generic URI for the other end of this hyperlink.

        This is useful for resources that are obtained through means
        other than fetching a single URL via HTTP. It lets us get a
        URI that's most likely unique, so we can create a Resource
        object without violating the uniqueness constraint.

        If the output of this method isn't unique in your situation
        (because the data source provides more than one link with a
        given link relation for a given identifier), you'll need some
        other way of coming up with generic URIs.

        """
        l = [identifier.urn, urllib.quote(data_source.name), urllib.quote(rel)]
        if content:
            m = md5.new()
            if isinstance(content, unicode):
                content = content.encode("utf8")
            m.update(content)
            l.append(m.hexdigest())
        return ":".join(l)

    @classmethod
    def _default_filename(self, rel):
        if rel == self.OPEN_ACCESS_DOWNLOAD:
            return 'content'
        elif rel == self.IMAGE:
            return 'cover'
        elif rel == self.THUMBNAIL_IMAGE:
            return 'cover-thumbnail'

    @property
    def default_filename(self):
        return self._default_filename(self.rel)


class Resource(Base):
    """An external resource that may be mirrored locally.
    E.g: a cover image, an epub, a description.
    """
    from works import Work

    __tablename__ = 'resources'

    # How many votes is the initial quality estimate worth?
    ESTIMATED_QUALITY_WEIGHT = 5

    # The point at which a generic geometric image is better
    # than a lousy cover we got from the Internet.
    MINIMUM_IMAGE_QUALITY = 0.25

    id = Column(Integer, primary_key=True)

    # A URI that uniquely identifies this resource. Most of the time
    # this will be an HTTP URL, which is why we're calling it 'url',
    # but it may also be a made-up URI.
    url = Column(Unicode, index=True)

    # Many Editions may choose this resource (as opposed to other
    # resources linked to them with rel="image") as their cover image.
    cover_editions = relationship("Edition", backref="cover", foreign_keys=[Edition.cover_id])

    # Many Works may use this resource (as opposed to other resources
    # linked to them with rel="description") as their summary.
    summary_works = relationship("Work", backref="summary", foreign_keys=[Work.summary_id])

    # Many LicensePools (but probably one at most) may use this
    # resource in a delivery mechanism.
    licensepooldeliverymechanisms = relationship(
        "LicensePoolDeliveryMechanism", backref="resource",
        foreign_keys=[LicensePoolDeliveryMechanism.resource_id]
    )

    links = relationship("Hyperlink", backref="resource")

    # The DataSource that is the controlling authority for this Resource.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    # An archived Representation of this Resource.
    representation_id = Column(
        Integer, ForeignKey('representations.id'), index=True)

    # The rights status of this Resource.
    rights_status_id = Column(Integer, ForeignKey('rightsstatus.id'))

    # An optional explanation of the rights status.
    rights_explanation = Column(Unicode)

    # A Resource may be transformed into many derivatives.
    transformations = relationship(
        'ResourceTransformation',
        primaryjoin="ResourceTransformation.original_id==Resource.id",
        foreign_keys=id,
        lazy="joined",
        backref=backref('original', uselist=False),
        uselist=True,
    )

    # A derivative resource may have one original.
    derived_through = relationship(
        'ResourceTransformation',
        primaryjoin="ResourceTransformation.derivative_id==Resource.id",
        foreign_keys=id,
        backref=backref('derivative', uselist=False),
        lazy="joined",
        uselist=False,
    )

    # A calculated value for the quality of this resource, based on an
    # algorithmic treatment of its content.
    estimated_quality = Column(Float)

    # The average of human-entered values for the quality of this
    # resource.
    voted_quality = Column(Float, default=float(0))

    # How many votes contributed to the voted_quality value. This lets
    # us scale new votes proportionately while keeping only two pieces
    # of information.
    votes_for_quality = Column(Integer, default=0)

    # A combination of the calculated quality value and the
    # human-entered quality value.
    quality = Column(Float, index=True)

    # URL must be unique.
    __table_args__ = (
        UniqueConstraint('url'),
    )

    @property
    def final_url(self):
        """URL to the final, mirrored version of this resource, suitable
        for serving to the client.

        :return: A URL, or None if the resource has no mirrored
        representation.
        """
        if not self.representation:
            return None
        if not self.representation.mirror_url:
            return None
        return self.representation.mirror_url

    def as_delivery_mechanism_for(self, licensepool):
        """If this Resource is used in a LicensePoolDeliveryMechanism for the
        given LicensePool, return that LicensePoolDeliveryMechanism.
        """
        for lpdm in licensepool.delivery_mechanisms:
            if lpdm.resource == self:
                return lpdm

    def set_fetched_content(self, media_type, content, content_path):
        """Simulate a successful HTTP request for a representation
        of this resource.

        This is used when the content of the representation is obtained
        through some other means.
        """
        _db = Session.object_session(self)

        if not (content or content_path):
            raise ValueError(
                "One of content and content_path must be specified.")
        if content and content_path:
            raise ValueError(
                "Only one of content and content_path may be specified.")
        representation, is_new = get_one_or_create(
            _db, Representation, url=self.url, media_type=media_type)
        self.representation = representation
        representation.set_fetched_content(content, content_path)

    def set_estimated_quality(self, estimated_quality):
        """Update the estimated quality."""
        self.estimated_quality = estimated_quality
        self.update_quality()

    def add_quality_votes(self, quality, weight=1):
        """Record someone's vote as to the quality of this resource."""
        self.voted_quality = self.voted_quality or 0
        self.votes_for_quality = self.votes_for_quality or 0

        total_quality = self.voted_quality * self.votes_for_quality
        total_quality += (quality * weight)
        self.votes_for_quality += weight
        self.voted_quality = total_quality / float(self.votes_for_quality)
        self.update_quality()

    def reject(self):
        """Reject a Resource by making its voted_quality negative.

        If the Resource is a cover, this rejection will render it unusable to
        all Editions and Identifiers. Even if the cover is later `approved`
        a rejection impacts the overall weight of the `vote_quality`.
        """
        if not self.voted_quality:
            self.add_quality_votes(-1)
            return

        if self.voted_quality < 0:
            # This Resource has already been rejected.
            return

        # Humans have voted positively on this Resource, and now it's
        # being rejected regardless.
        logging.warn("Rejecting Resource with positive votes: %r", self)

        # Make the voted_quality negative without impacting the weight
        # of existing votes so the value can be restored relatively
        # painlessly if necessary.
        self.voted_quality = -self.voted_quality

        # However, because `votes_for_quality` is incremented, a
        # rejection will impact the weight of all `voted_quality` votes
        # even if the Resource is later approved.
        self.votes_for_quality += 1
        self.update_quality()

    def approve(self):
        """Approve a rejected Resource by making its human-generated
        voted_quality positive while taking its rejection into account.
        """
        if self.voted_quality < 0:
            # This Resource has been rejected. Reset its value to be
            # positive.
            if self.voted_quality == -1 and self.votes_for_quality == 1:
                # We're undoing a single rejection.
                self.voted_quality = 0
            else:
                # An existing positive voted_quality was made negative.
                self.voted_quality = abs(self.voted_quality)
            self.votes_for_quality += 1
            self.update_quality()
            return

        self.add_quality_votes(1)

    def update_quality(self):
        """Combine computer-generated `estimated_quality` with
        human-generated `voted_quality` to form overall `quality`.
        """
        estimated_weight = self.ESTIMATED_QUALITY_WEIGHT
        votes_for_quality = self.votes_for_quality or 0
        total_weight = estimated_weight + votes_for_quality

        voted_quality = (self.voted_quality or 0) * votes_for_quality
        total_quality = (((self.estimated_quality or 0) * self.ESTIMATED_QUALITY_WEIGHT) +
                         voted_quality)

        if voted_quality < 0 and total_quality > 0:
            # If `voted_quality` is negative, the Resource has been
            # rejected by a human and should no longer be available.
            #
            # This human-generated negativity must be passed to the final
            # Resource.quality value.
            total_quality = -(total_quality)
        self.quality = total_quality / float(total_weight)

    @classmethod
    def image_type_priority(cls, media_type):
        """Where does the given image media type rank on our list of
        preferences?

        :return: A lower number is better. None means it's not an
        image type or we don't care about it at all.
        """
        if media_type in MediaTypes.IMAGE_MEDIA_TYPES:
            return MediaTypes.IMAGE_MEDIA_TYPES.index(media_type)
        return None

    @classmethod
    def best_covers_among(cls, resources):

        """Choose the best covers from a list of Resources."""
        champions = []
        champion_key = None

        for r in resources:
            rep = r.representation
            if not rep:
                # A Resource with no Representation is not usable, period
                continue
            media_priority = cls.image_type_priority(rep.media_type)
            if media_priority is None:
                media_priority = float('inf')

            # This method will set the quality if it hasn't been set before.
            r.quality_as_thumbnail_image
            # Now we can use it.
            quality = r.quality
            if not quality >= cls.MINIMUM_IMAGE_QUALITY:
                # A Resource below the minimum quality threshold is not
                # usable, period.
                continue

            # In order, our criteria are: whether we
            # mirrored the representation (which means we directly
            # control it), image quality, and media type suitability.
            #
            # We invert media type suitability because it's given to us
            # as a priority (where smaller is better), but we want to compare
            # it as a quantity (where larger is better).
            compare_key = (rep.mirror_url is not None, quality, -media_priority)
            if not champion_key or (compare_key > champion_key):
                # A new champion.
                champions = [r]
                champion_key = compare_key
            elif compare_key == champion_key:
                # This image is equally good as the existing champion.
                champions.append(r)

        return champions

    @property
    def quality_as_thumbnail_image(self):
        """Determine this image's suitability for use as a thumbnail image.
        """
        rep = self.representation
        if not rep:
            return 0

        quality = 1
        # If the size of the image is known, that might affect
        # the quality.
        quality = quality * rep.thumbnail_size_quality_penalty

        # Scale the estimated quality by the source of the image.
        source_name = self.data_source.name
        if source_name==DataSourceConstants.GUTENBERG_COVER_GENERATOR:
            quality = quality * 0.60
        elif source_name==DataSourceConstants.GUTENBERG:
            quality = quality * 0.50
        elif source_name==DataSourceConstants.OPEN_LIBRARY:
            quality = quality * 0.25
        elif source_name in DataSourceConstants.PRESENTATION_EDITION_PRIORITY:
            # Covers from the data sources listed in
            # PRESENTATION_EDITION_PRIORITY (e.g. the metadata wrangler
            # and the administrative interface) are given priority
            # over all others, relative to their position in
            # PRESENTATION_EDITION_PRIORITY.
            i = DataSourceConstants.PRESENTATION_EDITION_PRIORITY.index(source_name)
            quality = quality * (i+2)
        self.set_estimated_quality(quality)
        return quality

    def add_derivative(self, derivative_resource, settings=None):
        _db = Session.object_session(self)

        transformation, ignore = get_one_or_create(
            _db, ResourceTransformation, derivative_id=derivative_resource.id)
        transformation.original_id = self.id
        transformation.settings = settings or {}
        return transformation

class ResourceTransformation(Base):
    """A record that a resource is a derivative of another resource,
    and the settings that were used to transform the original into it.
    """

    __tablename__ = 'resourcetransformations'

    # The derivative resource. A resource can only be derived from one other resource.
    derivative_id = Column(
        Integer, ForeignKey('resources.id'), index=True, primary_key=True)

    # The original resource that was transformed into the derivative.
    original_id = Column(
        Integer, ForeignKey('resources.id'), index=True)

    # The settings used for the transformation.
    settings = Column(MutableDict.as_mutable(JSON), default={})

class LicensePool(Base):
    """A pool of undifferentiated licenses for a work from a given source.
    """

    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # A LicensePool may be associated with a Work. (If it's not, no one
    # can check it out.)
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # Each LicensePool is associated with one DataSource and one
    # Identifier.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    identifier_id = Column(Integer, ForeignKey('identifiers.id'), index=True)

    # Each LicensePool belongs to one Collection.
    collection_id = Column(Integer, ForeignKey('collections.id'),
                           index=True, nullable=False)

    # Each LicensePool has an Edition which contains the metadata used
    # to describe this book.
    presentation_edition_id = Column(Integer, ForeignKey('editions.id'), index=True)

    # One LicensePool can have many Loans.
    loans = relationship('Loan', backref='license_pool')

    # One LicensePool can have many Holds.
    holds = relationship('Hold', backref='license_pool')

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool")

    # One LicensePool can be associated with many Complaints.
    complaints = relationship('Complaint', backref='license_pool')

    # The date this LicensePool was first created in our db
    # (the date we first discovered that ​we had that book in ​our collection).
    availability_time = Column(DateTime, index=True)

    # A LicensePool may be superceded by some other LicensePool
    # associated with the same Work. This may happen if it's an
    # open-access LicensePool and a better-quality version of the same
    # book is available from another Open-Access source.
    superceded = Column(Boolean, default=False)

    # A LicensePool that seemingly looks fine may be manually suppressed
    # to be temporarily or permanently removed from the collection.
    suppressed = Column(Boolean, default=False, index=True)

    # A textual description of a problem with this license pool
    # that caused us to suppress it.
    license_exception = Column(Unicode, index=True)

    open_access = Column(Boolean, index=True)
    last_checked = Column(DateTime, index=True)
    licenses_owned = Column(Integer, default=0, index=True)
    licenses_available = Column(Integer,default=0, index=True)
    licenses_reserved = Column(Integer,default=0)
    patrons_in_hold_queue = Column(Integer,default=0)

    # This lets us cache the work of figuring out the best open access
    # link for this LicensePool.
    _open_access_download_url = Column(Unicode, name="open_access_download_url")

    # A Collection can not have more than one LicensePool for a given
    # Identifier from a given DataSource.
    __table_args__ = (
        UniqueConstraint('identifier_id', 'data_source_id', 'collection_id'),
    )

    delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism",
        primaryjoin="and_(LicensePool.data_source_id==LicensePoolDeliveryMechanism.data_source_id, LicensePool.identifier_id==LicensePoolDeliveryMechanism.identifier_id)",
        foreign_keys=(data_source_id, identifier_id),
        uselist=True,
    )

    def __repr__(self):
        if self.identifier:
            identifier = "%s/%s" % (self.identifier.type,
                                    self.identifier.identifier)
        else:
            identifier = "unknown identifier"
        return "<LicensePool #%s for %s: owned=%d available=%d reserved=%d holds=%d>" % (
            self.id, identifier, self.licenses_owned, self.licenses_available,
            self.licenses_reserved, self.patrons_in_hold_queue
        )

    @classmethod
    def for_foreign_id(self, _db, data_source, foreign_id_type, foreign_id,
                       rights_status=None, collection=None, autocreate=True):
        """Find or create a LicensePool for the given foreign ID."""

        if not collection:
            raise CollectionMissing()

        # Get the DataSource.
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)

        # The type of the foreign ID must be the primary identifier
        # type for the data source.
        if (data_source.primary_identifier_type and
            foreign_id_type != data_source.primary_identifier_type
            and foreign_id_type != Identifier.DEPRECATED_NAMES.get(data_source.primary_identifier_type)
        ):
            raise ValueError(
                "License pools for data source '%s' are keyed to "
                "identifier type '%s' (not '%s', which was provided)" % (
                    data_source.name, data_source.primary_identifier_type,
                    foreign_id_type
                )
            )

        # Get the Identifier.
        identifier, ignore = Identifier.for_foreign_id(
            _db, foreign_id_type, foreign_id
            )

        kw = dict(data_source=data_source, identifier=identifier,
                  collection=collection)
        if rights_status:
            kw['rights_status'] = rights_status

        # Get the LicensePool that corresponds to the
        # DataSource/Identifier/Collection.
        if autocreate:
            license_pool, was_new = get_one_or_create(_db, LicensePool, **kw)
        else:
            license_pool = get_one(_db, LicensePool, **kw)
            was_new = False

        if was_new and not license_pool.availability_time:
            now = datetime.datetime.utcnow()
            license_pool.availability_time = now

        if was_new:
            # Set the LicensePool's initial values to indicate
            # that we don't actually know how many copies we own.
            license_pool.licenses_owned = 0
            license_pool.licenses_available = 0
            license_pool.licenses_reserved = 0
            license_pool.patrons_in_hold_queue = 0

        return license_pool, was_new

    @classmethod
    def with_no_work(cls, _db):
        """Find LicensePools that have no corresponding Work."""
        return _db.query(LicensePool).outerjoin(Work).filter(
            Work.id==None).all()

    @property
    def deliverable(self):
        """This LicensePool can actually be delivered to patrons.
        """
        return (
            (self.open_access or self.licenses_owned > 0)
            and any(
                [dm.delivery_mechanism.default_client_can_fulfill
                for dm in self.delivery_mechanisms]
            )
        )

    @classmethod
    def with_complaint(cls, library, resolved=False):
        """Return query for LicensePools that have at least one Complaint."""
        _db = Session.object_session(library)
        subquery = _db.query(
                LicensePool.id,
                func.count(LicensePool.id).label("complaint_count")
            ).select_from(LicensePool).join(
                LicensePool.collection).join(
                    Collection.libraries).filter(
                        Library.id==library.id
                    ).join(
                        LicensePool.complaints
                    ).group_by(
                        LicensePool.id
                    )

        if resolved == False:
            subquery = subquery.filter(Complaint.resolved == None)
        elif resolved == True:
            subquery = subquery.filter(Complaint.resolved != None)

        subquery = subquery.subquery()

        return _db.query(LicensePool).\
            join(subquery, LicensePool.id == subquery.c.id).\
            order_by(subquery.c.complaint_count.desc()).\
            add_columns(subquery.c.complaint_count)

    @property
    def open_access_source_priority(self):
        """What priority does this LicensePool's DataSource have in
        our list of open-access content sources?

        e.g. GITenberg books are prefered over Gutenberg books,
        because there's a defined process for fixing errors and they
        are more likely to have good cover art.
        """
        try:
            priority = DataSourceConstants.OPEN_ACCESS_SOURCE_PRIORITY.index(
                self.data_source.name
            )
        except ValueError, e:
            # The source of this download is not mentioned in our
            # priority list. Treat it as the lowest priority.
            priority = -1
        return priority

    def better_open_access_pool_than(self, champion):
        """ Is this open-access pool generally known for better-quality
        download files than the passed-in pool?
        """
        # A license pool with no identifier shouldn't happen, but it
        # definitely shouldn't be considered.
        if not self.identifier:
            return False

        # A non-open-access license pool is not eligible for consideration.
        if not self.open_access:
            return False

        # At this point we have a LicensePool that is at least
        # better than nothing.
        if not champion:
            return True

        # A suppressed license pool should never be used unless there is
        # no alternative.
        if self.suppressed:
            return False

        # If the previous champion is suppressed but we have a license pool
        # that's not, it's definitely better.
        if champion.suppressed:
            return True

        challenger_resource = self.best_open_access_link
        if not challenger_resource:
            # This LicensePool is supposedly open-access but we don't
            # actually know where the book is. It will be chosen only
            # if there is no alternative.
            return False

        champion_priority = champion.open_access_source_priority
        challenger_priority = self.open_access_source_priority

        if challenger_priority > champion_priority:
            return True

        if challenger_priority < champion_priority:
            return False

        if (self.data_source.name == DataSourceConstants.GUTENBERG
            and champion.data_source == self.data_source):
            # These two LicensePools are both from Gutenberg, and
            # normally this wouldn't matter, but higher Gutenberg
            # numbers beat lower Gutenberg numbers.
            champion_id = int(champion.identifier.identifier)
            challenger_id = int(self.identifier.identifier)

            if challenger_id > champion_id:
                logging.info(
                    "Gutenberg %d beats Gutenberg %d",
                    challenger_id, champion_id
                )
                return True
        return False

    def set_open_access_status(self):
        """Set .open_access based on whether there is currently
        an open-access LicensePoolDeliveryMechanism for this LicensePool.
        """
        for dm in self.delivery_mechanisms:
            if dm.is_open_access:
                self.open_access = True
                break
        else:
            self.open_access = False

    def set_presentation_edition(self, equivalent_editions=None):
        """Create or update the presentation Edition for this LicensePool.

        The presentation Edition is made of metadata from all Editions
        associated with the LicensePool's identifier.

        :param equivalent_editions: An optional list of Edition objects
        that don't share this LicensePool's identifier but are associated
        with its equivalent identifiers in some way. This option is used
        to create Works on the Metadata Wrangler.

        :return: A boolean explaining whether any of the presentation
        information associated with this LicensePool actually changed.
        """
        _db = Session.object_session(self)
        old_presentation_edition = self.presentation_edition
        changed = False

        editions = equivalent_editions
        if not editions:
            editions = self.identifier.primarily_identifies
        all_editions = list(Edition.sort_by_priority(editions))

        # Note: We can do a cleaner solution, if we refactor to not use metadata's
        # methods to update editions.  For now, we're choosing to go with the below approach.
        from metadata_layer import (
            Metadata,
            IdentifierData,
            ReplacementPolicy,
        )

        if len(all_editions) == 1:
            # There's only one edition associated with this
            # LicensePool. Use it as the presentation edition rather
            # than creating an identical composite.
            self.presentation_edition = all_editions[0]
        else:
            edition_identifier = IdentifierData(self.identifier.type, self.identifier.identifier)
            metadata = Metadata(data_source=DataSourceConstants.PRESENTATION_EDITION, primary_identifier=edition_identifier)

            for edition in all_editions:
                if (edition.data_source.name != DataSourceConstants.PRESENTATION_EDITION):
                    metadata.update(Metadata.from_edition(edition))

            # Note: Since this is a presentation edition it does not have a
            # license data source, even if one of the editions it was
            # created from does have a license data source.
            metadata._license_data_source = None
            metadata.license_data_source_obj = None
            edition, is_new = metadata.edition(_db)

            policy = ReplacementPolicy.from_metadata_source()
            self.presentation_edition, edition_core_changed = metadata.apply(
                edition, collection=self.collection, replace=policy
            )
            changed = changed or edition_core_changed

        presentation_changed = self.presentation_edition.calculate_presentation()
        changed = changed or presentation_changed

        # if the license pool is associated with a work, and the work currently has no presentation edition,
        # then do a courtesy call to the work, and tell it about the presentation edition.
        if self.work and not self.work.presentation_edition:
            self.work.set_presentation_edition(self.presentation_edition)

        return (
            self.presentation_edition != old_presentation_edition
            or changed
        )

    def add_link(self, rel, href, data_source, media_type=None,
                 content=None, content_path=None,
                 rights_status_uri=None, rights_explanation=None,
                 original_resource=None, transformation_settings=None,
                 ):
        """Add a link between this LicensePool and a Resource.

        :param rel: The relationship between this LicensePool and the resource
               on the other end of the link.
        :param href: The URI of the resource on the other end of the link.
        :param media_type: Media type of the representation associated
               with the resource.
        :param content: Content of the representation associated with the
               resource.
        :param content_path: Path (relative to DATA_DIRECTORY) of the
               representation associated with the resource.
        :param rights_status_uri: The URI of the RightsStatus for this resource.
        :param rights_explanation: A free text explanation of why the RightsStatus
               applies.
        :param original_resource: Another resource that this resource was derived from.
        :param transformation_settings: The settings used to transform the original
               resource into this resource.
        """
        return self.identifier.add_link(
            rel, href, data_source, media_type, content, content_path,
            rights_status_uri, rights_explanation, original_resource,
            transformation_settings)

    def needs_update(self):
        """Is it time to update the circulation info for this license pool?"""
        now = datetime.datetime.utcnow()
        if not self.last_checked:
            # This pool has never had its circulation info checked.
            return True
        maximum_stale_time = self.data_source.extra.get(
            'circulation_refresh_rate_seconds')
        if maximum_stale_time is None:
            # This pool never needs to have its circulation info checked.
            return False
        age = now - self.last_checked
        return age > maximum_stale_time

    def update_availability(
            self, new_licenses_owned, new_licenses_available,
            new_licenses_reserved, new_patrons_in_hold_queue,
            analytics=None, as_of=None):
        """Update the LicensePool with new availability information.
        Log the implied changes with the analytics provider.
        """
        changes_made = False
        _db = Session.object_session(self)
        if not as_of:
            as_of = datetime.datetime.utcnow()
        elif as_of == CirculationEvent.NO_DATE:
            # The caller explicitly does not want
            # LicensePool.last_checked to be updated.
            as_of = None

        old_licenses_owned = self.licenses_owned
        old_licenses_available = self.licenses_available
        old_licenses_reserved = self.licenses_reserved
        old_patrons_in_hold_queue = self.patrons_in_hold_queue

        for old_value, new_value, more_event, fewer_event in (
                [self.patrons_in_hold_queue,  new_patrons_in_hold_queue,
                 CirculationEvent.DISTRIBUTOR_HOLD_PLACE, CirculationEvent.DISTRIBUTOR_HOLD_RELEASE],
                [self.licenses_available, new_licenses_available,
                 CirculationEvent.DISTRIBUTOR_CHECKIN, CirculationEvent.DISTRIBUTOR_CHECKOUT],
                [self.licenses_reserved, new_licenses_reserved,
                 CirculationEvent.DISTRIBUTOR_AVAILABILITY_NOTIFY, None],
                [self.licenses_owned, new_licenses_owned,
                 CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
                 CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE]):
            if new_value is None:
                continue
            if old_value == new_value:
                continue
            changes_made = True

            if old_value < new_value:
                event_name = more_event
            else:
                event_name = fewer_event

            if not event_name:
                continue

            self.collect_analytics_event(
                analytics, event_name, as_of, old_value, new_value
            )

        # Update the license pool with the latest information.
        any_data = False
        if new_licenses_owned is not None:
            self.licenses_owned = new_licenses_owned
            any_data = True
        if new_licenses_available is not None:
            self.licenses_available = new_licenses_available
            any_data = True
        if new_licenses_reserved is not None:
            self.licenses_reserved = new_licenses_reserved
            any_data = True
        if new_patrons_in_hold_queue is not None:
            self.patrons_in_hold_queue = new_patrons_in_hold_queue
            any_data = True

        if as_of and (any_data or changes_made):
            # Sometimes update_availability is called with no actual
            # numbers, but that's not the case this time. We got
            # numbers and they may have even changed our view of the
            # LicensePool.
            self.last_checked = as_of
            if self.work:
                self.work.last_update_time = as_of

        if changes_made:
            message, args = self.circulation_changelog(
                old_licenses_owned, old_licenses_available,
                old_licenses_reserved, old_patrons_in_hold_queue
            )
            logging.info(message, *args)

        return changes_made

    def collect_analytics_event(self, analytics, event_name, as_of,
                                old_value, new_value):
        if not analytics:
            return
        for library in self.collection.libraries:
            analytics.collect_event(
                library, self, event_name, as_of,
                old_value=old_value, new_value=new_value
            )

    def update_availability_from_delta(self, event_type, event_date, delta, analytics=None):
        """Call update_availability based on a single change seen in the
        distributor data, rather than a complete snapshot of
        distributor information as of a certain time.

        This information is unlikely to be completely accurate, but it
        should suffice until more accurate information can be
        obtained.

        No CirculationEvent is created until `update_availability` is
        called.

        Events must be processed in chronological order. Any event
        that happened than `LicensePool.last_checked` is ignored, and
        calling this method will update `LicensePool.last_checked` to
        the time of the event.

        :param event_type: A CirculationEvent constant representing the
        type of change that was seen.

        :param event_date: A datetime corresponding to when the
        change was seen.

        :param delta: The magnitude of the change that was seen.

        """
        ignore = False
        if event_date != CirculationEvent.NO_DATE and self.last_checked and event_date < self.last_checked:
            # This is an old event and its effect on availability has
            # already been taken into account.
            ignore = True

        elif self.last_checked and event_date == CirculationEvent.NO_DATE:
            # We have a history for this LicensePool and we don't know
            # where this event fits into that history. Ignore the
            # event.
            ignore = True

        if not ignore:
            (new_licenses_owned, new_licenses_available,
             new_licenses_reserved,
             new_patrons_in_hold_queue) = self._calculate_change_from_one_event(
                 event_type, delta
             )

            changes_made = self.update_availability(
                new_licenses_owned, new_licenses_available,
                new_licenses_reserved, new_patrons_in_hold_queue,
                analytics=analytics, as_of=event_date
            )
        if ignore or not changes_made:
            # Even if the event was ignored or didn't actually change
            # availability, we want to record receipt of the event
            # in the analytics.
            self.collect_analytics_event(
                analytics, event_type, event_date, 0, 0
            )

    def _calculate_change_from_one_event(self, type, delta):
        new_licenses_owned = self.licenses_owned
        new_licenses_available = self.licenses_available
        new_licenses_reserved = self.licenses_reserved
        new_patrons_in_hold_queue = self.patrons_in_hold_queue

        def deduct(value):
            # It's impossible for any of these numbers to be
            # negative.
            return max(value-delta, 0)

        CE = CirculationEvent
        added = False
        if type == CE.DISTRIBUTOR_HOLD_PLACE:
            new_patrons_in_hold_queue += delta
            if new_licenses_available:
                # If someone has put a book on hold, it must not be
                # immediately available.
                new_licenses_available = 0
        elif type == CE.DISTRIBUTOR_HOLD_RELEASE:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
        elif type == CE.DISTRIBUTOR_CHECKIN:
            if self.patrons_in_hold_queue == 0:
                new_licenses_available += delta
            else:
                # When there are patrons in the hold queue, checking
                # in a single book does not make new licenses
                # available.  Checking in more books than there are
                # patrons in the hold queue _does_ make books
                # available.  However, in neither case do patrons
                # leave the hold queue. That will happen in the near
                # future as DISTRIBUTOR_AVAILABILITY_NOTIFICATION events
                # are sent out.
                if delta > new_patrons_in_hold_queue:
                    new_licenses_available += (delta-new_patrons_in_hold_queue)
        elif type == CE.DISTRIBUTOR_CHECKOUT:
            if new_licenses_available == 0:
                # The only way to borrow books while there are no
                # licenses available is to borrow reserved copies.
                new_licenses_reserved = deduct(new_licenses_reserved)
            else:
                # We don't know whether this checkout came from
                # licenses available or from a lingering reserved
                # copy, but in most cases it came from licenses
                # available.
                new_licenses_available = deduct(new_licenses_available)
        elif type == CE.DISTRIBUTOR_LICENSE_ADD:
            new_licenses_owned += delta
            # Newly added licenses start out as available, unless there
            # are patrons in the holds queue.
            if new_patrons_in_hold_queue == 0:
                new_licenses_available += delta
        elif type == CE.DISTRIBUTOR_LICENSE_REMOVE:
            new_licenses_owned = deduct(new_licenses_owned)
            # We can't say whether or not the removed licenses should
            # be deducted from the list of available licenses, because they
            # might already be checked out.
        elif type == CE.DISTRIBUTOR_AVAILABILITY_NOTIFY:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
            new_licenses_reserved += delta
        if new_licenses_owned < new_licenses_available:
            # It's impossible to have more licenses available than
            # owned. We don't know whether this means there are some
            # extra licenses we never heard about, or whether some
            # licenses expired without us being notified, but the
            # latter is more likely.
            new_licenses_available = new_licenses_owned

        return (new_licenses_owned, new_licenses_available,
                new_licenses_reserved, new_patrons_in_hold_queue)

    def circulation_changelog(self, old_licenses_owned, old_licenses_available,
                              old_licenses_reserved, old_patrons_in_hold_queue):
        """Generate a log message describing a change to the circulation.

        :return: a 2-tuple (message, args) suitable for passing into
        logging.info or a similar method
        """
        edition = self.presentation_edition
        message = u'CHANGED '
        args = []
        if self.identifier:
            identifier_template = '%s/%s'
            identifier_args = [self.identifier.type, self.identifier.identifier]
        else:
            identifier_template = '%s'
            identifier_args = [self.identifier]
        if edition:
            message += u'%s "%s" %s (' + identifier_template + ')'
            args.extend([edition.medium,
                         edition.title or "[NO TITLE]",
                         edition.author or "[NO AUTHOR]"]
                    )
            args.extend(identifier_args)
        else:
            message += identifier_template
            args.extend(identifier_args)

        def _part(message, args, string, old_value, new_value):
            if old_value != new_value:
                args.extend([string, old_value, new_value])
                message += ' %s: %s=>%s'
            return message, args

        message, args = _part(
            message, args, "OWN", old_licenses_owned, self.licenses_owned
        )

        message, args = _part(
            message, args, "AVAIL", old_licenses_available,
            self.licenses_available
        )

        message, args = _part(
            message, args, "RSRV", old_licenses_reserved,
            self.licenses_reserved
        )

        message, args =_part(
            message, args, "HOLD", old_patrons_in_hold_queue,
            self.patrons_in_hold_queue
        )
        return message, tuple(args)

    def loan_to(self, patron_or_client, start=None, end=None, fulfillment=None, external_identifier=None):
        _db = Session.object_session(patron_or_client)
        kwargs = dict(start=start or datetime.datetime.utcnow(),
                      end=end)
        if isinstance(patron_or_client, Patron):
            loan, is_new = get_one_or_create(
                _db, Loan, patron=patron_or_client, license_pool=self,
                create_method_kwargs=kwargs)
        else:
            # An IntegrationClient can have multiple loans, so this always creates
            # a new loan rather than returning an existing loan.
            loan, is_new = create(
                _db, Loan, integration_client=patron_or_client, license_pool=self,
                create_method_kwargs=kwargs)
        if fulfillment:
            loan.fulfillment = fulfillment
        if external_identifier:
            loan.external_identifier = external_identifier
        return loan, is_new

    def on_hold_to(self, patron_or_client, start=None, end=None, position=None, external_identifier=None):
        _db = Session.object_session(patron_or_client)
        if isinstance(patron_or_client, Patron) and not patron_or_client.library.allow_holds:
            raise PolicyException("Holds are disabled for this library.")
        start = start or datetime.datetime.utcnow()
        if isinstance(patron_or_client, Patron):
            hold, new = get_one_or_create(
                _db, Hold, patron=patron_or_client, license_pool=self)
        else:
            # An IntegrationClient can have multiple holds, so this always creates
            # a new hold rather than returning an existing loan.
            hold, new = create(
                _db, Hold, integration_client=patron_or_client, license_pool=self)
        hold.update(start, end, position)
        if external_identifier:
            hold.external_identifier = external_identifier
        return hold, new

    @classmethod
    def consolidate_works(cls, _db, batch_size=10):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        a = 0
        lps = cls.with_no_work(_db)
        logging.info(
            "Assigning Works to %d LicensePools with no Work.", len(lps)
        )
        for unassigned in lps:
            etext, new = unassigned.calculate_work()
            if not etext:
                # We could not create a work for this LicensePool,
                # most likely because it does not yet have any
                # associated Edition.
                continue
            a += 1
            logging.info("When consolidating works, created %r", etext)
            if a and not a % batch_size:
                _db.commit()
        _db.commit()


    def calculate_work(
        self, known_edition=None, exclude_search=False,
        even_if_no_title=False
    ):
        """Find or create a Work for this LicensePool.

        A pool that is not open-access will always have its own
        Work. Open-access LicensePools will be grouped together with
        other open-access LicensePools based on the permanent work ID
        of the LicensePool's presentation edition.

        :param even_if_no_title: Ordinarily this method will refuse to
        create a Work for a LicensePool whose Edition has no title.
        However, in components that don't present information directly
        to readers, it's sometimes useful to create a Work even if the
        title is unknown. In that case, pass in even_if_no_title=True
        and the Work will be created.

        TODO: I think known_edition is mostly useless. We should
        either remove it or replace it with a boolean that stops us
        from calling set_presentation_edition() and assumes we've
        already done that work.

        """
        if not self.identifier:
            # A LicensePool with no Identifier should never have a Work.
            self.work = None
            return None, False

        if known_edition:
            presentation_edition = known_edition
        else:
            self.set_presentation_edition()
            presentation_edition = self.presentation_edition

        if presentation_edition:
            if self not in presentation_edition.is_presentation_for:
                raise ValueError(
                    "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!"
                )

        logging.info("Calculating work for %r", presentation_edition)
        if not presentation_edition:
            # We don't have any information about the identifier
            # associated with this LicensePool, so we can't create a work.
            logging.warn("NO EDITION for %s, cowardly refusing to create work.",
                     self.identifier)

            # If there was a work associated with this LicensePool,
            # it was by mistake. Remove it.
            self.work = None
            return None, False

        if not presentation_edition.title or not presentation_edition.author:
            presentation_edition.calculate_presentation()

        if not presentation_edition.title and not even_if_no_title:
            if presentation_edition.work:
                logging.warn(
                    "Edition %r has no title but has a Work assigned. This will not stand.", presentation_edition
                )
            else:
                logging.info("Edition %r has no title and it will not get a Work.", presentation_edition)
            self.work = None
            self.work_id = None
            return None, False

        presentation_edition.calculate_permanent_work_id()

        _db = Session.object_session(self)
        work = None
        is_new = False
        licensepools_changed = False
        if self.open_access and presentation_edition.permanent_work_id:
            # This is an open-access book. Use the Work for all
            # open-access books associated with this book's permanent
            # work ID.
            #
            # If the dataset is in an inconsistent state, calling
            # Work.open_access_for_permanent_work_id may result in works being
            # merged.
            work, is_new = Work.open_access_for_permanent_work_id(
                _db, presentation_edition.permanent_work_id,
                presentation_edition.medium, presentation_edition.language
            )

            # Run a sanity check to make sure every LicensePool
            # associated with this Work actually belongs there. This
            # may result in new Works being created.
            #
            # This could go into Work.for_permanent_work_id, but that
            # could conceivably lead to an infinite loop, or at least
            # a very long recursive call, so I've put it here.
            work.make_exclusive_open_access_for_permanent_work_id(
                presentation_edition.permanent_work_id,
                presentation_edition.medium,
                presentation_edition.language,
            )
            self.work = work
            licensepools_changed = True

        # All LicensePools with a given Identifier must share a work.
        existing_works = set([x.work for x in self.identifier.licensed_through])
        if len(existing_works) > 1:
            logging.warn(
                "LicensePools for %r have more than one Work between them. Removing them all and starting over.", self.identifier
            )
            for lp in self.identifier.licensed_through:
                lp.work = None
                if lp.presentation_edition:
                    lp.presentation_edition.work = None
        else:
            # There is a consensus Work for this Identifier.
            [self.work] = existing_works

        if self.work:
            # This pool is already associated with a Work. Use that
            # Work.
            work = self.work
        elif presentation_edition.work:
            # This pool's presentation edition is already associated with
            # a Work. Use that Work.
            work = presentation_edition.work
            self.work = work

        if work:
            # There is already a Work associated with this LicensePool,
            # but we need to run a sanity check because occasionally
            # LicensePools get mis-grouped due to bugs.
            #
            # A commercially-licensed book should have a Work to
            # itself. All other LicensePools need to be kicked out and
            # associated with some other work.
            #
            # This won't cause an infinite recursion because we're
            # setting pool.work to None before calling
            # pool.calculate_work(), and the recursive call only
            # happens if self.work is set.
            for pool in list(work.license_pools):
                if pool is self:
                    continue
                if not (self.open_access and pool.open_access):
                    pool.work = None
                    pool.calculate_work(
                        exclude_search=exclude_search,
                        even_if_no_title=even_if_no_title
                    )
                    licensepools_changed = True

        else:
            # There is no better choice than creating a brand new Work.
            is_new = True
            logging.info(
                "Creating a new work for %r" % presentation_edition.title
            )
            work = Work()
            _db = Session.object_session(self)
            _db.add(work)
            flush(_db)
            licensepools_changed = True

        # Associate this LicensePool and its Edition with the work we
        # chose or created.
        if not self in work.license_pools:
            work.license_pools.append(self)
            licensepools_changed = True

        # Recalculate the display information for the Work, since the
        # associated LicensePools have changed, which may have caused
        # the Work's presentation Edition to change.
        #
        # TODO: In theory we can speed things up by only calling
        # calculate_presentation if licensepools_changed is
        # True. However, some bits of other code call calculate_work()
        # under the assumption that it always calls
        # calculate_presentation(), so we'd need to evaluate those
        # call points first.
        work.calculate_presentation(exclude_search=exclude_search)

        # Ensure that all LicensePools with this Identifier share
        # the same Work. (We may have wiped out their .work earlier
        # in this method.)
        for lp in self.identifier.licensed_through:
            lp.work = work

        if is_new:
            logging.info("Created a new work: %r", work)

        # All done!
        return work, is_new


    @property
    def open_access_links(self):
        """Yield all open-access Resources for this LicensePool."""

        open_access = Hyperlink.OPEN_ACCESS_DOWNLOAD
        _db = Session.object_session(self)
        if not self.identifier:
            return
        q = Identifier.resources_for_identifier_ids(
            _db, [self.identifier.id], open_access
        )
        for resource in q:
            yield resource

    @property
    def open_access_download_url(self):
        """Alias for best_open_access_link.

        If _open_access_download_url is currently None, this will set
        to a good value if possible.
        """
        return self.best_open_access_link

    @property
    def best_open_access_link(self):
        """Find the best open-access link for this LicensePool.

        Cache it so that the next access will be faster.
        """
        if not self.open_access:
            return None
        if not self._open_access_download_url:
            url = None
            resource = self.best_open_access_resource
            if resource and resource.representation:
                url = resource.representation.public_url
            self._open_access_download_url = url
        return self._open_access_download_url

    @property
    def best_open_access_resource(self):
        """Determine the best open-access Resource currently provided by this
        LicensePool.
        """
        best = None
        best_priority = -1
        for resource in self.open_access_links:
            if not any(
                    [resource.representation and
                     resource.representation.media_type and
                     resource.representation.media_type.startswith(x)
                     for x in MediaTypes.SUPPORTED_BOOK_MEDIA_TYPES]):
                # This representation is not in a media type we
                # support. We can't serve it, so we won't consider it.
                continue

            data_source_priority = self.open_access_source_priority
            if not best or data_source_priority > best_priority:
                # Something is better than nothing.
                best = resource
                best_priority = data_source_priority
                continue

            if (best.data_source.name==DataSourceConstants.GUTENBERG
                and resource.data_source.name==DataSourceConstants.GUTENBERG
                and 'noimages' in best.representation.public_url
                and not 'noimages' in resource.representation.public_url):
                # A Project Gutenberg-ism: an epub without 'noimages'
                # in the filename is better than an epub with
                # 'noimages' in the filename.
                best = resource
                best_priority = data_source_priority
                continue

        return best

    @property
    def best_license_link(self):
        """Find the best available licensing link for the work associated
        with this LicensePool.

        # TODO: This needs work and may not be necessary anymore.
        """
        edition = self.edition
        if not edition:
            return self, None
        link = edition.best_open_access_link
        if link:
            return self, link

        # Either this work is not open-access, or there was no epub
        # link associated with it.
        work = self.work
        for pool in work.license_pools:
            edition = pool.edition
            link = edition.best_open_access_link
            if link:
                return pool, link
        return self, None

    def set_delivery_mechanism(self, *args, **kwargs):
        """Ensure that this LicensePool (and any other LicensePools for the same
        book) have a LicensePoolDeliveryMechanism for this media type,
        DRM scheme, rights status, and resource.
        """
        return LicensePoolDeliveryMechanism.set(
            self.data_source, self.identifier, *args, **kwargs
        )

Index("ix_licensepools_data_source_id_identifier_id_collection_id", LicensePool.collection_id, LicensePool.data_source_id, LicensePool.identifier_id, unique=True)


class RightsStatus(Base):

    """The terms under which a book has been made available to the general
    public.

    This will normally be 'in copyright', or 'public domain', or a
    Creative Commons license.
    """

    # Currently in copyright.
    IN_COPYRIGHT = u"http://librarysimplified.org/terms/rights-status/in-copyright"

    # Public domain in the USA.
    PUBLIC_DOMAIN_USA = u"http://librarysimplified.org/terms/rights-status/public-domain-usa"

    # Public domain in some unknown territory
    PUBLIC_DOMAIN_UNKNOWN = u"http://librarysimplified.org/terms/rights-status/public-domain-unknown"

    # Creative Commons Public Domain Dedication (No rights reserved)
    CC0 = u"https://creativecommons.org/publicdomain/zero/1.0/"

    # Creative Commons Attribution (CC BY)
    CC_BY = u"http://creativecommons.org/licenses/by/4.0/"

    # Creative Commons Attribution-ShareAlike (CC BY-SA)
    CC_BY_SA = u"https://creativecommons.org/licenses/by-sa/4.0"

    # Creative Commons Attribution-NoDerivs (CC BY-ND)
    CC_BY_ND = u"https://creativecommons.org/licenses/by-nd/4.0"

    # Creative Commons Attribution-NonCommercial (CC BY-NC)
    CC_BY_NC = u"https://creativecommons.org/licenses/by-nc/4.0"

    # Creative Commons Attribution-NonCommercial-ShareAlike (CC BY-NC-SA)
    CC_BY_NC_SA = u"https://creativecommons.org/licenses/by-nc-sa/4.0"

    # Creative Commons Attribution-NonCommercial-NoDerivs (CC BY-NC-ND)
    CC_BY_NC_ND = u"https://creativecommons.org/licenses/by-nc-nd/4.0"

    # Open access download but no explicit license
    GENERIC_OPEN_ACCESS = u"http://librarysimplified.org/terms/rights-status/generic-open-access"

    # Unknown copyright status.
    UNKNOWN = u"http://librarysimplified.org/terms/rights-status/unknown"

    OPEN_ACCESS = [
        PUBLIC_DOMAIN_USA,
        CC0,
        CC_BY,
        CC_BY_SA,
        CC_BY_ND,
        CC_BY_NC,
        CC_BY_NC_SA,
        CC_BY_NC_ND,
        GENERIC_OPEN_ACCESS,
    ]

    # These open access rights allow derivative works to be created, but may
    # require attribution or prohibit commercial use.
    ALLOWS_DERIVATIVES = [
        PUBLIC_DOMAIN_USA,
        CC0,
        CC_BY,
        CC_BY_SA,
        CC_BY_NC,
        CC_BY_NC_SA,
    ]

    NAMES = {
        IN_COPYRIGHT: "In Copyright",
        PUBLIC_DOMAIN_USA: "Public domain in the USA",
        CC0: "Creative Commons Public Domain Dedication (CC0)",
        CC_BY: "Creative Commons Attribution (CC BY)",
        CC_BY_SA: "Creative Commons Attribution-ShareAlike (CC BY-SA)",
        CC_BY_ND: "Creative Commons Attribution-NoDerivs (CC BY-ND)",
        CC_BY_NC: "Creative Commons Attribution-NonCommercial (CC BY-NC)",
        CC_BY_NC_SA: "Creative Commons Attribution-NonCommercial-ShareAlike (CC BY-NC-SA)",
        CC_BY_NC_ND: "Creative Commons Attribution-NonCommercial-NoDerivs (CC BY-NC-ND)",
        GENERIC_OPEN_ACCESS: "Open access with no specific license",
        UNKNOWN: "Unknown",
    }

    DATA_SOURCE_DEFAULT_RIGHTS_STATUS = {
        DataSourceConstants.GUTENBERG: PUBLIC_DOMAIN_USA,
        DataSourceConstants.PLYMPTON: CC_BY_NC,
        # workaround for opds-imported license pools with 'content server' as data source
        DataSourceConstants.OA_CONTENT_SERVER : GENERIC_OPEN_ACCESS,

        DataSourceConstants.OVERDRIVE: IN_COPYRIGHT,
        DataSourceConstants.BIBLIOTHECA: IN_COPYRIGHT,
        DataSourceConstants.AXIS_360: IN_COPYRIGHT,
    }

    __tablename__ = 'rightsstatus'
    id = Column(Integer, primary_key=True)

    # A URI unique to the license. This may be a URL (e.g. Creative
    # Commons)
    uri = Column(String, index=True, unique=True)

    # Human-readable name of the license.
    name = Column(String, index=True)

    # One RightsStatus may apply to many LicensePoolDeliveryMechanisms.
    licensepooldeliverymechanisms = relationship("LicensePoolDeliveryMechanism", backref="rights_status")

    # One RightsStatus may apply to many Resources.
    resources = relationship("Resource", backref="rights_status")

    @classmethod
    def lookup(cls, _db, uri):
        if not uri in cls.NAMES.keys():
            uri = cls.UNKNOWN
        name = cls.NAMES.get(uri)
        create_method_kwargs = dict(name=name)
        status, ignore = get_one_or_create(
            _db, RightsStatus, uri=uri,
            create_method_kwargs=create_method_kwargs
        )
        return status

    @classmethod
    def rights_uri_from_string(cls, rights):
        rights = rights.lower()
        if rights == 'public domain in the usa.':
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights == 'public domain in the united states.':
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights == 'pd-us':
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights.startswith('public domain'):
            return RightsStatus.PUBLIC_DOMAIN_UNKNOWN
        elif rights.startswith('copyrighted.'):
            return RightsStatus.IN_COPYRIGHT
        elif rights == 'cc0':
            return RightsStatus.CC0
        elif rights == 'cc by':
            return RightsStatus.CC_BY
        elif rights == 'cc by-sa':
            return RightsStatus.CC_BY_SA
        elif rights == 'cc by-nd':
            return RightsStatus.CC_BY_ND
        elif rights == 'cc by-nc':
            return RightsStatus.CC_BY_NC
        elif rights == 'cc by-nc-sa':
            return RightsStatus.CC_BY_NC_SA
        elif rights == 'cc by-nc-nd':
            return RightsStatus.CC_BY_NC_ND
        elif (rights in RightsStatus.OPEN_ACCESS
              or rights == RightsStatus.IN_COPYRIGHT):
            return rights
        else:
            return RightsStatus.UNKNOWN

class Representation(Base, MediaTypes):
    """A cached document obtained from (and possibly mirrored to) the Web
    at large.

    Sometimes this is a DataSource's representation of a specific
    book.

    Sometimes it's associated with a database Resource (which has a
    well-defined relationship to one specific book).

    Sometimes it's just a web page that we need a cached local copy
    of.
    """
    __tablename__ = 'representations'
    id = Column(Integer, primary_key=True)

    # URL from which the representation was fetched.
    url = Column(Unicode, index=True)

    # The media type of the representation.
    media_type = Column(Unicode)

    resource = relationship("Resource", backref="representation", uselist=False)

    ### Records of things we tried to do with this representation.

    # When the representation was last fetched from `url`.
    fetched_at = Column(DateTime, index=True)

    # A textual description of the error encountered the last time
    # we tried to fetch the representation
    fetch_exception = Column(Unicode, index=True)

    # A URL under our control to which this representation has been
    # mirrored.
    mirror_url = Column(Unicode, index=True)

    # When the representation was last pushed to `mirror_url`.
    mirrored_at = Column(DateTime, index=True)

    # An exception that happened while pushing this representation
    # to `mirror_url.
    mirror_exception = Column(Unicode, index=True)

    # If this image is a scaled-down version of some other image,
    # `scaled_at` is the time it was last generated.
    scaled_at = Column(DateTime, index=True)

    # If this image is a scaled-down version of some other image,
    # this is the exception that happened the last time we tried
    # to scale it down.
    scale_exception = Column(Unicode, index=True)

    ### End records of things we tried to do with this representation.

    # An image Representation may be a thumbnail version of another
    # Representation.
    thumbnail_of_id = Column(
        Integer, ForeignKey('representations.id'), index=True)

    thumbnails = relationship(
        "Representation",
        backref=backref("thumbnail_of", remote_side = [id]),
        lazy="joined", post_update=True)

    # The HTTP status code from the last fetch.
    status_code = Column(Integer)

    # A textual representation of the HTTP headers sent along with the
    # representation.
    headers = Column(Unicode)

    # The Location header from the last representation.
    location = Column(Unicode)

    # The Last-Modified header from the last representation.
    last_modified = Column(Unicode)

    # The Etag header from the last representation.
    etag = Column(Unicode)

    # The size of the representation, in bytes.
    file_size = Column(Integer)

    # If this representation is an image, the height of the image.
    image_height = Column(Integer, index=True)

    # If this representation is an image, the width of the image.
    image_width = Column(Integer, index=True)

    # The content of the representation itself.
    content = Column(Binary)

    # Instead of being stored in the database, the content of the
    # representation may be stored on a local file relative to the
    # data root.
    local_content_path = Column(Unicode)

    # At any given time, we will have a single representation for a
    # given URL and media type.
    __table_args__ = (
        UniqueConstraint('url', 'media_type'),
    )

    # A User-Agent to use when acting like a web browser.
    # BROWSER_USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36 (Simplified)"
    BROWSER_USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:37.0) Gecko/20100101 Firefox/37.0"

    @property
    def age(self):
        if not self.fetched_at:
            return 1000000
        return (datetime.datetime.utcnow() - self.fetched_at).total_seconds()

    @property
    def has_content(self):
        if self.content and self.status_code == 200 and self.fetch_exception is None:
            return True
        if self.local_content_path and os.path.exists(self.local_content_path) and self.fetch_exception is None:
            return True
        return False

    @property
    def public_url(self):
        """Find the best URL to publish when referencing this Representation
        in a public space.

        :return: a bytestring
        """
        url = None
        if self.mirror_url:
            url = self.mirror_url
        elif self.url:
            url = self.url
        elif self.resource:
            # This really shouldn't happen.
            url = self.resource.url
        if isinstance(url, unicode):
            url = url.encode("utf8")
        return url

    @property
    def is_usable(self):
        """Returns True if the Representation has some data or received
        a status code that's not in the 5xx series.
        """
        if not self.fetch_exception and (
            self.content or self.local_path or self.status_code
            and self.status_code / 100 != 5
        ):
            return True
        return False

    @classmethod
    def is_media_type(cls, s):
        """Return true if the given string looks like a media type."""
        if not s:
            return False
        s = s.lower()
        return any(s.startswith(x) for x in [
                   'application/',
                   'audio/',
                   'example/',
                   'image/',
                   'message/',
                   'model/',
                   'multipart/',
                   'text/',
                   'video/'
        ])

    @classmethod
    def guess_media_type(cls, filename):
        """Guess a likely media type from a filename."""
        if not filename:
            return None
        filename = filename.lower()
        for extension, media_type in cls.MEDIA_TYPE_FOR_EXTENSION.items():
            if filename.endswith(extension):
                return media_type
        return None

    def is_fresher_than(self, max_age):
        # Convert a max_age timedelta to a number of seconds.
        if isinstance(max_age, datetime.timedelta):
            max_age = max_age.total_seconds()

        if not self.is_usable:
            return False
        return (max_age is None or max_age > self.age)

    @classmethod
    def get(cls, _db, url, do_get=None, extra_request_headers=None,
            accept=None, max_age=None, pause_before=0, allow_redirects=True,
            presumed_media_type=None, debug=True, response_reviewer=None,
            exception_handler=None):
        """Retrieve a representation from the cache if possible.

        If not possible, retrieve it from the web and store it in the
        cache.

        :param do_get: A function that takes arguments (url, headers)
        and retrieves a representation over the network.

        :param max_age: A timedelta object representing the maximum
        time to consider a cached representation fresh. (We ignore the
        caching directives from web servers because they're usually
        far too conservative for our purposes.)

        :return: A 2-tuple (representation, obtained_from_cache)

        """
        representation = None
        do_get = do_get or cls.simple_http_get

        exception_handler = exception_handler or cls.record_exception

        # TODO: We allow representations of the same URL in different
        # media types, but we don't have a good solution here for
        # doing content negotiation (letting the caller ask for a
        # specific set of media types and matching against what we
        # have cached). Fortunately this isn't an issue with any of
        # the data sources we currently use, so for now we can treat
        # different representations of a URL as interchangeable.

        a = dict(url=url)
        if accept:
            a['media_type'] = accept
        representation = get_one(_db, Representation, 'interchangeable', **a)

        usable_representation = fresh_representation = False
        if representation:
            # Do we already have a usable representation?
            usable_representation = representation.is_usable

            # Assuming we have a usable representation, is it fresh?
            fresh_representation = representation.is_fresher_than(max_age)

        if debug is True:
            debug_level = logging.DEBUG
        elif debug is False:
            debug_level = None
        else:
            debug_level = debug

        if fresh_representation:
            if debug_level is not None:
                logging.info("Cached %s", url)
            return representation, True

        # We have a representation that is either not fresh or not usable.
        # We must make an HTTP request.
        if debug_level is not None:
            logging.log(debug_level, "Fetching %s", url)
        headers = {}
        if extra_request_headers:
            headers.update(extra_request_headers)
        if accept:
            headers['Accept'] = accept

        if usable_representation:
            # We have a representation but it's not fresh. We will
            # be making a conditional HTTP request to see if there's
            # a new version.
            if representation.last_modified:
                headers['If-Modified-Since'] = representation.last_modified
            if representation.etag:
                headers['If-None-Match'] = representation.etag

        fetched_at = datetime.datetime.utcnow()
        if pause_before:
            time.sleep(pause_before)
        media_type = None
        fetch_exception = None
        exception_traceback = None
        try:
            status_code, headers, content = do_get(url, headers)
            if response_reviewer:
                # An optional function passed to raise errors if the
                # post response isn't worth caching.
                response_reviewer((status_code, headers, content))
            exception = None
            media_type = cls._best_media_type(url, headers, presumed_media_type)
            if isinstance(content, unicode):
                content = content.encode("utf8")
        except Exception, fetch_exception:
            # This indicates there was a problem with making the HTTP
            # request, not that the HTTP request returned an error
            # condition.
            logging.error("Error making HTTP request to %s", url, exc_info=fetch_exception)
            exception_traceback = traceback.format_exc()

            status_code = None
            headers = None
            content = None
            media_type = None

        # At this point we can create/fetch a Representation object if
        # we don't have one already, or if the URL or media type we
        # actually got from the server differs from what we thought
        # we had.
        if (not usable_representation
            or media_type != representation.media_type
            or url != representation.url):
            representation, is_new = get_one_or_create(
                _db, Representation, url=url, media_type=unicode(media_type))

        if fetch_exception:
            exception_handler(
                representation, fetch_exception, exception_traceback
            )
        representation.fetched_at = fetched_at

        if status_code == 304:
            # The representation hasn't changed since we last checked.
            # Set its fetched_at property and return the cached
            # version as though it were new.
            representation.fetched_at = fetched_at
            representation.status_code = status_code
            return representation, False

        if status_code:
            status_code_series = status_code / 100
        else:
            status_code_series = None

        if status_code_series in (2,3) or status_code in (404, 410):
            # We have a new, good representation. Update the
            # Representation object and return it as fresh.
            representation.status_code = status_code
            representation.content = content
            representation.media_type = media_type

            for header, field in (
                    ('etag', 'etag'),
                    ('last-modified', 'last_modified'),
                    ('location', 'location')):
                if header in headers:
                    value = headers[header]
                else:
                    value = None
                setattr(representation, field, value)

            representation.headers = cls.headers_to_string(headers)
            representation.content = content
            representation.update_image_size()
            return representation, False

        # Okay, things didn't go so well.
        date_string = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
        representation.fetch_exception = representation.fetch_exception or (
            "Most recent fetch attempt (at %s) got status code %s" % (
                date_string, status_code))
        if usable_representation:
            # If we have a usable (but stale) representation, we'd
            # rather return the cached data than destroy the information.
            return representation, True

        # We didn't have a usable representation before, and we still don't.
        # At this point we're just logging an error.
        representation.status_code = status_code
        representation.headers = cls.headers_to_string(headers)
        representation.content = content
        return representation, False

    @classmethod
    def _best_media_type(cls, url, headers, default):
        """Determine the most likely media type for the given HTTP headers.

        Almost all the time, this is the value of the content-type
        header, if present. However, if the content-type header has a
        really generic value like "application/octet-stream" (as often
        happens with binary files hosted on Github), we'll privilege
        the default value. If there's no default value, we'll try to
        derive one from the URL extension.
        """
        default = default or cls.guess_media_type(url)
        if not headers or not 'content-type' in headers:
            return default
        headers_type = headers['content-type'].lower()
        clean = cls._clean_media_type(headers_type)
        if clean in MediaTypes.GENERIC_MEDIA_TYPES and default:
            return default
        return headers_type

    @classmethod
    def reraise_exception(cls, representation, exception, traceback):
        """Deal with a fetch exception by re-raising it."""
        raise exception

    @classmethod
    def record_exception(cls, representation, exception, traceback):
        """Deal with a fetch exception by recording it
        and moving on.
        """
        representation.fetch_exception = traceback

    @classmethod
    def post(cls, _db, url, data, max_age=None, response_reviewer=None):
        """Finds or creates POST request as a Representation"""

        def do_post(url, headers, **kwargs):
            kwargs.update({'data' : data})
            return cls.simple_http_post(url, headers, **kwargs)

        return cls.get(
            _db, url, do_get=do_post, max_age=max_age,
            response_reviewer=response_reviewer
        )

    @property
    def mirrorable_media_type(self):
        """Does this Representation look like the kind of thing we
        create mirrors of?

        Basically, images and books.
        """
        return any(
            self.media_type in x for x in
            (MediaTypes.BOOK_MEDIA_TYPES,
             MediaTypes.IMAGE_MEDIA_TYPES)
        )

    def update_image_size(self):
        """Make sure .image_height and .image_width are up to date.

        Clears .image_height and .image_width if the representation
        is not an image.
        """
        if self.media_type and self.media_type.startswith('image/'):
            image = self.as_image()
            if image:
                self.image_width, self.image_height = image.size
                return
        self.image_width = self.image_height = None

    @classmethod
    def normalize_content_path(cls, content_path, base=None):
        if not content_path:
            return None
        base = base or Configuration.data_directory()
        if content_path.startswith(base):
            content_path = content_path[len(base):]
            if content_path.startswith('/'):
                content_path = content_path[1:]
        return content_path

    @property
    def unicode_content(self):
        """Attempt to convert the content into Unicode.

        If all attempts fail, we will return None rather than raise an exception.
        """
        content = None
        for encoding in ('utf-8', 'windows-1252'):
            try:
                content = self.content.decode(encoding)
                break
            except UnicodeDecodeError, e:
                pass
        return content

    def set_fetched_content(self, content, content_path=None):
        """Simulate a successful HTTP request for this representation.

        This is used when the content of the representation is obtained
        through some other means.
        """
        if isinstance(content, unicode):
            content = content.encode("utf8")
        self.content = content

        self.local_content_path = self.normalize_content_path(content_path)
        self.status_code = 200
        self.fetched_at = datetime.datetime.utcnow()
        self.fetch_exception = None
        self.update_image_size()

    def set_as_mirrored(self, mirror_url):
        """Record the fact that the representation has been mirrored
        to the given URL.

        This should only be called upon successful completion of the
        mirror operation.
        """
        self.mirror_url = mirror_url
        self.mirrored_at = datetime.datetime.utcnow()
        self.mirror_exception = None

    @classmethod
    def headers_to_string(cls, d):
        if d is None:
            return None
        return json.dumps(dict(d))

    @classmethod
    def simple_http_get(cls, url, headers, **kwargs):
        """The most simple HTTP-based GET."""
        if not 'allow_redirects' in kwargs:
            kwargs['allow_redirects'] = True
        response = HTTP.get_with_timeout(url, headers=headers, **kwargs)
        return response.status_code, response.headers, response.content

    @classmethod
    def simple_http_post(cls, url, headers, **kwargs):
        """The most simple HTTP-based POST."""
        data = kwargs.get('data')
        if 'data' in kwargs:
            del kwargs['data']
        response = HTTP.post_with_timeout(url, data, headers=headers, **kwargs)
        return response.status_code, response.headers, response.content

    @classmethod
    def http_get_no_timeout(cls, url, headers, **kwargs):
        return Representation.simple_http_get(url, headers, timeout=None, **kwargs)

    @classmethod
    def http_get_no_redirect(cls, url, headers, **kwargs):
        """HTTP-based GET with no redirects."""
        return cls.simple_http_get(url, headers, allow_redirects=False, **kwargs)

    @classmethod
    def browser_http_get(cls, url, headers, **kwargs):
        """GET the representation that would be displayed to a web browser.
        """
        headers = dict(headers)
        headers['User-Agent'] = cls.BROWSER_USER_AGENT
        return cls.simple_http_get(url, headers, **kwargs)

    @classmethod
    def cautious_http_get(cls, url, headers, **kwargs):
        """Examine the URL we're about to GET, possibly going so far as to
        perform a HEAD request, to avoid making a request (or
        following a redirect) to a site known to cause problems.

        The motivating case is that unglue.it contains gutenberg.org
        links that appear to be direct links to EPUBs, but 1) they're
        not direct links to EPUBs, and 2) automated requests to
        gutenberg.org quickly result in IP bans. So we don't make those
        requests.
        """
        do_not_access = kwargs.pop(
            'do_not_access', cls.AVOID_WHEN_CAUTIOUS_DOMAINS
        )
        check_for_redirect = kwargs.pop(
            'check_for_redirect', cls.EXERCISE_CAUTION_DOMAINS
        )
        do_get = kwargs.pop('do_get', cls.simple_http_get)
        head_client = kwargs.pop('cautious_head_client', requests.head)

        if cls.get_would_be_useful(
                url, headers, do_not_access, check_for_redirect,
                head_client
        ):
            # Go ahead and make the GET request.
            return do_get(url, headers, **kwargs)
        else:
            logging.info(
                "Declining to make non-useful HTTP request to %s", url
            )
            # 417 Expectation Failed - "... if the server is a proxy,
            # the server has unambiguous evidence that the request
            # could not be met by the next-hop server."
            #
            # Not quite accurate, but I think it's the closest match
            # to "the HTTP client decided to not even make your
            # request".
            return (
                417,
                {"content-type" :
                 "application/vnd.librarysimplified-did-not-make-request"},
                "Cautiously decided not to make a GET request to %s" % url
            )

    # Sites known to host both free books and redirects to a domain in
    # AVOID_WHEN_CAUTIOUS_DOMAINS.
    EXERCISE_CAUTION_DOMAINS = ['unglue.it']

    # Sites that cause problems for us if we make automated
    # HTTP requests to them while trying to find free books.
    AVOID_WHEN_CAUTIOUS_DOMAINS = ['gutenberg.org', 'books.google.com']

    @classmethod
    def get_would_be_useful(
            cls, url, headers, do_not_access=None, check_for_redirect=None,
            head_client=None
    ):
        """Determine whether making a GET request to a given URL is likely to
        have a useful result.

        :param URL: URL under consideration.
        :param headers: Headers that would be sent with the GET request.
        :param do_not_access: Domains to which GET requests are not useful.
        :param check_for_redirect: Domains to which we should make a HEAD
            request, in case they redirect to a `do_not_access` domain.
        :param head_client: Function for making the HEAD request, if
            one becomes necessary. Should return requests.Response or a mock.
        """
        do_not_access = do_not_access or cls.AVOID_WHEN_CAUTIOUS_DOMAINS
        check_for_redirect = check_for_redirect or cls.EXERCISE_CAUTION_DOMAINS
        head_client = head_client or requests.head

        def has_domain(domain, check_against):
            """Is the given `domain` in `check_against`,
            or maybe a subdomain of one of the domains in `check_against`?
            """
            return any(domain == x or domain.endswith('.' + x)
                       for x in check_against)

        netloc = urlparse.urlparse(url).netloc
        if has_domain(netloc, do_not_access):
            # The link points directly to a domain we don't want to
            # access.
            return False

        if not has_domain(netloc, check_for_redirect):
            # We trust this domain not to redirect to a domain we don't
            # want to access.
            return True

        # We might be fine, or we might get redirected to a domain we
        # don't want to access. Make a HEAD request to see what
        # happens.
        head_response = head_client(url, headers=headers)
        if head_response.status_code / 100 != 3:
            # It's not a redirect. Go ahead and make the GET request.
            return True

        # Yes, it's a redirect. Does it redirect to a
        # domain we don't want to access?
        location = head_response.headers.get('location', '')
        netloc = urlparse.urlparse(location).netloc
        return not has_domain(netloc, do_not_access)

    @property
    def is_image(self):
        return self.media_type and self.media_type.startswith("image/")

    @property
    def local_path(self):
        """Return the full local path to the representation on disk."""
        if not self.local_content_path:
            return None
        return os.path.join(Configuration.data_directory(),
                            self.local_content_path)

    @property
    def clean_media_type(self):
        """The most basic version of this representation's media type.

        No profiles or anything.
        """
        return self._clean_media_type(self.media_type)

    @property
    def url_extension(self):
        """The file extension in this representation's original url."""

        url_path = urlparse.urlparse(self.url).path

        # Known extensions can be followed by a version number (.epub3)
        # or an additional extension (.epub.noimages)
        known_extensions = "|".join(self.FILE_EXTENSIONS.values())
        known_extension_re = re.compile("\.(%s)\d?\.?[\w\d]*$" % known_extensions, re.I)

        known_match = known_extension_re.search(url_path)

        if known_match:
            return known_match.group()

        else:
            any_extension_re = re.compile("\.[\w\d]*$", re.I)

            any_match = any_extension_re.search(url_path)

            if any_match:
                return any_match.group()
        return None

    def extension(self, destination_type=None):
        """Try to come up with a good file extension for this representation."""
        if destination_type:
            return self._extension(destination_type)

        # We'd like to use url_extension because it has some extra
        # features for preserving information present in the original
        # URL. But if we're going to be changing the media type of the
        # resource when mirroring it, the original URL is irrelevant
        # and we need to use an extension associated with the
        # outward-facing media type.
        internal = self.clean_media_type
        external = self._clean_media_type(self.external_media_type)
        if internal != external:
            # External media type overrides any information that might
            # be present in the URL.
            return self._extension(external)

        # If there is information in the URL, use it.
        extension = self.url_extension
        if extension:
            return extension

        # Take a guess based on the internal media type.
        return self._extension(internal)

    @classmethod
    def _clean_media_type(cls, media_type):
        if not media_type:
            return media_type
        if ';' in media_type:
            media_type = media_type[:media_type.index(';')].strip()
        return media_type

    @classmethod
    def _extension(cls, media_type):
        value = cls.FILE_EXTENSIONS.get(media_type, '')
        if not value:
            return value
        return '.' + value

    def default_filename(self, link=None, destination_type=None):
        """Try to come up with a good filename for this representation."""

        scheme, netloc, path, query, fragment = urlparse.urlsplit(self.url)
        path_parts = path.split("/")
        filename = None
        if path_parts:
            filename = path_parts[-1]

        if not filename and link:
            filename = link.default_filename
        if not filename:
            # This is the absolute last-ditch filename solution, and
            # it's basically only used when we try to mirror the root
            # URL of a domain.
            filename = 'resource'

        default_extension = self.extension()
        extension = self.extension(destination_type)
        if default_extension and default_extension != extension and filename.endswith(default_extension):
            filename = filename[:-len(default_extension)] + extension
        elif extension and not filename.endswith(extension):
            filename += extension
        return filename

    @property
    def external_media_type(self):
        return self.media_type

    def external_content(self):
        """Return a filehandle to the representation's contents, as they
        should be mirrored externally, and the media type to be used
        when mirroring.
        """
        return self.content_fh()

    def content_fh(self):
        """Return an open filehandle to the representation's contents.

        This works whether the representation is kept in the database
        or in a file on disk.
        """
        if self.content:
            return StringIO(self.content)
        elif self.local_path:
            if not os.path.exists(self.local_path):
                raise ValueError("%s does not exist." % self.local_path)
            return open(self.local_path)
        return None

    def as_image(self):
        """Load this Representation's contents as a PIL image."""
        if not self.is_image:
            raise ValueError(
                "Cannot load non-image representation as image: type %s."
                % self.media_type)
        if not self.content and not self.local_path:
            raise ValueError("Image representation has no content.")

        fh = self.content_fh()
        if not fh or self.clean_media_type == self.SVG_MEDIA_TYPE:
            return None
        return Image.open(fh)

    pil_format_for_media_type = {
        "image/gif": "gif",
        "image/png": "png",
        "image/jpeg": "jpeg",
    }

    def scale(self, max_height, max_width,
              destination_url, destination_media_type, force=False):
        """Return a Representation that's a scaled-down version of this
        Representation, creating it if necessary.

        :param destination_url: The URL the scaled-down resource will
        (eventually) be uploaded to.

        :return: A 2-tuple (Representation, is_new)

        """
        _db = Session.object_session(self)

        if not destination_media_type in self.pil_format_for_media_type:
            raise ValueError("Unsupported destination media type: %s" % destination_media_type)

        pil_format = self.pil_format_for_media_type[destination_media_type]

        # Make sure we actually have an image to scale.
        image = None
        try:
            image = self.as_image()
        except Exception, e:
            self.scale_exception = traceback.format_exc()
            self.scaled_at = None
            # This most likely indicates an error during the fetch
            # phrase.
            self.fetch_exception = "Error found while scaling: %s" % (
                self.scale_exception)
            logging.error("Error found while scaling %r", self, exc_info=e)

        if not image:
            return self, False

        # Now that we've loaded the image, take the opportunity to set
        # the image size of the original representation.
        self.image_width, self.image_height = image.size

        # If the image is already a thumbnail-size bitmap, don't bother.
        if (self.clean_media_type != MediaTypes.SVG_MEDIA_TYPE
            and self.image_height <= max_height
            and self.image_width <= max_width):
            self.thumbnails = []
            return self, False

        # Do we already have a representation for the given URL?
        thumbnail, is_new = get_one_or_create(
            _db, Representation, url=destination_url,
            media_type=destination_media_type
        )
        if thumbnail not in self.thumbnails:
            thumbnail.thumbnail_of = self

        if not is_new and not force:
            # We found a preexisting thumbnail and we're allowed to
            # use it.
            return thumbnail, is_new

        # At this point we have a parent Representation (self), we
        # have a Representation that will contain a thumbnail
        # (thumbnail), and we know we need to actually thumbnail the
        # parent into the thumbnail.
        #
        # Because the representation of this image is being
        # changed, it will need to be mirrored later on.
        now = datetime.datetime.utcnow()
        thumbnail.mirrored_at = None
        thumbnail.mirror_exception = None

        args = [(max_width, max_height),
                Image.ANTIALIAS]
        try:
            image.thumbnail(*args)
        except IOError, e:
            # I'm not sure why, but sometimes just trying
            # it again works.
            original_exception = traceback.format_exc()
            try:
                image.thumbnail(*args)
            except IOError, e:
                self.scale_exception = original_exception
                self.scaled_at = None
                return self, False

        # Save the thumbnail image to the database under
        # thumbnail.content.
        output = StringIO()
        if image.mode != 'RGB':
            image = image.convert('RGB')
        try:
            image.save(output, pil_format)
        except Exception, e:
            self.scale_exception = traceback.format_exc()
            self.scaled_at = None
            # This most likely indicates a problem during the fetch phase,
            # Set fetch_exception so we'll retry the fetch.
            self.fetch_exception = "Error found while scaling: %s" % (self.scale_exception)
            return self, False
        thumbnail.content = output.getvalue()
        thumbnail.image_width, thumbnail.image_height = image.size
        output.close()
        thumbnail.scale_exception = None
        thumbnail.scaled_at = now
        return thumbnail, True

    @property
    def thumbnail_size_quality_penalty(self):
        return self._thumbnail_size_quality_penalty(
            self.image_width, self.image_height
        )

    @classmethod
    def _thumbnail_size_quality_penalty(cls, width, height):
        """Measure a cover image's deviation from the ideal aspect ratio, and
        by its deviation (in the "too small" direction only) from the
        ideal thumbnail resolution.
        """

        quotient = 1

        if not width or not height:
            # In the absence of any information, assume the cover is
            # just dandy.
            #
            # This is obviously less than ideal, but this code is used
            # pretty rarely now that we no longer have hundreds of
            # covers competing for the privilege of representing a
            # public domain book, so I'm not too concerned about it.
            #
            # Look at it this way: this escape hatch only causes a
            # problem if we compare an image whose size we know
            # against an image whose size we don't know.
            #
            # In the circulation manager, we never know what size an
            # image is, and we must always trust that the cover
            # (e.g. Overdrive and the metadata wrangler) give us
            # "thumbnail" images that are approximately the right
            # size. So we always use this escape hatch.
            #
            # In the metadata wrangler and content server, we always
            # have access to the covers themselves, so we always have
            # size information and we never use this escape hatch.
            return quotient

        # Penalize an image for deviation from the ideal aspect ratio.
        aspect_ratio = width / float(height)
        ideal = Identifier.IDEAL_COVER_ASPECT_RATIO
        if aspect_ratio > ideal:
            deviation = ideal / aspect_ratio
        else:
            deviation = aspect_ratio/ideal
        if deviation != 1:
            quotient *= deviation

        # Penalize an image for not being wide enough.
        width_shortfall = (
            float(width - Identifier.IDEAL_IMAGE_WIDTH) / Identifier.IDEAL_IMAGE_WIDTH)
        if width_shortfall < 0:
            quotient *= (1+width_shortfall)

        # Penalize an image for not being tall enough.
        height_shortfall = (
            float(height - Identifier.IDEAL_IMAGE_HEIGHT) / Identifier.IDEAL_IMAGE_HEIGHT)
        if height_shortfall < 0:
            quotient *= (1+height_shortfall)
        return quotient

    @property
    def best_thumbnail(self):
        """Find the best thumbnail among all the thumbnails associated with
        this Representation.

        Basically, we prefer a thumbnail that has been mirrored.
        """
        champion = None
        for thumbnail in self.thumbnails:
            if thumbnail.mirror_url:
                champion = thumbnail
                break
            elif not champion:
                champion = thumbnail
        return champion

class DeliveryMechanism(Base, HasFullTableCache):
    """A technique for delivering a book to a patron.

    There are two parts to this: a DRM scheme and a content
    type. Either may be identified with a MIME media type
    (e.g. "application/vnd.adobe.adept+xml" or "application/epub+zip") or an
    informal name ("Kindle via Amazon").
    """
    KINDLE_CONTENT_TYPE = u"Kindle via Amazon"
    NOOK_CONTENT_TYPE = u"Nook via B&N"
    STREAMING_TEXT_CONTENT_TYPE = u"Streaming Text"
    STREAMING_AUDIO_CONTENT_TYPE = u"Streaming Audio"
    STREAMING_VIDEO_CONTENT_TYPE = u"Streaming Video"

    NO_DRM = None
    ADOBE_DRM = u"application/vnd.adobe.adept+xml"
    FINDAWAY_DRM = u"application/vnd.librarysimplified.findaway.license+json"
    KINDLE_DRM = u"Kindle DRM"
    NOOK_DRM = u"Nook DRM"
    STREAMING_DRM = u"Streaming"
    OVERDRIVE_DRM = u"Overdrive DRM"
    BEARER_TOKEN = u"application/vnd.librarysimplified.bearer-token+json"

    STREAMING_PROFILE = ";profile=http://librarysimplified.org/terms/profiles/streaming-media"
    MEDIA_TYPES_FOR_STREAMING = {
        STREAMING_TEXT_CONTENT_TYPE: MediaTypes.TEXT_HTML_MEDIA_TYPE
    }

    __tablename__ = 'deliverymechanisms'
    id = Column(Integer, primary_key=True)
    content_type = Column(String, nullable=False)
    drm_scheme = Column(String)

    # Can the Library Simplified client fulfill a book with this
    # content type and this DRM scheme?
    default_client_can_fulfill = Column(Boolean, default=False, index=True)

    # These are the media type/DRM scheme combos known to be supported
    # by the default Library Simplified client.
    default_client_can_fulfill_lookup = set([
        (MediaTypes.EPUB_MEDIA_TYPE, NO_DRM),
        (MediaTypes.EPUB_MEDIA_TYPE, ADOBE_DRM),
        (MediaTypes.EPUB_MEDIA_TYPE, BEARER_TOKEN),
    ])

    license_pool_delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism",
        backref="delivery_mechanism",
    )

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    @property
    def name(self):
        if self.drm_scheme is self.NO_DRM:
            drm_scheme = "DRM-free"
        else:
            drm_scheme = self.drm_scheme
        return "%s (%s)" % (self.content_type, drm_scheme)

    def cache_key(self):
        return (self.content_type, self.drm_scheme)

    def __repr__(self):

        if self.default_client_can_fulfill:
            fulfillable = "fulfillable"
        else:
            fulfillable = "not fulfillable"

        return "<Delivery mechanism: %s, %s)>" % (
            self.name, fulfillable
        )

    @classmethod
    def lookup(cls, _db, content_type, drm_scheme):
        def lookup_hook():
            return get_one_or_create(
                _db, DeliveryMechanism, content_type=content_type,
                drm_scheme=drm_scheme
            )
        return cls.by_cache_key(_db, (content_type, drm_scheme), lookup_hook)

    @property
    def implicit_medium(self):
        """What would be a good setting for Edition.MEDIUM for an edition
        available through this DeliveryMechanism?
        """
        if self.content_type in (
                MediaTypes.EPUB_MEDIA_TYPE,
                MediaTypes.PDF_MEDIA_TYPE,
                "Kindle via Amazon",
                "Streaming Text"):
            return Edition.BOOK_MEDIUM
        elif self.content_type in (
                "Streaming Video" or self.content_type.startswith('video/')
        ):
            return Edition.VIDEO_MEDIUM
        else:
            return None

    @classmethod
    def is_media_type(cls, x):
        "Does this string look like a media type?"
        if x is None:
            return False

        return any(x.startswith(prefix) for prefix in
                   ['vnd.', 'application', 'text', 'video', 'audio', 'image'])

    @property
    def is_streaming(self):
        return self.content_type in self.MEDIA_TYPES_FOR_STREAMING.keys()

    @property
    def drm_scheme_media_type(self):
        """Return the media type for this delivery mechanism's
        DRM scheme, assuming it's represented that way.
        """
        if self.is_media_type(self.drm_scheme):
            return self.drm_scheme
        return None

    @property
    def content_type_media_type(self):
        """Return the media type for this delivery mechanism's
        content type, assuming it's represented as a media type.
        """
        if self.is_media_type(self.content_type):
            return self.content_type

        media_type_for_streaming = self.MEDIA_TYPES_FOR_STREAMING.get(self.content_type)
        if media_type_for_streaming:
            return media_type_for_streaming + self.STREAMING_PROFILE

        return None

    def compatible_with(self, other, open_access_rules=False):
        """Can a single loan be fulfilled with both this delivery mechanism
        and the given one?

        :param other: A DeliveryMechanism

        :param open_access: If this is True, the rules for open-access
            fulfillment will be applied. If not, the stricted rules
            for commercial fulfillment will be applied.
        """
        if not isinstance(other, DeliveryMechanism):
            return False

        if self.id == other.id:
            # The two DeliveryMechanisms are the same.
            return True

        # Streaming delivery mechanisms can be used even when a
        # license pool is locked into a non-streaming delivery
        # mechanism.
        if self.is_streaming or other.is_streaming:
            return True

        # For an open-access book, loans are not locked to delivery
        # mechanisms, so as long as neither delivery mechanism has
        # DRM, they're compatible.
        if (open_access_rules and self.drm_scheme==self.NO_DRM
            and other.drm_scheme==self.NO_DRM):
            return True

        # For non-open-access books, locking a license pool to a
        # non-streaming delivery mechanism prohibits the use of any
        # other non-streaming delivery mechanism.
        return False

Index("ix_deliverymechanisms_drm_scheme_content_type",
      DeliveryMechanism.drm_scheme,
      DeliveryMechanism.content_type,
      unique=True)

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

class Collection(Base, HasFullTableCache):

    """A Collection is a set of LicensePools obtained through some mechanism.
    """

    __tablename__ = 'collections'
    id = Column(Integer, primary_key=True)

    name = Column(Unicode, unique=True, nullable=False, index=True)

    DATA_SOURCE_NAME_SETTING = u'data_source'

    # For use in forms that edit Collections.
    EXTERNAL_ACCOUNT_ID_KEY = u'external_account_id'

    # How does the provider of this collection distinguish it from
    # other collections it provides? On the other side this is usually
    # called a "library ID".
    external_account_id = Column(Unicode, nullable=True)

    # How do we connect to the provider of this collection? Any url,
    # authentication information, or additional configuration goes
    # into the external integration, as does the 'protocol', which
    # designates the integration technique we will use to actually get
    # the metadata and licenses. Each Collection has a distinct
    # ExternalIntegration.
    external_integration_id = Column(
        Integer, ForeignKey('externalintegrations.id'), unique=True, index=True)

    # A Collection may specialize some other Collection. For instance,
    # an Overdrive Advantage collection is a specialization of an
    # ordinary Overdrive collection. It uses the same access key and
    # secret as the Overdrive collection, but it has a distinct
    # external_account_id.
    parent_id = Column(Integer, ForeignKey('collections.id'), index=True)

    # Some Collections use an ExternalIntegration to mirror books and
    # cover images they discover. Such a collection should use an
    # ExternalIntegration to set up its mirroring technique, and keep
    # a reference to that ExternalIntegration here.
    mirror_integration_id = Column(
        Integer, ForeignKey('externalintegrations.id'), nullable=True
    )

    # A collection may have many child collections. For example,
    # An Overdrive collection may have many children corresponding
    # to Overdrive Advantage collections.
    children = relationship(
        "Collection", backref=backref("parent", remote_side = [id]),
        uselist=True
    )

    # A Collection can provide books to many Libraries.
    libraries = relationship(
        "Library", secondary=lambda: collections_libraries,
        backref="collections"
    )

    # A Collection can include many LicensePools.
    licensepools = relationship(
        "LicensePool", backref="collection",
        cascade="all, delete-orphan"
    )

    # A Collection can be monitored by many Monitors, each of which
    # will have its own Timestamp.
    timestamps = relationship("Timestamp", backref="collection")

    catalog = relationship(
        "Identifier", secondary=lambda: collections_identifiers,
        backref="collections"
    )

    # A Collection can be associated with multiple CoverageRecords
    # for Identifiers in its catalog.
    coverage_records = relationship(
        "CoverageRecord", backref="collection",
        cascade="all"
    )

    # A collection may be associated with one or more custom lists.
    # When a new license pool is added to the collection, it will
    # also be added to the list. Admins can remove items from the
    # the list and they won't be added back, so the list doesn't
    # necessarily match the collection.
    customlists = relationship(
        "CustomList", secondary=lambda: collections_customlists,
        backref="collections"
    )

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    def __repr__(self):
        return (u'<Collection "%s"/"%s" ID=%d>' %
                (self.name, self.protocol, self.id)).encode('utf8')

    def cache_key(self):
        return (self.name, self.external_integration.protocol)

    @classmethod
    def by_name_and_protocol(cls, _db, name, protocol):
        """Find or create a Collection with the given name and the given
        protocol.

        This method uses the full-table cache if possible.

        :return: A 2-tuple (collection, is_new)
        """
        key = (name, protocol)
        def lookup_hook():
            return cls._by_name_and_protocol(_db, key)
        return cls.by_cache_key(_db, key, lookup_hook)

    @classmethod
    def _by_name_and_protocol(cls, _db, cache_key):
        """Find or create a Collection with the given name and the given
        protocol.

        We can't use get_one_or_create because the protocol is kept in
        a separate database object, (an ExternalIntegration).

        :return: A 2-tuple (collection, is_new)
        """
        name, protocol = cache_key

        qu = cls.by_protocol(_db, protocol)
        qu = qu.filter(Collection.name==name)
        try:
            collection = qu.one()
            is_new = False
        except NoResultFound, e:
            # Make a new Collection.
            collection, is_new = get_one_or_create(_db, Collection, name=name)
            if not is_new and collection.protocol != protocol:
                # The collection already exists, it just uses a different
                # protocol than the one we asked about.
                raise ValueError(
                    'Collection "%s" does not use protocol "%s".' % (
                        name, protocol
                    )
                )
            integration = collection.create_external_integration(
                protocol=protocol
            )
            collection.external_integration.protocol=protocol
        return collection, is_new

    @classmethod
    def by_protocol(cls, _db, protocol):
        """Query collections that get their licenses through the given protocol.

        :param protocol: Protocol to use. If this is None, all
        Collections will be returned.
        """
        qu = _db.query(Collection)
        if protocol:
            qu = qu.join(
            ExternalIntegration,
            ExternalIntegration.id==Collection.external_integration_id).filter(
                ExternalIntegration.goal==ExternalIntegration.LICENSE_GOAL
            ).filter(ExternalIntegration.protocol==protocol)
        return qu

    @classmethod
    def by_datasource(cls, _db, data_source):
        """Query collections that are associated with the given DataSource."""
        if isinstance(data_source, DataSource):
            data_source = data_source.name

        qu = _db.query(cls).join(ExternalIntegration,
                cls.external_integration_id==ExternalIntegration.id)\
            .join(ExternalIntegration.settings)\
            .filter(ConfigurationSetting.key==Collection.DATA_SOURCE_NAME_SETTING)\
            .filter(ConfigurationSetting.value==data_source)
        return qu

    @hybrid_property
    def protocol(self):
        """What protocol do we need to use to get licenses for this
        collection?
        """
        return self.external_integration.protocol

    @protocol.setter
    def protocol(self, new_protocol):
        """Modify the protocol in use by this Collection."""
        if self.parent and self.parent.protocol != new_protocol:
            raise ValueError(
                "Proposed new protocol (%s) contradicts parent collection's protocol (%s)." % (
                    new_protocol, self.parent.protocol
                )
            )
        self.external_integration.protocol = new_protocol
        for child in self.children:
            child.protocol = new_protocol

    # For collections that can control the duration of the loans they
    # create, the durations are stored in these settings and new loans are
    # expected to be created using these settings. For collections
    # where loan duration is negotiated out-of-bounds, all loans are
    # _assumed_ to have these durations unless we hear otherwise from
    # the server.
    AUDIOBOOK_LOAN_DURATION_KEY = 'audio_loan_duration'
    EBOOK_LOAN_DURATION_KEY = 'ebook_loan_duration'
    STANDARD_DEFAULT_LOAN_PERIOD = 21

    def default_loan_period(self, library, medium=Edition.BOOK_MEDIUM):
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        return self.default_loan_period_setting(
            library, medium).int_value or self.STANDARD_DEFAULT_LOAN_PERIOD

    def default_loan_period_setting(self, library, medium=Edition.BOOK_MEDIUM):
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        _db = Session.object_session(library)
        if medium == Edition.AUDIO_MEDIUM:
            key = self.AUDIOBOOK_LOAN_DURATION_KEY
        else:
            key = self.EBOOK_LOAN_DURATION_KEY
        if isinstance(library, Library):
            return (
                ConfigurationSetting.for_library_and_externalintegration(
                    _db, key, library, self.external_integration
                )
            )
        elif isinstance(library, IntegrationClient):
            return self.external_integration.setting(key)


    DEFAULT_RESERVATION_PERIOD_KEY = 'default_reservation_period'
    STANDARD_DEFAULT_RESERVATION_PERIOD = 3

    @hybrid_property
    def default_reservation_period(self):
        """Until we hear otherwise from the license provider, we assume
        that someone who puts an item on hold has this many days to
        check it out before it goes to the next person in line.
        """
        return (
            self.external_integration.setting(
                self.DEFAULT_RESERVATION_PERIOD_KEY,
            ).int_value or self.STANDARD_DEFAULT_RESERVATION_PERIOD
        )

    @default_reservation_period.setter
    def default_reservation_period(self, new_value):
        new_value = int(new_value)
        self.external_integration.setting(
            self.DEFAULT_RESERVATION_PERIOD_KEY).value = str(new_value)

    def create_external_integration(self, protocol):
        """Create an ExternalIntegration for this Collection.

        To be used immediately after creating a new Collection,
        e.g. in by_name_and_protocol, from_metadata_identifier, and
        various test methods that create mock Collections.

        If an external integration already exists, return it instead
        of creating another one.

        :param protocol: The protocol known to be in use when getting
        licenses for this collection.
        """
        _db = Session.object_session(self)
        goal = ExternalIntegration.LICENSE_GOAL
        external_integration, is_new = get_one_or_create(
            _db, ExternalIntegration, id=self.external_integration_id,
            create_method_kwargs=dict(protocol=protocol, goal=goal)
        )
        if external_integration.protocol != protocol:
            raise ValueError(
                "Located ExternalIntegration, but its protocol (%s) does not match desired protocol (%s)." % (
                    external_integration.protocol, protocol
                )
            )
        self.external_integration_id = external_integration.id
        return external_integration

    @property
    def external_integration(self):
        """Find the external integration for this Collection, assuming
        it already exists.

        This is generally a safe assumption since by_name_and_protocol and
        from_metadata_identifier both create ExternalIntegrations for the
        Collections they create.
        """
        # We don't enforce this on the database level because it is
        # legitimate for a newly created Collection to have no
        # ExternalIntegration. But by the time it's being used for real,
        # it needs to have one.
        if not self.external_integration_id:
            raise ValueError(
                "No known external integration for collection %s" % self.name
            )
        return self._external_integration

    @property
    def unique_account_id(self):
        """Identifier that uniquely represents this Collection of works"""
        unique_account_id = self.external_account_id

        if not unique_account_id:
            raise ValueError("Unique account identifier not set")

        if self.parent:
            return self.parent.unique_account_id + '+' + unique_account_id
        return unique_account_id

    @hybrid_property
    def data_source(self):
        """Find the data source associated with this Collection.

        Bibliographic metadata obtained through the collection
        protocol is recorded as coming from this data source. A
        LicensePool inserted into this collection will be associated
        with this data source, unless its bibliographic metadata
        indicates some other data source.

        For most Collections, the integration protocol sets the data
        source.  For collections that use the OPDS import protocol,
        the data source is a Collection-specific setting.
        """
        data_source = None
        name = ExternalIntegration.DATA_SOURCE_FOR_LICENSE_PROTOCOL.get(
            self.protocol
        )
        if not name:
            name = self.external_integration.setting(
                Collection.DATA_SOURCE_NAME_SETTING
            ).value
        _db = Session.object_session(self)
        if name:
            data_source = DataSource.lookup(_db, name, autocreate=True)
        return data_source

    @data_source.setter
    def data_source(self, new_value):
        if isinstance(new_value, DataSource):
            new_value = new_value.name
        if self.protocol == new_value:
            return

        # Only set a DataSource for Collections that don't have an
        # implied source.
        if self.protocol not in ExternalIntegration.DATA_SOURCE_FOR_LICENSE_PROTOCOL:
            setting = self.external_integration.setting(
                Collection.DATA_SOURCE_NAME_SETTING
            )
            if new_value is not None:
                new_value = unicode(new_value)
            setting.value = new_value

    @property
    def parents(self):
        if self.parent_id:
            _db = Session.object_session(self)
            parent = Collection.by_id(_db, self.parent_id)
            yield parent
            for collection in parent.parents:
                yield collection

    @property
    def metadata_identifier(self):
        """Identifier based on collection details that uniquely represents
        this Collection on the metadata wrangler. This identifier is
        composed of the Collection protocol and account identifier.

        In the metadata wrangler, this identifier is used as the unique
        name of the collection.
        """
        def encode(detail):
            return base64.urlsafe_b64encode(detail.encode('utf-8'))

        account_id = self.unique_account_id
        if self.protocol == ExternalIntegration.OPDS_IMPORT:
            # Remove ending / from OPDS url that could duplicate the collection
            # on the Metadata Wrangler.
            while account_id.endswith('/'):
                account_id = account_id[:-1]

        account_id = encode(account_id)
        protocol = encode(self.protocol)

        metadata_identifier = protocol + ':' + account_id
        return encode(metadata_identifier)

    @classmethod
    def from_metadata_identifier(cls, _db, metadata_identifier, data_source=None):
        """Finds or creates a Collection on the metadata wrangler, based
        on its unique metadata_identifier
        """
        collection = get_one(_db, Collection, name=metadata_identifier)
        is_new = False

        opds_collection_without_url = (
            collection and collection.protocol==ExternalIntegration.OPDS_IMPORT
            and not collection.external_account_id
        )

        if not collection or opds_collection_without_url:
            def decode(detail):
                return base64.urlsafe_b64decode(detail.encode('utf-8'))

            details = decode(metadata_identifier)
            encoded_details  = details.split(':', 1)
            [protocol, account_id] = [decode(d) for d in encoded_details]

            if not collection:
                collection, is_new = create(
                    _db, Collection, name=metadata_identifier
                )
                collection.create_external_integration(protocol)

            if protocol == ExternalIntegration.OPDS_IMPORT:
                # Share the feed URL so the Metadata Wrangler can find it.
               collection.external_account_id = unicode(account_id)

        if data_source:
            collection.data_source = data_source

        return collection, is_new

    def explain(self, include_secrets=False):
        """Create a series of human-readable strings to explain a collection's
        settings.

        :param include_secrets: For security reasons,
           sensitive settings such as passwords are not displayed by default.

        :return: A list of explanatory strings.
        """
        lines = []
        if self.name:
            lines.append('Name: "%s"' % self.name)
        if self.parent:
            lines.append('Parent: %s' % self.parent.name)
        integration = self.external_integration
        if integration.protocol:
            lines.append('Protocol: "%s"' % integration.protocol)
        for library in self.libraries:
            lines.append('Used by library: "%s"' % library.short_name)
        if self.external_account_id:
            lines.append('External account ID: "%s"' % self.external_account_id)
        for setting in sorted(integration.settings, key=lambda x: x.key):
            if (include_secrets or not setting.is_secret) and setting.value is not None:
                lines.append('Setting "%s": "%s"' % (setting.key, setting.value))
        return lines

    def catalog_identifier(self, identifier):
        """Inserts an identifier into a catalog"""
        self.catalog_identifiers([identifier])

    def catalog_identifiers(self, identifiers):
        """Inserts identifiers into the catalog"""
        if not identifiers:
            # Nothing to do.
            return

        _db = Session.object_session(identifiers[0])
        already_in_catalog = _db.query(Identifier).join(
            CollectionIdentifier
        ).filter(
            CollectionIdentifier.collection_id==self.id
        ).filter(
             Identifier.id.in_([x.id for x in identifiers])
        ).all()

        new_catalog_entries = [
            dict(collection_id=self.id, identifier_id=identifier.id)
            for identifier in identifiers
            if identifier not in already_in_catalog
        ]
        _db.bulk_insert_mappings(CollectionIdentifier, new_catalog_entries)
        _db.commit()

    def unresolved_catalog(self, _db, data_source_name, operation):
        """Returns a query with all identifiers in a Collection's catalog that
        have unsuccessfully attempted resolution. This method is used on the
        metadata wrangler.

        :return: a sqlalchemy.Query
        """
        coverage_source = DataSource.lookup(_db, data_source_name)
        is_not_resolved = and_(
            CoverageRecord.operation==operation,
            CoverageRecord.data_source_id==coverage_source.id,
            CoverageRecord.status!=CoverageRecord.SUCCESS,
        )

        query = _db.query(Identifier)\
            .outerjoin(Identifier.licensed_through)\
            .outerjoin(Identifier.coverage_records)\
            .outerjoin(LicensePool.work).outerjoin(Identifier.collections)\
            .filter(
                Collection.id==self.id, is_not_resolved, Work.id==None
            ).order_by(Identifier.id)

        return query

    def works_updated_since(self, _db, timestamp):
        """Finds all works in a collection's catalog that have been updated
           since the timestamp. Used in the metadata wrangler.

           :return: a Query that yields (Work, LicensePool,
              Identifier) 3-tuples. This gives caller all the
              information necessary to create full OPDS entries for
              the works.
        """
        opds_operation = WorkCoverageRecord.GENERATE_OPDS_OPERATION
        qu = _db.query(
            Work, LicensePool, Identifier
        ).join(
            Work.coverage_records,
        ).join(
            Identifier.collections,
        )
        qu = qu.filter(
            Work.id==WorkCoverageRecord.work_id,
            Work.id==LicensePool.work_id,
            LicensePool.identifier_id==Identifier.id,
            WorkCoverageRecord.operation==opds_operation,
            CollectionIdentifier.identifier_id==Identifier.id,
            CollectionIdentifier.collection_id==self.id
        ).options(joinedload(Work.license_pools, LicensePool.identifier))

        if timestamp:
            qu = qu.filter(
                WorkCoverageRecord.timestamp > timestamp
            )

        qu = qu.order_by(WorkCoverageRecord.timestamp)
        return qu

    def isbns_updated_since(self, _db, timestamp):
        """Finds all ISBNs in a collection's catalog that have been updated
           since the timestamp but don't have a Work to show for it. Used in
           the metadata wrangler.

           :return: a Query
        """
        isbns = _db.query(Identifier, func.max(CoverageRecord.timestamp).label('latest'))\
            .join(Identifier.collections)\
            .join(Identifier.coverage_records)\
            .outerjoin(Identifier.licensed_through)\
            .group_by(Identifier.id).order_by('latest')\
            .filter(
                Collection.id==self.id,
                LicensePool.work_id==None,
                CoverageRecord.status==CoverageRecord.SUCCESS,
            ).enable_eagerloads(False).options(joinedload(Identifier.coverage_records))

        if timestamp:
            isbns = isbns.filter(CoverageRecord.timestamp > timestamp)

        return isbns

    @classmethod
    def restrict_to_ready_deliverable_works(
        cls, query, work_model, collection_ids=None, show_suppressed=False,
        allow_holds=True,
    ):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool.

        :param query: The query to restrict.

        :param work_model: Either Work or one of the MaterializedWork
        materialized view classes.

        :param show_suppressed: Include titles that have nothing but
        suppressed LicensePools.

        :param collection_ids: Only include titles in the given
        collections.

        :param allow_holds: If false, pools with no available copies
        will be hidden.
        """
        # Only find presentation-ready works.
        #
        # Such works are automatically filtered out of
        # the materialized view, but we need to filter them out of Work.
        if work_model == Work:
            query = query.filter(
                work_model.presentation_ready == True,
            )

        # Only find books that have some kind of DeliveryMechanism.
        LPDM = LicensePoolDeliveryMechanism
        exists_clause = exists().where(
            and_(LicensePool.data_source_id==LPDM.data_source_id,
                LicensePool.identifier_id==LPDM.identifier_id)
        )
        query = query.filter(exists_clause)

        # Only find books with unsuppressed LicensePools.
        if not show_suppressed:
            query = query.filter(LicensePool.suppressed==False)

        # Only find books with available licenses.
        query = query.filter(
                or_(LicensePool.licenses_owned > 0, LicensePool.open_access)
        )

        # Only find books in an appropriate collection.
        query = query.filter(
            LicensePool.collection_id.in_(collection_ids)
        )

        # If we don't allow holds, hide any books with no available copies.
        if not allow_holds:
            query = query.filter(
                or_(LicensePool.licenses_available > 0, LicensePool.open_access)
            )
        return query


collections_libraries = Table(
    'collections_libraries', Base.metadata,
     Column(
         'collection_id', Integer, ForeignKey('collections.id'),
         index=True, nullable=False
     ),
     Column(
         'library_id', Integer, ForeignKey('libraries.id'),
         index=True, nullable=False
     ),
     UniqueConstraint('collection_id', 'library_id'),
 )


collections_identifiers = Table(
    'collections_identifiers', Base.metadata,
    Column(
        'collection_id', Integer, ForeignKey('collections.id'),
        index=True, nullable=False
    ),
    Column(
        'identifier_id', Integer, ForeignKey('identifiers.id'),
        index=True, nullable=False
    ),
    UniqueConstraint('collection_id', 'identifier_id'),
)

# Create an ORM model for the collections_identifiers join table
# so it can be used in a bulk_insert_mappings call.
class CollectionIdentifier(object):
    pass

mapper(
    CollectionIdentifier, collections_identifiers,
    primary_key=(
        collections_identifiers.columns.collection_id,
        collections_identifiers.columns.identifier_id
    )
)

collections_customlists = Table(
    'collections_customlists', Base.metadata,
    Column(
        'collection_id', Integer, ForeignKey('collections.id'),
        index=True, nullable=False,
    ),
    Column(
        'customlist_id', Integer, ForeignKey('customlists.id'),
        index=True, nullable=False,
    ),
    UniqueConstraint('collection_id', 'customlist_id'),
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
from configuration import (
    ExternalIntegration,
    ConfigurationSetting,
    Admin,
    AdminRole,
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
