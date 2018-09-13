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
from hyperlink_constants import HyperlinkConstants
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

    def default_loan_period(self, library, medium=EditionConstants.BOOK_MEDIUM):
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        return self.default_loan_period_setting(
            library, medium).int_value or self.STANDARD_DEFAULT_LOAN_PERIOD

    def default_loan_period_setting(self, library, medium=EditionConstants.BOOK_MEDIUM):
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        _db = Session.object_session(library)
        if medium == EditionConstants.AUDIO_MEDIUM:
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
