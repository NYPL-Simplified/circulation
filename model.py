# encoding: utf-8
from cStringIO import StringIO
from collections import (
    Counter,
    defaultdict,
)
from lxml import etree
from nose.tools import set_trace
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
import time
import traceback
import urllib
import uuid
import warnings

from PIL import (
    Image,
)

from psycopg2.extras import NumericRange
from sqlalchemy.engine.url import URL
from sqlalchemy import exc as sa_exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    backref,
    defer,
    relationship,
)
from sqlalchemy import (
    or_,
    MetaData,
)
from sqlalchemy.orm import (
    aliased,
    backref,
    defer,
    contains_eager,
    joinedload,
    lazyload,
)
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
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import (
    cast,
    and_,
    or_,
)
from sqlalchemy.exc import (
    IntegrityError
)
from sqlalchemy import (
    create_engine, 
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
from config import Configuration
from external_search import ExternalSearchIndex
import classifier
from classifier import (
    Classifier,
    Erotica,
    COMICS_AND_GRAPHIC_NOVELS,
    GenreData,
    WorkClassifier,
)
from util import (
    LanguageCodes,
    MetadataSimilarity,
    TitleProcessor,
)
from util.permanent_work_id import WorkIDCalculator
from util.summary import SummaryEvaluator

from sqlalchemy.orm.session import Session

from sqlalchemy.dialects.postgresql import (
    ARRAY,
    HSTORE,
    JSON,
    INT4RANGE,
)
from sqlalchemy.orm import sessionmaker
from s3 import S3Uploader

DEBUG = False

def production_session():
    url = Configuration.database_url()
    if url.startswith('"'):
        url = url[1:]
    logging.debug("Database url: %s", url)
    return SessionManager.session(url)

class PolicyException(Exception):
    pass

class BaseMaterializedWork(object):
    """A mixin class for materialized views that incorporate Work and Edition."""
    pass


class SessionManager(object):

    # Materialized views need to be created and indexed from SQL
    # commands kept in files. This dictionary maps the views to the
    # SQL files.

    MATERIALIZED_VIEW_WORKS = 'mv_works_editions_datasources_identifiers'
    MATERIALIZED_VIEW_WORKS_WORKGENRES = 'mv_works_editions_workgenres_datasources_identifiers'
    MATERIALIZED_VIEWS = {
        MATERIALIZED_VIEW_WORKS : 'materialized_view_works.sql',
        MATERIALIZED_VIEW_WORKS_WORKGENRES : 'materialized_view_works_workgenres.sql',
    }


    engine_for_url = {}

    @classmethod
    def engine(cls, url=None):
        url = url or Configuration.database_url()
        return create_engine(url, echo=DEBUG)

    @classmethod
    def initialize(cls, url):
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
            connection.execute(sql)                
        if connection:
            connection.close()

        class MaterializedWorkWithGenre(Base, BaseMaterializedWork):
            __table__ = Table(
                cls.MATERIALIZED_VIEW_WORKS_WORKGENRES, 
                Base.metadata, 
                Column('works_id', Integer, primary_key=True),
                Column('workgenres_id', Integer, primary_key=True),
                Column('license_pool_id', Integer, ForeignKey('licensepools.id')),
                autoload=True,
                autoload_with=engine
            )
            license_pool = relationship(
                LicensePool, 
                primaryjoin="LicensePool.id==MaterializedWorkWithGenre.license_pool_id",
                foreign_keys=LicensePool.id, lazy='joined', uselist=False)

        class MaterializedWork(Base, BaseMaterializedWork):
            __table__ = Table(
                cls.MATERIALIZED_VIEW_WORKS, 
                Base.metadata, 
                Column('works_id', Integer, primary_key=True),
                Column('license_pool_id', Integer, ForeignKey('licensepools.id')),
              autoload=True,
                autoload_with=engine
            )
            license_pool = relationship(
                LicensePool, 
                primaryjoin="LicensePool.id==MaterializedWork.license_pool_id",
                foreign_keys=LicensePool.id, lazy='joined', uselist=False)

            def __repr__(self):
                return (u'%s "%s" (%s) %s' % (
                    self.works_id, self.sort_title, self.sort_author, self.language,
                    )).encode("utf8")


        globals()['MaterializedWork'] = MaterializedWork
        globals()['MaterializedWorkWithGenre'] = MaterializedWorkWithGenre
        cls.engine_for_url[url] = engine
        return engine, engine.connect()

    @classmethod
    def refresh_materialized_views(self, _db):
        for view_name in self.MATERIALIZED_VIEWS.keys():
            _db.execute("refresh materialized view %s;" % view_name)
            _db.commit()

    @classmethod
    def session(cls, url):
        engine = connection = 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            engine, connection = cls.initialize(url)
        session = Session(connection)
        cls.initialize_data(session)
        session.commit()
        return session

    @classmethod
    def initialize_data(cls, session):
        # Create initial data sources.
        list(DataSource.well_known_sources(session))

        # Create all genres.
        Genre.load_all(session)
        for g in classifier.genres.values():
            Genre.lookup(session, g, autocreate=True)

        # Load all delivery mechanisms from the database.
        DeliveryMechanism.load_all(session)

        # Make sure that the mechanisms fulfillable by the default
        # client are marked as such.
        for content_type, drm_scheme in DeliveryMechanism.default_client_can_fulfill_lookup:
            mechanism, is_new = DeliveryMechanism.lookup(
                session, content_type, drm_scheme
            )
            mechanism.default_client_can_fulfill = True

        session.commit()

def get_one(db, model, on_multiple='error', **kwargs):
    q = db.query(model).filter_by(**kwargs)
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
            if 'on_multiple' in kwargs:
                # This kwarg is supported by get_one() but not by create().
                del kwargs['on_multiple']
            obj = create(db, model, create_method, create_method_kwargs, **kwargs)
            __transaction.commit()
            return obj
        except IntegrityError, e:
            logging.error(
                "INTEGRITY ERROR on %r %r, %r: %r", model, create_method_kwargs, 
                kwargs, e)
            __transaction.rollback()
            return db.query(model).filter_by(**kwargs).one(), False

def create(db, model, create_method='',
           create_method_kwargs=None,
           **kwargs):
    kwargs.update(create_method_kwargs or {})
    created = getattr(model, create_method, model)(**kwargs)
    db.add(created)
    db.flush()
    return created, True

Base = declarative_base()

class Patron(Base):

    __tablename__ = 'patrons'
    id = Column(Integer, primary_key=True)

    # The patron's permanent unique identifier in an external library
    # system, probably never seen by the patron.
    #
    # This is not stored as a ForeignIdentifier because it corresponds
    # to the patron's identifier in the library responsible for the
    # Simplified instance, not a third party.
    external_identifier = Column(Unicode, unique=True, index=True)

    # The patron's account type, as reckoned by an external library
    # system. Different account types may be subject to different
    # library policies.
    #
    # Depending on library policy it may be possible to automatically
    # derive the patron's account type from their authorization
    # identifier.
    _external_type = Column(Unicode, index=True, name="external_type")

    # An identifier used by the patron that gives them the authority
    # to borrow books. This identifier may change over time.
    authorization_identifier = Column(Unicode, unique=True, index=True)

    # An identifier used by the patron that authenticates them,
    # but does not give them the authority to borrow books. i.e. their
    # website username.
    username = Column(Unicode, unique=True, index=True)

    # The last time this record was synced up with an external library
    # system.
    last_external_sync = Column(DateTime)

    # The time, if any, at which the user's authorization to borrow
    # books expires.
    authorization_expires = Column(Date, index=True)

    # Outstanding fines the user has, if any.
    fines = Column(Unicode)

    loans = relationship('Loan', backref='patron')
    holds = relationship('Hold', backref='patron')

    # One Patron can have many associated Credentials.
    credentials = relationship("Credential", backref="patron")

    AUDIENCE_RESTRICTION_POLICY = 'audiences'
    EXTERNAL_TYPE_REGULAR_EXPRESSION = 'external_type_regular_expression'

    def works_on_loan(self):
        db = Session.object_session(self)
        loans = db.query(Loan).filter(Loan.patron==self)
        return [loan.work for loan in self.loans if loan.work]

    def works_on_loan_or_on_hold(self):
        db = Session.object_session(self)
        results = set()
        holds = [hold.work for hold in self.holds if hold.work]
        loans = self.works_on_loan()
        return set(holds + loans)

    @property
    def external_type(self):
        if self.authorization_identifier and not self._external_type:
            policy = Configuration.policy(
                self.EXTERNAL_TYPE_REGULAR_EXPRESSION)
            if policy:
                match = re.compile(policy).search(
                    self.authorization_identifier)
                if match:
                    groups = match.groups()
                    if groups:
                        self._external_type = groups[0]
        return self._external_type

    def can_borrow(self, work, policy):
        """Return true if the given policy allows this patron to borrow the
        given work.
        
        This will return False when the policy for this patron's
        .external_type prevents access to this book's audience.
        """
        if not self.external_type in policy:
            return True
        if not work:
            # Shouldn't happen, but not this method's problem.
            return True
        p = policy[self.external_type]
        if not self.AUDIENCE_RESTRICTION_POLICY in p:
            return True
        allowed = p[self.AUDIENCE_RESTRICTION_POLICY]
        if work.audience in allowed:
            return True
        return False


    @property
    def authorization_is_active(self):
        # Unlike pretty much every other place in this app, I use
        # (server) local time here instead of UTC. This is to make it
        # less likely that a patron's authorization will expire before
        # they think it should.
        if (self.authorization_expires
            and self.authorization_expires 
            < datetime.datetime.now().date()):
            return False
        return True


class LoanAndHoldMixin(object):

    @property
    def work(self):
        """Try to find the corresponding work for this Loan/Hold."""
        license_pool = self.license_pool
        if not license_pool:
            return None
        if license_pool.work:
            return license_pool.work
        if license_pool.edition and license_pool.edition.work:
            return license_pool.edition.work
        return None        


class Loan(Base, LoanAndHoldMixin):
    __tablename__ = 'loans'
    id = Column(Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)
    fulfillment_id = Column(Integer, ForeignKey('licensepooldeliveries.id'))
    start = Column(DateTime)
    end = Column(DateTime)

    __table_args__ = (
        UniqueConstraint('patron_id', 'license_pool_id'),
    )

    def until(self, default_loan_period):
        """Give or estimate the time at which the loan will end."""
        if self.end:
            return self.end
        if default_loan_period is None:
            # This loan will last forever.
            return None
        start = self.start or datetime.datetime.utcnow()
        return start + default_loan_period

class Hold(Base, LoanAndHoldMixin):
    """A patron is in line to check out a book.
    """
    __tablename__ = 'holds'
    id = Column(Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)
    start = Column(DateTime, index=True)
    end = Column(DateTime, index=True)
    position = Column(Integer, index=True)

    @classmethod
    def _calculate_until(
            self, start, queue_position, total_licenses, default_loan_period,
            default_reservation_period):
        """Helper method for `Hold.until` that can be tested independently.

        We have to wait for the available licenses to cycle a
        certain number of times before we get a turn.
        
        Example: 4 licenses, queue position 21
        After 1 cycle: queue position 17
              2      : queue position 13
              3      : queue position 9
              4      : queue position 5
              5      : queue position 1
              6      : available

        The worst-case cycle time is the loan period plus the reservation
        period.
        """
        if queue_position == 0:
            # The book is currently reserved to this patron--they need
            # to hurry up and check it out.
            return start + default_reservation_period

        if total_licenses == 0:
            # The book will never be available
            return None

        # Start with the default loan period to clear out everyone who
        # currently has the book checked out.
        duration = default_loan_period

        if queue_position < total_licenses:
            # After that period, the book will be available to this patron.
            # Do nothing.
            pass
        else:
            # Otherwise, add a number of cycles in which other people are
            # notified that it's their turn.
            cycle_period = (default_loan_period + default_reservation_period)
            cycles = queue_position / total_licenses
            if (total_licenses > 1 and queue_position % total_licenses == 0):
                cycles -= 1
            duration += (cycle_period * cycles)
        return start + duration


    def until(self, default_loan_period, default_reservation_period):
        """Give or estimate the time at which the book will be available
        to this patron.

        This is a *very* rough estimate that should be treated more or
        less as a worst case. (Though it could be even worse than
        this--the library's license might expire and then you'll
        _never_ get the book.)
        """
        if self.end:
            # Whew, the server provided its own estimate.
            return self.end

        if default_reservation_period is None:
            # This hold has no definite end date.
            return None

        start = datetime.datetime.utcnow()
        licenses_available = self.license_pool.licenses_owned
        position = self.position
        if not position:
            # We don't know where in line we are. Assume we're at the
            # end.
            position = self.license_pool.patrons_in_hold_queue
        return self._calculate_until(
            start, position, licenses_available,
            default_loan_period, default_reservation_period)

    def update(self, start, end, position):
        """When the book becomes available, position will be 0 and end will be
        set to the time at which point the patron will lose their place in
        line.
        
        Otherwise, end is irrelevant and is set to None.
        """
        if start is not None:
            self.start = start
        if position == 0 and end is not None:
            self.end = end
        else:
            self.end = None
        if position is not None:
            self.position = position

    __table_args__ = (
        UniqueConstraint('patron_id', 'license_pool_id'),
    )


class DataSource(Base):

    """A source for information about books, and possibly the books themselves."""

    GUTENBERG = "Gutenberg"
    OVERDRIVE = "Overdrive"
    PROJECT_GITENBERG = "Project GITenberg"
    STANDARD_EBOOKS = "Standard Ebooks"
    UNGLUE_IT = "unglue.it"
    THREEM = "3M"
    OCLC = "OCLC Classify"
    OCLC_LINKED_DATA = "OCLC Linked Data"
    AMAZON = "Amazon"
    XID = "WorldCat xID"
    AXIS_360 = "Axis 360"
    WEB = "Web"
    OPEN_LIBRARY = "Open Library"
    CONTENT_CAFE = "Content Cafe"
    VIAF = "VIAF"
    GUTENBERG_COVER_GENERATOR = "Gutenberg Illustrated"
    GUTENBERG_EPUB_GENERATOR = "Project Gutenberg EPUB Generator"
    METADATA_WRANGLER = "Library Simplified metadata wrangler"
    MANUAL = "Manual intervention"
    NYT = "New York Times"
    NYPL_SHADOWCAT = "NYPL Shadowcat"
    LIBRARY_STAFF = "Library staff"
    ADOBE = "Adobe DRM"
    PLYMPTON = "Plympton"
    OA_CONTENT_SERVER = "Library Simplified Open Access Content Server"

    __tablename__ = 'datasources'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)
    offers_licenses = Column(Boolean, default=False)
    primary_identifier_type = Column(String, index=True)
    extra = Column(MutableDict.as_mutable(JSON), default={})

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

    @classmethod
    def lookup(cls, _db, name):
        if hasattr(_db, 'data_sources'):
            return _db.data_sources.get(name)
        else:
            # This should only happen during tests.
            return get_one(_db, DataSource, name=name)

    URI_PREFIX = "http://librarysimplified.org/terms/sources/"

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

    # These are pretty standard values but each library needs to
    # define the policy they've negotiated in their configuration.
    default_default_loan_period = datetime.timedelta(days=21)
    default_default_reservation_period = datetime.timedelta(days=3)

    def _datetime_config_value(self, key, default):
        integration = Configuration.integration(self.name)
        if not integration:
            return default
        value = integration.get(key)
        if not value:
            return default
        value = int(value)
        return datetime.timedelta(days=value)

    @property
    def default_loan_period(self):
        return self._datetime_config_value(
            Configuration.DEFAULT_LOAN_PERIOD,
            self.default_default_loan_period
        )

    @property
    def default_reservation_period(self):
        return self._datetime_config_value(
            Configuration.DEFAULT_RESERVATION_PERIOD,
            self.default_default_reservation_period
        )

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

        if not hasattr(_db, 'metadata_lookups_by_identifier_type'):
            # This should only happen during testing.
            list(DataSource.well_known_sources(_db))
        return _db.metadata_lookups_by_identifier_type[identifier.type]

    @classmethod
    def well_known_sources(cls, _db):
        """Make sure all the well-known sources exist and are loaded into
        the cache associated with the database connection.
        """

        _db.data_sources = dict()
        _db.metadata_lookups_by_identifier_type = defaultdict(list)

        for (name, offers_licenses, offers_metadata_lookup, primary_identifier_type, refresh_rate) in (
                (cls.GUTENBERG, True, False, Identifier.GUTENBERG_ID, None),
                (cls.OVERDRIVE, True, False, Identifier.OVERDRIVE_ID, 0),
                (cls.THREEM, True, False, Identifier.THREEM_ID, 60*60*6),
                (cls.AXIS_360, True, False, Identifier.AXIS_360_ID, 0),
                (cls.OCLC, False, False, Identifier.OCLC_NUMBER, None),
                (cls.OCLC_LINKED_DATA, False, False, Identifier.OCLC_NUMBER, None),
                (cls.AMAZON, False, False, Identifier.ASIN, None),
                (cls.OPEN_LIBRARY, False, False, Identifier.OPEN_LIBRARY_ID, None),
                (cls.GUTENBERG_COVER_GENERATOR, False, False, Identifier.GUTENBERG_ID, None),
                (cls.GUTENBERG_EPUB_GENERATOR, False, False, Identifier.GUTENBERG_ID, None),
                (cls.WEB, True, False, Identifier.URI, None),
                (cls.VIAF, False, False, None, None),
                (cls.CONTENT_CAFE, True, True, Identifier.ISBN, None),
                (cls.MANUAL, False, False, None, None),
                (cls.NYT, False, False, Identifier.ISBN, None),
                (cls.LIBRARY_STAFF, False, False, Identifier.ISBN, None),
                (cls.METADATA_WRANGLER, False, False, Identifier.URI, None),
                (cls.PROJECT_GITENBERG, True, False, Identifier.GUTENBERG_ID, None),
                (cls.STANDARD_EBOOKS, True, False, Identifier.URI, None),
                (cls.UNGLUE_IT, True, False, Identifier.URI, None),
                (cls.ADOBE, False, False, None, None),
                (cls.PLYMPTON, True, False, Identifier.ISBN, None),
                (cls.OA_CONTENT_SERVER, True, False, Identifier.URI, None),
        ):

            extra = dict()
            if refresh_rate:
                extra['circulation_refresh_rate_seconds'] = refresh_rate

            obj, new = get_one_or_create(
                _db, DataSource,
                name=name,
                create_method_kwargs=dict(
                    offers_licenses=offers_licenses,
                    primary_identifier_type=primary_identifier_type,
                    extra=extra,
                )
            )

            _db.data_sources[obj.name] = obj
            if offers_metadata_lookup:
                l = _db.metadata_lookups_by_identifier_type[primary_identifier_type]
                l.append(obj)
            yield obj


class CoverageRecord(Base):
    """A record of a Identifier being used as input into some process."""
    __tablename__ = 'coveragerecords'

    SET_EDITION_METADATA_OPERATION = 'set-edition-metadata'
    CHOOSE_COVER_OPERATION = 'choose-cover'

    id = Column(Integer, primary_key=True)
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)
    # If applicable, this is the ID of the data source that took the
    # Identifier as input.
    data_source_id = Column(
        Integer, ForeignKey('datasources.id')
    )
    operation = Column(String(255), default=None)
        
    timestamp = Column(DateTime, index=True)
    exception = Column(Unicode, index=True)

    __table_args__ = (
        UniqueConstraint('identifier_id', 'data_source_id', 'operation'),
    )

    def __repr__(self):
        if self.operation:
            operation = ' operation="%s"' % self.operation
        else:
            operation = ''
        if self.exception:
            exception = ' exception="%s"' % self.exception
        else:
            exception = ''
        template = '<CoverageRecord: identifier=%s/%s data_source="%s"%s timestamp="%s"%s>'
        return template % (
            self.identifier.type, 
            self.identifier.identifier,
            self.data_source.name,
            operation, 
            self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            exception
        )

    @classmethod
    def lookup(self, edition_or_identifier, data_source, operation=None):
        _db = Session.object_session(edition_or_identifier)
        if isinstance(edition_or_identifier, Identifier):
            identifier = edition_or_identifier
        elif isinstance(edition_or_identifier, Edition):
            identifier = edition_or_identifier.primary_identifier
        else:
            raise ValueError(
                "Cannot look up a coverage record for %r." % edition) 
        return get_one(
            _db, CoverageRecord,
            identifier=identifier,
            data_source=data_source,
            operation=operation,
            on_multiple='interchangeable',
        )

    @classmethod
    def add_for(self, edition, data_source, operation=None, timestamp=None):
        _db = Session.object_session(edition)
        if isinstance(edition, Identifier):
            identifier = edition
        elif isinstance(edition, Edition):
            identifier = edition.primary_identifier
        else:
            raise ValueError(
                "Cannot create a coverage record for %r." % edition) 
        timestamp = timestamp or datetime.datetime.utcnow()
        coverage_record, is_new = get_one_or_create(
            _db, CoverageRecord,
            identifier=identifier,
            data_source=data_source,
            operation=operation,
            on_multiple='interchangeable'
        )
        coverage_record.timestamp = timestamp
        return coverage_record, is_new

Index("ix_coveragerecords_data_source_id_operation_identifier_id", CoverageRecord.data_source_id, CoverageRecord.operation, CoverageRecord.identifier_id)

class WorkCoverageRecord(Base):
    """A record of some operation that was performed on a Work.

    This is similar to CoverageRecord, which operates on Identifiers,
    but since Work identifiers have no meaning outside of the database,
    we presume that all the operations involve internal work only,
    and as such there is no data_source_id.
    """
    __tablename__ = 'workcoveragerecords'

    CHOOSE_EDITION_OPERATION = 'choose-edition'
    CLASSIFY_OPERATION = 'classify'
    SUMMARY_OPERATION = 'summary'
    QUALITY_OPERATION = 'quality'
    GENERATE_OPDS_OPERATION = 'generate-opds'
    UPDATE_SEARCH_INDEX_OPERATION = 'update-search-index'

    id = Column(Integer, primary_key=True)
    work_id = Column(
        Integer, ForeignKey('works.id'), index=True)
    operation = Column(String(255), index=True, default=None)
        
    timestamp = Column(DateTime, index=True)
    exception = Column(Unicode, index=True)

    __table_args__ = (
        UniqueConstraint('work_id', 'operation'),
    )

    def __repr__(self):
        if self.exception:
            exception = ' exception="%s"' % self.exception
        else:
            exception = ''
        template = '<WorkCoverageRecord: work_id=%s operation="%s" timestamp="%s"%s>'
        return template % (
            self.work_id, self.operation, 
            self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            exception
        )

    @classmethod
    def lookup(self, work, operation):
        _db = Session.object_session(work)
        return get_one(
            _db, WorkCoverageRecord,
            work=work,
            operation=operation,
            on_multiple='interchangeable',
        )

    @classmethod
    def add_for(self, work, operation, timestamp=None):
        _db = Session.object_session(work)
        timestamp = timestamp or datetime.datetime.utcnow()
        coverage_record, is_new = get_one_or_create(
            _db, WorkCoverageRecord,
            work=work,
            operation=operation,
            on_multiple='interchangeable'
        )
        coverage_record.timestamp = timestamp
        return coverage_record, is_new
Index("ix_workcoveragerecords_operation_work_id", WorkCoverageRecord.operation, WorkCoverageRecord.work_id)

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
    OVERDRIVE_ID = "Overdrive ID"
    THREEM_ID = "3M ID"
    GUTENBERG_ID = "Gutenberg ID"
    AXIS_360_ID = "Axis 360 ID"
    ASIN = "ASIN"
    ISBN = "ISBN"
    OCLC_WORK = "OCLC Work ID"
    OCLC_NUMBER = "OCLC Number"
    OPEN_LIBRARY_ID = "OLID"
    BIBLIOCOMMONS_ID = "Bibliocommons ID"
    URI = "URI"
    DOI = "DOI"
    UPC = "UPC"

    LICENSE_PROVIDING_IDENTIFIER_TYPES = [
        THREEM_ID, OVERDRIVE_ID, AXIS_360_ID,
        GUTENBERG_ID
    ]

    URN_SCHEME_PREFIX = "urn:librarysimplified.org/terms/id/"
    ISBN_URN_SCHEME_PREFIX = "urn:isbn:"
    GUTENBERG_URN_SCHEME_PREFIX = "http://www.gutenberg.org/ebooks/"
    GUTENBERG_URN_SCHEME_RE = re.compile(
        GUTENBERG_URN_SCHEME_PREFIX + "([0-9]+)")

    __tablename__ = 'identifiers'
    id = Column(Integer, primary_key=True)
    type = Column(String(64), index=True)
    identifier = Column(String, index=True)

    equivalencies = relationship(
        "Equivalency",
        primaryjoin=("Identifier.id==Equivalency.input_id"),
        backref="input_identifiers",
    )

    inbound_equivalencies = relationship(
        "Equivalency",
        primaryjoin=("Identifier.id==Equivalency.output_id"),
        backref="output_identifiers",
    )

    unresolved_identifier = relationship(
        "UnresolvedIdentifier", backref="identifier", uselist=False
    )

    # One Identifier may have many associated CoverageRecords.
    coverage_records = relationship("CoverageRecord", backref="identifier")

    def __repr__(self):
        records = self.primarily_identifies
        if records and records[0].title:
            title = u' wr=%d ("%s")' % (records[0].id, records[0].title)
        else:
            title = ""
        return (u"%s/%s ID=%s%s" % (self.type, self.identifier, self.id,
                                    title)).encode("utf8")

    # One Identifier may serve as the primary identifier for
    # several Editions.
    primarily_identifies = relationship(
        "Edition", backref="primary_identifier"
    )

    # One Identifier may serve as the identifier for
    # a single LicensePool.
    licensed_through = relationship(
        "LicensePool", backref="identifier", uselist=False, lazy='joined',
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
        was_new = None
        if foreign_identifier_type in (
                Identifier.OVERDRIVE_ID, Identifier.THREEM_ID):
            foreign_id = foreign_id.lower()
        if autocreate:
            m = get_one_or_create
        else:
            m = get_one
            was_new = False

        result = m(_db, cls, type=foreign_identifier_type,
                   identifier=foreign_id)
        if isinstance(result, tuple):
            return result
        else:
            return result, False

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

    class UnresolvableIdentifierException(Exception):
        # Raised when an identifier that can't be resolved into a LicensePool
        # is provided in a context that requires a resolvable identifier
        pass

    @classmethod
    def type_and_identifier_for_urn(cls, identifier_string):
        if not identifier_string:
            return None
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
        else:
            raise ValueError(
                "Could not turn %s into a recognized identifier." %
                identifier_string)
        return (type, identifier_string)


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
    def recursively_equivalent_identifier_ids(
            cls, _db, identifier_ids, levels=5, threshold=0.50, debug=False):
        """All Identifier IDs equivalent to the given set of Identifier
        IDs at the given confidence threshold.

        This is an inefficient but simple implementation, performing
        one SQL query for each level of recursion.

        Four levels is enough to go from a Gutenberg text to an ISBN.
        Gutenberg ID -> OCLC Work IS -> OCLC Number -> ISBN

        Returns a dictionary mapping each ID in the original to a
        dictionary mapping the equivalent IDs to (confidence, strength
        of confidence) 2-tuples.
        """

        if not identifier_ids:
            return {}

        if isinstance(identifier_ids[0], Identifier):
            identifier_ids = [x.id for x in identifier_ids]

        (working_set, seen_equivalency_ids, seen_identifier_ids,
         equivalents) = cls._recursively_equivalent_identifier_ids(
             _db, identifier_ids, identifier_ids, levels, threshold)

        if working_set:
            # This is not a big deal, but it means we could be getting
            # more IDs by increasing the level.
            logging.warn(
                "Leftover working set at level %d: %r", levels, working_set)

        return equivalents

    @classmethod
    def _recursively_equivalent_identifier_ids(
            cls, _db, original_working_set, working_set, levels, threshold):

        if levels == 0:
            equivalents = defaultdict(lambda : defaultdict(list))
            for id in original_working_set:
                # Every identifier is unshakeably equivalent to itself.
                equivalents[id][id] = (1, 1000000)
            return (working_set, set(), set(), equivalents)

        if not working_set:
            return working_set, seen_equivalency_ids, seen_identifier_ids

        # First make the recursive call.        
        (working_set, seen_equivalency_ids, seen_identifier_ids,
         equivalents) = cls._recursively_equivalent_identifier_ids(
             _db, original_working_set, working_set, levels-1, threshold)

        if not working_set:
            # We're done.
            return (working_set, seen_equivalency_ids, seen_identifier_ids,
                    equivalents)

        new_working_set = set()
        seen_identifier_ids = seen_identifier_ids.union(working_set)

        equivalencies = Equivalency.for_identifiers(
            _db, working_set, seen_equivalency_ids)
        for e in equivalencies:
            #logging.debug("%r => %r", e.input, e.output)
            seen_equivalency_ids.add(e.id)

            # Signal strength decreases monotonically, so
            # if it dips below the threshold, we can
            # ignore it from this point on.

            # I -> O becomes "I is a precursor of O with distance
            # equal to the I->O strength."
            if e.strength > threshold:
                #logging.debug("Strong signal: %r", e)
                
                cls._update_equivalents(
                    equivalents, e.output_id, e.input_id, e.strength, e.votes)
                cls._update_equivalents(
                    equivalents, e.input_id, e.output_id, e.strength, e.votes)
            else:
                logging.debug("Ignoring signal below threshold: %r", e)

            if e.output_id not in seen_identifier_ids:
                # This is our first time encountering the
                # Identifier that is the output of this
                # Equivalency. We will look at its equivalencies
                # in the next round.
                new_working_set.add(e.output_id)
            if e.input_id not in seen_identifier_ids:
                # This is our first time encountering the
                # Identifier that is the input to this
                # Equivalency. We will look at its equivalencies
                # in the next round.
                new_working_set.add(e.input_id)

        #logging.debug("At level %d.", levels)
        #logging.debug(" Original working set: %r", sorted(original_working_set))
        #logging.debug(" New working set: %r", sorted(new_working_set))
        #logging.debug(" %d equivalencies seen so far.",  len(seen_equivalency_ids))
        #logging.debug(" %d identifiers seen so far.", len(seen_identifier_ids))
        #logging.debug(" %d equivalents", len(equivalents))

        if new_working_set:

            q = _db.query(Identifier).filter(Identifier.id.in_(new_working_set))
            new_identifiers = [repr(i) for i in q]
            new_working_set_repr = ", ".join(new_identifiers)
            #logging.debug(
            #    " Here's the new working set: %r", new_working_set_repr)

        surviving_working_set = set()
        for id in original_working_set:
            for new_id in new_working_set:
                for neighbor in list(equivalents[id]):
                    if neighbor == id:
                        continue
                    if neighbor == new_id:
                        # The new ID is directly adjacent to one of
                        # the original working set.
                        surviving_working_set.add(new_id)
                        continue
                    if new_id in equivalents[neighbor]:
                        # The new ID is adjacent to an ID adjacent to
                        # one of the original working set. But how
                        # strong is the signal?
                        o2n_weight, o2n_votes = equivalents[id][neighbor]
                        n2new_weight, n2new_votes = equivalents[neighbor][new_id]
                        new_weight = (o2n_weight * n2new_weight)
                        if new_weight > threshold:
                            equivalents[id][new_id] = (new_weight, o2n_votes + n2new_votes)
                            surviving_working_set.add(new_id)

        #logging.debug(
        #    "Pruned %d from working set",
        #    len(surviving_working_set.intersection(new_working_set))
        #)
        return (surviving_working_set, seen_equivalency_ids, seen_identifier_ids,
                equivalents)

    @classmethod
    def _update_equivalents(original_working_set, equivalents, input_id,
                            output_id, strength, votes):
        if not equivalents[input_id][output_id]:
            equivalents[input_id][output_id] = (strength, votes)
        else:
            old_strength, old_votes = equivalents[input_id][output_id]
            total_strength = (old_strength * old_votes) + (strength * votes)
            total_votes = (old_votes + votes)
            new_strength = total_strength / total_votes
            equivalents[input_id][output_id] = (new_strength, total_votes)

    @classmethod
    def recursively_equivalent_identifier_ids_flat(
            cls, _db, identifier_ids, levels=5, threshold=0.5):
        data = cls.recursively_equivalent_identifier_ids(
            _db, identifier_ids, levels, threshold)
        return cls.flatten_identifier_ids(data)

    @classmethod
    def flatten_identifier_ids(cls, data):
        ids = set()
        for equivalents in data.values():
            ids = ids.union(set(equivalents.keys()))
        return ids

    def equivalent_identifier_ids(self, levels=5, threshold=0.5):
        _db = Session.object_session(self)
        return Identifier.recursively_equivalent_identifier_ids_flat(
            _db, [self.id], levels, threshold)

    def add_link(self, rel, href, data_source, license_pool=None,
                 media_type=None, content=None, content_path=None):
        """Create a link between this Identifier and a (potentially new)
        Resource."""
        _db = Session.object_session(self)

        if license_pool and license_pool.identifier != self:
            raise ValueError(
                "License pool is associated with %r, not %r!" % (
                    license_pool.identifier, self))
        
        # Find or create the Resource.
        if not href:
            href = Hyperlink.generic_uri(data_source, self, rel, content)
        resource, new_resource = get_one_or_create(
            _db, Resource, url=href,
            create_method_kwargs=dict(data_source=data_source)
        )

        # Find or create the Hyperlink.
        link, new_link = get_one_or_create(
            _db, Hyperlink, rel=rel, data_source=data_source,
            identifier=self, resource=resource,
            create_method_kwargs=dict(license_pool=license_pool)
        )

        if content or content_path:
            resource.set_fetched_content(media_type, content, content_path)
        elif media_type:
            resource.set_mirrored_elsewhere(media_type)

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
            _db, subject_type, subject_identifier, subject_name)
        #if is_new:
        #    print repr(subject)

        logging.debug(
            "CLASSIFICATION: %s on %s/%s: %s %s/%s",
            data_source.name, self.type, self.identifier,
            subject.type, subject.identifier, subject.name
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
            resources = resources.filter(Hyperlink.data_source==data_source)
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

    # The point at which a generic geometric image is better
    # than some other image.
    MINIMUM_IMAGE_QUALITY = 0.25

    @classmethod
    def best_cover_for(cls, _db, identifier_ids):
        # Find all image resources associated with any of
        # these identifiers.
        images = cls.resources_for_identifier_ids(
            _db, identifier_ids, Hyperlink.IMAGE)
        images = images.join(Resource.representation)
        images = images.filter(Representation.mirrored_at != None).filter(
            Representation.mirror_url != None)
        images = images.all()

        champion = None
        champions = []
        champion_score = None
        # Judge the image resource by its deviation from the ideal
        # aspect ratio, and by its deviation (in the "too small"
        # direction only) from the ideal resolution.
        for r in images:
            for link in r.links:
                if link.license_pool and not link.license_pool.open_access:
                    # For licensed works, always present the cover
                    # provided by the licensing authority.
                    r.quality = 1
                    champion = r
                    break

            if champion and champion.quality == 1:
                # No need to look further
                break

            rep = r.representation
            if not rep:
                continue

            if not champion:
                champion = r
                continue

            if not rep.image_width or not rep.image_height:
                continue
            aspect_ratio = rep.image_width / float(rep.image_height)
            aspect_difference = abs(aspect_ratio-cls.IDEAL_COVER_ASPECT_RATIO)
            quality = 1 - aspect_difference
            width_difference = (
                float(rep.image_width - cls.IDEAL_IMAGE_WIDTH) / cls.IDEAL_IMAGE_WIDTH)
            if width_difference < 0:
                # Image is not wide enough.
                quality = quality * (1+width_difference)
            height_difference = (
                float(rep.image_height - cls.IDEAL_IMAGE_HEIGHT) / cls.IDEAL_IMAGE_HEIGHT)
            if height_difference < 0:
                # Image is not tall enough.
                quality = quality * (1+height_difference)

            # Scale the estimated quality by the source of the image.
            source_name = r.data_source.name
            if source_name==DataSource.GUTENBERG_COVER_GENERATOR:
                quality = quality * 0.60
            elif source_name==DataSource.GUTENBERG:
                quality = quality * 0.50
            elif source_name==DataSource.OPEN_LIBRARY:
                quality = quality * 0.25

            r.set_estimated_quality(quality)

            # TODO: that says how good the image is as an image. But
            # how good is it as an image for this particular book?
            # Determining this requires measuring the conceptual
            # distance from the image to a Edition, and then from
            # the Edition to the Work in question. This is much
            # too big a project to work on right now.

            if not r.quality >= cls.MINIMUM_IMAGE_QUALITY:
                continue
            if r.quality > champion_score:
                champions = [r]
                champion_score = r.quality
            elif r.quality == champion_score:
                champions.append(r)
        if champions and not champion:
            champion = random.choice(champions)
            
        return champion, images

    @classmethod
    def evaluate_summary_quality(cls, _db, identifier_ids, 
                                 privileged_data_source=None):
        """Evaluate the summaries for the given group of Identifier IDs.

        This is an automatic evaluation based solely on the content of
        the summaries. It will be combined with human-entered ratings
        to form an overall quality score.

        We need to evaluate summaries from a set of Identifiers
        (typically those associated with a single work) because we
        need to see which noun phrases are most frequently used to
        describe the underlying work.

        :param privileged_data_source: If present, a summary from this
        data source will be instantly chosen, short-circuiting the
        decision process.

        :return: The single highest-rated summary Resource.

        """
        evaluator = SummaryEvaluator()

        # Find all rel="description" resources associated with any of
        # these records.
        rels = [Hyperlink.DESCRIPTION, Hyperlink.SHORT_DESCRIPTION]
        descriptions = cls.resources_for_identifier_ids(
            _db, identifier_ids, rels, privileged_data_source)
        descriptions = descriptions.join(
            Resource.representation).filter(
                Representation.content != None).all()

        champion = None
        # Add each resource's content to the evaluator's corpus.
        for r in descriptions:
            evaluator.add(r.representation.content)
        evaluator.ready()

        # Then have the evaluator rank each resource.
        for r in descriptions:
            content = r.representation.content
            quality = evaluator.score(content)
            r.set_estimated_quality(quality)
            if not champion or r.quality > champion.quality:
                champion = r

        if privileged_data_source and not champion:
            # We could not find any descriptions from the privileged
            # data source. Try relaxing that restriction.
            return cls.evaluate_summary_quality(_db, identifier_ids)
        return champion, descriptions

    @classmethod
    def missing_coverage_from(
            cls, _db, identifier_types, coverage_data_source):
        """Find identifiers of the given types which have no CoverageRecord
        from `coverage_data_source`.
        """
        clause = and_(Identifier.id==CoverageRecord.identifier_id,
                      CoverageRecord.data_source==coverage_data_source)
        q = _db.query(Identifier).outerjoin(CoverageRecord, clause)
        if identifier_types:
            q = q.filter(Identifier.type.in_(identifier_types))
        q2 = q.filter(CoverageRecord.id==None)
        return q2

    def opds_entry(self):
        """Create an OPDS entry using only resources directly
        associated with this Identifier.

        This makes it possible to create an OPDS entry even when there
        is no Edition.

        Currently the only things in this OPDS entry will be description,
        cover image, and popularity.
        """
        id = self.urn
        cover_image = None
        description = None
        for link in self.links:
            resource = link.resource
            if link.rel == Hyperlink.IMAGE:
                if not cover_image or (
                        not cover_image.representation.thumbnails and
                        resource.representation.thumbnails):
                    cover_image = resource
            elif link.rel == Hyperlink.DESCRIPTION:
                if not description or resource.quality > description.quality:
                    description = resource

        quality = Measurement.overall_quality(self.measurements)
        from opds import AcquisitionFeed
        return AcquisitionFeed.minimal_opds_entry(
            identifier=self, cover=cover_image, 
            description=description, quality=quality)


class UnresolvedIdentifier(Base):
    """An identifier that the metadata wrangler has heard of but hasn't
    yet been able to connect with a book being offered by someone.
    """

    __tablename__ = 'unresolvedidentifiers'
    id = Column(Integer, primary_key=True)

    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)

    # A numeric status code, analogous to an HTTP status code,
    # describing the status of the process of resolving this
    # identifier.
    status = Column(Integer, index=True)

    # Timestamp of the first time we tried to resolve this identifier.
    first_attempt = Column(DateTime, index=True)

    # Timestamp of the most recent time we tried to resolve this identifier.
    most_recent_attempt = Column(DateTime, index=True)

    # The problem that's stopping this identifier from being resolved.
    exception = Column(Unicode, index=True)

    @classmethod
    def register(cls, _db, identifier, force=False):
        if identifier.licensed_through and not force:
            # There's already a license pool for this identifier, and
            # thus no need to do anything.
            raise ValueError(
                "%r has already been resolved. Not creating an UnresolvedIdentifier record for it." % identifier)

        # There must be some way of 'resolving' the work to be done
        # here: either a license source or a metadata lookup.
        has_metadata_lookup = DataSource.metadata_sources_for(_db, identifier)

        if not has_metadata_lookup:
            datasources = DataSource.license_sources_for(_db, identifier)
            if datasources.count() == 0:
                # This is not okay--we have no way of resolving this identifier.
                raise Identifier.UnresolvableIdentifierException()

        return get_one_or_create(
            _db, UnresolvedIdentifier, identifier=identifier,
            create_method_kwargs=dict(status=202), on_multiple='interchangeable'
        )

    DEFAULT_RETRY_TIME = datetime.timedelta(days=1)

    @classmethod
    def ready_to_process(cls, _db, retry_after=None, randomize=True):
        """Find all UnresolvedIdentifiers that are ready for processing.

        This is all UnresolvedIdentifiers that have never raised an
        exception, plus all UnresolvedIdentifiers that were attempted
        more than `retry_after` ago.

        :param retry_after: a `datetime.timedelta`.
        """
        now = datetime.datetime.utcnow()
        retry_after = retry_after or cls.DEFAULT_RETRY_TIME
        cutoff = now - retry_after
        needs_processing = or_(
            UnresolvedIdentifier.exception==None,
            UnresolvedIdentifier.most_recent_attempt < cutoff
        )
        q = _db.query(UnresolvedIdentifier).join(
            UnresolvedIdentifier.identifier).filter(needs_processing)
        if randomize:
            q = q.order_by(func.random())
        return q


    def set_attempt(self, time=None):
        """Set most_recent_attempt (and possibly first_attempt) to the given
        time.
        """
        time = time or datetime.datetime.utcnow()
        self.most_recent_attempt = time
        if not self.first_attempt:
            self.first_attempt = time

class Contributor(Base):

    """Someone (usually human) who contributes to books."""
    __tablename__ = 'contributors'
    id = Column(Integer, primary_key=True)

    # Standard identifiers for this contributor.
    lc = Column(Unicode, index=True)
    viaf = Column(Unicode, index=True)

    # This is the name by which this person is known in the original
    # catalog. It is sortable, e.g. "Twain, Mark".
    name = Column(Unicode, index=True)
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
    AUTHOR_ROLE = "Author"
    PRIMARY_AUTHOR_ROLE = "Primary Author"
    PERFORMER_ROLE = "Performer"
    EDITOR_ROLE = "Editor"
    ARTIST_ROLE = "Artist"
    PHOTOGRAPHER_ROLE = "Photographer"
    TRANSLATOR_ROLE = "Translator"
    ILLUSTRATOR_ROLE = "Illustrator"
    INTRODUCTION_ROLE = "Introduction Author"
    FOREWORD_ROLE = "Foreword Author" 
    AFTERWORD_ROLE = "Afterword Author" 
    COLOPHON_ROLE = "Colophon Author"
    UNKNOWN_ROLE = 'Unknown'
    DIRECTOR_ROLE = 'Director'
    PRODUCER_ROLE = 'Producer'
    EXECUTIVE_PRODUCER_ROLE = 'Executive Producer'
    ACTOR_ROLE = 'Actor'
    LYRICIST_ROLE = 'Lyricist'
    CONTRIBUTOR_ROLE = 'Contributor'
    COMPOSER_ROLE = 'Composer'
    NARRATOR_ROLE = 'Narrator'
    COMPILER_ROLE = 'Compiler'
    ADAPTER_ROLE = 'Adapter'
    PERFORMER_ROLE = 'Performer'
    MUSICIAN_ROLE = 'Musician'
    ASSOCIATED_ROLE = 'Associated name'
    COLLABORATOR_ROLE = 'Collaborator'
    ENGINEER_ROLE = 'Engineer'
    COPYRIGHT_HOLDER_ROLE = 'Copyright holder'
    TRANSCRIBER_ROLE = 'Transcriber'
    DESIGNER_ROLE = 'Designer'
    AUTHOR_ROLES = set([PRIMARY_AUTHOR_ROLE, AUTHOR_ROLE])

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
        return (u"Contributor %d (%s)" % (self.id, self.name)).encode("utf8")

    @classmethod
    def author_contributor_tiers(cls):
        yield [cls.PRIMARY_AUTHOR_ROLE]
        yield cls.AUTHOR_ROLES
        yield cls.AUTHOR_SUBSTITUTE_ROLES
        yield cls.PERFORMER_ROLES

    @classmethod
    def lookup(cls, _db, name=None, viaf=None, lc=None, aliases=None,
               extra=None):
        """Find or create a record for the given Contributor."""
        extra = extra or dict()

        create_method_kwargs = {
            Contributor.name.name : name,
            Contributor.aliases.name : aliases,
            Contributor.extra.name : extra
        }

        if not name and not lc and not viaf:
            raise ValueError(
                "Cannot look up a Contributor without any identifying "
                "information whatsoever!")

        if name and not lc and not viaf:
            # We will not create a Contributor based solely on a name
            # unless there is no existing Contributor with that name.
            #
            # If there *are* contributors with that name, we will
            # return all of them.
            #
            # We currently do not check aliases when doing name lookups.
            q = _db.query(Contributor).filter(Contributor.name==name)
            contributors = q.all()
            if contributors:
                return contributors, False
            else:
                try:
                    contributor = Contributor(**create_method_kwargs)
                    _db.add(contributor)
                    _db.flush()
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

            contributors, new = get_one_or_create(
                _db, Contributor, create_method_kwargs=create_method_kwargs,
                **query)

        return contributors, new

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
        existing_aliases = set(destination.aliases)
        new_aliases = list(destination.aliases)
        for name in [self.name] + self.aliases:
            if name != destination.name and name not in existing_aliases:
                new_aliases.append(name)
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases
        for k, v in self.extra.items():
            if not k in destination.extra:
                destination.extra[k] = v
        if not destination.lc:
            destination.lc = self.lc
        if not destination.viaf:
            destination.viaf = self.viaf
        if not destination.family_name:
            destination.family_name = self.family_name
        if not destination.display_name:
            destination.display_name = self.display_name
        if not destination.wikipedia_name:
            destination.wikipedia_name = self.wikipedia_name

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
        # print "Commit before deletion."
        _db.commit()
        # print "Final deletion."
        _db.delete(self)
        # print "Committing after deletion."
        _db.commit()
        # _db.query(Contributor).filter(Contributor.id==self.id).delete()
        #_db.commit()
        #print "All done."

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
        return self._default_names(self.name, default_display_name)

    @classmethod
    def _default_names(cls, name, default_display_name=None):
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
        #print " Default names for %s" % original_name
        #print "  Family name: %s" % family_name
        #print "  Display name: %s" % display_name
        #print
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

    # This Edition is associated with one particular
    # identifier--the one used by its data source to identify
    # it. Through the Equivalency class, it is associated with a
    # (probably huge) number of other identifiers.
    primary_identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)

    # A Edition may be associated with a single Work.
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # A Edition may be the primary identifier associated with its
    # Work, or it may not be.
    is_primary_for_work = Column(Boolean, index=True, default=False)

    # An Edition may show up in many CustomListEntries.
    custom_list_entries = relationship("CustomListEntry", backref="edition")

    title = Column(Unicode, index=True)
    sort_title = Column(Unicode, index=True)
    subtitle = Column(Unicode, index=True)
    series = Column(Unicode, index=True)

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

    # `published is the original publication date of the
    # text. `issued` is when made available in this ebook edition. A
    # Project Gutenberg text was likely `published` long before being
    # `issued`.
    issued = Column(Date)
    published = Column(Date)

    BOOK_MEDIUM = u"Book"
    PERIODICAL_MEDIUM = u"Periodical"
    AUDIO_MEDIUM = u"Audio"
    MUSIC_MEDIUM = u"Music"
    VIDEO_MEDIUM = u"Video"

    ELECTRONIC_FORMAT = u"Electronic"
    CODEX_FORMAT = u"Codex"

    medium_to_additional_type = {
        BOOK_MEDIUM : u"http://schema.org/Book",
        AUDIO_MEDIUM : u"http://schema.org/AudioObject",
        PERIODICAL_MEDIUM : u"http://schema.org/PublicationIssue",
        MUSIC_MEDIUM :  u"http://schema.org/MusicRecording",
        VIDEO_MEDIUM :  u"http://schema.org/VideoObject",
    }

    additional_type_to_medium = {}
    for k, v in medium_to_additional_type.items():
        additional_type_to_medium[v] = k

    medium = Column(
        Enum(BOOK_MEDIUM, PERIODICAL_MEDIUM, AUDIO_MEDIUM,
             MUSIC_MEDIUM, VIDEO_MEDIUM, name="medium"),
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
    
    # This lets us avoid a lot of work figuring out the best open
    # access link for this Edition.
    open_access_download_url = Column(Unicode)

    # This is set to True if we know there just isn't a cover for this
    # edition. That lets us know it's okay to set the corresponding
    # work to presentation ready even in the absence of a cover for
    # its primary edition.
    no_known_cover = Column(Boolean, default=False)

    # An OPDS entry containing all metadata about this entry that
    # would be relevant to display to a library patron.
    simple_opds_entry = Column(Unicode, default=None)

    # Information kept in here probably won't be used.
    extra = Column(MutableDict.as_mutable(JSON), default={})

    def __repr__(self):
        id_repr = repr(self.primary_identifier).decode("utf8")
        a = (u"Edition %s [%r] (%s/%s/%s)" % (
            self.id, id_repr, self.title,
            ", ".join([x.name for x in self.contributors]),
            self.language))
        return a.encode("utf8")

    @property
    def language_code(self):
        return LanguageCodes.three_to_two.get(self.language, self.language)

    @property
    def contributors(self):
        return [x.contributor for x in self.contributions]

    @property
    def author_contributors(self):
        """All 'author'-type contributors, with the primary author first,
        other authors sorted by sort name.
        """
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
        if primary_author:
            return [primary_author] + sorted(other_authors, key=lambda x: x.name)

        if other_authors:
            return other_authors

        for role in (
                Contributor.AUTHOR_SUBSTITUTE_ROLES 
                + Contributor.PERFORMER_ROLES):
            if role in acceptable_substitutes:
                contributors = acceptable_substitutes[role]
                return sorted(contributors, key=lambda x: x.name)
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

        e.g. for_foreign_id(_db, DataSource.OVERDRIVE,
                            Identifier.OVERDRIVE_ID, uuid)

        finds the Edition for Overdrive's view of a book identified
        by Overdrive UUID.

        This:

        for_foreign_id(_db, DataSource.OVERDRIVE, Identifier.ISBN, isbn)

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
    def license_pool(self):
        """The Edition's corresponding LicensePool, if any.
        """
        _db = Session.object_session(self)
        return get_one(_db, LicensePool,
                       data_source=self.data_source,
                       identifier=self.primary_identifier)

    def equivalencies(self, _db):
        """All the direct equivalencies between this record's primary
        identifier and other Identifiers.
        """
        return self.primary_identifier.equivalencies
        
    def equivalent_identifier_ids(self, levels=3, threshold=0.5):
        """All Identifiers equivalent to this record's primary identifier,
        at the given level of recursion."""
        return self.primary_identifier.equivalent_identifier_ids(
            levels, threshold)

    def equivalent_identifiers(self, levels=3, threshold=0.5, type=None):
        """All Identifiers equivalent to this
        Edition's primary identifier, at the given level of recursion.
        """
        _db = Session.object_session(self)
        identifier_ids = self.equivalent_identifier_ids(levels, threshold)
        q = _db.query(Identifier).filter(
            Identifier.id.in_(identifier_ids))
        if type:
            if isinstance(type, list):
                q = q.filter(Identifier.type.in_(type))
            else:
                q = q.filter(Identifier.type==type)
        return q

    def equivalent_editions(self, levels=5, threshold=0.5):
        """All Editions whose primary ID is equivalent to this Edition's
        primary ID, at the given level of recursion.

        Five levels is enough to go from a Gutenberg ID to an Overdrive ID
        (Gutenberg ID -> OCLC Work ID -> OCLC Number -> ISBN -> Overdrive ID)
        """
        _db = Session.object_session(self)
        identifier_ids = self.equivalent_identifier_ids(levels, threshold)
        return _db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids))

    @classmethod
    def missing_coverage_from(
            cls, _db, edition_data_sources, coverage_data_source):
        """Find Editions from `edition_data_source` whose primary
        identifiers have no CoverageRecord from
        `coverage_data_source`.

        e.g.

         gutenberg = DataSource.lookup(_db, DataSource.GUTENBERG)
         oclc_classify = DataSource.lookup(_db, DataSource.OCLC)
         missing_coverage_from(_db, gutenberg, oclc_classify)

        will find Editions that came from Project Gutenberg and
        have never been used as input to the OCLC Classify web
        service.

        """
        if isinstance(edition_data_sources, DataSource):
            edition_data_sources = [edition_data_sources]
        edition_data_source_ids = [x.id for x in edition_data_sources]
        join_clause = ((Edition.primary_identifier_id==CoverageRecord.identifier_id) &
                       (CoverageRecord.data_source_id==coverage_data_source.id))
        
        q = _db.query(Edition).outerjoin(
            CoverageRecord, join_clause)
        if edition_data_source_ids:
            q = q.filter(Edition.data_source_id.in_(edition_data_source_ids))
        q2 = q.filter(CoverageRecord.id==None)
        return q2


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

    def set_open_access_link(self):
        resource = self.best_open_access_link
        if resource and resource.representation:
            url = resource.representation.mirror_url
        else:
            url = None
        self.open_access_download_url = url

    def set_cover(self, resource):
        old_cover = self.cover
        old_cover_full_url = self.cover_full_url
        self.cover = resource
        self.cover_full_url = resource.representation.mirror_url

        # TODO: In theory there could be multiple scaled-down
        # versions of this representation and we need some way of
        # choosing between them. Right now we just pick the first one
        # that works.
        if (resource.representation.image_height
            and resource.representation.image_height <= self.MAX_THUMBNAIL_HEIGHT):
            # This image doesn't need a thumbnail.
            self.cover_thumbnail_url = resource.representation.mirror_url
        else:
            for scaled_down in resource.representation.thumbnails:
                if scaled_down.mirror_url and scaled_down.mirrored_at:
                    self.cover_thumbnail_url = scaled_down.mirror_url
                    break
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

    @property
    def best_open_access_link(self):
        """Find the best open-access Resource for this Edition."""
        pool = self.license_pool
        if not pool:
            return None

        best = None
        for resource in pool.open_access_links:
            if not any(
                    [resource.representation and
                     resource.representation.media_type.startswith(x) 
                     for x in Representation.SUPPORTED_BOOK_MEDIA_TYPES]):
                # This representation is not in a media type we 
                # support. We can't serve it, so we won't consider it.
                continue
                
            data_source_priority = resource.open_access_source_priority
            if not best or data_source_priority > best_priority:
                # Something is better than nothing.
                best = resource
                best_priority = data_source_priority
                continue

            if (best.data_source.name==DataSource.GUTENBERG
                and resource.data_source.name==DataSource.GUTENBERG
                and 'noimages' in best.representation.mirror_url
                and not 'noimages' in resource.representation.mirror_url):
                # A Project Gutenberg-ism: an epub without 'noimages'
                # in the filename is better than an epub with
                # 'noimages' in the filename.
                best = resource
                best_priority = data_source_priority
                continue

        return best

    def best_cover_within_distance(self, distance, threshold=0.5):
        _db = Session.object_session(self)
        flattened_data = [self.primary_identifier.id]
        if distance > 0:
            data = Identifier.recursively_equivalent_identifier_ids(
                _db, flattened_data, distance, threshold=threshold)
            flattened_data = Identifier.flatten_identifier_ids(data)

        return Identifier.best_cover_for(_db, flattened_data)

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
            author = authors[0].name
        else:
            # This may be an Edition that represents an item on a best-seller list
            # or something like that. In this case it wouldn't have any Contributor
            # objects, just an author string. Use that.
            author = self.sort_author or self.author
        return author

    def calculate_permanent_work_id(self, debug=False):
        title = self.title_for_permanent_work_id
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

    def better_primary_edition_than(self, champion):
        # Something is better than nothing.
        if not champion:
            return True

        # A edition with no license pool will only be chosen above,
        # under the 'something is better than nothing' rule.
        pool = self.license_pool
        if not pool:
            return False

        if not champion.license_pool:
            # An edition with a license pool beats a previous
            # champion-by-default without one.
            return True

        if pool.open_access:
            # Keep track of where the best open-access link for
            # this license pool comes from. It may affect which
            # license pool to use.
            open_access_resource = self.best_open_access_link
            if not open_access_resource:
                # An open-access edition with no usable download link will
                # only be chosen if there is no alternative.
                return False

            if not champion.license_pool.open_access:
                # Open access is better than not.
                return True

            # Both this pool and the champion are open access. But
            # open-access with a high-quality text beats open
            # access with a low-quality text.
            champion_resource = champion.best_open_access_link
            if not champion.best_open_access_link:
                champion_book_source_priority = -100
            else:
                champion_book_source_priority = champion_resource.open_access_source_priority
            book_source_priority = open_access_resource.open_access_source_priority
            if book_source_priority > champion_book_source_priority:
                if champion_resource:
                    champion_resource_url = champion_resource.url
                else:
                    champion_resource_url = 'None'
                logging.info(
                    "%s beats %s",
                    open_access_resource.url, champion_resource_url
                )
                return True
            elif book_source_priority < champion_book_source_priority:
                return False
            elif (self.data_source.name == DataSource.GUTENBERG
                  and champion.data_source.name == DataSource.GUTENBERG):
                # Higher Gutenberg numbers beat lower Gutenberg numbers.
                champion_id = int(
                    champion.primary_identifier.identifier)
                competitor_id = int(
                    self.primary_identifier.identifier)

                if competitor_id > champion_id:
                    champion = self
                    champion_book_source_priority = book_source_priority
                    logging.info(
                        "Gutenberg %d beats Gutenberg %d",
                        competitor_id, champion_id
                    )
                    return True

        # More licenses is better than fewer.
        if (self.license_pool.licenses_owned
            > champion.license_pool.licenses_owned):
            return True

        # More available licenses is better than fewer.
        if (self.license_pool.licenses_available
            > champion.license_pool.licenses_available):
            return True

        # Fewer patrons in the hold queue is better than more.
        if (self.license_pool.patrons_in_hold_queue
            < champion.license_pool.patrons_in_hold_queue):
            return True

        return False

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
        old_open_access_download_url = self.open_access_download_url
        old_cover = self.cover
        old_cover_full_url = self.cover_full_url

        if policy.set_edition_metadata:
            self.author, self.sort_author = self.calculate_author()
            self.sort_title = TitleProcessor.sort_title_for(self.title)
            self.calculate_permanent_work_id()
            self.set_open_access_link()
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
            or self.open_access_download_url != old_open_access_download_url
            or self.cover != old_cover
            or self.cover_full_url != old_cover_full_url
        ):
            changed = True

        if changed:
            # last_update_time tracks the last time the data 
            # actually changed.
            self.last_update_time = datetime.datetime.utcnow()

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
                args.append(self.cover.representation.mirror_url)
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
            if author.name and not author.display_name or not author.family_name:
                default_family, default_display = author.default_names()
            display_name = author.display_name or default_display or author.name
            family_name = author.family_name or default_family or author.name
            display_names.append([family_name, display_name])
            sort_names.append(author.name)
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
                    if not rep.mirrored_at and not rep.thumbnails:
                        logging.warn(
                            "Best cover for %r (%s) was never mirrored or thumbnailed!",
                            self.primary_identiifer, 
                            rep.url
                        )
                self.set_cover(best_cover)
                break

        # Whether or not we succeeded in setting the cover,
        # record the fact that we tried.
        CoverageRecord.add_for(
            self, data_source=self.data_source, 
            operation=CoverageRecord.CHOOSE_COVER_OPERATION
        )


Index("ix_editions_data_source_id_identifier_id", Edition.data_source_id, Edition.primary_identifier_id, unique=True)
Index("ix_editions_work_id_is_primary_for_work_id", Edition.work_id, Edition.is_primary_for_work)

class WorkGenre(Base):
    """An assignment of a genre to a work."""

    __tablename__ = 'workgenres'
    id = Column(Integer, primary_key=True)
    genre_id = Column(Integer, ForeignKey('genres.id'), index=True)
    work_id = Column(Integer, ForeignKey('works.id'), index=True)
    affinity = Column(Float, index=True, default=0)

    @classmethod
    def from_genre(cls, genre):
        wg = WorkGenre()
        wg.genre = genre
        return wg

    def __repr__(self):
        return "%s (%d%%)" % (self.genre.name, self.affinity*100)


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
        self.choose_summary=choose_summary, 
        self.calculate_quality=calculate_quality,
        self.choose_cover = choose_cover

        # We will regenerate OPDS entries if any of the metadata
        # changes, but if regenerate_opds_entries is True we will
        # _always_ do so. This is so we can regenerate _all_ the OPDS
        # entries if the OPDS presentation algorithm changes.
        self.regenerate_opds_entries = regenerate_opds_entries

        # Similarly for update_search_index.
        self.update_search_index = update_search_index

        self.verbose = verbose


class Work(Base):

    APPEALS_URI = "http://librarysimplified.org/terms/appeals/"

    CHARACTER_APPEAL = "Character"
    LANGUAGE_APPEAL = "Language"
    SETTING_APPEAL = "Setting"
    STORY_APPEAL = "Story"
    UNKNOWN_APPEAL = "Unknown"
    NOT_APPLICABLE_APPEAL = "Not Applicable"
    NO_APPEAL = "None"

    CURRENTLY_AVAILABLE = "currently_available"
    ALL = "all"

    # If no quality data is available for a work, it will be assigned
    # a default quality based on where we got it.
    #
    # The assumption is that a librarian would not have ordered a book
    # if it didn't meet a minimum level of quality.
    #
    # For data sources where librarians tend to order big packages of
    # books instead of selecting individual titles, the default
    # quality is lower. For data sources where there is no curation at
    # all, the default quality is zero.
    #
    # If there is absolutely no way to get quality data for a curated
    # data source, each work is assigned the minimum level of quality
    # necessary to show up in featured feeds.
    default_quality_by_data_source = {
        DataSource.GUTENBERG: 0,
        DataSource.OVERDRIVE: 0.4,
        DataSource.THREEM : 0.65,
        DataSource.AXIS_360: 0.65,
        DataSource.STANDARD_EBOOKS: 0.8,
        DataSource.UNGLUE_IT: 0.4,
        DataSource.PLYMPTON: 0.5,
    }

    __tablename__ = 'works'
    id = Column(Integer, primary_key=True)

    # One Work may have copies scattered across many LicensePools.
    license_pools = relationship("LicensePool", backref="work", lazy='joined')

    # A single Work may claim many Editions.
    editions = relationship("Edition", backref="work")

    # But for consistency's sake, a Work takes its presentation
    # metadata from a single Edition.

    clause = "and_(Edition.work_id==Work.id, Edition.is_primary_for_work==True)"
    primary_edition = relationship(
        "Edition", primaryjoin=clause, uselist=False, lazy='joined')

    # One Work may have many asosciated WorkCoverageRecords.
    coverage_records = relationship("WorkCoverageRecord", backref="work")

    # One Work may participate in many WorkGenre assignments.
    genres = association_proxy('work_genres', 'genre',
                               creator=WorkGenre.from_genre)
    work_genres = relationship("WorkGenre", backref="work",
                               cascade="all, delete-orphan")
    audience = Column(Unicode, index=True)
    target_age = Column(INT4RANGE, index=True)
    fiction = Column(Boolean, index=True)

    summary_id = Column(
        Integer, ForeignKey(
            'resources.id', use_alter=True, name='fk_works_summary_id'), 
        index=True)
    # This gives us a convenient place to store a cleaned-up version of
    # the content of the summary Resource.
    summary_text = Column(Unicode)

    # The overall suitability of this work for unsolicited
    # presentation to a patron. This is a calculated value taking both
    # rating and popularity into account.
    quality = Column(Numeric(4,3), index=True)

    # The overall rating given to this work.
    rating = Column(Float, index=True)

    # The overall current popularity of this work.
    popularity = Column(Float, index=True)

    # A random number associated with this work, used for sampling/
    random = Column(Numeric(4,3), index=True)

    appeal_type = Enum(CHARACTER_APPEAL, LANGUAGE_APPEAL, SETTING_APPEAL,
                       STORY_APPEAL, NOT_APPLICABLE_APPEAL, NO_APPEAL,
                       UNKNOWN_APPEAL, name="appeal")

    primary_appeal = Column(appeal_type, default=None, index=True)
    secondary_appeal = Column(appeal_type, default=None, index=True)

    appeal_character = Column(Float, default=None, index=True)
    appeal_language = Column(Float, default=None, index=True)
    appeal_setting = Column(Float, default=None, index=True)
    appeal_story = Column(Float, default=None, index=True)

    # The last time the availability or metadata changed for this Work.
    last_update_time = Column(DateTime, index=True)

    # This is set to True once all metadata and availability
    # information has been obtained for this Work. Until this is True,
    # the work will not show up in feeds.
    presentation_ready = Column(Boolean, default=False, index=True)

    # This is the last time we tried to make this work presentation ready.
    presentation_ready_attempt = Column(DateTime, default=None, index=True)

    # This is the error that occured while trying to make this Work
    # presentation ready. Until this is cleared, no further attempt
    # will be made to make the Work presentation ready.
    presentation_ready_exception = Column(Unicode, default=None, index=True)

    # A Work may be merged into one other Work.
    was_merged_into_id = Column(Integer, ForeignKey('works.id'), index=True)
    was_merged_into = relationship("Work", remote_side = [id])

    # A precalculated OPDS entry containing all metadata about this
    # work that would be relevant to display to a library patron.
    simple_opds_entry = Column(Unicode, default=None)

    # A precalculated OPDS entry containing all metadata about this
    # work that would be relevant to display in a machine-to-machine
    # integration context.
    verbose_opds_entry = Column(Unicode, default=None)

    @property
    def title(self):
        if self.primary_edition:
            return self.primary_edition.title
        return None

    @property
    def sort_title(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.sort_title or self.primary_edition.title

    @property
    def subtitle(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.subtitle

    @property
    def series(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.series

    @property
    def author(self):
        if self.primary_edition:
            return self.primary_edition.author
        return None

    @property
    def sort_author(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.sort_author or self.primary_edition.author

    @property
    def language(self):
        if self.primary_edition:
            return self.primary_edition.language
        return None

    @property
    def language_code(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.language_code

    @property
    def publisher(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.publisher

    @property
    def imprint(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.imprint

    @property
    def cover_full_url(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.cover_full_url

    @property
    def cover_thumbnail_url(self):
        if not self.primary_edition:
            return None
        return self.primary_edition.cover_thumbnail_url

    @property
    def target_age_string(self):
        lower = self.target_age.lower
        upper = self.target_age.upper
        if not upper and not lower:
            return None
        if lower and not upper:
            return str(lower)
        if upper and not lower:
            return str(upper)
        return "%s-%s" % (lower,upper)

    @property
    def has_open_access_license(self):
        return any(x.open_access for x in self.license_pools)

    def __repr__(self):
        return (u'%s "%s" (%s) %s %s (%s wr, %s lp)' % (
                self.id, self.title, self.author, ", ".join([g.name for g in self.genres]), self.language,
                len(self.editions), len(self.license_pools))).encode("utf8")

    def set_summary(self, resource):
        self.summary = resource
        # TODO: clean up the content
        if resource:
            self.summary_text = resource.representation.content
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.SUMMARY_OPERATION
        )

    @classmethod
    def feed_query(cls, _db, languages, availability=CURRENTLY_AVAILABLE):
        """Return a query against Work suitable for using in OPDS feeds."""
        q = _db.query(Work).join(Work.primary_edition)
        q = q.join(Work.license_pools).join(LicensePool.data_source).join(LicensePool.identifier)
        q = q.options(
            contains_eager(Work.license_pools),
            contains_eager(Work.primary_edition),
            contains_eager(Work.license_pools, LicensePool.data_source),
            contains_eager(Work.license_pools, LicensePool.edition),
            contains_eager(Work.license_pools, LicensePool.identifier),
            defer(Work.verbose_opds_entry),
            defer(Work.primary_edition, Edition.extra),
            defer(Work.license_pools, LicensePool.edition, Edition.extra),
        )
        if availability == cls.CURRENTLY_AVAILABLE:
            or_clause = or_(
                LicensePool.open_access==True,
                LicensePool.licenses_available > 0)
        else:
            or_clause = or_(
                LicensePool.open_access==True,
                LicensePool.licenses_owned > 0)
        q = q.filter(or_clause)
        q = q.filter(
            Edition.language.in_(languages),
            Work.was_merged_into == None,
            Work.presentation_ready == True,
            Edition.medium == Edition.BOOK_MEDIUM,
        )

        q = q.filter(LicensePool.delivery_mechanisms.any(
            DeliveryMechanism.default_client_can_fulfill==True)
        )
        return q

    @classmethod
    def with_genre(cls, _db, genre):
        """Find all Works classified under the given genre."""
        if isinstance(genre, basestring):
            genre, ignore = Genre.lookup(_db, genre)
        return _db.query(Work).join(WorkGenre).filter(WorkGenre.genre==genre)

    @classmethod
    def with_no_genres(self, q):
        """Modify a query so it finds only Works that are not classified under
        any genre."""
        q = q.outerjoin(Work.work_genres)
        q = q.options(contains_eager(Work.work_genres))
        q = q.filter(WorkGenre.genre==None)
        return q

    @classmethod
    def with_complaint(cls, _db):
        """Return query for Works that have at least one Complaint."""
        subquery = _db.query(
                Work.id,
                Complaint.type.label("complaint_type"),
                func.count(Complaint.type).label("complaint_type_count")
            ).\
            select_from(Work).\
            join(Work.license_pools, LicensePool.complaints).\
            group_by(Work.id, Complaint.type).\
            subquery()
        return _db.query(Work).\
            join(subquery, Work.id == subquery.c.id).\
            order_by(subquery.c.complaint_type_count.desc()).\
            add_columns(subquery.c.complaint_type, subquery.c.complaint_type_count)

    def all_editions(self, recursion_level=5):
        """All Editions identified by a Identifier equivalent to 
        any of the primary identifiers of this Work's Editions.

        `recursion_level` controls how far to go when looking for equivalent
        Identifiers.
        """
        _db = Session.object_session(self)
        identifier_ids = self.all_identifier_ids(recursion_level)
        q = _db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids))
        return q

    def all_identifier_ids(self, recursion_level=5):
        _db = Session.object_session(self)
        primary_identifier_ids = [
            x.primary_identifier.id for x in self.editions]
        identifier_ids = Identifier.recursively_equivalent_identifier_ids_flat(
            _db, primary_identifier_ids, recursion_level)
        return identifier_ids

    @property
    def language_code(self):
        """A single 2-letter language code for display purposes."""
        if not self.language:
            return None
        language = self.language
        if language in LanguageCodes.three_to_two:
            language = LanguageCodes.three_to_two[language]
        return language

    def similarity_to(self, other_work):
        """How likely is it that this Work describes the same book as the
        given Work (or Edition)?

        This is more accurate than Edition.similarity_to because we
        (hopefully) have a lot of Editions associated with each
        Work. If their metadata has a lot of overlap, the two Works
        are probably the same.
        """
        my_languages = Counter()
        my_authors = Counter()
        total_my_languages = 0
        total_my_authors = 0
        my_titles = []
        other_languages = Counter()
        total_other_languages = 0
        other_titles = []
        other_authors = Counter()
        total_other_authors = 0
        for record in self.editions:
            if record.language:
                my_languages[record.language] += 1
                total_my_languages += 1
            my_titles.append(record.title)
            for author in record.author_contributors:
                my_authors[author] += 1
                total_my_authors += 1

        if isinstance(other_work, Work):
            other_editions = other_work.editions
        else:
            other_editions = [other_work]

        for record in other_editions:
            if record.language:
                other_languages[record.language] += 1
                total_other_languages += 1
            other_titles.append(record.title)
            for author in record.author_contributors:
                other_authors[author] += 1
                total_other_authors += 1

        title_distance = MetadataSimilarity.histogram_distance(
            my_titles, other_titles)

        my_authors = MetadataSimilarity.normalize_histogram(
            my_authors, total_my_authors)
        other_authors = MetadataSimilarity.normalize_histogram(
            other_authors, total_other_authors)

        author_distance = MetadataSimilarity.counter_distance(
            my_authors, other_authors)

        my_languages = MetadataSimilarity.normalize_histogram(
            my_languages, total_my_languages)
        other_languages = MetadataSimilarity.normalize_histogram(
            other_languages, total_other_languages)

        if not other_languages or not my_languages:
            language_factor = 1
        else:
            language_distance = MetadataSimilarity.counter_distance(
                my_languages, other_languages)
            language_factor = 1-language_distance
        title_quotient = 1-title_distance
        author_quotient = 1-author_distance

        return language_factor * (
            (title_quotient * 0.80) + (author_quotient * 0.20))

    def merge_into(self, target_work, similarity_threshold=0.5):
        """This Work is replaced by target_work.

        The two works must be similar to within similarity_threshold,
        or nothing will happen.

        All of this work's Editions will be assigned to target_work,
        and it will be marked as merged into target_work.
        """
        _db = Session.object_session(self)
        similarity = self.similarity_to(target_work)
        if similarity < similarity_threshold:
            logging.info(
                "NOT MERGING %r into %r, similarity is only %.3f.",
                self, target_work, similarity
            )
        else:
            logging.info(
                "MERGING %r into %r, similarity is %.3f.",
                self, target_work, similarity
            )
            target_work.license_pools.extend(list(self.license_pools))
            target_work.editions.extend(list(self.editions))
            target_work.calculate_presentation()
            logging.info(
                "The resulting work from merge: %r", target_work)
            self.was_merged_into = target_work
            self.license_pools = []
            self.editions = []

    def all_cover_images(self):
        _db = Session.object_session(self)
        primary_identifier_ids = [
            x.primary_identifier.id for x in self.editions]
        data = Identifier.recursively_equivalent_identifier_ids(
            _db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = Identifier.flatten_identifier_ids(data)
        return Identifier.resources_for_identifier_ids(
            _db, flattened_data, Hyperlink.IMAGE).join(
            Resource.representation).filter(
                Representation.mirrored_at!=None).filter(
                Representation.scaled_at!=None).order_by(
                Resource.quality.desc())

    def all_descriptions(self):
        _db = Session.object_session(self)
        primary_identifier_ids = [
            x.primary_identifier.id for x in self.editions]
        data = Identifier.recursively_equivalent_identifier_ids(
            _db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = Identifier.flatten_identifier_ids(data)
        return Identifier.resources_for_identifier_ids(
            _db, flattened_data, Hyperlink.DESCRIPTION).filter(
                Resource.content != None).order_by(
                Resource.quality.desc())

    def set_primary_edition(self):
        """Which of this Work's Editions should be used as the default?
        """
        old_primary = self.primary_edition
        champion = None
        old_champion = None
        champion_book_source_priority = None
        best_text_source = None

        for edition in self.editions:
            if edition.better_primary_edition_than(champion):
                champion = edition

        for edition in self.editions:
            # There can be only one.
            if edition != champion:
                edition.is_primary_for_work = False
            else:
                edition.is_primary_for_work = True
                self.primary_edition = edition

    def calculate_presentation(self, policy=None, search_index_client=None):
        """Determine the following information:
        
        * Which Edition is the 'primary'. The default view of the
        Work will be taken from the primary Edition.

        * Subject-matter classifications for the work.
        * Whether or not the work is fiction.
        * The intended audience for the work.
        * The best available summary for the work.
        * The overall popularity of the work.
        """
        policy = policy or PresentationCalculationPolicy()

        # Gather information up front so we can see if anything
        # actually changed.
        changed = False
        edition_metadata_changed = False
        classification_changed = False

        primary_edition = self.primary_edition
        summary = self.summary
        summary_text = self.summary_text
        quality = self.quality

        if policy.choose_edition or not self.primary_edition:
            self.set_primary_edition()
            WorkCoverageRecord.add_for(
                self, operation=WorkCoverageRecord.CHOOSE_EDITION_OPERATION
            )


        # The privileged data source may short-circuit the process of
        # finding a good cover or description.
        privileged_data_source = None
        if self.primary_edition:
            privileged_data_source = self.primary_edition.data_source
            # Descriptions from Gutenberg are useless, so it can't
            # be a privileged data source.
            if privileged_data_source.name == DataSource.GUTENBERG:
                privileged_data_source = None

        if self.primary_edition:
            edition_metadata_changed = self.primary_edition.calculate_presentation(
                policy
            )

        if policy.classify or policy.choose_summary or policy.calculate_quality:
            # Find all related IDs that might have associated descriptions,
            # classifications, or measurements.
            _db = Session.object_session(self)
            primary_identifier_ids = [
                x.primary_identifier.id for x in self.editions
            ]
            data = Identifier.recursively_equivalent_identifier_ids(
                _db, primary_identifier_ids, 5, threshold=0.5
            )
            flattened_data = Identifier.flatten_identifier_ids(data)
        else:
            flattened_data = []

        if policy.classify:
            classification_changed = self.assign_genres(flattened_data)
            WorkCoverageRecord.add_for(
                self, operation=WorkCoverageRecord.CLASSIFY_OPERATION
            )

        if policy.choose_summary:
            summary, summaries = Identifier.evaluate_summary_quality(
                _db, flattened_data, privileged_data_source
            )
            # TODO: clean up the content
            self.set_summary(summary)      

        if policy.calculate_quality:
            default_quality = 0
            if self.primary_edition:
                data_source_name = self.primary_edition.data_source.name
                default_quality = self.default_quality_by_data_source.get(
                    data_source_name, 0
                )
            self.calculate_quality(flattened_data, default_quality)

        if self.summary_text:
            if isinstance(self.summary_text, unicode):
                new_summary_text = self.summary_text
            else:
                new_summary_text = self.summary_text.decode("utf8")
        else:
            new_summary_text = self.summary_text

        changed = (
            edition_metadata_changed or
            classification_changed or
            primary_edition != self.primary_edition or
            summary != self.summary or
            summary_text != new_summary_text or
            float(quality) != float(self.quality)
        )

        if changed:
            # last_update_time tracks the last time the data actually
            # changed, not the last time we checked whether or not to
            # change it.
            self.last_update_time = datetime.datetime.utcnow()

        if changed or policy.regenerate_opds_entries:
            self.calculate_opds_entries()

        if changed or policy.update_search_index:
            if not search_index_client:
                search_index_client = ExternalSearchIndex()
            self.update_external_index(search_index_client)

        # Now that everything's calculated, print it out.
        if policy.verbose:            
            if changed:
                changed = "changed"
                representation = self.detailed_representation
            else:
                changed = "unchanged"
                representation = repr(self)                
            logging.info("Presentation %s for work: %s", changed, representation)

    @property
    def detailed_representation(self):
        """A description of this work more detailed than repr()"""
        l = ["%s (by %s)" % (self.title, self.author)]
        l.append(" language=%s" % self.language)
        l.append(" quality=%s" % self.quality)

        if self.primary_edition and self.primary_edition.primary_identifier:
            primary_identifier = self.primary_edition.primary_identifier
        else:
            primary_identifier=None
        l.append(" primary id=%s" % primary_identifier)
        if self.fiction:
            fiction = "Fiction"
        elif self.fiction == False:
            fiction = "Nonfiction"
        else:
            fiction = "???"
        if self.target_age and (self.target_age.upper or self.target_age.lower):
            target_age = " age=" + self.target_age_string
        else:
            target_age = ""
        l.append(" %(fiction)s a=%(audience)s%(target_age)r" % (
                dict(fiction=fiction,
                     audience=self.audience, target_age=target_age)))
        l.append(" " + ", ".join(repr(wg) for wg in self.work_genres))

        def _ensure(s):
            if not s:
                return ""
            elif isinstance(s, unicode):
                return s
            else:
                return s.decode("utf8", "replace")

        if self.summary:
            snippet = _ensure(self.summary.representation.content)[:100]
            d = " Description (%.2f) %s" % (self.summary.quality, snippet)
            l.append(d)

        l = [_ensure(s) for s in l]
        return u"\n".join(l)

    def calculate_opds_entries(self, verbose=True):
        from opds import (
            AcquisitionFeed,
            Annotator,
            VerboseAnnotator,
        )
        _db = Session.object_session(self)
        simple = AcquisitionFeed.single_entry(_db, self, Annotator,
                                              force_create=True)
        if simple is not None:
            self.simple_opds_entry = etree.tostring(simple)
        verbose = AcquisitionFeed.single_entry(_db, self, VerboseAnnotator, 
                                               force_create=True)
        if verbose is not None:
            self.verbose_opds_entry = etree.tostring(verbose)
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.GENERATE_OPDS_OPERATION
        )
        # print self.id, self.simple_opds_entry, self.verbose_opds_entry


    def update_external_index(self, client):
        args = dict(index=client.works_index,
                    doc_type=client.work_document_type,
                    id=self.id)
        if not client.works_index:
            # There is no index set up on this instance.
            return
        if self.presentation_ready:
            doc = self.to_search_document()
            if doc:
                args['body'] = doc
                if logging.getLogger().level == logging.DEBUG:
                    logging.debug(
                        "Indexed work %d (%s): %r", self.id, self.title, doc
                    )
                else:
                    logging.info("Indexed work %d (%s)", self.id, self.title)
                client.index(**args)
            else:
                logging.warn(
                    "Could not generate a search document for allegedly presentation-ready work %d (%s).",
                    self.id, self.title
                )
        else:
            if client.exists(**args):
                client.delete(**args)
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        )

    def set_presentation_ready(self, as_of=None):
        as_of = as_of or datetime.datetime.utcnow()
        self.presentation_ready = True
        self.presentation_ready_exception = None
        self.presentation_ready_attempt = as_of
        self.random = random.random()

    def set_presentation_ready_based_on_content(
            self, require_author=True, require_thumbnail=True
    ):
        """Set this work as presentation ready, if it appears to
        be ready based on its data.

        Presentation ready means the book is ready to be shown to
        patrons and (pending availability) checked out. It doesn't
        necessarily mean the presentation is complete.

        A work with no summary can still be presentation ready,
        since many public domain books have no summary.

        A work with no cover can be presentation ready 

        A work with no genres can be presentation ready, but we do
        at least need to know whether it's fiction or nonfiction.
        """
        if (not self.primary_edition
            or not self.license_pools
            or not self.title
            or (require_author and not self.primary_edition.author)
            or not self.language
            or self.fiction is None
            or (
                require_thumbnail and not (
                    self.cover_thumbnail_url
                    or self.primary_edition.no_known_cover
                )
            )
        ):
            self.presentation_ready = False
        else:
            self.set_presentation_ready()

    def calculate_quality(self, flattened_data, default_quality=0):
        _db = Session.object_session(self)
        quantities = [Measurement.POPULARITY, Measurement.RATING,
                      Measurement.DOWNLOADS, Measurement.QUALITY]
        measurements = _db.query(Measurement).filter(
            Measurement.identifier_id.in_(flattened_data)).filter(
                Measurement.is_most_recent==True).filter(
                    Measurement.quantity_measured.in_(quantities)).all()

        self.quality = Measurement.overall_quality(
            measurements, default_value=default_quality)
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.QUALITY_OPERATION
        )

    def assign_genres(self, identifier_ids, cutoff=0.15):
        """Set classification information for this work based on the
        given flattened set of equivalent identifiers (`identifier_ids`).

        :return: A boolean explaining whether or not any data actually
        changed.
        """
        classifier = WorkClassifier(self)

        old_fiction = self.fiction
        old_audience = self.audience
        old_target_age = self.target_age

        _db = Session.object_session(self)
        classifications = Identifier.classifications_for_identifier_ids(
            _db, identifier_ids
        )
        for classification in classifications:
            classifier.add(classification)

        (genre_weights, self.fiction, self.audience, 
         self.target_age) = classifier.classify

        workgenres, workgenres_changed = self.assign_genres_from_weights(
            genre_weights
        )

        classification_changed = (
            workgenres_changed or 
            old_fiction != self.fiction or
            old_audience != self.audience or
            numericrange_to_tuple(old_target_age) != numericrange_to_tuple(self.target_age)
        )

        return classification_changed

    def assign_genres_from_weights(self, genre_weights):
        # Assign WorkGenre objects to the remainder.
        changed = False
        _db = Session.object_session(self)
        total_genre_weight = float(sum(genre_weights.values()))
        workgenres = []
        current_workgenres = _db.query(WorkGenre).filter(WorkGenre.work==self)
        by_genre = dict()
        for wg in current_workgenres:
            by_genre[wg.genre] = wg
        for g, score in genre_weights.items():
            affinity = score / total_genre_weight
            if not isinstance(g, Genre):
                g, ignore = Genre.lookup(_db, g.name)
            if g in by_genre:
                wg = by_genre[g]
                is_new = False
                del by_genre[g]
            else:
                wg, is_new = get_one_or_create(
                    _db, WorkGenre, work=self, genre=g)
            if is_new or round(wg.affinity,2) != round(affinity, 2):
                changed = True
            wg.affinity = affinity
            workgenres.append(wg)

        # Any WorkGenre objects left over represent genres the Work
        # was once classified under, but is no longer. Delete them.
        for wg in by_genre.values():
            _db.delete(wg)
            changed = True
        return workgenres, changed


    def assign_appeals(self, character, language, setting, story,
                       cutoff=0.20):
        """Assign the given appeals to the corresponding database fields,
        as well as calculating the primary and secondary appeal.
        """
        self.appeal_character = character
        self.appeal_language = language
        self.appeal_setting = setting
        self.appeal_story = story

        c = Counter()
        c[self.CHARACTER_APPEAL] = character
        c[self.LANGUAGE_APPEAL] = language
        c[self.SETTING_APPEAL] = setting
        c[self.STORY_APPEAL] = story
        primary, secondary = c.most_common(2)
        if primary[1] > cutoff:
            self.primary_appeal = primary[0]
        else:
            self.primary_appeal = self.UNKNOWN_APPEAL

        if secondary[1] > cutoff:
            self.secondary_appeal = secondary[0]
        else:
            self.secondary_appeal = self.NO_APPEAL

    def to_search_document(self):
        """Generate a search document for this Work."""

        _db = Session.object_session(self)
        if not self.primary_edition:
            return None
        doc = dict(_id=self.id,
                   title=self.title,
                   subtitle=self.subtitle,
                   series=self.series,
                   language=self.language,
                   sort_title=self.sort_title, 
                   author=self.author,
                   sort_author=self.sort_author,
                   medium=self.primary_edition.medium,
                   publisher=self.publisher,
                   imprint=self.imprint,
                   permanent_work_id=self.primary_edition.permanent_work_id,
                   fiction= "Fiction" if self.fiction else "Nonfiction",
                   audience=self.audience.replace(" ", ""),
                   summary = self.summary_text,
                   quality = self.quality,
                   rating = self.rating,
                   popularity = self.popularity,
                   was_merged_into_id = self.was_merged_into_id,
               )

        contribution_desc = []
        doc['contributors'] = contribution_desc
        for contribution in self.primary_edition.contributions:
            contributor = contribution.contributor
            contribution_desc.append(
                dict(name=contributor.name, family_name=contributor.family_name,
                     role=contribution.role))

        # identifier_ids = self.all_identifier_ids()
        # classifications = Identifier.classifications_for_identifier_ids(
        #     _db, identifier_ids)
        # by_scheme_and_term = Counter()
        # classification_total_weight = 0.0
        # for c in classifications:
        #     subject = c.subject
        #     if subject.type in Subject.uri_lookup:
        #         scheme = Subject.uri_lookup[subject.type]
        #         term = subject.name or subject.identifier
        #         if not term:
        #             # There's no text to search for.
        #             continue
        #         key = (scheme, term)
        #         by_scheme_and_term[key] += c.weight
        #         classification_total_weight += c.weight

        classification_desc = []
        doc['classifications'] = classification_desc
        # for (scheme, term), weight in by_scheme_and_term.items():
        #     classification_desc.append(
        #         dict(scheme=scheme, term=term,
        #              weight=weight/classification_total_weight))


        for workgenre in self.work_genres:
            classification_desc.append(
                dict(scheme=Subject.SIMPLIFIED_GENRE, name=workgenre.genre.name,
                     term=workgenre.genre.id, weight=workgenre.affinity))

        # for term, weight in (
        #         (Work.CHARACTER_APPEAL, self.appeal_character),
        #         (Work.LANGUAGE_APPEAL, self.appeal_language),
        #         (Work.SETTING_APPEAL, self.appeal_setting),
        #         (Work.STORY_APPEAL, self.appeal_story)):
        #     if weight:
        #         classification_desc.append(
        #             dict(scheme=Work.APPEALS_URI, term=term,
        #                  weight=weight))

        if self.target_age:
            doc['target_age'] = {}
            if self.target_age.lower:
                doc['target_age']['lower'] = self.target_age.lower
            if self.target_age.upper:
                doc['target_age']['upper'] = self.target_age.upper

        return doc

    @classmethod
    def restrict_to_custom_lists_from_data_source(
            cls, _db, base_query, data_source, on_list_as_of=None):
        """Annotate a query that joins Work against Edition to match only
        Works that are on a custom list from the given data source."""

        condition = CustomList.data_source==data_source
        return cls._restrict_to_customlist_subquery_condition(
            _db, base_query, condition, on_list_as_of)

    @classmethod
    def restrict_to_custom_lists(
            cls, _db, base_query, custom_lists, on_list_as_of=None):
        """Annotate a query that joins Work against Edition to match only
        Works that are on one of the given custom lists."""
        condition = CustomList.id.in_([x.id for x in custom_lists])
        return cls._restrict_to_customlist_subquery_condition(
            _db, base_query, condition, on_list_as_of)

    @classmethod
    def _restrict_to_customlist_subquery_condition(
            cls, _db, base_query, condition, on_list_as_of=None):
        """Annotate a query that joins Work against Edition to match only
        Works that are on a custom list from the given data source."""
        # Find works that are on a list that meets the given condition.
        qu = base_query.join(LicensePool.custom_list_entries).join(
            CustomListEntry.customlist)
        if on_list_as_of:
            qu = qu.filter(
                CustomListEntry.most_recent_appearance >= on_list_as_of)
        qu = qu.filter(condition)
        return qu


# Used for quality filter queries.
Index("ix_works_audience_target_age_quality_random", Work.audience, Work.target_age, Work.quality, Work.random)
Index("ix_works_audience_fiction_quality_random", Work.audience, Work.fiction, Work.quality, Work.random)

class Measurement(Base):
    """A  measurement of some numeric quantity associated with a
    Identifier.
    """
    __tablename__ = 'measurements'

    # Some common measurement types
    POPULARITY = u"http://librarysimplified.org/terms/rel/popularity"
    QUALITY = u"http://librarysimplified.org/terms/rel/quality"
    PUBLISHED_EDITIONS = u"http://librarysimplified.org/terms/rel/editions"
    HOLDINGS = u"http://librarysimplified.org/terms/rel/holdings"
    RATING = u"http://schema.org/ratingValue"
    DOWNLOADS = u"https://schema.org/UserDownloads"
    PAGE_COUNT = u"https://schema.org/numberOfPages"
    AWARDS = u"http://librarysimplified.org/terms/rel/awards"

    GUTENBERG_FAVORITE = u"http://librarysimplified.org/terms/rel/lists/gutenberg-favorite"

    # If a book's popularity measurement is found between index n and
    # index n+1 on this list, it is in the nth percentile for
    # popularity and its 'popularity' value should be n * 0.01.
    # 
    # These values are empirically determined and may change over
    # time.
    POPULARITY_PERCENTILES = {
        DataSource.OVERDRIVE : [1, 1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 9, 10, 10, 11, 12, 13, 14, 15, 15, 16, 18, 19, 20, 21, 22, 24, 25, 26, 28, 30, 31, 33, 35, 37, 39, 41, 43, 46, 48, 51, 53, 56, 59, 63, 66, 70, 74, 78, 82, 87, 92, 97, 102, 108, 115, 121, 128, 135, 142, 150, 159, 168, 179, 190, 202, 216, 230, 245, 260, 277, 297, 319, 346, 372, 402, 436, 478, 521, 575, 632, 702, 777, 861, 965, 1100, 1248, 1428, 1665, 2020, 2560, 3535, 5805],
        DataSource.AMAZON : [14937330, 1974074, 1702163, 1553600, 1432635, 1327323, 1251089, 1184878, 1131998, 1075720, 1024272, 978514, 937726, 898606, 868506, 837523, 799879, 770211, 743194, 718052, 693932, 668030, 647121, 627642, 609399, 591843, 575970, 559942, 540713, 524397, 511183, 497576, 483884, 470850, 458438, 444475, 432528, 420088, 408785, 398420, 387895, 377244, 366837, 355406, 344288, 333747, 324280, 315002, 305918, 296420, 288522, 279185, 270824, 262801, 253865, 246224, 238239, 230537, 222611, 215989, 208641, 202597, 195817, 188939, 181095, 173967, 166058, 160032, 153526, 146706, 139981, 133348, 126689, 119201, 112447, 106795, 101250, 96534, 91052, 85837, 80619, 75292, 69957, 65075, 59901, 55616, 51624, 47598, 43645, 39403, 35645, 31795, 27990, 24496, 20780, 17740, 14102, 10498, 7090, 3861],

        # This is as measured by the criteria defined in
        # ContentCafeSOAPClient.estimate_popularity(), in which
        # popularity is the maximum of a) the largest number of books
        # ordered in a single month within the last year, or b)
        # one-half the largest number of books ever ordered in a
        # single month.
        DataSource.CONTENT_CAFE : [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 8, 9, 10, 11, 14, 18, 25, 41, 125, 387]

        # This is a percentile list of OCLC Work IDs and OCLC Numbers
        # associated with Project Gutenberg texts via OCLC Linked
        # Data.
        #
        # TODO: Calculate a separate distribution for more modern works.
        # DataSource.OCLC_LINKED_DATA : [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 7, 8, 8, 9, 10, 11, 12, 14, 15, 18, 21, 29, 41, 81],
    }

    DOWNLOAD_PERCENTILES = {
        DataSource.GUTENBERG : [0, 1, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 12, 12, 12, 13, 14, 14, 15, 15, 16, 16, 17, 18, 18, 19, 19, 20, 21, 21, 22, 23, 23, 24, 25, 26, 27, 28, 28, 29, 30, 32, 33, 34, 35, 36, 37, 38, 40, 41, 43, 45, 46, 48, 50, 52, 55, 57, 60, 62, 65, 69, 72, 76, 79, 83, 87, 93, 99, 106, 114, 122, 130, 140, 152, 163, 179, 197, 220, 251, 281, 317, 367, 432, 501, 597, 658, 718, 801, 939, 1065, 1286, 1668, 2291, 4139]
    }

    RATING_SCALES = {
        DataSource.OVERDRIVE : [1, 5],
        DataSource.AMAZON : [1, 5],
        DataSource.UNGLUE_IT: [1, 5],
    }

    id = Column(Integer, primary_key=True)

    # A Measurement is always associated with some Identifier.
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)

    # A Measurement always comes from some DataSource.
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True)

    # The quantity being measured.
    quantity_measured = Column(Unicode, index=True)

    # The measurement itself.
    value = Column(Float)

    # The measurement normalized to a 0...1 scale.
    _normalized_value = Column(Float, name="normalized_value")

    # How much weight should be assigned this measurement, relative to
    # other measurements of the same quantity from the same source.
    weight = Column(Float, default=1)

    # When the measurement was taken
    taken_at = Column(DateTime, index=True)
    
    # True if this is the most recent measurement of this quantity for
    # this Identifier.
    #
    is_most_recent = Column(Boolean, index=True)

    def __repr__(self):
        return "%s(%r)=%s (norm=%.2f)" % (
            self.quantity_measured, self.identifier, self.value,
            self.normalized_value or 0)

    @classmethod
    def overall_quality(cls, measurements, popularity_weight=0.3,
                        rating_weight=0.7, default_value=0):
        """Turn a bunch of measurements into an overall measure of quality."""
        if popularity_weight + rating_weight != 1.0:
            raise ValueError(
                "Popularity weight and rating weight must sum to 1! (%.2f + %.2f)" % (
                    popularity_weight, rating_weight)
        )
        popularities = []
        ratings = []
        qualities = []
        for m in measurements:
            l = None
            if m.quantity_measured in (cls.POPULARITY, cls.DOWNLOADS):
                l = popularities
            elif m.quantity_measured == cls.RATING:
                l = ratings
            elif m.quantity_measured == cls.QUALITY:
                l = qualities
            if l is not None:
                l.append(m)
        popularity = cls._average_normalized_value(popularities)
        rating = cls._average_normalized_value(ratings)
        quality = cls._average_normalized_value(qualities)
        if popularity is None and rating is None and quality is None:
            # We have absolutely no idea about the quality of this work.
            return default_value
        if popularity is not None and rating is None and quality is None:
            # Our idea of the quality depends entirely on the work's popularity.
            return popularity
        if rating is not None and popularity is None and quality is None:
            # Our idea of the quality depends entirely on the work's rating.
            return rating
        if quality is not None and rating is None and popularity is None:
            # Our idea of the quality depends entirely on the work's quality scores.
            return quality

        # We have both popularity and rating.
        if popularity is None:
            final = rating
        if rating is None:
            final = popularity
        else:
            final = (popularity * popularity_weight) + (rating * rating_weight)
            logging.debug(
                "(%.2f * %.2f) + (%.2f * %.2f) = %.2f", 
                popularity, popularity_weight, rating, rating_weight, final
            )
        if quality:
            logging.debug("Popularity+Rating: %.2f, Quality: %.2f" % (final, quality))
            final = (final / 2) + (quality / 2)
            logging.debug("Final value: %.2f" % final)
        return final

    @classmethod
    def _average_normalized_value(cls, measurements):
        num_measurements = 0
        measurement_total = 0
        for m in measurements:
            v = m.normalized_value
            if v is None:
                continue
            num_measurements += m.weight
            measurement_total += (v * m.weight)
        if num_measurements:
            return measurement_total / num_measurements
        else:
            return None

    @property
    def normalized_value(self):
        if self._normalized_value:
            pass
        elif not self.value:
            return None
        elif (self.quantity_measured == self.POPULARITY
              and self.data_source.name in self.POPULARITY_PERCENTILES):
            d = self.POPULARITY_PERCENTILES[self.data_source.name]
            position = bisect.bisect_left(d, self.value)
            self._normalized_value = position * 0.01            
        elif (self.quantity_measured == self.DOWNLOADS
              and self.data_source.name in self.DOWNLOAD_PERCENTILES):
            d = self.DOWNLOAD_PERCENTILES[self.data_source.name]
            position = bisect.bisect_left(d, self.value)
            self._normalized_value = position * 0.01            
        elif (self.quantity_measured == self.RATING
              and self.data_source.name in self.RATING_SCALES):
            scale_min, scale_max = self.RATING_SCALES[self.data_source.name]
            width = float(scale_max-scale_min)
            value = self.value-scale_min
            self._normalized_value = value / width
        elif self.data_source.name == DataSource.METADATA_WRANGLER:
            # Data from the metadata wrangler comes in pre-normalized.
            self._normalized_value = self.value
            
        return self._normalized_value


class LicensePoolDeliveryMechanism(Base):
    """A mechanism for delivering a specific book.

    This is mostly an association class between LicensePool and
    DeliveryMechanism, but it also may incorporate a specific Resource
    (i.e. a static link to a downloadable file) which explains exactly
    where to go for delivery.
    """
    __tablename__ = 'licensepooldeliveries'

    id = Column(Integer, primary_key=True)

    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True,
        nullable=False
    )

    delivery_mechanism_id = Column(
        Integer, ForeignKey('deliverymechanisms.id'), 
        index=True,
        nullable=False
    )

    resource_id = Column(Integer, ForeignKey('resources.id'), nullable=True)

    # One LicensePoolDeliveryMechanism may fulfill many Loans.
    fulfills = relationship("Loan", backref="fulfillment")

    def __repr__(self):
        return "%r %r" % (self.license_pool, self.delivery_mechanism)

class Hyperlink(Base):
    """A link between an Identifier and a Resource."""

    __tablename__ = 'hyperlinks'

    # Some common link relations.
    CANONICAL = u"canonical"
    OPEN_ACCESS_DOWNLOAD = u"http://opds-spec.org/acquisition/open-access"
    IMAGE = u"http://opds-spec.org/image"
    THUMBNAIL_IMAGE = u"http://opds-spec.org/image/thumbnail"
    SAMPLE = u"http://opds-spec.org/acquisition/sample"
    ILLUSTRATION = u"http://librarysimplified.org/terms/rel/illustration"
    REVIEW = u"http://schema.org/Review"
    DESCRIPTION = u"http://schema.org/description"
    SHORT_DESCRIPTION = u"http://librarysimplified.org/terms/rel/short-description"
    AUTHOR = u"http://schema.org/author"

    # TODO: Is this the appropriate relation?
    DRM_ENCRYPTED_DOWNLOAD = u"http://opds-spec.org/acquisition/"

    id = Column(Integer, primary_key=True)

    # A Hyperlink is always associated with some Identifier.
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True, nullable=False)

    # The DataSource through which this link was discovered.
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True, nullable=False)

    # A Resource may also be associated with some LicensePool which
    # controls scarce access to it.
    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True)

    # The link relation between the Identifier and the Resource.
    rel = Column(Unicode, index=True, nullable=False)

    # The Resource on the other end of the link.
    resource_id = Column(
        Integer, ForeignKey('resources.id'), index=True, nullable=False)

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


class Resource(Base):
    """An external resource that may be mirrored locally."""

    __tablename__ = 'resources'

    # How many votes is the initial quality estimate worth?
    ESTIMATED_QUALITY_WEIGHT = 5

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

    # A calculated value for the quality of this resource, based on an
    # algorithmic treatment of its content.
    estimated_quality = Column(Float)

    # The average of human-entered values for the quality of this
    # resource.
    voted_quality = Column(Float)

    # How many votes contributed to the voted_quality value. This lets
    # us scale new votes proportionately while keeping only two pieces
    # of information.
    votes_for_quality = Column(Integer)

    # A combination of the calculated quality value and the
    # human-entered quality value.
    quality = Column(Float, index=True)

    # URL must be unique.
    __table_args__ = (
        UniqueConstraint('url'),
    )


    # Some sources of open-access ebooks are better than others. This
    # list shows which sources we prefer, in ascending order of
    # priority. unglue.it is lowest priority because it tends to
    # aggregate books from other sources. We prefer books from their
    # original sources.
    OPEN_ACCESS_SOURCE_PRIORITY = [
        DataSource.UNGLUE_IT,
        DataSource.GUTENBERG,
        DataSource.GUTENBERG_EPUB_GENERATOR,
        DataSource.PROJECT_GITENBERG,
        DataSource.PLYMPTON,
        DataSource.STANDARD_EBOOKS,
    ]

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

    def set_mirrored_elsewhere(self, media_type):
        """We don't need our own copy of this resource's representation--
        a copy of it has been mirrored already.
        """
        _db = Session.object_session(self)
        if not self.representation:
            self.representation, is_new = get_one_or_create(
                _db, Representation, url=self.url, media_type=media_type)
        self.representation.mirror_url = self.url
        self.representation.set_as_mirrored()

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
        total_quality = self.voted_quality * self.votes_for_quality
        total_quality += (quality * weight)
        self.votes_for_quality += weight
        self.voted_quality = total_quality / float(self.votes_for_quality)
        self.update_quality()

    @property
    def open_access_source_priority(self):
        """What priority does this resource's source have in
        our list of open-access content sources?
        
        e.g. GITenberg books are prefered over Gutenberg books,
        because there's a defined process for fixing errors and they
        are more likely to have good cover art.
        """
        try:
            priority = self.OPEN_ACCESS_SOURCE_PRIORITY.index(
                self.data_source.name)
        except ValueError, e:
            # The source of this download is not mentioned in our
            # priority list. Treat it as the lowest priority.
            priority = -1
        return priority


    def update_quality(self):
        """Combine `estimated_quality` with `voted_quality` to form `quality`.
        """
        estimated_weight = self.ESTIMATED_QUALITY_WEIGHT
        votes_for_quality = self.votes_for_quality or 0
        total_weight = estimated_weight + votes_for_quality

        total_quality = (((self.estimated_quality or 0) * self.ESTIMATED_QUALITY_WEIGHT) + 
                         ((self.voted_quality or 0) * votes_for_quality))
        self.quality = total_quality / float(total_weight)

    def set_representation(self, media_type, content, uri=None,
                           content_path=None):

        if not uri:
            uri = self.generic_uri
        representation, ignore = get_one_or_create(
            _db, Representation, url=uri, media_type=media_type)
        representation.set_fetched_content(content, content_path)
        self.representation = representation
        

class Genre(Base):
    """A subject-matter classification for a book.

    Much, much more general than Classification.
    """
    __tablename__ = 'genres'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)

    # One Genre may have affinity with many Subjects.
    subjects = relationship("Subject", backref="genre")

    # One Genre may participate in many WorkGenre assignments.
    works = association_proxy('work_genres', 'work')

    work_genres = relationship("WorkGenre", backref="genre", 
                               cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Genre %s (%d subjects, %d works, %d subcategories)>" % (
            self.name, len(self.subjects), len(self.works),
            len(classifier.genres[self.name].subgenres))

    @classmethod
    def load_all(cls, _db):
        """Load all Genre objects into the cache associated with the
        database connection.
        """
        if not hasattr(_db, '_genre_cache'):
            _db._genre_cache = dict()
        for g in _db.query(Genre):
            _db._genre_cache[g.name] = g

    @classmethod
    def lookup(cls, _db, name, autocreate=False):
        if not hasattr(_db, '_genre_cache'):
            _db._genre_cache = dict()
        if isinstance(name, Genre):
            return name, False
        if isinstance(name, GenreData):
            name = name.name
        if name in _db._genre_cache:
            return _db._genre_cache[name], False
        if autocreate:
            m = get_one_or_create
        else:
            m = get_one
        result = m(_db, Genre, name=name)
        if result is None:
            logging.getLogger().error('"%s" is not a recognized genre.', name)
        if isinstance(result, tuple):
            _db._genre_cache[name] = result[0]
            return result
        else:
            _db._genre_cache[name] = result
            return result, False

    @property
    def genredata(self):
        return classifier.genres[self.name]

    @property
    def subgenres(self):
        for genre in self.self_and_subgenres:
            if genre != self:
                yield genre

    @property
    def self_and_subgenres(self):
        _db = Session.object_session(self)
        genres = []
        for genre_data in self.genredata.self_and_subgenres:
            genres.append(self.lookup(_db, genre_data.name)[0])
        return genres

    @property
    def default_fiction(self):
        return classifier.genres[self.name].is_fiction

class Subject(Base):
    """A subject under which books might be classified."""

    # Types of subjects.
    LCC = Classifier.LCC              # Library of Congress Classification
    LCSH = Classifier.LCSH            # Library of Congress Subject Headings
    FAST = Classifier.FAST
    DDC = Classifier.DDC              # Dewey Decimal Classification
    OVERDRIVE = Classifier.OVERDRIVE  # Overdrive's classification system
    THREEM = Classifier.THREEM  # 3M's classification system
    BISAC = Classifier.BISAC
    TAG = Classifier.TAG   # Folksonomic tags.
    FREEFORM_AUDIENCE = Classifier.FREEFORM_AUDIENCE
    NYPL_APPEAL = Classifier.NYPL_APPEAL

    AXIS_360_AUDIENCE = Classifier.AXIS_360_AUDIENCE
    GRADE_LEVEL = Classifier.GRADE_LEVEL
    AGE_RANGE = Classifier.AGE_RANGE
    LEXILE_SCORE = Classifier.LEXILE_SCORE
    ATOS_SCORE = Classifier.ATOS_SCORE
    INTEREST_LEVEL = Classifier.INTEREST_LEVEL

    GUTENBERG_BOOKSHELF = Classifier.GUTENBERG_BOOKSHELF
    TOPIC = Classifier.TOPIC
    PLACE = Classifier.PLACE
    PERSON = Classifier.PERSON
    ORGANIZATION = Classifier.ORGANIZATION
    SIMPLIFIED_GENRE = "http://librarysimplified.org/terms/genres/Simplified/"

    by_uri = {
        SIMPLIFIED_GENRE : SIMPLIFIED_GENRE,
        "http://librarysimplified.org/terms/genres/Overdrive/" : OVERDRIVE,
        "http://librarysimplified.org/terms/genres/3M/" : THREEM,
        "http://id.worldcat.org/fast/" : FAST, # I don't think this is official.
        "http://purl.org/dc/terms/LCC" : LCC,
        "http://purl.org/dc/terms/LCSH" : LCSH,
        "http://purl.org/dc/terms/DDC" : DDC,
        "http://schema.org/typicalAgeRange" : AGE_RANGE,
        "http://schema.org/audience" : FREEFORM_AUDIENCE,
    }

    uri_lookup = dict()
    for k, v in by_uri.items():
        uri_lookup[v] = k

    __tablename__ = 'subjects'
    id = Column(Integer, primary_key=True)
    # Type should be one of the constants in this class.
    type = Column(Unicode, index=True)

    # Formal identifier for the subject (e.g. "300" for Dewey Decimal
    # System's Social Sciences subject.)
    identifier = Column(Unicode, index=True)

    # Human-readable name, if different from the
    # identifier. (e.g. "Social Sciences" for DDC 300)
    name = Column(Unicode, default=None, index=True)

    # Whether classification under this subject implies anything about
    # the fiction/nonfiction status of a book.
    fiction = Column(Boolean, default=None)

    # Whether classification under this subject implies anything about
    # the book's audience.
    audience = Column(
        Enum("Adult", "Young Adult", "Children", "Adults Only", 
             name="audience"),
        default=None, index=True)

    # For children's books, the target age implied by this subject.
    target_age = Column(INT4RANGE, default=None, index=True)

    # Each Subject may claim affinity with one Genre.
    genre_id = Column(Integer, ForeignKey('genres.id'), index=True)

    # A locked Subject has been reviewed by a human and software will
    # not mess with it without permission.
    locked = Column(Boolean, default=False, index=True)

    # A checked Subject has been reviewed by software and will
    # not be checked again unless forced.
    checked = Column(Boolean, default=False, index=True)

    # One Subject may participate in many Classifications.
    classifications = relationship(
        "Classification", backref="subject"
    )

    # Type + identifier must be unique.
    __table_args__ = (
        UniqueConstraint('type', 'identifier'),
    )

    def __repr__(self):
        if self.name:
            name = u' ("%s")' % self.name
        else:
            name = u""
        if self.audience:
            audience = " audience=%s" % self.audience
        else:
            audience = ""
        if self.fiction:
            fiction = " (Fiction)"
        elif self.fiction == False:
            fiction = " (Nonfiction)"
        else:
            fiction = ""
        if self.genre:
            genre = ' genre="%s"' % self.genre.name
        else:
            genre = ""
        if (self.target_age is not None
            and (self.target_age.lower or self.target_age.upper)
        ):
            age_range= " " + self.target_age_string
        else:
            age_range = ""
        a = u'[%s:%s%s%s%s%s%s]' % (
            self.type, self.identifier, name, fiction, audience, genre, age_range)
        return a.encode("utf8")

    @property
    def target_age_string(self):
        lower = self.target_age.lower
        upper = self.target_age.upper           
        if lower and not upper:
            return str(lower)
        if upper and not lower:
            return str(upper)
        if not self.target_age.upper_inc:
            upper -= 1
        if not self.target_age.lower_inc:
            lower += 1
        return "%s-%s" % (lower,upper)

    @property
    def describes_format(self):
        """Does this Subject describe a format of book rather than
        subject matter, audience, etc?

        If so, there are limitations on when we believe this Subject
        actually applies to a given book--it may describe a very
        different adaptation of the same underlying work.

        TODO: See note in assign_genres about the hacky way this is used.
        """
        if self.genre and self.genre.name==COMICS_AND_GRAPHIC_NOVELS:
            return True
        return False

    @classmethod
    def lookup(cls, _db, type, identifier, name):
        """Turn a subject type and identifier into a Subject."""
        classifier = Classifier.lookup(type)
        subject, new = get_one_or_create(
            _db, Subject, type=type,
            identifier=identifier,
            create_method_kwargs=dict(
                name=name,
            )
        )
        if name and not subject.name:
            # We just discovered the name of a subject that previously
            # had only an ID.
            subject.name = name
        return subject, new

    @classmethod
    def common_but_not_assigned_to_genre(cls, _db, min_occurances=1000, 
                                         type_restriction=None):
        q = _db.query(Subject).join(Classification).filter(Subject.genre==None)

        if type_restriction:
            q = q.filter(Subject.type==type_restriction)
        q = q.group_by(Subject.id).having(
            func.count(Subject.id) > min_occurances).order_by(
            func.count(Classification.id).desc())
        return q

    @classmethod
    def assign_to_genres(cls, _db, type_restriction=None, force=False,
                         batch_size=1000):
        """Find subjects that have not been checked yet, assign each a
        genre/audience/fiction status if possible, and mark each as
        checked.

        :param type_restriction: Only consider subjects of the given type.
        :param force: Assign a genre to all subjects not just the ones that
                      have been checked.
        :param batch_size: Perform a database commit every time this many
                           subjects have been checked.
        """
        q = _db.query(Subject).filter(Subject.locked==False)

        if type_restriction:
            q = q.filter(Subject.type==type_restriction)

        if not force:
            q = q.filter(Subject.checked==False)

        counter = 0
        for subject in q:
            subject.assign_to_genre()
            counter += 1
            if not counter % batch_size:
                _db.commit()
        _db.commit()

    def assign_to_genre(self):
        """Assign this subject to a genre."""
        classifier = Classifier.classifiers.get(self.type, None)
        if not classifier:
            return
        self.checked = True
        log = logging.getLogger("Subject-genre assignment")

        genredata, audience, target_age, fiction = classifier.classify(self)
        # If the genre is erotica, the audience will always be ADULTS_ONLY,
        # no matter what the classifier says.
        if genredata == Erotica:
            audience = Classifier.AUDIENCE_ADULTS_ONLY

        if audience in Classifier.AUDIENCES_ADULT:
            target_age = Classifier.default_target_age_for_audience(audience)
        if not audience:
            # We have no audience but some target age information.
            # Try to determine an audience based on that.
            audience = Classifier.default_audience_for_target_age(target_age)

        if genredata:
            _db = Session.object_session(self)
            genre, was_new = Genre.lookup(_db, genredata.name, True)
        else:
            genre = None
        if genre != self.genre:
            log.info(
                "%s:%s genre %r=>%r", self.type, self.identifier,
                self.genre, genre
            )
        self.genre = genre

        if audience:
            if self.audience != audience:
                log.info(
                    "%s:%s audience %s=>%s", self.type, self.identifier,
                    self.audience, audience
                )
        self.audience = audience

        if fiction is not None:
            if self.fiction != fiction:
                log.info(
                    "%s:%s fiction %s=>%s", self.type, self.identifier,
                    self.fiction, fiction
                )
        self.fiction = fiction

        if self.target_age != target_age:
            log.info(
                "%s:%s target_age %r=>%r", self.type, self.identifier,
                self.target_age, target_age
            )        
        self.target_age = target_age


class Classification(Base):
    """The assignment of a Identifier to a Subject."""
    __tablename__ = 'classifications'
    id = Column(Integer, primary_key=True)
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)
    subject_id = Column(Integer, ForeignKey('subjects.id'), index=True)
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    # How much weight the data source gives to this classification.
    weight = Column(Integer)

    @property
    def scaled_weight(self):
        weight = self.weight
        if self.data_source.name == DataSource.OCLC_LINKED_DATA:
            weight = weight / 10.0
        elif self.data_source.name == DataSource.OVERDRIVE:
            weight = weight * 50
        return weight

    _quality_as_indicator_of_target_age = {
        # These measure age appropriateness.
        (DataSource.METADATA_WRANGLER, Subject.AGE_RANGE) : 100,
        Subject.AXIS_360_AUDIENCE : 30,
        (DataSource.OVERDRIVE, Subject.INTEREST_LEVEL) : 20,
        Subject.OVERDRIVE : 15,
        (DataSource.AMAZON, Subject.AGE_RANGE) : 10,
        (DataSource.AMAZON, Subject.GRADE_LEVEL) : 9,

        # These measure reading level, except for TAG, which measures
        # who-knows-what.
        (DataSource.OVERDRIVE, Subject.GRADE_LEVEL) : 5,
        Subject.TAG : 2,
        Subject.LEXILE_SCORE : 1,
        Subject.ATOS_SCORE: 1,

        Subject.AGE_RANGE : 10,
        Subject.GRADE_LEVEL : 10,
    }
    
    @property
    def quality_as_indicator_of_target_age(self):
        if not self.subject.target_age:
            return 0
        data_source = self.data_source.name
        subject_type = self.subject.type
        q = self._quality_as_indicator_of_target_age
        if (data_source, subject_type) in q:
            return q[(data_source, subject_type)]
        if subject_type in q:
            return q[subject_type]
        return 0.1

    @property
    def comes_from_license_source(self):
        if not self.identifier.licensed_through:
            return False
        return self.identifier.licensed_through.data_source == self.data_source


class WillNotGenerateExpensiveFeed(Exception):
    """This exception is raised when a feed is not cached, but it's too
    expensive to generate.
    """
    pass

class CachedFeed(Base):

    __tablename__ = 'cachedfeeds'
    id = Column(Integer, primary_key=True)

    # Every feed is associated with a lane. If null, this is a feed
    # for the top level.
    lane_name = Column(Unicode, nullable=True)

    # Every feed includes book from a subset of available languages
    languages = Column(Unicode)

    # Every feed has a timestamp reflecting when it was created.
    timestamp = Column(DateTime, nullable=True)

    # A feed is of a certain type--currently either 'page' or 'groups'.
    type = Column(Unicode, nullable=False)

    # A 'page' feed is associated with a set of values for the facet
    # groups.
    facets = Column(Unicode, nullable=True)

    # A 'page' feed is associated with a set of values for pagination.
    pagination = Column(Unicode, nullable=False)

    # The content of the feed.
    content = Column(Unicode, nullable=True)

    GROUPS_TYPE = 'groups'
    PAGE_TYPE = 'page'

    log = logging.getLogger("CachedFeed")

    @classmethod
    def fetch(cls, _db, lane, type, facets, pagination, annotator,
              force_refresh=False, max_age=None):
        if max_age is None:
            if type == cls.GROUPS_TYPE:
                max_age = Configuration.groups_max_age()
            elif type == cls.PAGE_TYPE:
                max_age = Configuration.page_max_age()
        if isinstance(max_age, int):
            max_age = datetime.timedelta(seconds=max_age)

        if lane:
            lane_name = lane.name
        else:
            lane_name = None

        if not lane.languages:
            languages_key = None
        else:
            languages_key = ",".join(lane.languages)

        if facets:
            facets_key = facets.query_string
        else:
            facets_key = ""

        if pagination:
            pagination_key = pagination.query_string
        else:
            pagination_key = ""

        # Get a CachedFeed object. We will either return its .content,
        # or update its .content.
        feed, is_new = get_one_or_create(
            _db, CachedFeed, on_multiple='interchangeable',
            lane_name=lane_name,
            type=type,
            languages=languages_key,
            facets=facets_key,
            pagination=pagination_key,
            )
        if force_refresh is True:
            # No matter what, we've been directed to treat this
            # cached feed as stale.
            return feed, False

        if max_age is Configuration.CACHE_FOREVER:
            # This feed is so expensive to generate that it must be cached
            # forever (unless force_refresh is True).
            if not is_new and feed.content:
                # Cacheable!
                return feed, True
            else:
                # We're supposed to generate this feed, but as a group
                # feed, it's too expensive.
                #
                # Rather than generate an error (which will provide a
                # terrible user experience), fall back to generating a
                # default page-type feed, which should be cheap to fetch.
                cls.log.warn(
                    "Could not generate a groups feed for %s, falling back to a page feed.",
                    lane.name
                )
                return cls.fetch(
                    _db, lane, CachedFeed.PAGE_TYPE, facets, pagination, 
                    annotator, force_refresh, max_age=None
                )
        else:
            # This feed is cheap enough to generate on the fly.
            cutoff = datetime.datetime.utcnow() - max_age
            fresh = False
            if feed.timestamp and feed.content:
                if feed.timestamp >= cutoff:
                    fresh = True
            return feed, fresh

        # Either there is no cached feed or it's time to update it.
        return feed, False

    def update(self, content):
        self.content = content
        self.timestamp = datetime.datetime.utcnow()

    def __repr__(self):
        if self.content:
            length = len(self.content)
        else:
            length = "No content"
        return "<CachedFeed #%s %s %s %s %s %s %s %s >" % (
            self.id, self.languages, self.lane_name, self.type, 
            self.facets, self.pagination,
            self.timestamp, length
        )
    

Index(
    "ix_cachedfeeds_lane_name_type_facets_pagination", CachedFeed.lane_name, CachedFeed.type,
    CachedFeed.facets, CachedFeed.pagination
)

class LicensePool(Base):

    """A pool of undifferentiated licenses for a work from a given source.
    """

    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # A LicensePool may be associated with a Work. (If it's not, no one
    # can check it out.)
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # Each LicensePool is associated with one DataSource and one
    # Identifier, and therefore with one original Edition.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    identifier_id = Column(Integer, ForeignKey('identifiers.id'), index=True)

    # One LicensePool may be associated with one RightsStatus.
    rightsstatus_id = Column(
        Integer, ForeignKey('rightsstatus.id'), index=True)

    # One LicensePool can have many Loans.
    loans = relationship('Loan', backref='license_pool')

    # One LicensePool can have many Holds.
    holds = relationship('Hold', backref='license_pool')

    # One LicensePool can be associated with many CustomListEntries.
    custom_list_entries = relationship(
        'CustomListEntry', backref='license_pool')

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool")

    # One LicensePool may have many associated Hyperlinks.
    links = relationship("Hyperlink", backref="license_pool")

    # One LicensePool can be associated with many Complaints.
    complaints = relationship('Complaint', backref='license_pool')

    # The date this LicensePool first became available.
    availability_time = Column(DateTime, index=True)

    # One LicensePool may have multiple DeliveryMechanisms, and vice
    # versa.
    delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism", backref="license_pool"
    )

    # A LicensePool that seemingly looks fine may be manually suppressed
    # to be temporarily or permanently removed from the collection.
    suppressed = Column(Boolean, default=False, index=True)

    # Index the combination of DataSource and Identifier to make joins easier.

    clause = "and_(Edition.data_source_id==LicensePool.data_source_id, Edition.primary_identifier_id==LicensePool.identifier_id)"
    edition = relationship(
        "Edition", primaryjoin=clause, uselist=False, lazy='joined',
        foreign_keys=[Edition.data_source_id, Edition.primary_identifier_id])

    open_access = Column(Boolean, index=True)
    last_checked = Column(DateTime, index=True)
    licenses_owned = Column(Integer,default=0)
    licenses_available = Column(Integer,default=0, index=True)
    licenses_reserved = Column(Integer,default=0)
    patrons_in_hold_queue = Column(Integer,default=0)

    # A Identifier should have at most one LicensePool.
    __table_args__ = (
        UniqueConstraint('identifier_id'),
    )

    def __repr__(self):
        return "<LicensePool #%s owned=%d available=%d reserved=%d holds=%d>" % (
            self.id, self.licenses_owned, self.licenses_available, 
            self.licenses_reserved, self.patrons_in_hold_queue
        )

    @classmethod
    def for_foreign_id(self, _db, data_source, foreign_id_type, foreign_id, rights_status=None):
        """Create a LicensePool for the given foreign ID."""

        # Get the DataSource.
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)

        # The data source must be one that offers licenses.
        if not data_source.offers_licenses:
            raise ValueError(
                'Data source "%s" does not offer licenses.' % data_source.name)

        # The type of the foreign ID must be the primary identifier
        # type for the data source.
        if foreign_id_type != data_source.primary_identifier_type:
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

        kw = dict(data_source=data_source, identifier=identifier)
        if rights_status:
            kw['rights_status'] = rights_status

        # Get the LicensePool that corresponds to the DataSource and
        # the Identifier.
        license_pool, was_new = get_one_or_create(
            _db, LicensePool, **kw)
        if was_new and not license_pool.availability_time:
            now = datetime.datetime.utcnow()
            license_pool.availability_time = now
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

    def add_link(self, rel, href, data_source, media_type=None,
                 content=None, content_path=None):
        """Add a link between this LicensePool and a Resource.

        :param rel: The relationship between this LicensePooland the resource
               on the other end of the link.
        :param href: The URI of the resource on the other end of the link.
        :param media_type: Media type of the representation associated
               with the resource.
        :param content: Content of the representation associated with the
               resource.
        :param content_path: Path (relative to DATA_DIRECTORY) of the
               representation associated with the resource.
        """
        return self.identifier.add_link(
            rel, href, data_source, self, media_type, content, content_path)

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
            new_licenses_reserved, new_patrons_in_hold_queue, as_of=None):
        """Update the LicensePool with new availability information.
        Log the implied changes as CirculationEvents.
        """

        _db = Session.object_session(self)
        if not as_of:
            as_of = datetime.datetime.utcnow()

        for old_value, new_value, more_event, fewer_event in (
                [self.patrons_in_hold_queue,  new_patrons_in_hold_queue,
                 CirculationEvent.HOLD_PLACE, CirculationEvent.HOLD_RELEASE], 
                [self.licenses_available, new_licenses_available,
                 CirculationEvent.CHECKIN, CirculationEvent.CHECKOUT], 
                [self.licenses_reserved, new_licenses_reserved,
                 CirculationEvent.AVAILABILITY_NOTIFY, None], 
                [self.licenses_owned, new_licenses_owned,
                 CirculationEvent.LICENSE_ADD,
                 CirculationEvent.LICENSE_REMOVE]):
            if new_value is None:
                continue
            if old_value == new_value:
                continue

            if old_value < new_value:
                event_name = more_event
            else:
                event_name = fewer_event

            if not event_name:
                continue

            CirculationEvent.log(
                _db, self, event_name, old_value, new_value, as_of)

        # Update the license pool with the latest information.
        self.licenses_owned = new_licenses_owned
        self.licenses_available = new_licenses_available
        self.licenses_reserved = new_licenses_reserved
        self.patrons_in_hold_queue = new_patrons_in_hold_queue
        self.last_checked = as_of

        # Update the last update time of the Work.
        if self.work:
            self.work.last_update_time = as_of

    def set_rights_status(self, uri, name=None):
        _db = Session.object_session(self)
        status, ignore = get_one_or_create(
            _db, RightsStatus, uri=uri,
            create_method_kwargs=dict(name=name))
        self.rights_status = status
        if status.uri in RightsStatus.OPEN_ACCESS:
            self.open_access = True
        else:
            self.open_access = False
        return status

    def loan_to(self, patron, start=None, end=None, fulfillment=None):
        _db = Session.object_session(patron)
        kwargs = dict(start=start or datetime.datetime.utcnow(),
                      end=end)
        loan, is_new = get_one_or_create(
            _db, Loan, patron=patron, license_pool=self, 
            create_method_kwargs=kwargs)
        if fulfillment:
            loan.fulfillment = fulfillment
        return loan, is_new

    def on_hold_to(self, patron, start=None, end=None, position=None):
        _db = Session.object_session(patron)
        if (Configuration.hold_policy() 
            != Configuration.HOLD_POLICY_ALLOW):
            raise PolicyException("Holds are disabled on this system.")
        start = start or datetime.datetime.utcnow()
        hold, new = get_one_or_create(
            _db, Hold, patron=patron, license_pool=self)
        hold.update(start, end, position)
        return hold, new

    @classmethod
    def consolidate_works(cls, _db, calculate_work_even_if_no_author=False):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        a = 0
        for unassigned in cls.with_no_work(_db):
            etext, new = unassigned.calculate_work(
                even_if_no_author=calculate_work_even_if_no_author)
            if not etext:
                # We could not create a work for this LicensePool,
                # most likely because it does not yet have any
                # associated Edition.
                continue
            a += 1
            logging.info("When consolidating works, created %r", etext)
            if a and not a % 100:
                _db.commit()

    def calculate_work(self, even_if_no_author=False, known_edition=None):
        """Try to find an existing Work for this LicensePool.

        If there are no Works for the permanent work ID associated
        with this LicensePool's primary edition, create a new Work.

        Pools that are not open-access will always have a new Work
        created for them.

        :param even_if_no_author: Ordinarily this method will refuse
        to create a Work for a LicensePool whose Edition has no title
        or author. But sometimes a book just has no known author. If
        that's really the case, pass in even_if_no_author=True and the
        Work will be created.
        """

        primary_edition = known_edition or self.edition

        if self.work:
            if known_edition:
                known_edition.work = self.work
            # The work has already been done.
            return self.work, False

        logging.info("Calculating work for %r", primary_edition)
        if not primary_edition:
            # We don't have any information about the identifier
            # associated with this LicensePool, so we can't create a work.
            logging.warn("NO EDITION for %s, cowardly refusing to create work.",
                     self.identifier)
            
            return None, False
        if primary_edition.license_pool != self:
            raise ValueError(
                "Primary edition's license pool is not the license pool for which work is being calculated!")

        if not primary_edition.title or not primary_edition.author:
            primary_edition.calculate_presentation()

        if not primary_edition.work and (
                not primary_edition.title or (
                    (primary_edition.author in (None, Edition.UNKNOWN_AUTHOR)
                     and not even_if_no_author))
        ):
            logging.warn(
                "Edition %r has no author or title, not assigning Work to Edition.", 
                primary_edition
            )
            # msg = u"WARN: NO TITLE/AUTHOR for %s/%s/%s/%s, cowardly refusing to create work." % (
            #    self.identifier.type, self.identifier.identifier,
            #    primary_edition.title, primary_edition.author)
            #print msg.encode("utf8")
            return None, False

        if not primary_edition.permanent_work_id:
            primary_edition.calculate_permanent_work_id()

        if primary_edition.work:
            # This pool's primary edition is already associated with
            # a Work. Use that Work.
            work = primary_edition.work

        else:
            _db = Session.object_session(self)
            work = None
            if self.open_access:
                # Is there already an open-access Work which includes editions
                # with this edition's permanent work ID?
                q = _db.query(Edition).filter(
                    Edition.permanent_work_id
                    ==primary_edition.permanent_work_id).filter(
                        Edition.work != None).filter(
                            Edition.id != primary_edition.id)
                for edition in q:
                    if edition.work.has_open_access_license:
                        work = edition.work
                        break

        if work:
            created = False
        else:
            # There is no better choice than creating a brand new Work.
            created = True
            logging.info("NEW WORK for %s" % primary_edition.title)
            work = Work()
            _db = Session.object_session(self)
            _db.add(work)
            _db.flush()

        # Associate this LicensePool and its Edition with the work we
        # chose or created.
        work.license_pools.append(self)
        primary_edition.work = work

        # Recalculate the display information for the Work, since the
        # associated Editions have changed.
        work.calculate_presentation()

        if created:
            logging.info("Created a new work: %r", work)
        # All done!
        return work, created

    @property
    def open_access_links(self):
        """Yield all open-access Resources for this LicensePool."""

        open_access = Hyperlink.OPEN_ACCESS_DOWNLOAD
        _db = Session.object_session(self)
        q = Identifier.resources_for_identifier_ids(
            _db, [self.identifier.id], open_access
        )
        for resource in q:
            yield resource

    @property
    def best_license_link(self):
        """Find the best available licensing link for the work associated
        with this LicensePool.
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

    def set_delivery_mechanism(
            self, content_type, drm_scheme, resource):
        _db = Session.object_session(self)
        delivery_mechanism, ignore = DeliveryMechanism.lookup(
            _db, content_type, drm_scheme)
        lpdm, ignore = get_one_or_create(
            _db, LicensePoolDeliveryMechanism,
            license_pool=self,
            delivery_mechanism=delivery_mechanism
        )
        lpdm.resource = resource
        return lpdm
        

Index("ix_licensepools_data_source_id_identifier_id", LicensePool.data_source_id, LicensePool.identifier_id, unique=True)


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

    DATA_SOURCE_DEFAULT_RIGHTS_STATUS = {
        DataSource.GUTENBERG: PUBLIC_DOMAIN_USA,
        DataSource.PLYMPTON: CC_BY_NC,
    }
    
    __tablename__ = 'rightsstatus'
    id = Column(Integer, primary_key=True)

    # A URI unique to the license. This may be a URL (e.g. Creative
    # Commons)
    uri = Column(String, index=True)

    # Human-readable name of the license.
    name = Column(String, index=True)

    # One RightsStatus may apply to many LicensePools.
    licensepools = relationship("LicensePool", backref="rights_status")

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
        else:
            return RightsStatus.UNKNOWN

    
class CirculationEvent(Base):

    """Changes to a license pool's circulation status.

    We log these so we can measure things like the velocity of
    individual books.
    """
    __tablename__ = 'circulationevents'

    id = Column(Integer, primary_key=True)

    # One LicensePool can have many circulation events.
    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True)

    type = Column(String(32), index=True)
    start = Column(DateTime, index=True)
    end = Column(DateTime)
    old_value = Column(Integer)
    delta = Column(Integer)
    new_value = Column(Integer)
    foreign_patron_id = Column(String)

    # A given license pool can only have one event of a given type for
    # a given patron at a given time.
    __table_args__ = (UniqueConstraint('license_pool_id', 'type', 'start',
                                       'foreign_patron_id'),)

    # Constants for use in logging circulation events to JSON
    SOURCE = u"source"
    TYPE = u"event"

    # The names of the circulation events we recognize.
    CHECKOUT = u"check_out"
    CHECKIN = u"check_in"
    HOLD_PLACE = u"hold_place"
    HOLD_RELEASE = u"hold_release"
    LICENSE_ADD = u"license_add"
    LICENSE_REMOVE = u"license_remove"
    AVAILABILITY_NOTIFY = u"availability_notify"
    CIRCULATION_CHECK = u"circulation_check"
    SERVER_NOTIFICATION = u"server_notification"
    TITLE_ADD = u"title_add"
    TITLE_REMOVE = u"title_remove"
    UNKNOWN = u"unknown"

    # The time format used when exporting to JSON.
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"

    @classmethod
    def log(cls, _db, license_pool, event_name, old_value, new_value,
            start=None, end=None, foreign_patron_id=None):
        if new_value is None or old_value is None:
            delta = None
        else:
            delta = new_value - old_value
        if not start:
            start = datetime.datetime.utcnow()
        if not end:
            end = start
        logging.info("EVENT %s %s=>%s", event_name, old_value, new_value)
        event, was_new = get_one_or_create(
            _db, CirculationEvent, license_pool=license_pool,
            type=event_name, start=start, foreign_patron_id=foreign_patron_id,
            create_method_kwargs=dict(
                old_value=old_value,
                new_value=new_value,
                delta=delta,
                end=end)
            )
        return event, was_new


class Credential(Base):
    """A place to store credentials for external services."""
    __tablename__ = 'credentials'
    id = Column(Integer, primary_key=True)
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    type = Column(String(255), index=True)
    credential = Column(String)
    expires = Column(DateTime)

    __table_args__ = (
        UniqueConstraint('data_source_id', 'patron_id', 'type'),
    )

    @classmethod
    def lookup(self, _db, data_source, type, patron, refresher_method,
               allow_permanent_token=False):
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)
        credential, is_new = get_one_or_create(
            _db, Credential, data_source=data_source, type=type, patron=patron)
        if (is_new or (not credential.expires and not allow_permanent_token)
            or (credential.expires 
                and credential.expires <= datetime.datetime.utcnow())):
            if refresher_method:
                refresher_method(credential)
        return credential

    @classmethod
    def lookup_by_token(self, _db, data_source, type, token,
                               allow_permanent_token=False):
        """Look up a unique token.

        Lookup will fail on expired tokens. Unless permanent tokens
        are specifically allowed, lookup will fail on permanent tokens.
        """

        credential = get_one(
            _db, Credential, data_source=data_source, type=type, 
            credential=token)

        if not credential:
            # No matching token.
            return None

        if not credential.expires:
            if allow_permanent_token:
                return credential
            else:
                # It's an error that this token never expires. It's invalid.
                return None
        elif credential.expires > datetime.datetime.utcnow():
            return credential
        else:
            # Token has expired.
            return None

    @classmethod
    def lookup_by_temporary_token(cls, _db, data_source, type, token):
        """Look up a temporary token and expire it immediately."""
        credential = cls.lookup_by_token(_db, data_source, type, token)
        if not credential:
            return None
        credential.expires = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=5)
        return credential

    @classmethod
    def temporary_token_create(self, _db, data_source, type, patron, duration):
        """Create a temporary token for the given data_source/type/patron.

        The token will be good for the specified `duration`.
        """
        expires = datetime.datetime.utcnow() + duration
        token_string = str(uuid.uuid1())
        credential, is_new = get_one_or_create(
            _db, Credential, data_source=data_source, type=type, patron=patron)
        credential.credential=token_string
        credential.expires=expires
        return credential, is_new

# Index to make temporary_token_lookup() fast.
Index("ix_credentials_data_source_id_type_token", Credential.data_source_id, Credential.type, Credential.credential, unique=True)

class Timestamp(Base):
    """A general-purpose timestamp for external services."""

    __tablename__ = 'timestamps'
    service = Column(String(255), primary_key=True)
    timestamp = Column(DateTime)
    counter = Column(Integer)

    @classmethod
    def stamp(self, _db, service):
        now = datetime.datetime.utcnow()
        stamp, was_new = get_one_or_create(
            _db, Timestamp,
            service=service,
            create_method_kwargs=dict(timestamp=now))
        if not was_new:
            stamp.timestamp = now
        return stamp

class Representation(Base):
    """A cached document obtained from (and possibly mirrored to) the Web
    at large.

    Sometimes this is a DataSource's representation of a specific
    book.

    Sometimes it's associated with a database Resource (which has a
    well-defined relationship to one specific book).

    Sometimes it's just a web page that we need a cached local copy
    of.
    """

    EPUB_MEDIA_TYPE = u"application/epub+zip"
    PDF_MEDIA_TYPE = u"application/pdf"
    TEXT_XML_MEDIA_TYPE = u"text/xml"
    APPLICATION_XML_MEDIA_TYPE = u"application/xml"
    JPEG_MEDIA_TYPE = u"image/jpeg"
    PNG_MEDIA_TYPE = u"image/png"
    GIF_MEDIA_TYPE = u"image/gif"
    MP3_MEDIA_TYPE = u"audio/mpeg"
    TEXT_PLAIN = u"text/plain"

    BOOK_MEDIA_TYPES = [
        EPUB_MEDIA_TYPE,
        PDF_MEDIA_TYPE,
        MP3_MEDIA_TYPE,
    ]

    IMAGE_MEDIA_TYPES = [
        JPEG_MEDIA_TYPE,
        PNG_MEDIA_TYPE,
        GIF_MEDIA_TYPE,
    ]

    SUPPORTED_BOOK_MEDIA_TYPES = [
        EPUB_MEDIA_TYPE
    ]

    FILE_EXTENSIONS = {
        EPUB_MEDIA_TYPE: "epub",
        PDF_MEDIA_TYPE: "pdf",
        MP3_MEDIA_TYPE: "mp3",
        JPEG_MEDIA_TYPE: "jpg",
        PNG_MEDIA_TYPE: "png",
        GIF_MEDIA_TYPE: "gif",
    }

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

    # A URL under our control to which this representation will be
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
        lazy="joined")

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
    def get(cls, _db, url, do_get=None, extra_request_headers=None,
            accept=None,
            max_age=None, pause_before=0, allow_redirects=True, debug=True):
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
        do_get = do_get or cls.simple_http_get

        representation = None

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

        # Convert a max_age timedelta to a number of seconds.
        if isinstance(max_age, datetime.timedelta):
            max_age = max_age.total_seconds()

        # Do we already have a usable representation?
        usable_representation = (
            representation and not representation.fetch_exception)

        # Assuming we have a usable representation, is it
        # fresh?
        fresh_representation = (
            usable_representation and (
                max_age is None or max_age > representation.age))

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
        try:
            status_code, headers, content = do_get(url, headers)
            exception = None
            if 'content-type' in headers:
                media_type = headers['content-type'].lower()
            else:
                media_type = None
            if isinstance(content, unicode):
                content = content.encode("utf8")
        except Exception, e:
            # This indicates there was a problem with making the HTTP
            # request, not that the HTTP request returned an error
            # condition.
            logging.error("Error making HTTP request to %s", url, exc_info=e)
            exception = traceback.format_exc()
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
                _db, Representation, url=url, media_type=media_type)

        representation.fetch_exception = exception
        representation.fetched_at = fetched_at

        if status_code == 304:
            # The representation hasn't changed since we last checked.
            # Set its fetched_at property and return the cached
            # version as though it were new.
            representation.fetched_at = fetched_at
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

    def update_image_size(self):
        """Make sure .image_height and .image_width are up to date.
       
        Clears .image_height and .image_width if the representation
        is not an image.
        """
        if self.media_type and self.media_type.startswith('image/'):
            image = self.as_image()
            self.image_width, self.image_height = image.size
            # print "%s is %dx%d" % (self.url, self.image_width, self.image_height)
        else:
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


    def set_as_mirrored(self):
        """Record the fact that the representation has been mirrored
        to its .mirror_url.
        """
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
        if not 'timeout' in kwargs:
            kwargs['timeout'] = 20
        
        if not 'allow_redirects' in kwargs:
            kwargs['allow_redirects'] = True
        response = requests.get(url, headers=headers, **kwargs)
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

    def content_fh(self):
        """Return an open filehandle to the representation's contents.

        This works whether the representation is kept in the database
        or in a file on disk.
        """
        if self.content:
            return StringIO(self.content)
        else:
            if not os.path.exists(self.local_path):
                raise ValueError("%s does not exist." % self.local_path)
            return open(self.local_path)
            

    def as_image(self):
        """Load this Representation's contents as a PIL image."""
        if not self.is_image:
            raise ValueError(
                "Cannot load non-image representation as image: type %s." 
                % self.media_type)
        if not self.content and not self.local_path:
            raise ValueError("Image representation has no content.")
        return Image.open(self.content_fh())

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
            return self, False

        # Now that we've loaded the image, take the opportunity to set
        # the image size of the original representation.
        self.image_width, self.image_height = image.size
        # If the image is already thumbnail-size, don't bother.
        if self.image_height <= max_height and self.image_width <= max_width:
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
        thumbnail.mirror_url = thumbnail.url
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


class DeliveryMechanism(Base):
    """A technique for delivering a book to a patron.

    There are two parts to this: a DRM scheme and a content
    type. Either may be identified with a MIME media type
    (e.g. "vnd.adobe/adept+xml" or "application/epub+zip") or an
    informal name ("Kindle via Amazon").
    """
    KINDLE_CONTENT_TYPE = "Kindle via Amazon"
    NOOK_CONTENT_TYPE = "Nook via B&N"
    STREAMING_TEXT_CONTENT_TYPE = "Streaming Text"
    STREAMING_AUDIO_CONTENT_TYPE = "Streaming Audio"
    STREAMING_VIDEO_CONTENT_TYPE = "Streaming Video"

    NO_DRM = None
    ADOBE_DRM = "vnd.adobe/adept+xml"
    KINDLE_DRM = "Kindle DRM"
    NOOK_DRM = "Nook DRM"
    STREAMING_DRM = "Streaming"
    OVERDRIVE_DRM = "Overdrive DRM"

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
        (Representation.EPUB_MEDIA_TYPE, NO_DRM),
        (Representation.EPUB_MEDIA_TYPE, ADOBE_DRM),
    ])

    license_pool_delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism",
        backref="delivery_mechanism"
    )

    @property
    def name(self):
        if self.drm_scheme is self.NO_DRM:
            drm_scheme = "DRM-free"
        else:
            drm_scheme = self.drm_scheme
        return "%s (%s)" % (self.content_type, drm_scheme)

    def __repr__(self):   

        if self.default_client_can_fulfill:
            fulfillable = "fulfillable"
        else:
            fulfillable = "not fulfillable"

        return "<Delivery mechanism: %s, %s)>" % (
            self.name, fulfillable
        )

    @classmethod
    def load_all(cls, _db):
        """Load all DeliveryMechanism objects into the cache associated with
        the database connection.
        """
        if not hasattr(_db, '_deliverymechanism_cache'):
            _db._deliverymechanism_cache = dict()
        for m in _db.query(DeliveryMechanism):
            _db._deliverymechanism_cache[(m.content_type, m.drm_scheme)] = m

    @classmethod
    def lookup(cls, _db, content_type, drm_scheme):
        if not hasattr(_db, '_deliverymechanism_cache'):
            _db._deliverymechanism_cache = dict()
        key = (content_type, drm_scheme)
        cache = _db._deliverymechanism_cache
        if key in cache:
            return cache[key], False
        result, is_new = get_one_or_create(
            _db, DeliveryMechanism, content_type=content_type,
            drm_scheme=drm_scheme
        )
        cache[key] = result
        return result, is_new

    @property
    def implicit_medium(self):
        """What would be a good setting for Edition.MEDIUM for an edition
        available through this DeliveryMechanism?
        """
        if self.content_type in (
                Representation.EPUB_MEDIA_TYPE,
                Representation.PDF_MEDIA_TYPE,
                "Kindle via Amazon",
                "Streaming Text"):
            return Edition.BOOK_MEDIUM
        elif self.content_type in (
                "Streaming Video" or self.content_type.startswith('video/')
        ):
            return Edition.VIDEO_MEDIUM
        else:
            return None

    def is_media_type(self, x):
        "Does this string look like a media type?"
        if x is None:
            return False

        if x in (self.KINDLE_CONTENT_TYPE,
                 self.NOOK_CONTENT_TYPE,
                 self.STREAMING_TEXT_CONTENT_TYPE,
                 self.STREAMING_AUDIO_CONTENT_TYPE,
                 self.STREAMING_VIDEO_CONTENT_TYPE):
            return False

        if x in (
                self.KINDLE_DRM,
                self.NOOK_DRM,
                self.STREAMING_DRM,
                self.OVERDRIVE_DRM):
            return False

        return any(x.startswith(prefix) for prefix in 
                   ['vnd.', 'application', 'text', 'video', 'audio', 'image'])

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
        return None


Index("ix_deliverymechanisms_drm_scheme_content_type", 
      DeliveryMechanism.drm_scheme, 
      DeliveryMechanism.content_type,
      unique=True)


class CustomList(Base):
    """A custom grouping of Editions."""

    STAFF_PICKS_NAME = u"Staff Picks"

    __tablename__ = 'customlists'
    id = Column(Integer, primary_key=True)
    primary_language = Column(Unicode, index=True)
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    foreign_identifier = Column(Unicode, index=True)
    name = Column(Unicode)
    description = Column(Unicode)
    created = Column(DateTime, index=True)
    updated = Column(DateTime, index=True)
    responsible_party = Column(Unicode)

    entries = relationship(
        "CustomListEntry", backref="customlist", lazy="joined")

    # TODO: It should be possible to associate a CustomList with an
    # audience, fiction status, and subject, but there is no planned
    # interface for managing this.

    @classmethod
    def all_from_data_sources(cls, _db, data_sources):
        """All custom lists from the given data sources."""
        if not isinstance(data_sources, list):
            data_sources = [data_sources]
        ids = []
        for ds in data_sources:
            if isinstance(ds, basestring):
                ds = DataSource.lookup(_db, ds)
            ids.append(ds.id)
        return _db.query(CustomList).filter(CustomList.data_source_id.in_(ids))

    def add_entry(self, edition, annotation=None, first_appearance=None):
        first_appearance = first_appearance or datetime.datetime.utcnow()
        _db = Session.object_session(self)
        entry, was_new = get_one_or_create(
            _db, CustomListEntry,
            customlist=self, edition=edition,
            create_method_kwargs=dict(first_appearance=first_appearance)
        )
        if (not entry.most_recent_appearance 
            or entry.most_recent_appearance < first_appearance):
            entry.most_recent_appearance = first_appearance
        entry.annotation = annotation
        if edition.license_pool and not entry.license_pool:
            entry.license_pool = edition.license_pool
        return entry, was_new

class CustomListEntry(Base):

    __tablename__ = 'customlistentries'
    id = Column(Integer, primary_key=True)    
    list_id = Column(Integer, ForeignKey('customlists.id'), index=True)
    edition_id = Column(Integer, ForeignKey('editions.id'), index=True)
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)
    annotation = Column(Unicode)

    # These two fields are for best-seller lists. Even after a book
    # drops off the list, the fact that it once was on the list is
    # still relevant.
    first_appearance = Column(DateTime, index=True)
    most_recent_appearance = Column(DateTime, index=True)

    def set_license_pool(self, metadata=None, metadata_client=None):
        """If possible, set the best available LicensePool to be used when
        fulfilling requests for this CustomListEntry.

        'Best' means it has the most copies of the book available
        right now.
        """
        _db = Session.object_session(self)
        edition = self.edition
        if not self.edition:
            # This shouldn't happen, but no edition means no license pool.
            self.license_pool = None
            return self.license_pool

        new_license_pool = None
        if not metadata:
            from metadata_layer import Metadata
            metadata = Metadata.from_edition(edition)

        # Try to guess based on metadata, if we can get a high-quality
        # guess.
        potential_license_pools = metadata.guess_license_pools(
            _db, metadata_client)
        for lp, quality in sorted(
                potential_license_pools.items(), key=lambda x: -x[1]):
            if lp.deliverable and quality >= 0.8:
                new_license_pool = lp
                break

        if not new_license_pool:
            # Try using the less reliable, more expensive method of
            # matching based on equivalent identifiers.
            equivalent_identifier_ids = self.edition.equivalent_identifier_ids()
            pool_q = _db.query(LicensePool).filter(
                LicensePool.identifier_id.in_(equivalent_identifier_ids)).order_by(
                    LicensePool.licenses_available.desc(),
                    LicensePool.patrons_in_hold_queue.asc())
            pools = [x for x in pool_q if x.deliverable]
            if pools:
                new_license_pool = pools[0]

        old_license_pool = self.license_pool
        if old_license_pool != new_license_pool:
            if old_license_pool:
                old_id = old_license_pool.identifier
            else:
                old_id = None
            if new_license_pool:
                new_id = new_license_pool.identifier
            else:
                new_id = None
            logging.info(
                "Changing license pool for list entry %r to %r (was %r)", 
                self.edition, new_id, old_id
            )
        self.license_pool = new_license_pool
        return self.license_pool


class Complaint(Base):
    """A complaint about a LicensePool (or, potentially, something else)."""

    __tablename__ = 'complaints'

    VALID_TYPES = set([
        "http://librarysimplified.org/terms/problem/" + x
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

    @classmethod
    def register(self, license_pool, type, source, detail):
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
                detail=detail
            )
        return complaint, is_new


class Admin(Base):

    __tablename__ = 'admins'

    id = Column(Integer, primary_key=True)
    email = Column(Unicode, unique=True, nullable=False)
    access_token = Column(Unicode, index=True)
    credential = Column(Unicode)

    def update_credentials(self, _db, access_token, credential):
        self.access_token = access_token
        self.credential = credential
        _db.commit()


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
