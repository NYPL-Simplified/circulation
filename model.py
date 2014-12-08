# encoding: utf-8
from collections import (
    Counter,
    defaultdict,
)
import bisect
from cStringIO import StringIO
import datetime
import json
import os
from nose.tools import set_trace
import random
import re
import requests
import time

from PIL import (
    Image,
)

from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    backref,
    relationship,
)
from sqlalchemy import or_
from sqlalchemy.orm import (
    aliased,
    backref,
    joinedload,
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
    String,
    Table,
    Unicode,
    UniqueConstraint,
)

import classifier
from classifier import (
    Classifier,
    GenreData,
)
from util import (
    LanguageCodes,
    MetadataSimilarity,
    TitleProcessor,
)
from util.summary import SummaryEvaluator

#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

from sqlalchemy.orm.session import Session

from sqlalchemy.dialects.postgresql import (
    ARRAY,
    HSTORE,
    JSON,
)
from sqlalchemy.orm import sessionmaker

DEBUG = False

def production_session():
    url = os.environ['DATABASE_URL']
    print url
    if url.startswith('"'):
        url = url[1:]
    print "ENVIRONMENT: %s" % os.environ['DATABASE_URL'] 
    print "MODIFIED: %s" % url
    return SessionManager.session(url)

class SessionManager(object):

    @classmethod
    def engine(cls, url=None):
        url = url or os.environ['DATABASE_URL']
        return create_engine(url, echo=DEBUG)

    @classmethod
    def initialize(cls, url):
        engine = cls.engine(url)
        Base.metadata.create_all(engine)
        return engine, engine.connect()

    @classmethod
    def session(cls, url):
        engine, connection = cls.initialize(url)
        session = Session(connection)
        print "INITIALIZING DATA"
        cls.initialize_data(session)
        session.commit()
        print "DONE INITIALIZING DATA"
        return session

    @classmethod
    def initialize_data(cls, session):
        # Create initial data sources.
        list(DataSource.well_known_sources(session))

        # Create all genres.
        for g in classifier.genres.values():
            Genre.lookup(session, g, autocreate=True)
        session.commit()

def get_one(db, model, **kwargs):
    try:
        return db.query(model).filter_by(**kwargs).one()
    except NoResultFound:
        return None


def get_one_or_create(db, model, create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    one = get_one(db, model, **kwargs)
    if one:
        return one, False
    else:
        try:
            return create(db, model, create_method, create_method_kwargs, **kwargs)
        except IntegrityError:
            db.rollback()
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
    external_identifier = Column(Unicode, unique=True, index=True)

    # An identifier used by the patron that gives them the authority
    # to borrow books. This identifier may change over time.
    authorization_identifier = Column(Unicode, unique=True, index=True)

    # TODO: An identifier used by the patron that authenticates them,
    # but does not give them the authority to borrow books. i.e. their
    # website username.

    # The last time this record was synced up with an external library
    # system.
    last_external_sync = Column(DateTime)

    # The time, if any, at which the user's authorization to borrow
    # books expires.
    authorization_expires = Column(Date, index=True)

    loans = relationship('Loan', backref='patron')

    # One Patron can have many associated Credentials.
    credentials = relationship("Credential", backref="patron")

    def works_on_loan(self):
        db = Session.object_session(self)
        loans = db.query(Loan).filter(Loan.patron==self)
        return [loan.license_pool.work for loan in loans]

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


class Loan(Base):
    __tablename__ = 'loans'
    id = Column(Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)
    start = Column(DateTime)
    end = Column(DateTime)


class DataSource(Base):

    """A source for information about books, and possibly the books themselves."""

    GUTENBERG = "Gutenberg"
    OVERDRIVE = "Overdrive"
    THREEM = "3M"
    OCLC = "OCLC Classify"
    OCLC_LINKED_DATA = "OCLC Linked Data"
    AMAZON = "Amazon"
    XID = "WorldCat xID"
    AXIS_360 = "Axis 360"
    WEB = "Web"
    OPEN_LIBRARY = "Open Library"
    CONTENT_CAFE = "Content Cafe"
    VIAF = "Content Cafe"
    GUTENBERG_COVER_GENERATOR = "Project Gutenberg eBook Cover Generator"
    BIBLIOCOMMONS = "BiblioCommons"
    MANUAL = "Manual intervention"

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

    # One DataSource can provide many Resources.
    resources = relationship("Resource", backref="data_source")

    # One DataSource can provide many Representations.
    representations = relationship("Representation", backref="data_source")

    # One DataSource can generate many Measurements.
    measurements = relationship("Measurement", backref="data_source")

    # One DataSource can provide many Classifications.
    classifications = relationship("Classification", backref="data_source")

    # One DataSource can have many associated Credentials.
    credentials = relationship("Credential", backref="data_source")

    @classmethod
    def lookup(cls, _db, name):
        try:
            q = _db.query(cls).filter_by(name=name)
            return q.one()
        except NoResultFound:
            return None

    @classmethod
    def well_known_sources(cls, _db):
        """Make sure all the well-known sources exist."""

        for (name, offers_licenses, primary_identifier_type, refresh_rate) in (
                (cls.GUTENBERG, True, Identifier.GUTENBERG_ID, None),
                (cls.OVERDRIVE, True, Identifier.OVERDRIVE_ID, 0),
                (cls.THREEM, True, Identifier.THREEM_ID, 60*60*6),
                (cls.AXIS_360, True, Identifier.AXIS_360_ID, 0),
                (cls.OCLC, False, Identifier.OCLC_NUMBER, None),
                (cls.OCLC_LINKED_DATA, False, Identifier.OCLC_NUMBER, None),
                (cls.AMAZON, False, Identifier.ASIN, None),
                (cls.OPEN_LIBRARY, False, Identifier.OPEN_LIBRARY_ID, None),
                (cls.GUTENBERG_COVER_GENERATOR, False, Identifier.GUTENBERG_ID, None),
                (cls.WEB, True, Identifier.URI, None),
                (cls.VIAF, False, None, None),
                (cls.CONTENT_CAFE, False, None, None),
                (cls.BIBLIOCOMMONS, False, Identifier.BIBLIOCOMMONS_ID, None),
                (cls.MANUAL, False, None, None),
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
            yield obj


class CoverageRecord(Base):
    """A record of a Identifier being used as input into another data
    source.
    """
    __tablename__ = 'coveragerecords'

    id = Column(Integer, primary_key=True)
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True)
    date = Column(Date, index=True)
    exception = Column(Unicode, index=True)


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

    # One Identifier may have many associated CoverageRecords.
    coverage_records = relationship("CoverageRecord", backref="identifier")

    # One Identifier can have many Representations.
    representations = relationship("Representation", backref="identifier")

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

    # One Identifier may serve to identify many Resources.
    resources = relationship(
        "Resource", backref="identifier"
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
    def for_foreign_id(cls, _db, foreign_identifier_type, foreign_id,
                       autocreate=True):
        """Turn a foreign ID into an Identifier."""
        was_new = None
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

    def equivalent_to(self, data_source, identifier, strength):
        """Make one Identifier equivalent to another.
        
        `data_source` is the DataSource that believes the two 
        identifiers are equivalent.
        """
        _db = Session.object_session(self)
        eq, new = get_one_or_create(
            _db, Equivalency,
            data_source=data_source,
            input=self,
            output=identifier,
            create_method_kwargs=dict(strength=strength))
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
             _db, identifier_ids, identifier_ids, levels, threshold, debug)

        if debug and working_set:
            # This is not a big deal, but it means we could be getting
            # more IDs by increasing the level.
            print "Leftover working set at level %d." % levels

        return equivalents

    @classmethod
    def _recursively_equivalent_identifier_ids(
            cls, _db, original_working_set, working_set, levels, threshold, debug):

        if levels == 0:
            equivalents = defaultdict(lambda : defaultdict(list))
            for id in original_working_set:
                # Every identifier is unshakeably equivalent to itself.
                equivalents[id][id].append((1, 1000000))
            return (working_set, set(), set(), equivalents)

        if not working_set:
            return working_set, seen_equivalency_ids, seen_identifier_ids

        # First make the recursive call.        
        (working_set, seen_equivalency_ids, seen_identifier_ids,
         equivalents) = cls._recursively_equivalent_identifier_ids(
             _db, original_working_set, working_set, levels-1, threshold, debug)

        if not working_set:
            # We're done.
            return (working_set, seen_equivalency_ids, seen_identifier_ids,
                    equivalents)

        new_working_set = set()
        seen_identifier_ids = seen_identifier_ids.union(working_set)

        equivalencies = Equivalency.for_identifiers(
            _db, working_set, seen_equivalency_ids)
        for e in equivalencies:
            if debug:
                print "%r => %r" % (e.input, e.output)
            seen_equivalency_ids.add(e.id)

            # Signal strength decreases monotonically, so
            # if it dips below the threshold, we can
            # ignore it from this point on.

            # I -> O becomes "I is a precursor of O with distance
            # equal to the I->O strength."
            if e.strength > threshold:
                if debug:
                    print "Strong signal: %r" % e
                
                cls._update_equivalents(
                    equivalents, e.output_id, e.input_id, e.strength, e.votes)
                cls._update_equivalents(
                    equivalents, e.input_id, e.output_id, e.strength, e.votes)
            else:
                if debug:
                    print "Ignoring signal below threshold: %r" % e

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

        if debug:
            print "At level %d."
            print " New working set: %r" % sorted(new_working_set)
            print " %d equivalencies seen so far." % len(seen_equivalency_ids)
            print " %d identifiers seen so far." % len(seen_identifier_ids)
            print " %d equivalents" % len(equivalents)

        if debug and new_working_set:
            print " Here's the new working set:",
            for i in _db.query(Identifier).filter(Identifier.id.in_(new_working_set)):
                print "", i

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

        if debug:
            print "Pruned %d from working set" % len(surviving_working_set.intersection(new_working_set))
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

    def add_resource(self, rel, href, data_source, license_pool=None,
                     media_type=None, content=None):
        """Associated a resource with this Identifier."""
        _db = Session.object_session(self)
        try:
            resource, new = get_one_or_create(
                _db, Resource, identifier=self,
                rel=rel,
                href=href,
                media_type=media_type,
                content=content,
                create_method_kwargs=dict(
                    data_source=data_source,
                    license_pool=license_pool))
        except MultipleResultsFound, e:
            # TODO: This is a hack.
            all_resources = _db.query(Resource).filter(
                Resource.identifier==self,
                Resource.rel==rel,
                Resource.href==href,
                Resource.media_type==media_type,
                Resource.content==content)
            all_resources = all_resources.all()
            resource = all_resources[0]
            new = False
            for i in all_resources[1:]:
                _db.delete(i)

        if content:
            resource.set_content(content, media_type)
        return resource, new

    def add_measurement(self, data_source, quantity_measured, value,
                        weight=1, taken_at=None):
        """Associate a new Measurement with this Identifier."""
        _db = Session.object_session(self)

        now = datetime.datetime.now()
        taken_at = taken_at or now
        # Is there an existing most recent measurement?
        most_recent = get_one(
            _db, Measurement, identifier=self,
            data_source=data_source,
            quantity_measured=quantity_measured,
            is_most_recent=True,
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
        if is_new:
            print repr(subject)

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
    def resources_for_identifier_ids(self, _db, identifier_ids, rel=None):
        resources = _db.query(Resource).filter(
                Resource.identifier_id.in_(identifier_ids))
        if rel:
            resources = resources.filter(Resource.rel==rel)
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
            _db, identifier_ids, Resource.IMAGE)
        images = images.join(Resource.data_source)
        licensed_sources = (
            DataSource.OVERDRIVE, DataSource.THREEM,
            DataSource.AXIS_360)
        mirrored_or_embeddable = or_(
            Resource.mirrored==True,
            DataSource.name.in_(licensed_sources)
            )

        images = images.filter(mirrored_or_embeddable).all()

        champion = None
        champions = []
        champion_score = None
        # Judge the image resource by its deviation from the ideal
        # aspect ratio, and by its deviation (in the "too small"
        # direction only) from the ideal resolution.
        for r in images:
            if r.data_source.name in licensed_sources:
                # For licensed works, always present the cover
                # provided by the licensing authority.
                r.quality = 1
                champion = r
                continue
            if not r.image_width or not r.image_height:
                continue
            aspect_ratio = r.image_width / float(r.image_height)
            aspect_difference = abs(aspect_ratio-cls.IDEAL_COVER_ASPECT_RATIO)
            quality = 1 - aspect_difference
            width_difference = (
                float(r.image_width - cls.IDEAL_IMAGE_WIDTH) / cls.IDEAL_IMAGE_WIDTH)
            if width_difference < 0:
                # Image is not wide enough.
                quality = quality * (1+width_difference)
            height_difference = (
                float(r.image_height - cls.IDEAL_IMAGE_HEIGHT) / cls.IDEAL_IMAGE_HEIGHT)
            if height_difference < 0:
                # Image is not tall enough.
                quality = quality * (1+height_difference)

            # Scale the estimated quality by the source of the image.
            source_name = r.data_source.name
            if source_name==DataSource.CONTENT_CAFE:
                quality = quality * 0.70
            elif source_name==DataSource.GUTENBERG_COVER_GENERATOR:
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
    def evaluate_summary_quality(cls, _db, identifier_ids):
        """Evaluate the summaries for the given group of Identifier IDs.

        This is an automatic evaluation based solely on the content of
        the summaries. It will be combined with human-entered ratings
        to form an overall quality score.

        We need to evaluate summaries from a set of Identifiers
        (typically those associated with a single work) because we
        need to see which noun phrases are most frequently used to
        describe the underlying work.

        :return: The single highest-rated summary Resource.

        """
        evaluator = SummaryEvaluator()
        # Find all rel="description" resources associated with any of
        # these records.
        summaries = cls.resources_for_identifier_ids(
            _db, identifier_ids, Resource.DESCRIPTION)
        summaries = summaries.filter(Resource.content != None).all()

        champion = None
        # Add each resource's content to the evaluator's corpus.
        has_short_description = False
        has_full_description = False
        for r in summaries:
            if r.href=="tag:short":
                has_short_description = True
            elif r.href=="tag:full":
                has_full_description = True
            if has_full_description and has_short_description:
                break

        for r in summaries:
            if not has_full_description or r.href != "tag:short":
                evaluator.add(r.content)
        evaluator.ready()

        # Then have the evaluator rank each resource.
        for r in summaries:
            if has_full_description and r.href == "tag:short":
                continue
            quality = evaluator.score(r.content)
            r.set_estimated_quality(quality)
            if not champion or r.quality > champion.quality:
                champion = r
        return champion, summaries

    @classmethod
    def missing_coverage_from(
            cls, _db, identifier_types, coverage_data_source):
        """Find identifiers of the given types which have no CoverageRecord
        from `coverage_data_source`.
        """
        q = _db.query(Identifier).outerjoin(
            CoverageRecord, Identifier.id==CoverageRecord.identifier_id).filter(
                Identifier.type.in_(identifier_types))
        q2 = q.filter(CoverageRecord.id==None)
        return q2

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


    extra = Column(MutableDict.as_mutable(JSON), default={})

    contributions = relationship("Contribution", backref="contributor")
    work_contributions = relationship("WorkContribution", backref="contributor",
                                      )
    # Types of roles
    AUTHOR_ROLE = "Author"
    PRIMARY_AUTHOR_ROLE = "Primary Author"
    UNKNOWN_ROLE = 'Unknown'
    AUTHOR_ROLES = set([PRIMARY_AUTHOR_ROLE, AUTHOR_ROLE])

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

            try:
                contributors, new = get_one_or_create(
                    _db, Contributor, create_method_kwargs=create_method_kwargs,
                    **query)
            except Exception, e:
                set_trace()

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
        msg = u"MERGING %s (%s) into %s (%s)" % (
            repr(self).decode("utf8"), self.viaf,
            repr(destination).decode("utf8"),
            destination.viaf)
        print msg.encode("utf8")
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
        print " Merging edition contributions."
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
        print " Merging work contributions."
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
        print "Commit before deletion."
        _db.commit()
        print "Final deletion."
        _db.delete(self)
        print "Committing after deletion."
        _db.commit()
        # _db.query(Contributor).filter(Contributor.id==self.id).delete()
        #_db.commit()
        print "All done."

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

    title = Column(Unicode, index=True)
    sort_title = Column(Unicode, index=True)
    subtitle = Column(Unicode, index=True)
    series = Column(Unicode, index=True)

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

    BOOK_MEDIUM = "Book"
    PERIODICAL_MEDIUM = "Periodical"
    AUDIO_MEDIUM = "Audio"
    MUSIC_MEDIUM = "Music"
    VIDEO_MEDIUM = "Video"

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

    # Information kept in here probably won't be used.
    extra = Column(MutableDict.as_mutable(JSON), default={})

    def __repr__(self):
        id_repr = repr(self.primary_identifier).decode("utf8")
        a = (u"Edition %s [%r] (%s/%s/%s)" % (
            self.id, id_repr, self.title,
            ", ".join([x.name for x in self.contributors]),
            self.language))
        try:
            a.encode("utf8")
        except Exception, e:
            set_trace()
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
        for x in self.contributions:
            if not primary_author and x.role == Contributor.PRIMARY_AUTHOR_ROLE:
                primary_author = x.contributor
            elif x.role in Contributor.AUTHOR_ROLES:
                other_authors.append(x.contributor)
        if primary_author:
            return [primary_author] + sorted(other_authors, key=lambda x: x.name)
        else:
            return other_authors

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
            CoverageRecord, join_clause).filter(
                Edition.data_source_id.in_(edition_data_source_ids))
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

    def set_cover(self, resource):
        self.cover = resource
        self.cover_full_url = resource.final_url
        self.cover_thumbnail_url = resource.scaled_url
        print self.cover_full_url, self.cover_thumbnail_url

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
            get_one_or_create(
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
        """Find the best open-access Resource for this LicensePool."""
        open_access = Resource.OPEN_ACCESS_DOWNLOAD

        best = None
        for l in self.primary_identifier.resources:
            if l.rel != open_access:
                continue
            if l.media_type.startswith("application/epub+zip"):
                best = l
                # A Project Gutenberg-ism: if we find a 'noimages' epub,
                # we'll keep looking in hopes of finding a better one.
                if not 'noimages' in best.href:
                    break
        return best

    def best_cover_within_distance(self, distance, threshold=0.5):
        _db = Session.object_session(self)
        flattened_data = [self.primary_identifier.id]
        if distance > 0:
            data = Identifier.recursively_equivalent_identifier_ids(
                _db, flattened_data, distance, threshold=threshold)
            flattened_data = Identifier.flatten_identifier_ids(data)

        return Identifier.best_cover_for(_db, flattened_data)
        

    def calculate_presentation(self, debug=False):
        if not self.sort_title:
            self.sort_title = TitleProcessor.sort_title_for(self.title)
        sort_names = []
        display_names = []
        for author in self.author_contributors:
            display_name = author.display_name or author.name
            family_name = author.family_name or author.name
            display_names.append([family_name, display_name])
            sort_names.append(author.name)
        self.author = ", ".join([x[1] for x in sorted(display_names)])
        self.sort_author = " ; ".join(sorted(sort_names))

        for distance in (0, 5):
            # If there's a cover directly associated with the
            # Edition's primary ID, use it. Otherwise, find the
            # best cover associated with any related identifier.
            best_cover, covers = self.best_cover_within_distance(distance)
            if best_cover:
                self.set_cover(best_cover)
                break

        # Now that everything's calculated, print it out.
        if debug:
            t = u"%s (by %s, pub=%s)" % (
                self.title, self.author, self.publisher)
            print t.encode("utf8")
            print " language=%s" % self.language
            if self.cover:
                print " cover=" + self.cover.mirrored_path
            print


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


class Work(Base):

    CHARACTER_APPEAL = "Character"
    LANGUAGE_APPEAL = "Language"
    SETTING_APPEAL = "Setting"
    STORY_APPEAL = "Story"
    UNKNOWN_APPEAL = "Unknown"
    NOT_APPLICABLE_APPEAL = "Not Applicable"
    NO_APPEAL = "None"

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

    # One Work may participate in many WorkGenre assignments.
    genres = association_proxy('work_genres', 'genre',
                               creator=WorkGenre.from_genre)
    work_genres = relationship("WorkGenre", backref="work",
                               cascade="all, delete-orphan")
    audience = Column(Unicode, index=True)
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
    quality = Column(Float, index=True)

    # The overall rating given to this work.
    rating = Column(Float, index=True)

    # The overall current popularity of this work.
    popularity = Column(Float, index=True)

    appeal_type = Enum(CHARACTER_APPEAL, LANGUAGE_APPEAL, SETTING_APPEAL,
                       STORY_APPEAL, NOT_APPLICABLE_APPEAL, NO_APPEAL,
                       name="appeal")

    primary_appeal = Column(appeal_type, default=None, index=True)
    secondary_appeal = Column(appeal_type, default=None, index=True)

    appeal_character = Column(Float, default=None, index=True)
    appeal_language = Column(Float, default=None, index=True)
    appeal_setting = Column(Float, default=None, index=True)
    appeal_story = Column(Float, default=None, index=True)

    # A Work may be merged into one other Work.
    was_merged_into_id = Column(Integer, ForeignKey('works.id'), index=True)
    was_merged_into = relationship("Work", remote_side = [id])

    @property
    def title(self):
        if self.primary_edition:
            return self.primary_edition.title
        return None

    @property
    def sort_title(self):
        return self.primary_edition.sort_title or self.primary_edition.title

    @property
    def subtitle(self):
        return self.primary_edition.subtitle

    @property
    def series(self):
        return self.primary_edition.series

    @property
    def author(self):
        if self.primary_edition:
            return self.primary_edition.author
        return None

    @property
    def sort_author(self):
        return self.primary_edition.sort_author or self.primary_edition.author

    @property
    def language(self):
        if self.primary_edition:
            return self.primary_edition.language
        return None

    @property
    def language_code(self):
        return self.primary_edition.language_code

    @property
    def publisher(self):
        return self.primary_edition.publisher

    @property
    def imprint(self):
        return self.primary_edition.imprint

    @property
    def cover_full_url(self):
        return self.primary_edition.cover_full_url

    @property
    def cover_thumbnail_url(self):
        return self.primary_edition.cover_thumbnail_url

    def __repr__(self):
        return (u'%s "%s" (%s) %s %s (%s wr, %s lp)' % (
                self.id, self.title, self.author, ", ".join([g.name for g in self.genres]), self.language,
                len(self.editions), len(self.license_pools))).encode("utf8")

    def set_summary(self, resource):
        self.summary = resource
        # TODO: clean up the content
        if resource:
            self.summary_text = resource.content

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
        q = q.filter(WorkGenre.genre==None)
        return q

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
            print "NOT MERGING %r into %r, similarity is only %.3f." % (
                self, target_work, similarity)
        else:
            print "MERGING %r into %r, similarity is %.3f." % (
                self, target_work, similarity)
            target_work.license_pools.extend(self.license_pools)
            target_work.editions.extend(self.editions)
            target_work.calculate_presentation()
            print "The resulting work: %r" % target_work
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
            _db, flattened_data, Resource.IMAGE).filter(
                Resource.mirrored==True).filter(Resource.scaled==True).order_by(
                Resource.quality.desc())

    def all_descriptions(self):
        _db = Session.object_session(self)
        primary_identifier_ids = [
            x.primary_identifier.id for x in self.editions]
        data = Identifier.recursively_equivalent_identifier_ids(
            _db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = Identifier.flatten_identifier_ids(data)
        return Identifier.resources_for_identifier_ids(
            _db, flattened_data, Resource.DESCRIPTION).filter(
                Resource.content != None).order_by(
                Resource.quality.desc())

    def set_primary_edition(self):
        """Which of this Work's Editions should be used as the default?
        """
        old_primary = self.primary_edition
        champion = None
        for wr in self.editions:
            # Something is better than nothing.
            if not champion:
                champion = wr
                continue

            # A edition with no license pool will only be chosen if
            # there is no other alternatice.
            if not wr.license_pool:
                continue

            # Something with a license pool is better than something
            # without.
            if not champion.license_pool:
                champion = wr

            # Open access is better than not.
            if (wr.license_pool.open_access
                and not champion.license_pool.open_access):
                champion = wr
                continue

            # Higher Gutenberg numbers beat lower Gutenberg numbers.
            if (wr.data_source.name == DataSource.GUTENBERG
                and champion.data_source.name == DataSource.GUTENBERG):
                champion_id = int(champion.primary_identifier.identifier)
                competitor_id = int(wr.primary_identifier.identifier)
                if competitor_id > champion_id:
                    champion = wr
                    continue

            # At the moment, anything is better than 3M, because we
            # can't actually check out 3M books.
            if (champion.data_source.name == DataSource.THREEM
                and wr.data_source.name != DataSource.THREEM):
                champion = wr
                continue

            # More licenses is better than fewer.
            if (wr.license_pool.licenses_owned
                > champion.license_pool.licenses_owned):
                champion = wr
                continue

            # More available licenses is better than fewer.
            if (wr.license_pool.licenses_available
                > champion.license_pool.licenses_available):
                champion = wr
                continue

            # Fewer patrons in the hold queue is better than more.
            if (wr.license_pool.patrons_in_hold_queue
                < champion.license_pool.patrons_in_hold_queue):
                champion = wr
                continue

        if old_primary and old_primary != champion:
            old_primary.is_primary_for_work = False
        if champion:
            champion.is_primary_for_work = True
        self.primary_edition = champion


    def calculate_presentation(self, choose_edition=True,
                               classify=True, choose_summary=True,
                               calculate_quality=True, debug=True):
        """Determine the following information:
        
        * Which Edition is the 'primary'. The default view of the
        Work will be taken from the primary Edition.

        * Subject-matter classifications for the work.
        * Whether or not the work is fiction.
        * The intended audience for the work.
        * The best available summary for the work.
        * The overall popularity of the work.
        """
        if choose_edition or not self.primary_edition:
            self.set_primary_edition()

        if self.primary_edition:
            self.primary_edition.calculate_presentation()

        if not (classify or choose_summary or calculate_quality):
            return

        # Find all related IDs that might have associated descriptions
        # or classifications.
        _db = Session.object_session(self)
        primary_identifier_ids = [
            x.primary_identifier.id for x in self.editions]
        data = Identifier.recursively_equivalent_identifier_ids(
            _db, primary_identifier_ids, 5, threshold=0.5)
        flattened_data = Identifier.flatten_identifier_ids(data)

        if classify:
            workgenres, self.fiction, self.audience = self.assign_genres(
                flattened_data)

        if choose_summary:
            summary, summaries = Identifier.evaluate_summary_quality(
                _db, flattened_data)
            # TODO: clean up the content
            self.set_summary(summary)

        # If this is a Project Gutenberg book, treat the number of IDs
        # associated with the work (~the number of editions of the
        # work published in modern times) as a measurement of
        # popularity.
        if self.primary_edition and self.primary_edition.data_source.name==DataSource.GUTENBERG:
            oclc_linked_data = DataSource.lookup(
                _db, DataSource.OCLC_LINKED_DATA)
            self.primary_edition.primary_identifier.add_measurement(
                oclc_linked_data, Measurement.POPULARITY, 
                len(flattened_data)/3.0)
            # Only consider the quality signals associated with the
            # primary edition. Otherwise texts that have multiple
            # Gutenberg editions will drag down the quality of popular
            # books.
            flattened_data = [self.primary_edition.primary_identifier.id]

        if calculate_quality:
            self.calculate_quality(flattened_data)

        # Now that everything's calculated, print it out.
        if debug:
            t = u"WORK %s (by %s)" % (self.title, self.author)
            print t.encode("utf8")
            print " language=%s" % self.language
            print " quality=%s" % self.quality
            if self.fiction:
                fiction = "Fiction"
            elif self.fiction == False:
                fiction = "Nonfiction"
            else:
                fiction = "???"
            print " %(fiction)s a=%(audience)s" % (
                dict(fiction=fiction,
                     audience=self.audience))
            print " " + ", ".join(repr(wg) for wg in self.work_genres)
            if self.summary:
                d = " Description (%.2f) %s" % (
                    self.summary.quality, self.summary.content[:100])
                print d.encode("utf8")
            print

    def calculate_quality(self, flattened_data):
        _db = Session.object_session(self)
        quantities = [Measurement.POPULARITY, Measurement.RATING,
                      Measurement.DOWNLOADS]
        measurements = _db.query(Measurement).filter(
            Measurement.identifier_id.in_(flattened_data)).filter(
                Measurement.is_most_recent==True).filter(
                    Measurement.quantity_measured.in_(quantities)).all()

        self.quality = Measurement.overall_quality(measurements)

    def assign_genres(self, identifier_ids, cutoff=0.15):
        _db = Session.object_session(self)

        classifications = Identifier.classifications_for_identifier_ids(
            _db, identifier_ids)
        fiction_s = Counter()
        genre_s = Counter()
        audience_s = Counter()
        for classification in classifications:
            subject = classification.subject
            if (not subject.fiction and not subject.genre
                and not subject.audience):
                continue
            weight = classification.scaled_weight
            fiction_s[subject.fiction] += weight
            audience_s[subject.audience] += weight
            if subject.genre:
                genre_s[subject.genre] += weight
        if fiction_s[True] > fiction_s[False]:
            fiction = True
        elif fiction_s[False] > fiction_s[True]:
            fiction = False
        else:
            fiction = None
        unmarked = audience_s[None]
        audience = Classifier.AUDIENCE_ADULT

        if audience_s[Classifier.AUDIENCE_YOUNG_ADULT] > unmarked:
            audience = Classifier.AUDIENCE_YOUNG_ADULT
        elif audience_s[Classifier.AUDIENCE_CHILDREN] > unmarked:
            audience = Classifier.AUDIENCE_CHILDREN

        # Clear any previous genre assignments.
        for i in self.work_genres:
            _db.delete(i)
        self.work_genres = []

        # Consolidate parent genres into their heaviest subgenre.
        genre_s = Classifier.consolidate_weights(genre_s)
        total_weight = float(sum(genre_s.values()))
        workgenres = []

        # First, strip out the stragglers.
        for g, score in genre_s.items():
            affinity = score / total_weight
            if affinity < cutoff:
                total_weight -= score
                del genre_s[g]

        # Assign WorkGenre objects to the remainder.
        for g, score in genre_s.items():
            affinity = score / total_weight
            if not isinstance(g, Genre):
                g, ignore = Genre.lookup(_db, g.name)
            wg, ignore = get_one_or_create(
                _db, WorkGenre, work=self, genre=g)
            wg.affinity = score/total_weight
            workgenres.append(wg)

        return workgenres, fiction, audience

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

class Measurement(Base):
    """A  measurement of some numeric quantity associated with a
    Identifier.
    """
    __tablename__ = 'measurements'

    # Some common measurement types
    POPULARITY = "http://library-simplified.com/rel/popularity"
    RATING = "http://schema.org/ratingValue"
    DOWNLOADS = "https://schema.org/UserDownloads"
    PAGE_COUNT = "https://schema.org/numberOfPages"

    GUTENBERG_FAVORITE = "http://library-simplified.com/rel/lists/gutenberg-favorite"

    # If a book's popularity measurement is found between index n and
    # index n+1 on this list, it is in the nth percentile for
    # popularity and its 'popularity' value should be n * 0.01.
    # 
    # These values are empirically determined and may change over
    # time.
    POPULARITY_PERCENTILES = {
        DataSource.OVERDRIVE : [1, 1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 9, 10, 10, 11, 12, 13, 14, 15, 15, 16, 18, 19, 20, 21, 22, 24, 25, 26, 28, 30, 31, 33, 35, 37, 39, 41, 43, 46, 48, 51, 53, 56, 59, 63, 66, 70, 74, 78, 82, 87, 92, 97, 102, 108, 115, 121, 128, 135, 142, 150, 159, 168, 179, 190, 202, 216, 230, 245, 260, 277, 297, 319, 346, 372, 402, 436, 478, 521, 575, 632, 702, 777, 861, 965, 1100, 1248, 1428, 1665, 2020, 2560, 3535, 5805],
        DataSource.AMAZON : [14937330, 1974074, 1702163, 1553600, 1432635, 1327323, 1251089, 1184878, 1131998, 1075720, 1024272, 978514, 937726, 898606, 868506, 837523, 799879, 770211, 743194, 718052, 693932, 668030, 647121, 627642, 609399, 591843, 575970, 559942, 540713, 524397, 511183, 497576, 483884, 470850, 458438, 444475, 432528, 420088, 408785, 398420, 387895, 377244, 366837, 355406, 344288, 333747, 324280, 315002, 305918, 296420, 288522, 279185, 270824, 262801, 253865, 246224, 238239, 230537, 222611, 215989, 208641, 202597, 195817, 188939, 181095, 173967, 166058, 160032, 153526, 146706, 139981, 133348, 126689, 119201, 112447, 106795, 101250, 96534, 91052, 85837, 80619, 75292, 69957, 65075, 59901, 55616, 51624, 47598, 43645, 39403, 35645, 31795, 27990, 24496, 20780, 17740, 14102, 10498, 7090, 3861],
        # This is a percentile list of OCLC Work IDs and OCLC Numbers
        # associated with Project Gutenberg texts via OCLC Linked
        # Data.
        DataSource.OCLC_LINKED_DATA : [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 7, 8, 8, 9, 10, 11, 12, 14, 15, 18, 21, 29, 41, 81],
    }

    DOWNLOAD_PERCENTILES = {
        DataSource.GUTENBERG : [0, 1, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 12, 12, 12, 13, 14, 14, 15, 15, 16, 16, 17, 18, 18, 19, 19, 20, 21, 21, 22, 23, 23, 24, 25, 26, 27, 28, 28, 29, 30, 32, 33, 34, 35, 36, 37, 38, 40, 41, 43, 45, 46, 48, 50, 52, 55, 57, 60, 62, 65, 69, 72, 76, 79, 83, 87, 93, 99, 106, 114, 122, 130, 140, 152, 163, 179, 197, 220, 251, 281, 317, 367, 432, 501, 597, 658, 718, 801, 939, 1065, 1286, 1668, 2291, 4139]
    }

    RATING_SCALES = {
        DataSource.OVERDRIVE : [1, 5],
        DataSource.AMAZON : [1, 5],
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
                        rating_weight=0.7):
        """Turn a bunch of measurements into an overall measure of quality."""
        if popularity_weight + rating_weight != 1.0:
            raise ValueError(
                "Popularity weight and rating weight must sum to 1! (%.2f + %.2f)" % (
                    popularity_weight, rating_weight)
        )
        popularities = []
        ratings = []
        for m in measurements:
            l = None
            if m.quantity_measured in (cls.POPULARITY, cls.DOWNLOADS):
                l = popularities
            elif m.quantity_measured == cls.RATING:
                l = ratings
            if l is not None:
                l.append(m)
        popularity = cls._average_normalized_value(popularities)
        rating = cls._average_normalized_value(ratings)
        if popularity is None and rating is None:
            # We have absolutely no idea about the quality of this work.
            return 0
        if popularity is not None and rating is None:
            # Our idea of the quality depends entirely on the work's popularity.
            return popularity
        if rating is not None and popularity is None:
            # Our idea of the quality depends entirely on the work's rating.
            return rating

        # We have both popularity and rating.
        final = (popularity * popularity_weight) + (rating * rating_weight)
        print "(%.2f * %.2f) + (%.2f * %.2f) = %.2f" % (
            popularity, popularity_weight, rating, rating_weight, final)
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

        return self._normalized_value


class Resource(Base):
    """An external resource that may be mirrored locally."""

    __tablename__ = 'resources'

    # Some common link relations.
    CANONICAL = "canonical"
    OPEN_ACCESS_DOWNLOAD = "http://opds-spec.org/acquisition/open-access"
    IMAGE = "http://opds-spec.org/image"
    THUMBNAIL_IMAGE = "http://opds-spec.org/image/thumbnail"
    SAMPLE = "http://opds-spec.org/acquisition/sample"
    ILLUSTRATION = "http://library-simplified.com/rel/illustration"
    REVIEW = "http://schema.org/Review"
    DESCRIPTION = "http://schema.org/description"
    AUTHOR = "http://schema.org/author"

    # TODO: Is this the appropriate relation?
    DRM_ENCRYPTED_DOWNLOAD = "http://opds-spec.org/acquisition/"

    # How many votes is the initial quality estimate worth?
    ESTIMATED_QUALITY_WEIGHT = 5

    id = Column(Integer, primary_key=True)

    # A Resource is always associated with some Identifier.
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)

    # A Resource may also be associated with some LicensePool which
    # controls scarce access to it.
    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True)

    # Who provides this resource?
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True)

    # Many Editions may use this resource as their cover image.
    cover_editions = relationship("Edition", backref="cover", foreign_keys=[Edition.cover_id])

    # Many Works may use this resource as their summary.
    summary_works = relationship("Work", backref="summary", foreign_keys=[Work.summary_id])

    # The relation between the book identified by the Identifier
    # and the resource.
    rel = Column(Unicode, index=True)

    # The actual URL to the resource.
    href = Column(Unicode)

    # Whether or not we have a local copy of the representation.
    mirrored = Column(Boolean, default=False, index=True)

    # The path to our mirrored representation. This can be converted
    # into a URL for serving to a client.
    mirrored_path = Column(Unicode)

    # Whether or not we have a local scaled copy of the
    # representation.
    scaled = Column(Boolean, default=False, index=True)

    # The path to our scaled-down representation. This can be converted
    # into a URL for serving to a client.
    scaled_path = Column(Unicode)

    # The last time we tried to update the mirror.
    mirror_date = Column(DateTime, index=True)

    # The HTTP status code the last time we updated the mirror
    mirror_status = Column(Unicode)

    # A human-readable description of what happened the last time
    # we updated the mirror.
    mirror_exception = Column(Unicode)

    # Sometimes the content of a resource can just be stuck into the
    # database.
    content = Column(Unicode)

    # We need this information to determine the appropriateness of this
    # resource without neccessarily having access to the file.
    media_type = Column(Unicode, index=True)
    language = Column(Unicode, index=True)
    file_size = Column(Integer)
    image_height = Column(Integer, index=True)
    image_width = Column(Integer, index=True)

    scaled_size = Column(Integer)
    scaled_height = Column(Integer, index=True)
    scaled_width = Column(Integer, index=True)

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

    URL_ROOTS = dict(
        content_cafe_mirror="https://s3.amazonaws.com/book-covers.nypl.org/CC",
        scaled_content_cafe_mirror="https://s3.amazonaws.com/book-covers.nypl.org/scaled/CC",
        original_overdrive_covers_mirror="https://s3.amazonaws.com/book-covers.nypl.org/Overdrive",
        scaled_overdrive_covers_mirror="https://s3.amazonaws.com/book-covers.nypl.org/scaled/300/Overdrive",
        original_threem_covers_mirror="https://s3.amazonaws.com/book-covers.nypl.org/3M",
        scaled_threem_covers_mirror="https://s3.amazonaws.com/book-covers.nypl.org/scaled/300/3M",
        gutenberg_illustrated_mirror="https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated"
    )

    @property
    def final_url(self):        
        """URL to the full version of this resource.
        
        This link will be served to the client.
        """
        if self.mirrored_path:
            url = self.mirrored_path % self.URL_ROOTS
        else:
            url = self.href
        return url

    @property
    def scaled_url(self):        
        """URL to the scaled-down version of this resource.

        This link will be served to the client.
        """
        if not self.scaled_path:
            return self.final_url
        return self.scaled_path % self.URL_ROOTS

    def local_path(self, expansions):
        """Path to the original representation on disk."""
        return self.mirrored_path % expansions

    def local_scaled_path(self, expansions):
        """Path to the scaled representation on disk."""
        return self.scaled_path % expansions

    @property
    def is_image(self):
        return self.media_type and self.media_type.startswith("image/")

    def could_not_mirror(self):
        """We tried to mirror this resource and failed."""
        if self.mirrored:
            # We already have a mirrored copy, so just leave it alone.
            return
        self.mirrored = False
        self.mirror_date = datetime.datetime.utcnow()
        self.mirrored_path = None
        self.mirror_status = 404
        self.media_type = None
        self.file_size = None
        self.image_height = None
        self.image_width = None

    def set_content(self, content, media_type):
        """Store the content directly in the database."""
        self.content = content
        self.mirrored = True
        self.mirror_status = 200
        if media_type:
            media_type = media_type.lower()
        self.media_type = media_type
        self.file_size = len(content)

    def mirrored_to(self, path, media_type, content=None):
        """We successfully mirrored this resource to disk."""
        self.mirrored = True
        self.mirrored_path = path
        self.mirror_status = 200
        self.mirror_date = datetime.datetime.utcnow()
        if media_type:
            self.media_type = media_type

        # If we were provided with the content, make sure the
        # metadata reflects the content.
        #
        # TODO: We don't check the actual file because it's got a
        # variable expansion in it at this point.
        if content is not None:
            self.file_size = len(content)
        if content and self.is_image:
            # Try to load it into PIL and determine height and width.
            try:
                image = Image.open(StringIO(content))
                self.image_width, self.image_height = image.size
            except IOError, e:
                self.mirror_exception = "Content is not an image."

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

    def update_quality(self):
        """Combine `estimated_quality` with `voted_quality` to form `quality`.
        """
        estimated_weight = self.ESTIMATED_QUALITY_WEIGHT
        votes_for_quality = self.votes_for_quality or 0
        total_weight = estimated_weight + votes_for_quality

        total_quality = (((self.estimated_quality or 0) * self.ESTIMATED_QUALITY_WEIGHT) + 
                         ((self.voted_quality or 0) * votes_for_quality))
        self.quality = total_quality / float(total_weight)

    def scale(self, destination_width, destination_height,
              original_path_expansions, 
              scaled_path_expansions, original_variable_to_scaled_variable,
              force=False):
        """Create a scaled-down version of this resource."""
        if not self.is_image:
            raise ValueError(
                "Cannot scale down non-image resource: type %s." 
                % self.media_type)
        if not self.mirrored:
            raise ValueError(
                "Cannot scale down an image that has not been mirrored.")

        scaled_path_template = self.mirrored_path % (
            original_variable_to_scaled_variable)
        scaled_path = scaled_path_template % scaled_path_expansions

        already_scaled = False
        if os.path.exists(scaled_path) and not force:
            scaled_image = Image.open(scaled_path)
            already_scaled = True
        else:
            path = self.local_path(original_path_expansions)
            try:
                image = Image.open(path)
                width, height = image.size

                if height <= destination_height:
                    # The image doesn't need to be scaled; just save it.
                    scaled_image = image
                else:
                    proportion = float(destination_height) / height
                    destination_width = int(width * proportion)
                    try:
                        scaled_image = image.resize(
                            (destination_width, destination_height), 
                            Image.ANTIALIAS)
                    except IOError, e:
                        # I'm not sure why, but sometimes just trying
                        # it again works.
                        scaled_image = image.resize(
                            (destination_width, destination_height), 
                            Image.ANTIALIAS)
            except IOError, e:
                scaled_image = None

        # Save the scaled image.
        if scaled_image:
            if not already_scaled:
                d, f = os.path.split(scaled_path)
                if not os.path.exists(d):
                    os.makedirs(d)
                scaled_image.save(scaled_path)
            self.scaled = True
            self.scaled_path = scaled_path_template
            self.scaled_width, self.scaled_height = scaled_image.size
        else:
            self.scaled_path = None
            self.scaled = False
        return already_scaled

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
    def lookup(cls, _db, name, autocreate=False):
        if autocreate:
            m = get_one_or_create
        else:
            m = get_one
        if isinstance(name, GenreData):
            name = name.name
        result = m(_db, Genre, name=name)
        if isinstance(result, tuple):
            return result
        else:
            return result, False

    @property
    def self_and_subgenres(self):
        _db = Session.object_session(self)
        genres = []
        for genre_data in classifier.genres[self.name].self_and_subgenres:
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
    TAG = Classifier.TAG   # Folksonomic tags.
    GUTENBERG_BOOKSHELF = Classifier.GUTENBERG_BOOKSHELF
    TOPIC = Classifier.TOPIC
    PLACE = Classifier.PLACE
    PERSON = Classifier.PERSON
    ORGANIZATION = Classifier.ORGANIZATION


    by_uri = {
        "http://purl.org/dc/terms/LCC" : LCC,
        "http://purl.org/dc/terms/LCSH" : LCSH,
    }

    __tablename__ = 'subjects'
    id = Column(Integer, primary_key=True)
    # Type should be one of the constants in this class.
    type = Column(Unicode, index=True)

    # Formal identifier for the subject (e.g. "300" for Dewey Decimal
    # System's Social Sciences subject.)
    identifier = Column(Unicode, index=True)

    # Human-readable name, if different from the
    # identifier. (e.g. "Social Sciences" for DDC 300)
    name = Column(Unicode, default=None)

    # Whether classification under this subject implies anything about
    # the fiction/nonfiction status of a book.
    fiction = Column(Boolean, default=None)

    # Whether classification under this subject implies anything about
    # the book's audience.
    audience = Column(
        Enum("Adult", "Young Adult", "Children", name="audience"),
        default=None)

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
        a = u'[%s:%s%s%s%s%s]' % (
            self.type, self.identifier, name, fiction, audience, genre)
        return a.encode("utf8")

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
            subject.checked = True
            classifier = Classifier.classifiers.get(subject.type, None)
            if not classifier:
                continue
            genredata, audience, fiction = classifier.classify(subject)
            if genredata:
                genre, was_new = Genre.lookup(_db, genredata.name, True)
                subject.genre = genre
            if audience:
                subject.audience = audience
            if fiction is not None:
                subject.fiction = fiction
            if genredata or audience or fiction:
                print subject
            counter += 1
            if not counter % batch_size:
                _db.commit()
        _db.commit()

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

# Non-database objects.

class LaneList(object):
    """A list of lanes such as you might see in an OPDS feed."""

    def __repr__(self):
        parent = ""
        if self.parent:
            parent = "parent=%s, " % self.parent.name

        return "<LaneList: %slanes=[%s]>" % (
            parent,
            ", ".join([repr(x) for x in self.lanes])
        )

    @classmethod
    def from_description(self, _db, parent_lane, description):
        lanes = LaneList(parent_lane)
        if parent_lane:
            default_fiction = parent_lane.fiction
            default_audience = parent_lane.audience
        else:
            default_fiction = Lane.FICTION_DEFAULT_FOR_GENRE
            default_audience = Classifier.AUDIENCE_ADULT

        for lane_description in description:
            if isinstance(lane_description, GenreData):
                # This very simple lane is the default view for a genre.
                genre = lane_description
                lane = Lane(_db, genre.name, [genre], True, default_fiction,
                            default_audience, parent_lane)
            elif isinstance(lane_description, Lane):
                # The Lane object has already been created.
                lane = lane_description
                lane.parent = parent_lane
            else:
                # A more complicated lane. Its description is a bunch
                # of arguments to the Lane constructor.
                l = lane_description
                lane = Lane(_db, l['name'], l.get('genres', []), 
                            l.get('include_subgenres', True),
                            l.get('fiction', default_fiction),
                            l.get('audience', default_audience),
                            parent_lane,
                            l.get('sublanes', [])
                        )                            
            lanes.add(lane)
            for sublane in lane.sublanes.lanes:
                lanes.add(sublane)

        return lanes

    def __init__(self, parent=None):
        self.parent = parent
        self.lanes = []
        self.by_name = dict()

    def __iter__(self):
        return self.lanes.__iter__()

    def add(self, lane):
        if lane.parent == self.parent:
            self.lanes.append(lane)
        if lane.name in self.by_name:
            raise ValueError("Duplicate lane: %s" % lane.name)
        self.by_name[lane.name] = lane


class Lane(object):

    """A set of books that would go together in a display."""

    UNCLASSIFIED = "unclassified"
    BOTH_FICTION_AND_NONFICTION = "both fiction and nonfiction"
    FICTION_DEFAULT_FOR_GENRE = "fiction default for genre"

    def __repr__(self):
        if self.sublanes.lanes:
            sublanes = " (sublanes=%d)" % len(self.sublanes.lanes)
        else:
            sublanes = ""
        return "<Lane %s%s>" % (self.name, sublanes)

    @classmethod
    def everything(cls, _db):
        """Return a synthetic Lane that matches everything."""
        return Lane(_db, "", [], True, Lane.BOTH_FICTION_AND_NONFICTION,
                    None)

    def __init__(self, _db, name, genres, include_subgenres=True,
                 fiction=True, audience=Classifier.AUDIENCE_ADULT,
                 parent=None, sublanes=[], appeal=None):
        self.name = name
        self.parent = parent
        self._db = _db
        self.appeal = appeal

        if genres in (None, self.UNCLASSIFIED):
            # We will only be considering works that are not
            # classified under a genre.
            self.genres = None
            self.include_subgenres = None
        else:
            if not isinstance(genres, list):
                genres = [genres]
            # Turn names or GenreData objects into Genre objects. 
            self.genres = []
            for genre in genres:
                if not isinstance(genre, Genre):
                    genre, ignore = Genre.lookup(_db, genre)
                self.genres.append(genre)
            self.include_subgenres=include_subgenres
        self.fiction = fiction
        self.audience = audience
        self.sublanes = LaneList.from_description(_db, self, sublanes)

    def search(self, languages, query):
        """Find works in this lane that match a search query.
        
        TODO: Current implementation is incredibly bad and does
        a direct database search using ILIKE.
        """
        if isinstance(languages, basestring):
            languages = [languages]

        k = "%" + query + "%"
        q = self.works(languages=languages, fiction=None).filter(
            or_(Edition.title.ilike(k),
                Edition.author.ilike(k)))
        q = q.order_by(Work.quality.desc())
        return q

    def quality_sample(
            self, languages, quality_min_start,
            quality_min_rock_bottom, target_size, availability):
        """Randomly select Works from this Lane that meet minimum quality
        criteria.

        Bring the quality criteria as low as necessary to fill a feed
        of the given size, but not below `quality_min_rock_bottom`.
        """
        if isinstance(languages, basestring):
            languages = [languages]

        quality_min = quality_min_start
        previous_quality_min = None
        results = []
        while (quality_min >= quality_min_rock_bottom
               and len(results) < target_size):
            remaining = target_size - len(results)
            query = self.works(languages=languages, availability=availability)
            query = query.filter(
                Work.quality >= quality_min,
            )

            if previous_quality_min is not None:
                query = query.filter(
                    Work.quality < previous_quality_min)
            start = time.time()
            query = query.order_by(func.random()).limit(remaining)
            #results.extend([x for x in query.all() if x.license_pools])
            results.extend(query.all())
            print "Quality %.1f got %d results for %s in %.2fsec" % (
                quality_min, len(results), self.name, time.time()-start
                )

            if quality_min == quality_min_rock_bottom:
                # We can't lower the bar any more.
                break

            # Lower the bar, in case we didn't get enough results.
            previous_quality_min = quality_min
            quality_min *= 0.5
            if quality_min < quality_min_rock_bottom:
                quality_min = quality_min_rock_bottom
        return results

    CURRENTLY_AVAILABLE = "currently_available"
    ALL = "all"

    def works(self, languages, fiction=None, availability=ALL):
        """Find Works that will go together in this Lane.

        Works will:

        * Be in one of the languages listed in `languages`.

        * Be filed under of the genres listed in `self.genres` (or, if
          `self.include_subgenres` is True, any of those genres'
          subgenres).

        * Have the same appeal as `self.appeal`, if `self.appeal` is present.

        * Are intended for the audience in `self.audience`.

        * Are fiction (if `self.fiction` is True), or nonfiction (if fiction
          is false), or of the default fiction status for the genre
          (if fiction==FICTION_DEFAULT_FOR_GENRE and all genres have
          the same default fiction status). If fiction==None, no fiction
          restriction is applied.

        :param fiction: Override the fiction setting found in `self.fiction`.

        """
        audience = self.audience
        if fiction is None:
            if self.fiction is not None:
                fiction = self.fiction
            else:
                fiction = self.FICTION_DEFAULT_FOR_GENRE
        q = self._db.query(Work).join(Work.primary_edition).options(
            joinedload('license_pools').joinedload('data_source'),
            joinedload('work_genres')
        )
        if availability == self.CURRENTLY_AVAILABLE:
            q = q.join(Work.license_pools)
            or_clause = or_(
                LicensePool.open_access==True,
                LicensePool.licenses_available > 0)
            q = q.filter(or_clause)

        if self.genres is None and fiction in (True, False, self.UNCLASSIFIED):
            # No genre plus a boolean value for `fiction` means
            # fiction or nonfiction not associated with any genre.
            q = Work.with_no_genres(q)
        elif self.genres is not None:
            # Find works that are assigned to the given genres. This
            # may also turn into a restriction on the fiction status.
            fiction_default_by_genre = (fiction == self.FICTION_DEFAULT_FOR_GENRE)
            if fiction_default_by_genre:
                # Unset `fiction`. We'll set it again when we find out
                # whether we've got fiction or nonfiction genres.
                fiction = None

            genres = []
            for genre in self.genres:
                if self.include_subgenres:
                    genres.extend(genre.self_and_subgenres)
                else:
                    genres.append(genre)

                if fiction_default_by_genre:
                    if fiction is None:
                        fiction = genre.default_fiction
                    elif fiction != genre.default_fiction:
                        raise ValueError(
                            "I was told to use the default fiction restriction, but the genres %r include contradictory fiction restrictions.")
            if genres:
                q = q.join(Work.work_genres)
                q = q.filter(WorkGenre.genre_id.in_([g.id for g in genres]))

        if self.audience != None:
            q = q.filter(Work.audience==self.audience)

        if self.appeal != None:
            q = q.filter(Work.primary_appeal==self.appeal)

        if fiction == self.UNCLASSIFIED:
            q = q.filter(Work.fiction==None)
        elif fiction != self.BOTH_FICTION_AND_NONFICTION:
            q = q.filter(Work.fiction==fiction)

        q = q.filter(
            Edition.language.in_(languages),
            Work.was_merged_into == None,
        )
        return q


class WorkFeed(object):
    
    """Identify a certain page in a certain feed."""

    active_facet_for_field = {
        Edition.title : "title",
        Edition.sort_title : "title",
        Edition.sort_author : "author",
        Edition.author : "author"
    }

    CURRENTLY_AVAILABLE = "available"
    ALL = "all"

    def __init__(self, lane, languages, order_by=None,
                 availability=CURRENTLY_AVAILABLE):
        if isinstance(languages, basestring):
            languages = [languages]
        self.languages = languages
        self.lane = lane
        if not order_by:
            order_by = []
        elif not isinstance(order_by, list):
            order_by = [order_by]
        self.order_by = order_by
        # In addition to the given order, we order by author,
        # then title, then work ID.
        for i in (Edition.sort_author, 
                  Edition.sort_title, 
                  Work.id):
            if i not in self.order_by:
                self.order_by.append(i)
        self.active_facet = self.active_facet_for_field.get(order_by[0], None)
        self.availability = availability

    def page_query(self, _db, last_edition_seen, page_size):
        """A page of works."""

        query = self.lane.works(self.languages, availability=self.availability)

        if last_edition_seen:
            # Only find records that show up after the last one seen.
            primary_order_field = self.order_by[0]
            last_value = getattr(last_edition_seen, primary_order_field.name)

            # This means works where the primary ordering field has a
            # higher value.
            clause = (primary_order_field > last_value)

            base_and_clause = (primary_order_field == last_value)
            for next_order_field in self.order_by[1:]:
                # OR, it means works where all the previous ordering
                # fields have the same value as the last work seen,
                # and this next ordering field has a higher value.
                new_value = getattr(last_edition_seen, next_order_field.name)
                if new_value != None:
                    clause = or_(clause,
                                 and_(base_and_clause, 
                                      (next_order_field > new_value)))
                base_and_clause = and_(base_and_clause,
                                       (next_order_field == new_value))
            query = query.filter(clause)

        query = query.order_by(*self.order_by).limit(page_size)
        return query

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

    # One LicensePool can have many Loans.
    loans = relationship('Loan', backref='license_pool')

    # One LicensePool can have many Representations.
    representations = relationship("Representation", backref="license_pool")

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool")

    # One LicensePool can control access to many Resources.
    resources = relationship("Resource", backref="license_pool")

    # The date this LicensePool first became available.
    availability_time = Column(DateTime, index=True)

    open_access = Column(Boolean)
    last_checked = Column(DateTime)
    licenses_owned = Column(Integer,default=0)
    licenses_available = Column(Integer,default=0)
    licenses_reserved = Column(Integer,default=0)
    patrons_in_hold_queue = Column(Integer,default=0)

    # A Identifier should have at most one LicensePool.
    __table_args__ = (UniqueConstraint('identifier_id'),)

    @classmethod
    def for_foreign_id(self, _db, data_source, foreign_id_type, foreign_id):
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

        # Get the LicensePool that corresponds to the DataSource and
        # the Identifier.
        license_pool, was_new = get_one_or_create(
            _db, LicensePool, data_source=data_source, identifier=identifier)
        if was_new and not license_pool.availability_time:
            license_pool.availability_time = datetime.datetime.utcnow()
        return license_pool, was_new

    def edition(self):
        """The LicencePool's primary Edition.

        This is (our view of) the book's entry on whatever website
        hosts the licenses.
        """
        _db = Session.object_session(self)
        return _db.query(Edition).filter_by(
            data_source=self.data_source,
            primary_identifier=self.identifier).one()

    @classmethod
    def with_no_work(cls, _db):
        """Find LicensePools that have no corresponding Work."""
        return _db.query(LicensePool).outerjoin(Work).filter(
            Work.id==None).all()

    def add_resource(self, rel, href, data_source, media_type=None,
                     content=None):
        """Associate a Resource with this LicensePool.

        `rel`: The relationship between a LicensePool and the resource
               on the other end of the link.
        `media_type`: Media type of the representation available at the
                      other end of the link.
        """
        return self.identifier.add_resource(
            rel, href, data_source, self, media_type, content)

    def needs_update(self):
        """Is it time to update the circulation info for this license pool?"""
        now = datetime.datetime.now()
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
            new_licenses_reserved, new_patrons_in_hold_queue):
        """Update the LicensePool with new availability information.
        Log the implied changes as CirculationEvents.
        """

        _db = Session.object_session(self)
        now = datetime.datetime.utcnow()

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
            if old_value == new_value:
                continue

            if old_value < new_value:
                event_name = more_event
            else:
                event_name = fewer_event

            if not event_name:
                continue

            CirculationEvent.log(
                _db, self, event_name, old_value, new_value, now)

        # Update the license pool with the latest information.
        self.licenses_owned = new_licenses_owned
        self.licenses_available = new_licenses_available
        self.licenses_reserved = new_licenses_reserved
        self.patrons_in_hold_queue = new_patrons_in_hold_queue
        self.last_checked = now
            
    def loan_to(self, patron, start=None, end=None):
        _db = Session.object_session(patron)
        kwargs = dict(start=start or datetime.datetime.utcnow(),
                      end=end)
        return get_one_or_create(
            _db, Loan, patron=patron, license_pool=self, 
            create_method_kwargs=kwargs)

    @classmethod
    def consolidate_works(cls, _db):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        a = 0
        for unassigned in cls.with_no_work(_db):
            etext, new = unassigned.calculate_work()
            a += 1
            print "Created %r" % etext
            if a and not a % 100:
                _db.commit()

    def potential_works(self, initial_threshold=0.2, final_threshold=0.8):
        """Find all existing works that have claimed this pool's 
        editions.

        :return: A 3-tuple ({Work: [Edition]}, [Edition])
        Element 0 is a mapping of Works to the Editions they've claimed.
        Element 1 is a list of Editions that are unclaimed by any Work.
        """
        _db = Session.object_session(self)
        primary_edition = self.edition()

        claimed_records_by_work = defaultdict(list)
        unclaimed_records = []

        # If this pool is not an open-access pool, it will never be
        # grouped together with any other pools.
        if not self.open_access:
            if self.work:
                claimed_records_by_work[self.work] = [primary_edition]
            else:
                unclaimed_records.append(primary_edition)
            return claimed_records_by_work, unclaimed_records

        # Beyond this point we can assume this is an open-access pool.
        # It will only be combined with other open-access pools.

        # Find all editions connected to this LicensePool's primary
        # editions. We are very lenient about scooping up as many
        # editions as possible here, but we will be very strict when
        # we apply the similarity threshold.
        equivalent_editions = primary_edition.equivalent_editions(
            threshold=initial_threshold)

        for e in equivalent_editions:
            if e.work:
                # This edition has been claimed by a Work. This
                # strengthens the tie between this LicensePool and that
                # Work.
                l = claimed_records_by_work[e.work]
                check_against = e.work
            else:
                # This edition has not been claimed by anyone. 
                l = unclaimed_records
                check_against = primary_edition

            # Apply the similarity threshold filter.
            if check_against.similarity_to(e) >= final_threshold:
                other_pool = e.license_pool
                if other_pool and not other_pool.open_access:
                    # An open access pool will never be combined
                    # with a non-open-access pool.
                    continue
                l.append(e)
        return claimed_records_by_work, unclaimed_records

    def calculate_work(self, record_similarity_threshold=0.4,
                       work_similarity_threshold=0.4):
        """Find or create a Work for this LicensePool."""
        try:
            primary_edition = self.edition()
        except NoResultFound, e:
            return None, False
        self.language = primary_edition.language
        if primary_edition.work is not None:
            # That was a freebie.
            #print "ALREADY CLAIMED: %s by %s" % (
            #    primary_edition.title, self.work
            #)
            self.work = primary_edition.work
            return primary_edition.work, False

        # Figure out what existing works have claimed this
        # LicensePool's Editions, and which Editions are still
        # unclaimed.
        claimed, unclaimed = self.potential_works(
            final_threshold=record_similarity_threshold)
        # We're only going to consider records that meet a similarity
        # threshold vis-a-vis this LicensePool's primary work.
        print "Calculating work for %r" % primary_edition
        print " There are %s unclaimed work records" % len(unclaimed)
        for i in unclaimed:
            print "  %.3f %r" % (
                primary_edition.similarity_to(i), i)
        print

        # Now we know how many unclaimed Editions this LicensePool
        # will claim if it becomes a new Work. Find all existing Works
        # that claimed *more* Editions than that. These are all
        # better choices for this LicensePool than creating a new
        # Work. In fact, there's a good chance they are all the same
        # Work, and should be merged.
        more_popular_choices = [
            (work, len(records))
            for work, records in claimed.items()
            if len(records) > len(unclaimed)
            and work.language
            and work.language == self.language
            and work.similarity_to(primary_edition) >= work_similarity_threshold
        ]
        for work, records in claimed.items():
            sim = work.similarity_to(primary_edition)
            if sim < work_similarity_threshold:
                print " REJECTED %r as more popular choice for\n %r (similarity: %.2f)" % (
                    work, primary_edition, sim
                    )

        if more_popular_choices:
            # One or more Works seem to be better choices than
            # creating a new Work for this LicensePool. Merge them all
            # into the most popular Work.
            by_popularity = sorted(
                more_popular_choices, key=lambda x: x[1], reverse=True)

            # This is the work with the most claimed Editions, so
            # it's the one we'll merge the others into. We chose
            # the most popular because we have the most data for it, so 
            # it's the most accurate choice when calculating similarity.
            work = by_popularity[0][0]
            print " MORE POPULAR CHOICE for %s: %r" % (
                primary_edition.title.encode("utf8"), work)
            for less_popular, claimed_records in by_popularity[1:]:
                less_popular.merge_into(work, work_similarity_threshold)
            created = False
        else:
            # There is no better choice than creating a brand new Work.
            # print "NEW WORK for %r" % primary_edition.title
            work = Work()
            _db = Session.object_session(self)
            _db.add(work)
            _db.flush()
            created = True

        # Associate this LicensePool with the work we chose or
        # created.
        work.license_pools.append(self)

        # Associate the unclaimed Editions with the Work.
        work.editions.extend(unclaimed)
        for wr in unclaimed:
            wr.work = work

        # Recalculate the display information for the Work, since the
        # associated Editions have changed.
        # work.calculate_presentation()
        #if created:
        #    print "Created %r" % work
        # All done!
        return work, created

    @property
    def best_license_link(self):
        """Find the best available licensing link for the work associated
        with this LicensePool.
        """
        wr = self.edition()
        link = wr.best_open_access_link
        if link:
            return self, link

        # Either this work is not open-access, or there was no epub
        # link associated with it.
        work = self.work
        for pool in work.license_pools:
            wr = pool.edition()
            link = wr.best_open_access_link
            if link:
                return pool, link
        return self, None


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

    type = Column(String(32))
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
    SOURCE = "source"
    TYPE = "event"

    # The names of the circulation events we recognize.
    CHECKOUT = "check_out"
    CHECKIN = "check_in"
    HOLD_PLACE = "hold_place"
    HOLD_RELEASE = "hold_release"
    LICENSE_ADD = "license_add"
    LICENSE_REMOVE = "license_remove"
    AVAILABILITY_NOTIFY = "availability_notify"
    CIRCULATION_CHECK = "circulation_check"
    SERVER_NOTIFICATION = "server_notification"
    TITLE_ADD = "title_add"
    TITLE_REMOVE = "title_remove"
    UNKNOWN = "unknown"

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
        print " EVENT %s %s=>%s" % (event_name, old_value, new_value)
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
    credential = Column(String)
    expires = Column(DateTime)

    __table_args__ = (
        UniqueConstraint('data_source_id', 'patron_id'),
    )

    @classmethod
    def lookup(self, _db, data_source, patron, refresher_method):
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)
        credential, is_new = get_one_or_create(
            _db, Credential, data_source=data_source, patron=patron)
        if (is_new or not credential.expires 
            or credential.expires <= datetime.datetime.utcnow()):
            refresher_method(credential)
        return credential


class Timestamp(Base):
    """A general-purpose timestamp for external services."""

    __tablename__ = 'timestamps'
    service = Column(String(255), primary_key=True)
    timestamp = Column(DateTime)

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
    """A cached document from the Web at large."""

    __tablename__ = 'representations'
    id = Column(Integer, primary_key=True)

    # URL from which the representation was fetched.
    url = Column(Unicode, index=True)

    # The representation is probably obtained from a particular data source.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    # The representation may be the data source's representation of a
    # particular identifier.
    identifier_id = Column(Integer, ForeignKey('identifiers.id'), index=True)

    # Or (less likely) the representation may be the data source's
    # representation of a particular license pool.
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)

    # When the representation was fetched.
    fetched_at = Column(DateTime, index=True)

    # The HTTP status code from the last representation.
    status_code = Column(Integer)

    # A textual description of the error encountered the last time
    # we tried to fetch the representation
    exception = Column(Unicode, index=True)

    # A textual representation of the HTTP headers sent along with the
    # representation.
    headers = Column(Unicode)

    # The Content-Type header from the last representation.
    content_type = Column(Unicode)

    # The Location header from the last representation.
    location = Column(Unicode)

    # The Last-Modified header from the last representation.
    last_modified = Column(Unicode)

    # The Etag header from the last representation.
    etag = Column(Unicode)

    # The representation itself.
    content = Column(Binary)

    BROWSER_USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36 (Simplified)"

    @property
    def age(self):
        if not self.fetched_at:
            return 1000000
        return (datetime.datetime.utcnow() - self.fetched_at).total_seconds()

    @property
    def has_content(self):
        return (self.status_code == 200 and self.exception == None
                and self.content is not None)

    @classmethod
    def get(cls, _db, url, do_get=None, extra_request_headers=None, data_source=None,
            identifier=None, license_pool=None, max_age=None, pause_before=0,
            allow_redirects=True, debug=False):
        """Retrieve a representation from the cache if possible.
        
        If not possible, retrieve it from the web and store it in the
        cache.
        
        :param do_get: A function that takes arguments (url, headers)
        and retrieves a representation over the network.

        :param max_age: A timedelta object representing the maximum
        time to consider a cached representation fresh. (We ignore the
        caching directives from the server because they're usually far
        too conservative for our purposes.)

        :return: A 2-tuple (representation, obtained_from_cache)

        """
        do_get = do_get or cls.simple_http_get

        representation = None
        q = _db.query(Representation).filter(
            Representation.url==url).filter(
                Representation.data_source==data_source).order_by(
                    Representation.fetched_at.desc()).limit(1)
        try:
            representation = q.one()
        except NoResultFound, e:
            representation = None

        # Do we already have a usable representation?
        usable_representation = (
            representation and not representation.exception)

        if isinstance(max_age, datetime.timedelta):
            max_age = max_age.total_seconds()
        if usable_representation and (
                max_age is None or max_age > representation.age):
            if debug:
                print "Cached %s" % url
            return representation, True

        if debug:
            print "Fetching %s" % url
        headers = {}
        if extra_request_headers:
            headers.update(extra_request_headers)
        if usable_representation:
            if representation.last_modified:
                headers['If-Modified-Since'] = representation.last_modified
            if representation.etag:
                headers['If-None-Match'] = representation.etag
        # Either the representation was not cached, or the cache is stale.
        # We need to get a new representation.
        fetched_at = datetime.datetime.utcnow()
        if pause_before:
            time.sleep(pause_before)
        try:
            status_code, headers, content = do_get(url, headers)
            exception = None
        except Exception, e:
            exception = str(e)
            status_code = None
            headers = None
            content = None

        if exception:
            print "EXCEPTION: %s" % exception
        
        if not status_code:
            raise IOError("No status code!")

        if status_code / 100 == 4 and status_code != 404:
            raise IOError("%s status code" % status_code)

        if status_code / 100 == 5:
            raise IOError("%s status code" % status_code)

        if usable_representation and status_code == 304:
            # The representation has not been modified since the last
            # time we retrieved it. Return the cached version.
            representation.fetched_at = fetched_at
            return representation, True

        if not representation:
            # This is our first time retrieving a representation of
            # this url.
            representation = Representation(
                url=url, data_source=data_source, identifier=identifier,
                license_pool=license_pool)

        if exception:
            representation.exception = exception
        else:
            representation.exception = None

        representation.status_code = status_code
        if 'content-type' in headers:
            representation.content_type = headers['content-type']
        if 'etag' in headers:
            representation.etag = headers['etag']
        if 'last-modified' in headers:
            representation.last_modified = headers['last-modified']
        if 'location' in headers:
            representation.location = headers['location']
        representation.headers = cls.headers_to_string(headers)
        representation.content = content
        representation.fetched_at = fetched_at
        return representation, False

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


class CoverageProvider(object):

    """Run Editions from one DataSource (the input DataSource) through
    code associated with another DataSource (the output
    DataSource). If the code returns success, add a CoverageRecord for
    the Edition and the output DataSource, so that the record
    doesn't get processed next time.
    """

    def __init__(self, service_name, input_sources, output_source,
                 workset_size=100):
        self._db = Session.object_session(output_source)
        self.service_name = service_name
        self.input_sources = input_sources
        self.output_source = output_source
        self.workset_size = workset_size

    @property
    def editions_that_need_coverage(self):
        return Edition.missing_coverage_from(
            self._db, self.input_sources, self.output_source).order_by(func.random())

    def run(self):
        remaining = True
        failures = set([])
        print "%d records need coverage." % (self.editions_that_need_coverage.count())
        while remaining:
            successes = 0
            if len(failures) >= self.workset_size:
                raise Exception(
                    "Number of failures equals workset size, cannot continue.")
            workset = self.editions_that_need_coverage.limit(
                self.workset_size)
            remaining = False
            for record in workset:
                if record in failures:
                    continue
                remaining = True
                if self.process_edition(record):
                    # Success! Now there's coverage! Add a CoverageRecord.
                    successes += 1
                    self.add_coverage_record_for(record)
                else:
                    failures.add(record)
            # Commit this workset before moving on to the next one.
            self.commit_workset()
            print "Workset processed with %d successes, %d failures." % (
                successes, len(failures))            

        # Now that we're done, update the timestamp
        Timestamp.stamp(self._db, self.service_name)
        self._db.commit()

    def add_coverage_record_for(self, identifier):
        if isinstance(identifier, Identifier):
            identifier = identifier
        else:
            identifier = identifier.primary_identifier
        now = datetime.datetime.utcnow()
        coverage_record, is_new = get_one_or_create(
            self._db, CoverageRecord,
            identifier=identifier,
            data_source=self.output_source,
        )
        coverage_record.date = now
        return coverage_record, is_new

    def process_edition(self, edition):
        raise NotImplementedError()

    def commit_workset(self):
        self._db.commit()

class ImageScaler(object):

    def __init__(self, db, data_directory, mirrors):
        self._db = db
        self.original_expansions = {}
        self.scaled_expansions = {}
        self.original_variable_to_scaled_variable = {}
        self.data_source_ids = []

        for mirror in mirrors:
            original = mirror.ORIGINAL_PATH_VARIABLE
            scaled = mirror.SCALED_PATH_VARIABLE
            data_source_name = mirror.DATA_SOURCE
            data_source = DataSource.lookup(self._db, data_source_name)
            self.data_source_ids.append(data_source.id)
            self.original_expansions[original] = mirror.data_directory(
                data_directory)
            self.scaled_expansions[scaled] = mirror.scaled_image_directory(data_directory)
            self.original_variable_to_scaled_variable[original] = "%(" + scaled + ")s"

    def run(self, destination_width, destination_height, force,
            batch_size=100, upload=True):
        q = self._db.query(Resource).filter(
            Resource.rel==Resource.IMAGE).filter(
                Resource.mirrored==True).filter(
                    Resource.data_source_id.in_(self.data_source_ids))

        if not force:
            q = q.filter(Resource.scaled==False)
        if upload:
            from integration.s3 import S3Uploader
            uploader = S3Uploader()
        print "Scaling %d images." % q.count()
        resultset = q.limit(batch_size).all()
        while resultset:
            total = 0
            a = time.time()
            to_upload = []
            for r in resultset:
                already_scaled = r.scale(destination_width, destination_height, self.original_expansions, self.scaled_expansions, self.original_variable_to_scaled_variable, force)
                if not r.scaled_path:
                    print "Could not scale %s" % r.href
                elif already_scaled:
                    pass
                else:
                    local_path = r.local_scaled_path(self.scaled_expansions)
                    #print "%dx%d %s" % (r.scaled_height, r.scaled_width,
                    #                    local_path)
                    to_upload.append((local_path, r.scaled_url))
                    total += 1
            print "%.2f sec to scale %d" % ((time.time()-a), total)
            a = time.time()
            if upload:
                uploader.upload_resources(to_upload)
            self._db.commit()
            print "%.2f sec to upload %d" % ((time.time()-a), total)
            a = time.time()
            resultset = q.limit(batch_size).all()
        self._db.commit()

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
