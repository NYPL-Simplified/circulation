# encoding: utf-8
# DataSource, Identifier, Equivalency, Edition
from . import (
    Base,
    PresentationCalculationPolicy,
)
from contributions import (
    Contribution,
    Contributor,
)
from has_full_table_cache import HasFullTableCache
from helper_methods import (
    create,
    get_one,
    get_one_or_create,
)
from background import CoverageRecord
from measurement import Measurement
from licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from datasource_constants import DataSourceConstants
from edition_constants import EditionConstants
from link_relations import LinkRelations
from identifier_constants import IdentifierConstants
from classification import (
    Classification,
    Subject,
)
from collections import defaultdict
from nose.tools import set_trace
import datetime
import isbnlib
import logging
import random
import re
import urllib
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Enum,
    Float,
    ForeignKey,
    func,
    Index,
    Integer,
    String,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import (
    and_,
    or_,
)
from sqlalchemy.orm import (
    backref,
    joinedload,
    relationship,
)
from sqlalchemy.ext.mutable import (
    MutableDict,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm.session import Session
from util import (
    LanguageCodes,
    TitleProcessor,
)
from util.permanent_work_id import WorkIDCalculator
from util.personal_names import display_name_to_sort_name
from util.summary import SummaryEvaluator

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

class Identifier(Base, IdentifierConstants):
    """A way of uniquely referring to a particular edition.
    """

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
        from resources import (
            Resource,
            Hyperlink,
            Representation,
        )
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
        from resources import (
            Resource,
            Hyperlink,
        )
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

    @classmethod
    def best_cover_for(cls, _db, identifier_ids, rel=None):
        # Find all image resources associated with any of
        # these identifiers.
        from resources import (
            Resource,
            Hyperlink,
        )
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
        rels = [LinkRelations.DESCRIPTION, LinkRelations.SHORT_DESCRIPTION]
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
            if link.rel == LinkRelations.IMAGE:
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
            elif link.rel == LinkRelations.DESCRIPTION:
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

class Edition(Base, EditionConstants):

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

    # medium_to_additional_type = {
    #     BOOK_MEDIUM : u"http://schema.org/EBook",
    #     AUDIO_MEDIUM : u"http://bib.schema.org/Audiobook",
    #     PERIODICAL_MEDIUM : u"http://schema.org/PublicationIssue",
    #     MUSIC_MEDIUM :  u"http://schema.org/MusicRecording",
    #     VIDEO_MEDIUM :  u"http://schema.org/VideoObject",
    #     IMAGE_MEDIUM: u"http://schema.org/ImageObject",
    #     COURSEWARE_MEDIUM: u"http://schema.org/Course"
    # }
    #
    # additional_type_to_medium = {}
    # for k, v in medium_to_additional_type.items():
    #     additional_type_to_medium[v] = k

    medium = Column(
        Enum(EditionConstants.BOOK_MEDIUM,
            EditionConstants.PERIODICAL_MEDIUM,
            EditionConstants.AUDIO_MEDIUM,
            EditionConstants.MUSIC_MEDIUM,
            EditionConstants.VIDEO_MEDIUM,
            EditionConstants.IMAGE_MEDIUM,
            EditionConstants.COURSEWARE_MEDIUM,
             name="medium"),
        default=EditionConstants.BOOK_MEDIUM, index=True
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

            if source.name in DataSource.PRESENTATION_EDITION_PRIORITY:
                id_type = edition.primary_identifier.type
                if (id_type == Identifier.ISBN and
                    source.name == DataSource.METADATA_WRANGLER):
                    # This ISBN edition was pieced together from OCLC data.
                    # To avoid overwriting better author and title data from
                    # the license source, rank this edition lower.
                    return -1.5
                return DataSource.PRESENTATION_EDITION_PRIORITY.index(source.name)
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
                best_thumbnail, thumbnails = self.best_cover_within_distance(distance, rel=LinkRelations.THUMBNAIL_IMAGE)
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
