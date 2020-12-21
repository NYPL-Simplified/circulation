# encoding: utf-8
# Identifier, Equivalency
import datetime
import logging
import random
import urllib
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from functools import total_ordering

import isbnlib
import six
from classification import Classification, Subject
from constants import IdentifierConstants, LinkRelations
from coverage import CoverageRecord
from datasource import DataSource
from licensing import LicensePoolDeliveryMechanism, RightsStatus
from measurement import Measurement
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import joinedload, relationship
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import and_, or_

from ..util.string_helpers import native_string
from ..util.summary import SummaryEvaluator
from . import Base, PresentationCalculationPolicy, create, get_one, get_one_or_create


@six.add_metaclass(ABCMeta)
class IdentifierParser(object):
    """Interface for identifier parsers."""

    @abstractmethod
    def parse(self, identifier_string):
        """Parse a string containing an identifier, extract it and determine its type.

        :param identifier_string: String containing an identifier
        :type identifier_string: str

        :return: 2-tuple containing the identifier's type and identifier itself or None
            if the string contains an incorrect identifier
        :rtype: Optional[Tuple[str, str]]
        """
        raise NotImplementedError()


@total_ordering
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
        return native_string(
            u"%s/%s ID=%s%s" % (self.type, self.identifier, self.id, title)
        )

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
    def _parse_urn(cls, _db, identifier_string, identifier_type, must_support_license_pools=False):
        """Parse identifier string.

        :param _db: Database session
        :type _db: sqlalchemy.orm.session.Session

        :param identifier_string: Identifier itself
        :type identifier_string: str

        :param identifier_type: Identifier's type
        :type identifier_type: str

        :param must_support_license_pools: Boolean value indicating whether there should be a DataSource that provides
            licenses for books identified by the given identifier
        :type must_support_license_pools: bool

        :return: 2-tuple containing Identifier object and a boolean value indicating whether it's new
        :rtype: Tuple[core.model.identifier.Identifier, bool]
        """
        if must_support_license_pools:
            try:
                _ = DataSource.license_source_for(_db, identifier_type)
            except NoResultFound:
                raise Identifier.UnresolvableIdentifierException()
            except MultipleResultsFound:
                # This is fine.
                pass

        return cls.for_foreign_id(_db, identifier_type, identifier_string)

    @classmethod
    def parse_urn(cls, _db, identifier_string, must_support_license_pools=False):
        """Parse identifier string.

        :param _db: Database session
        :type _db: sqlalchemy.orm.session.Session

        :param identifier_string: String containing an identifier
        :type identifier_string: str

        :param must_support_license_pools: Boolean value indicating whether there should be a DataSource that provides
            licenses for books identified by the given identifier
        :type must_support_license_pools: bool

        :return: 2-tuple containing Identifier object and a boolean value indicating whether it's new
        :rtype: Tuple[core.model.identifier.Identifier, bool]
        """
        identifier_type, identifier_string = cls.type_and_identifier_for_urn(identifier_string)

        return cls._parse_urn(_db, identifier_string, identifier_type, must_support_license_pools)

    @classmethod
    def parse(cls, _db, identifier_string, parser, must_support_license_pools=False):
        """Parse identifier string.

        :param _db: Database session
        :type _db: sqlalchemy.orm.session.Session

        :param identifier_string: String containing an identifier
        :type identifier_string: str

        :param parser: Identifier parser
        :type parser: IdentifierParser

        :param must_support_license_pools: Boolean value indicating whether there should be a DataSource that provides
            licenses for books identified by the given identifier
        :type must_support_license_pools: bool

        :return: 2-tuple containing Identifier object and a boolean value indicating whether it's new
        :rtype: Tuple[core.model.identifier.Identifier, bool]
        """
        identifier_type, identifier_string = parser.parse(identifier_string)

        return cls._parse_urn(_db, identifier_string, identifier_type, must_support_license_pools)

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
            cls, identifier_id_column, policy=None):
        """Get a SQL statement that will return all Identifier IDs
        equivalent to a given ID at the given confidence threshold.
        `identifier_id_column` can be a single Identifier ID, or a column
        like `Edition.primary_identifier_id` if the query will be used as
        a subquery.
        This uses the function defined in files/recursive_equivalents.sql.
        """
        fn = cls._recursively_equivalent_identifier_ids_query(
            identifier_id_column, policy
        )
        return select([fn])

    @classmethod
    def _recursively_equivalent_identifier_ids_query(
        cls, identifier_id_column, policy=None
    ):
        policy = policy or PresentationCalculationPolicy()
        levels = policy.equivalent_identifier_levels
        threshold = policy.equivalent_identifier_threshold
        cutoff = policy.equivalent_identifier_cutoff

        return func.fn_recursive_equivalents(
            identifier_id_column, levels, threshold, cutoff
        )

    @classmethod
    def recursively_equivalent_identifier_ids(
            cls, _db, identifier_ids, policy=None):
        """All Identifier IDs equivalent to the given set of Identifier
        IDs at the given confidence threshold.
        This uses the function defined in files/recursive_equivalents.sql.
        Four levels is enough to go from a Gutenberg text to an ISBN.
        Gutenberg ID -> OCLC Work IS -> OCLC Number -> ISBN
        Returns a dictionary mapping each ID in the original to a
        list of equivalent IDs.

        :param policy: A PresentationCalculationPolicy that explains
           how you've chosen to make the tradeoff between performance,
           data quality, and sheer number of equivalent identifiers.
        """
        fn = cls._recursively_equivalent_identifier_ids_query(
            Identifier.id, policy
        )
        query = select([Identifier.id, fn], Identifier.id.in_(identifier_ids))
        results = _db.execute(query)
        equivalents = defaultdict(list)
        for r in results:
            original = r[0]
            equivalent = r[1]
            equivalents[original].append(equivalent)
        return equivalents

    def equivalent_identifier_ids(self, policy=None):
        _db = Session.object_session(self)
        return Identifier.recursively_equivalent_identifier_ids(
            _db, [self.id], policy
        )

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
        from resource import Hyperlink, Representation, Resource
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
        :param value: Human-readable description of the subject, if different
            from the ID.
        :param weight: How confident the data source is in classifying a
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
        from resource import Hyperlink, Resource
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
        from resource import Hyperlink, Resource
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
        from ..opds import AcquisitionFeed
        return AcquisitionFeed.minimal_opds_entry(
            identifier=self, cover=cover_image, description=description,
            quality=quality, most_recent_update=most_recent_update
        )


    def __eq__(self, other):
        """Equality implementation for total_ordering."""
        # We don't want an Identifier to be == an IdentifierData
        # with the same data.
        if other is None or not isinstance(other, Identifier):
            return False
        return (self.type, self.identifier) == (other.type, other.identifier)

    def __lt__(self, other):
        """Comparison implementation for total_ordering."""
        if other is None or not isinstance(other, Identifier):
            return False
        return (self.type, self.identifier) < (other.type, other.identifier)


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

    # Should this equivalency actually be used in calculations? This
    # is not manipulated directly, but it gives us the ability to use
    # manual intervention to defuse large chunks of problematic code
    # without actually deleting the data.
    enabled = Column(Boolean, default=True, index=True)

    def __repr__(self):
        r = u"[%s ->\n %s\n source=%s strength=%.2f votes=%d)]" % (
            repr(self.input).decode("utf8"),
            repr(self.output).decode("utf8"),
            self.data_source.name, self.strength, self.votes
        )
        return r

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
