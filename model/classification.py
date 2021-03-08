# encoding: utf-8
# Subject, Classification, Genre
from nose.tools import set_trace

from . import (
    Base,
    get_one,
    get_one_or_create,
    numericrange_to_string,
    numericrange_to_tuple,
    tuple_to_numericrange,
)
from .constants import DataSourceConstants
from .hasfulltablecache import HasFullTableCache

from .. import classifier
from ..classifier import (
    Classifier,
    COMICS_AND_GRAPHIC_NOVELS,
    Erotica,
    GenreData,
)

import logging

from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    ForeignKey,
    func,
    Integer,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func

class Subject(Base):
    """A subject under which books might be classified."""

    # Types of subjects.
    LCC = Classifier.LCC              # Library of Congress Classification
    LCSH = Classifier.LCSH            # Library of Congress Subject Headings
    FAST = Classifier.FAST
    DDC = Classifier.DDC              # Dewey Decimal Classification
    OVERDRIVE = Classifier.OVERDRIVE  # Overdrive's classification system
    RBDIGITAL = Classifier.RBDIGITAL  # RBdigital's genre system
    BISAC = Classifier.BISAC
    BIC = Classifier.BIC              # BIC Subject Categories
    TAG = Classifier.TAG              # Folksonomic tags.
    FREEFORM_AUDIENCE = Classifier.FREEFORM_AUDIENCE
    NYPL_APPEAL = Classifier.NYPL_APPEAL

    # Types with terms that are suitable for search.
    TYPES_FOR_SEARCH = [
        FAST, OVERDRIVE, BISAC, TAG
    ]

    AXIS_360_AUDIENCE = Classifier.AXIS_360_AUDIENCE
    RBDIGITAL_AUDIENCE = Classifier.RBDIGITAL_AUDIENCE
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
    SIMPLIFIED_GENRE = Classifier.SIMPLIFIED_GENRE
    SIMPLIFIED_FICTION_STATUS = Classifier.SIMPLIFIED_FICTION_STATUS

    by_uri = {
        SIMPLIFIED_GENRE : SIMPLIFIED_GENRE,
        SIMPLIFIED_FICTION_STATUS : SIMPLIFIED_FICTION_STATUS,
        "http://librarysimplified.org/terms/genres/Overdrive/" : OVERDRIVE,
        "http://librarysimplified.org/terms/genres/3M/" : BISAC,
        "http://id.worldcat.org/fast/" : FAST, # I don't think this is official.
        "http://purl.org/dc/terms/LCC" : LCC,
        "http://purl.org/dc/terms/LCSH" : LCSH,
        "http://purl.org/dc/terms/DDC" : DDC,
        "http://schema.org/typicalAgeRange" : AGE_RANGE,
        "http://schema.org/audience" : FREEFORM_AUDIENCE,
        "http://www.bisg.org/standards/bisac_subject/" : BISAC,
        # Feedbooks uses a modified BISAC which we know how to handle.
        "http://www.feedbooks.com/categories" : BISAC,
    }

    uri_lookup = dict()
    for k, v in list(by_uri.items()):
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
            "All Ages", "Research",
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
            name = ' ("%s")' % self.name
        else:
            name = ""
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
        a = '[%s:%s%s%s%s%s%s]' % (
            self.type, self.identifier, name, fiction, audience, genre, age_range)
        return str(a)

    @property
    def target_age_string(self):
        return numericrange_to_string(self.target_age)

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
    def lookup(cls, _db, type, identifier, name, autocreate=True):
        """Turn a subject type and identifier into a Subject."""
        classifier = Classifier.lookup(type)
        if not type:
            raise ValueError("Cannot look up Subject with no type.")
        if not identifier and not name:
            raise ValueError(
                "Cannot look up Subject when neither identifier nor name is provided."
            )

        # An identifier is more reliable than a name, so we would rather
        # search based on identifier. But if we only have a name, we'll
        # search based on name.
        if identifier:
            find_with = dict(identifier=identifier)
            create_with = dict(name=name)
        else:
            # Type + identifier is unique, but type + name is not
            # (though maybe it should be). So we need to provide
            # on_multiple.
            find_with = dict(name=name, on_multiple='interchangeable')
            create_with = dict()

        if autocreate:
            subject, new = get_one_or_create(
                _db, Subject, type=type,
                create_method_kwargs=create_with,
                **find_with
            )
        else:
            subject = get_one(_db, Subject, type=type, **find_with)
            new = False
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
        genre/audience/fiction status if possible, and mark each as checked.

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

        # Create a shorthand way of referring to this Subject in log
        # messages.
        parts = [self.type, self.identifier, self.name]
        shorthand = ":".join(x for x in parts if x)

        if genre != self.genre:
            log.info(
                "%s genre %r=>%r", shorthand, self.genre, genre
            )
        self.genre = genre

        if audience:
            if self.audience != audience:
                log.info(
                    "%s audience %s=>%s", shorthand, self.audience, audience
                )
        self.audience = audience

        if fiction is not None:
            if self.fiction != fiction:
                log.info(
                    "%s fiction %s=>%s", shorthand, self.fiction, fiction
                )
        self.fiction = fiction

        if (numericrange_to_tuple(self.target_age) != target_age and
            not (not self.target_age and not target_age)):
            log.info(
                "%s target_age %r=>%r", shorthand,
                self.target_age, tuple_to_numericrange(target_age)
            )
        self.target_age = tuple_to_numericrange(target_age)


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

    # If we hear about a classification from a distributor (and we
    # trust the distributor to have accurate classifications), we
    # should give it this weight. This lets us keep the weights
    # consistent across distributors.
    TRUSTED_DISTRIBUTOR_WEIGHT = 100.0

    @property
    def scaled_weight(self):
        weight = self.weight
        if self.data_source.name == DataSourceConstants.OCLC_LINKED_DATA:
            weight = weight / 10.0
        elif self.data_source.name == DataSourceConstants.OVERDRIVE:
            weight = weight * 50
        return weight

    # These subject types are known to be problematic in that their
    # "Juvenile" classifications are applied indiscriminately to both
    # YA books and Children's books. As such, we need to split the
    # difference when weighing a classification whose subject is of
    # this type.
    #
    # This goes into Classification rather than Subject because it's
    # possible that one particular data source could use a certain
    # subject type in an unreliable way.
    _juvenile_subject_types = set([
        Subject.LCC
    ])

    _quality_as_indicator_of_target_age = {

        # Not all classifications are equally reliable as indicators
        # of a target age. This dictionary contains the coefficients
        # we multiply against the weights of incoming classifications
        # to reflect the overall reliability of that type of
        # classification.
        #
        # If we had a ton of information about target age this might
        # not be necessary--it doesn't seem necessary for genre
        # classifications. But we sometimes have very little
        # information about target age, so being careful about how
        # much we trust different data sources can become important.

        DataSourceConstants.MANUAL : 1.0,
        DataSourceConstants.LIBRARY_STAFF: 1.0,
        (DataSourceConstants.METADATA_WRANGLER, Subject.AGE_RANGE) : 1.0,

        Subject.AXIS_360_AUDIENCE : 0.9,
        (DataSourceConstants.OVERDRIVE, Subject.INTEREST_LEVEL) : 0.9,
        (DataSourceConstants.OVERDRIVE, Subject.OVERDRIVE) : 0.9, # But see below
        (DataSourceConstants.AMAZON, Subject.AGE_RANGE) : 0.85,
        (DataSourceConstants.AMAZON, Subject.GRADE_LEVEL) : 0.85,

        # Although Overdrive usually reserves Fiction and Nonfiction
        # for books for adults, it's not as reliable an indicator as
        # other Overdrive classifications.
        (DataSourceConstants.OVERDRIVE, Subject.OVERDRIVE, "Fiction") : 0.7,
        (DataSourceConstants.OVERDRIVE, Subject.OVERDRIVE, "Nonfiction") : 0.7,

        Subject.AGE_RANGE : 0.6,
        Subject.GRADE_LEVEL : 0.6,

        # There's no real way to know what this measures, since it
        # could be anything. If a tag mentions a target age or a grade
        # level, the accuracy seems to be... not terrible.
        Subject.TAG : 0.45,

        # Tags that come from OCLC Linked Data are of lower quality
        # because they sometimes talk about completely the wrong book.
        (DataSourceConstants.OCLC_LINKED_DATA, Subject.TAG) : 0.3,

        # These measure reading level, not age appropriateness.
        # However, if the book is a remedial work for adults we won't
        # be calculating a target age in the first place, so it's okay
        # to use reading level as a proxy for age appropriateness in a
        # pinch. (But not outside of a pinch.)
        (DataSourceConstants.OVERDRIVE, Subject.GRADE_LEVEL) : 0.35,
        Subject.LEXILE_SCORE : 0.1,
        Subject.ATOS_SCORE: 0.1,
    }

    @property
    def generic_juvenile_audience(self):
        """Is this a classification that mentions (e.g.) a Children's audience
        but is actually a generic 'Juvenile' classification?
        """
        return (
            self.subject.audience in Classifier.AUDIENCES_JUVENILE
            and self.subject.type in self._juvenile_subject_types
        )

    @property
    def quality_as_indicator_of_target_age(self):
        if not self.subject.target_age:
            return 0
        data_source = self.data_source.name
        subject_type = self.subject.type
        q = self._quality_as_indicator_of_target_age

        keys = [
            (data_source, subject_type, self.subject.identifier),
            (data_source, subject_type),
            data_source,
            subject_type
        ]
        for key in keys:
            if key in q:
                return q[key]
        return 0.1

    @property
    def weight_as_indicator_of_target_age(self):
        return self.weight * self.quality_as_indicator_of_target_age

    @property
    def comes_from_license_source(self):
        """Does this Classification come from a data source that also
        provided a license for this book?
        """
        if not self.identifier.licensed_through:
            return False
        for pool in self.identifier.licensed_through:
            if self.data_source == pool.data_source:
                return True
        return False


class Genre(Base, HasFullTableCache):
    """A subject-matter classification for a book.
    Much, much more general than Classification.
    """
    __tablename__ = 'genres'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, unique=True, index=True)

    # One Genre may have affinity with many Subjects.
    subjects = relationship("Subject", backref="genre")

    # One Genre may participate in many WorkGenre assignments.
    works = association_proxy('work_genres', 'work')

    work_genres = relationship("WorkGenre", backref="genre",
                               cascade="all, delete-orphan")

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    def __repr__(self):
        if classifier.genres.get(self.name):
            length = len(classifier.genres[self.name].subgenres)
        else:
            length = 0
        return "<Genre %s (%d subjects, %d works, %d subcategories)>" % (
            self.name, len(self.subjects), len(self.works), length)

    def cache_key(self):
        return self.name

    @classmethod
    def lookup(cls, _db, name, autocreate=False, use_cache=True):
        if isinstance(name, GenreData):
            name = name.name

        def create():
            """Function called when a Genre is not found in cache and must be
            created."""
            new = False
            args = (_db, Genre)
            if autocreate:
                genre, new = get_one_or_create(*args, name=name)
            else:
                genre = get_one(*args, name=name)
                if genre is None:
                    logging.getLogger().error('"%s" is not a recognized genre.', name)
                    return None, False
            return genre, new

        if use_cache:
            return cls.by_cache_key(_db, name, create)
        else:
            return create()

    @property
    def genredata(self):
        if classifier.genres.get(self.name):
            return classifier.genres[self.name]
        else:
            return GenreData(self.name, False)

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
        if self.name not in classifier.genres:
            return None
        return classifier.genres[self.name].is_fiction
