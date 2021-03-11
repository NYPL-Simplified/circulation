# encoding: utf-8
# Edition
from nose.tools import set_trace

from . import (
    Base,
    get_one,
    get_one_or_create,
    PresentationCalculationPolicy,
)
from .coverage import CoverageRecord
from .constants import (
    DataSourceConstants,
    EditionConstants,
    LinkRelations,
    MediaTypes,
)
from .contributor import (
    Contributor,
    Contribution,
)
from .datasource import DataSource
from .identifier import Identifier
from .licensing import (
    DeliveryMechanism,
    LicensePool,
)

from collections import defaultdict
import logging
from sqlalchemy import (
    Column,
    Date,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session
from ..util import (
    LanguageCodes,
    TitleProcessor
)
from ..util.permanent_work_id import WorkIDCalculator

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

    MEDIUM_ENUM = Enum(*EditionConstants.KNOWN_MEDIA, name="medium")

    medium = Column(MEDIUM_ENUM, index=True)

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
        id_repr = repr(self.primary_identifier)
        return "Edition %s [%r] (%s/%s/%s)" % (
            self.id, id_repr, self.title,
            ", ".join([x.sort_name for x in self.contributors]),
            self.language
        )

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
    def medium_from_media_type(cls, media_type):
        """Derive a value for Edition.medium from a media type.

        TODO: It's not necessary right now, but we could theoretically
        derive this information from some other types such as
        our internal types for Overdrive manifests.

        :param media_type: A media type with optional parameters
        :return: A value for Edition.medium.
        """
        if not media_type:
            return None

        for types, conclusion in (
            (MediaTypes.AUDIOBOOK_MEDIA_TYPES, Edition.AUDIO_MEDIUM),
            (MediaTypes.BOOK_MEDIA_TYPES, Edition.BOOK_MEDIUM),
        ):
            if any(media_type.startswith(x) for x in types):
                return conclusion

        if media_type.startswith(DeliveryMechanism.ADOBE_DRM):
            # Adobe DRM is only applied to ebooks.
            return Edition.BOOK_MEDIUM
        return None

    @classmethod
    def for_foreign_id(cls, _db, data_source,
                       foreign_id_type, foreign_id,
                       create_if_not_exists=True):
        """Find the Edition representing the given data source's view of
        the work that it primarily identifies by foreign ID.
        e.g. for_foreign_id(_db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, uuid)
        finds the Edition for Overdrive's view of a book identified
        by Overdrive UUID.
        This:
        for_foreign_id(_db, DataSource.OVERDRIVE, Identifier.ISBN, isbn)
        will probably return nothing, because although Overdrive knows
        that books have ISBNs, it doesn't use ISBN as a primary
        identifier.

        """
        # Look up the data source if necessary.
        if isinstance(data_source, (bytes, str)):
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

    def equivalent_identifiers(self, type=None, policy=None):
        """All Identifiers equivalent to this
        Edition's primary identifier, according to the given
        PresentationCalculationPolicy
        """
        _db = Session.object_session(self)
        identifier_id_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            self.primary_identifier.id, policy=policy
        )
        q = _db.query(Identifier).filter(
            Identifier.id.in_(identifier_id_subquery))
        if type:
            if isinstance(type, list):
                q = q.filter(Identifier.type.in_(type))
            else:
                q = q.filter(Identifier.type==type)
        return q.all()

    def equivalent_editions(self, policy=None):
        """All Editions whose primary ID is equivalent to this Edition's
        primary ID, according to the given PresentationCalculationPolicy.
        """
        _db = Session.object_session(self)
        identifier_id_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            self.primary_identifier.id, policy=policy
        )
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
        e.g.::
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
    def sort_by_priority(cls, editions, license_source=None):
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

            if source == license_source:
                # This Edition contains information from the same data
                # source as the LicensePool itself. Put it below any
                # Edition from one of the data sources in
                # PRESENTATION_EDITION_PRIORITY, but above all other
                # Editions.
                return -1

            elif source.name == DataSourceConstants.METADATA_WRANGLER:
                # The metadata wrangler is slightly less trustworthy
                # than the license source, for everything except cover
                # image (which is handled by
                # Representation.quality_as_thumbnail_image)
                return -1.5

            if source.name in DataSourceConstants.PRESENTATION_EDITION_PRIORITY:
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
        if isinstance(roles, (bytes, str)):
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

    def best_cover_within_distance(self, distance, rel=None, policy=None):
        _db = Session.object_session(self)
        identifier_ids = [self.primary_identifier.id]
        
        if distance > 0:
            if policy is None:
                new_policy = PresentationCalculationPolicy()
            else:
                new_policy = PresentationCalculationPolicy(
                    equivalent_identifier_levels=distance,
                    equivalent_identifier_cutoff=policy.equivalent_identifier_cutoff,
                    equivalent_identifier_threshold=policy.equivalent_identifier_threshold,
                )

            identifier_ids_dict = Identifier.recursively_equivalent_identifier_ids(
                _db, identifier_ids, policy=new_policy
            )
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
        medium = self.medium_for_permanent_work_id.get(self.medium, None)
        if not title or not medium:
            # If a book has no title or medium, it has no permanent work ID.
            self.permanent_work_id = None
            return

        author = self.author_for_permanent_work_id

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

    UNKNOWN_AUTHOR = "[Unknown]"



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
            self.choose_cover(policy=policy)

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

            msg = "Presentation %s for Edition %s (by %s, pub=%s, ident=%s/%s, pwid=%s, language=%s, cover=%r)"
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

    def choose_cover(self, policy=None):
        """Try to find a cover that can be used for this Edition."""
        self.cover_full_url = None
        self.cover_thumbnail_url = None
        for distance in (0, 5):
            # If there's a cover directly associated with the
            # Edition's primary ID, use it. Otherwise, find the
            # best cover associated with any related identifier.
            best_cover, covers = self.best_cover_within_distance(
                distance=distance, policy=policy
            )

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
                best_thumbnail, thumbnails = self.best_cover_within_distance(
                    distance=distance, policy=policy,
                    rel=LinkRelations.THUMBNAIL_IMAGE,
                )
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
