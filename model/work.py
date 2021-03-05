# encoding: utf-8
# WorkGenre, Work

import datetime
import logging
from collections import Counter

from sqlalchemy import (
    Binary,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Unicode,
)
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import (
    contains_eager,
    relationship,
)
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import (
    and_,
    or_,
    select,
    join,
    literal_column,
    case,
)
from sqlalchemy.sql.functions import func

from .constants import (
    DataSourceConstants,
)
from .contributor import (
    Contribution,
    Contributor,
)
from .coverage import (
    CoverageRecord,
    WorkCoverageRecord,
)
from .datasource import DataSource
from .edition import Edition
from .identifier import Identifier
from .measurement import Measurement
from . import (
    Base,
    flush,
    get_one_or_create,
    numericrange_to_string,
    numericrange_to_tuple,
    PresentationCalculationPolicy,
    tuple_to_numericrange,
)
from ..classifier import (
    Classifier,
    WorkClassifier,
)
from ..config import CannotLoadConfiguration
from ..util import LanguageCodes
from ..util.string_helpers import native_string


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
        DataSourceConstants.GUTENBERG: 0,
        DataSourceConstants.RB_DIGITAL: 0.4,
        DataSourceConstants.OVERDRIVE: 0.4,
        DataSourceConstants.BIBLIOTHECA : 0.65,
        DataSourceConstants.AXIS_360: 0.65,
        DataSourceConstants.STANDARD_EBOOKS: 0.8,
        DataSourceConstants.UNGLUE_IT: 0.4,
        DataSourceConstants.PLYMPTON: 0.5,
    }

    __tablename__ = 'works'
    id = Column(Integer, primary_key=True)

    # One Work may have copies scattered across many LicensePools.
    license_pools = relationship("LicensePool", backref="work", lazy='joined')

    # A Work takes its presentation metadata from a single Edition.
    # But this Edition is a composite of provider, metadata wrangler, admin interface, etc.-derived Editions.
    presentation_edition_id = Column(Integer, ForeignKey('editions.id'), index=True)

    # One Work may have many associated WorkCoverageRecords.
    coverage_records = relationship(
        "WorkCoverageRecord", backref="work",
        cascade="all, delete-orphan"
    )

    # One Work may be associated with many CustomListEntries.
    # However, a CustomListEntry may lose its Work without
    # ceasing to exist.
    custom_list_entries = relationship('CustomListEntry', backref='work')

    # One Work may have multiple CachedFeeds, and if a CachedFeed
    # loses its Work, it ceases to exist.
    cached_feeds = relationship(
        'CachedFeed', backref='work', cascade="all, delete-orphan"
    )

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

    # A precalculated OPDS entry containing all metadata about this
    # work that would be relevant to display to a library patron.
    simple_opds_entry = Column(Unicode, default=None)

    # A precalculated OPDS entry containing all metadata about this
    # work that would be relevant to display in a machine-to-machine
    # integration context.
    verbose_opds_entry = Column(Unicode, default=None)

    # A precalculated MARC record containing metadata about this
    # work that would be relevant to display in a library's public
    # catalog.
    marc_record = Column(Binary, default=None)

    # These fields are potentially large and can be deferred if you
    # don't need all the data in a Work.
    LARGE_FIELDS = [
        'simple_opds_entry', 'verbose_opds_entry', 'marc_record',
        'summary_text',
    ]

    @property
    def title(self):
        if self.presentation_edition:
            return self.presentation_edition.title
        return None

    @property
    def sort_title(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.sort_title or self.presentation_edition.title

    @property
    def subtitle(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.subtitle

    @property
    def series(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.series

    @property
    def series_position(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.series_position

    @property
    def author(self):
        if self.presentation_edition:
            return self.presentation_edition.author
        return None

    @property
    def sort_author(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.sort_author or self.presentation_edition.author

    @property
    def language(self):
        if self.presentation_edition:
            return self.presentation_edition.language
        return None

    @property
    def language_code(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.language_code

    @property
    def publisher(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.publisher

    @property
    def imprint(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.imprint

    @property
    def cover_full_url(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.cover_full_url

    @property
    def cover_thumbnail_url(self):
        if not self.presentation_edition:
            return None
        return self.presentation_edition.cover_thumbnail_url

    @property
    def target_age_string(self):
        return numericrange_to_string(self.target_age)

    @property
    def has_open_access_license(self):
        return any(x.open_access for x in self.license_pools)

    @property
    def complaints(self):
        complaints = list()
        [complaints.extend(pool.complaints) for pool in self.license_pools]
        return complaints

    def __repr__(self):
        return native_string(
            '<Work #%s "%s" (by %s) %s lang=%s (%s lp)>' % (
                self.id, self.title, self.author,
                ", ".join([g.name for g in self.genres]), self.language,
                len(self.license_pools)
            )
        )

    @classmethod
    def missing_coverage_from(
            cls, _db, operation=None, count_as_covered=None,
            count_as_missing_before=None
    ):
        """Find Works which have no WorkCoverageRecord for the given
        `operation`.
        """

        clause = and_(Work.id==WorkCoverageRecord.work_id,
                      WorkCoverageRecord.operation==operation)
        q = _db.query(Work).outerjoin(WorkCoverageRecord, clause)

        missing = WorkCoverageRecord.not_covered(
            count_as_covered, count_as_missing_before
        )
        q2 = q.filter(missing)
        return q2

    @classmethod
    def for_unchecked_subjects(cls, _db):
        from .classification import (
            Classification,
            Subject,
        )
        from .licensing import LicensePool
        """Find all Works whose LicensePools have an Identifier that
        is classified under an unchecked Subject.
        This is a good indicator that the Work needs to be
        reclassified.
        """
        qu = _db.query(Work).join(Work.license_pools).join(
            LicensePool.identifier).join(
                Identifier.classifications).join(
                    Classification.subject)
        return qu.filter(Subject.checked==False).order_by(Subject.id)

    @classmethod
    def _potential_open_access_works_for_permanent_work_id(
            cls, _db, pwid, medium, language
    ):
        """Find all Works that might be suitable for use as the
        canonical open-access Work for the given `pwid`, `medium`,
        and `language`.
        :return: A 2-tuple (pools, counts_by_work). `pools` is a set
        containing all affected LicensePools; `counts_by_work is a
        Counter tallying the number of affected LicensePools
        associated with a given work.
        """
        from .licensing import LicensePool
        qu = _db.query(LicensePool).join(
            LicensePool.presentation_edition).filter(
                LicensePool.open_access==True
            ).filter(
                Edition.permanent_work_id==pwid
            ).filter(
                Edition.medium==medium
            ).filter(
                Edition.language==language
            )
        pools = set(qu.all())

        # Build the Counter of Works that are eligible to represent
        # this pwid/medium/language combination.
        affected_licensepools_for_work = Counter()
        for lp in pools:
            work = lp.work
            if not lp.work:
                continue
            if affected_licensepools_for_work[lp.work]:
                # We already got this information earlier in the loop.
                continue
            pe = work.presentation_edition
            if pe and (
                    pe.language != language or pe.medium != medium
                    or pe.permanent_work_id != pwid
            ):
                # This Work's presentation edition doesn't match
                # this LicensePool's presentation edition.
                # It would be better to create a brand new Work and
                # remove this LicensePool from its current Work.
                continue
            affected_licensepools_for_work[lp.work] = len(
                [x for x in pools if x.work == lp.work]
            )
        return pools, affected_licensepools_for_work

    @classmethod
    def open_access_for_permanent_work_id(cls, _db, pwid, medium, language):
        """Find or create the Work encompassing all open-access LicensePools
        whose presentation Editions have the given permanent work ID,
        the given medium, and the given language.
        This may result in the consolidation or splitting of Works, if
        a book's permanent work ID has changed without
        calculate_work() being called, or if the data is in an
        inconsistent state for any other reason.
        """
        is_new = False

        licensepools, licensepools_for_work = cls._potential_open_access_works_for_permanent_work_id(
            _db, pwid, medium, language
        )
        if not licensepools:
            # There is no work for this PWID/medium/language combination
            # because no LicensePools offer it.
            return None, is_new

        work = None
        if len(licensepools_for_work) == 0:
            # None of these LicensePools have a Work. Create a new one.
            work = Work()
            is_new = True
        else:
            # Pick the Work with the most LicensePools.
            work, count = licensepools_for_work.most_common(1)[0]

            # In the simple case, there will only be the one Work.
            if len(licensepools_for_work) > 1:
                # But in this case, for whatever reason (probably bad
                # data caused by a bug) there's more than one
                # Work. Merge the other Works into the one we chose
                # earlier.  (This is why we chose the work with the
                # most LicensePools--it minimizes the disruption
                # here.)

                # First, make sure this Work is the exclusive
                # open-access work for its permanent work ID.
                # Otherwise the merge may fail.
                work.make_exclusive_open_access_for_permanent_work_id(
                    pwid, medium, language
                )
                for needs_merge in list(licensepools_for_work.keys()):
                    if needs_merge != work:

                        # Make sure that Work we're about to merge has
                        # nothing but LicensePools whose permanent
                        # work ID matches the permanent work ID of the
                        # Work we're about to merge into.
                        needs_merge.make_exclusive_open_access_for_permanent_work_id(pwid, medium, language)
                        needs_merge.merge_into(work)

        # At this point we have one, and only one, Work for this
        # permanent work ID. Assign it to every LicensePool whose
        # presentation Edition has that permanent work ID/medium/language
        # combination.
        for lp in licensepools:
            lp.work = work
        return work, is_new

    def make_exclusive_open_access_for_permanent_work_id(self, pwid, medium, language):
        """Ensure that every open-access LicensePool associated with this Work
        has the given PWID and medium. Any non-open-access
        LicensePool, and any LicensePool with a different PWID or a
        different medium, is kicked out and assigned to a different
        Work. LicensePools with no presentation edition or no PWID
        are kicked out.
        In most cases this Work will be the _only_ work for this PWID,
        but inside open_access_for_permanent_work_id this is called as
        a preparatory step for merging two Works, and after the call
        (but before the merge) there may be two Works for a given PWID.
        """
        _db = Session.object_session(self)
        for pool in list(self.license_pools):
            other_work = is_new = None
            if not pool.open_access:
                # This needs to have its own Work--we don't mix
                # open-access and commercial versions of the same book.
                pool.work = None
                if pool.presentation_edition:
                    pool.presentation_edition.work = None
                other_work, is_new = pool.calculate_work()
            elif not pool.presentation_edition:
                # A LicensePool with no presentation edition
                # cannot have an associated Work.
                logging.warn(
                    "LicensePool %r has no presentation edition, setting .work to None.",
                    pool
                )
                pool.work = None
            else:
                e = pool.presentation_edition
                this_pwid = e.permanent_work_id
                if not this_pwid:
                    # A LicensePool with no permanent work ID
                    # cannot have an associated Work.
                    logging.warn(
                        "Presentation edition for LicensePool %r has no PWID, setting .work to None.",
                        pool
                    )
                    e.work = None
                    pool.work = None
                    continue
                if this_pwid != pwid or e.medium != medium or e.language != language:
                    # This LicensePool should not belong to this Work.
                    # Make sure it gets its own Work, creating a new one
                    # if necessary.
                    pool.work = None
                    pool.presentation_edition.work = None
                    other_work, is_new = Work.open_access_for_permanent_work_id(
                        _db, this_pwid, e.medium, e.language
                    )
            if other_work and is_new:
                other_work.calculate_presentation()

    @property
    def pwids(self):
        """Return the set of permanent work IDs associated with this Work.
        There should only be one permanent work ID associated with a
        given work, but if there is more than one, this will find all
        of them.
        """
        pwids = set()
        for pool in self.license_pools:
            if pool.presentation_edition and pool.presentation_edition.permanent_work_id:
                pwids.add(pool.presentation_edition.permanent_work_id)
        return pwids

    def merge_into(self, other_work):
        """Merge this Work into another Work and delete it."""

        # Neither the source nor the destination work may have any
        # non-open-access LicensePools.
        for w in self, other_work:
            for pool in w.license_pools:
                if not pool.open_access:
                    raise ValueError(

                        "Refusing to merge %r into %r because it would put an open-access LicensePool into the same work as a non-open-access LicensePool." %
                        (self, other_work)
                        )

        my_pwids = self.pwids
        other_pwids = other_work.pwids
        if not my_pwids == other_pwids:
            raise ValueError(
                "Refusing to merge %r into %r because permanent work IDs don't match: %s vs. %s" % (
                    self, other_work, ",".join(sorted(my_pwids)),
                    ",".join(sorted(other_pwids))
                )
            )

        # Every LicensePool associated with this work becomes
        # associated instead with the other work.
        for pool in self.license_pools:
            other_work.license_pools.append(pool)

        # All WorkGenres and WorkCoverageRecords for this Work are
        # deleted. (WorkGenres are deleted via cascade.)
        _db = Session.object_session(self)
        for cr in self.coverage_records:
            _db.delete(cr)
        _db.delete(self)

        other_work.calculate_presentation()

    def set_summary(self, resource):
        self.summary = resource
        # TODO: clean up the content
        if resource and resource.representation:
            self.summary_text = resource.representation.unicode_content
        else:
            self.summary_text = ""
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.SUMMARY_OPERATION
        )

    @classmethod
    def with_genre(cls, _db, genre):
        """Find all Works classified under the given genre."""
        from .classification import Genre
        if isinstance(genre, str):
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
    def from_identifiers(cls, _db, identifiers, base_query=None, policy=None):
        """Returns all of the works that have one or more license_pools
        associated with either an identifier in the given list or an
        identifier considered equivalent to one of those listed.

        :param policy: A PresentationCalculationPolicy, used to
           determine how far to go when looking for equivalent
           Identifiers. By default, this method will be very strict
           about equivalencies.
        """
        from .licensing import LicensePool
        identifier_ids = [identifier.id for identifier in identifiers]
        if not identifier_ids:
            return None

        if not base_query:
            # A raw base query that makes no accommodations for works that are
            # suppressed or otherwise undeliverable.
            base_query = _db.query(Work).join(Work.license_pools).\
                join(LicensePool.identifier)

        if policy is None:
            policy = PresentationCalculationPolicy(
                equivalent_identifier_levels=1,
                equivalent_identifier_threshold=0.999
            )

        identifier_ids_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            Identifier.id, policy=policy)
        identifier_ids_subquery = identifier_ids_subquery.where(Identifier.id.in_(identifier_ids))

        query = base_query.filter(Identifier.id.in_(identifier_ids_subquery))
        return query

    @classmethod
    def reject_covers(cls, _db, works_or_identifiers,
                        search_index_client=None):
        """Suppresses the currently visible covers of a number of Works"""
        from .licensing import LicensePool
        from .resource import (
            Resource,
            Hyperlink,
        )

        works = list(set(works_or_identifiers))
        if not isinstance(works[0], cls):
            # This assumes that everything in the provided list is the
            # same class: either Work or Identifier.
            works = cls.from_identifiers(_db, works_or_identifiers).all()
        work_ids = [w.id for w in works]

        if len(works) == 1:
            logging.info("Suppressing cover for %r", works[0])
        else:
            logging.info("Supressing covers for %i Works", len(works))

        cover_urls = list()
        for work in works:
            # Create a list of the URLs of the works' active cover images.
            edition = work.presentation_edition
            if edition:
                if edition.cover_full_url:
                    cover_urls.append(edition.cover_full_url)
                if edition.cover_thumbnail_url:
                    cover_urls.append(edition.cover_thumbnail_url)

        if not cover_urls:
            # All of the target Works have already had their
            # covers suppressed. Nothing to see here.
            return

        covers = _db.query(Resource).join(Hyperlink.identifier).\
            join(Identifier.licensed_through).filter(
                Resource.url.in_(cover_urls),
                LicensePool.work_id.in_(work_ids)
            )

        editions = list()
        for cover in covers:
            # Record a downvote that will dismiss the Resource.
            cover.reject()
            if len(cover.cover_editions) > 1:
                editions += cover.cover_editions
        flush(_db)

        editions = list(set(editions))
        if editions:
            # More Editions and Works have been impacted by this cover
            # suppression.
            works += [ed.work for ed in editions if ed.work]
            editions = [ed for ed in editions if not ed.work]

        # Remove the cover from the Work and its Edition and reset
        # cached OPDS entries.
        policy = PresentationCalculationPolicy.reset_cover()
        for work in works:
            work.calculate_presentation(
                policy=policy, search_index_client=search_index_client
            )
        for edition in editions:
            edition.calculate_presentation(policy=policy)
        _db.commit()

    def reject_cover(self, search_index_client=None):
        """Suppresses the current cover of the Work"""
        _db = Session.object_session(self)
        self.suppress_covers(
            _db, [self], search_index_client=search_index_client
        )

    def all_editions(self, policy=None):
        """All Editions identified by an Identifier equivalent to
        the identifiers of this Work's license pools.

        :param policy: A PresentationCalculationPolicy, used to
           determine how far to go when looking for equivalent
           Identifiers.
        """
        from .licensing import LicensePool
        _db = Session.object_session(self)
        identifier_ids_subquery = Identifier.recursively_equivalent_identifier_ids_query(
            LicensePool.identifier_id, policy=policy
        )
        identifier_ids_subquery = identifier_ids_subquery.where(LicensePool.work_id==self.id)

        q = _db.query(Edition).filter(
            Edition.primary_identifier_id.in_(identifier_ids_subquery)
        )
        return q

    @property
    def _direct_identifier_ids(self):
        """Return all Identifier IDs associated with one of this
        Work's LicensePools.
        """
        return [
            lp.identifier.id for lp in self.license_pools
            if lp.identifier
        ]

    def all_identifier_ids(self, policy=None):
        """Return all Identifier IDs associated with this Work.

        :param policy: A `PresentationCalculationPolicy`.
        :return: A set containing all Identifier IDs associated
             with this Work (as per the rules set down in `policy`).
        """
        _db = Session.object_session(self)
        # Get a dict that maps identifier ids to lists of their equivalents.
        equivalent_lists = Identifier.recursively_equivalent_identifier_ids(
            _db, self._direct_identifier_ids, policy=policy
        )

        all_identifier_ids = set()
        for equivs in list(equivalent_lists.values()):
            all_identifier_ids.update(equivs)
        return all_identifier_ids

    @property
    def language_code(self):
        """A single 2-letter language code for display purposes."""
        if not self.language:
            return None
        language = self.language
        if language in LanguageCodes.three_to_two:
            language = LanguageCodes.three_to_two[language]
        return language

    def age_appropriate_for_patron(self, patron):
        """Is this Work age-appropriate for the given Patron?

        :param patron: A Patron.
        :return: A boolean
        """
        if patron is None:
            return True
        return patron.work_is_age_appropriate(self.audience, self.target_age)

    def set_presentation_edition(self, new_presentation_edition):
        """ Sets presentation edition and lets owned pools and editions know.
            Raises exception if edition to set to is None.
        """
        # only bother if something changed, or if were explicitly told to
        # set (useful for setting to None)
        if not new_presentation_edition:
            error_message = "Trying to set presentation_edition to None on Work [%s]" % self.id
            raise ValueError(error_message)

        self.presentation_edition = new_presentation_edition

        # if the edition is the presentation edition for any license
        # pools, let them know they have a Work.
        for pool in self.presentation_edition.is_presentation_for:
            pool.work = self

    def calculate_presentation_edition(self, policy=None):
        """ Which of this Work's Editions should be used as the default?
        First, every LicensePool associated with this work must have
        its presentation edition set.
        Then, we go through the pools, see which has the best presentation edition,
        and make it our presentation edition.
        """
        changed = False
        policy = policy or PresentationCalculationPolicy()
        if not policy.choose_edition:
            return changed

        # For each owned edition, see if its LicensePool was superceded or suppressed
        # if yes, the edition is unlikely to be the best.
        # An open access pool may be "superceded", if there's a better-quality
        # open-access pool available.
        self.mark_licensepools_as_superceded()
        edition_metadata_changed = False
        old_presentation_edition = self.presentation_edition
        new_presentation_edition = None

        for pool in self.license_pools:
            # a superceded pool's composite edition is not good enough
            # Note:  making the assumption here that we won't have a situation
            # where we marked all of the work's pools as superceded or suppressed.
            if pool.superceded or pool.suppressed:
                continue

            # make sure the pool has most up-to-date idea of its presentation edition,
            # and then ask what it is.
            pool_edition_changed = pool.set_presentation_edition()
            edition_metadata_changed = (
                edition_metadata_changed or
                pool_edition_changed
            )
            potential_presentation_edition = pool.presentation_edition

            # We currently have no real way to choose between
            # competing presentation editions. But it doesn't matter much
            # because in the current system there should never be more
            # than one non-superceded license pool per Work.
            #
            # So basically we pick the first available edition and
            # make it the presentation edition.
            if (not new_presentation_edition
                or (potential_presentation_edition is old_presentation_edition and old_presentation_edition)):
                # We would prefer not to change the Work's presentation
                # edition unnecessarily, so if the current presentation
                # edition is still an option, choose it.
                new_presentation_edition = potential_presentation_edition

        if ((self.presentation_edition != new_presentation_edition) and new_presentation_edition != None):
            # did we find a pool whose presentation edition was better than the work's?
            self.set_presentation_edition(new_presentation_edition)

        # tell everyone else we tried to set work's presentation edition
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.CHOOSE_EDITION_OPERATION
        )

        changed = (
            edition_metadata_changed or
            old_presentation_edition != self.presentation_edition
        )
        return changed

    def _get_default_audience(self):
        """Return the default audience.

        :return: Default audience
        :rtype: Optional[str]
        """
        for license_pool in self.license_pools:
            if license_pool.collection.default_audience:
                return license_pool.collection.default_audience

        return None

    def calculate_presentation(
        self, policy=None, search_index_client=None, exclude_search=False,
        default_fiction=None, default_audience=None
    ):
        """Make a Work ready to show to patrons.
        Call calculate_presentation_edition() to find the best-quality presentation edition
        that could represent this work.
        Then determine the following information, global to the work:
        * Subject-matter classifications for the work.
        * Whether or not the work is fiction.
        * The intended audience for the work.
        * The best available summary for the work.
        * The overall popularity of the work.
        """
        if not default_audience:
            default_audience = self._get_default_audience()

        # Gather information up front so we can see if anything
        # actually changed.
        changed = False
        edition_changed = False
        classification_changed = False

        policy = policy or PresentationCalculationPolicy()

        edition_changed = self.calculate_presentation_edition(policy)

        if not self.presentation_edition:
            # Without a presentation edition, we can't calculate presentation
            # for the work.
            return

        if policy.choose_cover or policy.set_edition_metadata:
            cover_changed = self.presentation_edition.calculate_presentation(policy)
            edition_changed = edition_changed or cover_changed

        summary = self.summary
        summary_text = self.summary_text
        quality = self.quality

        # If we find a cover or description that comes direct from a
        # license source, it may short-circuit the process of finding
        # a good cover or description.
        licensed_data_sources = set()
        for pool in self.license_pools:
            # Descriptions from Gutenberg are useless, so we
            # specifically exclude it from being a privileged data
            # source.
            if pool.data_source.name != DataSourceConstants.GUTENBERG:
                licensed_data_sources.add(pool.data_source)

        if policy.classify or policy.choose_summary or policy.calculate_quality:
            # Find all related IDs that might have associated descriptions,
            # classifications, or measurements.
            _db = Session.object_session(self)

            direct_identifier_ids = self._direct_identifier_ids
            all_identifier_ids = self.all_identifier_ids(policy=policy)
        else:
            # Don't bother.
            direct_identifier_ids = all_identifier_ids = []

        if policy.classify:
            classification_changed = self.assign_genres(
                all_identifier_ids,
                default_fiction=default_fiction,
                default_audience=default_audience
            )
            WorkCoverageRecord.add_for(
                self, operation=WorkCoverageRecord.CLASSIFY_OPERATION
            )

        if policy.choose_summary:
            self._choose_summary(
                direct_identifier_ids, all_identifier_ids,
                licensed_data_sources
            )

        if policy.calculate_quality:
            # In the absense of other data, we will make a rough
            # judgement as to the quality of a book based on the
            # license source. Commercial data sources have higher
            # default quality, because it's presumed that a librarian
            # put some work into deciding which books to buy.
            default_quality = None
            for source in licensed_data_sources:
                q = self.default_quality_by_data_source.get(
                    source.name, None
                )
                if q is None:
                    continue
                if default_quality is None or q > default_quality:
                    default_quality = q

            if not default_quality:
                # if we still haven't found anything of a quality measurement,
                # then at least make it an integer zero, not none.
                default_quality = 0
            self.calculate_quality(
                all_identifier_ids, default_quality
            )

        if self.summary_text:
            if isinstance(self.summary_text, str):
                new_summary_text = self.summary_text
            else:
                new_summary_text = self.summary_text.decode("utf8")
        else:
            new_summary_text = self.summary_text

        changed = (
            edition_changed or
            classification_changed or
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

        if changed or policy.regenerate_marc_record:
            self.calculate_marc_record()

        if (changed or policy.update_search_index) and not exclude_search:
            self.external_index_needs_updating()

        # Now that everything's calculated, print it out.
        if policy.verbose:
            if changed:
                changed = "changed"
                representation = self.detailed_representation
            else:
                # TODO: maybe change changed to a boolean, and return it as method result
                changed = "unchanged"
                representation = repr(self)
            logging.info("Presentation %s for work: %s", changed, representation)

        # We want works to be presentation-ready as soon as possible,
        # unless they are missing crucial information like language or
        # title.
        self.set_presentation_ready_based_on_content()

    def _choose_summary(
        self, direct_identifier_ids, all_identifier_ids,
        licensed_data_sources
    ):
        """Helper method for choosing a summary as part of presentation
        calculation.

        Summaries closer to a LicensePool, or from a more trusted source
        will be preferred.

        :param direct_identifier_ids: All IDs of Identifiers of LicensePools
            directly associated with this Work. Summaries associated with
            these IDs will be preferred. In the real world, this will happen
            almost all the time.

        :param all_identifier_ids: All IDs of Identifiers of
            LicensePools associated (directly or indirectly) with this
            Work. Summaries associated with these IDs will be
            used only if none are found from direct_identifier_ids.

        :param licensed_data_sources: A list of DataSources that should be
            given priority -- either because they provided the books or because
            they are trusted sources such as library staff.
        """
        _db = Session.object_session(self)
        staff_data_source = DataSource.lookup(
            _db, DataSourceConstants.LIBRARY_STAFF
        )
        data_sources = [staff_data_source, licensed_data_sources]
        summary = None
        for id_set in (direct_identifier_ids, all_identifier_ids):
            summary, summaries = Identifier.evaluate_summary_quality(
                _db, id_set, data_sources
            )
            if summary:
                # We found a summary.
                break
        self.set_summary(summary)

    @property
    def detailed_representation(self):
        """A description of this work more detailed than repr()"""
        l = ["%s (by %s)" % (self.title, self.author)]
        l.append(" language=%s" % self.language)
        l.append(" quality=%s" % self.quality)

        if self.presentation_edition and self.presentation_edition.primary_identifier:
            primary_identifier = self.presentation_edition.primary_identifier
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

        if self.cover_full_url:
            l.append(" Full cover: %s" % self.cover_full_url)
        else:
            l.append(" No full cover.")

        if self.cover_thumbnail_url:
            l.append(" Cover thumbnail: %s" % self.cover_thumbnail_url)
        else:
            l.append(" No thumbnail cover.")

        downloads = []
        expect_downloads = False
        for pool in self.license_pools:
            if pool.superceded:
                continue
            if pool.open_access:
                expect_downloads = True
            for lpdm in pool.delivery_mechanisms:
                if lpdm.resource and lpdm.resource.final_url:
                    downloads.append(lpdm.resource)

        if downloads:
            l.append(" Open-access downloads:")
            for r in downloads:
                l.append("  " + r.final_url)
        elif expect_downloads:
            l.append(" Expected open-access downloads but found none.")
        def _ensure(s):
            if not s:
                return ""
            elif isinstance(s, str):
                return s
            else:
                return s.decode("utf8", "replace")

        if self.summary and self.summary.representation:
            snippet = _ensure(self.summary.representation.content)[:100]
            d = " Description (%.2f) %s" % (self.summary.quality, snippet)
            l.append(d)

        l = [_ensure(s) for s in l]
        return "\n".join(l)

    def calculate_opds_entries(self, verbose=True):
        from ..opds import (
            AcquisitionFeed,
            Annotator,
            VerboseAnnotator,
        )
        _db = Session.object_session(self)
        simple = AcquisitionFeed.single_entry(
            _db, self, Annotator, force_create=True
        )
        if verbose is True:
            verbose = AcquisitionFeed.single_entry(
                _db, self, VerboseAnnotator, force_create=True
            )
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.GENERATE_OPDS_OPERATION
        )

    def calculate_marc_record(self):
        from ..marc import (
            Annotator,
            MARCExporter
        )
        _db = Session.object_session(self)
        record = MARCExporter.create_record(
            self, annotator=Annotator, force_create=True)
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.GENERATE_MARC_OPERATION
        )

    def active_license_pool(self):
        # The active license pool is the one that *would* be
        # associated with a loan, were a loan to be issued right
        # now.
        active_license_pool = None
        for p in self.license_pools:
            if p.superceded:
                continue
            edition = p.presentation_edition
            if p.open_access:
                if p.best_open_access_link:
                    active_license_pool = p
                    # We have an unlimited source for this book.
                    # There's no need to keep looking.
                    break
            elif p.unlimited_access or p.self_hosted:
                active_license_pool = p
            elif edition and edition.title and p.licenses_owned > 0:
                active_license_pool = p
        return active_license_pool

    def _reset_coverage(self, operation):
        """Put this work's WorkCoverageRecord for the given `operation`
        into the REGISTERED state.

        This is useful for erasing the record of work that was done,
        so that automated scripts know the work needs to be done
        again.

        :return: A WorkCoverageRecord.
        """
        _db = Session.object_session(self)
        record, is_new = WorkCoverageRecord.add_for(
            self, operation=operation, status=CoverageRecord.REGISTERED
        )
        return record

    def external_index_needs_updating(self):
        """Mark this work as needing to have its search document reindexed.
        This is a more efficient alternative to reindexing immediately,
        since these WorkCoverageRecords are handled in large batches.
        """
        return self._reset_coverage(
            WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
        )

    def update_external_index(self, client, add_coverage_record=True):
        """Create a WorkCoverageRecord so that this work's
        entry in the search index can be modified or deleted.
        This method is deprecated -- call
        external_index_needs_updating() instead.
        """
        self.external_index_needs_updating()

    def needs_full_presentation_recalculation(self):
        """Mark this work as needing to have its presentation completely
        recalculated.

        This shifts the time spent recalculating presentation to a
        script dedicated to this purpose, rather than a script that
        interacts with APIs. It's also more efficient, since a work
        might be flagged multiple times before we actually get around
        to recalculating the presentation.
        """
        return self._reset_coverage(WorkCoverageRecord.CLASSIFY_OPERATION)

    def needs_new_presentation_edition(self):
        """Mark this work as needing to have its presentation edition
        regenerated. This is significantly less work than
        calling needs_full_presentation_recalculation, but it will
        not update a Work's quality score, summary, or genre classification.
        """
        return self._reset_coverage(WorkCoverageRecord.CHOOSE_EDITION_OPERATION)

    def set_presentation_ready(
        self, as_of=None, search_index_client=None, exclude_search=False
    ):
        """Set this work as presentation-ready, no matter what.

        This assumes that we know the work has the minimal information
        necessary to be found with typical queries and that patrons
        will be able to understand what work we're talking about.

        In most cases you should call set_presentation_ready_based_on_content
        instead, which runs those checks.
        """
        as_of = as_of or datetime.datetime.utcnow()
        self.presentation_ready = True
        self.presentation_ready_exception = None
        self.presentation_ready_attempt = as_of
        if not exclude_search:
            self.external_index_needs_updating()

    def set_presentation_ready_based_on_content(self, search_index_client=None):
        """Set this work as presentation ready, if it appears to
        be ready based on its data.

        Presentation ready means the book is ready to be shown to
        patrons and (pending availability) checked out. It doesn't
        necessarily mean the presentation is complete.

        The absolute minimum data necessary is a title, a language,
        and a medium. We don't need a cover or an author -- we can
        fill in that info later if it exists.

        TODO: search_index_client is redundant here.
        """
        if (not self.presentation_edition
            or not self.license_pools
            or not self.title
            or not self.language
            or not self.presentation_edition.medium
        ):
            self.presentation_ready = False
            # The next time the search index WorkCoverageRecords are
            # processed, this work will be removed from the search
            # index.
            self.external_index_needs_updating()
            logging.warn("Work is not presentation ready: %r", self)
        else:
            self.set_presentation_ready(search_index_client=search_index_client)

    def calculate_quality(self, identifier_ids, default_quality=0):
        _db = Session.object_session(self)
        # Relevant Measurements are direct measurements of popularity
        # and quality, plus any quantity that might be mapppable to the 0..1
        # range -- ratings, and measurements with an associated percentile
        # score.
        quantities = set([
            Measurement.POPULARITY, Measurement.QUALITY, Measurement.RATING
        ])
        quantities = quantities.union(list(Measurement.PERCENTILE_SCALES.keys()))
        measurements = _db.query(Measurement).filter(
            Measurement.identifier_id.in_(identifier_ids)).filter(
                Measurement.is_most_recent==True).filter(
                    Measurement.quantity_measured.in_(quantities)).all()

        self.quality = Measurement.overall_quality(
            measurements, default_value=default_quality)
        WorkCoverageRecord.add_for(
            self, operation=WorkCoverageRecord.QUALITY_OPERATION
        )

    def assign_genres(self, identifier_ids, default_fiction=False, default_audience=Classifier.AUDIENCE_ADULT):
        """Set classification information for this work based on the
        subquery to get equivalent identifiers.
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
         target_age) = classifier.classify(default_fiction=default_fiction,
                                           default_audience=default_audience)
        self.target_age = tuple_to_numericrange(target_age)

        workgenres, workgenres_changed = self.assign_genres_from_weights(
            genre_weights
        )

        classification_changed = (
            workgenres_changed or
            old_fiction != self.fiction or
            old_audience != self.audience or
            numericrange_to_tuple(old_target_age) != target_age
        )

        return classification_changed

    def assign_genres_from_weights(self, genre_weights):
        # Assign WorkGenre objects to the remainder.
        from .classification import Genre
        changed = False
        _db = Session.object_session(self)
        total_genre_weight = float(sum(genre_weights.values()))
        workgenres = []
        current_workgenres = _db.query(WorkGenre).filter(WorkGenre.work==self)
        by_genre = dict()
        for wg in current_workgenres:
            by_genre[wg.genre] = wg
        for g, score in list(genre_weights.items()):
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
        for wg in list(by_genre.values()):
            _db.delete(wg)
            changed = True

        # ensure that work_genres is up to date without having to read from database again
        self.work_genres = workgenres

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

    # This can be used in func.to_char to convert a SQL datetime into a string
    # that Elasticsearch can parse as a date.
    ELASTICSEARCH_TIME_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS"."MS'

    @classmethod
    def to_search_documents(cls, works, policy=None):
        """Generate search documents for these Works.
        This is done by constructing an extremely complicated
        SQL query. The code is ugly, but it's about 100 times
        faster than using python to create documents for
        each work individually. When working on the search
        index, it's very important for this to be fast.

        :param policy: A PresentationCalculationPolicy to use when
           deciding how deep to go to find Identifiers equivalent to
           these works.
        """

        if not works:
            return []

        _db = Session.object_session(works[0])

        # If this is a batch of search documents, postgres needs extra working
        # memory to process the query quickly.
        if len(works) > 50:
            _db.execute("set work_mem='200MB'")

        # This query gets relevant columns from Work and Edition for the Works we're
        # interested in. The work_id, edition_id, and identifier_id columns are used
        # by other subqueries to filter, and the remaining columns are used directly
        # to create the json document.
        works_alias = select(
            [Work.id.label('work_id'),
             Edition.id.label('edition_id'),
             Edition.primary_identifier_id.label('identifier_id'),
             Edition.title,
             Edition.subtitle,
             Edition.series,
             Edition.series_position,
             Edition.language,
             Edition.sort_title,
             Edition.author,
             Edition.sort_author,
             Edition.medium,
             Edition.publisher,
             Edition.imprint,
             Edition.permanent_work_id,
             Work.fiction,
             Work.audience,
             Work.summary_text,
             Work.quality,
             Work.rating,
             Work.popularity,
             Work.presentation_ready,
             Work.presentation_edition_id,
             func.extract(
                 "EPOCH",
                 Work.last_update_time,
             ).label('last_update_time')
            ],
            Work.id.in_((w.id for w in works))
        ).select_from(
            join(
                Work, Edition,
                Work.presentation_edition_id==Edition.id
            )
        ).alias('works_alias')

        work_id_column = literal_column(
            works_alias.name + '.' + works_alias.c.work_id.name
        )

        work_presentation_edition_id_column = literal_column(
            works_alias.name + '.' + works_alias.c.presentation_edition_id.name
        )

        work_quality_column = literal_column(
            works_alias.name + '.' + works_alias.c.quality.name
        )

        def query_to_json(query):
            """Convert the results of a query to a JSON object."""
            return select(
                [func.row_to_json(literal_column(query.name))]
            ).select_from(query)

        def query_to_json_array(query):
            """Convert the results of a query into a JSON array."""
            return select(
                [func.array_to_json(
                    func.array_agg(
                        func.row_to_json(
                            literal_column(query.name)
                        )))]
            ).select_from(query)

        # This subquery gets Collection IDs for collections
        # that own more than zero licenses for this book.
        from .classification import (
            Genre,
            Subject,
        )
        from .customlist import CustomListEntry
        from .licensing import LicensePool

        # We need information about LicensePools for a few reasons:
        #
        # * We always want to filter out Works that are not available
        #   in any of the collections associated with a given library
        #   -- either because no licenses are owned, because the
        #   LicensePools are suppressed, or (TODO) because there are no
        #   delivery mechanisms.
        # * A patron may want to sort a list of books by availability
        #   date.
        # * A patron may want to show only books currently available,
        #   or only open-access books.
        #
        # Whenever LicensePool.open_access is changed, or
        # licenses_available moves to zero or away from zero, the
        # LicensePool signals that its Work needs reindexing.
        #
        # The work quality field is stored in the main document, but
        # it's also stored here, so that we can apply a nested filter
        # that combines quality with other fields found only in the subdocument.

        def explicit_bool(label, t):
            # Ensure we always generate True/False instead of
            # True/None. Elasticsearch can't filter on null values.
            return case([(t, True)], else_=False).label(label)

        licensepools = select(
            [
                LicensePool.id.label('licensepool_id'),
                LicensePool.data_source_id.label('data_source_id'),
                LicensePool.collection_id.label('collection_id'),
                LicensePool.open_access.label('open_access'),
                LicensePool.suppressed,

                explicit_bool(
                    'available',
                    or_(
                        LicensePool.unlimited_access,
                        LicensePool.self_hosted,
                        LicensePool.licenses_available > 0,
                    )
                ),
                explicit_bool(
                    'licensed',
                    or_(
                        LicensePool.unlimited_access,
                        LicensePool.self_hosted,
                        LicensePool.licenses_owned > 0
                    )
                ),
                work_quality_column,
                Edition.medium,
                func.extract(
                    "EPOCH",
                    LicensePool.availability_time,
                ).label('availability_time')
            ]
        ).where(
            and_(
                LicensePool.work_id==work_id_column,
                work_presentation_edition_id_column==Edition.id,
                or_(
                    LicensePool.open_access,
                    LicensePool.unlimited_access,
                    LicensePool.self_hosted,
                    LicensePool.licenses_owned>0,
                ),
            )
        ).alias("licensepools_subquery")
        licensepools_json = query_to_json_array(licensepools)

        # This subquery gets CustomList IDs for all lists
        # that contain the work.
        #
        # We also keep track of whether the work is featured on each
        # list. This is used when determining which works should be
        # featured for a lane based on CustomLists.
        #
        # And we keep track of the first time the work appears on the list.
        # This is used when generating a crawlable feed for the customlist,
        # which is ordered by a work's first appearance on the list.
        customlists = select(
            [
                CustomListEntry.list_id.label('list_id'),
                CustomListEntry.featured.label('featured'),
                func.extract(
                    "EPOCH",
                    CustomListEntry.first_appearance,
                ).label('first_appearance')
            ]
        ).where(
            CustomListEntry.work_id==work_id_column
        ).alias("listentries_subquery")
        customlists_json = query_to_json_array(customlists)

        # This subquery gets Contributors, filtered on edition_id.
        contributors = select(
            [Contributor.sort_name,
             Contributor.display_name,
             Contributor.family_name,
             Contributor.lc,
             Contributor.viaf,
             Contribution.role,
            ]
        ).where(
            Contribution.edition_id==literal_column(works_alias.name + "." + works_alias.c.edition_id.name)
        ).select_from(
            join(
                Contributor, Contribution,
                Contributor.id==Contribution.contributor_id
            )
        ).alias("contributors_subquery")
        contributors_json = query_to_json_array(contributors)

        # Use a subquery to get recursively equivalent Identifiers
        # for the Edition's primary_identifier_id.
        #
        # NOTE: we don't reliably reindex works when this information
        # changes, but it's not critical that this information be
        # totally up to date -- we only use it for subject searches
        # and recommendations. The index is completely rebuilt once a
        # day, and that's good enough.
        equivalent_identifiers = Identifier.recursively_equivalent_identifier_ids_query(
            literal_column(
                works_alias.name + "." + works_alias.c.identifier_id.name
            ),
            policy=policy
        ).alias("equivalent_identifiers_subquery")

        identifiers = select(
            [
                Identifier.identifier.label('identifier'),
                Identifier.type.label('type'),
            ]
        ).where(
            Identifier.id.in_(equivalent_identifiers)
        ).alias("identifier_subquery")
        identifiers_json = query_to_json_array(identifiers)

        # Map our constants for Subject type to their URIs.
        scheme_column = case(
            [(Subject.type==key, literal_column("'%s'" % val)) for key, val in list(Subject.uri_lookup.items())]
        )

        # If the Subject has a name, use that, otherwise use the Subject's identifier.
        # Also, 3M's classifications have slashes, e.g. "FICTION/Adventure". Make sure
        # we get separated words for search.
        term_column = func.replace(case([(Subject.name != None, Subject.name)], else_=Subject.identifier), "/", " ")

        # Normalize by dividing each weight by the sum of the weights for that Identifier's Classifications.
        from .classification import Classification
        weight_column = func.sum(Classification.weight) / func.sum(func.sum(Classification.weight)).over()

        # The subquery for Subjects, with those three columns. The labels will become keys in json objects.
        subjects = select(
            [scheme_column.label('scheme'),
             term_column.label('term'),
             weight_column.label('weight'),
            ],
            # Only include Subjects with terms that are useful for search.
            and_(Subject.type.in_(Subject.TYPES_FOR_SEARCH),
                 term_column != None)
        ).group_by(
            scheme_column, term_column
        ).where(
            Classification.identifier_id.in_(equivalent_identifiers)
        ).select_from(
            join(Classification, Subject, Classification.subject_id==Subject.id)
        ).alias("subjects_subquery")
        subjects_json = query_to_json_array(subjects)


        # Subquery for genres.
        genres = select(
            # All Genres have the same scheme - the simplified genre URI.
            [literal_column("'%s'" % Subject.SIMPLIFIED_GENRE).label('scheme'),
             Genre.name,
             Genre.id.label('term'),
             WorkGenre.affinity.label('weight'),
            ]
        ).where(
            WorkGenre.work_id==literal_column(works_alias.name + "." + works_alias.c.work_id.name)
        ).select_from(
            join(WorkGenre, Genre, WorkGenre.genre_id==Genre.id)
        ).alias("genres_subquery")
        genres_json = query_to_json_array(genres)

        target_age = cls.target_age_query(
            literal_column(works_alias.name + "." + works_alias.c.work_id.name)
        ).alias('target_age_subquery')
        target_age_json = query_to_json(target_age)

        # Now, create a query that brings together everything we need for the final
        # search document.
        search_data = select(
            [works_alias.c.work_id.label("_id"),
             works_alias.c.work_id.label("work_id"),
             works_alias.c.title,
             works_alias.c.sort_title,
             works_alias.c.subtitle,
             works_alias.c.series,
             works_alias.c.series_position,
             works_alias.c.language,
             works_alias.c.author,
             works_alias.c.sort_author,
             works_alias.c.medium,
             works_alias.c.publisher,
             works_alias.c.imprint,
             works_alias.c.permanent_work_id,
             works_alias.c.presentation_ready,
             works_alias.c.last_update_time,

             # Convert true/false to "Fiction"/"Nonfiction".
             case(
                    [(works_alias.c.fiction==True, literal_column("'Fiction'"))],
                    else_=literal_column("'Nonfiction'")
                    ).label("fiction"),

             # Replace "Young Adult" with "YoungAdult" and "Adults Only" with "AdultsOnly".
             func.replace(works_alias.c.audience, " ", "").label('audience'),

             works_alias.c.summary_text.label('summary'),
             works_alias.c.quality,
             works_alias.c.rating,
             works_alias.c.popularity,

             # Here are all the subqueries.
             licensepools_json.label("licensepools"),
             customlists_json.label("customlists"),
             contributors_json.label("contributors"),
             identifiers_json.label("identifiers"),
             subjects_json.label("classifications"),
             genres_json.label('genres'),
             target_age_json.label('target_age'),
            ]
        ).select_from(
            works_alias
        ).alias("search_data_subquery")

        # Finally, convert everything to json.
        search_json = query_to_json(search_data)

        result = _db.execute(search_json)
        if result:
            return [r[0] for r in result]

    @classmethod
    def target_age_query(self, foreign_work_id_field):
        # If the upper limit of the target age is inclusive, we leave
        # it alone. Otherwise, we subtract one to make it inclusive.
        upper_field = func.upper(Work.target_age)
        upper = case(
            [(func.upper_inc(Work.target_age), upper_field)],
            else_=upper_field-1
        ).label('upper')

        # If the lower limit of the target age is inclusive, we leave
        # it alone. Otherwise, we add one to make it inclusive.
        lower_field = func.lower(Work.target_age)
        lower = case(
            [(func.lower_inc(Work.target_age), lower_field)],
            else_=lower_field+1
        ).label('lower')

        # Subquery for target age. This has to be a subquery so it can
        # become a nested object in the final json.
        target_age = select(
            [upper, lower]
        ).where(
            Work.id==foreign_work_id_field
        )
        return target_age

    def to_search_document(self):
        """Generate a search document for this Work."""
        return Work.to_search_documents([self])[0]

    def mark_licensepools_as_superceded(self):
        """Make sure that all but the single best open-access LicensePool for
        this Work are superceded. A non-open-access LicensePool should
        never be superceded, and this method will mark them as
        un-superceded.
        """
        champion_open_access_license_pool = None
        for pool in self.license_pools:
            if not pool.open_access:
                pool.superceded = False
                continue
            if pool.better_open_access_pool_than(champion_open_access_license_pool):
                if champion_open_access_license_pool:
                    champion_open_access_license_pool.superceded = True
                champion_open_access_license_pool = pool
                pool.superceded = False
            else:
                pool.superceded = True

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

    def classifications_with_genre(self):
        from .classification import (
            Classification,
            Subject,
        )
        _db = Session.object_session(self)
        identifier = self.presentation_edition.primary_identifier
        return _db.query(Classification) \
            .join(Subject) \
            .filter(Classification.identifier_id == identifier.id) \
            .filter(Subject.genre_id != None) \
            .order_by(Classification.weight.desc())

    def top_genre(self):
        from .classification import Genre
        _db = Session.object_session(self)
        genre = _db.query(Genre) \
            .join(WorkGenre) \
            .filter(WorkGenre.work_id == self.id) \
            .order_by(WorkGenre.affinity.desc()) \
            .first()
        return genre.name if genre else None

    def delete(self, search_index=None):
        """Delete the work from both the DB and search index."""
        _db = Session.object_session(self)
        if search_index is None:
            try:
                from ..external_search import ExternalSearchIndex
                search_index = ExternalSearchIndex(_db)
            except CannotLoadConfiguration as e:
                # No search index is configured. This is fine -- just skip that part.
                pass
        if search_index is not None:
            search_index.remove_work(self)
        _db.delete(self)
