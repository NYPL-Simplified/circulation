import datetime
import logging
from urllib.parse import quote
from collections import (
    defaultdict,
)
from lxml import etree

from sqlalchemy.orm.session import Session

from .cdn import cdnify
from .classifier import Classifier
from .entrypoint import EntryPoint
from .facets import FacetConstants
from .lane import (
    Facets,
    FacetsWithEntryPoint,
    FeaturedFacets,
    Lane,
    Pagination,
    SearchFacets,
)
from .lcp.credential import LCPCredentialFactory
from .model import (
    CachedFeed,
    Contributor,
    DataSource,
    Hyperlink,
    PresentationCalculationPolicy,
    Identifier,
    Edition,
    Measurement,
    Subject,
    Work,
    ExternalIntegration
)
from .util.flask_util import (
    OPDSEntryResponse,
    OPDSFeedResponse,
)
from .util.opds_writer import (
    AtomFeed,
    OPDSFeed,
    OPDSMessage,
)
from .util.datetime_helpers import utc_now


class UnfulfillableWork(Exception):
    """Raise this exception when it turns out a Work currently cannot be
    fulfilled through any means, *and* this is a problem sufficient to
    cancel the creation of an <entry> for the Work.

    For commercial works, this might be because the collection
    contains no licenses. For open-access works, it might be because
    none of the delivery mechanisms could be mirrored.
    """

class Annotator(object):
    """The Annotator knows how to present an OPDS feed in a specific
    application context.
    """

    opds_cache_field = Work.simple_opds_entry.name

    def is_work_entry_solo(self, work):
        """Return a boolean value indicating whether the work's OPDS catalog entry is served by itself,
            rather than as a part of the feed.

        :param work: Work object
        :type work: core.model.work.Work

        :return: Boolean value indicating whether the work's OPDS catalog entry is served by itself,
            rather than as a part of the feed
        :rtype: bool
        """
        return False

    def annotate_work_entry(self, work, active_license_pool, edition,
                            identifier, feed, entry, updated=None):
        """Make any custom modifications necessary to integrate this
        OPDS entry into the application's workflow.

        :work: The Work whose OPDS entry is being annotated.
        :active_license_pool: Of all the LicensePools associated with this
           Work, the client has expressed interest in this one.
        :edition: The Edition to use when associating bibliographic
           metadata with this entry. You will probably not need to use
           this, because bibliographic metadata was associated with
           the entry when it was created.
        :identifier: Of all the Identifiers associated with this
           Work, the client has expressed interest in this one.
        :param feed: An OPDSFeed -- the feed in which this entry will be
           situated.
        :param entry: An lxml Element object, the entry that will be added
           to the feed.
        """

        # First, try really hard to find an Identifier that we can
        # use to make the <id> tag.
        if not identifier:
            if active_license_pool:
                identifier = active_license_pool.identifier
            elif edition:
                identifier = edition.primary_identifier

        if identifier:
            entry.append(AtomFeed.id(identifier.urn))

        # Add a permalink if one is available.
        permalink_uri, permalink_type = self.permalink_for(
            work, active_license_pool, identifier
        )
        if permalink_uri:
            OPDSFeed.add_link_to_entry(
                entry, rel='alternate', href=permalink_uri,
                type=permalink_type
            )

            if self.is_work_entry_solo(work):
                OPDSFeed.add_link_to_entry(
                    entry, rel='self', href=permalink_uri,
                    type=permalink_type
                )

        if active_license_pool:
            data_source = active_license_pool.data_source.name
            if data_source != DataSource.INTERNAL_PROCESSING:
                # INTERNAL_PROCESSING indicates a dummy LicensePool
                # created as a stand-in, e.g. by the metadata wrangler.
                # This component is not actually distributing the book,
                # so it should not have a bibframe:distribution tag.
                provider_name_attr = "{%s}ProviderName" % AtomFeed.BIBFRAME_NS
                kwargs = {provider_name_attr : data_source}
                data_source_tag = AtomFeed.makeelement(
                    "{%s}distribution" % AtomFeed.BIBFRAME_NS,
                    **kwargs
                )
                entry.extend([data_source_tag])

            # We use Atom 'published' for the date the book first became
            # available to people using this application.
            avail = active_license_pool.availability_time
            if avail:
                now = utc_now()
                today = datetime.date.today()
                if isinstance(avail, datetime.datetime):
                    avail = avail.date()
                if avail <= today: # Avoid obviously wrong values.
                    availability_tag = AtomFeed.makeelement("published")
                    # TODO: convert to local timezone.
                    availability_tag.text = AtomFeed._strftime(avail)
                    entry.extend([availability_tag])

        # If this OPDS entry is being used as part of a grouped feed
        # (which is up to the Annotator subclass), we need to add a
        # group link.
        group_uri, group_title = self.group_uri(
            work, active_license_pool, identifier
        )
        if group_uri:
            OPDSFeed.add_link_to_entry(
                entry, rel=OPDSFeed.GROUP_REL, href=group_uri,
                title=str(group_title)
            )

        if not updated and work.last_update_time:
            # NOTE: This is a default that works in most cases. When
            # ordering ElasticSearch results by last update time,
            # `work` is a WorkSearchResult object containing a more
            # reliable value that you can use if you want.
            updated = work.last_update_time
        if updated:
            entry.extend([AtomFeed.updated(AtomFeed._strftime(updated))])

    @classmethod
    def annotate_feed(cls, feed, lane, list=None):
        """Make any custom modifications necessary to integrate this
        OPDS feed into the application's workflow.
        """
        pass

    @classmethod
    def group_uri(cls, work, license_pool, identifier):
        """The URI to be associated with this Work when making it part of
        a grouped feed.

        By default, this does nothing. See circulation/LibraryAnnotator
        for a subclass that does something.

        :return: A 2-tuple (URI, title)
        """
        return None, ""

    @classmethod
    def rating_tag(cls, type_uri, value):
        """Generate a schema:Rating tag for the given type and value."""
        rating_tag = AtomFeed.makeelement(AtomFeed.schema_("Rating"))
        value_key = AtomFeed.schema_('ratingValue')
        rating_tag.set(value_key, "%.4f" % value)
        if type_uri:
            type_key = AtomFeed.schema_('additionalType')
            rating_tag.set(type_key, type_uri)
        return rating_tag

    @classmethod
    def cover_links(cls, work):
        """Return all links to be used as cover links for this work.

        In a distribution application, each work will have only one
        link. In a content server-type application, each work may have
        a large number of links.

        :return: A 2-tuple (thumbnail_links, full_links)
        """
        thumbnails = []
        full = []
        if work:
            _db = Session.object_session(work)
            if work.cover_thumbnail_url:
                thumbnails = [cdnify(work.cover_thumbnail_url)]

            if work.cover_full_url:
                full = [cdnify(work.cover_full_url)]
        return thumbnails, full

    @classmethod
    def categories(cls, work):
        """Return all relevant classifications of this work.

        :return: A dictionary mapping 'scheme' URLs to dictionaries of
            attribute-value pairs.

        Notable attributes: 'term', 'label', 'http://schema.org/ratingValue'
        """
        if not work:
            return {}

        categories = {}

        fiction_term = None
        if work.fiction == True:
            fiction_term = 'Fiction'
        elif work.fiction == False:
            fiction_term = 'Nonfiction'
        if fiction_term:
            fiction_scheme = Subject.SIMPLIFIED_FICTION_STATUS
            categories[fiction_scheme] = [
                dict(term=fiction_scheme + fiction_term,
                     label=fiction_term)
            ]

        simplified_genres = []
        for wg in work.work_genres:
            simplified_genres.append(wg.genre.name)

        if simplified_genres:
            categories[Subject.SIMPLIFIED_GENRE] = [
                dict(term=Subject.SIMPLIFIED_GENRE + quote(x),
                     label=x)
                for x in simplified_genres
            ]

        # Add the appeals as a category of schema
        # http://librarysimplified.org/terms/appeal
        schema_url = AtomFeed.SIMPLIFIED_NS + "appeals/"
        appeals = []
        categories[schema_url] = appeals
        for name, value in (
                (Work.CHARACTER_APPEAL, work.appeal_character),
                (Work.LANGUAGE_APPEAL, work.appeal_language),
                (Work.SETTING_APPEAL, work.appeal_setting),
                (Work.STORY_APPEAL, work.appeal_story),
        ):
            if value:
                appeal = dict(term=schema_url + name, label=name)
                weight_field = AtomFeed.schema_("ratingValue")
                appeal[weight_field] = value
                appeals.append(appeal)

        # Add the audience as a category of schema
        # http://schema.org/audience
        if work.audience:
            audience_uri = AtomFeed.SCHEMA_NS + "audience"
            categories[audience_uri] = [
                dict(term=work.audience, label=work.audience)
            ]

        # Any book can have a target age, but the target age
        # is only relevant for childrens' and YA books.
        audiences_with_target_age = (
            Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT
        )
        if (work.target_age and work.audience in audiences_with_target_age):
            uri = Subject.uri_lookup[Subject.AGE_RANGE]
            target_age = work.target_age_string
            if target_age:
                categories[uri] = [dict(term=target_age, label=target_age)]

        return categories

    @classmethod
    def authors(cls, work, edition):
        """Create one or more <author> and <contributor> tags for the given
        Work.

        :param work: The Work under consideration.
        :param edition: The Edition to use as a reference
            for bibliographic information, including the list of
            Contributions.
        """
        authors = list()
        state = defaultdict(set)
        for contribution in edition.contributions:
            tag = cls.contributor_tag(contribution, state)
            if tag is None:
                # contributor_tag decided that this contribution doesn't
                # need a tag.
                continue
            authors.append(tag)

        if authors:
            return authors

        # We have no author information, so we add empty <author> tag
        # to avoid the implication (per RFC 4287 4.2.1) that this book
        # was written by whoever wrote the OPDS feed.
        return [AtomFeed.author(AtomFeed.name(""))]

    @classmethod
    def contributor_tag(cls, contribution, state):
        """Build an <author> or <contributor> tag for a Contribution.

        :param contribution: A Contribution.
        :param state: A defaultdict of sets, which may be used to keep
            track of what happened during previous calls to
            contributor_tag for a given Work.
        :return: A Tag, or None if creating a Tag for this Contribution
            would be redundant or of low value.

        """
        contributor = contribution.contributor
        role = contribution.role

        if role in Contributor.AUTHOR_ROLES:
            tag_f = AtomFeed.author
            marc_role = None
        else:
            tag_f = AtomFeed.contributor
            marc_role = Contributor.MARC_ROLE_CODES.get(role)
            if not marc_role:
                # This contribution is not one that we publish as
                # a <atom:contributor> tag. Skip it.
                return None

        name = contributor.display_name or contributor.sort_name
        name_key = name.lower()
        if name_key in state[marc_role]:
            # We've already credited this person with this
            # MARC role. Returning a tag would be redundant.
            return None

        # Okay, we're creating a tag.
        properties = dict()
        if marc_role:
            properties['{%s}role' % AtomFeed.OPF_NS] = marc_role
        tag = tag_f(AtomFeed.name(name), **properties)

        # Record the fact that we credited this person with this role,
        # so that we don't do it again on a subsequent call.
        state[marc_role].add(name_key)

        return tag

    @classmethod
    def series(cls, series_name, series_position):
        """Generate a schema:Series tag for the given name and position."""
        if not series_name:
            return None
        series_details = dict()
        series_details['name'] = series_name
        if series_position != None:
            series_details[AtomFeed.schema_('position')] = str(series_position)
        series_tag = AtomFeed.makeelement(AtomFeed.schema_("Series"), **series_details)
        return series_tag

    @classmethod
    def content(cls, work):
        """Return an HTML summary of this work."""
        summary = ""
        if work:
            if work.summary_text != None:
                summary = work.summary_text
            elif work.summary and work.summary.content:
                work.summary_text = work.summary.content
                summary = work.summary_text
        return summary

    @classmethod
    def lane_id(cls, lane):
        return cls.featured_feed_url(lane)

    @classmethod
    def work_id(cls, work):
        return work.presentation_edition.primary_identifier.urn

    @classmethod
    def permalink_for(cls, work, license_pool, identifier):
        """Generate a permanent link a client can follow for information about
        this entry, and only this entry.

        Note that permalink is distinct from the Atom <id>,
        which is always the identifier's URN.

        :return: A 2-tuple (URL, media type). If a single value is
            returned, the media type will be presumed to be that of an
            OPDS entry.
        """
        # In the absence of any specific controllers, there is no
        # permalink. This method must be defined in a subclass.
        return None, None

    @classmethod
    def lane_url(cls, lane, facets=None):
        raise NotImplementedError()

    @classmethod
    def feed_url(cls, lane, facets=None, pagination=None):
        raise NotImplementedError()

    @classmethod
    def groups_url(cls, lane, facets=None):
        raise NotImplementedError()

    @classmethod
    def search_url(cls, lane, query, pagination, facets=None):
        raise NotImplementedError()

    @classmethod
    def default_lane_url(cls):
        raise NotImplementedError()

    @classmethod
    def featured_feed_url(cls, lane, order=None, facets=None):
        raise NotImplementedError()

    @classmethod
    def facet_url(cls, facets, facet=None):
        return None

    @classmethod
    def navigation_url(cls, lane):
        raise NotImplementedError()

    @classmethod
    def active_licensepool_for(cls, work):
        """Which license pool would be/has been used to issue a license for
        this work?
        """
        if not work:
            return None

        return work.active_license_pool()

    def sort_works_for_groups_feed(self, works, **kwargs):
        return works


class VerboseAnnotator(Annotator):
    """The default Annotator for machine-to-machine integration.

    This Annotator describes all categories and authors for the book
    in great detail.
    """

    opds_cache_field = Work.verbose_opds_entry.name

    def annotate_work_entry(self, work, active_license_pool, edition,
                            identifier, feed, entry):
        super(VerboseAnnotator, self).annotate_work_entry(
            work, active_license_pool, edition, identifier, feed, entry
        )
        self.add_ratings(work, entry)

    @classmethod
    def add_ratings(cls, work, entry):
        """Add a quality rating to the work.
        """
        for type_uri, value in [
                (Measurement.QUALITY, work.quality),
                (None, work.rating),
                (Measurement.POPULARITY, work.popularity),
        ]:
            if value:
                entry.append(cls.rating_tag(type_uri, value))

    @classmethod
    def categories(cls, work, policy=None):
        """Send out _all_ categories for the work.

        (So long as the category type has a URI associated with it in
        Subject.uri_lookup.)

        :param policy: A PresentationCalculationPolicy to
            use when deciding how deep to go when finding equivalent
            identifiers for the work.
        """
        policy = policy or PresentationCalculationPolicy(
            equivalent_identifier_cutoff=100
        )
        _db = Session.object_session(work)
        by_scheme_and_term = dict()
        identifier_ids = work.all_identifier_ids(policy=policy)
        classifications = Identifier.classifications_for_identifier_ids(
            _db, identifier_ids)
        for c in classifications:
            subject = c.subject
            if subject.type in Subject.uri_lookup:
                scheme = Subject.uri_lookup[subject.type]
                term = subject.identifier
                weight_field = AtomFeed.schema_("ratingValue")
                key = (scheme, term)
                if not key in by_scheme_and_term:
                    value = dict(term=subject.identifier)
                    if subject.name:
                        value['label'] = subject.name
                    value[weight_field] = 0
                    by_scheme_and_term[key] = value
                by_scheme_and_term[key][weight_field] += c.weight

        # Collapse by_scheme_and_term to by_scheme
        by_scheme = defaultdict(list)
        for (scheme, term), value in list(by_scheme_and_term.items()):
            by_scheme[scheme].append(value)
        by_scheme.update(super(VerboseAnnotator, cls).categories(work))
        return by_scheme

    @classmethod
    def authors(cls, work, edition):
        """Create a detailed <author> tag for each author."""
        return [cls.detailed_author(author)
                for author in edition.author_contributors]

    @classmethod
    def detailed_author(cls, contributor):
        """Turn a Contributor into a detailed <author> tag."""
        children = []
        children.append(AtomFeed.name(contributor.display_name or ""))
        sort_name = AtomFeed.makeelement("{%s}sort_name" % AtomFeed.SIMPLIFIED_NS)
        sort_name.text = contributor.sort_name

        children.append(sort_name)

        if contributor.family_name:
            family_name = AtomFeed.makeelement(AtomFeed.schema_("family_name"))
            family_name.text = contributor.family_name
            children.append(family_name)

        if contributor.wikipedia_name:
            wikipedia_name = AtomFeed.makeelement(
                "{%s}wikipedia_name" % AtomFeed.SIMPLIFIED_NS)
            wikipedia_name.text = contributor.wikipedia_name
            children.append(wikipedia_name)

        if contributor.viaf:
            viaf_tag = AtomFeed.makeelement(AtomFeed.schema_("sameas"))
            viaf_tag.text = "http://viaf.org/viaf/%s" % contributor.viaf
            children.append(viaf_tag)

        if contributor.lc:
            lc_tag = AtomFeed.makeelement(AtomFeed.schema_("sameas"))
            lc_tag.text = "http://id.loc.gov/authorities/names/%s" % contributor.lc
            children.append(lc_tag)


        return AtomFeed.author(*children)



class AcquisitionFeed(OPDSFeed):

    FACET_REL = "http://opds-spec.org/facet"

    @classmethod
    def groups(cls, _db, title, url, worklist, annotator,
               pagination=None, facets=None, max_age=None,
               search_engine=None, search_debug=False,
               **response_kwargs
    ):
        """The acquisition feed for 'featured' items from a given lane's
        sublanes, organized into per-lane groups.

        NOTE: If the lane has no sublanes, a grouped feed will
        probably be unsatisfying. Call page() instead with an
        appropriate Facets object.

        :param pagination: A Pagination object. No single child of this lane
            will contain more than `pagination.size` items.
        :param facets: A GroupsFacet object.

        :param response_kwargs: Extra keyword arguments to pass into
            the OPDSFeedResponse constructor.

        :return: An OPDSFeedResponse containing the feed.
        """
        annotator = cls._make_annotator(annotator)
        facets = facets or FeaturedFacets.default(worklist.get_library(_db))

        def refresh():
            return cls._generate_groups(
                _db=_db, title=title, url=url, worklist=worklist,
                annotator=annotator, pagination=pagination, facets=facets,
                search_engine=search_engine, search_debug=search_debug
            )

        return CachedFeed.fetch(
            _db=_db, worklist=worklist, pagination=pagination,
            facets=facets, refresher_method=refresh, max_age=max_age,
            **response_kwargs
        )

    @classmethod
    def _generate_groups(
        cls, _db, title, url, worklist, annotator,
        pagination, facets, search_engine, search_debug
    ):
        """Internal method called by groups() when a grouped feed
        must be regenerated.
        """

        # Try to get a set of (Work, WorkList) 2-tuples
        # to make a normal grouped feed.
        works_and_lanes = [
            x for x in worklist.groups(
                _db=_db, pagination=pagination, facets=facets,
                search_engine=search_engine, debug=search_debug
            )
        ]
        # Make a typical grouped feed.
        all_works = []
        for work, sublane in works_and_lanes:
            if sublane==worklist:
                # We are looking at the groups feed for (e.g.)
                # "Science Fiction", and we're seeing a book
                # that is featured within "Science Fiction" itself
                # rather than one of the sublanes.
                #
                # We want to assign this work to a group called "All
                # Science Fiction" and point its 'group URI' to
                # the linear feed of the "Science Fiction" lane
                # (as opposed to the groups feed, which is where we
                # are now).
                v = dict(
                    lane=worklist,
                    label=worklist.display_name_for_all,
                    link_to_list_feed=True,
                )
            else:
                # We are looking at the groups feed for (e.g.)
                # "Science Fiction", and we're seeing a book
                # that is featured within one of its sublanes,
                # such as "Space Opera".
                #
                # We want to assign this work to a group derived
                # from the sublane.
                v = dict(lane=sublane)
            annotator.lanes_by_work[work].append(v)
            all_works.append(work)

        all_works = annotator.sort_works_for_groups_feed(all_works)
        feed = AcquisitionFeed(_db, title, url, all_works, annotator)

        # Regardless of whether or not the entries in feed can be
        # grouped together, we want to apply certain feed-level
        # annotations.

        # A grouped feed may link to alternate entry points into
        # the data.
        entrypoints = facets.selectable_entrypoints(worklist)
        if entrypoints:
            def make_link(ep):
                return annotator.groups_url(
                    worklist, facets=facets.navigate(entrypoint=ep)
                )
            cls.add_entrypoint_links(
                feed, make_link, entrypoints, facets.entrypoint
            )

        # A grouped feed may have breadcrumb links.
        feed.add_breadcrumb_links(worklist, facets.entrypoint)

        # Miscellaneous.
        annotator.annotate_feed(feed, worklist)

        return feed

    @classmethod
    def page(cls, _db, title, url, worklist, annotator,
             facets=None, pagination=None,
             max_age=None, search_engine=None, search_debug=False,
             **response_kwargs
    ):
        """Create a feed representing one page of works from a given lane.

        :param response_kwargs: Extra keyword arguments to pass into
            the OPDSFeedResponse constructor.

        :return: An OPDSFeedResponse containing the feed.
        """
        library = worklist.get_library(_db)
        facets = facets or Facets.default(library)
        pagination = pagination or Pagination.default()
        annotator = cls._make_annotator(annotator)

        def refresh():
            return cls._generate_page(
                _db, title, url, worklist, annotator, facets, pagination,
                search_engine, search_debug
            )

        response_kwargs.setdefault('max_age', max_age)
        return CachedFeed.fetch(
            _db, worklist=worklist, pagination=pagination, facets=facets,
            refresher_method=refresh, **response_kwargs
        )

    @classmethod
    def _generate_page(
        cls, _db, title, url, lane, annotator, facets, pagination,
        search_engine, search_debug
    ):
        """Internal method called by page() when a cached feed
        must be regenerated.
        """
        works = lane.works(
            _db, pagination=pagination, facets=facets,
            search_engine=search_engine, debug=search_debug
        )

        if not isinstance(works, list):
            # It's possible that works() returned a database query or
            # other generator-like object, but at this point we want
            # an actual list of Work objects.
            works = [x for x in works]

        if not pagination.page_has_loaded:
            # Depending on how the works were obtained,
            # Pagination.page_loaded may or may not have been called
            # yet.
            pagination.page_loaded(works)
        feed = cls(_db, title, url, works, annotator)

        entrypoints = facets.selectable_entrypoints(lane)
        if entrypoints:
            # A paginated feed may have multiple entry points into the
            # same dataset.
            def make_link(ep):
                return annotator.feed_url(
                    lane, facets=facets.navigate(entrypoint=ep)
                )
            cls.add_entrypoint_links(
                feed, make_link, entrypoints, facets.entrypoint
            )

        # Add URLs to change faceted views of the collection.
        for args in cls.facet_links(annotator, facets):
            OPDSFeed.add_link_to_feed(feed=feed.feed, **args)

        if len(works) > 0 and pagination.has_next_page:
            # There are works in this list. Add a 'next' link.
            OPDSFeed.add_link_to_feed(feed=feed.feed, rel="next", href=annotator.feed_url(lane, facets, pagination.next_page))

        if pagination.offset > 0:
            OPDSFeed.add_link_to_feed(feed=feed.feed, rel="first", href=annotator.feed_url(lane, facets, pagination.first_page))

        previous_page = pagination.previous_page
        if previous_page:
            OPDSFeed.add_link_to_feed(feed=feed.feed, rel="previous", href=annotator.feed_url(lane, facets, previous_page))

        if isinstance(facets, FacetsWithEntryPoint):
            feed.add_breadcrumb_links(lane, facets.entrypoint)

        annotator.annotate_feed(feed, lane)
        return feed

    @classmethod
    def from_query(cls, query, _db, feed_name, url, pagination, url_fn, annotator):
        """Build  a feed representing one page of a given list. Currently used for
        creating an OPDS feed for a custom list and not cached.

        TODO: This is used by the circulation manager admin interface.
        Investigate changing the code that uses this to use the search
        index -- this is inefficient and creates an alternate code path
        that may harbor bugs.

        TODO: This cannot currently return OPDSFeedResponse because the
        admin interface modifies the feed after it's generated.

        """
        page_of_works = pagination.modify_database_query(_db, query)
        pagination.total_size = int(query.count())

        feed = cls(_db, feed_name, url, page_of_works, annotator)

        if pagination.total_size > 0 and pagination.has_next_page:
            OPDSFeed.add_link_to_feed(feed=feed.feed, rel="next", href=url_fn(pagination.next_page.offset))
        if pagination.offset > 0:
            OPDSFeed.add_link_to_feed(feed=feed.feed, rel="first", href=url_fn(pagination.first_page.offset))
        if pagination.previous_page:
            OPDSFeed.add_link_to_feed(feed=feed.feed, rel="previous", href=url_fn(pagination.previous_page.offset))

        return feed

    def as_response(self, **kwargs):
        """Convert this feed into an OPDSFEedResponse."""
        return OPDSFeedResponse(self, **kwargs)

    def as_error_response(self, **kwargs):
        """Convert this feed into an OPDSFEedResponse that should be treated
        by intermediaries as an error -- that is, treated as private
        and not cached.
        """
        kwargs['max_age'] = 0
        kwargs['private'] = True
        return self.as_response(**kwargs)

    @classmethod
    def _make_annotator(cls, annotator):
        """Helper method to make sure there's some kind of Annotator."""
        if not annotator:
            annotator = Annotator
        if callable(annotator):
            annotator = annotator()
        return annotator

    @classmethod
    def facet_link(cls, href, title, facet_group_name, is_active):
        """Build a set of attributes for a facet link.

        :param href: Destination of the link.
        :param title: Human-readable description of the facet.
        :param facet_group_name: The facet group to which the facet belongs,
           e.g. "Sort By".
        :param is_active: True if this is the client's currently
           selected facet.

        :return: A dictionary of attributes, suitable for passing as
            keyword arguments into OPDSFeed.add_link_to_feed.
        """
        args = dict(href=href, title=title)
        args['rel'] = cls.FACET_REL
        args['{%s}facetGroup' % AtomFeed.OPDS_NS] = facet_group_name
        if is_active:
            args['{%s}activeFacet' % AtomFeed.OPDS_NS] = "true"
        return args

    @classmethod
    def add_entrypoint_links(cls, feed, url_generator, entrypoints,
                             selected_entrypoint, group_name='Formats'):
        """Add links to a feed forming an OPDS facet group for a set of
        EntryPoints.

        :param feed: A lxml Tag object.
        :param url_generator: A callable that returns the entry point
            URL when passed an EntryPoint.
        :param entrypoints: A list of all EntryPoints in the facet group.
        :param selected_entrypoint: The current EntryPoint, if selected.
        """
        if (len(entrypoints) == 1
            and selected_entrypoint in (None, entrypoints[0])):
            # There is only one entry point. Unless the currently
            # selected entry point is somehow different, there's no
            # need to put any links at all here -- a facet group with
            # one one facet might as well not be there.
            return

        is_default = True
        for entrypoint in entrypoints:
            link = cls._entrypoint_link(
                url_generator, entrypoint, selected_entrypoint, is_default,
                group_name
            )
            if link is not None:
                cls.add_link_to_feed(feed.feed, **link)
                is_default = False

    @classmethod
    def _entrypoint_link(
            cls, url_generator, entrypoint, selected_entrypoint,
            is_default, group_name
    ):
        """Create arguments for add_link_to_feed for a link that navigates
        between EntryPoints.
        """
        display_title = EntryPoint.DISPLAY_TITLES.get(entrypoint)
        if not display_title:
            # Shouldn't happen.
            return

        url = url_generator(entrypoint)
        is_selected = entrypoint is selected_entrypoint
        link = cls.facet_link(url, display_title, group_name, is_selected)

        # Unlike a normal facet group, every link in this facet
        # group has an additional attribute marking it as an entry
        # point.
        #
        # In OPDS 2 this can become an additional rel value,
        # removing the need for a custom attribute.
        link['{%s}facetGroupType' % AtomFeed.SIMPLIFIED_NS] = FacetConstants.ENTRY_POINT_REL
        return link

    def add_breadcrumb_links(self, lane, entrypoint=None):
        """Add information necessary to find your current place in the
        site's navigation.

        A link with rel="start" points to the start of the site

        A <simplified:entrypoint> section describes the current entry point.

        A <simplified:breadcrumbs> section contains a sequence of
        breadcrumb links.
        """
        # Add the top-level link with rel='start'
        xml = self.feed
        annotator = self.annotator
        top_level_title = annotator.top_level_title() or "Collection Home"
        self.add_link_to_feed(
            feed=xml, rel='start', href=annotator.default_lane_url(),
            title=top_level_title
        )

        # Add a link to the direct parent with rel="up".
        #
        # TODO: the 'direct parent' may be the same lane but without
        # the entry point specified. Fixing this would also be a good
        # opportunity to refactor the code for figuring out parent and
        # parent_title.
        parent = None
        if isinstance(lane, Lane):
            parent = lane.parent
        if parent and parent.display_name:
            parent_title = parent.display_name
        else:
            parent_title = top_level_title

        if parent:
            up_uri = annotator.lane_url(parent)
            self.add_link_to_feed(
                feed=xml, href=up_uri, rel="up", title=parent_title
            )
        self.add_breadcrumbs(lane, entrypoint=entrypoint)

        # Annotate the feed with a simplified:entryPoint for the
        # current EntryPoint.
        self.show_current_entrypoint(entrypoint)

    @classmethod
    def search(cls, _db, title, url, lane, search_engine, query,
               pagination=None, facets=None, annotator=None,
               **response_kwargs
    ):
        """Run a search against the given search engine and return
        the results as a Flask Response.

        :param _db: A database connection
        :param title: The title of the resulting OPDS feed.
        :param url: The URL from which the feed will be served.
        :param search_engine: An ExternalSearchIndex.
        :param query: The search query
        :param pagination: A Pagination
        :param facets: A Facets
        :param annotator: An Annotator
        :param response_kwargs: Keyword arguments to pass into the OPDSFeedResponse
            constructor.
        :return: An ODPSFeedResponse
        """
        facets = facets or SearchFacets()
        pagination = pagination or Pagination.default()
        results = lane.search(
            _db, query, search_engine, pagination=pagination, facets=facets
        )
        opds_feed = AcquisitionFeed(
            _db, title, url, results, annotator=annotator
        )
        AcquisitionFeed.add_link_to_feed(
            feed=opds_feed.feed, rel='start',
            href=annotator.default_lane_url(),
            title=annotator.top_level_title()
        )

        # A feed of search results may link to alternate entry points
        # into those results.
        entrypoints = facets.selectable_entrypoints(lane)
        if entrypoints:
            def make_link(ep):
                return annotator.search_url(
                    lane, query, pagination=None,
                    facets=facets.navigate(entrypoint=ep)
                )
            cls.add_entrypoint_links(
                opds_feed, make_link, entrypoints, facets.entrypoint
            )

        if len(results) > 0:
            # There are works in this list. Add a 'next' link.
            next_url = annotator.search_url(lane, query, pagination.next_page, facets)
            AcquisitionFeed.add_link_to_feed(feed=opds_feed.feed, rel="next", href=next_url)

        if pagination.offset > 0:
            first_url = annotator.search_url(lane, query, pagination.first_page, facets)
            AcquisitionFeed.add_link_to_feed(feed=opds_feed.feed, rel="first", href=first_url)

        previous_page = pagination.previous_page
        if previous_page:
            previous_url = annotator.search_url(lane, query, previous_page, facets)
            AcquisitionFeed.add_link_to_feed(feed=opds_feed.feed, rel="previous", href=previous_url)

        # Add "up" link.
        AcquisitionFeed.add_link_to_feed(feed=opds_feed.feed, rel="up", href=annotator.lane_url(lane), title=str(lane.display_name))

        # Add URLs to change faceted views
        for args in cls.facet_links(annotator, facets):
            # Put the query parameter into the url
            args["href"] += f"&q={query}"
            AcquisitionFeed.add_link_to_feed(feed=opds_feed.feed, **args)

        # We do not add breadcrumbs to this feed since you're not
        # technically searching the this lane; you are searching the
        # library's entire collection, using _some_ of the constraints
        # imposed by this lane (notably language and audience).

        annotator.annotate_feed(opds_feed, lane)
        return OPDSFeedResponse(response=str(opds_feed), **response_kwargs)

    @classmethod
    def single_entry(
            cls, _db, work, annotator, force_create=False, raw=False, use_cache=True, **response_kwargs
    ):
        """Create a single-entry OPDS document for one specific work.

        :param _db: A database connection.
        :param work: A Work
        :param work: An Annotator
        :param force_create: Create the OPDS entry from scratch even
            if there's already a cached one.
        :param raw: If this is False (the default), a Flask Response will be returned,
            ready to be sent over the network. Otherwise an object representing
            the underlying OPDS entry will be returned.
        :param use_cache: Boolean value determining whether the OPDS cache shall be used.
        :param response_kwargs: These keyword arguments will be passed into the Response
            constructor, if it is invoked.
        :return: A Response, if `raw` is false. Otherwise, an OPDSMessage
            or an etree._Element -- whatever was returned by
            OPDSFeed.create_entry.
        """

        feed = cls(_db, '', '', [], annotator=annotator)
        if not isinstance(work, Edition) and not work.presentation_edition:
            return None
        entry = feed.create_entry(work, even_if_no_license_pool=True,
                                  force_create=force_create, use_cache=use_cache)

        # Since this <entry> tag is going to be the root of an XML
        # document it's essential that it include an up-to-date nsmap,
        # even if it was generated from an old cached <entry> tag that
        # had an older nsmap.
        if isinstance(entry, etree._Element) and not 'drm' in entry.nsmap:
            # This workaround (creating a brand new tag) is necessary
            # because the nsmap attribute is immutable. See
            # https://bugs.launchpad.net/lxml/+bug/555602
            nsmap = entry.nsmap
            nsmap['drm'] = AtomFeed.DRM_NS
            new_root = etree.Element(entry.tag, nsmap=nsmap)
            new_root[:] = entry[:]
            entry = new_root
        if raw or entry is None:
            return entry
        if isinstance(entry, OPDSMessage):
            entry = str(entry)
            # This is probably an error message; don't cache it
            # even if it would otherwise be cached.
            response_kwargs['max_age'] = 0
            response_kwargs['private'] = True
        elif isinstance(entry, etree._Element):
            entry = etree.tostring(entry, encoding="unicode")

        # It's common for a single OPDS entry to be returned as the
        # result of an unsafe operation, so we will default to setting
        # the response as private and uncacheable.
        response_kwargs.setdefault('max_age', 0)
        response_kwargs.setdefault('private', True)

        return OPDSEntryResponse(response=entry, **response_kwargs)

    @classmethod
    def error_message(cls, identifier, error_status, error_message):
        """Turn an error result into an OPDSMessage suitable for
        adding to a feed.
        """
        return OPDSMessage(identifier.urn, error_status, error_message)

    @classmethod
    def facet_links(cls, annotator, facets):
        """Create links for this feed's navigational facet groups.

        This does not create links for the entry point facet group,
        because those links should only be present in certain
        circumstances, and this method doesn't know if those
        circumstances apply. You need to decide whether to call
        add_entrypoint_links in addition to calling this method.
        """
        for group, value, new_facets, selected in facets.facet_groups:
            url = annotator.facet_url(new_facets)
            if not url:
                continue
            group_title = Facets.GROUP_DISPLAY_TITLES.get(group)
            facet_title = Facets.FACET_DISPLAY_TITLES.get(value)
            if not (group_title and facet_title):
                # This facet group or facet, is not recognized by the
                # system. It may be left over from an earlier version,
                # or just weird junk data.
                continue
            yield cls.facet_link(
                url, str(facet_title), str(group_title), selected
            )

    def __init__(self, _db, title, url, works, annotator=None,
                 precomposed_entries=[]):
        """Turn a list of works, messages, and precomposed <opds> entries
        into a feed.
        """
        if not annotator:
            annotator = Annotator
        if callable(annotator):
            annotator = annotator()
        self.annotator = annotator

        super(AcquisitionFeed, self).__init__(title, url)

        for work in works:
            self.add_entry(work)

        # Add the precomposed entries and the messages.
        for entry in precomposed_entries:
            if isinstance(entry, OPDSMessage):
                entry = entry.tag
            self.feed.append(entry)

    def add_entry(self, work):
        """Attempt to create an OPDS <entry>. If successful, append it to
        the feed.
        """
        entry = self.create_entry(work)

        if entry is not None:
            if isinstance(entry, OPDSMessage):
                entry = entry.tag
            self.feed.append(entry)
        return entry

    def create_entry(self, work, even_if_no_license_pool=False,
                     force_create=False, use_cache=True):
        """Turn a work into an entry for an acquisition feed."""
        identifier = None
        if isinstance(work, Edition):
            active_edition = work
            identifier = active_edition.primary_identifier
            active_license_pool = None
            work = None
        else:
            active_license_pool = self.annotator.active_licensepool_for(work)
            if not work:
                # We have a license pool but no work. Most likely we don't have
                # metadata for this work yet.
                return None

            if active_license_pool:
                identifier = active_license_pool.identifier
                active_edition = active_license_pool.presentation_edition
            elif work.presentation_edition:
                active_edition = work.presentation_edition
                identifier = active_edition.primary_identifier

        # There's no reason to present a book that has no active license pool.
        if not identifier:
            logging.warning("%r HAS NO IDENTIFIER", work)
            return None

        if not active_license_pool and not even_if_no_license_pool:
            logging.warning("NO ACTIVE LICENSE POOL FOR %r", work)
            return self.error_message(
                identifier,
                403,
                "I've heard about this work but have no active licenses for it."
            )

        if not active_edition:
            logging.warning("NO ACTIVE EDITION FOR %r", active_license_pool)
            return self.error_message(
                identifier,
                403,
                "I've heard about this work but have no metadata for it."
            )

        try:
            return self._create_entry(
                work, active_license_pool, active_edition, identifier,
                force_create, use_cache
            )
        except UnfulfillableWork as e:
            logging.info(
                "Work %r is not fulfillable, refusing to create an <entry>.",
                work,
            )
            return self.error_message(
                identifier,
                403,
                "I know about this work but can offer no way of fulfilling it."
            )
        except Exception as e:
            logging.error(
                "Exception generating OPDS entry for %r", work,
                exc_info = e
            )
            return None

    def _create_entry(self, work, active_license_pool, edition,
                      identifier, force_create=False, use_cache=True):
        """Build a complete OPDS entry for the given Work.

        The OPDS entry will contain bibliographic information about
        the Work, as well as information derived from a specific
        LicensePool and Identifier associated with the Work.

        :param work: The Work whose OPDS entry the client is interested in.
        :active_license_pool: Of all the LicensePools associated with this
           Work, the client has expressed interest in this one.
        :param edition: The edition to use as the presentation edition
            when creating the entry. If this is not present, the work's
            existing presentation edition will be used.
        :identifier: Of all the Identifiers associated with this
           Work, the client has expressed interest in this one.
        :param force_create: Create this entry even if there's already
            a cached one.
        :param use_cache: If true, a newly created entry will be cached
            in the appropriate storage field of Work -- either
            simple_opds_entry or verbose_opds_entry. (NOTE: this has some
            overlap with force_create which is difficult to explain.)
        :return: An lxml Element object
        """
        xml = None
        field = self.annotator.opds_cache_field

        if field and work and not force_create and use_cache:
            xml = getattr(work, field)

        if xml:
            xml = etree.fromstring(xml)
        else:
            xml = self._make_entry_xml(work, edition)
            data = etree.tounicode(xml)
            if field and use_cache:
                setattr(work, field, data)

        # Now add the stuff specific to the selected Identifier
        # and LicensePool.
        self.annotator.annotate_work_entry(
            work, active_license_pool, edition, identifier, self, xml)

        return xml

    def _make_entry_xml(self, work, edition):
        """Create a new (incomplete) OPDS entry for the given work.

        It will be completed later, in an application-specific way,
        in annotate_work_entry().

        :param work: The Work that needs an OPDS entry.
        :param edition: The edition to use as the presentation edition
            when creating the entry.
        """
        if not work:
            return None

        if not edition:
            edition = work.presentation_edition

        # Find the .epub link
        epub_href = None
        p = None

        links = []
        cover_quality = 0
        qualities = []
        if work:
            qualities.append(("Work quality", work.quality))
        full_url = None

        thumbnail_urls, full_urls = self.annotator.cover_links(work)
        for rel, urls in (
                (Hyperlink.IMAGE, full_urls),
                (Hyperlink.THUMBNAIL_IMAGE, thumbnail_urls)):
            for url in urls:
                # TODO: This is suboptimal. We know the media types
                # associated with these URLs when they are
                # Representations, but we don't have a way to connect
                # the cover_full_url with the corresponding
                # Representation, and performance considerations make
                # it impractical to follow the object reference every
                # time.
                image_type = "image/png"
                if url.endswith(".jpeg") or url.endswith(".jpg"):
                    image_type = "image/jpeg"
                elif url.endswith(".gif"):
                    image_type = "image/gif"
                links.append(AtomFeed.link(rel=rel, href=url, type=image_type))

        content = self.annotator.content(work)
        if isinstance(content, bytes):
            content = content.decode("utf8")

        content_type = 'html'
        kw = {}
        if edition.medium:
            additional_type = Edition.medium_to_additional_type.get(
                edition.medium)
            if not additional_type:
                logging.warning("No additionalType for medium %s",
                             edition.medium)
            additional_type_field = AtomFeed.schema_("additionalType")
            kw[additional_type_field] = additional_type

        entry = AtomFeed.entry(
            AtomFeed.title(edition.title or OPDSFeed.NO_TITLE),
            **kw
        )
        if edition.subtitle:
            subtitle_tag = AtomFeed.makeelement(AtomFeed.schema_("alternativeHeadline"))
            subtitle_tag.text = edition.subtitle
            entry.append(subtitle_tag)

        author_tags = self.annotator.authors(work, edition)
        entry.extend(author_tags)

        if edition.series:
            entry.extend([self.annotator.series(edition.series, edition.series_position)])

        if content:
            entry.extend([AtomFeed.summary(content, type=content_type)])


        permanent_work_id_tag = AtomFeed.makeelement("{%s}pwid" % AtomFeed.SIMPLIFIED_NS)
        permanent_work_id_tag.text = edition.permanent_work_id
        entry.append(permanent_work_id_tag)

        entry.extend(links)

        categories_by_scheme = self.annotator.categories(work)
        category_tags = []
        for scheme, categories in list(categories_by_scheme.items()):
            for category in categories:
                if isinstance(category, (bytes, str)):
                    category = dict(term=category)
                category = dict(list(map(str, (k, v))) for k, v in list(category.items()))
                category_tag = AtomFeed.category(scheme=scheme, **category)
                category_tags.append(category_tag)
        entry.extend(category_tags)

        # print(" ID %s TITLE %s AUTHORS %s" % (tag, work.title, work.authors))
        language = edition.language_code
        if language:
            language_tag = AtomFeed.makeelement("{%s}language" % AtomFeed.DCTERMS_NS)
            language_tag.text = language
            entry.append(language_tag)

        if edition.publisher:
            publisher_tag = AtomFeed.makeelement("{%s}publisher" % AtomFeed.DCTERMS_NS)
            publisher_tag.text = edition.publisher
            entry.extend([publisher_tag])

        if edition.imprint:
            imprint_tag = AtomFeed.makeelement("{%s}publisherImprint" % AtomFeed.BIB_SCHEMA_NS)
            imprint_tag.text = edition.imprint
            entry.extend([imprint_tag])

        # Entry.issued is the date the ebook came out, as distinct
        # from Entry.published (which may refer to the print edition
        # or some original edition way back when).
        #
        # For Dublin Core 'issued' we use Entry.issued if we have it
        # and Entry.published if not. In general this means we use
        # issued date for Gutenberg and published date for other
        # sources.
        #
        # For the date the book was added to our collection we use
        # atom:published.
        #
        # Note: feedparser conflates dc:issued and atom:published, so
        # it can't be used to extract this information. However, these
        # tags are consistent with the OPDS spec.
        issued = edition.issued or edition.published
        if (isinstance(issued, datetime.datetime)
            or isinstance(issued, datetime.date)):
            now = utc_now()
            today = datetime.date.today()
            issued_already = False
            if isinstance(issued, datetime.datetime):
                issued_already = (issued <= now)
            elif isinstance(issued, datetime.date):
                issued_already = (issued <= today)
            if issued_already:
                issued_tag = AtomFeed.makeelement("{%s}issued" % AtomFeed.DCTERMS_NS)
                # Use datetime.isoformat instead of datetime.strftime because
                # strftime only works on dates after 1890, and we have works
                # that were issued much earlier than that.
                # TODO: convert to local timezone, not that it matters much.
                issued_tag.text = issued.isoformat().split('T')[0]
                entry.extend([issued_tag])

        return entry


    CURRENT_ENTRYPOINT_ATTRIBUTE = "{%s}entryPoint" % AtomFeed.SIMPLIFIED_NS

    def show_current_entrypoint(self, entrypoint):
        """Annotate this given feed with a simplified:entryPoint
        attribute pointing to the current entrypoint's TYPE_URI.

        This gives clients an overall picture of the type of works in
        the feed, and a way to distinguish between one EntryPoint
        and another.

        :param entrypoint: An EntryPoint.
        """
        if not entrypoint:
            return

        if not entrypoint.URI:
            return
        self.feed.attrib[self.CURRENT_ENTRYPOINT_ATTRIBUTE] = entrypoint.URI

    def add_breadcrumbs(self, lane, include_lane=False, entrypoint=None):
        """Add list of ancestor links in a breadcrumbs element.

        :param lane: Add breadcrumbs from up to this lane.
        :param include_lane: Include `lane` itself in the breadcrumbs.
        :param entrypoint: The currently selected entrypoint, if any.

        TODO: The switchover from "no entry point" to "entry point" needs
        its own breadcrumb link.
        """
        if entrypoint is None:
            entrypoint_query = ""
        else:
            entrypoint_query = "?entrypoint=" + entrypoint.INTERNAL_NAME

        # Breadcrumbs for lanes may be end up being cut off by a
        # patron-type-specific root lane. If so, that lane -- not the
        # site root -- should become the first breadcrumb.
        site_root_lane = None
        usable_parentage = []
        if lane is not None:
            for ancestor in [lane] + list(lane.parentage):
                if isinstance(ancestor, Lane) and ancestor.root_for_patron_type:
                    # Root lane for a specific patron type. The root is
                    # treated specially, so it should not be added to
                    # usable_parentage. Any lanes between this lane and the
                    # library root should not be included at all.
                    site_root_lane = ancestor
                    break

                if ancestor != lane or include_lane:
                    # A lane may appear in its own breadcrumbs
                    # only if include_lane is True.
                    usable_parentage.append(ancestor)

        annotator = self.annotator
        if (
            lane == site_root_lane or
            (
                site_root_lane is None and
                annotator.lane_url(lane) == annotator.default_lane_url()
            )
        ):
            # There are no extra breadcrumbs: either we are at the
            # site root, or we are at a lane that is the root for a
            # specific patron type.
            return

        # Start work on a simplified:breadcrumbs tag.
        breadcrumbs = AtomFeed.makeelement(
            "{%s}breadcrumbs" % AtomFeed.SIMPLIFIED_NS
        )

        # Add root link. This is either the link to the site root
        # or to the root lane for some patron type.
        if site_root_lane is None:
            root_url = annotator.default_lane_url()
            root_title = annotator.top_level_title()
        else:
            root_url = annotator.lane_url(site_root_lane)
            root_title = site_root_lane.display_name
        root_link = AtomFeed.link(title=root_title, href=root_url)
        breadcrumbs.append(root_link)

        # Add entrypoint selection link
        if entrypoint:
            breadcrumbs.append(
                AtomFeed.link(
                    title=entrypoint.INTERNAL_NAME,
                    href=root_url + entrypoint_query
                )
            )

        # Add links for all usable lanes between `lane` and `site_root_lane`
        # (possibly including `lane` itself).
        for ancestor in reversed(usable_parentage):
            lane_url = annotator.lane_url(ancestor)
            if lane_url == root_url:
                # Root lane for the entire site.
                break

            breadcrumbs.append(
                AtomFeed.link(
                    title=ancestor.display_name,
                    href=lane_url + entrypoint_query
                )
            )

        # Append the breadcrumbs to the feed.
        self.feed.append(breadcrumbs)

    @classmethod
    def minimal_opds_entry(cls, identifier, cover, description, quality,
        most_recent_update=None
    ):
        elements = []
        representations = []
        if cover:
            cover_representation = cover.representation
            representations.append(cover.representation)
            cover_link = AtomFeed.makeelement(
                "link", href=cover_representation.public_url,
                type=cover_representation.media_type, rel=Hyperlink.IMAGE)
            elements.append(cover_link)
            if cover_representation.thumbnails:
                thumbnail = cover_representation.thumbnails[0]
                representations.append(thumbnail)
                thumbnail_link = AtomFeed.makeelement(
                    "link", href=thumbnail.public_url,
                    type=thumbnail.media_type,
                    rel=Hyperlink.THUMBNAIL_IMAGE
                )
                elements.append(thumbnail_link)
        if description:
            content = description.representation.content
            if isinstance(content, bytes):
                content = content.decode("utf8")
            description_e = AtomFeed.summary(content, type='html')
            elements.append(description_e)
            representations.append(description.representation)

        if quality:
            elements.append(
                Annotator.rating_tag(Measurement.QUALITY, quality))

        # The update date is the most recent date any of these
        # resources were mirrored/fetched.
        potential_update_dates = [
            r.mirrored_at or r.fetched_at for r in representations
            if r.mirrored_at or r.fetched_at
        ]
        if most_recent_update:
            potential_update_dates.append(most_recent_update)

        if potential_update_dates:
            update_date = max(potential_update_dates)
            elements.append(AtomFeed.updated(AtomFeed._strftime(update_date)))
        entry = AtomFeed.entry(
            AtomFeed.id(identifier.urn),
            AtomFeed.title(OPDSFeed.NO_TITLE),
            *elements
        )
        return entry

    @classmethod
    def link(cls, rel, href, type):
        return AtomFeed.makeelement("link", type=type, rel=rel, href=href)

    @classmethod
    def acquisition_link(cls, rel, href, types, active_loan=None):
        if types:
            initial_type = types[0]
            indirect_types = types[1:]
        else:
            initial_type = None
            indirect_types = []
        link = cls.link(rel, href, initial_type)
        indirect = cls.indirect_acquisition(indirect_types)

        # In the case of LCP we have to include a patron's hashed passphrase
        # inside the acquisition link so client applications can use it to bypass authentication
        # and will not ask patrons to enter their passphrases
        # For more information please look here:
        # https://readium.org/lcp-specs/notes/lcp-key-retrieval.html#including-a-hashed-passphrase-in-an-opds-1-catalog
        if active_loan and active_loan.license_pool.collection.protocol == ExternalIntegration.LCP:
            db = Session.object_session(active_loan)
            lcp_credential_factory = LCPCredentialFactory()
            hashed_passphrase = lcp_credential_factory.get_hashed_passphrase(db, active_loan.patron)

            hashed_passphrase_element = AtomFeed.makeelement(
                "{%s}hashed_passphrase" % AtomFeed.LCP_NS)
            hashed_passphrase_element.text = hashed_passphrase

            link.append(hashed_passphrase_element)

        if indirect is not None:
            link.append(indirect)
        return link

    @classmethod
    def indirect_acquisition(cls, indirect_types):
        top_level_parent = None
        parent = None
        for t in indirect_types:
            indirect_link = AtomFeed.makeelement(
                "{%s}indirectAcquisition" % AtomFeed.OPDS_NS, type=t)
            if parent is not None:
                parent.extend([indirect_link])
            parent = indirect_link
            if top_level_parent is None:
                top_level_parent = indirect_link
        return top_level_parent

    @classmethod
    def license_tags(cls, license_pool, loan, hold):
        # Generate a list of licensing tags. These should be inserted
        # into a <link> tag.
        tags = []
        availability_tag_name = None
        suppress_since = False
        status = None
        since = None
        until = None

        if not license_pool:
            return
        default_loan_period = default_reservation_period = None
        collection = license_pool.collection
        if (loan or hold) and not license_pool.open_access:
            if loan:
                obj = loan
            elif hold:
                obj = hold
            default_loan_period = datetime.timedelta(
                collection.default_loan_period(obj.library or obj.integration_client)
            )
        if loan:
            status = 'available'
            since = loan.start
            until = loan.until(default_loan_period)
        elif hold:
            if not license_pool.open_access:
                default_reservation_period = datetime.timedelta(
                    collection.default_reservation_period
                )
            until = hold.until(default_loan_period, default_reservation_period)
            if hold.position == 0:
                status = 'ready'
                since = None
            else:
                status = 'reserved'
                since = hold.start
        elif (license_pool.open_access or license_pool.unlimited_access or license_pool.self_hosted or (
                license_pool.licenses_available > 0 and
                license_pool.licenses_owned > 0)
          ):
            status = 'available'
        else:
            status='unavailable'

        kw = dict(status=status)
        if since:
            kw['since'] = AtomFeed._strftime(since)
        if until:
            kw['until'] = AtomFeed._strftime(until)
        tag_name = "{%s}availability" % AtomFeed.OPDS_NS
        availability_tag = AtomFeed.makeelement(tag_name, **kw)
        tags.append(availability_tag)

        # Open-access pools do not need to display <opds:holds> or <opds:copies>.
        if license_pool.open_access or license_pool.unlimited_access or license_pool.self_hosted:
            return tags

        holds_kw = dict()
        total = license_pool.patrons_in_hold_queue or 0

        if hold:
            if hold.position is None:
                # This shouldn't happen, but if it does, assume we're last
                # in the list.
                position = total
            else:
                position = hold.position

            if position > 0:
                holds_kw['position'] = str(position)
            if position > total:
                # The patron's hold position appears larger than the total
                # number of holds. This happens frequently because the
                # number of holds and a given patron's hold position are
                # updated by different processes. Don't propagate this
                # appearance to the client.
                total = position
            elif position == 0 and total == 0:
                # The book is reserved for this patron but they're not
                # counted as having it on hold. This is the only case
                # where we know that the total number of holds is
                # *greater* than the hold position.
                total = 1
        holds_kw['total'] = str(total)

        holds = AtomFeed.makeelement("{%s}holds" % AtomFeed.OPDS_NS, **holds_kw)
        tags.append(holds)

        copies_kw = dict(
            total=str(license_pool.licenses_owned or 0),
            available=str(license_pool.licenses_available or 0),
        )
        copies = AtomFeed.makeelement("{%s}copies" % AtomFeed.OPDS_NS, **copies_kw)
        tags.append(copies)

        return tags

    @classmethod
    def format_types(cls, delivery_mechanism):
        """Generate a set of types suitable for passing into
        acquisition_link().
        """
        types = []
        # If this is a streaming book, you have to get an OPDS entry, then
        # get a direct link to the streaming reader from that.
        if delivery_mechanism.is_streaming:
            types.append(OPDSFeed.ENTRY_TYPE)

        # If this is a DRM-encrypted book, you have to get through the DRM
        # to get the goodies inside.
        drm = delivery_mechanism.drm_scheme_media_type
        if drm:
            types.append(drm)

        # Finally, you get the goodies.
        media = delivery_mechanism.content_type_media_type
        if media:
            types.append(media)

        return types


class LookupAcquisitionFeed(AcquisitionFeed):
    """Used when the user has requested a lookup of a specific identifier,
    which may be different from the identifier used by the Work's
    default LicensePool.
    """

    def create_entry(self, work):
        """Turn an Identifier and a Work into an entry for an acquisition
        feed.
        """
        identifier, work = work

        # Unless the client is asking for something impossible
        # (e.g. the Identifier is not really associated with the
        # Work), we should be able to use the cached OPDS entry for
        # the Work.
        if identifier.licensed_through:
            active_licensepool = identifier.licensed_through[0]
        else:
            # Use the default active LicensePool for the Work.
            active_licensepool = self.annotator.active_licensepool_for(work)

        error_status = error_message = None
        if not active_licensepool:
            error_status = 404
            error_message = "Identifier not found in collection"
        elif identifier.work != work:
            error_status = 500
            error_message = 'I tried to generate an OPDS entry for the identifier "%s" using a Work not associated with that identifier.' % identifier.urn

        if error_status:
            return self.error_message(identifier, error_status, error_message)

        if active_licensepool:
            edition = active_licensepool.presentation_edition
        else:
            edition = work.presentation_edition
        try:
            return self._create_entry(
                work, active_licensepool, edition, identifier
            )
        except UnfulfillableWork as e:
            logging.info(
                "Work %r is not fulfillable, refusing to create an <entry>.",
                work
            )
            return self.error_message(
                identifier,
                403,
                "I know about this work but can offer no way of fulfilling it."
            )

class NavigationFacets(FeaturedFacets):
    CACHED_FEED_TYPE = CachedFeed.NAVIGATION_TYPE

class NavigationFeed(OPDSFeed):

    @classmethod
    def navigation(cls, _db, title, url, worklist, annotator,
                   facets=None, max_age=None, **response_kwargs):
        """The navigation feed with links to a given lane's sublanes.

        :param response_kwargs: Extra keyword arguments to pass into
            the OPDSFeedResponse constructor.

        :return: A Response
        """

        annotator = AcquisitionFeed._make_annotator(annotator)
        facets = facets or NavigationFacets.default(worklist)

        def refresh():
            return cls._generate_navigation(
                _db, title, url, worklist, annotator
            )

        response_kwargs.setdefault('mimetype', OPDSFeed.NAVIGATION_FEED_TYPE)
        return CachedFeed.fetch(
            _db,
            worklist=worklist,
            pagination=None,
            facets=facets,
            refresher_method=refresh,
            max_age=max_age,
            **response_kwargs
        )

    @classmethod
    def _generate_navigation(cls, _db, title, url, worklist,
                             annotator):

        feed = NavigationFeed(title, url)

        if not worklist.children:
            # We can't generate links to children, since this Worklist
            # has no children, so we'll generate a link to the
            # Worklist's page-type feed instead.
            title = "All " + worklist.display_name
            page_url = annotator.feed_url(worklist)
            feed.add_entry(page_url, title, cls.ACQUISITION_FEED_TYPE)

        for child in worklist.visible_children:
            title = child.display_name
            if child.children:
                child_url = annotator.navigation_url(child)
                feed.add_entry(child_url, title, cls.NAVIGATION_FEED_TYPE)
            else:
                child_url = annotator.feed_url(child)
                feed.add_entry(child_url, title, cls.ACQUISITION_FEED_TYPE)

        annotator.annotate_feed(feed, worklist)
        return feed

    def add_entry(self, url, title, type=OPDSFeed.NAVIGATION_FEED_TYPE):
        """Create an OPDS navigation entry for a URL."""
        entry = AtomFeed.entry(
            AtomFeed.title(title))
        entry.extend([AtomFeed.id(url)])
        entry.extend([AtomFeed.link(rel="subsection", href=url, type=type)])
        self.feed.append(entry)


# Mock annotators for use in unit tests.

class TestAnnotator(Annotator):

    def __init__(self):
        self.lanes_by_work = defaultdict(list)

    @classmethod
    def lane_url(cls, lane):
        if lane and lane.has_visible_children:
            return cls.groups_url(lane)
        elif lane:
            return cls.feed_url(lane)
        else:
            return ""

    @classmethod
    def feed_url(cls, lane, facets=None, pagination=None):
        if isinstance(lane, Lane):
            base = "http://%s/" % lane.url_name
        else:
            base = "http://%s/" % lane.display_name
        sep = '?'
        if facets:
            base += sep + facets.query_string
            sep = '&'
        if pagination:
            base += sep + pagination.query_string
        return base

    @classmethod
    def search_url(cls, lane, query, pagination, facets=None):
        if isinstance(lane, Lane):
            base = "http://%s/" % lane.url_name
        else:
            base = "http://%s/" % lane.display_name
        sep = '?'
        if pagination:
            base += sep + pagination.query_string
            sep = '&'
        if facets:
            facet_query_string = facets.query_string
            if facet_query_string:
                base += sep + facet_query_string
        return base

    @classmethod
    def groups_url(cls, lane, facets=None):
        if lane and isinstance(lane, Lane):
            identifier = lane.id
        else:
            identifier = ""
        if facets:
            facet_string = '?' + facets.query_string
        else:
            facet_string = ''

        return "http://groups/%s%s" % (identifier, facet_string)

    @classmethod
    def default_lane_url(cls):
        return cls.groups_url(None)

    @classmethod
    def facet_url(cls, facets):
        return "http://facet/" + "&".join(
            ["%s=%s" % (k, v) for k, v in sorted(facets.items())]
        )

    @classmethod
    def navigation_url(cls, lane):
        if lane and isinstance(lane, Lane):
            identifier = lane.id
        else:
            identifier = ""
        return "http://navigation/%s" % identifier

    @classmethod
    def top_level_title(cls):
        return "Test Top Level Title"


class TestAnnotatorWithGroup(TestAnnotator):

    def group_uri(self, work, license_pool, identifier):
        lanes = self.lanes_by_work.get(work, None)

        if lanes:
            lane_dic = lanes.pop(0)
            lane_name = lane_dic['lane'].display_name
        else:
            lane_name = str(work.id)
        return ("http://group/%s" % lane_name,
                "Group Title for %s!" % lane_name)

    def group_uri_for_lane(self, lane):
        if lane:
            return ("http://groups/%s" % lane.display_name,
                    "Groups of %s" % lane.display_name)
        else:
            return "http://groups/", "Top-level groups"

    def top_level_title(self):
        return "Test Top Level Title"


class TestUnfulfillableAnnotator(TestAnnotator):
    """Raise an UnfulfillableWork exception when asked to annotate an entry."""

    def annotate_work_entry(self, *args, **kwargs):
        raise UnfulfillableWork()
