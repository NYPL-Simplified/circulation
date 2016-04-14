from collections import (
    defaultdict,
    Counter,
)
from urlparse import urlparse, urljoin
import copy
import datetime
import feedparser
import logging
import md5
import os
import random
import re
import site
import sys
import time
import urllib

from nose.tools import set_trace

from sqlalchemy.orm.query import Query
from sqlalchemy.sql.expression import func
from sqlalchemy.orm.session import Session

import requests

from lxml import builder, etree

from config import Configuration
from classifier import Classifier
from model import (
    BaseMaterializedWork,
    CachedFeed,
    CustomList,
    CustomListEntry,
    DataSource,
    Hyperlink,
    Resource,
    Identifier,
    Edition,
    Measurement,
    Subject,
    Work,
    )
from lane import (
    Facets,
    Lane,
    Pagination,
)
from util.cdn import cdnify

ATOM_NAMESPACE = atom_ns = 'http://www.w3.org/2005/Atom'
app_ns = 'http://www.w3.org/2007/app'
bibframe_ns = 'http://bibframe.org/vocab/'
xhtml_ns = 'http://www.w3.org/1999/xhtml'
dcterms_ns = 'http://purl.org/dc/terms/'
opds_ns = 'http://opds-spec.org/2010/catalog'
schema_ns = 'http://schema.org/'

# This is a placeholder namespace for stuff we've invented.
simplified_ns = 'http://librarysimplified.org/terms/'


nsmap = {
    None: atom_ns,
    'app': app_ns,
    'dcterms' : dcterms_ns,
    'opds' : opds_ns,
    'schema' : schema_ns,
    'simplified' : simplified_ns,
    'bibframe' : bibframe_ns,
}

def _strftime(d):
    """
Format a date the way Atom likes it (RFC3339?)
"""
    return d.strftime(AtomFeed.TIME_FORMAT)

default_typemap = {datetime: lambda e, v: _strftime(v)}

E = builder.ElementMaker(typemap=default_typemap, nsmap=nsmap)
SCHEMA = builder.ElementMaker(
    typemap=default_typemap, nsmap=nsmap, namespace="http://schema.org/")


class Annotator(object):
    """The Annotator knows how to present an OPDS feed in a specific
    application context.
    """

    opds_cache_field = Work.simple_opds_entry.name

    @classmethod
    def annotate_work_entry(cls, work, license_pool, edition, identifier, feed,
                            entry):
        """Make any custom modifications necessary to integrate this
        OPDS entry into the application's workflow.
        """
        pass

    @classmethod
    def annotate_feed(cls, feed, lane):
        """Make any custom modifications necessary to integrate this
        OPDS feed into the application's workflow.
        """
        pass

    @classmethod
    def group_uri(cls, work, license_pool, identifier):
        return None, ""

    @classmethod
    def rating_tag(cls, type_uri, value):
        """Generate a schema:Rating tag for the given type and value."""
        rating_tag = E._makeelement("{%s}Rating" % schema_ns)
        value_key = '{%s}ratingValue' % schema_ns
        rating_tag.set(value_key, "%.4f" % value)
        if type_uri:
            type_key = '{%s}additionalType' % schema_ns
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
        cdn_host = Configuration.cdn_host(Configuration.CDN_BOOK_COVERS)
        if work:
            if work.cover_thumbnail_url:
                thumb = work.cover_thumbnail_url
                old_thumb = thumb
                thumbnails = [cdnify(thumb, cdn_host)]

            if work.cover_full_url:
                full = work.cover_full_url
                full = [cdnify(full, cdn_host)]
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
        simplified_genres = []
        for wg in work.work_genres:
            simplified_genres.append(wg.genre.name)
        if not simplified_genres:
            sole_genre = None
            if work.fiction == True:
                sole_genre = 'Fiction'
            elif work.fiction == False:
                sole_genre = 'Nonfiction'
            if sole_genre:
                simplified_genres.append(sole_genre)

        categories = {}
        if simplified_genres:
            categories[Subject.SIMPLIFIED_GENRE] = [
                dict(term=Subject.SIMPLIFIED_GENRE + urllib.quote(x),
                     label=x)
                for x in simplified_genres
            ]

        # Add the appeals as a category of schema
        # http://librarysimplified.org/terms/appeal
        schema_url = simplified_ns + "appeals/"
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
                weight_field = "{%s}ratingValue" % schema_ns
                appeal[weight_field] = value
                appeals.append(appeal)

        # Add the audience as a category of schema
        # http://schema.org/audience
        if work.audience:
            audience_uri = schema_ns + "audience"
            categories[audience_uri] = [
                dict(term=work.audience, label=work.audience)
            ]

        if work.target_age:
            uri = Subject.uri_lookup[Subject.AGE_RANGE]
            target_age = work.target_age_string
            if target_age:
                categories[uri] = [dict(term=target_age, label=target_age)]

        return categories

    @classmethod
    def authors(cls, work, license_pool, edition, identifier):
        """Create one or more <author> tags for the given work."""
        return [E.author(E.name(edition.author or ""))]

    @classmethod
    def content(cls, work):
        """Return an HTML summary of this work."""
        summary = ""
        if work: 
            if work.summary_text:
                summary = work.summary_text
            elif work.summary:
                work.summary_text = work.summary.content
                summary = work.summary_text
        return summary

    @classmethod
    def lane_id(cls, lane):
        return cls.featured_feed_url(lane)
        # return "tag:%s" % (lane.name)

    @classmethod
    def work_id(cls, work):
        return work.primary_edition.primary_identifier.urn

    @classmethod
    def permalink_for(cls, work, license_pool, identifier):
        """In the absence of any specific URLs, the best we can do
        is a URN.
        """
        return identifier.urn

    @classmethod
    def feed_url(cls, lane, facets, pagination):
        raise NotImplementedError()

    @classmethod
    def groups_url(cls, lane):
        raise NotImplementedError()

    @classmethod
    def search_url(cls, lane, query, pagination):
        raise NotImplementedError()

    @classmethod
    def default_lane_url(cls):
        raise NotImplementedError()

    @classmethod
    def featured_feed_url(cls, lane, order=None):
        raise NotImplementedError()

    @classmethod
    def facet_url(cls, facets):
        return None

    @classmethod
    def active_licensepool_for(cls, work):
        """Which license pool would be/has been used to issue a license for
        this work?
        """
        open_access_license_pool = None
        active_license_pool = None

        if not work:
            return None

        if isinstance(work, BaseMaterializedWork):
            # Active license pool is preloaded from database.
            return work.license_pool
            
        if work.has_open_access_license:
            # All licenses are issued from the license pool associated with
            # the work's primary edition.
            edition = work.primary_edition

            if edition and edition.license_pool and edition.open_access_download_url and edition.title:
                # Looks good.
                open_access_license_pool = edition.license_pool

        if not open_access_license_pool:
            # The active license pool is the one that *would* be
            # associated with a loan, were a loan to be issued right
            # now.
            for p in work.license_pools:
                edition = p.edition
                if p.open_access:
                    # Make sure there's a usable link--it might be
                    # audio-only or something.
                    if edition and edition.open_access_download_url:
                        open_access_license_pool = p
                elif edition and edition.title:
                    # TODO: It's OK to have a non-open-access license pool,
                    # but the pool needs to have copies available.
                    active_license_pool = p
                    break
        if not active_license_pool:
            active_license_pool = open_access_license_pool
        return active_license_pool


class VerboseAnnotator(Annotator):
    """The default Annotator for machine-to-machine integration.

    This Annotator describes all categories and authors for the book
    in great detail.
    """

    opds_cache_field = Work.verbose_opds_entry.name

    @classmethod
    def annotate_work_entry(cls, work, license_pool, edition, identifier, feed,
                            entry):
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
    def categories(cls, work):
        """Send out _all_ categories for the work.

        (So long as the category type has a URI associated with it in
        Subject.uri_lookup.)
        """
        _db = Session.object_session(work)
        by_scheme_and_term = dict()
        identifier_ids = work.all_identifier_ids()
        classifications = Identifier.classifications_for_identifier_ids(
            _db, identifier_ids)
        for c in classifications:
            subject = c.subject
            if subject.type in Subject.uri_lookup:
                scheme = Subject.uri_lookup[subject.type]
                term = subject.identifier
                weight_field = "{%s}ratingValue" % schema_ns
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
        for (scheme, term), value in by_scheme_and_term.items():
            by_scheme[scheme].append(value)
        by_scheme.update(super(VerboseAnnotator, cls).categories(work))
        return by_scheme

    @classmethod
    def authors(cls, work, license_pool, edition, identifier):
        """Create a detailed <author> tag for each author."""
        return [cls.detailed_author(author)
                for author in edition.author_contributors]

    @classmethod
    def detailed_author(cls, contributor):
        """Turn a Contributor into a detailed <author> tag."""
        children = []
        children.append(E.name(contributor.display_name or ""))
        sort_name = E._makeelement("{%s}sort_name" % simplified_ns)
        sort_name.text = contributor.name

        children.append(sort_name)

        if contributor.family_name:
            family_name = E._makeelement("{%s}family_name" % schema_ns)
            family_name.text = contributor.family_name
            children.append(family_name)

        if contributor.wikipedia_name:
            wikipedia_name = E._makeelement(
                "{%s}wikipedia_name" % simplified_ns)
            wikipedia_name.text = contributor.wikipedia_name
            children.append(wikipedia_name)

        if contributor.viaf:
            viaf_tag = E._makeelement("{%s}sameas" % schema_ns)
            viaf_tag.text = "http://viaf.org/viaf/%s" % contributor.viaf
            children.append(viaf_tag)

        if contributor.lc:
            lc_tag = E._makeelement("{%s}sameas" % schema_ns)
            lc_tag.text = "http://id.loc.gov/authorities/names/%s" % contributor.lc
            children.append(lc_tag)


        return E.author(*children)


class AtomFeed(object):

    TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ%z'

    def __init__(self, title, url):
        self.feed = E.feed(
            E.id(url),
            E.title(title),
            E.updated(_strftime(datetime.datetime.utcnow())),
            E.link(href=url, rel="self"),
        )

    def add_link(self, children=None, **kwargs):
        link = E.link(**kwargs)
        self.feed.append(link)
        if children:
            for i in children:
                link.append(i)

    def add_link_to_entry(self, entry, children=None, **kwargs):
        link = E.link(**kwargs)
        entry.append(link)
        if children:
            for i in children:
                link.append(i)

    def __unicode__(self):
        return etree.tostring(self.feed, pretty_print=True)

class OPDSFeed(AtomFeed):

    ACQUISITION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=acquisition"
    NAVIGATION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=navigation"
    ENTRY_TYPE = "application/atom+xml;type=entry;profile=opds-catalog"

    GROUP_REL = "collection"
    FEATURED_REL = "http://opds-spec.org/featured"
    RECOMMENDED_REL = "http://opds-spec.org/recommended"
    POPULAR_REL = "http://opds-spec.org/sort/popular"
    OPEN_ACCESS_REL = "http://opds-spec.org/acquisition/open-access"
    ACQUISITION_REL = "http://opds-spec.org/acquisition"
    BORROW_REL = "http://opds-spec.org/acquisition/borrow"
    FULL_IMAGE_REL = "http://opds-spec.org/image" 
    EPUB_MEDIA_TYPE = "application/epub+zip"

    REVOKE_LOAN_REL = "http://librarysimplified.org/terms/rel/revoke"

    FEED_CACHE_TIME = int(Configuration.get('default_feed_cache_time', 600))

    NO_TITLE = "http://librarysimplified.org/terms/problem/no-title"

    def __init__(self, title, url, annotator):
        if not annotator:
            annotator = Annotator()
        self.annotator = annotator
        super(OPDSFeed, self).__init__(title, url)

class AcquisitionFeed(OPDSFeed):

    FACET_REL = "http://opds-spec.org/facet"

    @classmethod
    def groups(cls, _db, title, url, lane, annotator, 
               force_refresh=False,
               use_materialized_works=True):
        """The acquisition feed for 'featured' items from a given lane's
        sublanes, organized into per-lane groups.
        """
        # Find or create a CachedFeed.
        cached, usable = CachedFeed.fetch(
            _db,
            lane=lane, 
            type=CachedFeed.GROUPS_TYPE, 
            facets=None,
            pagination=None,
            annotator=annotator,
            force_refresh=force_refresh
        )
        if usable:
            return cached

        feed_size = Configuration.featured_lane_size()
       
        # This is a list rather than a dict because we want to 
        # preserve the ordering of the lanes.
        works_and_lanes = []

        def get_visible_sublanes(lane):
            visible_sublanes = []
            for sublane in lane.sublanes:
                if not sublane.invisible:
                    visible_sublanes.append(sublane)
                else:
                    visible_sublanes += get_visible_sublanes(sublane)
            return visible_sublanes

        for sublane in get_visible_sublanes(lane):
            # .featured_works will try more and more desperately
            # to find works to fill the 'featured' group.
            works = sublane.featured_works(
                feed_size, use_materialized_works=use_materialized_works)
            if not works or len(works) < (feed_size-5):
                # This is pathetic. Every single book in this
                # lane won't fill up the 'featured' group. Don't
                # show the lane at all.
                pass
            else:
                for work in works:
                    works_and_lanes.append((work, sublane))

        if not works_and_lanes:
            # We did not find any works whatsoever. The groups feed is
            # useless. Instead we need to display a flat feed--the
            # contents of what would have been the 'all' feed.
            if not isinstance(lane, Lane):
                # This is probably a top-level controller or
                # application object.  Create a dummy lane that
                # contains everything.
                lane = Lane(_db, "Everything")
            cached = cls.page(
                _db, title, url, lane, annotator, 
                force_refresh=force_refresh,
                use_materialized_works=use_materialized_works
            )

            # The feed was generated as a page-type feed. 
            # File it as a groups-type feed so it will show up when
            # a client asks for the feed.
            cached.type = CachedFeed.GROUPS_TYPE
            return cached

        if lane.include_all_feed:
            # Create an 'all' group so that patrons can browse every
            # book in this lane.
            works = lane.featured_works(feed_size)
            for work in works:
                works_and_lanes.append((work, None))

        all_works = []
        for work, sublane in works_and_lanes:
            if sublane is None:
                # This work is in the (e.g.) 'All Science Fiction'
                # group. Whether or not this lane has sublanes,
                # the group URI will point to a linear feed, not a
                # groups feed.
                v = dict(
                    lane=lane,
                    label='All ' + lane.display_name,
                    link_to_list_feed=True,
                )
            else:
                v = dict(
                    lane=sublane
                )
            annotator.lanes_by_work[work].append(v)
            all_works.append(work)

        feed = AcquisitionFeed(
            _db, title, url, all_works, annotator,
        )

        # Render a 'start' link and an 'up' link.
        top_level_title = "Collection Home"
        start_uri = annotator.groups_url(None)
        feed.add_link(href=start_uri, rel="start", title=top_level_title)

        if lane.parent:
            parent = lane.parent
            if isinstance(parent, Lane):
                title = lane.parent.display_name
            else:
                title = top_level_title
            up_uri = annotator.groups_url(lane.parent)
            feed.add_link(href=up_uri, rel="up", title=title)

        annotator.annotate_feed(feed, lane)

        content = unicode(feed)
        cached.update(content)
        return cached

    @classmethod
    def page(cls, _db, title, url, lane, annotator=None,
             facets=None, pagination=None, 
             force_refresh=False,
             use_materialized_works=True
    ):
        """Create a feed representing one page of works from a given lane."""
        facets = facets or Facets.default()
        pagination = pagination or Pagination.default()

        # Find or create a CachedFeed.
        cached, usable = CachedFeed.fetch(
            _db,
            lane=lane, 
            type=CachedFeed.PAGE_TYPE, 
            facets=facets, 
            pagination=pagination, 
            annotator=annotator,
            force_refresh=force_refresh
        )
        if usable:
            return cached

        if use_materialized_works:
            works_q = lane.materialized_works(facets, pagination)
        else:
            works_q = lane.works(facets, pagination)
        works = works_q.all()

        feed = cls(_db, title, url, works, annotator)

        # Add URLs to change faceted views of the collection.
        for args in cls.facet_links(annotator, facets):
            feed.add_link(**args)

        if len(works) > 0:
            # There are works in this list. Add a 'next' link.
            feed.add_link(rel="next", href=annotator.feed_url(lane, facets, pagination.next_page))

        if pagination.offset > 0:
            feed.add_link(rel="first", href=annotator.feed_url(lane, facets, pagination.first_page))

        previous_page = pagination.previous_page
        if previous_page:
            feed.add_link(rel="previous", href=annotator.feed_url(lane, facets, previous_page))

        if lane.parent:
            feed.add_link(rel='up', href=annotator.groups_url(lane.parent))
        feed.add_link(rel='start', href=annotator.default_lane_url())

        annotator.annotate_feed(feed, lane)

        content = unicode(feed)
        cached.update(content)
        return cached

    @classmethod
    def search(cls, _db, title, url, lane, search_engine, query, pagination=None,
               annotator=None
    ):
        if not isinstance(lane, Lane):
            search_lane = Lane(
                _db, "Everything", searchable=True, fiction=Lane.BOTH_FICTION_AND_NONFICTION)
        else:
            search_lane = lane

        results = search_lane.search(query, search_engine, pagination=pagination)
        opds_feed = AcquisitionFeed(_db, title, url, results, annotator=annotator)
        opds_feed.add_link(rel='start', href=annotator.default_lane_url())

        if len(results) > 0:
            # There are works in this list. Add a 'next' link.
            opds_feed.add_link(rel="next", href=annotator.search_url(lane, query, pagination.next_page))

        if pagination.offset > 0:
            opds_feed.add_link(rel="first", href=annotator.search_url(lane, query, pagination.first_page))

        previous_page = pagination.previous_page
        if previous_page:
            opds_feed.add_link(rel="previous", href=annotator.search_url(lane, query, previous_page))

        annotator.annotate_feed(opds_feed, lane)
        return unicode(opds_feed)

    @classmethod
    def single_entry(cls, _db, work, annotator, force_create=False):
        """Create a single-entry feed for one specific work."""
        feed = cls(_db, '', '', [], annotator=annotator)
        if not isinstance(work, Edition) and not work.primary_edition:
            return None
        return feed.create_entry(work, None, even_if_no_license_pool=True,
                                 force_create=force_create)

    @classmethod
    def render_messages(cls, messages_by_urn):
        """Create minimal OPDS entries for custom messages."""
        for urn, (status, message) in messages_by_urn.items():
            entry = E.entry(
                E.id(urn)
            )
            status_tag = E._makeelement("{%s}status_code" % simplified_ns)
            status_tag.text = str(status)
            entry.append(status_tag)

            message_tag = E._makeelement("{%s}message" % simplified_ns)
            message_tag.text = message
            entry.append(message_tag)
            yield entry

    @classmethod
    def facet_links(self, annotator, facets):
        for group, value, new_facets, selected, in facets.facet_groups:
            url = annotator.facet_url(new_facets)
            if not url:
                continue
            group_title = Facets.GROUP_DISPLAY_TITLES[group]
            facet_title = Facets.FACET_DISPLAY_TITLES[value]
            link = dict(href=url, title=facet_title)
            link['rel'] = self.FACET_REL
            link['{%s}facetGroup' % opds_ns] = group_title
            if selected:
                link['{%s}activeFacet' % opds_ns] = "true"
            yield link

    def __init__(self, _db, title, url, works, annotator=None,
                 messages_by_urn={}, precomposed_entries=[]):
        """Turn a list of works, messages, and precomposed <opds> entries
        into a feed.
        """
        super(AcquisitionFeed, self).__init__(title, url, annotator)

        # Add minimal entries for the messages.
        for entry in self.render_messages(messages_by_urn):
            self.feed.append(entry)

        lane_link = dict(rel="collection", href=url)
        for work in works:
            self.add_entry(work, lane_link)

        # Add the precomposed entries.
        for entry in precomposed_entries:
            self.feed.append(entry)

    def add_entry(self, work, lane_link):
        """Attempt to create an OPDS <entry>. If successful, append it to
        the feed.
        """
        entry = self.create_entry(work, lane_link)
        if entry is not None:
            self.feed.append(entry)
        return entry

    def create_entry(self, work, lane_link, even_if_no_license_pool=False,
                     force_create=False):
        """Turn a work into an entry for an acquisition feed."""
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

            if isinstance(work, BaseMaterializedWork):
                identifier = work.identifier
                active_edition = None
            elif active_license_pool:        
                identifier = active_license_pool.identifier
                active_edition = active_license_pool.edition
            else:
                active_edition = work.primary_edition
                identifier = active_edition.primary_identifier

        # There's no reason to present a book that has no active license pool.
        if not active_license_pool and not even_if_no_license_pool:
            logging.warn("NO ACTIVE LICENSE POOL FOR %r", work)
            return None

        if not active_edition and not isinstance(work, BaseMaterializedWork):
            logging.warn("NO ACTIVE EDITION FOR %r", active_license_pool)
            return None

        return self._create_entry(work, active_license_pool, active_edition,
                                  identifier,
                                  lane_link, force_create)

    def _create_entry(self, work, license_pool, edition, identifier, lane_link,
                      force_create=False):

        xml = None
        cache_hit = False
        field = self.annotator.opds_cache_field
        if field and work and not force_create:
            xml = getattr(work, field)

        if xml:
            cache_hit = True
            xml = etree.fromstring(xml)
        else:
            if isinstance(work, BaseMaterializedWork):
                raise Exception(
                    "Cannot build an OPDS entry for a MaterializedWork.")
            xml = self._make_entry_xml(
                work, license_pool, edition, identifier, lane_link)
            data = etree.tostring(xml)
            if field:
                setattr(work, field, data)

        self.annotator.annotate_work_entry(
            work, license_pool, edition, identifier, self, xml)

        group_uri, group_title = self.annotator.group_uri(
            work, license_pool, identifier)
        if group_uri:
            self.add_link_to_entry(
                xml, rel=OPDSFeed.GROUP_REL, href=group_uri,
                title=group_title)

        if edition:
            title = (edition.title or "") + " "
        else:
            title = ""
        return xml

    def _make_entry_xml(self, work, license_pool, edition, identifier,
                        lane_link):

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
                image_type = "image/png"
                if url.endswith(".jpeg") or url.endswith(".jpg"):
                    image_type = "image/jpeg"
                elif url.endswith(".gif"):
                    image_type = "image/gif"
                links.append(E.link(rel=rel, href=url, type=image_type))
           

        permalink = self.annotator.permalink_for(work, license_pool, identifier)
        content = self.annotator.content(work)
        if isinstance(content, str):
            content = content.decode("utf8")

        content_type = 'html'

        kw = {}
        if edition.medium:
            additional_type = Edition.medium_to_additional_type.get(
                edition.medium)
            if not additional_type:
                logging.warn("No additionalType for medium %s",
                             edition.medium)
            additional_type_field = "{%s}additionalType" % schema_ns
            kw[additional_type_field] = additional_type

        entry = E.entry(
            E.id(permalink),
            E.title(edition.title or OPDSFeed.NO_TITLE),
            **kw
        )
        if edition.subtitle:
            entry.extend([E.alternativeHeadline(edition.subtitle)])

        if license_pool:
            provider_name_attr = "{%s}ProviderName" % bibframe_ns
            kwargs = {provider_name_attr : license_pool.data_source.name}
            data_source_tag = E._makeelement(
                "{%s}distribution" % bibframe_ns,
                **kwargs
            )
            entry.extend([data_source_tag])

        author_tags = self.annotator.authors(work, license_pool, edition, identifier)
        entry.extend(author_tags)

        if content:
            entry.extend([E.summary(content, type=content_type)])

        entry.extend([
            E.updated(_strftime(datetime.datetime.utcnow())),
        ])

        permanent_work_id_tag = E._makeelement("{%s}pwid" % simplified_ns)
        permanent_work_id_tag.text = edition.permanent_work_id
        entry.append(permanent_work_id_tag)

        entry.extend(links)

        categories_by_scheme = self.annotator.categories(work)
        category_tags = []
        for scheme, categories in categories_by_scheme.items():
            for category in categories:
                if isinstance(category, basestring):
                    category = dict(term=category)
                category = dict(map(unicode, (k, v)) for k, v in category.items())
                category_tag = E.category(scheme=scheme, **category)
                category_tags.append(category_tag)
        entry.extend(category_tags)

        # print " ID %s TITLE %s AUTHORS %s" % (tag, work.title, work.authors)
        language = edition.language_code
        if language:
            language_tag = E._makeelement("{%s}language" % dcterms_ns)
            language_tag.text = language
            entry.append(language_tag)

        if edition.publisher:
            publisher_tag = E._makeelement("{%s}publisher" % dcterms_ns)
            publisher_tag.text = edition.publisher
            entry.extend([publisher_tag])

        # We use Atom 'published' for the date the book first became
        # available to people using this application.
        now = datetime.datetime.utcnow()
        today = datetime.date.today()
        if license_pool and license_pool.availability_time:
            avail = license_pool.availability_time
            if isinstance(avail, datetime.datetime):
                avail = avail.date()
            if avail <= today:
                availability_tag = E._makeelement("published")
                # TODO: convert to local timezone.
                availability_tag.text = _strftime(license_pool.availability_time)
                entry.extend([availability_tag])

        # Entry.issued is the date the ebook came out, as distinct
        # from Entry.published (which may refer to the print edition
        # or some original edition way back when).
        #
        # For Dublin Core 'created' we use Entry.issued if we have it
        # and Entry.published if not. In general this means we use
        # issued date for Gutenberg and published date for other
        # sources.
        #
        # We use dc:created instead of dc:issued because dc:issued is
        # commonly conflated with atom:published.
        #
        # For the date the book was added to our collection we use
        # atom:published.
        issued = edition.issued or edition.published
        if (isinstance(issued, datetime.datetime) 
            or isinstance(issued, datetime.date)):
            issued_already = False
            if isinstance(issued, datetime.datetime):
                issued_already = (issued <= now)
            elif isinstance(issued, datetime.date):
                issued_already = (issued <= today)
            if issued_already:
                issued_tag = E._makeelement("{%s}created" % dcterms_ns)
                # TODO: convert to local timezone, not that it matters much.
                issued_tag.text = issued.strftime("%Y-%m-%d")
                entry.extend([issued_tag])

        return entry

    @classmethod
    def minimal_opds_entry(cls, identifier, cover, description, quality):
        elements = []
        representations = []
        most_recent_update = None
        if cover:
            cover_representation = cover.representation
            representations.append(cover.representation)
            cover_link = E._makeelement(
                "link", href=cover_representation.mirror_url,
                type=cover_representation.media_type, rel=Hyperlink.IMAGE)
            elements.append(cover_link)
            if cover_representation.thumbnails:
                thumbnail = cover_representation.thumbnails[0]
                representations.append(thumbnail)
                thumbnail_link = E._makeelement(
                    "link", href=thumbnail.mirror_url,
                    type=thumbnail.media_type,
                    rel=Hyperlink.THUMBNAIL_IMAGE
                )
                elements.append(thumbnail_link)
        if description:
            content = description.representation.content
            if isinstance(content, str):
                content = content.decode("utf8")
            description_e = E.summary(content, type='html')
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
        
        if potential_update_dates:
            update_date = max(potential_update_dates)
            elements.append(E.updated(_strftime(update_date)))
        entry = E.entry(
            E.id(identifier.urn),
            E.title(OPDSFeed.NO_TITLE),
            *elements
        )
        return entry

    @classmethod
    def link(cls, rel, href, type):
        return E._makeelement("link", type=type, rel=rel, href=href)

    @classmethod
    def acquisition_link(cls, rel, href, types):
        if types:            
            initial_type = types[0]
            indirect_types = types[1:]
        else:
            initial_type = None
            indirect_types = []
        link = cls.link(rel, href, initial_type)
        indirect = cls.indirect_acquisition(indirect_types)
        if indirect is not None:
            link.append(indirect)
        return link

    @classmethod
    def indirect_acquisition(cls, indirect_types):
        top_level_parent = None
        parent = None
        for t in indirect_types:
            indirect_link = E._makeelement(
                "{%s}indirectAcquisition" % opds_ns, type=t)
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

        if license_pool.open_access:
            default_loan_period = default_reservation_period = None
        else:
            ds = license_pool.data_source
            default_loan_period = ds.default_loan_period
            default_reservation_period = ds.default_reservation_period
        if loan:
            status = 'available'
            since = loan.start
            until = loan.until(default_loan_period)
        elif hold:
            until = hold.until(default_loan_period, default_reservation_period)
            if hold.position == 0:
                status = 'ready'
                since = None
            else:
                status = 'reserved'
                since = hold.start
        elif (license_pool.open_access or (
                license_pool.licenses_available > 0 and
                license_pool.licenses_owned > 0)
          ):
            status = 'available'
        else:
            status='unavailable'

        kw = dict(status=status)
        if since:
            kw['since'] = _strftime(since)
        if until:
            kw['until'] = _strftime(until)
        tag_name = "{%s}availability" % opds_ns
        availability_tag = E._makeelement(tag_name, **kw)
        tags.append(availability_tag)

        # Open-access pools do not need to display <opds:holds> or <opds:copies>.
        if license_pool.open_access:
            return tags


        holds_kw = dict(total=str(license_pool.patrons_in_hold_queue or 0))
        if hold and hold.position:
            holds_kw['position'] = str(hold.position)
        holds = E._makeelement("{%s}holds" % opds_ns, **holds_kw)
        tags.append(holds)

        copies_kw = dict(
            total=str(license_pool.licenses_owned or 0),
            available=str(license_pool.licenses_available or 0),
        )
        copies = E._makeelement("{%s}copies" % opds_ns, **copies_kw)
        tags.append(copies)

        return tags

    @classmethod
    def format_types(cls, delivery_mechanism):
        """Generate a set of types suitable for passing into
        acquisition_link().
        """
        types = []
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

    """Used when the work's primary identifier may be different
    from the identifier we should use in the feed.
    """

    def create_entry(self, work, lane_link):
        """Turn a work into an entry for an acquisition feed."""
        identifier, work = work
        active_license_pool = self.annotator.active_licensepool_for(work)
        # There's no reason to present a book that has no active license pool.
        if not active_license_pool:
            return None

        active_edition = active_license_pool.edition
        return self._create_entry(
            work, active_license_pool, work.primary_edition, 
            identifier, lane_link)
