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
    CustomList,
    CustomListEntry,
    CustomListFeed,
    DataSource,
    Hyperlink,
    Resource,
    Identifier,
    Edition,
    Measurement,
    Subject,
    Work,
    )
from util.cdn import cdnify

ATOM_NAMESPACE = atom_ns = 'http://www.w3.org/2005/Atom'
app_ns = 'http://www.w3.org/2007/app'
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
                if old_thumb != thumbnails[0]:
                    logging.debug(
                        "Thumbnail URL changed %s => %s",
                        old_thumb, thumbnails[0]
                    )

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
            target_age = str(work.target_age)
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
    def navigation_feed_url(cls, lane, order=None):
        raise NotImplementedError()

    @classmethod
    def featured_feed_url(cls, lane, order=None):
        raise NotImplementedError()

    @classmethod
    def facet_url(cls, order):
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

    def add_link(self, **kwargs):
        self.feed.append(E.link(**kwargs))

    def add_link_to_entry(self, entry, **kwargs):
        entry.append(E.link(**kwargs))

    def __unicode__(self):
        return etree.tostring(self.feed, pretty_print=True)

class OPDSFeed(AtomFeed):

    ACQUISITION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=acquisition"
    NAVIGATION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=navigation"

    GROUP_REL = "http://opds-spec.org/group"
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

    # The languages in which we have best-seller and staff picks information
    BEST_SELLER_LANGUAGES = ['eng']
    STAFF_PICKS_LANGUAGES = ['eng']

    @classmethod
    def featured(cls, languages, lane, annotator, quality_cutoff=0.3,
                 availability=Work.CURRENTLY_AVAILABLE, random_sample=True):
        """The acquisition feed for 'featured' items from a given lane.
        """
        url = annotator.featured_feed_url(lane)
        feed_size = 20
        works = lane.quality_sample(languages, 0.65, quality_cutoff, feed_size,
                                    availability, random_sample)
        return AcquisitionFeed(
            lane._db, "%s: featured" % lane.display_name, url, works, annotator, 
            sublanes=lane.sublanes)

    @classmethod
    def featured_groups(
            cls, url, best_sellers_url, staff_picks_url, languages, lane,
            annotator, quality_cutoff=0.0):
        """The acquisition feed for 'featured' items from a given lane's
        sublanes, organized into per-lane groups.
        """
        feed_size = 20
        _db = None
        all_works = []
        if isinstance(languages, basestring):
            languages = [languages]

        sublanes = list(lane.sublanes)
        if lane and lane.sublanes and lane.display_name:
            # Every lane that has sublanes also gets an 'all' group,
            # except the very top level.
            logging.info(
                "I intend to create an 'all' group for %s" % lane.name,
            )
            sublanes.append(lane)
            all_group_label = 'All ' + lane.display_name
        else:
            all_group_label = None

        for l in sublanes:
            if not _db:
                _db = l._db

            quality_min = 0.65

            works = l.quality_sample(
                languages, quality_min, quality_cutoff, feed_size,
                Work.CURRENTLY_AVAILABLE)

            for work in works:
                if l == lane:
                    # This work is in the (e.g.) 'All Science Fiction'
                    # group. Whether or not this lane has sublanes,
                    # the group URI will point to a linear feed, not a
                    # groups feed.
                    v = dict(
                        lane=lane,
                        label=all_group_label,
                        link_to_list_feed=True,
                    )
                else:
                    v = l                   
                annotator.lane_by_work[work] = v
                all_works.append(work)

        if lane.parent is None:
            # If lane.parent is None, this is the very top level or a
            # top-level lane (e.g. "Young Adult Fiction").
            #
            # These are the only lanes that get Staff Picks and
            # Best-Sellers.
            best_seller_cutoff = (
                datetime.datetime.utcnow() - CustomListFeed.best_seller_cutoff)
            for group_uri, title, data_source_name, cutoff_point, available_languages in (
                    (best_sellers_url, "Best Sellers", 
                     DataSource.NYT, best_seller_cutoff,
                     cls.BEST_SELLER_LANGUAGES), 
                    (staff_picks_url, "Staff Picks", 
                     DataSource.LIBRARY_STAFF, None,
                     cls.STAFF_PICKS_LANGUAGES),
            ):
                available = False
                for lang in languages:
                    if lang in available_languages:
                        available = True
                        break
                if not available:
                    continue
                data_source = DataSource.lookup(_db, data_source_name)
                q = l.works(languages, availability=Work.ALL)
                q = Work.restrict_to_custom_lists_from_data_source(
                    _db, q, data_source, cutoff_point)
                a = time.time()
                page = q.all()
                b = time.time()
                #print "Got %s %s for %s in %.2f" % (
                #    len(page), title, lane.name, (b-a))
                if len(page) > 20:
                    sample = random.sample(page, 20)
                else:
                    sample = page
                for work in sample:
                    annotator.lane_by_work[work] = (
                        group_uri, title)
                    all_works.append(work)

        feed = AcquisitionFeed(_db, "Featured", url, all_works, annotator,
                               facet_groups=[])
        return feed

    DEFAULT_FACET_GROUPS = [
        ('Title', 'title', 'Sort by'),
        ('Author', 'author', 'Sort by')
    ]

    def __init__(self, _db, title, url, works, annotator=None,
                 active_facet=None, sublanes=[], messages_by_urn={},
                 facet_groups=DEFAULT_FACET_GROUPS, precomposed_entries=[]):
        super(AcquisitionFeed, self).__init__(title, url, annotator)
        lane_link = dict(rel="collection", href=url)
        first_time = time.time()
        totals = []
        if isinstance(works, Query):
            a = time.time()
            works = works.all()
            b = time.time()
            # print "Query executed in %.2f" % (b-a)
        if annotator:
            annotator.cached_record = []
            annotator.uncached_record = []
        for work in works:
            a = time.time()
            self.add_entry(work, lane_link)
            totals.append(time.time()-a)
        for entry in precomposed_entries:
            self.feed.append(entry)

        # Add minimal entries for the messages.
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
            self.feed.append(entry)

        total_entries = len(totals) + len(precomposed_entries)
        #if total_entries:
        #    print "Feed contains %d entries, build times: %r" % (
        #        len(totals), totals)
        #else:
        #    print "Feed is empty."
        logging.debug(
            "Built feed of %d entries in %.2f sec" % (
                total_entries, time.time()-first_time))

        for title, order, facet_group, in facet_groups:
            url = self.annotator.facet_url(order)
            if not url:
                continue
            link = dict(href=url, title=title)
            link['rel'] = "http://opds-spec.org/facet"
            link['{%s}facetGroup' % opds_ns] = facet_group
            if order==active_facet:
                link['{%s}activeFacet' % opds_ns] = "true"
            self.add_link(**link)

    def add_entry(self, work, lane_link):
        entry = self.create_entry(work, lane_link)
        if entry is not None:
            self.feed.append(entry)
        return entry

    @classmethod
    def single_entry(cls, _db, work, annotator, force_create=False):
        feed = cls(_db, '', '', [], annotator=annotator)
        if not isinstance(work, Edition) and not work.primary_edition:
            return None
        return feed.create_entry(work, None, even_if_no_license_pool=True,
                                 force_create=force_create)

    def create_entry(self, work, lane_link, even_if_no_license_pool=False,
                     force_create=True):
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

        before = time.time()
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


        after = time.time()
        if edition:
            title = (edition.title or "") + " "
        else:
            title = ""
        if self.annotator:
            if cache_hit:
                record = self.annotator.cached_record
            else:
                record = self.annotator.uncached_record
            record.append((title, after-before))

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
            data_source_tag = E._makeelement("{%s}license_source" % simplified_ns)
            data_source_tag.text = license_pool.data_source.name
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
        if (license_pool and license_pool.availability_time and
            license_pool.availability_time <= now):
            availability_tag = E._makeelement("published")
            # TODO: convert to local timezone.
            availability_tag.text = _strftime(license_pool.availability_time)
            print "%s uses PUBLISHED %s" % (edition.title, availability_tag.text)
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
        if edition.issued:
            print "%s uses issued for dc:created: %s %s" % (edition.title, edition.issued, issued)
        else:
            print "%s uses published for dc:created: %s %s" % (edition.title, edition.published, issued)
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
                print "%s CREATED %s" % (edition.title, issued_tag.text)
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
    def acquisition_link(cls, rel, href, types):
        if len(types) == 0:
            raise ValueError("Acquisition link must specify at least one type.")
        initial_type = types[0]
        indirect_types = types[1:]
        link = E._makeelement("link", type=initial_type, rel=rel)
        parent = link
        for t in indirect_types:
            indirect_link = E._makeelement(
                "{%s}indirectAcquisition" % opds_ns, type=t)
            parent.extend([indirect_link])
            parent = indirect_link
        return link

    def loan_tag(self, loan=None):
        return self._event_tag('loan', loan.start, loan.end)

    def hold_tag(self, hold=None):
        if not hold:
            return None
        hold_tag = self._event_tag('hold', hold.start, hold.end)
        position = E._makeelement("{%s}position" % schema_ns)
        hold_tag.extend([position])
        position.text = str(hold.position)
        return hold_tag

    def _event_tag(self, name, start, end):
        """
        :param start: A datetime (MUST be in UTC)
        :param end: A datetime (MUST be in UTC)
        """
        tag = E._makeelement("{%s}Event" % schema_ns)
        name_tag = E._makeelement("{%s}name" % schema_ns)
        tag.extend([name_tag])
        name_tag.text = name

        if start:
            created = E._makeelement("{%s}startDate" % schema_ns)
            tag.extend([created])
            created.text = start.isoformat() + "Z"
        if end:
            expires = E._makeelement("{%s}endDate" % schema_ns)
            tag.extend([expires])
            expires.text = end.isoformat() + "Z"
        return tag

    def license_tags(self, license_pool):
        if license_pool.open_access:
            return None

        license = []
        concurrent_lends = E._makeelement(
            "{%s}total_licenses" % simplified_ns)
        license.append(concurrent_lends)
        concurrent_lends.text = str(license_pool.licenses_owned)

        available_lends = E._makeelement(
            "{%s}available_licenses" % simplified_ns)
        license.append(available_lends)
        available_lends.text = str(license_pool.licenses_available)

        active_holds = E._makeelement("{%s}active_holds" % simplified_ns)
        license.append(active_holds)
        active_holds.text = str(license_pool.patrons_in_hold_queue)

        return license

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

class NavigationFeed(OPDSFeed):

    @classmethod
    def main_feed(self, lane, annotator):
        """The main navigation feed for the given lane."""
        if lane.display_name:
            name = lane.display_name
        else:
            name = "Navigation feed"
        feed = NavigationFeed(
            name, annotator.navigation_feed_url(lane), annotator)

        top_level_feed = feed.add_link(
            rel="start",
            type=self.NAVIGATION_FEED_TYPE,
            href=annotator.navigation_feed_url(None),
        )

        # If this is not a top-level lane, link to the navigation feed
        # for the parent lane.
        if lane.display_name:
            if lane.parent:
                parent = lane.parent
            else:
                parent = None
            parent_url = annotator.navigation_feed_url(parent)
            feed.add_link(
                rel="up",
                href=parent_url,
                type=self.NAVIGATION_FEED_TYPE,
            )

        if lane.display_name:
            # Link to an acquisition feed that contains _all_ books in
            # this lane.
            feed.add_link(
                type=self.ACQUISITION_FEED_TYPE,
                href=annotator.featured_feed_url(lane, 'author'),
                title="All %s" % lane.display_name,
            )

        # Create an entry for each sublane of this lane.
        for sublane in lane.sublanes:
            links = []
            # The entry will link to an acquisition feed of featured
            # books in that lane.
            for title, order, rel in [
                    ('Featured', None, self.FEATURED_REL)
            ]:
                link = E.link(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=annotator.featured_feed_url(sublane, order),
                    rel=rel,
                    title=title,
                )
                links.append(link)

            if sublane.sublanes.lanes:
                # The sublane itself has sublanes. Link to the
                # equivalent of this feed for that sublane.
                sublane_link = E.link(
                    type=self.NAVIGATION_FEED_TYPE,
                    href=annotator.navigation_feed_url(sublane),
                    rel="subsection",
                    title="Look inside %s" % sublane.display_name,
                )
            else:
                # This sublane has no sublanes. Link to an acquisition
                # feed that contains all books in the lane.
                sublane_link = E.link(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=annotator.featured_feed_url(sublane, 'author'),
                    title="All %s" % sublane.display_name,
                )
            links.append(sublane_link)

            feed.feed.append(
                E.entry(
                    E.id(annotator.lane_id(sublane)),
                    E.title(sublane.display_name),
                    # E.link(href=annotator.featured_feed_url(lane), rel="self"),
                    E.updated(_strftime(datetime.datetime.utcnow())),
                    *links
                )
            )

        return feed
