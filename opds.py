from collections import (
    defaultdict,
    Counter,
)
from nose.tools import set_trace
import feedparser
import re
import os
import site
import sys
import datetime
import random
import urllib
from urlparse import urlparse, urljoin
import md5
from sqlalchemy.sql.expression import func
from sqlalchemy.orm.session import Session
import requests

from lxml import builder, etree

from model import (
    DataSource,
    Hyperlink,
    Resource,
    Identifier,
    Edition,
    Subject,
    Work,
    )

ATOM_NAMESPACE = atom_ns = 'http://www.w3.org/2005/Atom'
app_ns = 'http://www.w3.org/2007/app'
xhtml_ns = 'http://www.w3.org/1999/xhtml'
dcterms_ns = 'http://purl.org/dc/terms/'
opds_ns = 'http://opds-spec.org/2010/catalog'
schema_ns = 'http://schema.org/'

# This is a placeholder namespace for stuff we've invented.
simplified_ns = 'http://library-simplified.com/terms/'


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
    return d.strftime('%Y-%m-%dT%H:%M:%SZ%z')

default_typemap = {datetime: lambda e, v: _strftime(v)}

E = builder.ElementMaker(typemap=default_typemap, nsmap=nsmap)
SCHEMA = builder.ElementMaker(
    typemap=default_typemap, nsmap=nsmap, namespace="http://schema.org/")


class Annotator(object):
    """The Annotator knows how to present an OPDS feed in a specific
    application context.
    """

    @classmethod
    def annotate_work_entry(cls, work, license_pool, edition, identifier, feed,
                            entry, links):
        """Make any custom modifications necessary to integrate this
        OPDS entry into the application's workflow.
        """
        return

    @classmethod
    def cover_links(cls, work):
        """Return all links to be used as cover links for this work.

        In a distribution application, each work will have only one
        link. In a content server-type application, each work may have
        a large number of links.

        :return: A 2-tuple (thumbnail_links, full_links)

        """
        # TODO: This might not be necessary anymore.
        if not work.cover_full_url and work.primary_edition.cover:
            work.primary_edition.set_cover(work.primary_edition.cover)

        thumbnails = []
        if work.cover_thumbnail_url:
            thumbnails = [work.cover_thumbnail_url]

        full = []
        if work.cover_full_url:
            full = [work.cover_full_url]
        return thumbnails, full

    @classmethod
    def categories(cls, work):
        """Return all relevant classifications of this work.

        :return: A dictionary mapping 'scheme' URLs to 'term' attributes.
        """
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
            categories[Subject.SIMPLIFIED_GENRE] = simplified_genres

        # Add the audience as a category of schema
        # http://schema.org/audience
        if work.audience:
            categories[schema_ns + "audience"] = [work.audience]
        return categories

    @classmethod
    def authors(cls, work):
        """Create one or more <author> tags for the given work."""
        return [E.author(E.name(work.author or ""))]

    @classmethod
    def content(cls, work):
        """Return an HTML summary of this work."""
        summary = ""
        if work.summary_text:
            summary = work.summary_text
        elif work.summary:
            work.summary_text = work.summary.content
            summary = work.summary_text
        return summary

    @classmethod
    def lane_id(cls, lane):
        return "tag:%s" % (lane.name)

    @classmethod
    def work_id(cls, work):
        return work.primary_edition.primary_identifier.urn

    @classmethod
    def permalink_for(cls, identifier):
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

        if work.has_open_access_license:
            # All licenses are issued from the license pool associated with
            # the work's primary edition.
            edition = work.primary_edition

            if edition.license_pool and edition.best_open_access_link:
                # Looks good.
                open_access_license_pool = edition.license_pool

        if not open_access_license_pool:
            # The active license pool is the one that *would* be
            # associated with a loan, were a loan to be issued right
            # now.
            for p in work.license_pools:
                if p.open_access:
                    # Make sure there's a usable link--it might be
                    # audio-only or something.
                    if p.edition().best_open_access_link:
                        open_access_license_pool = p
                else:
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
        return by_scheme

    @classmethod
    def authors(cls, work):
        """Create a detailed <author> tag for each author."""
        return [cls.detailed_author(author)
                for author in work.primary_edition.author_contributors]

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

    def __init__(self, title, url):
        self.feed = E.feed(
            E.id(url),
            E.title(title),
            E.updated(_strftime(datetime.datetime.utcnow())),
            E.link(href=url),
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

    FEATURED_REL = "http://opds-spec.org/featured"
    RECOMMENDED_REL = "http://opds-spec.org/recommended"
    OPEN_ACCESS_REL = "http://opds-spec.org/acquisition/open-access"
    BORROW_REL = "http://opds-spec.org/acquisition/borrow"
    FULL_IMAGE_REL = "http://opds-spec.org/image" 
    EPUB_MEDIA_TYPE = "application/epub+zip"

    def __init__(self, title, url, annotator):
        if not annotator:
            annotator = Annotator()
        self.annotator = annotator
        super(OPDSFeed, self).__init__(title, url)


class AcquisitionFeed(OPDSFeed):

    @classmethod
    def featured(cls, languages, lane, annotator):
        """The acquisition feed for 'featured' items from a given lane.
        """
        url = annotator.featured_feed_url(lane)
        feed_size = 20
        works = lane.quality_sample(languages, 0.65, 0.3, feed_size,
                                    "currently_available")
        return AcquisitionFeed(
            lane._db, "%s: featured" % lane.name, url, works, annotator, 
            sublanes=lane.sublanes)

    def __init__(self, _db, title, url, works, annotator=None,
                 active_facet=None, sublanes=[], messages_by_urn={}):
        super(AcquisitionFeed, self).__init__(title, url, annotator)
        lane_link = dict(rel="collection", href=url)
        import time
        first_time = time.time()
        totals = []
        for work in works:
            a = time.time()
            self.add_entry(work, lane_link)
            totals.append(time.time()-a)

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

        # import numpy
        # print "Feed built in %.2f (mean %.2f, stdev %.2f)" % (
        #    time.time()-first_time, numpy.mean(totals), numpy.std(totals))

        for title, order, facet_group, in [
                ('Title', 'title', 'Sort by'),
                ('Author', 'author', 'Sort by')]:
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

    def create_entry(self, work, lane_link):
        """Turn a work into an entry for an acquisition feed."""
        active_license_pool = self.annotator.active_licensepool_for(work)
        # There's no reason to present a book that has no active license pool.
        if not active_license_pool:
            return None

        active_edition = active_license_pool.edition()
        identifier = active_license_pool.identifier
        return self._create_entry(work, active_license_pool, active_edition, identifier,
                           lane_link)

    def _create_entry(self, work, license_pool, edition, identifier, lane_link):

        # Find the .epub link
        epub_href = None
        p = None

        links = []
        cover_quality = 0
        qualities = [("Work quality", work.quality)]
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
           

        permalink = self.annotator.permalink_for(identifier)
        content = self.annotator.content(work)

        # TODO: This is a super cheesy way of estimating whether the
        # book's description contains HTML. We need to estimate this
        # better, and the estimate needs to happen ahead of time.
        if '<' in content and '>' in content:
            content_type = 'html'
        else:
            content_type = 'text'

        entry = E.entry(
            E.id(permalink),
            E.title(edition.title),
        )
        if work.subtitle:
            entry.extend([E.alternativeHeadline(edition.subtitle)])

        author_tags = self.annotator.authors(work)
        entry.extend(author_tags)

        entry.extend([
            E.content(content, type=content_type),
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
        if (license_pool.availability_time and
            license_pool.availability_time <= now):
            availability_tag = E._makeelement("published")
            # TODO: convert to local timezone.
            availability_tag.text = license_pool.availability_time.strftime(
                "%Y-%m-%d")
            entry.extend([availability_tag])

        # Entry.issued is the date the ebook came out, as distinct
        # from Entry.published (which may refer to the print edition
        # or some original edition way back when).
        #
        # For Dublin Core 'dateCopyrighted' (which is the closest we
        # can come to 'date the underlying book actually came out' in
        # a way that won't be confused with 'date the book was added
        # to our database' we use Entry.issued if we have it and
        # Entry.published if not. In general this means we use issued
        # date for Gutenberg and published date for other sources.
        issued = edition.published
        if (issued and issued <= today):
            issued_tag = E._makeelement("{%s}dateCopyrighted" % dcterms_ns)
            # TODO: convert to local timezone.
            issued_tag.text = issued.strftime("%Y-%m-%d")
            entry.extend([issued_tag])

        self.annotator.annotate_work_entry(
            work, license_pool, edition, identifier, self, entry, links)

        return entry

    def loan_tag(self, loan=None):
        # TODO: loan.start should be a datetime object that knows it's UTC.
        if not loan:
            return None
        loan_tag = E._makeelement("{%s}Event" % schema_ns)
        name = E._makeelement("{%s}name" % schema_ns)
        loan_tag.extend([name])
        name.text = 'loan'

        if loan.start:
            created = E._makeelement("{%s}startDate" % schema_ns)
            loan_tag.extend([created])
            created.text = loan.start.isoformat() + "Z"
        if loan.end:
            expires = E._makeelement("{%s}endDate" % schema_ns)
            loan_tag.extend([expires])
            expires.text = loan.end.isoformat() + "Z"
        return loan_tag

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

        active_edition = active_license_pool.edition()
        return self._create_entry(
            work, active_license_pool, work.primary_edition, 
            identifier, lane_link)

class NavigationFeed(OPDSFeed):

    @classmethod
    def main_feed(self, lane, annotator):
        """The main navigation feed for the given lane."""
        if lane.name:
            name = "Navigation feed for %s" % lane.name
        else:
            name = "Navigation feed"
        feed = NavigationFeed(
            name, annotator.navigation_feed_url(lane), annotator)

        top_level_feed = feed.add_link(
            rel="start",
            type=self.NAVIGATION_FEED_TYPE,
            href=annotator.navigation_feed_url(None),
        )

        if lane.name:
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

        for lane in lane.sublanes:
            links = []

            for title, order, rel in [
                    ('Featured', None, self.FEATURED_REL)
            ]:
                link = E.link(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=annotator.featured_feed_url(lane, order),
                    rel=rel,
                    title=title,
                )
                links.append(link)

            if lane.sublanes.lanes:
                navigation_link = E.link(
                    type=self.NAVIGATION_FEED_TYPE,
                    href=annotator.navigation_feed_url(lane),
                    rel="subsection",
                    title="Look inside %s" % lane.name,
                )
                links.append(navigation_link)
            else:
                link = E.link(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=annotator.featured_feed_url(lane, 'author'),
                    title="Look inside %s" % lane.name,
                )
                links.append(link)


            feed.feed.append(
                E.entry(
                    E.id(annotator.lane_id(lane)),
                    E.title(lane.name),
                    E.link(href=annotator.featured_feed_url(lane)),
                    E.updated(_strftime(datetime.datetime.utcnow())),
                    *links
                )
            )

        return feed
