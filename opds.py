from collections import defaultdict
from nose.tools import set_trace
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

from lxml import builder, etree

#d = os.path.split(__file__)[0]
#site.addsitedir(os.path.join(d, ".."))
from model import (
    DataSource,
    Resource,
    Identifier,
    Edition,
    Work,
    )
from flask import request, url_for

ATOM_NAMESPACE = atom_ns = 'http://www.w3.org/2005/Atom'
app_ns = 'http://www.w3.org/2007/app'
xhtml_ns = 'http://www.w3.org/1999/xhtml'
dcterms_ns = 'http://purl.org/dc/terms/'
opds_ns = 'http://opds-spec.org/2010/catalog'
# TODO: This is a placeholder.
opds_41_ns = 'http://opds-spec.org/2014/catalog'
schema_ns = 'http://schema.org/'
simplified_ns = 'http://library-simplified.com/'


nsmap = {
    None: atom_ns,
    'app': app_ns,
    'dcterms' : dcterms_ns,
    'opds' : opds_ns,
    'opds41' : opds_41_ns,
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


class URLRewriter(object):

    epub_id = re.compile("/([0-9]+)")

    GUTENBERG_ILLUSTRATED_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated"
    GENERATED_COVER_HOST = "https://s3.amazonaws.com/gutenberg-corpus.nypl.org/Generated+covers"
    CONTENT_CAFE_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/CC"
    SCALED_CONTENT_CAFE_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/scaled/300/CC"
    ORIGINAL_OVERDRIVE_IMAGE_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/Overdrive"
    SCALED_OVERDRIVE_IMAGE_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/scaled/300/Overdrive"
    ORIGINAL_THREEM_IMAGE_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/3M"
    SCALED_THREEM_IMAGE_MIRROR_HOST = "https://s3.amazonaws.com/book-covers.nypl.org/scaled/300/3M"
    GUTENBERG_MIRROR_HOST = "http://s3.amazonaws.com/gutenberg-corpus.nypl.org/gutenberg-epub"

    @classmethod
    def rewrite(cls, url):
        if not url or '%(original_overdrive_covers_mirror)s' in url:
            # This is not mirrored; use the Content Reserve version.
            return None
        parsed = urlparse(url)
        if parsed.hostname in ('www.gutenberg.org', 'gutenberg.org'):
            return cls._rewrite_gutenberg(parsed)
        elif "%(" in url:
            return url % dict(content_cafe_mirror=cls.CONTENT_CAFE_MIRROR_HOST,
                              scaled_content_cafe_mirror=cls.SCALED_CONTENT_CAFE_MIRROR_HOST,
                              gutenberg_illustrated_mirror=cls.GUTENBERG_ILLUSTRATED_HOST,
                              original_overdrive_covers_mirror=cls.ORIGINAL_OVERDRIVE_IMAGE_MIRROR_HOST,
                              scaled_overdrive_covers_mirror=cls.SCALED_OVERDRIVE_IMAGE_MIRROR_HOST,
                              original_threem_covers_mirror=cls.ORIGINAL_THREEM_IMAGE_MIRROR_HOST,
                              scaled_threem_covers_mirror=cls.SCALED_THREEM_IMAGE_MIRROR_HOST,
            )
        else:
            return url

    @classmethod
    def _rewrite_gutenberg(cls, parsed):
        if parsed.path.startswith('/cache/epub/'):
            new_path = parsed.path.replace('/cache/epub/', '', 1)
        elif '.epub' in parsed.path:
            text_id = cls.epub_id.search(parsed.path).groups()[0]
            if 'noimages' in parsed.path:
                new_path = "%(pub_id)s/pg%(pub_id)s.epub" 
            else:
                new_path = "%(pub_id)s/pg%(pub_id)s-images.epub"
            new_path = new_path % dict(pub_id=text_id)
        else:
            new_path = parsed_path
        return cls.GUTENBERG_MIRROR_HOST + '/' + new_path


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

    @classmethod
    def lane_url(cls, lane, order=None):
        return url_for('feed', lane=lane.name, order=order, _external=True)

class AcquisitionFeed(OPDSFeed):

    def __init__(self, _db, title, url, works, facet_url_generator=None,
                 active_facet=None, sublanes=[], active_loans_by_work={}):
        super(AcquisitionFeed, self).__init__(title, url=url)
        lane_link = dict(rel="collection", href=url)
        import time
        first_time = time.time()
        totals = []
        for work in works:
            a = time.time()
            self.add_entry(work, lane_link, active_loans_by_work.get(work))
            totals.append(time.time()-a)

        # import numpy
        # print "Feed built in %.2f (mean %.2f, stdev %.2f)" % (
        #    time.time()-first_time, numpy.mean(totals), numpy.std(totals))

        if facet_url_generator:
            for title, order, facet_group, in [
                    ('Title', 'title', 'Sort by'),
                    ('Author', 'author', 'Sort by')]:
                link = dict(href=facet_url_generator(order),
                            title=title)
                link['rel'] = "http://opds-spec.org/facet"
                link['{%s}facetGroup' % opds_ns] = facet_group
                if order==active_facet:
                    link['{%s}activeFacet' % opds_ns] = "true"
                self.add_link(**link)

    @classmethod
    def featured(cls, _db, languages, lane):
        url = cls.lane_url(lane)
        links = []
        feed_size = 20
        works = lane.quality_sample(languages, 0.65, 0.3, feed_size,
                                    "currently_available")
        return AcquisitionFeed(
            _db, "%s: featured" % lane.name, url, works, sublanes=lane.sublanes)

    @classmethod
    def active_loans_for(cls, patron):
        db = Session.object_session(patron)
        url = url_for('active_loans', _external=True)
        active_loans_by_work = {}
        for loan in patron.loans:
            active_loans_by_work[loan.license_pool.work] = loan
        return AcquisitionFeed(db, "Active loans", url, patron.works_on_loan(),
                               active_loans_by_work=active_loans_by_work)

    def add_entry(self, work, lane_link, loan=None):
        entry = self.create_entry(work, lane_link, loan)
        if entry is not None:
            self.feed.append(entry)
        return entry

    def create_entry(self, work, lane_link, loan=None):
        """Turn a work into an entry for an acquisition feed."""
        # Find the .epub link
        epub_href = None
        p = None

        active_license_pool = None
        if loan:
            # The active license pool is the one associated with
            # the loan.
            active_license_pool = loan.license_pool
        else:
            # The active license pool is the one that *would* be associated
            # with a loan, were a loan to be issued right now.
            open_access_license_pool = None
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

        # There's no reason to present a book that has no active license pool.
        if not active_license_pool:
            return None

        # TODO: If there's an active loan, the links and the license
        # information should be much different. But we currently don't
        # include license information at all, because OPDS For
        # Libraries is still in flux. So for now we always put up an
        # open access link that leads to the checkout URL.
        identifier = active_license_pool.identifier
        checkout_url = url_for(
            "checkout", data_source=active_license_pool.data_source.name,
            identifier=identifier.identifier, _external=True)

        if active_license_pool.open_access:
            rel = self.OPEN_ACCESS_REL
        else:
            rel = self.BORROW_REL
        links=[E.link(rel=rel, href=checkout_url)]

        cover_quality = 0
        qualities = [("Work quality", work.quality)]
        full_url = None

        active_edition = work.primary_edition

        if not work.cover_full_url and active_edition.cover:
            active_edition.set_cover(active_edition.cover)

        thumbnail_url = work.cover_thumbnail_url
        if work.cover_full_url:
            full_url = URLRewriter.rewrite(work.cover_full_url)
            #mirrored_url = URLRewriter.rewrite(work.cover.mirrored_path)
            #if mirrored_url:
            #    full_url = mirrored_url
                
            qualities.append(("Cover quality", active_edition.cover.quality))
            if active_edition.cover.scaled_path:
                thumbnail_url = URLRewriter.rewrite(active_edition.cover.scaled_path)
            elif active_edition.cover.data_source.name == DataSource.GUTENBERG_COVER_GENERATOR:
                thumbnail_url = full_url
        elif identifier.type == Identifier.GUTENBERG_ID:
            host = URLRewriter.GENERATED_COVER_HOST
            thumbnail_url = host + urllib.quote(
                "/Gutenberg ID/%s.png" % identifier.identifier)
            full_url = thumbnail_url
        if full_url:
            links.append(E.link(rel=Resource.IMAGE, href=full_url))

        if thumbnail_url:
            thumbnail_url = URLRewriter.rewrite(thumbnail_url)
            links.append(E.link(rel=Resource.THUMBNAIL_IMAGE, href=thumbnail_url))
        identifier = active_license_pool.identifier
        tag = url_for("work", identifier_type=identifier.type,
                      identifier=identifier.identifier, _external=True)

        if work.summary_text:
            summary = work.summary_text
            if work.summary:
                qualities.append(("Summary quality", work.summary.quality))
        elif work.summary:
            work.summary_text = work.summary.content
            summary = work.summary_text
        else:
            summary = ""
        summary += "<ul>"
        for name, value in qualities:
            if isinstance(value, basestring):
                summary += "<li>%s: %s</li>" % (name, value)
            else:
                summary += "<li>%s: %.1f</li>" % (name, value)
        summary += "<li>License Source: %s</li>" % active_license_pool.data_source.name
        summary += "</ul>"

        entry = E.entry(
            E.id(tag),
            E.title(work.title))
        if work.subtitle:
            entry.extend([E.alternativeHeadline(work.subtitle)])

        entry.extend([
            E.author(E.name(work.author or "")),
            E.summary(summary),
            E.link(href=checkout_url),
            E.updated(_strftime(datetime.datetime.utcnow())),
        ])
        entry.extend(links)

        genre_tags = []
        for wg in work.work_genres:
            genre_tags.append(E.category(term=wg.genre.name))
        if len(work.work_genres) == 0:
            sole_genre = None
            if work.fiction == True:
                sole_genre = 'Fiction'
            elif work.fiction == False:
                sole_genre = 'Nonfiction'
            if sole_genre:
                genre_tags.append(E.category(term=sole_genre))
        entry.extend(genre_tags)

        # print " ID %s TITLE %s AUTHORS %s" % (tag, work.title, work.authors)
        language = work.language_code
        if language:
            language_tag = E._makeelement("{%s}language" % dcterms_ns)
            language_tag.text = language
            entry.append(language_tag)

        if active_edition.publisher:
            publisher_tag = E._makeelement("{%s}publisher" % dcterms_ns)
            publisher_tag.text = active_edition.publisher
            entry.extend([publisher_tag])

        # We use Atom 'published' for the date the book first became
        # available to people using this application.
        now = datetime.datetime.utcnow()
        today = datetime.date.today()
        if (active_license_pool.availability_time and
            active_license_pool.availability_time <= now):
            availability_tag = E._makeelement("published")
            # TODO: convert to local timezone.
            availability_tag.text = active_license_pool.availability_time.strftime(
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
        issued = active_edition.published
        if (issued and issued <= today):
            issued_tag = E._makeelement("{%s}dateCopyrighted" % dcterms_ns)
            # TODO: convert to local timezone.
            issued_tag.text = issued.strftime("%Y-%m-%d")
            entry.extend([issued_tag])

        if work.audience:
            audience_tag = E._makeelement("{%s}audience" % schema_ns)
            audience_name_tag = E._makeelement("{%s}name" % schema_ns)
            audience_name_tag.text = work.audience
            audience_tag.extend([audience_name_tag])
            entry.extend([audience_tag])

        loan_tag = self.loan_tag(loan)
        if loan_tag is not None:
            entry.extend([loan_tag])

        license_tag = self.license_tag(active_license_pool)
        if license_tag is not None:
            entry.extend([license_tag])

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

    def license_tag(self, license_pool):
        if license_pool.open_access:
            return None

        licenses = E._makeelement("{%s}licenses" % opds_41_ns)
        license = E._makeelement("{%s}license" % opds_41_ns)
        concurrent_lends = E._makeelement(
            "{%s}concurrent_lends" % opds_41_ns)
        license.extend([concurrent_lends])
        concurrent_lends.text = str(license_pool.licenses_owned)

        available_lends = E._makeelement(
            "{%s}available_lends" % simplified_ns)
        license.extend([available_lends])
        available_lends.text = str(license_pool.licenses_available)

        active_holds = E._makeelement("{%s}active_holds" % simplified_ns)
        license.extend([active_holds])
        active_holds.text = str(license_pool.patrons_in_hold_queue)

        licenses.extend([license])
        return licenses

class NavigationFeed(OPDSFeed):

    @classmethod
    def main_feed(self, lane):
        if lane.name:
            name = "Navigation feed for %s" % lane.name
        else:
            name = "Navigation feed"
        feed = NavigationFeed(
            name,
            url=url_for('navigation_feed', lane=lane.name, _external=True))

        top_level_feed = feed.add_link(
            rel="start",
            type=self.NAVIGATION_FEED_TYPE,
            href=url_for('navigation_feed', _external=True),
        )

        if lane.name:
            if lane.parent:
                parent_name = lane.parent.name
            else:
                parent_name = None
            parent_url = url_for(
                'navigation_feed', lane=parent_name, _external=True)
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
                    href=self.lane_url(lane, order),
                    rel=rel,
                    title=title,
                )
                links.append(link)

            if lane.sublanes.lanes:
                navigation_link = E.link(
                    type=self.NAVIGATION_FEED_TYPE,
                    href=url_for("navigation_feed", lane=lane.name, _external=True),
                    rel="subsection",
                    title="Look inside %s" % lane.name,
                )
                links.append(navigation_link)
            else:
                link = E.link(
                    type=self.ACQUISITION_FEED_TYPE,
                    href=self.lane_url(lane, 'author'),
                    title="Look inside %s" % lane.name,
                )
                links.append(link)


            feed.feed.append(
                E.entry(
                    E.id("tag:%s" % (lane.name)),
                    E.title(lane.name),
                    E.link(href=self.lane_url(lane)),
                    E.updated(_strftime(datetime.datetime.utcnow())),
                    *links
                )
            )

        return feed
