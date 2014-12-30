from nose.tools import set_trace
from collections import defaultdict
import feedparser
import requests

from lxml import builder, etree

from monitor import Monitor
from util import LanguageCodes
from model import (
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Resource,
)

class BaseOPDSImporter(object):
    """Capable of importing editions from an OPDS feed.

    This importer should be used when a circulation server
    communicates with a content server. It ignores author and subject
    information, under the assumption that it will get better author
    and subject information from the metadata wrangler.
    """
   
    def __init__(self, _db, feed):
        self._db = _db
        self.raw_feed = unicode(feed)
        self.feedparser_parsed = feedparser.parse(self.raw_feed)

    def import_from_feed(self):
        imported = []
        for entry in self.feedparser_parsed['entries']:
            opds_id, edition, edition_was_new = self.import_from_feedparser_entry(
                entry)
            if edition:
                imported.append(edition)

                # This may or may not make the work
                # presentation-ready--it depends on whether we've
                # talked to the metadata wrangler.
                if edition.sort_author:
                    work, ignore = license_pool.calculate_work()
                    work.calculate_presentation()
        return imported

    def links_by_rel(self, entry):
        links = entry.get('links', [])
        links_by_rel = defaultdict(list)
        for link in links:
            if 'rel' not in link or 'href' not in link:
                continue
            links_by_rel[link['rel']].append(link)
        return links_by_rel

    def import_from_feedparser_entry(self, entry):
        identifier, ignore = Identifier.parse_urn(self._db, entry.get('id'))
        data_source = DataSource.license_source_for(self._db, identifier)

        title = entry.get('title', None)
        updated = entry.get('updated_parsed', None)
        publisher = entry.get('dcterms_publisher', None)
        language = entry.get('dcterms_language', None)
        t = LanguageCodes.two_to_three
        if language and len(language) == 2 and language in t:
            language = t[language]
        pwid = entry.get('simplified_pwid', None)

        title_detail = entry.get('title_detail', None)
        summary_detail = entry.get('summary_detail', None)

        links_by_rel = self.links_by_rel(entry)
        if not links_by_rel[Resource.OPEN_ACCESS_DOWNLOAD]:
            # If there's no open-access link, we can't create a
            # LicensePool.
            #
            # TODO: Eventually we should be able to handle
            # non-open-access works, but that requires a strategy for
            # negotiating a checkout.
            return identifier, None, False

        # Create or retrieve a LicensePool for this book.
        license_pool, pool_was_new = LicensePool.for_foreign_id(
            self._db, data_source, identifier.type, identifier.identifier)
        if pool_was_new:
            license_pool.open_access = True

        # Create or retrieve an Edition for this book.
        edition, edition_was_new = Edition.for_foreign_id(
            self._db, data_source, identifier.type, identifier.identifier)

        source_last_updated = entry['updated_parsed']
        if not pool_was_new and not edition_was_new and edition.work and edition.work.last_update_time >= source_last_updated:
            # The metadata has not changed since last time
            return identifier, edition, False

        # Remove all existing downloads and images, and descriptions
        # so as to avoid keeping old stuff around.
        rels = [Resource.OPEN_ACCESS_DOWNLOAD, Resource.IMAGE,
                Resource.DESCRIPTION]
        for resource in Identifier.resources_for_identifier_ids(
                self._db, [identifier.id], rels):
            self._db.delete(resource)

        # Associate covers and downloads with the identifier.
        for rel in [Resource.OPEN_ACCESS_DOWNLOAD, Resource.IMAGE]:
            for link in links_by_rel[rel]:
                identifier.add_resource(
                    rel, link['href'], data_source, 
                    license_pool, link.get('type', None))

        # If there's a summary, add it to the identifier.
        summary = entry.get('summary_detail', {})
        if 'value' in summary and summary['value']:
            identifier.add_resource(
                Resource.SUMMARY, None, data_source, license_pool,
                summary['value'])
       
        print title
        edition.title = title
        edition.language = language
        edition.publisher = publisher
        return identifier, edition, edition_was_new

class DetailedOPDSImporter(BaseOPDSImporter):

    """An OPDS importer that imports authors as contributors and
    tags as subjects.

    This should be used by circulation managers when talking to the
    metadata wrangler, and by the metadata wrangler when talking to
    content servers.
    """
    def import_from_feed(self, feed):
        lxml_parsed = etree.parse(feed)
        authors_by_id = self.authors_by_id(lxml_parsed)
        subjects = self.process_tags(entry.get('tags', []))


class OPDSImportMonitor(Monitor):
    """Periodically monitor an OPDS archive feed and import every edition
    it mentions.
    """
    
    def __init__(self, feed_url, import_class, interval_seconds=3600):
        self.feed_url = feed_url
        self.import_class = import_class
        super(OPDSImportMonitor, self).__init__(
            "OPDS Import %s" % feed_url, interval_seconds)

    def run_once(self, _db, start, cutoff):
        next_link = self.feed_url
        while next_link:
            imported = self.process_one_page(_db, next_link)
            if len(imported) == 0:
                # We did not see a single book on this page we haven't
                # already seen. There's no need to keep going.
                break
            # TODO: get the proper next link.
            set_trace()
            next_link = True

    def process_one_page(self, _db, url):
        response = requests.get(self.feed_url)
        importer = self.import_class(_db, response.content)
        return importer.import_from_feed()
