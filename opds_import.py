from nose.tools import set_trace
from StringIO import StringIO
from collections import defaultdict
import datetime
import feedparser
import requests
import urllib

from lxml import builder, etree

from monitor import Monitor
from util import LanguageCodes
from util.xmlparser import XMLParser
from model import (
    get_one,
    Contributor,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Resource,
    Subject,
)

class SimplifiedOPDSLookup(object):
    """Tiny integration class for the Simplified 'lookup' protocol."""

    LOOKUP_ENDPOINT = "lookup"
    CANONICALIZE_ENDPOINT = "canonical-author-name"

    def __init__(self, base_url):
        if not base_url.endswith('/'):
            base_url += "/"
        self.base_url = base_url

    def lookup(self, identifiers):
        """Retrieve an OPDS feed with metadata for the given identifiers."""
        args = "&".join(["urn=%s" % i.urn for i in identifiers])
        url = self.base_url + self.LOOKUP_ENDPOINT + "?" + args
        return requests.get(url)

    def canonicalize_author_name(self, identifier, working_display_name):
        """Attempt to find the canonical name for the author of a book.

        :param identifier: an ISBN-type Identifier.

        :param working_display_name: The display name of the author
        (i.e. the name format human being used as opposed to the name
        that goes into library records).
        """
        args = "urn=%s&display_name=%s" % (
            urllib.quote(identifier.urn), urllib.quote(
                working_display_name.encode("utf8")))
        url = self.base_url + self.CANONICALIZE_ENDPOINT + "?" + args
        return requests.get(url)

class OPDSXMLParser(XMLParser):

    NAMESPACES = { "simplified": "http://library-simplified.com/terms/",
                   "app" : "http://www.w3.org/2007/app",
                   "dcterms" : "http://purl.org/dc/terms/",
                   "opds": "http://opds-spec.org/2010/catalog",
                   "schema" : "http://schema.org",
                   "atom" : "http://www.w3.org/2005/Atom",
    }

class BaseOPDSImporter(object):

    """Capable of importing editions from an OPDS feed.

    This importer should be used when a circulation server
    communicates with a content server. It ignores author and subject
    information, under the assumption that it will get better author
    and subject information from the metadata wrangler.
    """
    COULD_NOT_CREATE_LICENSE_POOL = (
        "No existing license pool for this identifier and no way of creating one.")
   
    def __init__(self, _db, feed):
        self._db = _db
        self.raw_feed = unicode(feed)
        self.feedparser_parsed = feedparser.parse(self.raw_feed)

    def import_from_feed(self):
        imported = []
        messages_by_id = dict()
        for entry in self.feedparser_parsed['entries']:
            opds_id, edition, edition_was_new, status_code, message = self.import_from_feedparser_entry(
                entry)
            if edition:
                imported.append(edition)

                # This may or may not make the work
                # presentation-ready--it depends on whether we've
                # talked to the metadata wrangler.
                edition.calculate_presentation()
                if edition.sort_author:
                    work, ignore = edition.license_pool.calculate_work()
                    work.calculate_presentation()
            elif status_code:
                messages_by_id[opds_id] = (status_code, message)
        return imported, messages_by_id

    def links_by_rel(self, entry=None):
        if entry:
            source = entry
        else:
            source = self.feedparser_parsed['feed']
        links = source.get('links', [])
        links_by_rel = defaultdict(list)
        for link in links:
            if 'rel' not in link or 'href' not in link:
                continue
            links_by_rel[link['rel']].append(link)
        return links_by_rel

    def import_from_feedparser_entry(self, entry):
        identifier, ignore = Identifier.parse_urn(self._db, entry.get('id'))
        data_source = DataSource.license_source_for(self._db, identifier)

        status_code = entry.get('simplified_status_code', 200)
        message = entry.get('simplified_message', None)
        try:
            status_code = int(status_code)
            success = (status_code == 200)
        except ValueError, e:
            # The status code isn't a number. Leave it alone.
            success = False

        if not success:
            # There was an error or the data is not complete. Don't go
            # through with the import, even if there is data in the
            # entry.
            return identifier, None, False, status_code, message

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

        # Get an existing LicensePool for this book.
        pool = get_one(
            self._db, LicensePool, data_source=data_source,
            identifier=identifier)

        links_by_rel = self.links_by_rel(entry)
        if pool:
            pool_was_new = False
        else:
            # There is no existing license pool for this book. Can we
            # just create one?
            if links_by_rel[Resource.OPEN_ACCESS_DOWNLOAD]:
                # Yes. This is an open-access book and we know where
                # you can download it.
                pool, pool_was_new = LicensePool.for_foreign_id(
                    self._db, data_source, identifier.type, identifier.identifier)
            else:
                # No, we can't. This most likely indicates a problem.
                message = message or self.COULD_NOT_CREATE_LICENSE_POOL
                return (identifier, None, False, status_code, message)

        if pool_was_new:
            pool.open_access = True

        # Create or retrieve an Edition for this book.
        edition, edition_was_new = Edition.for_foreign_id(
            self._db, data_source, identifier.type, identifier.identifier)

        source_last_updated = entry.get('updated_parsed')
        if not source_last_updated:
            set_trace()
        # TODO: I'm not really happy with this but it will work as long
        # as the times are in UTC, possibly as long as the times are in
        # the same timezone.
        source_last_updated = datetime.datetime(*source_last_updated[:6])
        if not pool_was_new and not edition_was_new and edition.work and edition.work.last_update_time >= source_last_updated:
            # The metadata has not changed since last time
            return identifier, edition, False, status_code, message

        rels = [Resource.OPEN_ACCESS_DOWNLOAD, Resource.IMAGE,
                Resource.DESCRIPTION]
        self.destroy_resources(identifier, rels)

        download_resources, image_resource = self.set_resources(
            data_source, identifier, pool, links_by_rel)

        # If there's a summary, add it to the identifier.
        summary = entry.get('summary_detail', {})
        if 'value' in summary and summary['value']:
            identifier.add_resource(
                Resource.DESCRIPTION, None, data_source, pool,
                summary.get('type', 'text/plain'), summary['value'])
        for content in entry.get('content', []):
            identifier.add_resource(
                Resource.DESCRIPTION, None, data_source, pool,
                summary.get('type', 'text/html'), content['value'])
            

        edition.title = title
        edition.language = language
        edition.publisher = publisher

        # Assign the LicensePool to a Work.
        work = pool.calculate_work()

        return identifier, edition, edition_was_new, status_code, message

    def destroy_resources(self, identifier, rels):
        # Remove all existing downloads and images, and descriptions
        # so as to avoid keeping old stuff around.
        for resource in Identifier.resources_for_identifier_ids(
                self._db, [identifier.id], rels):
            self._db.delete(resource)

    def set_resources(self, data_source, identifier, pool, links):
        # Associate covers and downloads with the identifier.
        #
        # If there is both a full image and a thumbnail, we need
        # to make sure they're put into the same resource.
        download_resources = []
        image_resource = None

        for rel in [Resource.OPEN_ACCESS_DOWNLOAD, Resource.IMAGE,
                    Resource.THUMBNAIL_IMAGE]:
            for link in links[rel]:
                type = link.get('type', None)
                if type == 'text/html':
                    # Feedparser fills this in and it's just wrong.
                    type = None
                url = link['href']
                if rel == Resource.OPEN_ACCESS_DOWNLOAD or not image_resource:
                    resource, was_new = identifier.add_resource(
                        rel, url, data_source, pool, type)
                if rel == Resource.OPEN_ACCESS_DOWNLOAD:
                    download_resources.append(resource)
                else:
                    image_resource = resource

                # TODO: Metadata wrangler should include width and
                # height if possible, and we should pick it up here.

                # The metadata wrangler handles scaling and mirroring
                # resources, and we will trust what it says.
                if rel == Resource.IMAGE:
                    print "Resource %s was mirrored." % url
                    image_resource.href = url
                    image_resource.mirrored = True
                    image_resource.mirrored_path = url
                    image_resource.mirrored_date = datetime.datetime.utcnow()
                    image_resource.mirrored_status = 200
                elif rel == Resource.THUMBNAIL_IMAGE:
                    print "Resource %s was scaled." % url
                    image_resource.scaled = True
                    image_resource.scaled_path = url
                else:
                    print "Resource %s was neither scaled nor mirrored." % url
        return download_resources, image_resource

class DetailedOPDSImporter(BaseOPDSImporter):

    """An OPDS importer that imports authors as contributors and
    tags as subjects.

    This should be used by circulation managers when talking to the
    metadata wrangler, and by the metadata wrangler when talking to
    content servers.
    """

    def __init__(self, _db, feed):
        super(DetailedOPDSImporter, self).__init__(_db, feed)
        self.lxml_parsed = etree.fromstring(self.raw_feed)
        self.authors_by_id = self.authors_by_id(_db, self.lxml_parsed)

    def import_from_feedparser_entry(self, entry):
        identifier, edition, edition_was_new, status_code, message = super(
            DetailedOPDSImporter, self).import_from_feedparser_entry(entry)

        if edition_was_new:
            for contributor in self.authors_by_id[entry.id]:
                edition.add_contributor(contributor, Contributor.AUTHOR_ROLE)

            data_source = DataSource.license_source_for(self._db, identifier)
            for type, term, name in self.subjects_for(entry):
                identifier.classify(
                    data_source, type, term, name)

        return identifier, edition, edition_was_new, status_code, message

    @classmethod
    def subjects_for(cls, feedparser_entry):
        tags = feedparser_entry.get('tags', [])
        for i in tags:
            scheme = i.get('scheme')
            subject_type = Subject.by_uri.get(scheme)
            if not subject_type:
                # We can't represent this subject because we don't
                # know its scheme. Just treat it as a tag.
                subject_type = Subject.TAG
            identifier = i.get('term')
            name = i.get('label')
            yield subject_type, identifier, name

    @classmethod
    def authors_by_id(cls, _db, root):
        """Parse the OPDS as XML and extract all author information.

        Feedparser can't handle this so we have to use lxml.
        """
        parser = OPDSXMLParser()
        by_id = defaultdict(list)
        for entry in parser._xpath(root, '/atom:feed/atom:entry'):
            identifier = parser._xpath1(entry, 'atom:id')
            if identifier is None or not identifier.text:
                continue
            identifier = identifier.text
            for author_tag in parser._xpath(entry, 'atom:author'):
                subtag = parser.text_of_optional_subtag
                sort_name = subtag(author_tag, 'simplified:sort_name')

                # TODO: Also collect VIAF and LC numbers if present.
                # Only the metadata wrangler will provide this
                # information.

                # Look up or create a Contributor for this person.
                contributor, is_new = Contributor.lookup(_db, sort_name)
                contributor = contributor[0]

                # Set additional information for this person, if present
                # and not already set.
                if not contributor.display_name:
                    contributor.display_name = subtag(author_tag, 'atom:name')
                if not contributor.family_name:
                    contributor.family_name = subtag(
                        author_tag, "simplified:family_name")
                if not contributor.family_name:
                    contributor.wiki_name = subtag(
                        author_tag, "simplified:wikipedia_name")

                # Record that the given Contributor is an author of this
                # entry.
                by_id[identifier].append(contributor)
        return by_id


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
            importer, imported = self.process_one_page(_db, next_link)
            if len(imported) == 0:
                # We did not see a single book on this page we haven't
                # already seen. There's no need to keep going.
                break
            next_links = importer.links_by_rel()['next']
            if not next_links:
                # We're at the end of the list. There are no more books
                # to import.
                break
            next_link = next_links[0]['href']

    def process_one_page(self, _db, url):
        response = requests.get(url)
        importer = self.import_class(_db, response.content)
        return importer, importer.import_from_feed()
