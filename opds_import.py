from nose.tools import set_trace
from StringIO import StringIO
from collections import (
    defaultdict,
    Counter,
)
import datetime
import feedparser
import requests
import urllib
from sqlalchemy.orm.session import Session

from lxml import builder, etree

from monitor import Monitor
from util import LanguageCodes
from util.xmlparser import XMLParser
from model import (
    get_one,
    get_one_or_create,
    Contributor,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Measurement,
    Representation,
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
        args = "&".join(set(["urn=%s" % i.urn for i in identifiers]))
        url = self.base_url + self.LOOKUP_ENDPOINT + "?" + args
        print "Lookup URL: %s" % url
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

    NAMESPACES = { "simplified": "http://librarysimplified.org/terms/",
                   "app" : "http://www.w3.org/2007/app",
                   "dcterms" : "http://purl.org/dc/terms/",
                   "opds": "http://opds-spec.org/2010/catalog",
                   "schema" : "http://schema.org/",
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
   
    def __init__(self, _db, feed, data_source_name=DataSource.METADATA_WRANGLER,
                 overwrite_rels=None):
        self._db = _db
        self.raw_feed = unicode(feed)
        self.feedparser_parsed = feedparser.parse(self.raw_feed)
        self.data_source = DataSource.lookup(self._db, data_source_name)
        if overwrite_rels is None:
            overwrite_rels = [
                Hyperlink.OPEN_ACCESS_DOWNLOAD, Hyperlink.IMAGE,
                Hyperlink.DESCRIPTION
            ]
        self.overwrite_rels = overwrite_rels

    def import_from_feed(self):
        imported = []
        messages_by_id = dict()
        for entry in self.feedparser_parsed['entries']:
            opds_id, edition, edition_was_new, status_code, message = self.import_from_feedparser_entry(
                entry)
            if not edition and status_code == 200:
                print "EDITION NOT CREATED: %s" % message
                print "Raw data: %r" % entry
            set_trace()
            if edition:
                imported.append(edition)

                # This may or may not make the work
                # presentation-ready--it depends on whether we've
                # talked to the metadata wrangler.
                edition.calculate_presentation()
                if edition.sort_author:
                    work, is_new = edition.license_pool.calculate_work(
                        known_edition=edition)
                    pool = edition.license_pool
                    # TODO: If the edition was just created, it's
                    # likely that edition.license_pool works but
                    # edition.license_pool.edition is None. Passing in
                    # known_edition is a bit of a hack but so is the
                    # only other solution I could find, doing a
                    # database commit. I'm sure there's a better
                    # solution though.
                    work, is_new = pool.calculate_work(known_edition=edition)
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

        license_source_name = entry.get('simplified_license_source', None)
        if license_source_name:
            license_data_source = DataSource.lookup(
                self._db, license_source_name)
        else:
            license_data_source = DataSource.license_source_for(
                self._db, identifier)

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
            self._db, LicensePool, data_source=license_data_source,
            identifier=identifier)

        links_by_rel = self.links_by_rel(entry)
        if pool:
            pool_was_new = False
        else:
            # There is no existing license pool for this book. Can we
            # just create one?
            if links_by_rel[Hyperlink.OPEN_ACCESS_DOWNLOAD]:
                # Yes. This is an open-access book and we know where
                # you can download it.
                pool, pool_was_new = LicensePool.for_foreign_id(
                    self._db, license_data_source, identifier.type,
                    identifier.identifier)
            else:
                # No, we can't. This most likely indicates a problem.
                message = message or self.COULD_NOT_CREATE_LICENSE_POOL
                return (identifier, None, False, status_code, message)

        if pool_was_new:
            pool.open_access = True

        # Create or retrieve an Edition for this book.
        #
        # TODO: It would be better if we could use self.data_source
        # here, not license_data_source. This is the metadata
        # wrangler's view of the book, not the license provider's. But
        # we can't currently associate an Edition from one data source
        # with a LicensePool from another data source.
        edition, edition_was_new = Edition.for_foreign_id(
            self._db, license_data_source, identifier.type, identifier.identifier)

        source_last_updated = entry.get('updated_parsed')
        # TODO: I'm not really happy with this but it will work as long
        # as the times are in UTC, possibly as long as the times are in
        # the same timezone.
        source_last_updated = datetime.datetime(*source_last_updated[:6])
        if not pool_was_new and not edition_was_new and edition.work and edition.work.last_update_time >= source_last_updated:
            # The metadata has not changed since last time
            return identifier, edition, False, status_code, message

        self.destroy_resources(identifier, self.overwrite_rels)

        # Again, the links come from the OPDS feed, not from the entity
        # providing the original license.
        download_links, image_link, thumbnail_link = self.set_resources(
            self.data_source, identifier, pool, links_by_rel)

        # If there's a summary, add it to the identifier.
        summary = entry.get('summary_detail', {})
        rel = Hyperlink.DESCRIPTION
        if 'value' in summary and summary['value']:
            value = summary['value']
            uri = Hyperlink.generic_uri(self.data_source, identifier, rel,
                                        value)
            pool.add_link(
                rel, uri, self.data_source,
                summary.get('type', 'text/plain'), value)
        for content in entry.get('content', []):
            value = content['value']
            if not value:
                continue
            uri = Hyperlink.generic_uri(self.data_source, identifier, rel,
                                        value)
            pool.add_link(
                rel, uri, self.data_source,
                summary.get('type', 'text/html'), value)
            

        edition.title = title
        edition.language = language
        edition.publisher = publisher

        # Assign the LicensePool to a Work.
        work = pool.calculate_work(known_edition=edition)

        return identifier, edition, edition_was_new, status_code, message

    def destroy_resources(self, identifier, rels):
        # Remove all existing downloads and images, and descriptions
        # so as to avoid keeping old stuff around.
        for resource in Identifier.resources_for_identifier_ids(
                self._db, [identifier.id], rels):
            for l in resource.links:
                self._db.delete(l)
            self._db.delete(resource)

    def set_resources(self, data_source, identifier, pool, links):
        # Associate covers and downloads with the identifier.
        #
        # If there is both a full image and a thumbnail, we need
        # to make sure they're put into the same representation.
        download_links = []
        image_link = None
        thumbnail_link = None

        _db = Session.object_session(data_source)
        
        for link in links[Hyperlink.OPEN_ACCESS_DOWNLOAD]:
            type = link.get('type', None)
            if type == 'text/html':
                # Feedparser fills this in and it's just wrong.
                type = None
            url = link['href']
            hyperlink, was_new = pool.add_link(
                Hyperlink.OPEN_ACCESS_DOWNLOAD, url, data_source, type)
            hyperlink.resource.set_mirrored_elsewhere(type)
            download_links.append(hyperlink)

        for rel in (Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE):
            for link in links[rel]:
                type = link.get('type', None)
                if type == 'text/html':
                    # Feedparser fills this in and it's just wrong.
                    type = None
                url = link['href']
                hyperlink, was_new = pool.add_link(
                    rel, url, data_source, type)
                hyperlink.resource.set_mirrored_elsewhere(type)
                if rel == Hyperlink.IMAGE:
                    image_link = hyperlink
                else:
                    thumbnail_link = hyperlink

        if image_link and thumbnail_link and image_link.resource:
            if image_link.resource == thumbnail_link.resource:
                # TODO: This is hacky. We can't represent an image as a thumbnail
                # of itself, so we make sure the height is set so that
                # we'll know that it doesn't need a thumbnail.
                image_link.resource.representation.image_height = Edition.MAX_THUMBNAIL_HEIGHT
            else:
                # Represent the thumbnail as a thumbnail of the image.
                thumbnail_link.resource.representation.thumbnail_of = image_link.resource.representation
        return download_links, image_link, thumbnail_link

class DetailedOPDSImporter(BaseOPDSImporter):

    """An OPDS importer that imports authors as contributors and
    tags as subjects.

    It also imports any provided quality score, popularity score, or
    rating.

    This should be used by circulation managers when talking to the
    metadata wrangler, and by the metadata wrangler when talking to
    content servers.

    """

    def __init__(self, _db, feed,
                 data_source_name=DataSource.METADATA_WRANGLER,
                 overwrite_rels=None):
        super(DetailedOPDSImporter, self).__init__(_db, feed, data_source_name, overwrite_rels)
        self.lxml_parsed = etree.fromstring(self.raw_feed)
        self.medium_by_id, self.authors_by_id, self.subject_names_by_id, self.subject_weights_by_id, self.ratings_by_id = self.authors_and_subjects_by_id(
            _db, self.lxml_parsed)

    def import_from_feedparser_entry(self, entry):
        identifier, edition, edition_was_new, status_code, message = super(
            DetailedOPDSImporter, self).import_from_feedparser_entry(entry)

        if not edition:
            # No edition was created. Nothing to do.
            return identifier, edition, edition_was_new, status_code, message

        edition.medium = self.medium_by_id.get(entry.id)

        # Remove any old contributors and subjects.
        removed_contributions = 0
        contributions = edition.contributions
        for contribution in list(contributions):
            self._db.delete(contribution)
            removed_contributions += 1
        edition.contributions = []

        removed_classifications = 0
        new_set = []
        for classification in identifier.classifications:
            if classification.data_source == self.data_source:
                self._db.delete(classification)
                removed_classifications += 1
            else:
                new_set.append(classification)
        identifier.classifications = new_set

        print "Deleted %d contributions and %d classifications." % (
            removed_contributions, removed_classifications)

        for contributor in self.authors_by_id.get(entry.id, []):
            edition.add_contributor(contributor, Contributor.AUTHOR_ROLE)

        weights = self.subject_weights_by_id.get(entry.id, {})
        for key, weight in weights.items():
            type, term = key
            name = self.subject_names_by_id.get(entry.id).get(key, None)
            identifier.classify(
                self.data_source, type, term, name, weight=weight)

        print "Added %d contributions and %d classifications." % (
            len(edition.contributors), len(identifier.classifications)
        )

        ratings = self.ratings_by_id.get(entry.id, {})
        for rel, value in ratings.items():
            if not rel:
                rel = Measurement.RATING
            identifier.add_measurement(self.data_source, rel, value)

        return identifier, edition, edition_was_new, status_code, message

    @classmethod
    def authors_and_subjects_by_id(cls, _db, root):
        """Parse the OPDS as XML and extract all author and subject
        information, as well as ratings and medium.

        All the stuff that Feedparser can't handle so we have to use lxml.
        """
        parser = OPDSXMLParser()
        default_additional_type = Edition.medium_to_additional_type[Edition.BOOK_MEDIUM]
        medium_by_id = {}
        authors_by_id = defaultdict(list)
        subject_names_by_id = defaultdict(dict)
        subject_weights_by_id = defaultdict(Counter)
        ratings_by_id = defaultdict(dict)
        for entry in parser._xpath(root, '/atom:feed/atom:entry'):
            identifier = parser._xpath1(entry, 'atom:id')
            if identifier is None or not identifier.text:
                continue
            identifier = identifier.text
            additional_type = entry.get('{http://schema.org/}additionalType', 
                                        default_additional_type)
            medium = Edition.additional_type_to_medium.get(additional_type)
            medium_by_id[identifier] = medium

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
                authors_by_id[identifier].append(contributor)

            for subject_tag in parser._xpath(entry, 'atom:category'):
                a = subject_tag.attrib
                scheme = a.get('scheme')
                subject_type = Subject.by_uri.get(scheme)
                if not subject_type:
                    # We can't represent this subject because we don't
                    # know its scheme. Just treat it as a tag.
                    subject_type = Subject.TAG

                term = a.get('term')
                name = a.get('label')
                default_weight = 1
                if subject_type in (
                    Subject.FREEFORM_AUDIENCE, Subject.AGE_RANGE):
                    default_weight = 100

                weight = a.get('{http://schema.org/}ratingValue', default_weight)
                try:
                    weight = int(weight)
                except ValueError, e:
                    weight = 1
                key = (subject_type, term)
                subject_names_by_id[identifier][key] = name
                subject_weights_by_id[identifier][key] += weight

            for rating_tag in parser._xpath(entry, 'schema:Rating'):
                type = rating_tag.get('{http://schema.org/}additionalType')
                value = rating_tag.get('{http://schema.org/}ratingValue')
                try:
                    value = float(value)
                    ratings_by_id[identifier][type] = value
                except ValueError:
                    pass

        return medium_by_id, authors_by_id, subject_names_by_id, subject_weights_by_id, ratings_by_id


class OPDSImportMonitor(Monitor):
    """Periodically monitor an OPDS archive feed and import every edition
    it mentions.
    """
    
    def __init__(self, _db, feed_url, import_class, interval_seconds=3600,
                 keep_timestamp=True):
        self.feed_url = feed_url
        self.import_class = import_class
        super(OPDSImportMonitor, self).__init__(
            _db, "OPDS Import %s" % feed_url, interval_seconds,
            keep_timestamp=keep_timestamp)

    def run_once(self, start, cutoff):
        next_link = self.feed_url
        seen_links = set([next_link])
        while next_link:
            print next_link
            importer, imported = self.process_one_page(next_link)
            self._db.commit()
            #if len(imported) == 0:
            #    # We did not see a single book on this page we haven't
            #    # already seen. There's no need to keep going.
            #    break
            next_links = importer.links_by_rel()['next']
            if not next_links:
                # We're at the end of the list. There are no more books
                # to import.
                break
            next_link = next_links[0]['href']
            if next_link in seen_links:
                # Loop.
                break
        self._db.commit()

    def process_one_page(self, url):
        response = requests.get(url)
        importer = self.import_class(self._db, response.content)
        return importer, importer.import_from_feed()
