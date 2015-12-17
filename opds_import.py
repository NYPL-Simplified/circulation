from nose.tools import set_trace
from StringIO import StringIO
from collections import (
    defaultdict,
    Counter,
)
import datetime
import feedparser
import logging
import requests
import urllib
from sqlalchemy.orm.session import Session

from lxml import builder, etree

from monitor import Monitor
from util import LanguageCodes
from util.xmlparser import XMLParser
from config import Configuration
from metadata_layer import (
    Metadata,
    IdentifierData,
    ContributorData,
    LinkData,
    MeasurementData,
    SubjectData,
)
from model import (
    get_one,
    get_one_or_create,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Measurement,
    Representation,
    Resource,
    Subject,
)
from opds import OPDSFeed

class SimplifiedOPDSLookup(object):
    """Tiny integration class for the Simplified 'lookup' protocol."""

    LOOKUP_ENDPOINT = "lookup"
    CANONICALIZE_ENDPOINT = "canonical-author-name"

    @classmethod
    def from_config(cls, integration='Metadata Wrangler'):
        url = Configuration.integration_url(integration)
        return cls(url)

    def __init__(self, base_url):
        if not base_url.endswith('/'):
            base_url += "/"
        self.base_url = base_url

    def lookup(self, identifiers):
        """Retrieve an OPDS feed with metadata for the given identifiers."""
        args = "&".join(set(["urn=%s" % i.urn for i in identifiers]))
        url = self.base_url + self.LOOKUP_ENDPOINT + "?" + args
        logging.info("Lookup URL: %s", url)
        return requests.get(url)

    def canonicalize_author_name(self, identifier, working_display_name):
        """Attempt to find the canonical name for the author of a book.

        :param identifier: an ISBN-type Identifier.

        :param working_display_name: The display name of the author
        (i.e. the name format human being used as opposed to the name
        that goes into library records).
        """
        args = "display_name=%s" % (
            urllib.quote(
                working_display_name.encode("utf8"))
        )
        if identifier:
            args += "&urn=%s" % urllib.quote(identifier.urn)
        url = self.base_url + self.CANONICALIZE_ENDPOINT + "?" + args
        logging.info("GET %s", url)
        return requests.get(url)

class OPDSXMLParser(XMLParser):

    NAMESPACES = { "simplified": "http://librarysimplified.org/terms/",
                   "app" : "http://www.w3.org/2007/app",
                   "dcterms" : "http://purl.org/dc/terms/",
                   "opds": "http://opds-spec.org/2010/catalog",
                   "schema" : "http://schema.org/",
                   "atom" : "http://www.w3.org/2005/Atom",
    }

class StatusMessage(object):

    def __init__(self, status_code, message):
        try:
            status_code = int(status_code)
            success = (status_code == 200)
        except ValueError, e:
            # The status code isn't a number. Leave it alone.
            success = False
        self.status_code = status_code
        self.message = message
        self.success = success


class OPDSImporter(object):

    """Capable of importing editions from an OPDS feed.

    This importer should be used when a circulation server
    communicates with a content server. It ignores author and subject
    information, under the assumption that it will get better author
    and subject information from the metadata wrangler.
    """
    COULD_NOT_CREATE_LICENSE_POOL = (
        "No existing license pool for this identifier and no way of creating one.")
   
    def __init__(self, _db, feed, data_source_name=DataSource.METADATA_WRANGLER,
                 overwrite_rels=None, identifier_mapping=None, force=True):
        self._db = _db
        self.force = True
        self.log = logging.getLogger("OPDS Importer")
        self.raw_feed = unicode(feed)
        self.feedparser_parsed = feedparser.parse(self.raw_feed)
        self.data_source = DataSource.lookup(self._db, data_source_name)
        self.identifier_mapping = identifier_mapping
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
            internal_id, opds_id, edition, edition_was_new, status_code, message = self.import_from_feedparser_entry(
                entry)
            if not edition and status_code == 200:
                self.log.info("EDITION NOT CREATED: %s. Raw data: %r",
                              message, entry)
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
                    work, is_new = pool.calculate_work(
                        known_edition=edition, even_if_no_author=True
                    )
                    if work:
                        work.calculate_presentation()
            elif status_code:
                messages_by_id[opds_id] = (status_code, message)
        return imported, messages_by_id

    def import_from_feedparser_entry(self, entry):
        external_identifier, ignore = Identifier.parse_urn(
            self._db, entry.get('id'))
        if self.identifier_mapping:
             internal_identifier = self.identifier_mapping.get(
                 external_identifier, external_identifier)
        else:
            internal_identifier = external_identifier
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
            return internal_identifier, external_identifier, None, False, status_code, message

        # Get an existing LicensePool for this book.
        pool = None
        license_data_source = None
        for license_data_source in license_data_sources:
            pool = get_one(
                self._db, LicensePool, data_source=license_data_source,
                identifier=internal_identifier)
            if pool:
                break

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
                    self._db, license_data_source, internal_identifier.type,
                    internal_identifier.identifier)
            else:
                # No, we can't. This most likely indicates a problem.
                message = message or self.COULD_NOT_CREATE_LICENSE_POOL
                return (internal_identifier, external_identifier, None,
                        False, status_code, message)

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
            self._db, license_data_source, internal_identifier.type,
            internal_identifier.identifier)

        source_last_updated = entry.get('updated_parsed')
        # TODO: I'm not really happy with this but it will work as long
        # as the times are in UTC, possibly as long as the times are in
        # the same timezone.
        if source_last_updated:
            source_last_updated = datetime.datetime(*source_last_updated[:6])
            if not pool_was_new and not edition_was_new and edition.work and edition.work.last_update_time >= source_last_updated and not self.force:
                # The metadata has not changed since last time
                return (internal_identifier, external_identifier, edition, 
                        False, status_code, message)

        self.destroy_resources(internal_identifier, self.overwrite_rels)

        # Again, the links come from the OPDS feed, not from the entity
        # providing the original license.
        download_links, image_link, thumbnail_link = self.set_resources(
            self.data_source, internal_identifier, pool, links_by_rel)

        # Assign the LicensePool to a Work.
        work = pool.calculate_work(known_edition=edition)
        return (internal_identifier, external_identifier, edition, 
                edition_was_new, status_code, message)

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
            pool.set_delivery_mechanism(
                type, DeliveryMechanism.NO_DRM, hyperlink.resource)
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

    @classmethod
    def extract_metadata(self, metadata_data_source, feed):
        """Turn an OPDS feed into a list of Metadata objects and a list of
        messages.

        :param metadata_data_source: Where does the metadata come
        from?  This data source will be used as the source for links,
        classifications, etc., in the absence of any indicator as to
        where the books themselves come from.
        """
        raw_feed = unicode(feed)       
        data1, status_messages = self.extract_metadata_from_feedparser(feed)
        data2 = self.extract_metadata_from_elementtree(feed)

        metadata = []
        for id, args in data1.items():
            other_args = data2.get(id, {})
            combined = self.combine(args, other_args)
            if combined.get('data_source') is None:
                combined['data_source'] = metadata_data_source
            metadata.append(Metadata(**combined))
        return metadata, status_messages

    @classmethod
    def combine(self, d1, d2):
        """Combine two dictionaries that can be used as keyword arguments to
        the Metadata constructor.
        """
        new_dict = dict(d1)
        for k, v in d2.items():
            if k in new_dict and isinstance(v, list):
                new_dict[k].extend(v)
            else:
                new_dict[k] = v
        return new_dict


    @classmethod
    def extract_metadata_from_feedparser(cls, feed):
        feedparser_parsed = feedparser.parse(feed)
        values = {}
        status_messages = {}
        for entry in feedparser_parsed['entries']:
            identifier, detail, status_message = cls.detail_for_feedparser_entry(entry)
            if identifier:
                if detail:
                    values[identifier] = detail
                if status_message:
                    status_messages[identifier] = status_message
        return values, status_messages

    @classmethod
    def extract_metadata_from_elementtree(cls, feed):
        """Parse the OPDS as XML and extract all author and subject
        information, as well as ratings and medium.

        All the stuff that Feedparser can't handle so we have to use lxml.

        :return: a dictionary mapping IDs to dictionaries. The inner
        dictionary can be used as keyword arguments to the Metadata
        constructor.
        """
        values = {}
        parser = OPDSXMLParser()
        root = etree.parse(StringIO(feed))
        for entry in parser._xpath(root, '/atom:feed/atom:entry'):
            identifier, detail = cls.detail_for_elementtree_entry(parser, entry)
            if identifier:
                values[identifier] = detail
        return values

    @classmethod
    def detail_for_feedparser_entry(cls, entry):
        """Turn an entry dictionary created by feedparser into a dictionary of
        metadata that can be used as keyword arguments to the Metadata
        contructor.

        :return: A 3-tuple (identifier, kwargs, status message)
        """
        identifier = entry['id']
        if not identifier:
            return None, None, None

        status_message = None
        status_code = entry.get('simplified_status_code', None)
        message = entry.get('simplified_message', None)
        if status_code is not None:
            status_message = StatusMessage(status_code, message)

        if status_message and not status_message.success:
            return identifier, None, status_message

        # At this point we can assume that we successfully got some
        # metadata, and possibly a link to the actual book.

        # If this is present, it means that the entry also includes
        # information about an active distributor of this book.  Any
        # Edition created from this metadata should use the
        # distributor of the book as its data source.
        data_source = None
        distribution_tag = entry.get('bibframe_distribution', None)
        if distribution_tag:
            data_source = distribution_tag.get('bibframe:providername')

        title = entry.get('title', None)
        if title == OPDSFeed.NO_TITLE:
            title = None
        subtitle = entry.get('alternativeheadline', None)

        # TODO: updated needs to find its way to Work.last_update_time
        updated = entry.get('updated_parsed', None)

        publisher = entry.get('dcterms_publisher', None)
        language = entry.get('dcterms_language', None)

        links = []

        def summary_to_linkdata(detail):
            if not detail:
                return None
            if not 'value' in detail or not detail['value']:
                return None

            content = detail['value']
            media_type = detail.get('type', 'text/plain')
            return LinkData(
                rel=Hyperlink.DESCRIPTION,
                media_type=media_type,
                content=content
            )
            
        summary_detail = entry.get('summary_detail', None)
        link = summary_to_linkdata(summary_detail)
        if link:
            links.append(link)

        for content_detail in entry.get('content', []):
            link = summary_to_linkdata(content_detail)
            if link:
                links.append(link)

        kwargs = dict(
            data_source=data_source,
            title=title,
            subtitle=subtitle,
            language=language,
            publisher=publisher,
            links=links
        )
        return identifier, kwargs, status_message

    @classmethod
    def detail_for_elementtree_entry(cls, parser, entry_tag):

        """Turn an <atom:entry> tag into a dictionary of metadata that can be
        used as keyword arguments to the Metadata contructor.

        :return: A 2-tuple (identifier, kwargs)
        """

        identifier = parser._xpath1(entry_tag, 'atom:id')
        if identifier is None or not identifier.text:
            # This <entry> tag doesn't identify a book so we 
            # can't derive any information from it.
            return None, None
        identifier = identifier.text

        # We will fill this dictionary with all the information
        # we can find.
        data = dict()
           
        data['medium'] = cls.extract_medium(entry_tag)
        
        data['contributors'] = [
            cls.extract_contributor(parser, author_tag)
            for author_tag in parser._xpath(entry_tag, 'atom:author')
        ]

        data['subjects'] = [
            cls.extract_subject(parser, category_tag)
            for category_tag in parser._xpath(entry_tag, 'atom:category')
        ]

        ratings = []
        for rating_tag in parser._xpath(entry_tag, 'schema:Rating'):
            v = cls.extract_measurement(rating_tag)
            if v:
                ratings.append(v)
        data['measurements'] = ratings

        data['links'] = [
            cls.extract_link(link_tag)
            for link_tag in parser._xpath(entry_tag, 'atom:link')
        ]

        return identifier, data

    @classmethod
    def extract_medium(cls, entry_tag):
        """Derive a value for Edition.medium from <atom:entry
        schema:additionalType>.
        """

        # If no additionalType is given, assume we're talking about an
        # ebook.
        default_additional_type = Edition.medium_to_additional_type[
            Edition.BOOK_MEDIUM
        ]
        additional_type = entry_tag.get('{http://schema.org/}additionalType', 
                                        default_additional_type)
        return Edition.additional_type_to_medium.get(additional_type)

    @classmethod
    def extract_contributor(cls, parser, author_tag):
        """Turn an <atom:author> tag into a ContributorData object."""
        subtag = parser.text_of_optional_subtag
        sort_name = subtag(author_tag, 'simplified:sort_name')
        display_name = subtag(author_tag, 'atom:name')
        family_name = subtag(author_tag, "simplified:family_name")
        wikipedia_name = subtag(author_tag, "simplified:wikipedia_name")

        # TODO: we need a way of conveying roles. I believe Bibframe
        # has the answer.

        # TODO: Also collect VIAF and LC numbers if present.  This
        # requires parsing the URIs. Only the metadata wrangler will
        # provide this information.

        return ContributorData(
            sort_name=sort_name, display_name=display_name,
            family_name=family_name,
            wikipedia_name=wikipedia_name,
            roles=None
        )

    @classmethod
    def extract_subject(cls, parser, category_tag):
        """Turn an <atom:category> tag into a SubjectData object."""
        attr = category_tag.attrib

        # Retrieve the type of this subject - FAST, Dewey Decimal,
        # etc.
        scheme = attr.get('scheme')
        subject_type = Subject.by_uri.get(scheme)
        if not subject_type:
            # We can't represent this subject because we don't
            # know its scheme. Just treat it as a tag.
            subject_type = Subject.TAG

        # Retrieve the term (e.g. "827") and human-readable name
        # (e.g. "English Satire & Humor") for this subject.
        term = attr.get('term')
        name = attr.get('label')
        default_weight = 1
        if subject_type in (
                Subject.FREEFORM_AUDIENCE, Subject.AGE_RANGE
        ):
            default_weight = 100

        weight = attr.get('{http://schema.org/}ratingValue', default_weight)
        try:
            weight = int(weight)
        except ValueError, e:
            weight = 1

        return SubjectData(
            type=subject_type, 
            identifier=term,
            name=name, 
            weight=weight
        )

    @classmethod
    def extract_link(cls, link_tag):
        attr = link_tag.attrib
        rel = attr.get('rel')
        media_type = attr.get('type')
        href = attr.get('href')
        return LinkData(rel=rel, href=href, media_type=media_type)

    @classmethod
    def extract_measurement(cls, rating_tag):
        type = rating_tag.get('{http://schema.org/}additionalType')
        value = rating_tag.get('{http://schema.org/}ratingValue')
        try:
            value = float(value)
            return MeasurementData(
                quantity_measured=type, 
                value=value,
            )
        except ValueError:
            return None


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
            self.log.info("Following next link: %s", next_link)
            importer, imported = self.process_one_page(next_link)
            self._db.commit()
            if len(imported) == 0:
                # We did not see a single book on this page we haven't
                # already seen. There's no need to keep going.
                self.log.info(
                    "Saw a full page with no new books. Stopping.")
                break
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


