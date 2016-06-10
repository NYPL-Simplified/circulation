from nose.tools import set_trace
from StringIO import StringIO
from collections import (
    defaultdict,
    Counter,
)
import datetime
import feedparser
import logging
import traceback
import urllib
from urlparse import urlparse, urljoin
from sqlalchemy.orm.session import Session

from lxml import builder, etree

from monitor import Monitor
from util import LanguageCodes
from util.xmlparser import XMLParser
from config import (
    Configuration,
    CannotLoadConfiguration,
)
from metadata_layer import (
    CirculationData,
    Metadata,
    IdentifierData,
    ContributorData,
    LinkData,
    MeasurementData,
    SubjectData,
    ReplacementPolicy,
)
from model import (
    get_one,
    get_one_or_create,
    Contributor,
    CoverageRecord,
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
    RightsStatus,
)
from util.http import HTTP
from opds import OPDSFeed
from s3 import S3Uploader

class AccessNotAuthenticated(Exception):
    """No authentication is configured for this service"""
    pass


class DBImportException(Exception):
    """Problem creating SQLAlchemy object."""
    pass


class SimplifiedOPDSLookup(object):
    """Tiny integration class for the Simplified 'lookup' protocol."""

    LOOKUP_ENDPOINT = "lookup"
    CANONICALIZE_ENDPOINT = "canonical-author-name"
    UPDATES_ENDPOINT = "updates"
    REMOVAL_ENDPOINT = "remove"

    @classmethod
    def from_config(cls, integration='Metadata Wrangler'):
        url = Configuration.integration_url(integration)
        if not url:
            return None
        return cls(url)

    def __init__(self, base_url):
        if not base_url.endswith('/'):
            base_url += "/"
        self.base_url = base_url
        self._set_auth()

    def _set_auth(self):
        """Sets client authentication details for the Metadata Wrangler"""

        metadata_wrangler_url = Configuration.integration_url(
            Configuration.METADATA_WRANGLER_INTEGRATION
        )
        self.client_id = self.client_secret = None
        if (metadata_wrangler_url
            and self.base_url.startswith(metadata_wrangler_url)):
            values = Configuration.integration(Configuration.METADATA_WRANGLER_INTEGRATION)
            self.client_id = values.get(Configuration.METADATA_WRANGLER_CLIENT_ID)
            self.client_secret = values.get(Configuration.METADATA_WRANGLER_CLIENT_SECRET)

            details = [self.client_id, self.client_secret]
            if len([d for d in details if not d]) == 1:
                # Raise an error if one is set, but not the other.
                raise CannotLoadConfiguration("Metadata Wrangler improperly configured.")

    @property
    def authenticated(self):
        return bool(self.client_id and self.client_secret)

    def _get(self, url, **kwargs):
        """Make an HTTP request. This method is overridden in the mock class."""
        return HTTP.get_with_timeout(url, **kwargs)

    def opds_get(self, url):
        """Make the sort of HTTP request that's normal for an OPDS feed.

        Long timeout, raise error on anything but 2xx or 3xx.
        """
        kwargs = dict(timeout=120, allowed_response_codes=['2xx', '3xx'])
        if self.client_id and self.client_secret:
            kwargs['auth'] = (self.client_id, self.client_secret)
        return self._get(url, **kwargs)

    def lookup(self, identifiers):
        """Retrieve an OPDS feed with metadata for the given identifiers."""
        args = "&".join(set(["urn=%s" % i.urn for i in identifiers]))
        url = self.base_url + self.LOOKUP_ENDPOINT + "?" + args
        logging.info("Lookup URL: %s", url)
        return self.opds_get(url)

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
        return self._get(url)

    def remove(self, identifiers):
        """Remove items from an authenticated Metadata Wrangler collection"""

        if not self.authenticated:
            raise AccessNotAuthenticated("Metadata Wrangler Collection not authenticated.")
        args = "&".join(set(["urn=%s" % i.urn for i in identifiers]))
        url = self.base_url + self.REMOVAL_ENDPOINT + "?" + args
        logging.info("Metadata Wrangler Removal URL: %s", url)
        return self._get(url)


class MockSimplifiedOPDSLookup(SimplifiedOPDSLookup):

    def __init__(self, *args, **kwargs):
        self.responses = []
        super(MockSimplifiedOPDSLookup, self).__init__(*args, **kwargs)

    def queue_response(self, status_code, headers={}, content=None):
        from testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _get(self, url, *args, **kwargs):
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )


class OPDSXMLParser(XMLParser):

    NAMESPACES = { "simplified": "http://librarysimplified.org/terms/",
                   "app" : "http://www.w3.org/2007/app",
                   "dcterms" : "http://purl.org/dc/terms/",
                   "dc" : "http://purl.org/dc/elements/1.1/",
                   "opds": "http://opds-spec.org/2010/catalog",
                   "schema" : "http://schema.org/",
                   "atom" : "http://www.w3.org/2005/Atom",
    }


class StatusMessage(object):

    def __init__(self, status_code, message):
        try:
            status_code = int(status_code)
            success = (status_code == 200)
            transient = not success and status_code / 100 in (2, 3, 5)
        except ValueError, e:
            # The status code isn't a number. Leave it alone.
            success = False
            transient = False
        self.status_code = status_code
        self.message = message
        self.success = success
        self.transient = transient

    def __repr__(self):
        return '<StatusMessage: code=%s message="%s">' % (
            self.status_code, self.message
        )


class OPDSImporter(object):
    """ Imports editions and license pools from an OPDS feed.
    Creates Edition, LicensePool and Work rows in the database, if those 
    don't already exist.

    Should be used when a circulation server asks for data from 
    our internal content server, and also when our content server asks for data 
    from external content servers. 

    :param mirror: Use this MirrorUploader object to mirror all
    incoming open-access books and cover images.
    """

    COULD_NOT_CREATE_LICENSE_POOL = (
        "No existing license pool for this identifier and no way of creating one.")
   
    def __init__(self, _db, data_source_name=DataSource.METADATA_WRANGLER,
                 identifier_mapping=None, mirror=None, http_get=None, 
                 force=True):
        self._db = _db
        self.force = True
        self.log = logging.getLogger("OPDS Importer")
        self.data_source_name = data_source_name
        self.identifier_mapping = identifier_mapping
        self.metadata_client = SimplifiedOPDSLookup.from_config()
        self.mirror = mirror
        self.http_get = http_get


    def import_from_feed(self, feed, even_if_no_author=False, 
                         cutoff_date=None, 
                         immediately_presentation_ready=False):

        # Keep track of editions that were imported. Pools and works
        # for those editions may be looked up or created.
        imported_editions = {}
        pools = {}
        works = {}
        # status_messages notes business logic errors and non-success download statuses
        status_messages = {}

        # If parsing the overall feed throws an exception, we should address that before
        # moving on. Let the exception propagate.
        metadata_objs, status_messages = self.extract_feed_data(feed)

        # make editions.  if have problem, make sure associated pool and work aren't created.
        for key, metadata in metadata_objs.iteritems():
            # key is identifier.urn here

            # If there's a status message about this item, don't try to import it.
            if key in status_messages.keys():
                continue

            try:
                # Create an edition. This will also create a pool if there's circulation data.
                edition = self.import_edition_from_metadata(
                    metadata, even_if_no_author, cutoff_date, immediately_presentation_ready
                )
                if edition:
                    imported_editions[key] = edition
            except Exception, e:
                # Rather than scratch the whole import, treat this as a failure that only applies
                # to this item.
                message = StatusMessage(500, "Local exception during import:\n%s" % traceback.format_exc())
                status_messages[key] = message
                # clean up any edition might have created
                if key in imported_editions:
                    del imported_editions[key]
                # Move on to the next item, don't create a work.
                continue


            pool = None
            work = None
            try:
                # Find a pool for this edition. If we have CirculationData, a pool was created
                # when we imported the edition. If there was already a pool from a different data
                # source, that's fine too.
                pool = get_one(self._db, LicensePool, identifier=edition.primary_identifier)

                if pool:
                    pools[key] = pool
                        
                    # Note: pool.calculate_work will call self.set_presentation_edition(), 
                    # which will find editions attached to same Identifier.
                    work, is_new_work = pool.calculate_work(even_if_no_author=even_if_no_author)
                    # Note: if pool.calculate_work found or made a work, it already called work.calculate_presentation()
                    if work:
                        if immediately_presentation_ready:
                            # We want this book to be presentation-ready
                            # immediately upon import. As long as no crucial
                            # information is missing (like language or title),
                            # this will do it.
                            work.set_presentation_ready_based_on_content()

                        works[key] = work
                    else:
                        error_message = "No Work created for Edition %r, for an unknown reason." % edition
                        self.log.warn(error_message)
                        raise DBImportException(error_message)
            except Exception, e:
                message = StatusMessage(500, "Local exception during import:\n%s" % traceback.format_exc())
                status_messages[key] = message

        return imported_editions.values(), pools.values(), works.values(), status_messages


    def import_edition_from_metadata(
            self, metadata, even_if_no_author, cutoff_date, immediately_presentation_ready
    ):
        """ For the passed-in Metadata object, see if can find or create an Edition 
            in the database.  Do not set the edition's pool or work, yet.
        """

        # Locate or create an Edition for this book.
        edition, is_new_edition = metadata.edition(self._db)

        if (cutoff_date 
            and not is_new_edition
            and metadata.data_source_last_updated < cutoff_date
        ):
            # We've already imported this book, we've been told
            # not to bother with books that haven't changed since a
            # certain date, and this book hasn't changed since that
            # that date. There's no reason to do anything.
            return None

        policy = ReplacementPolicy(
            subjects=True,
            links=True,
            contributions=True,
            rights=True,
            link_content=True,
            even_if_not_apparently_updated=True,
            mirror=self.mirror,
            http_get=self.http_get,
        )
        metadata.apply(
            edition, self.metadata_client, replace=policy
        )

        return edition


    @classmethod
    def extract_next_links(self, feed):
        parsed = feedparser.parse(feed)
        feed = parsed['feed']
        next_links = []
        if feed and 'links' in feed:
            next_links = [
                link['href'] for link in feed['links'] 
                if link['rel'] == 'next'
            ]
        return next_links
        

    @classmethod
    def extract_last_update_dates(cls, feed):
        parsed_feed = feedparser.parse(feed)
        return [
            cls.last_update_date_for_feedparser_entry(entry)
            for entry in parsed_feed['entries']
        ]


    def extract_feed_data(self, feed):
        """Turn an OPDS feed into lists of Metadata and CirculationData objects, 
        with associated messages and next_links.
        """
        data_source = DataSource.lookup(self._db, self.data_source_name)
        fp_metadata, fp_status_messages = self.extract_data_from_feedparser(feed=feed, data_source=data_source)
        # gets: medium, measurements, links, contributors, etc.
        xml_data_meta, xml_status_messages = self.extract_metadata_from_elementtree(feed)

        # translate the id in status_messages to identifier.urn
        identified_messages = {}
        for id, message_data in fp_status_messages.items() + xml_status_messages.items():
            external_identifier, ignore = Identifier.parse_urn(self._db, id)
            if self.identifier_mapping:
                internal_identifier = self.identifier_mapping.get(
                    external_identifier, external_identifier)
            else:
                internal_identifier = external_identifier
            identified_messages[internal_identifier.urn] = message_data

        # Use one loop for both, since the id will be the same for both dictionaries.
        metadata = {}
        circulationdata = {}
        for id, m_data_dict in fp_metadata.items():
            external_identifier, ignore = Identifier.parse_urn(self._db, id)
            if self.identifier_mapping:
                internal_identifier = self.identifier_mapping.get(
                    external_identifier, external_identifier)
            else:
                internal_identifier = external_identifier

            # Don't process this item if there was already an error
            if internal_identifier.urn in identified_messages.keys():
                continue

            identifier_obj = IdentifierData(
                type=internal_identifier.type,
                identifier=internal_identifier.identifier
            )

            # form the Metadata object
            xml_data_dict = xml_data_meta.get(id, {})
            combined_meta = self.combine(m_data_dict, xml_data_dict)
            if combined_meta.get('data_source') is None:
                combined_meta['data_source'] = self.data_source_name
            
            combined_meta['primary_identifier'] = identifier_obj

            metadata[internal_identifier.urn] = Metadata(**combined_meta)

            # form the CirculationData that would correspond to this Metadata
            c_data_dict = m_data_dict.get('circulation')
            
            if c_data_dict:
                circ_links_dict = {}
                # extract just the links to pass to CirculationData constructor
                if 'links' in xml_data_dict:
                    circ_links_dict['links'] = xml_data_dict['links']
                combined_circ = self.combine(c_data_dict, circ_links_dict)
                if combined_circ.get('data_source') is None:
                    combined_circ['data_source'] = self.data_source_name
            
                combined_circ['primary_identifier'] = identifier_obj
                
                metadata[internal_identifier.urn].circulation = CirculationData(**combined_circ)

        return metadata, identified_messages


    @classmethod
    def combine(self, d1, d2):
        """Combine two dictionaries that can be used as keyword arguments to
        the Metadata constructor.
        """
        new_dict = dict(d1)
        for k, v in d2.items():
            if k in new_dict and isinstance(v, list):
                new_dict[k].extend(v)
            elif k not in new_dict or v != None:
                new_dict[k] = v
        return new_dict


    @classmethod
    def extract_data_from_feedparser(cls, feed, data_source):
        feedparser_parsed = feedparser.parse(feed)
        values = {}
        status_messages = {}
        for entry in feedparser_parsed['entries']:
            identifier, detail, status_message = cls.data_detail_for_feedparser_entry(entry=entry, data_source=data_source)

            if identifier:
                if status_message:
                    status_messages[identifier] = status_message
                else:
                    if detail:
                        values[identifier] = detail
            else:
                # That's bad. Can't make an item-specific error message, but write to 
                # log that something very wrong happened.
                logging.error("Tried to parse an element without a valid identifier.  feed=%s" % feed)

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
        status_messages = {}
        parser = OPDSXMLParser()
        root = etree.parse(StringIO(feed))

        # Some OPDS feeds (eg Standard Ebooks) contain relative urls, so we need the
        # feed's self URL to extract links.
        links = [child.attrib for child in root.getroot() if 'link' in child.tag]
        self_links = [link['href'] for link in links if link.get('rel') == 'self']
        if self_links:
            feed_url = self_links[0]
        else:
            feed_url = None

        for entry in parser._xpath(root, '/atom:feed/atom:entry'):
            identifier, detail, status_message = cls.detail_for_elementtree_entry(parser, entry, feed_url)
            if identifier:
                if status_message:
                    status_messages[identifier] = status_message
                values[identifier] = detail
        return values, status_messages

    @classmethod
    def _datetime(cls, entry, key):
        value = entry.get(key, None)
        if not value:
            return value
        return datetime.datetime(*value[:6])


    @classmethod
    def last_update_date_for_feedparser_entry(cls, entry):
        identifier = entry.get('id')
        updated = cls._datetime(entry, 'updated_parsed')
        return (identifier, updated)

    @classmethod
    def data_detail_for_feedparser_entry(cls, entry, data_source):
        """Turn an entry dictionary created by feedparser into dictionaries of data
        that can be used as keyword arguments to the Metadata and CirculationData constructors.

        :return: A 4-tuple (identifier, kwargs for Metadata constructor,  
            kwargs for CirculationData constructor, status message)
        """
        identifier = entry.get('id')
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

        # Note: will ignore bibframe_distribution from feed, and use data source passed into the 
        # importer as license_data_source, too.

        try:
            title = entry.get('title', None)
            if title == OPDSFeed.NO_TITLE:
                title = None
            subtitle = entry.get('schema_alternativeheadline', None)

            last_opds_update = cls._datetime(entry, 'updated_parsed')
            added_to_collection_time = cls._datetime(entry, 'published_parsed')
            
            publisher = entry.get('publisher', None)
            if not publisher:
                publisher = entry.get('dcterms_publisher', None)

            language = entry.get('language', None)
            if not language:
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

            rights = entry.get('rights', "")
            rights_uri = RightsStatus.rights_uri_from_string(rights)

            kwargs_meta = dict(
                title=title,
                subtitle=subtitle,
                language=language,
                publisher=publisher,
                links=links,
                # refers to when was updated in opds feed, not our db
                data_source_last_updated=last_opds_update,
            )
            
            # Only add circulation data if the data source is lendable.
            if data_source.offers_licenses:
                kwargs_circ = dict(
                    data_source=data_source.name,
                    links=list(links),
                    default_rights_uri=rights_uri,
                )
                
                kwargs_meta['circulation'] = kwargs_circ
            return identifier, kwargs_meta, status_message

        except Exception, e:
            message = StatusMessage(500, "Local exception during import:\n%s" % traceback.format_exc())
            return identifier, None, message


    @classmethod
    def detail_for_elementtree_entry(cls, parser, entry_tag, feed_url=None):

        """Turn an <atom:entry> tag into a dictionary of metadata that can be
        used as keyword arguments to the Metadata contructor.

        :return: A 2-tuple (identifier, kwargs)
        """

        identifier = parser._xpath1(entry_tag, 'atom:id')
        if identifier is None or not identifier.text:
            # This <entry> tag doesn't identify a book so we 
            # can't derive any information from it.
            return None, None, None
        identifier = identifier.text

        try:
            # We will fill this dictionary with all the information
            # we can find.
            data = dict()

            alternate_identifiers = []
            for id_tag in parser._xpath(entry_tag, "dcterms:identifier"):
                v = cls.extract_identifier(id_tag)
                if v:
                    alternate_identifiers.append(v)
            data['identifiers'] = alternate_identifiers
           
            data['medium'] = cls.extract_medium(entry_tag)
        
            data['contributors'] = []
            for author_tag in parser._xpath(entry_tag, 'atom:author'):
                contributor = cls.extract_contributor(parser, author_tag)
                if contributor is not None:
                    data['contributors'].append(contributor)

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

            data['links'] = cls.consolidate_links([
                cls.extract_link(link_tag, feed_url)
                for link_tag in parser._xpath(entry_tag, 'atom:link')
            ])
        
            return identifier, data, None

        except Exception, e:
            message = StatusMessage(500, "Local exception during import:\n%s" % traceback.format_exc())
            return identifier, None, message

    @classmethod
    def extract_identifier(cls, identifier_tag):
        """Turn a <dcterms:identifier> tag into an IdentifierData object."""
        try:
            type, identifier = Identifier.type_and_identifier_for_urn(identifier_tag.text.lower())
            return IdentifierData(type, identifier)
        except ValueError:
            return None

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

        viaf = None
        if sort_name or display_name or viaf:
            return ContributorData(
                sort_name=sort_name, display_name=display_name,
                family_name=family_name,
                wikipedia_name=wikipedia_name,
                roles=None
            )

        logging.info("Refusing to create ContributorData for contributor with no sort name, display name, or VIAF.")
        return None

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
    def extract_link(cls, link_tag, feed_url=None):
        attr = link_tag.attrib
        rel = attr.get('rel')
        media_type = attr.get('type')
        href = attr.get('href')
        rights = attr.get('{%s}rights' % OPDSXMLParser.NAMESPACES["dcterms"])
        if rights:
            rights_uri = RightsStatus.rights_uri_from_string(rights)
        else:
            rights_uri = None
        if feed_url and not urlparse(href).netloc:
            # This link is relative, so we need to get the absolute url
            href = urljoin(feed_url, href)
        return LinkData(rel=rel, href=href, media_type=media_type, rights_uri=rights_uri)

    @classmethod
    def consolidate_links(cls, links):
        """Try to match up links with their thumbnails.

        If link n is an image and link n+1 is a thumbnail, then the
        thumbnail is assumed to be the thumbnail of the image.

        Similarly if link n is a thumbnail and link n+1 is an image.
        """
        new_links = list(links)
        next_link_already_handled = False
        for i, link in enumerate(links):

            if link.rel not in (Hyperlink.THUMBNAIL_IMAGE, Hyperlink.IMAGE):
                # This is not any kind of image. Ignore it.
                continue

            if next_link_already_handled:
                # This link and the previous link were part of an
                # image-thumbnail pair.
                next_link_already_handled = False
                continue
                
            if i == len(links)-1:
                # This is the last link. Since there is no next link
                # there's nothing to do here.
                continue

            # Peek at the next link.
            next_link = links[i+1]


            if (link.rel == Hyperlink.THUMBNAIL_IMAGE
                and next_link.rel == Hyperlink.IMAGE):
                # This link is a thumbnail and the next link is
                # (presumably) the corresponding image.
                thumbnail_link = link
                image_link = next_link
            elif (link.rel == Hyperlink.IMAGE
                  and next_link.rel == Hyperlink.THUMBNAIL_IMAGE):
                thumbnail_link = next_link
                image_link = link
            else:
                # This link and the next link do not form an
                # image-thumbnail pair. Do nothing.
                continue

            image_link.thumbnail = thumbnail_link
            new_links.remove(thumbnail_link)
            next_link_already_handled = True

        return new_links

    @classmethod
    def extract_measurement(cls, rating_tag):
        type = rating_tag.get('{http://schema.org/}additionalType')
        value = rating_tag.get('{http://schema.org/}ratingValue')
        if not value:
            value = rating_tag.attrib.get('{http://schema.org}ratingValue')
        if not type:
            type = Measurement.RATING
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
    
    def __init__(self, _db, feed_url, default_data_source, import_class, 
                 interval_seconds=3600, keep_timestamp=True,
                 immediately_presentation_ready=False):
        self.feed_url = feed_url
        self.importer = import_class(_db, default_data_source)
        self.immediately_presentation_ready = immediately_presentation_ready
        super(OPDSImportMonitor, self).__init__(
            _db, "OPDS Import %s" % feed_url, interval_seconds,
            keep_timestamp=keep_timestamp, default_start_time=Monitor.NEVER
        )
    
    def _get(self, url):
        """Make the sort of HTTP request that's normal for an OPDS feed.

        Long timeout, raise error on anything but 2xx or 3xx.
        """
        kwargs = dict(timeout=120, allowed_response_codes=['2xx', '3xx'])
        return HTTP.get_with_timeout(url, **kwargs)

    def check_for_new_data(self, feed, cutoff_date=None):
        """Check if the feed contains any entries that haven't been imported yet
        or have been updated since the cutoff date.
        """
        last_update_dates = self.importer.extract_last_update_dates(feed)

        new_data = False
        for identifier, updated in last_update_dates:

            identifier = get_one(self._db, Identifier, identifier=identifier)
            data_source = DataSource.lookup(self._db, self.importer.data_source_name)
            record = None

            if identifier:
                record = CoverageRecord.lookup(
                    identifier, data_source, operation=CoverageRecord.IMPORT_OPERATION
                )

            # If we have a CoverageRecord, that's the most reliable indicator of the last time we tried
            # to import this book. But if we imported the book before we started creating CoverageRecords
            # for imports, we can still use the monitor's timestamp as the cutoff.
            if record:
                cutoff_date = record.timestamp

            if cutoff_date:
                # We've imported this book before, so don't import it again unless it's changed.

                if not updated:
                    # If we don't know if the book has been updated, import it again to be safe.
                    new_data = True
                    break

                if  updated >= cutoff_date:
                    # This book has been updated.
                    new_data = True
                    break

            if record and record.exception:
                # We tried to import this book before and it failed. The book hasn't been updated,
                # so it probably still won't work. Don't try again.
                continue

            # There's no record of a failure for this book. Has it been imported from this source already?
            existing_edition = get_one(
                self._db, Edition,
                primary_identifier=identifier,
                data_source=data_source,
            )
            if not existing_edition:
                new_data = True
                break

        return new_data

    def follow_one_link(self, link, cutoff_date):
        self.log.info("Following next link: %s, cutoff=%s", link, cutoff_date)
        response = HTTP.get_with_timeout(link, allowed_response_codes=['2xx', '3xx'])

        new_data = self.check_for_new_data(response.content, cutoff_date=cutoff_date)

        if new_data:
            # There's something new on this page, so we need to check the next page as well.
            next_links = self.importer.extract_next_links(response.content)
            return next_links, response.content
        else:
            # There's nothing new, so we don't need to import this feed or check the next page.
            return [], None

    def import_one_feed(self, feed, start):
        imported_editions, pools, works, messages = self.importer.import_from_feed(
            feed, even_if_no_author=True, cutoff_date=start,
            immediately_presentation_ready = self.immediately_presentation_ready
        )

        data_source = DataSource.lookup(self._db, self.importer.data_source_name)
        
        # Create CoverageRecords for the successful imports.
        for edition in imported_editions:
            CoverageRecord.add_for(
                edition, data_source, CoverageRecord.IMPORT_OPERATION
            )

        # Create CoverageRecords for the failures.
        for urn, message in messages.items():
            if message.status_code == 500:
                # This is a permanent failure. We shouldn't try again until something's
                # changed.
                identifier, ignore = Identifier.parse_urn(self._db, urn)
                record, ignore = CoverageRecord.add_for(
                    identifier, data_source, CoverageRecord.IMPORT_OPERATION
                )
                record.exception = message.message
            else:
                # This is a transient failure. A CoverageProvider will run again,
                # so we should try to import next time we see this identifier.
                self.log.info("Temporarily unable to import %s: %s" % (urn, message.message))
        
    def run_once(self, start, cutoff):
        feeds = []
        queue = [self.feed_url]
        seen_links = set([])
        
        # First, follow the feed's next links until we reach a page with
        # nothing new. If any link raises an exception, nothing will be imported.
        while queue:
            new_queue = []

            for link in queue:
                if link in seen_links:
                    continue
                next_links, feed = self.follow_one_link(link, start)
                new_queue.extend(next_links)
                if feed:
                    feeds.append((link, feed))
                seen_links.add(link)

            queue = new_queue

        # Start importing at the end. If something fails, it will be easier to
        # pick up where we left off.
        for link, feed in reversed(feeds):
            self.log.info("Importing next feed: %s, cutoff=%s", link, start)
            self.import_one_feed(feed, start)
            self._db.commit()


class OPDSImporterWithS3Mirror(OPDSImporter):
    """OPDS Importer that mirrors content to S3."""

    def __init__(self, _db, default_data_source, **kwargs):
        kwargs = dict(kwargs)
        kwargs['mirror'] = S3Uploader()
        super(OPDSImporterWithS3Mirror, self).__init__(
            _db, default_data_source, **kwargs
        )
