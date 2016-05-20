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
        if (self.base_url==metadata_wrangler_url or
            self.base_url==metadata_wrangler_url+'/'):
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

    def _get(self, url):
        """Runs requests with the appropriate authentication"""

        if self.client_id and self.client_secret:
            return requests.get(url, auth=(self.client_id, self.client_secret))
        return requests.get(url)

    def lookup(self, identifiers):
        """Retrieve an OPDS feed with metadata for the given identifiers."""
        args = "&".join(set(["urn=%s" % i.urn for i in identifiers]))
        url = self.base_url + self.LOOKUP_ENDPOINT + "?" + args
        logging.info("Lookup URL: %s", url)
        return self._get(url)

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

    Note: Ignores author and subject information, under the assumption that it will 
    get better author and subject information from the metadata wrangler.

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

        metadata_objs, circulation_objs, status_messages, next_links = self.extract_feed_data(feed)

        #set_trace()
        imported_editions = {}
        imported_pools = {}
        imported_works = {}

        # status_messages notes business logic errors and non-success download statuses
        if not status_messages:
            status_messages = {}

        # make editions.  if have problem, make sure associated pool and work aren't created.
        for key, metadata in metadata_objs.iteritems():
            # key is identifier.urn here
            try:
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
                # go into circulation_obs, and remove any on the same identifier, 
                # so we don't try to make pools and works from it
                if key in circulation_objs:
                    del circulation_objs[key]
                # clean up any edition might have created
                if key in imported_editions:
                    del imported_editions[key]
                # stop the import, something went terribly wrong
                return imported_editions.values(), imported_pools.values(), imported_works.values(), status_messages, next_links



        for key, circulationdata in circulation_objs.iteritems():
            pool = None
            work = None
            associated_metadata = None
            if metadata_objs[key]:
                associated_metadata = metadata_objs[key]
            obj = DataSource.lookup(self._db, circulationdata._data_source)
            # Note: is data_source is not lendable, don't make an error message, 
            # not creating a pool is expected response.
            if obj.offers_licenses:
                try:
                    pool = self.import_pool_from_circulationdata(
                        circulationdata, associated_metadata, cutoff_date
                    )

                    work = self.import_work_from_circulationdata(
                        pool, even_if_no_author, immediately_presentation_ready
                    )
                    # if error or cutoff making license pool, don't make work
                    # if error or cutoff making work, do make license pool
                    # always make editions
                    if pool:
                        imported_pools[key] = pool
                        if work:
                            imported_works[key] = work
                except Exception, e:
                    message = StatusMessage(500, "Local exception during import:\n%s" % traceback.format_exc())
                    status_messages[key] = message
                    # If problem importing a Work, then rather than scratch the whole import, 
                    # treat this as a failure that only applies to this item.
                    # But if problem importing a pool, something is very wrong, and should stop the import.
                    if not pool:
                        return imported_editions.values(), imported_pools.values(), imported_works.values(), status_messages, next_links
            else:
                self.log.debug(
                    "DataSource does not offer licenses.  No LicensePool created for CirculationData %r, not attempting to create Work.", 
                    circulationdata
                )


        return imported_editions.values(), imported_pools.values(), imported_works.values(), status_messages, next_links


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
            and metadata.opds_entry_updated < cutoff_date
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


    def import_pool_from_circulationdata(
            self, circulationdata, associated_metadata, cutoff_date
    ):
        """ For the passed-in CirculationData object, see if can find or create a license pool, 
        then see if can find one or more editions pre-created for that license pool, 
        then see if can create a work, and also update the editions 
        with the new license pool and work.  
        """

        # Locate or create a LicensePool for this book.
        license_pool, is_new_license_pool = circulationdata.license_pool(self._db)
        
        if (cutoff_date 
            and not is_new_license_pool 
            and associated_metadata 
            and associated_metadata.opds_entry_updated < cutoff_date):
            # We've already imported this book, we've been told
            # not to bother with books that appeared before a
            # certain date, and this book did in fact appear
            # before that date. There's no reason to do anything.
            return None

        policy = ReplacementPolicy(
            subjects=True,
            links=True,
            contributions=True,
            rights=True,
            even_if_not_apparently_updated=True,
            mirror=self.mirror,
            http_get=self.http_get,
        )
        circulationdata.apply(
            license_pool, replace=policy
        )

        if license_pool is None:
            # Without a LicensePool, we can't create a Work.
            error_message = "No LicensePool created for CirculationData %r, will not attempt to continue to create Work." % circulationdata
            self.log.warn(error_message)
            raise DBImportException(error_message)

        return license_pool


    def import_work_from_circulationdata(
            self, license_pool, even_if_no_author, immediately_presentation_ready
    ):
        """ Making work is not based on pool's cutoff logic, but based on whether have a pool and and edition, period.
        Don't mind if they're new or not.
        """

        # Note: pool.calculate_work will call self.set_presentation_edition(), 
        # which will find editions attached to same Identifier.
        # If the pool has no edition, pool.calculate_work is responsible for not allowing the work to be created.
        work = None
        work, is_new_work = license_pool.calculate_work(even_if_no_author=even_if_no_author,)
        # if pool.calculate_work made a new work, it already called calculate_presentation
        if (work and not is_new_work):
            work.calculate_presentation()
        if (work):
            if immediately_presentation_ready:
                # We want this book to be presentation-ready
                # immediately upon import. As long as no crucial
                # information is missing (like language or title),
                # this will do it.
                work.set_presentation_ready_based_on_content()
        return license_pool, work


    def extract_feed_data(self, feed):
        """Turn an OPDS feed into lists of Metadata and CirculationData objects, 
        with associated messages and next_links.
        """
        fp_data_meta, fp_data_circ, status_messages, next_links = self.extract_data_from_feedparser(feed=feed, data_source=self.data_source_name)
        # gets: medium, measurements, links, contributors, etc.
        xml_data_meta = self.extract_metadata_from_elementtree(feed)

        # translate the id in status_messages to identifier.urn
        identified_messages = {}
        for id, message_data in status_messages.items():
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
        for id, m_data_dict in fp_data_meta.items():
            external_identifier, ignore = Identifier.parse_urn(self._db, id)
            if self.identifier_mapping:
                internal_identifier = self.identifier_mapping.get(
                    external_identifier, external_identifier)
            else:
                internal_identifier = external_identifier

            #set_trace()
            identifier_obj = IdentifierData(
                type=internal_identifier.type,
                identifier=internal_identifier.identifier
            )
            '''
            (Pdb) xml_data_meta['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557']['links']
            [<LinkData: rel="http://opds-spec.org/acquisition/open-access" href="http://www.gutenberg.org/ebooks/10557.epub.images" media_type='application/epub+zip'>]

            (Pdb) xml_data_meta['urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441']['links']
            [<LinkData: rel="http://opds-spec.org/image" href="https://s3.amazonaws.com/book-covers.nypl.org/Gutenberg-Illustrated/10441/cover_10441_9.png" media_type=None, has thumbnail>, 
             <LinkData: rel="http://opds-spec.org/acquisition/open-access" href="http://www.gutenberg.org/ebooks/10441.epub.images" media_type='application/epub+zip'>]            
            '''

            # form the Metadata object
            xml_data_dict = xml_data_meta.get(id, {})
            combined_meta = self.combine(m_data_dict, xml_data_dict)
            if combined_meta.get('data_source') is None:
                combined_meta['data_source'] = self.data_source_name
            
            combined_meta['primary_identifier'] = identifier_obj

            metadata[internal_identifier.urn] = Metadata(**combined_meta)

            # form the CirculationData that would correspond to this Metadata
            c_data_dict = fp_data_circ[id]

            circ_links_dict = {}
            # extract just the links to pass to CirculationData constructor
            circ_links_dict['links'] = xml_data_dict['links']
            combined_circ = self.combine(c_data_dict, circ_links_dict)
            if combined_circ.get('data_source') is None:
                combined_circ['data_source'] = self.data_source_name
            
            combined_circ['primary_identifier'] = identifier_obj

            circulationdata[internal_identifier.urn] = CirculationData(**combined_circ)
            if id in fp_data_circ:
                del fp_data_circ[id]

        for id, fp_data_dict in fp_data_circ.items():
            # These are pool-generating data for which there were no edition data.
            # There should be none of these, so if we have some, put them onto the error message pile.
            message = "Cannot make CirculationData for id=[" + id + "].  Have no accompanying Metadata."
            status_messages[id] = StatusMessage(status_code=None, message=message)

        return metadata, circulationdata, status_messages, next_links


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
        values_meta = {}
        values_circ = {}
        status_messages = {}
        for entry in feedparser_parsed['entries']:
            identifier, detail_meta, detail_circ, status_message = cls.data_detail_for_feedparser_entry(entry=entry, data_source=data_source)

            if identifier:
                if status_message:
                    status_messages[identifier] = status_message
                else:
                    if detail_meta:
                        values_meta[identifier] = detail_meta
                    if detail_circ:
                        values_circ[identifier] = detail_circ
            else:
                # That's bad. Can't make an item-specific error message, but write to 
                # log that something very wrong happened.
                logging.error("Tried to parse an element without a valid identifier.")

        feed = feedparser_parsed['feed']
        next_links = []
        if feed and 'links' in feed:
            next_links = [
                link['href'] for link in feed['links'] 
                if link['rel'] == 'next'
            ]

        return values_meta, values_circ, status_messages, next_links


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

        # Some OPDS feeds (eg Standard Ebooks) contain relative urls, so we need the
        # feed's self URL to extract links.
        links = [child.attrib for child in root.getroot() if 'link' in child.tag]
        self_links = [link['href'] for link in links if link.get('rel') == 'self']
        if self_links:
            feed_url = self_links[0]
        else:
            feed_url = None

        for entry in parser._xpath(root, '/atom:feed/atom:entry'):
            identifier, detail = cls.detail_for_elementtree_entry(parser, entry, feed_url)
            if identifier:
                values[identifier] = detail
        return values


    @classmethod
    def data_detail_for_feedparser_entry(cls, entry, data_source):
        """Turn an entry dictionary created by feedparser into dictionaries of data
        that can be used as keyword arguments to the Metadata and CirculationData constructors.

        :return: A 4-tuple (identifier, kwargs for Metadata constructor,  
            kwargs for CirculationData constructor, status message)
        """
        identifier = entry['id']
        if not identifier:
            return None, None, None, None

        status_message = None
        status_code = entry.get('simplified_status_code', None)
        message = entry.get('simplified_message', None)
        if status_code is not None:
            status_message = StatusMessage(status_code, message)

        if status_message and not status_message.success:
            return identifier, None, None, status_message

        # At this point we can assume that we successfully got some
        # metadata, and possibly a link to the actual book.

        # Note: will ignore bibframe_distribution from feed, and use data source passed into the 
        # importer as license_data_source, too.

        title = entry.get('title', None)
        if title == OPDSFeed.NO_TITLE:
            title = None
        subtitle = entry.get('alternativeheadline', None)

        def _datetime(key):
            value = entry.get(key, None)
            if not value:
                return value
            return datetime.datetime(*value[:6])

        last_opds_update = _datetime('updated_parsed')
        added_to_collection_time = _datetime('published_parsed')

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
            opds_entry_updated=last_opds_update,
        )

        kwargs_circ = dict(
            # Note: calling code checks to make sure data_source is lendable, and if not, aborts creating a pool.
            data_source=data_source,
            links=list(links),
            # Note: CirculationData.default_rights_uri is not same as the old 
            # Metadata.rights_uri, but we're treating it same for now.
            default_rights_uri=rights_uri,
            # gets assigned to pool.last_checked on CirculationData.apply()
            last_checked=datetime.datetime.utcnow(), 
            # first appearance in our databases, gets assigned to pool, if have to make new pool. 
            first_appearance = datetime.datetime.utcnow()
        )

        return identifier, kwargs_meta, kwargs_circ, status_message


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
            return None, None
        identifier = identifier.text

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
        
        return identifier, data

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

    def follow_one_link(self, link, start):
        self.log.info("Following next link: %s, cutoff=%s", link, start)
        response = requests.get(link)

        if response.status_code / 100 not in [2, 3]:
            self.log.error("Fetching next link %s failed with status %i" % (link, response.status_code))
            return []

        imported, messages, next_links = self.importer.import_from_feed(
            response.content, even_if_no_author=True, cutoff_date=start,
            immediately_presentation_ready = self.immediately_presentation_ready
        )
        self._db.commit()
        
        if len(imported) == 0:
            # We did not end up importing a single book on this page.
            # There's no need to keep going.
            self.log.info(
                "Saw a full page with no new or updated books. Stopping."
            )
            return []
        else:
            return next_links

    def run_once(self, start, cutoff):
        queue = [self.feed_url]
        seen_links = set([])
        
        while queue:
            new_queue = []

            for link in queue:
                if link in seen_links:
                    continue
                new_queue.extend(self.follow_one_link(link, start))
                seen_links.add(link)
                self._db.commit()

            queue = new_queue


class OPDSImporterWithS3Mirror(OPDSImporter):
    """OPDS Importer that mirrors content to S3."""

    def __init__(self, _db, default_data_source, **kwargs):
        kwargs = dict(kwargs)
        kwargs['mirror'] = S3Uploader()
        super(OPDSImporterWithS3Mirror, self).__init__(
            _db, default_data_source, **kwargs
        )
