from nose.tools import set_trace
import base64
import datetime
import isbnlib
import os
import json
import logging
import requests
import urlparse
import urllib
import sys

from model import (
    Contributor,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    Hyperlink,
    Identifier,
    Measurement,
    Representation,
    Subject,
)

from metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    Metadata,
    MeasurementData,
    LinkData,
    SubjectData,
)

from coverage import (
    BibliographicCoverageProvider,
    CoverageFailure,
)

from config import (
    Configuration,
    CannotLoadConfiguration,
)

class OverdriveAPI(object):

    log = logging.getLogger("Overdrive API")

    TOKEN_ENDPOINT = "https://oauth.overdrive.com/token"
    PATRON_TOKEN_ENDPOINT = "https://oauth-patron.overdrive.com/patrontoken"

    LIBRARY_ENDPOINT = "https://api.overdrive.com/v1/libraries/%(library_id)s"
    ALL_PRODUCTS_ENDPOINT = "https://api.overdrive.com/v1/collections/%(collection_token)s/products?sort=%(sort)s"
    METADATA_ENDPOINT = "https://api.overdrive.com/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    EVENTS_ENDPOINT = "https://api.overdrive.com/v1/collections/%(collection_token)s/products?lastUpdateTime=%(lastupdatetime)s&sort=%(sort)s&limit=%(limit)s"
    AVAILABILITY_ENDPOINT = "https://api.overdrive.com/v1/collections/%(collection_token)s/products/%(product_id)s/availability"

    CHECKOUTS_ENDPOINT = "https://patron.api.overdrive.com/v1/patrons/me/checkouts"
    CHECKOUT_ENDPOINT = "https://patron.api.overdrive.com/v1/patrons/me/checkouts/%(overdrive_id)s"
    FORMATS_ENDPOINT = "https://patron.api.overdrive.com/v1/patrons/me/checkouts/%(overdrive_id)s/formats"
    HOLDS_ENDPOINT = "https://patron.api.overdrive.com/v1/patrons/me/holds"
    HOLD_ENDPOINT = "https://patron.api.overdrive.com/v1/patrons/me/holds/%(product_id)s"
    ME_ENDPOINT = "https://patron.api.overdrive.com/v1/patrons/me"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    EVENT_DELAY = datetime.timedelta(minutes=120)
    #EVENT_DELAY = datetime.timedelta(minutes=0)

    # The ebook formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open"

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    
    def __init__(self, _db, testing=False):
        self._db = _db
        self.source = DataSource.lookup(_db, DataSource.OVERDRIVE)

        # Set some stuff from environment variables
        if not testing:
            values = self.environment_values()
            if len([x for x in values if not x]):
                self.log.info(
                    "No Overdrive client configured."
                )
                raise CannotLoadConfiguration("No Overdrive client configured.")

            (self.client_key, self.client_secret, self.website_id, 
             self.library_id) = values

            # Get set up with up-to-date credentials from the API.
            self.check_creds()
            self.collection_token = self.get_library()['collectionToken']


    @classmethod
    def environment_values(cls):
        value = Configuration.integration('Overdrive')
        values = []
        for name in [
                'client_key',
                'client_secret',
                'website_id',
                'library_id',
        ]:
            var = value.get(name)
            if var:
                var = var.encode("utf8")
            values.append(var)
        return values

    @classmethod
    def from_environment(cls, _db):
        # Make sure all environment values are present. If any are missing,
        # return None. Otherwise return an OverdriveAPI object.
        values = cls.environment_values()
        try:
            return cls(_db)
        except CannotLoadConfiguration, e:
            return None

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        if force_refresh:
            refresh_on_lookup = lambda x: x
        else:
            refresh_on_lookup = self.refresh_creds

        credential = Credential.lookup(
            self._db, DataSource.OVERDRIVE, None, None, refresh_on_lookup)
        if force_refresh:
            self.refresh_creds(credential)
        self.token = credential.credential

    def refresh_creds(self, credential):
        """Fetch a new Bearer Token and update the given Credential object."""
        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"))
        data = response.json()
        self._update_credential(credential, data)
        self.token = credential.credential

    def get(self, url, extra_headers, exception_on_401=False):
        """Make an HTTP GET request using the active Bearer Token."""
        headers = dict(Authorization="Bearer %s" % self.token)
        headers.update(extra_headers)
        status_code, headers, content = Representation.simple_http_get(
            url, headers)
        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the OAuth Bearer Token!")
            else:
                # Refresh the token and try again.
                self.check_creds(True)
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    def token_post(self, url, payload, headers={}):
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        s = "%s:%s" % (self.client_key, self.client_secret)
        auth = base64.encodestring(s).strip()
        headers = dict(headers)
        headers['Authorization'] = "Basic %s" % auth
        return requests.post(url, payload, headers=headers)

    def _update_credential(self, credential, overdrive_data):
        """Copy Overdrive OAuth data into a Credential object."""
        credential.credential = overdrive_data['access_token']
        expires_in = (overdrive_data['expires_in'] * 0.9)
        credential.expires = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=expires_in)
        self._db.commit()

    def get_library(self):
        url = self.LIBRARY_ENDPOINT % dict(library_id=self.library_id)
        representation, cached = Representation.get(self._db, url, self.get)
        return json.loads(representation.content)

    def all_ids(self):
        """Get IDs for every book in the system, with the most recently added
        ones at the front.
        """
        params = dict(collection_token=self.collection_token,
                      sort="dateAdded:desc")
        next_link = self.make_link_safe(
            self.ALL_PRODUCTS_ENDPOINT % params)

        while next_link:
            try:
                page_inventory, next_link = self._get_book_list_page(
                    next_link, 'next')
            except Exception, e:
                self.log.error("OVERDRIVE ERROR: %r %r %r",
                               status_code, headers, content)
                return
            for i in page_inventory:
                yield i

    def _get_book_list_page(self, link, rel_to_follow='next'):
        """Process a page of inventory whose circulation we need to check.

        Returns a list of (title, id, availability_link) 3-tuples,
        plus a link to the next page of results.
        """
        # We don't cache this because it changes constantly.
        status_code, headers, content = self.get(link, {})
        try:
            data = json.loads(content)
        except Exception, e:
            self.log.error(
                "Error getting book list page: %r %r %r", 
                status_code, headers, content,
                exc_info=e
            )
            return [], None

        # Find the link to the next page of results, if any.
        next_link = OverdriveRepresentationExtractor.link(data, rel_to_follow)

        # Prepare to get availability information for all the books on
        # this page.
        availability_queue = (
            OverdriveRepresentationExtractor.availability_link_list(data))
        return availability_queue, next_link


    def recently_changed_ids(self, start, cutoff):
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        last_update_time = start-self.EVENT_DELAY
        self.log.info(
            "Asking for circulation changes since %s",
            last_update_time
        )
        last_update = last_update_time.strftime(self.TIME_FORMAT)

        params = dict(lastupdatetime=last_update,
                      sort="popularity:desc",
                      limit=self.PAGE_SIZE_LIMIT,
                      collection_token=self.collection_token)
        next_link = self.make_link_safe(self.EVENTS_ENDPOINT % params)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            for i in page_inventory:
                yield i

    def metadata_lookup(self, identifier):
        """Look up metadata for an Overdrive identifier.
        """
        url = self.METADATA_ENDPOINT % dict(
            collection_token=self.collection_token,
            item_id=identifier.identifier
        )
        status_code, headers, content = self.get(url, {})
        return json.loads(content)

    def metadata_lookup_obj(self, identifier):
        url = self.METADATA_ENDPOINT % dict(
            collection_token=self.collection_token,
            item_id=identifier
        )
        status_code, headers, content = self.get(url, {})
        data = json.loads(content)
        return OverdriveRepresentationExtractor.book_info_to_metadata(data)


    @classmethod
    def make_link_safe(self, url):
        """Turn a server-provided link into a link the server will accept!

        This is completely obnoxious and I have complained about it to
        Overdrive.
        """
        parts = list(urlparse.urlsplit(url))
        parts[2] = urllib.quote(parts[2])
        query_string = parts[3]
        query_string = query_string.replace("+", "%2B")
        query_string = query_string.replace(":", "%3A")
        query_string = query_string.replace("{", "%7B")
        query_string = query_string.replace("}", "%7D")
        parts[3] = query_string
        return urlparse.urlunsplit(tuple(parts))


class OverdriveRepresentationExtractor(object):

    """Extract useful information from Overdrive's JSON representations."""

    log = logging.getLogger("Overdrive representation extractor")

    @classmethod
    def availability_link_list(self, book_list):
        """:return: A list of dictionaries with keys `id`, `title`,
        `availability_link`.
        """
        l = []
        if not 'products' in book_list:
            return []

        products = book_list['products']
        for product in products:
            data = dict(id=product['id'],
                        title=product['title'],
                        author_name=None)
            
            if 'primaryCreator' in product:
                creator = product['primaryCreator']
                if creator.get('role') == 'Author':
                    data['author_name'] = creator.get('name')
            links = product.get('links', [])
            if 'availability' in links:
                link = links['availability']['href']
                data['availability_link'] = OverdriveAPI.make_link_safe(link)
            else:
                logging.getLogger("Overdrive API").warn(
                    "No availability link for %s", book_id)
            l.append(data)
        return l

    @classmethod
    def link(self, page, rel):
        if 'links' in page and rel in page['links']:
            raw_link = page['links'][rel]['href']
            link = OverdriveAPI.make_link_safe(raw_link)
        else:
            link = None
        return link

    media_type_for_overdrive_format = {
    }

    format_data_for_overdrive_format = {

        "ebook-pdf-adobe" : (
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        "ebook-pdf-open" : (
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        ),
        "ebook-epub-adobe" : (
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        "ebook-epub-open" : (
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        ),
        "audiobook-mp3" : (
            "application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM
        ),
        "music-mp3" : (
            "application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM
        ),
        "ebook-overdrive" : (
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM
        ),
        "audiobook-overdrive" : (
            DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM
        ),
        'video-streaming' : (
            DeliveryMechanism.STREAMING_VIDEO_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM
        ),
        "ebook-kindle" : (
            DeliveryMechanism.KINDLE_CONTENT_TYPE, 
            DeliveryMechanism.KINDLE_DRM
        ),
        "periodicals-nook" : (
            DeliveryMechanism.NOOK_CONTENT_TYPE,
            DeliveryMechanism.NOOK_DRM
        ),
    }

    ignorable_overdrive_formats = set([
        'ebook-overdrive',
        'audiobook-overdrive',
    ])

    overdrive_role_to_simplified_role = {
        "actor" : Contributor.ACTOR_ROLE,
        "artist" : Contributor.ARTIST_ROLE,
        "book producer" : Contributor.PRODUCER_ROLE,
        "associated name" : Contributor.ASSOCIATED_ROLE,
        "author" : Contributor.AUTHOR_ROLE,
        "author of introduction" : Contributor.INTRODUCTION_ROLE,
        "author of foreword" : Contributor.FOREWORD_ROLE,
        "author of afterword" : Contributor.AFTERWORD_ROLE,
        "contributor" : Contributor.CONTRIBUTOR_ROLE,
        "colophon" : Contributor.COLOPHON_ROLE,
        "adapter" : Contributor.ADAPTER_ROLE,
        "etc." : Contributor.UNKNOWN_ROLE,
        "cast member" : Contributor.ACTOR_ROLE,
        "collaborator" : Contributor.COLLABORATOR_ROLE,
        "compiler" : Contributor.COMPILER_ROLE,
        "composer" : Contributor.COMPOSER_ROLE,
        "copyright holder" : Contributor.COPYRIGHT_HOLDER_ROLE,
        "director" : Contributor.DIRECTOR_ROLE,
        "editor" : Contributor.EDITOR_ROLE,
        "engineer" : Contributor.ENGINEER_ROLE,
        "executive producer" : Contributor.EXECUTIVE_PRODUCER_ROLE,
        "illustrator" : Contributor.ILLUSTRATOR_ROLE,
        "musician" : Contributor.MUSICIAN_ROLE,
        "narrator" : Contributor.NARRATOR_ROLE,
        "other" : Contributor.UNKNOWN_ROLE,
        "performer" : Contributor.PERFORMER_ROLE,
        "producer" : Contributor.PRODUCER_ROLE,
        "translator" : Contributor.TRANSLATOR_ROLE,
        "photographer" : Contributor.PHOTOGRAPHER_ROLE,
        "lyricist" : Contributor.LYRICIST_ROLE,
        "transcriber" : Contributor.TRANSCRIBER_ROLE,
        "designer" : Contributor.DESIGNER_ROLE,
    }

    overdrive_medium_to_simplified_medium = {
        "eBook" : Edition.BOOK_MEDIUM,
        "Video" : Edition.VIDEO_MEDIUM,
        "Audiobook" : Edition.AUDIO_MEDIUM,
        "Music" : Edition.MUSIC_MEDIUM,
        "Periodicals" : Edition.PERIODICAL_MEDIUM,
    }

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_roles(cls, id, rolestring):
        rolestring = rolestring.lower()
        roles = [x.strip() for x in rolestring.split(",")]
        if ' and '  in roles[-1]:
            roles = roles[:-1] + [x.strip() for x in roles[-1].split(" and ")]
        processed = []
        for x in roles:
            if x not in cls.overdrive_role_to_simplified_role:
                cls.log.error(
                    "Could not process role %s for %s", x, id)
            else:
                processed.append(cls.overdrive_role_to_simplified_role[x])
        return processed

    @classmethod
    def book_info_to_circulation(cls, book):
        # In Overdrive, 'reserved' books show up as books on
        # hold. There is no separate notion of reserved books.
        licenses_reserved = 0

        licenses_owned = None
        licenses_available = None
        patrons_in_hold_queue = None

        if (book.get('isOwnedByCollections') is not False):
            # We own this book.
            for collection in book['collections']:
                if 'copiesOwned' in collection:
                    if licenses_owned is None:
                        licenses_owned = 0
                    licenses_owned += int(collection['copiesOwned'])
                if 'copiesAvailable' in collection:
                    if licenses_available is None:
                        licenses_available = 0
                    licenses_available += int(collection['copiesAvailable'])
                if 'numberOfHolds' in collection:
                    if patrons_in_hold_queue is None:
                        patrons_in_hold_queue = 0
                    patrons_in_hold_queue += collection['numberOfHolds']
        return CirculationData(
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
        )

    @classmethod
    def book_info_to_metadata(cls, book):
        """Turn Overdrive's JSON representation of a book into a Metadata
        object.
        """
        if not 'id' in book:
            return None
        overdrive_id = book['id']
        primary_identifier = IdentifierData(
            Identifier.OVERDRIVE_ID, overdrive_id
        )

        title = book.get('title', None)
        sort_title = book.get('sortTitle')
        subtitle = book.get('subtitle', None)
        series = book.get('series', None)
        publisher = book.get('publisher', None)
        imprint = book.get('imprint', None)

        if 'publishDate' in book:
            published = datetime.datetime.strptime(
                book['publishDate'][:10], cls.DATE_FORMAT)
        else:
            published = None

        languages = [l['code'] for l in book.get('languages', [])]
        if 'eng' in languages or not languages:
            language = 'eng'
        else:
            language = sorted(languages)[0]

        contributors = []
        for creator in book.get('creators', []):
            sort_name = creator['fileAs']
            display_name = creator['name']
            role = creator['role']
            roles = cls.parse_roles(overdrive_id, role) or [Contributor.UNKNOWN_ROLE]
            contributor = ContributorData(
                sort_name=sort_name, display_name=display_name,
                roles=roles, biography = creator.get('bioText', None)
            )
            contributors.append(contributor)

        subjects = []
        for sub in book.get('subjects', []):
            subject = SubjectData(
                type=Subject.OVERDRIVE, identifier=sub['value'],
                weight=100
            )
            subjects.append(subject)

        for sub in book.get('keywords', []):
            subject = SubjectData(
                type=Subject.TAG, identifier=sub['value'],
                weight=1
            )
            subjects.append(subject)

        extra = dict()
        if 'grade_levels' in book:
            for i in book['grade_levels']:
                subject = SubjectData(
                    type=Subject.GRADE_LEVEL,
                    identifier=i['value'],
                    weight=100
                )
                subjects.append(subject)

        overdrive_medium = book.get('mediaType', None)
        if overdrive_medium and overdrive_medium not in cls.overdrive_medium_to_simplified_medium:
            cls.log.error(
                "Could not process medium %s for %s", overdrive_medium, overdrive_id)

        medium = cls.overdrive_medium_to_simplified_medium.get(
            overdrive_medium, Edition.BOOK_MEDIUM
        )
        formats = []
        for format in book.get('formats', []):
            format_id = format['id']
            if format_id in cls.format_data_for_overdrive_format:
                content_type, drm_scheme = cls.format_data_for_overdrive_format.get(format_id)
                formats.append(FormatData(content_type, drm_scheme))
            elif format_id not in cls.ignorable_overdrive_formats:
                cls.log.error(
                    "Could not process Overdrive format %s for %s", 
                    format_id, overdrive_id
                )

            if format_id.startswith('audiobook-'):
                medium = Edition.AUDIO_MEDIUM
            elif format_id.startswith('video-'):
                medium = Edition.VIDEO_MEDIUM
            elif format_id.startswith('ebook-'):
                medium = Edition.BOOK_MEDIUM
            elif format_id.startswith('music-'):
                medium = Edition.MUSIC_MEDIUM
            else:
                cls.log.warn("Unfamiliar format: %s", format_id)

        measurements = []
        if 'awards' in book:
            extra['awards'] = book.get('awards', [])
            num_awards = len(extra['awards'])
            measurements.append(
                MeasurementData(
                    Measurement.AWARDS, str(num_awards)
                )
            )

        for name, subject_type in (
                ('ATOS', Subject.ATOS_SCORE),
                ('lexileScore', Subject.LEXILE_SCORE),
                ('interestLevel', Subject.INTEREST_LEVEL)
        ):
            if not name in book:
                continue
            identifier = str(book[name])
            subjects.append(
                SubjectData(type=subject_type, identifier=identifier,
                            weight=100
                        )
            )

        identifiers = []
        links = []
        for format in book.get('formats', []):
            for new_id in format.get('identifiers', []):
                t = new_id['type']
                v = new_id['value']
                type_key = None
                if t == 'ASIN':
                    type_key = Identifier.ASIN
                elif t == 'ISBN':
                    type_key = Identifier.ISBN
                    if len(v) == 10:
                        v = isbnlib.to_isbn13(v)
                elif t == 'DOI':
                    type_key = Identifier.DOI
                elif t == 'UPC':
                    type_key = Identifier.UPC
                elif t == 'PublisherCatalogNumber':
                    continue
                if type_key and v:
                    identifiers.append(
                        IdentifierData(type_key, v, 1)
                    )

            # Samples become links.
            if 'samples' in format:

                if not format['id'] in cls.format_data_for_overdrive_format:
                    # Useless to us.
                    continue
                content_type, drm_scheme = cls.format_data_for_overdrive_format.get(format['id'])
                if Representation.is_media_type(content_type):
                    for sample_info in format['samples']:
                        href = sample_info['url']
                        links.append(
                            LinkData(
                                rel=Hyperlink.SAMPLE, 
                                href=href,
                                media_type=content_type
                            )
                        )

        # Cover and descriptions become links.
        if 'images' in book:
            for name, rel in (('cover', Hyperlink.IMAGE),
                              ('thumbnail', Hyperlink.THUMBNAIL_IMAGE)):
                if not name in book['images']:
                    continue
                link = book['images'][name]
                href = OverdriveAPI.make_link_safe(link['href'])
                media_type = link['type']
                links.append(
                    LinkData(
                        rel=rel,
                        href=href,
                        media_type=media_type
                    )
                )

        short = book.get('shortDescription')
        full = book.get('fullDescription')
        if full:
            links.append(
                LinkData(
                    rel=Hyperlink.DESCRIPTION,
                    content=full,
                    media_type="text/html",
                )
            )

        if short and (not full or not full.startswith(short)):
            links.append(
                LinkData(
                    rel=Hyperlink.SHORT_DESCRIPTION,
                    content=short,
                    media_type="text/html",
                )
            )

        # Add measurements: rating and popularity
        if book.get('starRating') is not None and book['starRating'] > 0:
            measurements.append(
                MeasurementData(
                    quantity_measured=Measurement.RATING,
                    value=book['starRating']
                )
            )

        if book.get('popularity'):
            measurements.append(
                MeasurementData(
                    quantity_measured=Measurement.POPULARITY,
                    value=book['popularity']
                )
            )

        return Metadata(
            data_source=DataSource.OVERDRIVE,
            title=title,
            subtitle=subtitle,
            sort_title=sort_title,
            language=language,
            medium=medium,
            series=series,
            publisher=publisher,
            imprint=imprint,
            published=published,            
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors,
            formats=formats,
            measurements=measurements,
            links=links,
        )


class OverdriveBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Overdrive records."""

    def __init__(self, _db):
        super(OverdriveBibliographicCoverageProvider, self).__init__(
            _db, OverdriveAPI(_db), DataSource.OVERDRIVE,
            workset_size=10
        )

    def process_batch(self, identifiers):
        return [self.process_item(identifier) for identifier in identifiers]


    def process_item(self, identifier):
        info = self.api.metadata_lookup(identifier)
        if info.get('errorCode') == 'NotFound':
            e = "ID not recognized by Overdrive"
            return CoverageFailure(self, identifier, e, transient=False)
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(
            info
        )
        if not metadata:
            e = "Could not extract metadata from Overdrive data: %r" % info
            return CoverageFailure(self, identifier, e, transient=True)

        return self.set_metadata(identifier, metadata)

