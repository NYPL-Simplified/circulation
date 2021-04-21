# coding=utf-8
import base64
import datetime
import json
import isbnlib
import logging
from sqlalchemy.orm.session import Session

from flask_babel import lazy_gettext as _

from .circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
    BaseCirculationAPI,
)

from core.model import (
    Credential,
    DataSource,
    ExternalIntegration,
    Identifier
)

from .selftest import (
    HasSelfTests,
    SelfTestResult,
)
from core.monitor import (
    CollectionMonitor,
    TimelineMonitor,
)
from core.util.http import HTTP

from .circulation_exceptions import *

from core.model import (
    get_one_or_create,
    Classification,
    Collection,
    Contributor,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Representation,
    Subject,
)

from core.analytics import Analytics

from core.metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    Metadata,
    LinkData,
    ReplacementPolicy,
    SubjectData,
)

from core.coverage import (
    BibliographicCoverageProvider,
)

from core.config import (
    CannotLoadConfiguration,
)

from core.testing import DatabaseTest

from core.util.datetime_helpers import (
    from_timestamp,
    strptime_utc,
    utc_now,
)
from core.util.http import (
    HTTP,
    BadResponseException,
)

from core.util.personal_names import sort_name_to_display_name

from core.testing import MockRequestsResponse

class OdiloRepresentationExtractor(object):
    """Extract useful information from Odilo's JSON representations."""

    log = logging.getLogger("OdiloRepresentationExtractor")
    ACSM = 'ACSM'
    ACSM_EPUB = 'ACSM_EPUB'
    ACSM_PDF = 'ACSM_PDF'
    EBOOK_STREAMING = 'EBOOK_STREAMING'

    format_data_for_odilo_format = {
        ACSM_PDF: (
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        ACSM_EPUB: (
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        ),
        EBOOK_STREAMING: (
            Representation.TEXT_HTML_MEDIA_TYPE, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
        ),
        "MP3": (
            Representation.MP3_MEDIA_TYPE, DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE
        ),
        "MP4": (
            Representation.MP4_MEDIA_TYPE, DeliveryMechanism.STREAMING_VIDEO_CONTENT_TYPE
        ),
        "WMV": (
            Representation.WMV_MEDIA_TYPE, DeliveryMechanism.STREAMING_VIDEO_CONTENT_TYPE
        ),
        "JPG": (
            Representation.JPEG_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        ),
        "SCORM": (
            Representation.SCORM_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
    }

    odilo_medium_to_simplified_medium = {
        ACSM_PDF: Edition.BOOK_MEDIUM,
        ACSM_EPUB: Edition.BOOK_MEDIUM,
        EBOOK_STREAMING: Edition.BOOK_MEDIUM,
        "MP3": Edition.AUDIO_MEDIUM,
        "MP4": Edition.VIDEO_MEDIUM,
        "WMV": Edition.VIDEO_MEDIUM,
        "JPG": Edition.IMAGE_MEDIUM,
        "SCORM": Edition.COURSEWARE_MEDIUM
    }

    @classmethod
    def record_info_to_circulation(cls, availability):
        """ Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the record_info_to_metadata() method.
        """

        if 'recordId' not in availability:
            return None

        record_id = availability['recordId']
        primary_identifier = IdentifierData(Identifier.ODILO_ID, record_id)  # We own this availability.

        licenses_owned = int(availability['totalCopies'])
        licenses_available = int(availability['availableCopies'])

        # 'licenses_reserved' is the number of patrons who put the book on hold earlier,
        #  but who are now at the front of the queue and who could get the book right now if they wanted to.
        if 'notifiedHolds' in availability:
            licenses_reserved = int(availability['notifiedHolds'])
        else:
            licenses_reserved = 0

        # 'patrons_in_hold_queue' contains the number of patrons who are currently waiting for a copy of the book.
        if 'holdsQueueSize' in availability:
            patrons_in_hold_queue = int(availability['holdsQueueSize'])
        else:
            patrons_in_hold_queue = 0

        return CirculationData(
            data_source=DataSource.ODILO,
            primary_identifier=primary_identifier,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
        )

    @classmethod
    def image_link_to_linkdata(cls, link, rel):
        if not link:
            return None

        return LinkData(rel=rel, href=link, media_type=Representation.JPEG_MEDIA_TYPE)

    @classmethod
    def record_info_to_metadata(cls, book, availability):
        """Turn Odilo's JSON representation of a book into a Metadata
        object.

        Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_circulation() method.
        """
        if 'id' not in book:
            return None

        odilo_id = book['id']
        primary_identifier = IdentifierData(Identifier.ODILO_ID, odilo_id)
        active = book.get('active')

        title = book.get('title')
        subtitle = book.get('subtitle')
        series = book.get('series').strip() or None
        series_position = book.get('seriesPosition').strip() or None

        contributors = []
        sort_author = book.get('author')
        if sort_author:
            roles = [Contributor.AUTHOR_ROLE]
            display_author = sort_name_to_display_name(sort_author)
            contributor = ContributorData(
                sort_name=sort_author, display_name=display_author,
                roles=roles, biography=None
            )
            contributors.append(contributor)

        publisher = book.get('publisher')

        # Metadata --> Marc21 260$c
        published = book.get('publicationDate')
        if not published:
            # yyyyMMdd --> record creation date
            published = book.get('releaseDate')

        if published:
            try:
                published = strptime_utc(published, "%Y%m%d")
            except ValueError as e:
                cls.log.warn('Cannot parse publication date from: ' + published + ', message: ' + str(e))

        # yyyyMMdd --> record last modification date
        last_update = book.get('modificationDate')
        if last_update:
            try:
                last_update = strptime_utc(last_update, "%Y%m%d")
            except ValueError as e:
                cls.log.warn('Cannot parse last update date from: ' + last_update + ', message: ' + str(e))

        language = book.get('language', 'spa')

        subjects = []
        trusted_weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT
        for subject in book.get('subjects', []):
            subjects.append(SubjectData(type=Subject.TAG, identifier=subject, weight=trusted_weight))

        for subjectBisacCode in book.get('subjectsBisacCodes', []):
            subjects.append(SubjectData(type=Subject.BISAC, identifier=subjectBisacCode, weight=trusted_weight))

        grade_level = book.get('gradeLevel')
        if grade_level:
            subject = SubjectData(type=Subject.GRADE_LEVEL, identifier=grade_level, weight=trusted_weight)
            subjects.append(subject)

        medium = None
        file_format = book.get('fileFormat')
        formats = []
        for format_received in book.get('formats', []):
            if format_received in cls.format_data_for_odilo_format:
                medium = cls.set_format(format_received, formats)
            elif format_received == cls.ACSM and file_format:
                medium = cls.set_format(format_received + '_' + file_format.upper(), formats)
            else:
                cls.log.warn('Unrecognized format received: ' + format_received)

        if not medium:
            medium = Edition.BOOK_MEDIUM

        identifiers = []
        isbn = book.get('isbn')
        if isbn:
            if isbnlib.is_isbn10(isbn):
                isbn = isbnlib.to_isbn13(isbn)
            identifiers.append(IdentifierData(Identifier.ISBN, isbn, 1))

        # A cover
        links = []
        cover_image_url = book.get('coverImageUrl')
        if cover_image_url:
            image_data = cls.image_link_to_linkdata(cover_image_url, Hyperlink.THUMBNAIL_IMAGE)
            if image_data:
                links.append(image_data)

        original_image_url = book.get('originalImageUrl')
        if original_image_url:
            image_data = cls.image_link_to_linkdata(original_image_url, Hyperlink.IMAGE)
            if image_data:
                links.append(image_data)

        # Descriptions become links.
        description = book.get('description')
        if description:
            links.append(LinkData(rel=Hyperlink.DESCRIPTION, content=description, media_type="text/html"))

        metadata = Metadata(
            data_source=DataSource.ODILO,
            title=title,
            subtitle=subtitle,
            language=language,
            medium=medium,
            series=series,
            series_position=series_position,
            publisher=publisher,
            published=published,
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors,
            links=links,
            data_source_last_updated=last_update
        )

        metadata.circulation = OdiloRepresentationExtractor.record_info_to_circulation(availability)
        # 'active' --> means that the book exists but it's no longer in the collection
        # (it could be available again in the future)
        if metadata.circulation:
            if not active:
                metadata.circulation.licenses_owned = 0
            metadata.circulation.formats = formats

        return metadata, active

    @classmethod
    def set_format(cls, format_received, formats):
        content_type, drm_scheme = cls.format_data_for_odilo_format.get(format_received)
        formats.append(FormatData(content_type, drm_scheme))
        return cls.odilo_medium_to_simplified_medium.get(format_received)


class OdiloAPI(BaseCirculationAPI, HasSelfTests):
    log = logging.getLogger("Odilo API")
    LIBRARY_API_BASE_URL = "library_api_base_url"

    NAME = ExternalIntegration.ODILO
    DESCRIPTION = _("Integrate an Odilo library collection.")
    SETTINGS = [
                   {
                       "key": LIBRARY_API_BASE_URL,
                       "label": _("Library API base URL"),
                       "description": _("This might look like <code>https://[library].odilo.us/api/v2</code>."),
                       "required": True,
                       "format": "url",
                   },
                   { "key": ExternalIntegration.USERNAME, "label": _("Client Key"), "required": True },
                   { "key": ExternalIntegration.PASSWORD, "label": _("Client Secret"), "required": True },
               ] + BaseCirculationAPI.SETTINGS


    # --- OAuth ---
    TOKEN_ENDPOINT = "/token"

    # --- Discovery API ---
    ALL_PRODUCTS_ENDPOINT = "/records"
    RECORD_METADATA_ENDPOINT = "/records/{recordId}"
    RECORD_AVAILABILITY_ENDPOINT = "/records/{recordId}/availability"

    # --- Circulation API ---
    CHECKOUT_ENDPOINT = "/records/{recordId}/checkout"
    CHECKIN_ENDPOINT = "/checkouts/{checkoutId}/return?patronId={patronId}"

    PLACE_HOLD_ENDPOINT = "/records/{recordId}/hold"
    RELEASE_HOLD_ENDPOINT = "/holds/{holdId}/cancel"

    PATRON_CHECKOUTS_ENDPOINT = "/patrons/{patronId}/checkouts"
    PATRON_HOLDS_ENDPOINT = "/patrons/{patronId}/holds"

    # ---------------------------------------

    PAGE_SIZE_LIMIT = 200

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

    # maps a 2-tuple (media_type, drm_mechanism) to the internal string used in Odilo API to describe that setup.
    delivery_mechanism_to_internal_format = {
        v: k for k, v in list(OdiloRepresentationExtractor.format_data_for_odilo_format.items())
        }

    error_to_exception = {
        "TitleNotCheckedOut": NoActiveLoan,
        "patronNotFound": PatronNotFoundOnRemote,
        "ERROR_DATA_NOT_FOUND": NotFoundOnRemote,
        "LOAN_ALREADY_RESERVED": AlreadyOnHold,
        "CHECKOUT_NOT_FOUND": NotCheckedOut,
    }

    def __init__(self, _db, collection):
        self.odilo_bibliographic_coverage_provider = (
            OdiloBibliographicCoverageProvider(
                collection, api_class=self
            )
        )
        if collection.protocol != ExternalIntegration.ODILO:
            raise ValueError("Collection protocol is %s, but passed into OdiloAPI!" % collection.protocol)

        self._db = _db
        self.analytics = Analytics(self._db)

        self.collection_id = collection.id
        self.token = None
        self.client_key = collection.external_integration.username
        self.client_secret = collection.external_integration.password
        self.library_api_base_url = collection.external_integration.setting(self.LIBRARY_API_BASE_URL).value

        if not self.client_key or not self.client_secret or not self.library_api_base_url:
            raise CannotLoadConfiguration("Odilo configuration is incomplete.")

        # Use utf8 instead of unicode encoding
        settings = [self.client_key, self.client_secret, self.library_api_base_url]
        self.client_key, self.client_secret, self.library_api_base_url = (
            setting.encode('utf8') for setting in settings
        )

        # Get set up with up-to-date credentials from the API.
        self.check_creds()
        if not self.token:
            raise CannotLoadConfiguration("Invalid credentials for %s, cannot intialize API %s"
                                          % (self.client_key, self.library_api_base_url))
    @property
    def collection(self):
        return Collection.by_id(self._db, id=self.collection_id)

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.ODILO)

    def external_integration(self, _db):
        return self.collection.external_integration

    def _run_self_tests(self, _db):
        result = self.run_test(
            "Obtaining a sitewide access token", self.check_creds,
            force_refresh=True
        )
        yield result
        if not result.success:
            # We couldn't get a sitewide token, so there is no
            # point in continuing.
            return

        for result in self.default_patrons(self.collection):
            if isinstance(result, SelfTestResult):
                yield result
                continue

            library, patron, pin = result
            task = "Viewing the active loans for the test patron for library %s" % library.name
            yield self.run_test(
                task, self.get_patron_checkouts, patron, pin
            )

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        if force_refresh:
            refresh_on_lookup = lambda x: x
        else:
            refresh_on_lookup = self.refresh_creds

        credential = self.credential_object(refresh_on_lookup)
        if force_refresh:
            self.refresh_creds(credential)
        self.token = credential.credential

    def credential_object(self, refresh):
        """Look up the Credential object that allows us to use
        the Odilo API.
        """
        return Credential.lookup(self._db, DataSource.ODILO, None, None, refresh)

    def refresh_creds(self, credential):
        """Fetch a new Bearer Token and update the given Credential object."""

        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"),
            allowed_response_codes=[200, 400]
        )

        # If you put in the wrong URL, this is where you'll run into
        # problems, so it's useful to give a helpful error message if
        # Odilo doesn't provide anything more specific.
        generic_error = "%s may not be the right base URL. Response document was: %r" % (
            self.library_api_base_url, response.content.decode("utf-8")
        )
        generic_exception = BadResponseException(
            self.TOKEN_ENDPOINT, generic_error
        )

        try:
            data = response.json()
        except ValueError:
            raise generic_exception
        if response.status_code == 200:
            self._update_credential(credential, data)
            self.token = credential.credential
            return
        elif response.status_code == 400:
            if data and 'errors' in data and len(data['errors']) > 0:
                error = data['errors'][0]
                if 'description' in error:
                    message = error['description']
                else:
                    message = generic_error
                raise BadResponseException(self.TOKEN_ENDPOINT, message)
        raise generic_exception

    def patron_request(self, patron, pin, url, extra_headers={}, data=None, exception_on_401=False, method=None):
        """Make an HTTP request on behalf of a patron.

        The results are never cached.
        """
        headers = dict(Authorization="Bearer %s" % self.token)
        headers['Content-Type'] = 'application/json'
        headers.update(extra_headers)

        if method and method.lower() in ('get', 'post', 'put', 'delete'):
            method = method.lower()
        else:
            if data:
                method = 'post'
            else:
                method = 'get'

        url = self._make_absolute_url(url)
        response = HTTP.request_with_timeout(
            method, url, headers=headers, data=data,
            timeout=60
        )

        # TODO: If Odilo doesn't recognize the patron it will send
        # 404 in this case.
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise Exception("Something's wrong with the patron OAuth Bearer Token!")
            else:
                # Refresh the token and try again.
                self.check_creds(True)
                return self.patron_request(patron, pin, url, extra_headers, data, True)
        else:
            return response

    def _make_absolute_url(self, url):
        """Prepend the API base URL onto `url` unless it is already
        an absolute HTTP URL.
        """
        if not any(url.startswith(protocol)
                   for protocol in ('http://', 'https://')):
            url = self.library_api_base_url.decode("utf-8") + url
        return url

    def get(self, url, extra_headers={}, exception_on_401=False):
        """Make an HTTP GET request using the active Bearer Token."""
        if extra_headers is None:
            extra_headers = {}
        headers = dict(Authorization="Bearer %s" % self.token)
        headers.update(extra_headers)
        status_code, headers, content = self._do_get(self.library_api_base_url.decode("utf-8") + url, headers)
        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise BadResponseException.from_response(
                    url,
                    "Something's wrong with the Odilo OAuth Bearer Token!",
                    (status_code, headers, content)
                )
            else:
                # Refresh the token and try again.
                self.check_creds(True)
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    def token_post(self, url, payload, headers={}, **kwargs):
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        s = "%s:%s" % (self.client_key, self.client_secret)
        auth = base64.standard_b64encode(s).strip()
        headers = dict(headers)
        headers['Authorization'] = "Basic %s" % auth
        headers['Content-Type'] = "application/x-www-form-urlencoded"
        return self._do_post(self.library_api_base_url + url, payload, headers, **kwargs)


    def checkout(self, patron, pin, licensepool, internal_format):
        """Check out a book on behalf of a patron.

        :param patron: a Patron object for the patron who wants
            to check out the book.

        :param pin: The patron's alleged password.

        :param licensepool: Identifier of the book to be checked out is
            attached to this licensepool.

        :param internal_format: Represents the patron's desired book format.

        :return: a LoanInfo object.
        """
        record_id = licensepool.identifier.identifier

        # Data just as 'x-www-form-urlencoded', no JSON

        payload = dict(
            patronId=patron.authorization_identifier,
            format=internal_format,
        )

        response = self.patron_request(
            patron, pin, self.CHECKOUT_ENDPOINT.format(recordId=record_id),
            extra_headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=payload)
        if response.content:
            response_json = response.json()
            if response.status_code == 404:
                self.raise_exception_on_error(response_json, default_exception_class=CannotLoan)
            else:
                return self.loan_info_from_odilo_checkout(licensepool.collection, response_json)

        # TODO: we need to improve this at the API and use an error code
        elif response.status_code == 400:
            raise NoAcceptableFormat('record_id: %s, format: %s' % (record_id, internal_format))

        raise CannotLoan('patron: %s, record_id: %s, format: %s' % (patron, record_id, internal_format))

    def loan_info_from_odilo_checkout(self, collection, checkout):
        start_date = self.extract_date(checkout, 'startTime')
        end_date = self.extract_date(checkout, 'endTime')

        return LoanInfo(
            collection,
            DataSource.ODILO,
            Identifier.ODILO_ID,
            checkout['id'],
            start_date,
            end_date,
            checkout['downloadUrl']
        )

    def checkin(self, patron, pin, licensepool):
        record_id = licensepool.identifier.identifier
        loan = self.get_checkout(patron, pin, record_id)
        url = self.CHECKIN_ENDPOINT.format(checkoutId=loan['id'], patronId=patron.authorization_identifier)

        response = self.patron_request(patron, pin, url, method='POST')
        if response.status_code == 200:
            return response

        self.raise_exception_on_error(response.json(), default_exception_class=CannotReturn)

    @classmethod
    def extract_date(cls, data, field_name):
        if field_name not in data or not data[field_name]:
            d = None
        else:
            # OdiloAPI dates are timestamps in milliseconds
            d = from_timestamp(float(data[field_name]) / 1000.0)
        return d

    @classmethod
    def raise_exception_on_error(cls, data, default_exception_class=None, ignore_exception_codes=None):
        if not data or 'errors' not in data or len(data['errors']) <= 0:
            return '', ''

        error = data['errors'][0]
        error_code = error['id']
        message = ('description' in error and error['description']) or ''

        if not ignore_exception_codes or error_code not in ignore_exception_codes:
            if error_code in cls.error_to_exception:
                raise cls.error_to_exception[error_code](message)
            elif default_exception_class:
                raise default_exception_class(message)

    def get_checkout(self, patron, pin, record_id):
        patron_checkouts = self.get_patron_checkouts(patron, pin)
        for checkout in patron_checkouts:
            if checkout['recordId'] == record_id:
                return checkout

        raise NotFoundOnRemote("Could not find active loan for patron %s, record %s" % (patron, record_id))

    def get_hold(self, patron, pin, record_id):
        patron_holds = self.get_patron_holds(patron, pin)
        for hold in patron_holds:
            if hold['recordId'] == record_id and hold['status'] in ('informed', 'waiting'):
                return hold

        raise NotFoundOnRemote("Could not find active hold for patron %s, record %s" % (patron, record_id))

    def fulfill(self, patron, pin, licensepool, internal_format, **kwargs):
        """Get the actual resource file to the patron.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """
        record_id = licensepool.identifier.identifier
        content_link, content, content_type = self.get_fulfillment_link(patron, pin, record_id, internal_format)

        if not content_link and not content:
            self.log.info("Odilo record_id %s was not available as %s" % (record_id, internal_format))
        else:
            return FulfillmentInfo(
                licensepool.collection,
                DataSource.ODILO,
                Identifier.ODILO_ID,
                record_id,
                content_link=content_link,
                content=content,
                content_type=content_type,
                content_expires=None
            )

    def get_fulfillment_link(self, patron, pin, record_id, format_type):
        """Get the link corresponding to an existing checkout.
        """
        # Retrieve checkout with its download_ulr. It is necessary to generate a download token in our API
        checkout = self.get_checkout(patron, pin, record_id)
        loan_format = checkout['format']
        if format_type and loan_format and (
                        format_type == loan_format or
                        (loan_format == OdiloRepresentationExtractor.ACSM and format_type in (OdiloRepresentationExtractor.ACSM_EPUB, OdiloRepresentationExtractor.ACSM_PDF))
        ):
            if 'downloadUrl' in checkout and checkout['downloadUrl']:
                content_link = checkout['downloadUrl']
                content = None
                content_type = OdiloRepresentationExtractor.format_data_for_odilo_format[format_type]

                # Get also .acsm file
                if format_type in (OdiloRepresentationExtractor.ACSM_EPUB, OdiloRepresentationExtractor.ACSM_PDF):
                    response = self.patron_request(patron, pin, content_link)
                    if response.status_code == 200:
                        content = response.content
                    elif response.status_code == 404 and response.content:
                        self.raise_exception_on_error(response.json(), CannotFulfill)

                return content_link, content, content_type

        raise CannotFulfill("Cannot obtain a download link for patron[%r], record_id[%s], format_type[%s].", patron,
                            record_id, format_type)

    def get_patron_checkouts(self, patron, pin):
        data = self.patron_request(patron, pin, self.PATRON_CHECKOUTS_ENDPOINT.format(patronId=patron.authorization_identifier)).json()
        self.raise_exception_on_error(data)
        return data

    def get_patron_holds(self, patron, pin):
        data = self.patron_request(patron, pin, self.PATRON_HOLDS_ENDPOINT.format(patronId=patron.authorization_identifier)).json()
        self.raise_exception_on_error(data)
        return data

    def patron_activity(self, patron, pin):
        odilo_checkouts = self.get_patron_checkouts(patron, pin)
        odilo_holds = self.get_patron_holds(patron, pin)

        loans_info = []
        holds_info = []

        collection = self.collection

        for checkout in odilo_checkouts:
            loan_info = self.loan_info_from_odilo_checkout(collection, checkout)
            loans_info.append(loan_info)

        for hold in odilo_holds:
            hold_info = self.hold_from_odilo_hold(collection, hold)
            holds_info.append(hold_info)

        return loans_info + holds_info

    def hold_from_odilo_hold(self, collection, hold):
        start = self.extract_date(hold, 'startTime')
        # end_date: The estimated date the title will be available for the patron to borrow.
        end = self.extract_date(hold, 'notifiedTime')
        position = hold.get('holdQueuePosition')

        if position is not None:
            position = int(position)

        # Patron already notified to borrow the title
        if 'informed' == hold['status']:
            position = 0

        return HoldInfo(
            collection,
            DataSource.ODILO,
            Identifier.ODILO_ID,
            hold['id'],
            start_date=start,
            end_date=end,
            hold_position=position
        )

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldInfo object
        """

        record_id = licensepool.identifier.identifier

        # Data just as 'x-www-form-urlencoded', no JSON
        payload = dict(patronId=patron.authorization_identifier)

        response = self.patron_request(
            patron, pin, self.PLACE_HOLD_ENDPOINT.format(recordId=record_id),
            extra_headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=payload)

        data = response.json()
        if response.status_code == 200:
            return self.hold_from_odilo_hold(licensepool.collection, data)

        self.raise_exception_on_error(data, CannotHold)

    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.
        """

        record_id = licensepool.identifier.identifier
        hold = self.get_hold(patron, pin, record_id)
        url = self.RELEASE_HOLD_ENDPOINT.format(holdId=hold['id'])
        payload = json.dumps(dict(patronId=patron.authorization_identifier))

        response = self.patron_request(patron, pin, url, extra_headers={}, data=payload, method='POST')
        if response.status_code == 200:
            return True

        self.raise_exception_on_error(response.json(), default_exception_class=CannotReleaseHold,
                                      ignore_exception_codes=['HOLD_NOT_FOUND'])
        return True

    @staticmethod
    def _update_credential(credential, odilo_data):
        """Copy Odilo OAuth data into a Credential object."""
        credential.credential = odilo_data['token']
        if odilo_data['expiresIn'] == -1:
            # This token never expires.
            credential.expires = None
        else:
            expires_in = (odilo_data['expiresIn'] * 0.9)
            credential.expires = utc_now() + datetime.timedelta(seconds=expires_in)

    def get_metadata(self, record_id):
        identifier = record_id
        if isinstance(record_id, Identifier):
            identifier = record_id.identifier

        url = self.RECORD_METADATA_ENDPOINT.format(recordId=identifier)

        status_code, headers, content = self.get(url)
        if status_code == 200 and content:
            return content
        else:
            msg = 'Cannot retrieve metadata for record: ' + record_id + ' response http ' + status_code
            if content:
                msg += ' content: ' + content
            self.log.warn(msg)
            return None

    def get_availability(self, record_id):
        url = self.RECORD_AVAILABILITY_ENDPOINT.format(recordId=record_id)
        status_code, headers, content = self.get(url)
        content = json.loads(content)

        if status_code == 200 and len(content) > 0:
            return content
        else:
            msg = 'Cannot retrieve availability for record: ' + record_id + ' response http ' + status_code
            if content:
                msg += ' content: ' + content
            self.log.warn(msg)
            return None

    @staticmethod
    def _do_get(url, headers, **kwargs):
        # More time please
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 60

        if 'allow_redirects' not in kwargs:
            kwargs['allow_redirects'] = True

        response = HTTP.get_with_timeout(url, headers=headers, **kwargs)
        return response.status_code, response.headers, response.content

    @staticmethod
    def _do_post(url, payload, headers, **kwargs):
        # More time please
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 60

        return HTTP.post_with_timeout(url, payload, headers=headers, **kwargs)



class OdiloCirculationMonitor(CollectionMonitor, TimelineMonitor):
    """Maintain LicensePools for recently changed Odilo titles
    """
    SERVICE_NAME = "Odilo Circulation Monitor"
    INTERVAL_SECONDS = 500
    PROTOCOL = ExternalIntegration.ODILO
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    def __init__(self, _db, collection, api_class=OdiloAPI):
        """Constructor."""
        super(OdiloCirculationMonitor, self).__init__(_db, collection)
        self.api = api_class(_db, collection)

    def catch_up_from(self, start, cutoff, progress):
        """Find Odilo books that changed recently.

        :progress: A TimestampData representing the time previously
            covered by this Monitor.
        """

        self.log.info("Starting recently_changed_ids, start: " + str(start) + ", cutoff: " + str(cutoff))

        start_time = utc_now()
        updated, new = self.all_ids(start)
        finish_time = utc_now()

        time_elapsed = finish_time - start_time
        self.log.info("recently_changed_ids finished in: " + str(time_elapsed))
        progress.achievements = (
            "Updated records: %d. New records: %d." % (updated, new)
        )

    def all_ids(self, modification_date=None):
        """Get IDs for every book in the system, from modification date if any
        """

        retrieved = 0
        parsed = 0
        new = 0
        offset = 0
        limit = self.api.PAGE_SIZE_LIMIT

        if modification_date and isinstance(modification_date, datetime.date):
            modification_date = modification_date.strftime('%Y-%m-%d')  # Format YYYY-MM-DD

        # Retrieve first group of records
        url = self.get_url(limit, modification_date, offset)
        status_code, headers, content = self.api.get(url)
        content = json.loads(content)

        # Retrieve Odilo record in groups
        while status_code == 200 and len(content) > 0:
            offset += limit
            retrieved += len(content)
            self.log.info('Retrieved %i records' % retrieved)

            # Process a bunch of records retrieved
            for record in content:
                record_id = record['id']
                self.log.info('Processing record %i/%i: %s' % (parsed, retrieved, record_id))
                identifier, is_new = self.api.odilo_bibliographic_coverage_provider.process_item(
                    record_id, record
                )

                if is_new:
                    new += 1

                parsed += 1

            # Persist each bunch of retrieved records
            self._db.commit()

            # Retrieve next group of records
            url = self.get_url(limit, modification_date, offset)
            status_code, headers, content = self.api.get(url)
            content = json.loads(content)

        if status_code >= 400:
            self.log.error('ERROR: Fail while retrieving data from remote source: HTTP ' + status_code)
            if content:
                self.log.error('ERROR response content: ' + str(content))
        else:
            self.log.info('Retrieving all ids finished ok. Retrieved %i records. New records: %i!!' % (retrieved, new))
        return retrieved, new

    def get_url(self, limit, modification_date, offset):
        url = "%s?limit=%i&offset=%i" % (self.api.ALL_PRODUCTS_ENDPOINT, limit, offset)
        if modification_date:
            url = "%s&modificationDate=%s" % (url, modification_date)

        return url

class MockOdiloAPI(OdiloAPI):
    def patron_request(self, patron, pin, *args, **kwargs):
        response = self._make_request(*args, **kwargs)

        # Modify the record of the request to include the patron information.
        original_data = self.requests[-1]

        # The last item in the record of the request is keyword arguments.
        # Stick this information in there to minimize confusion.
        original_data[-1]['_patron'] = patron
        original_data[-1]['_pin'] = pin
        return response
    @classmethod
    def mock_collection(cls, _db):
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test Odilo Collection",
            create_method_kwargs=dict(
                external_account_id='library_id_123',
            )
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.ODILO
        )
        integration.username = 'username'
        integration.password = 'password'
        integration.setting(OdiloAPI.LIBRARY_API_BASE_URL).value = 'http://library_api_base_url/api/v2'
        library.collections.append(collection)

        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.access_token_requests = []
        self.requests = []
        self.responses = []

        self.access_token_response = self.mock_access_token_response('bearer token')
        super(MockOdiloAPI, self).__init__(_db, collection, *args, **kwargs)

    def token_post(self, url, payload, headers={}, **kwargs):
        """Mock the request for an OAuth token.
        """

        self.access_token_requests.append((url, payload, headers, kwargs))
        response = self.access_token_response
        return HTTP._process_response(url, response, **kwargs)

    def mock_access_token_response(self, credential, expires_in=-1):
        token = dict(token=credential, expiresIn=expires_in)
        return MockRequestsResponse(200, {}, json.dumps(token))

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _do_get(self, url, *args, **kwargs):
        """Simulate Representation.simple_http_get."""
        response = self._make_request(url, *args, **kwargs)
        return response.status_code, response.headers, response.content

    def _do_post(self, url, *args, **kwargs):
        return self._make_request(url, *args, **kwargs)

    def _make_request(self, url, *args, **kwargs):
        response = self.responses.pop()
        self.requests.append((url, args, kwargs))
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )


class OdiloBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Odilo records.

    This will occasionally fill in some availability information for a
    single Collection, but we rely on Monitors to keep availability
    information up to date for all Collections.
    """

    SERVICE_NAME = "Odilo Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.ODILO
    PROTOCOL = ExternalIntegration.ODILO
    INPUT_IDENTIFIER_TYPES = Identifier.ODILO_ID

    def __init__(self, collection, api_class=OdiloAPI, **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Odilo books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating OdiloAPI.
        """
        super(OdiloBibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )
        if isinstance(api_class, OdiloAPI):
            # Use a previously instantiated OdiloAPI instance
            # rather than creating a new one.
            self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection)

        self.replacement_policy = ReplacementPolicy(
            identifiers=True,
            subjects=True,
            contributions=True,
            links=True,
            formats=True,
            rights=True,
            link_content=True,
            # even_if_not_apparently_updated=False,
            analytics=Analytics(self._db)
        )

    def process_item(self, record_id, record=None):
        if not record:
            record = self.api.get_metadata(record_id)

        if not record:
            return self.failure(record_id, 'Record not found', transient=False)

        # Retrieve availability
        availability = self.api.get_availability(record_id)

        metadata, is_active = OdiloRepresentationExtractor.record_info_to_metadata(record, availability)
        if not metadata:
            e = "Could not extract metadata from Odilo data: %s" % record_id
            return self.failure(record_id, e)

        identifier, made_new = metadata.primary_identifier.load(_db=self._db)

        if not identifier:
            e = "Could not create identifier for Odilo data: %s" % record_id
            return self.failure(identifier, e)

        identifier = self.set_metadata(identifier, metadata)

        # calls work.set_presentation_ready() for us
        self.handle_success(identifier)

        return identifier, made_new
