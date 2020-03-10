from nose.tools import set_trace
import time
import datetime
import os
import json
import logging
from flask_babel import lazy_gettext as _

from config import (
    CannotLoadConfiguration,
)

from circulation import (
    LoanInfo,
    FulfillmentInfo,
    BaseCirculationAPI
)

from circulation_exceptions import *

from selftest import (
    HasSelfTests,
    SelfTestResult,
)

from core.util.http import (
    HTTP,
    RemoteIntegrationException,
    RequestTimedOut,
)

from core.model import (
    CirculationEvent,
    Classification,
    Collection,
    ConfigurationSetting,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    Session,
    Subject,
)

from core.metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LinkData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
)

from core.monitor import (
    Monitor,
    IdentifierSweepMonitor,
    CollectionMonitor,
    TimelineMonitor,
)

from core.analytics import Analytics
from core.testing import DatabaseTest


class EnkiAPI(BaseCirculationAPI, HasSelfTests):

    PRODUCTION_BASE_URL = "https://enkilibrary.org/API/"

    ENKI_LIBRARY_ID_KEY = u'enki_library_id'
    DESCRIPTION = _("Integrate an Enki collection.")
    SETTINGS = [
        { "key": ExternalIntegration.URL, "label": _("URL"), "default": PRODUCTION_BASE_URL, "required": True, "format": "url" },
    ] + BaseCirculationAPI.SETTINGS

    LIBRARY_SETTINGS = [
        { "key": ENKI_LIBRARY_ID_KEY, "label": _("Library ID"), "required": True },
    ]

    list_endpoint = "ListAPI"
    item_endpoint = "ItemAPI"
    user_endpoint = "UserAPI"

    NAME = u"Enki"
    ENKI = NAME
    ENKI_EXTERNAL = NAME
    ENKI_ID = u"Enki ID"

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Enki format types.
    epub = Representation.EPUB_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    no_drm = DeliveryMechanism.NO_DRM

    delivery_mechanism_to_internal_format = {
        (epub, no_drm): 'free',
        (epub, adobe_drm): 'acs',
    }

    # Enki API serves all responses with a 200 error code and a
    # text/html Content-Type. However, there's a string that
    # reliably shows up in error pages which is unlikely to show up
    # in normal API operation.
    ERROR_INDICATOR = '<h1>Oops, an error occurred</h1>'

    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP
    SERVICE_NAME = "Enki"
    log = logging.getLogger("Enki API")

    def __init__(self, _db, collection):
        self._db = _db
        if collection.protocol != self.ENKI:
            raise ValueError(
                "Collection protocol is %s, but passed into EnkiAPI!" %
                collection.protocol
            )

        self.collection_id = collection.id
        self.base_url = collection.external_integration.url or self.PRODUCTION_BASE_URL

    def external_integration(self, _db):
        return self.collection.external_integration

    def enki_library_id(self, library):
        """Find the Enki library ID for the given library."""
        _db = Session.object_session(library)
        return ConfigurationSetting.for_library_and_externalintegration(
            _db, self.ENKI_LIBRARY_ID_KEY, library,
            self.external_integration(_db)
        ).value

    @property
    def collection(self):
        return Collection.by_id(self._db, id=self.collection_id)

    def _run_self_tests(self, _db):

        now = datetime.datetime.utcnow()

        def count_loans_and_holds():
            """Count recent circulation events that affected loans or holds.
            """
            one_hour_ago = now - datetime.timedelta(hours=1)
            count = len(list(self.recent_activity(one_hour_ago, now)))
            return "%s circulation events in the last hour" % count

        yield self.run_test(
            "Counting recent circulation changes.",
            count_loans_and_holds
        )

        def count_title_changes():
            """Count changes to title metadata (usually because of
            new titles).
            """
            one_day_ago = now - datetime.timedelta(hours=24)
            return "%s titles added/updated in the last day" % (
                len(list(self.updated_titles(since=one_day_ago)))
            )

        yield self.run_test(
            "Counting recent collection changes.",
            count_title_changes,
        )

        for result in self.default_patrons(self.collection):
            if isinstance(result, SelfTestResult):
                yield result
                continue
            library, patron, pin = result
            task = "Checking patron activity, using test patron for library %s" % library.name
            def count_loans_and_holds(patron, pin):
                activity = list(self.patron_activity(patron, pin))
                return "Total loans and holds: %s" % len(activity)
            yield self.run_test(
                task, count_loans_and_holds, patron, pin
            )

    def request(self, url, method='get', extra_headers={}, data=None,
                params=None, retry_on_timeout=True, **kwargs):
        """Make an HTTP request to the Enki API."""
        headers = dict(extra_headers)
        response = None
        try:
            response = self._request(
                method, url, headers=headers, data=data,
                params=params,
                **kwargs
            )
        except RequestTimedOut, e:
            if not retry_on_timeout:
                raise e
            self.log.info(
                "Request to %s timed out once. Trying a second time.", url
            )
            return self.request(
                url, method, extra_headers,
                data, params, retry_on_timeout=False,
                **kwargs
            )

        # Look for the error indicator and raise
        # RemoteIntegrationException if it appears.
        if response.content and self.ERROR_INDICATOR in response.content:
            raise RemoteIntegrationException(url, "An unknown error occured")
        return response

    def _request(self, method, url, headers, data, params, **kwargs):
        """Actually make an HTTP request.

        MockEnkiAPI overrides this method.
        """
        return HTTP.request_with_timeout(
            method, url, headers=headers, data=data,
            params=params, timeout=90, disallowed_response_codes=None,
            **kwargs
        )

    @classmethod
    def _minutes_since(cls, since):
        """How many minutes have elapsed since `since`?

        This is a helper method to create the `minutes` parameter to
        the API.
        """
        now = datetime.datetime.utcnow()
        return int((now - since).total_seconds() / 60)

    def recent_activity(self, start, end):
        """Find circulation events from a certain timeframe that affected
        loans or holds.

        :param start: A DateTime
        :yield: A sequence of CirculationData objects.
        """
        epoch = datetime.datetime.utcfromtimestamp(0)
        start = int((start - epoch).total_seconds())
        end = int((end - epoch).total_seconds())

        url = self.base_url + self.item_endpoint
        args = dict(
            method='getRecentActivityTime',
            stime=str(start),
            etime=str(end)
        )
        response = self.request(url, params=args)
        data = json.loads(response.content)
        parser = BibliographicParser()
        for element in data['result']['recentactivity']:
            identifier = IdentifierData(Identifier.ENKI_ID, element['id'])
            yield parser.extract_circulation(
                identifier, element['availability'], None # The recent activity API does not include format info
            )

    def updated_titles(self, since):
        """Find recent changes to book metadata.

        NOTE: getUpdateTitles will return a maximum of 1000 items, so
        in theory this may need to be paginated. This shouldn't be a
        problem assuming the monitor is run regularly.

        :param since: A DateTime
        :yield: A sequence of Metadata objects.
        """
        minutes = self._minutes_since(since)
        url = self.base_url + self.list_endpoint
        args = dict(
            method='getUpdateTitles',
            minutes=minutes,
            id='secontent',
            lib='0', # This is a stand-in value -- it doesn't matter
                     # which library we ask about since they all have
                     # the same collection.
        )
        response = self.request(url, params=args)
        for metadata in BibliographicParser().process_all(response.content):
            yield metadata

    def get_item(self, enki_id):
        """Retrieve bibliographic and availability information for
        a specific title.

        :param enki_id: An Enki record ID.
        :return: If the book is in the library's collection, a
            Metadata object with attached CirculationData. Otherwise, None.
        """
        url = self.base_url + self.item_endpoint
        args = dict(
            method="getItem",
            recordid=enki_id,
            size="large",
            lib='0', # This is a stand-in value -- it doesn't matter
                     # which library we ask about since they all have
                     # the same collection.
        )
        response = self.request(url, params=args)
        try:
            data = json.loads(response.content)
        except ValueError, e:
            # This is most likely a 'not found' error.
            return None

        book = data.get('result', {})
        if book:
            return BibliographicParser().extract_bibliographic(book)
        return None

    def get_all_titles(self, strt=0, qty=10):
        """Retrieve a single page of items from the Enki collection.

        Iterating over the entire collection is very expensive and
        should only happen during initial data population.

        :yield: A sequence of Metadata objects, each with a
            CirculationData attached.
        """
        self.log.debug ("requesting : "+ str(qty) + " books starting at econtentRecord" +  str(strt))
        url = str(self.base_url) + str(self.list_endpoint)
        args = dict()
        args['method'] = "getAllTitles"
        args['id'] = "secontent"
        args['strt'] = strt
        args['qty'] = qty
        response = self.request(url, params=args)
        for metadata in BibliographicParser().process_all(response.content):
            yield metadata

    @classmethod
    def _epoch_to_struct(cls, epoch_string):
        # This will turn the time string we get from Enki into a
        # struct that the Circulation Manager can make use of.
        time_format = "%Y-%m-%dT%H:%M:%S"
        return datetime.datetime.strptime(
            time.strftime(time_format, time.gmtime(float(epoch_string))),
            time_format
        )

    def checkout(self, patron, pin, licensepool, internal_format):
        identifier = licensepool.identifier
        enki_id = identifier.identifier
        enki_library_id = self.enki_library_id(patron.library)
        response = self.loan_request(
            patron.authorization_identifier, pin, enki_id,
            enki_library_id
        )
        if response.status_code != 200:
            raise CannotLoan(response.status_code)
        result = json.loads(response.content)['result']
        if not result['success']:
            message = result['message']
            if "There are no available copies" in message:
                self.log.error("There are no copies of book %s available." % enki_id)
                raise NoAvailableCopies()
            elif "Login unsuccessful" in message:
                self.log.error("User validation against Enki server with %s / %s was unsuccessful."
                    % (patron.authorization_identifier, pin))
                raise AuthorizationFailedException()
        due_date = result['checkedOutItems'][0]['duedate']
        expires = self._epoch_to_struct(due_date)

        # Create the loan info.
        loan = LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            None,
            expires,
            None,
        )
        return loan

    def loan_request(self, barcode, pin, book_id, enki_library_id):
        self.log.debug ("Sending checkout request for %s" % book_id)
        url = str(self.base_url) + str(self.user_endpoint)
        args = dict()
        args['method'] = "getSELink"
        args['username'] = barcode
        args['password'] = pin
        args['lib'] = enki_library_id
        args['id'] = book_id

        response = self.request(url, method='get', params=args)
        return response

    def fulfill(self, patron, pin, licensepool, internal_format, **kwargs):
        """Get the actual resource file to the patron.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """
        book_id = licensepool.identifier.identifier
        enki_library_id = self.enki_library_id(patron.library)
        response = self.loan_request(
            patron.authorization_identifier, pin, book_id, enki_library_id
        )
        if response.status_code != 200:
            raise CannotFulfill(response.status_code)
        result = json.loads(response.content)['result']
        if not result['success']:
            message = result['message']
            if "There are no available copies" in message:
                self.log.error("There are no copies of book %s available." % book_id)
                raise NoAvailableCopies()
            elif "Login unsuccessful" in message:
                self.log.error("User validation against Enki server with %s / %s was unsuccessful."
                    % (patron.authorization_identifier, pin))
                raise AuthorizationFailedException()

        url, item_type, expires = self.parse_fulfill_result(result)
        # We don't know for sure which DRM scheme is in use, (that is,
        # whether the content link points to the actual book or an
        # ACSM file) but since Enki titles only have a single delivery
        # mechanism, it's easy to make a guess.
        drm_type = self.no_drm
        for lpdm in licensepool.delivery_mechanisms:
            delivery_mechanism = lpdm.delivery_mechanism
            if delivery_mechanism:
                drm_type = delivery_mechanism.drm_scheme
                break

        return FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link=url,
            content_type=drm_type,
            content=None,
            content_expires=expires
        )

    def parse_fulfill_result(self, result):
        links = result['checkedOutItems'][0]['links'][0]
        url = links['url']
        item_type = links['item_type']
        due_date = result['checkedOutItems'][0]['duedate']
        expires = self._epoch_to_struct(due_date)
        return (url, item_type, expires)

    def patron_activity(self, patron, pin):
        enki_library_id = self.enki_library_id(patron.library)
        response = self.patron_request(
            patron.authorization_identifier, pin, enki_library_id
        )
        if response.status_code != 200:
            raise PatronNotFoundOnRemote(response.status_code)
        result = json.loads(response.content).get('result', {})
        if not result.get('success'):
            message = result.get('message', '')
            if "Login unsuccessful" in message:
                raise AuthorizationFailedException()
            else:
                self.log.error(
                    "Unexpected error in patron_activity: %r",
                    response.content
                )
                raise CirculationException(response.content)
        for loan in result['checkedOutItems']:
            yield self.parse_patron_loans(loan)
        for type, holds in result['holds'].items():
            for hold in holds:
                yield self.parse_patron_holds(hold)

    def patron_request(self, patron, pin, enki_library_id):
        self.log.debug ("Querying Enki for information on patron %s" % patron)
        url = str(self.base_url) + str(self.user_endpoint)
        args = dict()
        args['method'] = "getSEPatronData"
        args['username'] = patron
        args['password'] = pin
        args['lib'] = enki_library_id

        return self.request(url, method='get', params=args)

    def parse_patron_loans(self, checkout_data):
        # We should receive a list of JSON objects
        enki_id = checkout_data['id']
        start_date = self._epoch_to_struct(checkout_data['checkoutdate'])
        end_date = self._epoch_to_struct(checkout_data['duedate'])
        return LoanInfo(
            self.collection,
            DataSource.ENKI,
            Identifier.ENKI_ID,
            enki_id,
            start_date=start_date,
            end_date=end_date,
            fulfillment_info=None
        )

    def parse_patron_holds(self, hold_data):
        pass

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        pass

    def release_hold(self, patron, pin, licensepool):
        pass


class MockEnkiAPI(EnkiAPI):
    def __init__(self, _db, collection=None, *args, **kwargs):
        self.responses = []
        self.requests = []

        library = DatabaseTest.make_default_library(_db)
        if not collection:
            collection, ignore = Collection.by_name_and_protocol(
                _db, name="Test Enki Collection", protocol=EnkiAPI.ENKI
            )
            collection.protocol=EnkiAPI.ENKI
        if collection not in library.collections:
            library.collections.append(collection)

        # Set the "Enki library ID" variable between the default library
        # and this Enki collection.
        ConfigurationSetting.for_library_and_externalintegration(
            _db, self.ENKI_LIBRARY_ID_KEY, library,
            collection.external_integration
        ).value = 'c'

        super(MockEnkiAPI, self).__init__(
            _db, collection, *args, **kwargs
        )

    def queue_response(self, status_code, headers={}, content=None):
        from core.testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _request(self, method, url, headers, data, params, **kwargs):
        """Override EnkiAPI._request to pull responses from a
        queue instead of making real HTTP requests
        """
        self.requests.append([method, url, headers, data, params, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes'),
        )


class BibliographicParser(object):
    """Parses Enki's representation of book information into
    Metadata and CirculationData objects.
    """

    log = logging.getLogger("Enki Bibliographic Parser")

    # Convert the English names of languages given in the Enki API to
    # the codes we use internally.
    LANGUAGE_CODES = {
        "English": u"eng",
        "French" : u"fre",
        "Spanish": u"spa",
    }

    def process_all(self, json_data):
        if isinstance(json_data, basestring):
            json_data = json.loads(json_data)
        returned_titles = json_data.get("result", {}).get("titles", [])
	for book in returned_titles:
            data = self.extract_bibliographic(book)
            if data:
                yield data

    def extract_bibliographic(self, element):
        """Extract Metadata and CirculationData from a dictionary
        of information from Enki.

        :return: A Metadata with attached CirculationData.
        """
        # TODO: it's not clear what these are or whether we'd find them
        # useful:
        #  dateSaved
        #  length
        #  publishDate
        primary_identifier = IdentifierData(EnkiAPI.ENKI_ID, element["id"])

        identifiers = []
        identifiers.append(IdentifierData(Identifier.ISBN, element["isbn"]))

        contributors = []
        sort_name = element.get("author", None) or Edition.UNKNOWN_AUTHOR
        contributors.append(ContributorData(sort_name=sort_name))

        links = []
        description = element.get('description')
        if description:
            links.append(
                LinkData(rel=Hyperlink.DESCRIPTION, content=description,
                         media_type="text/html")
            )

        # NOTE: When this method is called by, e.g. updated_titles(),
        # the large and small images are available separately. When
        # this method is called by get_item(), we only get a single
        # image, in 'cover'. In get_item() we ask that that image be 'large',
        # which means we'll be filing it as a normal-sized image.
        #
        full_image = None
        thumbnail_image = None
        for key, rel in (
                ('cover', Hyperlink.IMAGE),
                ('small_image', Hyperlink.THUMBNAIL_IMAGE),
                ('large_image', Hyperlink.IMAGE)
        ):
            url = element.get(key)
            if not url:
                continue
            link = LinkData(
                rel=rel, href=url, media_type=Representation.PNG_MEDIA_TYPE
            )
            if rel == Hyperlink.THUMBNAIL_IMAGE:
                # Don't add a thumbnail to the list of links -- wait
                # until the end and then make it a thumbnail of the
                # primary image.
                thumbnail_image = link
            else:
                if rel == Hyperlink.IMAGE:
                    full_image = link
                links.append(link)

        if thumbnail_image:
            if full_image:
                # Set the thumbnail as the thumbnail _of_ the full image.
                full_image.thumbnail = thumbnail_image
            else:
                # Treat the thumbnail as the full image.
                thumbnail_image.rel = Hyperlink.IMAGE
                links.append(thumbnail_image)

        # We treat 'subject', 'topic', and 'genre' as interchangeable
        # sets of tags. This data is based on BISAC but it's not reliably
        # presented in a form that can be parsed as BISAC.
        subjects = []
        seen_topics = set()
        for key in ('subject', 'topic', 'genre'):
            for topic in element.get(key, []):
                if not topic or topic in seen_topics:
                    continue
                subjects.append(
                    SubjectData(
                        Subject.TAG, topic,
                        weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT
                    )
                )
                seen_topics.add(topic)

        language_code = element.get("language", "English")
        language = self.LANGUAGE_CODES.get(language_code, "eng")

        metadata = Metadata(
            data_source=DataSource.ENKI,
            title=element.get("title"),
            language=language,
            medium=Edition.BOOK_MEDIUM,
            publisher=element.get("publisher"),
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            contributors=contributors,
            links=links,
            subjects=subjects,
        )
        circulationdata = self.extract_circulation(
            primary_identifier, element.get('availability', {}), element.get('formattype', None)
        )
        metadata.circulation = circulationdata
        return metadata

    def extract_circulation(self, primary_identifier, availability, formattype):
        """Turn the 'availability' portion of an Enki API response into
        a CirculationData.
        """
        if not availability:
            return None
        licenses_owned=availability.get("totalCopies", 0)
        licenses_available=availability.get("availableCopies", 0)
        hold=availability.get("onHold", 0)
        drm_type = EnkiAPI.no_drm
        if availability.get('accessType') == 'acs':
            drm_type = EnkiAPI.adobe_drm
        formats = []

        content_type = None
        if formattype == 'PDF':
            content_type = Representation.PDF_MEDIA_TYPE
        elif formattype == 'EPUB':
            content_type=Representation.EPUB_MEDIA_TYPE
        if content_type != None:
            formats.append(
                FormatData(
                    content_type,
                    drm_scheme=drm_type
                )
            )
        else:
            self.log.error("Unrecognized formattype: %s", formattype)

        circulationdata = CirculationData(
            data_source=DataSource.ENKI,
            primary_identifier=primary_identifier,
            formats=formats,
            licenses_owned = int(licenses_owned),
            licenses_available = int(licenses_available),
            licenses_reserved = 0,
            patrons_in_hold_queue = int(hold)
        )
        return circulationdata


class EnkiImport(CollectionMonitor, TimelineMonitor):
    """Make sure our local collection is up-to-date with the remote
    Enki collection.
    """
    SERVICE_NAME = "Enki Circulation Monitor"
    INTERVAL_SECONDS = 500
    PROTOCOL = EnkiAPI.ENKI_EXTERNAL
    DEFAULT_BATCH_SIZE = 10
    FIVE_MINUTES = datetime.timedelta(minutes=5)
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    def __init__(self, _db, collection, api_class=EnkiAPI, analytics=None):
        """Constructor."""
        super(EnkiImport, self).__init__(_db, collection)
        self._db = _db
        if callable(api_class):
            api = api_class(_db, collection)
        else:
            api = api_class
        self.api = api
        self.collection_id = collection.id
        self.analytics = analytics or Analytics(_db)

    @property
    def collection(self):
        return Collection.by_id(self._db, id=self.collection_id)

    def catch_up_from(self, start, cutoff, progress):
        """Find Enki books that changed recently.

        :param start: Find all books that changed since this date.
        """
        if start is None:
            # This is the first time the monitor has run, so it's
            # important that we get the entire collection, even though that
            # will take a long time.
            new_titles = self.full_import()
            circulation_updates = 0
        else:
            # We've run the monitor before, so we just need to learn
            # about new titles and circulation changes since the last time.
            #
            # Give us five minutes of overlap because it's very important
            # we don't miss anything.
            new_titles, circulation_updates = self.incremental_import(start)

        progress.achievements = (
            "New or modified titles: %d. Titles with circulation changes: %d." % (
                new_titles, circulation_updates
            )
        )

    def full_import(self):
        """Import the entire Enki collection, page by page."""
        id_start = 0
        batch_size = self.DEFAULT_BATCH_SIZE
        total_items = 0
        while True:
            items_this_page = 0
            for bibliographic in self.api.get_all_titles(
                strt=id_start, qty=batch_size
            ):
                self.process_book(bibliographic)
                items_this_page += 1
                total_items += 1
            self._db.commit()
            if items_this_page == 0:
                # When we get an empty page we know it's time to stop.
                break
            id_start += self.DEFAULT_BATCH_SIZE
        return total_items

    def incremental_import(self, since):
        # Take care of new titles and titles with updated metadata.
        new_titles = 0
        for metadata in self.api.updated_titles(since):
            self.process_book(metadata)
            new_titles += 1
        self._db.commit()

        # Take care of titles whose circulation status changed.
        circulation_changes = self.update_circulation(since)
        self._db.commit()
        return new_titles, circulation_changes

    def update_circulation(self, since):
        """Process circulation events that happened since `since`.

        :return: The total number of circulation events.
        """
        circulation_changes = 0
        # Slice the time since `since` into two-hour increments.
        # Experimentation shows that the Enki API can grab about 60
        # hours of activity at once before timing out, so this puts us
        # well below that threshold.
        now = datetime.datetime.utcnow()
        for start, end, full_slice in self.slice_timespan(
            since, now, datetime.timedelta(hours=2)
        ):
            circulation_changes += self._update_circulation(start, end)
        return circulation_changes

    def _update_circulation(self, start, end):
        """Process circulation events that happened between
        `start` and `end`.

        :return: The number of circulation events between `start`
        and `end`.
        """
        circulation_changes = 0
        for circulation in self.api.recent_activity(start, end):
            circulation_changes += 1
            license_pool, is_new = circulation.license_pool(
                self._db, self.collection
            )
            if not license_pool.work:
                # Either this is the first time we've heard about this
                # title, or we never made a Work for this
                # LicensePool. Look up its bibliographic data -- that
                # should let us make a Work.
                metadata = self.api.get_item(license_pool.identifier.identifier)
                if metadata:
                    self.process_book(metadata)
            else:
                license_pool, made_changes = circulation.apply(
                    self._db, self.collection
                )

        return circulation_changes

    def process_book(self, bibliographic):

        """Make the local database reflect the state of the remote Enki
        collection for the given book.

        :param bibliographic: A Metadata object with attached CirculationData

        :return: A 2-tuple (LicensePool, Edition). If possible, a
            presentation-ready Work will be created for the LicensePool.
        """
        availability = bibliographic.circulation
        edition, new_edition = bibliographic.edition(self._db)
        now = datetime.datetime.utcnow()
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
        )
        bibliographic.apply(edition, self.collection, replace=policy)
        license_pool, ignore = availability.license_pool(
            self._db, self.collection
        )

        if new_edition:
            for library in self.collection.libraries:
                self.analytics.collect_event(library, license_pool, CirculationEvent.DISTRIBUTOR_TITLE_ADD, now)

        return edition, license_pool


class EnkiCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left the Enki collection."""

    SERVICE_NAME = "Enki Collection Reaper"
    INTERVAL_SECONDS = 3600*4
    PROTOCOL = "Enki"

    def __init__(self, _db, collection, api_class=EnkiAPI):
        self._db = _db
        super(EnkiCollectionReaper, self).__init__(self._db, collection)
        if callable(api_class):
            api = api_class(self._db, collection)
        else:
            api = api_class
        self.api = api

    def process_item(self, identifier):
        self.log.debug(
            "Seeing if %s needs reaping", identifier.identifier
        )
        metadata = self.api.get_item(identifier.identifier)
        if metadata:
            # This title is still in the collection. Do nothing.
            return

        # Get this collection's license pool for this identifier.
        # We'll reap it by setting its licenses_owned to 0.
        pool = identifier.licensed_through_collection(self.collection)

        if not pool or pool.licenses_owned == 0:
            # It's already been reaped.
            return

        if pool.presentation_edition:
            self.log.warn(
                "Removing %r from circulation",
                pool.presentation_edition
            )
        else:
            self.log.warn(
                "Removing unknown title %s from circulation.",
                identifier.identifier
            )

        now = datetime.datetime.utcnow()
        circulationdata = CirculationData(
            data_source=DataSource.ENKI,
            primary_identifier= IdentifierData(
                identifier.type, identifier.identifier
            ),
            licenses_owned = 0,
            licenses_available = 0,
            patrons_in_hold_queue = 0,
            last_checked = now
        )

        circulationdata.apply(
            self._db,
            self.collection,
            replace=ReplacementPolicy.from_license_source(self._db)
        )
        return circulationdata
