import datetime
import feedparser
import json
from flask_babel import lazy_gettext as _

from core.opds_import import (
    OPDSImporter,
    OPDSImportMonitor,
)
from core.model import (
    Collection,
    Credential,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Loan,
    Representation,
    RightsStatus,
    Session,
    get_one,
    get_one_or_create,
)
from core.metadata_layer import (
    FormatData,
    TimestampData,
)
from core.selftest import HasSelfTests
from .circulation import (
    BaseCirculationAPI,
    LoanInfo,
    FulfillmentInfo,
)
from core.util.http import HTTP
from core.util.string_helpers import base64
from core.testing import (
    DatabaseTest,
    MockRequestsResponse,
)
from .config import IntegrationException
from .circulation_exceptions import *

class OPDSForDistributorsAPI(BaseCirculationAPI, HasSelfTests):
    NAME = "OPDS for Distributors"
    DESCRIPTION = _("Import books from a distributor that requires authentication to get the OPDS feed and download books.")

    SETTINGS = OPDSImporter.BASE_SETTINGS + [
        {
            "key": ExternalIntegration.USERNAME,
            "label": _("Library's username or access key"),
            "required": True,
        },
        {
            "key": ExternalIntegration.PASSWORD,
            "label": _("Library's password or secret key"),
            "required": True,
        }
    ]

    # In OPDS For Distributors, all items are gated through the
    # BEARER_TOKEN access control scheme.
    #
    # If the default client supports a given media type when
    # combined with the BEARER_TOKEN scheme, then we should import
    # titles with that media type...
    SUPPORTED_MEDIA_TYPES = [
        format for (format, drm) in
        DeliveryMechanism.default_client_can_fulfill_lookup
        if drm == (DeliveryMechanism.BEARER_TOKEN) and format is not None
    ]

    # ...and we should map requests for delivery of that media type to
    # the (type, BEARER_TOKEN) DeliveryMechanism.
    delivery_mechanism_to_internal_format = {
        (type, DeliveryMechanism.BEARER_TOKEN): type
        for type in SUPPORTED_MEDIA_TYPES
    }

    def __init__(self, _db, collection):
        self.collection_id = collection.id
        self.external_integration_id = collection.external_integration.id
        self.data_source_name = collection.external_integration.setting(Collection.DATA_SOURCE_NAME_SETTING).value
        self.username = collection.external_integration.username
        self.password = collection.external_integration.password
        self.feed_url = collection.external_account_id
        self.auth_url = None

    def external_integration(self, _db):
        return get_one(_db, ExternalIntegration,
                       id=self.external_integration_id)

    def _run_self_tests(self, _db):
        """Try to get a token."""
        yield self.run_test(
            "Negotiate a fulfillment token", self._get_token, _db
        )

    def _request_with_timeout(self, method, url, *args, **kwargs):
        """Wrapper around HTTP.request_with_timeout to be overridden for tests."""
        return HTTP.request_with_timeout(method, url, *args, **kwargs)

    def _get_token(self, _db):
        # If this is the first time we're getting a token, we
        # need to find the authenticate url in the OPDS
        # authentication document.
        if not self.auth_url:
            # Keep track of the most recent URL we retrieved for error
            # reporting purposes.
            current_url = self.feed_url
            response = self._request_with_timeout('GET', current_url)

            if response.status_code != 401:
                # This feed doesn't require authentication, so
                # we need to find a link to the authentication document.
                feed = feedparser.parse(response.content)
                links = feed.get('feed', {}).get('links', [])
                auth_doc_links = [l for l in links if l['rel'] == "http://opds-spec.org/auth/document"]
                if not auth_doc_links:
                    raise LibraryAuthorizationFailedException("No authentication document link found in %s" % current_url)
                current_url = auth_doc_links[0].get("href")

                response = self._request_with_timeout('GET', current_url)

            try:
                auth_doc = json.loads(response.content)
            except Exception as e:
                raise LibraryAuthorizationFailedException("Could not load authentication document from %s" % current_url)
            auth_types = auth_doc.get('authentication', [])
            credentials_types = [t for t in auth_types if t['type'] == "http://opds-spec.org/auth/oauth/client_credentials"]
            if not credentials_types:
                raise LibraryAuthorizationFailedException("Could not find any credential-based authentication mechanisms in %s" % current_url)

            links = credentials_types[0].get('links', [])
            auth_links = [l for l in links if l.get("rel") == "authenticate"]
            if not auth_links:
                raise LibraryAuthorizationFailedException("Could not find any authentication links in %s" % current_url)
            self.auth_url = auth_links[0].get("href")

        def refresh(credential):
            headers = dict()
            auth_header = "Basic %s" % base64.b64encode("%s:%s" % (self.username, self.password))
            headers['Authorization'] = auth_header
            headers['Content-Type'] = "application/x-www-form-urlencoded"
            body = dict(grant_type='client_credentials')
            token_response = self._request_with_timeout('POST', self.auth_url, data=body, headers=headers)
            token = json.loads(token_response.content)
            access_token = token.get("access_token")
            expires_in = token.get("expires_in")
            if not access_token or not expires_in:
                raise LibraryAuthorizationFailedException(
                    "Document retrieved from %s is not a bearer token: %s" % (
                        self.auth_url, token_response.content
                    )
                )
            credential.credential = access_token
            expires_in = expires_in
            # We'll avoid edge cases by assuming the token expires 75%
            # into its useful lifetime.
            credential.expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in*0.75)
        return Credential.lookup(_db, self.data_source_name,
                                 "OPDS For Distributors Bearer Token",
                                 patron=None,
                                 refresher_method=refresh,
                                 )

    def can_fulfill_without_loan(self, patron, licensepool, lpdm):
        """Since OPDS For Distributors delivers books to the library rather
        than creating loans, any book can be fulfilled without
        identifying the patron, assuming the library's policies
        allow it.

        Just to be safe, though, we require that the
        DeliveryMechanism's drm_scheme be either 'no DRM' or 'bearer
        token', since other DRM schemes require identifying a patron.
        """
        if not lpdm or not lpdm.delivery_mechanism:
            return False
        drm_scheme = lpdm.delivery_mechanism.drm_scheme
        if drm_scheme in (
            DeliveryMechanism.NO_DRM, DeliveryMechanism.BEARER_TOKEN
        ):
            return True
        return False

    def checkin(self, patron, pin, licensepool):
        # Delete the patron's loan for this licensepool.
        _db = Session.object_session(patron)
        try:
            loan = get_one(
                _db, Loan,
                patron_id=patron.id,
                license_pool_id=licensepool.id,
            )
            _db.delete(loan)
        except Exception as e:
            # The patron didn't have this book checked out.
            pass

    def checkout(self, patron, pin, licensepool, internal_format):
        now = datetime.datetime.utcnow()
        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=now,
            end_date=None,
        )

    def fulfill(self, patron, pin, licensepool, internal_format, **kwargs):
        """Retrieve a bearer token that can be used to download the book.

        :param kwargs: A container for arguments to fulfill()
           which are not relevant to this vendor.

        :return: a FulfillmentInfo object.
        """

        links = licensepool.identifier.links
        # Find the acquisition link with the right media type.
        for link in links:
            media_type = link.resource.representation.media_type
            if link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION and media_type == internal_format:
                url = link.resource.representation.url

                # Obtain a Credential with the information from our
                # bearer token.
                _db = Session.object_session(licensepool)
                credential = self._get_token(_db)

                # Build a application/vnd.librarysimplified.bearer-token
                # document using information from the credential.
                now = datetime.datetime.utcnow()
                expiration = int((credential.expires - now).total_seconds())
                token_document = dict(
                    token_type="Bearer",
                    access_token=credential.credential,
                    expires_in=expiration,
                    location=url,
                )

                return FulfillmentInfo(
                    licensepool.collection,
                    licensepool.data_source.name,
                    licensepool.identifier.type,
                    licensepool.identifier.identifier,
                    content_link=None,
                    content_type=DeliveryMechanism.BEARER_TOKEN,
                    content=json.dumps(token_document),
                    content_expires=credential.expires,
                )

        # We couldn't find an acquisition link for this book.
        raise CannotFulfill()

    def patron_activity(self, patron, pin):
        # Look up loans for this collection in the database.
        _db = Session.object_session(patron)
        loans = _db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.collection_id==self.collection_id
        ).filter(
            Loan.patron==patron
        )
        return [
            LoanInfo(
                loan.license_pool.collection,
                loan.license_pool.data_source.name,
                loan.license_pool.identifier.type,
                loan.license_pool.identifier.identifier,
                loan.start,
                loan.end
            ) for loan in loans]

class OPDSForDistributorsImporter(OPDSImporter):
    NAME = OPDSForDistributorsAPI.NAME

    def update_work_for_edition(self, *args, **kwargs):
        """After importing a LicensePool, set its availability
        appropriately. Books imported through OPDS For Distributors are
        not open-access, but a library that can perform this import has
        a license for the title and can distribute unlimited copies.
        """
        pool, work = super(
            OPDSForDistributorsImporter, self).update_work_for_edition(
                *args, **kwargs
        )
        pool.update_availability(
            new_licenses_owned=1, new_licenses_available=1,
            new_licenses_reserved=0, new_patrons_in_hold_queue=0
        )
        return pool, work

    @classmethod
    def _add_format_data(cls, circulation):
        for link in circulation.links:
            if (link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
                and link.media_type in
                OPDSForDistributorsAPI.SUPPORTED_MEDIA_TYPES):
                circulation.formats.append(
                    FormatData(
                        content_type=link.media_type,
                        drm_scheme=DeliveryMechanism.BEARER_TOKEN,
                        link=link,
                        rights_uri=RightsStatus.IN_COPYRIGHT,
                    )
                )


class OPDSForDistributorsImportMonitor(OPDSImportMonitor):
    """Monitor an OPDS feed that requires or allows authentication,
    such as Biblioboard or Plympton.
    """
    PROTOCOL = OPDSForDistributorsImporter.NAME

    def __init__(self, _db, collection, import_class, **kwargs):
        super(OPDSForDistributorsImportMonitor, self).__init__(_db, collection, import_class, **kwargs)

        self.api = OPDSForDistributorsAPI(_db, collection)

    def _get(self, url, headers):
        """Make a normal HTTP request for an OPDS feed, but add in an
        auth header with the credentials for the collection.
        """

        token = self.api._get_token(self._db).credential
        headers = dict(headers or {})
        auth_header = "Bearer %s" % token
        headers['Authorization'] = auth_header

        return super(OPDSForDistributorsImportMonitor, self)._get(url, headers)

class OPDSForDistributorsReaperMonitor(OPDSForDistributorsImportMonitor):
    """This is an unusual import monitor that crawls the entire OPDS feed
    and keeps track of every identifier it sees, to find out if anything
    has been removed from the collection.
    """

    def __init__(self, _db, collection, import_class, **kwargs):
        super(OPDSForDistributorsReaperMonitor, self).__init__(_db, collection, import_class, **kwargs)
        self.seen_identifiers = set()

    def feed_contains_new_data(self, feed):
        # Always return True so that the importer will crawl the
        # entire feed.
        return True

    def import_one_feed(self, feed):
        # Collect all the identifiers in the feed.
        parsed_feed = feedparser.parse(feed)
        identifiers = [entry.get("id") for entry in parsed_feed.get("entries", [])]
        self.seen_identifiers.update(identifiers)
        return [], {}

    def run_once(self, progress):
        """Check to see if any identifiers we know about are no longer
        present on the remote. If there are any, remove them.

        :param progress: A TimestampData, ignored.
        """
        super(OPDSForDistributorsReaperMonitor, self).run_once(progress)

        # self.seen_identifiers is full of URNs. We need the values
        # that go in Identifier.identifier.
        identifiers, failures = Identifier.parse_urns(
            self._db, self.seen_identifiers
        )
        identifier_ids = [x.id for x in list(identifiers.values())]

        # At this point we've gone through the feed and collected all the identifiers.
        # If there's anything we didn't see, we know it's no longer available.
        qu = self._db.query(
            LicensePool
        ).join(
            Identifier
        ).filter(
            LicensePool.collection_id==self.collection.id
        ).filter(
            ~Identifier.id.in_(identifier_ids)
        ).filter(
            LicensePool.licenses_available > 0
        )
        pools_reaped = qu.count()
        self.log.info(
            "Reaping %s license pools for collection %s." % (pools_reaped, self.collection.name)
        )

        for pool in qu:
            pool.licenses_available = 0
            pool.licenses_owned = 0
        self._db.commit()
        achievements = "License pools removed: %d." % pools_reaped
        return TimestampData(achievements=achievements)

class MockOPDSForDistributorsAPI(OPDSForDistributorsAPI):

    @classmethod
    def mock_collection(self, _db):
        """Create a mock OPDS For Distributors collection to use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test OPDS For Distributors Collection", create_method_kwargs=dict(
                external_account_id="http://opds",
            )
        )
        integration = collection.create_external_integration(
            protocol=OPDSForDistributorsAPI.NAME
        )
        integration.username = 'a'
        integration.password = 'b'
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        super(MockOPDSForDistributorsAPI, self).__init__(
            _db, collection, *args, **kwargs
        )

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )

    def _request_with_timeout(self, method, url, *args, **kwargs):
        self.requests.append([method, url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )
