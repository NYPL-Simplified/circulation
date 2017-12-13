from nose.tools import set_trace

import base64
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
from core.metadata_layer import FormatData
from circulation import (
    BaseCirculationAPI,
    LoanInfo,
    FulfillmentInfo,
)
from core.util.http import HTTP
from core.testing import (
    DatabaseTest,
    MockRequestsResponse,
)
from circulation_exceptions import *

class OPDSForDistributorsAPI(BaseCirculationAPI):
    NAME = "OPDS for Distributors"
    DESCRIPTION = _("Import books from a distributor that requires authentication to get the OPDS feed and download books.")

    SETTINGS = OPDSImporter.SETTINGS + [
        {
            "key": ExternalIntegration.USERNAME,
            "label": _("Library's username or access key"),
        },
        {
            "key": ExternalIntegration.PASSWORD,
            "label": _("Library's password or secret key"),
        }
    ]

    SUPPORTED_MEDIA_TYPES = [Representation.EPUB_MEDIA_TYPE]

    delivery_mechanism_to_internal_format = {
        (type, DeliveryMechanism.BEARER_TOKEN): type for type in SUPPORTED_MEDIA_TYPES
    }

    def __init__(self, _db, collection):
        self.collection_id = collection.id
        self.data_source_name = collection.external_integration.setting(Collection.DATA_SOURCE_NAME_SETTING).value
        self.username = collection.external_integration.username
        self.password = collection.external_integration.password
        self.feed_url = collection.external_account_id
        self.auth_url = None

    def _request_with_timeout(self, method, url, *args, **kwargs):
        """Wrapper around HTTP.request_with_timeout to be overridden for tests."""
        return HTTP.request_with_timeout(method, url, *args, **kwargs)

    def _get_token(self, _db):
        # If this is the first time we're getting a token, we
        # need to find the authenticate url in the OPDS
        # authentication document.
        if not self.auth_url:
            response = self._request_with_timeout('GET', self.feed_url)

            if response.status_code != 401:
                # This feed doesn't require authentication, so
                # we need to find a link to the authentication document.
                feed = feedparser.parse(response.content)
                links = feed.get('feed', {}).get('links', [])
                auth_doc_links = [l for l in links if l['rel'] == "http://opds-spec.org/auth/document"]
                if not auth_doc_links:
                    raise LibraryAuthorizationFailedException()
                auth_doc_link = auth_doc_links[0].get("href")

                response = self._request_with_timeout('GET', auth_doc_link)

            try:
                auth_doc = json.loads(response.content)
            except Exception, e:
                raise LibraryAuthorizationFailedException()
            auth_types = auth_doc.get('authentication', [])
            credentials_types = [t for t in auth_types if t['type'] == "http://opds-spec.org/auth/oauth/client_credentials"]
            if not credentials_types:
                raise LibraryAuthorizationFailedException()

            links = credentials_types[0].get('links', [])
            auth_links = [l for l in links if l.get("rel") == "authenticate"]
            if not auth_links:
                raise LibraryAuthorizationFailedException()
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
                raise LibraryAuthorizationFailedException()
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
        except Exception, e:
            # The patron didn't have this book checked out.
            pass

    def checkout(self, patron, pin, licensepool, internal_format):
        return LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            start_date=datetime.datetime.now(),
            end_date=None,
        )

    def fulfill(self, patron, pin, licensepool, internal_format):
        # Download the book from the appropriate acquisition link and return its content.
        # TODO: Implement https://github.com/NYPL-Simplified/Simplified/wiki/BearerTokenPropagation#advertising-bearer-token-propagation
        # instead.

        links = licensepool.identifier.links
        # Find the acquisition link with the right media type.
        for link in links:
            media_type = link.resource.representation.media_type
            if link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION and media_type == internal_format:
                url = link.resource.representation.url

                # Obtain a Credential with the information from our
                # bearer token.
                _db = Session.object_session(patron)
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

    def import_from_feed(self, feed, **kwargs):
        editions, pools, works, failures = super(OPDSForDistributorsImporter, self).import_from_feed(feed, **kwargs)

        # If we were able to import a pool, it means we have a license for it,
        # and can distribute unlimited copies.
        for pool in pools:
            pool.licenses_owned = 1
            pool.licenses_available = 1

        return editions, pools, works, failures

    @classmethod
    def _add_format_data(cls, circulation):
        for link in circulation.links:
            if link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION and link.media_type in OPDSForDistributorsAPI.SUPPORTED_MEDIA_TYPES:
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

    def run_once(self, start_ignore, cutoff_ignore):
        super(OPDSForDistributorsReaperMonitor, self).run_once(start_ignore, cutoff_ignore)

        # At this point we've gone through the feed and collected all the identifiers.
        # If there's anything we didn't see, we know it's no longer available.
        qu = self._db.query(
            LicensePool
        ).join(
            Identifier
        ).filter(
            LicensePool.collection_id==self.collection.id
        ).filter(
            ~Identifier.identifier.in_(self.seen_identifiers)
        ).filter(
            LicensePool.licenses_available > 0
        )

        self.log.info(
            "Reaping %s license pools for collection %s." % (qu.count(), self.collection.name)
        )

        for pool in qu:
            pool.licenses_available = 0
            pool.licenses_owned = 0
        self._db.commit()

class MockOPDSForDistributorsAPI(OPDSForDistributorsAPI):

    @classmethod
    def mock_collection(self, _db):
        """Create a mock OPDS For Distributors collection to use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db, Collection,
            name="Test OPDS For Distributors Collection", create_method_kwargs=dict(
                external_account_id=u"http://opds",
            )
        )
        integration = collection.create_external_integration(
            protocol=OPDSForDistributorsAPI.NAME
        )
        integration.username = u'a'
        integration.password = u'b'
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

