from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
    assert_raises_regexp,
)
import datetime
import os
import json

from api.opds_for_distributors import (
    OPDSForDistributorsAPI,
    OPDSForDistributorsImporter,
    OPDSForDistributorsReaperMonitor,
    MockOPDSForDistributorsAPI,
)
from api.circulation_exceptions import *
from core.testing import DatabaseTest
from core.metadata_layer import (
    CirculationData,
    LinkData,
    TimestampData,
)
from core.model import (
    Collection,
    Credential,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Loan,
    MediaTypes,
    Representation,
    RightsStatus,
)
from core.util.opds_writer import OPDSFeed


class BaseOPDSForDistributorsTest(object):
    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "opds_for_distributors")

    @classmethod
    def get_data(cls, filename):
        path = os.path.join(cls.resource_path, filename)
        return open(path).read()

class TestOPDSForDistributorsAPI(DatabaseTest):

    def setup_method(self):
        super(TestOPDSForDistributorsAPI, self).setup_method()
        self.collection = MockOPDSForDistributorsAPI.mock_collection(self._db)
        self.api = MockOPDSForDistributorsAPI(self._db, self.collection)

    def test_external_integration(self):
        eq_(self.collection.external_integration,
            self.api.external_integration(self._db))

    def test__run_self_tests(self):
        """The self-test for OPDSForDistributorsAPI just tries to negotiate
        a fulfillment token.
        """
        class Mock(OPDSForDistributorsAPI):
            def __init__(self):
                pass

            def _get_token(self, _db):
                self.called_with = _db
                return "a token"

        api = Mock()
        [result] = api._run_self_tests(self._db)
        eq_(self._db, api.called_with)
        eq_("Negotiate a fulfillment token", result.name)
        eq_(True, result.success)
        eq_("a token", result.result)

    def test_supported_media_types(self):
        # If the default client supports media type X with the
        # BEARER_TOKEN access control scheme, then X is a supported
        # media type for an OPDS For Distributors collection.
        supported = self.api.SUPPORTED_MEDIA_TYPES
        for (format, drm) in DeliveryMechanism.default_client_can_fulfill_lookup:
            if drm == (DeliveryMechanism.BEARER_TOKEN) and format is not None:
                assert format in supported

        # Here's a media type that sometimes shows up in OPDS For
        # Distributors collections but is _not_ supported. Incoming
        # items with this media type will _not_ be imported.
        assert MediaTypes.JPEG_MEDIA_TYPE not in supported

    def test_can_fulfill_without_loan(self):
        """A book made available through OPDS For Distributors can be
        fulfilled with no underlying loan, if its delivery mechanism
        uses bearer token fulfillment.
        """
        patron = object()
        pool = self._licensepool(edition=None, collection=self.collection)
        [lpdm] = pool.delivery_mechanisms

        m = self.api.can_fulfill_without_loan

        # No LicensePoolDeliveryMechanism -> False
        eq_(False, m(patron, pool, None))

        # No LicensePool -> False (there can be multiple LicensePools for
        # a single LicensePoolDeliveryMechanism).
        eq_(False, m(patron, None, lpdm))

        # No DeliveryMechanism -> False
        old_dm = lpdm.delivery_mechanism
        lpdm.delivery_mechanism = None
        eq_(False, m(patron, pool, lpdm))

        # DRM mechanism requires identifying a specific patron -> False
        lpdm.delivery_mechanism = old_dm
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.ADOBE_DRM
        eq_(False, m(patron, pool, lpdm))

        # Otherwise -> True
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.NO_DRM
        eq_(True, m(patron, pool, lpdm))

        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.BEARER_TOKEN
        eq_(True, m(patron, pool, lpdm))

    def test_get_token_success(self):
        # The API hasn't been used yet, so it will need to find the auth
        # document and authenticate url.
        feed = '<feed><link rel="http://opds-spec.org/auth/document" href="http://authdoc"/></feed>'
        self.api.queue_response(200, content=feed)
        auth_doc = json.dumps({
            "authentication": [{
                "type": "http://opds-spec.org/auth/oauth/client_credentials",
                "links": [{
                    "rel": "authenticate",
                    "href": "http://authenticate",
                }]
            }]
        })
        self.api.queue_response(200, content=auth_doc)
        token = self._str
        token_response = json.dumps({"access_token": token, "expires_in": 60})
        self.api.queue_response(200, content=token_response)

        eq_(token, self.api._get_token(self._db).credential)

        # Now that the API has the authenticate url, it only needs
        # to get the token.
        self.api.queue_response(200, content=token_response)
        eq_(token, self.api._get_token(self._db).credential)

        # A credential was created.
        [credential] = self._db.query(Credential).all()
        eq_(token, credential.credential)

        # If we call _get_token again, it uses the existing credential.
        eq_(token, self.api._get_token(self._db).credential)

        self._db.delete(credential)

        # Create a new API that doesn't have an auth url yet.
        self.api = MockOPDSForDistributorsAPI(self._db, self.collection)

        # This feed requires authentication and returns the auth document.
        auth_doc = json.dumps({
            "authentication": [{
                "type": "http://opds-spec.org/auth/oauth/client_credentials",
                "links": [{
                    "rel": "authenticate",
                    "href": "http://authenticate",
                }]
            }]
        })
        self.api.queue_response(401, content=auth_doc)
        token = self._str
        token_response = json.dumps({"access_token": token, "expires_in": 60})
        self.api.queue_response(200, content=token_response)

        eq_(token, self.api._get_token(self._db).credential)

    def test_get_token_errors(self):
        no_auth_document = '<feed></feed>'
        self.api.queue_response(200, content=no_auth_document)
        assert_raises_regexp(
            LibraryAuthorizationFailedException,
            "No authentication document link found in http://opds",
            self.api._get_token, self._db
        )

        feed = '<feed><link rel="http://opds-spec.org/auth/document" href="http://authdoc"/></feed>'
        self.api.queue_response(200, content=feed)
        auth_doc_without_client_credentials = json.dumps({
            "authentication": []
        })
        self.api.queue_response(200, content=auth_doc_without_client_credentials)
        assert_raises_regexp(
            LibraryAuthorizationFailedException,
            "Could not find any credential-based authentication mechanisms in http://authdoc",
            self.api._get_token, self._db
        )

        self.api.queue_response(200, content=feed)
        auth_doc_without_links = json.dumps({
            "authentication": [{
                "type": "http://opds-spec.org/auth/oauth/client_credentials",
            }]
        })
        self.api.queue_response(200, content=auth_doc_without_links)
        assert_raises_regexp(
            LibraryAuthorizationFailedException,
            "Could not find any authentication links in http://authdoc",
            self.api._get_token, self._db
        )

        self.api.queue_response(200, content=feed)
        auth_doc = json.dumps({
            "authentication": [{
                "type": "http://opds-spec.org/auth/oauth/client_credentials",
                "links": [{
                    "rel": "authenticate",
                    "href": "http://authenticate",
                }]
            }]
        })
        self.api.queue_response(200, content=auth_doc)
        token_response = json.dumps({"error": "unexpected error"})
        self.api.queue_response(200, content=token_response)
        assert_raises_regexp(
            LibraryAuthorizationFailedException,
            "Document retrieved from http://authenticate is not a bearer token: {.*unexpected error.*}",
            self.api._get_token, self._db
        )

    def test_checkin(self):
        # The patron has two loans, one from this API's collection and
        # one from a different collection.
        patron = self._patron()

        data_source = DataSource.lookup(self._db, "Biblioboard", autocreate=True)
        edition, pool = self._edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=self.collection,
        )
        pool.loan_to(patron)

        other_collection = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        other_edition, other_pool = self._edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=other_collection,
        )
        other_pool.loan_to(patron)

        eq_(2, self._db.query(Loan).count())

        self.api.checkin(patron, "1234", pool)

        # The loan from this API's collection has been deleted.
        # The loan from the other collection wasn't touched.
        eq_(1, self._db.query(Loan).count())
        [loan] = self._db.query(Loan).all()
        eq_(other_pool, loan.license_pool)

    def test_checkout(self):
        patron = self._patron()

        data_source = DataSource.lookup(self._db, "Biblioboard", autocreate=True)
        edition, pool = self._edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=self.collection,
        )

        loan_info = self.api.checkout(patron, "1234", pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection.id, loan_info.collection_id)
        eq_(data_source.name, loan_info.data_source_name)
        eq_(Identifier.URI, loan_info.identifier_type)
        eq_(pool.identifier.identifier, loan_info.identifier)

        # The loan's start date has been set to the current time.
        now = datetime.datetime.utcnow()
        assert (now - loan_info.start_date).seconds < 2

        # The loan is of indefinite duration.
        eq_(None, loan_info.end_date)

    def test_fulfill(self):
        patron = self._patron()

        data_source = DataSource.lookup(self._db, "Biblioboard", autocreate=True)
        edition, pool = self._edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=self.collection,
        )
        # This pool doesn't have an acquisition link, so
        # we can't fulfill it yet.
        assert_raises(CannotFulfill, self.api.fulfill,
                      patron, "1234", pool, Representation.EPUB_MEDIA_TYPE)

        # Set up an epub acquisition link for the pool.
        url = self._url
        link, ignore = pool.identifier.add_link(
            Hyperlink.GENERIC_OPDS_ACQUISITION,
            url, data_source,
            Representation.EPUB_MEDIA_TYPE,
        )
        pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.IN_COPYRIGHT,
            link.resource,
        )

        # Set the API's auth url so it doesn't have to get it -
        # that's tested in test_get_token.
        self.api.auth_url = "http://auth"

        token_response = json.dumps({"access_token": "token", "expires_in": 60})
        self.api.queue_response(200, content=token_response)

        fulfillment_time = datetime.datetime.utcnow()
        fulfillment_info = self.api.fulfill(patron, "1234", pool, Representation.EPUB_MEDIA_TYPE)
        eq_(self.collection.id, fulfillment_info.collection_id)
        eq_(data_source.name, fulfillment_info.data_source_name)
        eq_(Identifier.URI, fulfillment_info.identifier_type)
        eq_(pool.identifier.identifier, fulfillment_info.identifier)
        eq_(None, fulfillment_info.content_link)

        eq_(DeliveryMechanism.BEARER_TOKEN, fulfillment_info.content_type)
        bearer_token_document = json.loads(fulfillment_info.content)
        expires_in = bearer_token_document['expires_in']
        assert expires_in < 60
        eq_("Bearer", bearer_token_document['token_type'])
        eq_("token", bearer_token_document['access_token'])
        eq_(url, bearer_token_document['location'])

        # The FulfillmentInfo's content_expires is approximately the
        # time you get if you add the number of seconds until the
        # bearer token expires to the time at which the title was
        # originally fulfilled.
        expect_expiration = fulfillment_time + datetime.timedelta(
            seconds=expires_in
        )
        assert abs(
            (fulfillment_info.content_expires-expect_expiration).total_seconds()
        ) < 5


    def test_patron_activity(self):
        # The patron has two loans from this API's collection and
        # one from a different collection.
        patron = self._patron()

        data_source = DataSource.lookup(self._db, "Biblioboard", autocreate=True)
        e1, p1 = self._edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=self.collection,
        )
        p1.loan_to(patron)

        e2, p2 = self._edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=self.collection,
        )
        p2.loan_to(patron)

        other_collection = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        e3, p3 = self._edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=other_collection,
        )
        p3.loan_to(patron)

        activity = self.api.patron_activity(patron, "1234")
        eq_(2, len(activity))
        [l1, l2] = activity
        eq_(l1.collection_id, self.collection.id)
        eq_(l2.collection_id, self.collection.id)
        eq_(set([l1.identifier, l2.identifier]),
            set([p1.identifier.identifier, p2.identifier.identifier]))

class TestOPDSForDistributorsImporter(DatabaseTest, BaseOPDSForDistributorsTest):

    def test_import(self):
        feed = self.get_data("biblioboard_mini_feed.opds")

        data_source = DataSource.lookup(self._db, "Biblioboard", autocreate=True)
        collection = MockOPDSForDistributorsAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )

        class MockMetadataClient(object):
            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name
        metadata_client = MockMetadataClient()
        importer = OPDSForDistributorsImporter(
            self._db, collection=collection,
            metadata_client=metadata_client,
        )

        imported_editions, imported_pools, imported_works, failures = (
            importer.import_from_feed(feed)
        )

        # This importer works the same as the base OPDSImporter, except that
        # it adds delivery mechanisms for books with epub acquisition links
        # and sets pools' licenses_owned and licenses_available.

        # Both works were created, since we can use their acquisition links
        # to give copies to patrons.
        [camelot, southern] = sorted(imported_works, key=lambda x: x.title)

        # Each work has a license pool.
        [camelot_pool] = camelot.license_pools
        [southern_pool] = southern.license_pools
        now = datetime.datetime.utcnow()

        for pool in [camelot_pool, southern_pool]:
            eq_(False, pool.open_access)
            eq_(RightsStatus.IN_COPYRIGHT, pool.delivery_mechanisms[0].rights_status.uri)
            eq_(Representation.EPUB_MEDIA_TYPE, pool.delivery_mechanisms[0].delivery_mechanism.content_type)
            eq_(DeliveryMechanism.BEARER_TOKEN, pool.delivery_mechanisms[0].delivery_mechanism.drm_scheme)
            eq_(1, pool.licenses_owned)
            eq_(1, pool.licenses_available)
            assert (pool.work.last_update_time - now).total_seconds() <= 2

        [camelot_acquisition_link] = [l for l in camelot_pool.identifier.links
                                      if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
                                      and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE]
        camelot_acquisition_url = camelot_acquisition_link.resource.representation.url
        eq_("https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0952/assets/content.epub",
            camelot_acquisition_url)

        [southern_acquisition_link] = [l for l in southern_pool.identifier.links
                                      if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
                                      and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE]
        southern_acquisition_url = southern_acquisition_link.resource.representation.url
        eq_("https://library.biblioboard.com/ext/api/media/04da95cd-6cfc-4e82-810f-121d418b6963/assets/content.epub",
            southern_acquisition_url)

    def test__add_format_data(self):

        # Mock SUPPORTED_MEDIA_TYPES for purposes of test.
        api = OPDSForDistributorsAPI
        old_value = api.SUPPORTED_MEDIA_TYPES
        good_media_type = "media/type"
        api.SUPPORTED_MEDIA_TYPES = [good_media_type]

        # Create a CirculationData object with a number of links.
        # Only the third of these links will become a FormatData
        # object.
        circulation = CirculationData("data source", "identifier")
        good_rel = Hyperlink.GENERIC_OPDS_ACQUISITION
        for rel, media, href in (
            ("http://wrong/rel/", good_media_type, "http://url1/"),
            (good_rel, "wrong/media type", "http://url2/"),
            (good_rel, good_media_type, "http://url3/"),
        ):
            link = LinkData(rel=rel, href=href, media_type=media)
            circulation.links.append(link)

        eq_([], circulation.formats)
        OPDSForDistributorsImporter._add_format_data(circulation)

        # Only one FormatData was created.
        [format] = circulation.formats

        # It's the third link we created -- the one where both rel and
        # media_type were good.
        eq_("http://url3/", format.link.href)
        eq_(good_rel, format.link.rel)

        # The FormatData has the content type provided by the LinkData,
        # and the implicit Bearer Token access control scheme defined
        # by OPDS For Distrubutors.
        eq_(good_media_type, format.content_type)
        eq_(DeliveryMechanism.BEARER_TOKEN, format.drm_scheme)

        # Undo the mock of SUPPORTED_MEDIA_TYPES.
        api.SUPPORTED_MEDIA_TYPES = old_value


class TestOPDSForDistributorsReaperMonitor(DatabaseTest, BaseOPDSForDistributorsTest):

    def test_reaper(self):
        feed = self.get_data("biblioboard_mini_feed.opds")

        class MockOPDSForDistributorsReaperMonitor(OPDSForDistributorsReaperMonitor):
            """An OPDSForDistributorsReaperMonitor that overrides _get."""
            def _get(self, url, headers):
                return (
                    200, {'content-type': OPDSFeed.ACQUISITION_FEED_TYPE}, feed
                )

        data_source = DataSource.lookup(self._db, "Biblioboard", autocreate=True)
        collection = MockOPDSForDistributorsAPI.mock_collection(self._db)
        collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING,
            data_source.name
        )
        monitor = MockOPDSForDistributorsReaperMonitor(
            self._db, collection, OPDSForDistributorsImporter,
            metadata_client=object()
        )

        # There's a license pool in the database that isn't in the feed anymore.
        edition, now_gone = self._edition(
            identifier_type=Identifier.URI,
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=collection,
        )
        now_gone.licenses_owned = 1
        now_gone.licenses_available = 1

        edition, still_there = self._edition(
            identifier_type=Identifier.URI,
            identifier_id="urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952",
            data_source_name=data_source.name,
            with_license_pool=True,
            collection=collection,
        )
        still_there.licenses_owned = 1
        still_there.licenses_available = 1

        progress = monitor.run_once(monitor.timestamp().to_data())

        # One LicensePool has been cleared out.
        eq_(0, now_gone.licenses_owned)
        eq_(0, now_gone.licenses_available)

        # The other is still around.
        eq_(1, still_there.licenses_owned)
        eq_(1, still_there.licenses_available)

        # The TimestampData returned by run_once() describes its
        # achievements.
        eq_("License pools removed: 1.", progress.achievements)

        # The TimestampData does not include any timing information --
        # that will be applied by run().
        eq_(None, progress.start)
        eq_(None, progress.finish)
