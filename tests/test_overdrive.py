# encoding: utf-8
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
import os
import json
import pkgutil

from overdrive import (
    OverdriveAPI,
    MockOverdriveAPI,
    OverdriveAdvantageAccount,
    OverdriveRepresentationExtractor,
    OverdriveBibliographicCoverageProvider,
)

from coverage import (
    CoverageFailure,
)

from config import CannotLoadConfiguration

from model import (
    Collection,
    Contributor,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Identifier,
    Representation,
    Subject,
    Measurement,
    Hyperlink,
)
from scripts import RunCollectionCoverageProviderScript

from testing import MockRequestsResponse

from util.http import (
    BadResponseException,
    HTTP,
)

from . import DatabaseTest


class OverdriveTest(DatabaseTest):

    def setup(self):
        super(OverdriveTest, self).setup()
        self.collection = MockOverdriveAPI.mock_collection(self._db)
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "overdrive")

    def sample_json(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

class OverdriveTestWithAPI(OverdriveTest):
    """Automatically create a MockOverdriveAPI class during setup.

    We don't always do this because
    TestOverdriveBibliographicCoverageProvider needs to create a
    MockOverdriveAPI during the test, and at the moment the second
    MockOverdriveAPI request created in a test behaves differently
    from the first one.
    """
    def setup(self):
        super(OverdriveTestWithAPI, self).setup()
        self.api = MockOverdriveAPI(self._db, self.collection)


class TestOverdriveAPI(OverdriveTestWithAPI):

    def test_ils_name(self):
        """The 'ils_name' setting (defined in
        MockOverdriveAPI.mock_collection) is available through
        OverdriveAPI.ils_name().
        """
        eq_("e", self.api.ils_name(self._default_library))

        # The value must be explicitly set for a given library, or
        # else the default will be used.
        l2 = self._library()
        eq_("default", self.api.ils_name(l2))

    def test_make_link_safe(self):
        eq_("http://foo.com?q=%2B%3A%7B%7D",
            OverdriveAPI.make_link_safe("http://foo.com?q=+:{}"))

    def test_token_post_success(self):
        self.api.queue_response(200, content="some content")
        response = self.api.token_post(self._url, "the payload")
        eq_(200, response.status_code)
        eq_(self.api.access_token_response.content, response.content)

    def test_get_success(self):
        self.api.queue_response(200, content="some content")
        status_code, headers, content = self.api.get(self._url, {})
        eq_(200, status_code)
        eq_("some content", content)

    def test_failure_to_get_library_is_fatal(self):
        # We already called get_library while initializing the
        # Overdrive API, and when that happened we cached its
        # Representation. Delete the Representation so we stop using
        # the cached version.
        for r in self._db.query(Representation):
            self._db.delete(r)
        self._db.commit()

        self.api.queue_response(500)
        assert_raises_regexp(
            BadResponseException,
            ".*Got status code 500.*",
            self.api.get_library
        )

    def test_error_getting_library(self):
        class MisconfiguredOverdriveAPI(MockOverdriveAPI):
            """This Overdrive client has valid credentials but the library
            can't be found -- probably because the library ID is wrong."""
            def get_library(self):
                return {u'errorCode': u'Some error', u'message': u'Some message.', u'token': u'abc-def-ghi'}

        assert_raises_regexp(
            CannotLoadConfiguration,
            "Overdrive credentials are valid but could not fetch library: Some message.",
            MisconfiguredOverdriveAPI,
            self._db,
            self.collection
        )

    def test_401_on_get_refreshes_bearer_token(self):

        eq_("bearer token", self.api.token)

        # We try to GET and receive a 401.
        self.api.queue_response(401)

        # We refresh the bearer token. (This happens in
        # MockOverdriveAPI.token_post, so we don't mock the response
        # in the normal way.)
        self.api.access_token_response = self.api.mock_access_token_response(
            "new bearer token"
        )

        # Then we retry the GET and it succeeds this time.
        self.api.queue_response(200, content="at last, the content")

        status_code, headers, content = self.api.get(self._url, {})

        eq_(200, status_code)
        eq_("at last, the content", content)

        # The bearer token has been updated.
        eq_("new bearer token", self.api.token)

    def test_credential_refresh_success(self):
        """Verify the process of refreshing the Overdrive bearer token.
        """
        credential = self.api.credential_object(lambda x: x)
        eq_("bearer token", credential.credential)
        eq_(self.api.token, credential.credential)

        self.api.access_token_response = self.api.mock_access_token_response(
            "new bearer token"
        )

        self.api.refresh_creds(credential)
        eq_("new bearer token", credential.credential)
        eq_(self.api.token, credential.credential)

    def test_401_after_token_refresh_raises_error(self):

        eq_("bearer token", self.api.token)

        # We try to GET and receive a 401.
        self.api.queue_response(401)

        # We refresh the bearer token.
        self.api.access_token_response = self.api.mock_access_token_response(
            "new bearer token"
        )

        # Then we retry the GET but we get another 401.
        self.api.queue_response(401)

        # That raises a BadResponseException
        assert_raises_regexp(
            BadResponseException, "Bad response from .*:Something's wrong with the Overdrive OAuth Bearer Token!",
        )

    def test_401_during_refresh_raises_error(self):
        """If we fail to refresh the OAuth bearer token, an exception is
        raised.
        """
        self.api.access_token_response = MockRequestsResponse(401, {}, "")

        assert_raises_regexp(
            BadResponseException,
            ".*Got status code 401.*can only continue on: 200.",
            self.api.refresh_creds,
            None
        )

    def test_library_endpoint(self):
        """Verify that Advantage collections and regular Overdrive
        collections start at different endpoints.
        """
        # Here's an Overdrive collection.
        main = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id="1",
        )
        main.external_integration.username = "user"
        main.external_integration.password = "password"
        main.external_integration.setting('website_id').value = '100'
        main.external_integration.setting('ils_name').value = 'default'

        # Here's an Overdrive API client for that collection.
        overdrive_main = MockOverdriveAPI(self._db, main)
        eq_("https://api.overdrive.com/v1/libraries/1",
            overdrive_main._library_endpoint)

        # Here's an Overdrive Advantage collection associated with the
        # main Overdrive collection.
        child = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id="2",
        )
        child.parent = main
        overdrive_child = MockOverdriveAPI(self._db, child)
        eq_(
            'https://api.overdrive.com/v1/libraries/1/advantageAccounts/2',
            overdrive_child._library_endpoint
        )

class TestOverdriveRepresentationExtractor(OverdriveTestWithAPI):

    def test_availability_info(self):
        data, raw = self.sample_json("overdrive_book_list.json")
        availability = OverdriveRepresentationExtractor.availability_link_list(
            raw)
        for item in availability:
            for key in 'availability_link', 'id', 'title':
                assert key in item

    def test_availability_info_missing_data(self):
        data, raw = self.sample_json("overdrive_book_list_missing_data.json")
        [item] = OverdriveRepresentationExtractor.availability_link_list(
            raw)

        # We got a data structure for the item that has an ID but no title.
        # We did not get a data structure for the item that has a title
        # but no ID.
        eq_('title is missing', item['id'])
        eq_(None, item['title'])


    def test_link(self):
        data, raw = self.sample_json("overdrive_book_list.json")
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        eq_(expect, OverdriveRepresentationExtractor.link(raw, "first"))


    def test_book_info_with_circulationdata(self):
        # Tests that can convert an overdrive json block into a CirculationData object.

        raw, info = self.sample_json("overdrive_availability_information.json")
        circulationdata = OverdriveRepresentationExtractor.book_info_to_circulation(info)

        # Related IDs.
        identifier = circulationdata.primary_identifier(self._db)
        eq_((Identifier.OVERDRIVE_ID, '2a005d55-a417-4053-b90d-7a38ca6d2065'),
            (identifier.type, identifier.identifier))


    def test_book_info_with_metadata(self):
        # Tests that can convert an overdrive json block into a Metadata object.

        raw, info = self.sample_json("overdrive_metadata.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        eq_("Agile Documentation", metadata.title)
        eq_("Agile Documentation A Pattern Guide to Producing Lightweight Documents for Software Projects", metadata.sort_title)
        eq_("A Pattern Guide to Producing Lightweight Documents for Software Projects", metadata.subtitle)
        eq_(Edition.BOOK_MEDIUM, metadata.medium)
        eq_("Wiley Software Patterns", metadata.series)
        eq_("eng", metadata.language)
        eq_("Wiley", metadata.publisher)
        eq_("John Wiley & Sons, Inc.", metadata.imprint)
        eq_(2005, metadata.published.year)
        eq_(1, metadata.published.month)
        eq_(31, metadata.published.day)

        [author] = metadata.contributors
        eq_(u"Rüping, Andreas", author.sort_name)
        eq_("Andreas R&#252;ping", author.display_name)
        eq_([Contributor.AUTHOR_ROLE], author.roles)

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)

        eq_([("Computer Technology", Subject.OVERDRIVE, 100),
             ("Nonfiction", Subject.OVERDRIVE, 100),
             ('Object Technologies - Miscellaneous', 'tag', 1),
         ],
            [(x.identifier, x.type, x.weight) for x in subjects]
        )

        # Related IDs.
        eq_((Identifier.OVERDRIVE_ID, '3896665d-9d81-4cac-bd43-ffc5066de1f5'),
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))

        ids = [(x.type, x.identifier) for x in metadata.identifiers]

        # The original data contains a blank ASIN and an invalid ISBN
        # in addition to the actual ASIN and ISBN, but they don't show up here.
        eq_(
            [
                (Identifier.ASIN, "B000VI88N2"),
                (Identifier.ISBN, "9780470856246"),
                (Identifier.OVERDRIVE_ID, '3896665d-9d81-4cac-bd43-ffc5066de1f5'),
            ],
            sorted(ids)
        )

        # Available formats.
        [kindle, pdf] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(DeliveryMechanism.KINDLE_CONTENT_TYPE, kindle.content_type)
        eq_(DeliveryMechanism.KINDLE_DRM, kindle.drm_scheme)

        eq_(Representation.PDF_MEDIA_TYPE, pdf.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, pdf.drm_scheme)

        # Links to various resources.
        shortd, image, longd = sorted(
            metadata.links, key=lambda x:x.rel
        )

        eq_(Hyperlink.DESCRIPTION, longd.rel)
        assert longd.content.startswith("<p>Software documentation")

        eq_(Hyperlink.SHORT_DESCRIPTION, shortd.rel)
        assert shortd.content.startswith("<p>Software documentation")
        assert len(shortd.content) < len(longd.content)

        eq_(Hyperlink.IMAGE, image.rel)
        eq_('http://images.contentreserve.com/ImageType-100/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg100.jpg', image.href)

        thumbnail = image.thumbnail

        eq_(Hyperlink.THUMBNAIL_IMAGE, thumbnail.rel)
        eq_('http://images.contentreserve.com/ImageType-200/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg200.jpg', thumbnail.href)

        # Measurements associated with the book.

        measurements = metadata.measurements
        popularity = [x for x in measurements
                      if x.quantity_measured==Measurement.POPULARITY][0]
        eq_(2, popularity.value)

        rating = [x for x in measurements
                  if x.quantity_measured==Measurement.RATING][0]
        eq_(1, rating.value)

        # Request only the bibliographic information.
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info, include_bibliographic=True, include_formats=False)

        eq_("Agile Documentation", metadata.title)
        eq_(None, metadata.circulation)

        # Request only the format information.
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info, include_bibliographic=False, include_formats=True)

        eq_(None, metadata.title)

        [kindle, pdf] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(DeliveryMechanism.KINDLE_CONTENT_TYPE, kindle.content_type)
        eq_(DeliveryMechanism.KINDLE_DRM, kindle.drm_scheme)

        eq_(Representation.PDF_MEDIA_TYPE, pdf.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, pdf.drm_scheme)


    def test_book_info_with_sample(self):
        raw, info = self.sample_json("has_sample.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        [sample] = [x for x in metadata.links if x.rel == Hyperlink.SAMPLE]
        eq_("http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub", sample.href)

    def test_book_info_with_grade_levels(self):
        raw, info = self.sample_json("has_grade_levels.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        grade_levels = sorted(
            [x.identifier for x in metadata.subjects
             if x.type==Subject.GRADE_LEVEL]
        )
        eq_([u'Grade 4', u'Grade 5', u'Grade 6', u'Grade 7', u'Grade 8'],
            grade_levels)

    def test_book_info_with_awards(self):
        raw, info = self.sample_json("has_awards.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        [awards] = [x for x in metadata.measurements
                    if Measurement.AWARDS == x.quantity_measured
        ]
        eq_(1, awards.value)
        eq_(1, awards.weight)


class TestOverdriveAdvantageAccount(OverdriveTestWithAPI):

    def test_no_advantage_accounts(self):
        """When there are no Advantage accounts, get_advantage_accounts()
        returns an empty list.
        """
        eq_([], self.api.get_advantage_accounts())

    def test_from_representation(self):
        """Test the creation of OverdriveAdvantageAccount objects
        from Overdrive's representation of a list of accounts.
        """
        raw, data = self.sample_json("advantage_accounts.json")
        [ac1, ac2] = OverdriveAdvantageAccount.from_representation(raw)

        # The two Advantage accounts have the same parent library ID.
        eq_("1225", ac1.parent_library_id)
        eq_("1225", ac2.parent_library_id)

        # But they have different names and library IDs.
        eq_("3", ac1.library_id)
        eq_("The Other Side of Town Library", ac1.name)

        eq_("9", ac2.library_id)
        eq_("The Common Community Library", ac2.name)

    def test_to_collection(self):
        """Test that we can turn an OverdriveAdvantageAccount object into
        a Collection object.
        """

        account = OverdriveAdvantageAccount(
            "parent_id", "child_id", "Library Name",
        )

        # We can't just create a Collection object for this object because
        # the parent doesn't exist.
        assert_raises_regexp(
            ValueError,
            "Cannot create a Collection whose parent does not already exist.",
            account.to_collection, self._db
        )

        # So, create a Collection to be the parent.
        parent = self._collection(
            name="Parent", protocol=ExternalIntegration.OVERDRIVE,
            external_account_id="parent_id"
        )

        # Now it works.
        p, collection = account.to_collection(self._db)
        eq_(p, parent)
        eq_(parent, collection.parent)
        eq_(collection.external_account_id, account.library_id)
        eq_(ExternalIntegration.LICENSE_GOAL,
            collection.external_integration.goal)
        eq_(ExternalIntegration.OVERDRIVE,
            collection.protocol)

        # To ensure uniqueness, the collection was named after its
        # parent.
        eq_("%s / %s" % (parent.name, account.name), collection.name)


class TestOverdriveBibliographicCoverageProvider(OverdriveTest):
    """Test the code that looks up bibliographic information from Overdrive."""

    def setup(self):
        super(TestOverdriveBibliographicCoverageProvider, self).setup()
        self.provider = OverdriveBibliographicCoverageProvider(
            self.collection, api_class=MockOverdriveAPI
        )
        self.api = self.provider.api

    def test_script_instantiation(self):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            OverdriveBibliographicCoverageProvider, self._db,
            api_class=MockOverdriveAPI
        )
        [provider] = script.providers
        assert isinstance(provider,
                          OverdriveBibliographicCoverageProvider)
        assert isinstance(provider.api, MockOverdriveAPI)
        eq_(self.collection, provider.collection)

    def test_invalid_or_unrecognized_guid(self):
        """A bad or malformed GUID can't get coverage."""
        identifier = self._identifier()
        identifier.identifier = 'bad guid'

        error = '{"errorCode": "InvalidGuid", "message": "An invalid guid was given.", "token": "7aebce0e-2e88-41b3-b6d3-82bf15f8e1a2"}'
        self.api.queue_response(200, content=error)

        failure = self.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        eq_(False, failure.transient)
        eq_("Invalid Overdrive ID: bad guid", failure.exception)

        # This is for when the GUID is well-formed but doesn't
        # correspond to any real Overdrive book.
        error = '{"errorCode": "NotFound", "message": "Not found in Overdrive collection.", "token": "7aebce0e-2e88-41b3-b6d3-82bf15f8e1a2"}'
        self.api.queue_response(200, content=error)

        failure = self.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        eq_(False, failure.transient)
        eq_("ID not recognized by Overdrive: bad guid", failure.exception)

    def test_process_item_creates_presentation_ready_work(self):
        """Test the normal workflow where we ask Overdrive for data,
        Overdrive provides it, and we create a presentation-ready work.
        """
        # Here's the book mentioned in overdrive_metadata.json.
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        identifier.identifier = '3896665d-9d81-4cac-bd43-ffc5066de1f5'

        # This book has no LicensePool.
        eq_([], identifier.licensed_through)

        # Run it through the OverdriveBibliographicCoverageProvider
        raw, info = self.sample_json("overdrive_metadata.json")
        self.api.queue_response(200, content=raw)

        [result] = self.provider.process_batch([identifier])
        eq_(identifier, result)

        # A LicensePool was created, not because we know anything
        # about how we've licensed this book, but to have a place to
        # store the information about what formats the book is
        # available in.
        [pool] = identifier.licensed_through
        eq_(0, pool.licenses_owned)
        [lpdm1, lpdm2] = pool.delivery_mechanisms
        names = [x.delivery_mechanism.name for x in pool.delivery_mechanisms]
        eq_(sorted([u'application/pdf (application/vnd.adobe.adept+xml)',
                    u'Kindle via Amazon (Kindle DRM)']), sorted(names))

        # A Work was created and made presentation ready.
        eq_("Agile Documentation", pool.work.title)
        eq_(True, pool.work.presentation_ready)

