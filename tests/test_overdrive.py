# encoding: utf-8
import pytest
import os
import json
import pkgutil

from ..overdrive import (
    OverdriveAPI,
    MockOverdriveAPI,
    OverdriveAdvantageAccount,
    OverdriveRepresentationExtractor,
    OverdriveBibliographicCoverageProvider,
)

from ..coverage import (
    CoverageFailure,
)

from ..config import CannotLoadConfiguration

from ..metadata_layer import LinkData

from ..model import (
    Collection,
    Contributor,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Identifier,
    Representation,
    Subject,
    Measurement,
    MediaTypes,
    Hyperlink,
)
from ..scripts import RunCollectionCoverageProviderScript

from ..testing import MockRequestsResponse

from ..util.http import (
    BadResponseException,
    HTTP,
)

from ..testing import DatabaseTest


class OverdriveTest(DatabaseTest):

    def setup_method(self):
        super(OverdriveTest, self).setup_method()
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
    def setup_method(self):
        super(OverdriveTestWithAPI, self).setup_method()
        self.api = MockOverdriveAPI(self._db, self.collection)


class TestOverdriveAPI(OverdriveTestWithAPI):

    def test_constructor_makes_no_requests(self):
        # Invoking the OverdriveAPI constructor does not, by itself,
        # make any HTTP requests.
        collection = MockOverdriveAPI.mock_collection(self._db)

        class NoRequests(OverdriveAPI):
            MSG = "This is a unit test, you can't make HTTP requests!"
            def no_requests(self, *args, **kwargs):
                raise Exception(self.MSG)
            _do_get = no_requests
            _do_post = no_requests
            _make_request = no_requests
        api = NoRequests(self._db, collection)

        # Attempting to access .token or .collection_token _will_
        # try to make an HTTP request.
        for field in 'token', 'collection_token':
            with pytest.raises(Exception) as excinfo:
                getattr(api, field)
            assert api.MSG in str(excinfo.value)

    def test_ils_name(self):
        """The 'ils_name' setting (defined in
        MockOverdriveAPI.mock_collection) is available through
        OverdriveAPI.ils_name().
        """
        assert "e" == self.api.ils_name(self._default_library)

        # The value must be explicitly set for a given library, or
        # else the default will be used.
        l2 = self._library()
        assert "default" == self.api.ils_name(l2)

    def test_make_link_safe(self):
        # Unsafe characters are escaped.
        assert ("http://foo.com?q=%2B%3A%7B%7D" ==
            OverdriveAPI.make_link_safe("http://foo.com?q=+:{}"))

        # Links to version 1 of the availability API are converted
        # to links to version 2.
        v1 = "https://qa.api.overdrive.com/v1/collections/abcde/products/12345/availability"
        v2 = "https://qa.api.overdrive.com/v2/collections/abcde/products/12345/availability"
        assert v2 == OverdriveAPI.make_link_safe(v1)

        # We also handle the case of a trailing slash, just in case Overdrive
        # starts serving links with trailing slashes.
        v1 = v1 + "/"
        v2 = v2 + "/"
        assert v2 == OverdriveAPI.make_link_safe(v1)

        # Links to other endpoints are not converted
        leave_alone = "https://qa.api.overdrive.com/v1/collections/abcde/products/12345"
        assert leave_alone == OverdriveAPI.make_link_safe(leave_alone)

    def test_hosts(self):
        c = OverdriveAPI

        # By default, OverdriveAPI is initialized with the production
        # set of hostnames.
        assert self.api.hosts == c.HOSTS[c.PRODUCTION_SERVERS]

        # You can instead initialize it to use the testing set of
        # hostnames.
        def api_with_setting(x):
            integration = self.collection.external_integration
            integration.setting(c.SERVER_NICKNAME).value = x
            return c(self._db, self.collection)
        testing = api_with_setting(c.TESTING_SERVERS)
        assert testing.hosts == c.HOSTS[c.TESTING_SERVERS]

        # If the setting doesn't make sense, we default to production
        # hostnames.
        bad = api_with_setting("nonsensical")
        assert bad.hosts == c.HOSTS[c.PRODUCTION_SERVERS]

    def test_endpoint(self):
        # The .endpoint() method performs string interpolation, including
        # the names of servers.
        template = "%(host)s %(patron_host)s %(oauth_host)s %(oauth_patron_host)s %(extra)s"
        result = self.api.endpoint(template, extra="val")

        # The host names and the 'extra' argument have been used to
        # fill in the string interpolations.
        expect_args = dict(self.api.hosts)
        expect_args['extra'] = 'val'
        assert result == template % expect_args

        # The string has been completely interpolated.
        assert '%' not in result

        # Once interpolation has happened, doing it again has no effect.
        assert result == self.api.endpoint(result, extra="something else")

        # This is important because an interpolated URL may superficially
        # appear to contain extra formatting characters.
        assert (result + "%3A" ==
            self.api.endpoint(result +"%3A", extra="something else"))

    def test_token_post_success(self):
        self.api.queue_response(200, content="some content")
        response = self.api.token_post(self._url, "the payload")
        assert 200 == response.status_code
        assert self.api.access_token_response.content == response.content

    def test_get_success(self):
        self.api.queue_response(200, content="some content")
        status_code, headers, content = self.api.get(self._url, {})
        assert 200 == status_code
        assert "some content" == content

    def test_failure_to_get_library_is_fatal(self):
        self.api.queue_response(500)

        with pytest.raises(BadResponseException) as excinfo:
            self.api.get_library()
        assert "Got status code 500" in str(excinfo.value)

    def test_error_getting_library(self):
        class MisconfiguredOverdriveAPI(MockOverdriveAPI):
            """This Overdrive client has valid credentials but the library
            can't be found -- probably because the library ID is wrong."""
            def get_library(self):
                return {u'errorCode': u'Some error', u'message': u'Some message.', u'token': u'abc-def-ghi'}

        # Just instantiating the API doesn't cause this error.
        api = MisconfiguredOverdriveAPI(self._db, self.collection)

        # But trying to access the collection token will cause it.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            api.collection_token()
        assert "Overdrive credentials are valid but could not fetch library: Some message." in str(excinfo.value)

    def test_401_on_get_refreshes_bearer_token(self):
        # We have a token.
        assert "bearer token" == self.api.token

        # But then we try to GET, and receive a 401.
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

        assert 200 == status_code
        assert "at last, the content" == content

        # The bearer token has been updated.
        assert "new bearer token" == self.api.token

    def test_credential_refresh_success(self):
        """Verify the process of refreshing the Overdrive bearer token.
        """
        # Perform the initial credential check.
        self.api.check_creds()
        credential = self.api.credential_object(lambda x: x)
        assert "bearer token" == credential.credential
        assert self.api.token == credential.credential

        self.api.access_token_response = self.api.mock_access_token_response(
            "new bearer token"
        )

        # Refresh the credentials and the token will change to
        # the mocked value.
        self.api.refresh_creds(credential)
        assert "new bearer token" == credential.credential
        assert self.api.token == credential.credential

    def test_401_after_token_refresh_raises_error(self):

        assert "bearer token" == self.api.token

        # We try to GET and receive a 401.
        self.api.queue_response(401)

        # We refresh the bearer token.
        self.api.access_token_response = self.api.mock_access_token_response(
            "new bearer token"
        )

        # Then we retry the GET but we get another 401.
        self.api.queue_response(401)

        credential = self.api.credential_object(lambda x: x)
        self.api.refresh_creds(credential)

        # That raises a BadResponseException
        with pytest.raises(BadResponseException) as excinfo:
            self.api.get_library()
        assert "Bad response from" in str(excinfo.value)
        assert "Something's wrong with the Overdrive OAuth Bearer Token!" in str(excinfo.value)

    def test_401_during_refresh_raises_error(self):
        """If we fail to refresh the OAuth bearer token, an exception is
        raised.
        """
        self.api.access_token_response = MockRequestsResponse(401, {}, "")
        with pytest.raises(BadResponseException) as excinfo:
            self.api.refresh_creds(None)
        assert "Got status code 401" in str(excinfo.value)
        assert "can only continue on: 200." in str(excinfo.value)

    def test_advantage_differences(self):
        # Test the differences between Advantage collections and
        # regular Overdrive collections.

        # Here's a regular Overdrive collection.
        main = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id="1",
        )
        main.external_integration.username = "user"
        main.external_integration.password = "password"
        main.external_integration.setting('website_id').value = '100'
        main.external_integration.setting('ils_name').value = 'default'

        # Here's an Overdrive API client for that collection.
        overdrive_main = MockOverdriveAPI(self._db, main)

        # Note the "library" endpoint.
        assert ("https://api.overdrive.com/v1/libraries/1" ==
            overdrive_main._library_endpoint)

        # The advantage_library_id of a non-Advantage Overdrive account
        # is always -1.
        assert "1" == overdrive_main.library_id
        assert -1 == overdrive_main.advantage_library_id

        # Here's an Overdrive Advantage collection associated with the
        # main Overdrive collection.
        child = self._collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id="2",
        )
        child.parent = main
        overdrive_child = MockOverdriveAPI(self._db, child)

        # In URL-space, the "library" endpoint for the Advantage
        # collection is beneath the the parent collection's "library"
        # endpoint.
        assert (
            'https://api.overdrive.com/v1/libraries/1/advantageAccounts/2' ==
            overdrive_child._library_endpoint)

        # The advantage_library_id of an Advantage collection is the
        # numeric value of its external_account_id.
        assert "2" == overdrive_child.library_id
        assert 2 == overdrive_child.advantage_library_id


class TestOverdriveRepresentationExtractor(OverdriveTestWithAPI):

    def test_availability_info(self):
        data, raw = self.sample_json("overdrive_book_list.json")
        availability = OverdriveRepresentationExtractor.availability_link_list(
            raw)
        # Every item in the list has a few important values.
        for item in availability:
            for key in 'availability_link', 'author_name', 'id', 'title', 'date_added':
                assert key in item

        # Also run a spot check on the actual values.
        spot = availability[0]
        assert '210bdcad-29b7-445f-8d05-cdbb40abc03a' == spot['id']
        assert 'King and Maxwell' == spot['title']
        assert 'David Baldacci' == spot['author_name']
        assert '2013-11-12T14:13:00-05:00' == spot['date_added']

    def test_availability_info_missing_data(self):
        # overdrive_book_list_missing_data.json has two products. One
        # only has a title, the other only has an ID.
        data, raw = self.sample_json("overdrive_book_list_missing_data.json")
        [item] = OverdriveRepresentationExtractor.availability_link_list(
            raw)

        # We got a data structure -- full of missing data -- for the
        # item that has an ID.
        assert 'i only have an id' == item['id']
        assert None == item['title']
        assert None == item['author_name']
        assert None == item['date_added']

        # We did not get a data structure for the item that only has a
        # title, because an ID is required -- otherwise we don't know
        # what book we're talking about.

    def test_link(self):
        data, raw = self.sample_json("overdrive_book_list.json")
        expect = OverdriveAPI.make_link_safe("http://api.overdrive.com/v1/collections/collection-id/products?limit=300&offset=0&lastupdatetime=2014-04-28%2009:25:09&sort=popularity:desc&formats=ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open")
        assert expect == OverdriveRepresentationExtractor.link(raw, "first")


    def test_book_info_to_circulation(self):
        # Tests that can convert an overdrive json block into a CirculationData object.

        raw, info = self.sample_json("overdrive_availability_information.json")
        extractor = OverdriveRepresentationExtractor(self.api)
        circulationdata = extractor.book_info_to_circulation(info)

        # NOTE: It's not realistic for licenses_available and
        # patrons_in_hold_queue to both be nonzero; this is just to
        # verify that the test picks up whatever data is in the
        # document.
        assert 3 == circulationdata.licenses_owned
        assert 1 == circulationdata.licenses_available
        assert 10 == circulationdata.patrons_in_hold_queue

        # Related IDs.
        identifier = circulationdata.primary_identifier(self._db)
        assert ((Identifier.OVERDRIVE_ID, '2a005d55-a417-4053-b90d-7a38ca6d2065') ==
            (identifier.type, identifier.identifier))

    def test_book_info_to_circulation_advantage(self):
        # Overdrive Advantage accounts derive different information
        # from the same API responses as regular Overdrive accounts.
        raw, info = self.sample_json("overdrive_availability_advantage.json")

        extractor = OverdriveRepresentationExtractor(self.api)
        consortial_data = extractor.book_info_to_circulation(info)
        assert 2 == consortial_data.licenses_owned
        assert 2 == consortial_data.licenses_available

        class MockAPI(object):
            # Pretend to be an API for an Overdrive Advantage collection with
            # library ID 61.
            advantage_library_id = 61

        extractor = OverdriveRepresentationExtractor(MockAPI())
        advantage_data = extractor.book_info_to_circulation(info)
        assert 1 == advantage_data.licenses_owned
        assert 1 == advantage_data.licenses_available

        # Both collections have the same information about active
        # holds, because that information is not split out by
        # collection.
        assert 0 == advantage_data.patrons_in_hold_queue
        assert 0 == consortial_data.patrons_in_hold_queue

        # If for whatever reason Overdrive doesn't mention the
        # relevant collection at all, no collection-specific
        # information is gleaned.
        #
        # TODO: It would probably be better not to return a
        # CirculationData object at all, but this shouldn't happen in
        # a real scenario.
        class MockAPI(object):
            # Pretend to be an API for an Overdrive Advantage collection with
            # library ID 62.
            advantage_library_id = 62
        extractor = OverdriveRepresentationExtractor(MockAPI())
        advantage_data = extractor.book_info_to_circulation(info)
        assert None == advantage_data.licenses_owned
        assert None == advantage_data.licenses_available
        assert 0 == consortial_data.patrons_in_hold_queue


    def test_not_found_error_to_circulationdata(self):
        raw, info = self.sample_json("overdrive_availability_not_found.json")

        # By default, a "NotFound" error can't be converted to a
        # CirculationData object, because we don't know _which_ book it
        # was that wasn't found.
        extractor = OverdriveRepresentationExtractor(self.api)
        m = extractor.book_info_to_circulation
        assert None == m(info)

        # However, if an ID was added to `info` ahead of time (as the
        # circulation code does), we do know, and we can create a
        # CirculationData.
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        info['id'] = identifier.identifier
        data = m(info)
        assert identifier == data.primary_identifier(self._db)
        assert 0 == data.licenses_owned
        assert 0 == data.licenses_available
        assert 0 == data.patrons_in_hold_queue

    def test_book_info_with_metadata(self):
        # Tests that can convert an overdrive json block into a Metadata object.

        raw, info = self.sample_json("overdrive_metadata.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        assert "Agile Documentation" == metadata.title
        assert "Agile Documentation A Pattern Guide to Producing Lightweight Documents for Software Projects" == metadata.sort_title
        assert "A Pattern Guide to Producing Lightweight Documents for Software Projects" == metadata.subtitle
        assert Edition.BOOK_MEDIUM == metadata.medium
        assert "Wiley Software Patterns" == metadata.series
        assert "eng" == metadata.language
        assert "Wiley" == metadata.publisher
        assert "John Wiley & Sons, Inc." == metadata.imprint
        assert 2005 == metadata.published.year
        assert 1 == metadata.published.month
        assert 31 == metadata.published.day

        [author] = metadata.contributors
        assert u"Rüping, Andreas" == author.sort_name
        assert "Andreas R&#252;ping" == author.display_name
        assert [Contributor.AUTHOR_ROLE] == author.roles

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)

        assert ([("Computer Technology", Subject.OVERDRIVE, 100),
             ("Nonfiction", Subject.OVERDRIVE, 100),
             ('Object Technologies - Miscellaneous', 'tag', 1),
         ] ==
            [(x.identifier, x.type, x.weight) for x in subjects])

        # Related IDs.
        assert ((Identifier.OVERDRIVE_ID, '3896665d-9d81-4cac-bd43-ffc5066de1f5') ==
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))

        ids = [(x.type, x.identifier) for x in metadata.identifiers]

        # The original data contains an actual ASIN and ISBN, plus a blank
        # ASIN and three invalid ISBNs: one which is common placeholder
        # text, one which is mis-typed and has a bad check digit, and one
        # which has an invalid character; the bad identifiers do not show
        # up here.
        assert (
            [
                (Identifier.ASIN, "B000VI88N2"),
                (Identifier.ISBN, "9780470856246"),
                (Identifier.OVERDRIVE_ID, '3896665d-9d81-4cac-bd43-ffc5066de1f5'),
            ] ==
            sorted(ids))

        # Available formats.
        [kindle, pdf] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        assert DeliveryMechanism.KINDLE_CONTENT_TYPE == kindle.content_type
        assert DeliveryMechanism.KINDLE_DRM == kindle.drm_scheme

        assert Representation.PDF_MEDIA_TYPE == pdf.content_type
        assert DeliveryMechanism.ADOBE_DRM == pdf.drm_scheme

        # Links to various resources.
        shortd, image, longd = sorted(
            metadata.links, key=lambda x:x.rel
        )

        assert Hyperlink.DESCRIPTION == longd.rel
        assert longd.content.startswith("<p>Software documentation")

        assert Hyperlink.SHORT_DESCRIPTION == shortd.rel
        assert shortd.content.startswith("<p>Software documentation")
        assert len(shortd.content) < len(longd.content)

        assert Hyperlink.IMAGE == image.rel
        assert 'http://images.contentreserve.com/ImageType-100/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg100.jpg' == image.href

        thumbnail = image.thumbnail

        assert Hyperlink.THUMBNAIL_IMAGE == thumbnail.rel
        assert 'http://images.contentreserve.com/ImageType-200/0128-1/%7B3896665D-9D81-4CAC-BD43-FFC5066DE1F5%7DImg200.jpg' == thumbnail.href

        # Measurements associated with the book.

        measurements = metadata.measurements
        popularity = [x for x in measurements
                      if x.quantity_measured==Measurement.POPULARITY][0]
        assert 2 == popularity.value

        rating = [x for x in measurements
                  if x.quantity_measured==Measurement.RATING][0]
        assert 1 == rating.value

        # Request only the bibliographic information.
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info, include_bibliographic=True, include_formats=False)

        assert "Agile Documentation" == metadata.title
        assert None == metadata.circulation

        # Request only the format information.
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info, include_bibliographic=False, include_formats=True)

        assert None == metadata.title

        [kindle, pdf] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        assert DeliveryMechanism.KINDLE_CONTENT_TYPE == kindle.content_type
        assert DeliveryMechanism.KINDLE_DRM == kindle.drm_scheme

        assert Representation.PDF_MEDIA_TYPE == pdf.content_type
        assert DeliveryMechanism.ADOBE_DRM == pdf.drm_scheme

    def test_audiobook_info(self):
        # This book will be available in three formats: a link to the
        # Overdrive Read website, a manifest file that SimplyE can
        # download, and the legacy format used by the mobile app
        # called 'Overdrive'.
        raw, info = self.sample_json("audiobook.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        streaming, manifest, legacy = sorted(
            metadata.circulation.formats, key=lambda x: x.content_type
        )
        assert (DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE ==
            streaming.content_type)
        assert (MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE ==
            manifest.content_type)
        assert "application/x-od-media" == legacy.content_type

    def test_book_info_with_sample(self):
        # This book has two samples; one available as a direct download and
        # one available through a manifest file.
        raw, info = self.sample_json("has_sample.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)
        samples = [x for x in metadata.links if x.rel == Hyperlink.SAMPLE]
        epub_sample, manifest_sample = sorted(
            samples, key=lambda x: x.media_type
        )

        # Here's the direct download.
        assert ("http://excerpts.contentreserve.com/FormatType-410/1071-1/9BD/24F/82/BridesofConvenienceBundle9781426803697.epub" ==
            epub_sample.href)
        assert MediaTypes.EPUB_MEDIA_TYPE == epub_sample.media_type

        # Here's the manifest.
        assert ("https://samples.overdrive.com/?crid=9BD24F82-35C0-4E0A-B5E7-BCFED07835CF&.epub-sample.overdrive.com" ==
            manifest_sample.href)
        assert (MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE ==
            manifest_sample.media_type)

    def test_book_info_with_grade_levels(self):
        raw, info = self.sample_json("has_grade_levels.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        grade_levels = sorted(
            [x.identifier for x in metadata.subjects
             if x.type==Subject.GRADE_LEVEL]
        )
        assert ([u'Grade 4', u'Grade 5', u'Grade 6', u'Grade 7', u'Grade 8'] ==
            grade_levels)

    def test_book_info_with_awards(self):
        raw, info = self.sample_json("has_awards.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        [awards] = [x for x in metadata.measurements
                    if Measurement.AWARDS == x.quantity_measured
        ]
        assert 1 == awards.value
        assert 1 == awards.weight

    def test_image_link_to_linkdata(self):
        def m(link):
            return OverdriveRepresentationExtractor.image_link_to_linkdata(
                link, "rel"
            )

        # Test missing data.
        assert None == m(None)
        assert None == m(dict())

        # Test an ordinary success case.
        url = "http://images.overdrive.com/image.png"
        type = "image/type"
        data = m(dict(href=url, type=type))
        assert isinstance(data, LinkData)
        assert url == data.href
        assert type == data.media_type

        # Test a case where no media type is provided.
        data = m(dict(href=url))
        assert None == data.media_type

        # Verify that invalid URLs are made link-safe.
        data = m(dict(href="http://api.overdrive.com/v1/foo:bar"))
        assert "http://api.overdrive.com/v1/foo%3Abar" == data.href

        # Stand-in cover images are detected and filtered out.
        data = m(dict(href="https://img1.od-cdn.com/ImageType-100/0293-1/{00000000-0000-0000-0000-000000000002}Img100.jpg"))
        assert None == data

    def test_internal_formats(self):
        # Overdrive's internal format names may correspond to one or more
        # delivery mechanisms.
        def assert_formats(overdrive_name, *expect):
            actual = OverdriveRepresentationExtractor.internal_formats(
                overdrive_name
            )
            assert list(expect) == list(actual)

        # Most formats correspond to one delivery mechanism.
        assert_formats(
            'ebook-pdf-adobe',
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        )

        assert_formats(
            'ebook-epub-open',
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        )

        # ebook-overdrive and audiobook-overdrive each correspond to
        # two delivery mechanisms.
        assert_formats(
            'ebook-overdrive',
            (MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
             DeliveryMechanism.LIBBY_DRM),
            (DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
             DeliveryMechanism.STREAMING_DRM),
        )

        assert_formats(
            'audiobook-overdrive',
            (MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
             DeliveryMechanism.LIBBY_DRM),
            (DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
             DeliveryMechanism.STREAMING_DRM),
        )

        # An unrecognized format does not correspond to any delivery
        # mechanisms.
        assert_formats('no-such-format')

class TestOverdriveAdvantageAccount(OverdriveTestWithAPI):

    def test_no_advantage_accounts(self):
        """When there are no Advantage accounts, get_advantage_accounts()
        returns an empty list.
        """
        self.api.queue_collection_token()
        assert [] == self.api.get_advantage_accounts()

    def test_from_representation(self):
        """Test the creation of OverdriveAdvantageAccount objects
        from Overdrive's representation of a list of accounts.
        """
        raw, data = self.sample_json("advantage_accounts.json")
        [ac1, ac2] = OverdriveAdvantageAccount.from_representation(raw)

        # The two Advantage accounts have the same parent library ID.
        assert "1225" == ac1.parent_library_id
        assert "1225" == ac2.parent_library_id

        # But they have different names and library IDs.
        assert "3" == ac1.library_id
        assert "The Other Side of Town Library" == ac1.name

        assert "9" == ac2.library_id
        assert "The Common Community Library" == ac2.name

    def test_to_collection(self):
        # Test that we can turn an OverdriveAdvantageAccount object into
        # a Collection object.

        account = OverdriveAdvantageAccount(
            "parent_id", "child_id", "Library Name",
        )

        # We can't just create a Collection object for this object because
        # the parent doesn't exist.
        with pytest.raises(ValueError) as excinfo:
            account.to_collection(self._db)
        assert "Cannot create a Collection whose parent does not already exist." in str(excinfo.value)

        # So, create a Collection to be the parent.
        parent = self._collection(
            name="Parent", protocol=ExternalIntegration.OVERDRIVE,
            external_account_id="parent_id"
        )

        # Now it works.
        p, collection = account.to_collection(self._db)
        assert p == parent
        assert parent == collection.parent
        assert collection.external_account_id == account.library_id
        assert (ExternalIntegration.LICENSE_GOAL ==
            collection.external_integration.goal)
        assert (ExternalIntegration.OVERDRIVE ==
            collection.protocol)

        # To ensure uniqueness, the collection was named after its
        # parent.
        assert "%s / %s" % (parent.name, account.name) == collection.name


class TestOverdriveBibliographicCoverageProvider(OverdriveTest):
    """Test the code that looks up bibliographic information from Overdrive."""

    def setup_method(self):
        super(TestOverdriveBibliographicCoverageProvider, self).setup_method()
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
        assert self.collection == provider.collection

    def test_invalid_or_unrecognized_guid(self):
        """A bad or malformed GUID can't get coverage."""
        identifier = self._identifier()
        identifier.identifier = 'bad guid'
        self.api.queue_collection_token()

        error = '{"errorCode": "InvalidGuid", "message": "An invalid guid was given.", "token": "7aebce0e-2e88-41b3-b6d3-82bf15f8e1a2"}'
        self.api.queue_response(200, content=error)

        failure = self.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        assert False == failure.transient
        assert "Invalid Overdrive ID: bad guid" == failure.exception

        # This is for when the GUID is well-formed but doesn't
        # correspond to any real Overdrive book.
        error = '{"errorCode": "NotFound", "message": "Not found in Overdrive collection.", "token": "7aebce0e-2e88-41b3-b6d3-82bf15f8e1a2"}'
        self.api.queue_response(200, content=error)

        failure = self.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        assert False == failure.transient
        assert "ID not recognized by Overdrive: bad guid" == failure.exception

    def test_process_item_creates_presentation_ready_work(self):
        """Test the normal workflow where we ask Overdrive for data,
        Overdrive provides it, and we create a presentation-ready work.
        """
        self.api.queue_collection_token()

        # Here's the book mentioned in overdrive_metadata.json.
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        identifier.identifier = '3896665d-9d81-4cac-bd43-ffc5066de1f5'

        # This book has no LicensePool.
        assert [] == identifier.licensed_through

        # Run it through the OverdriveBibliographicCoverageProvider
        raw, info = self.sample_json("overdrive_metadata.json")
        self.api.queue_response(200, content=raw)

        [result] = self.provider.process_batch([identifier])
        assert identifier == result

        # A LicensePool was created, not because we know anything
        # about how we've licensed this book, but to have a place to
        # store the information about what formats the book is
        # available in.
        [pool] = identifier.licensed_through
        assert 0 == pool.licenses_owned
        [lpdm1, lpdm2] = pool.delivery_mechanisms
        names = [x.delivery_mechanism.name for x in pool.delivery_mechanisms]
        assert sorted([u'application/pdf (application/vnd.adobe.adept+xml)',
                    u'Kindle via Amazon (Kindle DRM)']) == sorted(names)

        # A Work was created and made presentation ready.
        assert "Agile Documentation" == pool.work.title
        assert True == pool.work.presentation_ready

