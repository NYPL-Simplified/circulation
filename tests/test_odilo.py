# encoding: utf-8
from nose.tools import (
    assert_raises_regexp,
    eq_,
    ok_,
    set_trace,
)

import datetime
import os
import json

from util.http import (
    BadResponseException,
)

from odilo import (
    MockOdiloAPI,
    OdiloRepresentationExtractor,
    OdiloBibliographicCoverageProvider
)

from model import (
    Contributor,
    DeliveryMechanism,
    Edition,
    Identifier,
    Representation,
    Hyperlink,
)

from testing import (
    DatabaseTest,
    MockRequestsResponse,
)


class OdiloTest(DatabaseTest):
    def setup(self):
        super(OdiloTest, self).setup()
        self.collection = MockOdiloAPI.mock_collection(self._db)
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "odilo")

    def sample_json(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)


class OdiloTestWithAPI(OdiloTest):
    """Automatically create a MockOdiloAPI class during setup.
    """

    def setup(self):
        super(OdiloTestWithAPI, self).setup()
        self.api = MockOdiloAPI(self._db, self.collection)


class TestOdiloAPI(OdiloTestWithAPI):
    def test_token_post_success(self):
        self.api.queue_response(200, content="some content")
        response = self.api.token_post(self._url, "the payload")
        eq_(200, response.status_code, msg="Status code != 200 --> %i" % response.status_code)
        eq_(self.api.access_token_response.content, response.content)
        self.api.log.info('Test token post success ok!')

    def test_get_success(self):
        self.api.queue_response(200, content="some content")
        status_code, headers, content = self.api.get(self._url, {})
        eq_(200, status_code)
        eq_("some content", content)
        self.api.log.info('Test get success ok!')

    def test_401_on_get_refreshes_bearer_token(self):
        eq_("bearer token", self.api.token)

        # We try to GET and receive a 401.
        self.api.queue_response(401)

        # We refresh the bearer token. (This happens in
        # MockOdiloAPI.token_post, so we don't mock the response
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

        self.api.log.info('Test 401 on get refreshes bearer token ok!')

    def test_credential_refresh_success(self):
        """Verify the process of refreshing the Odilo bearer token.
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

        # By default, the access token's 'expiresIn' value is -1,
        # indicating that the token will never expire.
        #
        # To reflect this fact, credential.expires is set to None.
        eq_(None, credential.expires)

        # But a token may specify a specific expiration time,
        # which is used to set a future value for credential.expires.
        self.api.access_token_response = self.api.mock_access_token_response(
            "new bearer token 2", 1000
        )
        self.api.refresh_creds(credential)
        eq_("new bearer token 2", credential.credential)
        eq_(self.api.token, credential.credential)
        assert credential.expires > datetime.datetime.now()

    def test_credential_refresh_failure(self):
        """Verify that a useful error message results when the Odilo bearer
        token cannot be refreshed, since this is the most likely point
        of failure on a new setup.
        """
        self.api.access_token_response = MockRequestsResponse(
            200, {"Content-Type": "text/html"},
            "Hi, this is the website, not the API."
        )
        credential = self.api.credential_object(lambda x: x)
        assert_raises_regexp(
            BadResponseException,
            "Bad response from .*: .* may not be the right base URL. Response document was: 'Hi, this is the website, not the API.'",
            self.api.refresh_creds,
            credential
        )

        # Also test a 400 response code.
        self.api.access_token_response = MockRequestsResponse(
            400, {"Content-Type": "application/json"},

            json.dumps(dict(errors=[dict(description="Oops")]))
        )
        assert_raises_regexp(
            BadResponseException, "Bad response from .*: Oops",
            self.api.refresh_creds,
            credential
        )

        # If there's a 400 response but no error information,
        # the generic error message is used.
        self.api.access_token_response = MockRequestsResponse(
            400, {"Content-Type": "application/json"},

            json.dumps(dict())
        )
        assert_raises_regexp(
            BadResponseException, "Bad response from .*: .* may not be the right base URL.",
            self.api.refresh_creds,
            credential
        )


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
            BadResponseException, "Bad response from .*:Something's wrong with the Odilo OAuth Bearer Token!",
        )

        self.api.log.info('Test 401 after token refresh raises error ok!')


class TestOdiloBibliographicCoverageProvider(OdiloTest):
    def setup(self):
        super(TestOdiloBibliographicCoverageProvider, self).setup()
        self.provider = OdiloBibliographicCoverageProvider(
            self.collection, api_class=MockOdiloAPI
        )
        self.api = self.provider.api

    def test_process_item(self):
        record_metadata, record_metadata_json = self.sample_json("odilo_metadata.json")
        self.api.queue_response(200, content=record_metadata_json)
        availability, availability_json = self.sample_json("odilo_availability.json")
        self.api.queue_response(200, content=availability)

        identifier, made_new = self.provider.process_item('00010982')

        # Check that the Identifier returned has the right .type and .identifier.
        ok_(identifier, msg="Problem while testing process item !!!")
        eq_(identifier.type, Identifier.ODILO_ID)
        eq_(identifier.identifier, '00010982')

        # Check that metadata and availability information were imported properly
        [pool] = identifier.licensed_through
        eq_("Busy Brownies", pool.work.title)

        eq_(2, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(2, pool.patrons_in_hold_queue)
        eq_(1, pool.licenses_reserved)

        names = [x.delivery_mechanism.name for x in pool.delivery_mechanisms]
        eq_(sorted([Representation.EPUB_MEDIA_TYPE + ' (' + DeliveryMechanism.ADOBE_DRM + ')',
                    Representation.TEXT_HTML_MEDIA_TYPE + ' (' + DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE + ')']),
            sorted(names))

        # Check that handle_success was called --> A Work was created and made presentation ready.
        eq_(True, pool.work.presentation_ready)

        self.api.log.info('Testing process item finished ok !!')

    def test_process_inactive_item(self):
        record_metadata, record_metadata_json = self.sample_json("odilo_metadata_inactive.json")
        self.api.queue_response(200, content=record_metadata_json)
        availability, availability_json = self.sample_json("odilo_availability_inactive.json")
        self.api.queue_response(200, content=availability)

        identifier, made_new = self.provider.process_item('00011135')

        # Check that the Identifier returned has the right .type and .identifier.
        ok_(identifier, msg="Problem while testing process inactive item !!!")
        eq_(identifier.type, Identifier.ODILO_ID)
        eq_(identifier.identifier, '00011135')

        [pool] = identifier.licensed_through
        eq_("!Tention A Story of Boy-Life during the Peninsular War", pool.work.title)

        # Check work not available
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)

        eq_(True, pool.work.presentation_ready)

        self.api.log.info('Testing process item inactive finished ok !!')


class TestOdiloRepresentationExtractor(OdiloTestWithAPI):
    def test_book_info_with_metadata(self):
        # Tests that can convert an odilo json block into a Metadata object.

        raw, book_json = self.sample_json("odilo_metadata.json")
        raw, availability = self.sample_json("odilo_availability.json")
        metadata, active = OdiloRepresentationExtractor.record_info_to_metadata(book_json, availability)

        eq_("Busy Brownies", metadata.title)
        eq_(" (The Classic Fantasy Literature of Elves for Children)", metadata.subtitle)
        eq_("eng", metadata.language)
        eq_(Edition.BOOK_MEDIUM, metadata.medium)
        eq_("The Classic Fantasy Literature for Children written in 1896 retold for Elves adventure.", metadata.series)
        eq_("1", metadata.series_position)
        eq_("ANBOCO", metadata.publisher)
        eq_(2013, metadata.published.year)
        eq_(02, metadata.published.month)
        eq_(02, metadata.published.day)
        eq_(2017, metadata.data_source_last_updated.year)
        eq_(03, metadata.data_source_last_updated.month)
        eq_(10, metadata.data_source_last_updated.day)
        # Related IDs.
        eq_((Identifier.ODILO_ID, '00010982'),
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))
        ids = [(x.type, x.identifier) for x in metadata.identifiers]
        eq_(
            [
                (Identifier.ISBN, '9783736418837'),
                (Identifier.ODILO_ID, '00010982')
            ],
            sorted(ids)
        )

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)
        eq_([(u'Children', 'tag', 100),
             (u'Classics', 'tag', 100),
             (u'FIC004000', 'BISAC', 100),
             (u'Fantasy', 'tag', 100),
             (u'K-12', 'Grade level', 10),
             (u'LIT009000', 'BISAC', 100),
             (u'YAF019020', 'BISAC', 100)],
            [(x.identifier, x.type, x.weight) for x in subjects]
            )

        [author] = metadata.contributors
        eq_("Veale, E.", author.sort_name)
        eq_("E. Veale", author.display_name)
        eq_([Contributor.AUTHOR_ROLE], author.roles)

        # Available formats.
        [acsm_epub, ebook_streaming] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(Representation.EPUB_MEDIA_TYPE, acsm_epub.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, acsm_epub.drm_scheme)

        eq_(Representation.TEXT_HTML_MEDIA_TYPE, ebook_streaming.content_type)
        eq_(DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, ebook_streaming.drm_scheme)

        # Links to various resources.
        image, thumbnail, description = sorted(metadata.links, key=lambda x: x.rel)

        eq_(Hyperlink.IMAGE, image.rel)
        eq_(
            'http://pruebasotk.odilotk.es/public/OdiloPlace_eduDistUS/pg54159.jpg',
            image.href)

        eq_(Hyperlink.THUMBNAIL_IMAGE, thumbnail.rel)
        eq_(
            'http://pruebasotk.odilotk.es/public/OdiloPlace_eduDistUS/pg54159_225x318.jpg',
            thumbnail.href)

        eq_(Hyperlink.DESCRIPTION, description.rel)
        assert description.content.startswith(
            "All the <b>Brownies</b> had promised to help, and when a Brownie undertakes a thing he works as busily")

        circulation = metadata.circulation
        eq_(2, circulation.licenses_owned)
        eq_(1, circulation.licenses_available)
        eq_(2, circulation.patrons_in_hold_queue)
        eq_(1, circulation.licenses_reserved)

        self.api.log.info('Testing book info with metadata finished ok !!')

    def test_default_language_spanish(self):
        """Since Odilo primarily distributes Spanish-language titles, if a
        title comes in with no specified language, we assume it's
        Spanish.
        """
        raw, book_json = self.sample_json("odilo_metadata.json")
        raw, availability = self.sample_json("odilo_availability.json")
        del book_json['language']
        metadata, active = OdiloRepresentationExtractor.record_info_to_metadata(book_json, availability)
        eq_('spa', metadata.language)
