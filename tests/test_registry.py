from nose.tools import (
    assert_raises_regexp,
    set_trace,
    eq_,
)
import json
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import os
from . import (
    DatabaseTest
)
from core.testing import (
    DummyHTTPClient,
    MockRequestsResponse,
)
from core.util.http import HTTP
from core.util.problem_detail import (
    ProblemDetail,
    JSON_MEDIA_TYPE as PROBLEM_DETAIL_JSON_MEDIA_TYPE,
)
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
)
from core.util.string_helpers import base64
from api.adobe_vendor_id import AuthdataUtility
from api.config import Configuration
from api.problem_details import *
from api.registry import (
    RemoteRegistry,
    Registration,
    LibraryRegistrationScript,
)

class TestRemoteRegistry(DatabaseTest):

    def setup(self):
        super(TestRemoteRegistry, self).setup()

        # Create an ExternalIntegration that can be used as the basis for
        # a RemoteRegistry.
        self.integration = self._external_integration(
            protocol="some protocol", goal=ExternalIntegration.DISCOVERY_GOAL
        )

    def test_constructor(self):
        registry = RemoteRegistry(self.integration)
        eq_(self.integration, registry.integration)

    def test_for_integration_id(self):
        """Test the ability to build a Registry for an ExternalIntegration
        given its ID.
        """
        m = RemoteRegistry.for_integration_id

        registry = m(
            self._db, self.integration.id, ExternalIntegration.DISCOVERY_GOAL
        )
        assert isinstance(registry, RemoteRegistry)
        eq_(self.integration, registry.integration)

        # If the ID doesn't exist you get None.
        eq_(None, m(self._db, -1, ExternalIntegration.DISCOVERY_GOAL))

        # If the integration's goal doesn't match what you provided,
        # you get None.
        eq_(None, m(self._db, self.integration.id, "some other goal"))

    def test_for_protocol_and_goal(self):

        # Create two ExternalIntegrations that have different protocols
        # or goals from our original.
        same_goal_different_protocol = self._external_integration(
            protocol="some other protocol", goal=self.integration.goal
        )

        same_protocol_different_goal = self._external_integration(
            protocol=self.integration.protocol, goal="some other goal"
        )

        # Only the original ExternalIntegration has both the requested
        # protocol and goal, so only it becomes a RemoteRegistry.
        [registry] = list(
            RemoteRegistry.for_protocol_and_goal(
                self._db, self.integration.protocol, self.integration.goal
            )
        )
        assert isinstance(registry, RemoteRegistry)
        eq_(self.integration, registry.integration)

    def test_for_protocol_goal_and_url(self):
        protocol = self._str
        goal = self._str
        url = self._url
        m = RemoteRegistry.for_protocol_goal_and_url

        registry = m(self._db, protocol, goal, url)
        assert isinstance(registry, RemoteRegistry)

        # A new ExternalIntegration was created.
        integration = registry.integration
        eq_(protocol, integration.protocol)
        eq_(goal, integration.goal)
        eq_(url, integration.url)

        # Calling the method again doesn't create a second
        # ExternalIntegration.
        registry2 = m(self._db, protocol, goal, url)
        eq_(registry2.integration, integration)

    def test_registrations(self):
        registry = RemoteRegistry(self.integration)

        # Associate the default library with the registry.
        Registration(registry, self._default_library)

        # Create another library not associated with the registry.
        library2 = self._library()

        # registrations() finds a single Registration.
        [registration] = list(registry.registrations)
        assert isinstance(registration, Registration)
        eq_(registry, registration.registry)
        eq_(self._default_library, registration.library)

    def test_fetch_catalog(self):
        # Test our ability to retrieve essential information from a
        # remote registry's root catalog.
        class Mock(RemoteRegistry):
            def _extract_catalog_information(self, response):
                self.extracted_from = response
                return "Essential information"

        # The behavior of fetch_catalog() depends on what comes back
        # when we ask the remote registry for its root catalog.
        client = DummyHTTPClient()

        # If the result is a problem detail document, that document is
        # the return value of fetch_catalog().
        problem = REMOTE_INTEGRATION_FAILED.detailed("oops")
        client.responses.append(problem)
        registry = Mock(self.integration)
        result = registry.fetch_catalog(do_get=client.do_get)
        eq_(self.integration.url, client.requests.pop())
        eq_(problem, result)

        # If the response looks good, it's passed into
        # _extract_catalog_information(), and the result of _that_
        # method is the return value of fetch_catalog.
        client.queue_requests_response(200, content="A root catalog")
        [queued] = client.responses
        eq_("Essential information",
            registry.fetch_catalog("custom catalog URL", do_get=client.do_get))
        eq_("custom catalog URL", client.requests.pop())

    def test__extract_catalog_information(self):
        # Test our ability to extract a registration link and an
        # Adobe Vendor ID from an OPDS 1 or OPDS 2 catalog.
        def extract(document, type=RemoteRegistry.OPDS_2_TYPE):
            response = MockRequestsResponse(
                200, { "Content-Type" : type }, document
            )
            return RemoteRegistry._extract_catalog_information(response)

        def assert_no_link(*args, **kwargs):
            """Verify that calling _extract_catalog_information on the
            given feed fails because there is no link with rel="register"
            """
            result = extract(*args, **kwargs)
            eq_(REMOTE_INTEGRATION_FAILED.uri, result.uri)
            eq_("The service at http://url/ did not provide a register link.",
                result.detail)

        # OPDS 2 feed with link and Adobe Vendor ID.
        link = { 'rel': 'register', 'href': 'register url' }
        metadata = { 'adobe_vendor_id': 'vendorid' }
        feed = json.dumps(dict(links=[link], metadata=metadata))
        eq_(("register url", "vendorid"), extract(feed))

        # OPDS 2 feed with link and no Adobe Vendor ID
        feed = json.dumps(dict(links=[link]))
        eq_(("register url", None), extract(feed))

        # OPDS 2 feed with no link.
        feed = json.dumps(dict(metadata=metadata))
        assert_no_link(feed)

        # OPDS 1 feed with link.
        feed = '<feed><link rel="register" href="register url"/></feed>'
        eq_(("register url", None),
            extract(feed, RemoteRegistry.OPDS_1_PREFIX + ";foo"))

        # OPDS 1 feed with no link.
        feed = '<feed></feed>'
        assert_no_link(feed, RemoteRegistry.OPDS_1_PREFIX + ";foo")

        # Non-OPDS document.
        result = extract("plain text here", "text/plain")
        eq_(REMOTE_INTEGRATION_FAILED.uri, result.uri)
        eq_("The service at http://url/ did not return OPDS.",
            result.detail)

    def test_fetch_registration_document(self):
        # Test our ability to retrieve terms-of-service information
        # from a remote registry, assuming the registry makes that
        # information available.

        # First, test the case where we can't even get the catalog
        # document.
        class Mock(RemoteRegistry):
            def fetch_catalog(self, do_get):
                self.fetch_catalog_called_with = do_get
                return REMOTE_INTEGRATION_FAILED

        registry = Mock(object())
        result = registry.fetch_registration_document()

        # Our mock fetch_catalog was called with a method that would
        # have made a real HTTP request.
        eq_(HTTP.debuggable_get, registry.fetch_catalog_called_with)

        # But the fetch_catalog method returned a problem detail,
        # which became the return value of
        # fetch_registration_document.
        eq_(REMOTE_INTEGRATION_FAILED, result)

        # Test the case where we get the catalog document but we can't
        # get the registration document.
        client = DummyHTTPClient()
        client.responses.append(REMOTE_INTEGRATION_FAILED)
        class Mock(RemoteRegistry):
            def fetch_catalog(self, do_get):
                return "http://register-here/", "vendor id"

            def _extract_registration_information(self, response):
                self._extract_registration_information_called_with = response
                return "TOS link", "TOS HTML data"

        registry = Mock(object())
        result = registry.fetch_registration_document(client.do_get)
        # A request was made to the registration URL mentioned in the catalog.
        eq_("http://register-here/", client.requests.pop())
        eq_([], client.requests)

        # But the request returned a problem detail, which became the
        # return value of the method.
        eq_(REMOTE_INTEGRATION_FAILED, result)

        # Finally, test the case where we can get both documents.

        client.queue_requests_response(200, content="a registration document")
        result = registry.fetch_registration_document(client.do_get)

        # Another request was made to the registration URL.
        eq_("http://register-here/", client.requests.pop())
        eq_([], client.requests)

        # Our mock of _extract_registration_information was called
        # with the mock response to that request.
        response = registry._extract_registration_information_called_with
        eq_("a registration document", response.content)

        # The return value of _extract_registration_information was
        # propagated as the return value of
        # fetch_registration_document.
        eq_(("TOS link", "TOS HTML data"), result)

    def test__extract_registration_information(self):
        # Test our ability to extract terms-of-service information --
        # a link and/or some HTML or textual instructions -- from a
        # registration document.

        def data_link(data, type="text/html"):
            encoded = base64.b64encode(data)
            return dict(
                rel="terms-of-service",
                href="data:%s;base64,%s" % (type, encoded)
            )

        class Mock(RemoteRegistry):
            @classmethod
            def _decode_data_url(cls, url):
                cls.decoded = url
                return "Decoded: " + RemoteRegistry._decode_data_url(url)

        def extract(document, type=RemoteRegistry.OPDS_2_TYPE):
            if type == RemoteRegistry.OPDS_2_TYPE:
                document = json.dumps(dict(links=document))
            response = MockRequestsResponse(
                200, { "Content-Type" : type }, document
            )
            return Mock._extract_registration_information(response)

        # OPDS 2 feed with TOS in http: and data: links.
        tos_link = dict(rel='terms-of-service', href='http://tos/')
        tos_data = data_link("<p>Some HTML</p>")
        eq_(("http://tos/", "Decoded: <p>Some HTML</p>"),
            extract([tos_link, tos_data]))

        # At this point it's clear that the data: URL found in
        # `tos_data` was run through `_decode_data()`. This gives us
        # permission to test all the fiddly bits of `_decode_data` in
        # isolation, below.
        eq_(tos_data['href'], Mock.decoded)

        # OPDS 2 feed with http: link only.
        eq_(("http://tos/", None), extract([tos_link]))

        # OPDS 2 feed with data: link only.
        eq_((None, "Decoded: <p>Some HTML</p>"), extract([tos_data]))

        # OPDS 2 feed with no links.
        eq_((None, None), extract([]))

        # OPDS 1 feed with link.
        feed = '<feed><link rel="terms-of-service" href="http://tos/"/></feed>'
        eq_(("http://tos/", None),
            extract(feed, RemoteRegistry.OPDS_1_PREFIX + ";foo"))

        # OPDS 1 feed with no link.
        feed = '<feed></feed>'
        eq_((None, None), extract(feed, RemoteRegistry.OPDS_1_PREFIX + ";foo"))

        # Non-OPDS document.
        eq_((None, None), extract("plain text here", "text/plain"))

        # Unrecognized URI schemes are ignored.
        ftp_link = dict(rel='terms-of-service', href='ftp://tos/')
        eq_((None, None), extract([ftp_link]))

    def test__decode_data_url(self):
        # Test edge cases of decoding data: URLs.
        m = RemoteRegistry._decode_data_url

        def data_url(data, type="text/html"):
            encoded = base64.b64encode(data)
            return "data:%s;base64,%s" % (type, encoded)

        # HTML is okay.
        html = data_url("some <strong>HTML</strong>", "text/html;charset=utf-8")
        eq_("some <strong>HTML</strong>", m(html))

        # Plain text is okay.
        text = data_url("some plain text", "text/plain")
        eq_("some plain text", m(text))

        # No other media type is allowed.
        image = data_url("an image!", "image/png")
        assert_raises_regexp(
            ValueError, "Unsupported media type in data: URL: image/png",
            m, image
        )

        # Incoming HTML is sanitized.
        dirty_html = data_url("<script>alert!</script><p>Some HTML</p>")
        eq_("<p>Some HTML</p>", m(dirty_html))

        # Now test various malformed data: URLs.
        no_header = "foobar"
        assert_raises_regexp(
            ValueError, "Not a data: URL: foobar",
            m, no_header
        )

        no_comma = "data:blah"
        assert_raises_regexp(
            ValueError, "Invalid data: URL: data:blah",
            m, no_comma
        )

        too_many_commas = "data:blah,blah,blah"
        assert_raises_regexp(
            ValueError, "Invalid data: URL: data:blah,blah,blah",
            m, too_many_commas
        )

        # data: URLs don't have to be base64-encoded, but those are the
        # only kind we support.
        not_encoded = "data:blah,content"
        assert_raises_regexp(
            ValueError, "data: URL not base64-encoded: data:blah,content",
            m, not_encoded
        )


class TestRegistration(DatabaseTest):

    def setup(self):
        super(TestRegistration, self).setup()

        # Create a RemoteRegistry.
        self.integration = self._external_integration(
            protocol="some protocol", goal="some goal"
        )
        self.registry = RemoteRegistry(self.integration)
        self.registration = Registration(self.registry, self._default_library)

    def test_constructor(self):
        # The Registration constructor was called during setup to create
        # self.registration.
        reg = self.registration
        eq_(self.registry, reg.registry)
        eq_(self._default_library, reg.library)

        settings = [x for x in reg.integration.settings
                    if x.library is not None]
        eq_(set([reg.status_field, reg.stage_field, reg.web_client_field]),
            set(settings))
        eq_(Registration.FAILURE_STATUS, reg.status_field.value)
        eq_(Registration.TESTING_STAGE, reg.stage_field.value)
        eq_(None, reg.web_client_field.value)

        # The Library has been associated with the ExternalIntegration.
        eq_([self._default_library], self.integration.libraries)

        # Creating another Registration doesn't add the library to the
        # ExternalIntegration again or override existing values for the
        # settings.
        reg.status_field.value = "new status"
        reg.stage_field.value = "new stage"
        reg2 = Registration(self.registry, self._default_library)
        eq_([self._default_library], self.integration.libraries)
        eq_("new status", reg2.status_field.value)
        eq_("new stage", reg2.stage_field.value)

    def test_setting(self):
        m = self.registration.setting

        def _find(key):
            """Find a ConfigurationSetting associated with the library.

            This is necessary because ConfigurationSetting.value
            creates _two_ ConfigurationSettings, one associated with
            the library and one not associated with any library, to
            store the default value.
            """
            values = [
                x for x in self.registration.integration.settings
                if x.library and x.key==key
            ]
            if len(values) == 1:
                return values[0]
            return None

        # Calling setting() creates a ConfigurationSetting object
        # associated with the library.
        setting = m("key")
        eq_("key", setting.key)
        eq_(None, setting.value)
        eq_(self._default_library, setting.library)
        eq_(setting, _find("key"))

        # You can specify a default value, which is used only if the
        # current value is None.
        setting2 = m("key", "default")
        eq_(setting, setting2)
        eq_("default", setting.value)

        setting3 = m("key", "default2")
        eq_(setting, setting3)
        eq_("default", setting.value)

    def test_push(self):
        # Test the other methods orchestrated by the push() method.

        class MockRegistry(RemoteRegistry):

            def fetch_catalog(self, catalog_url, do_get):
                # Pretend to fetch a root catalog and extract a
                # registration URL from it.
                self.fetch_catalog_called_with = (catalog_url, do_get)
                return "register_url", "vendor_id"

        class MockRegistration(Registration):

            def _create_registration_payload(self, url_for, stage):
                self.payload_ingredients = (url_for, stage)
                return dict(payload="this is it")

            def _create_registration_headers(self):
                self._create_registration_headers_called = True
                return dict(Header="Value")

            def _send_registration_request(
                    self, register_url, headers, payload, do_post
            ):
                self._send_registration_request_called_with = (
                    register_url, headers, payload, do_post
                )
                return MockRequestsResponse(
                    200, content=json.dumps("you did it!")
                )

            def _process_registration_result(self, catalog, encryptor, stage):
                self._process_registration_result_called_with = (
                    catalog, encryptor, stage
                )
                return "all done!"

        # If there is no preexisting key pair set up for the library,
        # registration fails. (This normally won't happen because the
        # key pair is set up when the LibraryAuthenticator is
        # initialized.)
        library = self._default_library
        registry = MockRegistry(self.integration)
        registration = MockRegistration(registry, library)
        stage = Registration.TESTING_STAGE
        url_for = object()
        catalog_url = "http://catalog/"
        do_get = object()
        do_post = object()
        def push():
            return registration.push(
                stage, url_for, catalog_url, do_get, do_post
            )

        result = push()
        expect = "Library %s has no key pair set." % library.short_name
        eq_(expect, result.detail)

        # When a key pair is present, registration is kicked off, and
        # in this case it succeeds.
        key_pair_setting = ConfigurationSetting.for_library(
            Configuration.KEY_PAIR, library
        )
        public_key, private_key = Configuration.key_pair(key_pair_setting)
        result = push()
        eq_("all done!", result)

        # But there were many steps towards this result.

        # First, MockRegistry.fetch_catalog() was called, in an attempt
        # to find the registration URL inside the root catalog.
        eq_((catalog_url, do_get), registry.fetch_catalog_called_with)

        # fetch_catalog() returned a registration URL and
        # a vendor ID. The registration URL was used later on...
        #
        # The vendor ID was set as a ConfigurationSetting on
        # the ExternalIntegration associated with this registry.
        eq_(
            "vendor_id",
            ConfigurationSetting.for_externalintegration(
                AuthdataUtility.VENDOR_ID_KEY, self.integration
            ).value
        )

        # _create_registration_payload was called to create the body
        # of the registration request.
        eq_((url_for, stage), registration.payload_ingredients)

        # _create_registration_headers was called to create the headers
        # sent along with the request.
        eq_(True, registration._create_registration_headers_called)

        # Then _send_registration_request was called, POSTing the
        # payload to "register_url", the registration URL we got earlier.
        results = registration._send_registration_request_called_with
        eq_(
            ("register_url", {"Header": "Value"}, dict(payload="this is it"),
             do_post),
            results
        )

        # Finally, the return value of that method was loaded as JSON
        # and passed into _process_registration_result, along with
        # a cipher created from the private key. (That cipher would be used
        # to decrypt anything the foreign site signed using this site's
        # public key.)
        results = registration._process_registration_result_called_with
        message, cipher, actual_stage = results
        eq_("you did it!", message)
        eq_(cipher._key.exportKey().decode("utf-8"), private_key)
        eq_(actual_stage, stage)

        # If a nonexistent stage is provided a ProblemDetail is the result.
        result = registration.push(
            "no such stage", url_for, catalog_url, do_get, do_post
        )
        eq_(INVALID_INPUT.uri, result.uri)
        eq_("'no such stage' is not a valid registration stage",
            result.detail)

        # Now in reverse order, let's replace the mocked methods so
        # that they return ProblemDetail documents. This tests that if
        # there is a failure at any stage, the ProblemDetail is
        # propagated.

        # The push() function will no longer push anything, so rename it.
        cause_problem = push

        def fail(*args, **kwargs):
            return INVALID_REGISTRATION.detailed(
                "could not process registration result"
            )
        registration._process_registration_result = fail
        problem = cause_problem()
        eq_("could not process registration result", problem.detail)

        def fail(*args, **kwargs):
            return INVALID_REGISTRATION.detailed(
                "could not send registration request"
            )
        registration._send_registration_request = fail
        problem = cause_problem()
        eq_("could not send registration request", problem.detail)

        def fail(*args, **kwargs):
            return INVALID_REGISTRATION.detailed(
                "could not create registration payload"
            )
        registration._create_registration_payload = fail
        problem = cause_problem()
        eq_("could not create registration payload", problem.detail)

        def fail(*args, **kwargs):
            return INVALID_REGISTRATION.detailed("could not fetch catalog")
        registry.fetch_catalog = fail
        problem = cause_problem()
        eq_("could not fetch catalog", problem.detail)

    def test__create_registration_payload(self):
        m = self.registration._create_registration_payload

        # Mock url_for to create good-looking callback URLs.
        def url_for(controller, library_short_name):
            return "http://server/%s/%s" % (library_short_name, controller)

        # First, test with no configuration contact configured for the
        # library.
        stage = object()
        expect_url = url_for(
            "authentication_document", self.registration.library.short_name
        )
        expect_payload = dict(url=expect_url, stage=stage)
        eq_(expect_payload, m(url_for, stage))

        # If a contact is configured, it shows up in the payload.
        contact = "mailto:ohno@library.org"
        ConfigurationSetting.for_library(
            Configuration.CONFIGURATION_CONTACT_EMAIL,
            self.registration.library,
        ).value=contact
        expect_payload['contact'] = contact
        eq_(expect_payload, m(url_for, stage))

    def test_create_registration_headers(self):
        m = self.registration._create_registration_headers
        # If no shared secret is configured, no custom headers are provided.
        expect_headers = {}
        eq_(expect_headers, m())

        # If a shared secret is configured, it shows up as part of
        # the Authorization header.
        setting = ConfigurationSetting.for_library_and_externalintegration(
            self._db, ExternalIntegration.PASSWORD, self.registration.library,
            self.registration.registry.integration
        ).value="a secret"
        expect_headers['Authorization'] = 'Bearer a secret'
        eq_(expect_headers, m())

    def test__send_registration_request(self):
        class Mock(object):
            def __init__(self, response):
                self.response = response

            def do_post(self, url, payload, **kwargs):
                self.called_with = (url, payload, kwargs)
                return self.response

        # If everything goes well, the return value of do_post is
        # passed through.
        mock = Mock(MockRequestsResponse(200, content="all good"))
        url = "url"
        payload = "payload"
        headers = "headers"
        m = Registration._send_registration_request
        result = m(url, headers, payload, mock.do_post)
        eq_(mock.response, result)
        called_with = mock.called_with
        eq_(called_with,
            (url, payload,
             dict(
                 headers=headers,
                 timeout=60,
                 allowed_response_codes=["2xx", "3xx", "400", "401"]
             )
            )
        )

        # Most error handling is expected to be handled by do_post
        # raising an exception, but certain responses get special
        # treatment:

        # The remote sends a 401 response with a problem detail.
        mock = Mock(
            MockRequestsResponse(
                401, { "Content-Type": PROBLEM_DETAIL_JSON_MEDIA_TYPE },
                content=json.dumps(dict(detail="this is a problem detail"))
            )
        )
        result = m(url, headers, payload, mock.do_post)
        assert isinstance(result, ProblemDetail)
        eq_(REMOTE_INTEGRATION_FAILED.uri, result.uri)
        eq_('Remote service returned: "this is a problem detail"',
            result.detail)

        # The remote sends some other kind of 401 response.
        mock = Mock(
            MockRequestsResponse(
                401, { "Content-Type": "text/html" },
                content="log in why don't you"
            )
        )
        result = m(url, headers, payload, mock.do_post)
        assert isinstance(result, ProblemDetail)
        eq_(REMOTE_INTEGRATION_FAILED.uri, result.uri)
        eq_('Remote service returned: "log in why don\'t you"', result.detail)

    def test__decrypt_shared_secret(self):
        key = RSA.generate(2048)
        encryptor = PKCS1_OAEP.new(key)

        key2 = RSA.generate(2048)
        encryptor2 = PKCS1_OAEP.new(key2)

        shared_secret = os.urandom(24).encode('hex')
        encrypted_secret = base64.b64encode(encryptor.encrypt(shared_secret))

        # Success.
        m = Registration._decrypt_shared_secret
        eq_(shared_secret, m(encryptor, encrypted_secret))

        # If we try to decrypt using the wrong key, a ProblemDetail is
        # returned explaining the problem.
        problem = m(encryptor2, encrypted_secret)
        assert isinstance(problem, ProblemDetail)
        eq_(SHARED_SECRET_DECRYPTION_ERROR.uri, problem.uri)
        assert encrypted_secret in problem.detail

    def test__process_registration_result(self):
        reg = self.registration
        m = reg._process_registration_result

        # Result must be a dictionary.
        result = m("not a dictionary", None, None)
        eq_(INTEGRATION_ERROR.uri, result.uri)
        eq_("Remote service served 'not a dictionary', which I can't make sense of as an OPDS document.", result.detail)

        # When the result is empty, the registration is marked as successful.
        new_stage = "new stage"
        encryptor = object()
        result = m(dict(), encryptor, new_stage)
        eq_(True, result)
        eq_(reg.SUCCESS_STATUS, reg.status_field.value)

        # The stage field has been set to the requested value.
        eq_(new_stage, reg.stage_field.value)

        # Now try with a result that includes a short name,
        # a shared secret, and a web client URL.

        class Mock(Registration):
            def _decrypt_shared_secret(self, encryptor, shared_secret):
                self._decrypt_shared_secret_called_with = (encryptor, shared_secret)
                return "cleartext"

        reg = Mock(self.registry, self._default_library)
        catalog = dict(
            metadata=dict(short_name="SHORT", shared_secret="ciphertext", id="uuid"),
            links=[dict(href="http://web/library", rel="self", type="text/html")],
        )
        result = reg._process_registration_result(
            catalog, encryptor, "another new stage"
        )
        eq_(True, result)

        # Short name is set.
        eq_("SHORT", reg.setting(ExternalIntegration.USERNAME).value)

        # Shared secret was decrypted and is set.
        eq_((encryptor, "ciphertext"), reg._decrypt_shared_secret_called_with)
        eq_("cleartext", reg.setting(ExternalIntegration.PASSWORD).value)

        # Web client URL is set.
        eq_("http://web/library", reg.setting(reg.LIBRARY_REGISTRATION_WEB_CLIENT).value)

        eq_("another new stage", reg.stage_field.value)

        # Now simulate a problem decrypting the shared secret.
        class Mock(Registration):
            def _decrypt_shared_secret(self, encryptor, shared_secret):
                return SHARED_SECRET_DECRYPTION_ERROR
        reg = Mock(self.registry, self._default_library)
        result = reg._process_registration_result(
            catalog, encryptor, "another new stage"
        )
        eq_(SHARED_SECRET_DECRYPTION_ERROR, result)


class TestLibraryRegistrationScript(DatabaseTest):

    def setup(self):
        """Make sure there's a base URL for url_for to use."""
        super(TestLibraryRegistrationScript, self).setup()

    def test_do_run(self):

        class Mock(LibraryRegistrationScript):
            processed = []
            def process_library(self, *args):
                self.processed.append(args)

        script = Mock(self._db)

        base_url_setting = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        )
        base_url_setting.value = 'http://test-circulation-manager/'

        library = self._default_library
        library2 = self._library()

        cmd_args = [library.short_name, "--stage=testing",
                    "--registry-url=http://registry/"]
        app = script.do_run(cmd_args=cmd_args, in_unit_test=True)

        # One library was processed.
        (registration, stage, url_for) = script.processed.pop()
        eq_([], script.processed)
        eq_(library, registration.library)
        eq_(Registration.TESTING_STAGE, stage)

        # A new ExternalIntegration was created for the newly defined
        # registry at http://registry/.
        eq_("http://registry/", registration.integration.url)

        # An application environment was created and the url_for
        # implementation for that environment was passed into
        # process_library.
        eq_(url_for, app.manager.url_for)

        # Let's say the other library was earlier registered in production.
        registration_2 = Registration(registration.registry, library2)
        registration_2.stage_field.value = Registration.PRODUCTION_STAGE

        # Now run the script again without specifying a particular
        # library or the --stage argument.
        app = script.do_run(cmd_args=[], in_unit_test=True)

        # Every library was processed.
        eq_(set([library, library2]),
            set([x[0].library for x in script.processed]))

        for i in script.processed:
            # Since no stage was provided, each library was registered
            # using the stage already associated with it.
            eq_(i[0].stage_field.value, i[1])

            # Every library was registered with the default
            # library registry.
            eq_(
                RemoteRegistry.DEFAULT_LIBRARY_REGISTRY_URL,
                i[0].integration.url
            )

    def test_process_library(self):
        """Test the things that might happen when process_library is called."""
        script = LibraryRegistrationScript(self._db)
        library = self._default_library
        integration = self._external_integration(
            protocol="some protocol", goal=ExternalIntegration.DISCOVERY_GOAL
        )
        registry = RemoteRegistry(integration)

        # First, simulate success.
        class Success(Registration):
            def push(self, stage, url_for):
                self.pushed = (stage, url_for)
                return True
        registration = Success(registry, library)

        stage = object()
        url_for = object()
        eq_(True, script.process_library(registration, stage, url_for))

        # The stage and url_for values were passed into
        # Registration.push()
        eq_((stage, url_for), registration.pushed)

        # Next, simulate an exception raised during push()
        # This can happen in real situations, though the next case
        # we'll test is more common.
        class FailsWithException(Registration):
            def push(self, stage, url_for):
                raise Exception("boo")

        registration = FailsWithException(registry, library)
        # We get False rather than the exception being propagated.
        # Useful information about the exception is added to the logs,
        # where someone actually running the script will see it.
        eq_(False, script.process_library(registration, stage, url_for))

        # Next, simulate push() returning a problem detail document.
        class FailsWithProblemDetail(Registration):
            def push(self, stage, url_for):
                return INVALID_INPUT.detailed("oops")
        registration = FailsWithProblemDetail(registry, library)
        result = script.process_library(registration, stage, url_for)

        # The problem document is returned. Useful information about
        # the exception is also added to the logs, where someone
        # actually running the script will see it.
        eq_(INVALID_INPUT.uri, result.uri)
        eq_("oops", result.detail)

