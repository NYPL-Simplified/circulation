from nose.tools import (
    set_trace,
    eq_,
)
import json
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import base64
import os
from . import (
    DatabaseTest
)
from core.testing import MockRequestsResponse
from core.util.problem_detail import ProblemDetail
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
)
from api.adobe_vendor_id import AuthdataUtility
from api.problem_details import *
from api.registry import (
    RemoteRegistry,
    Registration,
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
        eq_(set([reg.status_field, reg.stage_field]),
            set(settings))
        eq_(Registration.FAILURE_STATUS, reg.status_field.value)
        eq_(Registration.TESTING_STAGE, reg.stage_field.value)

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
        """Test the other methods orchestrated by the push() method.
        """

        class Mock(Registration):

            def _extract_catalog_information(self, response):
                self.initial_catalog_response = response
                return "register_url", "vendor_id"

            def _set_public_key(self, key):
                self._set_public_key_called_with = key
                return "an encryptor"
            
            def _create_registration_payload(self, url_for, stage):
                self.payload_ingredients = (url_for, stage)
                return dict(payload="this is it")

            def _send_registration_request(
                    self, register_url, payload, do_post
            ):
                self._send_registration_request_called_with = (
                    register_url, payload, do_post
                )
                return MockRequestsResponse(
                    200, content=json.dumps("you did it!")
                )

            def _process_registration_result(self, catalog, encryptor, stage):
                self._process_registration_result_called_with = (
                    catalog, encryptor, stage
                )
                return "all done!"

            def mock_do_get(self, url):
                self.do_get_called_with = url
                return "A fake catalog"

        # First of all, test success.
        registration = Mock(self.registry, self._default_library)
        stage = Registration.TESTING_STAGE
        url_for = object()
        catalog_url = "http://catalog/"
        do_post = object()
        key = object()
        result = registration.push(
            stage, url_for, catalog_url, registration.mock_do_get, do_post, key
        )

        # Ultimately the push succeeded.
        eq_("all done!", result)

        # But there were many steps towards this result.

        # First, do_get was called on the catalog URL.
        eq_(catalog_url, registration.do_get_called_with)

        # Then, the catalog was passed into _extract_catalog_information.
        eq_("A fake catalog", registration.initial_catalog_response)

        # _extract_catalog_information returned a registration URL and
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

        # _set_public_key() was called to create an encryptor object.
        # It returned an encryptor (here mocked as the string "an encryptor")
        # to be used later.
        eq_(key, registration._set_public_key_called_with)

        # _create_registration_payload was called to create the body
        # of the registration request.
        eq_((url_for, stage), registration.payload_ingredients)

        # Then _send_registration_request was called, POSTing the
        # payload to "register_url", the registration URL we got earlier.
        results = registration._send_registration_request_called_with
        eq_(("register_url", dict(payload="this is it"), do_post), results)

        # Finally, the return value of that method was loaded as JSON
        # and passed into _process_registration_result, along with
        # the encryptor obtained from _set_public_key()
        results = registration._process_registration_result_called_with
        eq_(("you did it!", "an encryptor", stage), results)

        # If a nonexistent stage is provided a ProblemDetail is the result.
        result = registration.push(
            "no such stage", url_for, catalog_url, registration.mock_do_get,
            do_post, key
        )
        eq_(INVALID_INPUT.uri, result.uri)
        eq_('"no such stage" is not a valid registration stage',
            result.detail)

        # Now in reverse order, let's replace the mocked methods so
        # that they return ProblemDetail documents. This tests that if
        # there is a failure at any stage, the ProblemDetail is
        # propagated.
        def cause_problem():
            """Try the same method call that worked before; it won't work
            anymore.
            """
            return registration.push(
                stage, url_for, catalog_url, registration.mock_do_get, do_post,
                key
            )

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
            return INVALID_REGISTRATION.detailed(
                "could not set public key"
            )
        registration._set_public_key = fail
        problem = cause_problem()
        eq_("could not set public key", problem.detail)

        def fail(*args, **kwargs):
            return INVALID_REGISTRATION.detailed(
                "could not extract catalog information"
            )
        registration._extract_catalog_information = fail
        problem = cause_problem()
        eq_("could not extract catalog information", problem.detail)

        # Lastly, test some other 

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
