
import base64
import flask
import json
import jwt
import os
from werkzeug.datastructures import MultiDict
from api.admin.problem_details import *
from api.config import Configuration
from core.model import (
    ExternalIntegration,
)
from core.testing import MockRequestsResponse
from core.util.problem_detail import ProblemDetail
from .test_controller import SettingsControllerTest

class TestSitewideRegistration(SettingsControllerTest):

    def test_sitewide_registration_post_errors(self):
        def assert_remote_integration_error(response, message=None):
            assert REMOTE_INTEGRATION_FAILED.uri == response.uri
            assert REMOTE_INTEGRATION_FAILED.title == response.title
            if message:
                assert message in response.detail

        metadata_wrangler_service = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url
        )
        default_form = None
        controller = self.manager.admin_metadata_services_controller

        # If ExternalIntegration is given, a ProblemDetail is returned.
        with self.request_context_with_admin("/"):
            response = controller.process_sitewide_registration(
                None, do_get=self.do_request
            )
            assert MISSING_SERVICE == response

        # If an error is raised during registration, a ProblemDetail is returned.
        def error_get(*args, **kwargs):
            raise RuntimeError('Mock error during request')

        with self.request_context_with_admin("/"):
            response = controller.process_sitewide_registration(
                metadata_wrangler_service, do_get=error_get
            )
            assert_remote_integration_error(response)

        # # If the response has the wrong media type, a ProblemDetail is returned.
        self.responses.append(
            MockRequestsResponse(200, headers={'Content-Type' : 'text/plain'})
        )

        with self.request_context_with_admin("/"):
            response = controller.process_sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request
            )
            assert_remote_integration_error(
                response, 'The service did not provide a valid catalog.'
            )

        # If the response returns a ProblemDetail, its contents are wrapped
        # in another ProblemDetail.
        status_code, content, headers = MULTIPLE_BASIC_AUTH_SERVICES.response
        self.responses.append(
            MockRequestsResponse(content, headers, status_code)
        )
        with self.request_context_with_admin("/"):
            response = controller.process_sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request
            )
            assert isinstance(response, ProblemDetail)
            assert response.detail.startswith(
                "Remote service returned a problem detail document:"
            )
            assert str(MULTIPLE_BASIC_AUTH_SERVICES.detail) in response.detail

        # If no registration link is available, a ProblemDetail is returned
        catalog = dict(id=self._url, links=[])
        headers = { 'Content-Type' : 'application/opds+json' }
        self.responses.append(
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        )

        with self.request_context_with_admin("/"):
            response = controller.process_sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request
            )
            assert_remote_integration_error(
                response, 'The service did not provide a register link.'
            )

        # If no registration details are given, a ProblemDetail is returned
        link_type = self.manager.admin_settings_controller.METADATA_SERVICE_URI_TYPE
        catalog['links'] = [dict(rel='register', href=self._url, type=link_type)]
        registration = dict(id=self._url, metadata={})
        self.responses.extend([
            MockRequestsResponse(200, content=json.dumps(registration), headers=headers),
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        ])

        with self.request_context_with_admin('/', method='POST'):
            response = controller.process_sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request, do_post=self.do_request
            )
            assert_remote_integration_error(
                response, 'The service did not provide registration information.'
            )

        # If we get all the way to the registration POST, but that
        # request results in a ProblemDetail, that ProblemDetail is
        # passed along.
        self.responses.extend([
            MockRequestsResponse(200, content=json.dumps(registration), headers=headers),
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        ])

        def bad_do_post(self, *args, **kwargs):
            return MULTIPLE_BASIC_AUTH_SERVICES
        with self.request_context_with_admin('/', method='POST'):
            flask.request.form = MultiDict([
                ('integration_id', metadata_wrangler_service.id),
            ])
            response = controller.process_sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request, do_post=bad_do_post
            )
        assert MULTIPLE_BASIC_AUTH_SERVICES == response


    def test_sitewide_registration_post_success(self):
        # A service to register with
        metadata_wrangler_service = self._external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL, url=self._url
        )

        # The service knows this site's public key, and is going
        # to use it to encrypt a shared secret.
        public_key, private_key = self.manager.sitewide_key_pair
        encryptor = Configuration.cipher(public_key)

        # A catalog with registration url
        register_link_type = self.manager.admin_settings_controller.METADATA_SERVICE_URI_TYPE
        registration_url = self._url
        catalog = dict(
            id = metadata_wrangler_service.url,
            links = [
                dict(rel='collection-add', href=self._url, type='collection'),
                dict(rel='register', href=registration_url, type=register_link_type),
                dict(rel='collection-remove', href=self._url, type='collection'),
            ]
        )
        headers = { 'Content-Type' : 'application/opds+json' }
        self.responses.append(
            MockRequestsResponse(200, content=json.dumps(catalog), headers=headers)
        )

        # A registration document with an encrypted secret
        shared_secret = os.urandom(24).encode('hex')
        encrypted_secret = base64.b64encode(encryptor.encrypt(shared_secret))
        registration = dict(
            id = metadata_wrangler_service.url,
            metadata = dict(shared_secret=encrypted_secret)
        )
        self.responses.insert(0, MockRequestsResponse(200, content=json.dumps(registration)))

        with self.request_context_with_admin('/', method='POST'):
            flask.request.form = MultiDict([
                ('integration_id', metadata_wrangler_service.id),
            ])
            response = self.manager.admin_metadata_services_controller.process_sitewide_registration(
                metadata_wrangler_service, do_get=self.do_request,
                do_post=self.do_request
            )
        assert None == response

        # We made two requests: a GET to get the service document from
        # the metadata wrangler, and a POST to the registration
        # service, with the entity-body containing a callback URL and
        # a JWT.
        metadata_wrangler_service_request, registration_request = self.requests
        url, i1, i2 = metadata_wrangler_service_request
        assert metadata_wrangler_service.url == url

        url, [document], ignore = registration_request
        assert url == registration_url
        for k in 'url', 'jwt':
            assert k in document

        # The end result is that our ExternalIntegration for the metadata
        # wrangler has been updated with a (decrypted) shared secret.
        assert shared_secret == metadata_wrangler_service.password

    def test_sitewide_registration_document(self):
        """Test the document sent along to sitewide registration."""
        controller = self.manager.admin_metadata_services_controller
        with self.request_context_with_admin('/'):
            doc = controller.sitewide_registration_document()

            # The registrar knows where to go to get our public key.
            assert doc['url'] == controller.url_for('public_key_document')

            # The JWT proves that we control the public/private key pair.
            public_key, private_key = self.manager.sitewide_key_pair
            parsed = jwt.decode(
                doc['jwt'], public_key, algorithm='RS256'
            )

            # The JWT must be valid or jwt.decode() would have raised
            # an exception. This simply verifies that the JWT includes
            # an expiration date and doesn't last forever.
            assert 'exp' in parsed
