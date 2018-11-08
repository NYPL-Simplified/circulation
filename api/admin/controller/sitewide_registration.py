from nose.tools import set_trace
import base64
from datetime import datetime, timedelta
from flask_babel import lazy_gettext as _
import jwt
from api.admin.problem_details import *
from api.config import Configuration
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail
from . import SettingsController

class SitewideRegistrationController(SettingsController):

    def process_sitewide_registration(self, integration, do_get=HTTP.debuggable_get,
                              do_post=HTTP.debuggable_post
    ):
        """Performs a sitewide registration for a particular service, currently
        only the Metadata Wrangler.

        :return: A ProblemDetail or, if successful, None
        """

        self.require_system_admin()

        if not integration:
            return MISSING_SERVICE

        catalog_response = self.get_catalog(do_get, integration.url)
        if isinstance(catalog_response, ProblemDetail):
            return catalog_response

        if isinstance(self.check_content_type(catalog_response), ProblemDetail):
            return self.check_content_type(catalog_response)

        catalog = catalog_response.json()
        links = catalog.get('links', [])

        register_url = self.get_registration_link(links)
        if isinstance(register_url, ProblemDetail):
            return register_url

        headers = self.update_headers(integration)
        if isinstance(headers, ProblemDetail):
            return headers

        response = self.register(register_url, headers, do_post)
        if isinstance(response, ProblemDetail):
            return response

        shared_secret = self.get_shared_secret(response)
        if isinstance(shared_secret, ProblemDetail):
            return shared_secret

        ignore, private_key = self.manager.sitewide_key_pair
        decryptor = Configuration.cipher(private_key)
        shared_secret = decryptor.decrypt(base64.b64decode(shared_secret))
        integration.password = unicode(shared_secret)

    def get_catalog(self, do_get, url):
        """Get the catalog for this service."""

        try:
            response = do_get(url)
        except Exception as e:
            return REMOTE_INTEGRATION_FAILED.detailed(e.message)

        if isinstance(response, ProblemDetail):
            return response
        return response

    def check_content_type(self, catalog_response):
        """Make sure the catalog for the service is in a valid format."""

        content_type = catalog_response.headers.get('Content-Type')
        if content_type != 'application/opds+json':
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide a valid catalog.')
            )

    def get_registration_link(self, links):
        """Get the link for registration from the catalog."""

        register_link_filter = lambda l: (
            l.get('rel')=='register' and
            l.get('type')==self.METADATA_SERVICE_URI_TYPE
        )

        register_urls = filter(register_link_filter, links)
        if not register_urls:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide a register link.')
            )

        # Get the full registration url.
        register_url = register_urls[0].get('href')
        if not register_url.startswith('http'):
            # We have a relative path. Create a full registration url.
            base_url = catalog.get('id')
            register_url = urlparse.urljoin(base_url, register_url)

        return register_url

    def update_headers(self, integration):
        """If the integration has an existing shared_secret, use it to access the
        server and update it."""

        # NOTE: This is no longer technically necessary since we prove
        # ownership with a signed JWT.
        headers = { 'Content-Type' : 'application/x-www-form-urlencoded' }
        if integration.password:
            token = base64.b64encode(integration.password.encode('utf-8'))
            headers['Authorization'] = 'Bearer ' + token

    def register(self, register_url, headers, do_post):
        """Register this server using the sitewide registration document."""

        try:
            body = self.sitewide_registration_document()
            response = do_post(
                register_url, body, allowed_response_codes=['2xx'],
                headers=headers
            )
        except Exception as e:
            return REMOTE_INTEGRATION_FAILED.detailed(e.message)
        return response

    def get_shared_secret(self, response):
        """Find the shared secret which we need to use in order to register this
        service, or return an error message if there is no shared secret."""

        registration_info = response.json()
        shared_secret = registration_info.get('metadata', {}).get('shared_secret')

        if not shared_secret:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide registration information.')
            )
        return shared_secret

    def sitewide_registration_document(self):
        """Generate the document to be sent as part of a sitewide registration
        request.

        :return: A dictionary with keys 'url' and 'jwt'. 'url' is the URL to
            this site's public key document, and 'jwt' is a JSON Web Token
            proving control over that URL.
        """

        public_key, private_key = self.manager.sitewide_key_pair
        # Advertise the public key so that the foreign site can encrypt
        # things for us.
        public_key_dict = dict(type='RSA', value=public_key)
        public_key_url = self.url_for('public_key_document')
        in_one_minute = datetime.utcnow() + timedelta(seconds=60)
        payload = {'exp': in_one_minute}
        # Sign a JWT with the private key to prove ownership of the site.
        token = jwt.encode(payload, private_key, algorithm='RS256')
        return dict(url=public_key_url, jwt=token)
