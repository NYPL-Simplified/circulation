from core.model import (
    get_one,
    get_one_or_create,
    ConfigurationSetting,
    ExternalIntegration,
)
from core.scripts import LibraryInputScript
from core.util.http import HTTP


class RemoteRegistry(object):
    """A circulation manager's view of a remote service that supports
    the OPDS Directory Registration Protocol:

    https://github.com/NYPL-Simplified/Simplified/wiki/OPDS-Directory-Registration-Protocol
    
    In practical terms, this may be a library registry (which has
    DISCOVERY_GOAL and wants to help patrons find their libraries) or
    it may be a metadata wrangler (which has METADATA_GOAL and wants
    libraries to tell it what's in their collections).
    """
    DEFAULT_LIBRARY_REGISTRY_URL = "https://libraryregistry.librarysimplified.org/"

    def __init__(self, integration):
        """Constructor."""
        self.integration = integration

    @classmethod
    def default(cls, _db, goal):
        """Find or create the default RemoteRegistry for the given goal.

        If any RemoteRegistry for this goal already exists, it will be
        returned.  Otherwise, a new RemoteRegistry will be created
        using the well-known URL for the goal.

        :return: A 2-tuple (RemoteRegistry, is_new)

        TODO: Not happy about this.
        """
        url, protocol = cls.DEFAULT_URL_AND_PROTOCOL_FOR_GOAL[goal]
        integration, is_new = get_one_or_create(
            _db, ExternalIntegration, protocol=protocol, goal=goal
        )
        set_trace()
        if is_new:
            integration.url = url
        return cls(integration), is_new

    @classmethod
    def for_integration_id(self, _db, integration_id, goal):
        """Create a LibraryRegistry object configured
        by the given ExternalIntegration ID.

        :param goal: Either DISCOVERY_GOAL or METADATA_GOAL
        """
        integration = get_one(_db, ExternalIntegration,
                              goal=goal,
                              id=integration_id)
        if not integration:
            return None
        return cls(integration)        

    @classmethod
    def for_goal(self, _db, goal):
        """Find all LibraryRegistry objects with the given goal."""
        for i in _db.query(ExternalIntegration).filter(
                ExternalIntegration.goal==goal
        ):
            yield cls(i)

    @property
    def registrations(self):
        """Find all of this site's successful registrations with
        this RemoteRegistry.

        :yield: A sequence of Registration objects.
        """
        for x in self.integration.libraries:
            yield Registration(self, x) 


class Registration(object):
    """A library's registration for a particular registry.

    The registration does not correspond to one specific data model
    object -- it's a relationship between a Library and an
    ExternalIntegration, and a set of ConfigurationSettings that
    configure the relationship between the two.
    """

    # A library may be succesfully registered with a registry, or the
    # registration may have failed.
    LIBRARY_REGISTRATION_STATUS = u"library-registration-status"
    SUCCESS_STATUS = u"success"
    FAILURE_STATUS = u"failure"

    # A library may be registered in a 'testing' stage or a
    # 'production' stage. This represents the _library's_ opinion
    # about whether it's ready for production. The library won't
    # actually show up in production feeds until the _registry_ also
    # thinks it should.
    #
    # TODO: Registration through the admin interface always happens in
    # 'production' because there is no UI for specifying which stage
    # to use.  When registration happens through a script, the admin gets
    # to specify 'testing' or 'production'.
    LIBRARY_REGISTRATION_STAGE = u"library-registration-stage"
    TESTING_STAGE = "testing"
    PRODUCTION_STAGE = "production"
    VALID_REGISTRATION_STAGES = [TESTING_STAGE, PRODUCTION_STAGE]

    def __init__(self, registry, library):
        self.registry = registry
        self.integration = self.registry.integration
        self.library = library

        if not library in self.integration.libraries:
            self.integration.libraries.append(library)

        # Find or create all the ConfigurationSettings that configure
        # this relationship between library and registry.
        # Has the registration succeeded? (Initial value: no.)
        self.status_field, is_new = self.setting(LIBRARY_REGISTRATION_STATUS)
        if is_new:
            self.status_field.value = self.FAILURE_STATUS

        # Does the library want to be in the testing or production stage?
        # (Initial value: testing.)
        self.stage_field, is_new = self.setting(LIBRARY_REGISTRATION_STAGE)
        if is_new:
            self.stage_field.value = self.TESTING_STAGE

    def setting(self, key):
        """Find or create a ConfigurationSetting that configures this
        relationship between library and registry.
        
        :param key: Name of the ConfigurationSetting.
        :return: A 2-tuple (ConfigurationSetting, is_new)
        """
        return ConfigurationSetting.for_library_and_externalintegration(
            _db, name, self.library, self.integration
        )

    def push(self, stage, url_for, do_get=HTTP.debuggable_get,
             do_post=HTTP.debuggable_post, key=None):
        """Attempt to register a library with a RemoteRegistry.

        NOTE: this method does a database commit (by calling
        _set_public_key) so that when the remote registry asks for the
        library's Authentication For OPDS document, the public key is
        found and included in that document.

        NOTE: This method is designed to be used in a
        controller. Other callers may use this method, but they must be
        able to render a ProblemDetail when there's a failure.
        
        NOTE: The application server must be running when this method
        is called, because part of the OPDS Directory Registration
        Protocol is the remote server retrieving the library's
        Authentication For OPDS document.

        :param stage: Either TESTING_STAGE or PRODUCTION_STAGE
        :param url_for: Flask url_for() or equivalent, used to generate URLs
            for the application server.
        :param do_get: Mockable method to make a GET request.
        :param do_post: Mockable method to make a POST request.
        :param key: Pass in an RsaKey object to use a specific public key
            rather than generating a new one.

        :return: A ProblemDetail if there was a problem; otherwise True.
        """
        if stage not in self.VALID_REGISTRATION_STAGES:
            return INVALID_INPUT.detailed(
                _("%r is not a valid registration stage") % stage
            ) 

        # Before we can start the registration protocol, we must fetch
        # the catalog URL and extract the URL to use when registering.
        catalog_url = self.integration.url
        response = do_get(catalog_url)
        if isinstance(response, ProblemDetail):
            return response

        result = self._extract_catalog_information(response)
        if isinstance(result, ProblemDetail):
            return result
        register_url, vendor_id = result

        # Store the vendor id as a ConfigurationSetting on the registry
        # -- it's the same value for all libraries.
        if vendor_id:
            ConfigurationSetting.for_externalintegration(
                AuthdataUtility.VENDOR_ID_KEY, self.integration
            ).value = vendor_id

        # Set a public key for the library.
        encryptor = self._set_public_key(key)

        # Build the document we'll be sending to the registration URL.
        auth_document_url = url_for(
            "authentication_document",
            library_short_name=library.short_name
        )
        payload = dict(url=auth_document_url, stage=stage)

        # Find the email address the administrator should use if they notice
        # a problem with the way the library is using an integration.
        contact = Configuration.configuration_contact_uri(library)
        if contact:
            payload['contact'] = contact

        response = self._send_registration_request(
            do_post, register_url, payload
        )
        if isinstance(response, ProblemDetail):
            return response
        catalog = json.loads(response.content)

        # Since we generated a public key, the catalog should have provided
        # credentials for future authenticated communication,
        # e.g. through Short Client Tokens or authenticated API
        # requests.
        metadata = catalog.get("metadata", {})
        short_name = metadata.get("short_name")
        shared_secret = metadata.get("shared_secret")

        if short_name:
             setting, ignore = self.secret(ExternalIntegration.USERNAME)
             setting.value = short_name
        if shared_secret:
            shared_secret = self._decrypt_shared_secret(
                encryptor, shared_secret
            )
            if isinstance(shared_secret, ProblemDetail):
                return shared_secret

            setting, ignore = self.setting(ExternalIntegration.PASSWORD)
            setting.value = shared_secret

        # We have successfully completed the registration.
        self.status_field.value = self.SUCCESS_STATUS

        # We're done with the library's public key, so remove the
        # setting.
        ConfigurationSetting.for_library(
            Configuration.PUBLIC_KEY, library
        ).value = None
        return True

    def _extract_catalog_information(self, response):
        """From an OPDS catalog, extract information that's essential to
        kickstarting the OPDS Directory Registration Protocol.

        :param response: A requests-style Response object.

        :return A ProblemDetail if there's a problem, otherwise a
        2-tuple (registration URL, Adobe vendor ID).
        """
        # The catalog URL must be either an OPDS 2 catalog or an OPDS 1 feed.
        type = response.headers.get("Content-Type")
        if type and type.startswith('application/opds+json'):
            # This is an OPDS 2 catalog.
            catalog = json.loads(response.content)
            links = catalog.get("links", [])
            vendor_id = catalog.get("metadata", {}).get("adobe_vendor_id")
        elif type and type.startswith("application/atom+xml;profile=opds-catalog"):
            # This is an OPDS 1 feed.
            feed = feedparser.parse(response.content)
            links = feed.get("feed", {}).get("links", [])
            vendor_id = None
        else:
            return REMOTE_INTEGRATION_FAILED.detailed(_("The service at %(url)s did not return OPDS.", url=catalog_url))

        register_url = None
        for link in links:
            if link.get("rel") == "register":
                register_url = link.get("href")
                break
        if not register_url:
            return REMOTE_INTEGRATION_FAILED.detailed(_("The service at %(url)s did not provide a register link.", url=catalog_url))
        return register_url, vendor_id

    def _set_public_key(self, key):
        """Set the public key for this library. This key will be published in
        the library's Authentication For OPDS document, allowing the
        remote registry to sign a shared secret for it.

        NOTE: This method commits to the database.

        :return: A Crypto.Cipher object that can be used to decrypt
        data encrypted with the public key.
        """
        if not key:
            key = RSA.generate(2048)
        public_key = key.publickey().exportKey()
        encryptor = PKCS1_OAEP.new(key)

        ConfigurationSetting.for_library(
            Configuration.PUBLIC_KEY, self.library
        ).value = public_key
        # Commit so the public key will be there when the registry gets the
        # OPDS Authentication document.
        self._db.commit()

    def _send_registration_request(self, register_url, payload, do_post):
        """Send the request that actually kicks off the OPDS Directory
        Registration Protocol.

        :return: Either a ProblemDetail or a requests-like Response object.
        """
        # Allow 401 so we can provide a more useful error message.
        response = do_post(
            register_url, payload, timeout=60,
            allowed_response_codes=["2xx", "3xx", "401"],
        )
        if response.status_code == 401:
            if response.headers.get("Content-Type") == PROBLEM_DETAIL_JSON_MEDIA_TYPE:
                problem = json.loads(response.content)
                return INTEGRATION_ERROR.detailed(
                    _("Remote service returned: \"%(problem)s\"", problem=problem.get("detail")))
            else:
                return INTEGRATION_ERROR.detailed(
                    _("Remote service returned: \"%(problem)s\"", problem=response.content))
        return response

    def _decrypt_shared_secret(self, encryptor, shared_secret):
        """Attempt to decrypt an encrypted shared secret.

        :return: The decrypted shared secret, or a ProblemDetail if
        it could not be decrypted.
        """
        try:
            shared_secret = encryptor.decrypt(base64.b64decode(shared_secret))
        except ValueError, e:
            return SHARED_SECRET_DECRYPTION_ERROR.detailed(
                _("Could not decrypt shared secret %s") % shared_secret
            )
        return shared_secret


class LibraryRegistrationScript(LibraryInputScript):

    DEFAULT_REGISTRY = "https://libraryregistry.librarysimplified.org/"

    @classmethod
    def arg_parser(cls, _db):
        parser = LibraryInputScript.arg_parser(_db)
        parser.add_argument(
            '--registry-url',
            help="Register libraries with the given registry.",
            default=cls.DEFAULT_REGISTRY
        )
        parser.add_argument(
            '--production',
            help="Flag libraries as ready for production.",
            action='store_true'
        )
        return parser

    
