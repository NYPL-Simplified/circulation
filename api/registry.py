class LibraryRegistry(object):
    """A circulation manager's view of the library registry."""
    DEFAULT_URL = "https://libraryregistry.librarysimplified.org/"
    
    LIBRARY_REGISTRATION_STATUS = u"library-registration-status"
    SUCCESS_STATUS = u"success"
    FAILURE_STATUS = u"failure"

    @classmethod
    def default_integration(cls, _db):
        """Find or create the ExternalIntegration for the default library
        registry integration.
        """
        default, ignore = get_one_or_create(
            _db, ExternalIntegration,
            goal=ExternalIntegration.DISCOVERY_GOAL,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            name="Library Simplified Registry")
        default.url = cls.DEFAULT_URL
        return default

    @classmethod
    def for_integration_id(self, _db, integration_id):
        integration = get_one(_db, ExternalIntegration,
                              goal=ExternalIntegration.DISCOVERY_GOAL,
                              id=integration_id)
        if not integration:
            return None
        return cls(integration)

    @classmethod
    def all(cls, _db):
        for integration in self._db.query(ExternalIntegration).filter(
                ExternalIntegration.goal==ExternalIntegration.DISCOVERY_GOAL):
            return cls(integration)

    def __init__(self, integration):
        self.integration = integration

    @property
    def libraries(self):
        return self.integration.libraries

class Registration(object):
    """A specific library's registration for a particular registry."""

    # A library may be registered in a 'testing' stage or a
    # 'production' stage.
    #
    # TODO: For now, the default is 'production' because there is no
    # UI for specifying which stage to use.  Once there is such a UI,
    # the default registration stage should be changed to 'testing'.
    TESTING_REGISTRATION_STAGE = "testing"
    PRODUCTION_REGISTRATION_STAGE = "production"
    DEFAULT_REGISTRATION_STAGE = PRODUCTION_REGISTRATION_STAGE
    VALID_REGISTRATION_STAGES = [TESTING_REGISTRATION_STAGE,
                                 PRODUCTION_REGISTRATION_STAGE]

    def __init__(self, registry, library):
        self.registry = registry
        self.library = library

    def status(self, _db):
        return ConfigurationSetting.for_library_and_externalintegration(
            _db, LIBRARY_REGISTRATION_STATUS, self.library, service
        )

    def register(self):
        if not library in self.registry.libraries:
            self.registry.libraries.append(library)
        status = registry.status(library)
        status.value = self.FAILURE_STATUS
        registered = self._register_library(*args, **kwargs)
        if isinstance(registered, ProblemDetail):
            return registered
        status.value = self.SUCCESS_STATUS

    def _register_library(self, catalog_url, library, integration,
                          stage=None, do_get=HTTP.debuggable_get,
                          do_post=HTTP.debuggable_post, key=None):
        """Attempt to register a library with an external service,
        such as a library registry or a shared collection on another
        circulation manager.

        Note: this method does a commit in order to set a public
        key for the external service to request.
        """
        if stage not in self.VALID_REGISTRATION_STAGES:
            stage = self.DEFAULT_REGISTRATION_STAGE
        response = do_get(catalog_url)
        if isinstance(response, ProblemDetail):
            return response
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

        # Store the vendor id as a ConfigurationSetting on the registry.
        if vendor_id:
            ConfigurationSetting.for_externalintegration(
                AuthdataUtility.VENDOR_ID_KEY, integration).value = vendor_id

        # Generate a public key for the library.
        if not key:
            key = RSA.generate(2048)
        public_key = key.publickey().exportKey()
        encryptor = PKCS1_OAEP.new(key)

        ConfigurationSetting.for_library(Configuration.PUBLIC_KEY, library).value = public_key
        # Commit so the public key will be there when the registry gets the
        # OPDS Authentication document.
        self._db.commit()

        auth_document_url = self.url_for(
            "authentication_document",
            library_short_name=library.short_name
        )
        payload = dict(url=auth_document_url, stage=stage)

        # Find the email address the administrator should use if they notice
        # a problem with the way the library is using an integration.
        contact = Configuration.configuration_contact_uri(library)
        if contact:
            payload['contact'] = contact
        # Allow 401 so we can provide a more useful error message.
        response = do_post(
            register_url, payload, timeout=60,
            allowed_response_codes=["2xx", "3xx", "401"],
        )
        if isinstance(response, ProblemDetail):
            return response
        if response.status_code == 401:
            if response.headers.get("Content-Type") == PROBLEM_DETAIL_JSON_MEDIA_TYPE:
                problem = json.loads(response.content)
                return INTEGRATION_ERROR.detailed(
                    _("Remote service returned: \"%(problem)s\"", problem=problem.get("detail")))
            else:
                return INTEGRATION_ERROR.detailed(
                    _("Remote service returned: \"%(problem)s\"", problem=response.content))

        catalog = json.loads(response.content)

        # Since we generated a public key, the catalog should provide credentials
        # for future authenticated communication, e.g. through Short Client Tokens
        # or authenticated API requests.
        short_name = catalog.get("metadata", {}).get("short_name")
        shared_secret = catalog.get("metadata", {}).get("shared_secret")

        if short_name:
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.USERNAME, library, integration
            ).value = short_name
        if shared_secret:
            shared_secret = self._decrypt_shared_secret(encryptor, shared_secret)
            if isinstance(shared_secret, ProblemDetail):
                return shared_secret

            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.PASSWORD, library, integration
            ).value = shared_secret
        integration.libraries += [library]

        # We're done with the key, so remove the setting.
        ConfigurationSetting.for_library(Configuration.PUBLIC_KEY, library).value = None
        return True

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

    
