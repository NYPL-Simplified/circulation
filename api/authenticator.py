from nose.tools import set_trace
from config import (
    Configuration,
    CannotLoadConfiguration,
)
from core.model import (
    get_one,
    get_one_or_create,
    CirculationEvent,
    Credential,
    DataSource,
    Library,
    Patron,
    Session,
)
from core.util.problem_detail import (
    ProblemDetail,
    json as pd_json,
)
from core.util.opds_authentication_document import OPDSAuthenticationDocument
from core.analytics import Analytics
from sqlalchemy.ext.hybrid import hybrid_property
from problem_details import *
from util.patron import PatronUtility

import datetime
import logging
from money import Money
import re
import urlparse
import urllib
import uuid
import json
import jwt
import flask
from flask import (
    Response,
    redirect,
    url_for,
)
from werkzeug.datastructures import Headers
from flask.ext.babel import lazy_gettext as _
import importlib


class PatronData(object):
    """A container for basic information about a patron.

    Like Metadata and CirculationData, this offers a layer of
    abstraction between various account managment systems and the
    circulation manager database. Unlike with those classes, some of
    this data cannot be written to the database for data retention
    reasons. But it can be passed from the account management system
    to the client application.
    """

    # Used to distinguish between "value has been unset" and "value
    # has not changed".
    class NoValue(object):
        def __nonzero__(self):
            """We want this object to act like None or False."""
            return False
    NO_VALUE = NoValue()

    # Reasons why a patron might be blocked.
    UNKNOWN_BLOCK = 'unknown'
    CARD_REPORTED_LOST = 'card reported lost'
    FINES = 'too many fines'
    
    def __init__(self,
                 permanent_id=None,
                 authorization_identifier=None,
                 username=None,
                 personal_name=None,
                 email_address=None,
                 authorization_expires=None,
                 external_type=None,
                 fines=None,
                 block_reason=None,
                 complete=True,
    ):
        """Store basic information about a patron.

        :param permanent_id: A unique and unchanging identifier for
        the patron, as used by the account management system and
        probably never seen by the patron. This is not required, but
        it is very useful to have because other identifiers tend to
        change.

        :param authorization_identifier: One or more assigned
        identifiers (usually numeric) the patron may use to identify
        themselves. This may be a list, because patrons may have
        multiple authorization identifiers. For example, an NYPL
        patron may have an NYPL library card, a Brooklyn Public
        Library card, and an IDNYC card: three different barcodes that
        all authenticate the same patron.

        The circulation manager does the best it can to maintain
        continuity of the patron's identity in the face of changes to
        this list. The two assumptions made are: 

        1) A patron tends to pick one of their authorization
        identifiers and stick with it until it stops working, rather
        than switching back and forth. This identifier is the one
        stored in Patron.authorization_identifier.

        2) In the absence of any other information, the authorization
        identifier at the _beginning_ of this list is the one that
        should be stored in Patron.authorization_identifier.

        :param username: An identifier (usually alphanumeric) chosen
        by the patron and used to identify themselves.

        :param personal_name: The name of the patron. This information
        is not stored in the circulation manager database but may be
        passed on to the client.

        :param authorization_expires: The date, if any, at which the patron's
        authorization to borrow items from the library expires.

        :param external_type: A string classifying the patron
        according to some library-specific scheme.

        :param fines: A Money object representing the amount the
        patron owes in fines. Note that only the value portion of the
        Money object will be stored in the database; the currency portion
        will be ignored. (e.g. "20 USD" will become 20)

        :param block_reason: A string indicating why the patron is
        blocked from borrowing items. (Even if this is set to None, it
        may turn out the patron cannot borrow items because their card
        has expired or their fines are excessive.)

        :param complete: Does this PatronData represent the most
        complete data we are likely to get for this patron from this
        data source, or is it an abbreviated version of more complete
        data we could get some other way?

        """
        self.permanent_id = permanent_id

        self.set_authorization_identifier(authorization_identifier)
        self.username = username
        self.authorization_expires = authorization_expires
        self.external_type = external_type
        self.fines = fines
        self.block_reason = block_reason
        self.complete = complete
        
        # We do not store personal_name in the database, but we provide
        # it to the client if possible.
        self.personal_name = personal_name
        
        # We do not store email address in the database, but we need
        # to have it available for notifications.
        self.email_address = email_address

    def __repr__(self):
        return "<PatronData permanent_id=%r authorization_identifier=%r username=%r>" % (
            self.permanent_id, self.authorization_identifier,
            self.username
        )

    @hybrid_property
    def fines(self):
        return self._fines

    @fines.setter
    def _set_fines(self, value):
        """When setting patron fines, only store the numeric portion of 
        a Money object.
        """
        if isinstance(value, Money):
            value = value.amount
        self._fines = value
    
    def apply(self, patron):
        """Take the portion of this data that can be stored in the database
        and write it to the given Patron record.
        """

        # First, handle the easy stuff -- everything except authorization
        # identifier.
        self.set_value(patron, 'external_identifier', self.permanent_id)
        self.set_value(patron, 'username', self.username)
        self.set_value(patron, '_external_type', self.external_type)
        self.set_value(patron, 'authorization_expires',
                       self.authorization_expires)
        self.set_value(patron, 'fines', self.fines)
        self.set_value(patron, 'block_reason', self.block_reason)

        # Now handle authorization identifier.
        if self.complete:
            # We have a complete picture of data from the ILS,
            # so we can be comfortable setting the authorization
            # identifier if necessary.
            if (patron.authorization_identifier is None or
                patron.authorization_identifier not in
                self.authorization_identifiers):
                # The patron's authorization_identifier is not set, or is
                # set to a value that is no longer valid. Set it again.
                self.set_value(patron, 'authorization_identifier',
                               self.authorization_identifier)
        elif patron.authorization_identifier != self.authorization_identifier:
            # It looks like we need to change
            # Patron.authorization_identifier.  However, we do not
            # have a complete picture of the patron's record. We don't
            # know if the current identifier is better than the one
            # the patron provided.

            # However, we can provisionally
            # Patron.authorization_identifier if it's not already set.
            if not patron.authorization_identifier:
                self.set_value(patron, 'authorization_identifier',
                               self.authorization_identifier)

            if patron.username and self.authorization_identifier == patron.username:
                # This should be fine. It looks like the patron's
                # .authorization_identifier is set to their barcode,
                # and they authenticated with their username. In this
                # case we can be confident there is no need to change
                # Patron.authorization_identifier.
                pass
            else:
                # We don't know what's going on and we need to sync
                # with the remote ASAP.
                patron.last_external_sync = None
           
        # Note that we do not store personal_name or email_address in the
        # database model.
        if self.complete:
            # We got a complete dataset from the ILS, which is what an
            # external sync does, so we can reset the timer on
            # external sync.
            patron.last_external_sync = datetime.datetime.utcnow()

    def set_value(self, patron, field_name, value):
        if value is None:
            # Do nothing
            return
        elif value is self.NO_VALUE:
            # Unset a previous value.
            value = None
        setattr(patron, field_name, value)
        
    def get_or_create_patron(self, _db, library_id):
        """Create a Patron with this information.

        TODO: I'm concerned in the general case with race
        conditions. It's theoretically possible that two newly created
        patrons could have the same username or authorization
        identifier, violating a uniqueness constraint. This could
        happen if one was identified by permanent ID and the other had
        no permanent ID and was identified by username. (This would
        only come up if the authentication provider has permanent IDs
        for some patrons but not others.)

        Something similar can happen if the authentication provider
        provides username and authorization identifier, but not
        permanent ID, and the patron's authorization identifier (but
        not their username) changes while two different circulation
        manager authentication requests are pending.

        When these race conditions do happen, I think the worst that
        will happen is the second request will fail. But it's very
        important that authorization providers give some unique,
        preferably unchanging way of identifying patrons.

        :param library_id: Database ID of the Library with which this
            patron is associated.
        """
        
        # We must be very careful when checking whether the patron
        # already exists because three different fields might be in use
        # as the patron identifier.
        if self.permanent_id:
            search_by = dict(external_identifier=self.permanent_id)
        elif self.username:
            search_by = dict(username=self.username)
        elif self.authorization_identifier:
            search_by = dict(
                authorization_identifier=self.authorization_identifier
            )
        else:
            raise ValueError(
                "Cannot create patron without some way of identifying them uniquely."
            )
        search_by['library_id'] = library_id
        __transaction = _db.begin_nested()
        patron, is_new = get_one_or_create(_db, Patron, **search_by)

        if is_new:
            # Send out an analytics event to record the fact
            # that a new patron was created.
            Analytics.collect_event(_db, None,
                                    CirculationEvent.NEW_PATRON)

        # This makes sure the Patron is brought into sync with the
        # other fields of this PatronData object, regardless of
        # whether or not it is newly created.
        if patron:
            self.apply(patron)
        __transaction.commit()
        return patron, is_new

    @property
    def to_response_parameters(self):
        """Return information about this patron which the client might
        find useful.

        This information will be sent to the client immediately after
        a patron's credentials are verified by an OAuth provider.
        """
        if self.personal_name:
            return dict(name=self.personal_name)
        return None

    def set_authorization_identifier(self, authorization_identifier):
        """Helper method to set both .authorization_identifier
        and .authorization_identifiers appropriately.
        """
        # The first authorization identifier in the list is the one
        # we should use for Patron.authorization_identifier, assuming
        # Patron.authorization_identifier needs to be updated.
        if isinstance(authorization_identifier, list):
            authorization_identifiers = authorization_identifier
            authorization_identifier = authorization_identifiers[0]
        elif authorization_identifier is None:
            authorization_identifiers = []
            authorization_identifier = None
        elif authorization_identifier is self.NO_VALUE:
            authorization_identifiers = []
            authorization_identifier = self.NO_VALUE
        else:
            authorization_identifiers = [authorization_identifier]
        self.authorization_identifier = authorization_identifier
        self.authorization_identifiers = authorization_identifiers

class Authenticator(object):
    """Use the registered AuthenticationProviders to turn incoming
    credentials into Patron objects.
    """    

    @classmethod
    def from_config(cls, _db):
        """Initialize an Authenticator from site configuration.
        """
        # TODO: This needs to change to get _all_ libraries
        # and instantiate an Authenticator for each, or else
        # a single Authenticator that can handle all of them.
        library = Library.instance(_db)
        
        # Commit just in case this is the first time the Library has
        # ever been loaded.
        _db.commit()
        
        authentication_policy = Configuration.policy("authentication")
        if not authentication_policy:
            raise CannotLoadConfiguration(
                "No authentication policy given."
            )
        
        if (not isinstance(authentication_policy, dict)
            or not 'providers' in authentication_policy):
            raise CannotLoadConfiguration(
                "Authentication policy must be a dictionary with key 'providers'."
            )
        bearer_token_signing_secret = authentication_policy.get(
            'bearer_token_signing_secret'
        )
        providers = authentication_policy['providers']        
        if isinstance(providers, dict):
            # There's only one provider.
            providers = [providers]
            
        # Start with an empty list of authenticators.        
        authenticator = cls(
            _db=_db, library=library,
            bearer_token_signing_secret=bearer_token_signing_secret
        )

        # Register each provider.
        for provider_dict in providers:
            if not isinstance(provider_dict, dict):
                raise CannotLoadConfiguration(
                    "Provider %r is invalid; must be a dictionary." %
                    provider_dict
                )
            authenticator.register_provider(provider_dict)
                
        if (not authenticator.basic_auth_provider
            and not authenticator.oauth_providers_by_name):
            # TODO: This isn't unacceptable: a fully open-access
            # collection doesn't need any authentication providers.
            # But supporting that case requires specialized work, e.g.
            # getting rid of all the links to controllers that require
            # authentication.
            raise CannotLoadConfiguration(
                "No authentication provider configured"
            )
        authenticator.assert_ready_for_oauth()
        return authenticator

    def __init__(self, _db, library, basic_auth_provider=None,
                 oauth_providers=None,
                 bearer_token_signing_secret=None):
        """Initialize an Authenticator from a list of AuthenticationProviders.

        :param _db: A database session (probably a scoped session, which is
            why we can't derive it from `library`)

        :param library: The Library to which this Authenticator guards
        access. TODO: This paramater will disappear if we decide that
        an Authenticator guards access to _all_ libraries.

        :param basic_auth_provider: The AuthenticatonProvider that handles
        HTTP Basic Auth requests.

        :param oauth_providers: A list of AuthenticationProviders that handle
        OAuth requests.

        :param bearer_token_signing_secret: The secret to use when
        signing JWTs for use as bearer tokens.

        """
        self._db = _db
        self.library_id = library.id
        self.library_uuid = library.uuid
        self.library_name = library.name
        self.basic_auth_provider = basic_auth_provider
        self.oauth_providers_by_name = dict()
        self.bearer_token_signing_secret = bearer_token_signing_secret
        if oauth_providers:
            for provider in oauth_providers:
                self.oauth_providers_by_name[provider.NAME] = provider
        self.assert_ready_for_oauth()
        
    def assert_ready_for_oauth(self):
        """If this Authenticator has OAuth providers, ensure that it
        also has a secret it can use to sign bearer tokens.
        """
        if self.oauth_providers_by_name and not self.bearer_token_signing_secret:
            raise CannotLoadConfiguration(
                "OAuth providers are configured, but secret for signing bearer tokens is not."
            )
                
    def register_provider(self, config):
        """Turn a description of a provider into an AuthenticationProvider
        object, and register it.

        :param config: A dictionary of parameters that configure
        the provider.
        """
        if not 'module' in config:
            raise CannotLoadConfiguration(
                "Provider configuration does not define 'module': %r" %
                config
            )
        module_name = config['module']
        config = dict(config)
        del config['module']
        provider_module = importlib.import_module(module_name)
        provider_class = getattr(provider_module, "AuthenticationProvider")
        if issubclass(provider_class, BasicAuthenticationProvider):
            provider = provider_class.from_config(self.library_id, config)
            self.register_basic_auth_provider(provider)
        elif issubclass(provider_class, OAuthAuthenticationProvider):
            provider = provider_class.from_config(self.library_id, config)
            self.register_oauth_provider(provider)
        else:
            raise CannotLoadConfiguration(
                "Unrecognized authentication provider: %s" % provider_class
            )

    def register_basic_auth_provider(self, provider):
        if (self.basic_auth_provider != None
            and self.basic_auth_provider != provider):
            raise CannotLoadConfiguration(
                "Two basic auth providers configured"
            )
        self.basic_auth_provider = provider
        
    def register_oauth_provider(self, provider):
        already_registered = self.oauth_providers_by_name.get(
            provider.NAME
        )
        if already_registered and already_registered != provider:
            raise CannotLoadConfiguration(
                'Two different OAuth providers claim the name "%s"' % (
                    provider.NAME
                )
            )
        self.oauth_providers_by_name[provider.NAME] = provider
        
    @property
    def providers(self):
        """An iterator over all registered AuthenticationProviders."""
        if self.basic_auth_provider:
            yield self.basic_auth_provider
        for provider in self.oauth_providers_by_name.values():
            yield provider
        
    def authenticated_patron(self, _db, header):
        """Go from an Authorization header value to a Patron object.

        :param header: If Basic Auth is in use, this is a dictionary
        with 'user' and 'password' components, derived from the HTTP
        header `Authorization`. Otherwise, this is the literal value
        of the `Authorization` HTTP header.

        :return: A Patron, if one can be authenticated. None, if the
        credentials do not authenticate any particular patron. A
        ProblemDetail if an error occurs.
        """
        if (self.basic_auth_provider
            and isinstance(header, dict) and 'username' in header):
            # The patron wants to authenticate with the
            # BasicAuthenticationProvider.
            return self.basic_auth_provider.authenticated_patron(_db, header)
        elif (self.oauth_providers_by_name
              and isinstance(header, basestring)
              and 'bearer' in header.lower()):

            # The patron wants to use an
            # OAuthAuthenticationProvider. Figure out which one.
            provider_name, provider_token = self.decode_bearer_token_from_header(
                header
            )
            provider = self.oauth_provider_lookup(provider_name)
            if isinstance(provider, ProblemDetail):
                # There was a problem turning the provider name into
                # a registered OAuthAuthenticationProvider.
                return provider

            # Ask the OAuthAuthenticationProvider to turn its token
            # into a Patron.
            return provider.authenticated_patron(_db, provider_token)

        # We were unable to determine what was going on with the
        # Authenticate header.
        return UNSUPPORTED_AUTHENTICATION_MECHANISM

    def get_credential_from_header(self, header):
        """Extract a password credential from a WWW-Authenticate header
        (or equivalent).

        This is used to pass on a patron's credential to a content provider,
        such as Overdrive, which performs independent validation of
        a patron's credentials.

        :return: The patron's password, or None if not available.
        """
        credential = None
        for provider in self.providers:
            credential = provider.get_credential_from_header(header)
            if credential is not None:
                break
            
        return credential
    
    def oauth_provider_lookup(self, provider_name):
        """Look up the OAuthAuthenticationProvider with the given name. If that
        doesn't work, return an appropriate ProblemDetail.
        """
        if not self.oauth_providers_by_name:
            # We don't support OAuth at all.
            return UNKNOWN_OAUTH_PROVIDER.detailed(
                _("No OAuth providers are configured.")
            )

        if (not provider_name
            or not provider_name in self.oauth_providers_by_name):
            # The patron neglected to specify a provider, or specified
            # one we don't support.
            possibilities = ", ".join(self.oauth_providers_by_name.keys())
            return UNKNOWN_OAUTH_PROVIDER.detailed(
                UNKNOWN_OAUTH_PROVIDER.detail +
                _(" The known providers are: %s") % possibilities
            )
        return self.oauth_providers_by_name[provider_name]

    def create_bearer_token(self, provider_name, provider_token):
        """Create a JSON web token with the given provider name and access
        token.

        The patron will use this as a bearer token in lieu of the
        token we got from their OAuth provider. The big advantage of
        this token is that it tells us _which_ OAuth provider the
        patron authenticated against.

        When the patron uses the bearer token in the Authenticate header,
        it will be decoded with `decode_bearer_token_from_header`.
        """
        payload = dict(
            token=provider_token,
            # I'm not sure this is the correct way to use an
            # Issuer claim (https://tools.ietf.org/html/rfc7519#section-4.1.1).
            # Maybe we should use something custom instead.
            iss=provider_name,
        )
        return jwt.encode(
            payload, self.bearer_token_signing_secret, algorithm='HS256'
        )
    
    def decode_bearer_token_from_header(self, header):
        """Extract auth provider name and access token from an Authenticate
        header value.
        """
        simplified_token = header.split(' ')[1]
        return self.decode_bearer_token(simplified_token)
    
    def decode_bearer_token(self, token):
        """Extract auth provider name and access token from JSON web token."""
        decoded = jwt.decode(token, self.bearer_token_signing_secret,
                             algorithms=['HS256'])
        provider_name = decoded['iss']
        token = decoded['token']
        return (provider_name, token)
    
    def create_authentication_document(self):
        """Create the OPDS authentication document to be used when
        a request comes in with no authentication.
        """
        base_opds_document = Configuration.base_opds_authentication_document()

        circulation_manager_url = Configuration.integration_url(
            Configuration.CIRCULATION_MANAGER_INTEGRATION, required=True)
        scheme, netloc, path, parameters, query, fragment = (
            urlparse.urlparse(circulation_manager_url))

        links = {}
        for rel, value in (
                ("terms-of-service", Configuration.terms_of_service_url()),
                ("privacy-policy", Configuration.privacy_policy_url()),
                ("copyright", Configuration.acknowledgements_url()),
                ("about", Configuration.about_url()),
                ("license", Configuration.license_url()),
        ):
            if value:
                links[rel] = dict(href=value, type="text/html")

        library_name = self.library_name or unicode(_("Library"))
        doc = OPDSAuthenticationDocument.fill_in(
            base_opds_document, list(self.providers),
            name=library_name, id=self.library_uuid, links=links,
        )
        return json.dumps(doc)

    def create_authentication_headers(self):
        """Create the HTTP headers to return with the OPDS
        authentication document."""
        headers = Headers()
        headers.add('Content-Type', OPDSAuthenticationDocument.MEDIA_TYPE)
        # if requested from a web client, don't include WWW-Authenticate header,
        # which forces the default browser authentication prompt
        if self.basic_auth_provider and not flask.request.headers.get("X-Requested-With") == "XMLHttpRequest":
            headers.add('WWW-Authenticate', self.basic_auth_provider.authentication_header)

        # TODO: We're leaving out headers for other providers to avoid breaking iOS
        # clients that don't support multiple auth headers. It's not clear what
        # the header for an oauth provider should look like. This means that there's
        # no auth header for app without a basic auth provider, but we don't have
        # any apps like that yet.

        return headers


class AuthenticationProvider(object):
    """Handle a specific patron authentication scheme.
    """

    # NOTE: Each subclass MUST define an attribute called NAME, which
    # is used to configure that subclass in the configuration file,
    # used to create the name of the log channel used by this
    # subclass, used to distinguish between tokens from different
    # OAuth providers, etc.
    
    # Each subclass MUST define a value for URI. This is used in the
    # Authentication for OPDS document to distinguish between
    # different types of authentication.
    URI = None

    def __init__(self, library_id):
        """Basic constructor.
        
        :param library_id: The database ID of the Library to be managed 
        by this AuthenticationProvider.
        """
        if not isinstance(library_id, int):
            raise Exception(
                "Expected library_id to be an integer, got %r" % library_id
            )
        self.library_id = library_id
    
    def library(self, _db):
        return get_one(_db, Library, self.library_id)
    
    def authenticated_patron(self, _db, header):
        """Go from a WWW-Authenticate header (or equivalent) to a Patron object.

        If the Patron needs to have their metadata updated, it happens
        transparently at this point.

        :return: A Patron if one can be authenticated; a ProblemDetail
        if an error occurs; None if the credentials are missing or
        wrong.
        """
        patron = self.authenticate(_db, header)
        if not isinstance(patron, Patron):
            return patron
        if PatronUtility.needs_external_sync(patron):
            self.update_patron_metadata(patron)
        return patron

    def update_patron_metadata(self, patron):
        """Refresh our local record of this patron's account information.

        :param patron: A Patron object.
        """
        remote_patron_info = self.remote_patron_lookup(patron)
        if isinstance(remote_patron_info, PatronData):
            remote_patron_info.apply(patron)

    def authenticate(self, _db, header):
        """Authenticate a patron based on a WWW-Authenticate header
        (or equivalent).

        :return: A Patron if one can be authenticated; a ProblemDetail
        if an error occurs; None if the credentials are missing or
        wrong.
        """
        raise NotImplementedError()

    def get_credential_from_header(self, header):
        """Extract a password credential from a WWW-Authenticate header
        (or equivalent).

        This is used to pass on a patron's credential to a content provider,
        such as Overdrive, which performs independent validation of
        a patron's credentials.

        :return: The patron's password, or None if not available.
        """
        return None
    
    def remote_patron_lookup(self, patron_or_patrondata):
        """Ask the remote for detailed information about a patron's account.

        This may be called in the course of authenticating a patron,
        or it may be called when the patron isn't around, for purposes
        of learning some personal information (primarily email
        address) that can't be stored in the database.

        The default implementation assumes there is no special
        lookup functionality, and returns exactly the information
        present in the object that was passed in.

        :param patron_or_patrondata: Either a Patron object, a PatronData
        object, or None (if no further information could be provided).

        :return: An updated PatronData object.

        """
        if not patron_or_patrondata:
            return None
        if (isinstance(patron_or_patrondata, PatronData)
            or isinstance(patron_or_patrondata, Patron)):
            return patron_or_patrondata
        raise ValueError(
            "Unexpected object %r passed into remote_patron_lookup." %
            patron_or_patrondata
        )       
    
    def authentication_provider_document(self):
        """Create a stanza for use in an Authentication for OPDS document.

        :return: A dictionary that can be associated with the
        provider's .URI in an Authentication for OPDS document, e.g.:

        { "providers": { [provider.URI] : [this document] } }

        For example:



        {"providers": {"http://librarysimplified.org/terms/auth/library-barcode": {"methods": {"http://opds-spec.org/auth/basic": {"labels": {"login": "Barcode", "password": "PIN"}}}}
        """
        raise NotImplementedError()

class BasicAuthenticationProvider(AuthenticationProvider):
    """Verify a username/password, obtained through HTTP Basic Auth, with
    a remote source of truth.
    """
    # NOTE: Each subclass MUST define an attribute called NAME, which
    # is used to configure that subclass in the configuration file,
    # used to create the name of the log channel used by this
    # subclass, used to distinguish between tokens from different
    # OAuth providers, etc.
    #
    # Each subclass MAY override the default value for DISPLAY_NAME.
    # This becomes the human-readable name of the authentication
    # mechanism in the OPDS authentication document.
    #
    # Each subclass MAY override the default values for LOGIN_LABEL
    # and PASSWORD_LABEL. These become the human-readable labels for
    # username and password in the OPDS authentication document
    #
    # Each subclass MAY override the default value for
    # AUTHENTICATION_REALM. This becomes the name of the HTTP Basic
    # Auth authentication realm.
    #
    # It's generally not necessary for a subclass to override URI, but
    # you might want to do it if your username/password doesn't fit
    # into the 'library barcode' paradigm.
    #
    # It's probably not necessary for a subclass to override METHOD,
    # since the default indicates HTTP Basic Auth.

    DISPLAY_NAME = _("Library Barcode")
    LOGIN_LABEL = _("Barcode")
    PASSWORD_LABEL = _("PIN")
    AUTHENTICATION_REALM = _("Library card")
    METHOD = "http://opds-spec.org/auth/basic"
    URI = "http://librarysimplified.org/terms/auth/library-barcode"
    NAME = 'Generic Basic Authentication provider'
    
    # By default, patron identifiers can only contain alphanumerics and
    # a few other characters. By default, there are no restrictions on
    # passwords.
    alphanumerics_plus = re.compile("^[A-Za-z0-9@.-]+$")
    DEFAULT_IDENTIFIER_REGULAR_EXPRESSION = alphanumerics_plus
    DEFAULT_PASSWORD_REGULAR_EXPRESSION = None        

    @classmethod
    def from_config(cls, library_id, config):
        """Load a BasicAuthenticationProvider from site configuration."""
        return cls(library_id, **config)

    # Used in the constructor to signify that the default argument
    # value for the class should be used (as distinct from None, which
    # indicates that no value should be used.)
    class_default = object()
    
    def __init__(self, library_id,
                 identifier_regular_expression=class_default,
                 password_regular_expression=class_default,
                 test_username=None, test_password=None):
        """Create a BasicAuthenticationProvider.

        :param library_id: Patrons authenticated through this provider
            are associated with the Library with the given ID. We don't
            pass the Library object to avoid contaminating with
            an object from a non-scoped session.
        """
        super(BasicAuthenticationProvider, self).__init__(library_id)
        if identifier_regular_expression is self.class_default:
            identifier_regular_expression = self.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION
        if identifier_regular_expression:
            identifier_regular_expression = re.compile(
                identifier_regular_expression
            )
        if password_regular_expression is self.class_default:
            password_regular_expression = self.DEFAULT_PASSWORD_REGULAR_EXPRESSION

        if password_regular_expression:
            password_regular_expression = re.compile(
                password_regular_expression
            )

        self.identifier_re = identifier_regular_expression
        self.password_re = password_regular_expression
        self.test_username = test_username
        self.test_password = test_password
        self.log = logging.getLogger(self.NAME)
        
    def testing_patron(self, _db):
        """Look up a Patron object reserved for testing purposes.

        :return: A 2-tuple (Patron, password)
        """
        if self.test_username is None or self.test_password is None:
            return None, None
        header = dict(username=self.test_username, password=self.test_password)
        return self.authenticated_patron(_db, header), self.test_password
    
    def authenticate(self, _db, credentials):
        """Turn a set of credentials into a Patron object.

        :param credentials: A dictionary with keys `username` and `password`.

        :return: A Patron if one can be authenticated; a ProblemDetail
        if an error occurs; None if the credentials are missing or
        wrong.
        """
        username = credentials.get('username')
        password = credentials.get('password')
        if not self.server_side_validation(username, password):
            # The credentials are prima facie invalid and do not
            # need to be checked with the source of truth.
            return None

        # Check these credentials with the source of truth.
        patrondata = self.remote_authenticate(username, password)

        if not patrondata or isinstance(patrondata, ProblemDetail):
            # Either an error occured or the credentials did not correspond
            # to any patron.
            return patrondata

        # At this point we know there is _some_ authenticated patron,
        # but it might not correspond to a Patron in our database, and
        # if it does, that Patron's authorization_identifier might be
        # different from the `username` passed in as part of the
        # credentials.
        
        # First, try to look up the Patron object in our database.
        patron = self.local_patron_lookup(_db, username, patrondata)
        if patron:
            # We found them! Make sure their data is up to date
            # with whatever we just got from remote.
            patrondata.apply(patron)
            return patron
        
        # We didn't find them. Now the question is: _why_ didn't the
        # patron show up locally? Have we never seen them before or
        # has their authorization identifier changed?
        #
        # Look up the patron's account remotely to get that
        # information.  In some providers this step may be a no-op
        # because we may have gotten patron account information as a
        # side effect of remote validation.
        patrondata = self.remote_patron_lookup(patrondata)
        if not patrondata or isinstance(patrondata, ProblemDetail):
            # Either there was a problem looking up the patron data, or
            # the patron does not exist on the remote. How we passed
            # remote validation is a mystery, but ours not to reason
            # why. There is no authenticated patron.
            return patrondata

        if isinstance(patrondata, Patron):
            # For whatever reason, the remote lookup implementation
            # returned a Patron object instead of a PatronData. Just
            # use that Patron object.
            return patrondata

        # At this point we have an updated PatronData object which
        # we know represents an existing patron on the remote
        # side. Try the local lookup again.
        patron = self.local_patron_lookup(_db, username, patrondata)

        if not patron:
            # We have a PatronData from the ILS that does not
            # correspond to any local Patron. Create the local Patron.
            patron, is_new = patrondata.get_or_create_patron(
                _db, self.library_id
            )
            
        # The lookup failed in the first place either because the
        # Patron did not exist on the local side, or because one of
        # the patron's identifiers changed. Either way, we need to
        # update the Patron record with the account information we
        # just got from the source of truth.
        patrondata.apply(patron)
        return patron

    def get_credential_from_header(self, header):
        """Extract a password credential from a WWW-Authenticate header
        (or equivalent).

        This is used to pass on a patron's credential to a content provider,
        such as Overdrive, which performs independent validation of
        a patron's credentials.

        :param header: A dictionary with keys `username` and `password`.
        """
        if not isinstance(header, dict):
            return None
        return header.get('password', None)
    
    def server_side_validation(self, username, password):
        """Do these credentials even look right?

        Sometimes egregious problems can be caught without needing to
        check with the ILS.
        """
        valid = True
        if self.identifier_re:
            valid = valid and username is not None and (
                self.identifier_re.match(username) is not None
            )
        if self.password_re:
            valid = valid and password is not None and (
                self.password_re.match(password) is not None
            )
        return valid
        
    def remote_authenticate(self, username, password):
        """Does the source of truth approve of these credentials?

        :return: If the credentials are valid, but nothing more is
        known about the patron, return True.

        If the credentials are valid, _and_ enough information came
        back in the request to also create a PatronInfo object, you
        may create that object and return it to save a
        remote patron lookup later.

        If the credentials are invalid, return False or None.
        """
        raise NotImplementedError()

    def local_patron_lookup(self, _db, username, patrondata):
        """Try to find a Patron object in the local database.

        :param username: An HTTP Basic Auth username. May or may not
        correspond to the `Patron.username` field.

        :param patrondata: A PatronData object recently obtained from
        the source of truth, possibly as a side effect of validating
        the username and password. This may make it possible to
        identify the patron more precisely. Or it may be None, in
        which case it's no help at all.
        """
        # We're going to try a number of different strategies to look up
        # the appropriate patron.
        lookups = []
       
        if patrondata:
            if patrondata.permanent_id:
                # Permanent ID is the most reliable way of identifying
                # a patron, since this is supposed to be an internal
                # ID that never changes.
                lookups.append(
                    dict(external_identifier=patrondata.permanent_id)
                )
            if patrondata.username:
                # Username is fairly reliable, since the patron
                # generally has to decide to change it.
                lookups.append(
                    dict(username=patrondata.username)
                )

            if patrondata.authorization_identifier:
                # Authorization identifiers change all the time so
                # they're not terribly reliable.
                lookups.append(
                    dict(
                        authorization_identifier=
                        patrondata.authorization_identifier
                    )
                )

        if username:
            # This is a Basic Auth username, but it might correspond
            # to either Patron.authorization_identifier or
            # Patron.username.
            #
            # TODO: We could save a bit of time in a common case by
            # combining these into a single query.
            lookups.append(dict(authorization_identifier=username))
            lookups.append(dict(username=username))

        patron = None
        for lookup in lookups:
            lookup['library_id'] = self.library_id
            patron = get_one(_db, Patron, **lookup)
            if patron:
                # We found them!
                break            
        return patron

    @property
    def authentication_header(self):
        return 'Basic realm="%s"' % self.AUTHENTICATION_REALM
    
    @property
    def authentication_provider_document(self):
        """Create a stanza for use in an Authentication for OPDS document.

        Example:
        {
            'name': 'My Basic Provider',
            'methods': {
                'http://opds-spec.org/auth/basic': {
                    'labels': {'login': 'Barcode', 'password': 'PIN'}
                 }
            }
        }
        """
        method_doc = dict(
            labels=dict(login=unicode(self.LOGIN_LABEL),
                        password=unicode(self.PASSWORD_LABEL))
        )
        methods = {}
        methods[self.METHOD] = method_doc
        return dict(name=unicode(self.DISPLAY_NAME), methods=methods)

    
class OAuthAuthenticationProvider(AuthenticationProvider):

    # NOTE: Each subclass must define URI as per
    # AuthenticationProvider superclass. This is the URI used to
    # identify this particular authentication provider.
    #
    # Each subclass MAY define a value for METHOD. This is the URI
    # used to identify the authentication mechanism. The default is
    # used to indicate the Library Simplified variant of OAuth.
    #
    # Each subclass MUST define an attribute called
    # NAME, which is the name used to configure that
    # subclass in the configuration file. Failure to define this
    # attribute will result in an error in .from_config().
    #
    # Each subclass MUST define an attribute called TOKEN_TYPE, which
    # is the name used in the database to distinguish this provider's
    # tokens from other provider's tokens.
    #
    # Each subclass MUST define an attribute called
    # TOKEN_DATA_SOURCE_NAME, which is the name of the DataSource
    # under which bearer tokens for patrons will be registered.

    # Finally, each subclass MUST define an attribute called
    # EXTERNAL_AUTHENTICATE_URL. When the patron hits the
    # oauth_authentication_redirect controller, they will be
    # redirected to this URL on the OAuth provider's site.
    #
    # This URL template MUST contain Python variable interpolations
    # for 'client_id', 'oauth_callback_url', 'state'. This way the
    # OAuth provider knows which client is asking to authenticate a
    # user, and it knows to send the client back to our
    # oauth_authentication_callback controller. Finally, the
    # oauth_callback controller can maintain any state from the
    # initial request to oauth_authentication_redirect.
    #
    # As an example, here's the EXTERNAL_AUTHENTICATE_URL for the
    # Clever OAuth provider:
    #
    # EXTERNAL_AUTHENTICATE_URL = "https://clever.com/oauth/authorize?response_type=code&client_id=%(client_id)s&redirect_uri=%(oauth_callback_url)s&state=%(state)s"
    
    METHOD = "http://librarysimplified.org/authtype/OAuth-with-intermediary"
    
    # After verifying the patron's OAuth credentials, we send them a
    # token. This is how long they can use that token before we check
    # their OAuth credentials again.
    DEFAULT_TOKEN_EXPIRATION_DAYS = 42
    
    @classmethod
    def from_config(cls, library_id, config):
        """Load this OAuthAuthenticationProvider from the site configuration.
        """
        client_id = config.get(Configuration.OAUTH_CLIENT_ID)
        client_secret = config.get(Configuration.OAUTH_CLIENT_SECRET)
        token_expiration_days = config.get(
            Configuration.OAUTH_TOKEN_EXPIRATION_DAYS,
            cls.DEFAULT_TOKEN_EXPIRATION_DAYS
        )
        return cls(library_id, client_id, client_secret, token_expiration_days)
    
    def __init__(self, library_id, client_id, client_secret, token_expiration_days):
        """Initialize this OAuthAuthenticationProvider.

        :param library: Patrons authenticated through this provider
            are associated with the given Library.
        :param client_id: An ID given to us by the OAuth provider, used
            to distinguish between us and its other clients.
        :param client_secret: A secret key given to us by the OAuth 
            provider, used to validate that we are who we say we are.
        :param token_expiration_days: This many days may elapse before
            we ask the patron to go through the OAuth validation
            process again.
        """
        super(OAuthAuthenticationProvider, self).__init__(library_id)
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_expiration_days = token_expiration_days
        self.log = logging.getLogger(self.NAME)
        
    def authenticated_patron(self, _db, token):
        """Go from an OAuth provider token to an authenticated Patron.

        :param token: The provider token extracted from the
        Authorization header. This is _not_ the bearer token found in
        the Authorization header; it's the provider-specific token
        embedded in that token.

        :return: A Patron, if one can be authenticated. None, if the
        credentials do not authenticate any particular patron. A
        ProblemDetail if an error occurs.
        """
        data_source, ignore = self.token_data_source(_db)
        credential = Credential.lookup_by_token(
            _db, data_source, self.TOKEN_TYPE, token
        )
        if credential:
            return credential.patron

        # This token wasn't in our database, or was expired. The
        # patron will have to log in through the OAuth provider again
        # to get a new token.
        return None

    def create_token(self, _db, patron, token):
        """Create a Credential object that ties the given patron to the
        given provider token.
        """
        data_source, ignore = self.token_data_source(_db)
        duration = datetime.timedelta(days=self.token_expiration_days)
        return Credential.temporary_token_create(
            _db, data_source, self.TOKEN_TYPE, patron, duration, token
        )
    
    def remote_patron_lookup(self, patron_or_patrondata):
        """Ask the remote for detailed information about a patron's account.

        By default, there is no way to ask an OAuth provider for
        information about a specific patron after the fact.
        """
        return None

    def external_authenticate_url(self, state):
        """Generate the URL provided by the OAuth provider which will present
        the patron with a login form.

        :param state: A state variable to be propagated through to the OAuth
        callback.
        """
        template = self.EXTERNAL_AUTHENTICATE_URL
        arguments = self.external_authenticate_url_parameters(state)
        return template % arguments

    def external_authenticate_url_parameters(self, state):
        """Arguments used to fill in the template EXTERNAL_AUTHENTICATE_URL.
        """
        return dict(
            client_id=self.client_id,
            state=state,
            # When the patron finishes logging in to the OAuth provider,
            # we want them to send the patron to this URL.
            oauth_callback_url=OAuthController.oauth_authentication_callback_url()
        )


    def oauth_callback(self, _db, code):
        """Verify the incoming parameters with the OAuth provider. Exchange
        the authorization code for an access token. Create or look up
        appropriate database records.

        :param code: The authorization code generated by the
        authorization server, as per section 4.1.2 of RFC 6749. This
        method will exchange the authorization code for an access
        token.

        :return: A ProblemDetail if there's a problem. Otherwise, a
        3-tuple (Credential, Patron, PatronData). The Credential
        contains the access token provided by the OAuth provider. The
        Patron object represents the authenticated Patron, and the
        PatronData object includes information about the patron
        obtained from the OAuth provider which cannot be stored in the
        circulation manager's database, but which should be passed on
        to the client.

        """
        # Ask the OAuth provider to verify the code that was passed
        # in.  This will give us an access token we can use to look up
        # detailed patron information.
        token = self.remote_exchange_code_for_access_token(code)
        if isinstance(token, ProblemDetail):
            return token
        
        # Now that we have a bearer token, use it to look up patron
        # information.
        patrondata = self.remote_patron_lookup(token)
        if isinstance(patrondata, ProblemDetail):
            return patrondata
        
        # Convert the PatronData into a Patron object.
        patron, is_new = patrondata.get_or_create_patron(
            _db, self.library_id
        )

        # Create a credential for the Patron.
        credential, is_new = self.create_token(_db, patron, token)
        return credential, patron, patrondata

    def remote_exchange_authorization_code_for_access_token(self, code):
        """Ask the OAuth provider to convert a code (passed in to the OAuth
        callback) into a bearer token.

        We can use the bearer token to act on behalf of a specific
        patron. It also gives us confidence that the patron
        authenticated correctly with the OAuth provider.

        :return: A ProblemDetail if there's a problem; otherwise, the
        bearer token.
        """
        raise NotImplementedError()

    def remote_patron_lookup(self, access_token):
        """Use a bearer token to look up as much information as possible about
        a patron.

        :return: A ProblemDetail if there's a problem. Otherwise, a
        PatronData.
        """
        raise NotImplementedError()
    
    def _internal_authenticate_url(self):
        """A patron who wants to log in should hit this URL on the circulation
        manager. They'll be redirected to the OAuth provider, which will 
        take care of it.
        """
        return url_for('oauth_authenticate', _external=True,
                       provider=self.NAME)

    @property
    def authentication_provider_document(self):
        """Create a stanza for use in an Authentication for OPDS document.

        Example:
        {
            "name": "My OAuth Provider",
            "methods": {
                "http://librarysimplified.org/authtype/MyOAuthProvider" : {
                "links": {
                    "authenticate": "https://circulation.library.org/oauth_authenticate?provider=MyOAuth"
                 }
            }
        }
        """
        method_doc = dict(links=dict(authenticate=self._internal_authenticate_url()))
        methods = {}
        methods[self.METHOD] = method_doc
        return dict(name=self.NAME, methods=methods)

    def token_data_source(self, _db):
        return get_one_or_create(
            _db, DataSource, name=self.TOKEN_DATA_SOURCE_NAME
        )


class OAuthController(object):

    """A controller for handling requests that are part of the OAuth
    credential dance.
    """
    
    def __init__(self, authenticator):
        self.authenticator = authenticator

    @classmethod
    def oauth_authentication_callback_url(cls):
        """The URL to the oauth_authentication_callback controller.

        This is its own method because sometimes an
        OAuthAuthenticationProvider needs to send it to the OAuth
        provider to demonstrate that it knows which URL a patron was
        redirected to.
        """
        return url_for('oauth_callback', _external=True)
        
    def oauth_authentication_redirect(self, params):
        """Redirect an unauthenticated patron to the authentication URL of the
        appropriate OAuth provider.

        Over on that other site, the patron will authenticate and be
        redirected back to the circulation manager, ending up in
        oauth_authentication_callback.
        """
        redirect_uri = params.get('redirect_uri', '')
        provider_name = params.get('provider')
        provider = self.authenticator.oauth_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)
        state = dict(
            provider=provider.NAME, redirect_uri=redirect_uri
        )
        state = json.dumps(state)
        state = urllib.quote(state)
        return redirect(provider.external_authenticate_url(state))

    def oauth_authentication_callback(self, _db, params):
        """Create a Patron object and a bearer token for a patron who has just
        authenticated with one of our OAuth providers.

        :return: A redirect to the `redirect_uri` kept in
        `params['state']`, with the bearer token encoded into the
        fragment identifier as `access_token` and useful information
        about the patron encoded into the fragment identifier as
        `patron_info`. For example, if params is 

            dict(state="http://oauthprovider.org/success")

        Then the redirect URI might be:

            http://oauthprovider.org/success#access_token=1234&patron_info=%7B%22name%22%3A+%22Mary+Shell%22%7D

        It's the client's responsibility to extract the access_token,
        start using it as a bearer token, and make sense of the
        patron_info.
        """
        code = params.get('code')
        state = params.get('state')
        if not code or not state:
            return INVALID_OAUTH_CALLBACK_PARAMETERS

        state = json.loads(urllib.unquote(state))
        client_redirect_uri = state.get('redirect_uri') or ""
        provider_name = state.get('provider')
        provider = self.authenticator.oauth_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(client_redirect_uri, provider)

        # Send the incoming parameters to the OAuth provider and get
        # back a provider token (a Credential object), the
        # authenticated patron (a Patron object), and a PatronData
        # including any personal information obtained from the OAuth
        # provider (such as patron name) which we can't store in the
        # database.
        response = provider.oauth_callback(_db, code)
        if isinstance(response, ProblemDetail):
            # Most likely the OAuth provider didn't like the credentials
            # we sent.
            return self._redirect_with_error(
                client_redirect_uri, response
            )
        provider_token, patron, patrondata = response
        
        # Turn the provider token into a bearer token we can give to
        # the patron.
        simplified_token = self.authenticator.create_bearer_token(
            provider.NAME, provider_token.credential
        )

        patron_info = json.dumps(patrondata.to_response_parameters)
        params = dict(
            access_token=simplified_token,
            patron_info=patron_info
        )
        return redirect(client_redirect_uri + "#" + urllib.urlencode(params))

    def _redirect_with_error(self, redirect_uri, pd):
        """Redirect the patron to the given URL, with the given ProblemDetail
        encoded into the fragment identifier.
        """
        return redirect(self._error_uri(redirect_uri, pd))
    
    def _error_uri(self, redirect_uri, pd):
        """Encode the given ProblemDetail into the fragment identifier
        of the given URI.
        """
        problem_detail_json = pd_json(
            pd.uri, pd.status_code, pd.title, pd.detail, pd.instance,
            pd.debug_message
        )
        params = dict(error=problem_detail_json)
        return redirect_uri + "#" + urllib.urlencode(params)
