from nose.tools import set_trace
from config import (
    Configuration,
    CannotLoadConfiguration,
)
from core.model import (
    get_one,
    get_one_or_create,
    Credential,
    DataSource,
    Patron,
)
from core.util.problem_detail import (
    ProblemDetail,
    json as pd_json,
)
from core.util.opds_authentication_document import OPDSAuthenticationDocument
from problem_details import *

import datetime
import logging
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
    NO_VALUE = object()
    
    def __init__(self,
                 permanent_id=None,
                 authorization_identifier=None,
                 username=None,
                 personal_name=None,
                 email_address=None,
                 authorization_expires=None,
                 external_type=None,
                 fines=None,
                 blocked=None,
                 complete=True,
    ):
        """Store basic information about a patron.

        :param permanent_id: A unique and unchanging identifier for
        the patron, as used by the account management system and
        probably never seen by the patron. This is not required, but
        it is very useful to have because other identifiers tend to
        change.

        :param authorization_identifier: An assigned identifier
        (usually numeric) the patron uses to identify
        themselves. Whenever possible this should be the authorization
        identifier *actually used by the patron*. Patrons sometimes
        have multiple authorization identifiers, but generally choose
        one and consistently log in with it. We should keep track of
        the one they use.

        :param username: An identifier (usually alphanumeric) chosen
        by the patron and used to identify themselves.

        :param personal_name: The name of the patron. This information
        is not stored in the circulation manager database but may be
        passed on to the client.

        :param authorization_expires: The date, if any, at which the patron's
        authorization to borrow items from the library expires.

        :param external_type: A string classifying the patron
        according to some library-specific scheme.

        :param fines: An amount of money representing the amount the
        patron owes in fines.

        :param blocked: A boolean indicating whether or not the patron
        is blocked from borrowing items for any reason. (Even if this
        is set to False, it may turn out the patron cannot borrow
        items because their card has expired or their fines are
        excessive.)

        :param complete: Does this PatronData represent the most
        complete data we are likely to get for this patron from this
        data source, or is it an abbreviated version of more complete
        data we could get some other way?

        """
        self.permanent_id = permanent_id
        self.authorization_identifier = authorization_identifier
        self.username = username
        self.authorization_expires = authorization_expires
        self.external_type = external_type
        self.fines = fines
        self.blocked = blocked
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
        
    def apply(self, patron):
        """Take the portion of this data that can be stored in the database
        and write it to the given Patron record.
        """
        self.set_value(patron, 'external_identifier', self.permanent_id)
        self.set_value(patron, 'username', self.username)
        self.set_value(patron, 'authorization_identifier',
                       self.authorization_identifier)
        self.set_value(patron, '_external_type', self.external_type)
        self.set_value(patron, 'authorization_expires',
                       self.authorization_expires)
        self.set_value(patron, 'fines', self.fines)
        if self.complete:
            patron.last_external_sync = datetime.datetime.utcnow()

        # Note that we do not store personal_name or email_address in the
        # database model.

    def set_value(patron, field_name, value):
        if value is None:
            # Do nothing
            return
        elif value is self.NO_VALUE:
            # Unset a previous value.
            value = None
        setattr(patron, field_name, value)
        
    def create(self, _db):
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
        """
        # We must be very careful when checking whether the patron
        # already exists because three different fields might be in use
        # as the patron identifier.
        if self.permanent_id:
            search_by = dict(external_identifier=self.permanent_id)
        elif self.username:
            search_by = dict(username=self.username)
        if self.authorization_identifier:
            search_by = dict(
                authorization_identifier=self.authorization_identifier
            )
        else:
            raise ValueError(
                "Cannot create patron without some way of identifying them uniquely."
            )
        __transaction = _db.begin_nested()
        patron, is_new = get_one_or_create(_db, Patron, **search_by)

        # This makes sure the Patron is brought into sync with the
        # other fields of this PatronData object, regardless of
        # whether or not it is newly created.
        if patron:
            self.apply(patron)
        __transaction.commit()
        return patron

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


class Authenticator(object):
    """Use the registered AuthenticationProviders to turn incoming
    credentials into Patron objects.
    """    

    @classmethod
    def from_config(cls, _db):
        """Initialize an Authenticator from site configuration.
        """       
        authentication_policy = Configuration.policy("authentication")
        if not authentication_policy:
            raise CannotLoadConfiguration(
                "No authentication policy given."
            )
        bearer_token_signing_secret = authentication_policy.get(
            'bearer_token_signing_secret'
        )
        providers = authentication_policy.get('providers')
        if isinstance(providers, basestring):
            providers = [providers]

        # Start with an empty list of authenticators.
        authenticator = cls(
            bearer_token_signing_secret=bearer_token_signing_secret
        )

        # Load each provider.
        for provider_string in providers:
            authenticator.load_provider(provider_string)

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
        self.assert_ready_for_oauth()
        return authenticator

    def __init__(self, basic_auth_provider=None, oauth_providers=None,
                 bearer_token_signing_secret=None):
        """Initialize an Authenticator from a list of AuthenticationProviders.

        :param basic_auth_provider: The AuthenticatonProvider that handles
        HTTP Basic Auth requests.

        :param oauth_providers: A list of AuthenticationProviders that handle
        OAuth requests.

        :param jwt_secret: The secret to use when signing JWTs for use
        as bearer tokens.
        """
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
                
    def register_provider(self, provider_string):
        """Turn a description of a provider into an AuthenticationProvider
        object, and register it.
        """
        provider_module = importlib.import_module(provider_string)
        provider_class = getattr(provider_module, "AuthenticationProvider")
        if isinstance(provider_class, BasicAuthenticationProvider):
            provider = provider_class.from_config()
            self.register_basic_auth_provider(provider)
        elif isinstance(provider_class, OAuthProvider):
            provider = provider_class.from_config()
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

        opds_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(netloc)))

        links = {}
        for rel, value in (
                ("terms-of-service", Configuration.terms_of_service_url()),
                ("privacy-policy", Configuration.privacy_policy_url()),
                ("copyright", Configuration.acknowledgements_url()),
                ("about", Configuration.about_url()),
        ):
            if value:
                links[rel] = dict(href=value, type="text/html")

        doc = OPDSAuthenticationDocument.fill_in(
            base_opds_document, list(self.providers),
            name=unicode(_("Library")), id=opds_id, links=links,
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

    # A subclass MUST define a value for URI. This is used in the
    # Authentication for OPDS document to distinguish between
    # different types of authentication.
    URI = None
    
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
        if patron.needs_metadata_update:
            self.update_patron_metadata(patron)
        if not patron.has_borrowing_privileges:
            # TODO: This should be checked at the point the patron
            # actually does something that requires borrowing
            # privileges.
            return None
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
    # NOTE: Each subclass MUST define an attribute called
    # CONFIGURATION_NAME, which is the name used to configure that
    # subclass in the configuration file. Failure to define this
    # attribute will result in an error in .from_config().
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

    # By default, patron identifiers can only contain alphanumerics and
    # a few other characters. By default, there are no restrictions on
    # passwords.
    alphanumerics_plus = re.compile("^[A-Za-z0-9@.-]+$")
    DEFAULT_IDENTIFIER_REGULAR_EXPRESSION = alphanumerics_plus
    DEFAULT_PASSWORD_REGULAR_EXPRESSION = None
   
    @classmethod
    def config_values(cls, configuration_name=None, required=False):
        """Retrieve constructor values from site configuration.

        Can be overridden from a subclass to pull additional values.

        :param required: Whether or not the absence of any configuration
        should be considered an error.
        """
        configuration_name = configuration_name or cls.CONFIGURATION_NAME
        config = Configuration.integration(
            configuration_name, required=required
        )
        args = dict()
        if config:
            args['identifier_re'] = config.get(
                Configuration.IDENTIFIER_REGULAR_EXPRESSION,
                cls.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION
            )
            args['password_re'] = config.get(
                Configuration.PASSWORD_REGULAR_EXPRESSION,
                cls.DEFAULT_PASSWORD_REGULAR_EXPRESSION
            )
            args['test_username'] = config.get(
                Configuration.AUTHENTICATION_TEST_USERNAME,
                None
            )
            args['test_password'] = config.get(
                Configuration.AUTHENTICATION_TEST_PASSWORD,
                None
            )
        return config, args
        
    
    @classmethod
    def from_config(cls):
        """Load a BasicAuthenticationProvider from site configuration."""
        config, args = cls.config_values()
        return cls(**args)

    def __init__(self, identifier_re=None, password_re=None,
                 test_username=None, test_password=None):
        """Create a BasicAuthenticationProvider.
        """
        if identifier_re:
            identifier_re = re.compile(identifier_re)
        if password_re:
            password_re = re.compile(password_re)

        self.identifier_re = identifier_re
        self.password_re = password_re
        self.test_username = test_username
        self.test_password = test_password
        self.log = logging.getLogger(self.CONFIGURATION_NAME)
        
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
            patron = patrondata.create(_db)
            
        # The lookup failed in the first place either because the
        # Patron did not exist on the local side, or because one of
        # the patron's identifiers changed. Either way, we need to
        # update the Patron record with the account information we
        # just got from the source of truth.
        patrondata.apply(patron)
        return patron
    
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
    # CONFIGURATION_NAME, which is the name used to configure that
    # subclass in the configuration file. Failure to define this
    # attribute will result in an error in .from_config().
    #
    # Each subclass MUST define an attribute called TOKEN_TYPE, which
    # is the name of the JWT given to patrons for use as a bearer
    # token.
    #
    # Each subclass MUST define an attribute called
    # TOKEN_DATA_SOURCE_NAME, which is the name of the DataSource
    # under which bearer tokens for patrons will be registered.
    
    METHOD = "http://librarysimplified.org/authtype/OAuth-with-intermediary"
    
    # After verifying the patron's OAuth credentials, we send them a
    # token. This is how long they can use that token before we check
    # their OAuth credentials again.
    DEFAULT_TOKEN_EXPIRATION_DAYS = 42
    
    @classmethod
    def from_config(cls):
        """Load this OAuthAuthenticationProvider from the site configuration.
        """
        config = Configuration.integration(
            cls.CONFIGURATION_NAME, required=True
        )
        client_id = config.get(Configuration.OAUTH_CLIENT_ID)
        client_secret = config.get(Configuration.OAUTH_CLIENT_SECRET)
        token_expiration_days = config.get(
            Configuration.OAUTH_TOKEN_EXPIRATION_DAYS,
            cls.DEFAULT_TOKEN_EXPIRATION_DAYS
        )
        return cls(client_id, client_secret, token_expiration_days)
    
    def __init__(self, client_id, client_secret, token_expiration_days):
        """Initialize this OAuthAuthenticationProvider.

        :param client_id: An ID given to us by the OAuth provider, used
            to distinguish between us and its other clients.
        :param client_secret: A secret key given to us by the OAuth 
            provider, used to validate that we are who we say we are.
        :param token_expiration_days: This many days may elapse before
            we ask the patron to go through the OAuth validation
            process again.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_expiration_days = token_expiration_days

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
        credential = Credential.lookup_by_token(
            _db, self.token_data_source(_db), self.TOKEN_TYPE, token
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
        data_source = self.token_data_source(_db)
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

    def external_authenticate_url(self, client_redirect_uri):
        """Generate the URL provided by the OAuth provider which will present
        the patron with a login form.

        :param client_redirect_url: Tell the OAuth provider to
        redirect the authenticated patron to this URL.
        """
        raise NotImplementedError()

    def oauth_callback(self, _db, params):
        """Verify the incoming parameters with the OAuth provider.

        :return: A 3-tuple (Credential, Patron, PatronData). The
        Credential contains the access token provided by the OAuth
        provider. (This is not the bearer token the patron will use to
        authenticate -- that is a JWT token _based_ on this token,
        created in
        OAuthAuthenticationController.oauth_authentication_callback). The
        Patron object represents the authenticated Patron, and the
        PatronData object may include information about the patron
        obtained from the OAuth provider which cannot be stored in the
        circulation manager's database but which should be passed on
        to the client.
        """
        # TODO: should probably be some code in here that implements
        # the token_expiration_days thing.
        raise NotImplementedError()

    def _internal_authenticate_url(self):
        """A patron who wants to log in should hit this URL on the circulation
        manager. They'll be redirected to the OAuth provider, which will 
        take care of it.
        """
        return url_for('oauth_authenticate', _external=True, provider=self.NAME)

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
        # TODO: This is one of those situations where we'd like to
        # create the data source if it doesn't already exist.
        return DataSource.lookup(_db, self.TOKEN_DATA_SOURCE_NAME)


class OAuthController(object):

    """A controller for handling requests that are part of the OAuth
    credential dance.
    """
    
    def __init__(self, authenticator):
        self.authenticator = authenticator

    def oauth_authentication_redirect(self, params):
        """Redirect an unauthenticated patron to the authentication URL of the
        appropriate OAuth provider.

        Over on that other site, the patron will authenticate and be
        redirected back to the circulation manager (the URL is stored
        in params['redirect_uri']), ending up in
        oauth_authentication_callback.
        """
        redirect_uri = params.get('redirect_uri') or ""

        provider_name = params.get('provider')
        provider = self.authenticator.oauth_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)
        state = dict(provider=provider.NAME, redirect_uri=redirect_uri)
        return redirect(provider.external_authenticate_url(json.dumps(state)))

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

        client_redirect_uri = state.get('redirect_uri') or ""
        provider_name = state.get('provider')
        provider = self.oauth_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        # Send the incoming parameters to the OAuth provider and get
        # back a provider token (a Credential object), the
        # authenticated patron (a Patron object), and a PatronData
        # including any personal information obtained from the OAuth
        # provider (such as patron name) which we can't store in the
        # database.
        response = provider.oauth_callback(_db, params)
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
            provider.NAME, provider_token
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
        return redirect(self._error_uri, redirect_uri, pd)
    
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
