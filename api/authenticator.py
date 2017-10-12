from nose.tools import set_trace
from config import (
    Configuration,
    CannotLoadConfiguration,
)
from core.opds import OPDSFeed
from core.model import (
    get_one,
    get_one_or_create,
    CirculationEvent,
    ConfigurationSetting,
    Credential,
    DataSource,
    ExternalIntegration,
    Library,
    Patron,
    Session,
)
from core.util.problem_detail import (
    ProblemDetail,
    json as pd_json,
)
from core.util.authentication_for_opds import (
    AuthenticationForOPDSDocument,
    OPDSAuthenticationFlow,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import or_
from problem_details import *
from util.patron import PatronUtility
from api.opds import CirculationManagerAnnotator

import datetime
import logging
from money import Money
import os
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
    EXCESSIVE_FINES = 'excessive fines'
    EXCESSIVE_FEES = 'excessive fees'
    NO_BORROWING_PRIVILEGES = 'no borrowing privileges'
    TOO_MANY_LOANS = 'too many active loans'
    TOO_MANY_RENEWALS = 'too many renewals'
    TOO_MANY_OVERDUE = 'too many items overdue'
    TOO_MANY_LOST = 'too many items lost'

    # Patron is being billed for too many items (as opposed to
    # excessive fines, which means patron's fines have exceeded a
    # certain amount).
    TOO_MANY_ITEMS_BILLED = 'too many items billed'

    # Patron was asked to return an item so someone else could borrow it,
    # but didn't return the item.
    RECALL_OVERDUE = 'recall overdue'
    
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
        self.set_value(patron, 'external_type', self.external_type)
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
        
    def get_or_create_patron(self, _db, library_id, analytics=None):
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

        :param analytics: Analytics instance to track the new patron
            creation event.
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

        if is_new and analytics:
            # Send out an analytics event to record the fact
            # that a new patron was created.
            analytics.collect_event(patron.library, None,
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
    """Route requests to the appropriate LibraryAuthenticator.
    """

    def __init__(self, _db, analytics=None):
        self.library_authenticators = {}

        self.populate_authenticators(_db, analytics)

    @property
    def current_library_short_name(self):
        return flask.request.library.short_name

    def populate_authenticators(self, _db, analytics):
        for library in _db.query(Library):
            self.library_authenticators[library.short_name] = LibraryAuthenticator.from_config(_db, library, analytics)

    def invoke_authenticator_method(self, method_name, *args, **kwargs):
        short_name = self.current_library_short_name
        if short_name not in self.library_authenticators:
            return LIBRARY_NOT_FOUND
        return getattr(self.library_authenticators[short_name], method_name)(*args, **kwargs)

    def authenticated_patron(self, _db, header):
        return self.invoke_authenticator_method("authenticated_patron", _db, header)

    def create_authentication_document(self):
        return self.invoke_authenticator_method("create_authentication_document")

    def create_authentication_headers(self):
        return self.invoke_authenticator_method("create_authentication_headers")

    def get_credential_from_header(self, header):
        return self.invoke_authenticator_method("get_credential_from_header", header)

    def create_bearer_token(self, *args, **kwargs):
        return self.invoke_authenticator_method(
            "create_bearer_token", *args, **kwargs
        )

    def oauth_provider_lookup(self, *args, **kwargs):
        return self.invoke_authenticator_method(
            "oauth_provider_lookup", *args, **kwargs
        )

    def decode_bearer_token(self, *args, **kwargs):
        return self.invoke_authenticator_method(
            "decode_bearer_token", *args, **kwargs
        )


class LibraryAuthenticator(object):
    """Use the registered AuthenticationProviders to turn incoming
    credentials into Patron objects.
    """    

    @classmethod
    def from_config(cls, _db, library, analytics=None):
        """Initialize an Authenticator for the given Library based on its
        configured ExternalIntegrations.
        """
        # Start with an empty list of authenticators.
        authenticator = cls(_db=_db, library=library)

        # Find all of this library's ExternalIntegrations set up with
        # the goal of authenticating patrons.
        integrations = _db.query(ExternalIntegration).join(
            ExternalIntegration.libraries).filter(
            ExternalIntegration.goal==ExternalIntegration.PATRON_AUTH_GOAL
        ).filter(
            Library.id==library.id
        )
        # Turn each such ExternalIntegration into an
        # AuthenticationProvider.
        for integration in integrations:
            try:
                authenticator.register_provider(integration, analytics)
            except (ImportError, CannotLoadConfiguration), e:
                # These are the two types of error that might be caused
                # by misconfiguration, as opposed to bad code.
                authenticator.initialization_exceptions[integration.id] = e
                
        if authenticator.oauth_providers_by_name:
            # NOTE: this will immediately commit the database session,
            # which may not be what you want during a test. To avoid
            # this, you can create the bearer token signing secret as
            # a regular site-wide ConfigurationSetting.
            authenticator.bearer_token_signing_secret = OAuthAuthenticationProvider.bearer_token_signing_secret(
                _db
            )
        authenticator.assert_ready_for_oauth()
        return authenticator

    def __init__(self, _db, library, basic_auth_provider=None,
                 oauth_providers=None,
                 bearer_token_signing_secret=None):
        """Initialize a LibraryAuthenticator from a list of AuthenticationProviders.

        :param _db: A database session (probably a scoped session, which is
            why we can't derive it from `library`)

        :param library: The Library to which this LibraryAuthenticator guards
        access.

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
        self.library_short_name = library.short_name

        self.basic_auth_provider = basic_auth_provider
        self.oauth_providers_by_name = dict()
        self.bearer_token_signing_secret = bearer_token_signing_secret
        self.initialization_exceptions = dict()
        if oauth_providers:
            for provider in oauth_providers:
                self.oauth_providers_by_name[provider.NAME] = provider
        self.assert_ready_for_oauth()

    @property
    def library(self):
        return Library.by_id(self._db, self.library_id)
        
    def assert_ready_for_oauth(self):
        """If this LibraryAuthenticator has OAuth providers, ensure that it
        also has a secret it can use to sign bearer tokens.
        """
        if self.oauth_providers_by_name and not self.bearer_token_signing_secret:
            raise CannotLoadConfiguration(
                "OAuth providers are configured, but secret for signing bearer tokens is not."
            )
                
    def register_provider(self, integration, analytics=None):
        """Turn an ExternalIntegration object into an AuthenticationProvider
        object, and register it.

        :param integration: An ExternalIntegration that configures
        a way of authenticating patrons.
        """           
        if integration.goal != integration.PATRON_AUTH_GOAL:
            raise CannotLoadConfiguration(
                "Was asked to register an integration with goal=%s as though it were a way of authenticating patrons." % integration.goal
            )

        library = self.library
        if library not in integration.libraries:
            raise CannotLoadConfiguration(
                "Was asked to register an integration with library %s, which doesn't use it." % library.name
            )
        
        module_name = integration.protocol
        if not module_name:
            # This should be impossible since protocol is not nullable.
            raise CannotLoadConfiguration(
                "Authentication provider configuration does not specify protocol."
            )
        provider_module = importlib.import_module(module_name)
        provider_class = getattr(provider_module, "AuthenticationProvider", None)
        if not provider_class:
            raise CannotLoadConfiguration(
                "Loaded module %s but could not find a class called AuthenticationProvider inside." % module_name
            )
        provider = provider_class(self.library, integration, analytics)
        if issubclass(provider_class, BasicAuthenticationProvider):
            self.register_basic_auth_provider(provider)
            # TODO: Run a self-test, or at least check that we have
            # the ability to run one.
        elif issubclass(provider_class, OAuthAuthenticationProvider):
            self.register_oauth_provider(provider)
        else:
            raise CannotLoadConfiguration(
                "Authentication provider %s is neither a BasicAuthenticationProvider nor an OAuthAuthenticationProvider. I can create it, but not sure where to put it." % provider_class
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

    def authentication_document_url(self, library):
        """Return the URL of the authentication document for the
        given library.
        """
        return url_for(
            "authentication_document", library_short_name=library.short_name,
            _external=True
        )

    def create_authentication_document(self):
        """Create the Authentication For OPDS document to be used when
        a request comes in with no authentication.
        """
        links = []
        library = Library.by_id(self._db, self.library_id)

        # Add the same links that we would show in an OPDS feed, plus
        # some extra like 'registration' that are specific to Authentication
        # For OPDS.
        for rel in (CirculationManagerAnnotator.CONFIGURATION_LINKS +
                    Configuration.AUTHENTICATION_FOR_OPDS_LINKS):
            value = ConfigurationSetting.for_library(rel, library).value
            if not value:
                continue
            link = dict(rel=rel, href=value)
            if any(value.startswith(x) for x in ('http:', 'https:')):
                # We assume that HTTP URLs lead to HTML, but we don't
                # assume anything about other URL schemes.
                link['type'] = "text/html"
            links.append(link)

        # Add a rel="start" link pointing to the root OPDS feed.
        index_url = url_for("index", _external=True,
                            library_short_name=library.short_name)
        links.append(
            dict(rel="start", href=index_url, 
                 type=OPDSFeed.ACQUISITION_FEED_TYPE)
        )
                
        # Add a rel="help" link for every type of URL scheme that
        # leads to library-specific help.
        for type, uri in Configuration.help_uris(library):
            links.append(dict(rel="help", href=uri, type=type))

        # Add a link to the web page of the library itself.
        library_uri = ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library).value
        if library_uri:
            links.append(
                dict(rel="alternate", type="text/html", href=library_uri)
            )

        # Add the library's logo, if it has one.
        logo = ConfigurationSetting.for_library(
            Configuration.LOGO, library).value
        if logo:
            links.append(dict(rel="logo", type="image/png", href=logo))
                
        library_name = self.library_name or unicode(_("Library"))
        auth_doc_url = self.authentication_document_url(library)
        doc = AuthenticationForOPDSDocument(
            id=auth_doc_url, title=library_name,
            authentication_flows=list(self.providers),
            links=links
        ).to_dict(self._db)

        # Add the library's color scheme, if it has one.
        description = ConfigurationSetting.for_library(
            Configuration.COLOR_SCHEME, library).value
        if description:
            doc['color_scheme'] = description

        # Add the description of the library as the OPDS feed's
        # service_description.
        description = ConfigurationSetting.for_library(
            Configuration.LIBRARY_DESCRIPTION, library).value
        if description:
            doc['service_description'] = description

        # Add the library's public key, if it has one.
        public_key = ConfigurationSetting.for_library(
            Configuration.PUBLIC_KEY, library).value
        if public_key:
            doc["public_key"] = dict(type="RSA", value=public_key)
        
        # Add feature flags to signal to clients what features they should
        # offer.
        enabled = []
        disabled = []
        if library.allow_holds:
            bucket = enabled
        else:
            bucket = disabled
        bucket.append(Configuration.RESERVATIONS_FEATURE)
        doc['features'] = dict(enabled=enabled, disabled=disabled)
        return json.dumps(doc)

    def create_authentication_headers(self):
        """Create the HTTP headers to return with the OPDS
        authentication document."""
        library = Library.by_id(self._db, self.library_id)
        headers = Headers()
        headers.add('Content-Type', AuthenticationForOPDSDocument.MEDIA_TYPE)
        headers.add('Link', "<%s>; rel=%s" % (
            self.authentication_document_url(library),
            AuthenticationForOPDSDocument.LINK_RELATION
        ))
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


class AuthenticationProvider(OPDSAuthenticationFlow):
    """Handle a specific patron authentication scheme.
    """

    # NOTE: Each subclass MUST define an attribute called NAME, which
    # is displayed in the admin interface when configuring patron auth,
    # used to create the name of the log channel used by this
    # subclass, used to distinguish between tokens from different
    # OAuth providers, etc.

    # Each subclass SHOULD define an attribute called DESCRIPTION, which
    # is displayed in the admin interface when an admin is configuring
    # the authentication provider.
    DESCRIPTION = ""

    # Each subclass MUST define a value for FLOW_TYPE. This is used in the
    # Authentication for OPDS document to distinguish between
    # different types of authentication.
    FLOW_TYPE = None

    # Each authentication mechanism may have a list of SETTINGS that
    # must be configured for that mechanism, and may have a list of
    # LIBRARY_SETTINGS that must be configured for each library using that
    # mechanism. Each setting must have a key that is used to store the
    # setting in the database, and a label that is displayed when configuring
    # the authentication mechanism in the admin interface.
    # For example: { "key": "username", "label": _("Client ID") }.
    # A setting is required by default, but may have "optional" set to True.

    SETTINGS = []

    # Each library and authentication mechanism may have a regular
    # expression for deriving a patron's external type from their
    # authentication identifier.
    EXTERNAL_TYPE_REGULAR_EXPRESSION = 'external_type_regular_expression'

    # When multiple libraries share an ILS, a person may be able to
    # authenticate with the ILS but not be considered a patron of
    # _this_ library. This setting contains the rule for determining
    # whether an identifier is valid for a specific library.
    #
    # Usually this is a prefix string which is compared against the
    # patron's identifiers, but if the string starts with a carat (^)
    # it will be interpreted as a regular expression.
    PATRON_IDENTIFIER_RESTRICTION = 'patron_identifier_restriction'
    
    LIBRARY_SETTINGS = [
        { "key": EXTERNAL_TYPE_REGULAR_EXPRESSION,
          "label": _("External Type Regular Expression"),
          "description": _("Derive a patron's type from their identifier."),
          "optional": True,
        },
        { "key": PATRON_IDENTIFIER_RESTRICTION,
          "label": _("Patron identifier prefix"),
          "description": _("<p>When multiple libraries share an ILS, a person may be able to " +
                           "authenticate with the ILS but not be considered a patron of " +
                           "<i>this</i> library. This setting contains the rule for determining " +
                           "whether an identifier is valid for this specific library.</p>" +
                           "<p>Usually this is a prefix string which is compared against the " +
                           "patron's identifiers, but if the string starts with a carat (^) " +
                           "it will be interpreted as a regular expression."),
          "optional": True,
        }
    ]

    def __init__(self, library, integration, analytics=None):
        """Basic constructor.
        
        :param library: Patrons authenticated through this provider
        are associated with this Library. Don't store this object!
        It's associated with a scoped database session. Just pull
        normal Python objects out of it.

        :param integration: The ExternalIntegration that
        configures this AuthenticationProvider. Don't store this
        object! It's associated with a scoped database session. Just
        pull normal Python objects out of it.
        """
        if not isinstance(library, Library):
            raise Exception(
                "Expected library to be a Library, got %r" % library
            )
        if not isinstance(integration, ExternalIntegration):
            raise Exception(
                "Expected integration to be an ExternalIntegration, got %r" % integration
            )

        self.library_id = library.id
        self.log = logging.getLogger(self.NAME)
        self.analytics = analytics
        # If there's a regular expression that maps authorization
        # identifier to external type, find it now.
        _db = Session.object_session(library)
        regexp = ConfigurationSetting.for_library_and_externalintegration(
            _db, self.EXTERNAL_TYPE_REGULAR_EXPRESSION, library, integration
        ).value
        if regexp:
            try:
                regexp = re.compile(regexp)
            except Exception, e:
                self.log.error(
                    "Could not configure external type regular expression: %r", e
                )
                regexp = None
        self.external_type_regular_expression = regexp

        restriction = ConfigurationSetting.for_library_and_externalintegration(
            _db, self.PATRON_IDENTIFIER_RESTRICTION, library, integration
        ).value
        if restriction and restriction.startswith("^"):
            # Interpret the value a regular expression
            try:
                restriction = re.compile(restriction)
            except Exception, e:
                self.log.error(
                    "Could not interpret identifier restriction as a regular expression: %r", e
                )
        self.patron_identifier_restriction = restriction

    @classmethod
    def _restriction_matches(cls, identifier, restriction):
        """Does the given patron identifier match the given 
        patron identifier restriction?
        """
        if not restriction:
            # No restriction -- anything matches.
            return True
        if not identifier:
            # No identifier -- it won't match any restriction.
            return False
        if isinstance(restriction, basestring):
            # It's a prefix string.
            if not identifier.startswith(restriction):
                # The prefix doesn't match.
                return False
        else:
            # It's a regexp.
            if not restriction.search(identifier):
                # The regex doesn't match.
                return False
        return True
    
    def patron_identifier_restriction_matches(self, identifier):
        """Does the given patron identifier match the configured
        patron identifier restriction?
        """
        return self._restriction_matches(
            identifier, self.patron_identifier_restriction
        )
    
    def library(self, _db):
        return Library.by_id(_db, self.library_id)
    
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
            self.apply_patrondata(remote_patron_info, patron)

    def update_patron_external_type(self, patron):
        """Make sure the patron's external type reflects
        what external_type_regular_expression says.
        """
        if not self.external_type_regular_expression:
            # External type is not determined by a regular expression.
            return
        if not patron.authorization_identifier:
            # Patron has no authorization identifier. Leave their
            # external_type alone.
            return

        match = self.external_type_regular_expression.search(
            patron.authorization_identifier
        )
        if not match:
            # Patron's authorization identifier doesn't match the
            # regular expression at all. Leave their external_type
            # alone.
            return
        groups = match.groups()
        if not groups:
            # The regular expression matched but didn't contain any groups.
            # This is a configuration error; do nothing.
            return
            
        patron.external_type = groups[0]

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
    
    def _authentication_flow_document(self, _db):
        """Create a Authentication Flow object for use in an Authentication for
        OPDS document.

        :return: A dictionary suitable for inclusion as one of the
        'authentication' list in an Authentication for OPDS document.

        For example:

        {
          "authentication": [
            { "type": "http://opds-spec.org/auth/basic",
              "labels": {"login": "Barcode", "password": "PIN"} }
          ]
        }
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
    # Each subclass MAY override the default values for
    # DEFAULT_LOGIN_LABEL and DEFAULT_PASSWORD_LABEL. These become the
    # default human-readable labels for username and password in the
    # OPDS authentication document
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
    AUTHENTICATION_REALM = _("Library card")
    FLOW_TYPE = "http://opds-spec.org/auth/basic"
    NAME = 'Generic Basic Authentication provider'
   
    # By default, patron identifiers can only contain alphanumerics and
    # a few other characters. By default, there are no restrictions on
    # passwords.
    alphanumerics_plus = re.compile("^[A-Za-z0-9@.-]+$")
    DEFAULT_IDENTIFIER_REGULAR_EXPRESSION = alphanumerics_plus
    DEFAULT_PASSWORD_REGULAR_EXPRESSION = None

    # Configuration settings that are common to all Basic Auth-type
    # authentication techniques.
    #
    
    # Identifiers can be presumed invalid if they don't match
    # this regular expression.
    IDENTIFIER_REGULAR_EXPRESSION = 'identifier_regular_expression'

    # Passwords can be presumed invalid if they don't match this regular
    # expression.
    PASSWORD_REGULAR_EXPRESSION = 'password_regular_expression'

    # The client should prefer one keyboard over another.
    IDENTIFIER_KEYBOARD = 'identifier_keyboard'
    PASSWORD_KEYBOARD = 'password_keyboard'

    # Constants describing different types of keyboards.
    DEFAULT_KEYBOARD = "Default"
    EMAIL_ADDRESS_KEYBOARD = "Email address"
    NUMBER_PAD = "Number pad"

    # The identifier and password can have a maximum
    # supported length.
    IDENTIFIER_MAXIMUM_LENGTH = "identifier_maximum_length"
    PASSWORD_MAXIMUM_LENGTH = "password_maximum_length"
    
    # The client should use a certain string when asking for a patron's
    # "identifier" and "password"
    IDENTIFIER_LABEL = 'identifier_label'
    PASSWORD_LABEL = 'password_label'
    DEFAULT_IDENTIFIER_LABEL = "Barcode"
    DEFAULT_PASSWORD_LABEL = "PIN"

    # If the identifier label is one of these strings, it will be
    # automatically localized. Otherwise, the same label will be displayed
    # to everyone.
    COMMON_IDENTIFIER_LABELS = {
        "Barcode": _("Barcode"),
        "Email Address": _("Email Address"),
        "Username": _("Username"),
        "Library Card": _("Library Card"),
        "Card Number": _("Card Number"),
    }

    # If the password label is one of these strings, it will be
    # automatically localized. Otherwise, the same label will be
    # displayed to everyone.
    COMMON_PASSWORD_LABELS = {
        "Password": _("Password"),
        "PIN": _("PIN"),
    }
    
    # These identifier and password are supposed to be valid
    # credentials.  If there's a problem using them, there's a problem
    # with the authenticator or with the way we have it configured.
    TEST_IDENTIFIER = 'test_identifier'
    TEST_PASSWORD = 'test_password'

    SETTINGS = [
        { "key": TEST_IDENTIFIER,
          "label": _("Test Identifier"),
          "description": _("A valid identifier that can be used to test that patron authentication is working.") },
        { "key": TEST_PASSWORD, "label": _("Test Password"), "description": _("The password for the test identifier.") },
        { "key": IDENTIFIER_REGULAR_EXPRESSION,
          "label": _("Identifier Regular Expression"),
          "description": _("A patron's identifier will be immediately rejected if it doesn't match this regular expression."),
          "optional": True },
        { "key": PASSWORD_REGULAR_EXPRESSION,
          "label": _("Password Regular Expression"),
          "description": _("A patron's password will be immediately rejected if it doesn't match this regular expression."),
          "optional": True },
        { "key": IDENTIFIER_KEYBOARD,
          "label": _("Keyboard for identifier entry"),
          "type": "select",
          "options": [
              { "key": DEFAULT_KEYBOARD, "label": _("System default") },
              { "key": EMAIL_ADDRESS_KEYBOARD,
                "label": _("Email address entry") },
              { "key": NUMBER_PAD, "label": _("Number pad") },
          ],
          "default": DEFAULT_KEYBOARD
        },
        { "key": PASSWORD_KEYBOARD,
          "label": _("Keyboard for password entry"),
          "type": "select",
          "options": [
              { "key": DEFAULT_KEYBOARD, "label": _("System default") },
              { "key": NUMBER_PAD, "label": _("Number pad") },
          ],
          "default": DEFAULT_KEYBOARD
        },
        { "key": IDENTIFIER_MAXIMUM_LENGTH,
          "label": _("Maximum identifier length"),
          "type": "number",
          "optional": True,
        },
        { "key": PASSWORD_MAXIMUM_LENGTH,
          "label": _("Maximum password length"),
          "type": "number",
          "optional": True,
        },
        { "key": IDENTIFIER_LABEL,
          "label": _("Label for identifier entry"),
          "optional": True,
        },
        { "key": PASSWORD_LABEL,
          "label": _("Label for password entry"),
          "optional": True,
        },
    ] + AuthenticationProvider.SETTINGS
    
    # Used in the constructor to signify that the default argument
    # value for the class should be used (as distinct from None, which
    # indicates that no value should be used.)
    class_default = object()
    
    def __init__(self, library, integration, analytics=None):
        """Create a BasicAuthenticationProvider.

        :param library: Patrons authenticated through this provider
        are associated with this Library. Don't store this object!
        It's associated with a scoped database session. Just pull
        normal Python objects out of it.

        :param externalintegration: The ExternalIntegration that
        configures this AuthenticationProvider. Don't store this
        object! It's associated with a scoped database session. Just
        pull normal Python objects out of it.
        """
        super(BasicAuthenticationProvider, self).__init__(library, integration, analytics)
        identifier_regular_expression = integration.setting(
            self.IDENTIFIER_REGULAR_EXPRESSION
        ).value or self.DEFAULT_IDENTIFIER_REGULAR_EXPRESSION

        if identifier_regular_expression:
            identifier_regular_expression = re.compile(
                identifier_regular_expression
            )
        self.identifier_re = identifier_regular_expression
        
        password_regular_expression = integration.setting(
            self.PASSWORD_REGULAR_EXPRESSION
        ).value or self.DEFAULT_PASSWORD_REGULAR_EXPRESSION
        if password_regular_expression:
            password_regular_expression = re.compile(
                password_regular_expression
            )            
        self.password_re = password_regular_expression

        self.test_username = integration.setting(self.TEST_IDENTIFIER).value
        self.test_password = integration.setting(self.TEST_PASSWORD).value

        self.identifier_maximum_length = integration.setting(
            self.IDENTIFIER_MAXIMUM_LENGTH).int_value
        self.password_maximum_length = integration.setting(
            self.PASSWORD_MAXIMUM_LENGTH).int_value
        self.identifier_keyboard = integration.setting(
            self.IDENTIFIER_KEYBOARD).value or self.DEFAULT_KEYBOARD
        self.password_keyboard = integration.setting(
            self.PASSWORD_KEYBOARD).value or self.DEFAULT_KEYBOARD
        
        self.identifier_label = (
            integration.setting(self.IDENTIFIER_LABEL).value
            or self.DEFAULT_IDENTIFIER_LABEL
        )
        self.password_label = (
            integration.setting(self.PASSWORD_LABEL).value
            or self.DEFAULT_PASSWORD_LABEL
        )
        
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
        server_side_validation_result = self.server_side_validation(
            username, password
        )
        if not server_side_validation_result:
            # False => None
            server_side_validation_result = None
        if (not server_side_validation_result
            or isinstance(server_side_validation_result, ProblemDetail)):
            # The credentials are prima facie invalid and do not
            # need to be checked with the source of truth.
            return server_side_validation_result

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
            self.apply_patrondata(patrondata, patron)
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
                _db, self.library_id, analytics=self.analytics
            )
            
        # The lookup failed in the first place either because the
        # Patron did not exist on the local side, or because one of
        # the patron's identifiers changed. Either way, we need to
        # update the Patron record with the account information we
        # just got from the source of truth.
        self.apply_patrondata(patrondata, patron)
        return patron

    def apply_patrondata(self, patrondata, patron):
        """Apply a PatronData object to the given patron and make sure
        any fields that need to be updated as a result of new data
        are updated.
        """
        patrondata.apply(patron)
        if self.external_type_regular_expression:
            self.update_patron_external_type(patron)

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
        if not self.patron_identifier_restriction_matches(username):
            # Don't apply any other checks -- they have the wrong library.
            return PATRON_OF_ANOTHER_LIBRARY

        if self.identifier_re:
            valid = valid and username is not None and (
                self.identifier_re.match(username) is not None
            )
        if self.password_re:
            valid = valid and password is not None and (
                self.password_re.match(password) is not None
            )
        
        if self.identifier_maximum_length:
            valid = valid and (len(username) <= self.identifier_maximum_length)

        if self.password_maximum_length:
            valid = valid and password and (len(password) <= self.password_maximum_length)
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

        # We're going to try a number of different strategies to look
        # up the appropriate patron based on PatronData. In theory we
        # could employ all these strategies at once (see the code at
        # the end of this method), but if the source of truth is
        # well-behaved, the first available lookup should work, and if
        # it's not, it's better to check the more reliable mechanisms
        # before the less reliable.
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

        patron = None
        for lookup in lookups:
            lookup['library_id'] = self.library_id
            patron = get_one(_db, Patron, **lookup)
            if patron:
                # We found them!
                break            

        if not patron and username:
            # This is a Basic Auth username, but it might correspond
            # to either Patron.authorization_identifier or
            # Patron.username.
            #
            # NOTE: If patrons are allowed to choose their own
            # usernames, it's possible that a username and
            # authorization_identifier can conflict. In that case it's
            # undefined which Patron is returned from this query. If
            # this happens, it's a problem with the ILS and needs to
            # be resolved over there.
            clause = or_(Patron.authorization_identifier==username,
                         Patron.username==username)
            qu = _db.query(Patron).filter(clause).limit(1)
            try:
                patron = qu.one()
            except NoResultFound:
                patron = None
        return patron

    @property
    def authentication_header(self):
        return 'Basic realm="%s"' % self.AUTHENTICATION_REALM
    
    def _authentication_flow_document(self, _db):
        """Create a Authentication Flow object for use in an Authentication for
        OPDS document.
        """

        login_inputs = dict(keyboard=self.identifier_keyboard)
        if self.identifier_maximum_length:
            login_inputs['maximum_length'] = self.identifier_maximum_length

        password_inputs = dict(keyboard=self.password_keyboard)
        if self.password_maximum_length:
            login_inputs['maximum_length'] = self.password_maximum_length

        # Localize the labels if possible.
        localized_identifier_label = self.COMMON_IDENTIFIER_LABELS.get(
            self.identifier_label,
            self.identifier_label
        )
        localized_password_label = self.COMMON_PASSWORD_LABELS.get(
            self.password_label,
            self.password_label
        )
        return dict(
            description=unicode(self.DISPLAY_NAME),
            labels=dict(login=unicode(localized_identifier_label),
                        password=unicode(localized_password_label)),
            inputs = dict(login=login_inputs,
                          password=password_inputs)
        )

    
class OAuthAuthenticationProvider(AuthenticationProvider):

    # NOTE: Each subclass must define URI as per
    # AuthenticationProvider superclass. This is the URI used to
    # identify this particular authentication provider.
    #
    # Each subclass MAY define a value for FLOW_TYPE. This is the URI
    # used to identify the authentication mechanism in Authentication
    # For OPDS documents. The default is used to indicate the Library
    # Simplified variant of OAuth.
    #
    # Each subclass MUST define an attribute called
    # NAME, which is the name used to configure that
    # subclass in the configuration file. Failure to define this
    # attribute will result in an error in the constructor.
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
    
    FLOW_TYPE = "http://librarysimplified.org/authtype/OAuth-with-intermediary"
   
    # After verifying the patron's OAuth credentials, we send them a
    # token. This configuration setting controls how long they can use
    # that token before we check their OAuth credentials again.
    OAUTH_TOKEN_EXPIRATION_DAYS = 'token_expiration_days'

    # This is the default value for that configuration setting.
    DEFAULT_TOKEN_EXPIRATION_DAYS = 42

    SETTINGS = [
        { "key": OAUTH_TOKEN_EXPIRATION_DAYS, "label": _("Days until OAuth token expires"), "optional": True },
    ] + AuthenticationProvider.SETTINGS

    # Name of the site-wide ConfigurationSetting containing the secret
    # used to sign bearer tokens.
    BEARER_TOKEN_SIGNING_SECRET = Configuration.BEARER_TOKEN_SIGNING_SECRET

    @classmethod
    def bearer_token_signing_secret(cls, _db):
        """Find or generate the site-wide bearer token signing secret."""
        return ConfigurationSetting.sitewide_secret(
            _db, cls.BEARER_TOKEN_SIGNING_SECRET
        )
        
    def __init__(self, library, integration, analytics=None):
        """Initialize this OAuthAuthenticationProvider.

        :param library: Patrons authenticated through this provider
            are associated with this Library. Don't store this object!
            It's associated with a scoped database session. Just pull
            normal Python objects out of it.
        :param externalintegration: The ExternalIntegration that
            configures this AuthenticationProvider. Don't store this
            object! It's associated with a scoped database session. Just
            pull normal Python objects out of it.
        :param client_id: An ID given to us by the OAuth provider, used
            to distinguish between us and its other clients.
        :param client_secret: A secret key given to us by the OAuth 
            provider, used to validate that we are who we say we are.
        :param token_expiration_days: This many days may elapse before
            we ask the patron to go through the OAuth validation
            process again.
        """
        super(OAuthAuthenticationProvider, self).__init__(
            library, integration, analytics
        )
        self.client_id = integration.username
        self.client_secret = integration.password
        self.token_expiration_days = integration.setting(
            self.OAUTH_TOKEN_EXPIRATION_DAYS
        ).int_value or self.DEFAULT_TOKEN_EXPIRATION_DAYS
        
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

    def external_authenticate_url(self, state, _db):
        """Generate the URL provided by the OAuth provider which will present
        the patron with a login form.

        :param state: A state variable to be propagated through to the OAuth
        callback.
        """
        template = self.EXTERNAL_AUTHENTICATE_URL
        arguments = self.external_authenticate_url_parameters(state, _db)
        return template % arguments

    def external_authenticate_url_parameters(self, state, _db):
        """Arguments used to fill in the template EXTERNAL_AUTHENTICATE_URL.
        """
        library_short_name = self.library(_db).short_name
        return dict(
            client_id=self.client_id,
            state=state,
            # When the patron finishes logging in to the OAuth provider,
            # we want them to send the patron to this URL.
            oauth_callback_url=OAuthController.oauth_authentication_callback_url(library_short_name)
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
        token = self.remote_exchange_code_for_access_token(_db, code)
        if isinstance(token, ProblemDetail):
            return token
        
        # Now that we have a bearer token, use it to look up patron
        # information.
        patrondata = self.remote_patron_lookup(token)
        if isinstance(patrondata, ProblemDetail):
            return patrondata

        for identifier in patrondata.authorization_identifiers:
            if self.patron_identifier_restriction_matches(identifier):
                break
        else:
            # None of the patron's authorization identifiers match.
            # This patron was able to validate with the OAuth provider,
            # but they are not a patron of _this_ library.
            return PATRON_OF_ANOTHER_LIBRARY
            
        # Convert the PatronData into a Patron object.
        patron, is_new = patrondata.get_or_create_patron(
            _db, self.library_id, analytics=self.analytics
        )

        # Create a credential for the Patron.
        credential, is_new = self.create_token(_db, patron, token)
        return credential, patron, patrondata

    def remote_exchange_authorization_code_for_access_token(self, _db, code):
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
    
    def _internal_authenticate_url(self, _db):
        """A patron who wants to log in should hit this URL on the circulation
        manager. They'll be redirected to the OAuth provider, which will 
        take care of it.
        """
        library = self.library(_db)
        
        return url_for('oauth_authenticate', _external=True,
                       provider=self.NAME,
                       library_short_name=library.short_name)

    def _authentication_flow_document(self, _db):
        """Create a Authentication Flow object for use in an Authentication for
        OPDS document.

        Example:
        {
            "type": "http://librarysimplified.org/authtype/OAuth-with-intermediary"
            "description": "My OAuth Provider",
            "links": [
              { "rel" : "authenticate"
                "href": "https://circulation.library.org/oauth_authenticate?provider=MyOAuth" }
            ]
        }
        """
        flow_doc = dict(
            description=self.NAME,
            links=[dict(rel="authenticate",
                        href=self._internal_authenticate_url(_db))]
        )
        return flow_doc

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
    def oauth_authentication_callback_url(cls, library_short_name):
        """The URL to the oauth_authentication_callback controller.

        This is its own method because sometimes an
        OAuthAuthenticationProvider needs to send it to the OAuth
        provider to demonstrate that it knows which URL a patron was
        redirected to.
        """
        return url_for('oauth_callback', library_short_name=library_short_name, _external=True)
        
    def oauth_authentication_redirect(self, params, _db):
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
        return redirect(provider.external_authenticate_url(state, _db))

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
