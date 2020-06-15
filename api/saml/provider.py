import datetime
import json
import logging

from flask import url_for
from flask_babel import lazy_gettext as _

from api.authenticator import BaseSAMLAuthenticationProvider, PatronData
from api.problem_details import *
from api.saml.auth import SAMLAuthenticationManagerFactory
from api.saml.metadata import Subject, SubjectUIDExtractor, SubjectJSONEncoder
from core.model import Credential, get_one_or_create, DataSource
from core.util.problem_detail import ProblemDetail

SAML_INVALID_SUBJECT = pd(
    'http://librarysimplified.org/terms/problem/saml/invalid-subject',
    status_code=401,
    title=_('SAML invalid subject.'),
    detail=_('SAML invalid subject.')
)


class SAMLAuthenticationProvider(BaseSAMLAuthenticationProvider):
    """SAML authentication provider implementing Web Browser SSO profile using the following bindings:
    - HTTP-Redirect Binding for requests
    - HTTP-POST Binding for responses
    """

    def __init__(self, library, integration, analytics=None):
        """Initializes a new instance of SAMLAuthenticationProvider class

        :param library: Patrons authenticated through this provider
            are associated with this Library. Don't store this object!
            It's associated with a scoped database session. Just pull
            normal Python objects out of it.
        :type library: Library

        :param integration: The ExternalIntegration that
            configures this AuthenticationProvider. Don't store this
            object! It's associated with a scoped database session. Just
            pull normal Python objects out of it.
        :type integration: ExternalIntegration
        """
        super(BaseSAMLAuthenticationProvider, self).__init__(
            library, integration, analytics
        )

        self._logger = logging.getLogger(__name__)
        self._authentication_manager = None

        self.token_expiration_days = integration.setting(
            self.SAML_TOKEN_EXPIRATION_DAYS
        ).int_value or self.DEFAULT_TOKEN_EXPIRATION_DAYS

    def _authentication_flow_document(self, db):
        """Creates a Authentication Flow object for use in an Authentication for OPDS document.

        Example:
        {
            "type": "http://opds-spec.org/auth/saml"
            "description": "SAML 2.0 authentication provider",
            "links": [
                {
                    "rel" : "authenticate"
                    "href": "https://circulation.library.org/saml_authenticate?provider=SAML+2.0?idp_entity_id=https%3A%2F%2Fidp.saml.net%2Fidp%2Fshibboleth",
                    "display_names": [
                        {
                            "language": "en",
                            "value": "Test Shibboleth IdP Provider"
                        },
                        {
                            "language": "es",
                            "value": "Prueba de proveedor de IdP Shibboleth"
                        }
                    ],
                    "descriptions": [
                        {
                            "language": "en",
                            "value": "Test Shibboleth IdP Provider"
                        },
                        {
                            "language": "es",
                            "value": "Prueba de proveedor de IdP Shibboleth"
                        }
                    ],
                    "information_urls": [
                        {
                            "language": "en",
                            "value": "https://idp.saml.net/info/en"
                        },
                        {
                            "language": "es",
                            "value": "https://idp.saml.net/info/es"
                        }
                    ],
                    "privacy_statement_urls": [
                        {
                            "language": "en",
                            "value": "https://idp.saml.net/privacy/en"
                        },
                        {
                            "language": "es",
                            "value": "https://idp.saml.net/privacy/es"
                        }
                    ],
                    "logo_urls": [
                        {
                            "language": "en",
                            "height": 16,
                            "width": 16,
                            "value": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAACXBIWXMAABYlAAAWJQFJUiTwAAABWWlUWHRYTUw6Y29tLmFkb2JlLnhtcAAAAAAAPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iWE1QIENvcmUgNS40LjAiPgogICA8cmRmOlJERiB4bWxuczpyZGY9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkvMDIvMjItcmRmLXN5bnRheC1ucyMiPgogICAgICA8cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0iIgogICAgICAgICAgICB4bWxuczp0aWZmPSJodHRwOi8vbnMuYWRvYmUuY29tL3RpZmYvMS4wLyI+CiAgICAgICAgIDx0aWZmOk9yaWVudGF0aW9uPjE8L3RpZmY6T3JpZW50YXRpb24+CiAgICAgIDwvcmRmOkRlc2NyaXB0aW9uPgogICA8L3JkZjpSREY+CjwveDp4bXBtZXRhPgpMwidZAAADA0lEQVQ4EV2TzW9VRRjGfzPn3N7b76KAClFCuRFiEbxtgYSFIjEqNGGhscGF0AIr/gnqyrhlB4G0pWoMdeNH3BgVTEgJvfTShBaMRahaLR/ho7eU3ttzzvicqWycxZw5M+/zzDPP+76GD84GDHfHtJ1tILfwBo56LIskNGLYgHN/YM1vYJq0jrHO4hKDMQvEptaQjsKpdQThW0TuF670Klhjx2dNRNW8gLsUPEWx5xu//3TaOriW8YMzhtf6WwjN+xRv9UNfQseJZlzmXZyNsMlfWDsvVZuE20Biv2XswDXyx7M0N+7jcu9wKLm7cTVf/wfO4LLvELjzjPbMPr1M3wk6Bz7ERq9TOFNPPlfixuPF9DwEWyX3pELn4HZMUiWyv2MrIR1D7SRVvTma4dGLZdzsr5ggxERNAu8U9mFKYEGGVGwXDTfHiN0zmPgllsLN+u4hCNZBzRGabr+tyLsybwdjh36SuTsJZLQnsHqrCf5hbv0rereR43LYVuT4TRL3QETfyfk1LBmpMC+nIA3js6WFFLgWRg+c194S9tkRZeJHGpMRGfQFtckVikeuUjx0QrGrBLvNrp/1DDOtC+ZSplAvWM+2z1uJq81QrlMa76UHGoYLh8secO7NSJnaIoU/UL6VF+aOLvYmS0F8UeD9StkKqKxcxrq0Ppxfp+DO/v0qmr9pzRV1exvG1VH60xOEBGZSzFmxPi/MRoGu03EyJHc6x2KUhexRVeUspYMjlHTaMfic0jzq006ftdRP3yNJahnrUSFxkfaBT+V8F1XzKuS6RXpHZye9mvZTeelqoRJM+n+OeamwbaCNhbppJrrnKQxtVT20yt4ZgTfxpO5LauZewAYFnx1kXhzPUzo8nvaRCkkko2bCM+a/z1LaO07n0CIufk/SL5Etd2Ezai57lcsfTVE4vUZkm3388KQTgZFZqZKPDVN7K2w5sxqi7UrjJ8sy/zc32/vMuYbl3b5EWUhHaroaKW3pjNtDpv4rv522uj/sS+sljTWc61UFWue7VRv/Ajz9JzIyIjhZAAAAAElFTkSuQmCC"
                        },
                        {
                            "language": "es",
                            "height": 16,
                            "width": 16,
                            "value": "https://idp.saml.net/logo.png"
                        }
                    ],
                }
            ]
        }

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Authentication Flow object for use in an Authentication for OPDS document
        :rtype: Dict
        """
        flow_doc = {
            'type': self.FLOW_TYPE,
            'description': self.NAME,
            'links': []
        }

        configuration = self.get_authentication_manager(db).configuration

        for identity_provider in configuration.configuration.identity_providers:
            link = {
                'rel': 'authenticate',
                'href': self._create_authenticate_url(db, identity_provider.entity_id),
                'display_names': self._join_ui_info_items(identity_provider.ui_info.display_names),
                'descriptions': self._join_ui_info_items(identity_provider.ui_info.descriptions),
                'information_urls': self._join_ui_info_items(identity_provider.ui_info.information_urls),
                'privacy_statement_urls': self._join_ui_info_items(
                    identity_provider.ui_info.privacy_statement_urls),
                'logo_urls': self._join_ui_info_items(identity_provider.ui_info.logo_urls)
            }

            flow_doc['links'].append(link)

        return flow_doc

    def _create_token(self, db, patron, token):
        """Creates a Credential object that ties the given patron to the
        given provider token.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: Patron

        :param token: Token containing SAML subject's metadata
        :type token: Dict

        :return: Credential object
        :rtype: Credential
        """
        data_source, ignore = self._get_token_data_source(db)
        duration = datetime.timedelta(days=self.token_expiration_days)
        return Credential.temporary_token_create(
            db, data_source, self.TOKEN_TYPE, patron, duration, token
        )

    def _create_authenticate_url(self, db, idp_entity_id):
        """Returns an authentication link used by clients to authenticate patrons

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: Entity ID of the IdP which will be used for authentication
        :type idp_entity_id: string

        :return: URL for authentication using the chosen IdP
        :rtype: string
        """

        library = self.library(db)

        return url_for(
            'saml_authenticate',
            _external=True,
            library_short_name=library.short_name,
            provider=self.NAME,
            idp_entity_id=idp_entity_id
        )

    def _create_authentication_manager(self, db):
        """Creates a new SAML authentication manager

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: SAML authentication manager
        :rtype: SAMLAuthenticationManager
        """
        authentication_manager_factory = SAMLAuthenticationManagerFactory()
        authentication_manager = authentication_manager_factory.create(self.external_integration(db))

        return authentication_manager

    def _get_token_data_source(self, db):
        """Returns a token data source

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Token data source
        :rtype: DataSource
        """
        # FIXME: This code will probably not work in a situation where a library has multiple SAML
        #  authentication mechanisms for its patrons.
        #  It'll look up a Credential from this data source but it won't be able to tell which IdP it came from.
        return get_one_or_create(
            db, DataSource, name=self.TOKEN_DATA_SOURCE_NAME
        )

    def _join_ui_info_items(self, ui_info_items):
        """Joins all UIInfo items (like, display names, descriptions, etc.) to a single list of dicts

        :param ui_info_items: List of child UIInfo objects
        :type: List[LocalizableMetadataItem]

        :return: List of dicts containing UI information (display names, descriptions, etc.)
        :rtype: List[Dict]
        """
        result = []

        if ui_info_items:
            for ui_info_item in ui_info_items:
                result.append({
                    'value': ui_info_item.value,
                    'language': ui_info_item.language
                })

        return result

    def _run_self_tests(self, _db):
        pass

    def authenticate(self, db, header):
        pass

    def authenticated_patron(self, db, token):
        """Go from a token to an authenticated Patron.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param token: The provider token extracted from the Authorization
            header. This is _not_ the bearer token found in
            the Authorization header; it's the provider-specific token
            embedded in that token.
        :type token: Dict

        :return: A Patron, if one can be authenticated. None, if the
            credentials do not authenticate any particular patron. A
            ProblemDetail if an error occurs.
        :rtype: Union[Patron, ProblemDetail]
        """
        data_source, ignore = self._get_token_data_source(db)
        credential = Credential.lookup_by_token(
            db, data_source, self.TOKEN_TYPE, token
        )
        if credential:
            return credential.patron

        # This token wasn't in our database, or was expired. The
        # patron will have to log in through the SAML provider again
        # to get a new token.
        return None

    def get_authentication_manager(self, db):
        """Returns SAML authentication manager used by this provider

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: SAML authentication manager used by this provider
        :rtype: SAMLAuthenticationManager
        """
        if self._authentication_manager is None:
            self._authentication_manager = self._create_authentication_manager(db)

        return self._authentication_manager

    def remote_patron_lookup(self, subject):
        """Creates a PatronData object based on Subject object containing SAML Subject and AttributeStatement

        :param subject: Subject object containing SAML Subject and AttributeStatement
        :type subject: api.saml.metadata.Subject

        :return: PatronData object containing information about the authenticated SAML subject or
            ProblemDetail object in the case of any errors
        :rtype: Union[PatronData, ProblemDetail]
        """
        if not subject:
            return SAML_INVALID_SUBJECT.detailed('Subject is empty')

        if isinstance(subject, PatronData):
            return subject

        if not isinstance(subject, Subject):
            return SAML_INVALID_SUBJECT.detailed('Incorrect subject type')

        extractor = SubjectUIDExtractor()
        uid = extractor.extract(subject)

        if uid is None:
            return SAML_INVALID_SUBJECT.detailed('Subject does not have a unique ID')

        patron_data = PatronData(
            permanent_id=uid,
            authorization_identifier=uid,
            external_type='A',
            complete=True
        )

        return patron_data

    def saml_callback(self, db, subject):
        """Verifies the SAML subject, generates a Bearer token in the case of successful authentication and returns it

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param subject: Subject object containing SAML Subject and AttributeStatement
        :type subject: api.saml.metadata.Subject

        :return: A ProblemDetail if there's a problem. Otherwise, a
            3-tuple (Credential, Patron, PatronData). The Credential
            contains the access token provided by the SAML provider. The
            Patron object represents the authenticated Patron, and the
            PatronData object includes information about the patron
            obtained from the OAuth provider which cannot be stored in the
            circulation manager's database, but which should be passed on
            to the client.
        :rtype: Union[Tuple[Credential, Patron, PatronData], ProblemDetail]
        """
        patron_data = self.remote_patron_lookup(subject)
        if isinstance(patron_data, ProblemDetail):
            return patron_data

        # Convert the PatronData into a Patron object
        patron, is_new = patron_data.get_or_create_patron(db, self.library_id)

        # Create a credential for the Patron
        token = json.dumps(subject, cls=SubjectJSONEncoder)
        credential, is_new = self._create_token(db, patron, token)

        return credential, patron, patron_data


AuthenticationProvider = SAMLAuthenticationProvider
