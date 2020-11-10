import datetime
import json
import logging
from enum import Enum

from api.saml.metadata.model import SAMLAttributeType, SAMLSubjectJSONDecoder

from core.model import Credential, DataSource, DataSourceConstants, Patron
from core.util import first_or_default, is_session
from core.util.string_helpers import is_string


class ProQuestCredentialType(Enum):
    """Contains an enumeration of different ProQuest credential types"""

    PROQUEST_JWT_TOKEN = "ProQuest JWT Token"


class ProQuestCredentialManager(object):
    """Manages ProQuest credentials."""

    def __init__(self):
        """Initialize a new instance of ProQuestCredentialManager class."""
        self._logger = logging.getLogger(__name__)

    def _extract_saml_subject(self, credential):
        """Extract a SAML subject from SAML token.

        :param credential: Credential object containing a SAML token
        :type credential: core.model.credential.Credential

        :return: SAML subject
        :rtype: api.saml.metadata.Subject
        """
        self._logger.debug("Started deserializing SAML token {0}".format(credential))

        subject = json.loads(credential.credential, cls=SAMLSubjectJSONDecoder)

        self._logger.debug(
            "Finished deserializing SAML token {0}: {1}".format(credential, subject)
        )

        return subject

    def _lookup_saml_token(self, db, patron):
        """Look up for a SAML token.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :return: SAML subject (if any)
        :rtype: Optional[api.saml.metadata.Subject]
        """
        self._logger.debug("Started looking up for a SAML token")

        from api.authenticator import BaseSAMLAuthenticationProvider

        credential = Credential.lookup_by_patron(
            db,
            BaseSAMLAuthenticationProvider.TOKEN_DATA_SOURCE_NAME,
            BaseSAMLAuthenticationProvider.TOKEN_TYPE,
            patron,
            allow_persistent_token=False,
            auto_create_datasource=True,
        )

        self._logger.debug(
            "Finished looking up for a SAML token: {0}".format(credential)
        )

        return credential

    def lookup_proquest_token(self, db, patron):
        """Look up for a JWT bearer token used required to use ProQuest API.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :return: ProQuest JWT bearer token (if any)
        :rtype: Optional[str]
        """
        if not is_session(db):
            raise ValueError('"db" argument must be a valid SQLAlchemy session')
        if not isinstance(patron, Patron):
            raise ValueError('"patron" argument must be an instance of Patron class')

        self._logger.debug("Started looking up for a ProQuest JWT token")

        credential = Credential.lookup_by_patron(
            db,
            DataSourceConstants.PROQUEST,
            ProQuestCredentialType.PROQUEST_JWT_TOKEN.value,
            patron,
            allow_persistent_token=False,
            auto_create_datasource=True,
        )

        self._logger.debug(
            "Finished looking up for a ProQuest JWT token: {0}".format(credential)
        )

        if credential:
            return credential.credential

        return None

    def save_proquest_token(self, db, patron, duration, token):
        """Save a ProQuest JWT bearer token for later use.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param duration: How long this token can be valid
        :type duration: datetime.timedelta

        :param token: ProQuest JWT bearer token
        :type token: str
        """
        if not is_session(db):
            raise ValueError('"db" argument must be a valid SQLAlchemy session')
        if not isinstance(patron, Patron):
            raise ValueError('"patron" argument must be an instance of Patron class')
        if not isinstance(duration, datetime.timedelta):
            raise ValueError(
                '"duration" argument must be an instance of datetime.timedelta class'
            )
        if not is_string(token) or not token:
            raise ValueError('"token" argument must be a non-empty string')

        self._logger.debug(
            "Started saving a ProQuest JWT bearer token {0}".format(token)
        )

        data_source = DataSource.lookup(
            db, DataSourceConstants.PROQUEST, autocreate=True
        )
        credential, is_new = Credential.temporary_token_create(
            db,
            data_source,
            ProQuestCredentialType.PROQUEST_JWT_TOKEN.value,
            patron,
            duration,
            token,
        )

        self._logger.debug(
            "Finished saving a ProQuest JWT bearer token {0}: {1} (new = {2})".format(
                token, credential, is_new
            )
        )

    def lookup_patron_affiliation_id(
        self,
        db,
        patron,
        affiliation_attributes=(
            SAMLAttributeType.eduPersonPrincipalName.name,
            SAMLAttributeType.eduPersonScopedAffiliation.name,
        ),
    ):
        """Look up for patron's SAML affiliation ID.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param affiliation_attributes: SAML attributes containing an affiliation ID
        :type affiliation_attributes: Tuple

        :return: Patron's SAML affiliation ID (if any)
        :rtype: Optional[str]
        """
        if not is_session(db):
            raise ValueError('"db" argument must be a valid SQLAlchemy session')
        if not isinstance(patron, Patron):
            raise ValueError('"patron" argument must be an instance of Patron class')
        if affiliation_attributes and not isinstance(affiliation_attributes, tuple):
            raise ValueError('"affiliation_attributes" argument must be a tuple')

        self._logger.debug(
            "Started looking for SAML affiliation ID in for patron {0} in {1}".format(
                patron, affiliation_attributes
            )
        )

        saml_credential = self._lookup_saml_token(db, patron)

        if not saml_credential:
            self._logger.debug("Patron {0} does not have a SAML token".format(patron))
            return None

        saml_subject = self._extract_saml_subject(saml_credential)

        self._logger.debug(
            "Patron {0} has the following SAML subject: {1}".format(
                patron, saml_subject
            )
        )

        affiliation_id = None

        for attribute_name in affiliation_attributes:
            self._logger.debug("Trying to find attribute {0}".format(attribute_name))

            if attribute_name in saml_subject.attribute_statement.attributes:
                attribute = saml_subject.attribute_statement.attributes[attribute_name]

                self._logger.debug(
                    "Found {0} with the following values: {1}".format(
                        attribute, attribute.values
                    )
                )

                affiliation_id = first_or_default(attribute.values)
                break

        self._logger.debug(
            "Finished looking for SAML affiliation ID in for patron {0} in {1}: {2}".format(
                patron, affiliation_attributes, affiliation_id
            )
        )

        return affiliation_id
