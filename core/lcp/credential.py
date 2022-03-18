import logging

from enum import Enum

from .exceptions import LCPError
from ..model import Credential, DataSource


class LCPCredentialType(Enum):
    """Contains an enumeration of different LCP credential types"""

    PATRON_ID = 'Patron ID passed to the LCP License Server'
    LCP_PASSPHRASE = 'LCP Passphrase passed to the LCP License Server'
    LCP_HASHED_PASSPHRASE = 'Hashed LCP Passphrase passed to the LCP License Server'


class LCPCredentialFactory(object):
    """Generates patron's credentials used by the LCP License Server"""

    def __init__(self):
        """Initializes a new instance of LCPCredentialFactory class"""
        self._logger = logging.getLogger(__name__)

    def _get_or_create_persistent_token(self, db, patron, data_source_type, credential_type, value=None):
        """Gets or creates a new persistent token

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param value: Optional value of the token
        :type value: Optional[string]
        """
        self._logger.info(
            'Getting or creating "{0}" credentials for {1} in "{2}" data source with value "{3}"'.format(
                credential_type, patron, data_source_type, value
            )
        )

        data_source = DataSource.lookup(db, data_source_type)

        transaction = db.begin_nested()
        credential, is_new = Credential.persistent_token_create(db, data_source, credential_type, patron, value)
        transaction.commit()

        self._logger.info(
            'Successfully {0} "{1}" {2} for {3} in "{4}" data source with value "{5}"'.format(
                'created new' if is_new else 'fetched existing',
                credential_type,
                credential,
                patron,
                data_source_type,
                value
            )
        )

        return credential.credential, is_new

    def get_patron_id(self, db, patron):
        """Generates a new or returns an existing patron's ID associated with an LCP license

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :return: Newly generated or existing patron's ID associated with an LCP license
        :rtype: string
        """
        patron_id, _ = self._get_or_create_persistent_token(
            db, patron, DataSource.INTERNAL_PROCESSING, LCPCredentialType.PATRON_ID.value)

        return patron_id

    def get_patron_passphrase(self, db, patron):
        """Generates a new or returns an existing patron's passphrase associated with an LCP license

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :return: Newly generated or existing patron's passphrase associated with an LCP license
        :rtype: string
        """
        patron_passphrase, _ = self._get_or_create_persistent_token(
            db, patron, DataSource.INTERNAL_PROCESSING, LCPCredentialType.LCP_PASSPHRASE.value)

        return patron_passphrase

    def get_hashed_passphrase(self, db, patron):
        """Returns an existing hashed passphrase

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :return: Existing hashed passphrase
        :rtype: string
        """
        hashed_passphrase, is_new = self._get_or_create_persistent_token(
            db, patron, DataSource.INTERNAL_PROCESSING, LCPCredentialType.LCP_HASHED_PASSPHRASE.value)

        if is_new:
            raise LCPError('Passphrase have to be explicitly set')

        return hashed_passphrase

    def set_hashed_passphrase(self, db, patron, hashed_passphrase):
        """Stores the hashed passphrase as a persistent token

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param hashed_passphrase: Existing hashed passphrase
        :type hashed_passphrase: string
        """
        self._get_or_create_persistent_token(
            db, patron, DataSource.INTERNAL_PROCESSING, LCPCredentialType.LCP_HASHED_PASSPHRASE.value, hashed_passphrase)
