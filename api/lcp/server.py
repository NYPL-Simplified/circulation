import json
import os
import urllib.parse

import requests
from flask_babel import lazy_gettext as _
from requests.auth import HTTPBasicAuth

from api.lcp import utils
from api.lcp.encrypt import LCPEncryptionResult, LCPEncryptorResultJSONEncoder
from api.lcp.hash import HashingAlgorithm
from core.model.configuration import ConfigurationGrouping, ConfigurationMetadata, ConfigurationAttributeType, \
    ConfigurationOption


class LCPServerConfiguration(ConfigurationGrouping):
    """Contains LCP License Server's settings"""

    DEFAULT_PAGE_SIZE = 100
    DEFAULT_PASSPHRASE_HINT = 'If you do not remember your passphrase, please contact your administrator'
    DEFAULT_ENCRYPTION_ALGORITHM = HashingAlgorithm.SHA256.value

    lcpserver_url = ConfigurationMetadata(
        key='lcpserver_url',
        label=_('LCP License Server\'s URL'),
        description=_('URL of the LCP License Server'),
        type=ConfigurationAttributeType.TEXT,
        required=True
    )

    lcpserver_user = ConfigurationMetadata(
        key='lcpserver_user',
        label=_('LCP License Server\'s user'),
        description=_('Name of the user used to connect to the LCP License Server'),
        type=ConfigurationAttributeType.TEXT,
        required=True
    )

    lcpserver_password = ConfigurationMetadata(
        key='lcpserver_password',
        label=_('LCP License Server\'s password'),
        description=_('Password of the user used to connect to the LCP License Server'),
        type=ConfigurationAttributeType.TEXT,
        required=True
    )

    lcpserver_input_directory = ConfigurationMetadata(
        key='lcpserver_input_directory',
        label=_('LCP License Server\'s input directory'),
        description=_(
            'Full path to the directory containing encrypted books. '
            'This directory should be the same as lcpencrypt\'s output directory'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=True
    )

    lcpserver_page_size = ConfigurationMetadata(
        key='lcpserver_page_size',
        label=_('LCP License Server\'s page size'),
        description=_('Number of licences returned by the server'),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=DEFAULT_PAGE_SIZE
    )

    provider_name = ConfigurationMetadata(
        key='provider_name',
        label=_('LCP service provider\'s identifier'),
        description=_(
            'URI that identifies the provider in an unambiguous way'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=True
    )

    passphrase_hint = ConfigurationMetadata(
        key='passphrase_hint',
        label=_('Passphrase hint'),
        description=_('Hint proposed to the user for selecting their passphrase'),
        type=ConfigurationAttributeType.TEXT,
        required=False,
        default=DEFAULT_PASSPHRASE_HINT
    )

    encryption_algorithm = ConfigurationMetadata(
        key='encryption_algorithm',
        label=_('Passphrase encryption algorithm'),
        description=_('Algorithm used for encrypting the passphrase'),
        type=ConfigurationAttributeType.SELECT,
        required=False,
        default=DEFAULT_ENCRYPTION_ALGORITHM,
        options=ConfigurationOption.from_enum(HashingAlgorithm)
    )

    max_printable_pages = ConfigurationMetadata(
        key='max_printable_pages',
        label=_('Maximum number or printable pages'),
        description=_('Maximum number of pages that can be printed over the lifetime of the license'),
        type=ConfigurationAttributeType.NUMBER,
        required=False
    )

    max_copiable_pages = ConfigurationMetadata(
        key='max_copiable_pages',
        label=_('Maximum number or copiable characters'),
        description=_('Maximum number of characters that can be copied to the clipboard'),
        type=ConfigurationAttributeType.NUMBER,
        required=False
    )


class LCPServer(object):
    """Wrapper around LCP License Server's API"""

    def __init__(self, configuration_storage, configuration_factory, hasher_factory, credential_factory):
        """Initializes a new instance of LCPServer class

        :param configuration_storage: ConfigurationStorage object
        :type configuration_storage: ConfigurationStorage

        :param configuration_factory: Factory creating LCPEncryptionConfiguration instance
        :type configuration_factory: api.config.ConfigurationFactory

        :param hasher_factory: Factory responsible for creating Hasher implementations
        :type hasher_factory: hash.HasherFactory

        :param credential_factory: Factory responsible for creating Hasher implementations
        :type credential_factory: credential.CredentialFactory
        """
        self._configuration_storage = configuration_storage
        self._configuration_factory = configuration_factory
        self._hasher_factory = hasher_factory
        self._credential_factory = credential_factory
        self._hasher_instance = None

    def _get_hasher(self, configuration):
        """Returns a Hasher instance

        :param configuration: Configuration object
        :type configuration: LCPServerConfiguration

        :return: Hasher instance
        :rtype: hash.Hasher
        """
        if self._hasher_instance is None:
            self._hasher_instance = self._hasher_factory.create(
                configuration.encryption_algorithm)

        return self._hasher_instance

    def _create_partial_license(self, db, configuration, patron, license_start=None, license_end=None):
        """Creates a partial LCP license used an input by the LCP License Server for generation of LCP licenses

        :param configuration: Configuration object
        :type configuration: LCPServerConfiguration

        :param patron: Patron object
        :type patron: Patron

        :param license_start: Date and time when the license begins
        :type license_start: Optional[datetime.datetime]

        :param license_end: Date and time when the license ends
        :type license_end: Optional[datetime.datetime]

        :return: Partial LCP license
        :rtype: Dict
        """
        hasher = self._get_hasher(configuration)
        hashed_passphrase = hasher.hash(self._credential_factory.get_patron_passphrase(db, patron))

        self._credential_factory.set_hashed_passphrase(db, patron, hashed_passphrase)

        partial_license = {
            'provider': configuration.provider_name,
            'encryption': {
                'user_key': {
                    'text_hint': configuration.passphrase_hint,
                    'hex_value': hashed_passphrase,
                }
            }
        }

        if patron:
            partial_license['user'] = {
                'id': self._credential_factory.get_patron_id(db, patron)
            }

        rights_fields = [
            license_start, license_end, configuration.max_printable_pages, configuration.max_copiable_pages]

        if any([rights_field is not None and rights_field != '' for rights_field in rights_fields]):
            partial_license['rights'] = {}

        if license_start:
            partial_license['rights']['start'] = utils.format_datetime(license_start)
        if license_end:
            partial_license['rights']['end'] = utils.format_datetime(license_end)
        if configuration.max_printable_pages is not None and configuration.max_printable_pages != '':
            partial_license['rights']['print'] = int(configuration.max_printable_pages)
        if configuration.max_copiable_pages is not None and configuration.max_copiable_pages != '':
            partial_license['rights']['copy'] = int(configuration.max_copiable_pages)

        return partial_license

    @staticmethod
    def _send_request(configuration, method, path, payload, json_encoder=None):
        """Sends a request to the LCP License Server

        :param configuration: Configuration object
        :type configuration: LCPServerConfiguration

        :param path: URL path part
        :type path: string

        :param payload: Dictionary containing request's payload (should be JSON compatible)
        :type payload: Union[Dict, object]

        :param json_encoder: JSON encoder
        :type json_encoder: JSONEncoder

        :return: Dictionary containing LCP License Server's response
        :rtype: Dict
        """
        json_payload = json.dumps(payload, cls=json_encoder)
        url = urllib.parse.urljoin(configuration.lcpserver_url, path)
        response = requests.request(
            method,
            url,
            data=json_payload,
            headers={'Content-Type': 'application/json'},
            auth=HTTPBasicAuth(
                configuration.lcpserver_user,
                configuration.lcpserver_password
            )
        )

        response.raise_for_status()

        return response

    def add_content(self, db, encrypted_content):
        """Notifies LCP License Server about new encrypted content

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param encrypted_content: LCPEncryptionResult object containing information about encrypted content
        :type encrypted_content: LCPEncryptionResult
        """
        with self._configuration_factory.create(
                self._configuration_storage, db, LCPServerConfiguration) as configuration:
            content_location = os.path.join(
                configuration.lcpserver_input_directory, encrypted_content.protected_content_disposition)
            payload = LCPEncryptionResult(
                content_id=encrypted_content.content_id,
                content_encryption_key=encrypted_content.content_encryption_key,
                protected_content_location=content_location,
                protected_content_disposition=encrypted_content.protected_content_disposition,
                protected_content_type=encrypted_content.protected_content_type,
                protected_content_length=encrypted_content.protected_content_length,
                protected_content_sha256=encrypted_content.protected_content_sha256
            )
            path = '/contents/{0}'.format(encrypted_content.content_id)

            self._send_request(configuration, 'put', path, payload, LCPEncryptorResultJSONEncoder)

    def generate_license(self, db, content_id, patron, license_start, license_end):
        """Generates a new LCP license

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param content_id: Unique content ID
        :type content_id: string

        :param patron: Patron object
        :type patron: Patron

        :param license_start: Unique patron ID
        :type license_start: string

        :param license_start: Date and time when the license begins
        :type license_start: datetime.datetime

        :param license_end: Date and time when the license ends
        :type license_end: datetime.datetime

        :return: LCP license
        :rtype: Dict
        """
        with self._configuration_factory.create(
                self._configuration_storage, db, LCPServerConfiguration) as configuration:
            partial_license_payload = self._create_partial_license(
                db, configuration, patron, license_start, license_end)
            path = 'contents/{0}/license'.format(content_id)
            response = self._send_request(configuration, 'post', path, partial_license_payload)

            return response.json()

    def get_license(self, db, license_id, patron):
        """Returns an existing license

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param license_id: License's ID
        :type license_id: int

        :param patron: Patron object
        :type patron: Patron

        :return: Existing license
        :rtype: string
        """
        with self._configuration_factory.create(
                self._configuration_storage, db, LCPServerConfiguration) as configuration:
            partial_license_payload = self._create_partial_license(db, configuration, patron)
            path = 'licenses/{0}'.format(license_id)

            response = self._send_request(configuration, 'post', path, partial_license_payload)

            return response.json()
