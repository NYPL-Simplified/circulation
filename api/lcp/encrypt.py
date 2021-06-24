import json
import logging
import os
import re
import subprocess
from json import JSONEncoder

from flask_babel import lazy_gettext as _

from api.lcp import utils
from core.exceptions import BaseError
from core.model.configuration import ConfigurationGrouping, ConfigurationMetadata, ConfigurationAttributeType


class LCPEncryptionException(BaseError):
    """Raised in the case of any errors occurring during LCP encryption process"""


class LCPEncryptionConfiguration(ConfigurationGrouping):
    """Contains different settings required by LCPEncryptor"""

    DEFAULT_LCPENCRYPT_LOCATION = '/go/bin/lcpencrypt'
    DEFAULT_LCPENCRYPT_DOCKER_IMAGE = 'readium/lcpencrypt'

    lcpencrypt_location = ConfigurationMetadata(
        key='lcpencrypt_location',
        label=_('lcpencrypt\'s location'),
        description=_(
            'Full path to the local lcpencrypt binary. '
            'The default value is {0}'.format(
                DEFAULT_LCPENCRYPT_LOCATION
            )
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False,
        default=DEFAULT_LCPENCRYPT_LOCATION
    )

    lcpencrypt_output_directory = ConfigurationMetadata(
        key='lcpencrypt_output_directory',
        label=_('lcpencrypt\'s output directory'),
        description=_(
            'Full path to the directory where lcpencrypt stores encrypted content. '
            'If not set encrypted books will be stored in lcpencrypt\'s working directory'),
        type=ConfigurationAttributeType.TEXT,
        required=False
    )


class LCPEncryptionResult(object):
    """Represents an output sent by lcpencrypt"""

    CONTENT_ID = 'content-id'
    CONTENT_ENCRYPTION_KEY = 'content-encryption-key'
    PROTECTED_CONTENT_LOCATION = 'protected-content-location'
    PROTECTED_CONTENT_LENGTH = 'protected-content-length'
    PROTECTED_CONTENT_SHA256 = 'protected-content-sha256'
    PROTECTED_CONTENT_DISPOSITION = 'protected-content-disposition'
    PROTECTED_CONTENT_TYPE = 'protected-content-type'

    def __init__(
            self,
            content_id,
            content_encryption_key,
            protected_content_location,
            protected_content_disposition,
            protected_content_type,
            protected_content_length,
            protected_content_sha256):
        """Initializes a new instance of LCPEncryptorResult class

        :param: content_id: Content identifier
        :type content_id: Optional[string]

        :param: content_encryption_key: Content encryption key
        :type content_encryption_key: Optional[string]

        :param: protected_content_location: Complete file path of the encrypted content
        :type protected_content_location: Optional[string]

        :param: protected_content_disposition: File name of the encrypted content
        :type protected_content_disposition: Optional[string]

        :param: protected_content_type: Media type of the encrypted content
        :type protected_content_type: Optional[string]

        :param: protected_content_length: Size of the encrypted content
        :type protected_content_length: Optional[string]

        :param: protected_content_sha256: Hash of the encrypted content
        :type protected_content_sha256: Optional[string]
        """
        self._content_id = content_id
        self._content_encryption_key = content_encryption_key
        self._protected_content_location = protected_content_location
        self._protected_content_disposition = protected_content_disposition
        self._protected_content_type = protected_content_type
        self._protected_content_length = protected_content_length
        self._protected_content_sha256 = protected_content_sha256

    @property
    def content_id(self):
        """Returns a content encryption key

        :return: Content encryption key
        :rtype: Optional[string]
        """
        return self._content_id

    @property
    def content_encryption_key(self):
        """Returns a content identifier

        :return: Content identifier
        :rtype: Optional[string]
        """
        return self._content_encryption_key

    @property
    def protected_content_location(self):
        """Returns a complete file path of the encrypted content

        :return: Complete file path of the encrypted content
        :rtype: Optional[string]
        """
        return self._protected_content_location

    @property
    def protected_content_disposition(self):
        """Returns a file name of the encrypted content

        :return: File name of the encrypted content
        :rtype: Optional[string]
        """
        return self._protected_content_disposition

    @property
    def protected_content_type(self):
        """Returns a media type of the encrypted content

        :return: Media type of the encrypted content
        :rtype: Optional[string]
        """
        return self._protected_content_type

    @property
    def protected_content_length(self):
        """Returns a size of the encrypted content

        :return: Size of the encrypted content
        :rtype: Optional[string]
        """
        return self._protected_content_length

    @property
    def protected_content_sha256(self):
        """Returns a hash of the encrypted content

        :return: Hash of the encrypted content
        :rtype: Optional[string]
        """
        return self._protected_content_sha256

    @classmethod
    def from_dict(cls, result_dict):
        """Creates an LCPEncryptorResult object from a Python dictionary

        :param result_dict: Python dictionary containing an lcpencrypt output
        :type result_dict: Dict

        :return: LCPEncryptorResult object
        :rtype: LCPEncryptionResult
        """
        content_id = result_dict.get(cls.CONTENT_ID)
        content_encryption_key = result_dict.get(cls.CONTENT_ENCRYPTION_KEY)
        protected_content_location = result_dict.get(cls.PROTECTED_CONTENT_LOCATION)
        protected_content_length = result_dict.get(cls.PROTECTED_CONTENT_LENGTH)
        protected_content_sha256 = result_dict.get(cls.PROTECTED_CONTENT_SHA256)
        protected_content_disposition = result_dict.get(cls.PROTECTED_CONTENT_DISPOSITION)
        protected_content_type = result_dict.get(cls.PROTECTED_CONTENT_TYPE)

        return cls(
            content_id=content_id,
            content_encryption_key=content_encryption_key,
            protected_content_location=protected_content_location,
            protected_content_disposition=protected_content_disposition,
            protected_content_type=protected_content_type,
            protected_content_length=protected_content_length,
            protected_content_sha256=protected_content_sha256
        )

    def __eq__(self, other):
        """Compares two LCPEncryptorResult objects

        :param other: LCPEncryptorResult object
        :type other: LCPEncryptionResult

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, LCPEncryptionResult):
            return False

        return \
            self.content_id == other.content_id and \
            self.content_encryption_key == other.content_encryption_key and \
            self.protected_content_location == other.protected_content_location and \
            self.protected_content_length == other.protected_content_length and \
            self.protected_content_sha256 == other.protected_content_sha256 and \
            self.protected_content_disposition == other.protected_content_disposition and \
            self.protected_content_type == other.protected_content_type

    def __repr__(self):
        """Returns a string representation of a LCPEncryptorResult object

        :return: string representation of a LCPEncryptorResult object
        :rtype: string
        """
        return \
            '<LCPEncryptor.Result(' \
            'content_id={0}, ' \
            'content_encryption_key={1}, ' \
            'protected_content_location={2}, ' \
            'protected_content_length={3}, ' \
            'protected_content_sha256={4}, ' \
            'protected_content_disposition={5}, ' \
            'protected_content_type={6})>'.format(
                self.content_id,
                self.content_encryption_key,
                self.protected_content_location,
                self.protected_content_length,
                self.protected_content_sha256,
                self.protected_content_disposition,
                self.protected_content_type
            )


class LCPEncryptorResultJSONEncoder(JSONEncoder):
    """Serializes LCPEncryptorResult as a JSON object"""

    def default(self, result):
        """Serializers a Subject object to JSON

        :param result: LCPEncryptorResult object
        :type result: LCPEncryptionResult

        :return: String containing JSON representation of the LCPEncryptorResult object
        :rtype: string
        """
        if not isinstance(result, LCPEncryptionResult):
            raise ValueError('result must have type LCPEncryptorResult')

        result = {
            'content-id': result.content_id,
            'content-encryption-key': result.content_encryption_key,
            'protected-content-location': result.protected_content_location,
            'protected-content-length': result.protected_content_length,
            'protected-content-sha256': result.protected_content_sha256,
            'protected-content-disposition': result.protected_content_disposition,
            'protected-content-type': result.protected_content_type
        }

        return result


class LCPEncryptor(object):
    """Wrapper around lcpencrypt tool containing logic to run it locally and in a Docker container"""

    class Parameters(object):
        """Parses input parameters for lcpencrypt"""

        def __init__(self, file_path, identifier, configuration):
            """Initializes a new instance of Parameters class

            :param file_path: File path to the book to be encrypted
            :type file_path: string

            :param identifier: Book's identifier
            :type identifier: string

            :param configuration: LCPEncryptionConfiguration instance
            :type configuration: instance
            """
            self._lcpencrypt_location = configuration.lcpencrypt_location
            self._input_file_path = str(file_path)
            self._content_id = str(identifier)

            output_directory = configuration.lcpencrypt_output_directory

            self._output_file_path = None

            if output_directory:
                _, input_extension = os.path.splitext(file_path)
                target_extension = utils.get_target_extension(input_extension)
                output_file_path = os.path.join(
                    output_directory,
                    identifier + target_extension
                    if target_extension not in identifier
                    else identifier
                )

                self._output_file_path = output_file_path

        @property
        def lcpencrypt_location(self):
            """Returns location of lcpencrypt binary

            :return: Location of lcpencrypt binary
            :rtype: string
            """
            return self._lcpencrypt_location

        @property
        def input_file_path(self):
            """Returns path of the input file

            :return: Path of the input file
            :rtype: string
            """
            return self._input_file_path

        @property
        def content_id(self):
            """Returns content ID

            :return: Content ID
            :rtype: string
            """
            return self._content_id

        @property
        def output_file_path(self):
            """Returns path of the output file

            :return: Path of the output file
            :rtype: string
            """
            return self._output_file_path

        def to_array(self):
            """Returns parameters in an array

            :return: Parameters in an array
            :rtype: List
            """
            parameters = [
                self._lcpencrypt_location,
                '-input',
                self._input_file_path,
                '-contentid',
                self._content_id
            ]

            if self._output_file_path:
                parameters.extend([
                    '-output',
                    self._output_file_path
                ])

            return parameters

    OUTPUT_REGEX = re.compile(r'(\{.+\})?(.+)', re.DOTALL)

    def __init__(self, configuration_storage, configuration_factory):
        """Initializes a new instance of LCPEncryptor class

        :param configuration_storage: ConfigurationStorage object
        :type configuration_storage: ConfigurationStorage

        :param configuration_factory: Factory creating LCPEncryptionConfiguration instance
        :type configuration_factory: api.config.ConfigurationFactory
        """
        self._logger = logging.getLogger(__name__)
        self._configuration_storage = configuration_storage
        self._configuration_factory = configuration_factory

    def _lcpencrypt_exists_locally(self, configuration):
        """Returns a Boolean value indicating whether lcpencrypt exists locally

        :param configuration: LCPEncryptionConfiguration instance
        :type configuration: instance

        :return: Boolean value indicating whether lcpencrypt exists locally
        :rtype: bool
        """
        return os.path.isfile(configuration.lcpencrypt_location)

    def _parse_output(self, output):
        """Parses lcpencrypt's output

        :param output: lcpencrypt's output
        :type output: string

        :return: Encryption result
        :rtype: LCPEncryptionResult
        """
        bracket_index = output.find('{')

        if bracket_index > 0:
            output = output[bracket_index:]

        match = self.OUTPUT_REGEX.match(output)

        if not match:
            raise LCPEncryptionException('Output has a wrong format')

        match_groups = match.groups()

        if not match_groups:
            raise LCPEncryptionException('Output has a wrong format')

        if not match_groups[0]:
            raise LCPEncryptionException(match_groups[1].strip())

        json_output = match_groups[0]
        json_result = json.loads(json_output)
        result = LCPEncryptionResult.from_dict(json_result)

        if not result.protected_content_length or \
                not result.protected_content_sha256 or \
                not result.content_encryption_key:
            raise LCPEncryptionException('Encryption failed')

        return result

    def _run_lcpencrypt_locally(self, file_path, identifier, configuration):
        """Runs lcpencrypt using a local binary

        :param file_path: File path to the book to be encrypted
        :type file_path: string

        :param identifier: Book's identifier
        :type identifier: string

        :param configuration: LCPEncryptionConfiguration instance
        :type configuration: instance

        :return: Encryption result
        :rtype: LCPEncryptionResult
        """
        self._logger.info(
            'Started running a local lcpencrypt binary. File path: {0}. Identifier: {1}'.format(
                file_path, identifier
            )
        )

        parameters = LCPEncryptor.Parameters(file_path, identifier, configuration)

        try:
            if parameters.output_file_path:
                self._logger.info('Creating a directory tree for {0}'.format(parameters.output_file_path))

                output_directory = os.path.dirname(parameters.output_file_path)

                if not os.path.exists(output_directory):
                    os.makedirs(output_directory)

                self._logger.info('Directory tree {0} has been successfully created'.format(output_directory))

            self._logger.info('Running lcpencrypt using the following parameters: {0}'.format(parameters.to_array()))

            output = subprocess.check_output(parameters.to_array())
            result = self._parse_output(output)
        except Exception as exception:
            self._logger.exception('An unhandled exception occurred during running a local lcpencrypt binary')

            raise LCPEncryptionException(str(exception), inner_exception=exception)

        self._logger.info(
            'Finished running a local lcpencrypt binary. File path: {0}. Identifier: {1}. Result: {2}'.format(
                file_path, identifier, result
            )
        )

        return result

    def encrypt(self, db, file_path, identifier):
        """Encrypts a book

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param file_path: File path to the book to be encrypted
        :type file_path: string

        :param identifier: Book's identifier
        :type identifier: string

        :return: Encryption result
        :rtype: LCPEncryptionResult
        """
        with self._configuration_factory.create(
                self._configuration_storage, db, LCPEncryptionConfiguration) as configuration:
            if self._lcpencrypt_exists_locally(configuration):
                result = self._run_lcpencrypt_locally(file_path, identifier, configuration)

                return result
            else:
                raise NotImplementedError()
