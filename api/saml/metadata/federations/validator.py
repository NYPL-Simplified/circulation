import datetime
import logging
from abc import ABCMeta

import six
from defusedxml.lxml import fromstring
from onelogin.saml2.utils import OneLogin_Saml2_Utils

from core.exceptions import BaseError


class SAMLFederatedMetadataValidationError(BaseError):
    """Raised in the case of any errors happened during SAML metadata validation."""


@six.add_metaclass(ABCMeta)
class SAMLFederatedMetadataValidator(object):
    """Base class for all validators checking correctness of SAML federated metadata."""

    def validate(self, federation, metadata):
        """Validate SAML federated metadata.

        :param federation: SAML federation
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :param metadata: SAML federation's aggregated metadata
        :type metadata: str

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        raise NotImplementedError


class SAMLFederatedMetadataValidatorChain(SAMLFederatedMetadataValidator):
    def __init__(self, validators):
        """Initialize a new instance of SAMLFederatedMetadataValidatorChain class.

        :param validators: List of validators
        :type validators: List[SAMLFederatedMetadataValidator]
        """
        if not validators or not isinstance(validators, list):
            raise ValueError("Argument 'validators' must be a non-empty list")

        for validator in validators:
            if not isinstance(validator, SAMLFederatedMetadataValidator):
                raise ValueError(
                    "Argument 'validators' must contain only instances of {0} class".format(
                        SAMLFederatedMetadataValidator
                    )
                )

        self._validators = validators

    def validate(self, federation, metadata):
        """Validate SAML federated metadata using a chain of inner validators.

        :param federation: SAML federation
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :param metadata: SAML federation's aggregated metadata
        :type metadata: str

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        for validator in self._validators:
            validator.validate(federation, metadata)


class SAMLFederatedMetadataExpirationValidator(SAMLFederatedMetadataValidator):
    """Verifies that federated SAML metadata has not expired."""

    # We allow the metadata's expiration time to be only 5 minutes behind.
    MAX_CLOCK_SKEW = datetime.timedelta(minutes=5)

    # We allow the metadata's expiration time to be only 4 week ahead.
    MAX_VALID_TIME = datetime.timedelta(weeks=4)

    def __init__(self):
        """Initialize a new instance of SAMLFederatedMetadataExpirationValidator class."""
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _parse_saml_date_time(saml_date_time):
        """Parse the string containing date & time information in the SAML format into datetime object.

        :param saml_date_time: String containing date & time information in the SAML format
        :type saml_date_time: str
        """
        unix_timestamp = OneLogin_Saml2_Utils.parse_SAML_to_time(saml_date_time)
        parsed_date_time = datetime.datetime.utcfromtimestamp(unix_timestamp)

        return parsed_date_time

    def validate(self, federation, metadata):
        """Verify that federated SAML metadata has not expired.

        :param federation: SAML federation
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :param metadata: SAML federation's aggregated metadata
        :type metadata: str

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        self._logger.info(
            "Started validating the expiration time of the metadata belonging to {0}".format(
                federation
            )
        )

        try:
            root = fromstring(metadata.encode("utf-8"))
        except Exception as exception:
            raise SAMLFederatedMetadataValidationError(
                "Metadata's XML is not valid", str(exception)
            )

        if "EntitiesDescriptor" not in root.tag:
            raise SAMLFederatedMetadataValidationError(
                'Metadata\'s root element is not "EntitiesDescriptor"'
            )

        valid_until = root.get("validUntil", None)
        if not valid_until:
            raise SAMLFederatedMetadataValidationError(
                'Metadata does not contain "validUntil" attribute'
            )

        valid_until = self._parse_saml_date_time(valid_until)
        now = datetime.datetime.utcnow()

        if valid_until < now and (now - valid_until) > self.MAX_CLOCK_SKEW:
            raise SAMLFederatedMetadataValidationError(
                "Metadata has already expired. "
                '"validUntil" is {0} while the current time is {1}'.format(
                    valid_until, now
                )
            )

        if valid_until > now and (valid_until - now) > self.MAX_VALID_TIME:
            raise SAMLFederatedMetadataValidationError(
                "Expiration time is unexpectedly far into the future. "
                '"validUntil" is {0} while the current time is {1}'.format(
                    valid_until, now
                )
            )

        self._logger.info(
            "Finished validating the expiration time of the metadata belonging to {0}".format(
                federation
            )
        )


class SAMLMetadataSignatureValidator(SAMLFederatedMetadataValidator):
    """Verifies the validity of federated SAML metadata's signature."""

    def __init__(self):
        """Initialize a new instance of SAMLMetadataSignatureValidator class."""
        self._logger = logging.getLogger(__name__)

    def validate(self, federation, metadata):
        """Verify the validity of the SAML federated metadata's signature.

        :param federation: SAML federation
        :type federation: api.saml.metadata.federations.model.SAMLFederation

        :param metadata: SAML federation's aggregated metadata
        :type metadata: str

        :raises SAMLFederatedMetadataValidationError: in the case of validation errors
        """
        self._logger.info(
            "Started verifying the validity of the metadata's signature belonging to {0}".format(
                federation
            )
        )

        try:
            OneLogin_Saml2_Utils.validate_metadata_sign(
                metadata, federation.certificate, raise_exceptions=True
            )
        except Exception as exception:
            raise SAMLFederatedMetadataValidationError(
                six.ensure_text(str(exception)), exception
            )

        self._logger.info(
            "Finished verifying the validity of the metadata's signature belonging to {0}".format(
                federation
            )
        )
