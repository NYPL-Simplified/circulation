import datetime
import os

import pytest
from freezegun import freeze_time
from parameterized import parameterized

import tests.saml.fixtures
from api.saml.metadata.federations import incommon
from api.saml.metadata.federations.model import SAMLFederation
from api.saml.metadata.federations.validator import (
    SAMLFederatedMetadataExpirationValidator,
    SAMLFederatedMetadataValidationError,
    SAMLMetadataSignatureValidator,
)


class TestSAMLFederatedMetadataExpirationValidator(object):
    @parameterized.expand(
        [
            (
                "incorrect_xml",
                datetime.datetime.utcnow(),
                tests.saml.fixtures.INCORRECT_XML,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "without_valid_until_attribute",
                datetime.datetime.utcnow(),
                tests.saml.fixtures.FEDERATED_METADATA_WITHOUT_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_expired_valid_until_attribute",
                tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW
                + datetime.timedelta(minutes=1),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_valid_until_attribute_too_far_in_the_future",
                tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                - datetime.timedelta(minutes=1),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_valid_until_attribute_less_than_current_time_and_less_than_max_clock_skew",
                tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW,
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
            ),
            (
                "with_valid_until_attribute_greater_than_current_time_and_less_than_max_valid_time",
                tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                + datetime.timedelta(minutes=1),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
            ),
            (
                "with_real_incommon_metadata",
                datetime.datetime(2020, 11, 26, 14, 32, 42),
                open(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../../../files/saml/incommon-metadata-idp-only.xml",
                    )
                ).read(),
                None,
            ),
        ]
    )
    def test_validate(self, _, current_time, metadata, expected_exception):
        # Arrange
        validator = SAMLFederatedMetadataExpirationValidator()
        federation = SAMLFederation(
            incommon.FEDERATION_TYPE, incommon.IDP_METADATA_SERVICE_URL
        )

        # Act, assert
        with freeze_time(current_time):
            if expected_exception:
                with pytest.raises(expected_exception):
                    validator.validate(federation, metadata)
            else:
                validator.validate(federation, metadata)


class TestSAMLMetadataSignatureValidator(object):
    @parameterized.expand(
        [
            (
                "without_signature",
                tests.saml.fixtures.FEDERATED_METADATA_CERTIFICATE,
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_invalid_signature",
                tests.saml.fixtures.FEDERATED_METADATA_CERTIFICATE.strip(),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_INVALID_SIGNATURE,
                SAMLFederatedMetadataValidationError,
            ),
            (
                "with_valid_signature",
                tests.saml.fixtures.FEDERATED_METADATA_CERTIFICATE.strip(),
                open(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../../../files/saml/incommon-metadata-idp-only.xml",
                    )
                ).read(),
                None,
            ),
        ]
    )
    def test_validate(self, _, certificate, metadata, expected_exception):
        # Arrange
        validator = SAMLMetadataSignatureValidator()
        federation = SAMLFederation(
            incommon.FEDERATION_TYPE, incommon.IDP_METADATA_SERVICE_URL
        )
        federation.certificate = certificate

        # Act, assert
        if expected_exception:
            with pytest.raises(expected_exception):
                validator.validate(federation, metadata)
        else:
            validator.validate(federation, metadata)
