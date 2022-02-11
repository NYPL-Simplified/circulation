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
from core.util.datetime_helpers import datetime_utc, utc_now


INCOMMON_METADATA_FREEZE_TIME = datetime_utc(2020, 11, 26, 14, 32, 42)


@pytest.fixture(scope="session")
def incommon_metadata():
    incommon_metadata_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../../../files/saml/incommon-metadata-idp-only.xml",
    )
    with open(incommon_metadata_file, mode='r') as f:
        yield f.read()


@pytest.fixture(scope="class")
def metadata_expiration_validator():
    validator = SAMLFederatedMetadataExpirationValidator()
    yield validator


@pytest.fixture(scope="class")
def metadata_signature_validator():
    validator = SAMLMetadataSignatureValidator()
    yield validator


@pytest.fixture(scope="module")
def incommon_federation():
    federation = SAMLFederation(incommon.FEDERATION_TYPE, incommon.IDP_METADATA_SERVICE_URL)
    yield federation


class TestSAMLFederatedMetadataExpirationValidator:
    def test_validate_with_real_incommon_metadata(
        self, metadata_expiration_validator, incommon_federation, incommon_metadata
    ):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        with freeze_time(INCOMMON_METADATA_FREEZE_TIME):
            validation_result = metadata_expiration_validator.validate(
                incommon_federation,
                incommon_metadata
            )
            assert validation_result is None

    @pytest.mark.parametrize(
        "time_to_freeze,metadata,expected_exception",
        [
            pytest.param(
                utc_now(),
                tests.saml.fixtures.INCORRECT_XML,
                SAMLFederatedMetadataValidationError,
                id="incorrect_xml"
            ),
            pytest.param(
                utc_now(),
                tests.saml.fixtures.FEDERATED_METADATA_WITHOUT_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
                id="without_valid_until_attribute"
            ),
            pytest.param(
                (
                    tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                    + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW
                    + datetime.timedelta(minutes=1)
                ),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
                id="with_expired_valid_until_attribute",
            ),
            pytest.param(
                (
                    tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                    - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                    - datetime.timedelta(minutes=1)
                ),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                SAMLFederatedMetadataValidationError,
                id="with_valid_until_attribute_too_far_in_the_future",
            ),
            pytest.param(
                (
                    tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                    + SAMLFederatedMetadataExpirationValidator.MAX_CLOCK_SKEW
                ),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
                id="with_valid_until_attribute_less_than_current_time_and_less_than_max_clock_skew",
            ),
            pytest.param(
                (
                    tests.saml.fixtures.FEDERATED_METADATA_VALID_UNTIL
                    - SAMLFederatedMetadataExpirationValidator.MAX_VALID_TIME
                    + datetime.timedelta(minutes=1)
                ),
                tests.saml.fixtures.FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE,
                None,
                id="with_valid_until_attribute_greater_than_current_time_and_less_than_max_valid_time",
            ),
        ]
    )
    def test_validate(
        self, metadata_expiration_validator, incommon_federation,
        time_to_freeze, metadata, expected_exception
    ):
        with freeze_time(time_to_freeze):
            if expected_exception:
                with pytest.raises(expected_exception):
                    metadata_expiration_validator.validate(incommon_federation, metadata)
            else:
                metadata_expiration_validator.validate(incommon_federation, metadata)


class TestSAMLMetadataSignatureValidator(object):
    @pytest.mark.skip(reason="TODO: Needs new signature for shortened version of metadata file.")
    def test_validate_with_real_incommon_metadata(
        self, metadata_signature_validator, incommon_federation, incommon_metadata
    ):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        incommon_federation.certificate = tests.saml.fixtures.FEDERATED_METADATA_CERTIFICATE.strip()
        validation_result = metadata_signature_validator.validate(
            incommon_federation,
            incommon_metadata
        )
        assert validation_result is None

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
