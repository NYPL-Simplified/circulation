from sqlalchemy import ARRAY, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from core.model import Base


class SAMLFederation(Base):
    """Contains information about a SAML federation (for example, InCommon)."""

    __tablename__ = "samlfederations"

    id = Column(Integer, primary_key=True)
    type = Column(String(256), nullable=False, unique=True)
    idp_metadata_service_url = Column(String(2048), nullable=False)
    last_updated_at = Column(DateTime(), nullable=True)

    certificate = Column(Text(), nullable=True)

    identity_providers = relationship("SAMLFederatedIdentityProvider")

    def __init__(self, federation_type, idp_metadata_service_url, certificate=None):
        """Initialize a new instance of SAMLFederation class.

        :param federation_type: Federation's type
        :type federation_type: str

        :param idp_metadata_service_url: URL of the metadata service allowing to download IdP metadata
        :type idp_metadata_service_url: str

        :param certificate: Certificate used to validate metadata
        :type certificate: str
        """
        if not federation_type or not isinstance(federation_type, str):
            raise ValueError("Argument 'federation_type' must be a non-empty string")
        if not idp_metadata_service_url or not isinstance(idp_metadata_service_url, str):
            raise ValueError(
                "Argument 'idp_metadata_service_url' must be a non-empty string"
            )

        self.type = federation_type
        self.idp_metadata_service_url = idp_metadata_service_url
        self.certificate = certificate

    def __eq__(self, other):
        """Compare two SAMLFederation objects.

        :param other: SAMLFederation object
        :type other: Any

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLFederation):
            return False

        return (
            self.type == other.type
            and self.idp_metadata_service_url == other.idp_metadata_service_url
            and self.certificate == other.certificate
        )

    def __repr__(self):
        """Return a string representation.

        :return: String representation
        :rtype: str
        """
        return "<SAMLFederation(id={0}, type={1}, idp_metadata_service_url={2}, last_updated_at={3}".format(
            self.id, self.type, self.idp_metadata_service_url, self.last_updated_at
        )


class SAMLFederatedIdentityProvider(Base):
    """Contains information about a federated IdP."""

    __tablename__ = "samlfederatedidps"

    id = Column(Integer, primary_key=True)
    entity_id = Column(String(256), nullable=False)
    display_name = Column(String(256), nullable=False)

    xml_metadata = Column(Text(), nullable=False)

    federation_id = Column(Integer, ForeignKey("samlfederations.id"), index=True)
    federation = relationship("SAMLFederation", foreign_keys=federation_id)

    def __init__(self, federation, entity_id, display_name, xml_metadata):
        """Initialize a new instance of SAMLFederatedIdentityProvider class.

        :param federation: SAML federation this IdP belongs to
        :type federation: SAMLFederation

        :param entity_id: IdP's entity ID
        :type entity_id: str

        :param display_name: IdP's display name
        :type display_name: str

        :param xml_metadata: IdP's XML metadata
        :type xml_metadata: str
        """
        if not isinstance(federation, SAMLFederation):
            raise ValueError(
                "Argument 'federation' must be an instance of {0} class".format(
                    SAMLFederation
                )
            )
        if not entity_id or not isinstance(entity_id, str):
            raise ValueError("Argument 'entity_id' must be a non-empty string")
        if not display_name or not isinstance(display_name, str):
            raise ValueError("Argument 'display_name' must be a non-empty string")
        if not xml_metadata or not isinstance(xml_metadata, str):
            raise ValueError("Argument 'xml_metadata' must be a non-empty string")

        self.federation = federation
        self.entity_id = entity_id
        self.display_name = display_name
        self.xml_metadata = xml_metadata

    def __eq__(self, other):
        """Compare two SAMLFederatedIdentityProvider objects.

        :param other: SAMLFederatedIdentityProvider object
        :type other: Any

        :return: Boolean value indicating whether two items are equal
        :rtype: bool
        """
        if not isinstance(other, SAMLFederatedIdentityProvider):
            return False

        return (
            self.federation == other.federation
            and self.entity_id == other.entity_id
            and self.display_name == other.display_name
            and self.xml_metadata == other.xml_metadata
        )

    def __repr__(self):
        """Return a string representation.

        :return: String representation
        :rtype: str
        """
        return "<SAMLFederatedIdentityProvider(id={0}, federation={1}, entity_id={2}, display_name={3}".format(
            self.id, self.federation, self.entity_id, self.display_name
        )
