import tempfile

from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session

from api.lcp.encrypt import LCPEncryptor
from api.lcp.hash import HasherFactory
from api.lcp.importer import LCPImporter
from api.lcp.server import LCPServer
from core.lcp.credential import LCPCredentialFactory
from core.mirror import MirrorUploader
from core.model import ExternalIntegration, Collection
from core.model.collection import HasExternalIntegrationPerCollection, CollectionConfigurationStorage
from core.model.configuration import ConfigurationAttributeType, \
    ConfigurationMetadata, ConfigurationFactory
from core.s3 import MinIOUploader, S3UploaderConfiguration, MinIOUploaderConfiguration


class LCPMirrorConfiguration(S3UploaderConfiguration):
    endpoint_url = ConfigurationMetadata(
        key=MinIOUploaderConfiguration.endpoint_url.key,
        label=_('Endpoint URL'),
        description=_(
            'S3 endpoint URL'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False
    )


class LCPMirror(MinIOUploader, HasExternalIntegrationPerCollection):
    """Implements LCP import workflow:
    1. Encrypts unencrypted books using lcpencrypt
    2. Sends encrypted books to the LCP License Server
    3. LCP License Server generates license metadata and uploads encrypted books to the encrypted_repository
    """

    NAME = ExternalIntegration.LCP
    SETTINGS = [
        S3UploaderConfiguration.access_key.to_settings(),
        S3UploaderConfiguration.secret_key.to_settings(),
        S3UploaderConfiguration.protected_access_content_bucket.to_settings(),
        S3UploaderConfiguration.s3_region.to_settings(),
        S3UploaderConfiguration.s3_addressing_style.to_settings(),
        S3UploaderConfiguration.s3_presigned_url_expiration.to_settings(),
        S3UploaderConfiguration.url_template.to_settings(),
        LCPMirrorConfiguration.endpoint_url.to_settings()
    ]

    def __init__(self, integration):
        """Initializes a new instance of LCPMirror class

        :param integration: External integration containing mirror's properties
        :type integration: ExternalIntegration
        """
        super(LCPMirror, self).__init__(integration)

        self._lcp_importer_instance = None

    def _create_lcp_importer(self, collection):
        """Creates a new instance of LCPImporter

        :param collection: Collection object
        :type collection: Collection

        :return: New instance of LCPImporter
        :rtype: LCPImporter
        """
        configuration_storage = CollectionConfigurationStorage(self, collection)
        configuration_factory = ConfigurationFactory()
        hasher_factory = HasherFactory()
        credential_factory = LCPCredentialFactory()
        lcp_encryptor = LCPEncryptor(configuration_storage, configuration_factory)
        lcp_server = LCPServer(configuration_storage, configuration_factory, hasher_factory, credential_factory)
        lcp_importer = LCPImporter(lcp_encryptor, lcp_server)

        return lcp_importer

    def collection_external_integration(self, collection):
        """Returns an external integration associated with the collection

        :param collection: Collection
        :type collection: core.model.Collection

        :return: External integration associated with the collection
        :rtype: core.model.configuration.ExternalIntegration
        """
        db = Session.object_session(collection)
        external_integration = db \
            .query(ExternalIntegration) \
            .join(Collection) \
            .filter(
                Collection.id == collection.id
            ) \
            .one()

        return external_integration

    def cover_image_root(self, bucket, data_source, scaled_size=None):
        raise NotImplementedError()

    def marc_file_root(self, bucket, library):
        raise NotImplementedError()

    def book_url(self, identifier, extension='.epub', open_access=False, data_source=None, title=None):
        """Returns the path to the hosted EPUB file for the given identifier."""
        bucket = self.get_bucket(
            S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY if open_access
            else S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY)
        root = self.content_root(bucket)
        book_url = root + self.key_join([identifier.identifier])

        return book_url

    def cover_image_url(self, data_source, identifier, filename, scaled_size=None):
        raise NotImplementedError()

    def marc_file_url(self, library, lane, end_time, start_time=None):
        raise NotImplementedError()

    def mirror_one(self, representation, mirror_to, collection=None):
        """Uploads an encrypted book to the encrypted_repository via LCP License Server

        :param representation: Book's representation
        :type representation: Representation

        :param mirror_to: Mirror URL
        :type mirror_to: string

        :param collection: Collection
        :type collection: Optional[core.model.collection.Collection]
        """
        db = Session.object_session(representation)
        bucket = self.get_bucket(S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY)
        content_root = self.content_root(bucket)
        identifier = mirror_to.replace(content_root, '')
        lcp_importer = self._create_lcp_importer(collection)

        # First, we need to copy unencrypted book's content to a temporary file
        with tempfile.NamedTemporaryFile(suffix=representation.extension(representation.media_type)) as temporary_file:
            temporary_file.write(representation.content_fh().read())
            temporary_file.flush()

            # Secondly, we execute import:
            # 1. Encrypt the temporary file containing the unencrypted book using lcpencrypt
            # 2. Send the encrypted book to the LCP License Server
            # 3. LCP License Server generates license metadata
            # 4. LCP License Server uploads the encrypted book to the encrypted_repository (S3 or EFS)
            lcp_importer.import_book(db, temporary_file.name, identifier)

        # Thirdly, we remove unencrypted content from the database
        transaction = db.begin_nested()
        representation.content = None
        transaction.commit()

    def do_upload(self, representation):
        raise NotImplementedError()


MirrorUploader.IMPLEMENTATION_REGISTRY[LCPMirror.NAME] = LCPMirror
