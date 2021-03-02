import datetime
from nose.tools import (
    eq_,
    assert_raises_regexp,
)
from parameterized import parameterized

from . import DatabaseTest
from ..config import CannotLoadConfiguration
from ..mirror import MirrorUploader
from ..model import ExternalIntegration
from ..model.configuration import ExternalIntegrationLink
from ..s3 import S3Uploader, MinIOUploader, MinIOUploaderConfiguration, S3UploaderConfiguration


class DummySuccessUploader(MirrorUploader):
    def __init__(self, integration=None):
        pass

    def book_url(self, identifier, extension='.epub', open_access=True, data_source=None, title=None):
        pass

    def cover_image_url(self, data_source, identifier, filename=None, scaled_size=None):
        pass

    def sign_url(self, url, expiration=None):
        pass

    def split_url(self, url, unquote=True):
        pass

    def do_upload(self, representation):
        return None


class DummyFailureUploader(MirrorUploader):
    def __init__(self, integration=None):
        pass

    def book_url(self, identifier, extension='.epub', open_access=True, data_source=None, title=None):
        pass

    def cover_image_url(self, data_source, identifier, filename=None, scaled_size=None):
        pass

    def sign_url(self, url, expiration=None):
        pass

    def split_url(self, url, unquote=True):
        pass

    def do_upload(self, representation):
        return "I always fail."


class TestInitialization(DatabaseTest):
    """Test the ability to get a MirrorUploader for various aspects of site
    configuration.
    """

    @property
    def _integration(self):
        """Helper method to make a storage ExternalIntegration."""
        storage_name = "some storage"
        integration = self._external_integration("my protocol")
        integration.goal = ExternalIntegration.STORAGE_GOAL
        integration.name = storage_name
        return integration

    @parameterized.expand([
        ('s3_uploader', ExternalIntegration.S3, S3Uploader),
        (
                'minio_uploader',
                ExternalIntegration.MINIO,
                MinIOUploader,
                {MinIOUploaderConfiguration.ENDPOINT_URL: 'http://localhost'}
        )
    ])
    def test_mirror(self, name, protocol, uploader_class, settings=None):
        storage_name = "some storage"
        # If there's no integration with goal=STORAGE or name=storage_name,
        # MirrorUploader.mirror raises an exception.
        assert_raises_regexp(
            CannotLoadConfiguration,
            "No storage integration with name 'some storage' is configured",
            MirrorUploader.mirror, self._db, storage_name
        )

        # If there's only one, mirror() uses it to initialize a
        # MirrorUploader.
        integration = self._integration
        integration.protocol = protocol

        if settings:
            for key, value in settings.items():
                integration.setting(key).value = value

        uploader = MirrorUploader.mirror(self._db, integration=integration)

        assert isinstance(uploader, uploader_class)

    def test_integration_by_name(self):
        integration = self._integration

        # No name was passed so nothing is found
        assert_raises_regexp(
            CannotLoadConfiguration,
            "No storage integration with name 'None' is configured",
            MirrorUploader.integration_by_name, self._db
        )

        # Correct name was passed
        integration = MirrorUploader.integration_by_name(self._db, integration.name)
        assert isinstance(integration, ExternalIntegration)

    def test_for_collection(self):
        # This collection has no mirror_integration, so
        # there is no MirrorUploader for it.
        collection = self._collection()
        eq_(None, MirrorUploader.for_collection(collection, ExternalIntegrationLink.COVERS))

        # This collection has a properly configured mirror_integration,
        # so it can have an MirrorUploader.
        integration = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            username="username", password="password",
            settings={S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "some-covers"}
        )
        integration_link = self._external_integration_link(
            integration=collection._external_integration,
            other_integration=integration,
            purpose=ExternalIntegrationLink.COVERS
        )

        uploader = MirrorUploader.for_collection(collection, ExternalIntegrationLink.COVERS)
        assert isinstance(uploader, MirrorUploader)

    @parameterized.expand([
        (
                's3_uploader',
                ExternalIntegration.S3, S3Uploader
        ),
        (
                'minio_uploader',
                ExternalIntegration.MINIO,
                MinIOUploader,
                {MinIOUploaderConfiguration.ENDPOINT_URL: 'http://localhost'}
        )
    ])
    def test_constructor(self, name, protocol, uploader_class, settings=None):
        # You can't create a MirrorUploader with an integration
        # that's not designed for storage.
        integration = self._integration
        integration.goal = ExternalIntegration.LICENSE_GOAL
        integration.protocol = protocol

        if settings:
            for key, value in settings.items():
                integration.setting(key).value = value

        assert_raises_regexp(
            CannotLoadConfiguration,
            "from an integration with goal=licenses",
            uploader_class, integration
        )

    def test_implementation_registry(self):
        # The implementation class used for a given ExternalIntegration
        # is controlled by the integration's protocol and the contents
        # of the MirrorUploader's implementation registry.
        MirrorUploader.IMPLEMENTATION_REGISTRY["my protocol"] = DummyFailureUploader

        integration = self._integration
        uploader = MirrorUploader.mirror(self._db, integration=integration)
        assert isinstance(uploader, DummyFailureUploader)
        del MirrorUploader.IMPLEMENTATION_REGISTRY["my protocol"]


class TestMirrorUploader(DatabaseTest):
    """Test the basic workflow of MirrorUploader."""

    def test_mirror_batch(self):
        r1, ignore = self._representation()
        r2, ignore = self._representation()
        uploader = DummySuccessUploader()
        uploader.mirror_batch([r1, r2])
        assert r1.mirrored_at != None
        assert r2.mirrored_at != None

    def test_success_and_then_failure(self):
        r, ignore = self._representation()
        now = datetime.datetime.utcnow()
        DummySuccessUploader().mirror_one(r, '')
        assert r.mirrored_at > now
        eq_(None, r.mirror_exception)

        # Even if the original upload succeeds, a subsequent upload
        # may fail in a way that leaves the image in an inconsistent
        # state.
        DummyFailureUploader().mirror_one(r, '')
        eq_(None, r.mirrored_at)
        eq_("I always fail.", r.mirror_exception)
