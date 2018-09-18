import datetime
from nose.tools import (
    eq_,
    assert_raises_regexp,
)
from . import DatabaseTest
from core.config import CannotLoadConfiguration
from mirror import MirrorUploader
from model import ExternalIntegration

class DummySuccessUploader(MirrorUploader):

    def __init__(self, integration=None):
        pass

    def do_upload(self, representation):
        return None

class DummyFailureUploader(MirrorUploader):

    def __init__(self, integration=None):
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
        integration = self._external_integration("my protocol")
        integration.goal = ExternalIntegration.STORAGE_GOAL
        return integration

    def test_sitewide(self):
        # If there's no integration with goal=STORAGE,
        # MirrorUploader.sitewide raises an exception.
        assert_raises_regexp(
            CannotLoadConfiguration,
            'No storage integration is configured',
            MirrorUploader.sitewide, self._db
        )

        # If there's only one, sitewide() uses it to initialize a
        # MirrorUploader.
        integration = self._integration
        uploader = MirrorUploader.sitewide(self._db)
        assert isinstance(uploader, MirrorUploader)

        # If there are multiple integrations with goal=STORAGE, no
        # sitewide configuration can be determined.
        duplicate = self._integration
        assert_raises_regexp(
            CannotLoadConfiguration,
            'Multiple storage integrations are configured',
            MirrorUploader.sitewide, self._db
        )

    def test_for_collection(self):
        # This collection has no mirror_integration, so
        # there is no MirrorUploader for it.
        collection = self._collection()
        eq_(None, MirrorUploader.for_collection(collection))

        # We can tell the method that we're okay with a sitewide
        # integration instead of an integration specifically for this
        # collection.
        sitewide_integration = self._integration
        uploader = MirrorUploader.for_collection(collection, use_sitewide=True)
        assert isinstance(uploader, MirrorUploader)

        # This collection has a properly configured mirror_integration,
        # so it can have an MirrorUploader.
        collection.mirror_integration = self._integration
        uploader = MirrorUploader.for_collection(collection)
        assert isinstance(uploader, MirrorUploader)

        # This collection has a mirror_integration but it has the
        # wrong goal, so attempting to make an MirrorUploader for it
        # raises an exception.
        collection.mirror_integration.goal = ExternalIntegration.LICENSE_GOAL
        assert_raises_regexp(
            CannotLoadConfiguration,
            "from an integration with goal=licenses",
            MirrorUploader.for_collection, collection
        )

    def test_constructor(self):
        # You can't create a MirrorUploader with an integration
        # that's not designed for storage.
        integration = self._integration
        integration.goal = ExternalIntegration.LICENSE_GOAL
        assert_raises_regexp(
            CannotLoadConfiguration,
            "from an integration with goal=licenses",
            MirrorUploader, integration
        )

    def test_implementation_registry(self):
        """The implementation class used for a given ExternalIntegration
        is controlled by the integration's protocol and the contents
        of the MirrorUploader's implementation registry.
        """
        MirrorUploader.IMPLEMENTATION_REGISTRY["my protocol"] = DummyFailureUploader

        integration = self._integration
        uploader = MirrorUploader.sitewide(self._db)
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
        DummySuccessUploader().mirror_one(r)
        assert r.mirrored_at > now
        eq_(None, r.mirror_exception)

        # Even if the original upload succeeds, a subsequent upload
        # may fail in a way that leaves the image in an inconsistent
        # state.
        DummyFailureUploader().mirror_one(r)
        eq_(None, r.mirrored_at)
        eq_("I always fail.", r.mirror_exception)
