import datetime
from nose.tools import eq_
from . import DatabaseTest
from util.mirror import MirrorUploader

class DummySuccessUploader(MirrorUploader):

    def do_upload(self, representation):
        return None

class DummyFailureUploader(MirrorUploader):

    def do_upload(self, representation):
        return "I always fail."


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
