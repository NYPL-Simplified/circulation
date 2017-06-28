import os
import contextlib
from PIL import Image
from StringIO import StringIO
from nose.tools import (
    set_trace,
    assert_raises_regexp,
    eq_,
)
from . import (
    DatabaseTest
)
from model import (
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Representation,
)
from s3 import (
    S3Uploader,
    DummyS3Uploader,
    MockS3Pool,
)
from config import CannotLoadConfiguration

class TestS3URLGeneration(DatabaseTest):

    def teardown(self):
        S3Uploader.__buckets__ = S3Uploader.UNINITIALIZED_BUCKETS
        super(TestS3URLGeneration, self).teardown()

    def test_initializes_with_uninitialized_buckets(self):
        eq_(S3Uploader.UNINITIALIZED_BUCKETS, S3Uploader.__buckets__)

    def test_from_config(self):
        # If there's no configuration for S3, S3Uploader.from_config
        # raises an exception.
        assert_raises_regexp(
            CannotLoadConfiguration,
            'Required S3 integration is not configured',
            S3Uploader.from_config, self._db
        )
        
        # If there is a configuration but it's misconfigured, an error
        # is raised.
        integration = self._external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )
        assert_raises_regexp(
            CannotLoadConfiguration, 'without both access_key and secret_key',
            S3Uploader.from_config, self._db
        )

        # Otherwise, it builds just fine.
        integration.username = 'your-access-key'
        integration.password = 'your-secret-key'
        uploader = S3Uploader.from_config(self._db)
        eq_(True, isinstance(uploader, S3Uploader))

        # Well, unless there are multiple S3 integrations, and it
        # doesn't know which one to choose!
        duplicate = self._external_integration(ExternalIntegration.S3)
        duplicate.goal = ExternalIntegration.STORAGE_GOAL
        assert_raises_regexp(
            CannotLoadConfiguration, 'Multiple S3 ExternalIntegrations configured',
            S3Uploader.from_config, self._db
        )

    def test_get_buckets(self):
        # When no buckets have been set, it raises an error.
        assert_raises_regexp(
            CannotLoadConfiguration, 'have not been initialized and no database session',
            S3Uploader.get_bucket, S3Uploader.OA_CONTENT_BUCKET_KEY
        )

        # So let's use an ExternalIntegration to set some buckets.
        integration = self._external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL,
            username='access', password='secret', settings={
                S3Uploader.OA_CONTENT_BUCKET_KEY : 'banana',
                S3Uploader.BOOK_COVERS_BUCKET_KEY : 'bucket'
            }
        )

        # If an object from the database is given, the buckets
        # will be initialized, even though they hadn't been yet.
        identifier = self._identifier()
        result = S3Uploader.get_bucket(
            S3Uploader.OA_CONTENT_BUCKET_KEY, sessioned_object=identifier
        )
        eq_('banana', result)

        # Generating the S3Uploader from_config also gives us S3 buckets.
        S3Uploader.__buckets__ = S3Uploader.UNINITIALIZED_BUCKETS
        S3Uploader.from_config(self._db)
        eq_('bucket', S3Uploader.get_bucket(S3Uploader.BOOK_COVERS_BUCKET_KEY))

        # Despite our new buckets, if a requested bucket isn't set,
        # an error ir raised.
        assert_raises_regexp(
            CannotLoadConfiguration, 'No S3 bucket found', S3Uploader.get_bucket,
            'nonexistent_bucket_key'
        )

    def test_content_root(self):
        bucket = u'test-open-access-s3-bucket'
        eq_("http://s3.amazonaws.com/test-open-access-s3-bucket/",
            S3Uploader.content_root(bucket))

    def test_cover_image_root(self):
        bucket = u'test-book-covers-s3-bucket'

        gutenberg_illustrated = DataSource.lookup(
            self._db, DataSource.GUTENBERG_COVER_GENERATOR)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/Gutenberg%20Illustrated/",
            S3Uploader.cover_image_root(bucket, gutenberg_illustrated))
        eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/Overdrive/",
            S3Uploader.cover_image_root(bucket, overdrive))
        eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/scaled/300/Overdrive/",
            S3Uploader.cover_image_root(bucket, overdrive, 300))


class TestUpload(DatabaseTest):

    def test_automatic_conversion_while_mirroring(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url

        # Create an SVG cover for the book.
        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

<svg xmlns="http://www.w3.org/2000/svg" width="100" height="50">
    <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
</svg>"""
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, 
            Representation.SVG_MEDIA_TYPE,
            content=svg)

        # 'Upload' it to S3.
        s3pool = MockS3Pool()
        s3 = S3Uploader(None, None, pool=s3pool)
        s3.mirror_one(hyperlink.resource.representation)
        [[filename, data, bucket, media_type, ignore]] = s3pool.uploads

        # The thing that got uploaded was a PNG, not the original SVG
        # file.
        eq_(Representation.PNG_MEDIA_TYPE, media_type)
        assert 'PNG' in data
        assert 'svg' not in data
