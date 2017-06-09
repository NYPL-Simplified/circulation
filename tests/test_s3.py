import os
import contextlib
from PIL import Image
from StringIO import StringIO
from nose.tools import (
    set_trace,
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

class TestS3URLGeneration(DatabaseTest):

    def test_content_root(self):
        bucket = u'test-open-access-s3-bucket'
        self._external_integration(
            ExternalIntegration.S3,
            goal=ExternalIntegration.OA_CONTENT_GOAL,
            url=bucket,
        )
        eq_("http://s3.amazonaws.com/test-open-access-s3-bucket/",
            S3Uploader.content_root(bucket))

    def test_cover_image_root(self):
        bucket = u'test-book-covers-s3-bucket'
        self._external_integration(
            ExternalIntegration.S3,
            goal=ExternalIntegration.BOOK_COVERS_GOAL,
            url=bucket)

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
        s3 = S3Uploader(None, pool=s3pool)
        s3.mirror_one(hyperlink.resource.representation)
        [[filename, data, bucket, media_type, ignore]] = s3pool.uploads

        # The thing that got uploaded was a PNG, not the original SVG
        # file.
        eq_(Representation.PNG_MEDIA_TYPE, media_type)
        assert 'PNG' in data
        assert 'svg' not in data
