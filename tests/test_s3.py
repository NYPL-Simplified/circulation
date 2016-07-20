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
from config import (
    Configuration,
    temp_config as core_temp_config
)
from model import (
    DataSource,
    Hyperlink,
    Representation,
)
from s3 import (
    S3Uploader,
    DummyS3Uploader,
    MockS3Pool,
)

class TestS3URLGeneration(DatabaseTest):
    
    @contextlib.contextmanager
    def temp_config(self):
        with core_temp_config() as tmp:
            i = tmp['integrations']
            S3 = Configuration.S3_INTEGRATION
            i[S3] = {
                Configuration.S3_OPEN_ACCESS_CONTENT_BUCKET : 'test-open-access-s3-bucket',
                Configuration.S3_BOOK_COVERS_BUCKET : 'test-book-covers-s3-bucket'
            }
            yield tmp

    def test_content_root(self):
        with self.temp_config():
            eq_("http://s3.amazonaws.com/test-open-access-s3-bucket/",
                S3Uploader.content_root())

    def test_cover_image_root(self):
        with self.temp_config():
            gutenberg_illustrated = DataSource.lookup(
                self._db, DataSource.GUTENBERG_COVER_GENERATOR)
            overdrive = DataSource.lookup(
                self._db, DataSource.OVERDRIVE)
            eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/Gutenberg%20Illustrated/",
                S3Uploader.cover_image_root(gutenberg_illustrated))
            eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/Overdrive/",
                S3Uploader.cover_image_root(overdrive))
            eq_("http://s3.amazonaws.com/test-book-covers-s3-bucket/scaled/300/Overdrive/", 
                S3Uploader.cover_image_root(overdrive, 300))


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
        s3 = S3Uploader(pool=s3pool)
        s3.mirror_one(hyperlink.resource.representation)
        [[filename, data, bucket, media_type, ignore]] = s3pool.uploads

        # The thing that got uploaded was a PNG, not the original SVG
        # file.
        eq_(Representation.PNG_MEDIA_TYPE, media_type)
        assert 'PNG' in data
        assert 'svg' not in data
