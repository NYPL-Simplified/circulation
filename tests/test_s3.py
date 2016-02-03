import os
import contextlib
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
from model import DataSource
from s3 import S3Uploader

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
