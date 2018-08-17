# encoding: utf-8
import os
import datetime
from PIL import Image
from StringIO import StringIO
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
)
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
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
    MockS3Client,
)
from mirror import MirrorUploader
from config import CannotLoadConfiguration

class S3UploaderTest(DatabaseTest):

    def _integration(self, **settings):
        """Create and configure a simple S3 integration."""
        integration = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            settings=settings
        )
        integration.username = 'username'
        integration.password = 'password'
        return integration

    def _uploader(self, client_class=None, uploader_class=None, **settings):
        """Create a simple S3Uploader."""
        integration = self._integration(**settings)
        uploader_class = uploader_class or S3Uploader
        return uploader_class(integration, client_class=client_class)


class TestS3Uploader(S3UploaderTest):

    def test_names(self):
        # The NAME associated with this class must be the same as its
        # key in the MirrorUploader implementation registry, and it's
        # better if it's the same as the name of the external
        # integration.
        eq_(S3Uploader.NAME, ExternalIntegration.S3)
        eq_(S3Uploader,
            MirrorUploader.IMPLEMENTATION_REGISTRY[ExternalIntegration.S3])

    def test_instantiation(self):
        # If there is a configuration but it's misconfigured, an error
        # is raised.
        integration = self._external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )
        assert_raises_regexp(
            CannotLoadConfiguration, 'without both access_key and secret_key',
            MirrorUploader.implementation, integration
        )

        # Otherwise, it builds just fine.
        integration.username = 'your-access-key'
        integration.password = 'your-secret-key'
        integration.setting(S3Uploader.URL_TEMPLATE_KEY).value='a transform'
        uploader = MirrorUploader.implementation(integration)
        eq_(True, isinstance(uploader, S3Uploader))

        # The URL_TEMPLATE_KEY setting becomes the .url_transform
        # attribute on the S3Uploader object.
        eq_('a transform', uploader.url_transform)

    def test_custom_client_class(self):
        """You can specify a client class to use instead of boto3.client."""
        integration = self._integration()
        uploader = S3Uploader(integration, MockS3Client)
        assert isinstance(uploader.client, MockS3Client)

    def test_get_bucket(self):
        buckets = {
            S3Uploader.OA_CONTENT_BUCKET_KEY : 'banana',
            S3Uploader.BOOK_COVERS_BUCKET_KEY : 'bucket'
        }
        buckets_plus_irrelevant_setting = dict(buckets)
        buckets_plus_irrelevant_setting['not-a-bucket-at-all'] = "value"
        uploader = self._uploader(**buckets_plus_irrelevant_setting)

        # This S3Uploader knows about the configured buckets.  It
        # wasn't informed of the irrelevant 'not-a-bucket-at-all'
        # setting.
        eq_(buckets, uploader.buckets)

        # get_bucket just does a lookup in .buckets
        uploader.buckets['foo'] = object()
        result = uploader.get_bucket('foo')
        eq_(uploader.buckets['foo'], result)

    def test_url(self):
        m = S3Uploader.url
        eq_("https://s3.amazonaws.com/a-bucket/a-path", m("a-bucket", "a-path"))
        eq_("https://s3.amazonaws.com/a-bucket/a-path", m("a-bucket", "/a-path"))
        eq_("http://a-bucket.com/a-path", m("http://a-bucket.com/", "a-path"))
        eq_("https://a-bucket.com/a-path",
            m("https://a-bucket.com/", "/a-path"))

    def test_final_mirror_url(self):
        # By default, the mirror URL is not modified.
        uploader = self._uploader()
        eq_(S3Uploader.URL_TEMPLATE_DEFAULT, uploader.url_transform)
        eq_(u'https://s3.amazonaws.com/bucket/the+key',
            uploader.final_mirror_url("bucket", "the key"))

        uploader.url_transform = S3Uploader.URL_TEMPLATE_HTTP
        eq_(u'http://bucket/the+k%C3%ABy',
            uploader.final_mirror_url("bucket", "the këy"))

        uploader.url_transform = S3Uploader.URL_TEMPLATE_HTTPS
        eq_(u'https://bucket/key',
            uploader.final_mirror_url("bucket", "key"))

    def test_key_join(self):
        """Test the code used to build S3 keys from parts."""
        parts = ["Gutenberg", "Gutenberg ID", 1234, "Die Flügelmaus.epub"]
        eq_('Gutenberg/Gutenberg+ID/1234/Die+Fl%C3%BCgelmaus.epub',
            S3Uploader.key_join(parts))

    def test_cover_image_root(self):
        bucket = u'test-book-covers-s3-bucket'
        m = S3Uploader.cover_image_root

        gutenberg_illustrated = DataSource.lookup(
            self._db, DataSource.GUTENBERG_COVER_GENERATOR)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        eq_("https://s3.amazonaws.com/test-book-covers-s3-bucket/Gutenberg+Illustrated/",
            m(bucket, gutenberg_illustrated))
        eq_("https://s3.amazonaws.com/test-book-covers-s3-bucket/Overdrive/",
            m(bucket, overdrive))
        eq_("https://s3.amazonaws.com/test-book-covers-s3-bucket/scaled/300/Overdrive/",
            m(bucket, overdrive, 300))

    def test_content_root(self):
        bucket = u'test-open-access-s3-bucket'
        m = S3Uploader.content_root
        eq_(
            "https://s3.amazonaws.com/test-open-access-s3-bucket/",
            m(bucket)
        )

        # There is nowhere to store content that is not open-access.
        assert_raises(
            NotImplementedError,
            m, bucket, open_access=False
        )

    def test_book_url(self):
        identifier = self._identifier(foreign_id="ABOOK")
        buckets = {S3Uploader.OA_CONTENT_BUCKET_KEY : 'thebooks'}
        uploader = self._uploader(**buckets)
        m = uploader.book_url

        eq_(u'https://s3.amazonaws.com/thebooks/Gutenberg+ID/ABOOK.epub',
            m(identifier))

        # The default extension is .epub, but a custom extension can
        # be specified.
        eq_(u'https://s3.amazonaws.com/thebooks/Gutenberg+ID/ABOOK.pdf',
            m(identifier, extension='pdf'))

        eq_(u'https://s3.amazonaws.com/thebooks/Gutenberg+ID/ABOOK.pdf',
            m(identifier, extension='.pdf'))

        # If a data source is provided, the book is stored underneath the
        # data source.
        unglueit = DataSource.lookup(self._db, DataSource.UNGLUE_IT)
        eq_(u'https://s3.amazonaws.com/thebooks/unglue.it/Gutenberg+ID/ABOOK.epub',
            m(identifier, data_source=unglueit))

        # If a title is provided, the book's filename incorporates the
        # title, for the benefit of people who download the book onto
        # their hard drive.
        eq_(u'https://s3.amazonaws.com/thebooks/Gutenberg+ID/ABOOK/On+Books.epub',
            m(identifier, title="On Books"))

        # Non-open-access content can't be stored.
        assert_raises(NotImplementedError, m, identifier, open_access=False)

    def test_cover_image_url(self):
        identifier = self._identifier(foreign_id="ABOOK")
        buckets = {S3Uploader.BOOK_COVERS_BUCKET_KEY : 'thecovers'}
        uploader = self._uploader(**buckets)
        m = uploader.cover_image_url

        unglueit = DataSource.lookup(self._db, DataSource.UNGLUE_IT)
        identifier = self._identifier(foreign_id="ABOOK")
        eq_(u'https://s3.amazonaws.com/thecovers/scaled/601/unglue.it/Gutenberg+ID/ABOOK/filename',
            m(unglueit, identifier, "filename", scaled_size=601))

    def test_bucket_and_filename(self):
        m = S3Uploader.bucket_and_filename
        eq_(("bucket", "directory/filename.jpg"),
            m("https://s3.amazonaws.com/bucket/directory/filename.jpg"))

        eq_(("book-covers.nypl.org", "directory/filename.jpg"),
            m("http://book-covers.nypl.org/directory/filename.jpg"))

    def test_mirror_one(self):
        edition, pool = self._edition(with_license_pool=True)
        original_cover_location = "http://example.com/a-cover.png"
        content = open(self.sample_cover_path("test-book-cover.png")).read()
        cover, ignore = pool.add_link(
            Hyperlink.IMAGE, original_cover_location, edition.data_source,
            Representation.PNG_MEDIA_TYPE,
            content=content
        )
        cover_rep = cover.resource.representation
        eq_(None, cover_rep.mirrored_at)

        original_epub_location = "https://books.com/a-book.epub"
        epub, ignore = pool.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, original_epub_location,
            edition.data_source, Representation.EPUB_MEDIA_TYPE,
            content="i'm an epub"
        )
        epub_rep = epub.resource.representation
        eq_(None, epub_rep.mirrored_at)

        s3 = self._uploader(MockS3Client)

        # Mock final_mirror_url so we can verify that it's called with
        # the right arguments
        def mock_final_mirror_url(bucket, key):
            return "final_mirror_url was called with bucket %s, key %s" % (
                bucket, key
            )
        s3.final_mirror_url = mock_final_mirror_url

        book_url = "http://books-go/here.epub"
        cover_url = "http://s3.amazonaws.com/covers-go/here.png"
        s3.mirror_one(cover.resource.representation, cover_url)
        s3.mirror_one(epub.resource.representation, book_url)
        [[data1, bucket1, key1, args1, ignore1],
         [data2, bucket2, key2, args2, ignore2],] = s3.client.uploads

        # Both representations have had .mirror_url set and been
        # mirrored to those URLs.
        assert data1.startswith(b'\x89')
        eq_("covers-go", bucket1)
        eq_("here.png", key1)
        eq_(Representation.PNG_MEDIA_TYPE, args1['ContentType'])
        assert (datetime.datetime.utcnow() - cover_rep.mirrored_at).seconds < 10

        eq_("i'm an epub", data2)
        eq_("books-go", bucket2)
        eq_("here.epub", key2)
        eq_(Representation.EPUB_MEDIA_TYPE, args2['ContentType'])

        # In both cases, mirror_url was set to the result of final_mirror_url.
        eq_(
            u'final_mirror_url was called with bucket books-go, key here.epub',
            epub_rep.mirror_url
        )
        eq_(
            u'final_mirror_url was called with bucket covers-go, key here.png',
            cover_rep.mirror_url
        )

        # mirrored-at was set when the representation was 'mirrored'
        for rep in epub_rep, cover_rep:
            assert (datetime.datetime.utcnow() - rep.mirrored_at).seconds < 10

    def test_mirror_failure(self):
        edition, pool = self._edition(with_license_pool=True)
        original_epub_location = "https://books.com/a-book.epub"
        epub, ignore = pool.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, original_epub_location,
            edition.data_source, Representation.EPUB_MEDIA_TYPE,
            content="i'm an epub"
        )
        epub_rep = epub.resource.representation

        uploader = self._uploader(MockS3Client)

        # A network failure is treated as a transient error.
        uploader.client.fail_with = BotoCoreError()
        uploader.mirror_one(epub_rep, self._url)
        eq_(None, epub_rep.mirrored_at)
        eq_(None, epub_rep.mirror_exception)

        # An S3 credential failure is treated as a transient error.
        response = dict(
            Error=dict(
                Code=401,
                Message="Bad credentials",
            )
        )
        uploader.client.fail_with = ClientError(response, "SomeOperation")
        uploader.mirror_one(epub_rep, self._url)
        eq_(None, epub_rep.mirrored_at)
        eq_(None, epub_rep.mirror_exception)

        # Because the file was not successfully uploaded,
        # final_mirror_url was never called and mirror_url is
        # was not set.
        eq_(None, epub_rep.mirror_url)

        # A bug in the code is not treated as a transient error --
        # the exception propagates through.
        uploader.client.fail_with = Exception("crash!")
        assert_raises(Exception, uploader.mirror_one, epub_rep, self._url)

    def test_svg_mirroring(self):
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
        s3 = self._uploader(MockS3Client)
        s3.mirror_one(hyperlink.resource.representation, self._url)
        [[data, bucket, key, args, ignore]] = s3.client.uploads

        eq_(Representation.SVG_MEDIA_TYPE, args['ContentType'])
        assert 'svg' in data
        assert 'PNG' not in data
