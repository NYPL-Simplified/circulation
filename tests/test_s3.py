# encoding: utf-8
import os
import datetime
from PIL import Image
from StringIO import StringIO
import urllib
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
from ..model import (
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Representation,
    create,
)
from ..s3 import (
    S3Uploader,
    MockS3Client,
    MultipartS3Upload,
)
from ..mirror import MirrorUploader
from ..config import CannotLoadConfiguration

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
        integration = self._external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )
        integration.username = 'your-access-key'
        integration.password = 'your-secret-key'
        integration.setting(S3Uploader.URL_TEMPLATE_KEY).value='a transform'
        uploader = MirrorUploader.implementation(integration)
        eq_(True, isinstance(uploader, S3Uploader))

        # The URL_TEMPLATE_KEY setting becomes the .url_transform
        # attribute on the S3Uploader object.
        eq_('a transform', uploader.url_transform)

    def test_empty_string(self):
        settings = {'username': '', 'password': ''}
        integration = self._external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL, settings=settings
        )
        uploader = S3Uploader(integration, client_class=MockS3Client)
        eq_(uploader.client.access_key, None)
        eq_(uploader.client.secret_key, None)

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
        parts = ["Gutenberg", b"Gutenberg ID", 1234, "Die Flügelmaus.epub"]
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

    def test_marc_file_root(self):
        bucket = u'test-marc-s3-bucket'
        m = S3Uploader.marc_file_root
        library = self._library(short_name="SHORT")
        eq_("https://s3.amazonaws.com/test-marc-s3-bucket/SHORT/",
            m(bucket, library))

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

    def test_marc_file_url(self):
        library = self._library(short_name="SHORT")
        lane = self._lane(display_name="Lane")
        buckets = {S3Uploader.MARC_BUCKET_KEY : 'marc'}
        uploader = self._uploader(**buckets)
        m = uploader.marc_file_url
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)
        eq_(u'https://s3.amazonaws.com/marc/SHORT/%s/Lane.mrc' % urllib.quote_plus(str(now)),
            m(library, lane, now))
        eq_(u'https://s3.amazonaws.com/marc/SHORT/%s-%s/Lane.mrc' % (
                urllib.quote_plus(str(yesterday)),
                urllib.quote_plus(str(now)),
            ),
            m(library, lane, now, yesterday))

    def test_bucket_and_filename(self):
        m = S3Uploader.bucket_and_filename
        eq_(("bucket", "directory/filename.jpg"),
            m("https://s3.amazonaws.com/bucket/directory/filename.jpg"))

        eq_(("book-covers.nypl.org", "directory/filename.jpg"),
            m("http://book-covers.nypl.org/directory/filename.jpg"))

        # By default, escaped characters in the filename are unescaped.
        eq_(("book-covers.nypl.org", "directory/filename with spaces!.jpg"),
            m("http://book-covers.nypl.org/directory/filename+with+spaces%21.jpg"))

        # But you can choose to leave them alone.
        eq_(("book-covers.nypl.org", "directory/filename+with+spaces%21.jpg"),
            m("http://book-covers.nypl.org/directory/filename+with+spaces%21.jpg", False))


    def test_mirror_one(self):
        edition, pool = self._edition(with_license_pool=True)
        original_cover_location = "http://example.com/a-cover.png"
        content = open(
            self.sample_cover_path("test-book-cover.png"), 'rb'
        ).read()
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

        eq_(b"i'm an epub", data2)
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
        assert b'svg' in data
        assert b'PNG' not in data

    def test_multipart_upload(self):
        class MockMultipartS3Upload(MultipartS3Upload):
            completed = None
            aborted = None

            def __init__(self, uploader, representation, mirror_to):
                self.parts = []
                MockMultipartS3Upload.completed = False
                MockMultipartS3Upload.aborted = False

            def upload_part(self, content):
                self.parts.append(content)

            def complete(self):
                MockMultipartS3Upload.completed = True

            def abort(self):
                MockMultipartS3Upload.aborted = True

        rep, ignore = create(
            self._db, Representation, url="http://books.mrc",
            media_type=Representation.MARC_MEDIA_TYPE)
                                        

        s3 = self._uploader(MockS3Client)

        # Successful upload
        with s3.multipart_upload(rep, rep.url, upload_class=MockMultipartS3Upload) as upload:
            eq_([], upload.parts)
            eq_(False, upload.completed)
            eq_(False, upload.aborted)

            upload.upload_part("Part 1")
            upload.upload_part("Part 2")

            eq_(["Part 1", "Part 2"], upload.parts)

        eq_(True, MockMultipartS3Upload.completed)
        eq_(False, MockMultipartS3Upload.aborted)
        eq_(None, rep.mirror_exception)

        class FailingMultipartS3Upload(MockMultipartS3Upload):
            def upload_part(self, content):
                raise Exception("Error!")

        # Failed during upload
        with s3.multipart_upload(rep, rep.url, upload_class=FailingMultipartS3Upload) as upload:
            upload.upload_part("Part 1")

        eq_(False, MockMultipartS3Upload.completed)
        eq_(True, MockMultipartS3Upload.aborted)
        eq_("Error!", rep.mirror_exception)

        class AnotherFailingMultipartS3Upload(MockMultipartS3Upload):
            def complete(self):
                raise Exception("Error!")

        rep.mirror_exception = None
        # Failed during completion
        with s3.multipart_upload(rep, rep.url, upload_class=AnotherFailingMultipartS3Upload) as upload:
            upload.upload_part("Part 1")

        eq_(False, MockMultipartS3Upload.completed)
        eq_(True, MockMultipartS3Upload.aborted)
        eq_("Error!", rep.mirror_exception)

class TestMultiPartS3Upload(S3UploaderTest):
    def _representation(self):
        rep, ignore = create(
            self._db, Representation, url="http://bucket/books.mrc",
            media_type=Representation.MARC_MEDIA_TYPE)
        return rep

    def test_init(self):
        uploader = self._uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        eq_(uploader, upload.uploader)
        eq_(rep, upload.representation)
        eq_("bucket", upload.bucket)
        eq_("books.mrc", upload.filename)
        eq_(1, upload.part_number)
        eq_([], upload.parts)
        eq_(1, upload.upload.get("UploadId"))

        uploader.client.fail_with = Exception("Error!")
        assert_raises(Exception, MultipartS3Upload, uploader, rep, rep.url)

    def test_upload_part(self):
        uploader = self._uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        eq_([{'Body': 'Part 1', 'UploadId': 1, 'PartNumber': 1, 'Bucket': 'bucket','Key': 'books.mrc'},
             {'Body': 'Part 2', 'UploadId': 1, 'PartNumber': 2, 'Bucket': 'bucket', 'Key': 'books.mrc'}],
            uploader.client.parts)
        eq_(3, upload.part_number)
        eq_([{'ETag': 'etag', 'PartNumber': 1}, {'ETag': 'etag', 'PartNumber': 2}],
            upload.parts)

        uploader.client.fail_with = Exception("Error!")
        assert_raises(Exception, upload.upload_part, "Part 3")

    def test_complete(self):
        uploader = self._uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        upload.complete()
        eq_([{'Bucket': 'bucket', 'Key': 'books.mrc', 'UploadId': 1, 'MultipartUpload': {
                 'Parts': [{'ETag': 'etag', 'PartNumber': 1}, {'ETag': 'etag', 'PartNumber': 2}],
            }}], uploader.client.uploads)

    def test_abort(self):
        uploader = self._uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        upload.abort()
        eq_([], uploader.client.parts)
