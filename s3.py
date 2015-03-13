from nose.tools import set_trace
from cStringIO import StringIO
import tinys3
import os
from urlparse import urlsplit
import urllib
from util.mirror import MirrorUploader
from requests.exceptions import ConnectionError

class S3Uploader(MirrorUploader):

    def __init__(self, access_key=None, secret_key=None):
        access_key = access_key or os.environ['AWS_ACCESS_KEY_ID']
        secret_key = secret_key or os.environ['AWS_SECRET_ACCESS_KEY']
        self.pool = tinys3.Pool(access_key, secret_key)

    S3_BASE = "http://s3.amazonaws.com/"

    @classmethod
    def url(cls, bucket, path):
        """The URL to a resource on S3 identified by bucket and path."""
        if path.startswith('/'):
            path = path[1:]
        url = cls.S3_BASE + bucket
        if not url.endswith('/'):
            url += '/'
        return url + path

    @classmethod
    def cover_image_root(cls, data_source, scaled_size=None):
        """The root URL to the S3 location of cover images for
        the given data source.
        """
        bucket = os.environ['BOOK_COVERS_S3_BUCKET']
        return cls._cover_image_root(bucket, data_source, scaled_size)

    @classmethod
    def _cover_image_root(cls, bucket, data_source, scaled_size):
        if scaled_size:
            path = "/scaled/%d/" % scaled_size
        else:
            path = "/"
        data_source_name = urllib.quote(data_source.name)
        path += data_source_name + "/"
        url = cls.url(bucket, path)
        if not url.endswith('/'):
            url += '/'
        return url

    @classmethod
    def content_root(cls, open_access=True):
        """The root URL to the S3 location of hosted content of
        the given type.
        """
        bucket = os.environ['OPEN_ACCESS_CONTENT_S3_BUCKET']
        return cls._content_root(bucket, open_access)

    @classmethod
    def _content_root(cls, bucket, open_access):
        if not open_access:
            raise NotImplementedError()
        return cls.url(bucket, '/')

    @classmethod
    def book_url(cls, identifier, extension='epub', open_access=True):
        """The path to the hosted EPUB file for the given identifier."""
        root = cls.content_root(open_access)
        args = [identifier.type, identifier.identifier]
        args = [urllib.quote(x) for x in args]
        return root + "%s/%s.%s" % tuple(args + [extension])

    @classmethod
    def cover_image_url(cls, data_source, identifier, filename=None, scaled_size=None):
        """The path to the hosted cover image for the given identifier."""
        root = cls.cover_image_root(data_source, scaled_size)
        args = [identifier.identifier, filename]
        args = [urllib.quote(x) for x in args]
        return root + "%s/%s" % tuple(args)

    @classmethod
    def bucket_and_filename(cls, url):
        scheme, netloc, path, query, fragment = urlsplit(url)
        if path.startswith('/'):
            path = path[1:]
        bucket, filename = path.split("/", 1)
        return bucket, filename

    def mirror_one(self, representation):
        """Mirror a single representation."""
        return self.mirror_batch([representation])

    def mirror_batch(self, representations):
        """Mirror a bunch of Representations at once."""
        filehandles = []
        requests = []
        representations_by_mirror_url = dict()
        
        for representation in representations:
            if not representation.mirror_url:
                representation.mirror_url = representation.url
            representations_by_mirror_url[representation.mirror_url] = (
                representation)
            bucket, remote_filename = self.bucket_and_filename(
                representation.mirror_url)
            fh = representation.content_fh()
            filehandles.append(fh)
            request = self.pool.upload(remote_filename, fh, bucket=bucket)
            requests.append(request)
        # Do the upload.

        def process_response(response):
            representation = representations_by_mirror_url[response.url]
            if response.status_code == 200:
                source = representation.local_content_path
                if representation.url != representation.mirror_url:
                    source = representation.url
                if source:
                    print "MIRRORED %s => %s" % (
                        source, representation.mirror_url)
                else:
                    print "MIRRORED %s" % representation.mirror_url
                representation.set_as_mirrored()
            else:
                representation.mirrored_at = None
                representation.mirror_exception = "Status code %d: %s" % (
                    response.status_code, response.content)

        try:
            for response in self.pool.as_completed(requests):
                process_response(response)
        except ConnectionError, e:
            # This is a transient error; we can just try again.
            pass

        # Close the filehandles
        for fh in filehandles:
            fh.close()

class DummyS3Uploader(S3Uploader):
    """A dummy uploader for use in tests."""
    def __init__(self, *args, **kwargs):
        self.uploaded = []

    @classmethod
    def cover_image_root(cls, data_source, scaled_size=None):
        return cls._cover_image_root(
            'test.cover.bucket', data_source, scaled_size)

    @classmethod
    def content_root(cls, open_access=True):
        """The root URL to the S3 location of hosted content of
        the given type.
        """
        return cls._content_root('test.content.bucket', open_access)

    def mirror_batch(self, representations):
        self.uploaded.extend(representations)
        for r in representations:
            r.set_as_mirrored()
