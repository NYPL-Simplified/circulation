from nose.tools import set_trace
import tinys3
import os
from urlparse import urlsplit
import urllib

class S3Uploader(object):

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
        if scaled_size:
            path = "/scaled/%d/" % scaled_size
        else:
            path = "/"
        data_source_name = urllib.quote(data_source.name)
        path += data_source_name + "/"
        return cls.url(bucket, path)

    @classmethod
    def content_root(cls, open_access=True):
        """The root URL to the S3 location of hosted content of
        the given type.
        """
        if not open_access:
            raise NotImplementedError()
        bucket = os.environ['OPEN_ACCESS_CONTENT_S3_BUCKET']
        return cls.url(bucket, '/')

    @classmethod
    def bucket_and_filename(cls, url):
        scheme, netloc, path, query, fragment = urlsplit(url)
        if path.startswith('/'):
            path = path[1:]
        bucket, filename = path.split("/", 1)
        return bucket, filename

    def upload_resources(self, paths):
        """Upload a batch of resources in bulk."""
        requests = []
        filehandles = []
        for local_path, final_url in paths:
            bucket, remote_filename = self.bucket_and_filename(final_url)
            fh = open(local_path, 'rb')
            filehandles.append(fh)
            requests.append(self.pool.upload(remote_filename, fh, bucket=bucket))
        # Do the upload.
        for r in self.pool.as_completed(requests):
            print r.url
            pass
        # Close the filehandles.
        for i in filehandles:
            fh.close()

class DummyS3Uploader(S3Uploader):
    """A dummy uploader for use in tests."""
    def __init__(self, *args, **kwargs):
        self.uploaded = []

    def upload_resources(self, resources):
        self.uploaded.extend(resources)
