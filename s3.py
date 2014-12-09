from nose.tools import set_trace
import tinys3
import os
from urlparse import urlsplit

class S3Uploader(object):

    def __init__(self, access_key=None, secret_key=None):
        access_key = access_key or os.environ['AWS_ACCESS_KEY_ID']
        secret_key = secret_key or os.environ['AWS_SECRET_ACCESS_KEY']
        self.pool = tinys3.Pool(access_key, secret_key)

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

