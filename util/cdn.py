"""Turn local URLs into CDN URLs."""

from nose.tools import set_trace
import urlparse
from .. import s3

def cdnify(url, cdn_host):
    if not cdn_host:
        return url
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
    cdn_scheme, cdn_netloc, i1, i2, i3 = urlparse.urlsplit(cdn_host)

    if netloc == 's3.amazonaws.com':
        # This is a URL like "http://s3.amazonaws.com/bucket/foo".
        # It's equivalent to "http://bucket/foo".
        # It should be CDNified to "http://cdn/foo".
        #
        # i.e. eliminate the bucket name.
        bucket, path = s3.S3Uploader.bucket_and_filename(
            url)

    return urlparse.urlunsplit((cdn_scheme, cdn_netloc, path, query, fragment))
