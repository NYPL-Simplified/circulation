"""Turn local URLs into CDN URLs."""

from nose.tools import set_trace
import urlparse
from core.model import ExternalIntegration
from s3 import S3Uploader

def cdnify(_db, url):
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)

    if netloc == 's3.amazonaws.com':
        # This is a URL like "http://s3.amazonaws.com/bucket/foo".
        # It's equivalent to "http://bucket/foo".
        # i.e. treat the bucket name as the netloc.
        bucket, path = S3Uploader.bucket_and_filename(url)
        netloc = bucket

    cdn = ExternalIntegration.lookup(
        _db, ExternalIntegration.CDN, goal=netloc
    )
    if not cdn:
        # This domain name is not covered by any of our CDNs.
        return url

    cdn_host = cdn.url
    cdn_scheme, cdn_netloc, i1, i2, i3 = urlparse.urlsplit(cdn_host)
    return urlparse.urlunsplit((cdn_scheme, cdn_netloc, path, query, fragment))
