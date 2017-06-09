"""Turn local URLs into CDN URLs."""
from nose.tools import set_trace
import os, sys
util_dir = os.path.split(__file__)[0]
core_dir = os.path.split(util_dir)[0]

import urlparse
from s3 import S3Uploader

def cdnify(_db, url):
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)

    if netloc == 's3.amazonaws.com':
        # This is a URL like "http://s3.amazonaws.com/bucket/foo".
        # It's equivalent to "http://bucket/foo".
        # i.e. treat the bucket name as the netloc.
        bucket, path = S3Uploader.bucket_and_filename(url)
        netloc = bucket

    # TODO: Find a better way to import from model in core.util when
    # using the parent directory (e.g. circulation) or move cdnify into
    # an OPDS- or controller-focused class or file.
    if not core_dir in sys.path:
        sys.path.insert(0, core_dir)
        from model import ExternalIntegration
        sys.path = sys.path[1:]
    else:
        from model import ExternalIntegration

    cdn = ExternalIntegration.lookup(
        _db, ExternalIntegration.CDN, goal=netloc
    )
    if not cdn:
        # This domain name is not covered by any of our CDNs.
        return url

    cdn_host = cdn.url
    cdn_scheme, cdn_netloc, i1, i2, i3 = urlparse.urlsplit(cdn_host)
    return urlparse.urlunsplit((cdn_scheme, cdn_netloc, path, query, fragment))
