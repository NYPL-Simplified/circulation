"""Turn local URLs into CDN URLs."""
import os, sys
import urlparse
from nose.tools import set_trace

from config import Configuration, CannotLoadConfiguration
from s3 import S3Uploader

def cdnify(url, cdns=None):
    """Turn local URLs into CDN URLs"""
    try:
        cdns = cdns or Configuration.cdns()
    except CannotLoadConfiguration, e:
        pass

    if not cdns:
        # No CDNs configured
        return url
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)

    if netloc == 's3.amazonaws.com':
        # This is a URL like "http://s3.amazonaws.com/bucket/foo".
        # It's equivalent to "http://bucket/foo".
        # i.e. treat the bucket name as the netloc.
        bucket, path = S3Uploader.bucket_and_filename(url)
        netloc = bucket

    if netloc not in cdns:
        # This domain name is not covered by any of our CDNs.
        return url

    cdn_host = cdns[netloc]
    cdn_scheme, cdn_netloc, i1, i2, i3 = urlparse.urlsplit(cdn_host)
    return urlparse.urlunsplit((cdn_scheme, cdn_netloc, path, query, fragment))
