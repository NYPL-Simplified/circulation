"""Turn local URLs into CDN URLs."""
import urlparse

from config import Configuration, CannotLoadConfiguration


def cdnify(url, cdns=None):
    """Turn local URLs into CDN URLs"""
    try:
        cdns = cdns or Configuration.cdns()
    except CannotLoadConfiguration:
        pass

    if not cdns:
        # No CDNs configured
        return url

    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)

    if netloc not in cdns:
        # This domain name is not covered by any of our CDNs.
        return url

    cdn_host = cdns[netloc]
    cdn_scheme, cdn_netloc, i1, i2, i3 = urlparse.urlsplit(cdn_host)
    return urlparse.urlunsplit((cdn_scheme, cdn_netloc, path, query, fragment))
