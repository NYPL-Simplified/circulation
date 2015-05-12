"""Turn local URLs into CDN URLs."""

import urlparse

def cdnify(url, cdn_host):
    if not cdn_host:
        return url
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
    scheme, netloc, i1, i2, i3 = urlparse.urlsplit(cdn_host)
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))
