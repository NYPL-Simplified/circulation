"""Turn local URLs into CDN URLs."""
import urlparse

from config import Configuration, CannotLoadConfiguration
from core.mirror import MirrorUploader
from core.model import ExternalIntegration
from core.s3 import S3Uploader


def _get_mirrors():
    """Returns a list of configured mirrors

    :return: List of configured mirrors
    :rtype: List[MirrorUploader]
    """
    from model import SessionManager
    database_url = Configuration.database_url()
    _db = SessionManager.session(database_url)

    storage_integrations = ExternalIntegration.for_goal(_db, MirrorUploader.STORAGE_GOAL)
    has_s3_mirror = False

    for storage_integration in storage_integrations:
        mirror = MirrorUploader.implementation(storage_integration)

        if isinstance(mirror, S3Uploader):
            has_s3_mirror = True

        yield mirror

    if not has_s3_mirror:
        # We need to preserve backward compatibility and for S3 based URLs
        # even when there are no configured S3 integrations

        class CDNfiedS3Uploader(S3Uploader):
            """Dummy class used for backward compatibility
                and used for calling S3Uploader's is_self_url and split_url
            """
            def __init__(self):
                """Initializes a new instance of CDNfiedS3Uploader class"""
                self._host = S3Uploader.S3_HOST

        yield CDNfiedS3Uploader()


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

    for mirror in _get_mirrors():
        if mirror.is_self_url(url):
            netloc, path = mirror.split_url(url, unquote=False)
            break

    if netloc not in cdns:
        # This domain name is not covered by any of our CDNs.
        return url

    cdn_host = cdns[netloc]
    cdn_scheme, cdn_netloc, i1, i2, i3 = urlparse.urlsplit(cdn_host)
    return urlparse.urlunsplit((cdn_scheme, cdn_netloc, path, query, fragment))

