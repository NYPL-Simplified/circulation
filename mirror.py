import datetime
from abc import abstractmethod, ABCMeta
from urlparse import urlsplit

from config import CannotLoadConfiguration


class MirrorUploader(object):
    """Handles the job of uploading a representation's content to
    a mirror that we control.
    """

    __metaclass__ = ABCMeta

    STORAGE_GOAL = u'storage'

    # Depending on the .protocol of an ExternalIntegration with
    # .goal=STORAGE, a different subclass might be initialized by
    # sitewide() or for_collection(). A subclass that wants to take
    # advantage of this should add a mapping here from its .protocol
    # to itself.
    IMPLEMENTATION_REGISTRY = {}

    @classmethod
    def mirror(cls, _db, storage_name=None, integration=None):
        """Create a MirrorUploader from an integration or storage name.

        :param storage_name: The name of the storage integration.
        :param integration: The external integration.

        :return: A MirrorUploader.

        :raise: CannotLoadConfiguration if no integration with
            goal==STORAGE_GOAL is configured.
        """
        if not integration:
            integration = cls.integration_by_name(_db, storage_name)
        return cls.implementation(integration)

    @classmethod
    def integration_by_name(cls, _db, storage_name=None):
        """Find the ExternalIntegration for the mirror by storage name."""
        from model import ExternalIntegration
        qu = _db.query(ExternalIntegration).filter(
            ExternalIntegration.goal==cls.STORAGE_GOAL,
            ExternalIntegration.name==storage_name
        )
        integrations = qu.all()
        if not integrations:
            raise CannotLoadConfiguration(
                "No storage integration with name '%s' is configured." % storage_name
            )

        [integration] = integrations
        return integration

    @classmethod
    def for_collection(cls, collection, purpose):
        """Create a MirrorUploader for the given Collection.

        :param collection: Use the mirror configuration for this Collection.
        :param purpose: Use the purpose of the mirror configuration.

        :return: A MirrorUploader, or None if the Collection has no
            mirror integration.
        """
        from model import ExternalIntegration
        try:
            from model import Session
            _db = Session.object_session(collection)
            integration = ExternalIntegration.for_collection_and_purpose(_db, collection, purpose)
        except CannotLoadConfiguration, e:
            return None
        return cls.implementation(integration)

    @classmethod
    def implementation(cls, integration):
        """Instantiate the appropriate implementation of MirrorUploader
        for the given ExternalIntegration.
        """
        if not integration:
            return None
        implementation_class = cls.IMPLEMENTATION_REGISTRY.get(
            integration.protocol, cls
        )
        return implementation_class(integration)

    def __init__(self, integration, host):
        """Instantiate a MirrorUploader from an ExternalIntegration.

        :param integration: An ExternalIntegration configuring the credentials
           used to upload things.
        :type integration: ExternalIntegration

        :param host: Base host used by the mirror
        :type host: string
        """
        if integration.goal != self.STORAGE_GOAL:
            # This collection's 'mirror integration' isn't intended to
            # be used to mirror anything.
            raise CannotLoadConfiguration(
                "Cannot create an MirrorUploader from an integration with goal=%s" %
                integration.goal
            )

        self._host = host

        # Subclasses will override this to further configure the client
        # based on the credentials in the ExternalIntegration.

    def do_upload(self, representation):
        raise NotImplementedError()

    def mirror_one(self, representation):
        """Mirror a single Representation."""
        now = datetime.datetime.utcnow()
        exception = self.do_upload(representation)
        representation.mirror_exception = exception
        if exception:
            representation.mirrored_at = None
        else:
            representation.mirrored_at = now

    def mirror_batch(self, representations):
        """Mirror a batch of Representations at once."""

        for representation in representations:
            self.mirror_one(representation)

    def book_url(self, identifier, extension='.epub', open_access=True,
                 data_source=None, title=None):
        """The URL of the hosted EPUB file for the given identifier.

        This does not upload anything to the URL, but it is expected
        that calling mirror() on a certain Representation object will
        make that representation end up at that URL.
        """
        raise NotImplementedError()

    def cover_image_url(self, data_source, identifier, filename=None,
                        scaled_size=None):
        """The URL of the hosted cover image for the given identifier.

        This does not upload anything to the URL, but it is expected
        that calling mirror() on a certain Representation object will
        make that representation end up at that URL.
        """
        raise NotImplementedError()

    def sign_url(self, url, expiration=None):
        """Signs a URL and make it expirable

        :param url: URL
        :type url: string

        :param expiration: (Optional) Time in seconds for the presigned URL to remain valid.
            Default value depends on a specific implementation
        :type expiration: int

        :return: Signed expirable link
        :rtype: string
        """
        raise NotImplementedError()

    def is_self_url(self, url):
        """Determines whether the URL has the mirror's host or a custom domain

        :param url: The URL
        :type url: string

        :return: Boolean value indicating whether the URL has the mirror's host or a custom domain
        :rtype: bool
        """
        scheme, netloc, path, query, fragment = urlsplit(url)

        if netloc.endswith(self._host):
            return True
        else:
            return False

    @abstractmethod
    def split_url(self, url, unquote=True):
        """Splits the URL into the components: container (bucket) and file path

        :param url: URL
        :type url: string

        :param unquote: Boolean value indicating whether it's required to unquote URL elements
        :type unquote: bool

        :return: Tuple (bucket, file path)
        :rtype: Tuple[string, string]
        """
        raise NotImplementedError()
