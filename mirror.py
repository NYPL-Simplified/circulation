from nose.tools import set_trace
import datetime
from config import CannotLoadConfiguration

class MirrorUploader(object):

    """Handles the job of uploading a representation's content to 
    a mirror that we control.
    """

    STORAGE_GOAL = u'storage'

    # Depending on the .protocol of an ExternalIntegration with
    # .goal=STORAGE, a different subclass might be initialized by
    # sitewide() or for_collection(). A subclass that wants to take
    # advantage of this should add a mapping here from its .protocol
    # to itself.
    IMPLEMENTATION_REGISTRY = {}

    @classmethod
    def sitewide(cls, _db):
        """Create a MirrorUploader from a sitewide configuration.

        :return: A MirrorUploader.

        :raise: CannotLoadConfiguration if no integration with
        goal==STORAGE_GOAL is configured, or if multiple integrations
        are so configured.
        """
        integration = cls.sitewide_integration(_db)
        return cls.implementation(integration)

    @classmethod
    def sitewide_integration(cls, _db):
        """Find the ExternalIntegration for the site-wide mirror."""
        from model import ExternalIntegration
        qu = _db.query(ExternalIntegration).filter(
            ExternalIntegration.goal==cls.STORAGE_GOAL
        )
        integrations = qu.all()
        if not integrations:
            raise CannotLoadConfiguration(
                "No storage integration is configured."
            )
            return None

        if len(integrations) > 1:
            # If there are multiple integrations configured, none of
            # them can be the 'site-wide' configuration.
            raise CannotLoadConfiguration(
                'Multiple storage integrations are configured'
            )

        [integration] = integrations
        return integration

    @classmethod
    def for_collection(cls, collection, use_sitewide=False):
        """Create a MirrorUploader for the given Collection.

        :param collection: Use the mirror configuration for this Collection.

        :param use_sitewide: If there's no mirror for this specific Collection,
            should we return a sitewide mirror instead?

        :return: A MirrorUploader, or None if the Collection has no
            mirror integration.
        """
        integration = collection.mirror_integration
        if not integration:
            if use_sitewide:
                try:
                    from model import Session
                    _db = Session.object_session(collection)
                    return cls.sitewide(_db)
                except CannotLoadConfiguration, e:
                    return None
            else:
                return None
        return cls.implementation(integration)

    @classmethod
    def implementation(cls, integration):
        """Instantiate the appropriate implementation of MirrorUploader
        for the given ExternalIntegration.
        """
        implementation_class = cls.IMPLEMENTATION_REGISTRY.get(
            integration.protocol, cls
        )
        return implementation_class(integration)

    def __init__(self, integration):
        """Instantiate a MirrorUploader from an ExternalIntegration.

        :param integration: An ExternalIntegration configuring the credentials
           used to upload things.
        """
        if integration.goal != self.STORAGE_GOAL:
            # This collection's 'mirror integration' isn't intended to
            # be used to mirror anything.
            raise CannotLoadConfiguration(
                "Cannot create an MirrorUploader from an integration with goal=%s" %
                integration.goal
            )

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
