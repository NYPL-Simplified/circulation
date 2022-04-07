

class EntryPoint(object):

    """A EntryPoint is a top-level entry point into a library's Lane structure
    that may apply additional filters to the Lane structure.

    The "Books" and "Audiobooks" entry points (defined in the
    EbooksEntryPoint and AudiobooksEntryPoint classes) are different
    views on a library's Lane structure; each applies an additional
    filter against Edition.medium.

    Each individual EntryPoint should be represented as a subclass of
    EntryPoint, and should be registered with the overall EntryPoint
    class by calling EntryPoint.register.

    The list of entry points shows up as a facet group in a library's
    top-level grouped feed, and in search results. The SimplyE client
    renders entry points as a set of tabs.
    """

    # The name of the per-library setting that controls which entry points are
    # enabled.
    ENABLED_SETTING = "enabled_entry_points"

    ENTRY_POINTS = []
    DEFAULT_ENABLED = []
    DISPLAY_TITLES = {}
    BY_INTERNAL_NAME = {}

    # A distinctive URI designating the sort of thing found through this
    # EntryPoint.
    URI = None

    @classmethod
    def register(cls, entrypoint_class, display_title, default_enabled=False):
        """Register the given subclass with the master registry
        kept in the EntryPoint class.

        :param entrypoint_class: A subclass of EntryPoint.
        :param display_title: The title to use when displaying this entry point
            to patrons.
        :param default_enabled: New libraries should have this entry point
            enabled by default.
        """
        value = getattr(entrypoint_class, 'INTERNAL_NAME', None)
        if not value:
            raise ValueError(
                "EntryPoint class %s must define INTERNAL_NAME." % entrypoint_class.__name__
            )
        if value in cls.BY_INTERNAL_NAME:
            raise ValueError(
                "Duplicate entry point internal name: %s" % value
            )
        if display_title in list(cls.DISPLAY_TITLES.values()):
            raise ValueError(
                "Duplicate entry point display name: %s" % display_title
            )
        cls.DISPLAY_TITLES[entrypoint_class] = display_title
        cls.BY_INTERNAL_NAME[value] = entrypoint_class
        cls.ENTRY_POINTS.append(entrypoint_class)
        if default_enabled:
            cls.DEFAULT_ENABLED.append(entrypoint_class)
        cls.DISPLAY_TITLES[entrypoint_class] = display_title

    @classmethod
    def unregister(cls, entrypoint_class):
        """Undo a subclass's registration.

        Only used in tests.
        """
        cls.ENTRY_POINTS.remove(entrypoint_class)
        del cls.BY_INTERNAL_NAME[entrypoint_class.INTERNAL_NAME]
        del cls.DISPLAY_TITLES[entrypoint_class]
        if entrypoint_class in cls.DEFAULT_ENABLED:
            cls.DEFAULT_ENABLED.remove(entrypoint_class)

    @classmethod
    def modify_search_filter(cls, filter):
        """If necessary, modify an ElasticSearch Filter object so that it
        restricts results to items shown through this entry point.

        The default behavior is not to change the Filter object at all.

        :param filter: An external_search.Filter object.
        """
        return filter

    @classmethod
    def modify_database_query(cls, _db, qu):
        """If necessary, modify a database query so that it restricts results
        to items shown through this entry point.

        The default behavior is to not change a database query at all.
        """
        return qu


class EverythingEntryPoint(EntryPoint):
    """An entry point that has everything."""
    INTERNAL_NAME = "All"
    URI = "http://schema.org/CreativeWork"
EntryPoint.register(EverythingEntryPoint, "All")


class MediumEntryPoint(EntryPoint):
    """A entry point that creates a view on one specific medium.

    The medium is expected to be the entry point's INTERNAL_NAME.

    The URI is expected to be the one in
    Edition.schema_to_additional_type[INTERNAL_NAME]
    """

    @classmethod
    def modify_database_query(cls, _db, qu):
        """Modify a query against Work+LicensePool+Edition
        to match only items with the right medium.
        """
        from .model import Edition
        return qu.filter(Edition.medium==cls.INTERNAL_NAME)

    @classmethod
    def modify_search_filter(cls, filter):
        """Modify an external_search.Filter object so it only finds
        titles available through this EntryPoint.

        :param filter: An external_search.Filter object.
        """
        filter.media = [cls.INTERNAL_NAME]


class EbooksEntryPoint(MediumEntryPoint):
    INTERNAL_NAME = "Book"
    URI = "http://schema.org/EBook"
EntryPoint.register(EbooksEntryPoint, "eBooks", default_enabled=True)

class AudiobooksEntryPoint(MediumEntryPoint):
    INTERNAL_NAME = "Audio"
    URI = "http://bib.schema.org/Audiobook"
EntryPoint.register(AudiobooksEntryPoint, "Audiobooks")
