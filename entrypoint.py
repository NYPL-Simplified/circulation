from nose.tools import set_trace

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
    ENABLED_SETTING = "enabled_entry points"

    ENTRY_POINTS = []
    DEFAULT_ENABLED = []
    DISPLAY_TITLES = {}
    INTERNAL_NAMES = []

    @classmethod
    def register(cls, entry point_class, display_title, default_enabled=False):
        """Register the given subclass with the master registry
        kept in the EntryPoint class.

        :param entry point_class: A subclass of EntryPoint.
        :param display_title: The title to use when displaying this entry point
            to patrons.
        :param default_enabled: New libraries should have this entry point
            enabled by default.
        """
        value = getattr(entry point_class, 'INTERNAL_NAME', None)
        if not value:
            raise ValueError(
                "EntryPoint class %s must define INTERNAL_NAME." % entry point_class.__name__
            )
        if value in cls.INTERNAL_NAMES:
            raise ValueError(
                "Duplicate entry point internal name: %s" % value
            )
        cls.INTERNAL_NAMES.append(value)
        cls.ENTRY_POINTS.append(entry point_class)
        if default_enabled:
            cls.DEFAULT_ENABLED.append(entry point_class)
        cls.DISPLAY_TITLES[entry point_class] = display_title

    @classmethod
    def unregister(cls, entry point_class):
        """Undo a subclass's registration.

        Only used in tests.
        """
        cls.ENTRY_POINTS.remove(entry point_class)
        cls.INTERNAL_NAMES.remove(entry point_class.INTERNAL_NAME)
        del cls.DISPLAY_TITLES[entry point_class]

    @classmethod
    def modified_materialized_view_query(cls, qu):
        """Modify a query against the mv_works_for_lanes materialized view
        so it matches only items that belong in this entry point.
        """
        raise NotImplementedError()

    @classmethod
    def modified_search_arguments(cls, **kwargs):
        """If possible, modify the arguments to ExternalSearch.query_works()
        so that only items belonging to this entry point are found.

        Any items returned will be run through the materialized view
        lookup, which will filter any items that don't belong in this
        entry point, so this isn't required, but if you can't implement this
        there's a chance that every item returned by
        ExternalSearch.search() will be filtered out, giving the
        impression that there are no search results when there are.
        """
        return kwargs


class MediumEntryPoint(EntryPoint):
    """A entry point that creates a view on one specific medium.

    The medium is expected to be the entry point's INTERNAL_NAME.
    """

    @classmethod
    def apply(cls, qu):
        """Modify a query against the mv_works_for_lanes materialized view
        to match only items with the right medium.
        """
        from model import MaterializedWorkWithGenre as mv
        return qu.filter(mv.medium==cls.INTERNAL_NAME)

    @classmethod
    def modified_search_arguments(cls, **kwargs):
        """Modify a set of arguments to ExternalSearch.query_works to find
        only items with the given medium.
        """
        kwargs['media'] = [cls.INTERNAL_NAME]
        return kwargs


class EbooksEntryPoint(MediumEntryPoint):
    INTERNAL_NAME = "Book"
EntryPoint.register(EbooksEntryPoint, "Books", default_enabled=True)

class AudiobooksEntryPoint(MediumEntryPoint):
    INTERNAL_NAME = "Audio"
EntryPoint.register(AudiobooksEntryPoint, "Audiobooks")
