from nose.tools import set_trace

class Tab(object):

    """A Tab is a top-level entry point into a library's Lane structure
    that may apply additional filters to the Lane structure.

    The "Books" and "Audiobooks" tabs (defined in the EbooksTab and
    AudiobooksTab classes) are different views on a library's Lane
    structure; each applies an additional filter against
    Edition.medium.

    Each individual Tab should be represented as a subclass of Tab,
    and should be registered with the overall Tab class by calling
    Tab.register.

    The list of tabs shows up as a facet group in a library's
    top-level grouped feed, and in search results.
    """

    # The name of the per-library setting that controls which tabs are
    # enabled.
    ENABLED_SETTING = "enabled_tabs"

    TABS = []
    DEFAULT_ENABLED = []
    DISPLAY_TITLES = {}
    INTERNAL_NAMES = []

    @classmethod
    def register(cls, tab_class, display_title, default_enabled=False):
        """Register the given subclass with the master registry
        kept in the Tab class.

        :param tab_class: A subclass of Tab.
        :param display_title: The title to use when displaying this tab
            to patrons.
        :param default_enabled: New libraries should have this tab
            enabled by default.
        """
        value = getattr(tab_class, 'INTERNAL_NAME', None)
        if not value:
            raise ValueError(
                "Tab class %s must define INTERNAL_NAME." % tab_class.__name__
            )
        if value in cls.INTERNAL_NAMES:
            raise ValueError(
                "Duplicate tab internal name: %s" % value
            )
        cls.INTERNAL_NAMES.append(value)
        cls.TABS.append(tab_class)
        if default_enabled:
            cls.DEFAULT_ENABLED.append(tab_class)
        cls.DISPLAY_TITLES[tab_class] = display_title

    @classmethod
    def unregister(cls, tab_class):
        """Undo a subclass's registration.

        Only used in tests.
        """
        cls.TABS.remove(tab_class)
        cls.INTERNAL_NAMES.remove(tab_class.INTERNAL_NAME)
        del cls.DISPLAY_TITLES[tab_class]

    @classmethod
    def modified_materialized_view_query(cls, qu):
        """Modify a query against the mv_works_for_lanes materialized view
        so it matches only items that belong in this tab.
        """
        raise NotImplementedError()

    @classmethod
    def modified_search_arguments(cls, **kwargs):
        """If possible, modify the arguments to ExternalSearch.query_works()
        so that only items belonging to this tab are found.

        Any items returned will be run through the materialized view
        lookup, which will filter any items that don't belong in this
        tab, so this isn't required, but if you can't implement this
        there's a chance that every item returned by
        ExternalSearch.search() will be filtered out, giving the
        impression that there are no search results when there are.
        """
        return kwargs


class MediumTab(Tab):
    """A tab that creates a view on one specific medium.

    The medium is expected to be the tab's INTERNAL_NAME.
    """

    @classmethod
    def apply(cls, qu):
        """Modify a query against the mv_works_for_lanes materialized view
        to match only items with the right medium.
        """
        from core.model import MaterializedWorkForLane as mv
        return qu.filter(mv.edition==cls.INTERNAL_NAME)

    @classmethod
    def modified_search_arguments(cls, **kwargs):
        """Modify a set of arguments to ExternalSearch.query_works to find
        only items with the given medium.
        """
        kwargs['media'] = [self.INTERNAL_NAME]
        return kwargs


class EbooksTab(MediumTab):
    INTERNAL_NAME = "Book"
Tab.register(EbooksTab, "Books", default_enabled=True)

class AudiobooksTab(MediumTab):
    INTERNAL_NAME = "Audio"
Tab.register(AudiobooksTab, "Audiobooks")
