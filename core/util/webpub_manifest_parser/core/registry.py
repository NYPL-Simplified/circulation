import collections


class RegistryItem(object):
    """Single metadata registry item (collection role, media type, etc.)."""

    def __init__(self, key):
        """Initialize a new instance of RegistryItem class.

        :param key: Unique identifier of this registry item
        :type key: str
        """
        self._key = key

    @property
    def key(self):
        """Return a unique identifier of this registry item.

        :return: Unique identifier of this registry item
        :rtype: str
        """
        return self._key


class Registry(collections.abc.MutableMapping):
    """Collection of registry items with a particular type (collection roles, media types, etc.)."""

    def __init__(self, items=None):
        """Initialize a new instance of Registry class.

        :param items: (Optional) collection of registry items. Note that all items have to be RegistryItem descendants
        :type items: List[RegistryItems]
        """
        self._items = {}

        if items:
            self._add_items(items)

    def __setitem__(self, key, value):
        """Add a new item to the registry.

        :param key: Unique identifier of the item
        :type key: str

        :param value: Registry item
        :type value: RegistryItem
        """
        if not isinstance(value, RegistryItem):
            raise ValueError("Registry item must have RegistryItem type")

        self._items[key] = value

    def __delitem__(self, key):
        """Remove the item from the registry.

        :param key: Unique identifier of the item
        :type key: str
        """
        del self._items[key]

    def __getitem__(self, key):
        """Return an item from the registry by its key or raises a KeyError if it doesn't exist.

        :param key: Unique identifier of the item
        :type key: str

        :return: Registry item
        :rtype: RegistryItem
        """
        return self._items[key]

    def __iter__(self):
        """Return an iterator of all the registry items.

        :return: Iterator of the registry items
        :rtype: Iterator[RegistryItem]
        """
        return iter(self._items.values())

    def __len__(self):
        """Return a number of items in the registry.

        :return: Number of items in the registry
        :rtype: int
        """
        return len(self._items)

    def _add_items(self, items):
        """Add new items to the registry. Note that all the items must be RegistryItem descendants.

        :param items: New registry items
        :type items: List[RegistryItem]
        """
        for item in items:
            if not isinstance(item, RegistryItem):
                raise ValueError("Registry item must have RegistryItem type")

            self._items[item.key] = item


class MediaType(RegistryItem):
    """Registry item representing a specific media type."""


class LinkRelation(RegistryItem):
    """Registry item representing a link relation."""


class CollectionRole(RegistryItem):
    """Registry item representing a collection role."""

    def __init__(self, key, compact, required, multi=False):
        """Initialize a new instance of CollectionRole class.

        :param key: Name of the collection
        :type key: str

        :param compact: Boolean value indicating whether the collection shall be compact
        :type compact: bool

        :param required: Boolean value indicating whether the collection is required
        :type required: bool

        :param multi: Boolean value indicating whether there can be multiple collections with this role
        :type multi: bool
        """
        super(CollectionRole, self).__init__(key)

        self._compact = compact
        self._required = required
        self._multi = multi

    @property
    def compact(self):
        """Return the boolean value indicating whether the collection shall be compact.

        :return: Boolean value indicating whether the collection shall be compact
        :rtype: bool
        """
        return self._compact

    @property
    def required(self):
        """Return the boolean value indicating whether the collection is required.

        :return: Boolean value indicating whether the collection is required
        :rtype: bool
        """
        return self._required

    @property
    def multi(self):
        """Return the boolean value indicating whether there can be multiple collections with this role.

        :return: Boolean value indicating whether there can be multiple collections with this role
        :rtype: bool
        """
        return self._multi
