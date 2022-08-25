import six


def first_or_default(collection, default=None):
    """Return first element of the specified collection or the default value if the collection is empty.

    :param collection: Collection
    :type collection: Iterable

    :param default: Default value
    :type default: Any
    """
    element = next(iter(collection), None)

    if element is None:
        element = default

    return element


def all_unique(_list):
    """Return a boolean value indicating whether the list contains only unique items.

    :return: Boolean value indicating whether the list contains only unique items
    :rtype: bool
    """
    if not isinstance(_list, list):
        raise ValueError("List expected")

    seen = set()

    return not any(element in seen or seen.add(element) for element in _list)


def is_string(value):
    """Return a boolean value indicating whether the value is a string or not.

    :return: Boolean value indicating whether the value is a string or not
    :rtype: bool
    """
    return isinstance(value, six.string_types)


def encode(value):
    """Encode the string value using UTF-8 encoding.

    :param value: Value to be encoded
    :type value: Any

    :return: Encoded string value or the initial value if it has a non-string type
    :rtype: Any
    """
    if isinstance(value, list):
        return u"list({0}, ...)".format(first_or_default(value))

    return value if is_string(value) else value


def cast(value, klass):
    """Casts the value to the specified class.

    :param value: Value to be casted
    :type value: object

    :param klass: Target class
    :type klass: Type

    :return: Value casted to the specified class
    :rtype: object
    """
    if not isinstance(value, klass):
        raise ValueError(
            "Value must be a transitive instance of {0} class".format(klass)
        )

    value.__class__ = klass

    return value
