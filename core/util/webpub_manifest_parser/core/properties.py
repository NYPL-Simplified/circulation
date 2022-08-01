import inspect
from abc import ABCMeta, abstractmethod

import six

from core.util.webpub_manifest_parser.core.parsers import (
    AnyOfParser,
    ArrayParser,
    BooleanParser,
    DateParser,
    DateTimeParser,
    EnumParser,
    IntegerParser,
    LocalizableStringParser,
    StringParser,
    StringPatternParser,
    TypeParser,
    URIParser,
    URIReferenceParser,
    URITemplateParser,
    ValueParser,
)
from core.util.webpub_manifest_parser.utils import is_string


@six.add_metaclass(ABCMeta)
class HasProperties(object):
    """Interface representing class containing ObjectProperty meta-properties."""

    @abstractmethod
    def get_setting_value(self, setting_name, default_value=None):
        """Return the setting's value.

        :param setting_name: Name of the setting
        :type setting_name: string

        :param default_value: Default value
        :type default_value: Any

        :return: Setting's value
        :rtype: Any
        """
        raise NotImplementedError()

    @abstractmethod
    def set_setting_value(self, setting_name, setting_value):
        """Set the setting's value.

        :param setting_name: Name of the setting
        :type setting_name: string

        :param setting_value: New value of the setting
        :type setting_value: Any
        """
        raise NotImplementedError()


class Property(object):
    """Class representing object property, storing property's metadata and its value."""

    _counter = 0

    def __init__(self, key, required, parser, default_value=None):
        """Initialize a new instance of Property class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param parser: Parse used to validate and parse property's value
        :type parser: parsers.ValueParser

        :param default_value: Property's default value
        :type default_value: Any
        """
        if not is_string(key):
            raise ValueError("Key argument must be a string")
        if not isinstance(required, bool):
            raise ValueError("Required argument must be boolean")
        if not isinstance(parser, ValueParser):
            raise ValueError("Parser argument must be a ValueParser instance")

        self._key = key
        self._required = required
        self._parser = parser
        self._default_value = default_value

    def __get__(self, owner_instance, owner_type):
        """Return the property's value.

        :param owner_instance: Instance of the owner, class having instance of ObjectProperty as an attribute
        :type owner_instance: Optional[HasProperties]

        :param owner_type: Owner's class
        :type owner_type: Optional[Type]

        :return: ObjectProperty instance (when called via a static method) or
            the setting's value (when called via an instance method)
        :rtype: Union[ObjectProperty, Any]
        """
        # If owner_instance is empty, it means that this method was called via a static method
        # In this case we need to return the metadata instance itself
        if owner_instance is None:
            return self

        if not isinstance(owner_instance, HasProperties):
            raise ValueError("owner must be an instance of HasProperties type")

        return owner_instance.get_setting_value(self._key, self._default_value)

    def __set__(self, owner_instance, value):
        """Set the property's value.

        :param owner_instance: Instance of the owner, class having instance of ObjectProperty as an attribute
        :type owner_instance: Optional[HasProperties]

        :param value: New setting's value
        :type value: Any
        """
        if not isinstance(owner_instance, HasProperties):
            raise ValueError("owner must be an instance of HasProperties class")

        return owner_instance.set_setting_value(self._key, value)

    def __repr__(self):
        """Return a string representation of the object.

        :return: String representation
        :rtype: str
        """
        return (
            u"<Property(key={0}, required={1}, parser={2}, default_value={3})>".format(
                self.key, self.required, self.parser, self.default_value
            )
        )

    @property
    def key(self):
        """Return the property's key.

        :return: Property's key
        :rtype: str
        """
        return self._key

    @property
    def required(self):
        """Return a boolean value indicating whether this property is required or not.

        :return: Boolean value indicating whether this property is required or not.
        :rtype: bool
        """
        return self._required

    @property
    def parser(self):
        """Return the parser used to validate and parse this property's value.

        :return: Parser used to validate and parse this property's value.
        :rtype: parsers.Parser
        """
        return self._parser

    @property
    def default_value(self):
        """Return the property's default value.

        :return: Property's default value.
        :rtype: bool
        """
        return self._default_value


class PropertiesGrouping(HasProperties):
    """Group of properties."""

    def __init__(self):
        """Initialize a new instance of PropertiesGrouping class."""
        self._values = {}

    def get_setting_value(self, setting_name, default_value=None):
        """Return the setting's value.

        :param setting_name: Setting's name
        :type setting_name: str

        :param default_value: Setting's default value
        :type default_value: Any
        """
        return self._values.get(setting_name, default_value)

    def set_setting_value(self, setting_name, setting_value):
        """Set the setting's value.

        :param setting_name: Setting's name
        :type setting_name: str

        :param setting_value: New setting's value
        :type setting_value: Any
        """
        self._values[setting_name] = setting_value

    @staticmethod
    def get_class_properties(klass):
        """Return a list of 2-tuples containing information ConfigurationMetadata properties in the specified class.

        :param klass: Class
        :type klass: type

        :return: List of 2-tuples containing information ConfigurationMetadata properties in the specified class
        :rtype: List[Tuple[string, ConfigurationMetadata]]
        """
        members = inspect.getmembers(klass, lambda member: isinstance(member, Property))

        return members

    @staticmethod
    def get_required_class_properties(klass):
        """Return a list of 2-tuples containing information about required ConfigurationMetadata properties.

        :param klass: Class
        :type klass: type

        :return: List of 2-tuples containing information ConfigurationMetadata properties in the specified class
        :rtype: List[Tuple[string, ConfigurationMetadata]]
        """
        class_properties = PropertiesGrouping.get_class_properties(klass)
        required_class_properties = [
            (class_property_name, class_property)
            for (class_property_name, class_property) in class_properties
            if class_property.required
        ]

        return required_class_properties


@six.add_metaclass(ABCMeta)
class ParsableProperty(Property):
    """Base class for all property classes having predefined parsers."""

    PARSER = object()

    def __init__(self, key, required, default_value=None):
        """Initialize a new instance of ParsableProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param default_value: Property's default value
        :type default_value: Any
        """
        super(ParsableProperty, self).__init__(
            key, required, self.PARSER, default_value
        )


class IntegerProperty(Property):
    """Property allowing only integer values."""

    def __init__(  # pylint: disable=R0913
        self,
        key,
        required,
        minimum=None,
        exclusive_minimum=None,
        maximum=None,
        exclusive_maximum=None,
        default_value=None,
    ):
        """Initialize a new instance of IntegerProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param minimum: Minimum value
        :type minimum: Numeric

        :param exclusive_minimum: Exclusive minimum value
        :type exclusive_minimum: Numeric

        :param maximum: Maximum value
        :type maximum: Numeric

        :param exclusive_maximum: Exclusive maximum value
        :type exclusive_maximum: Numeric

        :param default_value: Property's default value
        :type default_value: Any
        """
        super(IntegerProperty, self).__init__(
            key,
            required,
            IntegerParser(minimum, exclusive_minimum, maximum, exclusive_maximum),
            default_value,
        )


class NumberProperty(Property):
    """Property allowing only float values."""

    def __init__(  # pylint: disable=R0913
        self,
        key,
        required,
        minimum=None,
        exclusive_minimum=None,
        maximum=None,
        exclusive_maximum=None,
        default_value=None,
    ):
        """Initialize a new instance of NumberProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param minimum: Minimum value
        :type minimum: Numeric

        :param exclusive_minimum: Exclusive minimum value
        :type exclusive_minimum: Numeric

        :param maximum: Maximum value
        :type maximum: Numeric

        :param exclusive_maximum: Exclusive maximum value
        :type exclusive_maximum: Numeric

        :param default_value: Property's default value
        :type default_value: Any
        """
        super(NumberProperty, self).__init__(
            key,
            required,
            IntegerParser(minimum, exclusive_minimum, maximum, exclusive_maximum),
            default_value,
        )


class BooleanProperty(ParsableProperty):
    """Property allowing only boolean values."""

    PARSER = BooleanParser()


class StringProperty(ParsableProperty):
    """Property allowing only string values."""

    PARSER = StringParser()


class EnumProperty(Property):
    """Property allowing only specific string values."""

    def __init__(self, key, required, items, default_value=None):
        """Initialize a new instance of EnumProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param items: Enumeration values
        :type items: List[str]

        :param default_value: Property's default value
        :type default_value: Any
        """
        if not isinstance(items, list):
            raise ValueError("Items argument must be a list")

        super(EnumProperty, self).__init__(
            key, required, EnumParser(items), default_value
        )


class URIProperty(ParsableProperty):
    """Property allowing only URI values."""

    PARSER = URIParser()


class URITemplateProperty(ParsableProperty):
    """Property allowing only URI templates."""

    PARSER = URITemplateParser()


class URIReferenceProperty(ParsableProperty):
    """Property allowing only URI-reference values."""

    PARSER = URIReferenceParser()


class DateProperty(ParsableProperty):
    """Property allowing only date values."""

    PARSER = DateParser()


class DateTimeProperty(ParsableProperty):
    """Property allowing only date & time values."""

    PARSER = DateTimeParser()


class DateOrTimeProperty(ParsableProperty):
    """Property allowing date or date & time values."""

    PARSER = AnyOfParser([DateParser(), DateTimeParser()])


class BaseArrayProperty(Property):
    """Property containing a list of items."""

    def __init__(
        self, key, required, parser, list_type=list, default_value=None
    ):  # pylint: disable=R0913
        """Initialize a new instance of ListProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param parser: Parse used to validate and parse property's value
        :type parser: parsers.ValueParser

        :param list_type: Class of a collection used to hold the items
        :type list_type: Type

        :param default_value: Property's default value
        :type default_value: Any
        """
        if not issubclass(list_type, list):
            raise ValueError("List type argument must be a subclass of list class")

        super(BaseArrayProperty, self).__init__(key, required, parser, default_value)

        self._list_type = list_type

    def __set__(self, owner_instance, value):
        """Set the property's value.

        :param owner_instance: Instance of the owner, class having instance of ObjectProperty as an attribute
        :type owner_instance: Optional[HasProperties]

        :param value: New setting's value
        :type value: Any
        """
        if value is not None and not isinstance(value, self._list_type):
            raise ValueError(
                "Value must be a subclass of {0} class".format(self._list_type)
            )

        super(BaseArrayProperty, self).__set__(owner_instance, value)

    @property
    def list_type(self):
        """Return the class of a collection used to hold the items.

        :return: Class of a collection used to hold the items
        :rtype: Type
        """
        return self._list_type


class ArrayProperty(BaseArrayProperty):
    """Property containing an array of items."""

    def __init__(  # pylint: disable=R0913
        self,
        key,
        required,
        item_parser,
        unique_items=False,
        list_type=list,
        default_value=None,
    ):
        """Initialize a new instance of ArrayProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param item_parser: Parser for array items
        :type item_parser: ValueParser

        :param unique_items: Boolean value indicating whether the array should contain unique items
        :type unique_items: bool

        :param list_type: Class of a collection used to hold the items
        :type list_type: Type

        :param default_value: Property's default value
        :type default_value: Any
        """
        if not isinstance(item_parser, ValueParser):
            raise ValueError(
                "Item parser argument must be an instance of ValueParser class"
            )
        if not isinstance(unique_items, bool):
            raise ValueError("Unique items argument must be boolean")

        super(ArrayProperty, self).__init__(
            key,
            required,
            ArrayParser(item_parser, unique_items),
            list_type,
            default_value,
        )


class ArrayOfStringsProperty(BaseArrayProperty):
    """Property allowing either a string or array of strings as its values."""

    def __init__(  # pylint: disable=R0913
        self, key, required, unique_items=False, list_type=list, default_value=None
    ):
        """Initialize a new instance of ArrayOfStringsProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param unique_items: Boolean value indicating whether the array should contain unique items
        :type unique_items: bool

        :param list_type: Class of a collection used to hold the items
        :type list_type: Type

        :param default_value: Property's default value
        :type default_value: Any
        """
        if not isinstance(unique_items, bool):
            raise ValueError("Unique items argument must be boolean")

        super(ArrayOfStringsProperty, self).__init__(
            key,
            required,
            AnyOfParser([ArrayParser(StringParser(), unique_items), StringParser()]),
            list_type,
            default_value,
        )


class ListOfLanguagesProperty(BaseArrayProperty):
    """Property allowing localizable strings.

    For example:
        - "en"
        - [
            "eng",
            "fre"
          ]
    """

    def __init__(self, key, required):  # pylint: disable=R0913
        """Initialize a new instance of ListOfLanguagesProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool
        """
        super(ListOfLanguagesProperty, self).__init__(
            key,
            required,
            AnyOfParser(
                [
                    StringPatternParser(LocalizableStringParser.LANGUAGE_PATTERN),
                    ArrayParser(
                        StringPatternParser(LocalizableStringParser.LANGUAGE_PATTERN)
                    ),
                ]
            ),
            list,
            [],
        )


class LocalizableStringProperty(ParsableProperty):
    """Property allowing either only string/localizable string values.

    For example:
    - "plain string"
    - {
        "eng": "Hello",
        "esp": "Hola"
      }
    """

    PARSER = AnyOfParser([LocalizableStringParser(), StringParser()])


class TypeProperty(Property):
    """Property allowing only specific values of a specific Type."""

    def __init__(self, key, required, nested_type, default_value=None):
        """Initialize a new instance of TypeProperty class.

        :param key: Property's key
        :type key: str

        :param required: Boolean value indicating whether the property is required or not
        :type required: bool

        :param nested_type: Value's type
        :type nested_type: Type

        :param default_value: Property's default value
        :type default_value: Any
        """
        super(TypeProperty, self).__init__(
            key, required, TypeParser(nested_type), default_value
        )
