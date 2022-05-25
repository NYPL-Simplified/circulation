import logging
from abc import ABCMeta, abstractmethod

import six

from core.util.webpub_manifest_parser.core.errors import BaseSyntaxError
from core.util.webpub_manifest_parser.core.parsers import ArrayParser, TypeParser, find_parser
from core.util.webpub_manifest_parser.core.properties import BaseArrayProperty, PropertiesGrouping
from core.util.webpub_manifest_parser.utils import encode, first_or_default, is_string


class MissingPropertyError(BaseSyntaxError):
    """Exception raised in the case of a missing required property."""

    def __init__(self, cls, object_property, message=None, inner_exception=None):
        """Initialize a new instance of MissingPropertyError class.

        :param cls: Object's class where the missing property is defined
        :type cls: Type

        :param object_property: Missing property
        :type object_property: python_rwpm_parser.metadata.ObjectProperty

        :param message: (Optional) Message
        :type message: Optional[str]

        :param inner_exception: (Optional) inner exception
        :type inner_exception: Optional[Exception]
        """
        if message is None:
            message = "{0}'s required property {1} is missing".format(
                cls, object_property.key
            )

        super(MissingPropertyError, self).__init__(
            message,
            inner_exception,
        )

        self._cls = cls
        self._object_property = object_property

    @property
    def cls(self):
        """Return the object's class where the missing property is defined.

        :return: Object's class where the missing property is defined
        :rtype: Type
        """
        return self._cls

    @property
    def object_property(self):
        """Return the missing property.

        :return: Missing property
        :rtype: python_rwpm_parser.metadata.ObjectProperty
        """
        return self._object_property


@six.add_metaclass(ABCMeta)
class SyntaxAnalyzer(object):
    """Base class for syntax analyzers checking the base grammar rules of RWPM and parsing raw JSON into AST."""

    CONTEXT = "@context"
    METADATA = "metadata"
    METADATA_IDENTIFIER = "identifier"
    LINKS = "links"

    def __init__(self):
        """Initialize a new instance of SyntaxParser class."""
        self._logger = logging.getLogger(__name__)

    @abstractmethod
    def _create_manifest(self):
        """Create a new manifest. The method should be overridden in child classes.

        :return: Manifest-like object
        :rtype: Manifestlike
        """
        raise NotImplementedError()

    def _get_property_value(self, json_content, object_property):
        """Extract the property's value from JSON object.

        :param json_content: JSON object
        :type json_content: Dict

        :param object_property: Object's property
        :type object_property: properties.Property

        :return: Property's value
        :rtype: Any
        """
        self._logger.debug(u"Started extracting {0} property".format(object_property))

        if isinstance(json_content, dict):
            property_value = json_content.get(object_property.key, None)
        else:
            property_value = json_content

        self._logger.debug(
            u"Finished extracting {0} property: {1}".format(
                object_property, encode(property_value)
            )
        )

        return property_value

    def _parse_nested_object(self, property_value, object_property):
        """Parse nested object(s) (if any) and return the result of parsing.

        :param property_value: Raw property's value (probably) containing nested object(s)
        :type property_value: Any

        :param object_property: Object's property
        :type object_property: properties.Property

        :return: Nested object's value
        :rtype: Any
        """
        if property_value is None:
            return property_value

        self._logger.debug(
            u"Started looking for nested property {0}".format(object_property)
        )

        type_parsers_result = find_parser(object_property.parser, TypeParser)

        self._logger.debug(
            u"Found the following type parsers: {0}".format(type_parsers_result)
        )

        found = False

        for parent_parser, type_parser in type_parsers_result:
            if isinstance(parent_parser, ArrayParser) and isinstance(
                property_value, list
            ):
                processed_items = []

                for item in property_value:
                    processed_item = self._parse_object(item, type_parser.type)
                    processed_items.append(processed_item)

                found = True
                property_value = processed_items
                break
        else:
            for parent_parser, type_parser in type_parsers_result:
                if not isinstance(parent_parser, ArrayParser):
                    found = True
                    property_value = self._parse_object(
                        property_value, type_parser.type
                    )
                    break

        if found:
            self._logger.debug(
                u"Finished parsing nested property {0}: {1}".format(
                    object_property, encode(property_value)
                )
            )
        else:
            self._logger.debug(u"Property {0} is not nested".format(object_property))

        return property_value

    @staticmethod
    def _format_property_value(property_value, object_property):
        """Format the property's value according to its format.

        For example, values of list properties have to be a collection of the type specified in the property.
        """
        if isinstance(object_property, BaseArrayProperty) and not isinstance(
            property_value, object_property.list_type
        ):
            if property_value is not None:
                property_value = object_property.list_type(
                    property_value
                    if isinstance(property_value, list)
                    else [property_value]
                )
            else:
                property_value = object_property.list_type()

        return property_value

    def _set_scalar_value(self, json_content, ast_object):
        """Parse a scalar string value and initialize an object's property with it.

        :param json_content: Scalar string value containing a required object's property
        :type json_content: str

        :param ast_object: AST object
        :type ast_object: Node
        """
        required_object_properties = PropertiesGrouping.get_required_class_properties(
            ast_object.__class__
        )

        if len(required_object_properties) != 1:
            raise BaseSyntaxError(
                u"There are {0} required properties in {1} but only a single value ({2} was provided".format(
                    len(required_object_properties), encode(ast_object), json_content
                )
            )

        required_object_property_name, required_object_property = first_or_default(
            required_object_properties
        )

        self._set_property_value(
            ast_object,
            required_object_property_name,
            required_object_property,
            json_content,
        )

        # We need to initialize other properties with default values
        self._set_non_scalar_value(None, ast_object, {required_object_property_name})

    def _set_non_scalar_value(
        self, json_content, ast_object, excluded_property_names=None
    ):
        """Parse a dictionary and initialize object's properties with its values.

        :param json_content: Dictionary containing property values
        :type json_content: Dict

        :param ast_object: AST object
        :type ast_object: Node

        :param excluded_property_names: Set of property names to exclude from consideration
        :type excluded_property_names: Set
        """
        ast_object_properties = PropertiesGrouping.get_class_properties(
            ast_object.__class__
        )

        for object_property_name, object_property in ast_object_properties:
            if (
                excluded_property_names
                and object_property_name in excluded_property_names
            ):
                continue

            property_value = self._get_property_value(json_content, object_property)
            property_value = self._parse_nested_object(property_value, object_property)

            self._set_property_value(
                ast_object, object_property_name, object_property, property_value
            )

    def _set_property_value(
        self, ast_object, object_property_name, object_property, property_value
    ):
        """Set the value of the specified property.

        :param ast_object: AST object
        :type ast_object: Node

        :param object_property_name: Name of the property
        :type object_property_name: str

        :param object_property: Object's property
        :type object_property: Property

        :param property_value: Value to be set
        :type property_value: Any
        """
        self._logger.debug(
            u"Property '{0}' has the following value: {1}".format(
                object_property.key, encode(property_value)
            )
        )

        if property_value is None and object_property.default_value is not None:
            property_value = object_property.default_value

        if object_property.required and property_value is None:
            raise MissingPropertyError(ast_object.__class__, object_property)

        if property_value is not None:
            property_value = object_property.parser.parse(property_value)

        property_value = self._format_property_value(property_value, object_property)

        setattr(ast_object, object_property_name, property_value)

    def _parse_object(self, json_content, cls):
        """Parse RWPM's object JSON into a corresponding AST object.

        :param json_content: Dictionary containing object's JSON
        :type json_content: Dict

        :param cls: Object's class
        :type cls: Type

        :return: Node object
        :rtype: Node
        """
        self._logger.debug(u"Started parsing {0} object".format(cls))

        extended_cls = cls.get_extension()
        ast_object = extended_cls()

        if is_string(json_content):
            self._set_scalar_value(json_content, ast_object)
        elif isinstance(json_content, (list, dict)):
            self._set_non_scalar_value(json_content, ast_object)

        self._logger.debug(u"Finished parsing {0} object: {1}".format(cls, ast_object))

        return ast_object

    def analyze(self, manifest_json):
        """Parse JSON file into RWPM AST.

        :param manifest_json: RWPM-compatible manifest
        :type manifest_json: Dict

        :return: RWPM AST
        :rtype: ManifestLike
        """
        self._logger.debug(u"Started analyzing {0}".format(manifest_json))

        manifest = self._create_manifest()
        manifest = self._parse_object(manifest_json, manifest.__class__)

        self._logger.debug(
            u"Finished analyzing {0}: {1}".format(manifest_json, manifest)
        )

        return manifest
