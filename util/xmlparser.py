import re
import sys

from lxml import etree
from io import BytesIO

class XMLParser(object):

    """Helper functions to process XML data."""

    NAMESPACES = {}

    @classmethod
    def _xpath(cls, tag, expression, namespaces=None):
        if not namespaces:
            namespaces = cls.NAMESPACES
        """Wrapper to do a namespaced XPath expression."""
        return tag.xpath(expression, namespaces=namespaces)

    @classmethod
    def _xpath1(cls, tag, expression, namespaces=None):
        """Wrapper to do a namespaced XPath expression."""
        values = cls._xpath(tag, expression, namespaces=namespaces)
        if not values:
            return None
        return values[0]

    def _cls(self, tag_name, class_name):
        """Return an XPath expression that will find a tag with the given CSS class."""
        return 'descendant-or-self::node()/%s[contains(concat(" ", normalize-space(@class), " "), " %s ")]' % (tag_name, class_name)

    def text_of_optional_subtag(self, tag, name, namespaces=None):
        tag = self._xpath1(tag, name, namespaces=namespaces)
        if tag is None or tag.text is None:
            return None
        else:
            return str(tag.text)

    def text_of_subtag(self, tag, name, namespaces=None):
        return str(tag.xpath(name, namespaces=namespaces)[0].text)

    def int_of_subtag(self, tag, name, namespaces=None):
        return int(self.text_of_subtag(tag, name, namespaces=namespaces))

    def int_of_optional_subtag(self, tag, name, namespaces=None):
        v = self.text_of_optional_subtag(tag, name, namespaces=namespaces)
        if not v:
            return v
        return int(v)

    def process_all(self, xml, xpath, namespaces=None, handler=None, parser=None):
        if not parser:
            parser = etree.XMLParser(recover=True)
        if not handler:
            handler = self.process_one
        if isinstance(xml, str):
            xml = xml.encode("utf8")

        if isinstance(xml, bytes):
            # XMLParser can handle most characters and entities that are
            # invalid in XML but it will stop processing a document if it
            # encounters the null character. Remove that character
            # immediately and XMLParser will handle the rest.
            xml = xml.replace(b"\x00", b"")
            root = etree.parse(BytesIO(xml), parser)
        else:
            root = xml

        for i in root.xpath(xpath, namespaces=namespaces):
            data = handler(i, namespaces)
            if data is not None:
                yield data

    def process_one(self, tag, namespaces):
        return None
