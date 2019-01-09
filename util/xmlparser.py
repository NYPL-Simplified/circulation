import re
import sys
from nose.tools import set_trace
from lxml import etree
from StringIO import StringIO

class XMLParser(object):

    """Helper functions to process XML data."""

    NAMESPACES = {}

    # Some Unicode characters are illegal within an XML document
    # _illegal_xml_chars_RE will match them so we can remove them.
    #
    # Source:
    # https://stackoverflow.com/questions/1707890/fast-way-to-filter-illegal-xml-unicode-chars-in-python
    _illegal_unichrs = [(0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F),
                       (0x7F, 0x84), (0x86, 0x9F),
                       (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF)]
    if sys.maxunicode >= 0x10000:  # not narrow build
        _illegal_unichrs.extend([(0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF),
                                 (0x3FFFE, 0x3FFFF), (0x4FFFE, 0x4FFFF),
                                 (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF),
                                 (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF),
                                 (0x9FFFE, 0x9FFFF), (0xAFFFE, 0xAFFFF),
                                 (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF),
                                 (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF),
                                 (0xFFFFE, 0xFFFFF), (0x10FFFE, 0x10FFFF)])

    _illegal_entities = []
    _illegal_ranges = []
    for (low, high) in _illegal_unichrs:
        _illegal_ranges.append("%s-%s" % (unichr(low), unichr(high)))
        for illegal in range(low, high+1):
            _illegal_entities.append("%02x" % illegal)

    _illegal_xml_chars_RE = re.compile(u'[%s]' % u''.join(_illegal_ranges))
    _illegal_xml_entities_RE = re.compile(
        "&#x(%s);" % "|".join(_illegal_entities), re.I
    )

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
            return unicode(tag.text)

    def text_of_subtag(self, tag, name, namespaces=None):
        return unicode(tag.xpath(name, namespaces=namespaces)[0].text)

    def int_of_subtag(self, tag, name, namespaces=None):
        return int(self.text_of_subtag(tag, name, namespaces=namespaces))

    def int_of_optional_subtag(self, tag, name, namespaces=None):
        v = self.text_of_optional_subtag(tag, name, namespaces=namespaces)
        if not v:
            return v
        return int(v)

    def scrub_xml(self, xml):
        """Remove invalid characters from XML.

        We remove the characters rather than replacing them with
        REPLACEMENT_CHARACTER, because they shouldn't have been in the
        XML file in the first place.
        """
        scrubbed = self._illegal_xml_chars_RE.sub("", xml)
        return self._illegal_xml_entities_RE.sub("", scrubbed)

    def process_all(self, xml, xpath, namespaces=None, handler=None, parser=None):
        if not parser:
            parser = etree.XMLParser()
        if not handler:
            handler = self.process_one
        if isinstance(xml, basestring):
            root = None
            exception = None
            for transform in (None, self.scrub_xml):
                if transform is None:
                    transformed = xml
                else:
                    transformed = transform(xml)
                try:
                    root = etree.parse(StringIO(transformed), parser)
                    break
                except etree.XMLSyntaxError, e:
                    exception = e
                    continue
            if root is None:
                # Re-raise the last exception raised. This should
                # never happen unless there is a problem with
                # scrub_xml.
                raise exception
        else:
            root = xml
        for i in root.xpath(xpath, namespaces=namespaces):
            data = handler(i, namespaces)
            if data is not None:
                yield data

    def process_one(self, tag, namespaces):
        return None
