import re
from nose.tools import (
    eq_,
    set_trace
)
from lxml import etree
from util.opds_writer import (
    OPDSMessage
)


class TestOPDSMessage(object):

    def test_equality(self):
        
        a = OPDSMessage("urn", 200, "message")
        eq_(a,a)
        assert a != None
        assert a != "message"

        eq_(a, OPDSMessage("urn", 200, "message"))
        assert a != OPDSMessage("urn2", 200, "message")
        assert a != OPDSMessage("urn", 201, "message")
        assert a != OPDSMessage("urn", 200, "message2")

    def test_tag(self):
        """Verify that an OPDSMessage becomes a reasonable XML tag."""
        a = OPDSMessage("urn", 200, "message")
        text = etree.tostring(a.tag)
        eq_(text, str(a))
        
        # Verify that we start with a simplified:message tag.
        assert text.startswith('<simplified:message')

        # Verify that the namespaces we need are in place.
        assert 'xmlns:schema="http://schema.org/"' in text
        assert 'xmlns:simplified="http://librarysimplified.org/terms/"' in text

        # Verify that the tags we want are in place.
        assert '<simplified:identifier>urn</simplified:identifier>' in text
        
        assert '<simplified:status_code>200</simplified:status_code>' in text
        assert '<schema:description>message</schema:description>' in text
        assert text.endswith('</simplified:message>')
