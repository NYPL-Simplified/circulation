import re
from lxml import etree
from ...util.opds_writer import (
    AtomFeed,
    OPDSMessage
)


class TestOPDSMessage(object):

    def test_equality(self):

        a = OPDSMessage("urn", 200, "message")
        assert a ==a
        assert a != None
        assert a != "message"

        assert a == OPDSMessage("urn", 200, "message")
        assert a != OPDSMessage("urn2", 200, "message")
        assert a != OPDSMessage("urn", 201, "message")
        assert a != OPDSMessage("urn", 200, "message2")

    def test_tag(self):
        """Verify that an OPDSMessage becomes a reasonable XML tag."""
        a = OPDSMessage("urn", 200, "message")
        text = etree.tounicode(a.tag)
        assert text == str(a)

        # Verify that we start with a simplified:message tag.
        assert text.startswith('<simplified:message')

        # Verify that the namespaces we need are in place.
        assert 'xmlns:schema="http://schema.org/"' in text
        assert 'xmlns:simplified="http://librarysimplified.org/terms/"' in text

        # Verify that the tags we want are in place.
        assert '<id>urn</id>' in text
        assert '<simplified:status_code>200</simplified:status_code>' in text
        assert '<schema:description>message</schema:description>' in text
        assert text.endswith('</simplified:message>')


class TestAtomFeed(object):

    def test_add_link_to_entry(self):
        kwargs = dict(title=1, href="url", extra="extra info")
        entry = AtomFeed.E.entry()
        link_child = AtomFeed.E.link_child()
        AtomFeed.add_link_to_entry(entry, [link_child], **kwargs)
        assert (
            etree.tostring(
                etree.fromstring(u'<link extra="extra info" href="url" title="1"><link_child/></link>'),
                method='c14n2'
            )
            in etree.tostring(entry, method='c14n2')
        )

    def test_contributor(self):
        kwargs = { '{%s}role' % AtomFeed.OPF_NS : 'ctb' }
        tag = etree.tounicode(AtomFeed.author(**kwargs))
        assert tag.startswith('<author')
        assert 'xmlns:opf="http://www.idpf.org/2007/opf"' in tag
        assert tag.endswith('opf:role="ctb"/>')
