from nose.tools import (
    eq_,
)
from util.web_publication_manifest import (
    JSONable,
    Manifest,
    TimelinePart,
    AudiobookManifest,
)

class TestJSONable(object):

    class Mock(JSONable):
        @property
        def as_dict(self):
            return dict(value=1)

    def test_as_dict(self):
        eq_(u'{"value": 1}', unicode(self.Mock()))

    def test_json_ready(self):
        m = JSONable.json_ready
        eq_(1, m(1))
        mock = self.Mock()
        eq_(dict(value=1), m(mock))
        eq_([dict(value=1), dict(value=1)], m([mock, mock]))

class TestManifest(object):

    def test_defaults(self):
        eq_("http://schema.org/Book", Manifest.DEFAULT_TYPE)
        eq_("http://readium.org/webpub/default.jsonld",
            Manifest.DEFAULT_CONTEXT)

        manifest = Manifest()
        eq_(Manifest.DEFAULT_CONTEXT, manifest.context)
        eq_(Manifest.DEFAULT_TYPE, manifest.type)
        eq_(
            {
                '@context' : manifest.context,
                'metadata' : {'@type': manifest.type}
            },
            manifest.as_dict
        )

    def test_add_link(self):
        manifest = Manifest()
        manifest.add_link("http://foo/", "self", extra="value")
        dict = manifest.as_dict
        eq_(
            [{'href': 'http://foo/', 'rel': 'self', 'extra': 'value'}], 
            dict['links']
        )

    def test_add_spine(self):
        manifest = Manifest()
        manifest.add_spine("http://foo/", "text/html", "Chapter 1",
                           extra="value")
        dict = manifest.as_dict
        eq_(
            [{'href': 'http://foo/', 'type': 'text/html', 'title': 'Chapter 1',
              'extra': 'value'}], 
            dict['spine']
        )

    def test_add_resource(self):
        manifest = Manifest()
        manifest.add_resource("http://foo/", "text/html", extra="value")
        dict = manifest.as_dict
        eq_(
            [{'href': 'http://foo/', 'type': 'text/html', 'extra': 'value'}], 
            dict['resources']
        )


class TestTimelinePart(object):

    expect = {
        'href': 'http://foo/pt1', 
        'title': 'Part 1',
        'children': [{'extra': 'value', 'href': 'http://foo/ch1', 
                      'title': 'Chapter 1'}]
    }

    def test_as_dict(self):
        chapter_1 = TimelinePart("http://foo/ch1", "Chapter 1", extra="value")
        part_1 = TimelinePart("http://foo/pt1", "Part 1", [chapter_1])
        eq_(self.expect, part_1.as_dict)

    def test_add_child(self):
        part_1 = TimelinePart("http://foo/pt1", "Part 1")
        part_1.add_child("http://foo/ch1", "Chapter 1", extra="value")
        eq_(self.expect, part_1.as_dict)


class TestAudiobookManifest(object):

    def test_defaults(self):
        eq_("http://bib.schema.org/Audiobook", AudiobookManifest.DEFAULT_TYPE)
        eq_("http://readium.org/webpub/default.jsonld",
            AudiobookManifest.DEFAULT_CONTEXT)

        manifest = AudiobookManifest()
        eq_(AudiobookManifest.DEFAULT_CONTEXT, manifest.context)
        eq_(AudiobookManifest.DEFAULT_TYPE, manifest.type)
        eq_(
            {
                '@context' : manifest.context,
                'metadata' : {'@type': manifest.type}
            },
            manifest.as_dict
        )

    def test_add_timeline(self):
        manifest = AudiobookManifest()
        part = manifest.add_timeline("http://foo/pt1", "Part 1", extra="value")
        # At this point you could add children to `part`.
        assert isinstance(part, TimelinePart)

        dict = manifest.as_dict
        eq_(
            [{'href': 'http://foo/pt1', 'title': 'Part 1', 'extra': 'value'}], 
            dict['timeline']
        )
