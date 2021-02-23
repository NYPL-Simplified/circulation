from ...util.web_publication_manifest import (
    JSONable,
    Manifest,
    AudiobookManifest,
)

from .. import DatabaseTest

class TestJSONable(object):

    class Mock(JSONable):
        @property
        def as_dict(self):
            return dict(value=1)

    def test_as_dict(self):
        assert u'{"value": 1}' == unicode(self.Mock())

    def test_json_ready(self):
        m = JSONable.json_ready
        assert 1 == m(1)
        mock = self.Mock()
        assert dict(value=1) == m(mock)
        assert [dict(value=1), dict(value=1)] == m([mock, mock])

class TestManifest(object):

    def test_defaults(self):
        assert "http://schema.org/Book" == Manifest.DEFAULT_TYPE
        assert ("http://readium.org/webpub/default.jsonld" ==
            Manifest.DEFAULT_CONTEXT)

        manifest = Manifest()
        assert Manifest.DEFAULT_CONTEXT == manifest.context
        assert Manifest.DEFAULT_TYPE == manifest.type
        assert (
            {
                '@context' : manifest.context,
                'metadata' : {'@type': manifest.type}
            } ==
            manifest.as_dict)

    def test_add_link(self):
        manifest = Manifest()
        manifest.add_link("http://foo/", "self", extra="value")
        dict = manifest.as_dict
        assert (
            [{'href': 'http://foo/', 'rel': 'self', 'extra': 'value'}] ==
            dict['links'])

    def test_add_reading_order(self):
        manifest = Manifest()
        manifest.add_reading_order("http://foo/", "text/html", "Chapter 1",
                           extra="value")
        dict = manifest.as_dict
        assert (
            [{'href': 'http://foo/', 'type': 'text/html', 'title': 'Chapter 1',
              'extra': 'value'}] ==
            dict['readingOrder'])

    def test_add_resource(self):
        manifest = Manifest()
        manifest.add_resource("http://foo/", "text/html", extra="value")
        dict = manifest.as_dict
        assert (
            [{'href': 'http://foo/', 'type': 'text/html', 'extra': 'value'}] ==
            dict['resources'])

    def test_null_properties_not_propagated(self):
        manifest = Manifest()
        additional_properties = dict(extra="value", missing=None)

        manifest.add_link("http://foo/", "self", **additional_properties)
        manifest.add_reading_order("http://foo/", "text/html", "Chapter 1", **additional_properties)
        manifest.add_resource("http://foo/", "text/html", **additional_properties)

        manifest_dict = manifest.as_dict
        top_level_properties = ["links", "readingOrder", "resources"]
        for prop in top_level_properties:
            [entry] = manifest_dict["links"]
            assert "extra" in entry
            assert "value" == entry["extra"]
            assert "missing" not in entry


class TestUpdateBibliographicMetadata(DatabaseTest):

    def test_update(self):
        edition, pool = self._edition(with_license_pool=True)
        edition.cover_thumbnail_url = self._url
        [author] = edition.contributors
        manifest = Manifest()
        manifest.update_bibliographic_metadata(pool)

        metadata = manifest.metadata
        assert edition.title == metadata['title']
        assert pool.identifier.urn == metadata['identifier']

        # The author's sort name is used because they have no display
        # name.
        assert [author.sort_name] == metadata['author']

        # The language has been converted from ISO-3166-1-alpha-3 to
        # ISO-3166-1-alpha-2.
        assert "en" == metadata['language']

        [cover_link] = manifest.links
        assert 'cover' == cover_link['rel']
        assert edition.cover_thumbnail_url == cover_link['href']

        # Add an author's display name, and it is used in preference
        # to the sort name.
        author.display_name = "a display name"
        manifest = Manifest()
        manifest.update_bibliographic_metadata(pool)
        assert ["a display name"] == manifest.metadata['author']

        # If the pool has no presentation edition, the only information
        # we get is the identifier.
        pool.presentation_edition = None
        manifest = Manifest()
        manifest.update_bibliographic_metadata(pool)
        assert pool.identifier.urn == metadata['identifier']
        for missing in ['title', 'language', 'author']:
            assert missing not in manifest.metadata
        assert [] == manifest.links


class TestAudiobookManifest(object):

    def test_defaults(self):
        assert "http://bib.schema.org/Audiobook" == AudiobookManifest.DEFAULT_TYPE
        assert ("http://readium.org/webpub/default.jsonld" ==
            AudiobookManifest.DEFAULT_CONTEXT)

        manifest = AudiobookManifest()
        assert AudiobookManifest.DEFAULT_CONTEXT == manifest.context
        assert AudiobookManifest.DEFAULT_TYPE == manifest.type
        assert (
            {
                '@context' : manifest.context,
                'metadata' : {'@type': manifest.type}
            } ==
            manifest.as_dict)
