"""Helper classes for the Readium Web Publication Manifest format
(https://github.com/readium/webpub-manifest) and its audiobook profile
(https://github.com/HadrienGardeur/audiobook-manifest).
"""

from nose.tools import set_trace
import json
from . import LanguageCodes

class JSONable(object):
    """An object whose Unicode representation is a JSON dump
    of a dictionary.
    """

    def __unicode__(self):
        return json.dumps(self.as_dict)

    @property
    def as_dict(self):
        raise NotImplementedError()

    @classmethod
    def json_ready(cls, value):
        if isinstance(value, JSONable):
            return value.as_dict
        elif isinstance(value, list):
            return [cls.json_ready(x) for x in value]
        else:
            return value


class Manifest(JSONable):
    """A Python object corresponding to a Readium Web Publication
    Manifest.
    """

    BOOK_TYPE = "http://schema.org/Book"
    AUDIOBOOK_TYPE = "http://bib.schema.org/Audiobook"

    DEFAULT_CONTEXT = "http://readium.org/webpub/default.jsonld"
    DEFAULT_TYPE = BOOK_TYPE

    def __init__(self, context=None, type=None):
        self.context = context or self.DEFAULT_CONTEXT
        self.type = type or self.DEFAULT_TYPE
        self.metadata = { "@type": self.type }

        # Initialize all component lists to the empty list.
        for name in self.component_lists:
            setattr(self, name, [])

    @property
    def as_dict(self):
        data = {
            "@context": self.context,
            "metadata": self.metadata
        }
        for key in self.component_lists:
            value = getattr(self, key)
            if value:
                data[key] = self.json_ready(value)
        return data

    @property
    def component_lists(self):
        return 'links', 'spine', 'resources'

    def _append(self, append_to, **kwargs):
        append_to.append(kwargs)

    def add_link(self, href, rel, **kwargs):
        self._append(self.links, href=href, rel=rel, **kwargs)

    def add_spine(self, href, type, title, **kwargs):
        self._append(self.spine, href=href, type=type, title=title, **kwargs)

    def add_resource(self, href, type, **kwargs):
        self._append(self.resources, href=href, type=type, **kwargs)

    def update_bibliographic_metadata(self, license_pool):
        """Update this Manifest with basic bibliographic metadata
        taken from a LicensePool object.

        Currently this assumes that there is no other source of
        bibliographic metadata, so it will overwrite any metadata that is
        already present and add a cover link even if the manifest
        already has one.
        """
        self.metadata['identifier'] = license_pool.identifier.urn

        edition = license_pool.presentation_edition
        if not edition:
            return
        self.metadata['title'] = edition.title

        self.metadata['language'] = LanguageCodes.three_to_two.get(
            edition.language, edition.language
        )
        authors = [author.display_name or author.sort_name
                   for author in edition.author_contributors
                   if author.display_name or author.sort_name]
        if authors:
            self.metadata['author'] = authors

        if edition.cover_thumbnail_url:
            self.add_link(edition.cover_thumbnail_url, 'cover')


class TimelinePart(JSONable):
    """A single element in an Audiobook Manifest's 'timeline'.

    This has its own class because it can contain child TimelineParts,
    recursively, making it qualitatively more complicated than an
    entry in 'links' or 'spine'.
    """
    def __init__(self, href, title, children=None, **kwargs):
        self.href = href
        self.title = title
        self.children = children or []
        self.extra = kwargs

    def add_child(self, href, title, children=None, **kwargs):
        self.children.append(TimelinePart(href, title, children, **kwargs))

    @property
    def as_dict(self):
        data = dict(href=self.href, title=self.title)
        if self.children:
            data['children'] = [x.as_dict for x in self.children]
        data.update(self.extra)
        return data


class AudiobookManifest(Manifest):
    """A Python object corresponding to a Readium Web Publication
    Manifest.
    """

    DEFAULT_TYPE = Manifest.AUDIOBOOK_TYPE

    @property
    def component_lists(self):
        return super(AudiobookManifest, self).component_lists + ('timeline',)

    def add_timeline(self, href, title, children=None, **kwargs):
        """Add an item to the timeline."""
        part = TimelinePart(href, title, children, **kwargs)
        self.timeline.append(part)
        return part
