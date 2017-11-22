"""Python generator for Readium Web Publication Manifest files."""

class Manifest(object):

    LD_TYPE = 'http://bib.schema.org/Book'
    MEDIA_TYPE = 'application/webpub+json'
    EXTENSION = '.json' # Just a guess

    def __init__(self, metadata={}, links=[], spine=[]):
        self.metadata = dict(metadata)
        self.links = list(links)
        self.spine = list(spine)

    def add_link(self, **kwargs):
        self.links.append(kwargs)

    def add_spine(self, **kwargs):
        self.spine.append(kwargs)


class AudiobookManifest(Manifest):

    LD_TYPE = 'http://bib.schema.org/Audiobook'
    MEDIA_TYPE = 'application/audiobook+json'
    EXTENSION = '.audiobook-manifest'
