
import datetime
import pytz
import logging
from flask import Response

from lxml import builder, etree


class ElementMaker(builder.ElementMaker):
    """A helper object for creating etree elements."""

    def __dict__(self):
        # Remove default_typemap from the dictionary -- it contains functions
        # that can't be pickled.
        return dict(
            (k, v) for k, v in super(ElementMaker, self).__dict__
            if k != 'default_typemap'
        )

class AtomFeed(object):

    ATOM_TYPE = 'application/atom+xml'

    ATOM_LIKE_TYPES = [ATOM_TYPE, 'application/xml']

    TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ%z'

    ATOM_NS = 'http://www.w3.org/2005/Atom'
    APP_NS = 'http://www.w3.org/2007/app'
    #xhtml_ns = 'http://www.w3.org/1999/xhtml'
    DCTERMS_NS = 'http://purl.org/dc/terms/'
    OPDS_NS = 'http://opds-spec.org/2010/catalog'
    SCHEMA_NS = 'http://schema.org/'
    DRM_NS = 'http://librarysimplified.org/terms/drm'
    OPF_NS = 'http://www.idpf.org/2007/opf'
    OPENSEARCH_NS = 'http://a9.com/-/spec/opensearch/1.1/'

    SIMPLIFIED_NS = "http://librarysimplified.org/terms/"
    BIBFRAME_NS = "http://bibframe.org/vocab/"
    BIB_SCHEMA_NS = "http://bib.schema.org/"

    LCP_NS = 'http://readium.org/lcp-specs/ns'

    nsmap = {
        None: ATOM_NS,
        'app': APP_NS,
        'dcterms' : DCTERMS_NS,
        'opds' : OPDS_NS,
        'opf' : OPF_NS,
        'drm' : DRM_NS,
        'schema' : SCHEMA_NS,
        'simplified' : SIMPLIFIED_NS,
        'bibframe' : BIBFRAME_NS,
        'bib': BIB_SCHEMA_NS,
        'opensearch': OPENSEARCH_NS,
        'lcp': LCP_NS
    }

    default_typemap = {datetime: lambda e, v: _strftime(v)}
    E = ElementMaker(typemap=default_typemap, nsmap=nsmap)
    SIMPLIFIED = ElementMaker(typemap=default_typemap, nsmap=nsmap, namespace=SIMPLIFIED_NS)
    SCHEMA = ElementMaker(typemap=default_typemap, nsmap=nsmap, namespace=SCHEMA_NS)

    @classmethod
    def _strftime(self, date):
        """
        Format a date the way Atom likes it (RFC3339?)
        """
        return date.strftime(self.TIME_FORMAT)


    @classmethod
    def add_link_to_feed(cls, feed, children=None, **kwargs):
        link = cls.E.link(**kwargs)
        feed.append(link)
        if children:
            for i in children:
                link.append(i)


    @classmethod
    def add_link_to_entry(cls, entry, children=None, **kwargs):
        if 'title' in kwargs:
            kwargs['title'] = str(kwargs['title'])
        link = cls.E.link(**kwargs)
        entry.append(link)
        if children:
            for i in children:
                link.append(i)

    @classmethod
    def author(cls, *args, **kwargs):
        return cls.E.author(*args, **kwargs)

    @classmethod
    def contributor(cls, *args, **kwargs):
        return cls.E.contributor(*args, **kwargs)

    @classmethod
    def category(cls, *args, **kwargs):
        return cls.E.category(*args, **kwargs)


    @classmethod
    def entry(cls, *args, **kwargs):
        return cls.E.entry(*args, **kwargs)


    @classmethod
    def id(cls, *args, **kwargs):
        return cls.E.id(*args, **kwargs)


    @classmethod
    def link(cls, *args, **kwargs):
        return cls.E.link(*args, **kwargs)


    @classmethod
    def makeelement(cls, *args, **kwargs):
        return cls.E._makeelement(*args, **kwargs)


    @classmethod
    def name(cls, *args, **kwargs):
        return cls.E.name(*args, **kwargs)


    @classmethod
    def schema_(cls, field_name):
        return "{%s}%s" % (cls.SCHEMA_NS, field_name)

    @classmethod
    def summary(cls, *args, **kwargs):
        return cls.E.summary(*args, **kwargs)


    @classmethod
    def title(cls, *args, **kwargs):
        return cls.E.title(*args, **kwargs)


    @classmethod
    def update(cls, *args, **kwargs):
        return cls.E.update(*args, **kwargs)


    @classmethod
    def updated(cls, *args, **kwargs):
        return cls.E.updated(*args, **kwargs)


    def __init__(self, title, url, **kwargs):
        """Constructor.

        :param title: The title of this feed.
        :param url: The URL at which clients can expect to find this feed.
        """
        self.feed = self.E.feed(
            self.E.id(url),
            self.E.title(str(title)),
            self.E.updated(self._strftime(datetime.datetime.now(tz=pytz.UTC))),
            self.E.link(href=url, rel="self"),
        )
        super(AtomFeed, self).__init__(**kwargs)

    def __str__(self):
        if self.feed is None:
            return None
        return etree.tostring(self.feed, encoding="unicode", pretty_print=True)


class OPDSFeed(AtomFeed):

    ACQUISITION_FEED_TYPE = AtomFeed.ATOM_TYPE + ";profile=opds-catalog;kind=acquisition"
    NAVIGATION_FEED_TYPE = AtomFeed.ATOM_TYPE + ";profile=opds-catalog;kind=navigation"
    ENTRY_TYPE = AtomFeed.ATOM_TYPE + ";type=entry;profile=opds-catalog"

    GROUP_REL = "collection"
    FEATURED_REL = "http://opds-spec.org/featured"
    RECOMMENDED_REL = "http://opds-spec.org/recommended"
    POPULAR_REL = "http://opds-spec.org/sort/popular"
    OPEN_ACCESS_REL = "http://opds-spec.org/acquisition/open-access"
    ACQUISITION_REL = "http://opds-spec.org/acquisition"
    BORROW_REL = "http://opds-spec.org/acquisition/borrow"
    FULL_IMAGE_REL = "http://opds-spec.org/image"
    EPUB_MEDIA_TYPE = "application/epub+zip"

    REVOKE_LOAN_REL = "http://librarysimplified.org/terms/rel/revoke"
    NO_TITLE = "http://librarysimplified.org/terms/problem/no-title"

    # Most types of OPDS feeds can be cached client-side for at least ten
    # minutes.
    DEFAULT_MAX_AGE = 60 * 10

    def __init__(self, title, url):
        super(OPDSFeed, self).__init__(title, url)


class OPDSMessage(object):
    """An indication that an <entry> could not be created for an
    identifier.

    Inserted into an OPDS feed as an extension tag.
    """

    def __init__(self, urn, status_code, message):
        self.urn = urn
        if status_code:
            status_code = int(status_code)
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return etree.tostring(self.tag, encoding="unicode")

    def __repr__(self):
        return etree.tostring(self.tag)

    def __eq__(self, other):
        if self is other:
            return True

        if not isinstance(other, OPDSMessage):
            return False

        if (self.urn != other.urn or self.status_code != other.status_code
            or self.message != other.message):
            return False
        return True

    @property
    def tag(self):
        message_tag = AtomFeed.SIMPLIFIED.message()
        identifier_tag = AtomFeed.E.id()
        identifier_tag.text = self.urn
        message_tag.append(identifier_tag)

        status_tag = AtomFeed.SIMPLIFIED.status_code()
        status_tag.text = str(self.status_code)
        message_tag.append(status_tag)

        description_tag = AtomFeed.SCHEMA.description()
        description_tag.text = str(self.message)
        message_tag.append(description_tag)
        return message_tag
