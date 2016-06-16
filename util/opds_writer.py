
import datetime
import logging

from lxml import builder, etree
from nose.tools import set_trace


class AtomFeed(object):

    TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ%z'

    ATOM_NS = 'http://www.w3.org/2005/Atom'
    APP_NS = 'http://www.w3.org/2007/app'
    #xhtml_ns = 'http://www.w3.org/1999/xhtml'
    DCTERMS_NS = 'http://purl.org/dc/terms/'
    OPDS_NS = 'http://opds-spec.org/2010/catalog'
    SCHEMA_NS = 'http://schema.org/'

    SIMPLIFIED_NS = "http://librarysimplified.org/terms/"
    BIBFRAME_NS = "http://bibframe.org/vocab/"

    nsmap = {
        None: ATOM_NS,
        'app': APP_NS,
        'dcterms' : DCTERMS_NS,
        'opds' : OPDS_NS,
        'schema' : SCHEMA_NS,
        'simplified' : SIMPLIFIED_NS,
        'bibframe' : BIBFRAME_NS,
    }

    default_typemap = {datetime: lambda e, v: _strftime(v)}
    E = builder.ElementMaker(typemap=default_typemap, nsmap=nsmap)
    SCHEMA = builder.ElementMaker(typemap=default_typemap, nsmap=nsmap, namespace="http://schema.org/")

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
        #links.append(E.link(rel=rel, href=url, type=image_type))
        link = cls.E.link(**kwargs)
        entry.append(link)
        if children:
            for i in children:
                link.append(i)


    @classmethod
    def author(cls, *args, **kwargs):
        return cls.E.author(*args, **kwargs)


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


    def __init__(self, title, url):
        self.feed = self.E.feed(
            self.E.id(url),
            self.E.title(title),
            self.E.updated(self._strftime(datetime.datetime.utcnow())),
            self.E.link(href=url, rel="self"),
        )


    def __unicode__(self):
        if not self.feed:
            return None

        string_tree = etree.tostring(self.feed, pretty_print=True)
        return string_tree.encode("utf8")



class OPDSFeed(AtomFeed):

    ACQUISITION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=acquisition"
    NAVIGATION_FEED_TYPE = "application/atom+xml;profile=opds-catalog;kind=navigation"
    ENTRY_TYPE = "application/atom+xml;type=entry;profile=opds-catalog"

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

    def __init__(self, title, url):
        super(OPDSFeed, self).__init__(title, url)





