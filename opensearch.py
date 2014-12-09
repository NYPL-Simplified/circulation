class OpenSearchDocument(object):
    """Generates OpenSearch documents."""

    TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
 <OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">
   <ShortName>%(name)s</ShortName>
   <Description>%(description)s</Description>
   <Tags>%(tags)s</Tags>
   <Url type="application/atom+xml;profile=opds-catalog" template="%(url_template)s"/>
 </OpenSearchDescription>"""

    @classmethod
    def search_info(cls, lane):

        d = dict(name="Search")
        tags = []
        
        if lane is not None:
            tags.append(lane.name.lower().replace(" ", "-").replace("&", "&amp;"))
            description = "Search for %s books" % lane.name.replace("&", "&amp;")
        else:
            description = "Search for books"
        d['description'] = description
        d['tags'] = " ".join(tags)
        return d

    @classmethod
    def for_lane(cls, lane, base_url):
        info = cls.search_info(lane)
        info['url_template'] = base_url + "?q={searchTerms}"

        return cls.TEMPLATE % info
