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

        if lane is not None and lane.search_target is not None:
            tags.append(lane.search_target.display_name.lower().replace(" ", "-"))
            description = "Search %s" % lane.search_target.display_name
        else:
            description = "Search"
        d['description'] = description
        d['tags'] = " ".join(tags)
        return d

    @classmethod
    def url_template(self, base_url):
        """Turn a base URL into an OpenSearch URL template."""
        if '?' in base_url:
            query = '&'
        else:
            query = '?'
        return base_url + query + "q={searchTerms}"

    @classmethod
    def for_lane(cls, lane, base_url):
        info = cls.search_info(lane)
        info['url_template'] = cls.url_template(base_url)
        info = cls.escape_entities(info)
        return cls.TEMPLATE % info

    @classmethod
    def escape_entities(cls, info):
        """Escape ampersands in the given dictionary's values."""
        return dict([(k, v.replace("&", "&amp;")) for (k, v) in info.items()])
