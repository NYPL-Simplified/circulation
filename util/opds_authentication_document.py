class OPDSAuthenticationDocument(object):

    MEDIA_TYPE = "application/vnd.opds.authentication.v1.0+json"

    @classmethod
    def fill_in(self, document, providers, _db=None, name=None, id=None, links={}):
        """Fill in any missing fields of an OPDS Authentication Document
        with the given values.
        """

        if document:
            data = dict(document)
        else:
            data = {}

        for key, value in (
                ('id', id), 
                ('name', name), 
        ):
            if value and (not key in data or not data[key]):
                data[key] = value
            if not key in data or not data[key]:
                raise ValueError('`%s` must be specified.' % key)

        if not isinstance(providers, list):
            raise ValueError('`providers` must be a list.')
        provider_docs = {}
        for provider in providers:
            if not getattr(provider, 'URI', None):
                raise ValueError("%r does not define .URI" % provider)
            provider_docs[provider.URI] = provider.authentication_provider_document(_db)
        data['providers'] = provider_docs

        if links:
            data['links'] = {}
            for rel, urls in links.items():
                if not isinstance(urls, list):
                    urls = [urls]
                dicts = []
                for url in urls:
                    if isinstance(url, basestring):
                        url = dict(href=url)
                    dicts.append(url)
                if len(dicts) == 1:
                    [dicts] = dicts 
                data['links'][rel] = dicts


        return data
