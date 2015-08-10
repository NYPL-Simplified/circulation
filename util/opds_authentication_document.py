class OPDSAuthenticationDocument(object):

    MEDIA_TYPE = "application/vnd.opds.authentication.v1.0+json"

    BASIC_AUTH_FLOW = "http://opds-spec.org/auth/basic"

    @classmethod
    def fill_in(self, document, type=None, title=None, id=None, text=None,
                login_label=None, password_label=None, links={}):
        """Fill in any missing fields of an OPDS Authentication Document
        with the given values.
        """

        if document:
            data = dict(document)
        else:
            data = {}

        for key, value in (
                ('id', id), 
                ('title', title), 
                ('type', type)
        ):
            if value and (not key in data or not data[key]):
                data[key] = value
            if not key in data or not data[key]:
                raise ValueError('`%s` must be specified.' % key)

        if not isinstance(data['type'], list):
            raise ValueError('`type` must be a List.')

        if not 'labels' in data and (password_label or login_label):
            data['labels'] = {}

        for name, value in (
                ('password', password_label), ('login', login_label)):
            if value and (not name in data['labels'] 
                          or not data['labels'][name]):
                data['labels'][name] = value

        if text and (not 'text' in data or not data['text']):
            data['text'] = text

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
                data['links'][rel] = dicts


        return data
