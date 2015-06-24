class OPDSAuthenticationDocument(object):

    MEDIA_TYPE = "application/vnd.opds.authentication.v1.0+json"

    BASIC_AUTH_FLOW = "http://opds-spec.org/auth/basic"

    @classmethod
    def create(self, type, title, id, text=None,
               login_label=None, password_label=None):
        if not isinstance(type, list):
            raise ValueError('`type` must be a List.')
        if not type:
            raise ValueError('`type` cannot be empty.')
        if not title:
            raise ValueError('`title` must be specified.')
        if not id:
            raise ValueError('`id` must be specified.')
        data = dict(id=id, type=type, title=title)
        if text:
            data['text'] = text
        if login_label or password_label:
            labels = dict()
            if login_label:
                labels['login'] = login_label
            if password_label:
                labels['password'] = password_label
            data['labels'] = labels
        return data
