

class OPDSAuthenticationFlow(object):
    """An object that can be represented as an Authentication Flow
    in an Authentication For OPDS document.
    """

    FLOW_TYPE = None

    def authentication_flow_document(self, _db):
        """Convert this object into a dictionary or a
        list of dictionaries that can be used in the
        `authentication` list of an AuthenticationFor OPDS document.
        """
        data = self._authentication_flow_document(_db)
        if isinstance(data, list):
            for entry in data:
                self._get_type(entry)
        else:
            self._get_type(data)

        return data

    def _get_type(self, data):
        if not data.get('type'):
            data['type'] = self.FLOW_TYPE
        if not data.get('type'):
            raise ValueError(
                "Authentication flow document for %r does not include required field 'type'" % self
            )

    def _authentication_flow_document(self, _db):
        raise NotImplementedError()


class AuthenticationForOPDSDocument(object):
    """A data structure that can become an Authentication For OPDS
    document.
    """

    MEDIA_TYPE = "application/vnd.opds.authentication.v1.0+json"
    LINK_RELATION = "http://opds-spec.org/auth/document"

    def __init__(self, id=None, title=None, authentication_flows=[], links=[]):
        """Initialize an Authentication For OPDS document.

        :param id: URL to use as the 'id' of the Authentication For
            OPDS document.
        :param title: String to use as the 'title' of the
            Authentication For OPDS document.
        :param authentication_flows: A list of
           `OPDSAuthenticationFlow` objects, used to construct the
           'authentication' list.
        :param links: A list of dictionaries representing hypermedia links.
        """
        self.id = id
        self.title = title
        self.authentication_flows = authentication_flows
        self.links = links

    def to_dict(self, _db):
        """Convert this data structure to a dictionary that becomes an
        Authentication For OPDS document when serialized to JSON.

        :param _db: Database connection or other argument to pass into
            OPDSAuthenticationFlow.to_dict().
        """
        for key, value in (('id', self.id), ('title', self.title)):
            if not value:
                raise ValueError(
                    "'%s' is required in an Authentication For OPDS document." % key
                )

        for key, value in [
            ('authentication_flows', self.authentication_flows),
            ('links', self.links)
        ]:
            if not isinstance(value, list):
                raise ValueError("'%s' must be a list." % key)

        document = dict(id=self.id, title=self.title)
        flow_documents = document.setdefault('authentication', [])
        for flow in self.authentication_flows:
            flow_documents.append(flow.authentication_flow_document(_db))
        if self.links:
            doc_links = document.setdefault('links', [])
            for link in self.links:
                if not isinstance(link, dict):
                    raise ValueError(
                        "Link %r is not a dictionary" % link
                    )
                for required_field in 'rel', 'href':
                    if not link.get(required_field):
                        raise ValueError(
                            "Link %r does not define required field '%s'" % (
                                link, required_field
                            )
                        )
                doc_links.append(link)
        return document
