import inspect
from apispec import APISpec
from apispec.exceptions import APISpecError
from apispec_webframeworks.flask import FlaskPlugin

from .plugins import CSRFPlugin
from api import routes as publicRoutes

import logging

logger = logging.getLogger(__name__)


class PublicAPIController:
    OPENAPI_VERSION = '3.0.1'
    DOC_VERSION = '0.1.0-public'

    def __init__(self):
        self.spec = self.generateSpecBase()

    def addComponent(self,
                     componentType, schemaName, schemaType, schemaProps,
                     requiredFields=None):
        addComponentFunc = getattr(self.spec.components, componentType)

        schemaProps = {
            'properties': schemaProps} if schemaType == 'object' else schemaProps

        schemaComponent = {
            'type': schemaType,
            **schemaProps
        }

        if requiredFields:
            schemaComponent['required'] = requiredFields

        addComponentFunc(
            schemaName,
            component=schemaComponent
        )

    def addSecuritySchemes(self):
        self.addComponent(
            'security_scheme', 'BasicAuth', 'http', {'scheme': 'basic'}
        )

    def addSchemes(self):
        self.addComponent(
            'schema', 'ProblemResponse', 'object',
            {
                'type': {'type': 'string'},
                'title': {'type': 'string'},
                'status': {'type': 'string'},
                'detail': {'type': 'string'},
                'instance': {'type': 'string'},
                'debug_message': {'type': 'string'}
            }
        )
        self.addComponent(
            'schema', 'BibframeProvider', 'object',
            {
                'bibframe:ProviderName': {'type': 'string'}
            }
        )
        self.addComponent(
            'schema', 'AtomSeries', 'object',
            {
                'atom:name': {'type': 'string'},
                'atom:position': {'type': 'integer'},
            }
        )
        self.addComponent(
            'schema', 'AtomAuthor', 'object',
            {
                'atom:name': {'type': 'string'},
                'atom:sort_name': {'type': 'string'},
                'atom:family_name': {'type': 'string'},
                'simplified:wikipedia_name': {'type': 'string'},
                'atom:sameas': {
                    'type': 'string',
                    'enum': ['http://viaf.org/viaf/[VIAF_ID]', 'http://id.loc.gov/authorities/names/[LCNAF_ID]'],
                    'description': 'A resolvable VIAF or LCNAF URI for the current author'
                }
            }
        )
        self.addComponent(
            'schema', 'AtomCategory', 'object',
            {
                'scheme': {'type': 'string'},
                'term': {'type': 'string'},
                'label': {'type': 'string'}
            },
            requiredFields=['schema', 'term', 'label']
        )
        self.addComponent(
            'schema', 'OPDSEntry', 'object',
            {
                'atom:id': {'type': 'string'},
                'dc:identifier': {'type': 'string'},
                'atom:updated': {'type': 'string'},
                'dcterms:issued': {'type': 'string'},
                'atom:title': {'type': 'string'},
                'atom:author': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/AtomAuthor'}
                },
                'atom:rights': {'type': 'string'},
                'atom:summary': {'type': 'string'},
                'atom:content': {'type': 'string'},
                'atom:contributor': {'type': 'string'},
                'atom:published': {'type': 'string'},
                'opds:price': {'type': 'string'},
                'atom:category': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/AtomCategory'}
                },
                'opds:link': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/OPDSLink'}
                },
                'schema:alternativeHeadline': {'type': 'string'},
                'simplified:pwid': {'type': 'string'},
                'atom:additionalType': {'type': 'string'},
                'atom:series': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/AtomSeries'}
                },
                'dcterms:language': {'type': 'string'},
                'dcterms:publisher': {'type': 'string'},
                'dcterms:publisherImprint': {'type': 'string'},
                'bibframe:distribution': {'$ref': '#/components/schemas/BibframeProvider'}
            },
            requiredFields=['atom:id', 'atom:updated', 'atom:title']
        )
        self.addComponent(
            'schema', 'OPDSRelations', 'string',
            {
                'enum': [
                    'self',  # OPDS Reference to current document
                    'alternate',  # Alternate, non-OPDS representation
                    # Generic Access link where a resource may be accessed
                    'http://opds-spec.org/acquisition',
                    # Free access (no payment, registration or log-in required)
                    'http://opds-spec.org/acquisition/open-access',
                    # Resource may be borrowed from source
                    'http://opds-spec.org/acquisition/borrow',
                    'http://opds-spec.org/acquisition/buy',  # Resource may be bought from source
                    # A subset of the resource can be viewed
                    'http://opds-spec.org/acquisition/sample',
                    # A complete resource may be retrieved on the basis of a larger subscription
                    'http://opds-spec.org/acquisition/subscribe',
                    # A representation of a resource (i.e. a cover)
                    'http://opds-spec.org/image',
                    # A smaller representation of a resource
                    'http://opds-spec.org/image/thumbnail'
                ]
            }
        )
        self.addComponent(
            'schema', 'OPDSLink', 'object',
            {
                'rel': {'$ref': '#/components/schemas/OPDSRelations'},
                'href': {'type': 'string'},
                'type': {'type': 'string'}
            },
            requiredFields=['rel', 'href', 'type']
        )
        self.addComponent(
            'schema', 'OPDSFeedResponse', 'object',
            {
                'atom:id': {'type': 'string'},
                'atom:title': {'type': 'string'},
                'updated': {'type': 'string', 'format': 'date-time'},
                'link': {'$ref': '#/components/schemas/OPDSLink'},
                'entry': {'$ref': '#/components/schemas/OPDSEntry'}
            }
        )
        self.addComponent(
            'schema', 'HTTPAuthToken', 'object',
            {
                'access_token': {
                    'type': 'string',
                    'example': 'JWT Auth Token'
                },
                'token_type': {
                    'type': 'string',
                    'example': 'Bearer'
                },
                'expires_in': {
                    'type': 'integer',
                    'description': 'Number of seconds remaining until token expires'
                }
            }
        )

    def addParameters(self):
        # TODO Extend addComponent to accomodate parameters
        self.spec.components.parameter(
            'X-CSRF-Token',
            'header',
            component={
                'schema': {
                    'type': 'string'
                },
                'required': True
            }
        )

    def addPaths(self):
        for name, method in publicRoutes.__dict__.items():
            self.addPath(name, method)

    def addPath(self, name, method):
        if inspect.isfunction(method):
            try:
                self.spec.path(view=method)
            except APISpecError:
                logger.warning(f'{name} unable to create view')
                pass

    @ classmethod
    def generateSpecBase(cls):
        # Initialize OpenAPI Spec Document
        return APISpec(
            openapi_version=cls.OPENAPI_VERSION,
            title='Library Simplified Circulation Manager',
            version=cls.DOC_VERSION,
            info={
                'version': cls.DOC_VERSION,
                'title': 'Library Simplified Circulation Manager',
                'description': 'The Circulation Manager is the main connection between a library\'s collection and Library Simplified\'s various client-side applications, including SimplyE. It handles user authentication, combines licensed works with open access content, maintains book metadata, and serves up available books in appropriately organized OPDS feeds.',
                'termsOfService': 'https://librarysimplified.org',
                'contact': {
                    'name': 'Library Simplified',
                    'url': 'https://librarysimplified.org/about/contact/',
                    'email': 'info@librarysimplified.org'
                },
                'license': {
                    'name': 'Apache License 2.0',
                    'url': 'http://www.apache.org/licenses/LICENSE-2.0'
                }
            },
            servers=[
                {
                    'url': 'http://localhost',
                    'description': 'localhost'
                },
                {
                    'url': 'https://circulation.librarysimplified.org/',
                    'description': 'NYPL Production Circulation Manager'
                },
                {
                    'url': 'https://qa-circulation.librarysimplified.org/',
                    'description': 'NYPL QA Circulation Manager'
                },
                {
                    'url': 'https://circulation.openebooks.org/',
                    'description': 'Open eBooks Production Circulation Manager'
                }
            ],
            plugins=[FlaskPlugin(), CSRFPlugin()]
        )

    @ classmethod
    def generateSpec(cls):
        specManager = cls()

        specManager.addSecuritySchemes()
        specManager.addSchemes()
        specManager.addParameters()
        specManager.addPaths()

        return specManager.spec.to_dict()
