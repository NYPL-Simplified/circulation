import inspect
from apispec import APISpec
from apispec.exceptions import APISpecError
from apispec_webframeworks.flask import FlaskPlugin

from .plugins import CSRFPlugin
from api.admin import routes as adminRoutes

import logging

logger = logging.getLogger(__name__)


class OpenAPIController:
    OPENAPI_VERSION = '3.0.1'
    DOC_VERSION = '0.1.0-alpha'

    def __init__(self):
        self.spec = self.generateSpecBase()
    
    def addComponent(self, componentType, schemaName, schemaType, schemaProps):
        addComponentFunc = getattr(self.spec.components, componentType)

        schemaProps = {'properties': schemaProps} if schemaType == 'object' else schemaProps

        schemaComponent = {
            'type': schemaType,
            **schemaProps
        }

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
            'schema', 'SiteAdminRoles', 'string',
            {'enum': ['system', 'manager-all', 'manager', 'librarian-all', 'librarian']}
        )

        self.addComponent(
            'schema', 'AdminRole', 'object',
            {
                'library': {'type': 'string'},
                'role': {'$ref': '#/components/schemas/SiteAdminroles'}
            }
        )

        self.addComponent(
            'schema', 'SiteAdmin', 'object',
            {
                'email': {'type': 'string'},
                'roles': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/AdminRole'}
                }
            }
        )

        self.addComponent(
            'schema', 'SiteAdminPost', 'object',
            {
                'email': {
                    'type': 'string',
                    'required': True
                },
                'password': {
                    'type': 'string',
                    'format': 'password'
                },
                'roles': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/AdminRole'}
                }
            }
        )

        self.addComponent(
            'schema', 'ChangePasswordPost', 'object',
            {
                'password': {
                    'type': 'string',
                    'format': 'password',
                    'required': True
                }
            }
        )

        self.addComponent(
            'schema', 'IndividualAdminResponse', 'object',
            {
                'individualAdmins': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/SiteAdmin'}
                }
            }
        )

        self.addComponent(
            'schema', 'AtomCategory', 'object',
            {
                'schema': {'type': 'string', 'required': True},
                'term': {'type': 'string', 'required': True},
                'label': {'type': 'string', 'required': True}
            }
        )

        self.addComponent(
            'schema', 'OPDSRelations', 'string',
            {
                'enum': [
                    'self',  # OPDS Reference to current document
                    'alternate',  # Alternate, non-OPDS representation
                    'http://opds-spec.org/acquisition',  # Generic Access link where a resource may be accessed
                    'http://opds-spec.org/acquisition/open-access',  # Free access (no payment, registration or log-in required)
                    'http://opds-spec.org/acquisition/borrow',  # Resource may be borrowed from source
                    'http://opds-spec.org/acquisition/buy',  # Resource may be bought from source
                    'http://opds-spec.org/acquisition/sample',  # A subset of the resource can be viewed
                    'http://opds-spec.org/acquisition/subscribe',  # A complete resource may be retrieved on the basis of a larger subscription
                    'http://opds-spec.org/image',  # A representation of a resource (i.e. a cover)
                    'http://opds-spec.org/image/thumbnail'  # A smaller representation of a resource
                ]
            }
        )

        self.addComponent(
            'schema', 'OPDSLink', 'object',
            {
                'rel': {
                    '$ref': '#/components/schemas/OPDSRelations',
                    'required': True
                },
                'href': {'type': 'string', 'required': True},
                'type': {'type': 'string', 'required': True}
            }
        )

        self.addComponent(
            'schema', 'OPDSEntry', 'object',
            {
                'atom:id': {'type': 'string', 'required': True},
                'dc:identifier': {'type': 'string'},
                'atom:updated': {'type': 'string', 'required': True},
                'dc:issued': {'type': 'string'},
                'atom:title': {'type': 'string', 'required': True},
                'atom:author': {'type': 'string',},
                'atom:rights': {'type': 'string'},
                'atom:summary': {'type': 'string'},
                'atom:content': {'type': 'string'},
                'atom:contributor': {'type': 'string'},
                'atom:published': {'type': 'string'},
                'opds:price': {'type': 'string'},
                'atom:category': {'$ref': '#/components/schemas/AtomCategory'},
                'opds:link': {'$ref': '#/components/schemas/OPDSLink'}
            }
        )

        self.addComponent(
            'schema', 'ProtocolString', 'string',
            {
                'enum': [
                    'OPDS Import', 'OPDS 2.0 Import', 'Overdrive', 'Odlio',
                    'Bibliotheca', 'Axis 360', 'RBDigital', 'OPDS for Distributors',
                    'Enki', 'Feedbooks', 'LCP', 'Manual intervention', 'Proquest'
                ]
            }
        )

        self.addComponent(
            'schema', 'CustomListCollection', 'object',
            {
                'id': {'type': 'string'},
                'name': {'type': 'string'},
                'protocol': {'$ref': '#/components/schemas/ProtocolString'}
            }
        )

        self.addComponent(
            'schema', 'CustomList', 'object',
            {
                'id': {'type': 'string'},
                'name': {'type': 'string'},
                'entry_count': {'type': 'integer'},
                'collections': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/CustomListCollection'}
                }
            }
        )

        self.addComponent(
            'schema', 'CustomListResponse', 'object',
            {
                'custom_lists': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/CustomList'}
                }
            }
        )

        self.addComponent(
            'schema', 'ListEntry', 'object',
            {
                'id': {
                    'type': 'string',
                    'required': True,
                    'description': 'A URN identifying the work of the entry'
                }
            }
        )

        self.addComponent(
            'schema', 'ListCollection', 'string',
            {
                'description': 'Identifier for an existing collection associated with the library'
            }
        )

        self.addComponent(
            'schema', 'CustomListUpsertPost', 'object',
            {
                'id': {'type': 'string', 'required': True},
                'name': {'type': 'string', 'required': True},
                'entries': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schema/ListEntry'}
                },
                'collections': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListCollection'}
                }
            }
        )

        self.addComponent(
            'schema', 'CustomListUpdatePost', 'object',
            {
                'id': {'type': 'string', 'required': True},
                'name': {'type': 'string', 'required': True},
                'entries': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schema/ListEntry'}
                },
                'collections': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListCollection'}
                },
                'deletedEntries': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListEntry'}
                }
            }
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
            'schema', 'LaneListResponse', 'object',
            {
                'lanes': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/Lane'}
                }
            }
        )

        self.addComponent(
            'schema', 'Lane', 'object',
            {
                'id': {'type': 'string'},
                'display_name': {'type': 'string'},
                'visible': {'type': 'boolean'},
                'count': {'type': 'integer'},
                'sublanes': {
                    'description': 'A nested array of Lane objects',
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/Lane'}
                },
                'custom_list_ids': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'inherit_parent_restrictions': {'type': 'boolean'}
            }
        )

        self.addComponent(
            'schema', 'LaneUpsertPost', 'object',
            {
                'id': {'type': 'string'},
                'parent_id': {'type': 'string'},
                'display_name': {'type': 'string', 'required': True},
                'custom_list_ids': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'inherit_parent_restrictions': {'type': 'boolean'}
            }
        )

        self.addComponent(
            'schema', 'ChangeOrderBody', 'array',
            {
                'items': {
                    'type': 'object',
                    'properties': {
                        'id': {'type': 'string'},
                        'sublanes': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'properties': {
                                    'id': {'type': 'string'}
                                }
                            }
                        }
                    }
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
        for name, method in adminRoutes.__dict__.items():
            self.addPath(name, method)
    
    def addPath(self, name, method):
        if inspect.isfunction(method):
            try:
                self.spec.path(view=method)
            except APISpecError:
                logger.warning(f'{name} unable to create view')
                pass

    @classmethod
    def generateSpecBase(cls):
        # Initialize OpenAPI Spec Document
        return APISpec(
            openapi_version=cls.OPENAPI_VERSION,
            title='Library Simplified Circulation Manager',
            version=cls.DOC_VERSION,
            info={
                'version': cls.DOC_VERSION,
                'title': 'Library Simplified Circulation Manager',
                'summary': 'Loan and hold management system for digital content for libraries participating in the Library Simplified project.',
                'description': 'The Circulation Manager is the main connection between a library\'s collection and Library Simplified\'s various client-side applications, including SimplyE. It handles user authentication, combines licensed works with open access content, maintains book metadata, and serves up available books in appropriately organized OPDS feeds.',
                'termsOfService': 'https://librarysimplified.org',
                'contact': {
                    'name': 'Library Simplified',
                    'url': 'https://librarysimplified.org/about/contact/',
                    'email': 'info@librarysimplified.org'
                },
                'license': {
                    'name': 'Apache License 2.0',
                    'identifier': 'Apache-2.0'
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

    @classmethod
    def generateSpec(cls):
        specManager = cls()

        specManager.addSecuritySchemes()
        specManager.addSchemes()
        specManager.addParameters()
        specManager.addPaths()

        return specManager.spec.to_dict()
