
from apispec import APISpec
from apispec_webframeworks.flask import FlaskPlugin


def generateSpecBase():
    # Initialize OpenAPI Spec Document
    spec = APISpec(
        openapi_version='3.0.1',
        title='Library Simplified Circulation Manager',
        version='0.1.0-alpha',
        info={
            'version': '0.1.0-alpha',
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
        plugins=[FlaskPlugin()]
    )

    # Add securitySchemes
    spec.components.security_scheme(
        'BearerAuth',
        component={
            'type': 'http',
            'scheme': 'bearer'
        },
    )
    spec.components.security_scheme(
        'BasicAuth',
        component={
            'type': 'http',
            'scheme': 'basic'
        }
    )

    # Shared Problem Response Format
    spec.components.schema(
        'ProblemResponse',
        component={
            'type': 'object',
            'properties': {
                'type': {
                    'type': 'string'
                },
                'title': {
                    'type': 'string'
                },
                'status': {
                    'type': 'string'
                },
                'detail': {
                    'type': 'string'
                },
                'instance': {
                    'type': 'string'
                },
                'debug_message': {
                    'type': 'string'
                }
            }
        }
    )

    # Add Schemas that can be shared by paths
    # Currently must be in order for cascading references
    spec.components.schema(
        'SiteAdminRoles',
        component={
            'type': 'string',
            'enum': ['system', 'manager-all', 'manager', 'librarian-all', 'librarian']
        }
    )

    spec.components.schema(
        'AdminRole',
        component={
            'type': 'object',
            'properties': {
                'library': {
                    'type': 'string'
                },
                'role': {
                    '$ref': '#/components/schemas/SiteAdminRoles'
                }
            }
        }
    )

    spec.components.schema(
        'SiteAdmin',
        component={
            'type': 'object',
            'properties': {
                'email': {
                    'type': 'string'
                },
                'roles': {
                    'type': 'array',
                    'items': {
                        '$ref': '#/components/schemas/AdminRole'
                    }
                }
            }
        }
    )

    spec.components.schema(
        'SiteAdminPost',
        component={
            'type': 'object',
            'properties': {
                'email': {
                    'type': 'string',
                    'required': True
                },
                'password': {
                    'type': 'string'
                },
                'roles': {
                    'type': 'array',
                    'items': {
                        '$ref': '#/components/schemas/AdminRole'
                    }
                }
            }
        }
    )

    spec.components.schema(
        'IndividualAdminResponse',
        component={
            'type': 'object',
            'properties': {
                'individualAdmins': {
                    'type': 'array',
                    'items': {
                        '$ref': '#/components/schemas/SiteAdmin'
                    }
                }
            }
        }
    )


    return spec
