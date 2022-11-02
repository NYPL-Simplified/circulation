import inspect
from apispec import APISpec
from apispec.exceptions import APISpecError
from apispec_webframeworks.flask import FlaskPlugin

from .plugins import CSRFPlugin
from api.admin import routes as adminRoutes

import logging

logger = logging.getLogger(__name__)


class AdminAPIController:
    OPENAPI_VERSION = '3.0.1'
    DOC_VERSION = '0.1.0-alpha'

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
            'schema', 'SiteAdminRoles', 'string',
            {'enum': ['system', 'manager-all',
                      'manager', 'librarian-all', 'librarian']}
        )

        self.addComponent(
            'schema', 'AdminRole', 'object',
            {
                'library': {'type': 'string'},
                'role': {'$ref': '#/components/schemas/SiteAdminRoles'}
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
                'email': {'type': 'string'},
                'password': {
                    'type': 'string',
                    'format': 'password'
                },
                'roles': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/AdminRole'}
                }
            },
            requiredFields=['email']
        )

        self.addComponent(
            'schema', 'ChangePasswordPost', 'object',
            {
                'password': {
                    'type': 'string',
                    'format': 'password'
                }
            },
            requiredFields=['password']
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
                'scheme': {'type': 'string'},
                'term': {'type': 'string'},
                'label': {'type': 'string'}
            },
            requiredFields=['schema', 'term', 'label']
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
            'schema', 'AtomSeries', 'object',
            {
                'atom:name': {'type': 'string'},
                'atom:position': {'type': 'integer'},
            }
        )

        self.addComponent(
            'schema', 'BibframeProvider', 'object',
            {
                'bibframe:ProviderName': {'type': 'string'}
            }
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
                    'description': 'A URN identifying the work of the entry'
                }
            },
            requiredFields=['id']
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
                'id': {'type': 'string'},
                'name': {'type': 'string'},
                'entries': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListEntry'}
                },
                'collections': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListCollection'}
                }
            },
            requiredFields=['id', 'name']
        )

        self.addComponent(
            'schema', 'CustomListUpdatePost', 'object',
            {
                'id': {'type': 'string'},
                'name': {'type': 'string'},
                'entries': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListEntry'}
                },
                'collections': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListCollection'}
                },
                'deletedEntries': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/ListEntry'}
                }
            },
            requiredFields=['id', 'name']
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
                'count': {
                    'type': 'integer',
                    'description': 'Count is determined by the `update_lane_size` script and is stored in the lane model in the circulation database in the `size` column. This counts only works in the current lane, and not sublanes'
                },
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
                'display_name': {'type': 'string'},
                'custom_list_ids': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'inherit_parent_restrictions': {'type': 'boolean'}
            },
            requiredFields=['display_name']
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

        self.addComponent(
            'schema', 'MARCRollContributorsDict', 'object',
            {
                'MARCRoleCode': {
                    'type': 'string',
                    'description': 'Contributor role'
                },
            }
        )
        self.addComponent(
            'schema', 'LanguageCodesSchema', 'object',
            {
                'language_code': {
                    'type': 'array',
                    'description': 'A dict of ISO language codes and associated language names',
                    'items': {
                        'type': 'string',
                        'description': 'Language names associated with the ISO code'

                    }
                }
            }
        )
        self.addComponent(
            'schema', 'MediaSchema', 'object',
            {
                'href': {
                    'type': 'string',
                    'description': 'String of media type with associated Schema URL'
                }
            }
        )
        self.addComponent(
            'schema', 'BulkCirculationEvents', 'array',
            {'items': {
                'type': 'string',
                'description': 'A CSV of circulation events for the selected library.'
            }
            }
        )
        self.addComponent(
            'schema', 'CustomListCollectionArray', 'array',
            {
                'array': {
                    'type': 'array',
                    'items': {'$ref': '#/components/schemas/CustomListCollection'}
                }
            }
        )
        self.addComponent(
            'schema', 'LicenseSchema', 'object',
            {
                'URI': {
                    'type': 'object',
                    'properties': {
                        'allows_derivatives': {'type': 'boolean'},
                        'name': {
                            'type': 'string',
                            'description': 'License name from associated URL'
                        },
                        'open_access': {'type': 'boolean'}
                    }
                }
            }
        )
        self.addComponent(
            'schema', 'CirculationEventSchema', 'object',
            {
                'circulation_events': {
                    'type': 'array',
                    'description': 'Array of circulation events',
                    'items': {
                        'type': 'object',
                        'description': 'Event',
                        'properties': {
                            'id': {'type': 'string'},
                            'type': {'type': 'string'},
                            'time': {
                                'type': 'string',
                                'format': 'date'
                            },
                            'book': {
                                'type': 'object',
                                'properties': {
                                    'title': {'type': 'string'},
                                    'url': {'$ref': '#/components/schemas/OPDSLink'},
                                }
                            }
                        }
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'LibraryStatsSchema', 'object',
            {
                'library_stats': {
                    'type': 'object',
                    'properties': {
                        'library': {
                            'type': 'object',
                            'properties': {
                                'patron': {
                                    'type': 'object',
                                    'properties': {
                                        'total': {'type': 'integer'},
                                        'with_active_loans': {'type': 'integer'},
                                        'with_active_loans_or_holds': {
                                            'type': 'integer'
                                        },
                                        'loans': {'type': 'integer'},
                                        'holds': {'type': 'integer'}
                                    }
                                },
                                'inventory': {
                                    'type': 'object',
                                    'properties': {
                                        'titles': {
                                            'type': 'integer',
                                            'description': 'total title count'
                                        },
                                        'licenses': {
                                            'type': 'integer',
                                            'description': 'total license count'
                                        },
                                        'available_license_count': {
                                            'type': 'integer',
                                            'description': 'total available'
                                        }
                                    }
                                },
                                'collections': {
                                    'type': 'object',
                                    'properties': {
                                        'licensed_titles': {'type': 'integer'},
                                        'open_access_titles': {'type': 'integer'},
                                        'licenses': {'type': 'integer'},
                                        'available_licenses': {'type': 'integer'}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'ProtocolDictSchema', 'array',
            {
                'items': {
                    'type': 'object',
                    'properties': {
                        'name': {'type': 'string'},
                        'label': {'type': 'string'},
                        'description': {'type': 'string'},
                        'instructions': {'type': 'string'},
                        'sitewide': {'type': 'string'},
                        'settings': {'type': 'string'},
                        'child_settings': {'type': 'string'},
                        'library_settings': {'type': 'string'},
                        'supports_registration': {'type': 'string'},
                        'supports_staging': {'type': 'string'},
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'ServicesDictSchema', 'array',
            {
                'items': {
                    'type': 'object',
                    'properties': {
                        'id': {'type': 'string'},
                        'name': {'type': 'string'},
                        'protocol': {'$ref': '#/components/schemas/ProtocolString'},
                        'settings': {
                            'type': 'object',
                            'properties': {
                                'key': {'type': 'string'}
                            }
                        },
                        'libraries': {
                            'type': 'object',
                            'properties': {
                                'short_name': {'type': 'string'},
                                'key': {'type': 'string'}
                            }
                        }
                    }
                }
            }
        )
        self.addComponent(
            'schema', 'ClassificationsSchema', 'object',
            {
                'book': {
                    'type': 'object',
                    'description': 'The work searched for in parameters',
                    'properties': {
                        'identifier_type': {'type': 'string'},
                        'identifier': {'type': 'string'},
                    }
                },
                'classifications': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'type ': {'type': 'string'},
                            'name': {'type': 'string'},
                            'source': {'type': 'string'},
                            'weight': {'type': 'integer'}
                        }
                    }
                }
            }
        )
        self.addComponent(
            'schema', 'EditClassificationsPost', 'object',
            {
                'genre': {'type': 'string'},
                'audiences': {'type': 'string'},
                'target_age_minimum': {'type': 'integer'},
                'target_age_maximum': {'type': 'integer'},
                'fiction': {'type': 'boolean'},
            },
        )

        self.addComponent(
            'schema', 'WorkListsPost', 'object',
            {
                'lists': {'type': 'object'}
            }
        )

        self.addComponent(
            'schema', 'AdminServicesSchema', 'object',
            {
                '*_services': {'$ref': '#/components/schemas/ServicesDictSchema'},
                'protocols': {'$ref': '#/components/schemas/ProtocolDictSchema'}
            }
        )

        self.addComponent(
            'schema', 'AdminProtocolPost', 'object',
            {
                'protocol': {
                    'type': 'string',
                    'description': 'The name of a protocol to lookup'
                },
                'id': {'type': 'string'},
                'name': {'type': 'string'}
            }
        )

        self.addComponent(
            'schema', 'AdminDiscoveryPost', 'object',
            {
                'protocol': {
                    'type': 'string',
                    'description': 'The name of a protocol to lookup'
                },
                'id': {'type': 'string'},
                'name': {'type': 'string'},
                'url': {
                    'type': 'string',
                    'format': 'url',
                }
            }
        )

        self.addComponent(
            'schema', 'CollectionLibRegistrations', 'object',
            {
                'library_registrations': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'id': {'type': 'string'},
                            'library_info': {
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'short_name': {'type': 'string'},
                                        'status': {'type': 'string'}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'CreatedServicesGetSchema', 'object',
            {
                '*_services': {
                    'type': 'object',
                    'description': 'A list of available _ services'
                },
                'protocol': {'$ref': '#/components/schemas/ProtocolDictSchema'}
            }
        )

        self.addComponent(
            'schema', 'PatronDataSchema', 'object',
            {
                'permanent_id': {'type': 'string'},
                'authorization_identifier': {'type': 'string'},
                'username': {'type': 'string'},
                'personal_name': {'type': 'string'},
                'email_address': {'type': 'string'},
                'block_reason': {'type': 'string'},
                'external_type': {'type': 'string'},
            }
        )

        self.addComponent(
            'schema', 'EditWorkPostForm', 'object',
            {
                'title': {'type': 'string'},
                'subtitle': {'type': 'string'},
                'contributor-role': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'contributor-name': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'series': {'type': 'string'},
                'series_position': {'type': 'integer'},
                'medium': {'type': 'string'},
                'language': {'type': 'string'},
                'publisher': {'type': 'string'},
                'imprint': {'type': 'string'},
                'issued': {
                    'type': 'string',
                    'format': 'date'
                },
                'rating': {'type': 'integer'},
                'summary': {'type': 'string'}
            }
        )

        self.addComponent(
            'schema', 'ChangeBookCoverForm', 'object',
            {
                'rights_status': {'type': 'string'},
                'rights_explanation': {'type': 'string'},
                'cover_file': {
                    'type': 'string',
                    'format': 'binary'
                },
                'cover_url': {'type': 'string'},
                'title_position': {'type': 'string'}
            }
        )

        self.addComponent(
            'schema', 'GenrePropsSchema', 'object',
            {
                'name': {'type': 'string'},
                'parents': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'subgenres': {
                    'type': 'array',
                    'items': {'type': 'string'}
                }
            }
        )

        self.addComponent(
            'schema', 'GenresSchema', 'object',
            {
                'Fiction': {'$ref': '#/components/schemas/GenrePropsSchema'},
                'Nonfiction': {'$ref': '#/components/schemas/GenrePropsSchema'}
            }
        )

        self.addComponent(
            'schema', 'CollectionsGetSchema', 'object',
            {
                'collections': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'id': {'type': 'string'},
                            'name': {'type': 'string'},
                            'protocol': {
                                'type': 'string',
                                'description': 'A protocol name string'
                            },
                            'parent_id': {'type': 'string'},
                            'settings': {'type': 'object'},
                            'libraries': {
                                'type': 'array',
                                'items': {'type': 'object'}
                            }
                        }
                    }
                },
                'protocols': {
                    'type': 'object',
                    'properties': {
                        'name': {'type': 'string'},
                        'label': {'type': 'string'},
                        'description': {'type': 'string'},
                        'settings': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'description': 'Mirror integration settings'
                            }
                        }
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'CollectionsLibraryRegistrationPost', 'object',
            {
                'collection_id': {'type': 'string'},
                'library_short_name': {'type': 'string'}
            }
        )

        self.addComponent(
            'schema', 'ServicesSchema', 'array',
            {
                'items': {
                    'type': 'object',
                    'properties': {
                            'libraries': {
                                'type': 'array',
                                'items': {'type': 'string'}
                            },
                        'id': {'type': 'string'},
                        'protocol': {'type': 'string'},
                        'settings': {'type': 'object'},
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'ServicesGetSchema', 'object',
            {
                '*_services': {'$ref': '#/components/schemas/ServicesSchema'
                               },
                'protocols': {'$ref': '#/components/schemas/ProtocolDictSchema'}
            }
        )

        self.addComponent(
            'schema', 'ServicesPostForm', 'object',
            {
                'protocol': {
                    'type': 'string',
                    'description': 'The name of a protocol to lookup'
                },
                'id': {'type': 'string'},
                'name': {'type': 'string'},
                'url': {'type': 'string'}
            }
        )

        self.addComponent(
            'schema', 'DiscoverServiceRegPost', 'object',
            {
                'integration_id': {'type': 'string'},
                'library_short_name': {'type': 'string'},
                'registration_stage': {'type': 'string'}
            }
        )

        self.addComponent(
            'schema', 'DiscoveryServiceGet', 'object',
            {
                'library_registrations': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'id': {
                                'type': 'string',
                                'description': 'Redistration integration id.'
                            },
                            'access_problem': {
                                'type': 'object',
                                'nullable': 'True'
                            },
                            'tems_of_service_link': {
                                'type': 'string',
                                'format': 'url'
                            },
                            'term_of_service_html': {
                                'type': 'string',
                                'format': 'html'
                            },
                            'libraries': {
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'library_short_name': {'type': 'string'},
                                        'status': {'type': 'string'},
                                        'stage': {'type': 'string'}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'WorkComplaintsSchema', 'object',
            {
                'book': {
                    'type': 'object',
                    'properties': {
                        'identifier_type': {'type': 'string'},
                        'identifier': {'type': 'string'}
                    }
                },
                'complaints': {
                    'type': 'object',
                    'description': 'A counter object of complaint types for a given work.  Types: wrong-genre, wrong-audience, wrong-age-range, wrong-title, wrong-medium, wrong-author, bad-cover-image, bad-description, cannot-fulfill-loan, cannot-issue-loan, cannot-render, cannot-return.',
                    'properties': {
                        'complaint_type': {'type': 'integer'}
                    }
                }
            }
        )

        self.addComponent(
            'schema', 'AdminViewPageSchema', 'object',
            {
                'admin_template': {
                    'type': 'object',
                            'description': 'HTML template for Admin View'
                },
                'csrf_token': {
                    'type': 'object',
                            'description': 'CSRF Token'
                },
                'sitewide_tos_href': {
                    'type': 'string',
                            'format': 'url'
                },
                'sitewide_tos_text': {'type': 'string'},
                'show_circ_events_download': {'type': 'boolean'},
                'setting_up': {'type': 'boolean'},
                'email': {
                    'type': 'string',
                            'format': 'email'
                },
                'roles': {
                    'type': 'object',
                            'description': 'All roles admin holds'
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
