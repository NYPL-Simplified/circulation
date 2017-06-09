class FacetConstants(object):

    # Subset the collection, roughly, by quality.
    COLLECTION_FACET_GROUP_NAME = 'collection'
    COLLECTION_FULL = "full"
    COLLECTION_MAIN = "main"
    COLLECTION_FEATURED = "featured"

    # Subset the collection by availability.
    AVAILABILITY_FACET_GROUP_NAME = 'available'
    AVAILABLE_NOW = "now"
    AVAILABLE_ALL = "all"
    AVAILABLE_OPEN_ACCESS = "always"

    # The names of the order facets.
    ORDER_FACET_GROUP_NAME = 'order'
    ORDER_TITLE = 'title'
    ORDER_AUTHOR = 'author'
    ORDER_LAST_UPDATE = 'last_update'
    ORDER_ADDED_TO_COLLECTION = 'added'
    ORDER_SERIES_POSITION = 'series'
    ORDER_WORK_ID = 'work_id'
    ORDER_RANDOM = 'random'

    ORDER_ASCENDING = "asc"
    ORDER_DESCENDING = "desc"

    GROUP_DISPLAY_TITLES = {
        ORDER_FACET_GROUP_NAME : "Sort by",
        AVAILABILITY_FACET_GROUP_NAME : "Availability",
        COLLECTION_FACET_GROUP_NAME : 'Collection',
    }

    FACET_DISPLAY_TITLES = {
        ORDER_TITLE : 'Title',
        ORDER_AUTHOR : 'Author',
        ORDER_LAST_UPDATE : 'Last Update',
        ORDER_ADDED_TO_COLLECTION : 'Recently Added',
        ORDER_SERIES_POSITION: 'Series Position',
        ORDER_WORK_ID : 'Work ID',
        ORDER_RANDOM : 'Random',

        AVAILABLE_NOW : "Available now",
        AVAILABLE_ALL : "All",
        AVAILABLE_OPEN_ACCESS : "Yours to keep",

        COLLECTION_FULL : "Everything",
        COLLECTION_MAIN : "Main Collection",
        COLLECTION_FEATURED : "Popular Books",
    }

    # Unless a library offers an alternate configuration, patrons will
    # see these facet groups.
    DEFAULT_ENABLED_FACETS = {
        ORDER_FACET_GROUP_NAME : [
            ORDER_AUTHOR, ORDER_TITLE, ORDER_ADDED_TO_COLLECTION
        ],
        AVAILABILITY_FACET_GROUP_NAME : [
            AVAILABLE_ALL, AVAILABLE_NOW, AVAILABLE_OPEN_ACCESS
        ],
        COLLECTION_FACET_GROUP_NAME : [
            COLLECTION_FULL, COLLECTION_MAIN, COLLECTION_FEATURED
        ]
    }

    # Unless a library offers an alternate configuration, these
    # facets will be the default selection for the facet groups.
    DEFAULT_FACET = {
        ORDER_FACET_GROUP_NAME : ORDER_AUTHOR,
        AVAILABILITY_FACET_GROUP_NAME : AVAILABLE_ALL,
        COLLECTION_FACET_GROUP_NAME : COLLECTION_MAIN,
    }
