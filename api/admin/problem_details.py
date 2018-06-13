from core.util.problem_detail import ProblemDetail as pd
from api.problem_details import *
from flask_babel import lazy_gettext as _

ADMIN_AUTH_NOT_CONFIGURED = pd(
    "http://librarysimplified.org/terms/problem/admin-auth-not-configured",
    500,
    _("Admin auth not configured"),
    _("This circulation manager has not been configured to authenticate admins."),
)

ADMIN_AUTH_MECHANISM_NOT_CONFIGURED = pd(
    "http://librarysimplified.org/terms/problem/admin-auth-mechanism-not-configured",
    400,
    _("Admin auth mechanism not configured"),
    _("This circulation manager has not been configured to authenticate admins with the mechanism you used"),
)

INVALID_ADMIN_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/admin-credentials-invalid",
      401,
      _("Invalid admin credentials"),
      _("Valid library staff credentials are required."),
)

ADMIN_NOT_AUTHORIZED = pd(
    "http://librarysimplified.org/terms/problem/admin-not-authorized",
    403,
    _("Admin not authorized"),
    _("Your admin account is not authorized to make this request."),
)

GOOGLE_OAUTH_FAILURE = pd(
      "http://librarysimplified.org/terms/problem/google-oauth-failure",
      400,
      _("Google OAuth Error"),
      _("There was an error connecting with Google OAuth."),
)

INVALID_CSRF_TOKEN = pd(
      "http://librarysimplified.org/terms/problem/invalid-csrf-token",
      400,
      _("Invalid CSRF token"),
      _("There was an error saving your changes."),
)

INVALID_EDIT = pd(
      "http://librarysimplified.org/terms/problem/invalid-edit",
      400,
      _("Invalid edit"),
      _("There was a problem with the edited metadata."),
)

METADATA_REFRESH_PENDING = pd(
      "http://librarysimplified.org/terms/problem/metadata-refresh-pending",
      201,
      _("Metadata refresh pending."),
      _("The Metadata Wrangler is looking for new data. Check back later."),
)

METADATA_REFRESH_FAILURE = pd(
      "http://librarysimplified.org/terms/problem/metadata-refresh-failure",
      400,
      _("Metadata could not be refreshed."),
      _("Metadata could not be refreshed."),
)

UNRECOGNIZED_COMPLAINT = pd(
    "http://librarysimplified.org/terms/problem/unrecognized-complaint",
    status_code=404,
    title=_("Unrecognized complaint."),
    detail=_("The complaint you are attempting to resolve could not be found."),
)

COMPLAINT_ALREADY_RESOLVED = pd(
    "http://librarysimplified.org/terms/problem/complaint-already-resolved",
    status_code=409,
    title=_("Complaint already resolved."),
    detail=_("You can't resolve a complaint that is already resolved."),
)

GENRE_NOT_FOUND = pd(
    "http://librarysimplified.org/terms/problem/genre-not-found",
    status_code=404,
    title=_("Genre not found."),
    detail=_("One of the submitted genres does not exist."),
)

INCOMPATIBLE_GENRE = pd(
    "http://librarysimplified.org/terms/problem/incompatible-genre",
    status_code=409,
    title=_("Incompatible genre."),
    detail=_("The genre is incompatible with the fiction status of the work."),
)

EROTICA_FOR_ADULTS_ONLY = pd(
    "http://librarysimplified.org/terms/problem/erotica-for-adults-only",
    status_code=409,
    title=_("Erotica is for Adults Only."),
    detail=_("The Erotica genre is incompatible with the submitted Audience."),
)

INVALID_SERIES_POSITION = pd(
    "http://librarysimplified.org/terms/problem/invalid-series-position",
    status_code=400,
    title=_("Invalid series position."),
    detail=_("The series position must be a number or blank."),
)

INVALID_DATE_FORMAT = pd(
    "http://librarysimplified.org/terms/problem/invalid-date-format",
    status_code=400,
    title=_("Invalid date format."),
    detail=_("A date must be in the format YYYY-MM-DD."),
)

UNKNOWN_LANGUAGE = pd(
    "http://librarysimplified.org/terms/problem/unknown-language",
    status_code=400,
    title=_("Unknown language."),
    detail=_("The submitted language is not one of the known languages."),
)

UNKNOWN_ROLE = pd(
    "http://librarysimplified.org/terms/problem/unknown-role",
    status_code=400,
    title=_("Unknown role."),
    detail=_("One of the submitted roles is not one of the known roles."),
)

UNKNOWN_MEDIUM = pd(
    "http://librarysimplified.org/terms/problem/unknown-medium",
    status_code=400,
    title=_("Unknown medium."),
    detail=_("The submitted medium is not one of the known media types."),
)

INVALID_RATING = pd(
    "http://librarysimplified.org/terms/problem/invalid-rating",
    status_code=400,
    title=_("Invalid rating."),
    detail=_("The rating must be a number in the rating scale."),
)

INVALID_IMAGE = pd(
    "http://librarysimplified.org/terms/problem/invalid-image",
    status_code=400,
    title=_("Invalid image"),
    detail=_("The submitted image is invalid."),
)

MISSING_LIBRARY_SHORT_NAME = pd(
    "http://librarysimplified.org/terms/problem/missing-library-short-name",
    status_code=400,
    title=_("Missing library short name"),
    detail=_("You must set a short name for the library."),
)

LIBRARY_SHORT_NAME_ALREADY_IN_USE = pd(
    "http://librarysimplified.org/terms/problem/library-short-name-already-in-use",
    status_code=400,
    title=_("Library short name already in use"),
    detail=_("The library short name must be unique, and there's already a library with the specified short name."),
)

MISSING_COLLECTION = pd(
    "http://librarysimplified.org/terms/problem/missing-collection",
    status_code=404,
    title=_("Missing collection."),
    detail=_("The specified collection does not exist."),
)

MISSING_COLLECTION_NAME = pd(
    "http://librarysimplified.org/terms/problem/missing-collection-name",
    status_code=400,
    title=_("Missing collection name."),
    detail=_("You must identify the collection by its name."),
)

COLLECTION_NAME_ALREADY_IN_USE = pd(
    "http://librarysimplified.org/terms/problem/collection-name-already-in-use",
    status_code=400,
    title=_("Collection name already in use"),
    detail=_("The collection name must be unique, and there's already a collection with the specified name."),
)

CANNOT_DELETE_COLLECTION_WITH_CHILDREN = pd(
    "http://librarysimplified.org/terms/problem/cannot-delete-collection-with-children",
    status_code=400,
    title=_("Cannot delete collection with children"),
    detail=_("The collection is the parent of at least one other collection, so it can't be deleted."),
)

NO_PROTOCOL_FOR_NEW_SERVICE = pd(
    "http://librarysimplified.org/terms/problem/no-protocol-for-new-service",
    status_code=400,
    title=_("No protocol for new service"),
    detail=_("The specified service doesn't exist. You can create it, but you must specify a protocol."),
)

UNKNOWN_PROTOCOL = pd(
    "http://librarysimplified.org/terms/problem/unknown-protocol",
    status_code=400,
    title=_("Unknown protocol"),
    detail=_("The protocol is not one of the known protocols."),
)

CANNOT_CHANGE_PROTOCOL = pd(
    "http://librarysimplified.org/terms/problem/cannot-change-protocol",
    status_code=400,
    title=_("Cannot change protocol"),
    detail=_("A protocol can't be changed once it has been set."),
)

PROTOCOL_DOES_NOT_SUPPORT_PARENTS = pd(
    "http://librarysimplified.org/terms/problem/protocol-does-not-support-parents",
    status_code=400,
    title=_("Protocol does not support parents"),
    detail=_("You attempted to add a parent but the protocol does not support parents."),
)

MISSING_PARENT = pd(
    "http://librarysimplified.org/terms/problem/missing-parent",
    status_code=400,
    title=_("Missing parent"),
    detail=_("You attempted to add a parent that does not exist."),
)

NO_SUCH_LIBRARY = pd(
    "http://librarysimplified.org/terms/problem/no-such-library",
    status_code=400,
    title=_("No such library"),
    detail=_("A library in your request does not exist."),
)

INCOMPLETE_CONFIGURATION = pd(
    "http://librarysimplified.org/terms/problem/incomplete-configuration",
    status_code=400,
    title=_("Incomplete configuration"),
    detail=_("The configuration is missing a required field."),
)

DUPLICATE_INTEGRATION = pd(
    "http://librarysimplified.org/terms/problem/duplicate-integration",
    status_code=400,
    title=_("Duplicate integration"),
    detail=_("A given site can only support one integration of this type.")
)

INTEGRATION_NAME_ALREADY_IN_USE = pd(
    "http://librarysimplified.org/terms/problem/integration-name-already-in-use",
    status_code=400,
    title=_("Integration name already in use"),
    detail=_("The integration name must be unique, and there's already an integration with the specified name."),
)

INTEGRATION_GOAL_CONFLICT = pd(
    "http://librarysimplified.org/terms/problem/integration-goal-conflict",
    status_code=409,
    title=_("Incompatible use of integration"),
    detail=_("You tried to use an integration in a way incompatible with the goal of that integration"),
)

MISSING_PGCRYPTO_EXTENSION = pd(
    "http://librarysimplified.org/terms/problem/missing-pgcrypto-extension",
    status_code=500,
    title=_("Missing pgcrypto database extension"),
    detail=_("You tried to store a password for an individual admin, but the database does not have the pgcrypto extension installed."),
)

SHARED_SECRET_DECRYPTION_ERROR = pd(
    "http://librarysimplified.org/terms/problem/decryption-error",
    status_code=502,
    title=_("Decryption error"),
    detail=_("Failed to decrypt a shared secret retrieved from another computer.")
)

MISSING_ADMIN = pd(
    "http://librarysimplified.org/terms/problem/missing-admin",
    status_code=404,
    title=_("Missing admin"),
    detail=_("The specified admin does not exist."),
)

MISSING_SERVICE = pd(
    "http://librarysimplified.org/terms/problem/missing-service",
    status_code=404,
    title=_("Missing service"),
    detail=_("The specified service does not exist."),
)

INVALID_CONFIGURATION_OPTION = pd(
    "http://librarysimplified.org/terms/problem/invalid-configuration-option",
    status_code=400,
    title=_("Invalid configuration option"),
    detail=_("The configuration has an invalid value."),
)

INVALID_EXTERNAL_TYPE_REGULAR_EXPRESSION = pd(
    "http://librarysimplified.org/terms/problem/invalid-external-type-regular-expression",
    status_code=400,
    title=_("Invalid external type regular expression"),
    detail=_("The specified external type regular expression does not compile."),
)

INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION = pd(
    "http://librarysimplified.org/terms/problem/invalid-library-identifier-restriction-regular-expression",
    status_code=400,
    title=_("Invalid library identifier restriction regular expression"),
    detail=_("The specified library identifier restriction regular expression does not compile."),
)

MULTIPLE_BASIC_AUTH_SERVICES = pd(
    "http://librarysimplified.org/terms/problem/multiple-basic-auth-services",
    status_code=400,
    title=_("Multiple basic authentication services"),
    detail=_("Each library can only have one patron authentication service using basic auth."),
)

MISSING_SITEWIDE_SETTING_KEY = pd(
    "http://librarysimplified.org/terms/problem/missing-sitewide-setting-key",
    status_code=400,
    title=_("Missing sitewide setting key"),
    detail=_("A key is required to change a sitewide setting."),
)

MISSING_SITEWIDE_SETTING_VALUE = pd(
    "http://librarysimplified.org/terms/problem/missing-sitewide-setting-value",
    status_code=400,
    title=_("Missing sitewide setting value"),
    detail=_("A value is required to change a sitewide setting."),
)

MULTIPLE_SITEWIDE_SERVICES = pd(
    "http://librarysimplified.org/terms/problem/multiple-search-services",
    status_code=400,
    title=_("Multiple sitewide services"),
    detail=_("You tried to create a new sitewide service, but a sitewide service of the same type is already configured."),
)

MISSING_CUSTOM_LIST = pd(
    "http://librarysimplified.org/terms/problem/missing-custom-list",
    status_code=404,
    title=_("Missing custom list"),
    detail=_("The specified custom list doesn't exist."),
)

CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST = pd(
    "http://librarysimplified.org/terms/problem/cannot-change-library-for-custom-list",
    status_code=400,
    title=_("Cannot change library for custom list"),
    detail=_("A custom list's associated library cannot be changed once it is set.."),
)

CUSTOM_LIST_NAME_ALREADY_IN_USE = pd(
    "http://librarysimplified.org/terms/problem/custom-list-name-already-in-use",
    status_code=400,
    title=_("Custom list name already in use"),
    detail=_("The library already has a custom list with that name."),
)

COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY = pd(
    "http://librarysimplified.org/terms/problem/collection-not-associated-with-library",
    status_code=400,
    title=_("Collection not associated with library"),
    detail=_("You can't add a collection to a list unless it is associated with the list's library."),
)

MISSING_LANE = pd(
    "http://librarysimplified.org/terms/problem/missing-lane",
    status_code=404,
    title=_("Missing lane"),
    detail=_("The specified lane doesn't exist, or is associated with a different library."),
)

CANNOT_EDIT_DEFAULT_LANE = pd(
    "http://librarysimplified.org/terms/problem/cannot-edit-default-lane",
    status_code=400,
    title=_("Cannot edit default lane"),
    detail=_("You can't change one of the default auto-generated lanes."),
)

NO_DISPLAY_NAME_FOR_LANE = pd(
    "http://librarysimplified.org/terms/problem/no-display-name-for-lane",
    status_code=400,
    title=_("No display name for lane"),
    detail=_("A custom lane must have a name."),
)

NO_CUSTOM_LISTS_FOR_LANE = pd(
    "http://librarysimplified.org/terms/problem/no-custom-lists-for-lane",
    status_code=400,
    title=_("No custom lists for lane"),
    detail=_("A custom lane must have at least one associated list."),
)    

LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS = pd(
    "http://librarysimplified.org/terms/problem/lane-with-parent-and-display-name-already-exists",
    status_code=400,
    title=_("Lane with parent and display name already exists"),
    detail=_("You cannot create a lane with the same parent and display name as an existing lane."),
)    

CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT = pd(
    "http://librarysimplified.org/terms/problem/cannot-show-lane-with-hidden-parent",
    status_code=400,
    title=_("Cannot show lane with hidden parent"),
    detail=_("You can only make a lane visible if its parent is already visible."),
)

COLLECTION_DOES_NOT_SUPPORT_REGISTRATION = pd(
    "http://librarysimplified.org/terms/problem/collection-does-not-support-registration",
    status_code=400,
    title=_("The collection does not support registration"),
    detail=_("The collection does not support registration."),
)
