from core.util.problem_detail import ProblemDetail as pd
from api.problem_details import *
from flask.ext.babel import lazy_gettext as _

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
    title=_("Invalid series positon."),
    detail=_("The series position must be a number or blank."),
)

LIBRARY_NOT_FOUND = pd(
    "http://librarysimplified.org/terms/problem/library-not-found",
    status_code=400,
    title=_("Library not found."),
    detail=_("Currently there can only be one library, and the request does not match the existing library."),
)

CANNOT_SET_BOTH_RANDOM_AND_SPECIFIC_SECRET = pd(
    "http://librarysimplified.org/terms/problem/cannot-set-both-random-and-specific-secret",
    status_code=400,
    title=_("Cannot set both random and specific secret"),
    detail=_("You can't set the shared secret to a random value and a specific value at the same time"),
)

CANNOT_REPLACE_EXISTING_SECRET_WITH_RANDOM_SECRET = pd(
    "http://librarysimplified.org/terms/problem/cannot-replace-existing-secret-with-random-secret",
    status_code=400,
    title=_("Cannot replace existing secret with random secret"),
    detail=_("You can't overwrite an existing shared secret with a random value"),
)

MISSING_COLLECTION_NAME = pd(
    "http://librarysimplified.org/terms/problem/missing-collection-name",
    status_code=400,
    title=_("Missing collection name."),
    detail=_("You must identify the collection by its name."),
)

NO_PROTOCOL_FOR_NEW_COLLECTION = pd(
    "http://librarysimplified.org/terms/problem/no-protocol-for-new-collection",
    status_code=400,
    title=_("No protocol for new collection"),
    detail=_("The specified collection doesn't exist. You can create it, but you must specify a protocol."),
)

UNKNOWN_COLLECTION_PROTOCOL = pd(
    "http://librarysimplified.org/terms/problem/unknown-collection-protocol",
    status_code=400,
    title=_("Unknown collection protocol"),
    detail=_("The protocol is not one of the known protocols."),
)

CANNOT_CHANGE_COLLECTION_PROTOCOL = pd(
    "http://librarysimplified.org/terms/problem/cannot-change-collection-protocol",
    status_code=400,
    title=_("Cannot change collection protocol"),
    detail=_("A collection's protocol can't be changed once it has been set."),
)

NO_SUCH_LIBRARY = pd(
    "http://librarysimplified.org/terms/problem/no-such-library",
    status_code=400,
    title=_("No such library"),
    detail=_("One of the libraries you attempted to add the collection to does not exist."),
)

INCOMPLETE_COLLECTION_CONFIGURATION = pd(
    "http://librarysimplified.org/terms/problem/incomplete-collection-configuration",
    status_code=400,
    title=_("Incomplete collection configuration"),
    detail=_("The collection's configuration is missing a required field."),
)

MISSING_ADMIN_AUTH_SERVICE_NAME = pd(
    "http://librarysimplified.org/terms/problem/missing-admin-auth-service-name",
    status_code=400,
    title=_("Missing admin authentication service name."),
    detail=_("You must identify the admin authentication service by its name."),
)

UNKNOWN_ADMIN_AUTH_SERVICE_PROVIDER = pd(
    "http://librarysimplified.org/terms/problem/unknown-admin-auth-service-provider",
    status_code=400,
    title=_("Unknown admin authentication service provider"),
    detail=_("The provider is not one of the known admin authentication service providers."),
)

ADMIN_AUTH_SERVICE_NOT_FOUND = pd(
    "http://librarysimplified.org/terms/problem/admin-auth-service-not-found",
    status_code=400,
    title=_("Admin authentication service not found."),
    detail=_("Currently there can only be one admin authentication service, and the request does not match the existing one."),
)

NO_PROVIDER_FOR_NEW_ADMIN_AUTH_SERVICE = pd(
    "http://librarysimplified.org/terms/problem/no-provider-for-new-admin-auth-service",
    status_code=400,
    title=_("No provider for new admin authentication service"),
    detail=_("The specified admin authentication service doesn't exist. You can create it, but you must specify a provider."),
)

CANNOT_CHANGE_ADMIN_AUTH_SERVICE_PROVIDER = pd(
    "http://librarysimplified.org/terms/problem/cannot-change-admin-auth-service-provider",
    status_code=400,
    title=_("Cannot change admin authentication service provider"),
    detail=_("An admin authentication service's provider can't be changed once it has been set."),
)

INCOMPLETE_ADMIN_AUTH_SERVICE_CONFIGURATION = pd(
    "http://librarysimplified.org/terms/problem/incomplete-admin-auth-service-configuration",
    status_code=400,
    title=_("Incomplete admin authentication service configuration"),
    detail=_("The admin authentication service's configuration is missing a required field."),
)

INVALID_ADMIN_AUTH_DOMAIN_LIST = pd(
    "http://librarysimplified.org/terms/problem/invalid-admin-auth-domain-list",
    status_code=400,
    title=_("Invalid admin authentication domain list"),
    detail=_("The admin authentication domain list isn't in a valid format."),
)

INVALID_ADMIN_AUTH_ADMINS_LIST = pd(
    "http://librarysimplified.org/terms/problem/invalid-admin-auth-admins-list",
    status_code=400,
    title=_("Invalid admin authentication list of admins"),
    detail=_("The admin authentication list of admins isn't in a valid format."),
)
