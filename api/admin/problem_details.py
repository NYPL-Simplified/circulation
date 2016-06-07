from core.util.problem_detail import ProblemDetail as pd
from api.problem_details import *
from flask.ext.babel import lazy_gettext as _

INVALID_ADMIN_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/admin-credentials-invalid",
      401,
      _("Invalid admin credentials"),
      _("A valid library staff email is required."),
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
