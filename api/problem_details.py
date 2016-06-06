from core.util.problem_detail import ProblemDetail as pd
from core.problem_details import *
from flask.ext.babel import lazy_gettext as _

REMOTE_INTEGRATION_FAILED = pd(
      "http://librarysimplified.org/terms/problem/remote-integration-failed",
      502,
      _("Third-party service failed."),
      _("The library could not complete your request because a third-party service has failed."),
)

CANNOT_GENERATE_FEED = pd(
      "http://librarysimplified.org/terms/problem/cannot-generate-feed",
      500,
      _("Feed should be been pre-cached."),
      _("This feed should have been pre-cached. It's too expensive to generate dynamically."),
)

INVALID_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/credentials-invalid",
      401,
      _("Invalid credentials"),
      _("A valid library card barcode number and PIN are required."),
)

EXPIRED_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/credentials-expired",
      403,
      _("Expired credentials."),
      _("Your library card has expired. You need to renew it."),
)

NO_LICENSES = pd(
      "http://librarysimplified.org/terms/problem/no-licenses",
      404,
      _("No licenses."),
      _("The library currently has no licenses for this book."),
)

NO_AVAILABLE_LICENSE = pd(
      "http://librarysimplified.org/terms/problem/no-available-license",
      403,
      _("No available license."),
      _("All licenses for this book are loaned out."),
)

NO_ACCEPTABLE_FORMAT = pd(
      "http://librarysimplified.org/terms/problem/no-acceptable-format",
      400,
      _("No acceptable format."),
      _("Could not deliver this book in an acceptable format."),
)

ALREADY_CHECKED_OUT = pd(
      "http://librarysimplified.org/terms/problem/loan-already-exists",
      400,
      _("Already checked out"),
      _("You have already checked out this book."),
)

LOAN_LIMIT_REACHED = pd(
      "http://librarysimplified.org/terms/problem/loan-limit-reached",
      403,
      _("Loan limit reached."),
      _("You have reached your loan limit. You cannot borrow anything further until you return something."),
)

OUTSTANDING_FINES = pd(
      "http://librarysimplified.org/terms/problem/outstanding-fines",
      403,
      _("Outstanding fines."),
      _("You must pay your outstanding fines before you can borrow more books."),
    )

CHECKOUT_FAILED = pd(
      "http://librarysimplified.org/terms/problem/cannot-issue-loan",
      502,
      _("Could not issue loan."),
      _("Could not issue loan (reason unknown)."),
)

HOLD_FAILED = pd(
      "http://librarysimplified.org/terms/problem/cannot-place-hold",
      502,
      _("Could not place hold."),
      _("Could not place hold (reason unknown)."),
)

RENEW_FAILED = pd(
      "http://librarysimplified.org/terms/problem/cannot-renew-loan",
      400,
      _("Could not renew loan."),
      _("Could not renew loan (reason unknown)."),
)

NOT_FOUND_ON_REMOTE = pd(
      "http://librarysimplified.org/terms/problem/not-found-on-remote",
      404,
      _("No longer in collection."),
      _("This book was recently removed from the collection."),
)

NO_ACTIVE_LOAN = pd(
      "http://librarysimplified.org/terms/problem/no-active-loan",
      400,
      _("No active loan."),
      _("You can't do this without first borrowing this book."),
)

NO_ACTIVE_HOLD = pd(
      "http://librarysimplified.org/terms/problem/no-active-hold",
      400,
      _("No active hold."),
      _("You can't do this without first putting this book on hold."),
)

NO_ACTIVE_LOAN_OR_HOLD = pd(
      "http://librarysimplified.org/terms/problem/no-active-loan",
      400,
      _("No active loan or hold."),
      _("You can't do this without first borrowing this book or putting it on hold."),
)

COULD_NOT_MIRROR_TO_REMOTE = pd(
      "http://librarysimplified.org/terms/problem/cannot-mirror-to-remote",
      502,
      _("Could not mirror local state to remote."),
      _("Could not convince a third party to accept the change you made. It's likely to show up again soon."),
)

NO_SUCH_LANE = pd(
      "http://librarysimplified.org/terms/problem/unknown-lane",
      404,
      _("No such lane."),
      _("You asked for a nonexistent lane."),
)

FORBIDDEN_BY_POLICY = pd(
      "http://librarysimplified.org/terms/problem/forbidden-by-policy",
      403,
      _("Forbidden by policy."),
      _("Library policy prevents us from carrying out your request."),
)

CANNOT_FULFILL = pd(
      "http://librarysimplified.org/terms/problem/cannot-fulfill-loan",
      400,
      _("Could not fulfill loan."),
      _("Could not fulfill loan."),
)

DELIVERY_CONFLICT = pd(
      "http://librarysimplified.org/terms/problem/delivery-mechanism-conflict",
      409,
      _("Delivery mechanism conflict."),
      _("The delivery mechanism for this book has been locked in and can't be changed."),
)

BAD_DELIVERY_MECHANISM = pd(
      "http://librarysimplified.org/terms/problem/bad-delivery-mechanism",
      400,
      _("Unsupported delivery mechanism."),
      _("You selected a delivery mechanism that's not supported by this book."),
)

CANNOT_RELEASE_HOLD = pd(
    "http://librarysimplified.org/terms/problem/cannot-release-hold",
    400,
    _("Could not release hold."),
    _("Could not release hold."),
)

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

INVALID_ANALYTICS_EVENT_TYPE = pd(
    "http://librarysimplified.org/terms/problem/invalid-analytics-event-type",
    status_code=400,
    title=_("Invalid analytics event type."),
    detail=_("The analytics event must be a supported type."),
)
