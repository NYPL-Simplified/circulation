from core.util.problem_detail import ProblemDetail as pd
from core.problem_details import *
from flask_babel import lazy_gettext as _

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

BLOCKED_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/credentials-suspended",
      403,
      _("Suspended credentials."),
      _("Your library card has been suspended. Contact your branch library."),
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

GENERIC_LOAN_LIMIT_MESSAGE = _("You have reached your loan limit. You cannot borrow anything further until you return something.")
SPECIFIC_LOAN_LIMIT_MESSAGE = _("You have reached your loan limit of %(limit)d. You cannot borrow anything further until you return something.")
LOAN_LIMIT_REACHED = pd(
      "http://librarysimplified.org/terms/problem/loan-limit-reached",
      403,
      _("Loan limit reached."),
      GENERIC_LOAN_LIMIT_MESSAGE
)

GENERIC_HOLD_LIMIT_MESSAGE = _("You have reached your hold limit. You cannot place another item on hold until you borrow something or remove a hold.")
SPECIFIC_HOLD_LIMIT_MESSAGE = _("You have reached your hold limit of %(limit)d. You cannot place another item on hold until you borrow something or remove a hold.")
HOLD_LIMIT_REACHED = pd(
      "http://librarysimplified.org/terms/problem/hold-limit-reached",
      403,
      _("Limit reached."),
      GENERIC_HOLD_LIMIT_MESSAGE
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

LOAN_NOT_FOUND = pd(
      "http://librarysimplified.org/terms/problem/loan-not-found",
      404,
      _("Loan not found."),
      _("You don't have a loan with the provided id."),
)

HOLD_NOT_FOUND = pd(
      "http://librarysimplified.org/terms/problem/hold-not-found",
      404,
      _("Hold not found."),
      _("You don't have a hold with the provided id."),
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

NO_SUCH_LIST = pd(
      "http://librarysimplified.org/terms/problem/unknown-list",
      404,
      _("No such list."),
      _("You asked for a nonexistent list."),
)

NO_SUCH_COLLECTION = pd(
      "http://librarysimplified.org/terms/problem/unknown-collection",
      404,
      _("No such collection."),
      _("You asked for a nonexistent collection."),
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

INVALID_OAUTH_CALLBACK_PARAMETERS = pd(
    "http://librarysimplified.org/terms/problem/invalid-oauth-callback-parameters",
    status_code=400,
    title=_("Invalid OAuth callback parameters."),
    detail=_("The OAuth callback must contain a code and a state parameter with the OAuth provider name."),
)

UNKNOWN_OAUTH_PROVIDER = pd(
    "http://librarysimplified.org/terms/problem/unknown-oauth-provider",
    status_code=400,
    title=_("Unknown OAuth provider."),
    detail=_("The specified OAuth provider name isn't one of the known providers."),
)

INVALID_OAUTH_BEARER_TOKEN = pd(
    "http://librarysimplified.org/terms/problem/credentials-invalid",
    status_code=400,
    title=_("Invalid OAuth bearer token."),
    detail=_("The provided OAuth bearer token couldn't be verified."),
)

UNKNOWN_SAML_PROVIDER = pd(
    "http://librarysimplified.org/terms/problem/unknown-saml-provider",
    status_code=400,
    title=_("Unknown SAML provider."),
    detail=_("The specified SAML provider name isn't one of the known providers."),
)

INVALID_SAML_BEARER_TOKEN = pd(
    "http://librarysimplified.org/terms/problem/credentials-invalid",
    status_code=400,
    title=_("Invalid SAML bearer token."),
    detail=_("The provided SAML bearer token couldn't be verified."),
)

UNSUPPORTED_AUTHENTICATION_MECHANISM = pd(
    "http://librarysimplified.org/terms/problem/unsupported-authentication-mechanism",
    status_code=400,
    title=_("Unsupported authentication mechanism."),
    detail=_("The specified authentication mechanism isn't supported."),
)

INVALID_ANALYTICS_EVENT_TYPE = pd(
    "http://librarysimplified.org/terms/problem/invalid-analytics-event-type",
    status_code=400,
    title=_("Invalid analytics event type."),
    detail=_("The analytics event must be a supported type."),
)

PATRON_NOT_OPTED_IN_TO_ANNOTATION_SYNC = pd(
    "http://librarysimplified.org/terms/problem/opt-in-required",
    status_code=403,
    title=_("Patron must opt in."),
    detail=_("The patron must opt in to synchronize annotations to a server."),
)

INVALID_ANNOTATION_MOTIVATION = pd(
    "http://librarysimplified.org/terms/problem/invalid-annotation-motivation",
    status_code=400,
    title=_("Invalid annotation motivation."),
    detail=_("The annotation must have a supported motivation."),
)

INVALID_ANNOTATION_TARGET = pd(
    "http://librarysimplified.org/terms/problem/invalid-annotation-target",
    status_code=400,
    title=_("Invalid annotation target."),
    detail=_("The annotation target must be a work in your current loans."),
)

INVALID_ANNOTATION_FORMAT = pd(
    "http://librarysimplified.org/terms/problem/invalid-annotation-format",
    status_code=400,
    title=_("Invalid annotation format."),
    detail=_("The annotation could not be parsed as JSON-LD."),
)

NO_ANNOTATION = pd(
    "http://librarysimplified.org/terms/problem/no-annotation",
    status_code=404,
    title=_("No annotation."),
    detail=_("The annotation you requested does not exist."),
)

LIBRARY_NOT_FOUND = pd(
    "http://librarysimplified.org/terms/problem/library-not-found",
    status_code=404,
    title=_("Library not found."),
    detail=_("No library with the requested name on this server."),
)

PATRON_OF_ANOTHER_LIBRARY = pd(
    "http://librarysimplified.org/terms/problem/patron-of-another-library",
    status_code=404,
    title=_("Wrong library"),
    detail=_("You are not a patron of the selected library."),
)

INVALID_LOAN_FOR_ODL_NOTIFICATION = pd(
    "http://librarysimplified.org/terms/problem/invalid-loan-for-odl-notification",
    status_code=400,
    title=_("Invalid loan for ODL notification"),
    detail=_("The ODL notification is for a loan that's not from an ODL collection."),
)

INVALID_REGISTRATION = pd(
    "http://librarysimplified.org/terms/problem/cannot-register",
    status_code=400,
    title=_("Invalid registration"),
    detail=_("You did not submit enough information to register with the collection."),
)

SHARED_SECRET_DECRYPTION_ERROR = pd(
    "http://librarysimplified.org/terms/problem/decryption-error",
    status_code=502,
    title=_("Decryption error"),
    detail=_("Failed to decrypt a shared secret retrieved from another computer.")
)
