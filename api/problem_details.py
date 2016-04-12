from core.util.problem_detail import ProblemDetail as pd
from core.problem_details import *

REMOTE_INTEGRATION_FAILED = pd(
      "http://librarysimplified.org/terms/problem/remote-integration-failed",
      500,
      "Third-party service failed.",
      "The library could not complete your request because a third-party service has failed.",
)

CANNOT_GENERATE_FEED = pd(
      "http://librarysimplified.org/terms/problem/cannot-generate-feed",
      500,
      "Feed should be been pre-cached.",
      "This feed should have been pre-cached. It's too expensive to generate dynamically.",
)

INVALID_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/credentials-invalid",
      401,
      "Invalid credentials",
      "A valid library card barcode number and PIN are required.",
)

EXPIRED_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/credentials-expired",
      403,
      "Expired credentials.",
      "Your library card has expired. You need to renew it.",
)

NO_LICENSES = pd(
      "http://librarysimplified.org/terms/problem/no-licenses",
      403,
      "No licenses.",
      "The library currently has no licenses for this book.",
)

NO_AVAILABLE_LICENSE = pd(
      "http://librarysimplified.org/terms/problem/no-available-license",
      403,
      "No available license.",
      "All licenses for this book are loaned out.",
)

NO_ACCEPTABLE_FORMAT = pd(
      "http://librarysimplified.org/terms/problem/no-acceptable-format",
      400,
      "No acceptable format.",
      "Could not deliver this book in an acceptable format.",
)

ALREADY_CHECKED_OUT = pd(
      "http://librarysimplified.org/terms/problem/loan-already-exists",
      400,
      "Already checked out",
      "You have already checked out this book.",
)

LOAN_LIMIT_REACHED = pd(
      "http://librarysimplified.org/terms/problem/loan-limit-reached",
      403,
      "Loan limit reached.",
      "You have reached your loan limit. You cannot borrow anything further until you return something.",
)

OUTSTANDING_FINES = pd(
      "http://librarysimplified.org/terms/problem/outstanding-fines",
      403,
      "Outstanding fines.",
      "You must pay your outstanding fines before you can borrow more books.",    
    )

CHECKOUT_FAILED = pd(
      "http://librarysimplified.org/terms/problem/cannot-issue-loan",
      500,
      "Could not issue loan.",
      "Could not issue loan (reason unknown).",
)

HOLD_FAILED = pd(
      "http://librarysimplified.org/terms/problem/cannot-place-hold",
      500,
      "Could not place hold.",
      "Could not place hold (reason unknown).",
)

RENEW_FAILED = pd(
      "http://librarysimplified.org/terms/problem/cannot-renew-loan",
      500,
      "Could not renew loan.",
      "Could not renew loan (reason unknown).",
)

NOT_FOUND_ON_REMOTE = pd(
      "http://librarysimplified.org/terms/problem/not-found-on-remote",
      404,
      "No longer in collection.",
      "This book was recently removed from the collection.",
)

NO_ACTIVE_LOAN = pd(
      "http://librarysimplified.org/terms/problem/no-active-loan",
      400,
      "No active loan.",
      "You can't do this without first borrowing this book.",
)

NO_ACTIVE_HOLD = pd(
      "http://librarysimplified.org/terms/problem/no-active-hold",
      400,
      "No active hold.",
      "You can't do this without first putting this book on hold.",
)

NO_ACTIVE_LOAN_OR_HOLD = pd(
      "http://librarysimplified.org/terms/problem/no-active-loan",
      400,
      "No active loan or hold.",
      "You can't do this without first borrowing this book or putting it on hold.",
)

COULD_NOT_MIRROR_TO_REMOTE = pd(
      "http://librarysimplified.org/terms/problem/cannot-mirror-to-remote",
      500,
      "Could not mirror local state to remote.",
      "Could not convince a third party to accept the change you made. It's likely to show up again soon.",
)

NO_SUCH_LANE = pd(
      "http://librarysimplified.org/terms/problem/unknown-lane",
      404,
      "No such lane.",
      "You asked for a nonexistent lane.",
)

FORBIDDEN_BY_POLICY = pd(
      "http://librarysimplified.org/terms/problem/forbidden-by-policy",
      403,
      "Forbidden by policy.",
      "Library policy prevents us from carrying out your request.",
)

CANNOT_FULFILL = pd(
      "http://librarysimplified.org/terms/problem/cannot-fulfill-loan",
      400,
      "Could not fulfill loan.",
      "Could not fulfill loan.",
)

DELIVERY_CONFLICT = pd(
      "http://librarysimplified.org/terms/problem/delivery-mechanism-conflict",
      409,
      "Delivery mechanism conflict.",
      "The delivery mechanism for this book has been locked in and can't be changed.",
)

BAD_DELIVERY_MECHANISM = pd(
      "http://librarysimplified.org/terms/problem/bad-delivery-mechanism",
      400,
      "Unsupported delivery mechanism.",
      "You selected a delivery mechanism that's not supported by this book.",
)

CANNOT_RELEASE_HOLD = pd(
    "http://librarysimplified.org/terms/problem/cannot-release-hold",
    400,
    "Could not release hold.",
    "Could not release hold."
)

INVALID_ADMIN_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/admin-credentials-invalid",
      401,
      "Invalid admin credentials",
      "A valid library staff email is required.",
)

GOOGLE_OAUTH_FAILURE = pd(
      "http://librarysimplified.org/terms/problem/google-oauth-failure",
      400,
      "Google OAuth Error",
      "There was an error connecting with Google OAuth.",
)

INVALID_CSRF_TOKEN = pd(
      "http://librarysimplified.org/terms/problem/invalid-csrf-token",
      400,
      "Invalid CSRF token",
      "There was an error saving your changes.",
)

METADATA_REFRESH_PENDING = pd(
      "http://librarysimplified.org/terms/problem/metadata-refresh-pending",
      201,
      "Metadata refresh pending.",
      "The Metadata Wrangler is looking for new data. Check back later.",
)

METADATA_REFRESH_FAILURE = pd(
      "http://librarysimplified.org/terms/problem/metadata-refresh-failure",
      400,
      "Metadata could not be refreshed.",
      "Metadata could not be refreshed.",
)

UNRECOGNIZED_COMPLAINT = pd(
    "http://librarysimplified.org/terms/problem/unrecognized-complaint",
    status_code=404,
    title="Unrecognized complaint.",
    detail="The complaint you are attempting to resolve could not be found."
)

COMPLAINT_ALREADY_RESOLVED = pd(
    "http://librarysimplified.org/terms/problem/complaint-already-resolved",
    status_code=409,
    title="Complaint already resolved.",
    detail="You can't resolve a complaint that is already resolved."
)
