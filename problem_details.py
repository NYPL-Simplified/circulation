class ProblemDetail(object):

      def __init__(self, uri, title=None):
            self.uri = uri
            self.title = title

pd = ProblemDetail()

REMOTE_INTEGRATION_FAILED = pd(
      "http://librarysimplified.org/terms/problem/remote-integration-failed",
      "The library cannot complete your request because a third-party service has failed."
)
CANNOT_GENERATE_FEED_PROBLEM = pd(
      "http://librarysimplified.org/terms/problem/cannot-generate-feed",
      "This feed should have been pre-cached. It's too expensive to generate dynamically."
)
INVALID_CREDENTIALS_PROBLEM = pd(
      "http://librarysimplified.org/terms/problem/credentials-invalid",
      "A valid library card barcode number and PIN are required."
)
EXPIRED_CREDENTIALS_PROBLEM = pd(
      "http://librarysimplified.org/terms/problem/credentials-expired",
      "Your library card has expired. You need to renew it."
)
NO_LICENSES_PROBLEM = pd("http://librarysimplified.org/terms/problem/no-licenses")
NO_AVAILABLE_LICENSE_PROBLEM = pd("http://librarysimplified.org/terms/problem/no-available-license")
NO_ACCEPTABLE_FORMAT_PROBLEM = pd("http://librarysimplified.org/terms/problem/no-acceptable-format")
ALREADY_CHECKED_OUT_PROBLEM = pd("http://librarysimplified.org/terms/problem/loan-already-exists")
LOAN_LIMIT_REACHED_PROBLEM = pd("http://librarysimplified.org/terms/problem/loan-limit-reached")
CHECKOUT_FAILED = pd("http://librarysimplified.org/terms/problem/cannot-issue-loan")
HOLD_FAILED_PROBLEM = pd("http://librarysimplified.org/terms/problem/cannot-place-hold")
RENEW_FAILED_PROBLEM = pd("http://librarysimplified.org/terms/problem/cannot-renew-loan")
NO_ACTIVE_LOAN_PROBLEM = pd("http://librarysimplified.org/terms/problem/no-active-loan")
NO_ACTIVE_HOLD_PROBLEM = pd("http://librarysimplified.org/terms/problem/no-active-hold")
NO_ACTIVE_LOAN_OR_HOLD_PROBLEM = pd("http://librarysimplified.org/terms/problem/no-active-loan")
COULD_NOT_MIRROR_TO_REMOTE = pd("http://librarysimplified.org/terms/problem/cannot-mirror-to-remote")
NO_SUCH_LANE_PROBLEM = pd("http://librarysimplified.org/terms/problem/unknown-lane")
FORBIDDEN_BY_POLICY_PROBLEM = pd("http://librarysimplified.org/terms/problem/forbidden-by-policy")
CANNOT_FULFILL_PROBLEM = pd("http://librarysimplified.org/terms/problem/cannot-fulfill-loan")
BAD_DELIVERY_MECHANISM_PROBLEM = pd("http://librarysimplified.org/terms/problem/bad-delivery-mechanism")
CANNOT_RELEASE_HOLD_PROBLEM = pd("http://librarysimplified.org/terms/problem/cannot-release-hold")
)
