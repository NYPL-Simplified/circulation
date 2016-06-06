from util.problem_detail import ProblemDetail as pd
from util.http import INTEGRATION_ERROR
from flask.ext.babel import lazy_gettext as _

INVALID_INPUT = pd(
      "http://librarysimplified.org/terms/problem/invalid-input",
      400,
      _("Invalid input."),
      _("You provided invalid or unrecognized input."),
)

UNRECOGNIZED_DATA_SOURCE = pd(
      "http://librarysimplified.org/terms/problem/unrecognized-data-source",
      400,
      _("Unrecognized data source."),
      _("I don't know anything about that data source."),
)

INVALID_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/credentials-invalid",
      401,
      _("Invalid credentials"),
      _("Valid credentials are required."),
)

INVALID_URN = pd(
      "http://librarysimplified.org/terms/problem/could-not-parse-urn",
      400,
      _("Invalid URN"),
      _("Could not parse identifier."),
)

INTERNAL_SERVER_ERROR = pd(
      "http://librarysimplified.org/terms/problem/internal-server-error",
      500,
      _("Internal server error."),
      _("Internal server error"),
)
