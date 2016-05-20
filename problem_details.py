from util.problem_detail import ProblemDetail as pd

INVALID_INPUT = pd(
      "http://librarysimplified.org/terms/problem/invalid-input",
      400,
      "Invalid input.",
      "You provided invalid or unrecognized input.",
)

UNRECOGNIZED_DATA_SOURCE = pd(
      "http://librarysimplified.org/terms/problem/unrecognized-data-source",
      400,
      "Unrecognized data source.",
      "I don't know anything about that data source.",
)

INVALID_CREDENTIALS = pd(
      "http://librarysimplified.org/terms/problem/credentials-invalid",
      401,
      "Invalid credentials",
      "Valid credentials are required.",
)

INVALID_URN = pd(
      "http://librarysimplified.org/terms/problem/could-not-parse-urn",
      400,
      "Invalid URN",
      "Could not parse identifier.",
)

INTERNAL_SERVER_ERROR = pd(
      "http://librarysimplified.org/terms/problem/internal-server-error",
      500,
      "Internal server error.",
      "Internal server error"
)

INTEGRATION_ERROR = pd(
      "http://librarysimplified.org/terms/problem/integration-error",
      502,
      "Integration error.",
      "Integration error"
)
