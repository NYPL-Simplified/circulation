import json
import os

from flask_babel import lazy_gettext as lgt

from api.authenticator import (
    OAuthAuthenticationProvider,
    OAuthController,
    PatronData,
)
from core.model import ExternalIntegration
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail
from core.util.string_helpers import base64
from api.problem_details import INVALID_CREDENTIALS


UNSUPPORTED_CLEVER_USER_TYPE = ProblemDetail(
    "http://librarysimplified.org/terms/problem/unsupported-clever-user-type",
    401,
    lgt("Your Clever user type is not supported."),
    lgt("Your Clever user type is not supported. You can request a code from First Book instead"),
)

CLEVER_NOT_ELIGIBLE = ProblemDetail(
    "http://librarysimplified.org/terms/problem/clever-not-eligible",
    401,
    lgt("Your Clever account is not eligible to access this application."),
    lgt("Your Clever account is not eligible to access this application."),
)

CLEVER_UNKNOWN_SCHOOL = ProblemDetail(
    "http://librarysimplified.org/terms/problem/clever-unknown-school",
    401,
    lgt("Clever did not provide the necessary information about your school to verify eligibility."),
    lgt("Clever did not provide the necessary information about your school to verify eligibility."),
)

# Load Title I NCES ID data from json.
TITLE_I_NCES_IDS = None
clever_dir = os.path.split(__file__)[0]

with open('%s/title_i.json' % clever_dir) as f:
    json_data = f.read()
    TITLE_I_NCES_IDS = json.loads(json_data)

CLEVER_GRADE_TO_EXTERNAL_TYPE_MAP = {
    "InfantToddler": "E",               # Early
    "Preschool": "E",
    "PreKindergarten": "E",
    "TransitionalKindergarten": "E",
    "Kindergarten": "E",
    "1": "E",
    "2": "E",
    "3": "E",
    "4": "M",                           # Middle
    "5": "M",
    "6": "M",
    "7": "M",
    "8": "M",
    "9": "H",                           # High
    "10": "H",
    "11": "H",
    "12": "H",
    "13": "H",
    "PostGraduate": "H",
    "Other": None,                      # Indeterminate
    "Ungraded": None,
}


def external_type_from_clever_grade(grade):
    """Maps a 'grade' value returned by the Clever API for student users to an external_type"""
    return CLEVER_GRADE_TO_EXTERNAL_TYPE_MAP.get(grade, None)


class CleverAuthenticationAPI(OAuthAuthenticationProvider):

    URI = "http://librarysimplified.org/terms/auth/clever"

    NAME = 'Clever'

    DESCRIPTION = lgt("""
        An authentication service for Open eBooks that uses Clever as an
        OAuth provider.""")

    LOGIN_BUTTON_IMAGE = "CleverLoginButton280.png"

    SETTINGS = [
        {"key": ExternalIntegration.USERNAME,
            "label": lgt("Client ID"), "required": True},
        {"key": ExternalIntegration.PASSWORD, "label": lgt(
            "Client Secret"), "required": True},
    ] + OAuthAuthenticationProvider.SETTINGS

    # Unlike other authentication providers, external type regular expression
    # doesn't make sense for Clever. This removes the LIBRARY_SETTINGS from the
    # parent class.
    LIBRARY_SETTINGS = []

    TOKEN_TYPE = "Clever token"
    TOKEN_DATA_SOURCE_NAME = 'Clever'

    EXTERNAL_AUTHENTICATE_URL = (
        "https://clever.com/oauth/authorize"
        "?response_type=code&client_id=%(client_id)s&redirect_uri=%(oauth_callback_url)s&state=%(state)s"
    )
    CLEVER_TOKEN_URL = "https://clever.com/oauth/tokens"

    # Not all calls should be made to a versioned endpoint. Please see the
    # Clever API documentation when adding a new endpoint.
    CLEVER_API_BASE_URL = "https://api.clever.com"
    CLEVER_API_VERSION = "3.0"
    CLEVER_API_VERSIONED_URL = f"{CLEVER_API_BASE_URL}/v{CLEVER_API_VERSION}"

    # To check Title I status we need state, which is associated with
    # a school in Clever's API. Any users at the district-level will
    # need to get a code from First Book instead.
    SUPPORTED_USER_TYPES = ['student', 'teacher']

    # Begin implementations of OAuthAuthenticationProvider abstract
    # methods.

    def oauth_callback(self, _db, code):
        """Verify the incoming parameters with the OAuth provider. Exchange
        the authorization code for an access token. Create or look up
        appropriate database records.

        :param code: The authorization code generated by the
            authorization server, as per section 4.1.2 of RFC 6749. This
            method will exchange the authorization code for an access
            token.

        :return: A ProblemDetail if there's a problem. Otherwise, a
            3-tuple (Credential, Patron, PatronData). The Credential
            contains the access token provided by the OAuth provider. The
            Patron object represents the authenticated Patron, and the
            PatronData object includes information about the patron
            obtained from the OAuth provider which cannot be stored in the
            circulation manager's database, but which should be passed on
            to the client.

        """
        # Ask the OAuth provider to verify the code that was passed
        # in.  This will give us a bearer token we can use to look up
        # detailed patron information.
        token = self.remote_exchange_code_for_bearer_token(_db, code)
        if isinstance(token, ProblemDetail):
            return token

        # Now that we have a bearer token, use it to look up patron
        # information.
        patrondata = self.remote_patron_lookup(token)
        if isinstance(patrondata, ProblemDetail):
            return patrondata

        # Convert the PatronData into a Patron object.
        patron, is_new = patrondata.get_or_create_patron(_db, self.library_id)

        # Create a credential for the Patron.
        credential, is_new = self.create_token(_db, patron, token)
        return credential, patron, patrondata

    # End implementations of OAuthAuthenticationProvider abstract
    # methods.

    def remote_exchange_code_for_bearer_token(self, _db, code):
        """Ask the OAuth provider to convert a code (passed in to the OAuth
        callback) into a bearer token.

        We can use the bearer token to act on behalf of a specific
        patron. It also gives us confidence that the patron
        authenticated correctly with Clever.

        :return: A ProblemDetail if there's a problem; otherwise, the
            bearer token.
        """
        payload = self._remote_exchange_payload(_db, code)
        authorization = base64.b64encode(
            self.client_id + ":" + self.client_secret)
        headers = {
            'Authorization': 'Basic %s' % authorization,
            'Content-Type': 'application/json',
        }
        response = self._get_token(payload, headers)
        invalid = INVALID_CREDENTIALS.detailed(
            lgt("A valid Clever login is required."))
        if not response:
            return invalid

        token = response.get('access_token', None)

        if not token:
            return invalid

        return token

    def _remote_exchange_payload(self, _db, code):
        library = self.library(_db)
        return dict(
            code=code,
            grant_type='authorization_code',
            redirect_uri=OAuthController.oauth_authentication_callback_url(
                library.short_name
            )
        )

    def remote_patron_lookup(self, token):
        """Use a bearer token for a patron to look up that patron's Clever
        record through the Clever API.

        This is the only method that has access to a patron's personal
        information as provided by Clever. Here's an inventory of the
        information we process and what happens to it:

        * The Clever 'id' associated with this patron is passed out of
          this method through the PatronData object, and persisted to
          two database fields: 'patrons.external_identifier' and
          'patrons.authorization_identifier'.

          As far as we know, the Clever ID is an opaque reference
          which uniquely identifies a given patron but contains no
          personal information about them.

        * If the patron is a student, their grade level
          ("Kindergarten" through "12") is converted into an Open
          eBooks patron type ("E" for "Early Grades", "M" for "Middle
          Grades", or "H" for "High School"). This is stored in the
          PatronData object returned from this method, and persisted
          to the database field 'patrons.external_type'. If the patron
          is not a student, their Open eBooks patron type is set to
          "A" for "All Access").

          This system does not track a patron's grade level or store
          it in the database. Only the coarser-grained Open eBooks
          patron type is tracked. This is used to show age-appropriate
          books to the patron.

        * The internal Clever ID of the patron's school is used to
          make a _second_ Clever API request to get information about
          the school. From that, we get the school's NCES ID, which we
          cross-check against data we've gathered separately to
          validate the school's Title I status. The school ID and NCES
          ID are not stored in the PatronData object or persisted to
          the database. Any patron who ends up in the database is
          presumed to have passed this check.

        To summarize, an opaque ID associated with the patron is
        persisted to the database, as is a coarse-grained indicator of
        the patron's age. No other information about the patron makes
        it out of this method.

        :return: A ProblemDetail if there's a problem. Otherwise, a PatronData
            with the data listed above.

        """
        bearer_headers = {'Authorization': 'Bearer %s' % token}
        result = self._get(self.CLEVER_API_VERSIONED_URL +
                           '/me', bearer_headers)
        data = result.get('data', {}) or {}

        identifier = data.get('id', None)

        if not identifier:
            return INVALID_CREDENTIALS.detailed(lgt("A valid Clever login is required."))

        if result.get('user_type') not in self.SUPPORTED_USER_TYPES:
            return UNSUPPORTED_CLEVER_USER_TYPE

        links = result['links']

        user_link = [link for link in links if link['rel']
                     == 'canonical'][0]['uri']
        # The canonical link includes the API version, so we use the base URL.
        user = self._get(self.CLEVER_API_BASE_URL + user_link, bearer_headers)

        user_data = user['data']
        school_id = user_data['school']
        school = self._get(
            f"{self.CLEVER_API_VERSIONED_URL}/schools/{school_id}", bearer_headers)
        school_nces_id = school['data'].get('nces_id')

        # TODO: check student free and reduced lunch status as well

        if school_nces_id is None:
            self.log.error(
                "No NCES ID found in Clever school data: %s", repr(school))
            return CLEVER_UNKNOWN_SCHOOL

        if school_nces_id not in TITLE_I_NCES_IDS:
            self.log.info("%s didn't match a Title I NCES ID", school_nces_id)
            return CLEVER_NOT_ELIGIBLE

        external_type = None

        if result['user_type'] == 'student':
            # We need to be able to assign an external_type to students, so that they
            # get the correct content level. To do so we rely on the grade field in the
            # user data we get back from Clever. Their API doesn't guarantee that the
            # grade field is present, so we supply a default.
            student_grade = user_data.get('grade', None)

            if not student_grade:   # If no grade was supplied, log the school/student
                msg = (f"CLEVER_UNKNOWN_PATRON_GRADE: School with NCES ID {school_nces_id} "
                       f"did not supply grade for student {user_data.get('id')}")
                self.log.info(msg)

            # If we can't determine a type from the grade level, set to "A"
            external_type = external_type_from_clever_grade(student_grade)
        else:
            external_type = "A"     # Non-students get content level "A"

        patrondata = PatronData(
            permanent_id=identifier,
            authorization_identifier=identifier,
            external_type=external_type,
            complete=True
        )
        return patrondata

    def _get_token(self, payload, headers):
        response = HTTP.post_with_timeout(
            self.CLEVER_TOKEN_URL, json.dumps(payload), headers=headers
        )
        return response.json()

    def _get(self, url, headers):
        return HTTP.get_with_timeout(url, headers=headers).json()


AuthenticationProvider = CleverAuthenticationAPI
