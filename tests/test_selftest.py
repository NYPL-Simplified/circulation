"""Test circulation-specific extensions to the self-test infrastructure."""
from nose.tools import (
    eq_,
    set_trace,
)

from core.testing import DatabaseTest
from core.model import (
    ExternalIntegration,
)

from api.authenticator import BasicAuthenticationProvider
from api.selftest import (
    HasSelfTests,
    SelfTestResult,
)

class TestHasSelfTests(DatabaseTest):

    def test_default_patrons(self):
        """Some self-tests must run with a patron's credentials.  The
        default_patrons() method finds the default Patron for every
        Library associated with a given Collection.
        """
        h = HasSelfTests()

        # This collection is not in any libraries, so there's no way
        # to test it.
        not_in_library = self._collection()
        [result] = h.default_patrons(not_in_library)
        eq_("Acquiring test patron credentials.", result.name)
        eq_(False, result.success)
        eq_("Collection is not associated with any libraries.",
            result.exception.message)
        eq_(
            "Add the collection to a library that has a patron authentication service.",
            result.exception.debug_message
        )

        # This collection is in two libraries.
        collection = self._default_collection

        # This library has no default patron set up.
        no_default_patron = self._library()
        collection.libraries.append(no_default_patron)

        # This library has a default patorn set up.
        integration = self._external_integration(
            "api.simple_authentication", ExternalIntegration.PATRON_AUTH_GOAL,
            libraries=[self._default_library]
        )
        p = BasicAuthenticationProvider
        integration.setting(p.TEST_IDENTIFIER).value = "username1"
        integration.setting(p.TEST_PASSWORD).value = "password1"

        # Calling default_patrons on the Collection returns one result for
        # each Library that uses that Collection.

        results = list(h.default_patrons(collection))
        eq_(2, len(results))
        [failure] = [x for x in results if isinstance(x, SelfTestResult)]
        [success] = [x for x in results if x != failure]

        # A SelfTestResult indicating failure was returned for the
        # library without a test patron, since the test cannot proceed with
        # a test patron.
        eq_(False, failure.success)
        eq_(
            "Acquiring test patron credentials for library %s" % no_default_patron.name,
            failure.name
        )
        eq_("Library has no test patron configured.", failure.exception.message)
        eq_("You can specify a test patron when you configure the library's patron authentication service.", failure.exception.debug_message)

        # The test patron for the library that has one was looked up,
        # and the test can proceed using this patron.
        library, patron, password = success
        eq_(self._default_library, library)
        eq_("username1", patron.authorization_identifier)
        eq_("password1", password)

