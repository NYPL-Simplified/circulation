import sys
from sqlalchemy.orm.session import Session

from authenticator import LibraryAuthenticator
from circulation import CirculationAPI
from feedbooks import (
    FeedbooksOPDSImporter,
    FeedbooksImportMonitor,
)
from core.config import IntegrationException
from core.model import (
    ExternalIntegration,
    LicensePool,
)
from core.opds_import import (
    OPDSImporter,
    OPDSImportMonitor,
)
from core.scripts import LibraryInputScript
from core.selftest import (
    HasSelfTests as CoreHasSelfTests,
    SelfTestResult,
)


class HasSelfTests(CoreHasSelfTests):
    """Circulation-specific enhancements for HasSelfTests.

    Circulation self-tests frequently need to test the ability to act
    on behalf of a specific patron.
    """

    def default_patrons(self, collection):
        """Find a usable default Patron for each of the libraries associated
        with the given Collection.

        :yield: A sequence of (Library, Patron, password) 3-tuples.
            Yields (SelfTestFailure, None, None) if the Collection is not
            associated with any libraries, if a library does not
            have a default patron configured, or if there is an
            exception acquiring a library's default patron.
        """
        _db = Session.object_session(collection)
        if not collection.libraries:
            yield self.test_failure(
                "Acquiring test patron credentials.",
                "Collection is not associated with any libraries.",
                "Add the collection to a library that has a patron authentication service."
            )

        for library in collection.libraries:
            name = library.name
            task = "Acquiring test patron credentials for library %s" % library.name
            try:
                library_authenticator = LibraryAuthenticator.from_config(
                    _db, library
                )
                patron = password = None
                auth = library_authenticator.basic_auth_provider
                if auth:
                    patron, password = auth.testing_patron(_db)

                if not patron:
                    yield self.test_failure(
                        task,
                        "Library has no test patron configured.",
                        "You can specify a test patron when you configure the library's patron authentication service."
                    )
                    continue

                yield (library, patron, password)
            except IntegrationException, e:
                yield self.test_failure(task, e)
            except Exception, e:
                yield self.test_failure(
                    task, "Exception getting default patron: %r" % e
                )


class RunSelfTestsScript(LibraryInputScript):
    """Run the self-tests for every collection in the given library
    where that's possible.
    """

    def __init__(self, _db=None, output=sys.stdout):
        super(RunSelfTestsScript, self).__init__(_db)
        self.out = output

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            api_map = CirculationAPI(self._db, library).default_api_map
            api_map[ExternalIntegration.OPDS_IMPORT] = OPDSImportMonitor
            api_map[ExternalIntegration.FEEDBOOKS] = FeedbooksImportMonitor
            self.out.write("Testing %s\n" % library.name)
            for collection in library.collections:
                try:
                    self.test_collection(collection, api_map)
                except Exception, e:
                    self.out.write("  Exception while running self-test: %r\n" % e)

    def test_collection(self, collection, api_map, extra_args=None):
        tester = api_map.get(collection.protocol)
        if not tester:
            self.out.write(
                " Cannot find a self-test for %s, ignoring.\n" % collection.name
            )
            return

        self.out.write(" Running self-test for %s.\n" % collection.name)
        # Some HasSelfTests classes require extra arguments to their
        # constructors.
        extra_args = extra_args or {
            OPDSImportMonitor: [OPDSImporter],
            FeedbooksImportMonitor: [FeedbooksOPDSImporter],
        }
        extra = extra_args.get(tester, [])
        constructor_args = [self._db, collection] + list(extra)
        results_dict, results_list = tester.run_self_tests(
            self._db, None, *constructor_args
        )
        for result in results_list:
            self.process_result(result)

    def process_result(self, result):
        """Process a single TestResult object."""
        if result.success:
            success = "SUCCESS"
        else:
            success = "FAILURE"
        self.out.write(
            "  %s %s (%.1fsec)\n" % (
                success, result.name, result.duration
            )
        )
        if isinstance(result.result, basestring):
            self.out.write("   Result: %s\n" % result.result)
        if result.exception:
            self.out.write("   Exception: %r\n" % result.exception)


class HasCollectionSelfTests(HasSelfTests):
    """Extra tests to verify the integrity of imported
    collections of books.

    This is a mixin method that requires that `self.collection`
    point to the Collection to be tested.
    """

    def _no_delivery_mechanisms_test(self):
        # Find works in the tested collection that have no delivery
        # mechanisms.
        titles = []

        qu = self.collection.pools_with_no_delivery_mechanisms
        qu = qu.filter(LicensePool.licenses_owned > 0)
        for lp in qu:
            edition = lp.presentation_edition
            if edition:
                title = edition.title
            else:
                title = "[title unknown]"
            identifier = lp.identifier.identifier
            titles.append(
                "%s (ID: %s)" % (title, identifier)
            )

        if titles:
            return titles
        else:
            return "All titles in this collection have delivery mechanisms."

    def _run_self_tests(self):
        yield self.run_test(
            "Checking for titles that have no delivery mechanisms.",
            self._no_delivery_mechanisms_test
        )
