from nose.tools import set_trace
from sqlalchemy.orm.session import Session

from authenticator import LibraryAuthenticator
from core.config import IntegrationException
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


