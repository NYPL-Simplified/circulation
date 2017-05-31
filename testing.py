from datetime import (
    datetime,
    timedelta,
)
import json
import logging
import os
import shutil
import tempfile
from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from config import Configuration

from model import (
    Base,
    Classification,
    IntegrationClient,
    Collection,
    Complaint,
    Contributor,
    CoverageRecord,
    Credential,
    CustomList,
    DataSource,
    DeliveryMechanism,
    DelegatedPatronIdentifier,
    Edition,
    Genre,
    Hyperlink,
    Identifier,
    LicensePool,
    Patron,
    Representation,
    Resource,
    RightsStatus,
    SessionManager,
    Subject,
    Work,
    WorkCoverageRecord,
    get_one_or_create,
    production_session
)
from classifier import Classifier
from coverage import (
    CoverageProvider,
    CoverageFailure,
    WorkCoverageProvider,
)

from external_search import DummyExternalSearchIndex
import mock
import model
import inspect


def package_setup():
    """Make sure the database schema is initialized and initial
    data is in place.
    """
    engine, connection = DatabaseTest.get_database_connection()

    # First, recreate the schema.
    #
    # Base.metadata.drop_all(connection) doesn't work here, so we
    # approximate by dropping everything except the materialized
    # views.
    for table in reversed(Base.metadata.sorted_tables):
        if not table.name.startswith('mv_'):
            engine.execute(table.delete())

    Base.metadata.create_all(connection)

    # Initialize basic database data needed by the application.
    _db = Session(connection)
    SessionManager.initialize_data(_db)
    _db.commit()
    connection.close()
    engine.dispose()

class DatabaseTest(object):

    engine = None
    connection = None

    @classmethod
    def get_database_connection(cls):
        url = Configuration.database_url(test=True)
        engine, connection = SessionManager.initialize(url)

        return engine, connection

    @classmethod
    def setup_class(cls):
        # Initialize a temporary data directory.
        cls.engine, cls.connection = cls.get_database_connection()
        cls.old_data_dir = Configuration.data_directory
        cls.tmp_data_dir = tempfile.mkdtemp(dir="/tmp")
        Configuration.instance[Configuration.DATA_DIRECTORY] = cls.tmp_data_dir

        os.environ['TESTING'] = 'true'

    @classmethod
    def teardown_class(cls):
        # Destroy the database connection and engine.
        cls.connection.close()
        cls.engine.dispose()

        if cls.tmp_data_dir.startswith("/tmp"):
            logging.debug("Removing temporary directory %s" % cls.tmp_data_dir)
            shutil.rmtree(cls.tmp_data_dir)
        else:
            logging.warn("Cowardly refusing to remove 'temporary' directory %s" % cls.tmp_data_dir)

        Configuration.instance[Configuration.DATA_DIRECTORY] = cls.old_data_dir
        if 'TESTING' in os.environ:
            del os.environ['TESTING']

    def setup(self):
        # Create a new connection to the database.
        self._db = Session(self.connection)
        self.transaction = self.connection.begin_nested()

        # Start with a high number so it won't interfere with tests that search for an age or grade
        self.counter = 2000

        self.time_counter = datetime(2014, 1, 1)
        self.isbns = ["9780674368279", "0636920028468", "9781936460236"]
        self.search_mock = mock.patch(model.__name__ + ".ExternalSearchIndex", DummyExternalSearchIndex)
        self.search_mock.start()

        # TODO:  keeping this for now, but need to fix it bc it hits _isbn, 
        # which pops an isbn off the list and messes tests up.  so exclude 
        # _ functions from participating.
        # also attempt to stop nosetest showing docstrings instead of function names.
        #for name, obj in inspect.getmembers(self):
        #    if inspect.isfunction(obj) and obj.__name__.startswith('test_'):
        #        obj.__doc__ = None


    def teardown(self):
        # Close the session.
        self._db.close()

        # Roll back all database changes that happened during this
        # test, whether in the session that was just closed or some
        # other session.
        self.transaction.rollback()
        self.search_mock.stop()

    def shortDescription(self):
        return None # Stop nosetests displaying docstrings instead of class names when verbosity level >= 2.

    @property
    def _id(self):
        self.counter += 1
        return self.counter

    @property
    def _str(self):
        return unicode(self._id)

    @property
    def _time(self):
        v = self.time_counter 
        self.time_counter = self.time_counter + timedelta(days=1)
        return v

    @property
    def _isbn(self):
        return self.isbns.pop()

    @property
    def _url(self):
        return "http://foo.com/" + self._str

    def _patron(self, external_identifier=None):
        external_identifier = external_identifier or self._str
        return get_one_or_create(
            self._db, Patron, external_identifier=external_identifier)[0]

    def _contributor(self, sort_name=None, name=None, **kw_args):
        name = sort_name or name or self._str
        return get_one_or_create(self._db, Contributor, sort_name=unicode(name), **kw_args)

    def _identifier(self, identifier_type=Identifier.GUTENBERG_ID, foreign_id=None):
        if foreign_id:
            id = foreign_id
        else:
            id = self._str
        return Identifier.for_foreign_id(self._db, identifier_type, id)[0]

    def _edition(self, data_source_name=DataSource.GUTENBERG,
                    identifier_type=Identifier.GUTENBERG_ID,
                    with_license_pool=False, with_open_access_download=False,
                    title=None, language="eng", authors=None, identifier_id=None,
                    series=None):
        id = identifier_id or self._str
        source = DataSource.lookup(self._db, data_source_name)
        wr = Edition.for_foreign_id(
            self._db, source, identifier_type, id)[0]
        if not title:
            title = self._str
        wr.title = unicode(title)
        if series:
            wr.series = series
        if language:
            wr.language = language
        if authors is None:
            authors = self._str
        if isinstance(authors, basestring):
            authors = [authors]
        if authors != []:
            wr.add_contributor(unicode(authors[0]), Contributor.PRIMARY_AUTHOR_ROLE)
            wr.author = unicode(authors[0])
        for author in authors[1:]:
            wr.add_contributor(unicode(author), Contributor.AUTHOR_ROLE)

        if with_license_pool or with_open_access_download:
            pool = self._licensepool(wr, data_source_name=data_source_name,
                                     with_open_access_download=with_open_access_download)  

            pool.set_presentation_edition()
            return wr, pool
        return wr

    def _work(self, title=None, authors=None, genre=None, language=None,
              audience=None, fiction=True, with_license_pool=False, 
              with_open_access_download=False, quality=0.5, series=None,
              presentation_edition=None):
        pool = None
        if with_open_access_download:
            with_license_pool = True
        language = language or "eng"
        title = unicode(title or self._str)
        audience = audience or Classifier.AUDIENCE_ADULT
        if audience == Classifier.AUDIENCE_CHILDREN:
            # TODO: This is necessary because Gutenberg's childrens books
            # get filtered out at the moment.
            data_source_name = DataSource.OVERDRIVE
        else:
            data_source_name = DataSource.GUTENBERG
        if fiction is None:
            fiction = True
        new_edition = False
        if not presentation_edition:
            new_edition = True
            presentation_edition = self._edition(
                title=title, language=language,
                authors=authors,
                with_license_pool=with_license_pool,
                with_open_access_download=with_open_access_download,
                data_source_name=data_source_name,
                series=series,
            )
            if with_license_pool:
                presentation_edition, pool = presentation_edition
        else:
            pool = presentation_edition.license_pool
        if with_open_access_download:
            pool.open_access = True
        if new_edition:
            presentation_edition.calculate_presentation()
        work, ignore = get_one_or_create(
            self._db, Work, create_method_kwargs=dict(
                audience=audience,
                fiction=fiction,
                quality=quality), id=self._id)
        if genre:
            if not isinstance(genre, Genre):
                genre, ignore = Genre.lookup(self._db, genre, autocreate=True)
            work.genres = [genre]
        work.random = 0.5

        work.set_presentation_edition(presentation_edition)
        work.calculate_presentation_edition()

        if pool != None:
            # make sure the pool's presentation_edition is set, 
            # bc loan tests assume that.
            if not work.license_pools:
                work.license_pools.append(pool)

            pool.set_presentation_edition()

            # This is probably going to be used in an OPDS feed, so
            # fake that the work is presentation ready.
            work.presentation_ready = True
            work.calculate_opds_entries(verbose=False)
        return work

    def _coverage_record(self, edition, coverage_source, operation=None,
                         status=CoverageRecord.SUCCESS):
        if isinstance(edition, Identifier):
            identifier = edition
        else:
            identifier = edition.primary_identifier
        record, ignore = get_one_or_create(
            self._db, CoverageRecord,
            identifier=identifier,
            data_source=coverage_source,
            operation=operation,
            create_method_kwargs = dict(
                timestamp=datetime.utcnow(),
                status=status,
            )
        )
        return record

    def _work_coverage_record(self, work, operation=None, 
                              status=CoverageRecord.SUCCESS):
        record, ignore = get_one_or_create(
            self._db, WorkCoverageRecord,
            work=work,
            operation=operation,
            create_method_kwargs = dict(
                timestamp=datetime.utcnow(),
                status=status,
            )
        )
        return record

    def _licensepool(self, edition, open_access=True, 
                     data_source_name=DataSource.GUTENBERG,
                     with_open_access_download=False, 
                     set_edition_as_presentation=False):
        source = DataSource.lookup(self._db, data_source_name)
        if not edition:
            edition = self._edition(data_source_name)

        pool, ignore = get_one_or_create(
            self._db, LicensePool,
            create_method_kwargs=dict(
                open_access=open_access),
            identifier=edition.primary_identifier, data_source=source,
            availability_time=datetime.utcnow()
        )

        if set_edition_as_presentation:
            pool.presentation_edition = edition

        if with_open_access_download:
            pool.open_access = True
            url = "http://foo.com/" + self._str
            media_type = Representation.EPUB_MEDIA_TYPE
            link, new = pool.identifier.add_link(
                Hyperlink.OPEN_ACCESS_DOWNLOAD, url,
                source, pool)

            # Add a DeliveryMechanism for this download
            pool.set_delivery_mechanism(
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.NO_DRM,
                RightsStatus.GENERIC_OPEN_ACCESS,
                link.resource,
            )

            representation, is_new = self._representation(
                url, media_type, "Dummy content", mirrored=True)
            link.resource.representation = representation
        else:

            # Add a DeliveryMechanism for this licensepool
            pool.set_delivery_mechanism(
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                RightsStatus.UNKNOWN,
                None
            )
            pool.licenses_owned = pool.licenses_available = 1

        return pool

    def _representation(self, url=None, media_type=None, content=None,
                        mirrored=False):
        url = url or "http://foo.com/" + self._str
        repr, is_new = get_one_or_create(
            self._db, Representation, url=url)
        repr.media_type = media_type
        if media_type and content:
            repr.content = content
            repr.fetched_at = datetime.utcnow()
            if mirrored:
                repr.mirror_url = "http://foo.com/" + self._str
                repr.mirrored_at = datetime.utcnow()            
        return repr, is_new

    def _customlist(self, foreign_identifier=None, 
                    name=None,
                    data_source_name=DataSource.NYT, num_entries=1,
                    entries_exist_as_works=True
    ):
        data_source = DataSource.lookup(self._db, data_source_name)
        foreign_identifier = foreign_identifier or self._str
        now = datetime.utcnow()
        customlist, ignore = get_one_or_create(
            self._db, CustomList,
            create_method_kwargs=dict(
                created=now,
                updated=now,
                name=name or self._str,
                description=self._str,
                ),
            data_source=data_source,
            foreign_identifier=foreign_identifier
        )

        editions = []
        for i in range(num_entries):
            if entries_exist_as_works:
                work = self._work(with_open_access_download=True)
                edition = work.presentation_edition
            else:
                edition = self._edition(
                    data_source_name, title="Item %s" % i)
                edition.permanent_work_id="Permanent work ID %s" % self._str
            customlist.add_entry(
                edition, "Annotation %s" % i, first_appearance=now)
            editions.append(edition)
        return customlist, editions

    def _complaint(self, license_pool, type, source, detail, resolved=None):
        complaint, is_new = Complaint.register(
            license_pool,
            type,
            source,
            detail,
            resolved
        )
        return complaint

    def _credential(self, data_source_name=DataSource.GUTENBERG,
                    type=None, patron=None):
        data_source = DataSource.lookup(self._db, data_source_name)
        type = type or self._str
        patron = patron or self._patron()
        credential, is_new = Credential.persistent_token_create(
            self._db, data_source, type, patron
        )
        return credential
    
    def _delegated_patron_identifier(
            self, library_uri=None, patron_identifier=None,
            identifier_type=DelegatedPatronIdentifier.ADOBE_ACCOUNT_ID,
            identifier=None
    ):
        """Create a sample DelegatedPatronIdentifier"""
        library_uri = library_uri or self._url
        patron_identifier = patron_identifier or self._str
        if callable(identifier):
            make_id = identifier
        else:
            if not identifier:
                identifier = self._str
            def make_id():
                return identifier
        patron, is_new = DelegatedPatronIdentifier.get_one_or_create(
            self._db, library_uri, patron_identifier, identifier_type,
            make_id
        )
        return patron

    def _sample_ecosystem(self):
        """ Creates an ecosystem of some sample work, pool, edition, and author 
        objects that all know each other. 
        """
        # make some authors
        [bob], ignore = Contributor.lookup(self._db, u"Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()
        [alice], ignore = Contributor.lookup(self._db, u"Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()

        edition_std_ebooks, pool_std_ebooks = self._edition(DataSource.STANDARD_EBOOKS, Identifier.URI, 
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_std_ebooks.title = u"The Standard Ebooks Title"
        edition_std_ebooks.subtitle = u"The Standard Ebooks Subtitle"
        edition_std_ebooks.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_git, pool_git = self._edition(DataSource.PROJECT_GITENBERG, Identifier.GUTENBERG_ID, 
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_git.title = u"The GItenberg Title"
        edition_git.subtitle = u"The GItenberg Subtitle"
        edition_git.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition_git.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_gut, pool_gut = self._edition(DataSource.GUTENBERG, Identifier.GUTENBERG_ID, 
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_gut.title = u"The GUtenberg Title"
        edition_gut.subtitle = u"The GUtenberg Subtitle"
        edition_gut.add_contributor(bob, Contributor.AUTHOR_ROLE)

        work = self._work(presentation_edition=edition_git)

        for p in pool_gut, pool_std_ebooks:
            work.license_pools.append(p)

        work.calculate_presentation()

        return (work, pool_std_ebooks, pool_git, pool_gut, 
            edition_std_ebooks, edition_git, edition_gut, alice, bob)


    def print_database_instance(self):
        """
        Calls the class method that examines the current state of the database model 
        (whether it's been committed or not).

        NOTE:  If you set_trace, and hit "continue", you'll start seeing console output right 
        away, without waiting for the whole test to run and the standard output section to display.
        You can also use nosetest --nocapture.
        I use:
        def test_name(self):
            [code...]
            set_trace()
            self.print_database_instance()  # TODO: remove before prod
            [code...]
        """
        if not 'TESTING' in os.environ:
            # we are on production, abort, abort!
            logging.warn("Forgot to remove call to testing.py:DatabaseTest.print_database_instance() before pushing to production.")
            return

        DatabaseTest.print_database_class(self._db)
        return


    @classmethod
    def print_database_class(cls, db_connection):
        """
        Prints to the console the entire contents of the database, as the unit test sees it. 
        Exists because unit tests don't persist db information, they create a memory 
        representation of the db state, and then roll the unit test-derived transactions back.
        So we cannot see what's going on by going into postgres and running selects.
        This is the in-test alternative to going into postgres.

        Can be called from model and metadata classes as well as tests.

        NOTE: The purpose of this method is for debugging.  
        Be careful of leaving it in code and potentially outputting 
        vast tracts of data into your output stream on production.

        Call like this:
        set_trace()
        from testing import (
            DatabaseTest, 
        )
        _db = Session.object_session(self)
        DatabaseTest.print_database_class(_db)  # TODO: remove before prod
        """
        if not 'TESTING' in os.environ:
            # we are on production, abort, abort!
            logging.warn("Forgot to remove call to testing.py:DatabaseTest.print_database_class() before pushing to production.")
            return

        works = db_connection.query(Work).all()
        identifiers = db_connection.query(Identifier).all()
        license_pools = db_connection.query(LicensePool).all()
        editions = db_connection.query(Edition).all()
        data_sources = db_connection.query(DataSource).all()
        representations = db_connection.query(Representation).all()

        if (not works):
            print "NO Work found"
        for wCount, work in enumerate(works):
            # pipe character at end of line helps see whitespace issues
            print "Work[%s]=%s|" % (wCount, work)

            if (not work.license_pools):
                print "    NO Work.LicensePool found"
            for lpCount, license_pool in enumerate(work.license_pools):
                print "    Work.LicensePool[%s]=%s|" % (lpCount, license_pool)

            print "    Work.presentation_edition=%s|" % work.presentation_edition

        print "__________________________________________________________________\n"
        if (not identifiers):
            print "NO Identifier found"
        for iCount, identifier in enumerate(identifiers):
            print "Identifier[%s]=%s|" % (iCount, identifier)
            print "    Identifier.licensed_through=%s|" % identifier.licensed_through           

        print "__________________________________________________________________\n"
        if (not license_pools):
            print "NO LicensePool found"
        for index, license_pool in enumerate(license_pools):
            print "LicensePool[%s]=%s|" % (index, license_pool)
            print "    LicensePool.work_id=%s|" % license_pool.work_id
            print "    LicensePool.data_source_id=%s|" % license_pool.data_source_id
            print "    LicensePool.identifier_id=%s|" % license_pool.identifier_id
            print "    LicensePool.presentation_edition_id=%s|" % license_pool.presentation_edition_id            
            print "    LicensePool.superceded=%s|" % license_pool.superceded
            print "    LicensePool.suppressed=%s|" % license_pool.suppressed

        print "__________________________________________________________________\n"
        if (not editions):
            print "NO Edition found"
        for index, edition in enumerate(editions):
            # pipe character at end of line helps see whitespace issues
            print "Edition[%s]=%s|" % (index, edition)
            print "    Edition.primary_identifier_id=%s|" % edition.primary_identifier_id
            print "    Edition.permanent_work_id=%s|" % edition.permanent_work_id
            if (edition.data_source):
                print "    Edition.data_source.id=%s|" % edition.data_source.id
                print "    Edition.data_source.name=%s|" % edition.data_source.name
            else:
                print "    No Edition.data_source."
            if (edition.license_pool):
                print "    Edition.license_pool.id=%s|" % edition.license_pool.id
            else:
                print "    No Edition.license_pool."

            print "    Edition.title=%s|" % edition.title
            print "    Edition.author=%s|" % edition.author
            if (not edition.author_contributors):
                print "    NO Edition.author_contributor found"
            for acCount, author_contributor in enumerate(edition.author_contributors):
                print "    Edition.author_contributor[%s]=%s|" % (acCount, author_contributor)

        print "__________________________________________________________________\n"
        if (not data_sources):
            print "NO DataSource found"
        for index, data_source in enumerate(data_sources):
            print "DataSource[%s]=%s|" % (index, data_source)
            print "    DataSource.id=%s|" % data_source.id
            print "    DataSource.name=%s|" % data_source.name
            print "    DataSource.offers_licenses=%s|" % data_source.offers_licenses
            print "    DataSource.editions=%s|" % data_source.editions            
            print "    DataSource.license_pools=%s|" % data_source.license_pools
            print "    DataSource.links=%s|" % data_source.links

        print "__________________________________________________________________\n"
        if (not representations):
            print "NO Representation found"
        for index, representation in enumerate(representations):
            print "Representation[%s]=%s|" % (index, representation)
            print "    Representation.id=%s|" % representation.id
            print "    Representation.url=%s|" % representation.url
            print "    Representation.mirror_url=%s|" % representation.mirror_url
            print "    Representation.fetch_exception=%s|" % representation.fetch_exception   
            print "    Representation.mirror_exception=%s|" % representation.mirror_exception

        return


    def _collection(self, name=None, protocol=Collection.OPDS_IMPORT,
                    external_account_id=None, url=None, username=None,
                    password=None):
        name = name or self._str
        collection, ignore = get_one_or_create(
            self._db, Collection, name=name, protocol=protocol
        )
        collection.external_account_id = external_account_id
        collection.external_integration.url = url
        collection.external_integration.username = username
        collection.external_integration.password = password
        return collection
        
    def _integration_client(self, url=None):
        url = url or self._url
        return get_one_or_create(
            self._db, IntegrationClient,
            url=url, key=u"abc", secret=u"def"
        )[0]

    def _subject(self, type, identifier):
        return get_one_or_create(
            self._db, Subject, type=type, identifier=identifier
        )[0]

    def _classification(self, identifier, subject, data_source, weight=1):
        return get_one_or_create(
            self._db, Classification, identifier=identifier, subject=subject, 
            data_source=data_source, weight=weight
        )[0]

class InstrumentedCoverageProvider(CoverageProvider):
    """A CoverageProvider that keeps track of every item it tried
    to cover.
    """
    def __init__(self, *args, **kwargs):
        super(InstrumentedCoverageProvider, self).__init__(*args, **kwargs)
        self.attempts = []

    def process_item(self, item):
        self.attempts.append(item)
        return item

class InstrumentedWorkCoverageProvider(WorkCoverageProvider):
    """A CoverageProvider that keeps track of every item it tried
    to cover.
    """
    def __init__(self, _db, *args, **kwargs):
        super(InstrumentedWorkCoverageProvider, self).__init__(_db, *args, **kwargs)
        self.attempts = []

    def process_item(self, item):
        self.attempts.append(item)
        return item

class AlwaysSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    """A CoverageProvider that does nothing and always succeeds."""

class AlwaysSuccessfulWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    """A WorkCoverageProvider that does nothing and always succeeds."""

class NeverSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    """A CoverageProvider that does nothing and always fails."""

    def __init__(self, _db, *args, **kwargs):
        super(NeverSuccessfulCoverageProvider, self).__init__(
            _db, *args, **kwargs
        )
        self.transient = kwargs.get('transient') or False

    def process_item(self, item):
        self.attempts.append(item)
        return CoverageFailure(
            item, "What did you expect?", self.output_source, self.transient
        )

class NeverSuccessfulWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    def process_item(self, item):
        self.attempts.append(item)
        return CoverageFailure(item, "What did you expect?", None, False)

class BrokenCoverageProvider(InstrumentedCoverageProvider):
    def process_item(self, item):
        raise Exception("I'm too broken to even return a CoverageFailure.")

class TransientFailureCoverageProvider(InstrumentedCoverageProvider):
    def process_item(self, item):
        self.attempts.append(item)
        return CoverageFailure(item, "Oops!", self.output_source, True)

class TransientFailureWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    def process_item(self, item):
        self.attempts.append(item)
        return CoverageFailure(item, "Oops!", None, True)

class TaskIgnoringCoverageProvider(InstrumentedCoverageProvider):
    """A coverage provider that ignores all work given to it."""
    def process_batch(self, batch):
        return []

class DummyCanonicalizeLookupResponse(object):

    @classmethod
    def success(cls, result):
        r = cls()
        r.status_code = 200
        r.headers = { "Content-Type" : "text/plain" }
        r.content = result
        return r

    @classmethod
    def failure(cls):
        r = cls()
        r.status_code = 404
        return r

class DummyMetadataClient(object):

    def __init__(self):
        self.lookups = {}

    def canonicalize_author_name(self, primary_identifier, display_author):
        if display_author in self.lookups:
            return DummyCanonicalizeLookupResponse.success(
                self.lookups[display_author])
        else:
            return DummyCanonicalizeLookupResponse.failure()

class DummyHTTPClient(object):

    def __init__(self):
        self.responses = []
        self.requests = []

    def queue_response(self, response_code, media_type="text/html",
                       other_headers=None, content=''):
        headers = {}
        if media_type:
            headers["content-type"] = media_type
        if other_headers:
            for k, v in other_headers.items():
                headers[k.lower()] = v
        self.responses.append((response_code, headers, content))

    def do_get(self, url, headers, **kwargs):
        self.requests.append(url)
        return self.responses.pop()


class MockRequestsResponse(object):
    """A mock object that simulates an HTTP response from the
    `requests` library.
    """
    def __init__(self, status_code, headers={}, content=None, url=None):
        self.status_code = status_code
        self.headers = headers
        self.content = content
        self.url = url or "http://url/"

    def json(self):
        content = self.content
        # The queued content might be a JSON string or it might
        # just be the object you'd get from loading a JSON string.
        if isinstance(content, basestring):
            content = json.loads(self.content)
        return content
        
    @property
    def text(self):
        return self.content.decode("utf8")


