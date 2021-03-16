from datetime import (
    datetime,
    timedelta,
)
import json
import logging
import os
import shutil
import time
import tempfile
import uuid
from psycopg2.errors import UndefinedTable
import pytest
from sqlalchemy.orm.session import Session
from sqlalchemy.exc import ProgrammingError
from .config import Configuration

from .lane import (
    Lane,
)
from .model.constants import MediaTypes
from .model import (
    Base,
    PresentationCalculationPolicy,
    SessionManager,
    get_one_or_create,
    create,
)

from .model import (
    CoverageRecord,
    Classification,
    Collection,
    Complaint,
    ConfigurationSetting,
    Contributor,
    Credential,
    CustomList,
    DataSource,
    DelegatedPatronIdentifier,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Genre,
    Hyperlink,
    Identifier,
    IntegrationClient,
    Library,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Patron,
    Representation,
    Resource,
    RightsStatus,
    Subject,
    Work,
    WorkCoverageRecord,
)
from .model.configuration import ExternalIntegrationLink

from .classifier import Classifier
from .coverage import (
    BibliographicCoverageProvider,
    CollectionCoverageProvider,
    IdentifierCoverageProvider,
    CoverageFailure,
    WorkCoverageProvider,
)

from .external_search import (
    MockExternalSearchIndex,
    ExternalSearchIndex,
    SearchIndexCoverageProvider,
)
from .log import LogConfiguration
from . import external_search
import mock
import inspect

class LogCaptureHandler(logging.Handler):
    """A `logging.Handler` context manager that captures the messages
    of emitted log records in the context of the specified `logger`.
    """
    _level_names = logging._levelToName.values()

    @staticmethod
    def _normalize_level(level):
        return level.lower()

    LEVEL_NAMES = list(map(_normalize_level.__func__, _level_names))

    def __init__(self, logger, *args, **kwargs):
        """Constructor.

        :param logger: `logger` to which this handler will be added.
        :param args: positional arguments to `logging.Handler.__init__`.
        :param kwargs: keyword arguments to `logging.Handler.__init__`.
        """
        self.logger = logger
        self._records = {}
        logging.Handler.__init__(self, *args, **kwargs)

    def __enter__(self):
        self.reset()
        self.logger.addHandler(self)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.logger.removeHandler(self)

    def emit(self, record):
        level = self._normalize_level(record.levelname)
        if level not in self.LEVEL_NAMES:
            message = "Unexpected log level: '%s'." % record.levelname
            raise ValueError(message)
        self._records[level].append(record.getMessage())

    def reset(self):
        """Empty the message accumulators.
        """
        self._records = {level: [] for level in self.LEVEL_NAMES}

    def __getitem__(self, item):
        if item in self.LEVEL_NAMES:
            return self._records[item]
        else:
            message = "'%s' object has no attribute '%s'" % (self.__class__.__name__, item)
            raise AttributeError(message)

    def __getattr__(self, item):
        return self.__getitem__(item)


class DatabaseTest(object):

    engine = None
    connection = None

    @classmethod
    def get_database_connection(cls):
        url = Configuration.database_url()
        engine, connection = SessionManager.initialize(url)

        return engine, connection

    @classmethod
    def setup_class(cls):
        # Initialize a temporary data directory.
        cls.engine, cls.connection = cls.get_database_connection()
        cls.old_data_dir = Configuration.data_directory
        cls.tmp_data_dir = tempfile.mkdtemp(dir="/tmp")
        Configuration.instance[Configuration.DATA_DIRECTORY] = cls.tmp_data_dir

        # Avoid CannotLoadConfiguration errors related to CDN integrations.
        Configuration.instance[Configuration.INTEGRATIONS] = Configuration.instance.get(
            Configuration.INTEGRATIONS, {}
        )
        Configuration.instance[Configuration.INTEGRATIONS][ExternalIntegration.CDN] = {}

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

    @pytest.fixture(autouse=True)
    def search_mock(self, request):
        # Only setup the elasticsearch mock if the elasticsearch mark isn't set
        elasticsearch_mark = request.node.get_closest_marker("elasticsearch")
        if elasticsearch_mark is not None:
            self.search_mock = None
        else:
            self.search_mock = mock.patch(external_search.__name__ + ".ExternalSearchIndex", MockExternalSearchIndex)
            self.search_mock.start()
        yield
        if self.search_mock:
            self.search_mock.stop()

    def setup_method(self):
        # Create a new connection to the database.
        self._db = Session(self.connection)
        self.transaction = self.connection.begin_nested()

        # Start with a high number so it won't interfere with tests that search for an age or grade
        self.counter = 2000

        self.time_counter = datetime(2014, 1, 1)
        self.isbns = [
            "9780674368279", "0636920028468", "9781936460236", "9780316075978"
        ]

    def teardown_method(self):
        # Close the session.
        self._db.close()

        # Roll back all database changes that happened during this
        # test, whether in the session that was just closed or some
        # other session.
        self.transaction.rollback()

        # Remove any database objects cached in the model classes but
        # associated with the now-rolled-back session.
        Collection.reset_cache()
        ConfigurationSetting.reset_cache()
        DataSource.reset_cache()
        DeliveryMechanism.reset_cache()
        ExternalIntegration.reset_cache()
        Genre.reset_cache()
        Library.reset_cache()

        # Also roll back any record of those changes in the
        # Configuration instance.
        for key in [
                Configuration.SITE_CONFIGURATION_LAST_UPDATE,
                Configuration.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE
        ]:
            if key in Configuration.instance:
                del(Configuration.instance[key])

    def time_eq(self, a, b):
        "Assert that two times are *approximately* the same -- within 2 seconds."
        if a < b:
            delta = b-a
        else:
            delta = a-b
        total_seconds = delta.total_seconds()
        assert (total_seconds < 2), ("Delta was too large: %.2f seconds." % total_seconds)

    def shortDescription(self):
        return None # Stop nosetests displaying docstrings instead of class names when verbosity level >= 2.

    @property
    def _id(self):
        self.counter += 1
        return self.counter

    @property
    def _str(self):
        return str(self._id)

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

    def _patron(self, external_identifier=None, library=None):
        external_identifier = external_identifier or self._str
        library = library or self._default_library
        return get_one_or_create(
            self._db, Patron, external_identifier=external_identifier,
            library=library
        )[0]

    def _contributor(self, sort_name=None, name=None, **kw_args):
        name = sort_name or name or self._str
        return get_one_or_create(self._db, Contributor, sort_name=str(name), **kw_args)

    def _identifier(self, identifier_type=Identifier.GUTENBERG_ID, foreign_id=None):
        if foreign_id:
            id = foreign_id
        else:
            id = self._str
        return Identifier.for_foreign_id(self._db, identifier_type, id)[0]

    def _edition(
            self,
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            with_license_pool=False,
            with_open_access_download=False,
            title=None,
            language="eng",
            authors=None,
            identifier_id=None,
            series=None,
            collection=None,
            publication_date=None,
            self_hosted=False,
            unlimited_access=False
    ):
        id = identifier_id or self._str
        source = DataSource.lookup(self._db, data_source_name)
        wr = Edition.for_foreign_id(
            self._db, source, identifier_type, id)[0]
        if not title:
            title = self._str
        wr.title = str(title)
        wr.medium = Edition.BOOK_MEDIUM
        if series:
            wr.series = series
        if language:
            wr.language = language
        if authors is None:
            authors = self._str
        if isinstance(authors, str):
            authors = [authors]
        if authors:
            primary_author_name = str(authors[0])
            contributor = wr.add_contributor(primary_author_name, Contributor.PRIMARY_AUTHOR_ROLE)
            # add_contributor assumes authors[0] is a sort_name,
            # but it may be a display name. If so, set that field as well.
            if not contributor.display_name and ',' not in primary_author_name:
                contributor.display_name = primary_author_name
            wr.author = primary_author_name

        for author in authors[1:]:
            wr.add_contributor(str(author), Contributor.AUTHOR_ROLE)
        if publication_date:
            wr.published = publication_date

        if with_license_pool or with_open_access_download:
            pool = self._licensepool(
                wr, data_source_name=data_source_name,
                with_open_access_download=with_open_access_download,
                collection=collection,
                self_hosted=self_hosted,
                unlimited_access=unlimited_access
            )

            pool.set_presentation_edition()
            return wr, pool
        return wr

    def _work(
            self,
            title=None,
            authors=None,
            genre=None,
            language=None,
            audience=None,
            fiction=True,
            with_license_pool=False,
            with_open_access_download=False,
            quality=0.5,
            series=None,
            presentation_edition=None,
            collection=None,
            data_source_name=None,
            self_hosted=False,
            unlimited_access=False
    ):
        """Create a Work.

        For performance reasons, this method does not generate OPDS
        entries or calculate a presentation edition for the new
        Work. Tests that rely on this information being present
        should call _slow_work() instead, which takes more care to present
        the sort of Work that would be created in a real environment.
        """
        pools = []
        if with_open_access_download:
            with_license_pool = True
        language = language or "eng"
        title = str(title or self._str)
        audience = audience or Classifier.AUDIENCE_ADULT
        if audience == Classifier.AUDIENCE_CHILDREN and not data_source_name:
            # TODO: This is necessary because Gutenberg's childrens books
            # get filtered out at the moment.
            data_source_name = DataSource.OVERDRIVE
        elif not data_source_name:
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
                collection=collection,
                self_hosted=self_hosted,
                unlimited_access=unlimited_access
            )
            if with_license_pool:
                presentation_edition, pool = presentation_edition
                if with_open_access_download:
                    pool.open_access = True
                if self_hosted:
                    pool.open_access = False
                    pool.self_hosted = True
                if unlimited_access:
                    pool.open_access = False
                    pool.unlimited_access = True

                pools = [pool]
        else:
            pools = presentation_edition.license_pools
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

        if pools:
            # make sure the pool's presentation_edition is set,
            # bc loan tests assume that.
            if not work.license_pools:
                for pool in pools:
                    work.license_pools.append(pool)

            for pool in pools:
                pool.set_presentation_edition()

            # This is probably going to be used in an OPDS feed, so
            # fake that the work is presentation ready.
            work.presentation_ready = True
            work.calculate_opds_entries(verbose=False)

        return work

    def _lane(self, display_name=None, library=None,
              parent=None, genres=None, languages=None,
              fiction=None, inherit_parent_restrictions=True
    ):
        display_name = display_name or self._str
        library = library or self._default_library
        lane, is_new = create(
            self._db, Lane,
            library=library,
            parent=parent, display_name=display_name,
            fiction=fiction,
            inherit_parent_restrictions=inherit_parent_restrictions
        )
        if is_new and parent:
            lane.priority = len(parent.sublanes)-1
        if genres:
            if not isinstance(genres, list):
                genres = [genres]
            for genre in genres:
                if isinstance(genre, str):
                    genre, ignore = Genre.lookup(self._db, genre)
                lane.genres.append(genre)
        if languages:
            if not isinstance(languages, list):
                languages = [languages]
            lane.languages = languages
        return lane

    def _slow_work(self, *args, **kwargs):
        """Create a work that closely resembles one that might be found in the
        wild.

        This is significantly slower than _work() but more reliable.
        """
        work = self._work(*args, **kwargs)
        work.calculate_presentation_edition()
        work.calculate_opds_entries(verbose=False)
        return work

    def _add_generic_delivery_mechanism(self, license_pool):
        """Give a license pool a generic non-open-access delivery mechanism."""
        data_source = license_pool.data_source
        identifier = license_pool.identifier
        content_type = Representation.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        return LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.IN_COPYRIGHT
        )

    def _coverage_record(self, edition, coverage_source, operation=None,
        status=CoverageRecord.SUCCESS, collection=None, exception=None,
    ):
        if isinstance(edition, Identifier):
            identifier = edition
        else:
            identifier = edition.primary_identifier
        record, ignore = get_one_or_create(
            self._db, CoverageRecord,
            identifier=identifier,
            data_source=coverage_source,
            operation=operation,
            collection=collection,
            create_method_kwargs = dict(
                timestamp=datetime.utcnow(),
                status=status,
                exception=exception,
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

    def _licensepool(
            self,
            edition,
            open_access=True,
            data_source_name=DataSource.GUTENBERG,
            with_open_access_download=False,
            set_edition_as_presentation=False,
            collection=None,
            self_hosted=False,
            unlimited_access=False
    ):
        source = DataSource.lookup(self._db, data_source_name)
        if not edition:
            edition = self._edition(data_source_name)
        collection = collection or self._default_collection
        pool, ignore = get_one_or_create(
            self._db, LicensePool,
            create_method_kwargs=dict(open_access=open_access),
            identifier=edition.primary_identifier,
            data_source=source,
            collection=collection,
            availability_time=datetime.utcnow(),
            self_hosted=self_hosted,
            unlimited_access=unlimited_access
        )

        if set_edition_as_presentation:
            pool.presentation_edition = edition

        if with_open_access_download:
            pool.open_access = True
            url = "http://foo.com/" + self._str
            media_type = MediaTypes.EPUB_MEDIA_TYPE
            link, new = pool.identifier.add_link(
                Hyperlink.OPEN_ACCESS_DOWNLOAD, url,
                source, media_type
            )

            # Add a DeliveryMechanism for this download
            pool.set_delivery_mechanism(
                media_type,
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
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                RightsStatus.UNKNOWN,
                None
            )

            if not unlimited_access:
                pool.licenses_owned = pool.licenses_available = 1

        return pool

    def _license(self, pool, identifier=None, checkout_url=None, status_url=None,
                 expires=None, remaining_checkouts=None, concurrent_checkouts=None):
        identifier = identifier or self._str
        checkout_url = checkout_url or self._str
        status_url = status_url or self._str
        license, ignore = get_one_or_create(
            self._db, License, identifier=identifier, license_pool=pool,
            checkout_url=checkout_url,
            status_url=status_url, expires=expires,
            remaining_checkouts=remaining_checkouts,
            concurrent_checkouts=concurrent_checkouts,
        )
        return license

    def _representation(self, url=None, media_type=None, content=None,
                        mirrored=False):
        url = url or "http://foo.com/" + self._str
        repr, is_new = get_one_or_create(
            self._db, Representation, url=url)
        repr.media_type = media_type
        if media_type and content:
            if isinstance(content, str):
                content = content.encode("utf8")
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

    def _external_integration(self, protocol, goal=None, settings=None,
                              libraries=None, **kwargs
    ):
        integration = None
        if not libraries:
            integration, ignore = get_one_or_create(
                self._db, ExternalIntegration, protocol=protocol, goal=goal
            )
        else:
            if not isinstance(libraries, list):
                libraries = [libraries]

            # Try to find an existing integration for one of the given
            # libraries.
            for library in libraries:
                integration = ExternalIntegration.lookup(
                    self._db, protocol, goal, library=libraries[0]
                )
                if integration:
                    break

            if not integration:
                # Otherwise, create a brand new integration specifically
                # for the library.
                integration = ExternalIntegration(
                    protocol=protocol, goal=goal,
                )
                integration.libraries.extend(libraries)
                self._db.add(integration)

        for attr, value in list(kwargs.items()):
            setattr(integration, attr, value)

        settings = settings or dict()
        for key, value in list(settings.items()):
            integration.set_setting(key, value)

        return integration

    def _external_integration_link(self, integration=None, library=None,
                                    other_integration=None, purpose="covers_mirror"):

        integration = integration or self._external_integration("some protocol")
        other_integration = other_integration or self._external_integration("some other protocol")

        library_id = library.id if library else None

        external_integration_link, ignore = get_one_or_create(
            self._db, ExternalIntegrationLink,
            library_id=library_id,
            external_integration_id=integration.id,
            other_integration_id=other_integration.id,
            purpose=purpose
        )

        return external_integration_link

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
        [bob], ignore = Contributor.lookup(self._db, "Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()
        [alice], ignore = Contributor.lookup(self._db, "Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()

        edition_std_ebooks, pool_std_ebooks = self._edition(DataSource.STANDARD_EBOOKS, Identifier.URI,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_std_ebooks.title = "The Standard Ebooks Title"
        edition_std_ebooks.subtitle = "The Standard Ebooks Subtitle"
        edition_std_ebooks.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_git, pool_git = self._edition(DataSource.PROJECT_GITENBERG, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_git.title = "The GItenberg Title"
        edition_git.subtitle = "The GItenberg Subtitle"
        edition_git.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition_git.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_gut, pool_gut = self._edition(DataSource.GUTENBERG, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_gut.title = "The GUtenberg Title"
        edition_gut.subtitle = "The GUtenberg Subtitle"
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

        NOTE: If you set_trace, and hit "continue", you'll start seeing console output right
        away, without waiting for the whole test to run and the standard output section to display.
        You can also use nosetest --nocapture.

        I use::

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

        Call like this::

            set_trace()
            from testing import (l=
                DatabaseTest,
            )
            _db = Session.object_session(self)
            DatabaseTest.print_database_class(_db)

            TODO: remove before prod
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
            print("NO Work found")
        for wCount, work in enumerate(works):
            # pipe character at end of line helps see whitespace issues
            print("Work[%s]=%s|" % (wCount, work))

            if (not work.license_pools):
                print("    NO Work.LicensePool found")
            for lpCount, license_pool in enumerate(work.license_pools):
                print("    Work.LicensePool[%s]=%s|" % (lpCount, license_pool))

            print("    Work.presentation_edition=%s|" % work.presentation_edition)

        print("__________________________________________________________________\n")
        if (not identifiers):
            print("NO Identifier found")
        for iCount, identifier in enumerate(identifiers):
            print("Identifier[%s]=%s|" % (iCount, identifier))
            print("    Identifier.licensed_through=%s|" % identifier.licensed_through)

        print("__________________________________________________________________\n")
        if (not license_pools):
            print("NO LicensePool found")
        for index, license_pool in enumerate(license_pools):
            print("LicensePool[%s]=%s|" % (index, license_pool))
            print("    LicensePool.work_id=%s|" % license_pool.work_id)
            print("    LicensePool.data_source_id=%s|" % license_pool.data_source_id)
            print("    LicensePool.identifier_id=%s|" % license_pool.identifier_id)
            print("    LicensePool.presentation_edition_id=%s|" % license_pool.presentation_edition_id)
            print("    LicensePool.superceded=%s|" % license_pool.superceded)
            print("    LicensePool.suppressed=%s|" % license_pool.suppressed)

        print("__________________________________________________________________\n")
        if (not editions):
            print("NO Edition found")
        for index, edition in enumerate(editions):
            # pipe character at end of line helps see whitespace issues
            print("Edition[%s]=%s|" % (index, edition))
            print("    Edition.primary_identifier_id=%s|" % edition.primary_identifier_id)
            print("    Edition.permanent_work_id=%s|" % edition.permanent_work_id)
            if (edition.data_source):
                print("    Edition.data_source.id=%s|" % edition.data_source.id)
                print("    Edition.data_source.name=%s|" % edition.data_source.name)
            else:
                print("    No Edition.data_source.")
            if (edition.license_pool):
                print("    Edition.license_pool.id=%s|" % edition.license_pool.id)
            else:
                print("    No Edition.license_pool.")

            print("    Edition.title=%s|" % edition.title)
            print("    Edition.author=%s|" % edition.author)
            if (not edition.author_contributors):
                print("    NO Edition.author_contributor found")
            for acCount, author_contributor in enumerate(edition.author_contributors):
                print("    Edition.author_contributor[%s]=%s|" % (acCount, author_contributor))

        print("__________________________________________________________________\n")
        if (not data_sources):
            print("NO DataSource found")
        for index, data_source in enumerate(data_sources):
            print("DataSource[%s]=%s|" % (index, data_source))
            print("    DataSource.id=%s|" % data_source.id)
            print("    DataSource.name=%s|" % data_source.name)
            print("    DataSource.offers_licenses=%s|" % data_source.offers_licenses)
            print("    DataSource.editions=%s|" % data_source.editions)
            print("    DataSource.license_pools=%s|" % data_source.license_pools)
            print("    DataSource.links=%s|" % data_source.links)

        print("__________________________________________________________________\n")
        if (not representations):
            print("NO Representation found")
        for index, representation in enumerate(representations):
            print("Representation[%s]=%s|" % (index, representation))
            print("    Representation.id=%s|" % representation.id)
            print("    Representation.url=%s|" % representation.url)
            print("    Representation.mirror_url=%s|" % representation.mirror_url)
            print("    Representation.fetch_exception=%s|" % representation.fetch_exception)
            print("    Representation.mirror_exception=%s|" % representation.mirror_exception)

        return


    def _library(self, name=None, short_name=None):
        name=name or self._str
        short_name = short_name or self._str
        library, ignore = get_one_or_create(
            self._db, Library, name=name, short_name=short_name,
            create_method_kwargs=dict(uuid=str(uuid.uuid4())),
        )
        return library

    def _collection(self, name=None, protocol=ExternalIntegration.OPDS_IMPORT,
                    external_account_id=None, url=None, username=None,
                    password=None, data_source_name=None):
        name = name or self._str
        collection, ignore = get_one_or_create(
            self._db, Collection, name=name
        )
        collection.external_account_id = external_account_id
        integration = collection.create_external_integration(protocol)
        integration.goal = ExternalIntegration.LICENSE_GOAL
        integration.url = url
        integration.username = username
        integration.password = password

        if data_source_name:
            collection.data_source = data_source_name
        return collection

    @property
    def _default_library(self):
        """A Library that will only be created once throughout a given test.

        By default, the `_default_collection` will be associated with
        the default library.
        """
        if not hasattr(self, '_default__library'):
            self._default__library = self.make_default_library(self._db)
        return self._default__library

    @property
    def _default_collection(self):
        """A Collection that will only be created once throughout
        a given test.

        For most tests there's no need to create a different
        Collection for every LicensePool. Using
        self._default_collection instead of calling self.collection()
        saves time.
        """
        if not hasattr(self, '_default__collection'):
            self._default__collection = self._default_library.collections[0]
        return self._default__collection

    @classmethod
    def make_default_library(cls, _db):
        """Ensure that the default library exists in the given database.

        This can be called by code intended for use in testing but not actually
        within a DatabaseTest subclass.
        """
        library, ignore = get_one_or_create(
            _db, Library, create_method_kwargs=dict(
                uuid=str(uuid.uuid4()),
                name="default",
            ), short_name="default"
        )
        collection, ignore = get_one_or_create(
            _db, Collection, name="Default Collection"
        )
        integration = collection.create_external_integration(
            ExternalIntegration.OPDS_IMPORT
        )
        integration.goal = ExternalIntegration.LICENSE_GOAL
        if collection not in library.collections:
            library.collections.append(collection)
        return library

    def _catalog(self, name="Faketown Public Library"):
        source, ignore = get_one_or_create(self._db, DataSource, name=name)

    def _integration_client(self, url=None, shared_secret=None):
        url = url or self._url
        secret = shared_secret or "secret"
        return get_one_or_create(
            self._db, IntegrationClient, shared_secret=secret,
            create_method_kwargs=dict(url=url)
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

    def sample_cover_path(self, name):
        """The path to the sample cover with the given filename."""
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "tests", "files", "covers")
        sample_cover_path = os.path.join(resource_path, name)
        return sample_cover_path

    def sample_cover_representation(self, name):
        """A Representation of the sample cover with the given filename."""
        sample_cover_path = self.sample_cover_path(name)
        return self._representation(
            media_type="image/png",
            content=open(sample_cover_path, 'rb').read()
        )[0]


class SearchClientForTesting(ExternalSearchIndex):
    """When creating an index, limit it to a single shard and disable
    replicas.

    This makes search results more predictable.
    """

    def setup_index(self, new_index=None):
        return super(SearchClientForTesting, self).setup_index(
            new_index, number_of_shards=1, number_of_replicas=0
        )


@pytest.mark.elasticsearch
class ExternalSearchTest(DatabaseTest):
    """
    These tests require elasticsearch to be running locally. If it's not, or there's
    an error creating the index, the tests will pass without doing anything.

    Tests for elasticsearch are useful for ensuring that we haven't accidentally broken
    a type of search by changing analyzers or queries, but search needs to be tested manually
    to ensure that it works well overall, with a realistic index.
    """

    SIMPLIFIED_TEST_ELASTICSEARCH = os.environ.get('SIMPLIFIED_TEST_ELASTICSEARCH', 'http://localhost:9200')

    def setup_method(self):

        super(ExternalSearchTest, self).setup_method()

        # Track the indexes created so they can be torn down at the
        # end of the test.
        self.indexes = []

        self.integration = self._external_integration(
            ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
            url=self.SIMPLIFIED_TEST_ELASTICSEARCH,
            settings={
                ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY : 'test_index',
                ExternalSearchIndex.TEST_SEARCH_TERM_KEY : 'test_search_term',
            }
        )

        try:
            self.search = SearchClientForTesting(self._db)
        except Exception as e:
            self.search = None
            logging.error(
                "Unable to set up elasticsearch index, search tests will be skipped.",
                exc_info=e
            )

    def setup_index(self, new_index):
        "Create an index and register it to be destroyed during teardown."
        self.search.setup_index(new_index=new_index)
        self.indexes.append(new_index)

    def teardown_method(self):
        if self.search:
            # Delete the works_index, which is almost always created.
            if self.search.works_index:
                self.search.indices.delete(
                    self.search.works_index, ignore=[404]
                )
            # Delete any other indexes created over the course of the test.
            for index in self.indexes:
                self.search.indices.delete(index, ignore=[404])
            ExternalSearchIndex.reset()
        super(ExternalSearchTest, self).teardown_method()

    def default_work(self, *args, **kwargs):
        """Convenience method to create a work with a license pool
        in the default collection.
        """
        work = self._work(
            *args, with_license_pool=True,
            collection=self._default_collection, **kwargs
        )
        work.set_presentation_ready()
        return work


class EndToEndSearchTest(ExternalSearchTest):
    """Subclasses of this class set up real works in a real
    search index and run searches against it.
    """

    def setup_method(self):
        super(EndToEndSearchTest, self).setup_method()

        # Create some works.
        if not self.search:
            # No search index is configured -- nothing to do.
            return

        self.populate_works()

        # Add all the works created in the setup to the search index.
        SearchIndexCoverageProvider(
            self._db, search_index_client=self.search
        ).run_once_and_update_timestamp()

        # Sleep to give the index time to catch up.
        time.sleep(2)

    def populate_works(self):
        raise NotImplementedError()

    def _assert_works(self, description, expect, actual, should_be_ordered=True):
        "Verify that two lists of works are the same."

        # Get the titles of the works that were actually returned, to
        # make comparisons easier.
        actual_ids = []
        actual_titles = []
        for work in actual:
            actual_titles.append(work.title)
            actual_ids.append(work.id)

        expect_ids = []
        expect_titles = []
        for work in expect:
            expect_titles.append(work.title)
            expect_ids.append(work.id)

        # We compare IDs rather than objects because the Works may
        # actually be WorkSearchResults.
        expect_compare = expect_ids
        actual_compare = actual_ids
        if not should_be_ordered:
            expect_compare = set(expect_compare)
            actual_compare = set(actual_compare)

        assert expect_compare == actual_compare, \
            "%r did not find %d works\n (%s/%s).\nInstead found %d\n (%s/%s)" % (
                description,
                len(expect), ", ".join(map(str, expect_ids)),
                    ", ".join(expect_titles),
                len(actual), ", ".join(map(str, actual_ids)),
                    ", ".join(actual_titles)
            )

    def _expect_results(self, expect, query_string=None, filter=None, pagination=None, **kwargs):
        """Helper function to call query_works() and verify that it
        returns certain work IDs.

        :param ordered: If this is True (the default), then the
        assertion will only succeed if the search results come in in
        the exact order specified in `works`. If this is False, then
        those exact results must come up, but their order is not
        what's being tested.
        """
        if isinstance(expect, Work):
            expect = [expect]
        should_be_ordered = kwargs.pop('ordered', True)
        hits = self.search.query_works(
            query_string, filter, pagination, debug=True, **kwargs
        )

        query_args = (query_string, filter, pagination)
        self._compare_hits(
            expect, hits, query_args, should_be_ordered, **kwargs
        )

    def _expect_results_multi(self, expect, queries, **kwargs):
        """Helper function to call query_works_multi() and verify that it
        returns certain work IDs.

        :param expect: A list of lists of Works that you expect
            to get back from each query in `queries`.
        :param queries: A list of (query string, Filter, Pagination)
            3-tuples.
        :param ordered: If this is True (the default), then the
           assertion will only succeed if the search results come in
           in the exact order specified in `works`. If this is False,
           then those exact results must come up, but their order is
           not what's being tested.
        """
        should_be_ordered = kwargs.pop('ordered', True)
        resultset = list(
            self.search.query_works_multi(
                queries, debug=True, **kwargs
            )
        )
        for i, expect_one_query in enumerate(expect):
            hits = resultset[i]
            query_args = queries[i]
            self._compare_hits(
                expect_one_query, hits, query_args,
                should_be_ordered, **kwargs
            )

    def _compare_hits(self, expect, hits, query_args,
                      should_be_ordered=True, **kwargs):
        query_string, filter, pagination = query_args
        results = [x.work_id for x in hits]
        actual = self._db.query(Work).filter(Work.id.in_(results)).all()
        if should_be_ordered:
            # Put the Work objects in the same order as the IDs returned
            # in `results`.
            works_by_id = dict()
            for w in actual:
                works_by_id[w.id] = w
            actual = [
                works_by_id[result] for result in results
                if result in works_by_id
            ]

        query_args = (query_string, filter, pagination)
        self._assert_works(query_args, expect, actual, should_be_ordered)

        if query_string is None and pagination is None and not kwargs:
            # Only a filter was provided -- this means if we pass the
            # filter into count_works() we'll get all the results we
            # got from query_works(). Take the opportunity to verify
            # that count_works() gives the right answer.
            count = self.search.count_works(filter)
            assert count == len(expect)


class MockCoverageProvider(object):
    """Mixin class for mock CoverageProviders that defines common constants."""
    SERVICE_NAME = "Generic mock CoverageProvider"

    # Whenever a CoverageRecord is created, the data_source of that
    # record will be Project Gutenberg.
    DATA_SOURCE_NAME = DataSource.GUTENBERG

    # For testing purposes, this CoverageProvider will try to cover
    # every identifier in the database.
    INPUT_IDENTIFIER_TYPES = None

    # This CoverageProvider can work with any Collection that supports
    # the OPDS import protocol (e.g. DatabaseTest._default_collection).
    PROTOCOL = ExternalIntegration.OPDS_IMPORT


class InstrumentedCoverageProvider(MockCoverageProvider,
                                   IdentifierCoverageProvider):
    """A CoverageProvider that keeps track of every item it tried
    to cover.
    """

    def __init__(self, *args, **kwargs):
        super(InstrumentedCoverageProvider, self).__init__(*args, **kwargs)
        self.attempts = []

    def process_item(self, item):
        self.attempts.append(item)
        return item


class InstrumentedWorkCoverageProvider(MockCoverageProvider,
                                       WorkCoverageProvider):
    """A WorkCoverageProvider that keeps track of every item it tried
    to cover.
    """
    def __init__(self, _db, *args, **kwargs):
        super(InstrumentedWorkCoverageProvider, self).__init__(_db, *args, **kwargs)
        self.attempts = []

    def process_item(self, item):
        self.attempts.append(item)
        return item

class AlwaysSuccessfulCollectionCoverageProvider(MockCoverageProvider,
                                                 CollectionCoverageProvider):
    """A CollectionCoverageProvider that does nothing and always succeeds."""
    SERVICE_NAME = "Always successful (collection)"

    def process_item(self, item):
        return item


class AlwaysSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    """A CoverageProvider that does nothing and always succeeds."""
    SERVICE_NAME = "Always successful"

class AlwaysSuccessfulWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    """A WorkCoverageProvider that does nothing and always succeeds."""
    SERVICE_NAME = "Always successful (works)"


class AlwaysSuccessfulBibliographicCoverageProvider(
        MockCoverageProvider, BibliographicCoverageProvider):
    """A BibliographicCoverageProvider that does nothing and is always
    successful.

    Note that this only works if you've put a working Edition and
    LicensePool in place beforehand. Otherwise the process will fail
    during handle_success().
    """
    SERVICE_NAME = "Always successful (bibliographic)"

    def process_item(self, identifier):
        return identifier


class NeverSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    """A CoverageProvider that does nothing and always fails."""
    SERVICE_NAME = "Never successful"

    def __init__(self, *args, **kwargs):
        super(NeverSuccessfulCoverageProvider, self).__init__(
            *args, **kwargs
        )
        self.transient = kwargs.get('transient') or False

    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "What did you expect?", self.transient)

class NeverSuccessfulWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    SERVICE_NAME = "Never successful (works)"
    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "What did you expect?", False)

class NeverSuccessfulBibliographicCoverageProvider(
        MockCoverageProvider, BibliographicCoverageProvider):
    """Simulates a BibliographicCoverageProvider that's never successful."""

    SERVICE_NAME = "Never successful (bibliographic)"

    def process_item(self, identifier):
        return self.failure(identifier, "Bitter failure", transient=True)


class BrokenCoverageProvider(InstrumentedCoverageProvider):
    SERVICE_NAME = "Broken"
    def process_item(self, item):
        raise Exception("I'm too broken to even return a CoverageFailure.")


class BrokenBibliographicCoverageProvider(
        BrokenCoverageProvider, BibliographicCoverageProvider):
    SERVICE_NAME = "Broken (bibliographic)"


class TransientFailureCoverageProvider(InstrumentedCoverageProvider):
    SERVICE_NAME = "Never successful (transient)"
    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "Oops!", True)

class TransientFailureWorkCoverageProvider(InstrumentedWorkCoverageProvider):
    SERVICE_NAME = "Never successful (transient, works)"
    def process_item(self, item):
        self.attempts.append(item)
        return self.failure(item, "Oops!", True)

class TaskIgnoringCoverageProvider(InstrumentedCoverageProvider):
    """A coverage provider that ignores all work given to it."""
    SERVICE_NAME = "I ignore all work."
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
        """Queue a response of the type produced by
        Representation.simple_http_get.
        """
        headers = {}
        if media_type:
            headers["content-type"] = media_type
        if other_headers:
            for k, v in list(other_headers.items()):
                headers[k.lower()] = v
        self.responses.append((response_code, headers, content))

    def queue_requests_response(
            self, response_code, media_type="text/html",
            other_headers=None, content=''
    ):
        """Queue a response of the type produced by HTTP.get_with_timeout."""
        headers = dict(other_headers or {})
        if media_type:
            headers['Content-Type'] = media_type
        response = MockRequestsResponse(response_code, headers, content)
        self.responses.append(response)

    def do_get(self, url, *args, **kwargs):
        self.requests.append(url)
        return self.responses.pop(0)

    def do_post(self, url, data, *wargs, **kwargs):
        self.requests.append((url, data))
        return self.responses.pop(0)


class MockRequestsRequest(object):
    """A mock object that simulates an HTTP request from the
    `requests` library.
    """
    def __init__(self, url, method="GET", headers=None):
        self.url = url
        self.method = method
        self.headers = headers or dict()


class MockRequestsResponse(object):
    """A mock object that simulates an HTTP response from the
    `requests` library.
    """
    def __init__(
        self, status_code, headers={}, content=None, url=None, request=None
    ):
        self.status_code = status_code
        self.headers = headers
        self.content = content
        if request and not url:
            url = request.url
        self.url = url or "http://url/"
        self.encoding = "utf-8"
        self.request = request

    def json(self):
        content = self.content
        # The queued content might be a JSON string or it might
        # just be the object you'd get from loading a JSON string.
        if isinstance(content, (str, bytes)):
            content = json.loads(self.content)
        return content

    @property
    def text(self):
        if isinstance(self.content, bytes):
            return self.content.decode("utf8")
        return self.content

    def raise_for_status(self):
        """Null implementation of raise_for_status, a method
        implemented by real requests Response objects.
        """
        pass


@pytest.fixture(autouse=True, scope="session")
def session_fixture():
    # This will make sure we always connect to the test database.
    os.environ['TESTING'] = 'true'

    # Ensure that the log configuration starts in a known state.
    LogConfiguration.initialize(None, testing=True)

    # Drop any existing schema. It will be recreated when
    # SessionManager.initialize() runs.
    engine = SessionManager.engine()
    Base.metadata.drop_all(engine)

    yield

    if 'TESTING' in os.environ:
        del os.environ['TESTING']


def pytest_configure(config):
    # register our custom marks with pytest
    config.addinivalue_line(
        "markers", "elasticsearch: mark test as requiring elasticsearch"
    )
    config.addinivalue_line(
        "markers", "minio: mark test as requiring minio"
    )
