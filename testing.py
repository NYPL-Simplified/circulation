from datetime import (
    datetime,
    timedelta,
)
import logging
import os
import shutil
import tempfile
from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from config import Configuration
os.environ['TESTING'] = 'true'
from model import (
    Base,
    Classification,
    Collection,
    Complaint,
    Contributor,
    CoverageRecord,
    CustomList,
    DataSource,
    DeliveryMechanism,
    Edition,
    Genre,
    Hyperlink,
    Identifier,
    LicensePool,
    Patron,
    Representation,
    Resource,
    SessionManager,
    Subject,
    Work,
    WorkCoverageRecord,
    UnresolvedIdentifier,
    get_one_or_create,
    production_session
)
from classifier import Classifier
from coverage import (
    CoverageProvider,
    CoverageFailure,
)
from external_search import DummyExternalSearchIndex
import mock
import model

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

    # Create the patron used by the dummy authentication mechanism.
    # TODO: This can be probably be moved to circulation.
    get_one_or_create(
        _db, Patron, authorization_identifier="200",
        create_method_kwargs=dict(external_identifier="200200200")
    )
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

    def teardown(self):
        # Close the session.
        self._db.close()

        # Roll back all database changes that happened during this
        # test, whether in the session that was just closed or some
        # other session.
        self.transaction.rollback()
        self.search_mock.stop()

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

    @property
    def default_patron(self):
        """The patron automatically created for the test dataset and 
        used by default when authenticating.
        """
        return self._db.query(Patron).filter(
            Patron.authorization_identifier=="200").one()

    def _patron(self, external_identifier=None):
        external_identifier = external_identifier or self._str
        return get_one_or_create(
            self._db, Patron, external_identifier=external_identifier)[0]

    def _contributor(self, name=None, **kw_args):
        name = name or self._str
        return get_one_or_create(self._db, Contributor, name=unicode(name), **kw_args)

    def _identifier(self, identifier_type=Identifier.GUTENBERG_ID):
        id = self._str
        return Identifier.for_foreign_id(self._db, identifier_type, id)[0]

    def _edition(self, data_source_name=DataSource.GUTENBERG,
                    identifier_type=Identifier.GUTENBERG_ID,
                    with_license_pool=False, with_open_access_download=False,
                    title=None, language="eng", authors=None, identifier_id=None):
        id = identifier_id or self._str
        source = DataSource.lookup(self._db, data_source_name)
        wr = Edition.for_foreign_id(
            self._db, source, identifier_type, id)[0]
        if not title:
            title = self._str
        wr.title = unicode(title)
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
            wr.add_contributor(unicode(authors[0]), Contributor.AUTHOR_ROLE)
            
        if with_license_pool or with_open_access_download:
            pool = self._licensepool(wr, data_source_name=data_source_name,
                                     with_open_access_download=with_open_access_download)                
            return wr, pool
        return wr

    def _work(self, title=None, authors=None, genre=None, language=None,
              audience=None, fiction=True, with_license_pool=False, 
              with_open_access_download=False, quality=0.5,
              primary_edition=None):
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
        if not primary_edition:
            new_edition = True
            primary_edition = self._edition(
                title=title, language=language,
                authors=authors,
                with_license_pool=with_license_pool,
                with_open_access_download=with_open_access_download,
                data_source_name=data_source_name
            )
            if with_license_pool:
                primary_edition, pool = primary_edition
        else:
            pool = primary_edition.license_pool
        if with_open_access_download:
            pool.open_access = True
            primary_edition.set_open_access_link()
        if new_edition:
            primary_edition.calculate_presentation()
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
        work.editions = [primary_edition]
        primary_edition.is_primary_for_work = True
        work.primary_edition = primary_edition
        if pool != None:
            work.license_pools.append(pool)
            # This is probably going to be used in an OPDS feed, so
            # fake that the work is presentation ready.
            work.presentation_ready = True
            work.calculate_opds_entries(verbose=False)
        return work

    def _coverage_record(self, edition, coverage_source, operation=None):
        record, ignore = get_one_or_create(
            self._db, CoverageRecord,
            identifier=edition.primary_identifier,
            data_source=coverage_source,
            operation=operation,
            create_method_kwargs = dict(timestamp=datetime.utcnow()))
        return record

    def _work_coverage_record(self, work, operation=None):
        record, ignore = get_one_or_create(
            self._db, WorkCoverageRecord,
            work=work,
            operation=operation,
            create_method_kwargs = dict(timestamp=datetime.utcnow())
        )
        return record

    def _licensepool(self, edition, open_access=True, 
                     data_source_name=DataSource.GUTENBERG,
                     with_open_access_download=False):
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
                None
            )
            pool.licenses_owned = pool.licenses_available = 1
        return pool

    def _representation(self, url=None, media_type=None, content=None,
                        mirrored=False):
        url = url or "http://foo.com/" + self._str
        repr, is_new = get_one_or_create(
            self._db, Representation, url=url)
        if media_type and content:
            repr.media_type=media_type
            repr.content = content
            repr.fetched_at = datetime.utcnow()
            if mirrored:
                repr.mirror_url = "http://foo.com/" + self._str
                repr.mirrored_at = datetime.utcnow()            
        return repr, is_new

    def _unresolved_identifier(self, identifier=None):
        identifier = identifier
        if not identifier:
            identifier  = self._identifier()
        return UnresolvedIdentifier.register(self._db, identifier, force=True)

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
                edition = work.editions[0]
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

    def _collection(self, name=u"Faketown Public Library"):
        source, ignore = get_one_or_create(self._db, DataSource, name=name)
        return get_one_or_create(
            self._db, Collection, name=name, data_source=source,
            client_id=u"abc", client_secret=u"def"
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

    def run_once(self, offset):
        super(InstrumentedCoverageProvider, self).run_once(offset)
        return None

    def process_item(self, item):
        self.attempts.append(item)
        return item

class AlwaysSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    """A CoverageProvider that does nothing and always succeeds."""


class NeverSuccessfulCoverageProvider(InstrumentedCoverageProvider):
    def process_item(self, item):
        self.attempts.append(item)
        return CoverageFailure(self, item, "What did you expect?", False)

class BrokenCoverageProvider(InstrumentedCoverageProvider):
    def process_item(self, item):
        raise Exception("I'm too broken to even return a CoverageFailure.")

class TransientFailureCoverageProvider(InstrumentedCoverageProvider):
    def process_item(self, item):
        self.attempts.append(item)
        return CoverageFailure(self, item, "Oops!", True)

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
        headers = {"content-type": media_type}
        if other_headers:
            for k, v in other_headers.items():
                headers[k.lower()] = v
        self.responses.append((response_code, headers, content))

    def do_get(self, url, headers, **kwargs):
        self.requests.append(url)
        return self.responses.pop()
