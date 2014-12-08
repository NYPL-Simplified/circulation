from datetime import datetime
from nose.tools import set_trace
from sqlalchemy.orm.session import Session
from model import (
    Base,
    Contributor,
    CoverageRecord,
    DataSource,
    Genre,
    LicensePool,
    Patron,
    Resource,
    Identifier,
    Edition,
    Work,
    get_one_or_create
)
from classifier import Classifier

class DatabaseTest(object):

    DBInfo = None

    def setup(self):
        self.__transaction = self.DBInfo.connection.begin_nested()
        self._db = Session(self.DBInfo.connection)
        self.counter = 0

    def teardown(self):
        self._db.close()
        self.__transaction.rollback()

    @property
    def _id(self):
        self.counter += 1
        return self.counter

    @property
    def _str(self):
        return str(self._id)

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

    def _contributor(self, name=None):
        name = name or self._str
        return get_one_or_create(self._db, Contributor, name=name)

    def _identifier(self, identifier_type=Identifier.GUTENBERG_ID):
        id = self._str
        return Identifier.for_foreign_id(self._db, identifier_type, id)[0]

    def _edition(self, data_source_name=DataSource.GUTENBERG,
                    identifier_type=Identifier.GUTENBERG_ID,
                    with_license_pool=False, with_open_access_download=False,
                    title=None, language=None, authors=None):
        id = self._str
        source = DataSource.lookup(self._db, data_source_name)
        wr = Edition.for_foreign_id(
            self._db, source, identifier_type, id)[0]
        if title:
            wr.title = title
        if language:
            wr.language = language
        if authors:
            wr.add_contributor(authors, Contributor.PRIMARY_AUTHOR_ROLE)
            
        if with_license_pool or with_open_access_download:
            pool = self._licensepool(wr, data_source_name=data_source_name,
                                     with_open_access_download=with_open_access_download)                
            return wr, pool
        return wr

    def _work(self, title=None, authors=None, genre=None, language=None,
              audience=None, fiction=True, with_license_pool=False, 
              with_open_access_download=False, quality=0.5,
              primary_edition=None):
        if with_open_access_download:
            with_license_pool = True
        language = language or "eng"
        title = title or self._str
        genre = genre or self._str
        audience = audience or Classifier.AUDIENCE_ADULT
        if fiction is None:
            fiction = True
        new_edition = False
        if not primary_edition:
            new_edition = True
            primary_edition = self._edition(
                title=title, language=language,
                authors=authors,
                with_license_pool=with_license_pool,
                with_open_access_download=with_open_access_download)
        if with_license_pool:
            primary_edition, pool = primary_edition
        if new_edition:
            primary_edition.calculate_presentation()
        work, ignore = get_one_or_create(
            self._db, Work, create_method_kwargs=dict(
                audience=audience,
                fiction=fiction,
                quality=quality), id=self._id)
        if not isinstance(genre, Genre):
            genre, ignore = Genre.lookup(self._db, genre, autocreate=True)
        work.genres = [genre]
        if with_license_pool:
            work.license_pools.append(pool)
        work.editions = [primary_edition]
        primary_edition.is_primary_for_work = True
        return work

    def _coverage_record(self, edition, coverage_source):
        record, ignore = get_one_or_create(
            self._db, CoverageRecord,
            identifier=edition.primary_identifier,
            data_source=coverage_source,
            create_method_kwargs = dict(date=datetime.utcnow()))
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
            pool.identifier.add_resource(
                Resource.OPEN_ACCESS_DOWNLOAD, "http://foo.com/" + self._str,
                source, pool, "application/epub+zip")

        return pool


from nose.tools import set_trace
import os
from sqlalchemy.orm.session import Session

from model import (
       Patron,
    SessionManager,
    get_one_or_create,
)

def _setup(dbinfo):
    # Connect to the database and create the schema within a transaction
    engine, connection = SessionManager.initialize(os.environ['DATABASE_URL_TEST'])
    Base.metadata.drop_all(connection)
    Base.metadata.create_all(connection)
    dbinfo.engine = engine
    dbinfo.connection = connection
    dbinfo.transaction = connection.begin_nested()

    db = Session(dbinfo.connection)
    SessionManager.initialize_data(db)

    # Test data: Create the patron used by the dummy authentication
    # mechanism.
    get_one_or_create(db, Patron, authorization_identifier="200",
                      create_method_kwargs=dict(external_identifier="200200200"))
    db.commit()

    print "Connection is now %r" % dbinfo.connection
    print "Transaction is now %r" % dbinfo.transaction

def _teardown(dbinfo):
    # Roll back the top level transaction and disconnect from the database
    dbinfo.transaction.rollback()
    dbinfo.connection.close()
    dbinfo.engine.dispose()
