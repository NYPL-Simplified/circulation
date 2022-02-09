
# This is kind of janky, but we import the session fixture
# into these tests here. Plugins need absolute import paths
# and we don't have a package structure that gives us a reliable
# import path, so we construct one.
# todo: reorg core file structure so we have a reliable package name
from os.path import abspath, dirname, basename
import random
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm.session import Session

from core.classifier import Classifier
from core.config import Configuration
from core.lane import Lane

from core.model import Base, classifier, get_one_or_create
from core.model.admin import Admin
from core.model.classification import Genre, Subject
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from core.model.constants import MediaTypes
from core.model.contributor import Contributor
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.identifier import Identifier
from core.model.library import Library
from core.model.licensing import DeliveryMechanism, LicensePool, LicensePoolDeliveryMechanism, RightsStatus
from core.model.resource import Hyperlink, Representation
from core.model.work import Work
from core.util.datetime_helpers import utc_now

# Pull in the session_fixture defined in core/testing.py
# which does the database setup and initialization
pytest_plugins = ["{}.testing".format(basename(dirname(dirname(abspath(__file__)))))]


@pytest.fixture(autouse=True, scope="session")
def init_test_db():
    db_url = Configuration.database_url()
    engine = create_engine(db_url)
    for table in reversed(Base.metadata.sorted_tables):
        try:
            engine.execute(table.delete())
        except ProgrammingError:
            pass

    with engine.connect() as conn:
        Base.metadata.create_all(conn)

    engine.dispose()


@pytest.fixture
def db_engine():
    db_url = Configuration.database_url()
    engine = create_engine(db_url)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine):
    with db_engine.connect() as connection:
        transaction = connection.begin_nested()
        session = Session(connection)
        yield session
        transaction.rollback()
        session.close()


@pytest.fixture
def populate_genres(db_session):
    # This probably needs a better name
    # This was lifted from core/model/__init__.py::SessionManager::initialize_data
    list(DataSource.well_known_sources(db_session))

    Genre.populate_cache(db_session)
    for genre in list(classifier.genres.values()):
        Genre.lookup(db_session, genre, autocreate=True)
    yield
    Genre.reset_cache()
    DataSource.reset_cache()


@pytest.fixture
def create_admin_user():
    """
    Returns a constructor function for creating an Admin user.
    """
    def _create_admin_user(db_session, email=None):
        email = email or "admin@nypl.org"
        admin, _ = get_one_or_create(db_session, Admin, email=email)

        return admin

    return _create_admin_user


@pytest.fixture
def create_collection():
    """
    Returns a constructor function for creating a Collection.
    """
    def _create_collection(db_session, name=None, protocol=ExternalIntegration.OPDS2_IMPORT,
                           external_account_id=None, url=None, username=None,
                           password=None, data_source_name=None):
        name = name or "Good Reads"
        collection, _ = get_one_or_create(db_session, Collection, name=name)
        collection.external_account_id = external_account_id
        integration = collection.create_external_integration(protocol)
        integration.goal = ExternalIntegration.LICENSE_GOAL
        integration.url = url
        integration.username = username
        integration.password = password

        if data_source_name:
            collection.data_source = data_source_name

        return collection

    return _create_collection


@pytest.fixture
def create_contributor():
    """
    Returns a constructor function for creating a Contributor.
    """
    def _create_contributor(db_session, sort_name=None, name=None, **kwargs):
        name = sort_name or name
        contributor, _ = get_one_or_create(db_session, Contributor, sort_name=name, **kwargs)

        return contributor
    
    return _create_contributor


@pytest.fixture
def create_edition(create_licensepool):
    """
    Returns a constructor function for creating an Edition.
    """
    def _create_edition(db_session, data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID,
                        with_license_pool=False, with_open_access_download=False, title=None, language="eng",
                        authors=None, identifier_id=None, series=None, collection=None, publication_date=None,
                        self_hosted=False, unlimited_access=False):
        id = identifier_id or random.randint(1, 9999)
        id = str(id)
        source = DataSource.lookup(db_session, data_source_name)
        if not source:
            # source can't be None, so for now this is the sensible default
            source, _ = get_one_or_create(db_session, DataSource, name=data_source_name)
        wr = Edition.for_foreign_id(db_session, source, identifier_type, id)[0]
        if not title:
            title = "Test Book"
        wr.title = str(title)
        wr.medium = Edition.BOOK_MEDIUM
        if series:
            wr.series = series
        if language:
            wr.language = language
        if authors is None:
            authors = "Test Author"
        if isinstance(authors, str):
            authors = [authors]
        if authors:
            primary_author_name = str(authors[0])
            contributor = wr.add_contributor(primary_author_name, Contributor.PRIMARY_AUTHOR_ROLE)
            if not contributor.display_name and ',' not in primary_author_name:
                contributor.display_name = primary_author_name
            wr.author = primary_author_name

        for author in authors[1:]:
            wr.add_contributor(str(author), Contributor.AUTHOR_ROLE)
        if publication_date:
            wr.published = publication_date

        if with_license_pool or with_open_access_download:
            pool = create_licensepool(
                db_session, wr, data_source_name=data_source_name,
                with_open_access_download=with_open_access_download,
                collection=collection, self_hosted=self_hosted,
                unlimited_access=unlimited_access
            )
            pool.set_presentation_edition()
            return wr, pool

        return wr

    return _create_edition


@pytest.fixture
def create_externalintegration():
    """
    Returns a constructor function for creating an ExternalIntegration.
    """
    def _create_externalintegration(db_session, protocol, goal=None, settings=None, libraries=None, **kwargs):
        integration = None
        if not libraries:
            integration, _ = get_one_or_create(
                db_session, ExternalIntegration, protocol=protocol, goal=goal
            )
        else:
            if not isinstance(libraries, list):
                libraries = [libraries]
            
            for library in libraries:
                integration = ExternalIntegration.lookup(
                    db_session, protocol, goal, library=library
                )
                if integration:
                    break
            
            if not integration:
                integration = ExternalIntegration(
                    protocol=protocol, goal=goal,
                )
                integration.libraries.extend(libraries)

        for attr, value in list(kwargs.items()):
            setattr(integration, attr, value)
        
        settings = settings or dict()
        for key, value in list(settings.items()):
            integration.set_setting(key, value)
        
        return integration
    
    return _create_externalintegration


@pytest.fixture
def create_identifier(db_session):
    def _create_identifier(identifier_type=Identifier.GUTENBERG_ID, foreign_id=None):
        if foreign_id:
            id = foreign_id
        else:
            id = str(random.randint(1, 9999))

        identifier, _ = Identifier.for_foreign_id(db_session, identifier_type, id)
        return identifier

    return _create_identifier


@pytest.fixture
def create_lane(create_library):
    """"
    Returns a constructor function for creating a Lane.
    """
    def _create_lane(db_session, display_name="default", library=None, parent=None, genres=None,
                     languages=None, fiction=None, inherit_parent_restrictions=True):

        display_name = display_name
        library = library or create_library(db_session)
        lane, is_new = get_one_or_create(db_session, Lane,
                                    library=library,
                                    parent=parent, display_name=display_name,
                                    fiction=fiction,
                                    inherit_parent_restrictions=inherit_parent_restrictions
        )
        if is_new and parent:
            lane.priority = len(parent.sublanes) - 1

        if genres:
            if not isinstance(genres, list):
                genres = [genres]
            for genre in genres:
                if isinstance(genre, str):
                    genre, _ = Genre.lookup(db_session, genre)
                lane.genres.append(genre)

        if languages:
            if not isinstance(languages, list):
                languages = [languages]
            lane.languages = languages
        
        return lane
    
    return _create_lane


@pytest.fixture
def create_library():
    """
    Returns a constructor function for creating a Library.
    """
    def _create_library(db_session, name="default", short_name="default"):
        create_kwargs = dict(
            uuid=str(uuid.uuid4()),
            name=name,
        )
        library, _ = get_one_or_create(db_session, Library, create_method_kwargs=create_kwargs, short_name=short_name)

        return library

    return _create_library


@pytest.fixture
def create_licensepool(create_collection, create_representation):
    """
    Returns a constructor function for creating a LicensePool.
    """
    def _create_licensepool(db_session, edition, open_access=True, data_source_name=DataSource.GUTENBERG,
                            with_open_access_download=False, set_edition_as_presentation=False,
                            collection=None, self_hosted=False, unlimited_access=False):
        source = DataSource.lookup(db_session, data_source_name)
        collection = collection or create_collection(db_session)
        pool, _ = get_one_or_create(
            db_session, LicensePool,
            create_method_kwargs=dict(open_access=open_access),
            identifier=edition.primary_identifier,
            data_source=source,
            collection=collection,
            availability_time=utc_now(),
            self_hosted=self_hosted,
            unlimited_access=unlimited_access
        )

        if set_edition_as_presentation:
            pool.presentation_edition = edition

        if with_open_access_download:
            pool.open_access = True
            url = "http://foo.com/" + str(random.randint(1, 999))
            media_type = MediaTypes.EPUB_MEDIA_TYPE
            link, _ = pool.identifier.add_link(
                Hyperlink.OPEN_ACCESS_DOWNLOAD, url, source, media_type
            )

            # Add a Deliverymechanism for this download
            pool.set_delivery_mechanism(
                media_type,
                DeliveryMechanism.NO_DRM,
                RightsStatus.GENERIC_OPEN_ACCESS,
                link.resource,
            )

            representation = create_representation(db_session, url, media_type, "Dummy content", mirrored=True)
            link.resource.representation = representation
        else:
            pool.set_delivery_mechanism(
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                RightsStatus.UNKNOWN,
                None
            )

            if not unlimited_access:
                pool.licenses_owned = pool.licenses_available = 1
        
        return pool

    return _create_licensepool


@pytest.fixture
def create_licensepooldeliverymechanism():
    """
    Returns a constructor function for creating a LicensePoolDeliveryMechanism.
    """
    def _create_licensepooldeliverymechanism(license_pool):
        data_source = license_pool.data_source
        identifier = license_pool.identifier
        content_type = Representation.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM

        return LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.IN_COPYRIGHT
        )

    return _create_licensepooldeliverymechanism


@pytest.fixture
def create_representation():
    """
    Returns a constructor function for creating a Representation.
    """
    def _create_representation(db_session, url=None, media_type=None, content=None, mirrored=False):
        url = url or "http://foo.com/" + str(random.randint(1, 9999))
        repr, _ = get_one_or_create(db_session, Representation, url=url)
        repr.media_type = media_type
        if media_type and content:
            if isinstance(content, str):
                content = content.encode("utf8")
            repr.content = content
            repr.fetched_at = utc_now()
            if mirrored:
                repr.mirror_url = "http://foo.com/" + str(random.randint(1, 9999))
                repr.mirrored_at = utc_now()

            return repr

    return _create_representation


@pytest.fixture
def create_subject():
    """
    Returns a constructor function for creating a Subject.
    """
    def _create_subject(db_session, type, identifier):
        subject, _ = get_one_or_create(
            db_session, Subject, type=type, identifier=identifier
        )
        return subject

    return _create_subject

@pytest.fixture
def create_work(create_edition):
    """
    Returns a constructor function for creating a Work.
    """
    def _create_work(db_session, title=None, authors=None, genre=None, language=None, audience=None,
                     fiction=None, with_license_pool=False, with_open_access_download=False,
                     quality=0.5, series=None, presentation_edition=None, collection=None,
                     data_source_name=None, self_hosted=False, unlimited_access=False):
        pools = []
        if with_open_access_download:
            with_license_pool = True
        language = language or "eng"
        if not title:
            title = "Test Book"
        title = str(title)
        audience = audience or Classifier.AUDIENCE_ADULT

        if audience == Classifier.AUDIENCE_CHILDREN and not data_source_name:
            data_source_name = DataSource.OVERDRIVE
        elif not data_source_name:
            data_source_name = DataSource.GUTENBERG

        if fiction is None:
            fiction = True

        if not presentation_edition:
            presentation_edition = create_edition(
                db_session,
                title=title,
                language=language,
                authors=authors,
                with_license_pool=with_license_pool,
                with_open_access_download=with_open_access_download,
                data_source_name=data_source_name,
                series=series,
                collection=collection,
                self_hosted=self_hosted,
                unlimited_access=unlimited_access,
                identifier_id=str(random.randint(1, 9999))
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

        work, _ = get_one_or_create(db_session, Work,
            create_method_kwargs=dict(audience=audience, fiction=fiction, quality=quality),
            id=random.randint(1, 9999)
        )
        if genre:
            if not isinstance(genre, Genre):
                genre, _ = Genre.lookup(db_session, genre, autocreate=True)
            work.genres = [genre]
        work.random = 0.5
        work.set_presentation_edition(presentation_edition)

        if pools:
            if not work.license_pools:
                for pool in pools:
                    work.license_pools.append(pool)

            for pool in pools:
                pool.set_presentation_edition()

            work.presentation_ready = True
            work.calculate_opds_entries(verbose=False)
        
        return work

    return _create_work
