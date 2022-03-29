
# This is kind of janky, but we import the session fixture
# into these tests here. Plugins need absolute import paths
# and we don't have a package structure that gives us a reliable
# import path, so we construct one.
# todo: reorg core file structure so we have a reliable package name
from os.path import abspath, dirname, basename
import random
import os
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm.session import Session

from ..classifier import Classifier
from ..config import Configuration
from ..lane import Lane
from ..log import LogConfiguration

from ..model import (
    Base,
    classifier,
    get_one_or_create,
    site_configuration_has_changed
)
from ..model.admin import Admin
from ..model.classification import (
    Classification,
    Genre,
    Subject
)
from ..model.collection import Collection
from ..model.complaint import Complaint
from ..model.configuration import (
    ConfigurationSetting,
    ExternalIntegration,
    ExternalIntegrationLink
)
from ..model.constants import MediaTypes
from ..model.contributor import Contributor
from ..model.coverage import (
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord
)
from ..model.customlist import CustomList
from ..model.datasource import DataSource
from ..model.edition import Edition
from ..model.identifier import Identifier
from ..model.integrationclient import IntegrationClient
from ..model.library import Library
from ..model.licensing import (
    DeliveryMechanism,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus
)
from ..model.patron import Patron
from ..model.resource import (
    Hyperlink,
    Representation
)
from ..model.work import Work
from ..util.datetime_helpers import utc_now

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

    with engine.connect() as connection:
        Base.metadata.create_all(connection)
        db_session = Session(connection)

        # Populate Genres
        list(DataSource.well_known_sources(db_session))
        Genre.populate_cache(db_session)
        for genre in list(classifier.genres.values()):
            Genre.lookup(db_session, genre, autocreate=True)

        # Populate DeliveryMechanisms
        for content_type, drm_scheme in DeliveryMechanism.default_client_can_fulfill_lookup:
            try:
                mechanism, _ = DeliveryMechanism.lookup(
                    db_session, content_type, drm_scheme
                )
                mechanism.default_client_can_fulfill = True
            except Exception:
                pass

        _, is_new = get_one_or_create(
            db_session, Timestamp, collection=None,
            service=Configuration.SITE_CONFIGURATION_CHANGED,
            create_method_kwargs=dict(finish=utc_now())
        )
        if is_new:
            site_configuration_has_changed(db_session)

        db_session.commit()

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

        Collection.reset_cache()
        ConfigurationSetting.reset_cache()
        DataSource.reset_cache()
        DeliveryMechanism.reset_cache()
        ExternalIntegration.reset_cache()
        Genre.reset_cache()
        Library.reset_cache()

        for key in [
                Configuration.SITE_CONFIGURATION_LAST_UPDATE,
                Configuration.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE
        ]:
            if key in Configuration.instance:
                del(Configuration.instance[key])

        session.close()


@pytest.fixture
def create_admin_user():
    """
    Returns a constructor function for creating an Admin user.
    """
    def _create_admin_user(db_session, email=None):
        email = email or "admin@example.com"
        admin, _ = get_one_or_create(db_session, Admin, email=email)

        return admin

    return _create_admin_user


@pytest.fixture
def create_classification():
    """
    Returns a constructor function for creating a Classification.
    """
    def _create_classification(db_session, identifier, subject, data_source, weight=1):
        classification, _ = get_one_or_create(
            db_session, Classification, identifier=identifier, subject=subject,
            data_source=data_source, weight=weight
        )

        return classification

    return _create_classification


@pytest.fixture
def create_collection():
    """
    Returns a constructor function for creating a Collection.
    """
    def _create_collection(db_session, name=None, protocol=ExternalIntegration.OPDS_IMPORT,
                           external_account_id=None, url=None, username=None,
                           password=None, data_source_name=None):
        name = name or f"Good Reads #{random.randint(1, 9999)}"
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
def create_complaint():
    """
    Returns a constructor function for creating a Complaint.
    """
    def _create_complaint(db_session, license_pool, type, source, detail, resolved=None):
        complaint, _ = Complaint.register(
            license_pool, type, source, detail, resolved
        )

        return complaint

    return _create_complaint


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
def create_coverage_record():
    """
    Returns a constructor function for creating a CoverageRecord.
    """
    def _create_coverage_record(db_session, edition, coverage_source, operation=None,
                                status=CoverageRecord.SUCCESS, collection=None, exception=None):
        if isinstance(edition, Identifier):
            identifier = edition
        else:
            identifier = edition.primary_identifier

        record, _ = get_one_or_create(
            db_session, CoverageRecord,
            identifier=identifier,
            data_source=coverage_source,
            operation=operation,
            collection=collection,
            create_method_kwargs=dict(
                timestamp=utc_now(),
                status=status,
                exception=exception
            )
        )

        return record

    return _create_coverage_record


@pytest.fixture
def create_customlist(create_edition, create_work):
    """
    Returns a constructor function for creating a CustomList.
    """
    def _create_customlist(db_session, foreign_identifier=None, name=None,
                           data_source_name=DataSource.NYT, num_entries=1,
                           entries_exist_as_works=True):
        data_source = DataSource.lookup(db_session, data_source_name)
        foreign_identifier = foreign_identifier or str(random.randint(1, 9999))
        name = name or str(random.randint(1, 9999))
        now = utc_now()

        customlist, _ = get_one_or_create(
            db_session, CustomList,
            create_method_kwargs=dict(
                created=now,
                updated=now,
                name=name,
                description=str(random.randint(1, 9999)),
            ),
            data_source=data_source,
            foreign_identifier=foreign_identifier
        )

        editions = []
        for i in range(num_entries):
            if entries_exist_as_works:
                work = create_work(db_session, with_open_access_download=True)
                edition = work.presentation_edition
                db_session.commit()
            else:
                edition = create_edition(db_session, data_source_name, title="Item %s" % i)
                edition.permanent_work_id = f"Permanent work ID {random.randint(1, 9999)}"

            customlist.add_entry(edition, "Annotation %s" % i, first_appearance=now)
            editions.append(edition)

        return customlist, editions

    return _create_customlist


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
            title = f"Test Book {random.randint(1, 9999)}"
        wr.title = str(title)
        wr.medium = Edition.BOOK_MEDIUM

        if series:
            wr.series = series

        if language:
            wr.language = language

        if authors is None:
            authors = f"Test Author {random.randint(1, 9999)}"

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
                db_session,
                wr,
                data_source_name=data_source_name,
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
def create_external_integration_link(create_externalintegration):
    """
    Returns a constructor function for creating an ExternalIntegrationLink
    """
    def _external_integration_link(db_session, integration=None, library=None,
                                   other_integration=None, purpose="covers_mirror"):

        integration = integration or create_externalintegration(db_session, "some protocol")
        other_integration = other_integration or create_externalintegration(db_session, "some other protocol")

        library_id = library.id if library else None

        external_integration_link, _ = get_one_or_create(
            db_session, ExternalIntegrationLink,
            library_id=library_id,
            external_integration_id=integration.id,
            other_integration_id=other_integration.id,
            purpose=purpose
        )

        return external_integration_link

    return _external_integration_link


@pytest.fixture
def create_identifier():
    """
    Returns a constructor function for creating an Identifier
    """
    def _create_identifier(db_session, identifier_type=Identifier.GUTENBERG_ID, foreign_id=None):
        if foreign_id:
            id = foreign_id
        else:
            id = str(random.randint(1, 9999))

        identifier, _ = Identifier.for_foreign_id(db_session, identifier_type, id)
        return identifier

    return _create_identifier


@pytest.fixture
def create_integration_client():
    """
    Returns a constructor function for creating an IntegrationClient.
    """
    def _create_integration_client(db_session, url=None, shared_secret=None):
        url = url or f"http://example.com/{random.randint(1,9999)}"
        secret = shared_secret or "secret"
        integration_client, _ = get_one_or_create(
            db_session, IntegrationClient, shared_secret=secret,
            create_method_kwargs=dict(url=url)
        )

        return integration_client

    return _create_integration_client


@pytest.fixture
def create_lane(create_library):
    """"
    Returns a constructor function for creating a Lane.
    """
    def _create_lane(db_session, display_name="default", library=None, parent=None, genres=None,
                     languages=None, fiction=None, inherit_parent_restrictions=True):

        display_name = display_name
        library = library or create_library(db_session)
        lane, is_new = get_one_or_create(
            db_session, Lane,
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
def create_license():
    """
    Returns a constructor function for creating a License.
    """
    def _create_license(db_session, pool, identifier=None, checkout_url=None, status_url=None,
                        expires=None, remaining_checkouts=None, concurrent_checkouts=None):
        identifier = identifier or str(random.randint(1, 9999))
        checkout_url = checkout_url or str(random.randint(1, 9999))
        status_url = status_url or str(random.randint(1, 9999))

        license, _ = get_one_or_create(
            db_session, License, identifier=identifier, license_pool=pool,
            checkout_url=checkout_url,
            status_url=status_url, expires=expires,
            remaining_checkouts=remaining_checkouts,
            concurrent_checkouts=concurrent_checkouts,
        )

        return license

    return _create_license


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
            url = "http://example.com/" + str(random.randint(1, 999))
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
def create_patron(create_library):
    """
    Constructor function for creating a Patron.
    """
    def _create_patron(db_session, external_identifier=None, library=None):
        external_identifier = external_identifier or str(random.randint(1, 9999))
        library = library or create_library(db_session)

        patron, _ = get_one_or_create(
            db_session, Patron,
            external_identifier=external_identifier,
            library=library
        )

        return patron

    return _create_patron


@pytest.fixture
def create_representation():
    """
    Returns a constructor function for creating a Representation.
    """
    def _create_representation(db_session, url=None, media_type=None, content=None, mirrored=False):
        url = url or "http://example.com/" + str(random.randint(1, 9999))
        repr, _ = get_one_or_create(db_session, Representation, url=url)
        repr.media_type = media_type
        if media_type and content:
            if isinstance(content, str):
                content = content.encode("utf8")
            repr.content = content
            repr.fetched_at = utc_now()
            if mirrored:
                repr.mirror_url = "http://example.com/" + str(random.randint(1, 9999))
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

        if not title:
            title = f"Test Book {random.randint(1, 9999)}"

        language = language or "eng"
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

        work, _ = get_one_or_create(
            db_session, Work,
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


@pytest.fixture
def create_work_coverage_record():
    """
    Returns a constructor function for creating a WorkCoverageRecord.
    """
    def _create_work_coverage_record(db_session, work, operation=None, status=CoverageRecord.SUCCESS):
        record, _ = get_one_or_create(
            db_session, WorkCoverageRecord,
            work=work,
            operation=operation,
            create_method_kwargs=dict(
                timestamp=utc_now(),
                status=status,
            )
        )

        return record

    return _create_work_coverage_record


@pytest.fixture
def default_library(db_session, create_collection, create_library):
    library = create_library(db_session, name="default", short_name="default")
    collection = create_collection(db_session, name="Default Collection")
    integration = collection.create_external_integration(ExternalIntegration.OPDS_IMPORT)
    integration.goal = ExternalIntegration.LICENSE_GOAL

    if collection not in library.collections:
        library.collections.append(collection)

    return library


@pytest.fixture
def get_sample_cover_path():
    """The path to the sample cover with the given filename."""
    def _get_sample_cover_path(name):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "covers")
        return os.path.join(resource_path, name)

    return _get_sample_cover_path


@pytest.fixture
def get_sample_cover_representation(db_session, create_representation, get_sample_cover_path):
    """A Representation of the sample cover with the given filename."""
    def _get_sample_cover_representation(name):
        sample_cover_path = get_sample_cover_path(name)
        return create_representation(
            db_session,
            media_type="image/png",
            content=open(sample_cover_path, 'rb').read()
        )

    return _get_sample_cover_representation


@pytest.fixture
def get_sample_ecosystem(create_edition, create_work):
    """ Creates an ecosystem of some sample work, pool, edition, and author
    objects that all know each other.
    """
    def _get_sample_ecosystem(db_session):
        # make some authors
        [bob], _ = Contributor.lookup(db_session, "Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()
        [alice], _ = Contributor.lookup(db_session, "Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()

        edition_std_ebooks, pool_std_ebooks = create_edition(
            db_session,
            DataSource.STANDARD_EBOOKS, Identifier.URI,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_std_ebooks.title = "The Standard Ebooks Title"
        edition_std_ebooks.subtitle = "The Standard Ebooks Subtitle"
        edition_std_ebooks.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_git, pool_git = create_edition(
            db_session, DataSource.PROJECT_GITENBERG, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_git.title = "The GItenberg Title"
        edition_git.subtitle = "The GItenberg Subtitle"
        edition_git.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition_git.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_gut, pool_gut = create_edition(
            db_session, DataSource.GUTENBERG, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition_gut.title = "The GUtenberg Title"
        edition_gut.subtitle = "The GUtenberg Subtitle"
        edition_gut.add_contributor(bob, Contributor.AUTHOR_ROLE)

        work = create_work(db_session, presentation_edition=edition_git)

        for p in pool_gut, pool_std_ebooks:
            work.license_pools.append(p)

        work.calculate_presentation()

        return (work, pool_std_ebooks, pool_git, pool_gut,
                edition_std_ebooks, edition_git, edition_gut, alice, bob)

    return _get_sample_ecosystem
