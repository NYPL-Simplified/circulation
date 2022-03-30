# encoding: utf-8
from mock import MagicMock, PropertyMock
import pytest
import datetime
from sqlalchemy.exc import IntegrityError

from ...mock_analytics_provider import MockAnalyticsProvider
from ...model import create
from ...model.circulationevent import CirculationEvent
from ...model.collection import CollectionMissing
from ...model.complaint import Complaint
from ...model.constants import MediaTypes
from ...model.contributor import Contributor
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier
from ...model.licensing import (
    DeliveryMechanism,
    Hold,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    RightsStatus,
)
from ...model.resource import Hyperlink, Representation
from ...util.datetime_helpers import utc_now


class TestDeliveryMechanism:

    @pytest.fixture
    def delivery_mechanisms(self, db_session):
        """
        Fixture to initialize various DeliveryMechanisms:
        - EPUB with no DRM
        - EPUB with Adobe DRM
        - Streaming Text with OverDrive DRM
        - Audiobook with Feedbooks DRM
        """
        self.epub_no_drm, _ = DeliveryMechanism.lookup(
            db_session, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        self.epub_adobe_drm, _ = DeliveryMechanism.lookup(
            db_session, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        self.overdrive_streaming_text, _ = DeliveryMechanism.lookup(
            db_session, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM)
        self.audiobook_drm_scheme, _ = DeliveryMechanism.lookup(
            db_session, Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM
        )

    def test_implicit_medium(self, delivery_mechanisms):
        """
        GIVEN: A non-audiobook DeliveryMechanism
        WHEN:  Inferring the implicit medium for the DeliveryMechanism
        THEN:  "Book" medium is the implicit medium
        """
        assert Edition.BOOK_MEDIUM == self.epub_no_drm.implicit_medium
        assert Edition.BOOK_MEDIUM == self.epub_adobe_drm.implicit_medium
        assert Edition.BOOK_MEDIUM == self.overdrive_streaming_text.implicit_medium

    def test_is_media_type(self):
        """
        GIVEN: A DeliveryMechanism
        WHEN:  Checking a media type
        THEN:  Returns the validity of the media type.
        """
        assert DeliveryMechanism.is_media_type(None) is False
        assert DeliveryMechanism.is_media_type(Representation.EPUB_MEDIA_TYPE) is True
        assert DeliveryMechanism.is_media_type(DeliveryMechanism.KINDLE_CONTENT_TYPE) is False
        assert DeliveryMechanism.is_media_type(DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE) is False

    def test_is_streaming(self, delivery_mechanisms):
        """
        GIVEN: A DeliveryMechanism
        WHEN:  Checking if the media type is a media type for streaming
        THEN:  Returns True/False if the media type is a media type for streaming
        """
        assert self.epub_no_drm.is_streaming is False
        assert self.epub_adobe_drm.is_streaming is False
        assert self.overdrive_streaming_text.is_streaming is True

    def test_drm_scheme_media_type(self, delivery_mechanisms):
        """
        GIVEN: A DeliveryMechanism
        WHEN:  Getting the media type for the DeliveryMechanism's DRM scheme
        THEN:  Returns the DRM scheme if possible otherwise returns None
        """
        assert self.epub_no_drm.drm_scheme_media_type is None
        assert self.epub_adobe_drm.drm_scheme_media_type == DeliveryMechanism.ADOBE_DRM
        assert self.overdrive_streaming_text.drm_scheme_media_type is None

    def test_content_type_media_type(self, delivery_mechanisms):
        """
        GIVEN: A DeliveryMechanism
        WHEN:  Getting the media type for the DeliveryMechanism's content type
        THEN:  Returns the media type if possible otherwise returns None
        """
        assert Representation.EPUB_MEDIA_TYPE == self.epub_no_drm.content_type_media_type
        assert Representation.EPUB_MEDIA_TYPE == self.epub_adobe_drm.content_type_media_type
        assert (Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE ==
            self.overdrive_streaming_text.content_type_media_type)
        assert (Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE + DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_PROFILE ==
            self.audiobook_drm_scheme.content_type_media_type)

    def test_default_fulfillable(self, db_session):
        """
        GIVEN: A content type and drm scheme
        WHEN:  Creating a new DeliveryMechanism
        THEN:  By default the new DeliveryMechanism cannot fulfill a book
        """
        # Try some well-known media type/DRM combinations known to be
        # fulfillable by the default client.
        for media, drm in (
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.BEARER_TOKEN),
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.BEARER_TOKEN),
            (None, DeliveryMechanism.FINDAWAY_DRM),
            (MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, DeliveryMechanism.BEARER_TOKEN),
        ):
            # All of these DeliveryMechanisms were created when the
            # database was initialized.
            mechanism, is_new = DeliveryMechanism.lookup(db_session, media, drm)
            assert is_new is False
            assert mechanism.default_client_can_fulfill is True

        # It's possible to create new DeliveryMechanisms at runtime,
        # but their .default_client_can_fulfill will be False.
        mechanism, is_new = DeliveryMechanism.lookup(
            db_session, MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )
        assert is_new is False
        assert mechanism.default_client_can_fulfill is True

        mechanism, is_new = DeliveryMechanism.lookup(
            db_session, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )
        assert is_new is True
        assert mechanism.default_client_can_fulfill is False

    def test_association_with_licensepool(self, db_session, create_edition):
        """
        GIVEN: An Edition that has a LicensePool that has a DeliveryMechanism
        WHEN:  Testing the DeliveryMechanism's associatoin with the LicensePool
        THEN:  The correct DeliveryMechanism is defined
        """
        _, with_download = create_edition(db_session, with_open_access_download=True)
        [lpmech] = with_download.delivery_mechanisms
        assert b"Dummy content" == lpmech.resource.representation.content
        mech = lpmech.delivery_mechanism
        assert MediaTypes.EPUB_MEDIA_TYPE == mech.content_type
        assert mech.NO_DRM == mech.drm_scheme

    def test_compatible_with(self, db_session):
        """
        GIVEN: A DeliveryMechanism with a content type and DRM scheme
        WHEN:  Testing the rules about which DeliveryMechanisms are
               mutually compatible and which are mutually exclusive.
        THEN:  Returns True/False depending on compatibility
        """
        epub_adobe, _ = DeliveryMechanism.lookup(
            db_session, MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        pdf_adobe, _ = DeliveryMechanism.lookup(
            db_session, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        epub_no_drm, _ = DeliveryMechanism.lookup(
            db_session, MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        pdf_no_drm, _ = DeliveryMechanism.lookup(
            db_session, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        streaming, _ = DeliveryMechanism.lookup(
            db_session, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )

        # A non-streaming DeliveryMechanism is compatible only with
        # itself or a streaming mechanism.
        assert epub_adobe.compatible_with(None) is False
        assert epub_adobe.compatible_with("Not a DeliveryMechanism") is False
        assert epub_adobe.compatible_with(epub_no_drm) is False
        assert epub_adobe.compatible_with(pdf_adobe) is False
        assert epub_no_drm.compatible_with(pdf_no_drm) is False
        assert epub_adobe.compatible_with(epub_adobe) is True
        assert epub_adobe.compatible_with(streaming) is True

        # A streaming mechanism is compatible with anything.
        assert streaming.compatible_with(epub_adobe) is True
        assert streaming.compatible_with(pdf_adobe) is True
        assert streaming.compatible_with(epub_no_drm) is True

        # Rules are slightly different for open-access books: books
        # in any format are compatible so long as they have no DRM.
        assert epub_no_drm.compatible_with(pdf_no_drm, True) is True
        assert epub_no_drm.compatible_with(pdf_adobe, True) is False

    def test_uniqueness_constraint(self, db_session):
        """
        GIVEN: A DeliveryMechanism with a content type and DRM scheme
        WHEN:  Creating a duplicate DeliveryMechanism
        THEN:  An IntegrityError is raised
        """
        dm = DeliveryMechanism

        # You can't create two DeliveryMechanisms with the same values
        # for content_type and drm_scheme.
        with_drm_args = dict(content_type="type1", drm_scheme="scheme1")
        without_drm_args = dict(content_type="type1", drm_scheme=None)
        create(db_session, dm, **with_drm_args)
        pytest.raises(IntegrityError, create, db_session, dm, **with_drm_args)
        db_session.rollback()

        # You can't create two DeliveryMechanisms with the same value
        # for content_type and a null value for drm_scheme.
        create(db_session, dm, **without_drm_args)
        pytest.raises(IntegrityError, create, db_session, dm, **without_drm_args)
        db_session.rollback()


class TestRightsStatus:

    def test_lookup(self, db_session):
        """
        GIVEN: A RightsStatus
        WHEN:  Looking up a RightsStatus with a URI
        THEN:  URI and name are correctly set
        """
        status = RightsStatus.lookup(db_session, RightsStatus.IN_COPYRIGHT)
        assert RightsStatus.IN_COPYRIGHT == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.IN_COPYRIGHT) == status.name

        status = RightsStatus.lookup(db_session, RightsStatus.CC0)
        assert RightsStatus.CC0 == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.CC0) == status.name

        status = RightsStatus.lookup(db_session, "not a known rights uri")
        assert RightsStatus.UNKNOWN == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.UNKNOWN) == status.name

    def test_unique_uri_constraint(self, db_session):
        """
        GIVEN: A RightsStatus
        WHEN:  Creating a duplicate RightsStatus
        THEN:  An IntegrityError is raised
        """
        # We already have this RightsStatus.
        _ = RightsStatus.lookup(db_session, RightsStatus.IN_COPYRIGHT)

        # Let's try to create another one with the same URI.
        dupe = RightsStatus(uri=RightsStatus.IN_COPYRIGHT)
        db_session.add(dupe)

        # Nope.
        pytest.raises(IntegrityError, db_session.commit)


class TestLicense:

    @pytest.fixture
    def setup_licenses(self, db_session, create_work, create_license, default_library):
        """
        Fixture to initialize 6 Licenses associated with a LicensePool.
        - Perpetual:
            No expiration, no remaining checkouts, 1 concurrent checkout
        - Time Limited:
            Expires in 1 year, no remaining checkouts, 1 concurrent checkout
        - Loan Limited:
            No expiration, 4 remaining checkouts, 2 concurrent checkout
        - Time and Loan Limited:
            Expires in 1 year + 1 day, 52 remaining checkouts, 1 concurrent checkout
        - Expired Time Limited:
            Expired yesterday, no remaining checkouts, 1 concurrent checkout
        - Expired Loan Limited:
            No expiration, no remaining checkouts, 1 concurrent checkout
        """
        [collection] = default_library.collections
        [self.pool] = create_work(db_session, with_license_pool=True, collection=collection).license_pools

        now = utc_now()
        next_year = now + datetime.timedelta(days=365)
        yesterday = now - datetime.timedelta(days=1)

        self.perpetual = create_license(
            db_session,
            self.pool, expires=None, remaining_checkouts=None,
            concurrent_checkouts=1)

        self.time_limited = create_license(
            db_session,
            self.pool, expires=next_year, remaining_checkouts=None,
            concurrent_checkouts=1)

        self.loan_limited = create_license(
            db_session,
            self.pool, expires=None, remaining_checkouts=4,
            concurrent_checkouts=2)

        self.time_and_loan_limited = create_license(
            db_session,
            self.pool, expires=next_year + datetime.timedelta(days=1),
            remaining_checkouts=52, concurrent_checkouts=1)

        self.expired_time_limited = create_license(
            db_session,
            self.pool, expires=yesterday, remaining_checkouts=None,
            concurrent_checkouts=1)

        self.expired_loan_limited = create_license(
            db_session,
            self.pool, expires=None, remaining_checkouts=0,
            concurrent_checkouts=1)

    def test_loan_to(self, db_session, create_patron, setup_licenses):
        """
        GIVEN: A License associated with a LicensePool
        WHEN:  Loaning a License to a Patron
        THEN:  The License also loans its pool
        """
        pool = self.pool
        license = self.perpetual
        patron = create_patron(db_session)
        patron.last_loan_activity_sync = utc_now()

        loan, is_new = license.loan_to(patron)
        assert loan.license == license
        assert loan.license_pool == pool
        assert is_new is True
        assert patron.last_loan_activity_sync is None

        loan2, is_new = license.loan_to(patron)
        assert loan2 == loan
        assert loan2.license == license
        assert loan2.license_pool == pool
        assert is_new is False

    def test_license_types(self, setup_licenses):
        """
        GIVEN: A License
        WHEN:  Checking the License duration type
        THEN:  Returns True/False for the duration type
        """
        assert self.perpetual.is_perpetual is True
        assert self.perpetual.is_time_limited is False
        assert self.perpetual.is_loan_limited is False
        assert self.perpetual.is_expired is False

        assert self.time_limited.is_perpetual is False
        assert self.time_limited.is_time_limited is True
        assert self.time_limited.is_loan_limited is False
        assert self.time_limited.is_expired is False

        assert self.loan_limited.is_perpetual is False
        assert self.loan_limited.is_time_limited is False
        assert self.loan_limited.is_loan_limited is True
        assert self.loan_limited.is_expired is False

        assert self.time_and_loan_limited.is_perpetual is False
        assert self.time_and_loan_limited.is_time_limited is True
        assert self.time_and_loan_limited.is_loan_limited is True
        assert self.time_and_loan_limited.is_expired is False

        assert self.expired_time_limited.is_perpetual is False
        assert self.expired_time_limited.is_time_limited is True
        assert self.expired_time_limited.is_loan_limited is False
        assert self.expired_time_limited.is_expired is True

        assert self.expired_loan_limited.is_perpetual is False
        assert self.expired_loan_limited.is_time_limited is False
        assert self.expired_loan_limited.is_loan_limited is True
        assert self.expired_loan_limited.is_expired is True

    def test_best_available_license(self, db_session, create_license, create_patron, default_library, setup_licenses):
        """
        GIVEN: A License
        WHEN:  Creating a Loan for a Patron through a License
        THEN:  LicensePool selects the best available license to use
        """
        def get_new_patron():
            """
            Helper function to generate Patrons
            """
            return create_patron(db_session, library=default_library)

        next_week = utc_now() + datetime.timedelta(days=7)
        time_limited_2 = create_license(
            db_session,
            self.pool, expires=next_week, remaining_checkouts=None,
            concurrent_checkouts=1)
        loan_limited_2 = create_license(
            db_session,
            self.pool, expires=None, remaining_checkouts=2,
            concurrent_checkouts=1)

        # First, we use the time-limited license that's expiring first.
        assert time_limited_2 == self.pool.best_available_license()
        time_limited_2.loan_to(get_new_patron())

        # When that's not available, we use the next time-limited license.
        assert self.time_limited == self.pool.best_available_license()
        self.time_limited.loan_to(get_new_patron())

        # The time-and-loan-limited license also counts as time-limited for this.
        assert self.time_and_loan_limited == self.pool.best_available_license()
        self.time_and_loan_limited.loan_to(get_new_patron())

        # Next is the perpetual license.
        assert self.perpetual == self.pool.best_available_license()
        self.perpetual.loan_to(get_new_patron())

        # Then the loan-limited license with the most remaining checkouts.
        assert self.loan_limited == self.pool.best_available_license()
        self.loan_limited.loan_to(get_new_patron())

        # That license allows 2 concurrent checkouts, so it's still the
        # best license until it's checked out again.
        assert self.loan_limited == self.pool.best_available_license()
        self.loan_limited.loan_to(get_new_patron())

        # There's one more loan-limited license.
        assert loan_limited_2 == self.pool.best_available_license()
        loan_limited_2.loan_to(get_new_patron())

        # Now all licenses are either loaned out or expired.
        assert self.pool.best_available_license() is None


class TestLicensePool:

    def test_for_foreign_id(self, db_session, create_collection):
        """
        GIVEN: A DataSource, an appropriate work identifier, and a Collection
        WHEN:  Getting the LicensePool for the given above
        THEN:  The correct LicensePool is retrieved or created and returned
        """
        now = utc_now()
        collection = create_collection(db_session)
        pool, was_new = LicensePool.for_foreign_id(
            db_session, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=collection
        )
        assert (pool.availability_time - now).total_seconds() < 2
        assert was_new is True
        assert pool.data_source.name == DataSource.GUTENBERG
        assert pool.identifier.type == Identifier.GUTENBERG_ID
        assert pool.identifier.identifier == "541"
        assert pool.licenses_owned == 0
        assert pool.licenses_available == 0
        assert pool.licenses_reserved == 0
        assert pool.patrons_in_hold_queue == 0

    def test_for_foreign_id_fails_when_no_collection_provided(self, db_session):
        """
        GIVEN: A DataSource and an appropriate work identifier
        WHEN:  Creating a LicensePool without a Collection
        THEN:  A CollectionMissing error is raised
        """
        pytest.raises(
            CollectionMissing,
            LicensePool.for_foreign_id,
            db_session, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=None
        )

    def test_with_no_delivery_mechanisms(self, db_session, create_edition):
        """
        GIVEN: A LicensePool
        WHEN:  Finding LicensePools that have no delivery mechanism
        THEN:  The correct LicensePools are returned
        """
        # LicensePool.with_no_delivery_mechanisms returns a
        # query that finds all LicensePools which are missing
        # delivery mechanisms.
        qu = LicensePool.with_no_delivery_mechanisms(db_session)
        _, pool = create_edition(db_session, with_license_pool=True)

        # The LicensePool was created with a delivery mechanism.
        assert [] == qu.all()

        # Let's delete it.
        [db_session.delete(x) for x in pool.delivery_mechanisms]
        assert [pool] == qu.all()

    def test_no_license_pool_for_non_primary_identifier(
            self, db_session, create_collection):
        """
        GIVEN: An OverDrive DataSource, an ISBN Identifier, and a Collection
        WHEN:  Finding/creating a LicensePool for the given foreign ID
        THEN:  A ValueError is raised
        """
        # Overdrive offers licenses, but to get an Overdrive license pool for
        # a book you must identify the book by Overdrive's primary
        # identifier, not some other kind of identifier.
        collection = create_collection(db_session)
        with pytest.raises(ValueError) as excinfo:
            LicensePool.for_foreign_id(
                db_session, DataSource.OVERDRIVE, Identifier.ISBN, "{1-2-3}",
                collection=collection)
        assert "License pools for data source 'Overdrive' are keyed to identifier type 'Overdrive ID' (not 'ISBN', which was provided)" \
            in str(excinfo.value)

    def test_licensepools_for_same_identifier_have_same_presentation_edition(
            self, db_session, create_edition, create_identifier):
        """
        GIVEN: Two LicensePools with the same Identifier
        WHEN:  Setting the presentation edition for the LicensePool
        THEN:  Both LicensePools have the same presentation edition
        """
        identifier = create_identifier(db_session)
        _, pool1 = create_edition(
            db_session,
            with_license_pool=True, data_source_name=DataSource.GUTENBERG,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        _, pool2 = create_edition(
            db_session,
            with_license_pool=True, data_source_name=DataSource.UNGLUE_IT,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        pool1.set_presentation_edition()
        pool2.set_presentation_edition()
        assert pool1.presentation_edition == pool2.presentation_edition

    def test_collection_datasource_identifier_must_be_unique(self, db_session, create_collection, create_identifier):
        """
        GIVEN: A LicensePool with a DataSource, an Identifier, and a Collection
        WHEN:  Creating a duplicate LicensePool
        THEN:  An IntegrityError is raised
        """
        data_source = DataSource.lookup(db_session, DataSource.GUTENBERG)
        identifier = create_identifier(db_session)
        collection = create_collection(db_session)
        create(
            db_session,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )

        pytest.raises(
            IntegrityError,
            create,
            db_session,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )

    def test_with_no_work(self, db_session, create_collection, create_work):
        """
        GIVEN: Two LicensePools, one associated with a Work
        WHEN:  Finding LicensePools with no corresponding Work
        THEN:  The correct LicensePool(s) are returned
        """
        collection = create_collection(db_session)
        p1, _ = LicensePool.for_foreign_id(
            db_session, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=collection
        )

        p2, _ = LicensePool.for_foreign_id(
            db_session, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, "2",
            collection=collection
        )

        work = create_work(db_session, title="Foo")
        p1.work = work

        assert p1 in work.license_pools

        assert [p2] == LicensePool.with_no_work(db_session)

    def test_update_availability(self, db_session, create_work):
        """
        GIVEN: A LicensePool associated with a Work
        WHEN:  Updating the LicensePool with new availability information
        THEN:  The LicensePool has been updated
        """
        work = create_work(db_session, with_license_pool=True)
        work.last_update_time = None

        [pool] = work.license_pools
        pool.update_availability(30, 20, 2, 0)
        assert 30 == pool.licenses_owned
        assert 20 == pool.licenses_available
        assert 2 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

        # Updating availability also modified work.last_update_time.
        assert (utc_now() - work.last_update_time) < datetime.timedelta(seconds=2)

    def test_update_availability_triggers_analytics(self, db_session, create_work, default_library):
        """
        GIVEN: A LicensePool associated with a Work, Library, and Collection
               and an analytics provider
        WHEN:  Updating the LicensePool with new availability information with an analytics provider
        THEN:  The analytics provider logs the number of events (updates in this case)
        """
        [collection] = default_library.collections
        work = create_work(db_session, with_license_pool=True, collection=collection)
        [pool] = work.license_pools
        provider = MockAnalyticsProvider()

        pool.update_availability(30, 20, 2, 0, analytics=provider)
        count = provider.count
        pool.update_availability(30, 21, 2, 0, analytics=provider)

        assert count + 1 == provider.count
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == provider.event_type

        pool.update_availability(30, 21, 2, 1, analytics=provider)
        assert count + 2 == provider.count
        assert CirculationEvent.DISTRIBUTOR_HOLD_PLACE == provider.event_type

    def test_update_availability_does_nothing_if_given_no_data(self, db_session, create_work):
        """
        GIVEN: A LicensePool associated with a Work
        WHEN:  Updating the LicensePool with new availability information consisting of None
        THEN:  The LicensePool information is not updated for any fields that had a value of None passed in
        """

        # Set up a Work.
        work = create_work(db_session, with_license_pool=True)
        work.last_update_time = None

        # Set up a LicensePool.
        [pool] = work.license_pools
        pool.last_checked = None
        pool.licenses_owned = 10
        pool.licenses_available = 20
        pool.licenses_reserved = 30
        pool.patrons_in_hold_queue = 40

        # Pass empty values into update_availability.
        pool.update_availability(None, None, None, None)

        # The LicensePool's circulation data is what it was before.
        assert pool.licenses_owned == 10
        assert pool.licenses_available == 20
        assert pool.licenses_reserved == 30
        assert pool.patrons_in_hold_queue == 40

        # Work.update_time and LicensePool.last_checked are unaffected.
        assert work.last_update_time is None
        assert pool.last_checked is None

        # If we pass a mix of good and null values...
        pool.update_availability(5, None, None, None)

        # Only the good values are changed.
        assert pool.licenses_owned == 5
        assert pool.licenses_available == 20
        assert pool.licenses_reserved == 30
        assert pool.patrons_in_hold_queue == 40

    def test_open_access_links(self, db_session, create_edition):
        """
        GIVEN: A LicensePool with an open access hyperlink Identifier
        WHEN:  Adding two hyperlink Identifiers, one with open access and one without
               and checking the LicensePool's open access links
        THEN:  The two open access links are correctly identified
        """
        _, pool = create_edition(db_session, with_open_access_download=True)
        source = DataSource.lookup(db_session, DataSource.GUTENBERG)

        [oa1] = list(pool.open_access_links)

        # We have one open-access download, let's
        # add another.
        url = "https://example.com"
        media_type = MediaTypes.EPUB_MEDIA_TYPE
        link2, _ = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url,
            source, media_type
        )
        oa2 = link2.resource

        # And let's add a link that's not an open-access download.
        url = "https://example.com"
        pool.identifier.add_link(
            Hyperlink.IMAGE, url, source, MediaTypes.JPEG_MEDIA_TYPE
        )
        db_session.commit()

        # Only the two open-access download links show up.
        assert set([oa1, oa2]) == set(pool.open_access_links)

    def test_better_open_access_pool_than(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: An assortment of LicensePools
        WHEN:  Determining if open-access pool is generall known for better-quality than another pool
        THEN:  Correct determination
        """
        def get_licensepool(open_access, data_source_name, with_open_access_download=False, id=None):
            """
            Helper function to generate license pools.
            """
            edition = create_edition(db_session, identifier_id=id)
            pool = create_licensepool(
                db_session,
                edition,
                open_access=open_access,
                data_source_name=data_source_name,
                with_open_access_download=with_open_access_download
            )
            return pool

        gutenberg_1 = get_licensepool(
            open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
            id=1
        )

        gutenberg_2 = get_licensepool(
            open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
            id=2
        )

        assert int(gutenberg_1.identifier.identifier) < int(gutenberg_2.identifier.identifier)

        standard_ebooks = get_licensepool(
            open_access=True, data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=True
        )

        # Make sure Feedbooks data source exists -- it's not created
        # by default.
        DataSource.lookup(
            db_session, DataSource.FEEDBOOKS, autocreate=True
        )
        feedbooks = get_licensepool(
            open_access=True, data_source_name=DataSource.FEEDBOOKS,
            with_open_access_download=True
        )

        overdrive = get_licensepool(
            open_access=False, data_source_name=DataSource.OVERDRIVE
        )

        suppressed = get_licensepool(
            open_access=True, data_source_name=DataSource.GUTENBERG
        )
        suppressed.suppressed = True

        def better(x, y):
            return x.better_open_access_pool_than(y)

        # We would rather have nothing at all than a suppressed
        # LicensePool.
        assert better(suppressed, None) is False

        # A non-open-access LicensePool is not considered at all.
        assert better(overdrive, None) is False

        # Something is better than nothing.
        assert better(gutenberg_1, None) is True

        # An open access book from a high-quality source beats one
        # from a low-quality source.
        assert better(standard_ebooks, gutenberg_1) is True
        assert better(feedbooks, gutenberg_1) is True
        assert better(gutenberg_1, standard_ebooks) is False

        # A high Gutenberg number beats a low Gutenberg number.
        assert better(gutenberg_2, gutenberg_1) is True  # NOTE: What is this even testing?? The primary key?
        assert better(gutenberg_1, gutenberg_2) is False

        # If a supposedly open-access LicensePool doesn't have an
        # open-access download resource, it will only be considered if
        # there is no other alternative.
        no_resource = get_licensepool(
            open_access=True,
            data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=False,
        )
        no_resource.open_access = True
        assert better(no_resource, None) is True
        assert better(no_resource, gutenberg_1) is False

    def test_with_complaint(self, db_session, create_collection, create_complaint, create_library, create_work):
        """
        GIVEN: Works associated with a Library's Collection
        WHEN:  Filing Complaints against the Work's LicensePool
        THEN:  LicensePool finds the correct complaints for a given Library
        """

        library = create_library(db_session, name="default", short_name="default")

        def get_new_collection():
            """
            Helper function to generate collections
            """
            collection = create_collection(db_session)
            library.collections.append(collection)
            return collection

        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)
        type3 = next(type)

        work1 = create_work(
            db_session,
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True,
            collection=get_new_collection())
        lp1 = work1.license_pools[0]
        create_complaint(
            db_session,
            lp1,
            type1,
            "lp1 complaint1 source",
            "lp1 complaint1 detail")
        create_complaint(
            db_session,
            lp1,
            type1,
            "lp1 complaint2 source",
            "lp1 complaint2 detail")
        create_complaint(
            db_session,
            lp1,
            type2,
            "work1 complaint3 source",
            "work1 complaint3 detail")
        create_complaint(
            db_session,
            lp1,
            type3,
            "work1 resolved complaint source",
            "work1 resolved complaint detail",
            utc_now())

        work2 = create_work(
            db_session,
            "nonfiction work with complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True,
            collection=get_new_collection())
        lp2 = work2.license_pools[0]
        create_complaint(
            db_session,
            lp2,
            type2,
            "work2 complaint1 source",
            "work2 complaint1 detail")
        create_complaint(
            db_session,
            lp2,
            type2,
            "work2 resolved complaint source",
            "work2 resolved complaint detail",
            utc_now())

        work3 = create_work(
            db_session,
            "fiction work without complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True,
            collection=get_new_collection())
        lp3 = work3.license_pools[0]
        create_complaint(
            db_session,
            lp3,
            type3,
            "work3 resolved complaint source",
            "work3 resolved complaint detail",
            utc_now())

        create_work(
            db_session,
            "nonfiction work without complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True,
            collection=get_new_collection())

        # Where do the Works go? 4 Editions get created
        # buto only 1 Work is in the database at this point, which is work2
        # I am missing something fundamental here...

        # excludes resolved complaints by default
        results = LicensePool.with_complaint(library).all()

        assert 2 == len(results)
        assert lp1.id == results[0][0].id
        assert 3 == results[0][1]
        assert lp2.id == results[1][0].id
        assert 1 == results[1][1]

        # include resolved complaints this time
        more_results = LicensePool.with_complaint(library, resolved=None).all()

        assert 3 == len(more_results)
        assert lp1.id == more_results[0][0].id
        assert 4 == more_results[0][1]
        assert lp2.id == more_results[1][0].id
        assert 2 == more_results[1][1]
        assert lp3.id == more_results[2][0].id
        assert 1 == more_results[2][1]

        # show only resolved complaints
        resolved_results = LicensePool.with_complaint(library, resolved=True).all()
        lp_ids = set([result[0].id for result in resolved_results])
        counts = set([result[1] for result in resolved_results])

        assert 3 == len(resolved_results)
        assert lp_ids == set([lp1.id, lp2.id, lp3.id])
        assert counts == set([1])

        # This library has none of the license pools that have complaints,
        # so passing it in to with_complaint() gives no results.
        library2 = create_library(db_session, name="library2", short_name="library2")
        assert 0 == LicensePool.with_complaint(library2).count()

        # If we add the default library's collection to this new library,
        # we start getting the same results.
        library2.collections.extend(library.collections)
        assert 3 == LicensePool.with_complaint(library2, resolved=None).count()

    def test_set_presentation_edition(self, db_session, create_edition):
        """
        GIVEN: A LicensePool and various Editions
        WHEN:  Setting the presentation edition for the LicensePool
        THEN:  Creates a new Edition with information from the various Editions
        """
        """
        Make sure composite edition creation makes good choices when combining
        field data from provider, metadata wrangler, admin interface, etc. editions.
        """

        # Here's an Overdrive audiobook which also has data from the metadata
        # wrangler and from library staff.
        od, pool = create_edition(db_session, data_source_name=DataSource.OVERDRIVE, with_license_pool=True)
        od.medium = Edition.AUDIO_MEDIUM

        admin = create_edition(db_session, data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        admin.primary_identifier = pool.identifier

        mw = create_edition(db_session, data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)
        mw.primary_identifier = pool.identifier

        # The library staff has no opinion on the book's medium,
        # and the metadata wrangler has an incorrect opinion.
        admin.medium = None
        mw.medium = Edition.BOOK_MEDIUM

        # Overdrive, the metadata wrangler, and the library staff all have
        # opinions on the book's title. The metadata wrangler has also
        # identified a subtitle.
        od.title = "OverdriveTitle1"

        mw.title = "MetadataWranglerTitle1"
        mw.subtitle = "MetadataWranglerSubTitle1"

        admin.title = "AdminInterfaceTitle1"

        # Create a presentation edition, a composite of the available
        # Editions.
        pool.set_presentation_edition()
        presentation = pool.presentation_edition
        assert [pool] == presentation.is_presentation_for

        # The presentation edition is a completely new Edition.
        assert mw != od
        assert od != admin
        assert admin != presentation
        assert od != presentation

        # Within the presentation edition, information from the
        # library staff takes precedence over anything else.
        assert presentation.title == "AdminInterfaceTitle1"
        assert admin.contributors == presentation.contributors

        # Where the library staff has no opinion, the license source
        # takes precedence over the metadata wrangler.
        assert Edition.AUDIO_MEDIUM == presentation.medium

        # The metadata wrangler fills in any missing information.
        assert presentation.subtitle == "MetadataWranglerSubTitle1"

        # Now, change the admin interface's opinion about who the
        # author is.
        for c in admin.contributions:
            db_session.delete(c)
        db_session.commit()
        [jane], _ = Contributor.lookup(db_session, "Doe, Jane")
        jane.family_name, jane.display_name = jane.default_names()
        admin.add_contributor(jane, Contributor.AUTHOR_ROLE)
        pool.set_presentation_edition()

        # The old contributor has been removed from the presentation
        # edition, and the new contributor added.
        assert set([jane]) == presentation.contributors

    def test_circulation_changelog(self, db_session, create_edition):
        """
        GIVEN: A LicensePool with an Edition
        WHEN:  Generating a log message describing a change to the circulation
        THEN:  Returns a tuple suitable for logging.info
        """
        edition, pool = create_edition(db_session, with_license_pool=True)
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 8
        pool.patrons_in_hold_queue = 7

        msg, args = pool.circulation_changelog(1, 2, 3, 4)

        # Since all four circulation values changed, the message is as
        # long as it could possibly get.
        assert (
            'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s %s: %s=>%s %s: %s=>%s %s: %s=>%s' ==
            msg)
        assert (
            args ==
            (edition.medium, edition.title, edition.author,
             pool.identifier.type, pool.identifier.identifier,
             'OWN', 1, 10, 'AVAIL', 2, 9, 'RSRV', 3, 8, 'HOLD', 4, 7))

        # If only one circulation value changes, the message is a lot shorter.
        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        assert (
            'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s' ==
            msg)
        assert (
            args ==
            (edition.medium, edition.title, edition.author,
             pool.identifier.type, pool.identifier.identifier,
             'HOLD', 15, 7))

        # This works even if, for whatever reason, the edition's
        # bibliographic data is missing.
        edition.title = None
        edition.author = None

        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        assert "[NO TITLE]" == args[1]
        assert "[NO AUTHOR]" == args[2]

    def test_update_availability_from_delta(self, db_session, create_edition, default_library):
        """
        GIVEN: A LicensePool with an Edition associated with a Collection
        WHEN:  Updating availability information based on a single change from the distributor data
        THEN:  Updates the LicensePool with the new availability information
        """
        [collection] = default_library.collections
        _, pool = create_edition(db_session, with_license_pool=True, collection=collection)
        assert pool.last_checked is None
        assert pool.licenses_owned == 1
        assert pool.licenses_available == 1

        add = CirculationEvent.DISTRIBUTOR_LICENSE_ADD
        checkout = CirculationEvent.DISTRIBUTOR_CHECKOUT
        analytics = MockAnalyticsProvider()
        assert analytics.count == 0

        # This observation has no timestamp, but the pool has no
        # history, so we process it.
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        assert pool.last_checked is None
        assert pool.licenses_owned == 2
        assert pool.licenses_available == 2

        # Processing triggered two analytics events -- one for creating
        # the license pool and one for making it available.
        assert analytics.count == 2

        # Now the pool has a history, and we can't fit an undated
        # observation into that history, so undated observations
        # have no effect on circulation data.
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        pool.last_checked = yesterday
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        assert pool.licenses_owned == 2
        assert pool.last_checked == yesterday

        # However, outdated events are passed on to analytics so that
        # we record the fact that they happened... at some point.
        assert analytics.count == 3

        # This observation is more recent than the last time the pool
        # was checked, so it's processed and the last check time is
        # updated.
        pool.update_availability_from_delta(checkout, now, 1, analytics)
        assert pool.licenses_owned == 2
        assert pool.licenses_available == 1
        assert pool.last_checked == now
        assert analytics.count == 4

        # This event is less recent than the last time the pool was
        # checked, so it's ignored. Processing it is likely to do more
        # harm than good.
        pool.update_availability_from_delta(add, yesterday, 1, analytics)
        assert pool.licenses_owned == 2
        assert pool.last_checked == now

        # It's still logged to analytics, though.
        assert analytics.count == 5

        # This event is new but does not actually cause the
        # circulation to change at all.
        pool.update_availability_from_delta(add, now, 0, analytics)
        assert pool.licenses_owned == 2
        assert pool.last_checked == now

        # We still send the analytics event.
        assert analytics.count == 6

    def test_calculate_change_from_one_event(self, db_session, create_edition):
        """
        GIVEN: A LicensePool
        WHEN:  Calculating a change based on a type and delta
        THEN:  The correct LicensePool information is updated
        """
        """Test the internal method called by update_availability_from_delta."""
        CE = CirculationEvent

        # Create a LicensePool with a large number of available licenses.
        _, pool = create_edition(db_session, with_license_pool=True)
        pool.licenses_owned = 5
        pool.licenses_available = 4
        pool.licenses_reserved = 0
        pool.patrons_in_hold_queue = 0

        # Calibrate _calculate_change_from_one_event by sending it an
        # event that makes no difference. This lets us see what a
        # 'status quo' response from the method would look like.
        calc = pool._calculate_change_from_one_event
        assert calc(CE.DISTRIBUTOR_CHECKIN, 0) == (5, 4, 0, 0)

        # If there ever appear to be more licenses available than
        # owned, the number of owned licenses is left alone. It's
        # possible that we have more licenses than we thought, but
        # it's more likely that a license has expired or otherwise
        # been removed.
        assert calc(CE.DISTRIBUTOR_CHECKIN, 3) == (5, 5, 0, 0)

        # But we don't bump up the number of available licenses just
        # because one becomes available.
        assert calc(CE.DISTRIBUTOR_CHECKIN, 1) == (5, 5, 0, 0)

        # When you signal a hold on a book that's available, we assume
        # that the book has stopped being available.
        assert calc(CE.DISTRIBUTOR_HOLD_PLACE, 3) == (5, 0, 0, 3)

        # If a license stops being owned, it implicitly stops being
        # available. (But we don't know if the license that became
        # unavailable is one of the ones currently checked out to
        # someone, or one of the other ones.)
        assert calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 2) == (3, 3, 0, 0)

        # If a license stops being available, it doesn't stop
        # being owned.
        assert calc(CE.DISTRIBUTOR_CHECKOUT, 1) == (5, 3, 0, 0)

        # None of these numbers will go below zero.
        assert calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 100) == (0, 0, 0, 0)

        # Newly added licenses start out available if there are no
        # patrons in the hold queue.
        assert calc(CE.DISTRIBUTOR_LICENSE_ADD, 1) == (6, 5, 0, 0)

        # Now let's run some tests with a LicensePool that has a large holds
        # queue.
        pool.licenses_owned = 5
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        pool.patrons_in_hold_queue = 3
        assert calc(CE.DISTRIBUTOR_HOLD_PLACE, 0) == (5, 0, 1, 3)

        # When you signal a hold on a book that already has holds, it
        # does nothing but increase the number of patrons in the hold
        # queue.
        assert calc(CE.DISTRIBUTOR_HOLD_PLACE, 3) == (5, 0, 1, 6)

        # A checkin event has no effect...
        assert calc(CE.DISTRIBUTOR_CHECKIN, 1) == (5, 0, 1, 3)

        # ...because it's presumed that it will be followed by an
        # availability notification event, which takes a patron off
        # the hold queue and adds them to the reserved list.
        assert calc(CE.DISTRIBUTOR_AVAILABILITY_NOTIFY, 1) == (5, 0, 2, 2)

        # The only exception is if the checkin event wipes out the
        # entire holds queue, in which case the number of available
        # licenses increases.  (But nothing else changes -- we're
        # still waiting for the availability notification events.)
        assert calc(CE.DISTRIBUTOR_CHECKIN, 6) == (5, 3, 1, 3)

        # Again, note that even though six copies were checked in,
        # we're not assuming we own more licenses than we
        # thought. It's more likely that the sixth license expired and
        # we weren't notified.

        # When there are no licenses available, a checkout event
        # draws from the pool of licenses reserved instead.
        assert calc(CE.DISTRIBUTOR_CHECKOUT, 2) == (5, 0, 0, 3)

        # Newly added licenses do not start out available if there are
        # patrons in the hold queue.
        assert calc(CE.DISTRIBUTOR_LICENSE_ADD, 1) == (6, 0, 1, 3)

    def test_loan_to_patron(self, db_session, create_edition, create_patron):
        """
        GIVEN: A LicensePool and Patron
        WHEN:  Creating a loan for the Patron through the LicensePool
        THEN:  Loan is succesfully created
        """
        # TODO: The path where the LicensePool is loaned to an
        # IntegrationClient rather than a Patron is currently not
        # directly tested.

        _, pool = create_edition(db_session, with_license_pool=True)
        patron = create_patron(db_session)
        now = utc_now()
        patron.last_loan_activity_sync = now

        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        fulfillment = pool.delivery_mechanisms[0]
        external_identifier = "external_identifier"
        loan, is_new = pool.loan_to(
            patron, start=yesterday, end=tomorrow,
            fulfillment=fulfillment, external_identifier=external_identifier
        )

        assert is_new is True
        assert isinstance(loan, Loan)
        assert loan.license_pool == pool
        assert loan.patron == patron
        assert loan.start == yesterday
        assert loan.end == tomorrow
        assert loan.fulfillment == fulfillment
        assert loan.external_identifier == external_identifier

        # Issuing a loan locally created uncertainty about a patron's
        # loans, since we don't know how the external vendor dealt
        # with the request. The last_loan_activity_sync has been
        # cleared out so we know to check back with the source of
        # truth.
        assert patron.last_loan_activity_sync is None

        # 'Creating' a loan that already exists does not create any
        # uncertainty.
        patron.last_loan_activity_sync = now
        loan2, is_new = pool.loan_to(
            patron, start=yesterday, end=tomorrow,
            fulfillment=fulfillment, external_identifier=external_identifier
        )
        assert is_new is False
        assert loan2 == loan
        assert patron.last_loan_activity_sync == now

    def test_on_hold_to_patron(self, db_session, create_edition, create_patron):
        """
        GIVEN: A LicensePool and Patron
        WHEN:  Putting the Patron in the holds queue for a LicensePool
        THEN:  A Hold is created for the Patron with a position in the queue
        """
        # TODO: The path where the 'patron' is an IntegrationClient
        # rather than a Patron is currently not directly tested.

        _, pool = create_edition(db_session, with_license_pool=True)
        patron = create_patron(db_session)
        now = utc_now()
        patron.last_loan_activity_sync = now

        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        position = 99
        external_identifier = "external_identifier"
        hold, is_new = pool.on_hold_to(
            patron, start=yesterday, end=tomorrow,
            position=position, external_identifier=external_identifier
        )

        assert is_new is True
        assert isinstance(hold, Hold)
        assert hold.license_pool == pool
        assert hold.patron == patron
        assert hold.start == yesterday
        assert hold.end == tomorrow
        assert hold.position == position
        assert hold.external_identifier == external_identifier

        # Issuing a hold locally created uncertainty about a patron's
        # loans, since we don't know how the external vendor dealt
        # with the request. The last_loan_activity_sync has been
        # cleared out so we know to check back with the source of
        # truth.
        assert patron.last_loan_activity_sync is None

        # 'Creating' a hold that already exists does not create any
        # uncertainty.
        patron.last_loan_activity_sync = now
        hold2, is_new = pool.on_hold_to(
            patron, start=yesterday, end=tomorrow,
            position=position, external_identifier=external_identifier
        )
        assert is_new is False
        assert hold2 == hold
        assert patron.last_loan_activity_sync == now


class TestLicensePoolDeliveryMechanism:

    def test_lpdm_change_may_change_open_access_status(self, db_session, create_edition):
        """
        GIVEN: An Edition with a LicensePool that is not open access
        WHEN:  Adding an open access delivery mechanism
        THEN:  LicensePool is now open access
        """
        # Here's a book that's not open access.
        _, pool = create_edition(db_session, with_license_pool=True)
        assert pool.open_access is False

        # We're going to use LicensePoolDeliveryMechanism.set to
        # to give it a non-open-access LPDM.
        data_source = pool.data_source
        identifier = pool.identifier
        content_type = MediaTypes.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.IN_COPYRIGHT
        )

        # Now there's a way to get the book, but it's not open access.
        assert pool.open_access is False

        # Now give it an open-access LPDM.
        link, _ = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, "https://www.example.com/",
            data_source, content_type
        )
        oa_lpdm = LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.GENERIC_OPEN_ACCESS, link.resource
        )

        # Now it's open access.
        assert pool.open_access is True

        # Delete the open-access LPDM, and it stops being open access.
        oa_lpdm.delete()
        assert pool.open_access is False

    def test_set_rights_status(self, db_session, create_edition):
        """
        GIVEN: A LicensePool with a LicensePoolDeliveryMechanism that is not open access
        WHEN:  Changing the rights status on the delivery mechanism
        THEN:  The delivery mechanism's open access setting is correlated to the rights status
        """
        # Here's a non-open-access book.
        _, pool = create_edition(db_session, with_license_pool=True)
        pool.open_access = False
        [lpdm] = pool.delivery_mechanisms

        # We set its rights status to 'in copyright', and nothing changes.
        uri = RightsStatus.IN_COPYRIGHT
        status = lpdm.set_rights_status(uri)
        assert lpdm.rights_status == status
        assert status.uri == uri
        assert status.name == RightsStatus.NAMES.get(uri)
        assert pool.open_access is False

        # Setting it again won't change anything.
        status2 = lpdm.set_rights_status(uri)
        assert status == status2

        # Set the rights status to a different URL, we change to a different
        # RightsStatus object.
        uri2 = "http://unknown"
        status3 = lpdm.set_rights_status(uri2)
        assert status3 != status
        assert status3.uri == RightsStatus.UNKNOWN
        assert status3.name == RightsStatus.NAMES.get(RightsStatus.UNKNOWN)

        # Set the rights status to a URL that implies open access,
        # and the status of the LicensePool is changed.
        open_access_uri = RightsStatus.GENERIC_OPEN_ACCESS
        open_access_status = lpdm.set_rights_status(open_access_uri)
        assert open_access_status.uri == open_access_uri
        assert open_access_status.name == RightsStatus.NAMES.get(open_access_uri)
        assert pool.open_access is True

        # Set it back to a URL that does not imply open access, and
        # the status of the LicensePool is changed back.
        lpdm.set_rights_status(uri)
        assert pool.open_access is False

        # Now add a second delivery mechanism, so the pool has one
        # open-access and one commercial delivery mechanism.
        lpdm2 = pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY, None)
        assert len(pool.delivery_mechanisms) == 2

        # Now the pool is open access again
        assert pool.open_access is True

        # But if we change the new delivery mechanism to non-open
        # access, the pool won't be open access anymore either.
        lpdm2.set_rights_status(uri)
        assert pool.open_access is False

    def test_uniqueness_constraint(self, db_session, create_edition):
        """
        GIVEN: A LicensePoolDeliveryMechanism
        WHEN:  Creating a duplicate delivery mechanism with the same
               identifier, data source, and resource
        THEN:  An IntegrityError is raised
        """
        # with_open_access_download will create a LPDM
        # for the open-access download.
        _, pool = create_edition(db_session, with_license_pool=True,
                                 with_open_access_download=True)
        [lpdm] = pool.delivery_mechanisms

        # We can create a second LPDM with the same data type and DRM status,
        # so long as the resource is different.
        link, _ = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, "https://www.example.com/",
            pool.data_source, "text/html"
        )
        lpdm2 = pool.set_delivery_mechanism(
            lpdm.delivery_mechanism.content_type,
            lpdm.delivery_mechanism.drm_scheme,
            lpdm.rights_status.uri,
            link.resource,
        )
        assert lpdm2.delivery_mechanism == lpdm.delivery_mechanism
        assert lpdm2.resource != lpdm.resource

        # We can even create an LPDM with the same data type and DRM
        # status and _no_ resource.
        lpdm3 = pool.set_delivery_mechanism(
            lpdm.delivery_mechanism.content_type,
            lpdm.delivery_mechanism.drm_scheme,
            lpdm.rights_status.uri,
            None
        )
        assert lpdm3.delivery_mechanism == lpdm.delivery_mechanism
        assert lpdm3.resource is None

        # But we can't create a second such LPDM -- it violates a
        # constraint of a unique index.
        pytest.raises(
            IntegrityError, create, db_session,
            LicensePoolDeliveryMechanism,
            delivery_mechanism=lpdm3.delivery_mechanism,
            identifier=pool.identifier,
            data_source=pool.data_source,
            resource=None
        )
        db_session.rollback()

    def test_compatible_with(self, db_session, create_edition, create_licensepooldeliverymechanism):
        """
        GIVEN: A LicensePoolDeliveryMechanism
        WHEN:  Testing compatability with another delivery mechanism
        THEN:  Mechanisms are either mutually compatible or mutually exclusive
        """
        _, pool = create_edition(db_session, with_license_pool=True,
                                 with_open_access_download=True)
        [mech] = pool.delivery_mechanisms

        # Test the simple cases.
        assert mech.compatible_with(None) is False
        assert mech.compatible_with("Not a LicensePoolDeliveryMechanism") is False
        assert mech.compatible_with(mech) is True

        # Now let's set up a scenario that works and then see how it fails.
        create_licensepooldeliverymechanism(pool)

        # This book has two different LicensePoolDeliveryMechanisms
        # with the same underlying DeliveryMechanism. They're
        # compatible.
        [mech1, mech2] = pool.delivery_mechanisms
        assert mech1.id != mech2.id
        assert mech1.delivery_mechanism == mech2.delivery_mechanism
        assert mech1.compatible_with(mech2) is True

        # The LicensePoolDeliveryMechanisms must identify the same
        # book from the same data source.
        mech1.data_source_id = "data_source_id"
        assert mech1.compatible_with(mech2) is False

        mech1.data_source_id = mech2.data_source_id
        mech1.identifier_id = "identifier_id"
        assert mech1.compatible_with(mech2) is False
        mech1.identifier_id = mech2.identifier_id

        # The underlying delivery mechanisms don't have to be exactly
        # the same, but they must be compatible.
        pdf_adobe, _ = DeliveryMechanism.lookup(
            db_session, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )
        mech1.delivery_mechanism = pdf_adobe
        db_session.commit()
        assert mech1.compatible_with(mech2) is False

        streaming, _ = DeliveryMechanism.lookup(
            db_session, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )
        mech1.delivery_mechanism = streaming
        db_session.commit()
        assert mech1.compatible_with(mech2) is True

    def test_compatible_with_calls_compatible_with_on_deliverymechanism(
            self, db_session, create_edition, create_licensepooldeliverymechanism):
        """
        GIVEN: Two LicensePoolDeliveryMechanisms
        WHEN:  Testing compatibility
        THEN:  Compatibility is returned
        """
        # Create two LicensePoolDeliveryMechanisms with different
        # media types.
        _, pool = create_edition(db_session, with_license_pool=True,
                                 with_open_access_download=True)
        [mech1] = pool.delivery_mechanisms
        mech2 = create_licensepooldeliverymechanism(pool)
        mech2.delivery_mechanism, _ = DeliveryMechanism.lookup(
            db_session, MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        db_session.commit()

        assert mech1.is_open_access is True
        assert mech2.is_open_access is False

        # Determining whether the mechanisms are compatible requires
        # calling compatible_with on the first mechanism's
        # DeliveryMechanism, passing in the second DeliveryMechanism
        # plus the answer to 'are both LicensePoolDeliveryMechanisms
        # open-access?'
        class Mock(object):
            called_with = None

            @classmethod
            def compatible_with(cls, other, open_access):
                cls.called_with = (other, open_access)
                return True
        mech1.delivery_mechanism.compatible_with = Mock.compatible_with

        # Call compatible_with, and the mock method is called with the
        # second DeliveryMechanism and (since one of the
        # LicensePoolDeliveryMechanisms is not open-access) the value
        # False.
        mech1.compatible_with(mech2)
        assert (
            (mech2.delivery_mechanism, False) ==
            Mock.called_with)

        # If both LicensePoolDeliveryMechanisms are open-access,
        # True is passed in instead, so that
        # DeliveryMechanism.compatible_with() applies the less strict
        # compatibility rules for open-access fulfillment.
        mech2.set_rights_status(RightsStatus.GENERIC_OPEN_ACCESS)
        mech1.compatible_with(mech2)
        assert (
            (mech2.delivery_mechanism, True) ==
            Mock.called_with)

    @pytest.mark.parametrize(
        'data_source,identifier,delivery_mechanism',
        [
            pytest.param('a', 'a', 'a', id='ascii_symbol'),
            pytest.param('ą', 'ą', 'ą', id='non-ascii_symbol'),
        ],
    )
    def test_repr(self, data_source, identifier, delivery_mechanism):
        """
        GIVEN: ASCII or non-ASCII symbols for a LicensePoolDeliveryMechanism
        WHEN:  Setting the delivery mechanism with the symbols
        THEN:  __repr__ functions correctly
        """
        # Arrange
        data_source_mock = DataSource()
        data_source_mock.__str__ = MagicMock(return_value=data_source)

        identifier_mock = Identifier()
        identifier_mock.__repr__ = MagicMock(return_value=identifier)

        delivery_mechanism_mock = DeliveryMechanism()
        delivery_mechanism_mock.__repr__ = MagicMock(return_value=delivery_mechanism)

        license_delivery_mechanism_mock = LicensePoolDeliveryMechanism()
        license_delivery_mechanism_mock.data_source = PropertyMock(return_value=data_source_mock)
        license_delivery_mechanism_mock.identifier = PropertyMock(return_value=identifier_mock)
        license_delivery_mechanism_mock.delivery_mechanism = PropertyMock(return_value=delivery_mechanism_mock)

        # Act
        # NOTE: we are not interested in the result returned by repr,
        # we just want to make sure that repr doesn't throw any unexpected exceptions
        repr(license_delivery_mechanism_mock)
