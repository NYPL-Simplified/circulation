# encoding: utf-8
from mock import MagicMock, PropertyMock
from parameterized import parameterized
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
from ...model.coverage import WorkCoverageRecord
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier
from ...model.licensing import (
    DeliveryMechanism,
    Hold,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    RightsStatus,
)
from ...model.resource import Hyperlink, Representation
from ...testing import DatabaseTest
from ...util.datetime_helpers import utc_now


class TestDeliveryMechanism(DatabaseTest):
    def setup_method(self):
        super(TestDeliveryMechanism, self).setup_method()
        self.epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        self.epub_adobe_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        self.overdrive_streaming_text, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM)
        self.audiobook_drm_scheme, ignore = DeliveryMechanism.lookup(
            self._db, Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM
        )

    def test_implicit_medium(self):
        assert Edition.BOOK_MEDIUM == self.epub_no_drm.implicit_medium
        assert Edition.BOOK_MEDIUM == self.epub_adobe_drm.implicit_medium
        assert Edition.BOOK_MEDIUM == self.overdrive_streaming_text.implicit_medium

    def test_is_media_type(self):
        assert False == DeliveryMechanism.is_media_type(None)
        assert True == DeliveryMechanism.is_media_type(Representation.EPUB_MEDIA_TYPE)
        assert False == DeliveryMechanism.is_media_type(DeliveryMechanism.KINDLE_CONTENT_TYPE)
        assert False == DeliveryMechanism.is_media_type(DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE)

    def test_is_streaming(self):
        assert False == self.epub_no_drm.is_streaming
        assert False == self.epub_adobe_drm.is_streaming
        assert True == self.overdrive_streaming_text.is_streaming

    def test_drm_scheme_media_type(self):
        assert None == self.epub_no_drm.drm_scheme_media_type
        assert DeliveryMechanism.ADOBE_DRM == self.epub_adobe_drm.drm_scheme_media_type
        assert None == self.overdrive_streaming_text.drm_scheme_media_type

    def test_content_type_media_type(self):
        assert Representation.EPUB_MEDIA_TYPE == self.epub_no_drm.content_type_media_type
        assert Representation.EPUB_MEDIA_TYPE == self.epub_adobe_drm.content_type_media_type
        assert (Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE ==
            self.overdrive_streaming_text.content_type_media_type)
        assert (Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE + DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_PROFILE ==
            self.audiobook_drm_scheme.content_type_media_type)

    def test_default_fulfillable(self):
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
            mechanism, is_new = DeliveryMechanism.lookup(self._db, media, drm)
            assert False == is_new
            assert True == mechanism.default_client_can_fulfill

        # It's possible to create new DeliveryMechanisms at runtime,
        # but their .default_client_can_fulfill will be False.
        mechanism, is_new = DeliveryMechanism.lookup(
            self._db, MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )
        assert False == is_new
        assert True == mechanism.default_client_can_fulfill

        mechanism, is_new = DeliveryMechanism.lookup(
            self._db, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )
        assert True == is_new
        assert False == mechanism.default_client_can_fulfill

    def test_association_with_licensepool(self):
        ignore, with_download = self._edition(with_open_access_download=True)
        [lpmech] = with_download.delivery_mechanisms
        assert b"Dummy content" == lpmech.resource.representation.content
        mech = lpmech.delivery_mechanism
        assert MediaTypes.EPUB_MEDIA_TYPE == mech.content_type
        assert mech.NO_DRM == mech.drm_scheme

    def test_compatible_with(self):
        """Test the rules about which DeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """
        epub_adobe, ignore = DeliveryMechanism.lookup(
            self._db, MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        pdf_adobe, ignore = DeliveryMechanism.lookup(
            self._db, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )

        epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        pdf_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM
        )

        streaming, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )

        # A non-streaming DeliveryMechanism is compatible only with
        # itself or a streaming mechanism.
        assert False == epub_adobe.compatible_with(None)
        assert False == epub_adobe.compatible_with("Not a DeliveryMechanism")
        assert False == epub_adobe.compatible_with(epub_no_drm)
        assert False == epub_adobe.compatible_with(pdf_adobe)
        assert False == epub_no_drm.compatible_with(pdf_no_drm)
        assert True == epub_adobe.compatible_with(epub_adobe)
        assert True == epub_adobe.compatible_with(streaming)

        # A streaming mechanism is compatible with anything.
        assert True == streaming.compatible_with(epub_adobe)
        assert True == streaming.compatible_with(pdf_adobe)
        assert True == streaming.compatible_with(epub_no_drm)

        # Rules are slightly different for open-access books: books
        # in any format are compatible so long as they have no DRM.
        assert True == epub_no_drm.compatible_with(pdf_no_drm, True)
        assert False == epub_no_drm.compatible_with(pdf_adobe, True)

    def test_uniqueness_constraint(self):

        dm = DeliveryMechanism

        # You can't create two DeliveryMechanisms with the same values
        # for content_type and drm_scheme.
        with_drm_args = dict(content_type="type1", drm_scheme="scheme1")
        without_drm_args = dict(content_type="type1", drm_scheme=None)
        with_drm = create(self._db, dm, **with_drm_args)
        pytest.raises(IntegrityError, create, self._db, dm, **with_drm_args)
        self._db.rollback()

        # You can't create two DeliveryMechanisms with the same value
        # for content_type and a null value for drm_scheme.
        without_drm = create(self._db, dm, **without_drm_args)
        pytest.raises(IntegrityError, create, self._db, dm, **without_drm_args)
        self._db.rollback()


class TestRightsStatus(DatabaseTest):

    def test_lookup(self):
        status = RightsStatus.lookup(self._db, RightsStatus.IN_COPYRIGHT)
        assert RightsStatus.IN_COPYRIGHT == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.IN_COPYRIGHT) == status.name

        status = RightsStatus.lookup(self._db, RightsStatus.CC0)
        assert RightsStatus.CC0 == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.CC0) == status.name

        status = RightsStatus.lookup(self._db, "not a known rights uri")
        assert RightsStatus.UNKNOWN == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.UNKNOWN) == status.name

    def test_unique_uri_constraint(self):
        # We already have this RightsStatus.
        status = RightsStatus.lookup(self._db, RightsStatus.IN_COPYRIGHT)

        # Let's try to create another one with the same URI.
        dupe = RightsStatus(uri=RightsStatus.IN_COPYRIGHT)
        self._db.add(dupe)

        # Nope.
        pytest.raises(IntegrityError, self._db.commit)


class TestLicense(DatabaseTest):

    def setup_method(self):
        super(TestLicense, self).setup_method()
        self.pool = self._licensepool(None)

        now = utc_now()
        next_year = now + datetime.timedelta(days=365)
        yesterday = now - datetime.timedelta(days=1)

        self.perpetual = self._license(
            self.pool, expires=None, remaining_checkouts=None,
            concurrent_checkouts=1)

        self.time_limited = self._license(
            self.pool, expires=next_year, remaining_checkouts=None,
            concurrent_checkouts=1)

        self.loan_limited = self._license(
            self.pool, expires=None, remaining_checkouts=4,
            concurrent_checkouts=2)

        self.time_and_loan_limited = self._license(
            self.pool, expires=next_year + datetime.timedelta(days=1),
            remaining_checkouts=52, concurrent_checkouts=1)

        self.expired_time_limited = self._license(
            self.pool, expires=yesterday, remaining_checkouts=None,
            concurrent_checkouts=1)

        self.expired_loan_limited = self._license(
            self.pool, expires=None, remaining_checkouts=0,
            concurrent_checkouts=1)

    def test_loan_to(self):
        # Verify that loaning a license also loans its pool.
        pool = self.pool
        license = self.perpetual
        patron = self._patron()
        patron.last_loan_activity_sync = utc_now()
        loan, is_new = license.loan_to(patron)
        assert license == loan.license
        assert pool == loan.license_pool
        assert True == is_new
        assert None == patron.last_loan_activity_sync

        loan2, is_new = license.loan_to(patron)
        assert loan == loan2
        assert license == loan2.license
        assert pool == loan2.license_pool
        assert False == is_new

    def test_license_types(self):
        assert True == self.perpetual.is_perpetual
        assert False == self.perpetual.is_time_limited
        assert False == self.perpetual.is_loan_limited
        assert False == self.perpetual.is_expired

        assert False == self.time_limited.is_perpetual
        assert True == self.time_limited.is_time_limited
        assert False == self.time_limited.is_loan_limited
        assert False == self.time_limited.is_expired

        assert False == self.loan_limited.is_perpetual
        assert False == self.loan_limited.is_time_limited
        assert True == self.loan_limited.is_loan_limited
        assert False == self.loan_limited.is_expired

        assert False == self.time_and_loan_limited.is_perpetual
        assert True == self.time_and_loan_limited.is_time_limited
        assert True == self.time_and_loan_limited.is_loan_limited
        assert False == self.time_and_loan_limited.is_expired

        assert False == self.expired_time_limited.is_perpetual
        assert True == self.expired_time_limited.is_time_limited
        assert False == self.expired_time_limited.is_loan_limited
        assert True == self.expired_time_limited.is_expired

        assert False == self.expired_loan_limited.is_perpetual
        assert False == self.expired_loan_limited.is_time_limited
        assert True == self.expired_loan_limited.is_loan_limited
        assert True == self.expired_loan_limited.is_expired

    def test_best_available_license(self):
        next_week = utc_now() + datetime.timedelta(days=7)
        time_limited_2 = self._license(
            self.pool, expires=next_week, remaining_checkouts=None,
            concurrent_checkouts=1)
        loan_limited_2 = self._license(
            self.pool, expires=None, remaining_checkouts=2,
            concurrent_checkouts=1)

        # First, we use the time-limited license that's expiring first.
        assert time_limited_2 == self.pool.best_available_license()
        time_limited_2.loan_to(self._patron())

        # When that's not available, we use the next time-limited license.
        assert self.time_limited == self.pool.best_available_license()
        self.time_limited.loan_to(self._patron())

        # The time-and-loan-limited license also counts as time-limited for this.
        assert self.time_and_loan_limited == self.pool.best_available_license()
        self.time_and_loan_limited.loan_to(self._patron())

        # Next is the perpetual license.
        assert self.perpetual == self.pool.best_available_license()
        self.perpetual.loan_to(self._patron())

        # Then the loan-limited license with the most remaining checkouts.
        assert self.loan_limited == self.pool.best_available_license()
        self.loan_limited.loan_to(self._patron())

        # That license allows 2 concurrent checkouts, so it's still the
        # best license until it's checked out again.
        assert self.loan_limited == self.pool.best_available_license()
        self.loan_limited.loan_to(self._patron())

        # There's one more loan-limited license.
        assert loan_limited_2 == self.pool.best_available_license()
        loan_limited_2.loan_to(self._patron())

        # Now all licenses are either loaned out or expired.
        assert None == self.pool.best_available_license()


class TestLicensePool(DatabaseTest):

    def test_for_foreign_id(self):
        """Verify we can get a LicensePool for a data source, an
        appropriate work identifier, and a Collection."""
        now = utc_now()
        pool, was_new = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=self._collection()
        )
        assert (pool.availability_time - now).total_seconds() < 2
        assert True == was_new
        assert DataSource.GUTENBERG == pool.data_source.name
        assert Identifier.GUTENBERG_ID == pool.identifier.type
        assert "541" == pool.identifier.identifier
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

    def test_for_foreign_id_fails_when_no_collection_provided(self):
        """We cannot create a LicensePool that is not associated
        with some Collection.
        """
        pytest.raises(
            CollectionMissing,
            LicensePool.for_foreign_id,
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "541",
            collection=None
        )

    def test_with_no_delivery_mechanisms(self):
        # LicensePool.with_no_delivery_mechanisms returns a
        # query that finds all LicensePools which are missing
        # delivery mechanisms.
        qu = LicensePool.with_no_delivery_mechanisms(self._db)
        pool = self._licensepool(None)

        # The LicensePool was created with a delivery mechanism.
        assert [] == qu.all()

        # Let's delete it.
        [self._db.delete(x) for x in pool.delivery_mechanisms]
        assert [pool] == qu.all()

    def test_no_license_pool_for_non_primary_identifier(self):
        """Overdrive offers licenses, but to get an Overdrive license pool for
        a book you must identify the book by Overdrive's primary
        identifier, not some other kind of identifier.
        """
        collection = self._collection()
        with pytest.raises(ValueError) as excinfo:
            LicensePool.for_foreign_id(
                self._db, DataSource.OVERDRIVE, Identifier.ISBN, "{1-2-3}",
                collection = collection)
        assert "License pools for data source 'Overdrive' are keyed to identifier type 'Overdrive ID' (not 'ISBN', which was provided)" \
            in str(excinfo.value)

    def test_licensepools_for_same_identifier_have_same_presentation_edition(self):
        """Two LicensePools for the same Identifier will get the same
        presentation edition.
        """
        identifier = self._identifier()
        edition1, pool1 = self._edition(
            with_license_pool=True, data_source_name=DataSource.GUTENBERG,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        edition2, pool2 = self._edition(
            with_license_pool=True, data_source_name=DataSource.UNGLUE_IT,
            identifier_type=identifier.type, identifier_id=identifier.identifier
        )
        pool1.set_presentation_edition()
        pool2.set_presentation_edition()
        assert pool1.presentation_edition == pool2.presentation_edition

    def test_collection_datasource_identifier_must_be_unique(self):
        """You can't have two LicensePools with the same Collection,
        DataSource, and Identifier.
        """
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = self._identifier()
        collection = self._default_collection
        pool = create(
            self._db,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )

        pytest.raises(
            IntegrityError,
            create,
            self._db,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection
        )

    def test_with_no_work(self):
        p1, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=self._default_collection
        )

        p2, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, "2",
            collection=self._default_collection
        )

        work = self._work(title="Foo")
        p1.work = work

        assert p1 in work.license_pools

        assert [p2] == LicensePool.with_no_work(self._db)

    def test_update_availability(self):
        work = self._work(with_license_pool=True)
        work.last_update_time = None

        [pool] = work.license_pools
        pool.update_availability(30, 20, 2, 0)
        assert 30 == pool.licenses_owned
        assert 20 == pool.licenses_available
        assert 2 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

        # Updating availability also modified work.last_update_time.
        assert (utc_now() - work.last_update_time) < datetime.timedelta(seconds=2)

    def test_update_availability_triggers_analytics(self):
        work = self._work(with_license_pool=True)
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

    def test_update_availability_does_nothing_if_given_no_data(self):
        """Passing an empty set of data into update_availability is
        a no-op.
        """

        # Set up a Work.
        work = self._work(with_license_pool=True)
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
        assert 10 == pool.licenses_owned
        assert 20 == pool.licenses_available
        assert 30 == pool.licenses_reserved
        assert 40 == pool.patrons_in_hold_queue

        # Work.update_time and LicensePool.last_checked are unaffected.
        assert None == work.last_update_time
        assert None == pool.last_checked

        # If we pass a mix of good and null values...
        pool.update_availability(5, None, None, None)

        # Only the good values are changed.
        assert 5 == pool.licenses_owned
        assert 20 == pool.licenses_available
        assert 30 == pool.licenses_reserved
        assert 40 == pool.patrons_in_hold_queue


    def test_open_access_links(self):
        edition, pool = self._edition(with_open_access_download=True)
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        [oa1] = list(pool.open_access_links)

        # We have one open-access download, let's
        # add another.
        url = self._url
        media_type = MediaTypes.EPUB_MEDIA_TYPE
        link2, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url,
            source, media_type
        )
        oa2 = link2.resource

        # And let's add a link that's not an open-access download.
        url = self._url
        image, new = pool.identifier.add_link(
            Hyperlink.IMAGE, url, source, MediaTypes.JPEG_MEDIA_TYPE
        )
        self._db.commit()

        # Only the two open-access download links show up.
        assert set([oa1, oa2]) == set(pool.open_access_links)

    def test_better_open_access_pool_than(self):

        gutenberg_1 = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )

        gutenberg_2 = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )

        assert int(gutenberg_1.identifier.identifier) < int(gutenberg_2.identifier.identifier)

        standard_ebooks = self._licensepool(
            None, open_access=True, data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=True
        )

        # Make sure Feedbooks data source exists -- it's not created
        # by default.
        feedbooks_data_source = DataSource.lookup(
            self._db, DataSource.FEEDBOOKS, autocreate=True
        )
        feedbooks = self._licensepool(
            None, open_access=True, data_source_name=DataSource.FEEDBOOKS,
            with_open_access_download=True
        )

        overdrive = self._licensepool(
            None, open_access=False, data_source_name=DataSource.OVERDRIVE
        )

        suppressed = self._licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG
        )
        suppressed.suppressed = True

        def better(x,y):
            return x.better_open_access_pool_than(y)

        # We would rather have nothing at all than a suppressed
        # LicensePool.
        assert False == better(suppressed, None)

        # A non-open-access LicensePool is not considered at all.
        assert False == better(overdrive, None)

        # Something is better than nothing.
        assert True == better(gutenberg_1, None)

        # An open access book from a high-quality source beats one
        # from a low-quality source.
        assert True == better(standard_ebooks, gutenberg_1)
        assert True == better(feedbooks, gutenberg_1)
        assert False == better(gutenberg_1, standard_ebooks)

        # A high Gutenberg number beats a low Gutenberg number.
        assert True == better(gutenberg_2, gutenberg_1)
        assert False == better(gutenberg_1, gutenberg_2)

        # If a supposedly open-access LicensePool doesn't have an
        # open-access download resource, it will only be considered if
        # there is no other alternative.
        no_resource = self._licensepool(
            None, open_access=True,
            data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=False,
        )
        no_resource.open_access = True
        assert True == better(no_resource, None)
        assert False == better(no_resource, gutenberg_1)

    def test_with_complaint(self):
        library = self._default_library
        type = iter(Complaint.VALID_TYPES)
        type1 = next(type)
        type2 = next(type)
        type3 = next(type)

        work1 = self._work(
            "fiction work with complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        lp1 = work1.license_pools[0]
        lp1_complaint1 = self._complaint(
            lp1,
            type1,
            "lp1 complaint1 source",
            "lp1 complaint1 detail")
        lp1_complaint2 = self._complaint(
            lp1,
            type1,
            "lp1 complaint2 source",
            "lp1 complaint2 detail")
        lp1_complaint3 = self._complaint(
            lp1,
            type2,
            "work1 complaint3 source",
            "work1 complaint3 detail")
        lp1_resolved_complaint = self._complaint(
            lp1,
            type3,
            "work3 resolved complaint source",
            "work3 resolved complaint detail",
            datetime.datetime.now())

        work2 = self._work(
            "nonfiction work with complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)
        lp2 = work2.license_pools[0]
        lp2_complaint1 = self._complaint(
            lp2,
            type2,
            "work2 complaint1 source",
            "work2 complaint1 detail")
        lp2_resolved_complaint = self._complaint(
            lp2,
            type2,
            "work2 resolved complaint source",
            "work2 resolved complaint detail",
            datetime.datetime.now())

        work3 = self._work(
            "fiction work without complaint",
            language="eng",
            fiction=True,
            with_open_access_download=True)
        lp3 = work3.license_pools[0]
        lp3_resolved_complaint = self._complaint(
            lp3,
            type3,
            "work3 resolved complaint source",
            "work3 resolved complaint detail",
            datetime.datetime.now())

        work4 = self._work(
            "nonfiction work without complaint",
            language="eng",
            fiction=False,
            with_open_access_download=True)

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
        resolved_results = LicensePool.with_complaint(
            library, resolved=True).all()
        lp_ids = set([result[0].id for result in resolved_results])
        counts = set([result[1] for result in resolved_results])

        assert 3 == len(resolved_results)
        assert lp_ids == set([lp1.id, lp2.id, lp3.id])
        assert counts == set([1])

        # This library has none of the license pools that have complaints,
        # so passing it in to with_complaint() gives no results.
        library2 = self._library()
        assert 0 == LicensePool.with_complaint(library2).count()

        # If we add the default library's collection to this new library,
        # we start getting the same results.
        library2.collections.extend(library.collections)
        assert 3 == LicensePool.with_complaint(library2, resolved=None).count()

    def test_set_presentation_edition(self):
        """
        Make sure composite edition creation makes good choices when combining
        field data from provider, metadata wrangler, admin interface, etc. editions.
        """

        # Here's an Overdrive audiobook which also has data from the metadata
        # wrangler and from library staff.
        od, pool = self._edition(data_source_name=DataSource.OVERDRIVE, with_license_pool=True)
        od.medium = Edition.AUDIO_MEDIUM

        admin = self._edition(data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        admin.primary_identifier = pool.identifier

        mw = self._edition(data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)
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
            self._db.delete(c)
        self._db.commit()
        [jane], ignore = Contributor.lookup(self._db, "Doe, Jane")
        jane.family_name, jane.display_name = jane.default_names()
        admin.add_contributor(jane, Contributor.AUTHOR_ROLE)
        pool.set_presentation_edition()

        # The old contributor has been removed from the presentation
        # edition, and the new contributor added.
        assert set([jane]) == presentation.contributors

    def test_circulation_changelog(self):

        edition, pool = self._edition(with_license_pool=True)
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

    def test_update_availability_from_delta(self):
        """A LicensePool may have its availability information updated based
        on a single observed change.
        """

        edition, pool = self._edition(with_license_pool=True)
        assert None == pool.last_checked
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available

        add = CirculationEvent.DISTRIBUTOR_LICENSE_ADD
        checkout = CirculationEvent.DISTRIBUTOR_CHECKOUT
        analytics = MockAnalyticsProvider()
        assert 0 == analytics.count

        # This observation has no timestamp, but the pool has no
        # history, so we process it.
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        assert None == pool.last_checked
        assert 2 == pool.licenses_owned
        assert 2 == pool.licenses_available

        # Processing triggered two analytics events -- one for creating
        # the license pool and one for making it available.
        assert 2 == analytics.count

        # Now the pool has a history, and we can't fit an undated
        # observation into that history, so undated observations
        # have no effect on circulation data.
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        pool.last_checked = yesterday
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        assert 2 == pool.licenses_owned
        assert yesterday == pool.last_checked

        # However, outdated events are passed on to analytics so that
        # we record the fact that they happened... at some point.
        assert 3 == analytics.count

        # This observation is more recent than the last time the pool
        # was checked, so it's processed and the last check time is
        # updated.
        pool.update_availability_from_delta(checkout, now, 1, analytics)
        assert 2 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert now == pool.last_checked
        assert 4 == analytics.count

        # This event is less recent than the last time the pool was
        # checked, so it's ignored. Processing it is likely to do more
        # harm than good.
        pool.update_availability_from_delta(add, yesterday, 1, analytics)
        assert 2 == pool.licenses_owned
        assert now == pool.last_checked

        # It's still logged to analytics, though.
        assert 5 == analytics.count

        # This event is new but does not actually cause the
        # circulation to change at all.
        pool.update_availability_from_delta(add, now, 0, analytics)
        assert 2 == pool.licenses_owned
        assert now == pool.last_checked

        # We still send the analytics event.
        assert 6 == analytics.count

    def test_calculate_change_from_one_event(self):
        """Test the internal method called by update_availability_from_delta."""
        CE = CirculationEvent

        # Create a LicensePool with a large number of available licenses.
        edition, pool = self._edition(with_license_pool=True)
        pool.licenses_owned = 5
        pool.licenses_available = 4
        pool.licenses_reserved = 0
        pool.patrons_in_hold_queue = 0

        # Calibrate _calculate_change_from_one_event by sending it an
        # event that makes no difference. This lets us see what a
        # 'status quo' response from the method would look like.
        calc = pool._calculate_change_from_one_event
        assert (5,4,0,0) == calc(CE.DISTRIBUTOR_CHECKIN, 0)

        # If there ever appear to be more licenses available than
        # owned, the number of owned licenses is left alone. It's
        # possible that we have more licenses than we thought, but
        # it's more likely that a license has expired or otherwise
        # been removed.
        assert (5,5,0,0) == calc(CE.DISTRIBUTOR_CHECKIN, 3)

        # But we don't bump up the number of available licenses just
        # because one becomes available.
        assert (5,5,0,0) == calc(CE.DISTRIBUTOR_CHECKIN, 1)

        # When you signal a hold on a book that's available, we assume
        # that the book has stopped being available.
        assert (5,0,0,3) == calc(CE.DISTRIBUTOR_HOLD_PLACE, 3)

        # If a license stops being owned, it implicitly stops being
        # available. (But we don't know if the license that became
        # unavailable is one of the ones currently checked out to
        # someone, or one of the other ones.)
        assert (3,3,0,0) == calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 2)

        # If a license stops being available, it doesn't stop
        # being owned.
        assert (5,3,0,0) == calc(CE.DISTRIBUTOR_CHECKOUT, 1)

        # None of these numbers will go below zero.
        assert (0,0,0,0) == calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 100)

        # Newly added licenses start out available if there are no
        # patrons in the hold queue.
        assert (6,5,0,0) == calc(CE.DISTRIBUTOR_LICENSE_ADD, 1)

        # Now let's run some tests with a LicensePool that has a large holds
        # queue.
        pool.licenses_owned = 5
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        pool.patrons_in_hold_queue = 3
        assert (5,0,1,3) == calc(CE.DISTRIBUTOR_HOLD_PLACE, 0)

        # When you signal a hold on a book that already has holds, it
        # does nothing but increase the number of patrons in the hold
        # queue.
        assert (5,0,1,6) == calc(CE.DISTRIBUTOR_HOLD_PLACE, 3)

        # A checkin event has no effect...
        assert (5,0,1,3) == calc(CE.DISTRIBUTOR_CHECKIN, 1)

        # ...because it's presumed that it will be followed by an
        # availability notification event, which takes a patron off
        # the hold queue and adds them to the reserved list.
        assert (5,0,2,2) == calc(CE.DISTRIBUTOR_AVAILABILITY_NOTIFY, 1)

        # The only exception is if the checkin event wipes out the
        # entire holds queue, in which case the number of available
        # licenses increases.  (But nothing else changes -- we're
        # still waiting for the availability notification events.)
        assert (5,3,1,3) == calc(CE.DISTRIBUTOR_CHECKIN, 6)

        # Again, note that even though six copies were checked in,
        # we're not assuming we own more licenses than we
        # thought. It's more likely that the sixth license expired and
        # we weren't notified.

        # When there are no licenses available, a checkout event
        # draws from the pool of licenses reserved instead.
        assert (5,0,0,3) == calc(CE.DISTRIBUTOR_CHECKOUT, 2)

        # Newly added licenses do not start out available if there are
        # patrons in the hold queue.
        assert (6,0,1,3) == calc(CE.DISTRIBUTOR_LICENSE_ADD, 1)

    def test_loan_to_patron(self):
        # Test our ability to loan LicensePools to Patrons.
        #
        # TODO: The path where the LicensePool is loaned to an
        # IntegrationClient rather than a Patron is currently not
        # directly tested.

        pool = self._licensepool(None)
        patron = self._patron()
        now = utc_now()
        patron.last_loan_activity_sync = now

        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        fulfillment = pool.delivery_mechanisms[0]
        external_identifier = self._str
        loan, is_new = pool.loan_to(
            patron, start=yesterday, end=tomorrow, 
            fulfillment=fulfillment, external_identifier=external_identifier
        )

        assert True == is_new
        assert isinstance(loan, Loan)
        assert pool == loan.license_pool
        assert patron == loan.patron
        assert yesterday == loan.start
        assert tomorrow == loan.end
        assert fulfillment == loan.fulfillment
        assert external_identifier == loan.external_identifier

        # Issuing a loan locally created uncertainty about a patron's
        # loans, since we don't know how the external vendor dealt
        # with the request. The last_loan_activity_sync has been
        # cleared out so we know to check back with the source of
        # truth.
        assert None == patron.last_loan_activity_sync

        # 'Creating' a loan that already exists does not create any
        # uncertainty.
        patron.last_loan_activity_sync = now
        loan2, is_new = pool.loan_to(
            patron, start=yesterday, end=tomorrow, 
            fulfillment=fulfillment, external_identifier=external_identifier
        )
        assert False == is_new
        assert loan == loan2
        assert now == patron.last_loan_activity_sync


    def test_on_hold_to_patron(self):
        # Test our ability to put a Patron in the holds queue for a LicensePool.
        #
        # TODO: The path where the 'patron' is an IntegrationClient
        # rather than a Patron is currently not directly tested.

        pool = self._licensepool(None)
        patron = self._patron()
        now = utc_now()
        patron.last_loan_activity_sync = now

        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        fulfillment = pool.delivery_mechanisms[0]
        position = 99
        external_identifier = self._str
        hold, is_new = pool.on_hold_to(
            patron, start=yesterday, end=tomorrow, 
            position=position, external_identifier=external_identifier
        )

        assert True == is_new
        assert isinstance(hold, Hold)
        assert pool == hold.license_pool
        assert patron == hold.patron
        assert yesterday == hold.start
        assert tomorrow == hold.end
        assert position == hold.position
        assert external_identifier == hold.external_identifier

        # Issuing a hold locally created uncertainty about a patron's
        # loans, since we don't know how the external vendor dealt
        # with the request. The last_loan_activity_sync has been
        # cleared out so we know to check back with the source of
        # truth.
        assert None == patron.last_loan_activity_sync

        # 'Creating' a hold that already exists does not create any
        # uncertainty.
        patron.last_loan_activity_sync = now
        hold2, is_new = pool.on_hold_to(
            patron, start=yesterday, end=tomorrow, 
            position=position, external_identifier=external_identifier
        )
        assert False == is_new
        assert hold == hold2
        assert now == patron.last_loan_activity_sync
        

class TestLicensePoolDeliveryMechanism(DatabaseTest):
    def test_lpdm_change_may_change_open_access_status(self):
        # Here's a book that's not open access.
        edition, pool = self._edition(with_license_pool=True)
        assert False == pool.open_access

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
        assert False == pool.open_access

        # Now give it an open-access LPDM.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
            data_source, content_type
        )
        oa_lpdm = LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme,
            RightsStatus.GENERIC_OPEN_ACCESS, link.resource
        )

        # Now it's open access.
        assert True == pool.open_access

        # Delete the open-access LPDM, and it stops being open access.
        oa_lpdm.delete()
        assert False == pool.open_access

    def test_set_rights_status(self):
        # Here's a non-open-access book.
        edition, pool = self._edition(with_license_pool=True)
        pool.open_access = False
        [lpdm] = pool.delivery_mechanisms

        # We set its rights status to 'in copyright', and nothing changes.
        uri = RightsStatus.IN_COPYRIGHT
        status = lpdm.set_rights_status(uri)
        assert status == lpdm.rights_status
        assert uri == status.uri
        assert RightsStatus.NAMES.get(uri) == status.name
        assert False == pool.open_access

        # Setting it again won't change anything.
        status2 = lpdm.set_rights_status(uri)
        assert status == status2

        # Set the rights status to a different URL, we change to a different
        # RightsStatus object.
        uri2 = "http://unknown"
        status3 = lpdm.set_rights_status(uri2)
        assert status != status3
        assert RightsStatus.UNKNOWN == status3.uri
        assert RightsStatus.NAMES.get(RightsStatus.UNKNOWN) == status3.name

        # Set the rights status to a URL that implies open access,
        # and the status of the LicensePool is changed.
        open_access_uri = RightsStatus.GENERIC_OPEN_ACCESS
        open_access_status = lpdm.set_rights_status(open_access_uri)
        assert open_access_uri == open_access_status.uri
        assert RightsStatus.NAMES.get(open_access_uri) == open_access_status.name
        assert True == pool.open_access

        # Set it back to a URL that does not imply open access, and
        # the status of the LicensePool is changed back.
        non_open_access_status = lpdm.set_rights_status(uri)
        assert False == pool.open_access

        # Now add a second delivery mechanism, so the pool has one
        # open-access and one commercial delivery mechanism.
        lpdm2 = pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY, None)
        assert 2 == len(pool.delivery_mechanisms)

        # Now the pool is open access again
        assert True == pool.open_access

        # But if we change the new delivery mechanism to non-open
        # access, the pool won't be open access anymore either.
        lpdm2.set_rights_status(uri)
        assert False == pool.open_access

    def test_uniqueness_constraint(self):
        # with_open_access_download will create a LPDM
        # for the open-access download.
        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        [lpdm] = pool.delivery_mechanisms

        # We can create a second LPDM with the same data type and DRM status,
        # so long as the resource is different.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, self._url,
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
        assert None == lpdm3.resource

        # But we can't create a second such LPDM -- it violates a
        # constraint of a unique index.
        pytest.raises(
            IntegrityError, create, self._db,
            LicensePoolDeliveryMechanism,
            delivery_mechanism=lpdm3.delivery_mechanism,
            identifier=pool.identifier,
            data_source=pool.data_source,
            resource=None
        )
        self._db.rollback()

    def test_compatible_with(self):
        """Test the rules about which LicensePoolDeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """

        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        [mech] = pool.delivery_mechanisms

        # Test the simple cases.
        assert False == mech.compatible_with(None)
        assert False == mech.compatible_with("Not a LicensePoolDeliveryMechanism")
        assert True == mech.compatible_with(mech)

        # Now let's set up a scenario that works and then see how it fails.
        self._add_generic_delivery_mechanism(pool)

        # This book has two different LicensePoolDeliveryMechanisms
        # with the same underlying DeliveryMechanism. They're
        # compatible.
        [mech1, mech2] = pool.delivery_mechanisms
        assert mech1.id != mech2.id
        assert mech1.delivery_mechanism == mech2.delivery_mechanism
        assert True == mech1.compatible_with(mech2)

        # The LicensePoolDeliveryMechanisms must identify the same
        # book from the same data source.
        mech1.data_source_id = self._id
        assert False == mech1.compatible_with(mech2)

        mech1.data_source_id = mech2.data_source_id
        mech1.identifier_id = self._id
        assert False == mech1.compatible_with(mech2)
        mech1.identifier_id = mech2.identifier_id

        # The underlying delivery mechanisms don't have to be exactly
        # the same, but they must be compatible.
        pdf_adobe, ignore = DeliveryMechanism.lookup(
            self._db, MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM
        )
        mech1.delivery_mechanism = pdf_adobe
        self._db.commit()
        assert False == mech1.compatible_with(mech2)

        streaming, ignore = DeliveryMechanism.lookup(
            self._db, DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM
        )
        mech1.delivery_mechanism = streaming
        self._db.commit()
        assert True == mech1.compatible_with(mech2)

    def test_compatible_with_calls_compatible_with_on_deliverymechanism(self):
        # Create two LicensePoolDeliveryMechanisms with different
        # media types.
        edition, pool = self._edition(with_license_pool=True,
                                      with_open_access_download=True)
        [mech1] = pool.delivery_mechanisms
        mech2 = self._add_generic_delivery_mechanism(pool)
        mech2.delivery_mechanism, ignore = DeliveryMechanism.lookup(
            self._db, MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        self._db.commit()

        assert True == mech1.is_open_access
        assert False == mech2.is_open_access

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

    @parameterized.expand([
        ('ascii_sy', 'a', 'a', 'a'),
        ('', 'ą', 'ą', 'ą')
    ])
    def test_repr(self, _, data_source, identifier, delivery_mechanism):
        """Test that LicensePoolDeliveryMechanism.__repr__ correctly works for both ASCII and non-ASCII symbols.

        :param _: Name of the test case
        :type _: str

        :param data_source: String representation of the data source
        :type data_source: str

        :param identifier: String representation of the publication's identifier
        :type identifier: str

        :param delivery_mechanism: String representation of the delivery mechanism
        :type delivery_mechanism: str
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
