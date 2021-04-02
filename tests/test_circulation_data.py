import pytest

from copy import deepcopy
import datetime
import pytz

from ..metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LicenseData,
    LinkData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
)

from ..model import (
    Collection,
    DataSource,
    DeliveryMechanism,
    Hyperlink,
    Identifier,
    License,
    Representation,
    RightsStatus,
    Subject,
)
from ..model.configuration import ExternalIntegrationLink

from ..testing import (
    DatabaseTest,
    DummyHTTPClient,
)

from ..s3 import MockS3Uploader


class TestCirculationData(DatabaseTest):

    def test_circulationdata_may_require_collection(self):
        """Depending on the information provided in a CirculationData
        object, it might or might not be possible to call apply()
        without providing a Collection.
        """

        identifier = IdentifierData(Identifier.OVERDRIVE_ID, "1")
        format = FormatData(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            rights_uri=RightsStatus.IN_COPYRIGHT
        )
        circdata = CirculationData(
            DataSource.OVERDRIVE,
            primary_identifier=identifier,
            formats=[format]
        )
        circdata.apply(self._db, collection=None)

        # apply() has created a LicensePoolDeliveryMechanism for this
        # title, even though there are no LicensePools for it.
        identifier_obj, ignore = identifier.load(self._db)
        assert [] == identifier_obj.licensed_through
        [lpdm] = identifier_obj.delivery_mechanisms
        assert DataSource.OVERDRIVE == lpdm.data_source.name
        assert RightsStatus.IN_COPYRIGHT == lpdm.rights_status.uri

        mechanism = lpdm.delivery_mechanism
        assert Representation.EPUB_MEDIA_TYPE == mechanism.content_type
        assert DeliveryMechanism.NO_DRM == mechanism.drm_scheme

        # But if we put some information in the CirculationData
        # that can only be stored in a LicensePool, there's trouble.
        circdata.licenses_owned = 0
        with pytest.raises(ValueError) as excinfo:
            circdata.apply(self._db, collection=None)
        assert 'Cannot store circulation information because no Collection was provided.' in str(excinfo.value)

    def test_circulationdata_can_be_deepcopied(self):
        # Check that we didn't put something in the CirculationData that
        # will prevent it from being copied. (e.g., self.log)

        subject = SubjectData(Subject.TAG, "subject")
        contributor = ContributorData()
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        link = LinkData(Hyperlink.OPEN_ACCESS_DOWNLOAD, "example.epub")
        format = FormatData(Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        rights_uri = RightsStatus.GENERIC_OPEN_ACCESS

        circulation_data = CirculationData(
            DataSource.GUTENBERG,
            primary_identifier=identifier,
            links=[link],
            licenses_owned=5,
            licenses_available=5,
            licenses_reserved=None,
            patrons_in_hold_queue=None,
            formats=[format],
            default_rights_uri=rights_uri,
        )

        circulation_data_copy = deepcopy(circulation_data)

        # If deepcopy didn't throw an exception we're ok.
        assert circulation_data_copy is not None


    def test_links_filtered(self):
        # Tests that passed-in links filter down to only the relevant ones.
        link1 = LinkData(Hyperlink.OPEN_ACCESS_DOWNLOAD, "example.epub")
        link2 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/")
        link3 = LinkData(rel=Hyperlink.DESCRIPTION, content="foo")
        link4 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href="http://thumbnail.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        link5 = LinkData(
            rel=Hyperlink.IMAGE, href="http://example.com/", thumbnail=link4,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        links = [link1, link2, link3, link4, link5]

        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        circulation_data = CirculationData(
            DataSource.GUTENBERG,
            primary_identifier=identifier,
            links=links,
        )

        filtered_links = sorted(circulation_data.links, key=lambda x:x.rel)

        assert [link1] == filtered_links


    def test_explicit_formatdata(self):
        # Creating an edition with an open-access download will
        # automatically create a delivery mechanism.
        edition, pool = self._edition(with_open_access_download=True)

        # Let's also add a DRM format.
        drm_format = FormatData(
            content_type=Representation.PDF_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )

        circulation_data = CirculationData(
            formats=[drm_format],
            data_source=edition.data_source,
            primary_identifier=edition.primary_identifier,
        )
        circulation_data.apply(self._db, pool.collection)

        [epub, pdf] = sorted(pool.delivery_mechanisms,
                             key=lambda x: x.delivery_mechanism.content_type)
        assert epub.resource == pool.best_open_access_resource

        assert Representation.PDF_MEDIA_TYPE == pdf.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == pdf.delivery_mechanism.drm_scheme

        # If we tell Metadata to replace the list of formats, we only
        # have the one format we manually created.
        replace = ReplacementPolicy(
                formats=True,
            )
        circulation_data.apply(self._db, pool.collection, replace=replace)
        [pdf] = pool.delivery_mechanisms
        assert Representation.PDF_MEDIA_TYPE == pdf.delivery_mechanism.content_type

    def test_apply_removes_old_formats_based_on_replacement_policy(self):
        edition, pool = self._edition(with_license_pool=True)

        # Start with one delivery mechanism for this pool.
        for lpdm in pool.delivery_mechanisms:
            self._db.delete(lpdm)

        old_lpdm = pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT, None)

        # And it has been loaned.
        patron = self._patron()
        loan, ignore = pool.loan_to(patron, fulfillment=old_lpdm)
        assert old_lpdm == loan.fulfillment

        # We have new circulation data that has a different format.
        format = FormatData(
            content_type=Representation.EPUB_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )
        circulation_data = CirculationData(
            formats=[format],
            data_source=edition.data_source,
            primary_identifier=edition.primary_identifier,
        )

        # If we apply the new CirculationData with formats false in the policy,
        # we'll add the new format, but keep the old one as well.
        replacement_policy = ReplacementPolicy(formats=False)
        circulation_data.apply(self._db, pool.collection, replacement_policy)

        assert 2 == len(pool.delivery_mechanisms)
        assert (set([Representation.PDF_MEDIA_TYPE, Representation.EPUB_MEDIA_TYPE]) ==
            set([lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms]))
        assert old_lpdm == loan.fulfillment

        # But if we make formats true in the policy, we'll delete the old format
        # and remove it from its loan.
        replacement_policy = ReplacementPolicy(formats=True)
        circulation_data.apply(self._db, pool.collection, replacement_policy)

        assert 1 == len(pool.delivery_mechanisms)
        assert Representation.EPUB_MEDIA_TYPE == pool.delivery_mechanisms[0].delivery_mechanism.content_type
        assert None == loan.fulfillment

    def test_apply_adds_new_licenses(self):
        edition, pool = self._edition(with_license_pool=True)

        # Start with one license for this pool.
        old_license = self._license(
            pool, expires=None, remaining_checkouts=2,
            concurrent_checkouts=3,
        )

        # And it has been loaned.
        patron = self._patron()
        loan, ignore = old_license.loan_to(patron)
        assert old_license == loan.license

        # We have new circulation data that has a different license.
        license_data = LicenseData(
            identifier="8c5fdbfe-c26e-11e8-8706-5254009434c4",
            checkout_url="https://borrow2",
            status_url="https://status2",
            expires=(datetime.datetime.now() + datetime.timedelta(days=7)),
            remaining_checkouts=None, concurrent_checkouts=1)

        circulation_data = CirculationData(
            licenses=[license_data],
            data_source=edition.data_source,
            primary_identifier=edition.primary_identifier,
        )

        # If we apply the new CirculationData, we'll add the new license,
        # but keep the old one as well.
        circulation_data.apply(self._db, pool.collection)
        self._db.commit()

        assert 2 == len(pool.licenses)
        assert (set([old_license.identifier, license_data.identifier]) ==
            set([license.identifier for license in pool.licenses]))
        assert old_license == loan.license

    def test_apply_creates_work_and_presentation_edition_if_needed(self):
        edition = self._edition()
        # This pool doesn't have a presentation edition or a work yet.
        pool = self._licensepool(edition)

        # We have new circulation data for this pool.
        circulation_data = CirculationData(
            formats=[],
            data_source=edition.data_source,
            primary_identifier=edition.primary_identifier,
        )

        # If we apply the new CirculationData the work gets both a
        # presentation and a work.
        replacement_policy = ReplacementPolicy()
        circulation_data.apply(self._db, pool.collection, replacement_policy)

        assert edition == pool.presentation_edition
        assert pool.work != None

        # If we have another new pool for the same book in another
        # collection, it will share the work.
        collection = self._collection()
        pool2 = self._licensepool(edition, collection=collection)
        circulation_data.apply(self._db, pool2.collection, replacement_policy)
        assert edition == pool2.presentation_edition
        assert pool.work == pool2.work

    def test_license_pool_sets_default_license_values(self):
        """We have no information about how many copies of the book we've
        actually licensed, but a LicensePool can be created anyway,
        so we can store format information.
        """
        identifier = IdentifierData(Identifier.OVERDRIVE_ID, "1")
        drm_format = FormatData(
            content_type=Representation.PDF_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )
        circulation = CirculationData(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=identifier,
            formats=[drm_format],
        )
        collection = self._default_collection
        pool, is_new = circulation.license_pool(
            self._db, collection
        )
        assert True == is_new
        assert collection == pool.collection

        # We start with the conservative assumption that we own no
        # licenses for the book.
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

    def test_implicit_format_for_open_access_link(self):
        # A format is a delivery mechanism.  We handle delivery on open access
        # pools from our mirrored content in S3.
        # Tests that when a link is open access, a pool can be delivered.

        edition, pool = self._edition(with_license_pool=True)

        # This is the delivery mechanism created by default when you
        # create a book with _edition().
        [epub] = pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == epub.delivery_mechanism.content_type
        assert DeliveryMechanism.ADOBE_DRM == epub.delivery_mechanism.drm_scheme


        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.PDF_MEDIA_TYPE,
            href=self._url
        )
        circulation_data = CirculationData(
            data_source=DataSource.GUTENBERG,
            primary_identifier=edition.primary_identifier,
            links=[link],
        )

        replace = ReplacementPolicy(
                formats=True,
            )
        circulation_data.apply(self._db, pool.collection, replace)

        # We destroyed the default delivery format and added a new,
        # open access delivery format.
        [pdf] = pool.delivery_mechanisms
        assert Representation.PDF_MEDIA_TYPE == pdf.delivery_mechanism.content_type
        assert DeliveryMechanism.NO_DRM == pdf.delivery_mechanism.drm_scheme

        circulation_data = CirculationData(
            data_source=DataSource.GUTENBERG,
            primary_identifier=edition.primary_identifier,
            links=[]
        )
        replace = ReplacementPolicy(
                formats=True,
                links=True,
            )
        circulation_data.apply(self._db, pool.collection, replace)

        # Now we have no formats at all.
        assert 0 == len(pool.delivery_mechanisms)

    def test_rights_status_default_rights_passed_in(self):
        identifier = IdentifierData(
            Identifier.GUTENBERG_ID,
            "abcd",
        )
        link = LinkData(
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url
        )

        circulation_data = CirculationData(
            data_source=DataSource.OA_CONTENT_SERVER,
            primary_identifier=identifier,
            default_rights_uri = RightsStatus.CC_BY,
            links=[link],
        )

        replace = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            self._db, self._default_collection
        )
        circulation_data.apply(self._db, pool.collection, replace)
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        # The rights status is the one that was passed in to CirculationData.
        assert RightsStatus.CC_BY == pool.delivery_mechanisms[0].rights_status.uri

    def test_rights_status_default_rights_from_data_source(self):
        identifier = IdentifierData(
            Identifier.GUTENBERG_ID,
            "abcd",
        )
        link = LinkData(
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url
        )

        circulation_data = CirculationData(
            data_source=DataSource.OA_CONTENT_SERVER,
            primary_identifier=identifier,
            links=[link],
        )

        replace = ReplacementPolicy(
            formats=True,
        )

        # This pool starts off as not being open-access.
        pool, ignore = circulation_data.license_pool(
            self._db, self._default_collection
        )
        assert False == pool.open_access

        circulation_data.apply(self._db, pool.collection, replace)

        # The pool became open-access because it was given a
        # link that came from the OS content server.
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        # The rights status is the default for the OA content server.
        assert RightsStatus.GENERIC_OPEN_ACCESS == pool.delivery_mechanisms[0].rights_status.uri

    def test_rights_status_open_access_link_no_rights_uses_data_source_default(self):
        identifier = IdentifierData(
            Identifier.GUTENBERG_ID,
            "abcd",
        )

        # Here's a CirculationData that will create an open-access
        # LicensePoolDeliveryMechanism.
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url
        )
        circulation_data = CirculationData(
            data_source=DataSource.GUTENBERG,
            primary_identifier=identifier,
            links=[link],
        )
        replace_formats = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            self._db, self._default_collection
        )
        pool.open_access = False

        # Applying this CirculationData to a LicensePool makes it
        # open-access.
        circulation_data.apply(self._db, pool.collection, replace_formats)
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)

        # The delivery mechanism's rights status is the default for
        # the data source.
        assert RightsStatus.PUBLIC_DOMAIN_USA == pool.delivery_mechanisms[0].rights_status.uri

        # Even if a commercial source like Overdrive should offer a
        # link with rel="open access", unless we know it's an
        # open-access link we will give it a RightsStatus of
        # IN_COPYRIGHT.
        identifier = IdentifierData(
            Identifier.OVERDRIVE_ID,
            "abcd",
        )
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url
        )

        circulation_data = CirculationData(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=identifier,
            links=[link],
        )

        pool, ignore = circulation_data.license_pool(
            self._db, self._default_collection
        )
        pool.open_access = False
        circulation_data.apply(self._db, pool.collection, replace_formats)
        assert (RightsStatus.IN_COPYRIGHT ==
            pool.delivery_mechanisms[0].rights_status.uri)

        assert False == pool.open_access

    def test_rights_status_open_access_link_with_rights(self):
        identifier = IdentifierData(
            Identifier.OVERDRIVE_ID,
            "abcd",
        )
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url,
            rights_uri=RightsStatus.CC_BY_ND,
        )

        circulation_data = CirculationData(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=identifier,
            links=[link],
        )
        replace = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            self._db, self._default_collection
        )
        circulation_data.apply(self._db, pool.collection, replace)
        assert True == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        assert RightsStatus.CC_BY_ND == pool.delivery_mechanisms[0].rights_status.uri

    def test_rights_status_commercial_link_with_rights(self):
        identifier = IdentifierData(
            Identifier.OVERDRIVE_ID,
            "abcd",
        )
        link = LinkData(
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )
        format = FormatData(
            content_type=link.media_type,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
            link=link,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )

        circulation_data = CirculationData(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=identifier,
            links=[link],
            formats=[format],
        )

        replace = ReplacementPolicy(
            formats=True,
        )

        pool, ignore = circulation_data.license_pool(
            self._db, self._default_collection
        )
        circulation_data.apply(self._db, pool.collection, replace)
        assert False == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)
        assert RightsStatus.IN_COPYRIGHT == pool.delivery_mechanisms[0].rights_status.uri

    def test_format_change_may_change_open_access_status(self):

        # In this test, whenever we call CirculationData.apply(), we
        # want to destroy the old list of formats and recreate it.
        replace_formats = ReplacementPolicy(formats=True)

        # Here's a seemingly ordinary non-open-access LicensePool.
        edition, pool = self._edition(with_license_pool=True)
        assert False == pool.open_access

        # One day, we learn that it has an open-access delivery mechanism.
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url,
            rights_uri=RightsStatus.CC_BY_ND,
        )

        circulation_data = CirculationData(
            data_source=pool.data_source,
            primary_identifier=pool.identifier,
            links=[link],
        )

        # Applying this information turns the pool into an open-access pool.
        circulation_data.apply(
            self._db, pool.collection, replace=replace_formats
        )
        assert True == pool.open_access

        # Then we find out it was a mistake -- the book is in copyright.
        format = FormatData(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            rights_uri=RightsStatus.IN_COPYRIGHT
        )
        circulation_data = CirculationData(
            data_source=pool.data_source,
            primary_identifier=pool.identifier,
            formats=[format]
        )
        circulation_data.apply(
            self._db, pool.collection, replace=replace_formats
        )

        # The original LPDM has been removed and only the new one remains.
        assert False == pool.open_access
        assert 1 == len(pool.delivery_mechanisms)


class TestMetaToModelUtility(DatabaseTest):

    def test_open_access_content_mirrored(self):
        # Make sure that open access material links are translated to our S3 buckets, and that
        # commercial material links are left as is.
        # Note: Mirroring tests passing does not guarantee that all code now
        # correctly calls on CirculationData, as well as Metadata.  This is a risk.

        mirrors = dict(books_mirror=MockS3Uploader(),covers_mirror=None)
        mirror_type = ExternalIntegrationLink.OPEN_ACCESS_BOOKS
        # Here's a book.
        edition, pool = self._edition(with_license_pool=True)

        # Here's a link to the content of the book, which will be mirrored.
        link_mirrored = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="http://example.com/",
            media_type=Representation.EPUB_MEDIA_TYPE,
            content="i am a tiny book"
        )

        # This link will not be mirrored.
        link_unmirrored = LinkData(
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD, href="http://example.com/2",
            media_type=Representation.EPUB_MEDIA_TYPE,
            content="i am a pricy book"
        )

        # Apply the metadata.
        policy = ReplacementPolicy(mirrors=mirrors)

        metadata = Metadata(data_source=edition.data_source,
        	links=[link_mirrored, link_unmirrored],
    	)
        metadata.apply(edition, pool.collection, replace=policy)
        # make sure the refactor is done right, and metadata does not upload
        assert 0 == len(mirrors[mirror_type].uploaded)


        circulation_data = CirculationData(
            data_source=edition.data_source,
            primary_identifier=edition.primary_identifier,
            links=[link_mirrored, link_unmirrored],
        )
        circulation_data.apply(self._db, pool.collection, replace=policy)

        # make sure the refactor is done right, and circulation does upload
        assert 1 == len(mirrors[mirror_type].uploaded)

        # Only the open-access link has been 'mirrored'.
        [book] = mirrors[mirror_type].uploaded

        # It's remained an open-access link.
        assert (
            [Hyperlink.OPEN_ACCESS_DOWNLOAD] ==
            [x.rel for x in book.resource.links])


        # It's been 'mirrored' to the appropriate S3 bucket.
        assert book.mirror_url.startswith('https://test-content-bucket.s3.amazonaws.com/')
        expect = '/%s/%s.epub' % (
            edition.primary_identifier.identifier,
            edition.title
        )
        assert book.mirror_url.endswith(expect)

        # make sure the mirrored link is safely on edition
        sorted_edition_links = sorted(pool.identifier.links, key=lambda x: x.rel)
        unmirrored_representation, mirrored_representation = [edlink.resource.representation for edlink in sorted_edition_links]
        assert mirrored_representation.mirror_url.startswith('https://test-content-bucket.s3.amazonaws.com/')

        # make sure the unmirrored link is safely on edition
        assert 'http://example.com/2' == unmirrored_representation.url
        # make sure the unmirrored link has not been translated to an S3 URL
        assert None == unmirrored_representation.mirror_url


    def test_mirror_open_access_link_fetch_failure(self):
        mirrors = dict(books_mirror=MockS3Uploader())
        h = DummyHTTPClient()

        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        policy = ReplacementPolicy(mirrors=mirrors, http_get=h.do_get)
        circulation_data = CirculationData(
            data_source=edition.data_source,
            primary_identifier=edition.primary_identifier,
        )

        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url,
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content,
        )

        h.queue_response(403)

        circulation_data.mirror_link(pool, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # Fetch failed, so we should have a fetch exception but no mirror url.
        assert representation.fetch_exception != None
        assert None == representation.mirror_exception
        assert None == representation.mirror_url
        assert link.href == representation.url
        assert representation.fetched_at != None
        assert None == representation.mirrored_at

        # The license pool is suppressed when fetch fails.
        assert True == pool.suppressed
        assert representation.fetch_exception in pool.license_exception


    def test_mirror_open_access_link_mirror_failure(self):
        mirrors = dict(books_mirror=MockS3Uploader(fail=True),covers_mirror=None)
        h = DummyHTTPClient()

        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        policy = ReplacementPolicy(mirrors=mirrors, http_get=h.do_get)

        circulation_data = CirculationData(
            data_source=edition.data_source,
            primary_identifier=edition.primary_identifier,
        )

        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url,
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content
        )

        h.queue_response(200, media_type=Representation.EPUB_MEDIA_TYPE)

        circulation_data.mirror_link(pool, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # The representation was fetched successfully.
        assert None == representation.fetch_exception
        assert representation.fetched_at != None

        # But mirroing failed.
        assert representation.mirror_exception != None
        assert None == representation.mirrored_at
        assert link.media_type == representation.media_type
        assert link.href == representation.url

        # The mirror url was never set.
        assert None == representation.mirror_url

        # Book content is still there since it wasn't mirrored.
        assert representation.content != None

        # The license pool is suppressed when mirroring fails.
        assert True == pool.suppressed
        assert representation.mirror_exception in pool.license_exception

    def test_has_open_access_link(self):
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")

        circulationdata = CirculationData(
            DataSource.GUTENBERG,
            identifier,
        )

        # No links
        assert False == circulationdata.has_open_access_link

        linkdata = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            href=self._url,
        )
        circulationdata.links = [linkdata]

        # Open-access link with no explicit rights URI.
        assert True == circulationdata.has_open_access_link

        # Open-access link with contradictory rights URI.
        linkdata.rights_uri = RightsStatus.IN_COPYRIGHT
        assert False == circulationdata.has_open_access_link

        # Open-access link with consistent rights URI.
        linkdata.rights_uri = RightsStatus.GENERIC_OPEN_ACCESS
        assert True == circulationdata.has_open_access_link

    def test_availability_needs_update(self):
        """Test the logic that controls whether a LicensePool's availability
        information should actually be updated.
        """
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        now = datetime.datetime.now(tz=pytz.UTC)
        yesterday = now - datetime.timedelta(days=1)
        recent_data = CirculationData(DataSource.GUTENBERG, identifier)
        # CirculationData.last_checked defaults to the current time.
        assert (recent_data.last_checked - now).total_seconds() < 10
        old_data = CirculationData(
            DataSource.GUTENBERG, identifier, last_checked=yesterday
        )

        edition, pool = self._edition(with_license_pool=True)

        # A pool that has never been checked always needs to be updated.
        pool.last_checked = None
        assert True == recent_data._availability_needs_update(pool)
        assert True == old_data._availability_needs_update(pool)

        # A pool that has been checked before only needs to be updated
        # if the information is at least as new as what we had before.
        pool.last_checked = now
        assert True == recent_data._availability_needs_update(pool)
        assert False == old_data._availability_needs_update(pool)

