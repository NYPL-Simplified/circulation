import datetime
import os

from ..model import (
    Contribution,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    EditionConstants,
    LicensePool,
    MediaTypes,
    Work,
)
from ..opds2_import import OPDS2Importer
from .test_opds_import import OPDSTest


class TestOPDS2Importer(OPDSTest):
    @staticmethod
    def _get_edition_by_identifier(editions, identifier):
        """Find an edition in the list by its identifier.

        :param editions: List of editions
        :type editions: List[Edition]

        :return: Edition with the specified id (if any)
        :rtype: Optional[Edition]
        """
        for edition in editions:
            if edition.primary_identifier.urn == identifier:
                return edition

        return None

    @staticmethod
    def _get_license_pool_by_identifier(pools, identifier):
        """Find a license pool in the list by its identifier.

        :param pools: List of license pools
        :type pools: List[LicensePool]

        :return: Edition with the specified id (if any)
        :rtype: Optional[LicensePool]
        """
        for pool in pools:
            if pool.identifier.urn == identifier:
                return pool

        return None

    @staticmethod
    def _get_work_by_identifier(works, identifier):
        """Find a license pool in the list by its identifier.

        :param works: List of license pools
        :type works: List[Work]

        :return: Edition with the specified id (if any)
        :rtype: Optional[Work]
        """
        for work in works:
            if work.presentation_edition.primary_identifier.urn == identifier:
                return work

        return None

    def sample_opds(self, filename):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "opds2")
        return open(os.path.join(resource_path, filename)).read()

    def test(self):
        # Arrange
        collection = self._default_collection
        data_source = DataSource.lookup(
            self._db, "OPDS 2.0 Data Source", autocreate=True
        )

        collection.data_source = data_source

        importer = OPDS2Importer(self._db, collection)
        content_server_feed = self.sample_opds("feed.json")

        # Act
        imported_editions, pools, works, failures = importer.import_from_feed(
            content_server_feed
        )

        # Assert

        # 1. Make sure that editions contain all required metadata
        assert True == isinstance(imported_editions, list)
        assert 2 == len(imported_editions)

        # 1.1. Edition with open-access links (Moby-Dick)
        moby_dick_edition = self._get_edition_by_identifier(
            imported_editions, "urn:isbn:978-3-16-148410-0"
        )
        assert True == isinstance(moby_dick_edition, Edition)

        assert "Moby-Dick" == moby_dick_edition.title
        assert "eng" == moby_dick_edition.language
        assert "eng" == moby_dick_edition.language
        assert EditionConstants.BOOK_MEDIUM == moby_dick_edition.medium
        assert "Herman Melville" == moby_dick_edition.author

        assert 1 == len(moby_dick_edition.author_contributors)
        [moby_dick_author] = moby_dick_edition.author_contributors
        assert True == isinstance(moby_dick_author, Contributor)
        assert "Herman Melville" == moby_dick_author.display_name
        assert "Melville, Herman" == moby_dick_author.sort_name

        assert 1 == len(moby_dick_author.contributions)
        [huckleberry_finn_author_contribution] = moby_dick_author.contributions
        assert True == isinstance(huckleberry_finn_author_contribution, Contribution)
        assert moby_dick_author == huckleberry_finn_author_contribution.contributor
        assert moby_dick_edition == huckleberry_finn_author_contribution.edition
        assert Contributor.AUTHOR_ROLE == huckleberry_finn_author_contribution.role

        assert data_source == moby_dick_edition.data_source

        assert "Test Publisher" == moby_dick_edition.publisher
        assert datetime.date(2015, 9, 29) == moby_dick_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url
        assert (
            "http://example.org/cover-small.jpg" == moby_dick_edition.cover_thumbnail_url)

        # 1.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_edition = self._get_edition_by_identifier(
            imported_editions, "urn:isbn:9781234567897"
        )
        assert True == isinstance(huckleberry_finn_edition, Edition)

        assert "Adventures of Huckleberry Finn" == huckleberry_finn_edition.title
        assert "eng" == huckleberry_finn_edition.language
        assert EditionConstants.BOOK_MEDIUM == huckleberry_finn_edition.medium
        assert "Samuel Langhorne Clemens, Mark Twain" == huckleberry_finn_edition.author

        assert 2 == len(huckleberry_finn_edition.author_contributors)
        huckleberry_finn_authors = huckleberry_finn_edition.author_contributors

        assert True == isinstance(huckleberry_finn_authors[0], Contributor)
        assert "Mark Twain" == huckleberry_finn_authors[0].display_name
        assert "Twain, Mark" == huckleberry_finn_authors[0].sort_name

        assert 1 == len(huckleberry_finn_authors[0].contributions)
        [huckleberry_finn_author_contribution] = huckleberry_finn_authors[
            0
        ].contributions
        assert True == isinstance(huckleberry_finn_author_contribution, Contribution)
        assert (
            huckleberry_finn_authors[0] ==
            huckleberry_finn_author_contribution.contributor)
        assert huckleberry_finn_edition == huckleberry_finn_author_contribution.edition
        assert Contributor.AUTHOR_ROLE == huckleberry_finn_author_contribution.role

        assert True == isinstance(huckleberry_finn_authors[1], Contributor)
        assert "Samuel Langhorne Clemens" == huckleberry_finn_authors[1].display_name
        assert "Clemens, Samuel Langhorne" == huckleberry_finn_authors[1].sort_name

        assert 1 == len(huckleberry_finn_authors[1].contributions)
        [huckleberry_finn_author_contribution] = huckleberry_finn_authors[
            1
        ].contributions
        assert True == isinstance(huckleberry_finn_author_contribution, Contribution)
        assert (
            huckleberry_finn_authors[1] ==
            huckleberry_finn_author_contribution.contributor)
        assert huckleberry_finn_edition == huckleberry_finn_author_contribution.edition
        assert Contributor.AUTHOR_ROLE == huckleberry_finn_author_contribution.role

        assert data_source == huckleberry_finn_edition.data_source

        assert "Test Publisher" == huckleberry_finn_edition.publisher
        assert datetime.date(2014, 9, 28) == huckleberry_finn_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url

        # 2. Make sure that license pools have correct configuration
        assert True == isinstance(pools, list)
        assert 2 == len(pools)

        # 2.1. Edition with open-access links (Moby-Dick)
        moby_dick_license_pool = self._get_license_pool_by_identifier(
            pools, "urn:isbn:978-3-16-148410-0"
        )
        assert True == isinstance(moby_dick_license_pool, LicensePool)
        assert True == moby_dick_license_pool.open_access
        assert LicensePool.UNLIMITED_ACCESS == moby_dick_license_pool.licenses_owned
        assert LicensePool.UNLIMITED_ACCESS == moby_dick_license_pool.licenses_available

        assert 1 == len(moby_dick_license_pool.delivery_mechanisms)
        [moby_dick_delivery_mechanism] = moby_dick_license_pool.delivery_mechanisms
        assert (
            DeliveryMechanism.NO_DRM ==
            moby_dick_delivery_mechanism.delivery_mechanism.drm_scheme)
        assert (
            MediaTypes.EPUB_MEDIA_TYPE ==
            moby_dick_delivery_mechanism.delivery_mechanism.content_type)

        # 2.2. Edition with non open-access acquisition links (Adventures of Huckleberry Finn)
        huckleberry_finn_license_pool = self._get_license_pool_by_identifier(
            pools, "urn:isbn:9781234567897"
        )
        assert True == isinstance(huckleberry_finn_license_pool, LicensePool)
        assert False == huckleberry_finn_license_pool.open_access
        assert LicensePool.UNLIMITED_ACCESS == huckleberry_finn_license_pool.licenses_owned
        assert (
            LicensePool.UNLIMITED_ACCESS ==
            huckleberry_finn_license_pool.licenses_available)

        assert 2 == len(huckleberry_finn_license_pool.delivery_mechanisms)
        huckleberry_finn_delivery_mechanisms = (
            huckleberry_finn_license_pool.delivery_mechanisms
        )

        assert (
            DeliveryMechanism.ADOBE_DRM ==
            huckleberry_finn_delivery_mechanisms[0].delivery_mechanism.drm_scheme)
        assert (
            MediaTypes.EPUB_MEDIA_TYPE ==
            huckleberry_finn_delivery_mechanisms[0].delivery_mechanism.content_type)

        assert (
            DeliveryMechanism.LCP_DRM ==
            huckleberry_finn_delivery_mechanisms[1].delivery_mechanism.drm_scheme)
        assert (
            MediaTypes.EPUB_MEDIA_TYPE ==
            huckleberry_finn_delivery_mechanisms[1].delivery_mechanism.content_type)

        # 3. Make sure that work objects contain all the required metadata
        assert True == isinstance(works, list)
        assert 2 == len(works)

        # 3.1. Edition with open-access links (Moby-Dick)
        moby_dick_work = self._get_work_by_identifier(
            works, "urn:isbn:978-3-16-148410-0"
        )
        assert True == isinstance(moby_dick_work, Work)
        assert moby_dick_edition == moby_dick_work.presentation_edition
        assert 1 == len(moby_dick_work.license_pools)
        assert moby_dick_license_pool == moby_dick_work.license_pools[0]

        # 3.2. Edition with open-access links (Moby-Dick)
        huckleberry_finn_work = self._get_work_by_identifier(
            works, "urn:isbn:9781234567897"
        )
        assert True == isinstance(huckleberry_finn_work, Work)
        assert huckleberry_finn_edition == huckleberry_finn_work.presentation_edition
        assert 1 == len(huckleberry_finn_work.license_pools)
        assert huckleberry_finn_license_pool == huckleberry_finn_work.license_pools[0]
        assert (
            "Adventures of Huckleberry Finn is a novel by Mark Twain, first published in the United Kingdom in "
            "December 1884 and in the United States in February 1885." ==
            huckleberry_finn_work.summary_text)
