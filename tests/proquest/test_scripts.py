import json
import sys

from mock import MagicMock, create_autospec, patch
from nose.tools import eq_

from api.proquest.client import ProQuestAPIClient, ProQuestAPIClientFactory
from api.proquest.importer import ProQuestOPDS2Importer, ProQuestOPDS2ImportMonitor
from api.proquest.scripts import ProQuestOPDS2ImportScript
from core.model import Collection, DataSource, ExternalIntegration, Identifier
from core.testing import DatabaseTest
from tests.proquest import fixtures


class TestProQuestOPDS2ImportScript(DatabaseTest):
    @staticmethod
    def _get_licensepool_by_identifier(license_pools, identifier):
        """Find and return a LicensePool object with the specified identifier.

        :param license_pools: List of LicensePool objects
        :type license_pools: List[core.model.licensing.LicensePool]

        :param identifier: Identifier to look for
        :type identifier: str

        :return: LicensePool object with the specified identifier, None otherwise
        :rtype: Optional[core.model.licensing.LicensePool]
        """
        for license_pool in license_pools:
            if license_pool.identifier.identifier == identifier:
                return license_pool

        return None

    def setup_method(self, mock_search=True):
        super(TestProQuestOPDS2ImportScript, self).setup_method()

        self._proquest_data_source = DataSource.lookup(
            self._db, DataSource.PROQUEST, autocreate=True
        )
        self._proquest_collection = self._collection(
            protocol=ExternalIntegration.PROQUEST
        )

        self._proquest_collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, DataSource.PROQUEST
        )

    def test_import_monitor_handles_proquest_feed_removals_correctly(self):
        """Make sure that the ProQuest import script handles removals correctly.

        If we run proquest_import_monitor with --process-removals flag,
        it detects the items that are no longer present in the ProQuest feed
        and makes them invisible in the CM's catalog.

        This test run proquest_import_monitor twice:
        - First time proquest_import_monitor gets the ProQuest feed containing two publications:
            Test Book 1
            Test Book 2.
        - Second time we run proquest_import_monitor the ProQuest feed contains another set of publications:
            Test Book 1
            Test Book 3.
          This means that Test Book 2 is no longer available in the feed and must be hidden in the CM's catalog.
        """
        api_client_mock = create_autospec(spec=ProQuestAPIClient)

        api_client_factory_mock = create_autospec(spec=ProQuestAPIClientFactory)
        api_client_factory_mock.create = MagicMock(return_value=api_client_mock)

        # 1. First, the monitor gets PROQUEST_RAW_FEED containing two publications:
        # - Test Book 1
        # - Test Book 2
        api_client_mock.download_all_feed_pages = MagicMock(
            return_value=[
                json.loads(fixtures.PROQUEST_RAW_FEED),
            ]
        )
        # We run the importer without --process-removals flag because it's not necessary for the first run.
        test_arguments = ["proquest_import_monitor"]

        with patch.object(sys, "argv", test_arguments), patch(
            "api.proquest.scripts.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock

            import_script = ProQuestOPDS2ImportScript(
                _db=self._db,
                importer_class=ProQuestOPDS2Importer,
                monitor_class=ProQuestOPDS2ImportMonitor,
                protocol=ExternalIntegration.PROQUEST,
            )

            import_script.run()

            # We want to make sure that the collection contains both publications.
            assert 2 == len(self._proquest_collection.licensepools)

            test_book_1_license_pool = self._get_licensepool_by_identifier(
                self._proquest_collection.licensepools,
                fixtures.PROQUEST_RAW_PUBLICATION_1_ID,
            )
            assert Identifier.PROQUEST_ID == test_book_1_license_pool.identifier.type
            assert (
                fixtures.PROQUEST_RAW_PUBLICATION_1_ID ==
                test_book_1_license_pool.identifier.identifier)
            assert True == test_book_1_license_pool.unlimited_access

            test_book_2_license_pool = self._get_licensepool_by_identifier(
                self._proquest_collection.licensepools,
                fixtures.PROQUEST_RAW_PUBLICATION_2_ID,
            )
            assert Identifier.PROQUEST_ID == test_book_2_license_pool.identifier.type
            assert (
                fixtures.PROQUEST_RAW_PUBLICATION_2_ID ==
                test_book_2_license_pool.identifier.identifier)
            assert True == test_book_2_license_pool.unlimited_access

        # 2. When we run the monitor for the second time it gets another feed containing:
        # - Test Book 1
        # - Test Book 3
        # Note: no Test Book 2
        api_client_mock.download_all_feed_pages = MagicMock(
            return_value=[
                json.loads(fixtures.PROQUEST_RAW_FEED_WITH_A_REMOVED_PUBLICATION),
            ]
        )
        # We explicitly use --process-removals flag to make the importer to handle removals.
        test_arguments = ["proquest_import_monitor", "--process-removals"]

        with patch.object(sys, "argv", test_arguments), patch(
            "api.proquest.scripts.ProQuestAPIClientFactory"
        ) as api_client_factory_constructor_mock:
            api_client_factory_constructor_mock.return_value = api_client_factory_mock

            import_script = ProQuestOPDS2ImportScript(
                _db=self._db,
                importer_class=ProQuestOPDS2Importer,
                monitor_class=ProQuestOPDS2ImportMonitor,
                protocol=ExternalIntegration.PROQUEST,
            )

            import_script.run()

            # The collection contains 3 items but only two of them are visible.
            assert 3 == len(self._proquest_collection.licensepools)
            test_book_1_license_pool = self._get_licensepool_by_identifier(
                self._proquest_collection.licensepools,
                fixtures.PROQUEST_RAW_PUBLICATION_1_ID,
            )
            assert Identifier.PROQUEST_ID == test_book_1_license_pool.identifier.type
            assert (
                fixtures.PROQUEST_RAW_PUBLICATION_1_ID ==
                test_book_1_license_pool.identifier.identifier)
            assert True == test_book_1_license_pool.unlimited_access

            test_book_2_license_pool = self._get_licensepool_by_identifier(
                self._proquest_collection.licensepools,
                fixtures.PROQUEST_RAW_PUBLICATION_2_ID,
            )
            assert Identifier.PROQUEST_ID == test_book_2_license_pool.identifier.type
            assert (
                fixtures.PROQUEST_RAW_PUBLICATION_2_ID ==
                test_book_2_license_pool.identifier.identifier)
            # We want to make sure that Test Book 2 is no longer visible in the CM's catalog
            # because it doesn't have any licenses.
            assert False == test_book_2_license_pool.unlimited_access
            assert 0 == test_book_2_license_pool.licenses_owned
            assert 0 == test_book_2_license_pool.licenses_available

            test_book_3_license_pool = self._get_licensepool_by_identifier(
                self._proquest_collection.licensepools,
                fixtures.PROQUEST_RAW_PUBLICATION_3_ID,
            )
            assert Identifier.PROQUEST_ID == test_book_3_license_pool.identifier.type
            assert (
                fixtures.PROQUEST_RAW_PUBLICATION_3_ID ==
                test_book_3_license_pool.identifier.identifier)
            assert True == test_book_3_license_pool.unlimited_access
