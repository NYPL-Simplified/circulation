import json

from flask import request
from mock import MagicMock, create_autospec, patch, call

from api.controller import CirculationManager
from api.lcp.collection import LCPAPI
from api.lcp.controller import LCPController
from api.lcp.factory import LCPServerFactory
from api.lcp.server import LCPServer
from core.lcp.credential import LCPCredentialFactory
from core.model import ExternalIntegration
from tests.lcp import fixtures
from tests.test_controller import ControllerTest


class TestLCPController(ControllerTest):
    def test_get_lcp_passphrase_returns_the_same_passphrase_for_authenticated_patron(self):
        # Arrange
        expected_passphrase = '1cde00b4-bea9-48fc-819b-bd17c578a22c'

        with patch('api.lcp.controller.LCPCredentialFactory') as credential_factory_constructor_mock:
            credential_factory = create_autospec(spec=LCPCredentialFactory)
            credential_factory.get_patron_passphrase = MagicMock(return_value=expected_passphrase)
            credential_factory_constructor_mock.return_value = credential_factory

            patron = self.default_patron
            manager = CirculationManager(self._db, testing=True)
            controller = LCPController(manager)
            controller.authenticated_patron_from_request = MagicMock(return_value=patron)

            url = 'http://circulationmanager.org/lcp/hint'

            with self.app.test_request_context(url):
                request.library = self._default_library

                # Act
                result1 = controller.get_lcp_passphrase()
                result2 = controller.get_lcp_passphrase()

                # Assert
                for result in [result1, result2]:
                    assert result.status_code == 200
                    assert ('passphrase' in result.json) == True
                    assert result.json['passphrase'] == expected_passphrase

                credential_factory.get_patron_passphrase.assert_has_calls(
                    [
                        call(self._db, patron),
                        call(self._db, patron)
                    ]
                )

    def test_get_lcp_license_returns_problem_detail_when_collection_is_missing(self):
        # Arrange
        missing_collection_name = 'missing-collection'
        license_id = 'e99be177-4902-426a-9b96-0872ae877e2f'
        expected_license = json.loads(fixtures.LCPSERVER_LICENSE)
        lcp_server = create_autospec(spec=LCPServer)
        lcp_server.get_license = MagicMock(return_value=expected_license)
        library = self.make_default_library(self._db)
        lcp_collection = self._collection(LCPAPI.NAME, ExternalIntegration.LCP)
        library.collections.append(lcp_collection)

        with patch('api.lcp.controller.LCPServerFactory') as lcp_server_factory_constructor_mock:
            lcp_server_factory = create_autospec(spec=LCPServerFactory)
            lcp_server_factory.create = MagicMock(return_value=lcp_server)
            lcp_server_factory_constructor_mock.return_value = lcp_server_factory

            patron = self.default_patron
            manager = CirculationManager(self._db, testing=True)
            controller = LCPController(manager)
            controller.authenticated_patron_from_request = MagicMock(return_value=patron)

            url = 'http://circulationmanager.org/{0}/licenses{1}'.format(
                missing_collection_name, license_id)

            with self.app.test_request_context(url):
                request.library = self._default_library

                # Act
                result = controller.get_lcp_license(missing_collection_name, license_id)

                # Assert
                assert result.status_code == 404

    def test_get_lcp_license_returns_the_same_license_for_authenticated_patron(self):
        # Arrange
        license_id = 'e99be177-4902-426a-9b96-0872ae877e2f'
        expected_license = json.loads(fixtures.LCPSERVER_LICENSE)
        lcp_server = create_autospec(spec=LCPServer)
        lcp_server.get_license = MagicMock(return_value=expected_license)
        library = self.make_default_library(self._db)
        lcp_collection = self._collection(LCPAPI.NAME, ExternalIntegration.LCP)
        library.collections.append(lcp_collection)

        with patch('api.lcp.controller.LCPServerFactory') as lcp_server_factory_constructor_mock:
            lcp_server_factory = create_autospec(spec=LCPServerFactory)
            lcp_server_factory.create = MagicMock(return_value=lcp_server)
            lcp_server_factory_constructor_mock.return_value = lcp_server_factory

            patron = self.default_patron
            manager = CirculationManager(self._db, testing=True)
            controller = LCPController(manager)
            controller.authenticated_patron_from_request = MagicMock(return_value=patron)

            url = 'http://circulationmanager.org/{0}/licenses{1}'.format(
                LCPAPI.NAME, license_id)

            with self.app.test_request_context(url):
                request.library = self._default_library

                # Act
                result1 = controller.get_lcp_license(LCPAPI.NAME, license_id)
                result2 = controller.get_lcp_license(LCPAPI.NAME, license_id)

                # Assert
                for result in [result1, result2]:
                    assert result.status_code == 200
                    assert result.json == expected_license
