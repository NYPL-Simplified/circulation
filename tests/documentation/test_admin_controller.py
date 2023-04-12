from apispec import APISpec
import pytest

from api.app import app
from api.documentation.admin_controller import AdminAPIController


class TestDocumentationController:
    @pytest.fixture
    def test_open_api_spec(self):
        return APISpec('Test Spec', '0.0.1-alpha', '3.1.0')

    @pytest.fixture
    def test_controller(self, test_open_api_spec):
        class TestController(AdminAPIController):
            def __init__(self):
                self.spec = test_open_api_spec

        return TestController()

    def test_generateSpec(self):
        with app.test_request_context():
            testSpec = AdminAPIController.generateSpec()

            # Assert presence of basic version numbers and title
            assert testSpec['info']['version']\
                == AdminAPIController.DOC_VERSION
            assert testSpec['openapi'] == AdminAPIController.OPENAPI_VERSION
            assert testSpec['info']['title'] == 'Library Simplified Circulation Manager'

            # Assert presence of described paths
            assert testSpec['paths']['/{library_short_name}/admin/custom_lists']\
                ['get']['responses']['200']['content']['application/json']\
                    ['schema']['$ref']\
                        == '#/components/schemas/CustomListResponse'

            # Assert presence of paths without docstrings
            assert testSpec['paths']['/admin/static/circulation-web.css'] == {}

            # Assert that localhost is the default server
            assert testSpec['servers'][0]['url'] == 'http://localhost'

            # Assert that parameters are present with most frequent one
            assert testSpec['components']['parameters']['X-CSRF-Token']['name']\
                == 'X-CSRF-Token'

            # Assert that schemas are present with test value
            assert testSpec['components']['schemas']['OPDSFeedResponse']\
                ['properties']['link']['$ref']\
                    == '#/components/schemas/OPDSLink'

    def test_addComponent_string(self, test_controller):
        test_controller.addComponent(
            'schema', 'TestString', 'string', {'enum': ['test1', 'test2']}
        )

        assert test_controller.spec.to_dict()['components']['schemas']['TestString']\
            == {'type': 'string', 'enum': ['test1', 'test2']}

    def test_addComponent_object(self, test_controller):
        test_controller.addComponent(
            'schema', 'TestObject', 'object', {
                'test1': {'type': 'string', 'required': True},
                'test2': {'type': 'integer'}
            }
        )

        assert test_controller.spec.to_dict()['components']['schemas']['TestObject']\
            == {
                'type': 'object',
                'properties': {
                    'test1': {'type': 'string', 'required': True},
                    'test2': {'type': 'integer'}
                }
            }
