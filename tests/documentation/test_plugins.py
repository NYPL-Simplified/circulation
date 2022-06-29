import pytest
from werkzeug.http import dump_cookie

from api.app import app
from api.documentation.plugins import CSRFPlugin


class TestCSRFPlugin:
    @pytest.fixture
    def test_csrf_plugin(self):
        return CSRFPlugin()

    @pytest.fixture
    def csrf_parameter(self):
        return {
            'in': 'header',
            'name': 'X-CSRF-Token',
            'required': True,
            'schema': {
                'type': 'string'
            }
        }

    @pytest.fixture
    def test_header(self):
        return dump_cookie('csrf_token', 'test_csrf_value')

    def test_parameter_helper_csrf_token(
        self, test_csrf_plugin, csrf_parameter, test_header):
        with app.test_request_context(environ_base={'HTTP_COOKIE': test_header}):
            updated_param = test_csrf_plugin.parameter_helper(csrf_parameter)

            assert updated_param['schema']['default'] == 'test_csrf_value'

    def test_parameter_helper_other_token(self,
        test_csrf_plugin, csrf_parameter, test_header):
        other_parameter = csrf_parameter
        other_parameter['name'] == 'X-Other-Token'

        with app.test_request_context(environ_base={'HTTP_COOKIE': test_header}):
            updated_param = test_csrf_plugin.parameter_helper(other_parameter)

            assert updated_param == other_parameter