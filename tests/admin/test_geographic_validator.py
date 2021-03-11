from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
from api.admin.controller.library_settings import LibrarySettingsController
from api.admin.geographic_validator import GeographicValidator
from api.admin.problem_details import *
from api.config import Configuration
from api.registry import RemoteRegistry
from core.model import (
    create,
    ExternalIntegration
)
from core.testing import MockRequestsResponse
import json
import pypostalcode
from tests.admin.controller.test_controller import SettingsControllerTest
import urllib
import uszipcode

class TestGeographicValidator(SettingsControllerTest):
    def test_validate_geographic_areas(self):
        original_validator = GeographicValidator
        db = self._db
        class Mock(GeographicValidator):
            def __init__(self):
                self._db = db
                self.value = None

            def mock_find_location_through_registry(self, value, db):
                self.value = value
            def mock_find_location_through_registry_with_error(self, value, db):
                self.value = value
                return REMOTE_INTEGRATION_FAILED
            def mock_find_location_through_registry_success(self, value, db):
                self.value = value
                return "CA"

        mock = Mock()
        mock.find_location_through_registry = mock.mock_find_location_through_registry

        # Invalid US zipcode
        response = mock.validate_geographic_areas('["00000"]', self._db)
        assert response.uri == UNKNOWN_LOCATION.uri
        assert response.detail == '"00000" is not a valid U.S. zipcode.'
        assert response.status_code == 400
        # The validator should have returned the problem detail without bothering to ask the registry.
        assert mock.value == None

        # Invalid Canadian zipcode
        response = mock.validate_geographic_areas('["X1Y"]', self._db)
        assert response.uri == UNKNOWN_LOCATION.uri
        assert response.detail == '"X1Y" is not a valid Canadian zipcode.'
        # The validator should have returned the problem detail without bothering to ask the registry.
        assert mock.value == None

        # Invalid 2-letter abbreviation
        response = mock.validate_geographic_areas('["ZZ"]', self._db)
        assert response.uri == UNKNOWN_LOCATION.uri
        assert response.detail == '"ZZ" is not a valid U.S. state or Canadian province abbreviation.'
        # The validator should have returned the problem detail without bothering to ask the registry.
        assert mock.value == None

        # Validator converts Canadian 2-letter abbreviations into province names, without needing to ask the registry.
        response = mock.validate_geographic_areas('["NL"]', self._db)
        assert response == {"CA": ["Newfoundland and Labrador"], "US": []}
        assert mock.value == None

        # County with wrong state
        response = mock.validate_geographic_areas('["Fairfield County, FL"]', self._db)
        assert response.uri == UNKNOWN_LOCATION.uri
        assert response.detail == 'Unable to locate "Fairfield County, FL".'
        # The validator should go ahead and call find_location_through_registry
        assert mock.value == "Fairfield County, FL"

        # City with wrong state
        response = mock.validate_geographic_areas('["Albany, NJ"]', self._db)
        assert response.uri == UNKNOWN_LOCATION.uri
        assert response.detail == 'Unable to locate "Albany, NJ".'
        # The validator should go ahead and call find_location_through_registry
        assert mock.value == "Albany, NJ"

        # The Canadian zip code is valid, but it corresponds to a place too small for the registry to know about it.
        response = mock.validate_geographic_areas('["J5J"]', self._db)
        assert response.uri == UNKNOWN_LOCATION.uri
        assert response.detail == 'Unable to locate "J5J" (Saint-Sophie, Quebec).  Try entering the name of a larger area.'
        assert mock.value == "Saint-Sophie, Quebec"

        # Can't connect to registry
        mock.find_location_through_registry = mock.mock_find_location_through_registry_with_error
        response = mock.validate_geographic_areas('["Victoria, BC"]', self._db)
        # The controller goes ahead and calls find_location_through_registry, but it can't connect to the registry.
        assert response.uri == REMOTE_INTEGRATION_FAILED.uri

        # The registry successfully finds the place
        mock.find_location_through_registry = mock.mock_find_location_through_registry_success
        response = mock.validate_geographic_areas('["Victoria, BC"]', self._db)
        assert response == {"CA": ["Victoria, BC"], "US": []}

    def test_format_as_string(self):
        # GeographicValidator.format_as_string just turns its output into JSON.
        value = {"CA": ["Victoria, BC"], "US": []}
        as_string = GeographicValidator().format_as_string(value)
        assert as_string == json.dumps(value)

    def test_find_location_through_registry(self):
        get = self.do_request
        test = self
        original_ask_registry = GeographicValidator().ask_registry

        class Mock(GeographicValidator):
            called_with = []
            def mock_ask_registry(self, service_area_object, db):
                places = {"US": ["Chicago"], "CA": ["Victoria, BC"]}
                service_area_info = json.loads(urllib.unquote(service_area_object))
                nation = service_area_info.keys()[0]
                city_or_county = service_area_info.values()[0]
                if city_or_county == "ERROR":
                    test.responses.append(MockRequestsResponse(502))
                elif city_or_county in places[nation]:
                    self.called_with.append(service_area_info)
                    test.responses.append(MockRequestsResponse(200, content=json.dumps(dict(unknown=None, ambiguous=None))))
                else:
                    self.called_with.append(service_area_info)
                    test.responses.append(MockRequestsResponse(200, content=json.dumps(dict(unknown=[city_or_county]))))
                return original_ask_registry(service_area_object, db, get)

        mock = Mock()
        mock.ask_registry = mock.mock_ask_registry

        self._registry("https://registry_url")

        us_response = mock.find_location_through_registry("Chicago", self._db)
        assert len(mock.called_with) == 1
        assert {"US": "Chicago"} == mock.called_with[0]
        assert us_response == "US"

        mock.called_with = []

        ca_response = mock.find_location_through_registry("Victoria, BC", self._db)
        assert len(mock.called_with) == 2
        assert {"US": "Victoria, BC"} == mock.called_with[0]
        assert {"CA": "Victoria, BC"} == mock.called_with[1]
        assert ca_response == "CA"

        mock.called_with = []

        nowhere_response = mock.find_location_through_registry("Not a real place", self._db)
        assert len(mock.called_with) == 2
        assert {"US": "Not a real place"} == mock.called_with[0]
        assert {"CA": "Not a real place"} == mock.called_with[1]
        assert nowhere_response == None

        error_response = mock.find_location_through_registry("ERROR", self._db)
        assert error_response.detail == "Unable to contact the registry at https://registry_url."
        assert error_response.status_code == 502

    def test_ask_registry(self):
        validator = GeographicValidator()

        registry_1 = self._registry("https://registry_1_url")
        registry_2 = self._registry("https://registry_2_url")
        registry_3 = self._registry("https://registry_3_url")

        true_response = MockRequestsResponse(200, content="{}")
        unknown_response = MockRequestsResponse(200, content='{"unknown": "place"}')
        ambiguous_response = MockRequestsResponse(200, content='{"ambiguous": "place"}')
        problem_response = MockRequestsResponse(404)

        # Registry 1 knows about the place
        self.responses.append(true_response)
        response_1 = validator.ask_registry(json.dumps({"CA": "Victoria, BC"}), self._db, self.do_request)
        assert response_1 == True
        assert len(self.requests) == 1
        request_1 = self.requests.pop()
        assert request_1[0] == 'https://registry_1_url/coverage?coverage={"CA": "Victoria, BC"}'

        # Registry 1 says the place is unknown, but Registry 2 finds it.
        self.responses.append(true_response)
        self.responses.append(unknown_response)
        response_2 = validator.ask_registry(json.dumps({"CA": "Victoria, BC"}), self._db, self.do_request)
        assert response_2 == True
        assert len(self.requests) == 2
        request_2 = self.requests.pop()
        assert request_2[0] == 'https://registry_2_url/coverage?coverage={"CA": "Victoria, BC"}'
        request_1 = self.requests.pop()
        assert request_1[0] == 'https://registry_1_url/coverage?coverage={"CA": "Victoria, BC"}'

        # Registry_1 says the place is ambiguous and Registry_2 says it's unknown, but Registry_3 finds it.
        self.responses.append(true_response)
        self.responses.append(unknown_response)
        self.responses.append(ambiguous_response)
        response_3 = validator.ask_registry(json.dumps({"CA": "Victoria, BC"}), self._db, self.do_request)
        assert response_3 == True
        assert len(self.requests) == 3
        request_3 = self.requests.pop()
        assert request_3[0] == 'https://registry_3_url/coverage?coverage={"CA": "Victoria, BC"}'
        request_2 = self.requests.pop()
        assert request_2[0] == 'https://registry_2_url/coverage?coverage={"CA": "Victoria, BC"}'
        request_1 = self.requests.pop()
        assert request_1[0] == 'https://registry_1_url/coverage?coverage={"CA": "Victoria, BC"}'

        # Registry 1 returns a problem detail, but Registry 2 finds the place
        self.responses.append(true_response)
        self.responses.append(problem_response)
        response_4 = validator.ask_registry(json.dumps({"CA": "Victoria, BC"}), self._db, self.do_request)
        assert response_4 == True
        assert len(self.requests) == 2
        request_2 = self.requests.pop()
        assert request_2[0] == 'https://registry_2_url/coverage?coverage={"CA": "Victoria, BC"}'
        request_1 = self.requests.pop()
        assert request_1[0] == 'https://registry_1_url/coverage?coverage={"CA": "Victoria, BC"}'

        # Registry 1 returns a problem detail and the other two registries can't find the place
        self.responses.append(unknown_response)
        self.responses.append(ambiguous_response)
        self.responses.append(problem_response)
        response_5 = validator.ask_registry(json.dumps({"CA": "Victoria, BC"}), self._db, self.do_request)
        assert response_5.status_code == 502
        assert response_5.detail == "Unable to contact the registry at https://registry_1_url."
        assert len(self.requests) == 3
        request_3 = self.requests.pop()
        assert request_3[0] == 'https://registry_3_url/coverage?coverage={"CA": "Victoria, BC"}'
        request_2 = self.requests.pop()
        assert request_2[0] == 'https://registry_2_url/coverage?coverage={"CA": "Victoria, BC"}'
        request_1 = self.requests.pop()
        assert request_1[0] == 'https://registry_1_url/coverage?coverage={"CA": "Victoria, BC"}'

    def _registry(self, url):
        integration, is_new = create(
            self._db, ExternalIntegration, protocol=ExternalIntegration.OPDS_REGISTRATION, goal=ExternalIntegration.DISCOVERY_GOAL
        )
        integration.url = url
        return RemoteRegistry(integration)

    def test_is_zip(self):
        validator = GeographicValidator()
        assert validator.is_zip("06759", "US") == True
        assert validator.is_zip("J2S", "US") == False
        assert validator.is_zip("1234", "US") == False
        assert validator.is_zip("1a234", "US") == False

        assert validator.is_zip("J2S", "CA") == True
        assert validator.is_zip("06759", "CA") == False
        assert validator.is_zip("12S", "CA") == False
        # "J2S 0A1" is a legit Canadian zipcode, but pypostalcode, which we use for looking up Canadian zipcodes,
        # only takes the FSA (the first three characters).
        assert validator.is_zip("J2S 0A1", "CA") == False

    def test_look_up_zip(self):
        validator = GeographicValidator()
        us_zip_unformatted = validator.look_up_zip("06759", "US")
        assert isinstance(us_zip_unformatted, uszipcode.SimpleZipcode)
        us_zip_formatted = validator.look_up_zip("06759", "US", True)
        assert us_zip_formatted == {'06759': u'Litchfield, CT'}

        ca_zip_unformatted = validator.look_up_zip("R2V", "CA")
        assert isinstance(ca_zip_unformatted, pypostalcode.PostalCode)
        ca_zip_formatted = validator.look_up_zip("R2V", "CA", True)
        assert ca_zip_formatted == {'R2V': u'Winnipeg (Seven Oaks East), Manitoba'}
