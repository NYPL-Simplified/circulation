from api.problem_details import *
from api.admin.exceptions import *
from api.admin.validator import Validator
from api.registry import RemoteRegistry
from core.model import (
    ExternalIntegration,
    Representation
)
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail
from flask_babel import lazy_gettext as _
import json
from nose.tools import set_trace
from pypostalcode import PostalCodeDatabase
import re
import urllib.request, urllib.parse, urllib.error
import uszipcode
import os

class GeographicValidator(Validator):

    @staticmethod
    def get_us_search():
        # Use a known path for the uszipcode db_file_dir that already contains the DB that the
        # library would otherwise download. This is done because the host for this file can
        # be flaky. There is an issue for this in the underlying library here:
        # https://github.com/MacHu-GWU/uszipcode-project/issues/40
        db_file_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "uszipcode")
        return uszipcode.SearchEngine(simple_zipcode=True, db_file_dir=db_file_path)

    def validate_geographic_areas(self, values, db):
        # Note: the validator does not recognize data from US territories other than Puerto Rico.

        us_search = self.get_us_search()
        ca_search = PostalCodeDatabase()
        CA_PROVINCES = {
            "AB": "Alberta",
            "BC": "British Columbia",
            "MB": "Manitoba",
            "NB": "New Brunswick",
            "NL": "Newfoundland and Labrador",
            "NT": "Northwest Territories",
            "NS": "Nova Scotia",
            "NU": "Nunavut",
            "ON": "Ontario",
            "PE": "Prince Edward Island",
            "QC": "Quebec",
            "SK": "Saskatchewan",
            "YT": "Yukon Territories"
        }

        locations = {"US": [], "CA": []}

        for value in json.loads(values):
            flagged = False
            if value == "everywhere":
                locations["US"].append(value)
            elif len(value) and isinstance(value, str):
                if len(value) == 2:
                    # Is it a US state or Canadian province abbreviation?
                    if value in CA_PROVINCES:
                        locations["CA"].append(CA_PROVINCES[value])
                    elif len(us_search.query(state=value)):
                        locations["US"].append(value)
                    else:
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid U.S. state or Canadian province abbreviation.', value=value))
                elif value in list(CA_PROVINCES.values()):
                    locations["CA"].append(value)
                elif self.is_zip(value, "CA"):
                    # Is it a Canadian zipcode?
                    try:
                        info = self.look_up_zip(value, "CA")
                        formatted = "%s, %s" % (info.city, info.province)
                        # In some cases--mainly involving very small towns--even if the zip code is valid,
                        # the registry won't recognize the name of the place to which it corresponds.
                        registry_response = self.find_location_through_registry(formatted, db)
                        if registry_response:
                            locations["CA"].append(formatted);
                        else:
                            return UNKNOWN_LOCATION.detailed(_('Unable to locate "%(value)s" (%(formatted)s).  Try entering the name of a larger area.', value=value, formatted=formatted))
                    except:
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid Canadian zipcode.', value=value))
                elif len(value.split(", ")) == 2:
                    # Is it in the format "[city], [state abbreviation]" or "[county], [state abbreviation]"?
                    city_or_county, state = value.split(", ")
                    if us_search.by_city_and_state(city_or_county, state):
                        locations["US"].append(value);
                    elif len([x for x in us_search.query(state=state, returns=None) if x.county == city_or_county]):
                        locations["US"].append(value);
                    else:
                        # Flag this as needing to be checked with the registry
                        flagged = True
                elif self.is_zip(value, "US"):
                    # Is it a US zipcode?
                    info = self.look_up_zip(value, "US")
                    if not info:
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid U.S. zipcode.', value=value))
                    locations["US"].append(value);
                else:
                    flagged = True

                if flagged:
                    registry_response = self.find_location_through_registry(value, db)
                    if registry_response and isinstance(registry_response, ProblemDetail):
                        return registry_response
                    elif registry_response:
                        locations[registry_response].append(value)
                    else:
                        return UNKNOWN_LOCATION.detailed(_('Unable to locate "%(value)s".', value=value))
        return locations

    def is_zip(self, value, country):
        if country == "US":
            return len(value) == 5 and value.isdigit()
        elif country == "CA":
            return len(value) == 3 and bool(re.search("^[A-Za-z]\\d[A-Za-z]", value))

    def look_up_zip(self, zip, country, formatted=False):
        if country == "US":
            info = self.get_us_search().by_zipcode(zip)
            if formatted:
                info = self.format_place(zip, info.major_city, info.state)
        elif country == "CA":
            info = PostalCodeDatabase()[zip]
            if formatted:
                info = self.format_place(zip, info.city, info.province)
        return info

    def format_place(self, zip, city, state_or_province):
        details = "%s, %s" % (city, state_or_province)
        return { zip: details }

    def find_location_through_registry(self, value, db):
        for nation in ["US", "CA"]:
            service_area_object = urllib.parse.quote('{"%s": "%s"}' % (nation, value))
            registry_check = self.ask_registry(service_area_object, db)
            if registry_check and isinstance(registry_check, ProblemDetail):
                return registry_check
            elif registry_check:
                # If the registry has established that this is a US location, don't bother also trying to find it in Canada
                return nation

    def ask_registry(self, service_area_object, db, do_get=HTTP.debuggable_get):
        # If the circulation manager doesn't know about this location, check whether the Library Registry does.
        result = None
        for registry in RemoteRegistry.for_protocol_and_goal(
            db, ExternalIntegration.OPDS_REGISTRATION, ExternalIntegration.DISCOVERY_GOAL
        ):
            base_url = registry.integration.url + "/coverage?coverage="

            response = do_get(base_url + service_area_object)
            if not response.status_code == 200:
                result = REMOTE_INTEGRATION_FAILED.detailed(_("Unable to contact the registry at %(url)s.", url=registry.integration.url))

            if hasattr(response, "content"):
                content = json.loads(response.content)
                found_place = not (content.get("unknown") or content.get("ambiguous"))
                if found_place:
                    return True

        return result

    def format_as_string(self, value):
        """Format the output of validate_geographic_areas for storage in ConfigurationSetting.value."""
        return json.dumps(value)
