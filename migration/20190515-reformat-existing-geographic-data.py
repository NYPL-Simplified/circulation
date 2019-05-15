from nose.tools import set_trace
from sqlalchemy.sql import *
import json
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from api.admin.geographic_validator import GeographicValidator
from core.model import (
    Library,
    production_session
)
from core.model.configuration import ConfigurationSetting

_db = production_session()

area_settings = _db.query(ConfigurationSetting).filter(
    or_(ConfigurationSetting.key == "service_area", ConfigurationSetting.key == "focus_area")).filter(
    or_(ConfigurationSetting._value != None, ConfigurationSetting._value == "")
    ).all()

settings_with_wrong_format = []
for s in area_settings:
    try:
        if not type(json.loads(s._value)) is dict:
            settings_with_wrong_format.append(s)
    except:
        settings_with_wrong_format.append(s)

for setting in settings_with_wrong_format:
    formatted_info = GeographicValidator().validate_geographic_areas(setting._value, _db)
    library = _db.query(Library).filter(Library.id == setting.library_id).first()
    ConfigurationSetting.for_library_and_externalintegration(_db, setting.key, library, None).value = formatted_info

_db.commit()
_db.close()
