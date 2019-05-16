from nose.tools import set_trace
from sqlalchemy.sql import *
import ast
import json
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
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

def convert_to_list(string):
    # If the string is in the format '["Waterford, CT"]', load it as a list
    # so that each item in it can be formatted
    try:
        return json.loads(ast.literal_eval(string))
    except:
        return None

def format(value):
    result = []
    try:
        value = convert_to_list(value) or json.loads(value)
        if type(value) is list:
            for x in value:
                result += format(x)
        elif type(value) is not dict:
            result += json.loads(value)
    except:
        result.append(value)

    return result

for setting in area_settings:
    library = _db.query(Library).filter(Library.id == setting.library_id).first()
    result = format(setting._value)
    formatted_info = None
    if result:
        formatted_info = json.dumps({"US": result})

    if formatted_info:
        ConfigurationSetting.for_library_and_externalintegration(_db, setting.key, library, None).value = formatted_info

_db.commit()
_db.close()
