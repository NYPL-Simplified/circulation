#!/usr/bin/env python

from sqlalchemy.sql import *
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
    ConfigurationSetting._value != None
    ).filter(
        ConfigurationSetting._value != ""
    ).all()
def format(value):
    result = []
    try:
        value = json.loads(value)
        if type(value) is list:
            for x in value:
                result += format(x)
        elif type(value) is not dict:
            result += json.loads(value)
    except:
        result.append(value)
    return result
def fix(value):
    result = format(value)
    formatted_info = None
    if result:
        formatted_info = json.dumps({"US": result})
    return formatted_info
expect = json.dumps({"US": ["Waterford, CT"]})
assert fix("Waterford, CT") == expect
assert fix(json.dumps("Waterford, CT")) == expect
assert fix(json.dumps(["Waterford, CT"])) == expect
# If the value is already in the correct format, fix() shouldn't return anything;
# there's no need to update the setting.
assert fix(expect) == None
for setting in area_settings:
    library = _db.query(Library).filter(Library.id == setting.library_id).first()
    formatted_info = fix(setting._value)
    if formatted_info:
        print "Changing %r to %s" % (setting._value, formatted_info)
        ConfigurationSetting.for_library_and_externalintegration(_db, setting.key, library, None).value = formatted_info
    else:
        print "Leaving %s alone" % (setting._value)

_db.commit()
_db.close()
