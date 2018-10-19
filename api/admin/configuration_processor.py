from nose.tools import set_trace
from core.model import ConfigurationSetting
from flask import Response
from problem_details import *
from flask_babel import lazy_gettext as _

class ConfigurationProcessor(object):

    def __init__(self, config_options, request, _db):
        self.config_options = config_options
        self.request = request
        self.form = request.form
        self._db = _db

    def process_get(self):
        sitewide_settings = self.config_options.SITEWIDE_SETTINGS
        settings = []

        for s in sitewide_settings:
            setting = ConfigurationSetting.sitewide(self._db, s.get("key"))
            if setting.value:
                settings += [{ "key": setting.key, "value": setting.value }]

        return dict(
            settings=settings,
            all_settings=sitewide_settings,
        )

    def process_post(self):
        error = self.validate_form_fields(self.form.keys())
        if error:
            return error

        setting = ConfigurationSetting.sitewide(self._db, self.form.get("key"))
        setting.value = self.form.get("value")
        return Response(unicode(setting.key), 200)

    def process_delete(self, key):
        setting = ConfigurationSetting.sitewide(self._db, key)
        setting.value = None
        return Response(unicode(_("Deleted")), 200)

    def validate_form_fields(self, fields):

        MISSING_FIELD_MESSAGES = dict(
            key = MISSING_SITEWIDE_SETTING_KEY,
            value = MISSING_SITEWIDE_SETTING_VALUE
        )

        for field in fields:
            if not self.form.get(field):
                return MISSING_FIELD_MESSAGES.get(field)
