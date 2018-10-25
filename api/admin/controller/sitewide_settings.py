from nose.tools import set_trace
from core.model import ConfigurationSetting
from . import AdminCirculationManagerController
from api.config import Configuration
from flask import Response
from api.admin.problem_details import *
import flask
from flask_babel import lazy_gettext as _

class SitewideConfigurationSettingsController(AdminCirculationManagerController):

    def process_settings(self):
         self.require_system_admin()
         if flask.request.method == 'GET':
            return self.process_get()
         elif flask.request.method == 'POST':
            return self.process_post()

    def process_get(self):
        sitewide_settings = Configuration.SITEWIDE_SETTINGS
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
        error = self.validate_form_fields(flask.request.form.keys())
        if error:
            return error

        setting = ConfigurationSetting.sitewide(self._db, flask.request.form.get("key"))
        setting.value = flask.request.form.get("value")
        return Response(unicode(setting.key), 200)

    def process_delete(self, key):
        self.require_system_admin()
        setting = ConfigurationSetting.sitewide(self._db, key)
        setting.value = None
        return Response(unicode(_("Deleted")), 200)

    def validate_form_fields(self, fields):

        MISSING_FIELD_MESSAGES = dict(
            key = MISSING_SITEWIDE_SETTING_KEY,
            value = MISSING_SITEWIDE_SETTING_VALUE
        )

        for field in fields:
            if not flask.request.form.get(field):
                return MISSING_FIELD_MESSAGES.get(field)
