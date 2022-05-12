import pytest

from api.admin.exceptions import *
from api.config import Configuration
from core.opds import AcquisitionFeed
from core.model import (
    AdminRole,
    ConfigurationSetting
)
from test_controller import SettingsControllerTest
from werkzeug.datastructures import ImmutableMultiDict, MultiDict
import flask

class TestSitewideSettings(SettingsControllerTest):

    def test_sitewide_settings_get(self):
        with self.request_context_with_admin("/"):
            response = self.manager.admin_sitewide_configuration_settings_controller.process_get()
            settings = response.get("settings")
            all_settings = response.get("all_settings")

            assert [] == settings
            keys = [s.get("key") for s in all_settings]
            assert Configuration.LOG_LEVEL in keys
            assert Configuration.DATABASE_LOG_LEVEL in keys
            assert Configuration.SECRET_KEY in keys

        ConfigurationSetting.sitewide(self._db, Configuration.DATABASE_LOG_LEVEL).value = 'INFO'
        ConfigurationSetting.sitewide(self._db, Configuration.SECRET_KEY).value = "secret"
        self._db.flush()

        with self.request_context_with_admin("/"):
            response = self.manager.admin_sitewide_configuration_settings_controller.process_get()
            settings = response.get("settings")
            all_settings = response.get("all_settings")

            assert 2 == len(settings)
            settings_by_key = { s.get("key") : s.get("value") for s in settings }
            assert "INFO" == settings_by_key.get(Configuration.DATABASE_LOG_LEVEL)
            assert "secret" == settings_by_key.get(Configuration.SECRET_KEY)
            keys = [s.get("key") for s in all_settings]
            assert Configuration.LOG_LEVEL in keys
            assert Configuration.DATABASE_LOG_LEVEL in keys
            assert Configuration.SECRET_KEY in keys

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_sitewide_configuration_settings_controller.process_get)

    def test_sitewide_settings_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([("key", None)])
            response = self.manager.admin_sitewide_configuration_settings_controller.process_post()
            assert response == MISSING_SITEWIDE_SETTING_KEY

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", Configuration.SECRET_KEY),
                ("value", None)
            ])
            response = self.manager.admin_sitewide_configuration_settings_controller.process_post()
            assert response == MISSING_SITEWIDE_SETTING_VALUE

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", Configuration.SECRET_KEY),
                ("value", "secret"),
            ])
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_sitewide_configuration_settings_controller.process_post)

    def test_sitewide_settings_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", Configuration.DATABASE_LOG_LEVEL),
                ("value", "10"),
            ])
            response = self.manager.admin_sitewide_configuration_settings_controller.process_post()
            assert response.status_code == 200

        # The setting was created.
        setting = ConfigurationSetting.sitewide(self._db, Configuration.DATABASE_LOG_LEVEL)
        assert setting.key == response.response[0]
        assert "10" == setting.value

    def test_sitewide_settings_post_edit(self):
        setting = ConfigurationSetting.sitewide(self._db, Configuration.DATABASE_LOG_LEVEL)
        setting.value = "WARN"

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("key", Configuration.DATABASE_LOG_LEVEL),
                ("value", "ERROR"),
            ])
            response = self.manager.admin_sitewide_configuration_settings_controller.process_post()
            assert response.status_code == 200

        # The setting was changed.
        assert setting.key == response.response[0]
        assert "ERROR" == setting.value

    def test_sitewide_setting_delete(self):
        setting = ConfigurationSetting.sitewide(self._db, Configuration.DATABASE_LOG_LEVEL)
        setting.value = "WARN"

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_sitewide_configuration_settings_controller.process_delete,
                          setting.key)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_sitewide_configuration_settings_controller.process_delete(setting.key)
            assert response.status_code == 200

        assert None == setting.value
