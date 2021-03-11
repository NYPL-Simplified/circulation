from nose.tools import (
    eq_,
    set_trace,
)
import datetime
import json

from api.testing import AnnouncementTest
from api.admin.announcement_list_validator import AnnouncementListValidator
from api.announcements import (
    Announcements,
    Announcement
)
from core.testing import DatabaseTest

class TestAnnouncements(AnnouncementTest, DatabaseTest):
    """Test the Announcements object."""

    def test_for_library(self):
        """Verify that we can create an Announcements object for a library."""
        l = self._default_library

        # By default, a library has no announcements.
        announcements = Announcements.for_library(l)
        assert [] == announcements.announcements

        # Give the library an announcement by setting its
        # "announcements" ConfigurationSetting.
        setting = l.setting(Announcements.SETTING_NAME)
        setting.value = json.dumps([self.active, self.expired])

        announcements = Announcements.for_library(l).announcements
        assert all(isinstance(a, Announcement) for a in announcements)

        active, expired = announcements
        assert "active" == active.id
        assert "expired" == expired.id

        # Put a bad value in the ConfigurationSetting, and it's
        # treated as an empty list. In real life this would only
        # happen due to a bug or a bad bit of manually entered SQL.
        invalid = dict(self.active)
        invalid['id'] = 'Another ID'
        invalid['finish'] = 'Not a date'
        setting.value = json.dumps([self.active, invalid, self.expired])
        assert [] == Announcements.for_library(l).announcements

    def test_active(self):
        # The Announcements object keeps track of all announcements, but
        # Announcements.active only yields the active ones.
        announcements = Announcements([self.active, self.expired, self.forthcoming])
        assert 3 == len(announcements.announcements)
        assert ["active"] == [x.id for x in announcements.active]

    # Throw in a few minor tests of Announcement while we're here.

    def test_is_active(self):
        # Test the rules about when an Announcement is 'active'
        assert True == Announcement(**self.active).is_active
        assert False == Announcement(**self.expired).is_active
        assert False == Announcement(**self.forthcoming).is_active

        # An announcement that ends today is still active.
        expires_today = dict(self.active)
        expires_today['finish'] = self.today
        assert True == Announcement(**self.active).is_active

    def test_for_authentication_document(self):
        # Demonstrate the publishable form of an Announcement.
        #
        # 'start' and 'finish' will be ignored, as will the extra value
        # that has no meaning within Announcement.
        announcement = Announcement(extra="extra value", **self.active)
        assert (dict(id="active", content="A sample announcement.") ==
            announcement.for_authentication_document)

    def test_json_ready(self):
        # Demonstrate the form of an Announcement used to store in the database.
        #
        # 'start' and 'finish' will be converted into strings the extra value
        # that has no meaning within Announcement will be ignored.
        announcement = Announcement(extra="extra value", **self.active)
        assert (
            dict(
                id="active",
                content="A sample announcement.",
                start=announcement.start.strftime(AnnouncementListValidator.DATE_FORMAT),
                finish=announcement.finish.strftime(AnnouncementListValidator.DATE_FORMAT),
            ) ==
            announcement.json_ready)
