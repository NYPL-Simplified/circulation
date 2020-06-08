from nose.tools import (
    eq_,
    set_trace,
)
import datetime
import json

from api.testing import AnnouncementTest
from api.announcements import (
    Announcements,
    Announcement
)

class TestAnnouncements(AnnouncementTest):
    """Test the Announcements object."""

    def test_for_library(self):
        """Verify that we can create an Announcements object for a library."""
        l = self._default_library

        # By default, a library has no announcements.
        announcements = Announcements.for_library(l)
        eq_([], announcements.announcements)

        # Give the library an announcement by setting its
        # "announcements" ConfigurationSetting.
        setting = l.setting(Announcements.SETTING_NAME)
        setting.value = json.dumps([self.active, self.expired])

        announcements = Announcements.for_library(l).announcements
        assert all(isinstance(a, Announcement) for a in announcements)

        active, expired = announcements
        eq_("active", active.id)
        eq_("expired", expired.id)

        # Put a bad value in the ConfigurationSetting, and it's
        # treated as an empty list. In real life this would only
        # happen due to a bug or a bad bit of manually entered SQL.
        invalid = dict(self.active)
        invalid['id'] = 'Another ID'
        invalid['finish'] = 'Not a date'
        setting.value = json.dumps([self.active, invalid, self.expired])
        eq_([], Announcements.for_library(l).announcements)

    def test_active(self):
        # The Announcements object keeps track of all announcements, but
        # Announcements.active only yields the active ones.
        announcements = Announcements([self.active, self.expired, self.forthcoming])
        eq_(3, len(announcements.announcements))
        eq_(["active"], [x.id for x in announcements.active])

    # Throw in a few minor tests of Announcement while we're here.

    def test_is_active(self):
        # Test the rules about when an Announcement is 'active'
        eq_(True, Announcement(**self.active).is_active)
        eq_(False, Announcement(**self.expired).is_active)
        eq_(False, Announcement(**self.forthcoming).is_active)

        # An announcement that ends today is still active.
        expires_today = dict(self.active)
        expires_today['finish'] = self.today
        eq_(True, Announcement(**self.active).is_active)

    def test_for_authentication_document(self):
        # Demonstrate the publishable form of an Announcement.
        #
        # 'start' and 'finish' will be ignored, as will the extra value
        # that has no meaning within Announcement.
        announcement = Announcement(extra="extra value", **self.active)
        eq_(dict(id="active", content="A sample announcement."),
            announcement.for_authentication_document
        )
