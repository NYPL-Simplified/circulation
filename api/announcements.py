import datetime

from core.util.problem_detail import ProblemDetail

from admin.announcement_list_validator import AnnouncementListValidator

class Announcements(object):
    """Data model class for a library's announcements.

    This entire list is stored as a single
    ConfigurationSetting, which is why this isn't in core/model.
    """
    SETTING_NAME = "announcements"

    @classmethod
    def for_library(cls, library):
        """Load an Announcements object for the given Library.

        :param library: A Library
        """
        announcements = library.setting(cls.SETTING_NAME).json_value or []
        return cls(announcements)

    def __init__(self, announcements):
        """Instantiate an Announcements object from a (potentially serialised)
        list.

        :param announcements: A value for the ANNOUNCEMENTS ConfigurationSetting,
            either serialized or un-.
        :return: A list of Announcement objects. The list will be empty if 
            there are validation errors in `announcements`.
        """
        validator = AnnouncementListValidator()
        validated = validator.validate_announcements(announcements)
        if isinstance(validated, ProblemDetail):
            # There's a problem with the way the announcements were
            # serialized to the database. Treat this as an empty list.
            validated = []

        self.announcements = [Announcement(**data) for data in validated]

    @property
    def active(self):
        """Yield only the active announcements."""
        for a in self.announcements:
            if a.is_active:
                yield a


class Announcement(object):
    """Data model class for a single library-wide announcement."""
    def __init__(self, **kwargs):
        """Instantiate an Announcement from a dictionary of data.

        It's assumed that the data is present and valid.

        :param id: Globally unique ID for the Announcement.
        :param content: Textual content of the announcement.
        :param start: The date (relative to the time zone of the server)
            on which the announcement should start being published.
        :param finish: The date (relative to the time zone of the server)
            on which the announcement should stop being published.
        """
        self.id = kwargs.pop('id')
        self.content = kwargs.pop('content')
        self.start = AnnouncementListValidator.validate_date("", kwargs.pop('start'))
        self.finish = AnnouncementListValidator.validate_date("", kwargs.pop('finish'))

    @property
    def is_active(self):
        """Should this announcement be displayed now?"""
        today = datetime.date.today()
        return self.start <= today and self.finish >= today

    @property
    def for_authentication_document(self):
        """The publishable representation of this announcement,
        for use in an authentication document.

        Basically just the ID and the content.
        """
        return dict(id=self.id, content=self.content)
