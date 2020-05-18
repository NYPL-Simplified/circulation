import datetime
import json

from flask_babel import lazy_gettext as _

from api.admin.validator import Validator

from core.problem_details import *


class AnnouncementListValidator(Validator):

    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, max_announcements=3, min_length=15, max_length=350, default_duration_days=60):
        super(AnnouncementListValidator, self).__init__()
        self.max_announcements = max_announcements
        self.min_length = min_length
        self.max_length = max_length
        self.default_duration_days = default_duration_days

    def validate_announcement_list(self, announcements, db):
        validated_announcements = []
        if isinstance(announcements, basestring):
            announcements = json.loads(announcements)
        if len(announcements) > self.max_announcements:
            return INVALID_FORMAT.detailed(
                _("Too many announcements: maximum is: %(maximum)d",
                  maximum=self.max_announcements)
            )
            
        for announcement in announcements:
            validated = self.validate_announcement(announcement)
            if isinstance(validated, ProblemDetail):
                return validated
            validated_announcements.append(validated)

        if all(('order' in x) for x in validated_announcements):
            # The announcements should be stored in a specified order.
            validated_announcements = sorted(
                validated_announcements, key=lambda x: x['order']
            )
        else:
            # The announcements should be stored in the order they were received.
            for i, announcement in enumerate(validated_announcements):
                announcement['order'] = i
        return validated_announcements

    def validate_announcement(self, announcement):
        validated = dict()
        if not isinstance(announcement, dict):
            return INVALID_FORMAT.detailed(
                _("Invalid announcement description: %(announcement)r", announcement=announcement)
            )

        validated['id'] = announcement.get('id', unicode(uuid.uuid4()))

        for required_field in ('start', 'content'):
            if not required_field in announcement:
                return INVALID_INPUT.detailed(
                    _("Missing required field: %(field)s", field=required_field)
                )

        # Validate the content of the announcement.
        content = announcement['content']
        if len(content) < self.min_length:
            return INVALID_INPUT.detailed(
                _('"%(announcement)" is too short: minimum length is %(size)d characters.',
                  announcement=content, size=self.min_length)
            )

        if len(content) < self.max_announcement_length:
            return INVALID_INPUT.detailed(
                _('"%(announcement)" is too long: maximum length is %(size)d characters.',
                  announcement=content, size=self.max_length)
            )
        validated['content'] = content

        # Validate the dates associated with the announcement
        today = datetime.date.today()
        tomorrow = today + datetime.timedelta(days=1)

        start = self.validate_date(announcement.get('start', today), minimum=today)
        if isinstance(start, ProblemDetail):
            return start
        validated['start'] = start

        default_finish = start + datetime.timedelta(days=self.default_duration_days)
        finish = self.validate_date(announcement.get('finish', default_finish), minimum=tomorrow)
        if isinstance(finish, ProblemDetail):
            return finish
        validated['finish'] = finish

        # That's it!
        return validated

    def validate_date(self, value, minimum):
        if isinstance(value, basestring):
            try:
                datetime.strptime(value, self.DATE_FORMAT).date()
            except ValueError, e:
                return INVALID_INPUT(
                    _("Invalid date: %(date)s", date=value)
                )
        if value < minimum:
            return INVALID_INPUT(
                _("Invalid choice: %(date)s must be on or after %(minimum)s",
                  date=value.strftime(self.DATE_FORMAT)
                  minimum=minimum.strftime(self.DATE_FORMAT)
                )
            )
        return value
