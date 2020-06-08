import datetime
import json
import uuid

from flask_babel import lazy_gettext as _

from api.admin.validator import Validator

from core.util.problem_detail import ProblemDetail
from core.problem_details import *


class AnnouncementListValidator(Validator):

    DATE_FORMAT = '%Y-%m-%d'

    def __init__(self, maximum_announcements=3, minimum_announcement_length=15,
                 maximum_announcement_length=350, default_duration_days=60):
        super(AnnouncementListValidator, self).__init__()
        self.maximum_announcements = maximum_announcements
        self.minimum_announcement_length = minimum_announcement_length
        self.maximum_announcement_length = maximum_announcement_length
        self.default_duration_days = default_duration_days

    def validate_announcements(self, announcements):
        validated_announcements = []
        bad_format = INVALID_INPUT.detailed(
            _("Invalid announcement list format: %(announcements)r",
              announcements=announcements)
        )
        if isinstance(announcements, basestring):
            try:
                announcements = json.loads(announcements)
            except ValueError:
                return bad_format
        if not isinstance(announcements, list):
            return bad_format
        if len(announcements) > self.maximum_announcements:
            return INVALID_INPUT.detailed(
                _("Too many announcements: maximum is %(maximum)d",
                  maximum=self.maximum_announcements)
            )
            
        seen_ids = set()
        for announcement in announcements:
            validated = self.validate_announcement(announcement)
            if isinstance(validated, ProblemDetail):
                return validated
            id = validated['id']
            if id in seen_ids:
                return INVALID_INPUT.detailed(_("Duplicate announcement ID: %s" % id))
            seen_ids.add(id)
            validated_announcements.append(validated)
        return validated_announcements

    def validate_announcement(self, announcement):
        validated = dict()
        if not isinstance(announcement, dict):
            return INVALID_INPUT.detailed(
                _("Invalid announcement format: %(announcement)r", announcement=announcement)
            )

        validated['id'] = announcement.get('id', unicode(uuid.uuid4()))

        for required_field in ('content',):
            if not required_field in announcement:
                return INVALID_INPUT.detailed(
                    _("Missing required field: %(field)s", field=required_field)
                )

        # Validate the content of the announcement.
        content = announcement['content']
        content = self.validate_length(
            content, self.minimum_announcement_length, self.maximum_announcement_length
        )
        if isinstance(content, ProblemDetail):
            return content
        validated['content'] = content

        # Validate the dates associated with the announcement
        today = datetime.date.today()

        start = self.validate_date(
            'start', announcement.get('start', today)
        )
        if isinstance(start, ProblemDetail):
            return start
        validated['start'] = start

        default_finish = start + datetime.timedelta(days=self.default_duration_days)
        day_after_start = start + datetime.timedelta(days=1)
        finish = self.validate_date(
            'finish',
            announcement.get('finish', default_finish),
            minimum=day_after_start,
        )
        if isinstance(finish, ProblemDetail):
            return finish
        validated['finish'] = finish

        # That's it!
        return validated

    @classmethod
    def validate_length(self, value, minimum, maximum):
        """Validate the length of a string value.

        :param value: Proposed value for a field.
        :param minimum: Minimum length.
        :param maximum: Maximum length.

        :return: A ProblemDetail if the validation fails; otherwise `value`.
        """
        if len(value) < minimum:
            return INVALID_INPUT.detailed(
                _('Value too short (%(length)d versus %(limit)d characters): %(value)s',
                  length=len(value), limit=minimum, value=value)
            )

        if len(value) > maximum:
            return INVALID_INPUT.detailed(
                _('Value too long (%(length)d versus %(limit)d characters): %(value)s',
                  length=len(value), limit=maximum, value=value)
            )
        return value

    @classmethod
    def validate_date(cls, field, value, minimum=None):
        """Validate a date value.

        :param field: Name of the field, used in error details.
        :param value: Proposed value for the field.
        :param minimum: The proposed value must not be earlier than
            this value.

        :return: A ProblemDetail if validation fails; otherwise a datetime.date.
        """
        if isinstance(value, basestring):
            try:
                value = datetime.datetime.strptime(value, cls.DATE_FORMAT)
            except ValueError, e:
                return INVALID_INPUT.detailed(
                    _("Value for %(field)s is not a date: %(date)s", field=field, date=value)
                )
        if isinstance(value, datetime.datetime):
            value = value.date()
        if isinstance(minimum, datetime.datetime):
            minimum = minimum.date()
        if minimum and value < minimum:
            return INVALID_INPUT.detailed(
                _("Value for %(field)s must be no earlier than %(minimum)s",
                  field=field, minimum=minimum.strftime(cls.DATE_FORMAT)
                )
            )
        return value

    def format(self, value):
        """Format the output of validate_announcements for storage in ConfigurationSetting.value"""
        from ..announcements import Announcements
        return json.dumps([x.json_ready for x in Announcements(value).announcements])

