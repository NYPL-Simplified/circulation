from nose.tools import (
    eq_,
    set_trace,
)

from datetime import (
    date,
    datetime,
    timedelta,
)
import json

from core.problem_details import INVALID_INPUT
from core.util.problem_detail import ProblemDetail
from api.announcements import Announcement
from api.admin.announcement_list_validator import AnnouncementListValidator
from api.testing import AnnouncementTest

class TestAnnouncementListValidator(AnnouncementTest):

    def assert_invalid(self, x, detail):
        assert isinstance(x, ProblemDetail)
        eq_(INVALID_INPUT.uri, x.uri)
        eq_(detail, x.detail)

    def test_defaults(self):
        validator = AnnouncementListValidator()
        eq_(3, validator.maximum_announcements)
        eq_(15, validator.minimum_announcement_length)
        eq_(350, validator.maximum_announcement_length)
        eq_(60, validator.default_duration_days)

    def test_validate_announcements(self):
        # validate_announcement succeeds if every individual announcment succeeds,
        # and if some additional checks pass on the announcement list as a whole.

        class AlwaysAcceptValidator(AnnouncementListValidator):
            def validate_announcement(self, announcement):
                announcement['validated'] = True
                return announcement

        validator = AlwaysAcceptValidator(maximum_announcements=2)
        m = validator.validate_announcements

        # validate_announcements calls validate_announcement on every
        # announcement in a list, so this...
        before = [
            {"id": "announcement1"},
            {"id": "announcement2"},
        ]

        # ...should become this.
        after = [
            {"id": "announcement1", "validated": True},
            {"id": "announcement2", "validated": True},
        ]
        validated = m(before)
        eq_(validated, after)

        # If a JSON string is passed in, it will be decoded before
        # processing.
        eq_(m(json.dumps(before)), after)

        # If you pass in something other than a list or JSON-encoded
        # list, you get a ProblemDetail.
        for invalid in dict(), json.dumps(dict()), "non-json string":
            self.assert_invalid(
                m(invalid),
                "Invalid announcement list format: %(announcements)r" % dict(
                    announcements=invalid
                )
            )

        # validate_announcements runs some checks on the list of announcements.
        # Each validator has a maximum length it will accept.
        too_many = [
            {"id": "announcement1"},
            {"id": "announcement2"},
            {"id": "announcement3"},
        ]
        self.assert_invalid(m(too_many), "Too many announcements: maximum is 2")

        # A list of announcements will be rejected if it contains duplicate IDs.
        duplicate_ids = [
            {"id": "announcement1"},
            {"id": "announcement1"},
        ]
        self.assert_invalid(
            m(duplicate_ids), "Duplicate announcement ID: announcement1"
        )

        # In addition, if validate_announcement ever rejects an
        # announcement, validate_announcements will fail with whatever
        # problem detail validate_announcement returned.
        class AlwaysRejectValidator(AnnouncementListValidator):
            def validate_announcement(self, announcement):
                return INVALID_INPUT.detailed("Rejected!")

        validator = AlwaysRejectValidator()
        self.assert_invalid(
            validator.validate_announcements(["an announcement"]),
            "Rejected!"
        )

    def test_validate_announcement_success(self):
        # End-to-end test of validate_announcement in successful scenarios.
        validator = AnnouncementListValidator()
        m = validator.validate_announcement

        # Simulate the creation of a new announcement -- no incoming ID.
        today = date.today()
        in_a_week = today + timedelta(days=7)
        valid = dict(
            start=today.strftime('%Y-%m-%d'),
            finish=in_a_week.strftime('%Y-%m-%d'),
            content="This is a test of announcement validation."
        )

        validated = m(valid)

        # A UUID has been added in the 'id' field.
        id = validated.pop('id')
        eq_(36, len(id))
        for position in 8, 13, 18, 23:
            eq_('-', id[position])

        # Date strings have been converted to date objects.
        eq_(today, validated['start'])
        eq_(in_a_week, validated['finish'])
        
        # Now simulate an edit, where an ID is provided.
        validated['id'] = 'an existing id'

        # Now the incoming data is validated but not changed at all.
        eq_(validated, m(validated))

        # If no start date is specified, today's date is used. If no
        # finish date is specified, a default associated with the
        # validator is used.
        no_finish_date = dict(
            content="This is a test of announcment validation"
        )
        validated = m(no_finish_date)
        eq_(today, validated['start'])
        eq_(
            today + timedelta(days=validator.default_duration_days),
            validated['finish']
        )

    def test_validate_announcement_failure(self):
        # End-to-end tests of validation failures for a single
        # announcement.
        validator = AnnouncementListValidator()
        m = validator.validate_announcement

        # Totally bogus format
        for invalid in '{"a": "string"}', ["a list"]:
            self.assert_invalid(
                m(invalid), 
                "Invalid announcement format: %(announcement)r" % dict(announcement=invalid)
            )

        # Some baseline valid value to use in tests where _some_ of the data is valid.
        today = date.today()
        tomorrow = today + timedelta(days=1)
        message = "An important message to all patrons: reading is FUN-damental!"

        # Missing a required field
        no_content = dict(start=today)
        self.assert_invalid(m(no_content), "Missing required field: content")
        
        # Bad content -- tested at greater length in another test.
        bad_content = dict(start=today, content="short")
        self.assert_invalid(
            m(bad_content),
            "Value too short (5 versus 15 characters): short"
        )

        # Bad start date -- tested at greater length in another test.
        bad_start_date = dict(start="not-a-date", content=message)
        self.assert_invalid(
            m(bad_start_date),
            "Value for start is not a date: not-a-date"
        )

        # Bad finish date.
        yesterday = today - timedelta(days=1)
        for bad_finish_date in (today, yesterday):
            bad_data = dict(start=today, finish=bad_finish_date, content=message)
            self.assert_invalid(
                m(bad_data),
                "Value for finish must be no earlier than %s" % (
                    tomorrow.strftime(validator.DATE_FORMAT)
                )
            )


    def test_validate_length(self):
        # Test the validate_length helper method in more detail than
        # it's tested in validate_announcement.
        m = AnnouncementListValidator.validate_length
        value = "four"
        eq_(value, m(value, 3, 5))
        
        self.assert_invalid(
            m(value, 10, 20),
            "Value too short (4 versus 10 characters): four"
        )

        self.assert_invalid(
            m(value, 1, 3),
            "Value too long (4 versus 3 characters): four"
        )

    def test_validate_date(self):
        # Test the validate_date helper method in more detail than
        # it's tested in validate_announcement.
        m = AnnouncementListValidator.validate_date

        february_1 = date(2020, 2, 1)

        # The incoming date can be either a string, date, or datetime.
        # The output is always a date.
        eq_(february_1, m("somedate", "2020-2-1"))
        eq_(february_1, m("somedate", february_1))
        eq_(february_1, m("somedate", datetime(2020, 2, 1)))

        # But if a string is used, it must be in a specific format.        
        self.assert_invalid(
            m("somedate", "not-a-date"), "Value for somedate is not a date: not-a-date"
        )

        # If a minimum (date or datetime) is provided, the selection
        # must be on or after that date.
        
        january_1 = date(2020, 1, 1)
        january_1_datetime = datetime(2020, 1, 1)
        eq_(february_1, m("somedate", february_1, minimum=january_1))
        eq_(february_1, m("somedate", february_1, minimum=january_1_datetime))

        self.assert_invalid(
            m("somedate", january_1, minimum=february_1),
            "Value for somedate must be no earlier than 2020-02-01",
        )

    def test_format(self):
        # Test our ability to format the output of validate_announcements for storage
        # in the database.

        validator = AnnouncementListValidator()
        announcements = [self.active, self.forthcoming]

        # Convert the announcements into a single JSON string.
        ready_for_storage = validator.format_as_string(announcements)

        # Now examine the string by converting it back from JSON to a list.
        as_list = json.loads(ready_for_storage)

        # The list contains dictionary representations of self.active
        # and self.forthcoming. But they're not exactly the same as
        # self.active and self.forthcoming -- they were converted into
        # Announcement objects and then back to dictionaries using
        # Announcement.json_ready.
        eq_([Announcement(**x).json_ready for x in announcements], as_list)

        
