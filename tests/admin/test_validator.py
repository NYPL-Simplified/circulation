from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import json
from api.admin.validator import Validator
from api.config import Configuration
from werkzeug import MultiDict

class TestValidator():

    def test_validate_email(self):
        valid = "valid_format@email.com"
        invalid = "invalid_format"

        # One valid input from form
        form = MultiDict([("help-email", valid)])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, form)
        eq_(response, None)

        # One invalid input from form
        form = MultiDict([("help-email", invalid)])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"invalid_format" is not a valid email address.')
        eq_(response.status_code, 400)

        # One valid and one invalid input from form
        form = MultiDict([("help-email", valid), ("configuration_contact_email_address", invalid)])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"invalid_format" is not a valid email address.')
        eq_(response.status_code, 400)

        # Valid string
        response = Validator().validate_email(valid, None)
        eq_(response, None)

        # Invalid string
        response = Validator().validate_email(invalid, None)
        eq_(response.detail, '"invalid_format" is not a valid email address.')
        eq_(response.status_code, 400)

    def test_validate_url(self):
        valid = "https://valid_url.com"
        invalid = "invalid_url"

        # Valid
        form = MultiDict([("help-web", valid)])
        response = Validator().validate_url(Configuration.LIBRARY_SETTINGS, form)
        eq_(response, None)

        # Invalid
        form = MultiDict([("help-web", invalid)])
        response = Validator().validate_url(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"invalid_url" is not a valid URL.')
        eq_(response.status_code, 400)

        # One valid, one invalid
        form = MultiDict([("help-web", valid), ("terms-of-service", invalid)])
        response = Validator().validate_url(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"invalid_url" is not a valid URL.')
        eq_(response.status_code, 400)

    def test_validate_number(self):
        valid = "10"
        invalid = "ten"

        # Valid
        form = MultiDict([("hold_limit", valid)])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, form)
        eq_(response, None)

        # Invalid
        form = MultiDict([("hold_limit", invalid)])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"ten" is not a number.')
        eq_(response.status_code, 400)

        # One valid, one invalid
        form = MultiDict([("hold_limit", valid), ("loan_limit", invalid)])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"ten" is not a number.')
        eq_(response.status_code, 400)

        # Valid: below maximum
        form = MultiDict([("minimum_featured_quality", ".9")])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, form)
        eq_(response, None)

        # Invalid: above maximum
        form = MultiDict([("minimum_featured_quality", "2")])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, "Minimum quality for books that show up in 'featured' lanes cannot be greater than 1.")
        eq_(response.status_code, 400)

    def test_validate_language_code(self):
        all_valid = ["eng", "spa", "ita"]
        all_invalid = ["abc", "def", "ghi"]
        mixed = ["eng", "abc", "spa"]

        form = MultiDict([("large_collections", all_valid)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, form)
        eq_(response, None)

        form = MultiDict([("large_collections", all_invalid)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"abc" is not a valid language code.')
        eq_(response.status_code, 400)

        form = MultiDict([("large_collections", mixed)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"abc" is not a valid language code.')
        eq_(response.status_code, 400)

        form = MultiDict([("large_collections", all_valid), ("small_collections", all_valid), ("tiny_collections", mixed)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, form)
        eq_(response.detail, '"abc" is not a valid language code.')
        eq_(response.status_code, 400)

    def test_validate(self):
        called = []
        class Mock(Validator):
            def validate_email(self, settings, form):
                called.append("validate_email")
            def validate_url(self, settings, form):
                called.append("validate_url")
            def validate_number(self, settings, form):
                called.append("validate_number")
            def validate_language_code(self, settings, form):
                called.append("validate_language_code")
        Mock().validate(Configuration.LIBRARY_SETTINGS, None)
        eq_(called, ['validate_email', 'validate_url', 'validate_number', 'validate_language_code'])
