from StringIO import StringIO

from nose.tools import (
    eq_
)
from parameterized import parameterized
from werkzeug.datastructures import MultiDict

from api.admin.validator import Validator, PatronAuthenticationValidatorFactory
from api.config import Configuration
from api.shared_collection import BaseSharedCollectionAPI
from tests.admin.fixtures.dummy_validator import DummyAuthenticationProviderValidator


class TestValidator(object):
    def test_validate_email(self):
        valid = "valid_format@email.com"
        invalid = "invalid_format"

        # One valid input from form
        form = MultiDict([("help-email", valid)])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response, None)

        # One invalid input from form
        form = MultiDict([("help-email", invalid)])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"invalid_format" is not a valid email address.')
        eq_(response.status_code, 400)

        # One valid and one invalid input from form
        form = MultiDict([("help-email", valid), ("configuration_contact_email_address", invalid)])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"invalid_format" is not a valid email address.')
        eq_(response.status_code, 400)

        # Valid string
        response = Validator().validate_email(valid, {})
        eq_(response, None)

        # Invalid string
        response = Validator().validate_email(invalid, {})
        eq_(response.detail, '"invalid_format" is not a valid email address.')
        eq_(response.status_code, 400)

        # Two valid in a list
        form = MultiDict([('help-email', valid), ('help-email', 'valid2@email.com')])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response, None)

        # One valid and one empty in a list
        form = MultiDict([('help-email', valid), ('help-email', '')])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response, None)

        # One valid and one invalid in a list
        form = MultiDict([('help-email', valid), ('help-email', invalid)])
        response = Validator().validate_email(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"invalid_format" is not a valid email address.')
        eq_(response.status_code, 400)

    def test_validate_url(self):
        valid = "https://valid_url.com"
        invalid = "invalid_url"

        # Valid
        form = MultiDict([("help-web", valid)])
        response = Validator().validate_url(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response, None)

        # Invalid
        form = MultiDict([("help-web", invalid)])
        response = Validator().validate_url(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"invalid_url" is not a valid URL.')
        eq_(response.status_code, 400)

        # One valid, one invalid
        form = MultiDict([("help-web", valid), ("terms-of-service", invalid)])
        response = Validator().validate_url(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"invalid_url" is not a valid URL.')
        eq_(response.status_code, 400)

        # Two valid in a list
        form = MultiDict([(BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, "http://library1.com"), (BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, "http://library2.com")])
        response = Validator().validate_url(BaseSharedCollectionAPI.SETTINGS, {"form": form})
        eq_(response, None)

        # One valid and one empty in a list
        form = MultiDict([(BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, "http://library1.com"), (BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, "")])
        response = Validator().validate_url(BaseSharedCollectionAPI.SETTINGS, {"form": form})
        eq_(response, None)

        # One valid and one invalid in a list
        form = MultiDict([(BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, "http://library1.com"), (BaseSharedCollectionAPI.EXTERNAL_LIBRARY_URLS, invalid)])
        response = Validator().validate_url(BaseSharedCollectionAPI.SETTINGS, {"form": form})
        eq_(response.detail, '"invalid_url" is not a valid URL.')
        eq_(response.status_code, 400)

    def test_validate_number(self):
        valid = "10"
        invalid = "ten"

        # Valid
        form = MultiDict([("hold_limit", valid)])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response, None)

        # Invalid
        form = MultiDict([("hold_limit", invalid)])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"ten" is not a number.')
        eq_(response.status_code, 400)

        # One valid, one invalid
        form = MultiDict([("hold_limit", valid), ("loan_limit", invalid)])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"ten" is not a number.')
        eq_(response.status_code, 400)

        # Invalid: below minimum
        form = MultiDict([("hold_limit", -5)])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, 'Maximum number of books a patron can have on hold at once must be greater than 0.')
        eq_(response.status_code, 400)

        # Valid: below maximum
        form = MultiDict([("minimum_featured_quality", ".9")])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response, None)

        # Invalid: above maximum
        form = MultiDict([("minimum_featured_quality", "2")])
        response = Validator().validate_number(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, "Minimum quality for books that show up in 'featured' lanes cannot be greater than 1.")
        eq_(response.status_code, 400)

    def test_validate_language_code(self):
        all_valid = ["eng", "spa", "ita"]
        all_invalid = ["abc", "def", "ghi"]
        mixed = ["eng", "abc", "spa"]

        form = MultiDict([("large_collections", all_valid)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response, None)

        form = MultiDict([("large_collections", all_invalid)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"abc" is not a valid language code.')
        eq_(response.status_code, 400)

        form = MultiDict([("large_collections", mixed)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"abc" is not a valid language code.')
        eq_(response.status_code, 400)

        form = MultiDict([("large_collections", all_valid), ("small_collections", all_valid), ("tiny_collections", mixed)])
        response = Validator().validate_language_code(Configuration.LIBRARY_SETTINGS, {"form": form})
        eq_(response.detail, '"abc" is not a valid language code.')
        eq_(response.status_code, 400)

    def test_validate_image(self):
        def create_image_file(format_string):
            image_data = '\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x01\x03\x00\x00\x00%\xdbV\xca\x00\x00\x00\x06PLTE\xffM\x00\x01\x01\x01\x8e\x1e\xe5\x1b\x00\x00\x00\x01tRNS\xcc\xd24V\xfd\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82'
            class TestImageFile(StringIO):
                headers = { "Content-Type": "image/" + format_string }
            return TestImageFile(image_data)
            return result

        [png, jpeg, gif, invalid] = [
            MultiDict([(Configuration.LOGO, create_image_file(x))]) for x in ["png", "jpeg", "gif", "abc"]
        ]

        png_response = Validator().validate_image(Configuration.LIBRARY_SETTINGS, {"files": png})
        eq_(png_response, None)
        jpeg_response = Validator().validate_image(Configuration.LIBRARY_SETTINGS, {"files": jpeg})
        eq_(jpeg_response, None)
        gif_response = Validator().validate_image(Configuration.LIBRARY_SETTINGS, {"files": gif})
        eq_(gif_response, None)

        abc_response = Validator().validate_image(Configuration.LIBRARY_SETTINGS, {"files": invalid})
        eq_(abc_response.detail, 'Upload for Logo image must be in GIF, PNG, or JPG format. (Upload was image/abc.)')
        eq_(abc_response.status_code, 400)

    def test_validate(self):
        called = []
        class Mock(Validator):
            def validate_email(self, settings, content):
                called.append("validate_email")
            def validate_url(self, settings, content):
                called.append("validate_url")
            def validate_number(self, settings, content):
                called.append("validate_number")
            def validate_language_code(self, settings, content):
                called.append("validate_language_code")
            def validate_image(self, settings, content):
                called.append("validate_image")
        Mock().validate(Configuration.LIBRARY_SETTINGS, {})
        eq_(called, [
            'validate_email',
            'validate_url',
            'validate_number',
            'validate_language_code',
            'validate_image'
        ])

    def test__is_url(self):
        m = Validator._is_url

        eq_(False, m(None, []))
        eq_(False, m("", []))
        eq_(False, m("not a url", []))

        # Only HTTP and HTTP URLs are allowed.
        eq_(True, m("http://server.com/", []))
        eq_(True, m("https://server.com/", []))
        eq_(False, m("gopher://server.com/", []))
        eq_(False, m("http:/server.com/", []))

        # You can make specific URLs go through even if they
        # wouldn't normally pass.
        eq_(True, m("Not a URL", ["Not a URL", "Also not a URL"]))


class PatronAuthenticationValidatorFactoryTest(object):
    @parameterized.expand([
        ('validator_using_class_name', 'tests.admin.fixtures.dummy_validator'),
        ('validator_using_factory_method', 'tests.admin.fixtures.dummy_validator_factory')
    ])
    def test_create_can_create(self, name, protocol):
        # Arrange
        factory = PatronAuthenticationValidatorFactory()

        # Act
        result = factory.create(protocol)

        # Assert
        assert isinstance(result, DummyAuthenticationProviderValidator)
