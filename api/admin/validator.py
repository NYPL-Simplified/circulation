from api.problem_details import *
from api.admin.exceptions import *
from core.model import Representation
from core.util.problem_detail import ProblemDetail
from core.util import LanguageCodes
from nose.tools import set_trace
from flask_babel import lazy_gettext as _
import re

class Validator(object):

    def validate(self, settings, form, files):
        validators = [
            self.validate_email,
            self.validate_url,
            self.validate_number,
            self.validate_language_code,
            self.validate_image,
        ]

        for validator in validators:
            error = validator(settings, form, files)
            if error:
                return error

    def validate_email(self, settings, form, files):
        """Find any email addresses that the user has submitted, and make sure that
        they are in a valid format.
        This method is used by individual_admin_settings and library_settings.
        """
        # If :param settings is a list of objects--i.e. the LibrarySettingsController
        # is calling this method--then we need to pull out the relevant input strings
        # to validate.
        if isinstance(settings, (list,)):
            # Find the fields that have to do with email addresses and are not blank
            email_fields = filter(lambda s: s.get("format") == "email" and self._value(s, form), settings)
            # Narrow the email-related fields down to the ones for which the user actually entered a value
            email_inputs = [self._value(field, form) for field in email_fields]
            # Now check that each email input is in a valid format
        else:
        # If the IndividualAdminSettingsController is calling this method, then we already have the
        # input string; it was passed in directly.
            email_inputs = [settings]
        for email in email_inputs:
            if not self._is_email(email):
                return INVALID_EMAIL.detailed(_('"%(email)s" is not a valid email address.', email=email))

    def _is_email(self, email):
        """Email addresses must be in the format 'x@y.z'."""
        email_format = ".+\@.+\..+"
        return re.search(email_format, email)

    def validate_url(self, settings, form, files):
        """Find any URLs that the user has submitted, and make sure that
        they are in a valid format."""
        # Find the fields that have to do with URLs and are not blank.
        if isinstance(settings, (list,)):
            url_fields = filter(lambda s: s.get("format") == "url" and self._value(s, form), settings)

            for field in url_fields:
                url = self._value(field, form)
                # In a few special cases, we want to allow a value that isn't a normal URL;
                # for example, the patron web client URL can be set to "*".
                allowed = field.get("allowed") or []
                if not self._is_url(url, allowed):
                    return INVALID_URL.detailed(_('"%(url)s" is not a valid URL.', url=url))

    def _is_url(self, url, allowed):
        has_protocol = any([url.startswith(protocol + "://") for protocol in "http", "https"])
        return has_protocol or (url in allowed)

    def validate_number(self, settings, form, files):
        """Find any numbers that the user has submitted, and make sure that they are 1) actually numbers,
        2) positive, and 3) lower than the specified maximum, if there is one."""
        # Find the fields that should have numeric input and are not blank.
        if isinstance(settings, (list,)):
            number_fields = filter(lambda s: s.get("type") == "number" and self._value(s, form), settings)
            for field in number_fields:
                if self._number_error(field, form):
                    return self._number_error(field, form)

    def _number_error(self, field, form):
        input = form.get(field.get("key")) or form.get("value")
        min = field.get("min") or 0
        max = field.get("max")

        try:
            input = float(input)
        except ValueError:
            return INVALID_NUMBER.detailed(_('"%(input)s" is not a number.', input=input))

        if input < min:
            return INVALID_NUMBER.detailed(_('%(field)s must be greater than %(min)s.', field=field.get("label"), min=min))
        if max and input > max:
            return INVALID_NUMBER.detailed(_('%(field)s cannot be greater than %(max)s.', field=field.get("label"), max=max))

    def validate_language_code(self, settings, form, files):
        # Find the fields that should contain language codes and are not blank.
        if isinstance(settings, (list,)):
            language_fields = filter(lambda s: s.get("format") == "language-code" and self._value(s, form), settings)

            for language in self._list_of_values(language_fields, form):
                if not self._is_language(language):
                    return UNKNOWN_LANGUAGE.detailed(_('"%(language)s" is not a valid language code.', language=language))

    def _is_language(self, language):
        # Check that the input string is in the list of recognized language codes.
        return LanguageCodes.string_to_alpha_3(language)

    def validate_image(self, settings, form, files):
        # Find the fields that contain image uploads and are not blank.
        if files and isinstance(settings, (list,)):
            image_settings = filter(lambda s: s.get("type") == "image" and self._value(s, files), settings)
            for setting in image_settings:
                image_file = files.get(setting.get("key"))
                invalid_format = self._image_format_error(image_file)
                if invalid_format:
                    return INVALID_CONFIGURATION_OPTION.detailed(_(
                        "Upload for %(setting)s must be in GIF, PNG, or JPG format. (Upload was %(format)s.)",
                        setting=setting.get("label"),
                        format=invalid_format))

    def _image_format_error(self, image_file):
        # Check that the uploaded image is in an acceptable format.
        allowed_types = [Representation.JPEG_MEDIA_TYPE, Representation.PNG_MEDIA_TYPE, Representation.GIF_MEDIA_TYPE]
        image_type = image_file.headers.get("Content-Type")
        if not image_type in allowed_types:
            return image_type

    def _list_of_values(self, fields, form):
        result = []
        for field in fields:
            result += self._value(field, form)
        return filter(None, result)

    def _value(self, field, form):
        # Extract the user's input for this field. If this is a sitewide setting,
        # then the input needs to be accessed via "value" rather than via the setting's key.
        # We use getlist instead of get so that, if the field is such that the user can input multiple values
        # (e.g. language codes), we'll extract all the values, not just the first one.
        value = form.getlist(field.get("key"))
        if not value:
            return form.get("value")
        elif len(value) == 1:
            return value[0]
        return value

class GeographicLookup(Validator):
    pass
