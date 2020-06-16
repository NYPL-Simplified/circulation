import importlib
import logging
import re

from flask_babel import lazy_gettext as _

from api.admin.exceptions import *
from core.model import Representation
from core.util import LanguageCodes


class Validator(object):

    def validate(self, settings, content):
        validators = [
            self.validate_email,
            self.validate_url,
            self.validate_number,
            self.validate_language_code,
            self.validate_image,
        ]

        for validator in validators:
            error = validator(settings, content)
            if error:
                return error

    def _extract_inputs(self, settings, value, form, key="format", is_list=False, should_zip=False):
        if not (isinstance(settings, (list,))):
            return []

        fields = filter(lambda s: s.get(key) == value and self._value(s, form), settings)

        if is_list:
            values = self._list_of_values(fields, form)
        else:
            values = [self._value(field, form) for field in fields]

        if should_zip:
            return zip(fields, values)
        else:
            return values

    def validate_email(self, settings, content):
        """Find any email addresses that the user has submitted, and make sure that
        they are in a valid format.
        This method is used by individual_admin_settings and library_settings.
        """
        if isinstance(settings, (list,)):
            # If :param settings is a list of objects--i.e. the LibrarySettingsController
            # is calling this method--then we need to pull out the relevant input strings
            # to validate.
            email_inputs = self._extract_inputs(settings, "email", content.get("form"))
        else:
            # If the IndividualAdminSettingsController is calling this method, then we already have the
            # input string; it was passed in directly.
            email_inputs = [settings]

        # Now check that each email input is in a valid format
        for emails in email_inputs:
            if not isinstance(emails, list):
                emails = [emails]
            for email in emails:
                if not self._is_email(email):
                    return INVALID_EMAIL.detailed(_('"%(email)s" is not a valid email address.', email=email))

    def _is_email(self, email):
        """Email addresses must be in the format 'x@y.z'."""
        email_format = ".+\@.+\..+"
        return re.search(email_format, email)

    def validate_url(self, settings, content):
        """Find any URLs that the user has submitted, and make sure that
        they are in a valid format."""
        # Find the fields that have to do with URLs and are not blank.
        url_inputs = self._extract_inputs(settings, "url", content.get("form"), should_zip=True)

        for field, urls in url_inputs:
            if not isinstance(urls, list):
                urls = [urls]
            for url in urls:
                # In a few special cases, we want to allow a value that isn't a normal URL;
                # for example, the patron web client URL can be set to "*".
                allowed = field.get("allowed") or []
                if not self._is_url(url, allowed):
                    return INVALID_URL.detailed(_('"%(url)s" is not a valid URL.', url=url))

    @classmethod
    def _is_url(cls, url, allowed):
        if not url:
            return False
        has_protocol = any([url.startswith(protocol + "://") for protocol in "http", "https"])
        return has_protocol or (url in allowed)

    def validate_number(self, settings, content):
        """Find any numbers that the user has submitted, and make sure that they are 1) actually numbers,
        2) positive, and 3) lower than the specified maximum, if there is one."""
        # Find the fields that should have numeric input and are not blank.
        number_inputs = self._extract_inputs(settings, "number", content.get("form"), key="type", should_zip=True)
        for field, number in number_inputs:
            error = self._number_error(field, number)
            if error:
                return error

    def _number_error(self, field, number):
        min = field.get("min") or 0
        max = field.get("max")

        try:
            number = float(number)
        except ValueError:
            return INVALID_NUMBER.detailed(_('"%(number)s" is not a number.', number=number))

        if number < min:
            return INVALID_NUMBER.detailed(_('%(field)s must be greater than %(min)s.', field=field.get("label"), min=min))
        if max and number > max:
            return INVALID_NUMBER.detailed(_('%(field)s cannot be greater than %(max)s.', field=field.get("label"), max=max))

    def validate_language_code(self, settings, content):
        # Find the fields that should contain language codes and are not blank.
        language_inputs = self._extract_inputs(settings, "language-code", content.get("form"), is_list=True)
        for language in language_inputs:
            if not self._is_language(language):
                return UNKNOWN_LANGUAGE.detailed(_('"%(language)s" is not a valid language code.', language=language))

    def _is_language(self, language):
        # Check that the input string is in the list of recognized language codes.
        return LanguageCodes.string_to_alpha_3(language)

    def validate_image(self, settings, content):
        # Find the fields that contain image uploads and are not blank.
        files = content.get("files")
        if files:
            image_inputs = self._extract_inputs(settings, "image", files, key="type", should_zip=True)

            for setting, image in image_inputs:
                invalid_format = self._image_format_error(image)
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
        return filter(lambda x: x != None and x != "", value)


class ValidatorFactory(object):
    """Creates Validator instances for particular authentication protocols"""

    def __init__(self):
        """Initializes a new instance of ValidatorFactory class"""
        self._logger = logging.getLogger(__name__)

    def create(self, protocol):
        """
        Returns a configured validator for a specific protocol

        :param protocol: Patron authentication protocol
        :type protocol: string

        :return: Validator object (if there is any for the specific protocol)
        :rtype: Optional[Validator]
        """
        module_name = protocol

        try:
            provider_module = importlib.import_module(module_name)
            validator_class = getattr(provider_module, 'Validator', None)

            if validator_class:
                validator = validator_class()

                return validator
        except:
            self._logger.warning(_('Could not load a validator defined in {0} module'.format(module_name)))

        return None
