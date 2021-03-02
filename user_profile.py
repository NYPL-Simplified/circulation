from nose.tools import set_trace
import json
from .problem_details import *
from flask_babel import lazy_gettext as _

class ProfileController(object):
    """Implement the User Profile Management Protocol.

    https://github.com/NYPL-Simplified/Simplified/wiki/User-Profile-Management-Protocol
    """

    MEDIA_TYPE = "vnd.librarysimplified/user-profile+json"
    LINK_RELATION = "http://librarysimplified.org/terms/rel/user-profile"

    def __init__(self, storage):
        """Constructor.

        :param storage: An instance of ProfileStorage.
        """
        self.storage = storage

    def get(self):
        """Turn the storage object into a Profile document and send out its
        JSON-based representation.

        :param return: A ProblemDetail if there is a problem; otherwise,
            a 3-tuple (entity-body, response code, headers)
        """
        profile_document = None
        try:
            profile_document = self.storage.profile_document
        except Exception as e:
            if hasattr(e, 'as_problem_detail_document'):
                return e.as_problem_detail_document()
            else:
                return INTERNAL_SERVER_ERROR.with_debug(str(e))
        if not isinstance(profile_document, dict):
            return INTERNAL_SERVER_ERROR.with_debug(
                _("Profile document is not a JSON object: %r.") % (
                    profile_document
                )
            )
        try:
            body = json.dumps(profile_document)
        except Exception as e:
            return INTERNAL_SERVER_ERROR.with_debug(
                _("Could not convert profile document to JSON: %r.") % (
                    profile_document
                )
            )

        return body, 200, {"Content-Type": self.MEDIA_TYPE}

    def put(self, headers, body):
        """Update the profile storage object with new settings
        from a Profile document sent with a PUT request.

        :param return: A ProblemDetail if there is a problem; otherwise,
            a 3-tuple (response code, media type, entity-body)
        """
        media_type = headers.get('Content-Type')
        if media_type != self.MEDIA_TYPE:
            return UNSUPPORTED_MEDIA_TYPE.detailed(
                _("Expected %s") % self.MEDIA_TYPE
            )
        try:
            profile_document = json.loads(body)
        except Exception as e:
            return INVALID_INPUT.detailed(
                _("Submitted profile document was not valid JSON.")
            )
        if not isinstance(profile_document, dict):
            return INVALID_INPUT.detailed(
                _("Submitted profile document was not a JSON object.")
            )
        new_settings = profile_document.get(ProfileStorage.SETTINGS_KEY)
        if new_settings:
            # The incoming document is a request to change at least one
            # setting in the profile.
            writable = set(self.storage.writable_setting_names)
            for k in list(new_settings.keys()):
                # A Profile document is invalid if it attempts to
                # change the value of a read-only profile setting.
                if k not in writable:
                    return INVALID_INPUT.detailed(
                        _('"%s" is not a writable setting.' % k)
                    )
            try:
                # Update the profile storage with the new settings.
                self.storage.update(new_settings, profile_document)
            except Exception as e:
                # There was a problem updating the profile storage.
                if hasattr(e, 'as_problem_detail_document'):
                    return e.as_problem_detail_document()
                else:
                    return INTERNAL_SERVER_ERROR.with_debug(str(e))
        return body, 200, {"Content-Type": "text/plain"}


class ProfileStorage(object):
    """An abstract class defining a specific user's profile.

    Subclasses should get profile information from somewhere specific,
    e.g. a database row.

    An instance of this class is responsible for one specific user's profile,
    not the set of all profiles.
    """

    NS = 'simplified:'
    FINES = NS + 'fines'
    AUTHORIZATION_IDENTIFIER = NS + "authorization_identifier"
    AUTHORIZATION_EXPIRES = NS + "authorization_expires"
    SYNCHRONIZE_ANNOTATIONS = NS + 'synchronize_annotations'
    SETTINGS_KEY = 'settings'

    @property
    def profile_document(self):
        """Create a Profile document representing the current state of
        the user's profile.

        :return: A dictionary that can be serialized as JSON.
        """
        raise NotImplementedError()

    def update(self, new_values, profile_document):
        """(Try to) change the user's profile so it looks like the provided
        Profile document.

        :param new_values: A dictionary of settings that the
            client wants to change.

        :param profile_document: The full Profile document as provided
            by the client. Should not be necessary, but provided in
            case it's useful.

        :raise Exception: If there's a problem making the user's profile
            look like the provided Profile document.
        """
        raise NotImplementedError()

    @property
    def writable_setting_names(self):
        """Return the subset of settings that are considered writable.

        An attempt to modify a setting that's not in this list will fail
        before update() is called.

        :return: An iterable.
        """
        raise NotImplementedError()


class MockProfileStorage(ProfileStorage):
    """A profile storage object for use in tests.

    Keeps information in in-memory dictionaries rather than in a database.
    """

    def __init__(self, read_only_settings=None, writable_settings=None):
        """Create a profile for a simulated user.

        :param read_only_settings: A dictionary of values that cannot be
            changed.

        :param writable_settings: A dictionary of values that can be changed
            through the User Profile Management Protocol.
        """
        self.read_only_settings = read_only_settings or dict()
        self.writable_settings = writable_settings or dict()

    @property
    def profile_document(self):
        body = dict(self.read_only_settings)
        body[self.SETTINGS_KEY] = dict(self.writable_settings)
        return body

    def update(self, new_values, profile_document):
        """(Try to) change the user's profile so it looks like the provided
        Profile document.
        """
        for k, v in list(new_values.items()):
            self.writable_settings[k] = v

    @property
    def writable_setting_names(self):
        """Return the subset of fields that are considered writable."""
        return list(self.writable_settings.keys())
