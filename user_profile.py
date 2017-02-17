from nose.tools import set_trace
import json
from problem_details import *
from flask.ext.babel import lazy_gettext as _

class ProfileController(object):
    """Implement the User Profile Management Protocol.

    https://github.com/NYPL-Simplified/Simplified/wiki/User-Profile-Management-Protocol
    """

    MEDIA_TYPE = "vnd.librarysimplified/user-profile+json"
    LINK_RELATION = "http://librarysimplified.org/terms/rel/user-profile"
    
    def __init__(self, store):
        """Constructor.

        :param store: An instance of ProfileStore.
        """
        self.store = store

    def get(self):
        """Turn the store into a Profile document and send out its JSON-based
        representation.

        :param return: A ProblemDetail if there is a problem; otherwise,
            a 3-tuple (response code, media type, entity-body)
        """
        representation = None
        try:
            representation = self.store.representation
        except Exception, e:
            if hasattr(e, 'as_problem_detail_document'):
                return e.as_problem_detail_document()
            else:
                return INTERNAL_SERVER_ERROR.with_debug(e.message)
        if not isinstance(representation, dict):
            return INTERNAL_SERVER_ERROR.with_debug(
                _("Profile representation is not a JSON object: %r.") % (
                    representation
                )
            )
        try:
            body = json.dumps(representation)
        except Exception, e:
            return INTERNAL_SERVER_ERROR.with_debug(
                _("Could not convert profile to JSON: %r.") % (
                    representation
                )
            )
            
        return 200, self.MEDIA_TYPE, body
        
    def put(self, headers, body):
        """Turn the store into a Profile document and send out its JSON-based
        representation.

        :param return: A ProblemDetail if there is a problem; otherwise,
            a 3-tuple (response code, media type, entity-body)
        """
        media_type = headers.get('Content-Type')
        if media_type != self.MEDIA_TYPE:
            return UNSUPPORTED_MEDIA_TYPE.detailed(
                _("Expected %s") % self.MEDIA_TYPE
            )
        try:
            full_data = json.loads(body)
        except Exception, e:
            return INVALID_INPUT.detailed(
                _("Submitted profile document was not valid JSON.")
            )
        if not isinstance(full_data, dict):
            return INVALID_INPUT.detailed(
                _("Submitted profile document was not a JSON object.")
            )
        settable = full_data.get(ProfileStore.SETTINGS_KEY)
        if settable:
            # The incoming document is a request to change at least one
            # setting.
            allowable = set(self.store.setting_names)
            for k in settable.keys():
                if k not in allowable:
                    return INVALID_INPUT.detailed(
                        _('"%s" is not a writable setting.' % k)
                    )
            try:
                self.store.set(settable, full_data)
            except Exception, e:
                if hasattr(e, 'as_problem_detail_document'):
                    return e.as_problem_detail_document()
                else:
                    return INTERNAL_SERVER_ERROR.with_debug(e.message)
        return 200, "text/plain", ""


class ProfileStore(object):
    """An abstract store for a user profile."""

    NS = 'simplified:'
    FINES = NS + 'fines'
    AUTHORIZATION_EXPIRES = NS + "authorization_expires"
    SYNCHRONIZE_ANNOTATIONS = NS + 'synchronize_annotations'
    SETTINGS_KEY = 'settings'
    
    @property
    def representation(self):
        """Represent the current state of the store as a dictionary.

        :return: A dictionary that can be converted to a Profile document.
        """
        raise NotImplementedError()


    @property
    def setting_names(self):
        """Return the subset of fields that are considered writable.
        
        :return: An iterable.
        """
        raise NotImplementedError()
    
    def set(self, settable, full):
        """(Try to) make the local store look like the provided Profile
        document.

        :param settable: The portion of the Profile document containing
            settings that the client wants to change.

        :param full: The full Profile document as provided by the client.
            Should not be necessary but provided in case it's userful.
        """
        raise NotImplementedError()
    

class DictionaryBasedProfileStore(object):
    """A simple in-memory store based on Python dictionaries."""
    
    def __init__(self, read_only=None, writable=None):
        """Constructor.
        
        :param read_only: A dictionary of profile information that cannot
            be changed.
        :param writable: A dictionary of settings that can be changed.
        """
        self.read_only = read_only or dict()
        self.writable = writable or dict()

    @property
    def representation(self):
        """Represent the current state of the store as a dictionary.

        :return: A dictionary that can be converted to a Profile document.
        """
        body = dict(self.read_only)
        body[ProfileStore.SETTINGS_KEY] = dict(self.writable)
        return body

    @property
    def setting_names(self):
        """Return the subset of fields that are considered writable.
        
        :return: An iterable.
        """
        return self.writable.keys()
        
    def set(self, settable, full):
        """(Try to) make the local store look like the provided Profile
        document.

        :param settable: The portion of the Profile document containing
            settings that the client wants to change.

        :param full: The full Profile document as provided by the client.
            Should not be necessary but provided in case it's userful.
        """
        for k, v in settable.items():
            self.writable[k] = v
