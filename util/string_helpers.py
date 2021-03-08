# Helper functions and objects regarding strings -- especially stuff
# that lets us negotiate the distinction between Unicode and
# bytestrings.

import base64 as stdlib_base64
import binascii
import os
import sys


class UnicodeAwareBase64(object):
    """Simulate the interface of the base64 module, but make it look as
    though base64-encoding and -decoding works on Unicode strings.

    Behind the scenes, Unicode strings are encoded to a particular
    encoding, then base64-encoded or -decoded, then decoded from that
    encoding.

    Since we get Unicode strings out of the database, this lets us
    base64-encode and -decode strings based on those strings, without
    worrying about encoding to bytes and then decoding.
    """

    def __init__(self, encoding):
        self.encoding = encoding

    def _ensure_bytes(self, s):
        if isinstance(s, bytes):
            return s
        return s.encode(self.encoding)

    def _ensure_unicode(self, s):
        if isinstance(s, bytes):
            return s.decode(self.encoding)
        return s

    def wrap(func):
        def wrapped(self, s, *args, **kwargs):
            s = self._ensure_bytes(s)
            value = func(s, *args, **kwargs)
            return self._ensure_unicode(value)
        return wrapped

    # Wrap most of the base64 module API so that Unicode is handled
    # transparently.
    b64encode = wrap(stdlib_base64.b64encode)
    b64decode = wrap(stdlib_base64.b64decode)
    standard_b64encode = wrap(stdlib_base64.standard_b64encode)
    standard_b64decode = wrap(stdlib_base64.standard_b64decode)
    urlsafe_b64encode = wrap(stdlib_base64.urlsafe_b64encode)
    urlsafe_b64decode = wrap(stdlib_base64.urlsafe_b64decode)

    # These are deprecated in base64 and we should stop using them.
    encodestring = wrap(stdlib_base64.encodestring)
    decodestring = wrap(stdlib_base64.decodestring)
    
# If you're okay with a Unicode strings being converted to/from UTF-8
# when you try to encode/decode them, you can use this object instead of
# the standard 'base64' module.
base64 = UnicodeAwareBase64("utf8")

def random_string(size):
    """Generate a random string of binary, encoded as hex digits.

    :param: Size of binary string in bytes.
    :return: A Unicode string.
    """
    return binascii.hexlify(os.urandom(size)).decode("utf8")
