# encoding: utf-8
# Test the helper objects in util.string.

import base64 as stdlib_base64
import re

from nose.tools import assert_raises, eq_
from parameterized import parameterized

from ...util.string_helpers import UnicodeAwareBase64, base64, is_string, random_string


class TestUnicodeAwareBase64(object):
    def test_encoding(self):
        string = "םולש"

        # Run the same tests against two different encodings that can
        # handle Hebrew characters.
        self._test_encoder(string, UnicodeAwareBase64("utf8"))
        self._test_encoder(string, UnicodeAwareBase64("iso-8859-8"))

        # If UnicodeAwareBase64 is given a string it can't encode in
        # its chosen encoding, an exception is the result.
        shift_jis = UnicodeAwareBase64("shift-jis")
        assert_raises(
            UnicodeEncodeError,
            shift_jis.b64encode,
            string
        )

    def _test_encoder(self, string, base64):
        # Create a binary version of the string in the encoder's
        # encoding, for use in comparisons.
        binary = string.encode(base64.encoding)

        # Test all supported methods of the base64 API.
        for encode, decode in [
            ('b64encode', 'b64decode'),
            ('standard_b64encode', 'standard_b64decode'),
            ('urlsafe_b64encode', 'urlsafe_b64decode'),
            ('encodestring', 'decodestring')
        ]:
            encode_method = getattr(base64, encode)
            decode_method = getattr(base64, decode)

            # Test a round-trip. Base64-encoding a Unicode string and
            # then decoding it should give the original string.
            encoded = encode_method(string)
            decoded = decode_method(encoded)
            eq_(string, decoded)

            # Test encoding on its own. Encoding with a
            # UnicodeAwareBase64 and then converting to ASCII should
            # give the same result as running the binary
            # representation of the string through the default bas64
            # module.
            base_encode = getattr(stdlib_base64, encode)
            base_encoded = base_encode(binary)
            eq_(base_encoded, encoded.encode("ascii"))

            # If you pass in a bytes object to a UnicodeAwareBase64
            # method, it's no problem. You get a Unicode string back.
            eq_(encoded, encode_method(binary))
            eq_(decoded, decode_method(base_encoded))

    def test_default_is_base64(self):
        # If you import "base64" from util.string, you get a
        # UnicodeAwareBase64 object that encodes as UTF-8 by default.
        assert isinstance(base64, UnicodeAwareBase64)
        eq_("utf8", base64.encoding)
        snowman = "☃"
        snowman_utf8 = snowman.encode("utf8")
        as_base64 = base64.b64encode(snowman)
        eq_("4piD", as_base64)

        # This is a Unicode representation of the string you'd get if
        # you encoded the snowman as UTF-8, then used the standard
        # library to base64-encode the bytestring.
        eq_(b"4piD", stdlib_base64.b64encode(snowman_utf8))


class TestRandomString(object):
    def test_random_string(self):
        m = random_string
        eq_("", m(0))

        # The strings are random.
        res1 = m(8)
        res2 = m(8)
        assert res1 != res2

        # We can't test exact values, because the randomness comes
        # from /dev/urandom, but we can test some of their properties:
        for size in range(1, 16):
            x = m(size)

            # The strings are Unicode strings, not bytestrings
            assert isinstance(x, str)

            # The strings are entirely composed of lowercase hex digits.
            eq_(None, re.compile("[^a-f0-9]").search(x))

            # Each byte is represented as two digits, so the length of the
            # string is twice the length passed in to the function.
            eq_(size * 2, len(x))


class TestIsString(object):
    @parameterized.expand(
        [
            ("byte_string", "test", True),
            ("unicode_string", "test", True),
            ("not_string", 1, False),
        ]
    )
    def test_is_string(self, _, value, expected_result):
        result = is_string(value)

        eq_(expected_result, result)
