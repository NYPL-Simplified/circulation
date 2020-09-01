from nose.tools import eq_
from parameterized import parameterized

from api.lcp.hash import HasherFactory, HashingAlgorithm


class TestHasherFactory(object):
    @parameterized.expand([
        (
                'sha256',
                HashingAlgorithm.SHA256,
                '12345',
                '5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5'
        ),
        (
                'sha256_value',
                HashingAlgorithm.SHA256.value,
                '12345',
                '5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5'
        ),
        (
                'sha512',
                HashingAlgorithm.SHA512,
                '12345',
                '3627909a29c31381a071ec27f7c9ca97726182aed29a7ddd2e54353322cfb30abb9e3a6df2ac2c20fe23436311d678564d0c8d305930575f60e2d3d048184d79'
        ),
        (
                'sha512_value',
                HashingAlgorithm.SHA512.value,
                '12345',
                '3627909a29c31381a071ec27f7c9ca97726182aed29a7ddd2e54353322cfb30abb9e3a6df2ac2c20fe23436311d678564d0c8d305930575f60e2d3d048184d79'
        )
    ])
    def test_create(self, _, hashing_algorithm, value, expected_value):
        #
        hasher_factory = HasherFactory()
        hasher = hasher_factory.create(hashing_algorithm)

        result = hasher.hash(value)

        eq_(result, expected_value)
