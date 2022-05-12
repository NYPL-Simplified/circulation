import hashlib
from abc import ABCMeta, abstractmethod

from enum import Enum

from core.exceptions import BaseError


class HashingAlgorithm(Enum):
    SHA256 = 'http://www.w3.org/2001/04/xmlenc#sha256'
    SHA512 = 'http://www.w3.org/2001/04/xmlenc#sha512'


class HashingError(BaseError):
    """Raised in the case of errors occurred during hashing"""


class Hasher(object):
    """Base class for all implementations of different hashing algorithms"""

    __metaclass__ = ABCMeta

    def __init__(self, hashing_algorithm):
        """Initializes a new instance of Hasher class

        :param hashing_algorithm: Hashing algorithm
        :type hashing_algorithm: HashingAlgorithm
        """
        self._hashing_algorithm = hashing_algorithm

    @abstractmethod
    def hash(self, value):
        raise NotImplementedError()


class UniversalHasher(Hasher):
    def hash(self, value):
        if self._hashing_algorithm in [HashingAlgorithm.SHA256, HashingAlgorithm.SHA256.value]:
            return hashlib.sha256(value).hexdigest()
        elif self._hashing_algorithm in [HashingAlgorithm.SHA512, HashingAlgorithm.SHA512.value]:
            return hashlib.sha512(value).hexdigest()
        else:
            raise HashingError('Unknown hashing algorithm {0}'.format(self._hashing_algorithm))


class HasherFactory(object):
    def create(self, hashing_algorithm):
        return UniversalHasher(hashing_algorithm)
