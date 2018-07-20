from nose.tools import (
    set_trace,
    eq_,
)
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import base64
import os
from . import (
    DatabaseTest
)
from core.util.problem_detail import ProblemDetail
from core.model import (
    ExternalIntegration,
)
from api.problem_details import *
from api.registry import (
    RemoteRegistry,
    Registration,
)

class TestRemoteRegistry(DatabaseTest):

    def setup(self):
        super(TestRemoteRegistry, self).setup()

        # Create an ExternalIntegration that can be used as the basis for
        # a RemoteRegistry.
        self.integration = self._external_integration(
            protocol="some protocol", goal=ExternalIntegration.DISCOVERY_GOAL
        )

    def test_constructor(self):
        registry = RemoteRegistry(self.integration)
        eq_(self.integration, registry.integration)

    def test_for_integration_id(self):
        """Test the ability to build a Registry for an ExternalIntegration
        given its ID.
        """
        m = RemoteRegistry.for_integration_id

        registry = m(
            self._db, self.integration.id, ExternalIntegration.DISCOVERY_GOAL
        )
        assert isinstance(registry, RemoteRegistry)
        eq_(self.integration, registry.integration)

        # If the ID doesn't exist you get None.
        eq_(None, m(self._db, -1, ExternalIntegration.DISCOVERY_GOAL))

        # If the integration's goal doesn't match what you provided,
        # you get None.
        eq_(None, m(self._db, self.integration.id, "some other goal"))

    def test_for_protocol_and_goal(self):

        # Create two ExternalIntegrations that have different protocols
        # or goals from our original.
        same_goal_different_protocol = self._external_integration(
            protocol="some other protocol", goal=self.integration.goal
        )

        same_protocol_different_goal = self._external_integration(
            protocol=self.integration.protocol, goal="some other goal"
        )

        # Only the original ExternalIntegration has both the requested
        # protocol and goal, so only it becomes a RemoteRegistry.
        [registry] = list(
            RemoteRegistry.for_protocol_and_goal(
                self._db, self.integration.protocol, self.integration.goal
            )
        )
        assert isinstance(registry, RemoteRegistry)
        eq_(self.integration, registry.integration)


class TestRegistration(DatabaseTest):

    def test__decrypt_shared_secret(self):
        key = RSA.generate(2048)
        encryptor = PKCS1_OAEP.new(key)

        key2 = RSA.generate(2048)
        encryptor2 = PKCS1_OAEP.new(key2)

        shared_secret = os.urandom(24).encode('hex')
        encrypted_secret = base64.b64encode(encryptor.encrypt(shared_secret))

        # Success.
        m = Registration._decrypt_shared_secret
        eq_(shared_secret, m(encryptor, encrypted_secret))

        # If we try to decrypt using the wrong key, a ProblemDetail is
        # returned explaining the problem.
        problem = m(encryptor2, encrypted_secret)
        assert isinstance(problem, ProblemDetail)
        eq_(SHARED_SECRET_DECRYPTION_ERROR.uri, problem.uri)
        assert encrypted_secret in problem.detail
