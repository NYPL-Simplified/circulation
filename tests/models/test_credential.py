# encoding: utf-8
import pytest
import datetime
from sqlalchemy.exc import IntegrityError

from ...model.credential import (
    Credential,
    DelegatedPatronIdentifier,
    DRMDeviceIdentifier,
)
from ...model.datasource import DataSource
from ...util.datetime_helpers import utc_now

class TestCredentials:

    def test_temporary_token(self, db_session, create_patron):
        """
        GIVEN: A Credential (token) tied to a Patron
        WHEN:  Looking up and/or expiring the token
        THEN:  Token status is as expected
        """
        # Create a temporary token good for one hour.
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        patron = create_patron(db_session)
        now = utc_now()
        expect_expires = now + duration
        token, _ = Credential.temporary_token_create(
            db_session, data_source, "some random type", patron, duration)
        assert data_source == token.data_source
        assert "some random type" == token.type
        assert patron == token.patron
        expires_difference = abs((token.expires-expect_expires).seconds)
        assert expires_difference < 2

        # Now try to look up the credential based solely on the UUID.
        new_token = Credential.lookup_by_token(
            db_session, data_source, token.type, token.credential)
        assert new_token == token

        # When we call lookup_and_expire_temporary_token, the token is automatically
        # expired and we cannot use it anymore.
        new_token = Credential.lookup_and_expire_temporary_token(
            db_session, data_source, token.type, token.credential)
        assert new_token == token
        assert new_token.expires < now

        new_token = Credential.lookup_by_token(
            db_session, data_source, token.type, token.credential)
        assert None == new_token

        new_token = Credential.lookup_and_expire_temporary_token(
            db_session, data_source, token.type, token.credential)
        assert None == new_token

        # A token with no expiration date is treated as expired...
        token.expires = None
        no_expiration_token = Credential.lookup_by_token(
            db_session, data_source, token.type, token.credential)
        assert None == no_expiration_token

        # ...unless we specifically say we're looking for a persistent token.
        no_expiration_token = Credential.lookup_by_token(
            db_session, data_source, token.type, token.credential,
            allow_persistent_token=True
        )
        assert token == no_expiration_token

    def test_specify_value_of_temporary_token(self, db_session, create_patron):
        """
        GIVEN: A Patron
        WHEN:  Creating a temporary token Credential for the Patron with
               a specific value
        THEN:  The specific value is returned as the token's credential
        """
        patron = create_patron(db_session)
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        token, _ = Credential.temporary_token_create(
            db_session, data_source, "some random type", patron, duration,
            "Some random value"
        )
        assert "Some random value" == token.credential

    def test_temporary_token_overwrites_old_token(self, db_session, create_patron):
        """
        GIVEN: A Patron with a temporary token Credential
        WHEN:  Creating a new temporary token Credential for the Patron
        THEN:  The old temporary token is overwritten
        """
        duration = datetime.timedelta(hours=1)
        data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        patron = create_patron(db_session)
        old_token, is_new = Credential.temporary_token_create(
            db_session, data_source, "some random type", patron, duration)
        assert True == is_new
        old_credential = old_token.credential

        # Creating a second temporary token overwrites the first.
        token, is_new = Credential.temporary_token_create(
            db_session, data_source, "some random type", patron, duration)
        assert False == is_new
        assert token.id == old_token.id
        assert old_credential != token.credential

    def test_persistent_token(self, db_session, create_patron):
        """
        GIVEN: A Patron with a persistent token Credential
        WHEN:  Looking up the token Credential
        THEN:  The persistent token is always returned
        """

        # Create a persistent token.
        data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        patron = create_patron(db_session)
        token, _ = Credential.persistent_token_create(
            db_session, data_source, "some random type", patron
        )
        assert data_source == token.data_source
        assert "some random type" == token.type
        assert patron == token.patron

        # Now try to look up the credential based solely on the UUID.
        new_token = Credential.lookup_by_token(
            db_session, data_source, token.type, token.credential,
            allow_persistent_token=True
        )
        assert new_token == token
        credential = new_token.credential

        # We can keep calling lookup_by_token and getting the same
        # Credential object with the same .credential -- it doesn't
        # expire.
        again_token = Credential.lookup_by_token(
            db_session, data_source, token.type, token.credential,
            allow_persistent_token=True
        )
        assert again_token == new_token
        assert again_token.credential == credential

    def test_cannot_look_up_nonexistent_token(self, db_session):
        """
        GIVEN: An invalid token and token type
        WHEN:  Looking up a Credential with invalid data
        THEN:  None is returned
        """
        data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        new_token = Credential.lookup_by_token(
            db_session, data_source, "no such type", "no such credential")
        assert None == new_token

    def test_empty_token(self, db_session):
        """
        GIVEN: A token tied to an empty Credential
        WHEN:  Looking up the Credential with allow_empty_token set to True and False
        THEN:  Token is either returned or the refresher method is called
        """
        # Test the behavior when a credential is empty.

        # First, create a token with an empty credential.
        data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        token, _ = Credential.persistent_token_create(
            db_session, data_source, "i am empty", None
        )
        token.credential = None

        # If allow_empty_token is true, the token is returned as-is
        # and the refresher method is not called.
        def refresher(self):
            raise Exception("Refresher method was called")
        args = db_session, data_source, token.type, None, refresher,
        again_token = Credential.lookup(
            *args, allow_persistent_token=True, allow_empty_token=True
        )
        assert again_token == token

        # If allow_empty_token is False, the refresher method is
        # created.
        with pytest.raises(Exception) as excinfo:
            Credential.lookup(*args, allow_persistent_token = True, allow_empty_token = False)
        assert "Refresher method was called" in str(excinfo.value)

    def test_force_refresher_method(self, db_session, create_patron):
        """
        GIVEN: A Patron with a persistent token Credential
        WHEN:  Looking up the Credential with force_refresh
        THEN:  Refresher method is called
        """
        # Ensure that passing `force_refresh=True` triggers the
        # refresher method, even when none of the usual conditions
        # are satisfied.

        def refresher(self):
            raise Exception("Refresher method was called")

        # Create a persistent token and ensure that it's present
        data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        patron = create_patron(db_session)
        token, _ = Credential.persistent_token_create(
            db_session, data_source, "some random type", patron
        )
        assert data_source == token.data_source
        assert "some random type" == token.type
        assert patron == token.patron

        # We'll vary the `force_refresh` setting, but otherwise
        # use the same parameters for the next to calls to `lookup`.
        args = db_session, data_source, token.type, patron, refresher

        # This call should should not run the refresher method.
        again_token = Credential.lookup(
            *args, allow_persistent_token=True, force_refresh=False
        )
        assert again_token == token

        # This call should run the refresher method.
        with pytest.raises(Exception) as excinfo:
            Credential.lookup(*args, allow_persistent_token = True, force_refresh=True)
        assert "Refresher method was called" in str(excinfo.value)

    def test_collection_token(self, db_session, create_collection, create_patron):
        """
        GIVEN: Two Collections and a Patron
        WHEN:  Creating token Credentials for the Patron tied to the respective Collection
        THEN:  Credentials match what was set
        """
        # Make sure we can have two tokens from the same data_source with
        # different collections.
        data_source = DataSource.lookup(db_session, DataSource.RB_DIGITAL)
        collection1 = create_collection(db_session, "test collection 1")
        collection2 = create_collection(db_session, "test collection 2")
        patron = create_patron(db_session)
        type = "super secret"

        # Create our credentials
        credential1 = Credential.lookup(db_session, data_source, type, patron, None, collection=collection1)
        credential2 = Credential.lookup(db_session, data_source, type, patron, None, collection=collection2)
        credential1.credential = 'test1'
        credential2.credential = 'test2'

        # Make sure the text matches what we expect
        assert 'test1' == Credential.lookup(db_session, data_source, type, patron, None, collection=collection1).credential
        assert 'test2' == Credential.lookup(db_session, data_source, type, patron, None, collection=collection2).credential

        # Make sure we don't get anything if we don't pass a collection
        assert None == Credential.lookup(db_session, data_source, type, patron, None).credential

class TestDelegatedPatronIdentifier:

    def test_get_one_or_create(self, db_session):
        """
        GIVEN: A DelegatedPatronIdentifier
        WHEN:  Calling get_one_or_create
        THEN:  A DelegatedPatronIdentifier is either retrieved or created
        """
        library_uri = "http://example.com"
        patron_identifier = "42"
        identifier_type = DelegatedPatronIdentifier.ADOBE_ACCOUNT_ID
        def make_id():
            return "id1"
        identifier, is_new = DelegatedPatronIdentifier.get_one_or_create(
            db_session, library_uri, patron_identifier, identifier_type,
            make_id
        )
        assert True == is_new
        assert library_uri == identifier.library_uri
        assert patron_identifier == identifier.patron_identifier
        # id_1() was called.
        assert "id1" == identifier.delegated_identifier

        # Try the same thing again but provide a different create_function
        # that raises an exception if called.
        def explode():
            raise Exception("I should never be called.")
        identifier2, is_new = DelegatedPatronIdentifier.get_one_or_create(
            db_session, library_uri, patron_identifier, identifier_type, explode
        )
        # The existing identifier was looked up.
        assert False == is_new
        assert identifier2.id == identifier.id
        # id_2() was not called.
        assert "id1" == identifier2.delegated_identifier


class TestUniquenessConstraints:

    @pytest.fixture(autouse=True)
    def setup_method(self, db_session, create_patron, create_collection, create_library, init_datasource_and_genres):
        self.data_source = DataSource.lookup(db_session, DataSource.OVERDRIVE)
        self.type = 'a credential type'

        # Create a default collection
        self.library = create_library(db_session, name="default", short_name="default")
        self.patron = create_patron(db_session, library=self.library)
        collection= create_collection(db_session, name="Default Collection")
        self.library.collections.append(collection)

        self.col1 = collection
        self.col2 = create_collection(db_session)

    def test_duplicate_sitewide_credential(self, db_session):
        """
        GIVEN: A sitewide Credential
        WHEN:  Creating another sitewide Credential with the same data
        THEN:  An IntegrityError is raised
        """
        # You can't create two credentials with the same data source,
        # type, and token value.
        token = 'a token'

        c1 = Credential(
            data_source=self.data_source, type=self.type, credential=token
        )
        db_session.flush()
        c2 = Credential(
            data_source=self.data_source, type=self.type, credential=token
        )
        pytest.raises(IntegrityError, db_session.flush)

    def test_duplicate_patron_credential(self, db_session, create_patron):
        """
        GIVEN: A Patron with Credentials
        WHEN:  Creating Credentials with the same data
        THEN:  An IntegrityError is raised
        """
        # A given patron can't have two global credentials with the same data
        # source and type.
        patron = create_patron(db_session, library=self.library)

        c1 = Credential(
            data_source=self.data_source, type=self.type, patron=self.patron
        )
        db_session.flush()
        c2 = Credential(
            data_source=self.data_source, type=self.type, patron=self.patron
        )
        pytest.raises(IntegrityError, db_session.flush)

    def test_duplicate_patron_collection_credential(self, db_session):
        """
        GIVEN: A Patron with collection-scoped Credentials
        WHEN:  Creating another collection-scoped Credential with the same data
        THEN:  An IntegrityError is raised
        """
        # A given patron can have two collection-scoped credentials
        # with the same data source and type, but only if the two
        # collections are different.

        c1 = Credential(
            data_source=self.data_source, type=self.type, patron=self.patron,
            collection=self.col1
        )
        c2 = Credential(
            data_source=self.data_source, type=self.type, patron=self.patron,
            collection=self.col2
        )
        db_session.flush()
        c3 = Credential(
            data_source=self.data_source, type=self.type, patron=self.patron,
            collection=self.col1
        )
        pytest.raises(IntegrityError, db_session.flush)

    def test_duplicate_collection_credential(self, db_session):
        """
        GIVEN: A Collection with global Credentials
        WHEN:  Creating another Credential with the same data for the Collection
        THEN:  An IntegrityError is raised
        """
        # A given collection can't have two global credentials with
        # the same data source and type.
        c1 = Credential(
            data_source=self.data_source, type=self.type, collection=self.col1
        )
        db_session.flush()
        c2 = Credential(
            data_source=self.data_source, type=self.type, collection=self.col1
        )
        pytest.raises(IntegrityError, db_session.flush)


class TestDRMDeviceIdentifier:

    @pytest.fixture(autouse=True)
    def setup_method(self, db_session, create_patron):
        self.data_source = DataSource.lookup(db_session, DataSource.ADOBE)
        self.patron = create_patron(db_session)
        self.credential, _ = Credential.persistent_token_create(
            db_session, self.data_source, "Some Credential", self.patron)

    def test_devices_for_credential(self):
        """
        GIVEN: A Credential tied to a Patron
        WHEN:  Registering a DRM Device Identifier
        THEN:  The DRM Device Identifiers are tied to the Credential
        """
        device_id_1, new = self.credential.register_drm_device_identifier("foo")
        assert "foo" == device_id_1.device_identifier
        assert self.credential == device_id_1.credential
        assert True == new

        device_id_2, new = self.credential.register_drm_device_identifier("foo")
        assert device_id_1 == device_id_2
        assert False == new

        device_id_3, new = self.credential.register_drm_device_identifier("bar")

        assert set([device_id_1, device_id_3]) == set(self.credential.drm_device_identifiers)

    def test_deregister(self, db_session):
        """
        GIVEN: A Credential tied to a Patron
        WHEN:  Registering and deregistering a DRM Device Identifier
        THEN:  No DRMDeviceIdentifiers are returned
        """
        self.credential.register_drm_device_identifier("foo")
        self.credential.deregister_drm_device_identifier("foo")
        assert [] == self.credential.drm_device_identifiers
        assert [] == db_session.query(DRMDeviceIdentifier).all()
