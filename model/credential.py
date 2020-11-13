# encoding: utf-8
# Credential, DRMDeviceIdentifier, DelegatedPatronIdentifier
import datetime
import uuid

import sqlalchemy
from nose.tools import set_trace
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import and_

from ..util import is_session
from ..util.string_helpers import is_string
from . import Base, get_one, get_one_or_create


class Credential(Base):
    """A place to store credentials for external services."""
    __tablename__ = 'credentials'
    id = Column(Integer, primary_key=True)
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    collection_id = Column(Integer, ForeignKey('collections.id'), index=True)
    type = Column(String(255), index=True)
    credential = Column(String)
    expires = Column(DateTime, index=True)

    # One Credential can have many associated DRMDeviceIdentifiers.
    drm_device_identifiers = relationship(
        "DRMDeviceIdentifier", backref=backref("credential", lazy='joined')
    )

    __table_args__ = (

        # Unique indexes to prevent the creation of redundant credentials.

        # If both patron_id and collection_id are null, then (data_source_id,
        # type, credential) must be unique.
        Index(
            "ix_credentials_data_source_id_type_token",
            data_source_id, type, credential, unique=True,
            postgresql_where=and_(patron_id==None, collection_id==None)
        ),

        # If patron_id is null but collection_id is not, then
        # (data_source, type, collection_id) must be unique.
        Index(
            "ix_credentials_data_source_id_type_collection_id",
            data_source_id, type, collection_id,
            unique=True, postgresql_where=(patron_id==None)
        ),

        # If collection_id is null but patron_id is not, then
        # (data_source, type, patron_id) must be unique.
        # (At the moment this never happens.)
        Index(
            "ix_credentials_data_source_id_type_patron_id",
            data_source_id, type, patron_id,
            unique=True, postgresql_where=(collection_id==None)
        ),

        # If neither collection_id nor patron_id is null, then
        # (data_source, type, patron_id, collection_id)
        # must be unique.
        Index(
            "ix_credentials_data_source_id_type_patron_id_collection_id",
            data_source_id, type, patron_id, collection_id,
            unique=True,
        ),
    )

    # A meaningless identifier used to identify this patron (and no other)
    # to a remote service.
    IDENTIFIER_TO_REMOTE_SERVICE = "Identifier Sent To Remote Service"

    # An identifier used by a remote service to identify this patron.
    IDENTIFIER_FROM_REMOTE_SERVICE = "Identifier Received From Remote Service"

    @classmethod
    def _filter_invalid_credential(cls, credential, allow_persistent_token):
        """Filter out invalid credentials based on their expiration time and persistence.

        :param credential: Credential object
        :type credential: Credential

        :param allow_persistent_token: Boolean value indicating whether persistent tokens are allowed
        :type allow_persistent_token: bool
        """
        if not credential:
            # No matching token.
            return None

        if not credential.expires:
            if allow_persistent_token:
                return credential
            else:
                # It's an error that this token never expires. It's invalid.
                return None
        elif credential.expires > datetime.datetime.utcnow():
            return credential
        else:
            # Token has expired.
            return None

    @classmethod
    def lookup(cls, _db, data_source, token_type, patron, refresher_method,
               allow_persistent_token=False, allow_empty_token=False,
               collection=None, force_refresh=False):
        from datasource import DataSource
        if is_string(data_source):
            data_source = DataSource.lookup(_db, data_source)
        credential, is_new = get_one_or_create(
            _db, Credential, data_source=data_source, type=token_type, patron=patron, collection=collection)
        if (is_new
            or force_refresh
            or (not credential.expires and not allow_persistent_token)
            or (not credential.credential and not allow_empty_token)
            or (credential.expires
                and credential.expires <= datetime.datetime.utcnow())):
            if refresher_method:
                refresher_method(credential)
        return credential

    @classmethod
    def lookup_by_token(
            cls,
            _db,
            data_source,
            token_type,
            token,
            allow_persistent_token=False
    ):
        """Look up a unique token.
        Lookup will fail on expired tokens. Unless persistent tokens
        are specifically allowed, lookup will fail on persistent tokens.
        """

        credential = get_one(
            _db, Credential, data_source=data_source, type=token_type,
            credential=token)

        return cls._filter_invalid_credential(credential, allow_persistent_token)

    @classmethod
    def lookup_by_patron(
            cls,
            _db,
            data_source_name,
            token_type,
            patron,
            allow_persistent_token=False,
            auto_create_datasource=True
    ):
        """Look up a unique token.
        Lookup will fail on expired tokens. Unless persistent tokens
        are specifically allowed, lookup will fail on persistent tokens.

        :param _db: Database session
        :type _db: sqlalchemy.orm.session.Session

        :param data_source_name: Name of the data source
        :type data_source_name: str

        :param token_type: Token type
        :type token_type: str

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param allow_persistent_token: Boolean value indicating whether persistent tokens are allowed or not
        :type allow_persistent_token: bool

        :param auto_create_datasource: Boolean value indicating whether
            a data source should be created in the case it doesn't
        :type auto_create_datasource: bool
        """
        from patron import Patron

        if not is_session(_db):
            raise ValueError('"_db" argument must be a valid SQLAlchemy session')
        if not is_string(data_source_name) or not data_source_name:
            raise ValueError('"data_source_name" argument must be a non-empty string')
        if not is_string(token_type) or not token_type:
            raise ValueError('"token_type" argument must be a non-empty string')
        if not isinstance(patron, Patron):
            raise ValueError('"patron" argument must be an instance of Patron class')
        if not isinstance(allow_persistent_token, bool):
            raise ValueError('"allow_persistent_token" argument must be boolean')
        if not isinstance(auto_create_datasource, bool):
            raise ValueError('"auto_create_datasource" argument must be boolean')

        from datasource import DataSource
        data_source = DataSource.lookup(
            _db,
            data_source_name,
            autocreate=auto_create_datasource
        )
        credential = get_one(
            _db,
            Credential,
            data_source=data_source,
            type=token_type,
            patron=patron
        )

        return cls._filter_invalid_credential(credential, allow_persistent_token)

    @classmethod
    def lookup_and_expire_temporary_token(cls, _db, data_source, type, token):
        """Look up a temporary token and expire it immediately."""
        credential = cls.lookup_by_token(_db, data_source, type, token)
        if not credential:
            return None
        credential.expires = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=5)
        return credential

    @classmethod
    def temporary_token_create(
            cls,
            _db,
            data_source,
            token_type,
            patron,
            duration,
            value=None
    ):
        """Create a temporary token for the given data_source/type/patron.
        The token will be good for the specified `duration`.
        """
        expires = datetime.datetime.utcnow() + duration
        token_string = value or str(uuid.uuid1())
        credential, is_new = get_one_or_create(
            _db, Credential, data_source=data_source, type=token_type, patron=patron)
        # If there was already a token of this type for this patron,
        # the new one overwrites the old one.
        credential.credential=token_string
        credential.expires=expires
        return credential, is_new

    @classmethod
    def persistent_token_create(self, _db, data_source, type, patron, token_string=None):
        """Create or retrieve a persistent token for the given
        data_source/type/patron.
        """
        if token_string is None:
            token_string = str(uuid.uuid1())
        credential, is_new = get_one_or_create(
            _db, Credential, data_source=data_source, type=type, patron=patron,
            create_method_kwargs=dict(credential=token_string)
        )
        credential.expires=None
        return credential, is_new

    # A Credential may have many associated DRMDeviceIdentifiers.
    def register_drm_device_identifier(self, device_identifier):
        _db = Session.object_session(self)
        return get_one_or_create(
            _db, DRMDeviceIdentifier,
            credential=self,
            device_identifier=device_identifier
        )

    def deregister_drm_device_identifier(self, device_identifier):
        _db = Session.object_session(self)
        device_id_obj = get_one(
            _db, DRMDeviceIdentifier,
            credential=self,
            device_identifier=device_identifier
        )
        if device_id_obj:
            _db.delete(device_id_obj)

    def __repr__(self):
        return \
            '<Credential(' \
            'data_source_id={0}, ' \
            'patron_id={1}, ' \
            'collection_id={2}, ' \
            'type={3}, ' \
            'credential={4}, ' \
            'expires={5}>)'.format(
                    self.data_source_id,
                    self.patron_id,
                    self.collection_id,
                    self.type,
                    self.credential,
                    self.expires
                )


class DRMDeviceIdentifier(Base):
    """A device identifier for a particular DRM scheme.
    Associated with a Credential, most commonly a patron's "Identifier
    for Adobe account ID purposes" Credential.
    """
    __tablename__ = 'drmdeviceidentifiers'
    id = Column(Integer, primary_key=True)
    credential_id = Column(Integer, ForeignKey('credentials.id'), index=True)
    device_identifier = Column(String(255), index=True)


class DelegatedPatronIdentifier(Base):
    """This library is in charge of coming up with, and storing,
    identifiers associated with the patrons of some other library.
    e.g. NYPL provides Adobe IDs for patrons of all libraries that use
    the SimplyE app.
    Those identifiers are stored here.
    """
    ADOBE_ACCOUNT_ID = u'Adobe Account ID'

    __tablename__ = 'delegatedpatronidentifiers'
    id = Column(Integer, primary_key=True)
    type = Column(String(255), index=True)
    library_uri = Column(String(255), index=True)

    # This is the ID the foreign library gives us when referring to
    # this patron.
    patron_identifier = Column(String(255), index=True)

    # This is the identifier we made up for the patron. This is what the
    # foreign library is trying to look up.
    delegated_identifier = Column(String)

    __table_args__ = (
        UniqueConstraint('type', 'library_uri', 'patron_identifier'),
    )

    @classmethod
    def get_one_or_create(
            cls, _db, library_uri, patron_identifier, identifier_type,
            create_function
    ):
        """Look up the delegated identifier for the given patron. If there is
        none, create one.

        :param library_uri: A URI identifying the patron's library.
        :param patron_identifier: An identifier used by that library to
            distinguish between this patron and others. This should be
            an identifier created solely for the purpose of identifying the
            patron with _this_ library, and not (e.g.) the patron's barcode.
        :param identifier_type: The type of the delegated identifier
            to look up. (probably ADOBE_ACCOUNT_ID)
        :param create_function: If this patron does not have a
            DelegatedPatronIdentifier, one will be created, and this
            function will be called to determine the value of
            DelegatedPatronIdentifier.delegated_identifier.
        :return: A 2-tuple (DelegatedPatronIdentifier, is_new)
        """
        identifier, is_new = get_one_or_create(
            _db, DelegatedPatronIdentifier, library_uri=library_uri,
            patron_identifier=patron_identifier, type=identifier_type
        )
        if is_new:
            identifier.delegated_identifier = create_function()
        return identifier, is_new
