# encoding: utf-8
# IntegrationClient
from nose.tools import set_trace

from . import (
    Base,
    get_one,
    get_one_or_create,
)

import datetime
import os
import re
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    Unicode,
)
from sqlalchemy.orm import (
    relationship,
)
from ..util.string_helpers import random_string

class IntegrationClient(Base):
    """A client that has authenticated access to this application.

    Currently used to represent circulation managers that have access
    to the metadata wrangler.
    """
    __tablename__ = 'integrationclients'

    id = Column(Integer, primary_key=True)

    # URL (or human readable name) to represent the server.
    url = Column(Unicode, unique=True)

    # Shared secret
    shared_secret = Column(Unicode, unique=True, index=True)

    # It may be necessary to disable an integration client until it
    # upgrades to fix a known bug.
    enabled = Column(Boolean, default=True)

    created = Column(DateTime)
    last_accessed = Column(DateTime)

    loans = relationship('Loan', backref='integration_client')
    holds = relationship('Hold', backref='integration_client')

    def __repr__(self):
        return "<IntegrationClient: URL=%s ID=%s>" % (self.url, self.id)

    @classmethod
    def for_url(cls, _db, url):
        """Finds the IntegrationClient for the given server URL.

        :return: an IntegrationClient. If it didn't already exist,
            it will be created. If it didn't already have a secret, no
            secret will be set.
        """
        url = cls.normalize_url(url)
        now = datetime.datetime.utcnow()
        client, is_new = get_one_or_create(
            _db, cls, url=url, create_method_kwargs=dict(created=now)
        )
        client.last_accessed = now
        return client, is_new

    @classmethod
    def register(cls, _db, url, submitted_secret=None):
        """Creates a new server with client details."""
        client, is_new = cls.for_url(_db, url)

        if not is_new and (not submitted_secret or submitted_secret != client.shared_secret):
            raise ValueError('Cannot update existing IntegratedClient without valid shared_secret')

        generate_secret = (client.shared_secret is None) or submitted_secret
        if generate_secret:
            client.randomize_secret()

        return client, is_new

    @classmethod
    def normalize_url(cls, url):
        url = re.sub(r'^(http://|https://)', '', url)
        url = re.sub(r'^www\.', '', url)
        if url.endswith('/'):
            url = url[:-1]
        return str(url.lower())

    @classmethod
    def authenticate(cls, _db, shared_secret):
        client = get_one(_db, cls, shared_secret=str(shared_secret))
        if client:
            client.last_accessed = datetime.datetime.utcnow()
            # Committing immediately reduces the risk of contention.
            _db.commit()
            return client
        return None

    def randomize_secret(self):
        self.shared_secret = random_string(24)
