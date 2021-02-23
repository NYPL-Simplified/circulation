import os

import pytest
from sqlalchemy.exc import ProgrammingError

from ..log import LogConfiguration
from ..model import (SessionManager,
    Base,
)


@pytest.fixture(autouse=True, scope="session")
def session_fixture():
    # This will make sure we always connect to the test database.
    os.environ['TESTING'] = 'true'

    # Ensure that the log configuration starts in a known state.
    LogConfiguration.initialize(None, testing=True)

    # Drop any existing schema. It will be recreated when
    # SessionManager.initialize() runs.
    #
    # Base.metadata.drop_all(connection) doesn't work here, so we
    # approximate by dropping every item individually.
    engine = SessionManager.engine()
    for table in reversed(Base.metadata.sorted_tables):
        statement = table.delete()
        try:
            engine.execute(statement)
        except ProgrammingError as e:
            # TODO PYTHON3
            # if isinstance(e.orig, UndefinedTable):
            if 'does not exist' in e.message:
                # This is the first time running these tests
                # on this server, and the tables don't exist yet.
                pass
            else:
                raise

    yield

    if 'TESTING' in os.environ:
        del os.environ['TESTING']
