# This code is not in testing.py because it's very important that this
# package only be imported once per test run.
from nose.tools import set_trace
import os
from sqlalchemy.orm.session import Session

from model import (
    Base,
    Patron,
    SessionManager,
    get_one_or_create,
)

def _setup(dbinfo):
    # Connect to the database and create the schema within a transaction
    engine, connection = SessionManager.initialize(os.environ['DATABASE_URL_TEST'])
    Base.metadata.drop_all(connection)
    Base.metadata.create_all(connection)
    dbinfo.engine = engine
    dbinfo.connection = connection
    dbinfo.transaction = connection.begin_nested()

    db = Session(dbinfo.connection)
    SessionManager.initialize_data(db)

    # Test data: Create the patron used by the dummy authentication
    # mechanism.
    get_one_or_create(db, Patron, authorization_identifier="200",
                      create_method_kwargs=dict(external_identifier="200200200"))
    db.commit()

    print "Connection is now %r" % dbinfo.connection
    print "Transaction is now %r" % dbinfo.transaction

def _teardown(dbinfo):
    # Roll back the top level transaction and disconnect from the database
    dbinfo.transaction.rollback()
    dbinfo.connection.close()
    dbinfo.engine.dispose()
