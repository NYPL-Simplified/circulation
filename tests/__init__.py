import sys
import os
from nose.tools import set_trace

# Add the parent directory to the path so that import statements will work
# the same in tests as in code.
this_dir = os.path.abspath(os.path.dirname(__file__))
parent = os.path.split(this_dir)[0]
sys.path.insert(0, parent)

# Having problems with the database not being initialized? This module is
# being imported twice through two different paths. Uncomment this
# set_trace() and see where the second one is happening.
#
# set_trace()
from testing import (
    DatabaseTest,
    DummyMetadataClient,
    DummyHTTPClient,
    _setup,
    _teardown,
)

class CoreDBInfo(object):
    connection = None
    engine = None
    transaction = None

DatabaseTest.DBInfo = CoreDBInfo

def setup():
    _setup(CoreDBInfo)

def teardown():
    _teardown(CoreDBInfo)
