import sys, os
from nose.tools import set_trace

from ..core.testing import (
    DatabaseTest,
    _setup,
    _teardown,
)

class CirculationDBInfo(object):
    connection = None
    engine = None
    transaction = None

DatabaseTest.DBInfo = CirculationDBInfo

def setup():
    _setup(CirculationDBInfo)

def teardown():
    _teardown(CirculationDBInfo)

