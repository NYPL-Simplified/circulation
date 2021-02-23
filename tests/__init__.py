import sys
import os

# NOTE: Don't be tempted to change sys.path to simplify imports.
# Those imports will mean something different in circulation and
# metadata, which will stop the applications that use core from
# working.

# Having problems with the database not being initialized? This module is
# being imported twice through two different paths. Uncomment this
# set_trace() and see where the second one is happening.
#
# set_trace()
from ..testing import (
    DatabaseTest,
    DummyMetadataClient,
    DummyHTTPClient,
)
