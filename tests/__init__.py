import sys, os
from nose.tools import set_trace

from core.testing import (
    DatabaseTest,
    package_setup,
)

package_setup()

def sample_data(filename, sample_data_dir):
    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", sample_data_dir)
    path = os.path.join(resource_path, filename)

    with open(path, "rb") as f:
        return f.read()
