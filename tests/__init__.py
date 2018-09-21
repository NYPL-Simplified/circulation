import sys, os
from nose.tools import set_trace

# Make sure core/ is in the path so that relative imports _within_ core
# don't need to mention 'core'.
bin_dir = os.path.split(__file__)[0]
core_dir = os.path.join(bin_dir, "..", "core")
sys.path.append(os.path.abspath(core_dir))

from core.testing import (
    DatabaseTest,
    package_setup,
)

package_setup()

def sample_data(filename, sample_data_dir):
    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", sample_data_dir)
    path = os.path.join(resource_path, filename)

    with open(path) as f:
        return f.read()
