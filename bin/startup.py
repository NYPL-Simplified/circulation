import os
from os.path import dirname
from nose.importer import Importer
bin_dir = os.path.split(__file__)[0]
component_dir = os.path.join(bin_dir, "..", "..")
importer = Importer()

# Load the 'core' module as though this script were being run from
# the parent component (either circulation or metadata).
importer.importFromDir(component_dir, 'core')
