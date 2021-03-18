
# This is kind of janky, but we import the session fixture
# into these tests here. Plugins need absolute import paths
# and we don't have a package structure that gives us a reliable
# import path, so we construct one.
# todo: reorg core file structure so we have a reliable package name
from os.path import abspath, dirname, basename

# Pull in the session_fixture defined in core/testing.py
# which does the database setup and initialization
pytest_plugins = ["{}.testing".format(basename(dirname(dirname(abspath(__file__)))))]
