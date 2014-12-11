import os
import sys
from nose.tools import set_trace
from model import production_session

class Script(object):

    @property
    def _db(self):
        return production_session()

    @property
    def data_directory(self):
        return self.required_environment_variable('DATA_DIRECTORY')

    def required_environment_variable(self, name):
        if not name in os.environ:
            print "Missing required environment variable: %s" % name
            sys.exit()
        return os.environ[name]

    def run(self):
        pass
