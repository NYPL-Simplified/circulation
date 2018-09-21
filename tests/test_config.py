import os
from nose.tools import eq_, set_trace

from testing import DatabaseTest

from config import Configuration as BaseConfiguration
from model import ConfigurationSetting


# Create a configuration object that the tests can run against without
# impacting the real configuration object.
class MockConfiguration(BaseConfiguration):
    instance = None

class TestConfiguration(DatabaseTest):

    Conf = MockConfiguration

    root_dir = os.path.join(os.path.split(__file__)[0], "..", "..")
    VERSION_FILENAME = os.path.join(root_dir, Conf.VERSION_FILENAME)

    def teardown(self):
        if os.path.exists(self.VERSION_FILENAME):
            os.remove(self.VERSION_FILENAME)
        super(TestConfiguration, self).teardown()

    def create_version_file(self, content):
        with open(self.VERSION_FILENAME, 'w') as f:
            f.write(content)

    def test_app_version(self):
        self.Conf._instance = dict()

        # Without a .version file, the key is set to a null object.
        result = self.Conf.app_version()
        assert self.Conf.APP_VERSION in self.Conf.instance
        eq_(self.Conf.NO_APP_VERSION_FOUND, result)
        eq_(
            self.Conf.NO_APP_VERSION_FOUND,
            self.Conf.get(self.Conf.APP_VERSION)
        )

        # An empty .version file yields the same results.
        self.Conf._instance = dict()
        self.create_version_file(' \n')
        result = self.Conf.app_version()
        eq_(self.Conf.NO_APP_VERSION_FOUND, result)
        eq_(
            self.Conf.NO_APP_VERSION_FOUND,
            self.Conf.get(self.Conf.APP_VERSION)
        )

        # A .version file with content loads the content.
        self.Conf._instance = dict()
        self.create_version_file('ba.na.na')
        result = self.Conf.app_version()
        eq_('ba.na.na', result)
        eq_('ba.na.na', self.Conf.get(self.Conf.APP_VERSION))
