import os
from nose.tools import eq_, set_trace
from sqlalchemy.orm.session import Session

from ..testing import DatabaseTest

from ..config import Configuration as BaseConfiguration
from ..model import (
    ConfigurationSetting,
    ExternalIntegration,
)

# Create a configuration object that the tests can run against without
# impacting the real configuration object.
class MockConfiguration(BaseConfiguration):
    instance = None

class TestConfiguration(DatabaseTest):

    Conf = MockConfiguration

    root_dir = os.path.join(os.path.split(__file__)[0], "..", "..")
    VERSION_FILENAME = os.path.join(root_dir, Conf.VERSION_FILENAME)

    def setup_method(self):
        super(TestConfiguration, self).setup_method()
        self.Conf.instance = dict()

    def teardown_method(self):
        if os.path.exists(self.VERSION_FILENAME):
            os.remove(self.VERSION_FILENAME)
        super(TestConfiguration, self).teardown_method()

    def create_version_file(self, content):
        with open(self.VERSION_FILENAME, 'w') as f:
            f.write(content)

    def test_app_version(self):
        self.Conf.instance = dict()

        # Without a .version file, the key is set to a null object.
        result = self.Conf.app_version()
        assert self.Conf.APP_VERSION in self.Conf.instance
        eq_(self.Conf.NO_APP_VERSION_FOUND, result)
        eq_(
            self.Conf.NO_APP_VERSION_FOUND,
            self.Conf.get(self.Conf.APP_VERSION)
        )

        # An empty .version file yields the same results.
        self.Conf.instance = dict()
        self.create_version_file(' \n')
        result = self.Conf.app_version()
        eq_(self.Conf.NO_APP_VERSION_FOUND, result)
        eq_(
            self.Conf.NO_APP_VERSION_FOUND,
            self.Conf.get(self.Conf.APP_VERSION)
        )

        # A .version file with content loads the content.
        self.Conf.instance = dict()
        self.create_version_file('ba.na.na')
        result = self.Conf.app_version()
        eq_('ba.na.na', result)
        eq_('ba.na.na', self.Conf.get(self.Conf.APP_VERSION))

    def test_load_cdns(self):
        """Test our ability to load CDN configuration from the database.
        """
        self._external_integration(
            protocol=ExternalIntegration.CDN,
            goal=ExternalIntegration.CDN_GOAL,
            settings = { self.Conf.CDN_MIRRORED_DOMAIN_KEY : "site.com",
                         ExternalIntegration.URL : "http://cdn/" }
        )

        self.Conf.load_cdns(self._db)

        integrations = self.Conf.instance[self.Conf.INTEGRATIONS]
        eq_({'site.com' : 'http://cdn/'}, integrations[ExternalIntegration.CDN])
        eq_(True, self.Conf.instance[self.Conf.CDNS_LOADED_FROM_DATABASE])

    def test_cdns_loaded_dynamically(self):
        # When you call cdns() on a Configuration object that was
        # never initialized, it creates a new database connection and
        # loads CDN configuration from the database. This lets
        # us avoid having to have a database connection handy to pass into
        # cdns().
        #
        # We can't do an end-to-end test, because any changes we
        # commit won't show up in the new connection (this test is
        # running inside a transaction that will be rolled back).
        #
        # But we can verify that load_cdns is called with a new
        # database connection.
        class Mock(MockConfiguration):
            @classmethod
            def load_cdns(cls, _db, config_instance=None):
                cls.called_with = (_db, config_instance)

        cdns = Mock.cdns()
        eq_({}, cdns)

        new_db, none = Mock.called_with
        assert new_db != self._db
        assert isinstance(new_db, Session)
        eq_(None, none)
