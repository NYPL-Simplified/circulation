# encoding: utf-8
from nose.tools import (
    eq_,
    set_trace,
)
import datetime

from .. import DatabaseTest
from config import Configuration
import lane
import model
from model import (
    CachedFeed,
    ConfigurationSetting,
    create,
    site_configuration_has_changed,
    Timestamp,
)

class TestSiteConfigurationHasChanged(DatabaseTest):

    class MockSiteConfigurationHasChanged(object):
        """Keep track of whether site_configuration_has_changed was
        ever called.
        """
        def __init__(self):
            self.was_called = False

        def run(self, _db):
            self.was_called = True
            site_configuration_has_changed(_db)

        def assert_was_called(self):
            "Assert that `was_called` is True, then reset it for the next assertion."
            assert self.was_called
            self.was_called = False

        def assert_was_not_called(self):
            assert not self.was_called

    def setup(self):
        super(TestSiteConfigurationHasChanged, self).setup()

        # Mock model.site_configuration_has_changed
        self.old_site_configuration_has_changed = model.listeners.site_configuration_has_changed
        self.mock = self.MockSiteConfigurationHasChanged()
        for module in model.listeners, lane:
            module.site_configuration_has_changed = self.mock.run

    def teardown(self):
        super(TestSiteConfigurationHasChanged, self).teardown()
        for module in model.listeners, lane:
            module.site_configuration_has_changed = self.old_site_configuration_has_changed

    def test_site_configuration_has_changed(self):
        """Test the site_configuration_has_changed() function and its
        effects on the Configuration object.
        """
        # The database configuration timestamp is initialized as part
        # of the default data. In that case, it happened during the
        # package_setup() for this test run.
        last_update = Configuration.site_configuration_last_update(self._db)

        timestamp_value = Timestamp.value(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        eq_(timestamp_value, last_update)

        # Now let's call site_configuration_has_changed().
        time_of_update = datetime.datetime.utcnow()
        site_configuration_has_changed(self._db, timeout=0)

        # The Timestamp has changed in the database.
        new_timestamp_value = Timestamp.value(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        assert new_timestamp_value > timestamp_value

        # The locally-stored last update value has been updated.
        new_last_update_time = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert new_last_update_time > last_update
        assert (new_last_update_time - time_of_update).total_seconds() < 1

        # Let's be sneaky and update the timestamp directly,
        # without calling site_configuration_has_changed(). This
        # simulates another process on a different machine calling
        # site_configuration_has_changed() -- they will know about the
        # change but we won't be informed.
        timestamp = Timestamp.stamp(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )

        # Calling Configuration.check_for_site_configuration_update
        # doesn't detect the change because by default we only go to
        # the database every ten minutes.
        eq_(new_last_update_time,
            Configuration.site_configuration_last_update(self._db))

        # Passing in a different timeout value forces the method to go
        # to the database and find the correct answer.
        newer_update = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert newer_update > last_update

        # It's also possible to change the timeout value through a
        # site-wide ConfigurationSetting
        ConfigurationSetting.sitewide(
            self._db, Configuration.SITE_CONFIGURATION_TIMEOUT
        ).value = 0
        timestamp = Timestamp.stamp(
            self._db, Configuration.SITE_CONFIGURATION_CHANGED, None
        )
        even_newer_update = Configuration.site_configuration_last_update(
            self._db, timeout=0
        )
        assert even_newer_update > newer_update

        # If ConfigurationSettings are updated twice within the
        # timeout period (default 1 second), the last update time is
        # only set once, to avoid spamming the Timestamp with updates.

        # The high site-wide value for 'timeout' saves this code. If we decided
        # that the timeout had expired and tried to check the
        # Timestamp, the code would crash because we're not passing
        # a database connection in.
        site_configuration_has_changed(None, timeout=100)

        # Nothing has changed -- how could it, with no database connection
        # to modify anything?
        eq_(even_newer_update,
            Configuration.site_configuration_last_update(self._db))

    # We don't test every event listener, but we do test one of each type.
    def test_configuration_relevant_lifecycle_event_updates_configuration(self):
        """When you create or modify a relevant item such as a
        ConfigurationSetting, site_configuration_has_changed is called.
        """
        ConfigurationSetting.sitewide(self._db, "setting").value = "value"
        self.mock.assert_was_called()

        ConfigurationSetting.sitewide(self._db, "setting").value = "value2"
        self.mock.assert_was_called()

    def test_lane_change_updates_configuration(self):
        """Verify that configuration-relevant changes work the same way
        in the lane module as they do in the model module.
        """
        lane = self._lane()
        self.mock.assert_was_called()

        lane.add_genre("Science Fiction")
        self.mock.assert_was_called()

    def test_configuration_relevant_collection_change_updates_configuration(self):
        """When you add a relevant item to a SQLAlchemy collection, such as
        adding a Collection to library.collections,
        site_configuration_has_changed is called.
        """

        # Creating a collection calls the method via an 'after_insert'
        # event on Collection.
        library = self._default_library
        collection = self._collection()
        self._db.commit()
        self.mock.assert_was_called()

        # Adding the collection to the library calls the method via
        # an 'append' event on Collection.libraries.
        library.collections.append(collection)
        self._db.commit()
        self.mock.assert_was_called()

        # Associating a CachedFeed with the library does _not_ call
        # the method, because nothing changed on the Library object and
        # we don't listen for 'append' events on Library.cachedfeeds.
        create(self._db, CachedFeed, type='page', pagination='',
               facets='', library=library)
        self._db.commit()
        self.mock.assert_was_not_called()
