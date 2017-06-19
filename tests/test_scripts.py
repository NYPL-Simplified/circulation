from nose.tools import (
    set_trace,
    eq_,
)

import contextlib
import datetime
import flask

from api.adobe_vendor_id import (
    AdobeVendorIDModel,
    AuthdataUtility,
)

from api.config import (
    temp_config,
    Configuration,
)

from core.lane import (
    Lane,
    Facets,
)

from core.model import (
    ConfigurationSetting,
    Credential,
    DataSource,
    get_one,
    Timestamp,
)

from . import (
    DatabaseTest,
)

from scripts import (
    AdobeAccountIDResetScript,
    CacheRepresentationPerLane,
    CacheFacetListsPerLane,
    InstanceInitializationScript,
    LoanReaperScript,
)

class TestAdobeAccountIDResetScript(DatabaseTest):

    def test_process_patron(self):
        patron = self._patron()
    
        # This patron has old-style and new-style Credentials that link
        # them to Adobe account IDs (hopefully the same ID, though that
        # doesn't matter here.
        def set_value(credential):
            credential.value = "a credential"

        # Data source doesn't matter -- even if it's incorrect, a Credential
        # of the appropriate type will be deleted.
        data_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # Create two Credentials that will be deleted and one that will be
        # left alone.
        for type in (AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE,
                     AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
                     "Some other type"
        ):

            credential = Credential.lookup(
                self._db, data_source, type, patron,
                set_value, True
            )

        eq_(3, len(patron.credentials))

        # Run the patron through the script.
        script = AdobeAccountIDResetScript(self._db)

        # A dry run does nothing.
        script.delete = False
        script.process_patron(patron)
        self._db.commit()
        eq_(3, len(patron.credentials))

        # Now try it for real.
        script.delete = True
        script.process_patron(patron)
        self._db.commit()
                
        # The two Adobe-related credentials are gone. The other one remains.
        [credential] = patron.credentials
        eq_("Some other type", credential.type)
    

class TestLaneScript(DatabaseTest):

    def setup(self):
        super(TestLaneScript, self).setup()
        base_url_setting = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY)
        base_url_setting.value = u'http://test-circulation-manager/'

    @contextlib.contextmanager
    def temp_config(self):
        """Create a temporary configuration with the bare-bones policies
        and integrations necessary to start up a script.
        """
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.LANGUAGE_POLICY : {
                    Configuration.LARGE_COLLECTION_LANGUAGES : 'eng',
                    Configuration.SMALL_COLLECTION_LANGUAGES : 'fre',
                }
            }
            yield config


class TestRepresentationPerLane(TestLaneScript):
   
    def test_language_filter(self):
        with self.temp_config() as config:
            script = CacheRepresentationPerLane(
                self._db, ["--language=fre", "--language=English", "--language=none", "--min-depth=0"],
                testing=True
            )
            eq_(['fre', 'eng'], script.languages)

            english_lane = Lane(self._default_library, self._str, languages=['eng'])
            eq_(True, script.should_process_lane(english_lane))

            no_english_lane = Lane(self._default_library, self._str, exclude_languages=['eng'])
            eq_(True, script.should_process_lane(no_english_lane))

            no_english_or_french_lane = Lane(
                self._default_library, self._str, exclude_languages=['eng', 'fre']
            )
            eq_(False, script.should_process_lane(no_english_or_french_lane))
            
    def test_max_and_min_depth(self):
        with self.temp_config() as config:
            script = CacheRepresentationPerLane(
                self._db, ["--max-depth=0", "--min-depth=0"],
                testing=True
            )
            eq_(0, script.max_depth)

            child = Lane(self._default_library, "sublane")
            parent = Lane(self._default_library, "parent", sublanes=[child])
            eq_(True, script.should_process_lane(parent))
            eq_(False, script.should_process_lane(child))

            script = CacheRepresentationPerLane(
                self._db, ["--min-depth=1"], testing=True
            )
            eq_(1, script.min_depth)
            eq_(False, script.should_process_lane(parent))
            eq_(True, script.should_process_lane(child))


class TestCacheFacetListsPerLane(TestLaneScript):

    def test_arguments(self):
        with self.temp_config() as config:
            script = CacheFacetListsPerLane(
                self._db, ["--order=title", "--order=added"],
                testing=True
            )
            eq_(['title', 'added'], script.orders)
            script = CacheFacetListsPerLane(
                self._db, ["--availability=all", "--availability=always"],
                testing=True
            )
            eq_(['all', 'always'], script.availabilities)

            script = CacheFacetListsPerLane(
                self._db, ["--collection=main", "--collection=full"],
                testing=True
            )
            eq_(['main', 'full'], script.collections)

            script = CacheFacetListsPerLane(
                self._db, ['--pages=1'], testing=True
            )
            eq_(1, script.pages)

    def test_process_lane(self):
        with self.temp_config() as config:
            lane = Lane(self._default_library, self._str)

            script = CacheFacetListsPerLane(
                self._db, ["--availability=all", "--availability=always",
                           "--collection=main", "--collection=full",
                           "--order=title", "--pages=1"],
                testing=True
            )
            with script.app.test_request_context("/"):
                flask.request.library = self._default_library
                cached_feeds = script.process_lane(lane)
                # 2 availabilities * 2 collections * 1 order * 1 page = 4 feeds
                eq_(4, len(cached_feeds))


class TestInstanceInitializationScript(DatabaseTest):

    def test_run(self):
        timestamp = get_one(self._db, Timestamp, service=u"Database Migration")
        eq_(None, timestamp)

        script = InstanceInitializationScript(_db=self._db)
        script.do_run(ignore_search=True)

        # It initializes the database.
        timestamp = get_one(self._db, Timestamp, service=u"Database Migration")
        assert timestamp


class TestLoanReaperScript(DatabaseTest):

    def test_reaping(self):

        # This patron stopped using the circulation manager a long time
        # ago.
        inactive_patron = self._patron()

        # This patron is still using the circulation manager.
        current_patron = self._patron()
        
        # We're going to give these patrons some loans and holds.
        edition, open_access = self._edition(
            with_license_pool=True, with_open_access_download=True)

        not_open_access_1 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.OVERDRIVE)
        not_open_access_2 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.BIBLIOTHECA)
        not_open_access_3 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.AXIS_360)
        not_open_access_4 = self._licensepool(edition,
            open_access=False, data_source_name=DataSource.ONECLICK)

        now = datetime.datetime.utcnow()
        a_long_time_ago = now - datetime.timedelta(days=1000)
        not_very_long_ago = now - datetime.timedelta(days=60)
        even_longer = now - datetime.timedelta(days=2000)
        the_future = now + datetime.timedelta(days=1)
        
        # This loan has expired.
        not_open_access_1.loan_to(
            inactive_patron, start=even_longer, end=a_long_time_ago
        )
        
        # This hold expired without ever becoming a loan (that we saw).
        not_open_access_2.on_hold_to(
            inactive_patron,
            start=even_longer,
            end=a_long_time_ago
        )
        
        # This hold has no end date and is older than a year.
        not_open_access_3.on_hold_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )
        
        # This loan has no end date and is older than 90 days.
        not_open_access_4.loan_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )
        
        # This loan has no end date, but it's for an open-access work.
        open_access_loan, ignore = open_access.loan_to(
            inactive_patron, start=a_long_time_ago, end=None,
        )

        # This loan has not expired yet.
        not_open_access_1.loan_to(
            current_patron, start=now, end=the_future
        )
        
        # This hold has not expired yet.
        not_open_access_2.on_hold_to(
            current_patron, start=now, end=the_future
        )

        # This loan has no end date but is pretty recent.
        not_open_access_3.loan_to(
            current_patron, start=not_very_long_ago, end=None
        )

        # This hold has no end date but is pretty recent.
        not_open_access_4.on_hold_to(
            current_patron, start=not_very_long_ago, end=None
        )
        
        eq_(3, len(inactive_patron.loans))
        eq_(2, len(inactive_patron.holds))

        eq_(2, len(current_patron.loans))
        eq_(2, len(current_patron.holds))

        # Now we fire up the loan reaper.
        script = LoanReaperScript(self._db)
        script.do_run()

        # All of the inactive patron's loans and holds have been reaped,
        # except for the open-access loan, which will never be reaped.
        eq_([open_access_loan], inactive_patron.loans)
        eq_([], inactive_patron.holds)

        # The active patron's loans and holds are unaffected, either
        # because they have not expired or because they have no known
        # expiration date and were created relatively recently.
        eq_(2, len(current_patron.loans))
        eq_(2, len(current_patron.holds))
