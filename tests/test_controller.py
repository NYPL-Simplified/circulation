from nose.tools import (
    eq_,
    set_trace,
)
from . import DatabaseTest
from ..config import (
    Configuration,
    temp_config,
)
from ..controller import (
    CirculationManager,
    CirculationManagerController,
)
from ..core.model import (
    Patron
)
from ..problem_details import (
    INVALID_CREDENTIALS,
    EXPIRED_CREDENTIALS,
    NO_SUCH_LANE,
)

from ..lanes import make_lanes_default

class TestBaseController(DatabaseTest):

    def setup(self):
        super(TestBaseController, self).setup()
        with temp_config() as config:
            languages = Configuration.language_policy()
            languages[Configuration.LARGE_COLLECTION_LANGUAGES] = 'eng'
            languages[Configuration.SMALL_COLLECTION_LANGUAGES] = 'spa,chi'
            lanes = make_lanes_default(self._db)
            self.manager = CirculationManager(self._db, lanes=lanes, testing=True)
            self.controller = CirculationManagerController(self.manager)

    def test_authenticated_patron_invalid_credentials(self):
        value = self.controller.authenticated_patron("5", "1234")
        eq_(value, INVALID_CREDENTIALS)

    def test_authenticated_patron_expired_credentials(self):
        value = self.controller.authenticated_patron("0", "0000")
        eq_(value, EXPIRED_CREDENTIALS)

    def test_authenticated_patron_correct_credentials(self):
        value = self.controller.authenticated_patron("5", "5555")
        assert isinstance(value, Patron)

    def test_load_lane(self):
        eq_(self.manager, self.controller.load_lane(None, None))
        chinese = self.controller.load_lane('chi', None)
        eq_(None, chinese.name)
        eq_("Chinese", chinese.display_name)
        eq_(["chi"], chinese.languages)

        english_sf = self.controller.load_lane('eng', "Science Fiction")
        eq_("Science Fiction", english_sf.display_name)
        eq_(["eng"], english_sf.languages)

        # __ is converted to /
        english_thriller = self.controller.load_lane('eng', "Suspense__Thriller")
        eq_("Suspense/Thriller", english_thriller.name)

        # Unlike with Chinese, there is no lane that contains all English books.
        english = self.controller.load_lane('eng', None)
        eq_(english.uri, NO_SUCH_LANE.uri)

        no_such_language = self.controller.load_lane('o10', None)
        eq_(no_such_language.uri, NO_SUCH_LANE.uri)
        eq_("Unrecognized language key: o10", no_such_language.detail)

        no_such_lane = self.controller.load_lane('eng', 'No such lane')
        eq_("No such lane: No such lane", no_such_lane.detail)
