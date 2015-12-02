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
from ..core.app_server import (
    load_lending_policy
)
from ..core.model import (
    Patron
)
from ..core.lane import (
    Facets,
    Pagination,
)
import flask
from ..problem_details import *

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

    def test_load_facets_from_request(self):
        testapp = flask.Flask(__name__)
        with testapp.test_request_context('/?order=%s' % Facets.ORDER_TITLE):
            facets = self.controller.load_facets_from_request()
            eq_(Facets.ORDER_TITLE, facets.order)

        with testapp.test_request_context('/?order=bad_facet'):
            problemdetail = self.controller.load_facets_from_request()
            eq_(INVALID_INPUT.uri, problemdetail.uri)

    def test_load_pagination_from_request(self):
        testapp = flask.Flask(__name__)
        with testapp.test_request_context('/?size=50&after=10'):
            pagination = self.controller.load_pagination_from_request()
            eq_(50, pagination.size)
            eq_(10, pagination.offset)

        with testapp.test_request_context('/'):
            pagination = self.controller.load_pagination_from_request()
            eq_(Pagination.DEFAULT_SIZE, pagination.size)
            eq_(0, pagination.offset)

        with testapp.test_request_context('/?size=string'):
            pagination = self.controller.load_pagination_from_request()
            eq_(INVALID_INPUT.uri, pagination.uri)
            eq_("Invalid size: string", pagination.detail)

        with testapp.test_request_context('/?after=string'):
            pagination = self.controller.load_pagination_from_request()
            eq_(INVALID_INPUT.uri, pagination.uri)
            eq_("Invalid offset: string", pagination.detail)

        with testapp.test_request_context('/?size=5000'):
            pagination = self.controller.load_pagination_from_request()
            eq_(100, pagination.size)

    def test_load_licensepool(self):
        licensepool = self._licensepool(edition=None)
        loaded_licensepool = self.controller.load_licensepool(licensepool.data_source.name, licensepool.identifier.identifier)
        eq_(licensepool, loaded_licensepool)

        problem_detail = self.controller.load_licensepool("bad data source", licensepool.identifier.identifier)
        eq_(INVALID_INPUT.uri, problem_detail.uri)
        
        problem_detail = self.controller.load_licensepool(licensepool.data_source.name, "bad identifier")
        eq_(NO_LICENSES.uri, problem_detail.uri)

    def test_apply_borrowing_policy_when_holds_prohibited(self):
        
        patron = self.controller.authenticated_patron("5", "5555")
        with temp_config() as config:
            config['policies'][Configuration.HOLD_POLICY] = Configuration.HOLD_POLICY_HIDE
            work = self._work(with_license_pool=True)
            [pool] = work.license_pools
            pool.licenses_available = 0
            problem = self.controller.apply_borrowing_policy(
                patron, pool
            )
            eq_(FORBIDDEN_BY_POLICY.uri, problem.uri)

    def test_apply_borrowing_policy_for_audience_restriction(self):

        patron = self.controller.authenticated_patron("5", "5555")
        work = self._work(with_license_pool=True)
        [pool] = work.license_pools

        self.manager.lending_policy = load_lending_policy(
            {
                "60": {"audiences": ["Children"]}, 
                "152": {"audiences": ["Children"]}, 
                "62": {"audiences": ["Children"]}
            }
        )

        patron._external_type = '10'
        eq_(None, self.controller.apply_borrowing_policy(patron, pool))

        patron._external_type = '152'
        problem = self.controller.apply_borrowing_policy(patron, pool)
        eq_(FORBIDDEN_BY_POLICY.uri, problem.uri)
