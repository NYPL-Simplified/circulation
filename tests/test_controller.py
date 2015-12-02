from nose.tools import (
    eq_,
    set_trace,
)
import os
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
from flask import url_for
from ..core.util.cdn import cdnify
import base64

class TestCirculationManager(CirculationManager):

    def cdn_url_for(self, view, *args, **kwargs):
        base_url = url_for(view, *args, **kwargs)
        return cdnify(base_url, "http://cdn/")

class ControllerTest(DatabaseTest):
    def setup(self):
        super(ControllerTest, self).setup()

        os.environ['TESTING'] = "True"
        from ..app import app
        del os.environ['TESTING']
        self.app = app

        self.valid_auth = 'Basic ' + base64.b64encode('200:2222')
        self.invalid_auth = 'Basic ' + base64.b64encode('200:2221')

        with temp_config() as config:
            languages = Configuration.language_policy()
            languages[Configuration.LARGE_COLLECTION_LANGUAGES] = 'eng'
            languages[Configuration.SMALL_COLLECTION_LANGUAGES] = 'spa,chi'
            lanes = make_lanes_default(self._db)
            self.manager = TestCirculationManager(self._db, lanes=lanes, testing=True)
            self.app.manager = self.manager
            self.controller = CirculationManagerController(self.manager)

class TestBaseController(ControllerTest):

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
        with self.app.test_request_context('/?order=%s' % Facets.ORDER_TITLE):
            facets = self.controller.load_facets_from_request()
            eq_(Facets.ORDER_TITLE, facets.order)

        with self.app.test_request_context('/?order=bad_facet'):
            problemdetail = self.controller.load_facets_from_request()
            eq_(INVALID_INPUT.uri, problemdetail.uri)

    def test_load_pagination_from_request(self):
        with self.app.test_request_context('/?size=50&after=10'):
            pagination = self.controller.load_pagination_from_request()
            eq_(50, pagination.size)
            eq_(10, pagination.offset)

        with self.app.test_request_context('/'):
            pagination = self.controller.load_pagination_from_request()
            eq_(Pagination.DEFAULT_SIZE, pagination.size)
            eq_(0, pagination.offset)

        with self.app.test_request_context('/?size=string'):
            pagination = self.controller.load_pagination_from_request()
            eq_(INVALID_INPUT.uri, pagination.uri)
            eq_("Invalid size: string", pagination.detail)

        with self.app.test_request_context('/?after=string'):
            pagination = self.controller.load_pagination_from_request()
            eq_(INVALID_INPUT.uri, pagination.uri)
            eq_("Invalid offset: string", pagination.detail)

        with self.app.test_request_context('/?size=5000'):
            pagination = self.controller.load_pagination_from_request()
            eq_(100, pagination.size)

    def test_load_licensepool(self):
        licensepool = self._licensepool(edition=None)
        loaded_licensepool = self.controller.load_licensepool(
            licensepool.data_source.name, licensepool.identifier.identifier
        )
        eq_(licensepool, loaded_licensepool)

        problem_detail = self.controller.load_licensepool("bad data source", licensepool.identifier.identifier)
        eq_(INVALID_INPUT.uri, problem_detail.uri)
        
        problem_detail = self.controller.load_licensepool(licensepool.data_source.name, "bad identifier")
        eq_(NO_LICENSES.uri, problem_detail.uri)

    def test_load_licensepooldelivery(self):
        licensepool = self._licensepool(edition=None, with_open_access_download=True)
        lpdm = licensepool.delivery_mechanisms[0]
        delivery = self.controller.load_licensepooldelivery(licensepool, lpdm.delivery_mechanism.id)
        eq_(lpdm, delivery)

        adobe_licensepool = self._licensepool(edition=None, with_open_access_download=False)
        problem_detail = self.controller.load_licensepooldelivery(adobe_licensepool, lpdm.delivery_mechanism.id)
        eq_(BAD_DELIVERY_MECHANISM.uri, problem_detail.uri)

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


class TestIndexController(ControllerTest):
    
    def test_simple_redirect(self):
        with temp_config() as config:
            config['policies'][Configuration.ROOT_LANE_POLICY] = None
            with self.app.test_request_context('/'):
                response = self.manager.index_controller()
                eq_(302, response.status_code)
                eq_("http://cdn/groups/", response.headers['location'])

    def test_authenticated_patron_root_lane(self):
        with temp_config() as config:
            config['policies'][Configuration.ROOT_LANE_POLICY] = { "2": ["eng", "Adult Fiction"] }
            config['policies'][Configuration.EXTERNAL_TYPE_REGULAR_EXPRESSION] = "^(.)"
            with self.app.test_request_context(
                "/", headers=dict(Authorization=self.invalid_auth)):
                response = self.manager.index_controller()
                eq_(401, response.status_code)

            with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
                response = self.manager.index_controller()
                eq_(302, response.status_code)
                eq_("http://cdn/groups/eng/Adult%20Fiction", response.headers['location'])

            config['policies'][Configuration.ROOT_LANE_POLICY] = { "2": None }
            with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
                response = self.manager.index_controller()
                eq_(302, response.status_code)
                eq_("http://cdn/groups/", response.headers['location'])

