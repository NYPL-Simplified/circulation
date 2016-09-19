# encoding=utf8
from nose.tools import (
    eq_,
    set_trace,
)
from contextlib import contextmanager
import os
import datetime
import re
from wsgiref.handlers import format_date_time
from time import mktime

import flask
from flask import url_for
from flask_sqlalchemy_session import current_session

from . import DatabaseTest
from api.config import (
    Configuration,
    temp_config,
)
from collections import Counter
from api.controller import (
    CirculationManager,
    CirculationManagerController,
)
from core.app_server import (
    load_lending_policy
)
from core.metadata_layer import Metadata
from core.model import (
    Annotation,
    Patron,
    DeliveryMechanism,
    Representation,
    Loan,
    Hold,
    DataSource,
    Edition,
    Identifier,
    Complaint,
    SessionManager,
    CachedFeed,
    Work,
    CirculationEvent,
    LicensePoolDeliveryMechanism,
    RightsStatus,
    get_one,
    create,
)
from core.lane import (
    Facets,
    Pagination,
)
from core.problem_details import *
from core.util.problem_detail import ProblemDetail
from core.util.http import RemoteIntegrationException
from core.testing import DummyHTTPClient

from api.problem_details import *
from api.circulation_exceptions import *
from api.circulation import (
    HoldInfo,
    LoanInfo,
    FulfillmentInfo,
)
from api.novelist import MockNoveListAPI

from api.lanes import make_lanes_default
from core.util.cdn import cdnify
import base64
import feedparser
from core.opds import (
    AcquisitionFeed,
)
from core.util.opds_writer import (    
    OPDSFeed,
)
from api.opds import CirculationManagerAnnotator
from api.annotations import AnnotationWriter
from api.admin.oauth import DummyGoogleClient
from lxml import etree
import random
import json
import urllib
from core.analytics import Analytics

class TestCirculationManager(CirculationManager):

    def cdn_url_for(self, view, *args, **kwargs):
        base_url = url_for(view, *args, **kwargs)
        return cdnify(base_url, {"": "http://cdn/"})

class ControllerTest(DatabaseTest):
    """A test that requires a functional app server."""

    valid_auth = 'Basic ' + base64.b64encode('200:2222')
    invalid_auth = 'Basic ' + base64.b64encode('200:2221')

    def setup(self, _db=None):
        super(ControllerTest, self).setup()

        _db = _db or self._db
        os.environ['AUTOINITIALIZE'] = "False"
        from api.app import app
        self.app = app
        del os.environ['AUTOINITIALIZE']

        # PRESERVE_CONTEXT_ON_EXCEPTION needs to be off in tests
        # to prevent one test failure from breaking later tests as well.
        # When used with flask's test_request_context, exceptions
        # from previous tests wuold cause flask to roll back the db
        # when you entered a new request context, deleting rows that
        # were created in the test setup.
        app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = False

        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.AUTHENTICATION_POLICY : "Millenium",
                Configuration.LANGUAGE_POLICY : {
                    Configuration.LARGE_COLLECTION_LANGUAGES : 'eng',
                    Configuration.SMALL_COLLECTION_LANGUAGES : 'spa,chi',
                }
            }
            config[Configuration.INTEGRATIONS] = {
                Configuration.CIRCULATION_MANAGER_INTEGRATION : {
                    "url": 'http://test-circulation-manager/'
                }
            }
            lanes = make_lanes_default(_db)
            self.manager = TestCirculationManager(
                _db, lanes=lanes, testing=True
            )
            app.manager = self.manager
            self.controller = CirculationManagerController(self.manager)

class CirculationControllerTest(ControllerTest):

    def setup(self):
        super(CirculationControllerTest, self).setup()

        # Create two English books and a French book.
        self.english_1 = self._work(
            "Quite British", "John Bull", language="eng", fiction=True,
            with_open_access_download=True
        )
        self.english_2 = self._work(
            "Totally American", "Uncle Sam", language="eng", fiction=False,
            with_open_access_download=True
        )
        self.french_1 = self._work(
            u"Très Français", "Marianne", language="fre", fiction=False,
            with_open_access_download=True
        )



class TestBaseController(CirculationControllerTest):

    def test_unscoped_session(self):
        """Compare to TestScopedSession.test_scoped_session to see
        how database sessions will be handled in production.
        """
        # Both requests used the self._db session used by most unit tests.
        with self.app.test_request_context("/"):
            response1 = self.manager.index_controller()
            eq_(self.app.manager._db, self._db)

        with self.app.test_request_context("/"):
            response2 = self.manager.index_controller()
            eq_(self.app.manager._db, self._db)

    def test_authenticated_patron_invalid_credentials(self):
        value = self.controller.authenticated_patron(dict(username="5", password="1234"))
        eq_(value, INVALID_CREDENTIALS)

    def test_authenticated_patron_expired_credentials(self):
        value = self.controller.authenticated_patron(dict(username="0", password="0000"))
        eq_(value, EXPIRED_CREDENTIALS)

    def test_authenticated_patron_correct_credentials(self):
        value = self.controller.authenticated_patron(dict(username="5", password="5555"))
        assert isinstance(value, Patron)


    def test_authentication_sends_proper_headers(self):

        # Make sure the realm header has quotes around the realm name.  
        # Without quotes, some iOS versions don't recognize the header value.
        
        with temp_config() as config:
            config[Configuration.INTEGRATIONS] = {
                Configuration.CIRCULATION_MANAGER_INTEGRATION: {
                    Configuration.URL: "http://url"
                }
            }

            with self.app.test_request_context("/"):
                response = self.controller.authenticate()
                eq_(response.headers['WWW-Authenticate'], u'Basic realm="Library card"')

            with self.app.test_request_context("/", headers={"X-Requested-With": "XMLHttpRequest"}):
                response = self.controller.authenticate()
                eq_(None, response.headers.get("WWW-Authenticate"))

    def test_load_lane(self):
        eq_(self.manager.top_level_lane, self.controller.load_lane(None, None))
        chinese = self.controller.load_lane('chi', None)
        eq_("Chinese", chinese.name)
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

    def test_load_licensepool(self):
        licensepool = self._licensepool(edition=None)
        loaded_licensepool = self.controller.load_licensepool(
            licensepool.data_source.name, licensepool.identifier.type, licensepool.identifier.identifier
        )
        eq_(licensepool, loaded_licensepool)

        problem_detail = self.controller.load_licensepool("bad data source", licensepool.identifier.type, licensepool.identifier.identifier)
        eq_(INVALID_INPUT.uri, problem_detail.uri)

        problem_detail = self.controller.load_licensepool(licensepool.data_source.name, "bad identifier type", licensepool.identifier.identifier)
        eq_(NO_LICENSES.uri, problem_detail.uri)
        expect = u"The item you're asking about (bad identifier type/%s) isn't in this collection." % licensepool.identifier.identifier
        eq_(expect, problem_detail.detail)
        
        problem_detail = self.controller.load_licensepool(licensepool.data_source.name, licensepool.identifier.type, "bad identifier")
        eq_(NO_LICENSES.uri, problem_detail.uri)

    def test_load_licensepooldelivery(self):

        licensepool = self._licensepool(edition=None, with_open_access_download=True)

        # Set a delivery mechanism that we won't be looking up, so we
        # can demonstrate that we find the right match thanks to more
        # than random chance.
        licensepool.set_delivery_mechanism(
            Representation.MOBI_MEDIA_TYPE, None, None, None
        )

        # If there is one matching delivery mechanism that matches the
        # request, we load it.
        lpdm = licensepool.delivery_mechanisms[0]
        delivery = self.controller.load_licensepooldelivery(
            licensepool, lpdm.delivery_mechanism.id
        )
        eq_(lpdm, delivery)

        # If there are multiple matching delivery mechanisms (that is,
        # multiple ways of getting a book with the same media type and
        # DRM scheme) we pick one arbitrarily.
        new_lpdm, is_new = create(
            self._db, 
            LicensePoolDeliveryMechanism,
            license_pool=licensepool, 
            delivery_mechanism=lpdm.delivery_mechanism,
        )        
        eq_(True, is_new)

        eq_(new_lpdm.delivery_mechanism, lpdm.delivery_mechanism)
        underlying_mechanism = lpdm.delivery_mechanism

        delivery = self.controller.load_licensepooldelivery(
            licensepool, lpdm.delivery_mechanism.id
        )

        # We don't know which LicensePoolDeliveryMechanism this is, 
        # but we know it's one of the matches.
        eq_(underlying_mechanism, delivery.delivery_mechanism)

        # If there is no matching delivery mechanism, we return a
        # problem detail.
        adobe_licensepool = self._licensepool(
            edition=None, with_open_access_download=False
        )
        problem_detail = self.controller.load_licensepooldelivery(
            adobe_licensepool, lpdm.delivery_mechanism.id
        )
        eq_(BAD_DELIVERY_MECHANISM.uri, problem_detail.uri)

    def test_apply_borrowing_policy_when_holds_prohibited(self):
        
        patron = self.controller.authenticated_patron(dict(username="5", password="5555"))
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.HOLD_POLICY : Configuration.HOLD_POLICY_HIDE
            }
            work = self._work(with_license_pool=True)
            [pool] = work.license_pools
            pool.licenses_available = 0
            
            # This is an open-access work, so there's no problem.
            eq_(True, pool.open_access)

            # Open-access books still be borrowed even if they have no
            # 'licenses' available.
            problem = self.controller.apply_borrowing_policy(
                patron, pool
            )
            eq_(None, problem)

            # But if it weren't an open-access work, there'd be a big
            # problem.
            pool.open_access = False
            problem = self.controller.apply_borrowing_policy(
                patron, pool
            )
            eq_(FORBIDDEN_BY_POLICY.uri, problem.uri)

    def test_apply_borrowing_policy_for_audience_restriction(self):

        patron = self.controller.authenticated_patron(dict(username="5", password="5555"))
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



class TestIndexController(CirculationControllerTest):
    
    def test_simple_redirect(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.ROOT_LANE_POLICY: None
            }
            with self.app.test_request_context('/'):
                response = self.manager.index_controller()
                eq_(302, response.status_code)
                eq_("http://cdn/groups/", response.headers['location'])

    def test_authenticated_patron_root_lane(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.ROOT_LANE_POLICY : { "2": ["eng", "Adult Fiction"]},
                Configuration.EXTERNAL_TYPE_REGULAR_EXPRESSION : "^(.)",
            }
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


class TestAccountController(CirculationControllerTest):

    def test_patron_info_no_username(self):
        with self.app.test_request_context(
            "/", headers=dict(Authorization=self.valid_auth)):
            account_info = json.loads(self.manager.accounts.account())
            eq_(None, account_info.get('username'))
            eq_("200", account_info.get('barcode'))
            
    def test_patron_info_with_username(self):
        auth = 'Basic ' + base64.b64encode('0:2222')
        with self.app.test_request_context(
            "/", headers=dict(Authorization=auth)):
            account_info = json.loads(self.manager.accounts.account())
            eq_("alice", account_info.get('username'))
            eq_("0", account_info.get('barcode'))


class TestLoanController(CirculationControllerTest):
    def setup(self):
        super(TestLoanController, self).setup()
        self.pool = self.english_1.license_pools[0]
        self.mech2 = self.pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY, None
        )
        self.edition = self.pool.presentation_edition
        self.data_source = self.edition.data_source
        self.identifier = self.edition.primary_identifier

    def test_borrow_success(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.borrow(
                self.data_source.name, self.identifier.type, self.identifier.identifier)

            # A loan has been created for this license pool.
            loan = get_one(self._db, Loan, license_pool=self.pool)
            assert loan != None
            # The loan has yet to be fulfilled.
            eq_(None, loan.fulfillment)

            # We've been given an OPDS feed with one entry, which tells us how 
            # to fulfill the license.
            eq_(201, response.status_code)
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            fulfillment_links = [x['href'] for x in entry['links']
                                if x['rel'] == OPDSFeed.ACQUISITION_REL]
            [mech1, mech2] = sorted(
                self.pool.delivery_mechanisms, 
                key=lambda x: x.delivery_mechanism.default_client_can_fulfill
            )

            fulfillable_mechanism = mech2

            expects = [url_for('fulfill', data_source=self.data_source.name,
                               identifier_type=self.identifier.type,
                               identifier=self.identifier.identifier, 
                               mechanism_id=mech.delivery_mechanism.id,
                               _external=True) for mech in [mech1, mech2]]
            eq_(set(expects), set(fulfillment_links))

            http = DummyHTTPClient()

            # Now let's try to fulfill the loan.
            http.queue_response(200, content="I am an ACSM file")

            response = self.manager.loans.fulfill(
                self.data_source.name, self.identifier.type, self.identifier.identifier,
                fulfillable_mechanism.delivery_mechanism.id, do_get=http.do_get
            )
            eq_(200, response.status_code)
            eq_(["I am an ACSM file"],
                response.response)
            eq_(http.requests, [fulfillable_mechanism.resource.url])

            # The mechanism we used has been registered with the loan.
            eq_(fulfillable_mechanism, loan.fulfillment)

            # Now that we've set a mechanism, we can fulfill the loan
            # again without specifying a mechanism.
            http.queue_response(200, content="I am an ACSM file")

            response = self.manager.loans.fulfill(
                self.data_source.name, self.identifier.type, self.identifier.identifier, do_get=http.do_get
            )
            eq_(200, response.status_code)
            eq_(["I am an ACSM file"],
                response.response)
            eq_(http.requests, [fulfillable_mechanism.resource.url, fulfillable_mechanism.resource.url])

            # But we can't use some other mechanism -- we're stuck with
            # the first one we chose.
            response = self.manager.loans.fulfill(
                self.data_source.name, self.identifier.type, self.identifier.identifier,
                mech1.delivery_mechanism.id
            )

            eq_(409, response.status_code)
            assert "You already fulfilled this loan as application/epub+zip (DRM-free), you can't also do it as application/pdf (DRM-free)" in response.detail

            # If the remote server fails, we get a problem detail.
            def doomed_get(url, headers, **kwargs):
                raise RemoteIntegrationException("fulfill service", "Error!")

            response = self.manager.loans.fulfill(
                self.data_source.name, self.identifier.type, self.identifier.identifier,
                do_get=doomed_get
            )
            assert isinstance(response, ProblemDetail)
            eq_(502, response.status_code)

    def test_borrow_and_fulfill_with_streaming_delivery_mechanism(self):
        # Create a pool with a streaming delivery mechanism
        work = self._work(with_license_pool=True, with_open_access_download=False)
        edition = work.presentation_edition
        pool = work.license_pools[0]
        pool.open_access = False
        streaming_mech = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE, DeliveryMechanism.OVERDRIVE_DRM,
            RightsStatus.IN_COPYRIGHT, None
        )
        data_source = edition.data_source
        identifier = edition.primary_identifier

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            self.manager.circulation.queue_checkout(
                pool,
                LoanInfo(
                    pool.identifier.type,
                    pool.identifier.identifier,
                    datetime.datetime.utcnow(),
                    datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                )
            )
            response = self.manager.loans.borrow(
                data_source.name, identifier.type, identifier.identifier)

            # A loan has been created for this license pool.
            loan = get_one(self._db, Loan, license_pool=pool)
            assert loan != None
            # The loan has yet to be fulfilled.
            eq_(None, loan.fulfillment)

            # We've been given an OPDS feed with two delivery mechanisms, which tell us how 
            # to fulfill the license.
            eq_(201, response.status_code)
            feed = feedparser.parse(response.get_data())
            [entry] = feed['entries']
            fulfillment_links = [x['href'] for x in entry['links']
                                if x['rel'] == OPDSFeed.ACQUISITION_REL]
            [mech1, mech2] = sorted(
                pool.delivery_mechanisms, 
                key=lambda x: x.delivery_mechanism.is_streaming
            )

            streaming_mechanism = mech2

            expects = [url_for('fulfill', data_source=data_source.name,
                               identifier_type=identifier.type,
                               identifier=identifier.identifier, 
                               mechanism_id=mech.delivery_mechanism.id,
                               _external=True) for mech in [mech1, mech2]]
            eq_(set(expects), set(fulfillment_links))

            # Now let's try to fulfill the loan using the streaming mechanism.
            self.manager.circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                    None,
                    None,
                )
            )
            response = self.manager.loans.fulfill(
                data_source.name, identifier.type, identifier.identifier,
                streaming_mechanism.delivery_mechanism.id
            )
            
            # We get an OPDS entry.
            eq_(200, response.status_code)
            opds_entries = feedparser.parse(response.response[0])['entries']
            eq_(1, len(opds_entries))
            links = opds_entries[0]['links']
        
            # The entry includes one fulfill link.
            fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
            eq_(1, len(fulfill_links))

            eq_(Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                fulfill_links[0]['type'])
            eq_("http://streaming-content-link", fulfill_links[0]['href'])


            # The mechanism has not been set, since fulfilling a streaming
            # mechanism does not lock in the format.
            eq_(None, loan.fulfillment)

            # We can still use the other mechanism too.
            http = DummyHTTPClient()
            http.queue_response(200, content="I am an ACSM file")

            self.manager.circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://other-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE,
                    None,
                    None,
                ),
            )
            response = self.manager.loans.fulfill(
                data_source.name, identifier.type, identifier.identifier,
                mech1.delivery_mechanism.id, do_get=http.do_get
            )
            eq_(200, response.status_code)

            # Now the fulfillment has been set to the other mechanism.
            eq_(mech1, loan.fulfillment)

            # But we can still fulfill the streaming mechanism again.
            self.manager.circulation.queue_fulfill(
                pool,
                FulfillmentInfo(
                    pool.identifier.type,
                    pool.identifier.identifier,
                    "http://streaming-content-link",
                    Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                    None,
                    None,
                )
            )

            response = self.manager.loans.fulfill(
                data_source.name, identifier.type, identifier.identifier,
                streaming_mechanism.delivery_mechanism.id
            )
            eq_(200, response.status_code)
            opds_entries = feedparser.parse(response.response[0])['entries']
            eq_(1, len(opds_entries))
            links = opds_entries[0]['links']
        
            fulfill_links = [link for link in links if link['rel'] == "http://opds-spec.org/acquisition"]
            eq_(1, len(fulfill_links))

            eq_(Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
                fulfill_links[0]['type'])
            eq_("http://streaming-content-link", fulfill_links[0]['href'])


    def test_borrow_nonexistent_delivery_mechanism(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            response = self.manager.loans.borrow(
                self.data_source.name, self.identifier.type, self.identifier.identifier,
                -100
            )
            eq_(BAD_DELIVERY_MECHANISM, response) 

    def test_borrow_creates_hold_when_no_available_copies(self):
        threem_edition, pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = self._work(
            presentation_edition=threem_edition,
        )
        pool.licenses_available = 0
        pool.open_access = False

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            self.manager.circulation.queue_checkout(
                pool, NoAvailableCopies()
            )
            self.manager.circulation.queue_hold(
                pool,
                HoldInfo(
                    pool.identifier.type,
                    pool.identifier.identifier,
                    datetime.datetime.utcnow(),
                    datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                    1,
                )
            )
            response = self.manager.loans.borrow(
                DataSource.THREEM, pool.identifier.type, pool.identifier.identifier)
            eq_(201, response.status_code)
            
            # A hold has been created for this license pool.
            hold = get_one(self._db, Hold, license_pool=pool)
            assert hold != None

    def test_borrow_creates_local_hold_if_remote_hold_exists(self):
        """We try to check out a book, but turns out we already have it 
        on hold.
        """
        threem_edition, pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = self._work(
            presentation_edition=threem_edition,
        )
        pool.licenses_available = 0
        pool.open_access = False

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            self.manager.circulation.queue_checkout(
                pool, AlreadyOnHold()
            )
            self.manager.circulation.queue_hold(
                pool, HoldInfo(
                    pool.identifier.type,
                    pool.identifier.identifier,
                    datetime.datetime.utcnow(),
                    datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                    1,
                )
            )
            response = self.manager.loans.borrow(
                DataSource.THREEM, pool.identifier.type, pool.identifier.identifier)
            eq_(201, response.status_code)

            # A hold has been created for this license pool.
            hold = get_one(self._db, Hold, license_pool=pool)
            assert hold != None

    def test_borrow_fails_when_work_not_present_on_remote(self):
         threem_edition, pool = self._edition(
             with_open_access_download=False,
             data_source_name=DataSource.THREEM,
             identifier_type=Identifier.THREEM_ID,
             with_license_pool=True,
         )
         threem_book = self._work(
             presentation_edition=threem_edition,
         )
         pool.licenses_available = 1
         pool.open_access = False

         with self.app.test_request_context(
                 "/", headers=dict(Authorization=self.valid_auth)):
             self.manager.loans.authenticated_patron_from_request()
             self.manager.circulation.queue_checkout(
                 pool, NotFoundOnRemote()
             )
             response = self.manager.loans.borrow(
                 DataSource.THREEM, pool.identifier.type, pool.identifier.identifier)
             eq_(404, response.status_code)
             eq_("http://librarysimplified.org/terms/problem/not-found-on-remote", response.uri)

    def test_revoke_loan(self):
         with self.app.test_request_context(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             loan, newly_created = self.pool.loan_to(patron)

             self.manager.circulation.queue_checkin(self.pool, True)

             response = self.manager.loans.revoke(self.pool.data_source.name, self.pool.identifier.type, self.pool.identifier.identifier)

             eq_(200, response.status_code)
             
    def test_revoke_hold(self):
         with self.app.test_request_context(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             hold, newly_created = self.pool.on_hold_to(patron, position=0)

             self.manager.circulation.queue_release_hold(self.pool, True)

             response = self.manager.loans.revoke(self.pool.data_source.name, self.pool.identifier.type, self.pool.identifier.identifier)

             eq_(200, response.status_code)

    def test_revoke_hold_nonexistent_licensepool(self):
         with self.app.test_request_context(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             response = self.manager.loans.revoke(
                 "No such data source", "No such identifier type", "No such identifier"
             )
             assert isinstance(response, ProblemDetail)
             eq_(INVALID_INPUT.uri, response.uri)

    def test_hold_fails_when_patron_is_at_hold_limit(self):
        edition, pool = self._edition(with_license_pool=True)
        pool.open_access = False
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            self.manager.circulation.queue_checkout(
                pool, NoAvailableCopies()
            )
            self.manager.circulation.queue_hold(
                pool, PatronHoldLimitReached()
            )
            response = self.manager.loans.borrow(
                pool.data_source.name, pool.identifier.type, 
                pool.identifier.identifier
            )
            assert isinstance(response, ProblemDetail)
            eq_(HOLD_LIMIT_REACHED.uri, response.uri)

    def test_borrow_fails_with_outstanding_fines(self):
        threem_edition, pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = self._work(
            presentation_edition=threem_edition,
        )
        pool.open_access = False

        # Patron with $1.00 fine
        auth = 'Basic ' + base64.b64encode('5:5555')
        
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.MAX_OUTSTANDING_FINES : "$0.50"
            }

            with self.app.test_request_context(
                    "/", headers=dict(Authorization=auth)):
                self.manager.loans.authenticated_patron_from_request()
                response = self.manager.loans.borrow(
                    DataSource.THREEM, pool.identifier.type, pool.identifier.identifier)
                
                eq_(403, response.status_code)
                eq_(OUTSTANDING_FINES.uri, response.uri)
                assert "outstanding fines" in response.detail
                assert "$1.00" in response.detail

        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.MAX_OUTSTANDING_FINES : "$20.00"
            }

            with self.app.test_request_context(
                    "/", headers=dict(Authorization=auth)):
                self.manager.loans.authenticated_patron_from_request()

                self.manager.circulation.queue_checkout(
                    pool,
                    LoanInfo(
                        pool.identifier.type,
                        pool.identifier.identifier,
                        datetime.datetime.utcnow(),
                        datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
                    )
                )
                response = self.manager.loans.borrow(
                    DataSource.THREEM, pool.identifier.type, pool.identifier.identifier)
                
                eq_(201, response.status_code)

    def test_3m_cant_revoke_hold_if_reserved(self):
         threem_edition, pool = self._edition(
             with_open_access_download=False,
             data_source_name=DataSource.THREEM,
             identifier_type=Identifier.THREEM_ID,
             with_license_pool=True,
         )
         threem_book = self._work(
             presentation_edition=threem_edition,
         )
         pool.open_access = False

         with self.app.test_request_context(
                 "/", headers=dict(Authorization=self.valid_auth)):
             patron = self.manager.loans.authenticated_patron_from_request()
             hold, newly_created = pool.on_hold_to(patron, position=0)
             response = self.manager.loans.revoke(pool.data_source.name, pool.identifier.type, pool.identifier.identifier)
             eq_(400, response.status_code)
             eq_(CANNOT_RELEASE_HOLD.uri, response.uri)
             eq_("Cannot release a hold once it enters reserved state.", response.detail)

    def test_active_loans(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()
            assert not "<entry>" in response.data
            assert response.headers['Cache-Control'].startswith('private,')

        overdrive_edition, overdrive_pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        overdrive_book = self._work(
            presentation_edition=overdrive_edition,
        )
        overdrive_pool.open_access = False

        threem_edition, threem_pool = self._edition(
            with_open_access_download=False,
            data_source_name=DataSource.THREEM,
            identifier_type=Identifier.THREEM_ID,
            with_license_pool=True,
        )
        threem_book = self._work(
            presentation_edition=threem_edition,
        )
        threem_pool.licenses_available = 0
        threem_pool.open_access = False
        
        self.manager.circulation.add_remote_loan(
            overdrive_pool.identifier.type,
            overdrive_pool.identifier.identifier,
            datetime.datetime.utcnow(),
            datetime.datetime.utcnow() + datetime.timedelta(seconds=3600)
        )
        self.manager.circulation.add_remote_hold(
            threem_pool.identifier.type,
            threem_pool.identifier.identifier,
            datetime.datetime.utcnow(),
            datetime.datetime.utcnow() + datetime.timedelta(seconds=3600),
            0,
        )

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            patron = self.manager.loans.authenticated_patron_from_request()
            response = self.manager.loans.sync()

            feed = feedparser.parse(response.data)
            entries = feed['entries']

            overdrive_entry = [entry for entry in entries if entry['title'] == overdrive_book.title][0]
            threem_entry = [entry for entry in entries if entry['title'] == threem_book.title][0]

            eq_(overdrive_entry['opds_availability']['status'], 'available')
            eq_(threem_entry['opds_availability']['status'], 'ready')
            
            overdrive_links = overdrive_entry['links']
            fulfill_link = [x for x in overdrive_links if x['rel'] == 'http://opds-spec.org/acquisition'][0]['href']
            revoke_link = [x for x in overdrive_links if x['rel'] == OPDSFeed.REVOKE_LOAN_REL][0]['href']
            threem_links = threem_entry['links']
            borrow_link = [x for x in threem_links if x['rel'] == 'http://opds-spec.org/acquisition/borrow'][0]['href']
            threem_revoke_links = [x for x in threem_links if x['rel'] == OPDSFeed.REVOKE_LOAN_REL]

            assert urllib.quote("%s/%s/%s/fulfill" % (overdrive_pool.data_source.name, overdrive_pool.identifier.type, overdrive_pool.identifier.identifier)) in fulfill_link
            assert urllib.quote("%s/%s/%s/revoke" % (overdrive_pool.data_source.name, overdrive_pool.identifier.type, overdrive_pool.identifier.identifier)) in revoke_link
            assert urllib.quote("%s/%s/%s/borrow" % (threem_pool.data_source.name, threem_pool.identifier.type, threem_pool.identifier.identifier)) in borrow_link
            eq_(0, len(threem_revoke_links))

            links = feed['feed']['links']
            account_links = [link for link in links if link['rel'] == 'http://librarysimplified.org/terms/rel/account']
            eq_(1, len(account_links))
            assert 'me' in account_links[0]['href']

class TestAnnotationController(CirculationControllerTest):
    def setup(self):
        super(TestAnnotationController, self).setup()
        self.pool = self.english_1.license_pools[0]
        self.edition = self.pool.presentation_edition
        self.data_source = self.edition.data_source
        self.identifier = self.edition.primary_identifier

    def test_get_empty_container(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.loans.authenticated_patron_from_request()
            response = self.manager.annotations.container()
            eq_(200, response.status_code)

            # We've been given an annotation container with no items.
            container = json.loads(response.data)
            eq_([], container['first']['items'])
            eq_(0, container['total'])

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'POST']:
                assert method in allow_header

            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Accept-Post'])
            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Content-Type'])
            eq_('W/""', response.headers['ETag'])

    def test_get_container_with_item(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True
        annotation.timestamp = datetime.datetime.now()

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.container()
            eq_(200, response.status_code)

            # We've been given an annotation container with one item.
            container = json.loads(response.data)
            eq_(1, container['total'])
            item = container['first']['items'][0]
            eq_(annotation.motivation, item['motivation'])

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'POST']:
                assert method in allow_header

            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Accept-Post'])
            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Content-Type'])
            expected_etag = 'W/"%s"' % annotation.timestamp
            eq_(expected_etag, response.headers['ETag'])
            expected_time = format_date_time(mktime(annotation.timestamp.timetuple()))
            eq_(expected_time, response.headers['Last-Modified'])

    def test_post_to_container(self):
        data = dict()
        data['@context'] = AnnotationWriter.JSONLD_CONTEXT
        data['type'] = "Annotation"
        data['motivation'] = Annotation.IDLING
        data['target'] = dict(source=self.identifier.urn, selector="epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)")

        with self.app.test_request_context(
            "/", headers=dict(Authorization=self.valid_auth), method='POST', data=json.dumps(data)):
            patron = self.manager.annotations.authenticated_patron_from_request()
            # The patron doesn't have any annotations yet.
            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            eq_(0, len(annotations))

            response = self.manager.annotations.container()

            # The patron doesn't have the pool on loan yet, so the request fails.
            eq_(400, response.status_code)
            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            eq_(0, len(annotations))

            # Give the patron a loan and try again, and the request creates an annotation.
            self.pool.loan_to(patron)
            response = self.manager.annotations.container()
            eq_(200, response.status_code)
            
            annotations = self._db.query(Annotation).filter(Annotation.patron==patron).all()
            eq_(1, len(annotations))
            annotation = annotations[0]
            eq_(Annotation.IDLING, annotation.motivation)
            selector = json.loads(annotation.target).get("http://www.w3.org/ns/oa#hasSelector")[0].get('@id')
            eq_(data['target']['selector'], selector)

    def test_detail(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.detail(annotation.id)
            eq_(200, response.status_code)

            # We've been given a single annotation item.
            item = json.loads(response.data)
            assert str(annotation.id) in item['id']
            eq_(annotation.motivation, item['motivation'])

            # The response has the appropriate headers.
            allow_header = response.headers['Allow']
            for method in ['GET', 'HEAD', 'OPTIONS', 'DELETE']:
                assert method in allow_header

            eq_(AnnotationWriter.CONTENT_TYPE, response.headers['Content-Type'])

    def test_detail_for_other_patrons_annotation_returns_404(self):
        patron = self._patron()
        self.pool.loan_to(patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()

            # The patron can't see that this annotation exists.
            response = self.manager.annotations.detail(annotation.id)
            eq_(404, response.status_code)

    def test_detail_for_missing_annotation_returns_404(self):
        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()

            # This annotation does not exist.
            response = self.manager.annotations.detail(100)
            eq_(404, response.status_code)

    def test_detail_for_deleted_annotation_returns_404(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = False

        with self.app.test_request_context(
                "/", headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.detail(annotation.id)
            eq_(404, response.status_code)

    def test_delete(self):
        self.pool.loan_to(self.default_patron)

        annotation, ignore = create(
            self._db, Annotation,
            patron=self.default_patron,
            identifier=self.identifier,
            motivation=Annotation.IDLING,
        )
        annotation.active = True

        with self.app.test_request_context(
                "/", method='DELETE', headers=dict(Authorization=self.valid_auth)):
            self.manager.annotations.authenticated_patron_from_request()
            response = self.manager.annotations.detail(annotation.id)
            eq_(200, response.status_code)

            # The annotation has been marked inactive.
            eq_(False, annotation.active)

class TestWorkController(CirculationControllerTest):
    def setup(self):
        super(TestWorkController, self).setup()
        [self.lp] = self.english_1.license_pools
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

    def test_contributor(self):
        # For works without a contributor name, a ProblemDetail is returned.
        with self.app.test_request_context('/'):
            response = self.manager.work_controller.contributor('')
        eq_(404, response.status_code)
        eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

        name = 'John Bull'
        
        # Similarly if the pagination data is bad.
        with self.app.test_request_context('/?size=abc'):
            response = self.manager.work_controller.contributor(name)
            eq_(400, response.status_code)

        # Or if the facet data is bad.
        with self.app.test_request_context('/?order=nosuchorder'):
            response = self.manager.work_controller.contributor(name)
            eq_(400, response.status_code)
        
        # If the work has a contributor, a feed is returned.
        SessionManager.refresh_materialized_views(self._db)
        with self.app.test_request_context('/'):
            response = self.manager.work_controller.contributor(name)

        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(name, feed['feed']['title'])
        [entry] = feed['entries']
        eq_(self.english_1.title, entry['title'])

    def test_permalink(self):
        with self.app.test_request_context("/"):
            response = self.manager.work_controller.permalink(self.datasource, self.identifier.type, self.identifier.identifier)
            annotator = CirculationManagerAnnotator(None, None)
            expect = etree.tostring(
                AcquisitionFeed.single_entry(
                    self._db, self.english_1, annotator
                )
            )
        eq_(200, response.status_code)
        eq_(expect, response.data)
        eq_(OPDSFeed.ENTRY_TYPE, response.headers['Content-Type'])

    def test_recommendations(self):
        # Prep an empty recommendation.
        source = DataSource.lookup(self._db, self.datasource)
        metadata = Metadata(source)
        mock_api = MockNoveListAPI()
        mock_api.setup(metadata)

        SessionManager.refresh_materialized_views(self._db)
        args = [self.datasource, self.identifier.type,
                self.identifier.identifier]
        kwargs = dict(novelist_api=mock_api)
        
        # We get a 400 response if the pagination data is bad.
        with self.app.test_request_context('/?size=abc'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
            eq_(400, response.status_code)

        # Or if the facet data is bad.
        mock_api.setup(metadata)
        with self.app.test_request_context('/?order=nosuchorder'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
            eq_(400, response.status_code)

        # Show it working.
        mock_api.setup(metadata)
        with self.app.test_request_context('/'):
            response = self.manager.work_controller.recommendations(
                *args, **kwargs
            )
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_('Recommended Books', feed['feed']['title'])
        eq_(0, len(feed['entries']))

       
        # Delete the cache and prep a recommendation result.
        [cached_empty_feed] = self._db.query(CachedFeed).all()
        self._db.delete(cached_empty_feed)
        metadata.recommendations = [self.english_2.license_pools[0].identifier]
        mock_api.setup(metadata)

        SessionManager.refresh_materialized_views(self._db)
        with self.app.test_request_context('/'):
            response = self.manager.work_controller.recommendations(
                self.datasource, self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )
        # A feed is returned with the proper recommendation.
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_('Recommended Books', feed['feed']['title'])
        eq_(1, len(feed['entries']))
        [entry] = feed['entries']
        eq_(self.english_2.title, entry['title'])
        eq_(self.english_2.author, entry['author'])

        with temp_config() as config:
            with self.app.test_request_context('/'):
                config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}
                response = self.manager.work_controller.recommendations(
                    self.datasource, self.identifier.type, self.identifier.identifier
                )
            eq_(404, response.status_code)
            eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

    def test_related_books(self):
        # A book with no related books returns a ProblemDetail.
        with temp_config() as config:
            # Don't set NoveList Integration.
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {}

            # Remove contribution.
            [contribution] = self.lp.presentation_edition.contributions
            [original, role] = [contribution.contributor, contribution.role]
            self._db.delete(contribution)
            self._db.commit()

            with self.app.test_request_context('/'):
                response = self.manager.work_controller.related(
                    self.datasource, self.identifier.type, self.identifier.identifier
                )
        eq_(404, response.status_code)
        eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

        # Prep book with a contribution, a series, and a recommendation.
        self.lp.presentation_edition.add_contributor(original, role)
        original.display_name = original.sort_name
        same_author = self._work(
            "What is Sunday?", original.display_name,
            language="eng", fiction=True, with_open_access_download=True
        )

        self.lp.presentation_edition.series = "Around the World"
        self.french_1.presentation_edition.series = "Around the World"
        SessionManager.refresh_materialized_views(self._db)

        source = DataSource.lookup(self._db, self.datasource)
        metadata = Metadata(source)
        mock_api = MockNoveListAPI()
        metadata.recommendations = [self.english_2.license_pools[0].identifier]
        mock_api.setup(metadata)

        # A grouped feed is returned with all of the related books
        with self.app.test_request_context('/'):
            response = self.manager.work_controller.related(
                self.datasource, self.identifier.type, self.identifier.identifier,
                novelist_api=mock_api
            )
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(5, len(feed['entries']))

        def collection_link(entry):
            [link] = [l for l in entry['links'] if l['rel']=='collection']
            return link['title'], link['href']

        # One book is in the recommendations feed.
        [e1] = [e for e in feed['entries'] if e['title'] == self.english_2.title]
        title, href = collection_link(e1)

        eq_("Recommended Books", title)
        work_url = "/works/%s/%s/%s/" % (self.datasource, self.identifier.type, self.identifier.identifier)
        expected = urllib.quote(work_url + 'recommendations')
        eq_(True, href.endswith(expected))

        # The other book in the series is in the series feed.
        [e2] = [e for e in feed['entries'] if e['title'] == self.french_1.title]
        title, href = collection_link(e2)
        eq_("Around the World", title)
        expected_series_link = urllib.quote('series/Around the World')
        eq_(True, href.endswith(expected_series_link))

        # The other book by this contributor is in the contributor feed.
        [e3] = [e for e in feed['entries'] if e['title'] == same_author.title]
        title, href = collection_link(e3)
        eq_("John Bull", title)
        expected_contributor_link = urllib.quote('contributor/John Bull')
        eq_(True, href.endswith(expected_contributor_link))

        # The original book is listed in both the series and contributor feeds.
        title_to_link_ending = {
            'Around the World' : expected_series_link,
            'John Bull' : expected_contributor_link
        }
        entries = [e for e in feed['entries'] if e['title']==self.english_1.title]
        eq_(2, len(entries))
        for entry in entries:
            title, href = collection_link(entry)
            eq_(True, href.endswith(title_to_link_ending[title]))
            del title_to_link_ending[title]

    def test_report_problem_get(self):
        with self.app.test_request_context("/"):
            response = self.manager.work_controller.report(self.datasource, self.identifier.type, self.identifier.identifier)
        eq_(200, response.status_code)
        eq_("text/uri-list", response.headers['Content-Type'])
        for i in Complaint.VALID_TYPES:
            assert i in response.data

    def test_report_problem_post_success(self):
        error_type = random.choice(list(Complaint.VALID_TYPES))
        data = json.dumps({ "type": error_type,
                            "source": "foo",
                            "detail": "bar"}
        )
        with self.app.test_request_context("/", method="POST", data=data):
            response = self.manager.work_controller.report(self.datasource, self.identifier.type, self.identifier.identifier)
        eq_(201, response.status_code)
        [complaint] = self.lp.complaints
        eq_(error_type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)

    def test_series(self):
        # If the work doesn't have a series, a ProblemDetail is returned.
        with self.app.test_request_context('/'):
            response = self.manager.work_controller.series("")
        eq_(404, response.status_code)
        eq_("http://librarysimplified.org/terms/problem/unknown-lane", response.uri)

        series_name = "Like As If Whatever Mysteries"
        self.lp.presentation_edition.series = series_name
        # Similarly if the pagination data is bad.
        with self.app.test_request_context('/?size=abc'):
            response = self.manager.work_controller.series(series_name)
            eq_(400, response.status_code)

        # Or if the facet data is bad
        with self.app.test_request_context('/?order=nosuchorder'):
            response = self.manager.work_controller.series(series_name)
            eq_(400, response.status_code)
            
        # If the work is in a series, a feed is returned.
        SessionManager.refresh_materialized_views(self._db)
        with self.app.test_request_context('/'):
            response = self.manager.work_controller.series(series_name)
        eq_(200, response.status_code)
        feed = feedparser.parse(response.data)
        eq_(series_name, feed['feed']['title'])
        [entry] = feed['entries']
        eq_(self.english_1.title, entry['title'])


class TestFeedController(CirculationControllerTest):

    def test_feed(self):
        SessionManager.refresh_materialized_views(self._db)
        with self.app.test_request_context("/"):
            with temp_config() as config:
                config['links'] = {
                    "terms_of_service": "a",
                    "privacy_policy": "b",
                    "copyright": "c",
                    "about": "d",
                }
                response = self.manager.opds_feeds.feed(
                    'eng', 'Adult Fiction'
                )

                assert self.english_1.title in response.data
                assert self.english_2.title not in response.data
                assert self.french_1.title not in response.data

                feed = feedparser.parse(response.data)
                links = feed['feed']['links']
                by_rel = dict()
                for i in links:
                    by_rel[i['rel']] = i['href']

                eq_("a", by_rel['terms-of-service'])
                eq_("b", by_rel['privacy-policy'])
                eq_("c", by_rel['copyright'])
                eq_("d", by_rel['about'])

    def test_multipage_feed(self):
        self._work("fiction work", language="eng", fiction=True, with_open_access_download=True)
        SessionManager.refresh_materialized_views(self._db)
        with self.app.test_request_context("/?size=1"):
            response = self.manager.opds_feeds.feed('eng', 'Adult Fiction')

            feed = feedparser.parse(response.data)
            entries = feed['entries']
            
            eq_(1, len(entries))

            links = feed['feed']['links']
            next_link = [x for x in links if x['rel'] == 'next'][0]['href']
            assert 'after=1' in next_link
            assert 'size=1' in next_link

            facet_links = [x for x in links if x['rel'] == 'http://opds-spec.org/facet']
            assert any('order=title' in x['href'] for x in facet_links)
            assert any('order=author' in x['href'] for x in facet_links)

            search_link = [x for x in links if x['rel'] == 'search'][0]['href']
            assert search_link.endswith('/search/eng/Adult%20Fiction')

            shelf_link = [x for x in links if x['rel'] == 'http://opds-spec.org/shelf'][0]['href']
            assert shelf_link.endswith('/loans/')

    def test_bad_order_gives_problem_detail(self):
        with self.app.test_request_context("/?order=nosuchorder"):
            response = self.manager.opds_feeds.feed('eng', 'Adult Fiction')
            eq_(400, response.status_code)
            eq_(
                "http://librarysimplified.org/terms/problem/invalid-input", 
                response.uri
            )

    def test_bad_pagination_gives_problem_detail(self):
        with self.app.test_request_context("/?size=abc"):
            response = self.manager.opds_feeds.feed('eng', 'Adult Fiction')
            eq_(400, response.status_code)
            eq_(
                "http://librarysimplified.org/terms/problem/invalid-input", 
                response.uri
            )            

    def test_groups(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {
                Configuration.GROUPS_MAX_AGE_POLICY : 10,
                Configuration.MINIMUM_FEATURED_QUALITY: 0,
                Configuration.FEATURED_LANE_SIZE: 2,
            }

            for i in range(2):
                self._work("fiction work %i" % i, language="eng", fiction=True, with_open_access_download=True)
                self._work("nonfiction work %i" % i, language="eng", fiction=False, with_open_access_download=True)
        
            SessionManager.refresh_materialized_views(self._db)
            with self.app.test_request_context("/"):
                response = self.manager.opds_feeds.groups(None, None)

                feed = feedparser.parse(response.data)
                entries = feed['entries']

                counter = Counter()
                for entry in entries:
                    links = [x for x in entry.links if x['rel'] == 'collection']
                    for link in links:
                        counter[link['title']] += 1
                eq_(2, counter['Nonfiction'])
                eq_(2, counter['Fiction'])

    def test_search(self):
        # Put two works into the search index
        self.english_1.update_external_index(self.manager.external_search)  # english_1 is "Quite British" by John Bull
        self.english_2.update_external_index(self.manager.external_search)  # english_2 is "Totally American" by Uncle Sam

        # Update the materialized view to make sure the works show up.
        SessionManager.refresh_materialized_views(self._db)

        # Execute a search query designed to find the second one.
        with self.app.test_request_context("/?q=t&size=1&after=1"):
            response = self.manager.opds_feeds.search(None, None)
            feed = feedparser.parse(response.data)
            entries = feed['entries']
            eq_(1, len(entries))
            entry = entries[0]
            eq_(self.english_2.author, entry.author)

            assert 'links' in entry
            assert len(entry.links) > 0

            borrow_links = [link for link in entry.links if link.rel == 'http://opds-spec.org/acquisition/borrow']
            eq_(1, len(borrow_links))

            next_links = [link for link in feed['feed']['links'] if link.rel == 'next']
            eq_(1, len(next_links))

            previous_links = [link for link in feed['feed']['links'] if link.rel == 'previous']
            eq_(1, len(previous_links))

    def test_preload(self):
        SessionManager.refresh_materialized_views(self._db)

        with temp_config() as config:
            urn = self.english_2.presentation_edition.primary_identifier.urn
            config[Configuration.POLICIES] = {
                Configuration.PRELOADED_CONTENT : [urn]
            }

            with self.app.test_request_context("/"):
                response = self.manager.opds_feeds.preload()

                assert self.english_1.title not in response.data
                assert self.english_2.title in response.data
                assert self.french_1.author not in response.data

class TestAnalyticsController(CirculationControllerTest):
    def setup(self):
        super(TestAnalyticsController, self).setup()
        [self.lp] = self.english_1.license_pools
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

    def test_track_event(self):
        with temp_config() as config:
            config = {
                Configuration.POLICIES : {
                    Configuration.ANALYTICS_POLICY : ["core.local_analytics_provider"]
                }
            }

            analytics = Analytics.initialize(
                ['core.local_analytics_provider'], config
            )            

            with self.app.test_request_context("/"):
                response = self.manager.analytics_controller.track_event(self.datasource, self.identifier.type, self.identifier.identifier, "invalid_type")
                eq_(400, response.status_code)
                eq_(INVALID_ANALYTICS_EVENT_TYPE.uri, response.uri)

            with self.app.test_request_context("/"):
                response = self.manager.analytics_controller.track_event(self.datasource, self.identifier.type, self.identifier.identifier, "open_book")
                eq_(200, response.status_code)

                circulation_event = get_one(
                    self._db, CirculationEvent,
                    type="open_book",
                    license_pool=self.lp
                )
                assert circulation_event != None

class TestScopedSession(ControllerTest):
    """Test that in production scenarios (as opposed to normal unit tests)
    the app server runs each incoming request in a separate database
    session.

    Compare to TestBaseController.test_unscoped_session, which tests
    the corresponding behavior in unit tests.
    """

    def setup(self):
        from api.app import _db
        super(TestScopedSession, self).setup(_db)

    @contextmanager
    def test_request_context_and_transaction(self, *args):
        """Run a simulated Flask request in a transaction that gets rolled
        back at the end of the request.
        """
        with self.app.test_request_context(*args) as ctx:
            transaction = current_session.begin_nested()
            yield ctx
            transaction.rollback()

    def test_scoped_session(self):
        # Start a simulated request to the Flask app server.
        with self.test_request_context_and_transaction("/"):
            # Each request is given its own database session distinct
            # from the one used by most unit tests or the one
            # associated with the CirculationManager object.
            session1 = current_session()
            assert session1 != self._db
            assert session1 != self.app.manager._db

            # Add an Identifier to the database.
            identifier = Identifier(type=DataSource.GUTENBERG, identifier="1024")
            session1.add(identifier)
            session1.flush()

            # The Identifier immediately shows up in the session that
            # created it.
            [identifier] = session1.query(Identifier).all()
            eq_("1024", identifier.identifier)

            # It doesn't show up in self._db, the database session
            # used by most other unit tests, because it was created
            # within the (still-active) context of a Flask request,
            # which happens within a nested database transaction.
            eq_([], self._db.query(Identifier).all())

            # It shows up in the flask_scoped_session object that
            # created the request-scoped session, because within the
            # context of a request, running database queries on that object
            # actually runs them against your request-scoped session.
            [identifier] = self.app.manager._db.query(Identifier).all()
            eq_("1024", identifier.identifier)

            # But if we were to use flask_scoped_session to create a
            # brand new session, it would not see the Identifier,
            # because it's running in a different database session.
            new_session = self.app.manager._db.session_factory()
            eq_([], new_session.query(Identifier).all())

        # Once we exit the context of the Flask request, the
        # transaction is rolled back. The Identifier never actually
        # enters the database.
        #
        # If it did enter the database, it would never leave.  Changes
        # that happen through self._db happen inside a nested
        # transaction which is rolled back after the test is over.
        # But changes that happen through a session-scoped database
        # connection are actually written to the database when we
        # leave the scope of the request.
        #
        # To avoid this, we use test_request_context_and_transaction
        # to create a nested transaction that's rolled back just
        # before we leave the scope of the request.
        eq_([], self._db.query(Identifier).all())

        # Now create a different simulated Flask request
        with self.test_request_context_and_transaction("/"):
            session2 = current_session()
            assert session2 != self._db
            assert session2 != self.app.manager._db

        # The two Flask requests got different sessions, neither of
        # which is the same as self._db, the unscoped database session
        # used by most other unit tests.
        assert session1 != session2
