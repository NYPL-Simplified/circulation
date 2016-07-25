from nose.tools import set_trace
import json
import logging
import sys
import urllib
import datetime

from lxml import etree

from functools import wraps
import flask
from flask import (
    Response,
    redirect,
)
from flask.ext.babel import lazy_gettext as _

from core.app_server import (
    entry_response,
    feed_response,
    cdn_url_for,
    url_for,
    load_lending_policy,
    load_facets_from_request,
    load_pagination_from_request,
    load_facets,
    load_pagination,
    ComplaintController,
    HeartbeatController,
    URNLookupController,
)
from core.external_search import (
    ExternalSearchIndex,
    DummyExternalSearchIndex,
)
from core.lane import (
    Facets, 
    Pagination,
    Lane,
    LaneList,
)
from core.model import (
    get_one,
    get_one_or_create,
    Admin,
    CachedFeed,
    CirculationEvent,
    Complaint,
    DataSource,
    Hold,
    Identifier,
    Loan,
    LicensePoolDeliveryMechanism,
    production_session,
    Work,
)
from core.opds import (
    AcquisitionFeed,
)
from core.util.opds_writer import (    
     OPDSFeed,
)
from core.opensearch import OpenSearchDocument
from core.util.flask_util import (
    problem,
)
from core.util.problem_detail import ProblemDetail
from core.util.opds_authentication_document import OPDSAuthenticationDocument

from circulation_exceptions import *

from opds import (
    CirculationManagerAnnotator,
    CirculationManagerLoanAndHoldAnnotator,
    PreloadFeed,
)
from problem_details import *

from authenticator import Authenticator
from config import (
    Configuration, 
    CannotLoadConfiguration
)

from lanes import (
    make_lanes,
    RecommendationLane,
    RelatedBooksLane,
    SeriesLane,
)

from adobe_vendor_id import AdobeVendorIDController
from axis import Axis360API
from overdrive import OverdriveAPI
from threem import ThreeMAPI
from circulation import CirculationAPI
from novelist import (
    NoveListAPI,
    MockNoveListAPI,
)
from testing import MockCirculationAPI
from services import ServiceStatus
from core.analytics import Analytics

class CirculationManager(object):

    def __init__(self, _db, lanes=None, testing=False):

        self.log = logging.getLogger("Circulation manager web app")

        if not testing:
            try:
                self.config = Configuration.load()
            except CannotLoadConfiguration, e:
                self.log.error("Could not load configuration file: %s" % e)
                sys.exit()
        self._db = _db

        self.testing = testing
        if isinstance(lanes, LaneList):
            lanes = lanes
        else:
            lanes = make_lanes(_db, lanes)
        self.top_level_lane = self.create_top_level_lane(lanes)

        self.auth = Authenticator.initialize(self._db, test=testing)
        self.setup_circulation()
        self.external_search = self.setup_search()
        self.lending_policy = load_lending_policy(
            Configuration.policy('lending', {})
        )

        self.setup_controllers()
        self.setup_adobe_vendor_id()

        self.opds_authentication_document = self.auth.create_authentication_document()

    def create_top_level_lane(self, lanelist):
        name = 'All Books'
        return Lane(
            self._db, name,
            display_name=name,
            parent=None,
            sublanes=lanelist.lanes,
            include_all=False,
            languages=None,
            searchable=True,
            invisible=True
        )

    def cdn_url_for(self, view, *args, **kwargs):
        return cdn_url_for(view, *args, **kwargs)

    def url_for(self, view, *args, **kwargs):
        kwargs['_external'] = True
        return url_for(view, *args, **kwargs)

    def log_lanes(self, lanelist=None, level=0):
        """Output information about the lane layout."""
        lanelist = lanelist or self.top_level_lane.sublanes
        for lane in lanelist:
            self.log.debug("%s%r", "-" * level, lane)
            if lane.sublanes:
                self.log_lanes(lane.sublanes, level+1)

    def setup_search(self):
        """Set up a search client."""
        if self.testing:
            return DummyExternalSearchIndex()
        else:
            if Configuration.integration(
                    Configuration.ELASTICSEARCH_INTEGRATION):
                return ExternalSearchIndex()
            else:
                self.log.warn("No external search server configured.")
                return None

    def setup_circulation(self):
        """Set up distributor APIs and a the Circulation object."""
        if self.testing:
            self.circulation = MockCirculationAPI(self._db)
        else:
            overdrive = OverdriveAPI.from_environment(self._db)
            threem = ThreeMAPI.from_environment(self._db)
            axis = Axis360API.from_environment(self._db)
            self.circulation = CirculationAPI(
                _db=self._db, 
                threem=threem, 
                overdrive=overdrive,
                axis=axis
            )

    def setup_controllers(self):
        """Set up all the controllers that will be used by the web app."""
        self.index_controller = IndexController(self)
        self.opds_feeds = OPDSFeedController(self)
        self.loans = LoanController(self)
        self.accounts = AccountController(self)
        self.urn_lookup = URNLookupController(self._db)
        self.work_controller = WorkController(self)
        self.analytics_controller = AnalyticsController(self)

        self.heartbeat = HeartbeatController()
        self.service_status = ServiceStatusController(self)

    def setup_adobe_vendor_id(self):
        """Set up the controller for Adobe Vendor ID."""
        adobe = Configuration.integration(
            Configuration.ADOBE_VENDOR_ID_INTEGRATION
        )
        vendor_id = adobe.get(Configuration.ADOBE_VENDOR_ID)
        node_value = adobe.get(Configuration.ADOBE_VENDOR_ID_NODE_VALUE)
        if vendor_id and node_value:
            self.adobe_vendor_id = AdobeVendorIDController(
                self._db,
                vendor_id,
                node_value,
                self.auth
            )
        else:
            self.log.warn("Adobe Vendor ID controller is disabled due to missing or incomplete configuration.")
            self.adobe_vendor_id = None

    def annotator(self, lane, *args, **kwargs):
        """Create an appropriate OPDS annotator for the given lane."""
        return CirculationManagerAnnotator(
            self.circulation, lane, top_level_title='All Books',
            *args, **kwargs
        )


class CirculationManagerController(object):

    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db
        self.circulation = self.manager.circulation
        self.url_for = self.manager.url_for
        self.cdn_url_for = self.manager.cdn_url_for

    def authorization_header(self):
        """Get the authentication header."""

        # This is the basic auth header.
        header = flask.request.authorization

        # If we're using a token instead, flask doesn't extract it for us.
        if not header:
            if 'Authorization' in flask.request.headers:
                header = flask.request.headers['Authorization']

        return header


    def authenticated_patron_from_request(self):
        header = self.authorization_header()

        if not header:
            # No credentials were provided.
            return self.authenticate()
        try:
            patron = self.authenticated_patron(header)
        except RemoteInitiatedServerError,e:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _("Error in authentication service")
            )
        if isinstance(patron, ProblemDetail):
            flask.request.patron = None
            return patron
        else:
            flask.request.patron = patron
            return patron

    def authenticated_patron(self, authorization_header):
        """Look up the patron authenticated by the given authorization header.

        The header could contain a barcode and pin or a token for an
        external service.

        If there's a problem, return a 2-tuple (URI, title) for use in a
        Problem Detail Document.

        If there's no problem, return a Patron object.
        """
        patron = self.manager.auth.authenticated_patron(
            self._db, authorization_header
        )
        if not patron:
            return INVALID_CREDENTIALS

        # Okay, we know who they are and their PIN is valid. But maybe the
        # account has expired?
        if not patron.authorization_is_active:
            return EXPIRED_CREDENTIALS

        # No, apparently we're fine.
        return patron

    def authenticate(self):
        """Sends a 401 response that demands authentication."""
        data = self.manager.opds_authentication_document
        headers = { 'Content-Type' : OPDSAuthenticationDocument.MEDIA_TYPE }
        # if requested from a web client, don't include WWW-Authenticate header,
        # which forces the default browser authentication prompt
        if not flask.request.headers.get("X-Requested-With") == "XMLHttpRequest":
            headers['WWW-Authenticate'] = 'Basic realm="%s"' % _("Library card")
        return Response(data, 401, headers)

    def load_lane(self, language_key, name):
        """Turn user input into a Lane object."""
        if language_key is None and name is None:
            return self.manager.top_level_lane

        lanelist = self.manager.top_level_lane.sublanes
        if not language_key in lanelist.by_languages:
            return NO_SUCH_LANE.detailed(
                _("Unrecognized language key: %(language_key)s", language_key=language_key)
            )

        if name:
            name = name.replace("__", "/")

        lanes = lanelist.by_languages[language_key]

        if not name:
            defaults = [x for x in lanes.values() if x.default_for_language]
            if len(defaults) == 1:
                # This language has one, and only one, default lane.
                return defaults[0]

        if name not in lanes:
            return NO_SUCH_LANE.detailed(
                _("No such lane: %(lane_name)s", lane_name=name)
            )
        return lanes[name]

    def load_licensepool(self, data_source, identifier_type, identifier):
        """Turn user input into a LicensePool object."""
        if isinstance(data_source, DataSource):
            source = data_source
        else:
            source = DataSource.lookup(self._db, data_source)
        if source is None:
            return INVALID_INPUT.detailed(_("No such data source: %(data_source)s", data_source=data_source))

        id_obj, ignore = Identifier.for_foreign_id(
            self._db, identifier_type, identifier, autocreate=False)
        if not id_obj:
            return NO_LICENSES.detailed(
                _("The item you're asking about (%s/%s) isn't in this collection.") % (
                    identifier_type, identifier
                )
            )
        pool = id_obj.licensed_through
        return pool

    def load_licensepooldelivery(self, pool, mechanism_id):
        """Turn user input into a LicensePoolDeliveryMechanism object.""" 
        mechanism = get_one(
            self._db, LicensePoolDeliveryMechanism, license_pool=pool,
            delivery_mechanism_id=mechanism_id, on_multiple='interchangeable'
        )
        return mechanism or BAD_DELIVERY_MECHANISM

    def apply_borrowing_policy(self, patron, license_pool):
        if not patron.can_borrow(license_pool.work, self.manager.lending_policy):
            return FORBIDDEN_BY_POLICY.detailed(
                _("Library policy prohibits us from lending you this book."),
                status_code=451
            )

        if (license_pool.licenses_available == 0 and
            not license_pool.open_access and
            Configuration.hold_policy() !=
            Configuration.HOLD_POLICY_ALLOW
        ):
            return FORBIDDEN_BY_POLICY.detailed(
                _("Library policy prohibits the placement of holds."),
                status_code=403
            )        
        return None


class IndexController(CirculationManagerController):
    """Redirect the patron to the appropriate feed."""

    def __call__(self):
        # The simple case: the app is equally open to all clients.
        policy = Configuration.root_lane_policy()
        if not policy:
            return redirect(self.cdn_url_for('acquisition_groups'))

        # The more complex case. We must authorize the patron, check
        # their type, and redirect them to an appropriate feed.
        return self.appropriate_index_for_patron_type()

    def authenticated_patron_root_lane(self):
        patron = self.authenticated_patron_from_request()
        if isinstance(patron, ProblemDetail):
            return patron
        if isinstance(patron, Response):
            return patron

        policy = Configuration.root_lane_policy()
        lane_info = policy.get(patron.external_type)
        if lane_info is None:
            return None
        else:
            lang_key, name = lane_info
            return self.load_lane(lang_key, name)

    def appropriate_index_for_patron_type(self):
        root_lane = self.authenticated_patron_root_lane()
        if isinstance(root_lane, ProblemDetail):
            return root_lane
        if isinstance(root_lane, Response):
            return root_lane
        if root_lane is None:
            return redirect(
                self.cdn_url_for(
                    'acquisition_groups'
                )
            )
    
        return redirect(
            self.cdn_url_for(
                'acquisition_groups', 
                languages=root_lane.language_key,
                lane_name=root_lane.url_name
            )
        )


class OPDSFeedController(CirculationManagerController):

    def groups(self, languages, lane_name):
        """Build or retrieve a grouped acquisition feed."""

        lane = self.load_lane(languages, lane_name)
        if isinstance(lane, ProblemDetail):
            return lane
        url = self.cdn_url_for(
            "acquisition_groups", languages=languages, lane_name=lane_name
        )

        title = lane.display_name

        annotator = self.manager.annotator(lane)
        feed = AcquisitionFeed.groups(self._db, title, url, lane, annotator)
        return feed_response(feed.content)

    def feed(self, languages, lane_name):
        """Build or retrieve a paginated acquisition feed."""

        lane = self.load_lane(languages, lane_name)
        if isinstance(lane, ProblemDetail):
            return lane
        url = self.cdn_url_for(
            "feed", languages=languages, lane_name=lane_name
        )

        title = lane.display_name

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request()
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        feed = AcquisitionFeed.page(
            self._db, title, url, lane, annotator=annotator,
            facets=facets,
            pagination=pagination,
        )
        return feed_response(feed.content)

    def search(self, languages, lane_name):

        lane = self.load_lane(languages, lane_name)
        if isinstance(lane, ProblemDetail):
            return lane
        query = flask.request.args.get('q')
        this_url = self.url_for(
            'lane_search', languages=languages, lane_name=lane_name,
        )
        if not query:
            # Send the search form
            return OpenSearchDocument.for_lane(lane, this_url)

        pagination = load_pagination_from_request(default_size=Pagination.DEFAULT_SEARCH_SIZE)
        if isinstance(pagination, ProblemDetail):
            return pagination

        # Run a search.    
        this_url += "?q=" + urllib.quote(query.encode("utf8"))
        annotator = self.manager.annotator(lane)
        info = OpenSearchDocument.search_info(lane)
        opds_feed = AcquisitionFeed.search(
            _db=self._db, title=info['name'], 
            url=this_url, lane=lane, search_engine=self.manager.external_search,
            query=query, annotator=annotator, pagination=pagination,
        )
        return feed_response(opds_feed)

    def preload(self):
        this_url = url_for("preload", _external=True)

        annotator = self.manager.annotator(None)
        opds_feed = PreloadFeed.page(
            self._db, "Content to Preload", this_url,
            annotator=annotator,
        )
        return feed_response(opds_feed)


class AccountController(CirculationManagerController):

    def account(self):
        header = self.authorization_header()

        patron_info = self.manager.auth.patron_info(header)
        return json.dumps(dict(
            username=patron_info.get('username', None),
            barcode=patron_info.get('barcode'),
        ))


class LoanController(CirculationManagerController):

    def sync(self):
        if flask.request.method=='HEAD':
            return Response()

        patron = flask.request.patron

        # First synchronize our local list of loans and holds with all
        # third-party loan providers.
        if patron.authorization_identifier:
            header = flask.request.authorization
            try:
                self.circulation.sync_bookshelf(patron, header.password)
            except Exception, e:
                # If anything goes wrong, omit the sync step and just
                # display the current active loans, as we understand them.
                self.manager.log.error("ERROR DURING SYNC: %r", e, exc_info=e)

        # Then make the feed.
        feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            self.circulation, patron)
        return feed_response(feed, cache_for=None)

    def borrow(self, data_source, identifier_type, identifier, mechanism_id=None):
        """Create a new loan or hold for a book.

        Return an OPDS Acquisition feed that includes a link of rel
        "http://opds-spec.org/acquisition", which can be used to fetch the
        book or the license file.
        """

        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            # Something went wrong.
            return pool

        # Find the delivery mechanism they asked for, if any.
        mechanism = None
        if mechanism_id:
            mechanism = self.load_licensepooldelivery(pool, mechanism_id)
            if isinstance(mechanism, ProblemDetail):
                return mechanism

        if not pool:
            # I've never heard of this book.
            return NO_LICENSES.detailed(
                _("I've never heard of this work.")
            )

        patron = flask.request.patron
        problem_doc = self.apply_borrowing_policy(patron, pool)
        if problem_doc:
            # As a matter of policy, the patron is not allowed to check
            # this book out.
            return problem_doc

        pin = flask.request.authorization.password
        problem_doc = None

        try:
            loan, hold, is_new = self.circulation.borrow(
                patron, pin, pool, mechanism
            )
        except NoOpenAccessDownload, e:
            problem_doc = NO_LICENSES.detailed(
                _("Couldn't find an open-access download link for this book."),
                status_code=404
            )
        except PatronAuthorizationFailedException, e:
            problem_doc = INVALID_CREDENTIALS
        except PatronLoanLimitReached, e:
            problem_doc = LOAN_LIMIT_REACHED.with_debug(str(e))
        except PatronHoldLimitReached, e:
            problem_doc = e.as_problem_detail_document()
        except DeliveryMechanismError, e:
            return BAD_DELIVERY_MECHANISM.with_debug(
                str(e), status_code=e.status_code
            )
        except OutstandingFines, e:
            problem_doc = OUTSTANDING_FINES.detailed(
                _("You must pay your %(fine_amount)s outstanding fines before you can borrow more books.", fine_amount=patron.fines)
            )
        except CannotLoan, e:
            problem_doc = CHECKOUT_FAILED.with_debug(str(e))
        except CannotHold, e:
            problem_doc = HOLD_FAILED.with_debug(str(e))
        except CannotRenew, e:
            problem_doc = RENEW_FAILED.with_debug(str(e))
        except NotFoundOnRemote, e:
            problem_doc = NOT_FOUND_ON_REMOTE
        except CirculationException, e:
            # Generic circulation error.
            problem_doc = CHECKOUT_FAILED.with_debug(str(e))

        if problem_doc:
            return problem_doc

        # At this point we have either a loan or a hold. If a loan, serve
        # a feed that tells the patron how to fulfill the loan. If a hold,
        # serve a feed that talks about the hold.
        if loan:
            feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(
                self.circulation, loan)
        elif hold:
            feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
                self.circulation, hold)
        else:
            # This should never happen -- we should have sent a more specific
            # error earlier.
            return HOLD_FAILED
        if isinstance(feed, OPDSFeed):
            content = unicode(feed)
        else:
            content = etree.tostring(feed)
        if is_new:
            status_code = 201
        else:
            status_code = 200
        headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }
        return Response(content, status_code, headers)

    def fulfill(self, data_source, identifier_type, identifier, mechanism_id=None):
        """Fulfill a book that has already been checked out.

        If successful, this will serve the patron a downloadable copy of
        the book, or a DRM license file which can be used to get the
        book). Alternatively, it may serve an HTTP redirect that sends the
        patron to a copy of the book or a license file.
        """
        patron = flask.request.patron
        header = flask.request.authorization
        pin = header.password
    
        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
    
        # Find the LicensePoolDeliveryMechanism they asked for.
        mechanism = None
        if mechanism_id:
            mechanism = self.load_licensepooldelivery(pool, mechanism_id)
            if isinstance(mechanism, ProblemDetail):
                return mechanism
    
        if not mechanism:
            # See if the loan already has a mechanism set. We can use that.
            loan = get_one(self._db, Loan, patron=patron, license_pool=pool)
            if loan and loan.fulfillment:
                mechanism =  loan.fulfillment
            else:
                return BAD_DELIVERY_MECHANISM.detailed(
                    _("You must specify a delivery mechanism to fulfill this loan.")
                )
    
        try:
            fulfillment = self.circulation.fulfill(patron, pin, pool, mechanism)
        except DeliveryMechanismConflict, e:
            return DELIVERY_CONFLICT.detailed(e.message)
        except NoActiveLoan, e:
            return NO_ACTIVE_LOAN.detailed( 
                    _('Can\'t fulfill loan because you have no active loan for this book.'),
                    status_code=e.status_code
            )
        except CannotFulfill, e:
            return CANNOT_FULFILL.with_debug(
                str(e), status_code=e.status_code
            )
        except FormatNotAvailable, e:
            return NO_ACCEPTABLE_FORMAT.with_debug(
                str(e), status_code=e.status_code
            )
        except DeliveryMechanismError, e:
            return BAD_DELIVERY_MECHANISM.with_debug(
                str(e), status_code=e.status_code
            )
    
        headers = dict()
        if fulfillment.content_link:
            status_code = 302
            headers["Location"] = fulfillment.content_link
        else:
            status_code = 200
        if fulfillment.content_type:
            headers['Content-Type'] = fulfillment.content_type
        return Response(fulfillment.content, status_code, headers)

    def revoke(self, data_source, identifier_type, identifier):
        patron = flask.request.patron
        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        loan = get_one(self._db, Loan, patron=patron, license_pool=pool)
        if loan:
            hold = None
        else:
            hold = get_one(self._db, Hold, patron=patron, license_pool=pool)

        if not loan and not hold:
            if not pool.work:
                title = 'this book'
            else:
                title = '"%s"' % pool.work.title
            return NO_ACTIVE_LOAN_OR_HOLD.detailed(
                _('Can\'t revoke because you have no active loan or hold for "%(title)s".', title=title),
                status_code=404
            )

        pin = flask.request.authorization.password
        if loan:
            try:
                self.circulation.revoke_loan(patron, pin, pool)
            except RemoteRefusedReturn, e:
                title = _("Loan deleted locally but remote refused. Loan is likely to show up again on next sync.")
                return COULD_NOT_MIRROR_TO_REMOTE.detailed(title, status_code=503)
            except CannotReturn, e:
                title = _("Loan deleted locally but remote failed.")
                return COULD_NOT_MIRROR_TO_REMOTE.detailed(title, 503).with_debug(str(e))
        elif hold:
            if not self.circulation.can_revoke_hold(pool, hold):
                title = _("Cannot release a hold once it enters reserved state.")
                return CANNOT_RELEASE_HOLD.detailed(title, 400)
            try:
                self.circulation.release_hold(patron, pin, pool)
            except CannotReleaseHold, e:
                title = _("Hold released locally but remote failed.")
                return CANNOT_RELEASE_HOLD.detailed(title, 503).with_debug(str(e))

        work = pool.work
        annotator = self.manager.annotator(None)
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator)
        )

    def detail(self, data_source, identifier_type, identifier):
        if flask.request.method=='DELETE':
            return self.revoke_loan_or_hold(data_source, identifier_type, identifier)

        patron = flask.request.patron
        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        loan = get_one(self._db, Loan, patron=patron, license_pool=pool)
        if loan:
            hold = None
        else:
            hold = get_one(self._db, Hold, patron=patron, license_pool=pool)

        if not loan and not hold:
            return NO_ACTIVE_LOAN_OR_HOLD.detailed( 
                _('You have no active loan or hold for "%(title)s".', title=pool.work.title),
                status_code=404
            )

        if flask.request.method=='GET':
            if loan:
                feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(
                    self.circulation, loan)
            else:
                feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
                    self.circulation, hold)
            feed = unicode(feed)
            return feed_response(feed, None)


class WorkController(CirculationManagerController):

    def permalink(self, data_source, identifier_type, identifier):
        """Serve an entry for a single book.

        This does not include any loan or hold-specific information for
        the authenticated patron.

        This is different from the /works lookup protocol, in that it
        returns a single entry while the /works lookup protocol returns a
        feed containing any number of entries.
        """

        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        work = pool.work
        annotator = self.manager.annotator(None)
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator)
        )

    def recommendations(self, data_source, identifier_type, identifier,
                        novelist_api=None):
        """Serve a feed of recommendations related to a given book."""

        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            return pool

        lane_name = "Recommendations for %s by %s" % (pool.work.title, pool.work.author)
        try:
            lane = RecommendationLane(
                self._db, pool, lane_name, novelist_api=novelist_api
            )
        except ValueError, e:
            # NoveList isn't configured.
            return NO_SUCH_LANE.detailed(_("Recommendations not available"))

        url = self.cdn_url_for(
            'recommendations', data_source=data_source,
            identifier_type=identifier_type, identifier=identifier
        )
        annotator = self.manager.annotator(lane)
        feed = AcquisitionFeed.page(
            self._db, lane.DISPLAY_NAME, url, lane,
            annotator=annotator, cache_type=CachedFeed.RECOMMENDATIONS_TYPE
        )

        return feed_response(unicode(feed.content))

    def related(self, data_source, identifier_type, identifier,
                novelist_api=None):
        """Serve a groups feed of books related to a given book."""

        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            return pool

        try:
            lane_name = "Books Related to %s by %s" % (
                pool.work.title, pool.work.author
            )
            lane = RelatedBooksLane(
                self._db, pool, lane_name, novelist_api=novelist_api
            )
        except ValueError, e:
            # No related books were found.
            return NO_SUCH_LANE.detailed(e.message)


        url = self.cdn_url_for(
            'related_books', data_source=data_source,
            identifier_type=identifier_type, identifier=identifier
        )
        annotator = self.manager.annotator(lane)
        feed = AcquisitionFeed.groups(
            self._db, lane.DISPLAY_NAME, url, lane, annotator=annotator
        )
        return feed_response(unicode(feed.content))

    def report(self, data_source, identifier_type, identifier):
        """Report a problem with a book."""
    
        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier_type, identifier)
        if isinstance(pool, ProblemDetail):
            # Something went wrong.
            return pool
    
        if flask.request.method == 'GET':
            # Return a list of valid URIs to use as the type of a problem detail
            # document.
            data = "\n".join(Complaint.VALID_TYPES)
            return Response(data, 200, {"Content-Type" : "text/uri-list"})
    
        data = flask.request.data
        controller = ComplaintController()
        return controller.register(pool, data)

    def series(self, series_name):
        """Serve a feed of books in the same series as a given book."""

        if not series_name:
            return NO_SUCH_LANE.detailed("No series provided")

        lane = SeriesLane(self._db, series_name)
        url = self.cdn_url_for('series', series_name=series_name)
        annotator = self.manager.annotator(lane)
        feed = AcquisitionFeed.page(
            self._db, lane.display_name, url, lane,
            annotator=annotator, cache_type=CachedFeed.SERIES_TYPE
        )

        return feed_response(unicode(feed.content))


class AnalyticsController(CirculationManagerController):

    def track_event(self, data_source, identifier_type, identifier, event_type):
        if event_type in [CirculationEvent.OPEN_BOOK]:
            pool = self.load_licensepool(data_source, identifier_type, identifier)
            Analytics.collect_event(self._db, pool, event_type, datetime.datetime.utcnow())
            return Response({}, 200)
        else:
            return INVALID_ANALYTICS_EVENT_TYPE


class ServiceStatusController(CirculationManagerController):

    template = """<!DOCTYPE HTML>
<html lang="en" class="">
<head>
<meta charset="utf8">
</head>
<body>
<ul>
%(statuses)s
</ul>
</body>
</html>
"""

    def __call__(self):
        service_status = ServiceStatus(self._db)
        timings = service_status.loans_status(response=True)
        statuses = []
        for k, v in sorted(timings.items()):
            statuses.append(" <li><b>%s</b>: %s</li>" % (k, v))

        doc = self.template % dict(statuses="\n".join(statuses))
        return Response(doc, 200, {"Content-Type": "text/html"})
