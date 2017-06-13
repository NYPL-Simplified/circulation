from nose.tools import set_trace
import json
import logging
import sys
import urllib
import datetime
from wsgiref.handlers import format_date_time
from time import mktime

from lxml import etree
from sqlalchemy.orm import eagerload

from functools import wraps
import flask
from flask import (
    make_response,
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
    ComplaintController,
    HeartbeatController,
    URNLookupController,
)
from core.external_search import (
    ExternalSearchIndex,
    DummyExternalSearchIndex,
)
from core.facets import FacetConfig
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
    Annotation,
    CachedFeed,
    CirculationEvent,
    Collection,
    Complaint,
    DataSource,
    Hold,
    Identifier,
    Library,
    LicensePool,
    Loan,
    LicensePoolDeliveryMechanism,
    production_session,
    PatronProfileStorage,
    Representation,
    Session,
    Work,
)
from core.opds import (
    AcquisitionFeed,
)
from core.util.opds_writer import (    
     OPDSFeed,
)
from core.opensearch import OpenSearchDocument
from core.user_profile import ProfileController as CoreProfileController
from core.util.flask_util import (
    problem,
)
from core.util.problem_detail import ProblemDetail
from core.util.http import (
    RemoteIntegrationException,
)

from circulation_exceptions import *

from opds import (
    CirculationManagerAnnotator,
    CirculationManagerLoanAndHoldAnnotator,
)
from annotations import (
  AnnotationWriter,
  AnnotationParser,
)
from problem_details import *

from authenticator import (
    Authenticator,
    OAuthController,
)
from config import (
    Configuration, 
    CannotLoadConfiguration,
)

from lanes import (
    make_lanes,
    ContributorLane,
    RecommendationLane,
    RelatedBooksLane,
    SeriesLane,
)

from adobe_vendor_id import (
    AdobeVendorIDController,
    DeviceManagementProtocolController,
    AuthdataUtility,
)
from axis import Axis360API
from overdrive import OverdriveAPI
from bibliotheca import BibliothecaAPI
from circulation import CirculationAPI
from novelist import (
    NoveListAPI,
    MockNoveListAPI,
)
from base_controller import BaseCirculationManagerController
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
        self.auth = Authenticator(self._db)
        # Track the Lane configuration for each library by mapping its
        # short name to the top-level lane.
        self.top_level_lanes = {}

        # Create a CirculationAPI for each library.
        self.circulation_apis = {}

        for library in _db.query(Library):
            if isinstance(lanes, LaneList):
                if not self.testing:
                    raise CannotLoadConfiguration(
                        "Cannot pass Lane objects into CirculationManager when not in testing mode."
                    )
                lanes = lanes
                # TODO: This is no longer supported since different
                # libraries need different Lane objects, even if the
                # lane layout is the same.
            else:
                lanes = make_lanes(library, lanes)
            
            self.top_level_lanes[library.id] = (
                self.create_top_level_lane(
                    library, lanes
                )
            )

            self.circulation_apis[library.id] = self.setup_circulation(
                library
            )
            
        self.__external_search = None
        self.lending_policy = load_lending_policy(
            Configuration.policy('lending', {})
        )

        self.setup_controllers()
        self.setup_adobe_vendor_id()

        self.opds_authentication_documents = {}

    @property
    def library(self):
        return get_one(self._db, Library, id=self.library_id)
    
    @property
    def external_search(self):
        """Retrieve or create a connection to the search interface.

        This is created lazily so that a failure to connect only
        affects searches, not the rest of the circulation manager.
        """
        if not self.__external_search:
            self.__external_search = self.setup_search()
        return self.__external_search

    def create_top_level_lane(self, library, lanelist):
        name = 'All Books'
        return Lane(
            library, name,
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

    def setup_circulation(self, library):
        """Set up the Circulation object."""        
        if self.testing:
            cls = MockCirculationAPI
        else:
            cls = CirculationAPI
        return cls(library)
        
    def setup_controllers(self):
        """Set up all the controllers that will be used by the web app."""
        self.index_controller = IndexController(self)
        self.opds_feeds = OPDSFeedController(self)
        self.loans = LoanController(self)
        self.annotations = AnnotationController(self)
        self.urn_lookup = URNLookupController(self._db)
        self.work_controller = WorkController(self)
        self.analytics_controller = AnalyticsController(self)
        self.oauth_controller = OAuthController(self.auth)
        self.profiles = ProfileController(self)
        
        self.heartbeat = HeartbeatController()
        self.service_status = ServiceStatusController(self)

    def setup_adobe_vendor_id(self):
        """Set up the controllers for Adobe Vendor ID and our Adobe endpoint
        for the DRM Device Management Protocol.
        """
        adobe = Configuration.integration(
            Configuration.ADOBE_VENDOR_ID_INTEGRATION
        )

        # Relatively few libraries will have this setup.
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
            self.log.warn("Adobe Vendor ID controller is disabled due to missing or incomplete configuration. This is probably nothing to worry about.")
            self.adobe_vendor_id = None

        # But almost all libraries will have this setup.
        library = Library.instance(self._db)
        if library.library_registry_shared_secret:
            try:
                authdata = AuthdataUtility.from_config(self._db)
                self.adobe_device_management = DeviceManagementProtocolController(self)
            except CannotLoadConfiguration, e:
                self.log.warn("DRM Device Management Protocol controller is disabled due to missing or incomplete Adobe configuration. This may be cause for concern.")

            
    def annotator(self, lane, *args, **kwargs):
        """Create an appropriate OPDS annotator for the given lane."""
        return CirculationManagerAnnotator(
            self.circulation, lane, flask.request.library, top_level_title='All Books',
            *args, **kwargs
        )


class CirculationManagerController(BaseCirculationManagerController):

    @property
    def circulation(self):
        """Return the appropriate CirculationAPI for the request Library."""
        library_id = flask.request.library.id
        return self.manager.circulation_apis[library_id]
    
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

    def load_work(self, library, identifier_type, identifier):
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools

        # We know there is at least one LicensePool, and all LicensePools
        # for an Identifier have the same Work.
        return pools[0].work
    
    def load_licensepools(self, library, identifier_type, identifier):
        """Turn user input into one or more LicensePool objects.

        :param library: The LicensePools must be associated with one of this
            Library's Collections.
        :param identifier_type: A type of identifier, e.g. "ISBN"
        :param identifier: An identifier string, used with `identifier_type`
            to look up an Identifier.
        """
        _db = Session.object_session(library)
        pools = _db.query(LicensePool).join(LicensePool.collection).join(
            LicensePool.identifier).join(Collection.libraries).filter(
                Identifier.type==identifier_type
            ).filter(
                Identifier.identifier==identifier
            ).filter(
                Library.id==library.id
            ).all()
        if not pools:
            return NO_LICENSES.detailed(
                _("The item you're asking about (%s/%s) isn't in this collection.") % (
                    identifier_type, identifier
                )
            )
        return pools

    def load_licensepool(self, license_pool_id):
        """Turns user input into a LicensePool"""
        license_pool = get_one(self._db, LicensePool, id=license_pool_id)
        if not license_pool:
            return INVALID_INPUT.detailed(
                _("License Pool #%d does not exist.") % license_pool_id
            )
        return license_pool

    def load_licensepooldelivery(self, pool, mechanism_id):
        """Turn user input into a LicensePoolDeliveryMechanism object.""" 
        mechanism = get_one(
            self._db, LicensePoolDeliveryMechanism,
            data_source=pool.data_source, identifier=pool.identifier,
            delivery_mechanism_id=mechanism_id, on_multiple='interchangeable'
        )
        return mechanism or BAD_DELIVERY_MECHANISM

    def apply_borrowing_policy(self, patron, license_pool):
        if isinstance(patron, ProblemDetail):
            return patron
        if not patron.can_borrow(license_pool.work, self.manager.lending_policy):
            return FORBIDDEN_BY_POLICY.detailed(
                _("Library policy prohibits us from lending you this book."),
                status_code=451
            )

        if (not patron.library.allow_holds and
            license_pool.licenses_available == 0 and
            not license_pool.open_access
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
        library_short_name = flask.request.library.short_name
        policy = Configuration.root_lane_policy()
        if not policy:
            return redirect(self.cdn_url_for('acquisition_groups', library_short_name=library_short_name))

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
        library_short_name = flask.request.library.short_name
        root_lane = self.authenticated_patron_root_lane()
        if isinstance(root_lane, ProblemDetail):
            return root_lane
        if isinstance(root_lane, Response):
            return root_lane
        if root_lane is None:
            return redirect(
                self.cdn_url_for(
                    'acquisition_groups',
                    library_short_name=library_short_name,
                )
            )
    
        return redirect(
            self.cdn_url_for(
                'acquisition_groups', 
                library_short_name=library_short_name,
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
        library_short_name = flask.request.library.short_name
        url = self.cdn_url_for(
            "acquisition_groups", languages=languages, lane_name=lane_name, library_short_name=library_short_name,
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
        library_short_name = flask.request.library.short_name
        url = self.cdn_url_for(
            "feed", languages=languages, lane_name=lane_name,
            library_short_name=library_short_name,
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
        library_short_name = flask.request.library.short_name
        this_url = self.url_for(
            'lane_search', languages=languages, lane_name=lane_name,
            library_short_name=library_short_name,
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


class LoanController(CirculationManagerController):

    def get_patron_circ_objects(self, object_class, patron, license_pools):
        pool_ids = [pool.id for pool in license_pools]

        return self._db.query(object_class).filter(
            object_class.patron_id==patron.id,
            object_class.license_pool_id.in_(pool_ids)
        ).options(eagerload(object_class.license_pool)).all()

    def get_patron_loan(self, patron, license_pools):
        loans = self.get_patron_circ_objects(Loan, patron, license_pools)
        if loans:
            loan = loans[0]
            return loan, loan.license_pool
        return None, None

    def get_patron_hold(self, patron, license_pools):
        holds = self.get_patron_circ_objects(Hold, patron, license_pools)
        if holds:
            hold = holds[0]
            return hold, hold.license_pool
        return None, None

    def sync(self):
        if flask.request.method=='HEAD':
            return Response()

        patron = flask.request.patron

        # First synchronize our local list of loans and holds with all
        # third-party loan providers.
        if patron.authorization_identifier:
            header = self.authorization_header()
            credential = self.manager.auth.get_credential_from_header(header)
            try:
                self.circulation.sync_bookshelf(patron, credential)
            except Exception, e:
                # If anything goes wrong, omit the sync step and just
                # display the current active loans, as we understand them.
                self.manager.log.error("ERROR DURING SYNC: %r", e, exc_info=e)

        # Then make the feed.
        feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            self.circulation, patron)
        return feed_response(feed, cache_for=None)

    def borrow(self, identifier_type, identifier, mechanism_id=None):
        """Create a new loan or hold for a book.

        Return an OPDS Acquisition feed that includes a link of rel
        "http://opds-spec.org/acquisition", which can be used to fetch the
        book or the license file.
        """
        patron = flask.request.patron
        library = flask.request.library

        result = self.best_lendable_pool(
            library, patron, identifier_type, identifier, mechanism_id
        )
        if not result:
            # No LicensePools were found and no ProblemDetail
            # was returned. Send a generic ProblemDetail.
            return NO_LICENSES.detailed(
                _("I've never heard of this work.")
            )
        if isinstance(result, ProblemDetail):
            return result
        pool, mechanism = result

        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)
        problem_doc = None

        try:
            loan, hold, is_new = self.circulation.borrow(
                patron, credential, pool, mechanism
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
                _("You must pay your $%(fine_amount).2f outstanding fines before you can borrow more books.", fine_amount=patron.fines)
            )
        except AuthorizationExpired, e:
            return e.as_problem_detail_document(debug=False)
        except AuthorizationBlocked, e:
            return e.as_problem_detail_document(debug=False)
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

    def best_lendable_pool(self, library, patron, identifier_type, identifier,
                           mechanism_id):
        """Of the available LicensePools for the given Identifier, return the
        one that's the best candidate for loaning out right now.
        """
        # Turn source + identifier into a set of LicensePools
        pools = self.load_licensepools(
            library, identifier_type, identifier
        )
        if isinstance(pools, ProblemDetail):
            # Something went wrong.
            return pools

        best = None
        mechanism = None
        problem_doc = None

        existing_loans = self._db.query(Loan).filter(
            Loan.license_pool_id.in_([lp.id for lp in pools]),
            Loan.patron==patron
        ).all()
        if existing_loans:
            return ALREADY_CHECKED_OUT

        # We found a number of LicensePools. Try to locate one that
        # we can actually loan to the patron.
        for pool in pools:
            problem_doc = self.apply_borrowing_policy(patron, pool)
            if problem_doc:
                # As a matter of policy, the patron is not allowed to borrow
                # this book.
                continue

            # Beyond this point we know that site policy does not prohibit
            # us from lending this pool to this patron.

            if mechanism_id:
                # But the patron has requested a license pool that
                # supports a specific delivery mechanism. This pool
                # must offer that mechanism.
                mechanism = self.load_licensepooldelivery(pool, mechanism_id)
                if isinstance(mechanism, ProblemDetail):
                    problem_doc = mechanism
                    continue

            # Beyond this point we have a license pool that we can
            # actually loan or put on hold.

            # But there might be many such LicensePools, and we want
            # to pick the one that will get the book to the patron
            # with the shortest wait.
            if (not best
                or pool.licenses_available > best.licenses_available
                or pool.patrons_in_hold_queue < best.patrons_in_hold_queue):
                best = pool

        if not best:
            # We were unable to find any LicensePool that fit the
            # criteria.
            return problem_doc
        return best, mechanism
    
    def fulfill(self, license_pool_id, mechanism_id=None, do_get=None):
        """Fulfill a book that has already been checked out.

        If successful, this will serve the patron a downloadable copy of
        the book, or a DRM license file which can be used to get the
        book). Alternatively, for a streaming delivery mechanism it may
        serve an OPDS entry with a link to a third-party web page that
        streams the content.
        """
        do_get = do_get or Representation.simple_http_get

        patron = flask.request.patron
        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)
    
        # Turn source + identifier into a LicensePool.
        pool = self.load_licensepool(license_pool_id)
        if isinstance(pool, ProblemDetail):
            return pool

        loan, loan_license_pool = self.get_patron_loan(patron, [pool])
        
        # Find the LicensePoolDeliveryMechanism they asked for.
        mechanism = None
        if mechanism_id:
            mechanism = self.load_licensepooldelivery(
                loan_license_pool, mechanism_id
            )
            if isinstance(mechanism, ProblemDetail):
                return mechanism
            
        if not mechanism:
            # See if the loan already has a mechanism set. We can use that.
            if loan and loan.fulfillment:
                mechanism =  loan.fulfillment
            else:
                return BAD_DELIVERY_MECHANISM.detailed(
                    _("You must specify a delivery mechanism to fulfill this loan.")
                )
    
        try:
            fulfillment = self.circulation.fulfill(
                patron, credential, loan.license_pool, mechanism
            )
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
        if mechanism.delivery_mechanism.is_streaming:
            # If this is a streaming delivery mechanism, create an OPDS entry
            # with a fulfillment link to the streaming reader url.
            feed = CirculationManagerLoanAndHoldAnnotator.single_fulfillment_feed(
                self.circulation, loan, fulfillment)
            if isinstance(feed, OPDSFeed):
                content = unicode(feed)
            else:
                content = etree.tostring(feed)
            status_code = 200
            headers["Content-Type"] = OPDSFeed.ACQUISITION_FEED_TYPE
        else:
            content = fulfillment.content
            if fulfillment.content_link:
                # If we have a link to the content on a remote server, web clients may not
                # be able to access it if the remote server does not support CORS requests.
                # We need to fetch the content and return it instead of redirecting to it.
                try:
                    status_code, headers, content = do_get(fulfillment.content_link, headers={})
                    headers = dict(headers)
                except RemoteIntegrationException, e:
                    return e.as_problem_detail_document(debug=False)
            else:
                status_code = 200
            if fulfillment.content_type:
                headers['Content-Type'] = fulfillment.content_type

        return Response(content, status_code, headers)

    def revoke(self, license_pool_id):
        patron = flask.request.patron
        pool = self.load_licensepool(license_pool_id)
        if isinstance(pool, ProblemDetail):
            return pool

        loan, _ignore = self.get_patron_loan(patron, [pool])

        if loan:
            hold = None
        else:
            hold, _ignore = self.get_patron_hold(patron, [pool])

        if not loan and not hold:
            if not pool.work:
                title = 'this book'
            else:
                title = '"%s"' % pool.work.title
            return NO_ACTIVE_LOAN_OR_HOLD.detailed(
                _('Can\'t revoke because you have no active loan or hold for "%(title)s".', title=title),
                status_code=404
            )

        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)
        if loan:
            try:
                self.circulation.revoke_loan(patron, credential, pool)
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
                self.circulation.release_hold(patron, credential, pool)
            except CannotReleaseHold, e:
                title = _("Hold released locally but remote failed.")
                return CANNOT_RELEASE_HOLD.detailed(title, 503).with_debug(str(e))

        work = pool.work
        annotator = self.manager.annotator(None)
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator)
        )

    def detail(self, identifier_type, identifier):
        if flask.request.method=='DELETE':
            return self.revoke_loan_or_hold(identifier_type, identifier)

        patron = flask.request.patron
        library = flask.request.library
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools

        loan, pool = self.get_patron_loan(patron, pools)
        if loan:
            hold = None
        else:
            hold, pool = self.get_patron_hold(patron, pools)

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

class AnnotationController(CirculationManagerController):

    def container(self, identifier=None, accept_post=True):
        headers = dict()
        if accept_post:
            headers['Allow'] = 'GET,HEAD,OPTIONS,POST'
            headers['Accept-Post'] = AnnotationWriter.CONTENT_TYPE
        else:
            headers['Allow'] = 'GET,HEAD,OPTIONS'

        if flask.request.method=='HEAD':
            return Response(status=200, headers=headers)

        patron = flask.request.patron

        if flask.request.method == 'GET':
            headers['Link'] = ['<http://www.w3.org/ns/ldp#BasicContainer>; rel="type"',
                               '<http://www.w3.org/TR/annotation-protocol/>; rel="http://www.w3.org/ns/ldp#constrainedBy"']
            headers['Content-Type'] = AnnotationWriter.CONTENT_TYPE

            container, timestamp = AnnotationWriter.annotation_container_for(patron, identifier=identifier)
            etag = 'W/""'
            if timestamp:
                etag = 'W/"%s"' % timestamp
                headers['Last-Modified'] = format_date_time(mktime(timestamp.timetuple()))
            headers['ETag'] = etag

            content = json.dumps(container)
            return Response(content, status=200, headers=headers)

        data = flask.request.data
        annotation = AnnotationParser.parse(self._db, data, patron)

        if isinstance(annotation, ProblemDetail):
            return annotation

        content = json.dumps(AnnotationWriter.detail(annotation))
        status_code = 200
        headers['Link'] = '<http://www.w3.org/ns/ldp#Resource>; rel="type"'
        headers['Content-Type'] = AnnotationWriter.CONTENT_TYPE
        return Response(content, status_code, headers)

    def container_for_work(self, identifier_type, identifier):
        id_obj, ignore = Identifier.for_foreign_id(
            self._db, identifier_type, identifier)
        return self.container(identifier=id_obj, accept_post=False)
 
    def detail(self, annotation_id):
        headers = dict()
        headers['Allow'] = 'GET,HEAD,OPTIONS,DELETE'

        if flask.request.method=='HEAD':
            return Response(status=200, headers=headers)

        patron = flask.request.patron

        annotation = get_one(
            self._db, Annotation,
            patron=patron,
            id=annotation_id,
            active=True)

        if not annotation:
            return NO_ANNOTATION

        if flask.request.method == 'DELETE':
            annotation.set_inactive()
            return Response()

        content = json.dumps(AnnotationWriter.detail(annotation))
        status_code = 200
        headers['Link'] = '<http://www.w3.org/ns/ldp#Resource>; rel="type"'
        headers['Content-Type'] = AnnotationWriter.CONTENT_TYPE
        return Response(content, status_code, headers)


class WorkController(CirculationManagerController):

    def _lane_details(self, languages, audiences):
        if languages:
            languages = languages.split(',')
        if audiences:
            audiences = [urllib.unquote_plus(a) for a in audiences.split(',')]

        return languages, audiences

    def contributor(self, contributor_name, languages, audiences):
        """Serve a feed of books written by a particular author"""

        if not contributor_name:
            return NO_SUCH_LANE.detailed(_("No contributor provided"))

        languages, audiences = self._lane_details(languages, audiences)

        lane = ContributorLane(
            self.manager.library, contributor_name, languages=languages, audiences=audiences
        )

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request()
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        feed = AcquisitionFeed.page(
            self._db, lane.display_name, url, lane,
            facets=facets, pagination=pagination,
            annotator=annotator, cache_type=CachedFeed.CONTRIBUTOR_TYPE
        )
        return feed_response(unicode(feed.content))

    def permalink(self, identifier_type, identifier):
        """Serve an entry for a single book.

        This does not include any loan or hold-specific information for
        the authenticated patron.

        This is different from the /works lookup protocol, in that it
        returns a single entry while the /works lookup protocol returns a
        feed containing any number of entries.
        """

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        annotator = self.manager.annotator(None)
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator)
        )

    def related(self, identifier_type, identifier, novelist_api=None):
        """Serve a groups feed of books related to a given book."""

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        try:
            lane_name = "Books Related to %s by %s" % (
                work.title, work.author
            )
            lane = RelatedBooksLane(
                self.manager.library, work, lane_name, novelist_api=novelist_api
            )
        except ValueError, e:
            # No related books were found.
            return NO_SUCH_LANE.detailed(e.message)

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request()
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        feed = AcquisitionFeed.groups(
            self._db, lane.DISPLAY_NAME, url, lane, annotator=annotator
        )
        return feed_response(unicode(feed.content))

    def recommendations(self, identifier_type, identifier, novelist_api=None):
        """Serve a feed of recommendations related to a given book."""

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        lane_name = "Recommendations for %s by %s" % (work.title, work.author)
        try:
            lane = RecommendationLane(
                self.manager.library, work, lane_name, novelist_api=novelist_api
            )
        except ValueError, e:
            # NoveList isn't configured.
            return NO_SUCH_LANE.detailed(_("Recommendations not available"))

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request()
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        feed = AcquisitionFeed.page(
            self._db, lane.DISPLAY_NAME, url, lane,
            facets=facets, pagination=pagination,
            annotator=annotator, cache_type=CachedFeed.RECOMMENDATIONS_TYPE
        )
        return feed_response(unicode(feed.content))

    def report(self, identifier_type, identifier):
        """Report a problem with a book."""

        # TODO: We don't have a reliable way of knowing whether the
        # complaing is being lodged against the work or against a
        # specific LicensePool.

        # Turn source + identifier into a set of LicensePools
        library = flask.request.library
        pools = self.load_licensepools(library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            # Something went wrong.
            return pools
    
        if flask.request.method == 'GET':
            # Return a list of valid URIs to use as the type of a problem detail
            # document.
            data = "\n".join(Complaint.VALID_TYPES)
            return Response(data, 200, {"Content-Type" : "text/uri-list"})
    
        data = flask.request.data
        controller = ComplaintController()
        return controller.register(pools[0], data)

    def series(self, series_name, languages, audiences):
        """Serve a feed of books in the same series as a given book."""

        if not series_name:
            return NO_SUCH_LANE.detailed(_("No series provided"))

        languages, audiences = self._lane_details(languages, audiences)
        lane = SeriesLane(self.manager.library, series_name=series_name,
                          languages=languages, audiences=audiences
        )
        annotator = self.manager.annotator(lane)

        # In addition to the orderings enabled for this library, a
        # series collection may be ordered by series position, and is
        # ordered that way by default.
        facet_config = FacetConfig.from_library(self.manager.library)
        facet_config.set_default_facet(
            Facets.ORDER_FACET_GROUP_NAME, Facets.ORDER_SERIES_POSITION
        )
        
        facets = load_facets_from_request(facet_config=facet_config)
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        feed = AcquisitionFeed.page(
            self._db, lane.display_name, url, lane,
            facets=facets, pagination=pagination,
            annotator=annotator, cache_type=CachedFeed.SERIES_TYPE
        )
        return feed_response(unicode(feed.content))


class ProfileController(CirculationManagerController):
    """Implement the User Profile Management Protocol."""

    @property
    def _controller(self):
        """Instantiate a CoreProfileController that actually does the work.
        """
        patron = self.authenticated_patron_from_request()
        storage = PatronProfileStorage(patron)
        return CoreProfileController(storage)
        
    def protocol(self):
        """Handle a UPMP request."""
        controller = self._controller
        if flask.request.method == 'GET':
            result = controller.get()
        else:
            result = controller.put(flask.request.headers, flask.request.data)
        if isinstance(result, ProblemDetail):
            return result
        return make_response(*result)


class AnalyticsController(CirculationManagerController):

    def track_event(self, identifier_type, identifier, event_type):
        # TODO: It usually doesn't matter, but there should be
        # a way to distinguish between different LicensePools for the
        # same book.
        if event_type in CirculationEvent.CLIENT_EVENTS:
            library = flask.request.library
            pools = self.load_licensepools(library, identifier_type, identifier)
            if isinstance(pools, ProblemDetail):
                return pools
            Analytics.collect_event(self._db, pools[0], event_type, datetime.datetime.utcnow())
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
        service_status = ServiceStatus(self._db, flask.request.library)
        timings = service_status.loans_status(response=True)
        statuses = []
        for k, v in sorted(timings.items()):
            statuses.append(" <li><b>%s</b>: %s</li>" % (k, v))

        doc = self.template % dict(statuses="\n".join(statuses))
        return Response(doc, 200, {"Content-Type": "text/html"})
