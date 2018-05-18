from nose.tools import set_trace
import json
import logging
import sys
import urllib
import datetime
import base64
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
from flask_babel import lazy_gettext as _

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
from core.log import LogConfiguration
from core.lane import (
    Facets,
    FeaturedFacets,
    Pagination,
    Lane,
    SearchFacets,
    WorkList,
)
from core.model import (
    get_one,
    get_one_or_create,
    production_session,
    Admin,
    Annotation,
    CachedFeed,
    CirculationEvent,
    Collection,
    Complaint,
    ConfigurationSetting,
    CustomList,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hold,
    Identifier,
    IntegrationClient,
    Library,
    LicensePool,
    Loan,
    LicensePoolDeliveryMechanism,
    Patron,
    PatronProfileStorage,
    Representation,
    Session,
    Work,
)
from core.opds import (
    AcquisitionFeed,
)
from core.util import LanguageCodes
from core.util.opds_writer import (
     OPDSFeed,
)
from core.opensearch import OpenSearchDocument
from core.user_profile import ProfileController as CoreProfileController
from core.util.flask_util import (
    problem,
)
from core.util.authentication_for_opds import AuthenticationForOPDSDocument
from core.util.problem_detail import ProblemDetail
from core.util.http import (
    HTTP,
    RemoteIntegrationException,
)

from circulation_exceptions import *
from custom_index import CustomIndexView

from opds import (
    CirculationManagerAnnotator,
    LibraryAnnotator,
    SharedCollectionAnnotator,
    LibraryLoanAndHoldAnnotator,
    SharedCollectionLoanAndHoldAnnotator,
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
    load_lanes,
    ContributorLane,
    FeaturedSeriesFacets,
    RecommendationLane,
    RelatedBooksLane,
    SeriesLane,
    CrawlableCollectionBasedLane,
    CrawlableCustomListBasedLane,
    CrawlableFacets,
)

from adobe_vendor_id import (
    AdobeVendorIDController,
    DeviceManagementProtocolController,
    AuthdataUtility,
)
from circulation import CirculationAPI
from shared_collection import SharedCollectionAPI
from odl import ODLWithConsolidatedCopiesAPI
from novelist import (
    NoveListAPI,
    MockNoveListAPI,
)
from base_controller import BaseCirculationManagerController
from testing import MockCirculationAPI, MockSharedCollectionAPI
from services import ServiceStatus
from core.analytics import Analytics
from accept_types import parse_header

class CirculationManager(object):

    def __init__(self, _db, testing=False):

        self.log = logging.getLogger("Circulation manager web app")
        self._db = _db

        if not testing:
            try:
                self.config = Configuration.load(_db)
            except CannotLoadConfiguration, e:
                self.log.error("Could not load configuration file: %s" % e)
                sys.exit()

        self.testing = testing
        self.site_configuration_last_update = (
            Configuration.site_configuration_last_update(self._db, timeout=0)
        )
        self.setup_one_time_controllers()
        self.load_settings()

    def reload_settings_if_changed(self):
        """If the site configuration has been updated, reload the
        CirculationManager's configuration from the database.
        """
        last_update = Configuration.site_configuration_last_update(self._db)
        if last_update > self.site_configuration_last_update:
            self.load_settings()
            self.site_configuration_last_update = last_update

    def load_settings(self):
        """Load all necessary configuration settings and external
        integrations from the database.

        This is called once when the CirculationManager is
        initialized.  It may also be called later to reload the site
        configuration after changes are made in the administrative
        interface.
        """
        LogConfiguration.initialize(self._db)
        self.analytics = Analytics(self._db)
        self.auth = Authenticator(self._db, self.analytics)

        self.setup_external_search()

        # Track the Lane configuration for each library by mapping its
        # short name to the top-level lane.
        new_top_level_lanes = {}
        # Create a CirculationAPI for each library.
        new_circulation_apis = {}

        # Potentially load a CustomIndexView for each library
        new_custom_index_views = {}

        new_adobe_device_management = None
        for library in self._db.query(Library):
            lanes = load_lanes(self._db, library)

            new_top_level_lanes[library.id] = lanes

            new_custom_index_views[library.id] = CustomIndexView.for_library(
                library
            )

            new_circulation_apis[library.id] = self.setup_circulation(
                library, self.analytics
            )
            authdata = self.setup_adobe_vendor_id(self._db, library)
            if authdata and not new_adobe_device_management:
                # There's at least one library on this system that
                # wants Vendor IDs. This means we need to advertise support
                # for the Device Management Protocol.
                new_adobe_device_management = DeviceManagementProtocolController(self)
        self.adobe_device_management = new_adobe_device_management
        self.top_level_lanes = new_top_level_lanes
        self.circulation_apis = new_circulation_apis
        self.custom_index_views = new_custom_index_views
        self.shared_collection_api = self.setup_shared_collection()
        self.lending_policy = load_lending_policy(
            Configuration.policy('lending', {})
        )

        self.patron_web_client_url = ConfigurationSetting.sitewide(
            self._db, Configuration.PATRON_WEB_CLIENT_URL).value

        self.setup_configuration_dependent_controllers()
        self.authentication_for_opds_documents = {}

    @property
    def external_search(self):
        """Retrieve or create a connection to the search interface.

        This is created lazily so that a failure to connect only
        affects searches, not the rest of the circulation manager.
        """
        if not self.__external_search:
            self.setup_external_search()
        return self.__external_search

    def setup_external_search(self):
        try:
            self.__external_search = self.setup_search()
            self.external_search_initialization_exception = None
        except CannotLoadConfiguration, e:
            self.log.error(
                "Exception loading search configuration: %s", e
            )
            self.__external_search = None
            self.external_search_initialization_exception = e
        return self.__external_search

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
            search = ExternalSearchIndex(self._db)
            if not search:
                self.log.warn("No external search server configured.")
                return None
            return search

    def setup_circulation(self, library, analytics):
        """Set up the Circulation object."""
        if self.testing:
            cls = MockCirculationAPI
        else:
            cls = CirculationAPI
        return cls(self._db, library, analytics)

    def setup_shared_collection(self):
        if self.testing:
            cls = MockSharedCollectionAPI
        else:
            cls = SharedCollectionAPI
        return cls(self._db)

    def setup_one_time_controllers(self):
        """Set up all the controllers that will be used by the web app.

        This method will be called only once, no matter how many times the
        site configuration changes.
        """
        self.index_controller = IndexController(self)
        self.opds_feeds = OPDSFeedController(self)
        self.loans = LoanController(self)
        self.annotations = AnnotationController(self)
        self.urn_lookup = URNLookupController(self._db)
        self.work_controller = WorkController(self)
        self.analytics_controller = AnalyticsController(self)
        self.profiles = ProfileController(self)
        self.heartbeat = HeartbeatController()
        self.service_status = ServiceStatusController(self)
        self.odl_notification_controller = ODLNotificationController(self)
        self.shared_collection_controller = SharedCollectionController(self)

    def setup_configuration_dependent_controllers(self):
        """Set up all the controllers that depend on the
        current site configuration.

        This method will be called fresh every time the site
        configuration changes.
        """
        self.oauth_controller = OAuthController(self.auth)

    def setup_adobe_vendor_id(self, _db, library):
        """If this Library has an Adobe Vendor ID integration,
        configure the controller for it.

        :return: An Authdata object for `library`, if one could be created.
        """
        short_client_token_initialization_exceptions = dict()
        adobe = ExternalIntegration.lookup(
            _db, ExternalIntegration.ADOBE_VENDOR_ID,
            ExternalIntegration.DRM_GOAL, library=library
        )
        warning = (
            'Adobe Vendor ID controller is disabled due to missing or'
            ' incomplete configuration. This is probably nothing to'
            ' worry about.'
        )

        new_adobe_vendor_id = None
        if adobe:
            # Relatively few libraries will have this setup.
            vendor_id = adobe.username
            node_value = adobe.password
            if vendor_id and node_value:
                if new_adobe_vendor_id:
                    self.log.warn(
                        "Multiple libraries define an Adobe Vendor ID integration. This is not supported and the last library seen will take precedence."
                    )
                new_adobe_vendor_id = AdobeVendorIDController(
                    _db,
                    library,
                    vendor_id,
                    node_value,
                    self.auth
                )
            else:
                self.log.warn("Adobe Vendor ID controller is disabled due to missing or incomplete configuration. This is probably nothing to worry about.")
        if new_adobe_vendor_id:
            self.adobe_vendor_id = new_adobe_vendor_id

        # But almost all libraries will have a Short Client Token
        # setup. We're not setting anything up here, but this is useful
        # information for the calling code to have so it knows
        # whether or not we should support the Device Management Protocol.
        registry = ExternalIntegration.lookup(
            _db, ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL, library=library
        )
        authdata = None
        if registry:
            try:
                authdata = AuthdataUtility.from_config(library, _db)
            except CannotLoadConfiguration, e:
                short_client_token_initialization_exceptions[library.id] = e
                self.log.error(
                    "Short Client Token configuration for %s is present but not working. This may be cause for concern. Original error: %s",
                    library.name, e
                )
        self.short_client_token_initialization_exceptions = short_client_token_initialization_exceptions
        return authdata

    def annotator(self, lane, *args, **kwargs):
        """Create an appropriate OPDS annotator for the given lane."""
        if lane and isinstance(lane, Lane):
            library = lane.library
        elif lane and isinstance(lane, WorkList):
            library = lane.get_library(self._db)
        else:
            library = flask.request.library

        # Some features are only available if a patron authentication
        # mechanism is set up for this library.
        authenticator = self.auth.library_authenticators.get(library.short_name)
        return LibraryAnnotator(
            self.circulation_apis[library.id], lane, library,
            top_level_title='All Books',
            library_identifies_patrons=authenticator.identifies_individuals,
            *args, **kwargs
        )

    @property
    def authentication_for_opds_document(self):
        """Make sure the current request's library has an Authentication For
        OPDS document in the cache, then return the cached version.
        """
        name = flask.request.library.short_name
        if name not in self.authentication_for_opds_documents:
            self.authentication_for_opds_documents[name] = self.auth.create_authentication_document()
        return self.authentication_for_opds_documents[name]

    @property
    def public_key_integration_document(self):
        site_id = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY).value
        document = dict(id=site_id)

        public_key_dict = dict()
        public_key = ConfigurationSetting.sitewide(self._db, Configuration.PUBLIC_KEY).value
        if public_key:
            public_key_dict['type'] = 'RSA'
            public_key_dict['value'] = public_key

        document['public_key'] = public_key_dict
        return json.dumps(document)


class CirculationManagerController(BaseCirculationManagerController):

    @property
    def circulation(self):
        """Return the appropriate CirculationAPI for the request Library."""
        library_id = flask.request.library.id
        return self.manager.circulation_apis[library_id]

    @property
    def shared_collection(self):
        """Return the appropriate SharedCollectionAPI for the request library."""
        return self.manager.shared_collection_api

    def load_lane(self, lane_identifier):
        """Turn user input into a Lane object."""
        library_id = flask.request.library.id

        if lane_identifier is None:
            # Return the top-level lane.
            lane = self.manager.top_level_lanes[library_id]
            if isinstance(lane, Lane):
                lane = self._db.merge(lane)
            elif isinstance(lane, WorkList):
                lane.children = [self._db.merge(child) for child in lane.children]
        else:
            lane = get_one(
                self._db, Lane, id=lane_identifier, library_id=library_id
            )

        if not lane:
            return NO_SUCH_LANE.detailed(
                _("Lane %(lane_identifier)s does not exist or is not associated with library %(library_id)s",
                  lane_identifier=lane_identifier, library_id=library_id
                )
            )

        return lane

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
                _("License Pool #%s does not exist.") % license_pool_id
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
        # If this library provides a custom index view, use that.
        library = flask.request.library
        custom = self.manager.custom_index_views.get(library.id)
        if custom is not None:
            annotator = self.manager.annotator(None)
            return custom(library, annotator)

        # The simple case: the app is equally open to all clients.
        library_short_name = flask.request.library.short_name
        if not self.has_root_lanes():
            return redirect(self.cdn_url_for('acquisition_groups', library_short_name=library_short_name))

        # The more complex case. We must authorize the patron, check
        # their type, and redirect them to an appropriate feed.
        return self.appropriate_index_for_patron_type()

    def authentication_document(self):
        """Serve this library's Authentication For OPDS document."""
        return Response(
            self.manager.authentication_for_opds_document,
            200,
            {
                "Content-Type" : AuthenticationForOPDSDocument.MEDIA_TYPE
            }
        )

    def has_root_lanes(self):
        root_lanes = self._db.query(Lane).filter(
            Lane.library==flask.request.library
        ).filter(
            Lane.root_for_patron_type!=None
        )
        return root_lanes.count() > 0

    def authenticated_patron_root_lane(self):
        patron = self.authenticated_patron_from_request()
        if isinstance(patron, ProblemDetail):
            return patron
        if isinstance(patron, Response):
            return patron

        lanes = self._db.query(Lane).filter(
            Lane.library==flask.request.library
        ).filter(
            Lane.root_for_patron_type.any(patron.external_type)
        )
        if lanes.count() < 1:
            return None
        else:
            return lanes.one()

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
                lane_identifier=root_lane.id,
            )
        )

    def public_key_document(self):
        """Serves a sitewide public key document"""
        return Response(
            self.manager.public_key_integration_document,
            200, { 'Content-Type' : 'application/opds+json' }
        )


class OPDSFeedController(CirculationManagerController):

    def groups(self, lane_identifier):
        """Build or retrieve a grouped acquisition feed."""

        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane
        library = flask.request.library
        library_short_name = library.short_name
        url = self.cdn_url_for(
            "acquisition_groups", lane_identifier=lane_identifier, library_short_name=library_short_name,
        )

        title = lane.display_name
        facet_class_kwargs = dict(
            minimum_featured_quality=library.minimum_featured_quality,
            uses_customlists=lane.uses_customlists
        )
        facets = load_facets_from_request(
            worklist=lane, base_class=FeaturedFacets,
            base_class_constructor_kwargs=facet_class_kwargs
        )
        annotator = self.manager.annotator(lane)
        feed = AcquisitionFeed.groups(
            self._db, title, url, lane, annotator, facets=facets
        )
        return feed_response(feed)

    def feed(self, lane_identifier):
        """Build or retrieve a paginated acquisition feed."""

        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane
        library_short_name = flask.request.library.short_name
        url = self.cdn_url_for(
            "feed", lane_identifier=lane_identifier,
            library_short_name=library_short_name,
        )

        title = lane.display_name

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request(worklist=lane)
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
        return feed_response(feed)

    def crawlable_library_feed(self):
        """Build or retrieve a crawlable acquisition feed for the
        request library.
        """
        library = flask.request.library
        library_short_name = flask.request.library.short_name
        url = self.cdn_url_for(
            "crawlable_library_feed",
            library_short_name=library_short_name,
        )
        title = library.name
        lane = CrawlableCollectionBasedLane(library)
        return self._crawlable_feed(library, title, url, lane)

    def crawlable_collection_feed(self, collection_name):
        """Build or retrieve a crawlable acquisition feed for the
        requested collection.
        """
        collection = get_one(self._db, Collection, name=collection_name)
        if not collection:
            return NO_SUCH_COLLECTION
        title = collection.name
        url = self.cdn_url_for(
            "crawlable_collection_feed",
            collection_name=collection.name
        )
        lane = CrawlableCollectionBasedLane(None, [collection])
        if collection.protocol in [ODLWithConsolidatedCopiesAPI.NAME]:
            annotator = SharedCollectionAnnotator(collection, lane)
        else:
            annotator = CirculationManagerAnnotator(lane)
        return self._crawlable_feed(None, title, url, lane, annotator=annotator)

    def crawlable_list_feed(self, list_name):
        """Build or retrieve a crawlable, paginated acquisition feed for the
        named CustomList, sorted by update date.
        """
        library = flask.request.library
        list = CustomList.find(self._db, list_name, library=library)
        if not list:
            return NO_SUCH_LIST
        library_short_name = library.short_name
        title = list.name
        url = self.cdn_url_for(
            "crawlable_list_feed", list_name=list.name,
            library_short_name=library_short_name,
        )
        lane = CrawlableCustomListBasedLane()
        lane.initialize(library, list)
        return self._crawlable_feed(library, title, url, lane)

    def _crawlable_feed(self, library, title, url, lane, annotator=None):
        annotator = annotator or self.manager.annotator(lane)
        facets = CrawlableFacets.default(library)
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        feed = AcquisitionFeed.page(
            self._db, title, url, lane, annotator=annotator,
            facets=facets,
            pagination=pagination,
        )
        return feed_response(feed)

    def search(self, lane_identifier):

        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane
        query = flask.request.args.get('q')
        library_short_name = flask.request.library.short_name

        language_header = flask.request.headers.get("Accept-Language")
        if language_header:
            languages = parse_header(language_header)
            languages = map(str, languages)
            languages = map(LanguageCodes.iso_639_2_for_locale, languages)
            languages = [l for l in languages if l]
        else:
            languages = None

        facets = load_facets_from_request(
            worklist=lane, base_class=SearchFacets
        )
        kwargs = dict()
        if languages:
            kwargs['language'] = languages
        kwargs.update(dict(facets.items()))

        # Create a function that, when called, generates a URL to the
        # search controller.
        make_url = lambda: self.url_for(
            'lane_search', lane_identifier=lane_identifier,
            library_short_name=library_short_name,
            **kwargs
        )
        if not query:
            # Send the search form
            return OpenSearchDocument.for_lane(lane, make_url())

        pagination = load_pagination_from_request(default_size=Pagination.DEFAULT_SEARCH_SIZE)
        if isinstance(pagination, ProblemDetail):
            return pagination

        # Run a search.
        kwargs['q'] = query.encode("utf8")
        this_url = make_url()

        annotator = self.manager.annotator(lane)
        info = OpenSearchDocument.search_info(lane)
        opds_feed = AcquisitionFeed.search(
            _db=self._db, title=info['name'],
            url=this_url, lane=lane, search_engine=self.manager.external_search,
            query=query, annotator=annotator, pagination=pagination,
            languages=languages, facets=facets
        )
        return feed_response(opds_feed)


class LoanController(CirculationManagerController):

    def get_patron_circ_objects(self, object_class, patron, license_pools):
        if not patron:
            return []
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
                self.manager.log.error(
                    "ERROR DURING SYNC for %s: %r", patron.id, e, exc_info=e
                )

        # Then make the feed.
        feed = LibraryLoanAndHoldAnnotator.active_loans_for(
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
            feed = LibraryLoanAndHoldAnnotator.single_loan_feed(
                self.circulation, loan)
        elif hold:
            feed = LibraryLoanAndHoldAnnotator.single_hold_feed(
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
        """Fulfill a book that has already been checked out,
        or which can be fulfilled with no active loan.

        If successful, this will serve the patron a downloadable copy
        of the book, a key (such as a DRM license file or bearer
        token) which can be used to get the book, or an OPDS entry
        containing a link to the book.
        """
        do_get = do_get or Representation.simple_http_get

        # Unlike most controller methods, this one has different
        # behavior whether or not the patron is authenticated. This is
        # why we're about to do something we don't usually do--call
        # authenticated_patron_from_request from within a controller
        # method.
        authentication_response = self.authenticated_patron_from_request()
        if isinstance(authentication_response, Patron):
            # The patron is authenticated.
            patron = authentication_response
        else:
            # The patron is not authenticated, either due to bad credentials
            # (in which case authentication_response is a Response)
            # or due to an integration error with the auth provider (in
            # which case it is a ProblemDetail).
            #
            # There's still a chance this request can succeed, but if not,
            # we'll be sending out authentication_response.
            patron = None
        library = flask.request.library
        header = self.authorization_header()
        credential = self.manager.auth.get_credential_from_header(header)

        # Turn source + identifier into a LicensePool.
        pool = self.load_licensepool(license_pool_id)
        if isinstance(pool, ProblemDetail):
            return pool

        loan, loan_license_pool = self.get_patron_loan(patron, [pool])

        requested_license_pool = loan_license_pool or pool

        # Find the LicensePoolDeliveryMechanism they asked for.
        mechanism = None
        if mechanism_id:
            mechanism = self.load_licensepooldelivery(
                requested_license_pool, mechanism_id
            )
            if isinstance(mechanism, ProblemDetail):
                return mechanism

        if (not loan or not loan_license_pool) and not (
            self.can_fulfill_without_loan(
                library, patron, requested_license_pool, mechanism
            )
        ):
            if patron:
                # Since a patron was identified, the problem is they have
                # no active loan.
                return NO_ACTIVE_LOAN.detailed(
                    _("You have no active loan for this title.")
                )
            else:
                # Since no patron was identified, the problem is
                # whatever problem was revealed by the earlier
                # authenticated_patron_from_request() call -- either the
                # patron didn't authenticate or there's a problem
                # integrating with the auth provider.
                return authentication_response

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
                patron, credential, requested_license_pool, mechanism
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
        encoding_header = dict()
        if (fulfillment.data_source_name == DataSource.ENKI
            and mechanism.delivery_mechanism.drm_scheme_media_type == DeliveryMechanism.NO_DRM):
                encoding_header["Accept-Encoding"] = "deflate"

        if mechanism.delivery_mechanism.is_streaming:
            # If this is a streaming delivery mechanism, create an OPDS entry
            # with a fulfillment link to the streaming reader url.
            feed = LibraryLoanAndHoldAnnotator.single_fulfillment_feed(
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

                # If the pool is open access though, the web client can link directly to the
                # file to download it, so it's safe to redirect.
                if requested_license_pool.open_access:
                    return redirect(fulfillment.content_link)

                # Otherwise, we need to fetch the content and return it instead
                # of redirecting to it, since it may be downloaded through an
                # indirect acquisition link.
                try:
                    status_code, headers, content = do_get(fulfillment.content_link, headers=encoding_header)
                    headers = dict(headers)
                except RemoteIntegrationException, e:
                    return e.as_problem_detail_document(debug=False)
            else:
                status_code = 200
            if fulfillment.content_type:
                headers['Content-Type'] = fulfillment.content_type

        return Response(content, status_code, headers)

    def can_fulfill_without_loan(self, library, patron, pool, lpdm):
        """Is it acceptable to fulfill the given LicensePoolDeliveryMechanism
        for the given Patron without creating a Loan first?

        This question is usually asked because no Patron has been
        authenticated, and thus no Loan can be created, but somebody
        wants a book anyway.

        :param library: A Library.
        :param patron: A Patron, probably None.
        :param lpdm: A LicensePoolDeliveryMechanism.
        """
        authenticator = self.manager.auth.library_authenticators.get(library.short_name)
        if authenticator and authenticator.identifies_individuals:
            # This library identifies individual patrons, so there is
            # no reason to fulfill books without a loan. Even if the
            # books are free and the 'loans' are nominal, having a
            # Loan object makes it possible for a patron to sync their
            # collection across devices, so that's the way we do it.
            return False

        # If the library doesn't require that individual patrons
        # identify themselves, it's up to the CirculationAPI object.
        # Most of them will say no. (This would indicate that the
        # collection is improperly associated with a library that
        # doesn't identify its patrons.)
        return self.circulation.can_fulfill_without_loan(patron, pool, lpdm)

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
                feed = LibraryLoanAndHoldAnnotator.single_hold_feed(
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
        library = flask.request.library
        if not contributor_name:
            return NO_SUCH_LANE.detailed(_("No contributor provided"))

        languages, audiences = self._lane_details(languages, audiences)

        lane = ContributorLane(
            library, contributor_name, languages=languages, audiences=audiences
        )

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request(worklist=lane)
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
        return feed_response(unicode(feed))

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
                library, work, lane_name, novelist_api=novelist_api
            )
        except ValueError, e:
            # No related books were found.
            return NO_SUCH_LANE.detailed(e.message)

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request(
            worklist=lane, base_class=FeaturedSeriesFacets
        )
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
            self._db, lane.DISPLAY_NAME, url, lane, annotator=annotator,
            facets=facets
        )
        return feed_response(unicode(feed))

    def recommendations(self, identifier_type, identifier, novelist_api=None):
        """Serve a feed of recommendations related to a given book."""

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        lane_name = "Recommendations for %s by %s" % (work.title, work.author)
        try:
            lane = RecommendationLane(
                library, work, lane_name, novelist_api=novelist_api
            )
        except ValueError, e:
            # NoveList isn't configured.
            return NO_SUCH_LANE.detailed(_("Recommendations not available"))

        annotator = self.manager.annotator(lane)
        facets = load_facets_from_request(worklist=lane)
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
        return feed_response(unicode(feed))

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
        library = flask.request.library
        if not series_name:
            return NO_SUCH_LANE.detailed(_("No series provided"))

        languages, audiences = self._lane_details(languages, audiences)
        lane = SeriesLane(library, series_name=series_name,
                          languages=languages, audiences=audiences
        )
        annotator = self.manager.annotator(lane)

        # In addition to the orderings enabled for this library, a
        # series collection may be ordered by series position, and is
        # ordered that way by default.
        facet_config = FacetConfig.from_library(library)
        facet_config.set_default_facet(
            Facets.ORDER_FACET_GROUP_NAME, Facets.ORDER_SERIES_POSITION
        )

        # TODO: It would be nice to be able to adapt
        # FeaturedSeriesFacets so that it can also be used as the
        # Facets object for the full series lane.
        facets = load_facets_from_request(
            worklist=lane, facet_config=facet_config
        )
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
        return feed_response(unicode(feed))


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
            self.manager.analytics.collect_event(library, pools[0], event_type, datetime.datetime.utcnow())
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
        library = flask.request.library
        circulation = self.manager.circulation_apis[library.id]
        service_status = ServiceStatus(circulation)
        timings = service_status.loans_status(response=True)
        statuses = []
        for k, v in sorted(timings.items()):
            statuses.append(" <li><b>%s</b>: %s</li>" % (k, v))

        doc = self.template % dict(statuses="\n".join(statuses))
        return Response(doc, 200, {"Content-Type": "text/html"})

class ODLNotificationController(CirculationManagerController):
    """Receive notifications from an ODL distributor when the
    status of a loan changes.
    """

    def notify(self, loan_id):
        library = flask.request.library
        status_doc = flask.request.data
        loan = get_one(self._db, Loan, id=loan_id)

        if not loan:
            return NO_ACTIVE_LOAN.detailed(_("No loan was found for this identifier."))

        collection = loan.license_pool.collection
        if collection.protocol != ODLWithConsolidatedCopiesAPI.NAME:
            return INVALID_LOAN_FOR_ODL_NOTIFICATION

        api = self.manager.circulation_apis[library.id].api_for_license_pool(loan.license_pool)
        api.update_loan(loan, json.loads(status_doc))
        return Response(_('Success'), 200)

class SharedCollectionController(CirculationManagerController):
    """Enable this circulation manager to share its collections with
    libraries on other circulation managers, for collection types that
    support it."""
    def info(self, collection_name):
        """Return an OPDS2 catalog-like document with a link to register."""
        collection = get_one(self._db, Collection, name=collection_name)
        if not collection:
            return NO_SUCH_COLLECTION

        register_url = self.url_for('shared_collection_register',
                                    collection_name=collection_name)
        register_link = dict(href=register_url, rel='register')
        content = json.dumps(dict(links=[register_link]))
        headers = dict()
        headers["Content-Type"] = "application/opds+json;profile=https://librarysimplified.org/rel/profile/directory"
        return Response(content, 200, headers)

    def load_collection(self, collection_name):
        collection = get_one(self._db, Collection, name=collection_name)
        if not collection:
            return NO_SUCH_COLLECTION
        return collection

    def register(self, collection_name):
        collection = self.load_collection(collection_name)
        if isinstance(collection, ProblemDetail):
            return collection
        url = flask.request.form.get("url")
        try:
            response = self.shared_collection.register(collection, url)
        except InvalidInputException, e:
            return INVALID_REGISTRATION.detailed(str(e))
        except AuthorizationFailedException, e:
            return INVALID_CREDENTIALS.detailed(str(e))
        except RemoteInitiatedServerError, e:
            return e.as_problem_detail_document(debug=False)

        return Response(json.dumps(response), 200)

    def authenticated_client_from_request(self):
        header = flask.request.headers.get('Authorization')
        if header and 'bearer' in header.lower():
            shared_secret = base64.b64decode(header.split(' ')[1])
            client = IntegrationClient.authenticate(self._db, shared_secret)
            if client:
                return client
        return INVALID_CREDENTIALS

    def loan_info(self, collection_name, loan_id):
        collection = self.load_collection(collection_name)
        if isinstance(collection, ProblemDetail):
            return collection
        client = self.authenticated_client_from_request()
        if isinstance(client, ProblemDetail):
            return client
        loan = get_one(self._db, Loan, id=loan_id, integration_client=client)
        if not loan or loan.license_pool.collection != collection:
            return LOAN_NOT_FOUND

        feed = SharedCollectionLoanAndHoldAnnotator.single_loan_feed(
            collection, loan)
        headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }
        return Response(etree.tostring(feed), 200, headers)

    def borrow(self, collection_name, identifier_type, identifier, hold_id):
        collection = self.load_collection(collection_name)
        if isinstance(collection, ProblemDetail):
            return collection
        client = self.authenticated_client_from_request()
        if isinstance(client, ProblemDetail):
            return client
        if identifier_type and identifier:
            pools = self._db.query(LicensePool).join(
                LicensePool.identifier).filter(
                    Identifier.type==identifier_type
                ).filter(
                    Identifier.identifier==identifier
                ).filter(
                    LicensePool.collection_id==collection.id
                ).all()
            if not pools:
                return NO_LICENSES.detailed(
                    _("The item you're asking about (%s/%s) isn't in this collection.") % (
                        identifier_type, identifier
                    )
                )
            pool = pools[0]
            hold = None
        elif hold_id:
            hold = get_one(self._db, Hold, id=hold_id)
            pool = hold.license_pool

        try:
            loan = self.shared_collection.borrow(collection, client, pool, hold)
        except AuthorizationFailedException, e:
            return INVALID_CREDENTIALS.detailed(str(e))
        except NoAvailableCopies, e:
            return NO_AVAILABLE_LICENSE.detailed(str(e))
        except CannotLoan, e:
            return CHECKOUT_FAILED.detailed(str(e))
        except RemoteIntegrationException, e:
            return e.as_problem_detail_document(debug=False)
        if loan and isinstance(loan, Loan):
            feed = SharedCollectionLoanAndHoldAnnotator.single_loan_feed(
                collection, loan)
            headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }
            return Response(etree.tostring(feed), 201, headers)
        elif loan and isinstance(loan, Hold):
            feed = SharedCollectionLoanAndHoldAnnotator.single_hold_feed(
                collection, loan)
            headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }
            return Response(etree.tostring(feed), 201, headers)

    def revoke_loan(self, collection_name, loan_id):
        collection = self.load_collection(collection_name)
        if isinstance(collection, ProblemDetail):
            return collection
        client = self.authenticated_client_from_request()
        if isinstance(client, ProblemDetail):
            return client
        loan = get_one(self._db, Loan, id=loan_id, integration_client=client)
        if not loan or not loan.license_pool.collection == collection:
            return LOAN_NOT_FOUND

        try:
            self.shared_collection.revoke_loan(collection, client, loan)
        except AuthorizationFailedException, e:
            return INVALID_CREDENTIALS.detailed(str(e))
        except NotCheckedOut, e:
            return NO_ACTIVE_LOAN.detailed(str(e))
        except CannotReturn, e:
            return COULD_NOT_MIRROR_TO_REMOTE.detailed(str(e))
        return Response(_("Success"), 200)

    def fulfill(self, collection_name, loan_id, mechanism_id, do_get=HTTP.get_with_timeout):
        collection = self.load_collection(collection_name)
        if isinstance(collection, ProblemDetail):
            return collection
        client = self.authenticated_client_from_request()
        if isinstance(client, ProblemDetail):
            return client
        loan = get_one(self._db, Loan, id=loan_id)
        if not loan or not loan.license_pool.collection == collection:
            return LOAN_NOT_FOUND

        mechanism = None
        if mechanism_id:
            mechanism = self.load_licensepooldelivery(
                loan.license_pool, mechanism_id
            )
            if isinstance(mechanism, ProblemDetail):
                return mechanism

        if not mechanism:
            # See if the loan already has a mechanism set. We can use that.
            if loan and loan.fulfillment:
                mechanism = loan.fulfillment
            else:
                return BAD_DELIVERY_MECHANISM.detailed(
                    _("You must specify a delivery mechanism to fulfill this loan.")
                )

        try:
            fulfillment = self.shared_collection.fulfill(collection, client, loan, mechanism)
        except AuthorizationFailedException, e:
            return INVALID_CREDENTIALS.detailed(str(e))
        except CannotFulfill, e:
            return CANNOT_FULFILL.detailed(str(e))
        except RemoteIntegrationException, e:
            return e.as_problem_detail_document(debug=False)
        headers = dict()
        content = fulfillment.content
        if fulfillment.content_link:
            # If we have a link to the content on a remote server, web clients may not
            # be able to access it if the remote server does not support CORS requests.
            # We need to fetch the content and return it instead of redirecting to it.
            try:
                response = do_get(fulfillment.content_link)
                status_code = response.status_code
                headers = dict(response.headers)
                content = response.content
            except RemoteIntegrationException, e:
                return e.as_problem_detail_document(debug=False)
        else:
            status_code = 200
        if fulfillment.content_type:
            headers['Content-Type'] = fulfillment.content_type

        return Response(content, status_code, headers)

    def hold_info(self, collection_name, hold_id):
        collection = self.load_collection(collection_name)
        if isinstance(collection, ProblemDetail):
            return collection
        client = self.authenticated_client_from_request()
        if isinstance(client, ProblemDetail):
            return client
        hold = get_one(self._db, Hold, id=hold_id, integration_client=client)
        if not hold or not hold.license_pool.collection == collection:
            return HOLD_NOT_FOUND

        feed = SharedCollectionLoanAndHoldAnnotator.single_hold_feed(
            collection, hold)
        headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }
        return Response(etree.tostring(feed), 200, headers)

    def revoke_hold(self, collection_name, hold_id):
        collection = self.load_collection(collection_name)
        if isinstance(collection, ProblemDetail):
            return collection
        client = self.authenticated_client_from_request()
        if isinstance(client, ProblemDetail):
            return client
        hold = get_one(self._db, Hold, id=hold_id, integration_client=client)
        if not hold or not hold.license_pool.collection == collection:
            return HOLD_NOT_FOUND

        try:
            self.shared_collection.revoke_hold(collection, client, hold)
        except AuthorizationFailedException, e:
            return INVALID_CREDENTIALS.detailed(str(e))
        except NotOnHold, e:
            return NO_ACTIVE_HOLD.detailed(str(e))
        except CannotReleaseHold, e:
            return CANNOT_RELEASE_HOLD.detailed(str(e))
        return Response(_("Success"), 200)
