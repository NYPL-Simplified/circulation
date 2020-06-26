from nose.tools import set_trace
import json
import logging
import sys
import urllib
import urlparse
import datetime
import base64
from wsgiref.handlers import format_date_time
from time import mktime
import os
from collections import defaultdict

from expiringdict import ExpiringDict
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

from api.saml.auth import SAMLAuthenticationManagerFactory
from api.saml.controller import SAMLController
from core.app_server import (
    cdn_url_for,
    url_for,
    load_lending_policy,
    load_facets_from_request,
    load_pagination_from_request,
    ComplaintController,
    HeartbeatController,
    URNLookupController as CoreURNLookupController,
)
from core.entrypoint import EverythingEntryPoint
from core.external_search import (
    ExternalSearchIndex,
    MockExternalSearchIndex,
    SortKeyPagination,
)
from core.facets import FacetConfig
from core.log import LogConfiguration
from core.lane import (
    DatabaseBackedFacets,
    Facets,
    FeaturedFacets,
    Pagination,
    Lane,
    SearchFacets,
    WorkList,
)
from core.metadata_layer import ContributorData
from core.model import (
    get_one,
    get_one_or_create,
    production_session,
    Admin,
    Annotation,
    CachedFeed,
    CachedMARCFile,
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
    Representation,
    Session,
    Work,
)
from core.opds import (
    AcquisitionFeed,
    NavigationFacets,
    NavigationFeed,
)
from core.util.opds_writer import (
     OPDSFeed,
)
from core.opensearch import OpenSearchDocument
from core.user_profile import ProfileController as CoreProfileController
from core.util.flask_util import (
    problem,
    OPDSFeedResponse
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
    CirculationPatronProfileStorage,
    OAuthController,
)
from config import (
    Configuration,
    CannotLoadConfiguration,
)

from lanes import (
    load_lanes,
    ContributorFacets,
    ContributorLane,
    RecommendationLane,
    RelatedBooksLane,
    SeriesFacets,
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
from odl import ODLAPI
from novelist import (
    NoveListAPI,
    MockNoveListAPI,
)
from base_controller import BaseCirculationManagerController
from testing import MockCirculationAPI, MockSharedCollectionAPI
from core.analytics import Analytics
from core.lane import BaseFacets
from core.marc import MARCExporter

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

    def load_facets_from_request(self, *args, **kwargs):
        """Load a faceting object from the incoming request, but also apply an authentication
        restriction.

        Specifically, in this application you can't use nonstandard
        caching rules unless you're an authenticated administrator.
        """
        facets = load_facets_from_request(*args, **kwargs)
        if isinstance(facets, BaseFacets) and getattr(facets, 'max_cache_age', None) is not None:
            # A faceting object was loaded, and it tried to do something nonstandard
            # with caching.

            # Try to get the AdminSignInController, which is
            # associated with the CirculationManager object by the
            # admin interface in admin/controller.
            #
            # If the admin interface wasn't initialized for whatever
            # reason, we'll default to assuming the user is not an
            # authenticated admin.
            authenticated = False
            controller = getattr(self, 'admin_sign_in_controller', None)
            if controller:
                admin = controller.authenticated_admin_from_request()
                # If authenticated_admin_from_request returns anything other than an admin (probably
                # a ProblemDetail), the user is not an authenticated admin.
                if isinstance(admin, Admin):
                    authenticated = True
            if not authenticated:
                facets.max_cache_age = None
        return facets

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

        # Make sure there's a site-wide public/private key pair.
        self.sitewide_key_pair

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

        # Assemble the list of patron web client domains from individual
        # library registration settings as well as a sitewide setting.
        patron_web_domains = set()

        def get_domain(url):
            url = url.strip()
            if url == "*":
                return url
            scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
            if scheme and netloc:
                return scheme + "://" + netloc
            else:
                return None

        sitewide_patron_web_client_urls = ConfigurationSetting.sitewide(
            self._db, Configuration.PATRON_WEB_HOSTNAMES).value
        if sitewide_patron_web_client_urls:
            for url in sitewide_patron_web_client_urls.split('|'):
                domain = get_domain(url)
                if domain:
                    patron_web_domains.add(domain)

        from registry import Registration
        for setting in self._db.query(
            ConfigurationSetting).filter(
            ConfigurationSetting.key==Registration.LIBRARY_REGISTRATION_WEB_CLIENT):
            if setting.value:
                patron_web_domains.add(get_domain(setting.value))

        self.patron_web_domains = patron_web_domains
        self.setup_configuration_dependent_controllers()
        self.authentication_for_opds_documents = ExpiringDict(
            max_len=1000, max_age_seconds=3600
        )

    @property
    def external_search(self):
        """Retrieve or create a connection to the search interface.

        This is created lazily so that a failure to connect only
        affects feeds that depend on the search engine, not the whole
        circulation manager.
        """
        if not self._external_search:
            self.setup_external_search()
        return self._external_search

    def setup_external_search(self):
        try:
            self._external_search = self.setup_search()
            self.external_search_initialization_exception = None
        except Exception, e:
            self.log.error(
                "Exception initializing search engine: %s", e
            )
            self._external_search = None
            self.external_search_initialization_exception = e
        return self._external_search

    def cdn_url_for(self, view, *args, **kwargs):
        """Generate a URL for a view that (probably) passes through a CDN.

        :param view: Name of the view.
        :param _facets: The faceting object used to generate the document that's calling
           this method. This may change which function is actually used to generate the
           URL; in particular, it may disable a CDN that would otherwise be used. This is
           called _facets just in case there's ever a view that takes 'facets' as a real
           keyword argument.
        :param args: Positional arguments to the view function.
        :param kwargs: Keyword arguments to the view function.
        """
        url_for = self._cdn_url_for
        facets = kwargs.pop('_facets', None)
        if facets and facets.max_cache_age is CachedFeed.IGNORE_CACHE:
            # The faceting object in play has disabled cache
            # checking. A CDN is also a cache, so we should disable
            # CDN URLs in the feed to make it more likely that the
            # client continues to see up-to-the-minute feeds as they
            # click around.
            url_for = self.url_for
        return url_for(view, *args, **kwargs)

    def _cdn_url_for(self, *args, **kwargs):
        """Call the cdn_url_for function.

        Defined solely to be overridden in tests.
        """
        return cdn_url_for(*args, **kwargs)

    def url_for(self, view, *args, **kwargs):
        """Call the url_for function, ensuring that Flask generates an absolute URL.
        """
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
            return MockExternalSearchIndex()
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
        self.marc_records = MARCRecordController(self)
        self.loans = LoanController(self)
        self.annotations = AnnotationController(self)
        self.urn_lookup = URNLookupController(self)
        self.work_controller = WorkController(self)
        self.analytics_controller = AnalyticsController(self)
        self.profiles = ProfileController(self)
        self.heartbeat = HeartbeatController()
        self.odl_notification_controller = ODLNotificationController(self)
        self.shared_collection_controller = SharedCollectionController(self)
        self.static_files = StaticFileController(self)

    def setup_configuration_dependent_controllers(self):
        """Set up all the controllers that depend on the
        current site configuration.

        This method will be called fresh every time the site
        configuration changes.
        """
        self.oauth_controller = OAuthController(self.auth)
        self.saml_controller = SAMLController(self, self.auth)

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

    def annotator(self, lane, facets=None, *args, **kwargs):
        """Create an appropriate OPDS annotator for the given lane.

        :param lane: A Lane or WorkList.
        :param facets: A faceting object.
        :param annotator_class: Instantiate this annotator class if possible.
           Intended for use in unit tests.
        """
        library = None
        if lane and isinstance(lane, Lane):
            library = lane.library
        elif lane and isinstance(lane, WorkList):
            library = lane.get_library(self._db)
        if not library and hasattr(flask.request, 'library'):
            library = flask.request.library

        # If no library is provided, the best we can do is a generic
        # annotator for this application.
        if not library:
            return CirculationManagerAnnotator(lane)

        # At this point we know the request is in a library context, so we
        # can create a LibraryAnnotator customized for that library.

        # Some features are only available if a patron authentication
        # mechanism is set up for this library.
        authenticator = self.auth.library_authenticators.get(library.short_name)
        library_identifies_patrons = (
            authenticator is not None and authenticator.identifies_individuals
        )
        annotator_class = kwargs.pop('annotator_class', LibraryAnnotator)
        return annotator_class(
            self.circulation_apis[library.id], lane,
            library, top_level_title='All Books',
            library_identifies_patrons=library_identifies_patrons,
            facets=facets, *args, **kwargs
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
    def sitewide_key_pair(self):
        """Look up or create the sitewide public/private key pair."""
        setting = ConfigurationSetting.sitewide(
            self._db, Configuration.KEY_PAIR
        )
        return Configuration.key_pair(setting)

    @property
    def public_key_integration_document(self):
        """Serve a document with the sitewide public key."""
        site_id = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY).value
        document = dict(id=site_id)

        public, private = self.sitewide_key_pair
        document['public_key'] = dict(type='RSA', value=public)
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

    @property
    def search_engine(self):
        """Return the configured external search engine, or a
        ProblemDetail if none is configured.
        """
        search_engine = self.manager.external_search
        if not search_engine:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _("The search index for this site is not properly configured.")
            )
        return search_engine

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

    def groups(self, lane_identifier, feed_class=AcquisitionFeed):
        """Build or retrieve a grouped acquisition feed.

        :param lane_identifier: An identifier that uniquely identifiers
            the WorkList whose feed we want.
        :param feed_class: A replacement for AcquisitionFeed, for use in
            tests.
        """
        library = flask.request.library

        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane

        if not lane.children:
            # This lane has no children. Although we can technically
            # create a grouped feed, it would be an unsatisfying
            # gateway to a paginated feed. We should just serve the
            # paginated feed.
            return self.feed(lane_identifier, feed_class)

        facet_class_kwargs = dict(
            minimum_featured_quality=library.minimum_featured_quality,
        )
        facets = self.manager.load_facets_from_request(
            worklist=lane, base_class=FeaturedFacets,
            base_class_constructor_kwargs=facet_class_kwargs
        )
        if isinstance(facets, ProblemDetail):
            return facets

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        url = self.cdn_url_for(
            "acquisition_groups", lane_identifier=lane_identifier,
            library_short_name=library.short_name, _facets=facets
        )

        annotator = self.manager.annotator(lane, facets)
        return feed_class.groups(
            _db=self._db, title=lane.display_name, url=url, worklist=lane,
            annotator=annotator, facets=facets, search_engine=search_engine
        )

    def feed(self, lane_identifier, feed_class=AcquisitionFeed):
        """Build or retrieve a paginated acquisition feed.

        :param lane_identifier: An identifier that uniquely identifiers
            the WorkList whose feed we want.
        :param feed_class: A replacement for AcquisitionFeed, for use in
            tests.
        """
        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane
        facets = self.manager.load_facets_from_request(worklist=lane)
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request(SortKeyPagination)
        if isinstance(pagination, ProblemDetail):
            return pagination
        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        library_short_name = flask.request.library.short_name
        url = self.cdn_url_for(
            "feed", lane_identifier=lane_identifier,
            library_short_name=library_short_name, _facets=facets
        )

        annotator = self.manager.annotator(lane, facets=facets)
        return feed_class.page(
            _db=self._db, title=lane.display_name,
            url=url, worklist=lane, annotator=annotator,
            facets=facets, pagination=pagination,
            search_engine=search_engine
        )

    def navigation(self, lane_identifier):
        """Build or retrieve a navigation feed, for clients that do not support groups."""

        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane
        library = flask.request.library
        library_short_name = library.short_name
        url = self.cdn_url_for(
            "navigation_feed", lane_identifier=lane_identifier, library_short_name=library_short_name,
        )

        title = lane.display_name
        facet_class_kwargs = dict(
            minimum_featured_quality=library.minimum_featured_quality,
        )
        facets = self.manager.load_facets_from_request(
            worklist=lane, base_class=NavigationFacets,
            base_class_constructor_kwargs=facet_class_kwargs
        )
        annotator = self.manager.annotator(lane, facets)
        return NavigationFeed.navigation(
            self._db, title, url, lane, annotator, facets=facets
        )

    def crawlable_library_feed(self):
        """Build or retrieve a crawlable acquisition feed for the
        request library.
        """
        library = flask.request.library
        url = self.cdn_url_for(
            "crawlable_library_feed",
            library_short_name=library.short_name,
        )
        title = library.name
        lane = CrawlableCollectionBasedLane()
        lane.initialize(library)
        return self._crawlable_feed(title=title, url=url, worklist=lane)

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
        lane = CrawlableCollectionBasedLane()
        lane.initialize([collection])
        if collection.protocol in [ODLAPI.NAME]:
            annotator = SharedCollectionAnnotator(collection, lane)
        else:
            # We'll get a generic CirculationManagerAnnotator.
            annotator = None
        return self._crawlable_feed(
            title=title, url=url, worklist=lane, annotator=annotator
        )

    def crawlable_list_feed(self, list_name):
        """Build or retrieve a crawlable, paginated acquisition feed for the
        named CustomList, sorted by update date.
        """
        # TODO: A library is not strictly required here, since some
        # CustomLists aren't associated with a library, but this isn't
        # a use case we need to support now.
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
        return self._crawlable_feed(title=title, url=url, worklist=lane)

    def _crawlable_feed(self, title, url, worklist, annotator=None,
                        feed_class=AcquisitionFeed):
        """Helper method to create a crawlable feed.

        :param title: The title to use for the feed.
        :param url: The URL from which the feed will be served.
        :param worklist: A crawlable Lane which controls which works show up
            in the feed.
        :param annotator: A custom Annotator to use when generating the feed.
        :param feed_class: A drop-in replacement for AcquisitionFeed
            for use in tests.
        """
        pagination = load_pagination_from_request(
            SortKeyPagination, default_size=Pagination.DEFAULT_CRAWLABLE_SIZE
        )
        if isinstance(pagination, ProblemDetail):
            return pagination

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        annotator = annotator or self.manager.annotator(worklist)

        # A crawlable feed has only one possible set of Facets,
        # so library settings are irrelevant.
        facets = CrawlableFacets.default(None)

        return feed_class.page(
            _db=self._db, title=title, url=url, worklist=worklist,
            annotator=annotator,
            facets=facets, pagination=pagination,
            search_engine=search_engine
        )

    def _load_search_facets(self, lane):
        entrypoints = list(flask.request.library.entrypoints)
        if len(entrypoints) > 1:
            # There is more than one enabled EntryPoint.
            # By default, search them all.
            default_entrypoint = EverythingEntryPoint
        else:
            # There is only one enabled EntryPoint,
            # and no need for a special default.
            default_entrypoint = None
        return self.manager.load_facets_from_request(
            worklist=lane, base_class=SearchFacets,
            default_entrypoint=default_entrypoint,
        )

    def search(self, lane_identifier, feed_class=AcquisitionFeed):
        """Search for books."""
        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane

        # Althoug the search query goes against Elasticsearch, we must
        # use normal pagination because the results are sorted by
        # match quality, not bibliographic information.
        pagination = load_pagination_from_request(
            Pagination, default_size=Pagination.DEFAULT_SEARCH_SIZE
        )
        if isinstance(pagination, ProblemDetail):
            return pagination

        facets = self._load_search_facets(lane)
        if isinstance(facets, ProblemDetail):
            return lane

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        # Check whether there is a query string -- if not, we want to
        # send an OpenSearch document explaining how to search.
        query = flask.request.args.get('q')
        library_short_name = flask.request.library.short_name

        # Create a function that, when called, generates a URL to the
        # search controller.
        #
        # We'll call this one way if there is no query string in the
        # request arguments, and another way if there is a query
        # string.
        make_url_kwargs = dict(facets.items())
        make_url = lambda: self.url_for(
            'lane_search', lane_identifier=lane_identifier,
            library_short_name=library_short_name,
            **make_url_kwargs
        )
        if not query:
            # Send the search form
            open_search_doc = OpenSearchDocument.for_lane(lane, make_url())
            headers = { "Content-Type" : "application/opensearchdescription+xml" }
            return Response(open_search_doc, 200, headers)

        # We have a query -- add it to the keyword arguments used when
        # generating a URL.
        make_url_kwargs['q'] = query.encode("utf8")

        # Run a search.
        annotator = self.manager.annotator(lane, facets)
        info = OpenSearchDocument.search_info(lane)
        return feed_class.search(
            _db=self._db, title=info['name'],
            url=make_url(), lane=lane, search_engine=search_engine,
            query=query, annotator=annotator, pagination=pagination,
            facets=facets
        )


class MARCRecordController(CirculationManagerController):
    DOWNLOAD_TEMPLATE = """
<html lang="en">
<head><meta charset="utf8"></head>
<body>
%(body)s
</body>
</html>"""

    def download_page(self):
        library = flask.request.library
        body = "<h2>Download MARC files for %s</h2>" % library.name
        time_format = "%B %-d, %Y"

        # Check if a MARC exporter is configured so we can show a
        # message if it's not.
        exporter = None
        try:
            exporter = MARCExporter.from_config(library)
        except CannotLoadConfiguration, e:
            body += "<p>" + _("No MARC exporter is currently configured for this library.") + "</p>"

        if len(library.cachedmarcfiles) < 1 and exporter:
            body += "<p>" + _("MARC files aren't ready to download yet.") + "</p>"

        files_by_lane = defaultdict(dict)
        for file in library.cachedmarcfiles:
            if file.start_time == None:
                files_by_lane[file.lane]["full"] = file
            else:
                if not files_by_lane[file.lane].get("updates"):
                    files_by_lane[file.lane]["updates"] = []
                files_by_lane[file.lane]["updates"].append(file)

        # TODO: By default the MARC script only caches one level of lanes,
        # so sorting by priority is good enough.
        lanes = sorted(files_by_lane.keys(), key=lambda x: x.priority if x else -1)

        for lane in lanes:
            files = files_by_lane[lane]
            body += "<section>"
            body += "<h3>%s</h3>" % (lane.display_name if lane else _("All Books"))
            if files.get("full"):
                file = files.get("full")
                full_url = file.representation.mirror_url
                full_label = _("Full file - last updated %(update_time)s", update_time=file.end_time.strftime(time_format))
                body += '<a href="%s">%s</a>' % (files.get("full").representation.mirror_url, full_label)

                if files.get("updates"):
                    body += "<h4>%s</h4>" % _("Update-only files")
                    body += "<ul>"
                    files.get("updates").sort(key=lambda x: x.end_time)
                    for update in files.get("updates"):
                        update_url = update.representation.mirror_url
                        update_label = _("Updates from %(start_time)s to %(end_time)s",
                                         start_time=update.start_time.strftime(time_format),
                                         end_time=update.end_time.strftime(time_format))
                        body += '<li><a href="%s">%s</a></li>' % (update_url, update_label)
                    body += "</ul>"

            body += "</section>"
            body += "<br />"

        html = self.DOWNLOAD_TEMPLATE % dict(body=body)
        headers = dict()
        headers['Content-Type'] = "text/html"
        return Response(html, 200, headers)

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
        """Sync the authenticated patron's loans and holds with all third-party
        providers.

        :return: A Response containing an OPDS feed with up-to-date information.
        """
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
        return LibraryLoanAndHoldAnnotator.active_loans_for(
            self.circulation, patron
        )

    def borrow(self, identifier_type, identifier, mechanism_id=None):
        """Create a new loan or hold for a book.

        :return: A Response containing an OPDS entry that includes a link of rel
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
        except (PatronLoanLimitReached, PatronHoldLimitReached), e:
            problem_doc = e.as_problem_detail_document().with_debug(unicode(e))
        except DeliveryMechanismError, e:
            return BAD_DELIVERY_MECHANISM.with_debug(
                unicode(e), status_code=e.status_code
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
            problem_doc = CHECKOUT_FAILED.with_debug(unicode(e))
        except CannotHold, e:
            problem_doc = HOLD_FAILED.with_debug(unicode(e))
        except CannotRenew, e:
            problem_doc = RENEW_FAILED.with_debug(unicode(e))
        except NotFoundOnRemote, e:
            problem_doc = NOT_FOUND_ON_REMOTE
        except CirculationException, e:
            # Generic circulation error.
            problem_doc = CHECKOUT_FAILED.with_debug(unicode(e))

        if problem_doc:
            return problem_doc

        # At this point we have either a loan or a hold. If a loan, serve
        # a feed that tells the patron how to fulfill the loan. If a hold,
        # serve a feed that talks about the hold.
        response_kwargs = {}
        if is_new:
            response_kwargs['status'] = 201
        else:
            response_kwargs['status'] = 200
        item = loan or hold
        if item:
            return LibraryLoanAndHoldAnnotator.single_item_feed(
                self.circulation, item, **response_kwargs
            )
        else:
            # This should never happen -- we should have sent a more specific
            # error earlier.
            return HOLD_FAILED

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

    def fulfill(self, license_pool_id, mechanism_id=None, part=None, do_get=None):
        """Fulfill a book that has already been checked out,
        or which can be fulfilled with no active loan.

        If successful, this will serve the patron a downloadable copy
        of the book, a key (such as a DRM license file or bearer
        token) which can be used to get the book, or an OPDS entry
        containing a link to the book.

        :param license_pool_id: Database ID of a LicensePool.
        :param mechanism_id: Database ID of a DeliveryMechanism.

        :param part: Vendor-specific identifier used when fulfilling a
           specific part of a book rather than the whole thing (e.g. a
           single chapter of an audiobook).
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

        # Define a function that, given a part identifier, will create
        # an appropriate link to this controller.
        def fulfill_part_url(part):
            return url_for(
                "fulfill", license_pool_id=requested_license_pool.id,
                mechanism_id=mechanism.delivery_mechanism.id,
                part=unicode(part), _external=True
            )

        try:
            fulfillment = self.circulation.fulfill(
                patron, credential, requested_license_pool, mechanism,
                part=part, fulfill_part_url=fulfill_part_url
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
                unicode(e), status_code=e.status_code
            )
        except FormatNotAvailable, e:
            return NO_ACCEPTABLE_FORMAT.with_debug(
                unicode(e), status_code=e.status_code
            )
        except DeliveryMechanismError, e:
            return BAD_DELIVERY_MECHANISM.with_debug(
                unicode(e), status_code=e.status_code
            )

        # A subclass of FulfillmentInfo may want to bypass the whole
        # response creation process.
        response = fulfillment.as_response
        if response:
            return response

        headers = dict()
        encoding_header = dict()
        if (fulfillment.data_source_name == DataSource.ENKI
            and mechanism.delivery_mechanism.drm_scheme_media_type == DeliveryMechanism.NO_DRM):
                encoding_header["Accept-Encoding"] = "deflate"

        if mechanism.delivery_mechanism.is_streaming:
            # If this is a streaming delivery mechanism, create an OPDS entry
            # with a fulfillment link to the streaming reader url.
            feed = LibraryLoanAndHoldAnnotator.single_item_feed(
                self.circulation, loan, fulfillment
            )
            if isinstance(feed, Response):
                return feed
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

        return Response(response=content, status=status_code, headers=headers)

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
                return COULD_NOT_MIRROR_TO_REMOTE.detailed(title, 503).with_debug(unicode(e))
        elif hold:
            if not self.circulation.can_revoke_hold(pool, hold):
                title = _("Cannot release a hold once it enters reserved state.")
                return CANNOT_RELEASE_HOLD.detailed(title, 400)
            try:
                self.circulation.release_hold(patron, credential, pool)
            except CannotReleaseHold, e:
                title = _("Hold released locally but remote failed.")
                return CANNOT_RELEASE_HOLD.detailed(title, 503).with_debug(unicode(e))

        work = pool.work
        annotator = self.manager.annotator(None)
        return AcquisitionFeed.single_entry(self._db, work, annotator)

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
                item = loan
            else:
                item = hold
            return CirculationManagerLoanAndHoldAnnotator.single_item_feed(
                self.circulation, item
            )

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

    def contributor(
        self, contributor_name, languages, audiences,
        feed_class=AcquisitionFeed
    ):
        """Serve a feed of books written by a particular author"""
        library = flask.request.library
        if not contributor_name:
            return NO_SUCH_LANE.detailed(_("No contributor provided"))

        # contributor_name is probably a display_name, but it could be a
        # sort_name. Pass it in for both fields and
        # ContributorData.lookup() will do its best to figure it out.
        contributor = ContributorData.lookup(
            self._db, sort_name=contributor_name, display_name=contributor_name
        )
        if not contributor:
            return NO_SUCH_LANE.detailed(
                _("Unknown contributor: %s") % contributor_name
            )

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        languages, audiences = self._lane_details(languages, audiences)

        lane = ContributorLane(
            library, contributor, languages=languages, audiences=audiences
        )
        facets = self.manager.load_facets_from_request(
            worklist=lane, base_class=ContributorFacets
        )
        if isinstance(facets, ProblemDetail):
            return facets

        pagination = load_pagination_from_request(SortKeyPagination)
        if isinstance(pagination, ProblemDetail):
            return pagination

        annotator = self.manager.annotator(lane, facets)

        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        return feed_class.page(
            _db=self._db, title=lane.display_name, url=url, worklist=lane,
            facets=facets, pagination=pagination,
            annotator=annotator, search_engine=search_engine
        )

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
        return AcquisitionFeed.single_entry(
            self._db, work, annotator,
            max_age=OPDSFeed.DEFAULT_MAX_AGE
        )

    def related(self, identifier_type, identifier, novelist_api=None,
                feed_class=AcquisitionFeed):
        """Serve a groups feed of books related to a given book."""

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

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

        facets = self.manager.load_facets_from_request(
            worklist=lane, base_class=FeaturedFacets,
            base_class_constructor_kwargs=dict(
                minimum_featured_quality=library.minimum_featured_quality
            )
        )
        if isinstance(facets, ProblemDetail):
            return facets

        annotator = self.manager.annotator(lane)
        url = annotator.feed_url(
            lane,
            facets=facets,
        )

        return feed_class.groups(
            _db=self._db, title=lane.DISPLAY_NAME,
            url=url, worklist=lane, annotator=annotator,
            facets=facets, search_engine=search_engine
        )

    def recommendations(self, identifier_type, identifier, novelist_api=None,
                        feed_class=AcquisitionFeed):
        """Serve a feed of recommendations related to a given book."""

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        lane_name = "Recommendations for %s by %s" % (work.title, work.author)
        try:
            lane = RecommendationLane(
                library=library, work=work, display_name=lane_name,
                novelist_api=novelist_api
            )
        except CannotLoadConfiguration, e:
            # NoveList isn't configured.
            return NO_SUCH_LANE.detailed(_("Recommendations not available"))

        facets = self.manager.load_facets_from_request(worklist=lane)
        if isinstance(facets, ProblemDetail):
            return facets

        # We use a normal Pagination object because recommendations
        # are looked up in a third-party API and paginated through the
        # database lookup.
        pagination = load_pagination_from_request(Pagination)
        if isinstance(pagination, ProblemDetail):
            return pagination

        annotator = self.manager.annotator(lane)
        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        return feed_class.page(
            _db=self._db, title=lane.DISPLAY_NAME, url=url, worklist=lane,
            facets=facets, pagination=pagination,
            annotator=annotator, search_engine=search_engine
        )

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

    def series(self, series_name, languages, audiences, feed_class=AcquisitionFeed):
        """Serve a feed of books in a given series."""
        library = flask.request.library
        if not series_name:
            return NO_SUCH_LANE.detailed(_("No series provided"))

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        languages, audiences = self._lane_details(languages, audiences)
        lane = SeriesLane(
            library, series_name=series_name, languages=languages,
            audiences=audiences
        )

        facets = self.manager.load_facets_from_request(
            worklist=lane, base_class=SeriesFacets
        )
        if isinstance(facets, ProblemDetail):
            return facets

        pagination = load_pagination_from_request(SortKeyPagination)
        if isinstance(pagination, ProblemDetail):
            return pagination

        annotator = self.manager.annotator(lane)

        url = annotator.feed_url(lane, facets=facets, pagination=pagination)
        return feed_class.page(
            _db=self._db, title=lane.display_name, url=url, worklist=lane,
            facets=facets, pagination=pagination,
            annotator=annotator, search_engine=search_engine
        )


class ProfileController(CirculationManagerController):
    """Implement the User Profile Management Protocol."""

    @property
    def _controller(self):
        """Instantiate a CoreProfileController that actually does the work.
        """
        patron = self.authenticated_patron_from_request()
        storage = CirculationPatronProfileStorage(patron, flask.url_for)
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


class URNLookupController(CoreURNLookupController):

    def __init__(self, manager):
        self.manager = manager
        super(URNLookupController, self).__init__(manager._db)

    def work_lookup(self, route_name):
        """Build a CirculationManagerAnnotor based on the current library's
        top-level WorkList, and use it to generate an OPDS lookup
        feed.
        """
        library = flask.request.library
        top_level_worklist = self.manager.top_level_lanes[library.id]
        annotator = CirculationManagerAnnotator(top_level_worklist)
        return super(URNLookupController, self).work_lookup(
            annotator, route_name
        )


class AnalyticsController(CirculationManagerController):

    def track_event(self, identifier_type, identifier, event_type):
        # TODO: It usually doesn't matter, but there should be
        # a way to distinguish between different LicensePools for the
        # same book.
        if event_type in CirculationEvent.CLIENT_EVENTS:
            library = flask.request.library
            # Authentication on the AnalyticsController is optional,
            # so flask.request.patron may or may not be set.
            patron = getattr(flask.request, 'patron', None)
            neighborhood = None
            if patron:
                neighborhood = getattr(patron, 'neighborhood', None)
            pools = self.load_licensepools(library, identifier_type, identifier)
            if isinstance(pools, ProblemDetail):
                return pools
            self.manager.analytics.collect_event(
                library, pools[0], event_type, datetime.datetime.utcnow(),
                neighborhood=neighborhood
            )
            return Response({}, 200)
        else:
            return INVALID_ANALYTICS_EVENT_TYPE


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
        if collection.protocol != ODLAPI.NAME:
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
            return INVALID_REGISTRATION.detailed(unicode(e))
        except AuthorizationFailedException, e:
            return INVALID_CREDENTIALS.detailed(unicode(e))
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

        return SharedCollectionLoanAndHoldAnnotator.single_item_feed(
            collection, loan
        )

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
            return INVALID_CREDENTIALS.detailed(unicode(e))
        except NoAvailableCopies, e:
            return NO_AVAILABLE_LICENSE.detailed(unicode(e))
        except CannotLoan, e:
            return CHECKOUT_FAILED.detailed(unicode(e))
        except RemoteIntegrationException, e:
            return e.as_problem_detail_document(debug=False)
        if loan:
            return SharedCollectionLoanAndHoldAnnotator.single_item_feed(
                collection, loan, status=201
            )

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
            return INVALID_CREDENTIALS.detailed(unicode(e))
        except NotCheckedOut, e:
            return NO_ACTIVE_LOAN.detailed(unicode(e))
        except CannotReturn, e:
            return COULD_NOT_MIRROR_TO_REMOTE.detailed(unicode(e))
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
            return INVALID_CREDENTIALS.detailed(unicode(e))
        except CannotFulfill, e:
            return CANNOT_FULFILL.detailed(unicode(e))
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

        return SharedCollectionLoanAndHoldAnnotator.single_item_feed(
            collection, hold
        )

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
            return INVALID_CREDENTIALS.detailed(unicode(e))
        except NotOnHold, e:
            return NO_ACTIVE_HOLD.detailed(unicode(e))
        except CannotReleaseHold, e:
            return CANNOT_RELEASE_HOLD.detailed(unicode(e))
        return Response(_("Success"), 200)

class StaticFileController(CirculationManagerController):
    def static_file(self, directory, filename):
        cache_timeout = ConfigurationSetting.sitewide(
            self._db, Configuration.STATIC_FILE_CACHE_TIME
        ).int_value
        return flask.send_from_directory(directory, filename, cache_timeout=cache_timeout)

    def image(self, filename):
        directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "resources", "images")
        return self.static_file(directory, filename)
