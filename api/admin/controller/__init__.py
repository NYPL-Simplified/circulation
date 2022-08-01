import base64
import copy
import json
import logging
import os
import sys
import urllib.parse
from datetime import date, datetime, timedelta

import flask
import jwt
from flask import Response, redirect
from flask_babel import lazy_gettext as lgt
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import (
    and_, desc, distinct, join, nullslast, select)

from api.admin.exceptions import (
    ADMIN_AUTH_MECHANISM_NOT_CONFIGURED, ADMIN_AUTH_NOT_CONFIGURED,
    CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST, CANNOT_CHANGE_PROTOCOL,
    CANNOT_EDIT_DEFAULT_LANE, CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT,
    COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY, CUSTOM_LIST_NAME_ALREADY_IN_USE,
    DUPLICATE_INTEGRATION, INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE, INTEGRATION_URL_ALREADY_IN_USE,
    INVALID_ADMIN_CREDENTIALS, INVALID_CONFIGURATION_OPTION,
    INVALID_CSRF_TOKEN, LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS,
    MISSING_COLLECTION, MISSING_CUSTOM_LIST, MISSING_LANE, MISSING_SERVICE,
    NO_CUSTOM_LISTS_FOR_LANE, NO_DISPLAY_NAME_FOR_LANE,
    NO_PROTOCOL_FOR_NEW_SERVICE, NO_SUCH_LIBRARY, NO_SUCH_PATRON,
    REMOTE_INTEGRATION_FAILED, UNKNOWN_PROTOCOL, AdminNotAuthorized
)
from api.admin.google_oauth_admin_authentication_provider import GoogleOAuthAdminAuthenticationProvider
from api.admin.opds import AdminAnnotator, AdminFeed
from api.admin.password_admin_authentication_provider import PasswordAdminAuthenticationProvider
from api.admin.template_styles import (
    body_style, error_style, hr_style, section_style, small_link_style)
from api.admin.templates import admin as admin_template
from api.admin.validator import Validator
from api.util.short_client_token import ShortClientTokenUtility
from api.authenticator import (
    CannotCreateLocalPatron, LibraryAuthenticator, PatronData)
from api.axis import Axis360API
from api.bibliotheca import BibliothecaAPI
from api.config import CannotLoadConfiguration, Configuration
from api.controller import CirculationManagerController
from api.enki import EnkiAPI
from api.feedbooks import FeedbooksOPDSImporter
from api.lanes import create_default_lanes
from api.lcp.collection import LCPAPI
from api.local_analytics_exporter import LocalAnalyticsExporter
from api.odilo import OdiloAPI
from api.odl import ODLAPI, SharedODLAPI
from api.opds_for_distributors import OPDSForDistributorsAPI
from api.overdrive import OverdriveAPI
from api.proquest.importer import ProQuestOPDS2Importer
from api.rbdigital import RBDigitalAPI
from core.app_server import load_pagination_from_request
from core.classifier import genres
from core.external_search import ExternalSearchIndex
from core.lane import Lane, WorkList
from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import (Admin, AdminRole, CirculationEvent, Collection,
                        ConfigurationSetting, CustomList, CustomListEntry,
                        DataSource, ExternalIntegration, Hold, Identifier,
                        Library, LicensePool, Loan, Patron, Timestamp, Work,
                        create, get_one, get_one_or_create)
from core.model.configuration import ExternalIntegrationLink
from core.opds import AcquisitionFeed
from core.opds2_import import OPDS2Importer
from core.opds_import import OPDSImporter, OPDSImportMonitor
from core.s3 import S3UploaderConfiguration
from core.selftest import HasSelfTests
from core.util.datetime_helpers import utc_now
from core.util.flask_util import OPDSFeedResponse
from core.util.http import HTTP
from core.util.problem_detail import ProblemDetail


def setup_admin_controllers(manager):
    """
    Set up all the controllers that will be used by the admin parts of the web app.
    """
    if not manager.testing:
        try:
            manager.config = Configuration.load(manager._db)
        except CannotLoadConfiguration as e:
            logging.error("Could not load configuration file: %s", e)
            sys.exit()

    manager.admin_view_controller = ViewController(manager)
    manager.admin_sign_in_controller = SignInController(manager)
    manager.timestamps_controller = TimestampsController(manager)
    from api.admin.controller.work_editor import WorkController
    manager.admin_work_controller = WorkController(manager)
    manager.admin_feed_controller = FeedController(manager)
    manager.admin_custom_lists_controller = CustomListsController(manager)
    manager.admin_lanes_controller = LanesController(manager)
    manager.admin_dashboard_controller = DashboardController(manager)
    manager.admin_settings_controller = SettingsController(manager)
    manager.admin_patron_controller = PatronController(manager)
    from api.admin.controller.self_tests import SelfTestsController
    manager.admin_self_tests_controller = SelfTestsController(manager)
    from api.admin.controller.discovery_services import DiscoveryServicesController
    manager.admin_discovery_services_controller = DiscoveryServicesController(
        manager)
    from api.admin.controller.discovery_service_library_registrations import DiscoveryServiceLibraryRegistrationsController  # noqa: E501
    manager.admin_discovery_service_library_registrations_controller = DiscoveryServiceLibraryRegistrationsController(manager)  # noqa: E501
    from api.admin.controller.cdn_services import CDNServicesController
    manager.admin_cdn_services_controller = CDNServicesController(manager)
    from api.admin.controller.analytics_services import AnalyticsServicesController
    manager.admin_analytics_services_controller = AnalyticsServicesController(
        manager)
    from api.admin.controller.metadata_services import MetadataServicesController
    manager.admin_metadata_services_controller = MetadataServicesController(
        manager)
    from api.admin.controller.metadata_service_self_tests import MetadataServiceSelfTestsController
    from api.admin.controller.patron_auth_services import PatronAuthServicesController
    manager.admin_metadata_service_self_tests_controller = MetadataServiceSelfTestsController(
        manager)
    manager.admin_patron_auth_services_controller = PatronAuthServicesController(
        manager)
    from api.admin.controller.patron_auth_service_self_tests import PatronAuthServiceSelfTestsController
    manager.admin_patron_auth_service_self_tests_controller = PatronAuthServiceSelfTestsController(
        manager)
    from api.admin.controller.admin_auth_services import AdminAuthServicesController
    manager.admin_auth_services_controller = AdminAuthServicesController(
        manager)
    from api.admin.controller.collection_settings import CollectionSettingsController
    manager.admin_collection_settings_controller = CollectionSettingsController(
        manager)
    from api.admin.controller.collection_self_tests import CollectionSelfTestsController
    manager.admin_collection_self_tests_controller = CollectionSelfTestsController(
        manager)
    from api.admin.controller.collection_library_registrations import CollectionLibraryRegistrationsController
    manager.admin_collection_library_registrations_controller = CollectionLibraryRegistrationsController(
        manager)
    from api.admin.controller.sitewide_settings import SitewideConfigurationSettingsController
    manager.admin_sitewide_configuration_settings_controller = SitewideConfigurationSettingsController(
        manager)
    from api.admin.controller.library_settings import LibrarySettingsController
    manager.admin_library_settings_controller = LibrarySettingsController(
        manager)
    from api.admin.controller.individual_admin_settings import IndividualAdminSettingsController
    manager.admin_individual_admin_settings_controller = IndividualAdminSettingsController(
        manager)
    from api.admin.controller.sitewide_services import (
        LoggingServicesController, SearchServicesController, SitewideServicesController
    )
    manager.admin_sitewide_services_controller = SitewideServicesController(
        manager)
    manager.admin_logging_services_controller = LoggingServicesController(
        manager)
    from api.admin.controller.search_service_self_tests import SearchServiceSelfTestsController
    manager.admin_search_service_self_tests_controller = SearchServiceSelfTestsController(
        manager)
    manager.admin_search_services_controller = SearchServicesController(
        manager)
    from api.admin.controller.storage_services import StorageServicesController
    manager.admin_storage_services_controller = StorageServicesController(
        manager)
    from api.admin.controller.catalog_services import CatalogServicesController
    manager.admin_catalog_services_controller = CatalogServicesController(
        manager)


class AdminController:
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266
    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db
        self.url_for = self.manager.url_for
        self.cdn_url_for = self.manager.cdn_url_for

    def admin_auth_provider(self, type):
        """
        Return an auth provider with the given type.
        If no auth provider has this type, return None.
        """
        for provider in self.admin_auth_providers:
            if provider.NAME == type:
                return provider

        return None

    def authenticated_admin_from_request(self):
        """Returns an authenticated admin or a problem detail."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        email = flask.session.get("admin_email")
        type = flask.session.get("auth_type")

        if email and type:
            admin = get_one(self._db, Admin, email=email)
            auth = self.admin_auth_provider(type)

            if not auth:
                return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

            if admin and auth.active_credentials(admin):
                flask.request.admin = admin
                return admin

        flask.request.admin = None
        return INVALID_ADMIN_CREDENTIALS

    def authenticated_admin(self, admin_details):
        """Creates or updates an admin with the given details"""
        (admin, is_new) = get_one_or_create(
            self._db, Admin, email=admin_details['email'])
        admin.update_credentials(
            self._db, credential=admin_details.get('credentials'))

        if is_new and admin_details.get("roles"):
            for role in admin_details.get("roles"):
                if role.get("role") in AdminRole.ROLES:
                    library = Library.lookup(self._db, role.get("library"))

                    if role.get("library") and not library:
                        msg = "%s authentication provider specifiec an unknown library for a new admin: %s"
                        self.log.warning(
                            msg % (admin_details.get("type"), role.get("library")))
                    else:
                        admin.add_role(role.get("role"), library)

                else:
                    msg = "%s authentication provider specified an unknown role for a new admin: %s"
                    self.log.warning(
                        msg % (admin_details.get("type"), role.get("role")))

        # Set up the admin's flask session.
        flask.session["admin_email"] = admin_details.get("email")
        flask.session["auth_type"] = admin_details.get("type")

        # A permanent session expires after a fixed time, rather than when the user closes the browser.
        flask.session.permanent = True

        # If this is the first time an admin has been authenticated, make sure there is a value set for
        # the sitewide BASE_URL_KEY setting. If it's not set, set it to the hostname of the current
        # request. This assumes the first authenticated admin is accessing the admin interface through
        # the hostname they want to be used for the site itself.
        base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY)

        if not base_url.value:
            base_url.value = urllib.parse.urljoin(flask.request.url, '/')

        return admin

    def check_csrf_token(self):
        """
        Verifies that the CSRF token in the form data or X-CSRF-Token header matches the one in the session cookie.
        """
        cookie_token = self.get_csrf_token()
        header_token = flask.request.headers.get("X-CSRF-Token")

        if not cookie_token or cookie_token != header_token:
            return INVALID_CSRF_TOKEN

        return cookie_token

    def get_csrf_token(self):
        """Returns the CSRF token for the current session."""
        return flask.request.cookies.get("csrf_token")

    def generate_csrf_token(self):
        """Generate a random CSRF token."""
        return base64.b64encode(os.urandom(24)).decode("utf-8")

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266
    @property
    def admin_auth_providers(self):
        auth_providers = []
        auth_service = ExternalIntegration.admin_authentication(self._db)

        if auth_service and auth_service.protocol == ExternalIntegration.GOOGLE_OAUTH:
            auth_providers.append(GoogleOAuthAdminAuthenticationProvider(
                auth_service, self.url_for('google_auth_callback'), test_mode=self.manager.testing,
            ))

        if Admin.with_password(self._db).count() != 0:
            auth_providers.append(
                PasswordAdminAuthenticationProvider(auth_service))

        return auth_providers

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class AdminCirculationManagerController(CirculationManagerController):
    """Parent class that provides methods for verifying an admin's roles."""
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266
    def require_system_admin(self):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_system_admin():
            raise AdminNotAuthorized()

    def require_sitewide_library_manager(self):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_sitewide_library_manager():
            raise AdminNotAuthorized()

    def require_library_manager(self, library):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_library_manager(library):
            raise AdminNotAuthorized()

    def require_librarian(self, library):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_librarian(library):
            raise AdminNotAuthorized()

    def require_higher_than_librarian(self):
        # A quick way to check the admin's permissions level without needing to already know the library;
        # used as a fail-safe in AnalyticsServicesController.process_post in case a librarian somehow manages
        # to submit a Local Analytics form despite the checks on the front end.
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.roles or admin.roles[0].role == "librarian":
            raise AdminNotAuthorized()

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class ViewController(AdminController):
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266
    def __call__(self, collection, book, path=None):
        setting_up = (self.admin_auth_providers == [])
        email = None
        roles = []

        if not setting_up:
            admin = self.authenticated_admin_from_request()

            if isinstance(admin, ProblemDetail):
                redirect_url = flask.request.url

                if (collection):
                    quoted_collection = urllib.parse.quote(collection)
                    redirect_url = redirect_url.replace(
                        quoted_collection,
                        quoted_collection.replace("/", "%2F"))

                if (book):
                    quoted_book = urllib.parse.quote(book)
                    redirect_url = redirect_url.replace(
                        quoted_book,
                        quoted_book.replace("/", "%2F"))

                return redirect(self.url_for('admin_sign_in', redirect=redirect_url))

            if not collection and not book and not path:

                if self._db.query(Library).count() > 0:
                    # Find the first library the admin is a librarian of.
                    library_name = None

                    for library in self._db.query(Library).order_by(Library.id.asc()):
                        if admin.is_librarian(library):
                            library_name = library.short_name
                            break

                    if not library_name:
                        msg = (
                            "Your admin account doesn't have access to any libraries. "
                            "Contact your library manager for assistance."
                        )
                        return Response(lgt(msg), 200)

                    return redirect(self.url_for('admin_view', collection=library_name))

            email = admin.email

            for role in admin.roles:
                if role.library:
                    roles.append({"role": role.role, "library": role.library})
                else:
                    roles.append({"role": role.role})

        csrf_token = flask.request.cookies.get(
            "csrf_token") or self.generate_csrf_token()

        # Find the URL and text to use when rendering the Terms of Service link in the footer.
        sitewide_tos_href = ConfigurationSetting.sitewide(
            self._db, Configuration.CUSTOM_TOS_HREF
        ).value or Configuration.DEFAULT_TOS_HREF

        sitewide_tos_text = ConfigurationSetting.sitewide(
            self._db, Configuration.CUSTOM_TOS_TEXT
        ).value or Configuration.DEFAULT_TOS_TEXT

        local_analytics = get_one(
            self._db, ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL
        )
        show_circ_events_download = bool(local_analytics is not None)

        response = Response(flask.render_template_string(
            admin_template,
            csrf_token=csrf_token,
            sitewide_tos_href=sitewide_tos_href,
            sitewide_tos_text=sitewide_tos_text,
            show_circ_events_download=show_circ_events_download,
            setting_up=setting_up,
            email=email,
            roles=roles,
        ))

        # The CSRF token is in its own cookie instead of the session cookie,
        # because if your session expires and you log in again, you should
        # be able to submit a form you already had open. The CSRF token lasts
        # until the user closes the browser window.
        response.set_cookie("csrf_token", csrf_token, httponly=True)
        return response

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class TimestampsController(AdminCirculationManagerController):
    """
    Returns a dict: each key is a type of service (script, monitor, or coverage provider);
    each value is a nested dict in which timestamps are organized by service name and then by collection ID.
    """
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266

    def diagnostics(self):
        self.require_system_admin()
        timestamps = self._db.query(Timestamp).order_by(Timestamp.start)
        sorted = self._sort_by_type(timestamps)

        for type, services in list(sorted.items()):
            for service in services:
                by_collection = self._sort_by_collection(sorted[type][service])
                sorted[type][service] = by_collection

        return sorted

    ##### Private Methods ####################################################  # noqa: E266

    def _sort_by_type(self, timestamps):
        """
        Takes a list of Timestamp objects.  Returns a dict: each key is a type of service
        (script, monitor, or coverage provider); each value is a dict in which the keys are the names
        of services and the values are lists of timestamps.
        """
        result = {}
        for ts in timestamps:
            info = self._extract_info(ts)
            result.setdefault((ts.service_type or "other"), []).append(info)

        for type, data in list(result.items()):
            result[type] = self._sort_by_service(data)

        return result

    def _sort_by_service(self, timestamps):
        """Returns a dict: each key is the name of a service; each value is a list of timestamps."""
        result = {}

        for timestamp in timestamps:
            result.setdefault(timestamp.get("service"), []).append(timestamp)

        return result

    def _sort_by_collection(self, timestamps):
        """
        Takes a list of timestamps; turns it into a dict in which each key is a
        collection ID and each value is a list of the timestamps associated with that collection.
        """
        result = {}

        for timestamp in timestamps:
            result.setdefault(timestamp.get(
                "collection_name"), []).append(timestamp)

        return result

    def _extract_info(self, timestamp):
        """Takes a Timestamp object and returns a dict"""

        duration = None
        if timestamp.start and timestamp.finish:
            duration = (timestamp.finish - timestamp.start).total_seconds()

        collection_name = "No associated collection"
        if timestamp.collection:
            collection_name = timestamp.collection.name

        return dict(
            id=timestamp.id,
            start=timestamp.start,
            duration=duration,
            exception=timestamp.exception,
            service=timestamp.service,
            collection_name=collection_name,
            achievements=timestamp.achievements
        )

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class SignInController(AdminController):
    ##### Class Constants ####################################################  # noqa: E266
    ERROR_RESPONSE_TEMPLATE = (
        '<!DOCTYPE HTML>'
        '<html lang="en">'
        '<head><meta charset="utf8"></head>'
        '<body style="{error}">'
        '<p><strong>%(status_code)d ERROR:</strong> %(message)s</p>'
        '<hr style="{hr}">'
        '<a href="/admin/sign_in" style="{link}">Try again</a>'
        '</body>'
        '</html>'
    ).format(error=error_style, hr=hr_style, link=small_link_style)

    SIGN_IN_TEMPLATE = (
        '<!DOCTYPE HTML>'
        '<html lang="en">'
        '<head><meta charset="utf8"></head>'
        '<body style="{}">'
        '<h1>Library Simplified</h1>'
        '%(auth_provider_html)s'
        '</body>'
        '</html>'
    ).format(body_style)

    ##### Public Interface / Magic Methods ###################################  # noqa: E266
    def sign_in(self):
        """Redirects admin if they're signed in, or shows the sign in page."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        admin = self.authenticated_admin_from_request()

        if isinstance(admin, ProblemDetail):
            redirect_url = flask.request.args.get("redirect")
            auth_provider_html = [auth.sign_in_template(
                redirect_url) for auth in self.admin_auth_providers]
            auth_provider_html = """
                <section style="{section}">
                <hr style="{hr}">or<hr style="{hr}">
                </section>
            """.format(section=section_style, hr=hr_style).join(auth_provider_html)

            html = self.SIGN_IN_TEMPLATE % dict(
                auth_provider_html=auth_provider_html
            )
            headers = dict()
            headers['Content-Type'] = "text/html"
            return Response(html, 200, headers)
        elif admin:
            return redirect(flask.request.args.get("redirect"), Response=Response)

    def redirect_after_google_sign_in(self):
        """Uses the Google OAuth client to determine admin details upon
        callback. Barring error, redirects to the provided redirect url.."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        auth = self.admin_auth_provider(
            GoogleOAuthAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin_details, redirect_url = auth.callback(
            self._db, flask.request.args)
        if isinstance(admin_details, ProblemDetail):
            return self.error_response(admin_details)

        _ = self.authenticated_admin(admin_details)
        return redirect(redirect_url, Response=Response)

    def password_sign_in(self):
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        auth = self.admin_auth_provider(
            PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin_details, redirect_url = auth.sign_in(
            self._db, flask.request.form)
        if isinstance(admin_details, ProblemDetail):
            return self.error_response(INVALID_ADMIN_CREDENTIALS)

        _ = self.authenticated_admin(admin_details)
        return redirect(redirect_url, Response=Response)

    def change_password(self):
        admin = flask.request.admin
        new_password = flask.request.form.get("password")
        if new_password:
            admin.password = new_password
        return Response(lgt("Success"), 200)

    def sign_out(self):
        # Clear out the admin's flask session.
        flask.session.pop("admin_email", None)
        flask.session.pop("auth_type", None)

        redirect_url = self.url_for(
            "admin_sign_in", redirect=self.url_for("admin_view"), _external=True)
        return redirect(redirect_url)

    def error_response(self, problem_detail):
        """Returns a problem detail as an HTML response"""
        html = self.ERROR_RESPONSE_TEMPLATE % dict(
            status_code=problem_detail.status_code,
            message=problem_detail.detail
        )
        return Response(html, problem_detail.status_code)

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class PatronController(AdminCirculationManagerController):
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266
    def lookup_patron(self, authenticator=None):
        """
        Look up personal information about a patron via the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normally.
        """
        patrondata = self._load_patrondata(authenticator)

        if isinstance(patrondata, ProblemDetail):
            return patrondata

        return patrondata.to_dict

    def reset_adobe_id(self, authenticator=None):
        """
        Delete all Credentials for a patron that are relevant to the patron's Adobe Account ID.

        :param authenticator: A LibraryAuthenticator. This is for mocking during tests;
            it's not necessary to provide it normal
        """
        patrondata = self._load_patrondata(authenticator)

        if isinstance(patrondata, ProblemDetail):
            return patrondata

        # Turn the Identifier into a Patron object.
        try:
            patron, is_new = patrondata.get_or_create_patron(
                self._db, flask.request.library.id
            )
        except CannotCreateLocalPatron:
            return NO_SUCH_PATRON.detailed(
                lgt(
                    "Could not create local patron object for %(patron_identifier)s",
                    patron_identifier=patrondata.authorization_identifier
                )
            )

        # Wipe the Patron's 'identifier for Adobe ID purposes'.
        for credential in ShortClientTokenUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)

        if patron.username:
            identifier = patron.username
        else:
            identifier = "with identifier " + patron.authorization_identifier

        return Response(
            str(lgt("Adobe ID for patron %(name_or_auth_id)s has been reset.",
                name_or_auth_id=identifier)),
            200
        )

    ##### Private Methods ####################################################  # noqa: E266
    def _load_patrondata(self, authenticator=None):
        """
        Extract a patron identifier from an incoming form submission, and ask the library's
        LibraryAuthenticator to turn it into a PatronData by doing a remote lookup in the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking during tests;
            it's not necessary to provide it normally.
        """
        self.require_librarian(flask.request.library)

        identifier = flask.request.form.get("identifier")
        if not identifier:
            return NO_SUCH_PATRON.detailed(lgt("Please enter a patron identifier"))

        if not authenticator:
            authenticator = LibraryAuthenticator.from_config(
                self._db, flask.request.library)

        patron_data = PatronData(authorization_identifier=identifier)
        complete_patron_data = None

        if not authenticator.providers:
            return NO_SUCH_PATRON.detailed(
                lgt("This library has no authentication providers, so it has no patrons.")
            )

        for provider in authenticator.providers:
            complete_patron_data = provider.remote_patron_lookup(patron_data)
            if complete_patron_data:
                return complete_patron_data

        # If we get here, none of the providers succeeded.
        if not complete_patron_data:
            return NO_SUCH_PATRON.detailed(
                lgt("No patron with identifier %(patron_identifier)s was found at your library",
                    patron_identifier=identifier),
            )

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class FeedController(AdminCirculationManagerController):
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266
    def complaints(self):
        self.require_librarian(flask.request.library)

        this_url = self.url_for('complaints')
        annotator = AdminAnnotator(self.circulation, flask.request.library)
        pagination = load_pagination_from_request()

        if isinstance(pagination, ProblemDetail):
            return pagination

        opds_feed = AdminFeed.complaints(
            library=flask.request.library, title="Complaints",
            url=this_url, annotator=annotator,
            pagination=pagination
        )

        return OPDSFeedResponse(opds_feed, max_age=0)

    def suppressed(self):
        self.require_librarian(flask.request.library)

        this_url = self.url_for('suppressed')
        annotator = AdminAnnotator(self.circulation, flask.request.library)
        pagination = load_pagination_from_request()

        if isinstance(pagination, ProblemDetail):
            return pagination

        opds_feed = AdminFeed.suppressed(
            _db=self._db, title="Hidden Books",
            url=this_url, annotator=annotator,
            pagination=pagination
        )

        return OPDSFeedResponse(opds_feed, max_age=0)

    def genres(self):
        data = dict({
            "Fiction": dict({}),
            "Nonfiction": dict({})
        })

        for name in genres:
            top = "Fiction" if genres[name].is_fiction else "Nonfiction"
            data[top][name] = dict({
                "name": name,
                "parents": [parent.name for parent in genres[name].parents],
                "subgenres": [subgenre.name for subgenre in genres[name].subgenres]
            })

        return data

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class CustomListsController(AdminCirculationManagerController):
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266
    def custom_lists(self):
        library = flask.request.library
        self.require_librarian(library)

        if flask.request.method == "GET":
            custom_lists = []

            for list in library.custom_lists:
                collections = []
                for collection in list.collections:
                    collections.append(
                        dict(id=collection.id, name=collection.name, protocol=collection.protocol))

                custom_lists.append(
                    dict(id=list.id, name=list.name, collections=collections, entry_count=list.size))

            return dict(custom_lists=custom_lists)

        if flask.request.method == "POST":
            id = flask.request.form.get("id")
            name = flask.request.form.get("name")
            entries = self._getJSONFromRequest(
                flask.request.form.get("entries"))
            collections = self._getJSONFromRequest(
                flask.request.form.get("collections"))
            return self._create_or_update_list(library, name, entries, collections, id=id)

    def url_for_custom_list(self, library, list):
        def url_fn(after):
            return self.url_for("custom_list", after=after, library_short_name=library.short_name, list_id=list.id)

        return url_fn

    def custom_list(self, list_id):
        library = flask.request.library
        self.require_librarian(library)
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        list = get_one(self._db, CustomList, id=list_id,
                       data_source=data_source)

        if not list:
            return MISSING_CUSTOM_LIST

        if flask.request.method == "GET":
            pagination = load_pagination_from_request()
            if isinstance(pagination, ProblemDetail):
                return pagination

            query = self._db.query(
                Work
            ).join(
                Work.custom_list_entries
            ).filter(
                CustomListEntry.list_id == list_id
            ).order_by(Work.id)

            url = self.url_for("custom_list", list_name=list.name,
                               library_short_name=library.short_name,
                               list_id=list_id)

            worklist = WorkList()
            worklist.initialize(library, customlists=[list])

            annotator = self.manager.annotator(worklist)
            url_fn = self.url_for_custom_list(library, list)
            feed = AcquisitionFeed.from_query(
                query, self._db, list.name, url, pagination, url_fn, annotator)
            annotator.annotate_feed(feed, worklist)

            return OPDSFeedResponse(str(feed), max_age=0)

        elif flask.request.method == "POST":
            name = flask.request.form.get("name")
            entries = self._getJSONFromRequest(
                flask.request.form.get("entries"))
            collections = self._getJSONFromRequest(
                flask.request.form.get("collections"))
            deletedEntries = self._getJSONFromRequest(
                flask.request.form.get("deletedEntries"))
            return self._create_or_update_list(library, name, entries, collections,
                                               deletedEntries=deletedEntries, id=list_id)

        elif flask.request.method == "DELETE":
            # Deleting requires a library manager.
            self.require_library_manager(flask.request.library)

            # Build the list of affected lanes before modifying the CustomList.
            affected_lanes = Lane.affected_by_customlist(list)
            surviving_lanes = []

            for lane in affected_lanes:
                if (lane.list_datasource is None and len(lane.customlist_ids) == 1):
                    # This Lane is based solely upon this custom list, which is about to be deleted.
                    self._db.delete(lane)   # Delete the Lane itself.
                else:
                    surviving_lanes.append(lane)

            for entry in list.entries:
                self._db.delete(entry)

            self._db.delete(list)
            self._db.flush()

            # Update the size for any lanes affected by this CustomList which _weren't_ deleted.
            for lane in surviving_lanes:
                lane.update_size(self._db, self.search_engine)

            return Response(str(lgt("Deleted")), 200)

    ##### Private Methods ####################################################  # noqa: E266
    def _getJSONFromRequest(self, values):
        if values:
            values = json.loads(values)
        else:
            values = []

        return values

    def _get_work_from_urn(self, library, urn):
        (identifier, _) = Identifier.parse_urn(self._db, urn)
        query = self._db.query(
            Work
        ).join(
            LicensePool, LicensePool.work_id == Work.id
        ).join(
            Collection, LicensePool.collection_id == Collection.id
        ).filter(
            LicensePool.identifier_id == identifier.id
        ).filter(
            Collection.id.in_([c.id for c in library.all_collections])
        )

        work = query.one()
        return work

    def _create_or_update_list(self, library, name, entries, collections, deletedEntries=None, id=None):
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        old_list_with_name = CustomList.find(self._db, name, library=library)

        if id:
            is_new = False
            list = get_one(self._db, CustomList, id=int(id),
                           data_source=data_source)

            if not list:
                return MISSING_CUSTOM_LIST

            if list.library != library:
                return CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST

            if old_list_with_name and old_list_with_name != list:
                return CUSTOM_LIST_NAME_ALREADY_IN_USE

        elif old_list_with_name:
            return CUSTOM_LIST_NAME_ALREADY_IN_USE
        else:
            list, is_new = create(self._db, CustomList,
                                  name=name, data_source=data_source)
            list.created = datetime.now()
            list.library = library

        list.updated = datetime.now()
        list.name = name
        membership_change = False

        works_to_update_in_search = set()

        for entry in entries:
            urn = entry.get("id")
            work = self._get_work_from_urn(library, urn)

            if work:
                entry, entry_is_new = list.add_entry(work, featured=True)
                if entry_is_new:
                    works_to_update_in_search.add(work)
                    membership_change = True

        if deletedEntries:
            for entry in deletedEntries:
                urn = entry.get("id")
                work = self._get_work_from_urn(library, urn)

                if work:
                    list.remove_entry(work)
                    works_to_update_in_search.add(work)
                    membership_change = True

        if membership_change:
            # We need to update the search index entries for works that caused a membership change,
            # so the upstream counts can be calculated correctly.
            self.search_engine.bulk_update(works_to_update_in_search)

            # If this list was used to populate any lanes, those lanes need to have their counts updated.
            for lane in Lane.affected_by_customlist(list):
                lane.update_size(self._db, self.search_engine)

        new_collections = []
        for collection_id in collections:
            collection = get_one(self._db, Collection, id=collection_id)

            if not collection:
                self._db.rollback()
                return MISSING_COLLECTION

            if list.library not in collection.libraries:
                self._db.rollback()
                return COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY

            new_collections.append(collection)

        list.collections = new_collections

        if is_new:
            return Response(str(list.id), 201)
        else:
            return Response(str(list.id), 200)

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class LanesController(AdminCirculationManagerController):
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266

    def lanes(self):
        library = flask.request.library
        self.require_librarian(library)

        if flask.request.method == "GET":
            def lanes_for_parent(parent):
                lanes = self._db.query(Lane).filter(
                    Lane.library == library
                ).filter(
                    Lane.parent == parent
                ).order_by(Lane.priority)

                return [
                    {
                        "id": lane.id,
                        "display_name": lane.display_name,
                        "visible": lane.visible,
                        "count": lane.size,
                        "sublanes": lanes_for_parent(lane),
                        "custom_list_ids": [list.id for list in lane.customlists],
                        "inherit_parent_restrictions": lane.inherit_parent_restrictions,
                    } for lane in lanes
                ]

            return dict(lanes=lanes_for_parent(None))

        if flask.request.method == "POST":
            self.require_library_manager(flask.request.library)

            id = flask.request.form.get("id")
            parent_id = flask.request.form.get("parent_id")
            display_name = flask.request.form.get("display_name")
            custom_list_ids = json.loads(
                flask.request.form.get("custom_list_ids", "[]"))
            inherit_parent_restrictions = flask.request.form.get(
                "inherit_parent_restrictions")

            if inherit_parent_restrictions == "true":
                inherit_parent_restrictions = True
            else:
                inherit_parent_restrictions = False

            if not display_name:
                return NO_DISPLAY_NAME_FOR_LANE

            if not custom_list_ids or len(custom_list_ids) == 0:
                return NO_CUSTOM_LISTS_FOR_LANE

            if id:
                is_new = False
                lane = get_one(self._db, Lane, id=id, library=library)

                if not lane:
                    return MISSING_LANE

                if not lane.customlists:
                    return CANNOT_EDIT_DEFAULT_LANE

                if display_name != lane.display_name:
                    old_lane = get_one(
                        self._db, Lane, display_name=display_name, parent=lane.parent)
                    if old_lane:
                        return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS

                lane.display_name = display_name
            else:
                parent = None

                if parent_id:
                    parent = get_one(
                        self._db, Lane, id=parent_id, library=library)
                    if not parent:
                        msg = "The specified parent lane does not exist, or is associated with a different library."
                        return MISSING_LANE.detailed(lgt(msg))

                old_lane = get_one(
                    self._db, Lane, display_name=display_name, parent=parent)

                if old_lane:
                    return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS

                (lane, is_new) = create(self._db, Lane,
                                        display_name=display_name, parent=parent, library=library)

                # Make a new lane the first child of its parent and bump all the siblings down in priority.
                siblings = self._db.query(Lane).filter(
                    Lane.library == library
                ).filter(
                    Lane.parent == lane.parent
                ).filter(
                    Lane.id != lane.id
                )

                for sibling in siblings:
                    sibling.priority += 1

                lane.priority = 0

            lane.inherit_parent_restrictions = inherit_parent_restrictions

            for list_id in custom_list_ids:
                list = get_one(self._db, CustomList,
                               library=library, id=list_id)
                if not list:
                    self._db.rollback()
                    return MISSING_CUSTOM_LIST.detailed(
                        lgt(
                            "The list with id %(list_id)s does not exist or is associated with a different library.",
                            list_id=list_id
                        )
                    )

                lane.customlists.append(list)

            for list in lane.customlists:
                if list.id not in custom_list_ids:
                    lane.customlists.remove(list)

            lane.update_size(self._db, self.search_engine)

            if is_new:
                return Response(str(lane.id), 201)
            else:
                return Response(str(lane.id), 200)

    def lane(self, lane_identifier):
        if flask.request.method == "DELETE":
            library = flask.request.library
            self.require_library_manager(library)

            lane = get_one(self._db, Lane, id=lane_identifier, library=library)

            if not lane:
                return MISSING_LANE

            if not lane.customlists:
                return CANNOT_EDIT_DEFAULT_LANE

            # Recursively delete all the lane's sublanes.
            def delete_lane_and_sublanes(lane):
                for sublane in lane.sublanes:
                    delete_lane_and_sublanes(sublane)
                self._db.delete(lane)

            delete_lane_and_sublanes(lane)

            return Response(str(lgt("Deleted")), 200)

    def show_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)

        if not lane:
            return MISSING_LANE

        if lane.parent and not lane.parent.visible:
            return CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT

        lane.visible = True

        return Response(str(lgt("Success")), 200)

    def hide_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)

        if not lane:
            return MISSING_LANE

        lane.visible = False

        return Response(str(lgt("Success")), 200)

    def reset(self):
        self.require_library_manager(flask.request.library)
        create_default_lanes(self._db, flask.request.library)
        return Response(str(lgt("Success")), 200)

    def change_order(self):
        self.require_library_manager(flask.request.library)
        submitted_lanes = json.loads(flask.request.data)

        def update_lane_order(lanes):
            for index, lane_data in enumerate(lanes):
                lane_id = lane_data.get("id")
                lane = self._db.query(Lane).filter(Lane.id == lane_id).one()
                lane.priority = index
                update_lane_order(lane_data.get("sublanes", []))

        update_lane_order(submitted_lanes)

        return Response(str(lgt("Success")), 200)

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class DashboardController(AdminCirculationManagerController):
    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266

    def stats(self):
        """Return an accounting of library statistics

        Returns:
            dict: Stats total licenses, available licenses, and patrons for each library.
        """
        library_stats = {}
        total_title_count = 0
        total_license_count = 0
        total_available_license_count = 0
        collection_counts = dict()

        for collection in self._db.query(Collection):
            if not flask.request.admin or not flask.request.admin.can_see_collection(collection):
                continue

            licensed_title_count = self._db.query(
                LicensePool
            ).filter(
                LicensePool.collection_id == collection.id
            ).filter(
                and_(
                    LicensePool.licenses_owned > 0,
                    LicensePool.open_access == False,       # noqa: E712
                )
            ).count()

            open_title_count = self._db.query(
                LicensePool
            ).filter(
                LicensePool.collection_id == collection.id
            ).filter(
                LicensePool.open_access == True             # noqa: E712
            ).count()

            # The sum queries return None instead of 0 if there are no license pools in the db.

            license_count = self._db.query(
                func.sum(LicensePool.licenses_owned)
            ).filter(
                LicensePool.collection_id == collection.id
            ).filter(
                LicensePool.open_access == False,           # noqa: E712
            ).all()[0][0] or 0

            available_license_count = self._db.query(
                func.sum(LicensePool.licenses_available)
            ).filter(
                LicensePool.collection_id == collection.id
            ).filter(
                LicensePool.open_access == False,           # noqa: E712
            ).all()[0][0] or 0

            total_title_count += licensed_title_count + open_title_count
            total_license_count += license_count
            total_available_license_count += available_license_count

            collection_counts[collection.name] = dict(
                licensed_titles=licensed_title_count,
                open_access_titles=open_title_count,
                licenses=license_count,
                available_licenses=available_license_count,
            )

        for library in self._db.query(Library):
            # Only include libraries this admin has librarian access to.
            if not flask.request.admin or not flask.request.admin.is_librarian(library):
                continue

            patron_count = self._db.query(Patron).filter(
                Patron.library_id == library.id).count()

            active_loans_patron_count = self._db.query(
                distinct(Patron.id)
            ).join(
                Patron.loans
            ).filter(
                Loan.end >= datetime.now(),
            ).filter(
                Patron.library_id == library.id
            ).count()

            active_patrons = select(
                [Patron.id]
            ).select_from(
                join(
                    Loan,
                    Patron,
                    and_(
                        Patron.id == Loan.patron_id,
                        Patron.library_id == library.id,
                        Loan.id != None,                        # noqa: E711
                        Loan.end >= datetime.now()
                    )
                )
            ).union(
                select(
                    [Patron.id]
                ).select_from(
                    join(
                        Hold,
                        Patron,
                        and_(
                            Patron.id == Hold.patron_id,
                            Patron.library_id == library.id,
                            Hold.id != None,
                        )
                    )
                )
            ).alias()

            active_loans_or_holds_patron_count_query = select(
                [func.count(distinct(active_patrons.c.id))]
            ).select_from(
                active_patrons
            )

            result = self._db.execute(active_loans_or_holds_patron_count_query)
            active_loans_or_holds_patron_count = [r[0] for r in result][0]

            loan_count = self._db.query(
                Loan
            ).join(
                Loan.patron
            ).filter(
                Patron.library_id == library.id
            ).filter(
                Loan.end >= datetime.now()
            ).count()

            hold_count = self._db.query(
                Hold
            ).join(
                Hold.patron
            ).filter(
                Patron.library_id == library.id
            ).count()

            title_count = 0
            license_count = 0
            available_license_count = 0

            library_collection_counts = dict()
            for collection in library.all_collections:
                counts = collection_counts[collection.name]
                library_collection_counts[collection.name] = counts
                title_count += counts.get("licensed_titles", 0) + \
                    counts.get("open_access_titles", 0)
                license_count += counts.get("licenses", 0)
                available_license_count += counts.get("available_licenses", 0)

            library_stats[library.short_name] = dict(
                patrons=dict(
                    total=patron_count,
                    with_active_loans=active_loans_patron_count,
                    with_active_loans_or_holds=active_loans_or_holds_patron_count,
                    loans=loan_count,
                    holds=hold_count,
                ),
                inventory=dict(
                    titles=title_count,
                    licenses=license_count,
                    available_licenses=available_license_count,
                ),
                collections=library_collection_counts,
            )

        total_patrons = sum([stats.get("patrons", {}).get("total", 0)
                             for stats in list(library_stats.values())])
        total_with_active_loans = sum([stats.get("patrons", {}).get("with_active_loans", 0)
                                       for stats in list(library_stats.values())])
        total_with_active_loans_or_holds = sum([stats.get("patrons", {}).get("with_active_loans_or_holds", 0)
                                                for stats in list(library_stats.values())])

        # TODO: show shared collection loans and holds for libraries outside this circ manager?
        total_loans = sum([stats.get("patrons", {}).get("loans", 0)
                           for stats in list(library_stats.values())])
        total_holds = sum([stats.get("patrons", {}).get("holds", 0)
                           for stats in list(library_stats.values())])

        library_stats["total"] = dict(
            patrons=dict(
                total=total_patrons,
                with_active_loans=total_with_active_loans,
                with_active_loans_or_holds=total_with_active_loans_or_holds,
                loans=total_loans,
                holds=total_holds,
            ),
            inventory=dict(
                titles=total_title_count,
                licenses=total_license_count,
                available_licenses=total_available_license_count,
            ),
            collections=collection_counts,
        )

        return library_stats

    def circulation_events(self):
        """Return dict of circulation events for a given work.

        Optional "num" parameter in request to change number of results.

        Returns:
            dict: A dictionary of events
        """
        annotator = AdminAnnotator(self.circulation, flask.request.library)
        num = min(int(flask.request.args.get("num", "100")), 500)

        results = self._db.query(CirculationEvent) \
            .join(LicensePool) \
            .join(Work) \
            .join(DataSource) \
            .join(Identifier) \
            .order_by(nullslast(desc(CirculationEvent.start))) \
            .limit(num) \
            .all()

        events = [{
            "id": result.id,
            "type": result.type,
            "time": result.start,
            "book": {
                "title": result.license_pool.work.title,
                "url": annotator.permalink_for(
                    result.license_pool.work,
                    result.license_pool,
                    result.license_pool.identifier
                )
            }
        } for result in results]

        return dict({"circulation_events": events})

    def bulk_circulation_events(self, analytics_exporter=None):
        date_format = "%Y-%m-%d"

        def get_date(field):
            # Return a date or datetime object representing the
            # _beginning_ of the asked-for day, local time.
            #
            # Unlike most places in this application we do not
            # use UTC since the sime was selected by a human user.
            today = date.today()
            value = flask.request.args.get(field, None)
            if not value:
                return today

            try:
                return datetime.strptime(value, date_format).date()
            except ValueError:
                # This won't happen in real life since the format is
                # controlled by the calendar widget. There's no need
                # to send an error message -- just use the default
                # date.
                return today

        # For the start date we should use the _beginning_ of the day,
        # which is what get_date returns.
        date_start = get_date("date")

        # When running the search, the cutoff is the first moment of
        # the day _after_ the end date. When generating the filename,
        # though, we should use the date provided by the user.
        date_end_label = get_date("dateEnd")
        date_end = date_end_label + timedelta(days=1)
        locations = flask.request.args.get("locations", None)
        library = getattr(flask.request, 'library', None)
        library_short_name = library.short_name if library else None

        analytics_exporter = analytics_exporter or LocalAnalyticsExporter()
        data = analytics_exporter.export(
            self._db, date_start, date_end, locations, library
        )
        return (data, date_start.strftime(date_format),
                date_end_label.strftime(date_format), library_short_name)

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266


class SettingsController(AdminCirculationManagerController):
    ##### Class Constants ####################################################  # noqa: E266

    METADATA_SERVICE_URI_TYPE = (
        'application/opds+json;profile=https://librarysimplified.org/rel/profile/metadata-service'
    )

    NO_MIRROR_INTEGRATION = "NO_MIRROR"

    PROVIDER_APIS = [
        OPDSImporter,
        OPDSForDistributorsAPI,
        OPDS2Importer,
        ProQuestOPDS2Importer,
        OverdriveAPI,
        OdiloAPI,
        BibliothecaAPI,
        Axis360API,
        RBDigitalAPI,
        EnkiAPI,
        ODLAPI,
        SharedODLAPI,
        FeedbooksOPDSImporter,
        LCPAPI
    ]

    ##### Public Interface / Magic Methods ###################################  # noqa: E266

    def check_name_unique(self, new_service, name):
        """
        A service cannot be created with, or edited to have, the same name as a service that
        already exists. This method is used by analytics_services, cdn_services,
        discovery_services, metadata_services, and sitewide_services.
        """
        existing_service = get_one(self._db, ExternalIntegration, name=name)
        if existing_service and not existing_service.id == new_service.id:
            # Without checking that the IDs are different, you can't save
            # changes to an existing service unless you've also changed its name.
            return INTEGRATION_NAME_ALREADY_IN_USE

    def check_url_unique(self, new_service, url, protocol, goal):
        """
        Enforce a rule that a given circulation manager can only have one integration that
        uses a given URL for a certain purpose.

        Whether to enforce this rule for a given type of integration is up to you -- it's
        a good general rule but there are conceivable exceptions.

        This method is used by discovery_services.
        """
        if not url:
            return

        # Look for the given URL as well as minor variations.
        #
        # We can't use urlparse to ignore minor differences in URLs
        # because we're doing the comparison in the database.
        urls = list(self.url_variants(url))

        qu = self._db.query(ExternalIntegration).join(
            ExternalIntegration.settings
        ).filter(
            # Protocol must match.
            ExternalIntegration.protocol == protocol
        ).filter(
            # Goal must match.
            ExternalIntegration.goal == goal
        ).filter(
            ConfigurationSetting.key == ExternalIntegration.URL
        ).filter(
            # URL must be one of the URLs we're concerned about.
            ConfigurationSetting.value.in_(urls)
        ).filter(
            # But don't count the service we're trying to edit.
            ExternalIntegration.id != new_service.id
        )
        if qu.count() > 0:
            return INTEGRATION_URL_ALREADY_IN_USE

    def look_up_service_by_id(self, id, protocol, goal=None):
        """
        Find an existing service, and make sure that the user is not trying to edit its protocol.
        This method is used by analytics_services, cdn_services, metadata_services,
        and sitewide_services.
        """
        if not goal:
            goal = self.goal

        service = get_one(self._db, ExternalIntegration, id=id, goal=goal)

        if not service:
            return MISSING_SERVICE

        if protocol and (protocol != service.protocol):
            return CANNOT_CHANGE_PROTOCOL

        return service

    def set_protocols(self, service, protocol, protocols=None):
        """
        Validate the protocol that the user has submitted; depending on whether the validations pass,
        either save it to this metadata service or return an error message. This method is used by
        analytics_services, cdn_services, discovery_services, metadata_services, and sitewide_services.
        """
        if not protocols:
            protocols = self.protocols

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(
            service, protocol)

        if isinstance(result, ProblemDetail):
            return result

    def validate_protocol(self, protocols=None):
        protocols = protocols or self.protocols
        if flask.request.form.get("protocol") not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

    def validate_formats(self, settings=None, validator=None):
        # If the service has self.protocols set, we can extract the list of settings here;
        # otherwise, the settings have to be passed in as an argument--either a list or
        # a string.
        validator = validator or Validator()
        settings = settings or self._get_settings()
        form = flask.request.form or None
        try:
            files = flask.request.files
        except Exception:
            files = None

        error = validator.validate(settings, dict(form=form, files=files))
        if error:
            return error

    ##### Private Methods ####################################################  # noqa: E266

    def _get_integration_library_info(self, integration, library, protocol):
        library_info = dict(short_name=library.short_name)
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")

            if setting.get("type") == "list":
                value = ConfigurationSetting.for_library_and_externalintegration(
                    self._db, key, library, integration
                ).json_value
            else:
                value = ConfigurationSetting.for_library_and_externalintegration(
                    self._db, key, library, integration
                ).value

            if value:
                library_info[key] = value

        return library_info

    def _get_integration_info(self, goal, protocols):
        services = []
        for service in self._db.query(ExternalIntegration).filter(ExternalIntegration.goal == goal):
            candidates = [p for p in protocols if p.get(
                "name") == service.protocol]

            if not candidates:
                continue

            protocol = candidates[0]
            libraries = []

            if not protocol.get("sitewide") or protocol.get("library_settings"):
                for library in service.libraries:
                    libraries.append(self._get_integration_library_info(
                        service, library, protocol))

            settings = dict()
            for setting in protocol.get("settings", []):
                key = setting.get("key")

                # If the setting is a covers or books mirror, we need to get the value from
                # ExternalIntegrationLink and not from a ConfigurationSetting.
                if key.endswith('mirror_integration_id'):
                    storage_integration = get_one(
                        self._db, ExternalIntegrationLink, external_integration_id=service.id
                    )

                    if storage_integration:
                        value = str(storage_integration.other_integration_id)
                    else:
                        value = self.NO_MIRROR_INTEGRATION

                else:
                    if setting.get("type") in ("list", "menu"):
                        value = ConfigurationSetting.for_externalintegration(
                            key, service).json_value
                    else:
                        value = ConfigurationSetting.for_externalintegration(
                            key, service).value

                settings[key] = value

            service_info = dict(id=service.id, name=service.name, protocol=service.protocol,
                                settings=settings, libraries=libraries)

            if "test_search_term" in [x.get("key") for x in protocol.get("settings")]:
                service_info["self_test_results"] = self._get_prior_test_results(
                    service)

            services.append(service_info)

        return services

    def _set_integration_setting(self, integration, setting):
        setting_key = setting.get("key")
        setting_type = setting.get("type")

        if setting_type == "list" and not setting.get("options"):
            value = [item for item in flask.request.form.getlist(
                setting_key) if item]
            if value:
                value = json.dumps(value)
        elif setting_type == 'menu':
            value = self._get_menu_values(setting_key, flask.request.form)
        else:
            value = flask.request.form.get(setting_key)

        if value and setting.get("options"):
            # This setting can only take on values that are in its list of options.
            allowed_values = [option.get("key")
                              for option in setting.get("options")]
            submitted_values = value

            if not isinstance(submitted_values, list):
                submitted_values = [submitted_values]

            for submitted_value in submitted_values:
                if submitted_value not in allowed_values:
                    return INVALID_CONFIGURATION_OPTION.detailed(lgt(
                        "The configuration value for %(setting)s is invalid.",
                        setting=setting.get("label"),
                    ))

        if not value and setting.get("required") and "default" not in list(setting.keys()):
            return INCOMPLETE_CONFIGURATION.detailed(
                lgt("The configuration is missing a required setting: %(setting)s",
                    setting=setting.get("label")))

        if isinstance(value, list):
            value = json.dumps(value)

        integration.setting(setting_key).value = value

    def _set_integration_library(self, integration, library_info, protocol):
        library = get_one(self._db, Library,
                          short_name=library_info.get("short_name"))
        if not library:
            msg = "You attempted to add the integration to %(library_short_name)s, but it does not exist."
            return NO_SUCH_LIBRARY.detailed(lgt(msg, library_short_name=library_info.get("short_name")))

        integration.libraries += [library]
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")
            value = library_info.get(key)

            if value and setting.get("type") == "list" and not setting.get("options"):
                value = json.dumps(value)

            if setting.get("options") and value not in [option.get("key") for option in setting.get("options")]:
                return INVALID_CONFIGURATION_OPTION.detailed(lgt(
                    "The configuration value for %(setting)s is invalid.",
                    setting=setting.get("label"),
                ))

            if not value and setting.get("required"):
                return INCOMPLETE_CONFIGURATION.detailed(
                    lgt("The configuration is missing a required setting: %(setting)s for library %(library)s",
                        setting=setting.get("label"),
                        library=library.short_name,))

            ConfigurationSetting.for_library_and_externalintegration(
                self._db, key, library, integration).value = value

    def _set_integration_settings_and_libraries(self, integration, protocol):
        settings = protocol.get("settings")
        for setting in settings:
            if not setting.get('key').endswith('mirror_integration_id'):
                result = self._set_integration_setting(integration, setting)
                if isinstance(result, ProblemDetail):
                    return result

        if not protocol.get("sitewide") or protocol.get("library_settings"):
            integration.libraries = []

            libraries = []
            if flask.request.form.get("libraries"):
                libraries = json.loads(flask.request.form.get("libraries"))

            for library_info in libraries:
                result = self._set_integration_library(
                    integration, library_info, protocol)
                if isinstance(result, ProblemDetail):
                    return result

        return True

    def _delete_integration(self, integration_id, goal):
        if flask.request.method != "DELETE":
            return

        self.require_system_admin()
        integration = get_one(self._db, ExternalIntegration,
                              id=integration_id, goal=goal)

        if not integration:
            return MISSING_SERVICE

        self._db.delete(integration)

        return Response(str(lgt("Deleted")), 200)

    def _get_collection_protocols(self, provider_apis):
        protocols = self._get_integration_protocols(
            provider_apis, protocol_name_attr="NAME")
        protocols.append(
            {
                'name': ExternalIntegration.MANUAL,
                'label': lgt('Manual import'),
                'description': lgt('Books will be manually added to the circulation manager, '
                                   'not imported automatically through a protocol.'),
                'settings': []
            }
        )

        return protocols

    def _get_prior_test_results(self, item, protocol_class=None, *extra_args):
        # :param item: An ExternalSearchIndex, an ExternalIntegration for patron authentication, or a Collection
        if not protocol_class and hasattr(self, "protocol_class"):
            protocol_class = self.protocol_class

        if not item:
            return None

        self_test_results = None

        try:
            if self.type == "collection":
                if not item.protocol or not len(item.protocol):
                    return None

                provider_apis = list(self.PROVIDER_APIS)
                provider_apis.append(OPDSImportMonitor)

                if item.protocol == OPDSImportMonitor.PROTOCOL:
                    protocol_class = OPDSImportMonitor

                if protocol_class in provider_apis and issubclass(protocol_class, HasSelfTests):
                    if (item.protocol == OPDSImportMonitor.PROTOCOL):
                        extra_args = (OPDSImporter,)
                    else:
                        extra_args = ()

                    self_test_results = protocol_class.prior_test_results(
                        self._db, protocol_class, self._db, item, *extra_args
                    )

            elif self.type == "search service":
                self_test_results = ExternalSearchIndex.prior_test_results(
                    self._db, None, self._db, item
                )
            elif self.type == "metadata service" and protocol_class:
                self_test_results = protocol_class.prior_test_results(
                    self._db, *extra_args
                )
            elif self.type == "patron authentication service":
                library = None
                if len(item.libraries):
                    library = item.libraries[0]
                    self_test_results = protocol_class.prior_test_results(
                        self._db, None, library, item
                    )
                else:
                    msg = (
                        "You must associate this service with at least one library "
                        "before you can run self tests for it."
                    )
                    self_test_results = dict(exception=lgt(msg), disabled=True)

        except Exception as e:
            # This is bad, but not so bad that we should short-circuit
            # this whole process -- that might prevent an admin from
            # making the configuration changes necessary to fix
            # this problem.
            message = lgt("Exception getting self-test results for %s %s: %s")
            error_message = str(e)
            args = (self.type, item.name, error_message)
            logging.warning(message, *args, exc_info=error_message)
            self_test_results = dict(exception=message % args)

        return self_test_results

    def _mirror_integration_settings(self):
        """
        Create a setting interface for selecting a storage integration to
        be used when mirroring items from a collection.
        """
        integrations = self._db.query(ExternalIntegration).filter(
            ExternalIntegration.goal == ExternalIntegration.STORAGE_GOAL
        ).order_by(
            ExternalIntegration.name
        )

        if not integrations.all():
            return

        mirror_integration_settings = copy.deepcopy(
            ExternalIntegrationLink.COLLECTION_MIRROR_SETTINGS)

        for integration in integrations:
            book_covers_bucket = integration.setting(
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY).value
            open_access_bucket = integration.setting(
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY).value
            protected_access_bucket = integration.setting(
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY).value

            for setting in mirror_integration_settings:
                if setting['key'] == ExternalIntegrationLink.COVERS_KEY and book_covers_bucket:
                    setting['options'].append(
                        {'key': str(integration.id), 'label': integration.name})
                elif setting['key'] == ExternalIntegrationLink.OPEN_ACCESS_BOOKS_KEY:
                    if open_access_bucket:
                        setting['options'].append(
                            {'key': str(integration.id), 'label': integration.name})
                elif setting['key'] == ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS_KEY:
                    if protected_access_bucket:
                        setting['options'].append(
                            {'key': str(integration.id), 'label': integration.name})

        return mirror_integration_settings

    def _create_integration(self, protocol_definitions, protocol, goal):
        """
        Create a new ExternalIntegration for the given protocol and goal, assuming that doing
        so is compatible with the protocol's definition.

        :return: A 2-tuple (result, is_new). `result` will be an ExternalIntegration if one
            could be created, and a ProblemDetail otherwise.
        """
        if not protocol:
            return NO_PROTOCOL_FOR_NEW_SERVICE, False

        matches = [x for x in protocol_definitions if x.get(
            'name') == protocol]

        if not matches:
            return UNKNOWN_PROTOCOL, False

        definition = matches[0]

        # Most of the time there can be multiple ExternalIntegrations with the same protocol and goal...
        allow_multiple = True
        m = create
        args = (self._db, ExternalIntegration)
        kwargs = dict(protocol=protocol, goal=goal)

        if definition.get('cardinality') == 1:  # ...but not all the time.
            allow_multiple = False
            existing = get_one(*args, **kwargs)

            if existing is not None:
                # We were asked to create a new ExternalIntegration but there's already one for
                # this protocol, which is not allowed.
                return DUPLICATE_INTEGRATION, False

            m = get_one_or_create

        (integration, is_new) = m(*args, **kwargs)

        if not is_new and not allow_multiple:
            # This can happen, despite our check above, in a race condition where two clients
            # try simultaneously to create two integrations of the same type.
            return DUPLICATE_INTEGRATION, False

        return integration, is_new

    def _get_settings(self):
        if hasattr(self, 'protocols'):
            [protocol] = [p for p in self.protocols if p.get(
                "name") == flask.request.form.get("protocol")]
            return protocol.get("settings")

        return []

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266
    @classmethod
    def url_variants(cls, url, check_protocol_variant=True):
        """
        Generate minor variants of a URL -- HTTP vs HTTPS, trailing slash vs not, etc.

        Technically these are all distinct URLs, but in real life they generally mean someone
        typed the same URL slightly differently. Since this isn't an exact science, this doesn't
        need to catch all variant URLs, only the most common ones.
        """
        if not Validator()._is_url(url, []):    # An invalid URL has no variants.
            return

        # A URL is a 'variant' of itself.
        yield url

        # Adding or removing a slash creates a variant.
        if url.endswith("/"):
            yield url[:-1]
        else:
            yield url + '/'

        # Changing protocols may create one or more variants.
        https = "https://"
        http = "http://"

        if check_protocol_variant:
            protocol_variant = None
            if url.startswith(https):
                protocol_variant = url.replace(https, http, 1)
            elif url.startswith(http):
                protocol_variant = url.replace(http, https, 1)

            if protocol_variant:
                for v in cls.url_variants(protocol_variant, False):
                    yield v

    ##### Private Class Methods ##############################################  # noqa: E266
    @classmethod
    def _get_integration_protocols(cls, provider_apis, protocol_name_attr="__module__"):
        protocols = []
        for api in provider_apis:
            protocol = dict()
            name = getattr(api, protocol_name_attr)
            protocol["name"] = name

            label = getattr(api, "NAME", name)
            protocol["label"] = label

            description = getattr(api, "DESCRIPTION", None)
            if description is not None:
                protocol["description"] = description

            instructions = getattr(api, "INSTRUCTIONS", None)
            if instructions is not None:
                protocol["instructions"] = instructions

            sitewide = getattr(api, "SITEWIDE", None)
            if sitewide is not None:
                protocol["sitewide"] = sitewide

            settings = getattr(api, "SETTINGS", [])
            protocol["settings"] = list(settings)

            child_settings = getattr(api, "CHILD_SETTINGS", None)
            if child_settings is not None:
                protocol["child_settings"] = list(child_settings)

            library_settings = getattr(api, "LIBRARY_SETTINGS", None)
            if library_settings is not None:
                protocol["library_settings"] = list(library_settings)

            cardinality = getattr(api, 'CARDINALITY', None)
            if cardinality is not None:
                protocol['cardinality'] = cardinality

            supports_registration = getattr(api, "SUPPORTS_REGISTRATION", None)
            if supports_registration is not None:
                protocol['supports_registration'] = supports_registration
            supports_staging = getattr(api, "SUPPORTS_STAGING", None)
            if supports_staging is not None:
                protocol['supports_staging'] = supports_staging

            protocols.append(protocol)
        return protocols

    @staticmethod
    def _get_menu_values(setting_key, form):
        """circulation-web returns "menu" values in a different format not compatible with werkzeug.MultiDict semantics:
            {setting_key}_{menu} = {value_in_the_dropdown_box}
            {setting_key}_{setting_value1} = {setting_label1}
            {setting_key}_{setting_value2} = {setting_label2}
            ...
            {setting_key}_{setting_valueN} = {setting_labelN}

        It means we can't use werkzeug.MultiDict.getlist method and have to extract them manually.

        :param setting_key: Setting's key
        :type setting_key: str

        :param form: Multi-dictionary containing input values submitted by the user
            and sent back to CM by circulation-web
        :type form: werkzeug.MultiDict

        :return: List of "menu" values
        :rtype: List[str]
        """
        values = []

        for form_item_key in list(form.keys()):
            if setting_key in form_item_key:
                value = form_item_key.replace(setting_key, '').lstrip('_')

                if value != 'menu':
                    values.append(value)

        return values


class SitewideRegistrationController(SettingsController):
    """
    A controller for managing a circulation manager's registrations with external services.

    Currently the only supported site-wide registration is with a metadata wrangler.
    The protocol for registration with library registries and ODL collections is similar,
    but those registrations happen on the level of the individual library, not on the level
    of the circulation manager.
    """

    ##### Class Constants ####################################################  # noqa: E266

    ##### Public Interface / Magic Methods ###################################  # noqa: E266

    def process_sitewide_registration(self, integration, do_get=HTTP.debuggable_get, do_post=HTTP.debuggable_post):
        """
        Performs a sitewide registration for a particular service.

        :return: A ProblemDetail or, if successful, None
        """

        self.require_system_admin()

        if not integration:
            return MISSING_SERVICE

        catalog_response = self.get_catalog(do_get, integration.url)
        if isinstance(catalog_response, ProblemDetail):
            return catalog_response

        if isinstance(self.check_content_type(catalog_response), ProblemDetail):
            return self.check_content_type(catalog_response)

        catalog = catalog_response.json()
        links = catalog.get('links', [])

        register_url = self.get_registration_link(catalog, links)
        if isinstance(register_url, ProblemDetail):
            return register_url

        headers = self.update_headers(integration)
        if isinstance(headers, ProblemDetail):
            return headers

        response = self.register(register_url, headers, do_post)
        if isinstance(response, ProblemDetail):
            return response

        shared_secret = self.get_shared_secret(response)
        if isinstance(shared_secret, ProblemDetail):
            return shared_secret

        (_, private_key) = self.manager.sitewide_key_pair
        decryptor = Configuration.cipher(private_key)
        shared_secret = decryptor.decrypt(base64.b64decode(shared_secret))
        integration.password = shared_secret.decode("utf-8")

    def get_catalog(self, do_get, url):
        """Get the catalog for this service."""

        try:
            response = do_get(url)
        except Exception as e:
            return REMOTE_INTEGRATION_FAILED.detailed(str(e))

        if isinstance(response, ProblemDetail):
            return response
        return response

    def check_content_type(self, catalog_response):
        """Make sure the catalog for the service is in a valid format."""

        content_type = catalog_response.headers.get('Content-Type')
        if content_type != 'application/opds+json':
            return REMOTE_INTEGRATION_FAILED.detailed(
                lgt('The service did not provide a valid catalog.')
            )

    def get_registration_link(self, catalog, links):
        """Get the link for registration from the catalog."""

        def _register_link_filter(link):
            return bool(link.get('rel') == 'register' and link.get('type') == self.METADATA_SERVICE_URI_TYPE)

        register_urls = list(filter(_register_link_filter, links))
        if not register_urls:
            return REMOTE_INTEGRATION_FAILED.detailed(
                lgt('The service did not provide a register link.')
            )

        # Get the full registration url.
        register_url = register_urls[0].get('href')
        if not register_url.startswith('http'):
            # We have a relative path. Create a full registration url.
            base_url = catalog.get('id')
            register_url = urllib.parse.urljoin(base_url, register_url)

        return register_url

    def update_headers(self, integration):
        """If the integration has an existing shared_secret, use it to access the
        server and update it."""

        # NOTE: This is no longer technically necessary since we prove
        # ownership with a signed JWT.
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        if integration.password:
            token = base64.b64encode(integration.password.encode('utf-8'))
            headers['Authorization'] = 'Bearer ' + token
        return headers

    def register(self, register_url, headers, do_post):
        """Register this server using the sitewide registration document."""

        try:
            body = self.sitewide_registration_document()
            response = do_post(
                register_url, body, allowed_response_codes=['2xx'],
                headers=headers
            )
        except Exception as e:
            return REMOTE_INTEGRATION_FAILED.detailed(str(e))
        return response

    def get_shared_secret(self, response):
        """Find the shared secret which we need to use in order to register this
        service, or return an error message if there is no shared secret."""

        registration_info = response.json()
        shared_secret = registration_info.get(
            'metadata', {}).get('shared_secret')
        if not shared_secret:
            return REMOTE_INTEGRATION_FAILED.detailed(
                lgt('The service did not provide registration information.')
            )
        return shared_secret

    def sitewide_registration_document(self):
        """Generate the document to be sent as part of a sitewide registration
        request.

        :return: A dictionary with keys 'url' and 'jwt'. 'url' is the URL to
            this site's public key document, and 'jwt' is a JSON Web Token
            proving control over that URL.
        """

        public_key, private_key = self.manager.sitewide_key_pair
        # Advertise the public key so that the foreign site can encrypt things for us.
        public_key_dict = dict(type='RSA', value=public_key)    # noqa: F841
        public_key_url = self.url_for('public_key_document')
        in_one_minute = utc_now() + timedelta(seconds=60)
        payload = {'exp': in_one_minute}
        # Sign a JWT with the private key to prove ownership of the site.
        token = jwt.encode(payload, private_key, algorithm='RS256')
        return dict(url=public_key_url, jwt=token)

    ##### Private Methods ####################################################  # noqa: E266

    ##### Properties and Getters/Setters #####################################  # noqa: E266

    ##### Class Methods ######################################################  # noqa: E266

    ##### Private Class Methods ##############################################  # noqa: E266
