from nose.tools import set_trace
import logging
import sys
import os
import base64
import random
import json
import jwt
import re
import urllib
import urlparse
import uszipcode

import flask
from flask import (
    Response,
    redirect,
)
from flask_babel import lazy_gettext as _
from PIL import Image, ImageDraw, ImageFont
import textwrap
from StringIO import StringIO
from api.authenticator import (
    CannotCreateLocalPatron,
    PatronData,
)
from core.external_search import ExternalSearchIndex
from core.model import (
    create,
    get_one,
    get_one_or_create,
    Admin,
    AdminRole,
    CirculationEvent,
    Classification,
    Collection,
    Complaint,
    ConfigurationSetting,
    Contributor,
    CustomList,
    CustomListEntry,
    DataSource,
    Edition,
    ExternalIntegration,
    Genre,
    Hold,
    Hyperlink,
    Identifier,
    Library,
    LicensePool,
    Loan,
    Measurement,
    Patron,
    PresentationCalculationPolicy,
    Representation,
    RightsStatus,
    Session,
    Subject,
    Timestamp,
    Work,
    WorkGenre,
)
from core.lane import (Lane, WorkList)
from core.log import (LogConfiguration, SysLogger, Loggly, CloudwatchLogs)
from core.util.problem_detail import ProblemDetail
from core.metadata_layer import (
    Metadata,
    LinkData,
    ReplacementPolicy,
)
from core.mirror import MirrorUploader
from core.util.http import HTTP
from api.problem_details import *
from api.admin.exceptions import *
from core.util import LanguageCodes

from api.config import (
    Configuration,
    CannotLoadConfiguration
)
from api.lanes import create_default_lanes
from api.admin.google_oauth_admin_authentication_provider import GoogleOAuthAdminAuthenticationProvider
from api.admin.password_admin_authentication_provider import PasswordAdminAuthenticationProvider

from api.controller import CirculationManagerController
from api.coverage import MetadataWranglerCollectionRegistrar
from core.app_server import (
    entry_response,
    feed_response,
    load_pagination_from_request,
)
from core.opds import AcquisitionFeed
from api.admin.opds import AdminAnnotator, AdminFeed
from collections import Counter
from core.classifier import (
    genres,
    SimplifiedGenreClassifier,
    NO_NUMBER,
    NO_VALUE
)
from datetime import datetime, timedelta
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import desc, nullslast, or_, and_, distinct, select, join
from sqlalchemy.orm import lazyload

from api.admin.templates import admin as admin_template
from api.authenticator import LibraryAuthenticator

from core.opds_import import (OPDSImporter, OPDSImportMonitor)
from api.feedbooks import FeedbooksOPDSImporter
from api.opds_for_distributors import OPDSForDistributorsAPI
from api.overdrive import OverdriveAPI
from api.odilo import OdiloAPI
from api.bibliotheca import BibliothecaAPI
from api.axis import Axis360API
from api.rbdigital import RBDigitalAPI
from api.enki import EnkiAPI
from api.odl import ODLWithConsolidatedCopiesAPI, SharedODLAPI
from core.local_analytics_provider import LocalAnalyticsProvider

from api.adobe_vendor_id import AuthdataUtility
from api.admin.template_styles import *

from core.selftest import HasSelfTests

def setup_admin_controllers(manager):
    """Set up all the controllers that will be used by the admin parts of the web app."""
    if not manager.testing:
        try:
            manager.config = Configuration.load(manager._db)
        except CannotLoadConfiguration, e:
            logging.error("Could not load configuration file: %s", e)
            sys.exit()

    manager.admin_view_controller = ViewController(manager)
    manager.admin_sign_in_controller = SignInController(manager)
    manager.timestamps_controller = TimestampsController(manager)
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
    manager.admin_discovery_services_controller = DiscoveryServicesController(manager)
    from api.admin.controller.discovery_service_library_registrations import DiscoveryServiceLibraryRegistrationsController
    manager.admin_discovery_service_library_registrations_controller = DiscoveryServiceLibraryRegistrationsController(manager)
    from api.admin.controller.cdn_services import CDNServicesController
    manager.admin_cdn_services_controller = CDNServicesController(manager)
    from api.admin.controller.analytics_services import AnalyticsServicesController
    manager.admin_analytics_services_controller = AnalyticsServicesController(manager)
    from api.admin.controller.metadata_services import MetadataServicesController
    manager.admin_metadata_services_controller = MetadataServicesController(manager)
    from api.admin.controller.patron_auth_services import PatronAuthServicesController
    manager.admin_patron_auth_services_controller = PatronAuthServicesController(manager)
    from api.admin.controller.patron_auth_service_self_tests import PatronAuthServiceSelfTestsController
    manager.admin_patron_auth_service_self_tests_controller = PatronAuthServiceSelfTestsController(manager)
    from api.admin.controller.admin_auth_services import AdminAuthServicesController
    manager.admin_auth_services_controller = AdminAuthServicesController(manager)
    from api.admin.controller.collection_settings import CollectionSettingsController
    manager.admin_collection_settings_controller = CollectionSettingsController(manager)
    from api.admin.controller.collection_self_tests import CollectionSelfTestsController
    manager.admin_collection_self_tests_controller = CollectionSelfTestsController(manager)
    from api.admin.controller.collection_library_registrations import CollectionLibraryRegistrationsController
    manager.admin_collection_library_registrations_controller = CollectionLibraryRegistrationsController(manager)
    from api.admin.controller.sitewide_settings import SitewideConfigurationSettingsController
    manager.admin_sitewide_configuration_settings_controller = SitewideConfigurationSettingsController(manager)
    from api.admin.controller.library_settings import LibrarySettingsController
    manager.admin_library_settings_controller = LibrarySettingsController(manager)
    from api.admin.controller.individual_admin_settings import IndividualAdminSettingsController
    manager.admin_individual_admin_settings_controller = IndividualAdminSettingsController(manager)
    from api.admin.controller.sitewide_services import *
    manager.admin_sitewide_services_controller = SitewideServicesController(manager)
    manager.admin_logging_services_controller = LoggingServicesController(manager)
    from api.admin.controller.search_service_self_tests import SearchServiceSelfTestsController
    manager.admin_search_service_self_tests_controller = SearchServiceSelfTestsController(manager)
    manager.admin_search_services_controller = SearchServicesController(manager)
    manager.admin_storage_services_controller = StorageServicesController(manager)
    from api.admin.controller.catalog_services import *
    manager.admin_catalog_services_controller = CatalogServicesController(manager)

class AdminController(object):

    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db
        self.url_for = self.manager.url_for
        self.cdn_url_for = self.manager.cdn_url_for

    @property
    def admin_auth_providers(self):
        auth_providers = []
        auth_service = ExternalIntegration.admin_authentication(self._db)
        if auth_service and auth_service.protocol == ExternalIntegration.GOOGLE_OAUTH:
            auth_providers.append(GoogleOAuthAdminAuthenticationProvider(
                auth_service,
                self.url_for('google_auth_callback'),
                test_mode=self.manager.testing,
            ))
        if Admin.with_password(self._db).count() != 0:
            auth_providers.append(PasswordAdminAuthenticationProvider(
                auth_service,
            ))
        return auth_providers

    def admin_auth_provider(self, type):
        # Return an auth provider with the given type.
        # If no auth provider has this type, return None.
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

        admin, is_new = get_one_or_create(
            self._db, Admin, email=admin_details['email']
        )
        admin.update_credentials(
            self._db,
            credential=admin_details.get('credentials'),
        )
        if is_new and admin_details.get("roles"):
            for role in admin_details.get("roles"):
                if role.get("role") in AdminRole.ROLES:
                    library = Library.lookup(self._db, role.get("library"))
                    if role.get("library") and not library:
                        self.log.warn("%s authentication provider specifiec an unknown library for a new admin: %s" % (admin_details.get("type"), role.get("library")))
                    else:
                        admin.add_role(role.get("role"), library)
                else:
                    self.log.warn("%s authentication provider specified an unknown role for a new admin: %s" % (admin_details.get("type"), role.get("role")))

        # Set up the admin's flask session.
        flask.session["admin_email"] = admin_details.get("email")
        flask.session["auth_type"] = admin_details.get("type")

        # A permanent session expires after a fixed time, rather than
        # when the user closes the browser.
        flask.session.permanent = True

        # If this is the first time an admin has been authenticated,
        # make sure there is a value set for the sitewide BASE_URL_KEY
        # setting. If it's not set, set it to the hostname of the
        # current request. This assumes the first authenticated admin
        # is accessing the admin interface through the hostname they
        # want to be used for the site itself.
        base_url = ConfigurationSetting.sitewide(
            self._db, Configuration.BASE_URL_KEY
        )
        if not base_url.value:
            base_url.value = urlparse.urljoin(flask.request.url, '/')

        return admin

    def check_csrf_token(self):
        """Verifies that the CSRF token in the form data or X-CSRF-Token header
        matches the one in the session cookie.
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
        return base64.b64encode(os.urandom(24))

class AdminCirculationManagerController(CirculationManagerController):
    """Parent class that provides methods for verifying an admin's roles."""

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

class ViewController(AdminController):
    def __call__(self, collection, book, path=None):
        setting_up = (self.admin_auth_providers == [])
        email = None
        roles = []
        if not setting_up:
            admin = self.authenticated_admin_from_request()
            if isinstance(admin, ProblemDetail):
                redirect_url = flask.request.url
                if (collection):
                    quoted_collection = urllib.quote(collection)
                    redirect_url = redirect_url.replace(
                        quoted_collection,
                        quoted_collection.replace("/", "%2F"))
                if (book):
                    quoted_book = urllib.quote(book)
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
                        return Response(_("Your admin account doesn't have access to any libraries. Contact your library manager for assistance."), 200)
                    return redirect(self.url_for('admin_view', collection=library_name))

            email = admin.email
            for role in admin.roles:
                if role.library:
                    roles.append({ "role": role.role, "library": role.library })
                else:
                    roles.append({ "role": role.role })

        csrf_token = flask.request.cookies.get("csrf_token") or self.generate_csrf_token()

        local_analytics = get_one(
            self._db, ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL)
        show_circ_events_download = (local_analytics != None)

        response = Response(flask.render_template_string(
            admin_template,
            csrf_token=csrf_token,
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

class TimestampsController(AdminCirculationManagerController):
    """Returns a dict: each key is a type of service (script, monitor, or coverage provider);
    each value is a nested dict in which timestamps are organized by service name and then by collection ID."""

    def diagnostics(self):
        self.require_system_admin()
        timestamps = self._db.query(Timestamp).order_by(Timestamp.start)
        sorted = self._sort_by_type(timestamps)
        for type, services in sorted.items():
            for service in services:
                by_collection = self._sort_by_collection(sorted[type][service])
                sorted[type][service] = by_collection
        return sorted

    def _sort_by_type(self, timestamps):
        """Takes a list of Timestamp objects.  Returns a dict: each key is a type of service
        (script, monitor, or coverage provider); each value is a dict in which the keys are the names
        of services and the values are lists of timestamps."""

        result = {}
        for ts in timestamps:
            info = self._extract_info(ts)
            result.setdefault((ts.service_type or "other"), []).append(info)

        for type, data in result.items():
            result[type] = self._sort_by_service(data)

        return result

    def _sort_by_service(self, timestamps):
        """Returns a dict: each key is the name of a service; each value is a list of timestamps."""

        result = {}
        for timestamp in timestamps:
            result.setdefault(timestamp.get("service"), []).append(timestamp)
        return result

    def _sort_by_collection(self, timestamps):
        """Takes a list of timestamps; turns it into a dict in which each key is a
        collection ID and each value is a list of the timestamps associated with that collection."""

        result = {}
        for timestamp in timestamps:
            result.setdefault(timestamp.get("collection_name"), []).append(timestamp)
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

class SignInController(AdminController):

    ERROR_RESPONSE_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
<head><meta charset="utf8"></head>
<body>
<p><strong>%(status_code)d ERROR:</strong> %(message)s</p>
</body>
</html>"""

    SIGN_IN_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
<head><meta charset="utf8"></head>
<body style="{}">
<h1>Library Simplified</h1>
%(auth_provider_html)s
</body>
</html>""".format(body_style)

    def sign_in(self):
        """Redirects admin if they're signed in, or shows the sign in page."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        admin = self.authenticated_admin_from_request()

        if isinstance(admin, ProblemDetail):
            redirect_url = flask.request.args.get("redirect")
            auth_provider_html = [auth.sign_in_template(redirect_url) for auth in self.admin_auth_providers]
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

        auth = self.admin_auth_provider(GoogleOAuthAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin_details, redirect_url = auth.callback(self._db, flask.request.args)
        if isinstance(admin_details, ProblemDetail):
            return self.error_response(admin_details)

        admin = self.authenticated_admin(admin_details)
        return redirect(redirect_url, Response=Response)

    def password_sign_in(self):
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        auth = self.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
        if not auth:
            return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED

        admin_details, redirect_url = auth.sign_in(self._db, flask.request.form)
        if isinstance(admin_details, ProblemDetail):
            return self.error_response(INVALID_ADMIN_CREDENTIALS)

        admin = self.authenticated_admin(admin_details)
        return redirect(redirect_url, Response=Response)

    def change_password(self):
        admin = flask.request.admin
        new_password = flask.request.form.get("password")
        if new_password:
            admin.password = new_password
        return Response(_("Success"), 200)

    def sign_out(self):
        # Clear out the admin's flask session.
        flask.session.pop("admin_email", None)
        flask.session.pop("auth_type", None)

        redirect_url = self.url_for("admin_sign_in", redirect=self.url_for("admin_view"), _external=True)
        return redirect(redirect_url)

    def error_response(self, problem_detail):
        """Returns a problem detail as an HTML response"""
        html = self.ERROR_RESPONSE_TEMPLATE % dict(
            status_code=problem_detail.status_code,
            message=problem_detail.detail
        )
        return Response(html, problem_detail.status_code)

class WorkController(AdminCirculationManagerController):

    STAFF_WEIGHT = 1000

    def details(self, identifier_type, identifier):
        """Return an OPDS entry with detailed information for admins.

        This includes relevant links for editing the book.
        """
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        annotator = AdminAnnotator(self.circulation, flask.request.library)
        # Don't cache these OPDS entries - they should update immediately
        # in the admin interface when an admin makes a change.
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator), cache_for=0,
        )

    def complaints(self, identifier_type, identifier):
        """Return detailed complaint information for admins."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        counter = self._count_complaints_for_work(work)
        response = dict({
            "book": {
                "identifier_type": identifier_type,
                "identifier": identifier
            },
            "complaints": counter
        })

        return response

    def roles(self):
        """Return a mapping from MARC codes to contributor roles."""
        # TODO: The admin interface only allows a subset of the roles
        # listed in model.py since it uses the OPDS representation of
        # the data, and some of the roles map to the same MARC code.
        CODES = Contributor.MARC_ROLE_CODES
        marc_to_role = dict()
        for role in [
            Contributor.ACTOR_ROLE,
            Contributor.ADAPTER_ROLE,
            Contributor.AFTERWORD_ROLE,
            Contributor.ARTIST_ROLE,
            Contributor.ASSOCIATED_ROLE,
            Contributor.AUTHOR_ROLE,
            Contributor.COMPILER_ROLE,
            Contributor.COMPOSER_ROLE,
            Contributor.CONTRIBUTOR_ROLE,
            Contributor.COPYRIGHT_HOLDER_ROLE,
            Contributor.DESIGNER_ROLE,
            Contributor.DIRECTOR_ROLE,
            Contributor.EDITOR_ROLE,
            Contributor.ENGINEER_ROLE,
            Contributor.FOREWORD_ROLE,
            Contributor.ILLUSTRATOR_ROLE,
            Contributor.INTRODUCTION_ROLE,
            Contributor.LYRICIST_ROLE,
            Contributor.MUSICIAN_ROLE,
            Contributor.NARRATOR_ROLE,
            Contributor.PERFORMER_ROLE,
            Contributor.PHOTOGRAPHER_ROLE,
            Contributor.PRODUCER_ROLE,
            Contributor.TRANSCRIBER_ROLE,
            Contributor.TRANSLATOR_ROLE,
            ]:
            marc_to_role[CODES[role]] = role
        return marc_to_role

    def languages(self):
        """Return the supported language codes and their English names."""
        return LanguageCodes.english_names

    def media(self):
        """Return the supported media types for a work and their schema.org values."""
        return Edition.additional_type_to_medium

    def rights_status(self):
        """Return the supported rights status values with their names and whether
        they are open access."""
        return {uri: dict(name=name,
                          open_access=(uri in RightsStatus.OPEN_ACCESS),
                          allows_derivatives=(uri in RightsStatus.ALLOWS_DERIVATIVES))
                for uri, name in RightsStatus.NAMES.iteritems()}

    def edit(self, identifier_type, identifier):
        """Edit a work's metadata."""
        self.require_librarian(flask.request.library)

        # TODO: It would be nice to use the metadata layer for this, but
        # this code handles empty values differently than other metadata
        # sources. When a staff member deletes a value, that indicates
        # they think it should be empty. This needs to be indicated in the
        # db so that it can overrule other data sources that set a value,
        # unlike other sources which set empty fields to None.

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        changed = False

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        primary_identifier = work.presentation_edition.primary_identifier
        staff_edition, is_new = get_one_or_create(
            self._db, Edition,
            primary_identifier_id=primary_identifier.id,
            data_source_id=staff_data_source.id
        )
        self._db.expire(primary_identifier)

        new_title = flask.request.form.get("title")
        if new_title and work.title != new_title:
            staff_edition.title = unicode(new_title)
            changed = True

        new_subtitle = flask.request.form.get("subtitle")
        if work.subtitle != new_subtitle:
            if work.subtitle and not new_subtitle:
                new_subtitle = NO_VALUE
            staff_edition.subtitle = unicode(new_subtitle)
            changed = True

        # The form data includes roles and names for contributors in the same order.
        new_contributor_roles = flask.request.form.getlist("contributor-role")
        new_contributor_names = [unicode(n) for n in flask.request.form.getlist("contributor-name")]
        # The first author in the form is considered the primary author, even
        # though there's no separate MARC code for that.
        for i, role in enumerate(new_contributor_roles):
            if role == Contributor.AUTHOR_ROLE:
                new_contributor_roles[i] = Contributor.PRIMARY_AUTHOR_ROLE
                break
        roles_and_names = zip(new_contributor_roles, new_contributor_names)

        # Remove any contributions that weren't in the form, and remove contributions
        # that already exist from the list so they won't be added again.
        deleted_contributions = False
        for contribution in staff_edition.contributions:
            if (contribution.role, contribution.contributor.display_name) not in roles_and_names:
                self._db.delete(contribution)
                deleted_contributions = True
                changed = True
            else:
                roles_and_names.remove((contribution.role, contribution.contributor.display_name))
        if deleted_contributions:
            # Ensure the staff edition's contributions are up-to-date when
            # calculating the presentation edition later.
            self._db.refresh(staff_edition)

        # Any remaining roles and names are new contributions.
        for role, name in roles_and_names:
            # There may be one extra role at the end from the input for
            # adding a contributor, in which case it will have no
            # corresponding name and can be ignored.
            if name:
                if role not in Contributor.MARC_ROLE_CODES.keys():
                    self._db.rollback()
                    return UNKNOWN_ROLE.detailed(
                        _("Role %(role)s is not one of the known contributor roles.",
                          role=role))
                contributor = staff_edition.add_contributor(name=name, roles=[role])
                contributor.display_name = name
                changed = True

        new_series = flask.request.form.get("series")
        if work.series != new_series:
            if work.series and not new_series:
                new_series = NO_VALUE
            staff_edition.series = unicode(new_series)
            changed = True

        new_series_position = flask.request.form.get("series_position")
        if new_series_position != None and new_series_position != '':
            try:
                new_series_position = int(new_series_position)
            except ValueError:
                self._db.rollback()
                return INVALID_SERIES_POSITION
        else:
            new_series_position = None
        if work.series_position != new_series_position:
            if work.series_position and new_series_position == None:
                new_series_position = NO_NUMBER
            staff_edition.series_position = new_series_position
            changed = True

        new_medium = flask.request.form.get("medium")
        if new_medium:
            if new_medium not in Edition.medium_to_additional_type.keys():
                self._db.rollback()
                return UNKNOWN_MEDIUM.detailed(
                    _("Medium %(medium)s is not one of the known media.",
                      medium=new_medium))
            staff_edition.medium = new_medium
            changed = True

        new_language = flask.request.form.get("language")
        if new_language != None and new_language != '':
            new_language = LanguageCodes.string_to_alpha_3(new_language)
            if not new_language:
                self._db.rollback()
                return UNKNOWN_LANGUAGE
        else:
            new_language = None
        if new_language != staff_edition.language:
            staff_edition.language = new_language
            changed = True

        new_publisher = flask.request.form.get("publisher")
        if new_publisher != staff_edition.publisher:
            if staff_edition.publisher and not new_publisher:
                new_publisher = NO_VALUE
            staff_edition.publisher = unicode(new_publisher)
            changed = True

        new_imprint = flask.request.form.get("imprint")
        if new_imprint != staff_edition.imprint:
            if staff_edition.imprint and not new_imprint:
                new_imprint = NO_VALUE
            staff_edition.imprint = unicode(new_imprint)
            changed = True

        new_issued = flask.request.form.get("issued")
        if new_issued != None and new_issued != '':
            try:
                new_issued = datetime.strptime(new_issued, '%Y-%m-%d')
            except ValueError:
                self._db.rollback()
                return INVALID_DATE_FORMAT
        else:
            new_issued = None
        if new_issued != staff_edition.issued:
            staff_edition.issued = new_issued
            changed = True

        # TODO: This lets library staff add a 1-5 rating, which is used in the
        # quality calculation. However, this doesn't work well if there are any
        # other measurements that contribute to the quality. The form will show
        # the calculated quality rather than the staff rating, which will be
        # confusing. It might also be useful to make it more clear how this
        # relates to the quality threshold in the library settings.
        changed_rating = False
        new_rating = flask.request.form.get("rating")
        if new_rating != None and new_rating != '':
            try:
                new_rating = float(new_rating)
            except ValueError:
                self._db.rollback()
                return INVALID_RATING
            scale = Measurement.RATING_SCALES[DataSource.LIBRARY_STAFF]
            if new_rating < scale[0] or new_rating > scale[1]:
                self._db.rollback()
                return INVALID_RATING.detailed(
                    _("The rating must be a number between %(low)s and %(high)s.",
                      low=scale[0], high=scale[1]))
            if (new_rating - scale[0]) / (scale[1] - scale[0]) != work.quality:
                primary_identifier.add_measurement(staff_data_source, Measurement.RATING, new_rating, weight=WorkController.STAFF_WEIGHT)
                changed = True
                changed_rating = True

        changed_summary = False
        new_summary = flask.request.form.get("summary") or ""
        if new_summary != work.summary_text:
            old_summary = None
            if work.summary and work.summary.data_source == staff_data_source:
                old_summary = work.summary

            work.presentation_edition.primary_identifier.add_link(
                Hyperlink.DESCRIPTION, None,
                staff_data_source, content=new_summary)

            # Delete previous staff summary
            if old_summary:
                for link in old_summary.links:
                    self._db.delete(link)
                self._db.delete(old_summary)

            changed = True
            changed_summary = True

        if changed:
            # Even if the presentation doesn't visibly change, we want
            # to regenerate the OPDS entries and update the search
            # index for the work, because that might be the 'real'
            # problem the user is trying to fix.
            policy = PresentationCalculationPolicy(
                classify=True,
                regenerate_opds_entries=True,
                regenerate_marc_record=True,
                update_search_index=True,
                calculate_quality=changed_rating,
                choose_summary=changed_summary,
            )
            work.calculate_presentation(policy=policy)

        return Response("", 200)

    def suppress(self, identifier_type, identifier):
        """Suppress the license pool associated with a book."""
        self.require_librarian(flask.request.library)

        # Turn source + identifier into a LicensePool
        pools = self.load_licensepools(flask.request.library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            # Something went wrong.
            return pools

        # Assume that the Work is being suppressed from the catalog, and
        # not just the LicensePool.
        # TODO: Suppress individual LicensePools when it's not that deep.
        for pool in pools:
            pool.suppressed = True
        return Response("", 200)

    def unsuppress(self, identifier_type, identifier):
        """Unsuppress all license pools associated with a book.

        TODO: This will need to be revisited when we distinguish
        between complaints about a work and complaints about a
        LicensePoool.
        """
        self.require_librarian(flask.request.library)

        # Turn source + identifier into a group of LicensePools
        pools = self.load_licensepools(flask.request.library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            # Something went wrong.
            return pools

        # Unsuppress each pool.
        for pool in pools:
            pool.suppressed = False
        return Response("", 200)

    def refresh_metadata(self, identifier_type, identifier, provider=None):
        """Refresh the metadata for a book from the content server"""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        if not provider and work.license_pools:
            provider = MetadataWranglerCollectionRegistrar(work.license_pools[0].collection)

        if not provider:
            return METADATA_REFRESH_FAILURE

        identifier = work.presentation_edition.primary_identifier
        try:
            record = provider.ensure_coverage(identifier, force=True)
        except Exception:
            # The coverage provider may raise an HTTPIntegrationException.
            return REMOTE_INTEGRATION_FAILED

        if record.exception:
            # There was a coverage failure.
            if (str(record.exception).startswith("201") or
                str(record.exception).startswith("202")):
                # A 201/202 error means it's never looked up this work before
                # so it's started the resolution process or looking for sources.
                return METADATA_REFRESH_PENDING
            # Otherwise, it just doesn't know anything.
            return METADATA_REFRESH_FAILURE

        return Response("", 200)

    def resolve_complaints(self, identifier_type, identifier):
        """Resolve all complaints for a particular license pool and complaint type."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        resolved = False
        found = False

        requested_type = flask.request.form.get("type")
        if requested_type:
            for complaint in work.complaints:
                if complaint.type == requested_type:
                    found = True
                    if complaint.resolved == None:
                        complaint.resolve()
                        resolved = True

        if not found:
            return UNRECOGNIZED_COMPLAINT
        elif not resolved:
            return COMPLAINT_ALREADY_RESOLVED
        return Response("", 200)

    def classifications(self, identifier_type, identifier):
        """Return list of this work's classifications."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        identifier_id = work.presentation_edition.primary_identifier.id
        results = self._db \
            .query(Classification) \
            .join(Subject) \
            .join(DataSource) \
            .filter(Classification.identifier_id == identifier_id) \
            .order_by(Classification.weight.desc()) \
            .all()

        data = []
        for result in results:
            data.append(dict({
                "type": result.subject.type,
                "name": result.subject.identifier,
                "source": result.data_source.name,
                "weight": result.weight
            }))

        return dict({
            "book": {
                "identifier_type": identifier_type,
                "identifier": identifier
            },
            "classifications": data
        })

    def edit_classifications(self, identifier_type, identifier):
        """Edit a work's audience, target age, fiction status, and genres."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        # Previous staff classifications
        primary_identifier = work.presentation_edition.primary_identifier
        old_classifications = self._db \
            .query(Classification) \
            .join(Subject) \
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source
            )
        old_genre_classifications = old_classifications \
            .filter(Subject.genre_id != None)
        old_staff_genres = [
            c.subject.genre.name
            for c in old_genre_classifications
            if c.subject.genre
        ]
        old_computed_genres = [
            work_genre.genre.name
            for work_genre in work.work_genres
        ]

        # New genres should be compared to previously computed genres
        new_genres = flask.request.form.getlist("genres")
        genres_changed = sorted(new_genres) != sorted(old_computed_genres)

        # Update audience
        new_audience = flask.request.form.get("audience")
        if new_audience != work.audience:
            # Delete all previous staff audience classifications
            for c in old_classifications:
                if c.subject.type == Subject.FREEFORM_AUDIENCE:
                    self._db.delete(c)

            # Create a new classification with a high weight
            primary_identifier.classify(
                data_source=staff_data_source,
                subject_type=Subject.FREEFORM_AUDIENCE,
                subject_identifier=new_audience,
                weight=WorkController.STAFF_WEIGHT,
            )

        # Update target age if present
        new_target_age_min = flask.request.form.get("target_age_min")
        new_target_age_min = int(new_target_age_min) if new_target_age_min else None
        new_target_age_max = flask.request.form.get("target_age_max")
        new_target_age_max = int(new_target_age_max) if new_target_age_max else None
        if new_target_age_max < new_target_age_min:
            return INVALID_EDIT.detailed(_("Minimum target age must be less than maximum target age."))

        if work.target_age:
            old_target_age_min = work.target_age.lower
            old_target_age_max = work.target_age.upper
        else:
            old_target_age_min = None
            old_target_age_max = None
        if new_target_age_min != old_target_age_min or new_target_age_max != old_target_age_max:
            # Delete all previous staff target age classifications
            for c in old_classifications:
                if c.subject.type == Subject.AGE_RANGE:
                    self._db.delete(c)

            # Create a new classification with a high weight - higher than audience
            if new_target_age_min and new_target_age_max:
                age_range_identifier = "%s-%s" % (new_target_age_min, new_target_age_max)
                primary_identifier.classify(
                    data_source=staff_data_source,
                    subject_type=Subject.AGE_RANGE,
                    subject_identifier=age_range_identifier,
                    weight=WorkController.STAFF_WEIGHT * 100,
                )

        # Update fiction status
        # If fiction status hasn't changed but genres have changed,
        # we still want to ensure that there's a staff classification
        new_fiction = True if flask.request.form.get("fiction") == "fiction" else False
        if new_fiction != work.fiction or genres_changed:
            # Delete previous staff fiction classifications
            for c in old_classifications:
                if c.subject.type == Subject.SIMPLIFIED_FICTION_STATUS:
                    self._db.delete(c)

            # Create a new classification with a high weight (higher than genre)
            fiction_term = "Fiction" if new_fiction else "Nonfiction"
            classification = primary_identifier.classify(
                data_source=staff_data_source,
                subject_type=Subject.SIMPLIFIED_FICTION_STATUS,
                subject_identifier=fiction_term,
                weight=WorkController.STAFF_WEIGHT,
            )
            classification.subject.fiction = new_fiction

        # Update genres
        # make sure all new genres are legit
        for name in new_genres:
            genre, is_new = Genre.lookup(self._db, name)
            if not isinstance(genre, Genre):
                return GENRE_NOT_FOUND
            if genres[name].is_fiction is not None and genres[name].is_fiction != new_fiction:
                return INCOMPATIBLE_GENRE
            if name == "Erotica" and new_audience != "Adults Only":
                return EROTICA_FOR_ADULTS_ONLY

        if genres_changed:
            # delete existing staff classifications for genres that aren't being kept
            for c in old_genre_classifications:
                if c.subject.genre.name not in new_genres:
                    self._db.delete(c)

            # add new staff classifications for new genres
            for genre in new_genres:
                if genre not in old_staff_genres:
                    classification = primary_identifier.classify(
                        data_source=staff_data_source,
                        subject_type=Subject.SIMPLIFIED_GENRE,
                        subject_identifier=genre,
                        weight=WorkController.STAFF_WEIGHT
                    )

            # add NONE genre classification if we aren't keeping any genres
            if len(new_genres) == 0:
                primary_identifier.classify(
                    data_source=staff_data_source,
                    subject_type=Subject.SIMPLIFIED_GENRE,
                    subject_identifier=SimplifiedGenreClassifier.NONE,
                    weight=WorkController.STAFF_WEIGHT
                )
            else:
                # otherwise delete existing NONE genre classification
                none_classifications = self._db \
                    .query(Classification) \
                    .join(Subject) \
                    .filter(
                        Classification.identifier == primary_identifier,
                        Subject.identifier == SimplifiedGenreClassifier.NONE
                    ) \
                    .all()
                for c in none_classifications:
                    self._db.delete(c)

        # Update presentation
        policy = PresentationCalculationPolicy(
            classify=True,
            regenerate_opds_entries=True,
            regenerate_marc_record=True,
            update_search_index=True
        )
        work.calculate_presentation(policy=policy)

        return Response("", 200)

    MINIMUM_COVER_WIDTH = 600
    MINIMUM_COVER_HEIGHT = 900
    TOP = 'top'
    CENTER = 'center'
    BOTTOM = 'bottom'
    TITLE_POSITIONS = [TOP, CENTER, BOTTOM]

    def _validate_cover_image(self, image):
        image_width, image_height = image.size
        if image_width < self.MINIMUM_COVER_WIDTH or image_height < self.MINIMUM_COVER_HEIGHT:
           return INVALID_IMAGE.detailed(_("Cover image must be at least %(width)spx in width and %(height)spx in height.",
                                                 width=self.MINIMUM_COVER_WIDTH, height=self.MINIMUM_COVER_HEIGHT))
        return True

    def _process_cover_image(self, work, image, title_position):
        title = work.presentation_edition.title
        author = work.presentation_edition.author
        if author == Edition.UNKNOWN_AUTHOR:
            author = ""

        if title_position in self.TITLE_POSITIONS:
            # Convert image to 'RGB' mode if it's not already, so drawing on it works.
            if image.mode != 'RGB':
                image = image.convert("RGB")

            draw = ImageDraw.Draw(image)
            image_width, image_height = image.size

            admin_dir = os.path.dirname(os.path.split(__file__)[0])
            package_dir = os.path.join(admin_dir, "../..")
            bold_font_path = os.path.join(package_dir, "resources/OpenSans-Bold.ttf")
            regular_font_path = os.path.join(package_dir, "resources/OpenSans-Regular.ttf")
            font_size = image_width / 20
            bold_font = ImageFont.truetype(bold_font_path, font_size)
            regular_font = ImageFont.truetype(regular_font_path, font_size)

            padding = image_width / 40

            max_line_width = 0
            bold_char_width = bold_font.getsize("n")[0]
            bold_char_count = image_width / bold_char_width
            regular_char_width = regular_font.getsize("n")[0]
            regular_char_count = image_width / regular_char_width
            title_lines = textwrap.wrap(title, bold_char_count)
            author_lines = textwrap.wrap(author, regular_char_count)
            for lines, font in [(title_lines, bold_font), (author_lines, regular_font)]:
                for line in lines:
                    line_width, ignore = font.getsize(line)
                    if line_width > max_line_width:
                        max_line_width = line_width

            ascent, descent = bold_font.getmetrics()
            line_height = ascent + descent

            total_text_height = line_height * (len(title_lines) + len(author_lines))
            rectangle_height = total_text_height + line_height

            rectangle_width = max_line_width + 2 * padding

            start_x = (image_width - rectangle_width) / 2
            if title_position == self.BOTTOM:
                start_y = image_height - rectangle_height - image_height / 14
            elif title_position == self.CENTER:
                start_y = (image_height - rectangle_height) / 2
            else:
                start_y = image_height / 14

            draw.rectangle([(start_x, start_y),
                            (start_x + rectangle_width, start_y + rectangle_height)],
                           fill=(255,255,255,255))

            current_y = start_y + line_height / 2
            for lines, font in [(title_lines, bold_font), (author_lines, regular_font)]:
                for line in lines:
                    line_width, ignore = font.getsize(line)
                    draw.text((start_x + (rectangle_width - line_width) / 2, current_y),
                              line, font=font, fill=(0,0,0,255))
                    current_y += line_height

            del draw

        return image

    def preview_book_cover(self, identifier_type, identifier):
        """Return a preview of the submitted cover image information."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        image_file = flask.request.files.get("cover_file")
        image_url = flask.request.form.get("cover_url")
        if not image_file and not image_url:
            return INVALID_IMAGE.detailed(_("Image file or image URL is required."))

        title_position = flask.request.form.get("title_position")

        if image_url and not image_file:
            image_file = StringIO(urllib.urlopen(image_url).read())

        image = Image.open(image_file)
        result = self._validate_cover_image(image)
        if isinstance(result, ProblemDetail):
            return result
        if title_position and title_position in self.TITLE_POSITIONS:
            image = self._process_cover_image(work, image, title_position)

        buffer = StringIO()
        image.save(buffer, format="PNG")
        b64 = base64.b64encode(buffer.getvalue())
        value = "data:image/png;base64,%s" % b64

        return Response(value, 200)

    def change_book_cover(self, identifier_type, identifier, mirror=None):
        """Save a new book cover based on the submitted form."""
        self.require_librarian(flask.request.library)

        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        pools = self.load_licensepools(flask.request.library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools

        if not pools:
            return NO_LICENSES

        collection = pools[0].collection

        image_file = flask.request.files.get("cover_file")
        image_url = flask.request.form.get("cover_url")
        if not image_file and not image_url:
            return INVALID_IMAGE.detailed(_("Image file or image URL is required."))

        title_position = flask.request.form.get("title_position")

        rights_uri = flask.request.form.get("rights_status")
        rights_explanation = flask.request.form.get("rights_explanation")

        if not rights_uri:
            return INVALID_IMAGE.detailed(_("You must specify the image's license."))

        # Look for an appropriate mirror to store this cover image.
        mirror = mirror or MirrorUploader.for_collection(collection, use_sitewide=True)
        if not mirror:
            return INVALID_CONFIGURATION_OPTION.detailed(_("Could not find a storage integration for uploading the cover."))

        if image_url and not image_file:
            image_file = StringIO(urllib.urlopen(image_url).read())

        image = Image.open(image_file)
        result = self._validate_cover_image(image)
        if isinstance(result, ProblemDetail):
            return result

        cover_href = None
        cover_rights_explanation = rights_explanation

        if title_position in self.TITLE_POSITIONS:
            original_href = image_url
            original_buffer = StringIO()
            image.save(original_buffer, format="PNG")
            original_content = original_buffer.getvalue()
            if not original_href:
                original_href = Hyperlink.generic_uri(data_source, work.presentation_edition.primary_identifier, Hyperlink.IMAGE, content=original_content)

            image = self._process_cover_image(work, image, title_position)

            original_rights_explanation = None
            if rights_uri != RightsStatus.IN_COPYRIGHT:
                original_rights_explanation = rights_explanation
            original = LinkData(
                Hyperlink.IMAGE, original_href, rights_uri=rights_uri,
                rights_explanation=original_rights_explanation, content=original_content,
            )
            derivation_settings = dict(title_position=title_position)
            if rights_uri in RightsStatus.ALLOWS_DERIVATIVES:
                cover_rights_explanation = "The original image license allows derivatives."
        else:
            original = None
            derivation_settings = None
            cover_href = image_url

        buffer = StringIO()
        image.save(buffer, format="PNG")
        content = buffer.getvalue()

        if not cover_href:
            cover_href = Hyperlink.generic_uri(data_source, work.presentation_edition.primary_identifier, Hyperlink.IMAGE, content=content)

        cover_data = LinkData(
            Hyperlink.IMAGE, href=cover_href,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=content, rights_uri=rights_uri,
            rights_explanation=cover_rights_explanation,
            original=original, transformation_settings=derivation_settings,
        )

        presentation_policy = PresentationCalculationPolicy(
            choose_edition=False,
            set_edition_metadata=False,
            classify=False,
            choose_summary=False,
            calculate_quality=False,
            choose_cover=True,
            regenerate_opds_entries=True,
            regenerate_marc_record=True,
            update_search_index=False,
        )

        replacement_policy = ReplacementPolicy(
            links=True,
            # link_content is false because we already have the content.
            # We don't want the metadata layer to try to fetch it again.
            link_content=False,
            mirror=mirror,
            presentation_calculation_policy=presentation_policy,
        )

        metadata = Metadata(data_source, links=[cover_data])
        metadata.apply(work.presentation_edition,
                       collection,
                       replace=replacement_policy)

        # metadata.apply only updates the edition, so we also need
        # to update the work.
        work.calculate_presentation(policy=presentation_policy)

        return Response(_("Success"), 200)

    def _count_complaints_for_work(self, work):
        complaint_types = [complaint.type for complaint in work.complaints if not complaint.resolved]
        return Counter(complaint_types)

    def custom_lists(self, identifier_type, identifier):
        self.require_librarian(flask.request.library)

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        if flask.request.method == "GET":
            lists = []
            for entry in work.custom_list_entries:
                list = entry.customlist
                lists.append(dict(id=list.id, name=list.name))
            return dict(custom_lists=lists)

        if flask.request.method == "POST":
            lists = flask.request.form.get("lists")
            if lists:
                lists = json.loads(lists)
            else:
                lists = []

            affected_lanes = set()

            # Remove entries for lists that were not in the submitted form.
            submitted_ids = [l.get("id") for l in lists if l.get("id")]
            for entry in work.custom_list_entries:
                if entry.list_id not in submitted_ids:
                    list = entry.customlist
                    list.remove_entry(work)
                    for lane in Lane.affected_by_customlist(list):
                        affected_lanes.add(lane)

            # Add entries for any new lists.
            for list_info in lists:
                id = list_info.get("id")
                name = list_info.get("name")

                if id:
                    is_new = False
                    list = get_one(self._db, CustomList, id=int(id), name=name, library=library, data_source=staff_data_source)
                    if not list:
                        self._db.rollback()
                        return MISSING_CUSTOM_LIST.detailed(_("Could not find list \"%(list_name)s\"", list_name=name))
                else:
                    list, is_new = create(self._db, CustomList, name=name, data_source=staff_data_source, library=library)
                    list.created = datetime.now()
                entry, was_new = list.add_entry(work, featured=True)
                if was_new:
                    for lane in Lane.affected_by_customlist(list):
                        affected_lanes.add(lane)

            # If any list changes affected lanes, update their sizes.
            for lane in affected_lanes:
                lane.update_size(self._db)

            return Response(unicode(_("Success")), 200)

class PatronController(AdminCirculationManagerController):

    def _load_patrondata(self, authenticator=None):
        """Extract a patron identifier from an incoming form submission,
        and ask the library's LibraryAuthenticator to turn it into a
        PatronData by doing a remote lookup in the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
        during tests; it's not necessary to provide it normally.
        """
        self.require_librarian(flask.request.library)

        identifier = flask.request.form.get("identifier")
        if not identifier:
            return NO_SUCH_PATRON.detailed(_("Please enter a patron identifier"))

        if not authenticator:
            authenticator = LibraryAuthenticator.from_config(
                self._db, flask.request.library
            )

        patron_data = PatronData(authorization_identifier=identifier)
        complete_patron_data = None

        if not authenticator.providers:
            return NO_SUCH_PATRON.detailed(
                _("This library has no authentication providers, so it has no patrons.")
            )

        for provider in authenticator.providers:
            complete_patron_data = provider.remote_patron_lookup(patron_data)
            if complete_patron_data:
                return complete_patron_data

        # If we get here, none of the providers succeeded.
        if not complete_patron_data:
            return NO_SUCH_PATRON.detailed(
                _("No patron with identifier %(patron_identifier)s was found at your library",
                  patron_identifier=identifier),
            )

    def lookup_patron(self, authenticator=None):
        """Look up personal information about a patron via the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
        during tests; it's not necessary to provide it normally.
        """
        patrondata = self._load_patrondata(authenticator)
        if isinstance(patrondata, ProblemDetail):
            return patrondata
        return patrondata.to_dict

    def reset_adobe_id(self, authenticator=None):
        """Delete all Credentials for a patron that are relevant
        to the patron's Adobe Account ID.

        :param authenticator: A LibraryAuthenticator. This is for mocking
        during tests; it's not necessary to provide it normal
        """
        patrondata = self._load_patrondata(authenticator)
        if isinstance(patrondata, ProblemDetail):
            return patrondata
        # Turn the Identifier into a Patron object.
        try:
            patron, is_new = patrondata.get_or_create_patron(
                self._db, flask.request.library.id
            )
        except CannotCreateLocalPatron, e:
            return NO_SUCH_PATRON.detailed(
                _("Could not create local patron object for %(patron_identifier)s",
                  patron_identifier=patrondata.authorization_identifier
                )
            )

        # Wipe the Patron's 'identifier for Adobe ID purposes'.
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)
        if patron.username:
            identifier = patron.username
        else:
            identifier = "with identifier " + patron.authorization_identifier
        return Response(
            unicode(_("Adobe ID for patron %(name_or_auth_id)s has been reset.", name_or_auth_id=identifier)),
            200
        )

class FeedController(AdminCirculationManagerController):

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
        return feed_response(opds_feed, cache_for=0)

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
        return feed_response(opds_feed, cache_for=0)

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

class CustomListsController(AdminCirculationManagerController):
    def custom_lists(self):
        library = flask.request.library
        self.require_librarian(library)

        if flask.request.method == "GET":
            custom_lists = []
            for list in library.custom_lists:
                collections = []
                for collection in list.collections:
                    collections.append(dict(id=collection.id, name=collection.name, protocol=collection.protocol))
                custom_lists.append(dict(id=list.id, name=list.name, collections=collections, entry_count=len(list.entries)))
            return dict(custom_lists=custom_lists)

        if flask.request.method == "POST":
            id = flask.request.form.get("id")
            name = flask.request.form.get("name")
            entries = self._getJSONFromRequest(flask.request.form.get("entries"))
            collections = self._getJSONFromRequest(flask.request.form.get("collections"))
            return self._create_or_update_list(library, name, entries, collections, id=id)

    def _getJSONFromRequest(self, values):
        if values:
            values = json.loads(values)
        else:
            values = []

        return values

    def _get_work_from_urn(self, library, urn):
        identifier, ignore = Identifier.parse_urn(self._db, urn)
        query = self._db.query(
            Work
        ).join(
            LicensePool, LicensePool.work_id==Work.id
        ).join(
            Collection, LicensePool.collection_id==Collection.id
        ).filter(
            LicensePool.identifier_id==identifier.id
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
            list = get_one(self._db, CustomList, id=int(id), data_source=data_source)
            if not list:
                return MISSING_CUSTOM_LIST
            if list.library != library:
                return CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST
            if old_list_with_name and old_list_with_name != list:
                return CUSTOM_LIST_NAME_ALREADY_IN_USE
        elif old_list_with_name:
            return CUSTOM_LIST_NAME_ALREADY_IN_USE
        else:
            list, is_new = create(self._db, CustomList, name=name, data_source=data_source)
            list.created = datetime.now()
            list.library = library

        list.updated = datetime.now()
        list.name = name
        membership_change = False

        for entry in entries:
            urn = entry.get("id")
            work = self._get_work_from_urn(library, urn)

            if work:
                entry, entry_is_new = list.add_entry(work, featured=True)
                if entry_is_new:
                    membership_change = True

        if deletedEntries:
            for entry in deletedEntries:
                urn = entry.get("id")
                work = self._get_work_from_urn(library, urn)

                if work:
                    list.remove_entry(work)
                    membership_change = True

        if membership_change:
            # If this list was used to populate any lanes, those
            # lanes need to have their counts updated.
            for lane in Lane.affected_by_customlist(list):
                lane.update_size(self._db)

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
            return Response(unicode(list.id), 201)
        else:
            return Response(unicode(list.id), 200)

    def url_for_custom_list(self, library, list):
        def url_fn(after):
            return self.url_for("custom_list", after=after, library_short_name=library.short_name, list_id=list.id)
        return url_fn

    def custom_list(self, list_id):
        library = flask.request.library
        self.require_librarian(library)
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        list = get_one(self._db, CustomList, id=list_id, data_source=data_source)
        if not list:
            return MISSING_CUSTOM_LIST

        if flask.request.method == "GET":
            pagination = load_pagination_from_request()
            if isinstance(pagination, ProblemDetail):
                return pagination

            query = self._db.query(Work).join(Work.custom_list_entries).filter(CustomListEntry.list_id==list_id)
            url = self.url_for(
                "custom_list", list_name=list.name,
                library_short_name=library.short_name,
                list_id=list_id,
            )

            worklist = WorkList()
            worklist.initialize(library, customlists=[list])

            annotator = self.manager.annotator(worklist)
            url_fn = self.url_for_custom_list(library, list)
            feed = AcquisitionFeed.from_query(
                query, self._db, list.name,
                url, pagination, url_fn, annotator
            )
            annotator.annotate_feed(feed, worklist)

            return feed_response(unicode(feed), cache_for=0)

        elif flask.request.method == "POST":
            name = flask.request.form.get("name")
            entries = self._getJSONFromRequest(flask.request.form.get("entries"))
            collections = self._getJSONFromRequest(flask.request.form.get("collections"))
            deletedEntries = self._getJSONFromRequest(flask.request.form.get("deletedEntries"))
            return self._create_or_update_list(library, name, entries, collections, deletedEntries=deletedEntries, id=list_id)

        elif flask.request.method == "DELETE":
            # Deleting requires a library manager.
            self.require_library_manager(flask.request.library)

            # Build the list of affected lanes before modifying the
            # CustomList.
            affected_lanes = Lane.affected_by_customlist(list)
            for entry in list.entries:
                self._db.delete(entry)
            self._db.delete(list)
            for lane in affected_lanes:
                lane.update_size(self._db)
            return Response(unicode(_("Deleted")), 200)


class LanesController(AdminCirculationManagerController):

    def lanes(self):
        library = flask.request.library
        self.require_librarian(library)

        if flask.request.method == "GET":
            def lanes_for_parent(parent):
                lanes = self._db.query(Lane).filter(Lane.library==library).filter(Lane.parent==parent).order_by(Lane.priority)
                return [{ "id": lane.id,
                          "display_name": lane.display_name,
                          "visible": lane.visible,
                          "count": lane.size,
                          "sublanes": lanes_for_parent(lane),
                          "custom_list_ids": [list.id for list in lane.customlists],
                          "inherit_parent_restrictions": lane.inherit_parent_restrictions,
                          } for lane in lanes]
            return dict(lanes=lanes_for_parent(None))

        if flask.request.method == "POST":
            self.require_library_manager(flask.request.library)

            id = flask.request.form.get("id")
            parent_id = flask.request.form.get("parent_id")
            display_name = flask.request.form.get("display_name")
            custom_list_ids = json.loads(flask.request.form.get("custom_list_ids", "[]"))
            inherit_parent_restrictions = flask.request.form.get("inherit_parent_restrictions")
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
                    old_lane = get_one(self._db, Lane, display_name=display_name, parent=lane.parent)
                    if old_lane:
                        return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS
                lane.display_name = display_name
            else:
                parent = None
                if parent_id:
                    parent = get_one(self._db, Lane, id=parent_id, library=library)
                    if not parent:
                        return MISSING_LANE.detailed(_("The specified parent lane does not exist, or is associated with a different library."))
                old_lane = get_one(self._db, Lane, display_name=display_name, parent=parent)
                if old_lane:
                    return LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS

                lane, is_new = create(
                    self._db, Lane, display_name=display_name,
                    parent=parent, library=library)
                lane.media = [Edition.BOOK_MEDIUM]

                # Make a new lane the first child of its parent and bump all the siblings down in priority.
                siblings = self._db.query(Lane).filter(Lane.library==library).filter(Lane.parent==lane.parent).filter(Lane.id!=lane.id)
                for sibling in siblings:
                    sibling.priority += 1
                lane.priority = 0

            lane.inherit_parent_restrictions = inherit_parent_restrictions

            for list_id in custom_list_ids:
                list = get_one(self._db, CustomList, library=library, id=list_id)
                if not list:
                    self._db.rollback()
                    return MISSING_CUSTOM_LIST.detailed(
                        _("The list with id %(list_id)s does not exist or is associated with a different library.", list_id=list_id))
                lane.customlists.append(list)

            for list in lane.customlists:
                if list.id not in custom_list_ids:
                    lane.customlists.remove(list)
            lane.update_size(self._db)

            if is_new:
                return Response(unicode(lane.id), 201)
            else:
                return Response(unicode(lane.id), 200)

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
            return Response(unicode(_("Deleted")), 200)

    def show_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)
        if not lane:
            return MISSING_LANE
        if lane.parent and not lane.parent.visible:
            return CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT
        lane.visible = True
        return Response(unicode(_("Success")), 200)

    def hide_lane(self, lane_identifier):
        library = flask.request.library
        self.require_library_manager(library)

        lane = get_one(self._db, Lane, id=lane_identifier, library=library)
        if not lane:
            return MISSING_LANE
        lane.visible = False
        return Response(unicode(_("Success")), 200)

    def reset(self):
        self.require_library_manager(flask.request.library)

        create_default_lanes(self._db, flask.request.library)
        return Response(unicode(_("Success")), 200)

    def change_order(self):
        self.require_library_manager(flask.request.library)

        submitted_lanes = json.loads(flask.request.data)

        def update_lane_order(lanes):
            for index, lane_data in enumerate(lanes):
                lane_id = lane_data.get("id")
                lane = self._db.query(Lane).filter(Lane.id==lane_id).one()
                lane.priority = index
                update_lane_order(lane_data.get("sublanes", []))

        update_lane_order(submitted_lanes)

        return Response(unicode(_("Success")), 200)


class DashboardController(AdminCirculationManagerController):

    def stats(self):
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
                    LicensePool.open_access == False,
                )
            ).count()

            open_title_count = self._db.query(
                LicensePool
            ).filter(
                LicensePool.collection_id == collection.id
            ).filter(
                LicensePool.open_access == True
            ).count()

            # The sum queries return None instead of 0 if there are
            # no license pools in the db.

            license_count = self._db.query(
                func.sum(LicensePool.licenses_owned)
            ).filter(
                LicensePool.collection_id == collection.id
            ).filter(
                LicensePool.open_access == False,
            ).all()[0][0] or 0

            available_license_count = self._db.query(
                func.sum(LicensePool.licenses_available)
            ).filter(
                LicensePool.collection_id == collection.id
            ).filter(
                LicensePool.open_access == False,
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

            patron_count = self._db.query(Patron).filter(Patron.library_id==library.id).count()

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
                        Loan.id != None,
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
                title_count += counts.get("licensed_titles", 0) + counts.get("open_access_titles", 0)
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

        total_patrons = sum([
            stats.get("patrons", {}).get("total", 0)
            for stats in library_stats.values()])
        total_with_active_loans = sum([
            stats.get("patrons", {}).get("with_active_loans", 0)
            for stats in library_stats.values()])
        total_with_active_loans_or_holds = sum([
            stats.get("patrons", {}).get("with_active_loans_or_holds", 0)
            for stats in library_stats.values()])

        # TODO: show shared collection loans and holds for libraries outside this
        # circ manager?
        total_loans = sum([
            stats.get("patrons", {}).get("loans", 0)
            for stats in library_stats.values()])
        total_holds = sum([
            stats.get("patrons", {}).get("holds", 0)
            for stats in library_stats.values()])

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

        events = map(lambda result: {
            "id": result.id,
            "type": result.type,
            "patron_id": result.foreign_patron_id,
            "time": result.start,
            "book": {
                "title": result.license_pool.work.title,
                "url": annotator.permalink_for(result.license_pool.work, result.license_pool, result.license_pool.identifier)
            }
        }, results)

        return dict({ "circulation_events": events })

    def bulk_circulation_events(self):
        default = str(datetime.today()).split(" ")[0]
        date = flask.request.args.get("date", default)
        next_date = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)

        query = self._db.query(
                CirculationEvent, Identifier, Work, Edition
            ) \
            .join(LicensePool, LicensePool.id == CirculationEvent.license_pool_id) \
            .join(Identifier, Identifier.id == LicensePool.identifier_id) \
            .join(Work, Work.id == LicensePool.work_id) \
            .join(Edition, Edition.id == Work.presentation_edition_id) \
            .filter(CirculationEvent.start >= date) \
            .filter(CirculationEvent.start < next_date) \
            .order_by(CirculationEvent.start.asc())
        query = query \
            .options(lazyload(Identifier.licensed_through)) \
            .options(lazyload(Work.license_pools))
        results = query.all()

        work_ids = map(lambda result: result[2].id, results)

        subquery = self._db \
            .query(WorkGenre.work_id, Genre.name) \
            .join(Genre) \
            .filter(WorkGenre.work_id.in_(work_ids)) \
            .order_by(WorkGenre.affinity.desc()) \
            .subquery()
        genre_query = self._db \
            .query(subquery.c.work_id, func.string_agg(subquery.c.name, ",")) \
            .select_from(subquery) \
            .group_by(subquery.c.work_id)
        genres = dict(genre_query.all())

        header = [
            "time", "event", "identifier", "identifier_type", "title", "author",
            "fiction", "audience", "publisher", "language", "target_age", "genres"
        ]

        def result_to_row(result):
            (event, identifier, work, edition) = result
            return [
                str(event.start) or "",
                event.type,
                identifier.identifier,
                identifier.type,
                edition.title,
                edition.author,
                "fiction" if work.fiction else "nonfiction",
                work.audience,
                edition.publisher,
                edition.language,
                work.target_age_string,
                genres.get(work.id)
            ]

        return [header] + map(result_to_row, results), date

class SettingsController(AdminCirculationManagerController):

    METADATA_SERVICE_URI_TYPE = 'application/opds+json;profile=https://librarysimplified.org/rel/profile/metadata-service'

    NO_MIRROR_INTEGRATION = u"NO_MIRROR"

    PROVIDER_APIS = [OPDSImporter,
                     OPDSForDistributorsAPI,
                     OverdriveAPI,
                     OdiloAPI,
                     BibliothecaAPI,
                     Axis360API,
                     RBDigitalAPI,
                     EnkiAPI,
                     ODLWithConsolidatedCopiesAPI,
                     SharedODLAPI,
                     FeedbooksOPDSImporter,
                    ]

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
            if description != None:
                protocol["description"] = description

            instructions = getattr(api, "INSTRUCTIONS", None)
            if instructions != None:
                protocol["instructions"] = instructions

            sitewide = getattr(api, "SITEWIDE", None)
            if sitewide != None:
                protocol["sitewide"] = sitewide

            settings = getattr(api, "SETTINGS", [])
            protocol["settings"] = list(settings)

            child_settings = getattr(api, "CHILD_SETTINGS", None)
            if child_settings != None:
                protocol["child_settings"] = list(child_settings)

            library_settings = getattr(api, "LIBRARY_SETTINGS", None)
            if library_settings != None:
                protocol["library_settings"] = list(library_settings)

            cardinality = getattr(api, 'CARDINALITY', None)
            if cardinality != None:
                protocol['cardinality'] = cardinality

            supports_registration = getattr(api, "SUPPORTS_REGISTRATION", None)
            if supports_registration != None:
                protocol['supports_registration'] = supports_registration
            supports_staging = getattr(api, "SUPPORTS_STAGING", None)
            if supports_staging != None:
                protocol['supports_staging'] = supports_staging

            protocols.append(protocol)
        return protocols

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
        for service in self._db.query(ExternalIntegration).filter(
            ExternalIntegration.goal==goal):
            candidates = [p for p in protocols if p.get("name") == service.protocol]
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
                if setting.get("type") == "list":
                    value = ConfigurationSetting.for_externalintegration(
                        key, service).json_value
                else:
                    value = ConfigurationSetting.for_externalintegration(
                        key, service).value
                settings[key] = value

            service_info = dict(
                id=service.id,
                name=service.name,
                protocol=service.protocol,
                settings=settings,
                libraries=libraries,
            )

            if "test_search_term" in [x.get("key") for x in protocol.get("settings")]:
                service_info["self_test_results"] = self._get_prior_test_results(service)

            services.append(service_info)
        return services

    def _set_integration_setting(self, integration, setting):
        key = setting.get("key")
        if setting.get("type") == "list" and not setting.get("options"):
            value = [item for item in flask.request.form.getlist(key) if item]
            if value:
                value = json.dumps(value)
        else:
            value = flask.request.form.get(key)
        if value and setting.get("options"):
            # This setting can only take on values that are in its
            # list of options.
            allowed = [option.get("key") for option in setting.get("options")]
            if value not in allowed:
                return INVALID_CONFIGURATION_OPTION.detailed(_(
                    "The configuration value for %(setting)s is invalid.",
                    setting=setting.get("label"),
                ))
        if not value and setting.get("required") and not "default" in setting.keys():
            return INCOMPLETE_CONFIGURATION.detailed(
                _("The configuration is missing a required setting: %(setting)s",
                  setting=setting.get("label")))
        integration.setting(key).value = value

    def _set_integration_library(self, integration, library_info, protocol):
        library = get_one(self._db, Library, short_name=library_info.get("short_name"))
        if not library:
            return NO_SUCH_LIBRARY.detailed(_("You attempted to add the integration to %(library_short_name)s, but it does not exist.", library_short_name=library_info.get("short_name")))

        integration.libraries += [library]
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")
            value = library_info.get(key)
            if value and setting.get("type") == "list" and not setting.get("options"):
                value = json.dumps(value)
            if setting.get("options") and value not in [option.get("key") for option in setting.get("options")]:
                return INVALID_CONFIGURATION_OPTION.detailed(_(
                    "The configuration value for %(setting)s is invalid.",
                    setting=setting.get("label"),
                ))
            if not value and setting.get("required"):
                return INCOMPLETE_CONFIGURATION.detailed(
                    _("The configuration is missing a required setting: %(setting)s for library %(library)s",
                      setting=setting.get("label"),
                      library=library.short_name,
                      ))
            ConfigurationSetting.for_library_and_externalintegration(self._db, key, library, integration).value = value

    def _set_integration_settings_and_libraries(self, integration, protocol):
        settings = protocol.get("settings")
        for setting in settings:
            result = self._set_integration_setting(integration, setting)
            if isinstance(result, ProblemDetail):
                return result

        if not protocol.get("sitewide") or protocol.get("library_settings"):
            integration.libraries = []

            libraries = []
            if flask.request.form.get("libraries"):
                libraries = json.loads(flask.request.form.get("libraries"))

            for library_info in libraries:
                result = self._set_integration_library(integration, library_info, protocol)
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
        return Response(unicode(_("Deleted")), 200)


    def _get_collection_protocols(self, provider_apis):
        protocols = self._get_integration_protocols(provider_apis, protocol_name_attr="NAME")
        protocols.append(dict(name=ExternalIntegration.MANUAL,
                              label=_("Manual import"),
                              description=_("Books will be manually added to the circulation manager, not imported automatically through a protocol."),
                              settings=[],
                              ))

        return protocols

    def _get_prior_test_results(self, item, protocol_class=None):
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

            elif self.type == "patron authentication service":
                library = None
                if len(item.libraries):
                    library = item.libraries[0]
                    self_test_results = protocol_class.prior_test_results(
                        self._db, None, library, item
                    )
                else:
                    self_test_results = dict(
                        exception=_("You must associate this service with at least one library before you can run self tests for it."),
                        disabled=True
                    )

        except Exception, e:
            # This is bad, but not so bad that we should short-circuit
            # this whole process -- that might prevent an admin from
            # making the configuration changes necessary to fix
            # this problem.
            message = _("Exception getting self-test results for %s %s: %s")
            args = (self.type, item.name, e.message)
            logging.warn(message, *args, exc_info=e)
            self_test_results = dict(exception=message % args)

        return self_test_results

    def _mirror_integration_setting(self):
        """Create a setting interface for selecting a storage integration to
        be used when mirroring items from a collection.
        """
        integrations = self._db.query(ExternalIntegration).filter(
            ExternalIntegration.goal==ExternalIntegration.STORAGE_GOAL
        ).order_by(
            ExternalIntegration.name
        ).all()
        if not integrations:
            return
        mirror_integration_setting = {
            "key": "mirror_integration_id",
            "label": _("Mirror"),
            "description": _("Any cover images or free books encountered while importing content from this collection can be mirrored to a server you control."),
            "type": "select",
            "options" : [
                dict(
                    key=self.NO_MIRROR_INTEGRATION,
                    label=_("None - Do not mirror cover images or free books")
                )
            ]
        }
        for integration in integrations:
            mirror_integration_setting['options'].append(
                dict(key=integration.id, label=integration.name)
            )
        return mirror_integration_setting

    def _create_integration(self, protocol_definitions, protocol, goal):
        """Create a new ExternalIntegration for the given protocol and
        goal, assuming that doing so is compatible with the protocol's
        definition.

        :return: A 2-tuple (result, is_new). `result` will be an
            ExternalIntegration if one could be created, and a
            ProblemDetail otherwise.
        """
        if not protocol:
            return NO_PROTOCOL_FOR_NEW_SERVICE, False
        matches = [x for x in protocol_definitions if x.get('name') == protocol]
        if not matches:
            return UNKNOWN_PROTOCOL, False
        definition = matches[0]

        # Most of the time there can be multiple ExternalIntegrations with
        # the same protocol and goal...
        allow_multiple = True
        m = create
        args = (self._db, ExternalIntegration)
        kwargs = dict(protocol=protocol, goal=goal)
        if definition.get('cardinality') == 1:
            # ...but not all the time.
            allow_multiple = False
            existing = get_one(*args, **kwargs)
            if existing is not None:
                # We were asked to create a new ExternalIntegration
                # but there's already one for this protocol, which is not
                # allowed.
                return DUPLICATE_INTEGRATION, False
            m = get_one_or_create

        integration, is_new = m(*args, **kwargs)
        if not is_new and not allow_multiple:
            # This can happen, despite our check above, in a race
            # condition where two clients try simultaneously to create
            # two integrations of the same type.
            return DUPLICATE_INTEGRATION, False
        return integration, is_new

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(auth_service, protocol)
        if isinstance(result, ProblemDetail):
            return result

    def check_name_unique(self, new_service, name):
        """A service cannot be created with, or edited to have, the same name
        as a service that already exists.
        This method is used by analytics_services, cdn_services, discovery_services,
        metadata_services, and sitewide_services.
        """

        existing_service = get_one(self._db, ExternalIntegration, name=name)
        if existing_service and not existing_service.id == new_service.id:
            # Without checking that the IDs are different, you can't save
            # changes to an existing service unless you've also changed its name.
            return INTEGRATION_NAME_ALREADY_IN_USE

    def look_up_service_by_id(self, id, protocol, goal=None):
        """Find an existing service, and make sure that the user is not trying to edit
        its protocol.
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
        """Validate the protocol that the user has submitted; depending on whether
        the validations pass, either save it to this metadata service or
        return an error message.
        This method is used by analytics_services, cdn_services, discovery_services,
        metadata_services, and sitewide_services.
        """

        if not protocols:
            protocols = self.protocols

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

    def validate_protocol(self, protocols=None):
        protocols = protocols or self.protocols
        if flask.request.form.get("protocol") not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

    def _get_settings(self):
        [protocol] = [p for p in self.protocols if p.get("name") == flask.request.form.get("protocol")]
        return protocol.get("settings")

    def validate_formats(self, settings=None):
        # If the service has self.protocols set, we can extract the list of settings here;
        # otherwise, the settings have to be passed in as an argument--either a list or
        # a string.
        settings = settings or self._get_settings()
        validators = [
            self.validate_email,
            self.validate_url,
            self.validate_number,
            self.validate_language_code,
            self.validate_geographic_areas
        ]
        for validator in validators:
            error = validator(settings)
            if error:
                return error

    def _value(self, field):
        # Extract the user's input for this field. If this is a sitewide setting,
        # then the input needs to be accessed via "value" rather than via the setting's key.
        # We use getlist instead of get so that, if the field is such that the user can input multiple values
        # (e.g. language codes), we'll extract all the values, not just the first one.

        value = flask.request.form.getlist(field.get("key"))
        if not value:
            return flask.request.form.get("value")
        elif len(value) == 1:
            return value[0]
        return value

    def validate_email(self, settings):
        """Find any email addresses that the user has submitted, and make sure that
        they are in a valid format.
        This method is used by individual_admin_settings and library_settings.
        """
        # If :param settings is a list of objects--i.e. the LibrarySettingsController
        # is calling this method--then we need to pull out the relevant input strings
        # to validate.
        if isinstance(settings, (list,)):
            # Find the fields that have to do with email addresses and are not blank
            email_fields = filter(lambda s: s.get("format") == "email" and self._value(s), settings)
            # Narrow the email-related fields down to the ones for which the user actually entered a value
            email_inputs = [self._value(field) for field in email_fields]
            # Now check that each email input is in a valid format
        else:
        # If the IndividualAdminSettingsController is calling this method, then we already have the
        # input string; it was passed in directly.
            email_inputs = [settings]
        for email in email_inputs:
            if not self._is_email(email):
                return INVALID_EMAIL.detailed(_('"%(email)s" is not a valid email address.', email=email))

    def _is_email(self, email):
        """Email addresses must be in the format 'x@y.z'."""
        email_format = ".+\@.+\..+"
        return re.search(email_format, email)

    def validate_url(self, settings):
        """Find any URLs that the user has submitted, and make sure that
        they are in a valid format."""
        # Find the fields that have to do with URLs and are not blank.
        url_fields = filter(lambda s: s.get("format") == "url" and self._value(s), settings)

        for field in url_fields:
            url = self._value(field)
            # In a few special cases, we want to allow a value that isn't a normal URL;
            # for example, the patron web client URL can be set to "*".
            allowed = field.get("allowed") or []
            if not self._is_url(url, allowed):
                return INVALID_URL.detailed(_('"%(url)s" is not a valid URL.', url=url))

    def _is_url(self, url, allowed):
        has_protocol = any([url.startswith(protocol + "://") for protocol in "http", "https"])
        return has_protocol or (url in allowed)

    def validate_number(self, settings):
        """Find any numbers that the user has submitted, and make sure that they are 1) actually numbers,
        2) positive, and 3) lower than the specified maximum, if there is one."""
        # Find the fields that should have numeric input and are not blank.
        number_fields = filter(lambda s: s.get("type") == "number" and self._value(s), settings)
        for field in number_fields:
            if self._number_error(field):
                return self._number_error(field)

    def _number_error(self, field):
        input = flask.request.form.get(field.get("key")) or flask.request.form.get("value")
        min = field.get("min") or 0
        max = field.get("max")

        try:
            input = float(input)
        except ValueError:
            return INVALID_NUMBER.detailed(_('"%(input)s" is not a number.', input=input))

        if input < min:
            return INVALID_NUMBER.detailed(_('%(field)s must be greater than %(min)s.', field=field.get("label"), min=min))
        if max and input > max:
            return INVALID_NUMBER.detailed(_('%(field)s cannot be greater than %(max)s.', field=field.get("label"), max=max))

    def validate_language_code(self, settings):
        # Find the fields that should contain language codes and are not blank.
        language_fields = filter(lambda s: s.get("format") == "language-code" and self._value(s), settings)

        for language in self._list_of_values(language_fields):
            if not self._is_language(language):
                return UNKNOWN_LANGUAGE.detailed(_('"%(language)s" is not a valid language code.', language=language))

    def _is_language(self, language):
        # Check that the input string is in the list of recognized language codes.
        return LanguageCodes.string_to_alpha_3(language)

    def validate_geographic_areas(self, settings):
        geographic_fields = filter(lambda s: s.get("format") == "geographic" and self._value(s), settings)
        search = uszipcode.SearchEngine(simple_zipcode=True)
        for value in self._list_of_values(geographic_fields):
            if value == "everywhere":
                continue
            elif isinstance(value, basestring):
                if len(value) == 2:
                    # Is it a state abbreviation?
                    if not len(search.query(state=value)):
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid U.S. state abbreviation.', value=value))

                elif len(value.split(", ")) == 2:
                    # Is it in the format "[city], [state abbreviation]" or "[county], [state abbreviation]"?
                    city_or_county, state = value.split(", ")
                    if not search.by_city_and_state(city_or_county, state) and not city_or_county in [x.county for x in search.query(state=state, returns=None)]:
                        return UNKNOWN_LOCATION.detailed(_('Unable to locate "%(value)s".', value=value))

                elif value.isdigit():
                    # Is it a zipcode?
                    if not search.by_zipcode(value):
                        return UNKNOWN_LOCATION.detailed(_('"%(value)s" is not a valid U.S. zipcode.', value=value))

    def _list_of_values(self, fields):
        result = []
        for field in fields:
            result += self._value(field)
        return filter(None, result)

class SitewideRegistrationController(SettingsController):
    """A controller for managing a circulation manager's registrations
    with external services.

    Currently the only supported site-wide registration is with a
    metadata wrangler. The protocol for registration with library
    registries and ODL collections is similar, but those registrations
    happen on the level of the individual library, not on the level of
    the circulation manager.
    """

    def process_sitewide_registration(self, integration, do_get=HTTP.debuggable_get,
                              do_post=HTTP.debuggable_post
    ):
        """Performs a sitewide registration for a particular service.

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

        ignore, private_key = self.manager.sitewide_key_pair
        decryptor = Configuration.cipher(private_key)
        shared_secret = decryptor.decrypt(base64.b64decode(shared_secret))
        integration.password = unicode(shared_secret)

    def get_catalog(self, do_get, url):
        """Get the catalog for this service."""

        try:
            response = do_get(url)
        except Exception as e:
            return REMOTE_INTEGRATION_FAILED.detailed(e.message)

        if isinstance(response, ProblemDetail):
            return response
        return response

    def check_content_type(self, catalog_response):
        """Make sure the catalog for the service is in a valid format."""

        content_type = catalog_response.headers.get('Content-Type')
        if content_type != 'application/opds+json':
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide a valid catalog.')
            )

    def get_registration_link(self, catalog, links):
        """Get the link for registration from the catalog."""

        register_link_filter = lambda l: (
            l.get('rel')=='register' and
            l.get('type')==self.METADATA_SERVICE_URI_TYPE
        )

        register_urls = filter(register_link_filter, links)
        if not register_urls:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide a register link.')
            )

        # Get the full registration url.
        register_url = register_urls[0].get('href')
        if not register_url.startswith('http'):
            # We have a relative path. Create a full registration url.
            base_url = catalog.get('id')
            register_url = urlparse.urljoin(base_url, register_url)

        return register_url

    def update_headers(self, integration):
        """If the integration has an existing shared_secret, use it to access the
        server and update it."""

        # NOTE: This is no longer technically necessary since we prove
        # ownership with a signed JWT.
        headers = { 'Content-Type' : 'application/x-www-form-urlencoded' }
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
            return REMOTE_INTEGRATION_FAILED.detailed(e.message)
        return response

    def get_shared_secret(self, response):
        """Find the shared secret which we need to use in order to register this
        service, or return an error message if there is no shared secret."""

        registration_info = response.json()
        shared_secret = registration_info.get('metadata', {}).get('shared_secret')

        if not shared_secret:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide registration information.')
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
        # Advertise the public key so that the foreign site can encrypt
        # things for us.
        public_key_dict = dict(type='RSA', value=public_key)
        public_key_url = self.url_for('public_key_document')
        in_one_minute = datetime.utcnow() + timedelta(seconds=60)
        payload = {'exp': in_one_minute}
        # Sign a JWT with the private key to prove ownership of the site.
        token = jwt.encode(payload, private_key, algorithm='RS256')
        return dict(url=public_key_url, jwt=token)
