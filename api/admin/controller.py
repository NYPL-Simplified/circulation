from nose.tools import set_trace
import logging
import sys
import os
import base64
import random
import uuid
import json
import re
import urllib
import urlparse

import flask
from flask import (
    Response,
    redirect,
)
from flask_babel import lazy_gettext as _
import jwt
from sqlalchemy.exc import ProgrammingError
from PIL import Image, ImageDraw, ImageFont
import textwrap
from StringIO import StringIO
import feedparser
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

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
    Work,
    WorkGenre,
)
from core.lane import Lane
from core.log import (LogConfiguration, SysLogger, Loggly)
from core.util.problem_detail import (
    ProblemDetail,
    JSON_MEDIA_TYPE as PROBLEM_DETAIL_JSON_MEDIA_TYPE,
)
from core.metadata_layer import (
    Metadata,
    LinkData,
    ReplacementPolicy,
)
from core.mirror import MirrorUploader
from core.util.http import HTTP
from problem_details import *
from exceptions import *
from core.util import (
    fast_query_count,
    LanguageCodes,
)

from api.config import (
    Configuration,
    CannotLoadConfiguration
)
from api.lanes import create_default_lanes

from google_oauth_admin_authentication_provider import GoogleOAuthAdminAuthenticationProvider
from password_admin_authentication_provider import PasswordAdminAuthenticationProvider

from api.controller import CirculationManagerController
from api.coverage import MetadataWranglerCollectionRegistrar
from core.app_server import entry_response
from core.app_server import (
    entry_response,
    feed_response,
    load_pagination_from_request
)
from core.opds import AcquisitionFeed
from opds import AdminAnnotator, AdminFeed
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

from templates import admin as admin_template

from api.authenticator import AuthenticationProvider
from api.simple_authentication import SimpleAuthenticationProvider
from api.millenium_patron import MilleniumPatronAPI
from api.sip import SIP2AuthenticationProvider
from api.firstbook import FirstBookAuthenticationAPI
from api.clever import CleverAuthenticationAPI

from core.opds_import import OPDSImporter
from api.feedbooks import FeedbooksOPDSImporter
from api.opds_for_distributors import OPDSForDistributorsAPI
from api.overdrive import OverdriveAPI
from api.odilo import OdiloAPI
from api.bibliotheca import BibliothecaAPI
from api.axis import Axis360API
from api.oneclick import OneClickAPI
from api.enki import EnkiAPI
from api.odl import ODLWithConsolidatedCopiesAPI, SharedODLAPI

from api.nyt import NYTBestSellerAPI
from api.novelist import NoveListAPI
from core.opds_import import MetadataWranglerOPDSLookup

from api.google_analytics_provider import GoogleAnalyticsProvider
from core.local_analytics_provider import LocalAnalyticsProvider

from api.adobe_vendor_id import AuthdataUtility

from core.external_search import ExternalSearchIndex

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
    manager.admin_work_controller = WorkController(manager)
    manager.admin_feed_controller = FeedController(manager)
    manager.admin_custom_lists_controller = CustomListsController(manager)
    manager.admin_lanes_controller = LanesController(manager)
    manager.admin_dashboard_controller = DashboardController(manager)
    manager.admin_settings_controller = SettingsController(manager)

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


class SignInController(AdminController):

    ERROR_RESPONSE_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
<head><meta charset="utf8"></head>
</body>
<p><strong>%(status_code)d ERROR:</strong> %(message)s</p>
</body>
</html>"""

    SIGN_IN_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
<head><meta charset="utf8"></head>
<body>
%(auth_provider_html)s
</body>
</html>"""


    def sign_in(self):
        """Redirects admin if they're signed in, or shows the sign in page."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        admin = self.authenticated_admin_from_request()

        if isinstance(admin, ProblemDetail):
            redirect_url = flask.request.args.get("redirect")
            auth_provider_html = [auth.sign_in_template(redirect_url) for auth in self.admin_auth_providers]
            auth_provider_html = "<br/><hr/>or<br/><br/>".join(auth_provider_html)
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

            admin_dir = os.path.split(__file__)[0]
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
            entries = flask.request.form.get("entries")
            collections = flask.request.form.get("collections")
            return self._create_or_update_list(library, name, entries, collections, id)

    def _create_or_update_list(self, library, name, entries, collections, id=None):
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

        if entries:
            entries = json.loads(entries)
        else:
            entries = []

        old_entries = [x for x in list.entries if x.edition]
        membership_change = False
        for entry in entries:
            urn = entry.get("identifier_urn")

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

            if work:
                entry, entry_is_new = list.add_entry(work, featured=True)
                if entry_is_new:
                    membership_change = True

        new_urns = [entry.get("identifier_urn") for entry in entries]
        for entry in old_entries:
            if entry.edition.primary_identifier.urn not in new_urns:
                list.remove_entry(entry.edition)
                membership_change = True

        if membership_change:
            # If this list was used to populate any lanes, those
            # lanes need to have their counts updated.
            for lane in Lane.affected_by_customlist(list):
                lane.update_size(self._db)

        if collections:
            collections = json.loads(collections)
        else:
            collections = []
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

    def custom_list(self, list_id):
        library = flask.request.library
        self.require_librarian(library)
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        list = get_one(self._db, CustomList, id=list_id, data_source=data_source)
        if not list:
            return MISSING_CUSTOM_LIST

        if flask.request.method == "GET":
            entries = []
            for entry in list.entries:
                if entry.edition:
                    url = self.url_for(
                        "permalink",
                        identifier_type=entry.edition.primary_identifier.type,
                        identifier=entry.edition.primary_identifier.identifier,
                        library_short_name=library.short_name,
                    )
                    entries.append(dict(identifier_urn=entry.edition.primary_identifier.urn,
                                        title=entry.edition.title,
                                        authors=[author.display_name for author in entry.edition.author_contributors],
                                        medium=Edition.medium_to_additional_type.get(entry.edition.medium, None),
                                        url=url,
                                        language=entry.edition.language,
                    ))
            collections = []
            for collection in list.collections:
                collections.append(dict(id=collection.id, name=collection.name, protocol=collection.protocol))
            return dict(id=list.id, name=list.name, entries=entries, collections=collections, entry_count=len(entries))

        elif flask.request.method == "POST":
            name = flask.request.form.get("name")
            entries = flask.request.form.get("entries")
            collections = flask.request.form.get("collections")
            return self._create_or_update_list(library, name, entries, collections, list_id)

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


class DashboardController(AdminCirculationManagerController):

    def stats(self):
        patron_count = self._db.query(Patron).count()

        active_loans_patron_count = self._db.query(
            distinct(Patron.id)
        ).join(
            Patron.loans
        ).filter(
            Loan.end >= datetime.now(),
        ).count()

        active_patrons = select(
            [Patron.id]
        ).select_from(
            join(
                Loan,
                Patron,
                and_(
                    Patron.id == Loan.patron_id,
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
                    Patron.id == Hold.patron_id
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
        ).filter(
            Loan.end >= datetime.now()
        ).count()

        hold_count = self._db.query(Hold).count()

        data_sources = dict(
            overdrive=DataSource.OVERDRIVE,
            bibliotheca=DataSource.BIBLIOTHECA,
            axis360=DataSource.AXIS_360,
        )
        vendor_counts = dict()

        for key, data_source in data_sources.iteritems():
            data_source_count = self._db.query(
                LicensePool
            ).join(
                DataSource
            ).filter(
                LicensePool.licenses_owned > 0
            ).filter(
                DataSource.name == data_source
            ).count()

            if data_source_count > 0:
                vendor_counts[key] = data_source_count

        open_access_count = self._db.query(
            LicensePool
         ).filter(
            LicensePool.open_access == True
         ).count()

        if open_access_count > 0:
            vendor_counts['open_access'] = open_access_count

        title_count = self._db.query(LicensePool).count()

        # The sum queries return None instead of 0 if there are
        # no license pools in the db.

        license_count = self._db.query(
            func.sum(LicensePool.licenses_owned)
        ).filter(
            LicensePool.open_access == False,
        ).all()[0][0] or 0

        available_license_count = self._db.query(
            func.sum(LicensePool.licenses_available)
        ).filter(
            LicensePool.open_access == False,
        ).all()[0][0] or 0

        return dict(
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
            vendors=vendor_counts,
        )

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

    def libraries(self):
        if flask.request.method == 'GET':
            libraries = []
            for library in self._db.query(Library).order_by(Library.name):
                # Only include libraries this admin has librarian access to.
                if not flask.request.admin or not flask.request.admin.is_librarian(library):
                    continue

                settings = dict()
                for setting in Configuration.LIBRARY_SETTINGS:
                    if setting.get("type") == "list":
                        value = ConfigurationSetting.for_library(setting.get("key"), library).json_value
                    else:
                        value = ConfigurationSetting.for_library(setting.get("key"), library).value
                    if value:
                        settings[setting.get("key")] = value
                libraries += [dict(
                    uuid=library.uuid,
                    name=library.name,
                    short_name=library.short_name,
                    settings=settings,
                )]
            return dict(libraries=libraries, settings=Configuration.LIBRARY_SETTINGS)


        library_uuid = flask.request.form.get("uuid")
        name = flask.request.form.get("name")
        short_name = flask.request.form.get("short_name")

        library = None
        is_new = False

        if not short_name:
            return MISSING_LIBRARY_SHORT_NAME

        if library_uuid:
            # Library UUID is required when editing an existing library
            # from the admin interface, and isn't present for new libraries.
            library = get_one(
                self._db, Library, uuid=library_uuid,
            )
            if not library:
                return LIBRARY_NOT_FOUND.detailed(_("The specified library uuid does not exist."))

        if not library or short_name != library.short_name:
            # If you're adding a new short_name, either by editing an
            # existing library or creating a new library, it must be unique.
            library_with_short_name = get_one(self._db, Library, short_name=short_name)
            if library_with_short_name:
                return LIBRARY_SHORT_NAME_ALREADY_IN_USE

        if not library:
            self.require_system_admin()
            library, is_new = create(
                self._db, Library, short_name=short_name,
                uuid=str(uuid.uuid4()))
        else:
            self.require_library_manager(library)

        if name:
            library.name = name
        if short_name:
            library.short_name = short_name

        NO_VALUE = object()
        for setting in Configuration.LIBRARY_SETTINGS:
            # Start off by assuming the value is not set.
            value = NO_VALUE
            if setting.get("type") == "list":
                if setting.get('options'):
                    # Restrict to the values in 'options'.
                    value = []
                    for option in setting.get("options"):
                        if setting["key"] + "_" + option["key"] in flask.request.form:
                            value += [option["key"]]
                else:
                    # Allow any entered values.
                    value = [item for item in flask.request.form.getlist(setting.get('key')) if item]
                value = json.dumps(value)
            elif setting.get("type") == "image":
                image_file = flask.request.files.get(setting.get("key"))
                if not image_file and not setting.get("optional"):
                    self._db.rollback()
                    return INCOMPLETE_CONFIGURATION.detailed(_(
                        "The library is missing a required setting: %s." % setting.get("key")))
                if image_file:
                    allowed_types = [Representation.JPEG_MEDIA_TYPE, Representation.PNG_MEDIA_TYPE, Representation.GIF_MEDIA_TYPE]
                    type = image_file.headers.get("Content-Type")
                    if type not in allowed_types:
                        self._db.rollback()
                        return INVALID_CONFIGURATION_OPTION.detailed(_(
                            "Upload for %(setting)s must be in GIF, PNG, or JPG format. (Upload was %(format)s.)",
                            setting=setting.get("label"),
                            format=type))
                    image = Image.open(image_file)
                    width, height = image.size
                    if width > 135 or height > 135:
                        image.thumbnail((135, 135), Image.ANTIALIAS)
                    buffer = StringIO()
                    image.save(buffer, format="PNG")
                    b64 = base64.b64encode(buffer.getvalue())
                    value = "data:image/png;base64,%s" % b64
            else:
                default = setting.get('default')
                value = flask.request.form.get(setting['key'], default)
            if value != NO_VALUE:
                ConfigurationSetting.for_library(setting['key'], library).value = value
            if not value and not setting.get("optional"):
                self._db.rollback()
                return INCOMPLETE_CONFIGURATION.detailed(
                    _("The configuration is missing a required setting: %(setting)s",
                      setting=setting.get("label"),
                    ))

        if is_new:
            # Now that the configuration settings are in place, create
            # a default set of lanes.
            create_default_lanes(self._db, library)

        if is_new:
            return Response(unicode(library.uuid), 201)
        else:
            return Response(unicode(library.uuid), 200)

    def library(self, library_uuid):
        if flask.request.method == "DELETE":
            self.require_system_admin()
            library = get_one(self._db, Library, uuid=library_uuid)
            if not library:
                return LIBRARY_NOT_FOUND.detailed(_("The specified library uuid does not exist."))
            self._db.delete(library)
            return Response(unicode(_("Deleted")), 200)

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

            services.append(
                dict(
                    id=service.id,
                    name=service.name,
                    protocol=service.protocol,
                    settings=settings,
                    libraries=libraries,
                )
            )

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
                self._db.rollback()
                return INVALID_CONFIGURATION_OPTION.detailed(_(
                    "The configuration value for %(setting)s is invalid.",
                    setting=setting.get("label"),
                ))
        if not value and not setting.get("optional"):
            # Roll back any changes to the integration that have already been made.
            self._db.rollback()
            return INCOMPLETE_CONFIGURATION.detailed(
                _("The configuration is missing a required setting: %(setting)s",
                  setting=setting.get("label")))
        integration.setting(key).value = value

    def _set_integration_library(self, integration, library_info, protocol):
        library = get_one(self._db, Library, short_name=library_info.get("short_name"))
        if not library:
            self._db.rollback()
            return NO_SUCH_LIBRARY.detailed(_("You attempted to add the integration to %(library_short_name)s, but it does not exist.", library_short_name=library_info.get("short_name")))

        integration.libraries += [library]
        for setting in protocol.get("library_settings", []):
            key = setting.get("key")
            value = library_info.get(key)
            if value and setting.get("type") == "list" and not setting.get("options"):
                value = json.dumps(value)
            if setting.get("options") and value not in [option.get("key") for option in setting.get("options")]:
                self._db.rollback()
                return INVALID_CONFIGURATION_OPTION.detailed(_(
                    "The configuration value for %(setting)s is invalid.",
                    setting=setting.get("label"),
                ))
            if not value and not setting.get("optional"):
                self._db.rollback()
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

    def _sitewide_settings_controller(self, configuration_object):
        self.require_system_admin()

        if flask.request.method == 'GET':
            settings = []
            for s in configuration_object.SITEWIDE_SETTINGS:
                setting = ConfigurationSetting.sitewide(self._db, s.get("key"))
                if setting.value:
                    settings += [{ "key": setting.key, "value": setting.value }]

            return dict(
                settings=settings,
                all_settings=configuration_object.SITEWIDE_SETTINGS,
            )

        key = flask.request.form.get("key")
        if not key:
            return MISSING_SITEWIDE_SETTING_KEY

        value = flask.request.form.get("value")
        if not value:
            return MISSING_SITEWIDE_SETTING_VALUE

        setting = ConfigurationSetting.sitewide(self._db, key)
        setting.value = value
        return Response(unicode(setting.key), 200)

    def collections(self):
        provider_apis = [OPDSImporter,
                         OPDSForDistributorsAPI,
                         OverdriveAPI,
                         OdiloAPI,
                         BibliothecaAPI,
                         Axis360API,
                         OneClickAPI,
                         EnkiAPI,
                         ODLWithConsolidatedCopiesAPI,
                         SharedODLAPI,
                         FeedbooksOPDSImporter,
                        ]
        protocols = self._get_integration_protocols(provider_apis, protocol_name_attr="NAME")
        protocols.append(dict(name=ExternalIntegration.MANUAL,
                              label=_("Manual import"),
                              description=_("Books will be manually added to the circulation manager, not imported automatically through a protocol."),
                              settings=[],
                              ))

        # If there are storage integrations, add a mirror integration
        # setting to every protocol's 'settings' block.
        mirror_integration_setting = self._mirror_integration_setting()
        if mirror_integration_setting:
            for protocol in protocols:
                protocol['settings'].append(mirror_integration_setting)

        if flask.request.method == 'GET':
            collections = []
            for c in self._db.query(Collection).order_by(Collection.name).all():
                visible = False
                for library in c.libraries:
                    if flask.request.admin and flask.request.admin.is_librarian(library):
                        visible = True
                # If the collection's not associated with any libraries, only system
                # admins can see it.
                if not c.libraries:
                    if flask.request.admin and flask.request.admin.is_system_admin():
                        visible = True
                if not visible:
                    continue

                collection = dict(
                    id=c.id,
                    name=c.name,
                    protocol=c.protocol,
                    parent_id=c.parent_id,
                    settings=dict(external_account_id=c.external_account_id),
                )
                if c.protocol in [p.get("name") for p in protocols]:
                    [protocol] = [p for p in protocols if p.get("name") == c.protocol]
                    libraries = []
                    for library in c.libraries:
                        if not flask.request.admin or not flask.request.admin.is_librarian(library):
                            continue
                        libraries.append(self._get_integration_library_info(
                                c.external_integration, library, protocol))
                    collection['libraries'] = libraries

                    for setting in protocol.get("settings"):
                        key = setting.get("key")
                        if key not in collection["settings"]:
                            if key == 'mirror_integration_id':
                                value = c.mirror_integration_id or self.NO_MIRROR_INTEGRATION
                            elif setting.get("type") == "list":
                                value = c.external_integration.setting(key).json_value
                            else:
                                value = c.external_integration.setting(key).value
                            collection["settings"][key] = value

                collections.append(collection)

            return dict(
                collections=collections,
                protocols=protocols,
            )

        self.require_system_admin()

        id = flask.request.form.get("id")

        name = flask.request.form.get("name")
        if not name:
            return MISSING_COLLECTION_NAME

        protocol = flask.request.form.get("protocol")

        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

        is_new = False
        collection = None
        if id:
            collection = get_one(self._db, Collection, id=id)
            if not collection:
                return MISSING_COLLECTION

        if collection:
            if protocol != collection.protocol:
                return CANNOT_CHANGE_PROTOCOL
            if name != collection.name:
                collection_with_name = get_one(self._db, Collection, name=name)
                if collection_with_name:
                    return COLLECTION_NAME_ALREADY_IN_USE

        else:
            if protocol:
                collection, is_new = get_one_or_create(self._db, Collection, name=name)
                if not is_new:
                    self._db.rollback()
                    return COLLECTION_NAME_ALREADY_IN_USE
                collection.create_external_integration(protocol)
            else:
                return NO_PROTOCOL_FOR_NEW_SERVICE

        collection.name = name
        [protocol] = [p for p in protocols if p.get("name") == protocol]

        parent_id = flask.request.form.get("parent_id")

        if parent_id and not protocol.get("child_settings"):
            self._db.rollback()
            return PROTOCOL_DOES_NOT_SUPPORT_PARENTS

        if parent_id:
            parent = get_one(self._db, Collection, id=parent_id)
            if not parent:
                self._db.rollback()
                return MISSING_PARENT
            collection.parent = parent
            settings = protocol.get("child_settings")
        else:
            collection.parent = None
            settings = protocol.get("settings")

        for setting in settings:
            key = setting.get("key")
            if key == "external_account_id":
                value = flask.request.form.get(key)
                if not value and not setting.get("optional"):
                    # Roll back any changes to the collection that have already been made.
                    self._db.rollback()
                    return INCOMPLETE_CONFIGURATION.detailed(
                        _("The collection configuration is missing a required setting: %(setting)s",
                          setting=setting.get("label")))
                collection.external_account_id = value
            elif key == 'mirror_integration_id':
                value = flask.request.form.get(key)
                if value == self.NO_MIRROR_INTEGRATION:
                    integration_id = None
                else:
                    integration = get_one(
                        self._db, ExternalIntegration, id=value
                    )
                    if not integration:
                        self._db.rollback()
                        return MISSING_SERVICE
                    if integration.goal != ExternalIntegration.STORAGE_GOAL:
                        self._db.rollback()
                        return INTEGRATION_GOAL_CONFLICT
                    integration_id = integration.id
                collection.mirror_integration_id = integration_id
            else:
                result = self._set_integration_setting(collection.external_integration, setting)
                if isinstance(result, ProblemDetail):
                    return result

        libraries = []
        if flask.request.form.get("libraries"):
            libraries = json.loads(flask.request.form.get("libraries"))

        for library_info in libraries:
            library = get_one(self._db, Library, short_name=library_info.get("short_name"))
            if not library:
                return NO_SUCH_LIBRARY.detailed(_("You attempted to add the collection to %(library_short_name)s, but it does not exist.", library_short_name=library_info.get("short_name")))
            if collection not in library.collections:
                library.collections.append(collection)
            result = self._set_integration_library(collection.external_integration, library_info, protocol)
            if isinstance(result, ProblemDetail):
                return result
        for library in collection.libraries:
            if library.short_name not in [l.get("short_name") for l in libraries]:
                library.collections.remove(collection)
                for setting in protocol.get("library_settings", []):
                    ConfigurationSetting.for_library_and_externalintegration(
                        self._db, setting.get("key"), library, collection.external_integration,
                    ).value = None
        if is_new:
            return Response(unicode(collection.id), 201)
        else:
            return Response(unicode(collection.id), 200)

    def collection_library_registrations(self, do_get=HTTP.debuggable_get,
                                 do_post=HTTP.debuggable_post, key=None):
        self.require_system_admin()
        # TODO: This method might be able to share code with discovery_service_library_registrations.
        shared_collection_provider_apis = [SharedODLAPI]
        LIBRARY_REGISTRATION_STATUS = u"library-registration-status"
        SUCCESS = u"success"
        FAILURE = u"failure"

        if flask.request.method == "GET":
            collections = []
            for collection in self._db.query(Collection):
                libraries = []
                for library in collection.libraries:
                    library_info = dict(short_name=library.short_name)
                    status = ConfigurationSetting.for_library_and_externalintegration(
                        self._db, LIBRARY_REGISTRATION_STATUS, library, collection.external_integration,
                    ).value
                    if status:
                        library_info["status"] = status
                        libraries.append(library_info)
                collections.append(
                    dict(
                        id=collection.id,
                        libraries=libraries,
                    )
                )
            return dict(library_registrations=collections)

        if flask.request.method == "POST":
            collection_id = flask.request.form.get("collection_id")
            library_short_name = flask.request.form.get("library_short_name")

            collection = get_one(self._db, Collection, id=collection_id)
            if not collection:
                return MISSING_COLLECTION
            if collection.protocol not in [api.NAME for api in shared_collection_provider_apis]:
                return COLLECTION_DOES_NOT_SUPPORT_REGISTRATION

            library = get_one(self._db, Library, short_name=library_short_name)
            if not library:
                return NO_SUCH_LIBRARY

            status = ConfigurationSetting.for_library_and_externalintegration(
                self._db, LIBRARY_REGISTRATION_STATUS, library, collection.external_integration)
            status.value = FAILURE
            registered = self._register_library(collection.external_account_id, library, collection.external_integration,
                                                do_get=do_get, do_post=do_post, key=key)
            if isinstance(registered, ProblemDetail):
                return registered
            status.value = SUCCESS
        return Response(unicode(_("Success")), 200)

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

    def collection(self, collection_id):
        if flask.request.method == "DELETE":
            self.require_system_admin()
            collection = get_one(self._db, Collection, id=collection_id)
            if not collection:
                return MISSING_COLLECTION
            if len(collection.children) > 0:
                return CANNOT_DELETE_COLLECTION_WITH_CHILDREN
            self._db.delete(collection)
            return Response(unicode(_("Deleted")), 200)

    def admin_auth_services(self):
        self.require_system_admin()
        provider_apis = [GoogleOAuthAdminAuthenticationProvider]
        protocols = self._get_integration_protocols(provider_apis, protocol_name_attr="NAME")

        if flask.request.method == 'GET':
            auth_services = self._get_integration_info(ExternalIntegration.ADMIN_AUTH_GOAL, protocols)
            return dict(
                admin_auth_services=auth_services,
                protocols=protocols,
            )

        protocol = flask.request.form.get("protocol")
        if protocol and protocol not in ExternalIntegration.ADMIN_AUTH_PROTOCOLS:
            return UNKNOWN_PROTOCOL

        id = flask.request.form.get("id")

        is_new = False
        auth_service = ExternalIntegration.admin_authentication(self._db)
        if auth_service:
            if id and int(id) != auth_service.id:
                return MISSING_SERVICE
            if protocol != auth_service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            if id:
                return MISSING_SERVICE

            if protocol:
                auth_service, is_new = get_one_or_create(
                    self._db, ExternalIntegration, protocol=protocol,
                    goal=ExternalIntegration.ADMIN_AUTH_GOAL
                )
            else:
                return NO_PROTOCOL_FOR_NEW_SERVICE

        name = flask.request.form.get("name")
        auth_service.name = name

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(auth_service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        if is_new:
            return Response(unicode(auth_service.protocol), 201)
        else:
            return Response(unicode(auth_service.protocol), 200)

    def admin_auth_service(self, protocol):
        if flask.request.method == "DELETE":
            self.require_system_admin()
            service = get_one(self._db, ExternalIntegration, protocol=protocol, goal=ExternalIntegration.ADMIN_AUTH_GOAL)
            if not service:
                return MISSING_SERVICE
            self._db.delete(service)
            return Response(unicode(_("Deleted")), 200)

    def individual_admins(self):
        if flask.request.method == 'GET':
            admins = []
            for admin in self._db.query(Admin):
                roles = []
                for role in admin.roles:
                    if role.library:
                        if not flask.request.admin or not flask.request.admin.is_librarian(role.library):
                            continue
                        roles.append(dict(role=role.role, library=role.library.short_name))
                    else:
                        roles.append(dict(role=role.role))
                admins.append(dict(email=admin.email, roles=roles))

            return dict(
                individualAdmins=admins,
            )

        email = flask.request.form.get("email")
        password = flask.request.form.get("password")
        roles = flask.request.form.get("roles")

        if not email:
            return INCOMPLETE_CONFIGURATION

        if roles:
            roles = json.loads(roles)
        else:
            roles = []

        # If there are no admins yet, anyone can create the first system admin.
        settingUp = (self._db.query(Admin).count() == 0)

        admin, is_new = get_one_or_create(self._db, Admin, email=email)
        if admin.is_sitewide_library_manager() and not settingUp:
            self.require_sitewide_library_manager()
        if admin.is_system_admin() and not settingUp:
            self.require_system_admin()

        if password:
            # If the admin we're editing has a sitewide manager role, we've already verified
            # the current admin's role above. Otherwise, an admin can only change that
            # admin's password if they are a library manager of one of that admin's
            # libraries, or if they are editing a new admin or an admin who has no
            # roles yet.
            # TODO: set up password reset emails instead.
            if not is_new and not admin.is_sitewide_library_manager():
                can_change_pw = False
                if not admin.roles:
                    can_change_pw = True
                if admin.is_sitewide_librarian():
                    # A manager of any library can change a sitewide librarian's password.
                    if flask.request.admin.is_sitewide_library_manager():
                        can_change_pw = True
                    else:
                        for role in flask.request.admin.roles:
                            if role.role == AdminRole.LIBRARY_MANAGER:
                                can_change_pw = True
                else:
                    for role in admin.roles:
                        if flask.request.admin.is_library_manager(role.library):
                            can_change_pw = True
                            break
                if not can_change_pw:
                    raise AdminNotAuthorized()
            admin.password = password
        try:
            self._db.flush()
        except ProgrammingError as e:
            self._db.rollback()
            return MISSING_PGCRYPTO_EXTENSION

        old_roles = admin.roles
        old_roles_set = set((role.role, role.library) for role in old_roles)
        for role in roles:
            if role.get("role") not in AdminRole.ROLES:
                self._db.rollback()
                return UNKNOWN_ROLE

            library = None
            library_short_name = role.get("library")
            if library_short_name:
                library = Library.lookup(self._db, library_short_name)
                if not library:
                    self._db.rollback()
                    return LIBRARY_NOT_FOUND.detailed(_("Library \"%(short_name)s\" does not exist.", short_name=library_short_name))

            if (role.get("role"), library) in old_roles_set:
                # The admin already has this role.
                continue

            if library:
                self.require_library_manager(library)
            elif role.get("role") == AdminRole.SYSTEM_ADMIN and not settingUp:
                self.require_system_admin()
            elif not settingUp:
                self.require_sitewide_library_manager()
            admin.add_role(role.get("role"), library)

        new_roles = set((role.get("role"), role.get("library")) for role in roles)
        for role in old_roles:
            library = None
            if role.library:
                library = role.library.short_name
            if not (role.role, library) in new_roles:
                if not library:
                    self.require_sitewide_library_manager()
                if flask.request.admin and flask.request.admin.is_librarian(role.library):
                    # A librarian can see roles for the library, but only a library manager
                    # can delete them.
                    self.require_library_manager(role.library)
                    admin.remove_role(role.role, role.library)
                else:
                    # An admin who isn't a librarian for the library won't be able to see
                    # its roles, so might make requests that change other roles without
                    # including this library's roles. Leave the non-visible roles alone.
                    continue

        if is_new:
            return Response(unicode(admin.email), 201)
        else:
            return Response(unicode(admin.email), 200)

    def individual_admin(self, email):
        if flask.request.method == "DELETE":
            self.require_sitewide_library_manager()
            admin = get_one(self._db, Admin, email=email)
            if admin.is_system_admin():
                self.require_system_admin()
            if not admin:
                return MISSING_ADMIN
            self._db.delete(admin)
            return Response(unicode(_("Deleted")), 200)

    def patron_auth_services(self):
        self.require_system_admin()

        provider_apis = [SimpleAuthenticationProvider,
                         MilleniumPatronAPI,
                         SIP2AuthenticationProvider,
                         FirstBookAuthenticationAPI,
                         CleverAuthenticationAPI,
                        ]
        protocols = self._get_integration_protocols(provider_apis)

        basic_auth_protocols = [SimpleAuthenticationProvider.__module__,
                                MilleniumPatronAPI.__module__,
                                SIP2AuthenticationProvider.__module__,
                                FirstBookAuthenticationAPI.__module__,
                               ]

        if flask.request.method == 'GET':
            services = self._get_integration_info(ExternalIntegration.PATRON_AUTH_GOAL, protocols)
            return dict(
                patron_auth_services=services,
                protocols=protocols,
            )

        id = flask.request.form.get("id")

        protocol = flask.request.form.get("protocol")
        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

        is_new = False
        if id:
            auth_service = get_one(self._db, ExternalIntegration, id=id, goal=ExternalIntegration.PATRON_AUTH_GOAL)
            if not auth_service:
                return MISSING_SERVICE
            if protocol != auth_service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            auth_service, is_new = self._create_integration(
                protocols, protocol, ExternalIntegration.PATRON_AUTH_GOAL
            )
            if isinstance(auth_service, ProblemDetail):
                return auth_service

        name = flask.request.form.get("name")
        if name:
            if auth_service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    self._db.rollback()
                    return INTEGRATION_NAME_ALREADY_IN_USE
            auth_service.name = name

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(auth_service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        for library in auth_service.libraries:
            # Check that the library didn't end up with multiple basic auth services.
            basic_auth_count = 0
            for integration in library.integrations:
                if integration.goal == ExternalIntegration.PATRON_AUTH_GOAL and integration.protocol in basic_auth_protocols:
                    basic_auth_count += 1
                    if basic_auth_count > 1:
                        self._db.rollback()
                        return MULTIPLE_BASIC_AUTH_SERVICES.detailed(_(
                            "You tried to add a patron authentication service that uses basic auth to %(library)s, but it already has one.",
                            library=library.short_name,
                        ))

            # Check that the library's external type regular express is valid, if it was set.
            value = ConfigurationSetting.for_library_and_externalintegration(
                self._db, AuthenticationProvider.EXTERNAL_TYPE_REGULAR_EXPRESSION,
                library, auth_service).value
            if value:
                try:
                    re.compile(value)
                except Exception, e:
                    self._db.rollback()
                    return INVALID_EXTERNAL_TYPE_REGULAR_EXPRESSION

            # Check that the library's identifier restriction regular express is valid, it its set
            # and its a regular expression.
            identifier_restriction_type = ConfigurationSetting.for_library_and_externalintegration(
                self._db, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE,
                library, auth_service).value
            identifier_restriction = ConfigurationSetting.for_library_and_externalintegration(
                self._db, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION,
                library, auth_service).value
            if identifier_restriction and identifier_restriction_type == AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX:
                try:
                    re.compile(identifier_restriction)
                except Exception, e:
                    self._db.rollback()
                    return INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION

        if is_new:
            return Response(unicode(auth_service.id), 201)
        else:
            return Response(unicode(auth_service.id), 200)

    def patron_auth_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.PATRON_AUTH_GOAL
        )

    def sitewide_settings(self):
        return self._sitewide_settings_controller(Configuration)

    def sitewide_setting(self, key):
        if flask.request.method == "DELETE":
            self.require_system_admin()
            setting = ConfigurationSetting.sitewide(self._db, key)
            setting.value = None
            return Response(unicode(_("Deleted")), 200)

    def logging_services(self):
        detail = _("You tried to create a new logging service, but a logging service is already configured.")
        return self._manage_sitewide_service(
            ExternalIntegration.LOGGING_GOAL,
            [Loggly, SysLogger],
            'logging_services', detail
        )

    def logging_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.LOGGING_GOAL
        )

    def metadata_services(
            self, do_get=HTTP.debuggable_get, do_post=HTTP.debuggable_post,
            key=None
    ):
        self.require_system_admin()
        provider_apis = [NYTBestSellerAPI,
                         NoveListAPI,
                         MetadataWranglerOPDSLookup,
                        ]
        protocols = self._get_integration_protocols(provider_apis, protocol_name_attr="PROTOCOL")

        if flask.request.method == 'GET':
            metadata_services = self._get_integration_info(ExternalIntegration.METADATA_GOAL, protocols)
            return dict(
                metadata_services=metadata_services,
                protocols=protocols,
            )

        id = flask.request.form.get("id")

        protocol = flask.request.form.get("protocol")
        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

        is_new = False
        if id:
            service = get_one(self._db, ExternalIntegration, id=id, goal=ExternalIntegration.METADATA_GOAL)
            if not service:
                return MISSING_SERVICE
            if protocol != service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            service, is_new = self._create_integration(
                protocols, protocol, ExternalIntegration.METADATA_GOAL
            )
            if isinstance(service, ProblemDetail):
                return service

        name = flask.request.form.get("name")
        if name:
            if service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    self._db.rollback()
                    return INTEGRATION_NAME_ALREADY_IN_USE
            service.name = name

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        # Register this site with the Metadata Wrangler.
        if ((is_new or not service.password) and
            service.protocol == ExternalIntegration.METADATA_WRANGLER):

            problem_detail = self.sitewide_registration(
                service, do_get=do_get, do_post=do_post, key=key
            )
            if problem_detail:
                self._db.rollback()
                return problem_detail

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def metadata_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.METADATA_GOAL
        )

    def sitewide_registration(self, integration, do_get=HTTP.debuggable_get,
                              do_post=HTTP.debuggable_post, key=None
    ):
        """Performs a sitewide registration for a particular service, currently
        only the Metadata Wrangler.

        :return: A ProblemDetail or, if successful, None
        """
        self.require_system_admin()
        if not integration:
            return MISSING_SERVICE

        # Get the catalog for this service.
        try:
            response = do_get(integration.url)
        except Exception as e:
            return REMOTE_INTEGRATION_FAILED.detailed(e.message)

        if isinstance(response, ProblemDetail):
            return response

        content_type = response.headers.get('Content-Type')
        if content_type != 'application/opds+json':
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide a valid catalog.')
            )

        catalog = response.json()
        links = catalog.get('links', [])

        # Get the link for registration from the catalog.
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

        # Generate a public key for this website.
        if not key:
            key = RSA.generate(2048)
        encryptor = PKCS1_OAEP.new(key)
        public_key = key.publickey().exportKey()

        # Save the public key to the database before generating the public key document.
        public_key_setting = ConfigurationSetting.sitewide(self._db, Configuration.PUBLIC_KEY)
        public_key_setting.value = public_key
        self._db.commit()

        # If the integration has an existing shared_secret, use it to access the
        # server and update it.
        #
        # NOTE: This is no longer technically necessary since we prove
        # ownership with a signed JWT.
        headers = { 'Content-Type' : 'application/x-www-form-urlencoded' }
        if integration.password:
            token = base64.b64encode(integration.password.encode('utf-8'))
            headers['Authorization'] = 'Bearer ' + token

        # Get the public key document URL and register this server.
        try:
            body = self.sitewide_registration_document(key.exportKey())
            response = do_post(
                register_url, body, allowed_response_codes=['2xx'],
                headers=headers
            )
        except Exception as e:
            public_key_setting.value = None
            return REMOTE_INTEGRATION_FAILED.detailed(e.message)

        if isinstance(response, ProblemDetail):
            return response
        registration_info = response.json()
        shared_secret = registration_info.get('metadata', {}).get('shared_secret')

        if not shared_secret:
            public_key_setting.value = None
            return REMOTE_INTEGRATION_FAILED.detailed(
                _('The service did not provide registration information.')
            )

        public_key_setting.value = None
        shared_secret = encryptor.decrypt(base64.b64decode(shared_secret))
        integration.password = unicode(shared_secret)

    def sitewide_registration_document(self, private_key):
        """Generate the document to be sent as part of a sitewide registration
        request.

        :param private_key: An string containing an RSA private key,
            e.g. the output of RsaKey.exportKey()
        :return: A dictionary with keys 'url' and 'jwt'. 'url' is the URL to
            this site's public key document, and 'jwt' is a JSON Web Token
            proving control over that URL.
        """
        public_key_url = self.url_for('public_key_document')
        in_one_minute = datetime.utcnow() + timedelta(seconds=60)
        payload = {'exp': in_one_minute}
        token = jwt.encode(payload, private_key, algorithm='RS256')
        return dict(url=public_key_url, jwt=token)

    def analytics_services(self):
        self.require_system_admin()
        provider_apis = [GoogleAnalyticsProvider,
                         LocalAnalyticsProvider,
                        ]
        protocols = self._get_integration_protocols(provider_apis)

        if flask.request.method == 'GET':
            services = self._get_integration_info(ExternalIntegration.ANALYTICS_GOAL, protocols)
            return dict(
                analytics_services=services,
                protocols=protocols,
            )

        id = flask.request.form.get("id")

        protocol = flask.request.form.get("protocol")
        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

        is_new = False
        if id:
            service = get_one(self._db, ExternalIntegration, id=id, goal=ExternalIntegration.ANALYTICS_GOAL)
            if not service:
                return MISSING_SERVICE
            if protocol != service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            service, is_new = self._create_integration(
                protocols, protocol, ExternalIntegration.ANALYTICS_GOAL
            )
            if isinstance(service, ProblemDetail):
                return service

        name = flask.request.form.get("name")
        if name:
            if service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    self._db.rollback()
                    return INTEGRATION_NAME_ALREADY_IN_USE
            service.name = name

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def analytics_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.ANALYTICS_GOAL
        )

    def cdn_services(self):
        self.require_system_admin()
        protocols = [
            {
                "name": ExternalIntegration.CDN,
                "sitewide": True,
                "settings": [
                    { "key": ExternalIntegration.URL, "label": _("CDN URL") },
                    { "key": Configuration.CDN_MIRRORED_DOMAIN_KEY, "label": _("Mirrored domain") },
                ],
            }
        ]

        if flask.request.method == 'GET':
            services = self._get_integration_info(ExternalIntegration.CDN_GOAL, protocols)
            return dict(
                cdn_services=services,
                protocols=protocols,
            )

        id = flask.request.form.get("id")

        protocol = flask.request.form.get("protocol")
        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

        is_new = False
        if id:
            service = get_one(self._db, ExternalIntegration, id=id, goal=ExternalIntegration.CDN_GOAL)
            if not service:
                return MISSING_SERVICE
            if protocol != service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            service, is_new = self._create_integration(
                protocols, protocol, ExternalIntegration.CDN_GOAL
            )
            if isinstance(service, ProblemDetail):
                return service

        name = flask.request.form.get("name")
        if name:
            if service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    self._db.rollback()
                    return INTEGRATION_NAME_ALREADY_IN_USE
            service.name = name

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def cdn_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.CDN_GOAL
        )

    def _manage_sitewide_service(
            self, goal, provider_apis, service_key_name,
            multiple_sitewide_services_detail, protocol_name_attr='NAME'
    ):
        self.require_system_admin()
        protocols = self._get_integration_protocols(provider_apis, protocol_name_attr=protocol_name_attr)

        if flask.request.method == 'GET':
            services = self._get_integration_info(goal, protocols)
            return {
                service_key_name : services,
                'protocols' : protocols,
            }

        id = flask.request.form.get("id")

        protocol = flask.request.form.get("protocol")
        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

        is_new = False
        if id:
            service = get_one(self._db, ExternalIntegration, id=id, goal=goal)
            if not service:
                return MISSING_SERVICE
            if protocol != service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            if protocol:
                service, is_new = get_one_or_create(
                    self._db, ExternalIntegration, protocol=protocol,
                    goal=goal
                )
                if not is_new:
                    self._db.rollback()
                    return MULTIPLE_SITEWIDE_SERVICES.detailed(
                        multiple_sitewide_services_detail
                    )
            else:
                return NO_PROTOCOL_FOR_NEW_SERVICE

        name = flask.request.form.get("name")
        if name:
            if service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    self._db.rollback()
                    return INTEGRATION_NAME_ALREADY_IN_USE
            service.name = name

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def search_services(self):
        detail = _("You tried to create a new search service, but a search service is already configured.")
        return self._manage_sitewide_service(
            ExternalIntegration.SEARCH_GOAL, [ExternalSearchIndex],
            'search_services', detail
        )

    def search_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.SEARCH_GOAL
        )

    def storage_services(self):
        detail = _("You tried to create a new storage service, but a storage service is already configured.")
        return self._manage_sitewide_service(
            ExternalIntegration.STORAGE_GOAL,
            MirrorUploader.IMPLEMENTATION_REGISTRY.values(),
            'storage_services', detail
        )

    def storage_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.STORAGE_GOAL
        )

    def discovery_services(self):
        self.require_system_admin()
        protocols = [
            {
                "name": ExternalIntegration.OPDS_REGISTRATION,
                "sitewide": True,
                "settings": [
                    { "key": ExternalIntegration.URL, "label": _("URL") },
                ],
                "supports_registration": True,
            }
        ]

        if flask.request.method == 'GET':
            registries = self._db.query(ExternalIntegration).filter(ExternalIntegration.goal==ExternalIntegration.DISCOVERY_GOAL)
            if registries.count() == 0:
                # Set up the default library registry if one doesn't exist yet.
                default, ignore = get_one_or_create(
                    self._db, ExternalIntegration,
                    goal=ExternalIntegration.DISCOVERY_GOAL,
                    protocol=ExternalIntegration.OPDS_REGISTRATION,
                    name="Library Simplified Registry")
                default.url = "https://libraryregistry.librarysimplified.org"

            services = self._get_integration_info(ExternalIntegration.DISCOVERY_GOAL, protocols)
            return dict(
                discovery_services=services,
                protocols=protocols,
            )

        id = flask.request.form.get("id")

        protocol = flask.request.form.get("protocol")
        if protocol and protocol not in [p.get("name") for p in protocols]:
            return UNKNOWN_PROTOCOL

        is_new = False
        if id:
            service = get_one(self._db, ExternalIntegration, id=id, goal=ExternalIntegration.DISCOVERY_GOAL)
            if not service:
                return MISSING_SERVICE
            if protocol != service.protocol:
                return CANNOT_CHANGE_PROTOCOL
        else:
            service, is_new = self._create_integration(
                protocols, protocol, ExternalIntegration.DISCOVERY_GOAL
            )
            if isinstance(service, ProblemDetail):
                return service

        name = flask.request.form.get("name")
        if name:
            if service.name != name:
                service_with_name = get_one(self._db, ExternalIntegration, name=name)
                if service_with_name:
                    self._db.rollback()
                    return INTEGRATION_NAME_ALREADY_IN_USE
            service.name = name

        [protocol] = [p for p in protocols if p.get("name") == protocol]
        result = self._set_integration_settings_and_libraries(service, protocol)
        if isinstance(result, ProblemDetail):
            return result

        if is_new:
            return Response(unicode(service.id), 201)
        else:
            return Response(unicode(service.id), 200)

    def discovery_service(self, service_id):
        return self._delete_integration(
            service_id, ExternalIntegration.DISCOVERY_GOAL
        )

    def discovery_service_library_registrations(self, do_get=HTTP.debuggable_get,
                              do_post=HTTP.debuggable_post, key=None):
        LIBRARY_REGISTRATION_STATUS = u"library-registration-status"
        SUCCESS = u"success"
        FAILURE = u"failure"

        self.require_system_admin()

        if flask.request.method == "GET":
            services = []
            for service in self._db.query(ExternalIntegration).filter(
                ExternalIntegration.goal==ExternalIntegration.DISCOVERY_GOAL):

                libraries = []
                for library in service.libraries:
                    library_info = dict(short_name=library.short_name)
                    status = ConfigurationSetting.for_library_and_externalintegration(
                        self._db, LIBRARY_REGISTRATION_STATUS, library, service).value
                    if status:
                        library_info["status"] = status
                        libraries.append(library_info)

                services.append(
                    dict(
                        id=service.id,
                        libraries=libraries,
                    )
                )

            return dict(library_registrations=services)

        if flask.request.method == "POST":

            integration_id = flask.request.form.get("integration_id")
            library_short_name = flask.request.form.get("library_short_name")

            integration = get_one(self._db, ExternalIntegration,
                                  goal=ExternalIntegration.DISCOVERY_GOAL,
                                  id=integration_id)
            if not integration:
                return MISSING_SERVICE

            library = get_one(self._db, Library, short_name=library_short_name)
            if not library:
                return NO_SUCH_LIBRARY

            integration.libraries += [library]
            status = ConfigurationSetting.for_library_and_externalintegration(
                self._db, LIBRARY_REGISTRATION_STATUS, library, integration)
            status.value = FAILURE
            registered = self._register_library(integration.url, library, integration, do_get=do_get, do_post=do_post, key=key)
            if isinstance(registered, ProblemDetail):
                return registered
            status.value = SUCCESS

        return Response(unicode(_("Success")), 200)

    def _decrypt_shared_secret(self, encryptor, shared_secret):
        """Attempt to decrypt an encrypted shared secret.

        :return: The decrypted shared secret, or a ProblemDetail if
        it could not be decrypted.
        """
        try:
            shared_secret = encryptor.decrypt(base64.b64decode(shared_secret))
        except ValueError, e:
            return SHARED_SECRET_DECRYPTION_ERROR.detailed(
                _("Could not decrypt shared secret %s") % shared_secret
            )
        return shared_secret

    def _register_library(self, catalog_url, library, integration,
                          do_get=HTTP.debuggable_get, do_post=HTTP.debuggable_post, key=None):
        """Attempt to register a library with an external service,
        such as a library registry or a shared collection on another
        circulation manager.

        Note: this method does a commit in order to set a public
        key for the external service to request.
        """
        response = do_get(catalog_url)
        if isinstance(response, ProblemDetail):
            return response
        type = response.headers.get("Content-Type")
        if type and type.startswith('application/opds+json'):
            # This is an OPDS 2 catalog.
            catalog = json.loads(response.content)
            links = catalog.get("links", [])
            vendor_id = catalog.get("metadata", {}).get("adobe_vendor_id")
        elif type and type.startswith("application/atom+xml;profile=opds-catalog"):
            # This is an OPDS 1 feed.
            feed = feedparser.parse(response.content)
            links = feed.get("feed", {}).get("links", [])
            vendor_id = None
        else:
            return REMOTE_INTEGRATION_FAILED.detailed(_("The service at %(url)s did not return OPDS.", url=catalog_url))

        register_url = None
        for link in links:
            if link.get("rel") == "register":
                register_url = link.get("href")
                break
        if not register_url:
            return REMOTE_INTEGRATION_FAILED.detailed(_("The service at %(url)s did not provide a register link.", url=catalog_url))

        # Store the vendor id as a ConfigurationSetting on the registry.
        if vendor_id:
            ConfigurationSetting.for_externalintegration(
                AuthdataUtility.VENDOR_ID_KEY, integration).value = vendor_id

        # Generate a public key for the library.
        if not key:
            key = RSA.generate(2048)
        public_key = key.publickey().exportKey()
        encryptor = PKCS1_OAEP.new(key)

        ConfigurationSetting.for_library(Configuration.PUBLIC_KEY, library).value = public_key
        # Commit so the public key will be there when the registry gets the
        # OPDS Authentication document.
        self._db.commit()

        auth_document_url = self.url_for(
            "authentication_document",
            library_short_name=library.short_name
        )
        payload = dict(url=auth_document_url)

        # Find the email address the administrator should use if they notice
        # a problem with the way the library is using an integration.
        contact = Configuration.configuration_contact_uri(library)
        if contact:
            payload['contact'] = contact
        # Allow 401 so we can provide a more useful error message.
        response = do_post(
            register_url, payload, timeout=60,
            allowed_response_codes=["2xx", "3xx", "401"],
        )
        if isinstance(response, ProblemDetail):
            return response
        if response.status_code == 401:
            if response.headers.get("Content-Type") == PROBLEM_DETAIL_JSON_MEDIA_TYPE:
                problem = json.loads(response.content)
                return INTEGRATION_ERROR.detailed(
                    _("Remote service returned: \"%(problem)s\"", problem=problem.get("detail")))
            else:
                return INTEGRATION_ERROR.detailed(
                    _("Remote service returned: \"%(problem)s\"", problem=response.content))

        catalog = json.loads(response.content)

        # Since we generated a public key, the catalog should provide credentials
        # for future authenticated communication, e.g. through Short Client Tokens
        # or authenticated API requests.
        short_name = catalog.get("metadata", {}).get("short_name")
        shared_secret = catalog.get("metadata", {}).get("shared_secret")

        if short_name:
            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.USERNAME, library, integration
            ).value = short_name
        if shared_secret:
            shared_secret = self._decrypt_shared_secret(encryptor, shared_secret)
            if isinstance(shared_secret, ProblemDetail):
                return shared_secret

            ConfigurationSetting.for_library_and_externalintegration(
                self._db, ExternalIntegration.PASSWORD, library, integration
            ).value = shared_secret
        integration.libraries += [library]

        # We're done with the key, so remove the setting.
        ConfigurationSetting.for_library(Configuration.PUBLIC_KEY, library).value = None
        return True
