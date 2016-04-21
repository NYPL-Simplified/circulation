from nose.tools import set_trace
import logging
import sys
import os
import base64

import flask
from flask import (
    Response,
    redirect,
)

from core.model import (
    get_one,
    get_one_or_create,
    Admin,
    Classification,
    DataSource,
    Hyperlink,
    PresentationCalculationPolicy,
    Subject,
)
from core.util.problem_detail import ProblemDetail
from api.problem_details import *

from config import (
    Configuration, 
    CannotLoadConfiguration
)

from oauth import GoogleAuthService

from api.controller import CirculationManagerController
from api.coverage import MetadataWranglerCoverageProvider
from core.app_server import entry_response
from core.app_server import (
    entry_response, 
    feed_response,
    load_pagination_from_request
)
from core.opds import AcquisitionFeed
from opds import AdminAnnotator, AdminFeed
from collections import Counter


def setup_admin_controllers(manager):
    """Set up all the controllers that will be used by the admin parts of the web app."""
    if not manager.testing:
        try:
            manager.config = Configuration.load()
        except CannotLoadConfiguration, e:
            self.log.error("Could not load configuration file: %s" % e)
            sys.exit()

    manager.admin_work_controller = WorkController(manager)
    manager.admin_sign_in_controller = SignInController(manager)
    manager.admin_feed_controller = FeedController(manager)


class AdminController(object):

    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db
        self.url_for = self.manager.url_for
        self.cdn_url_for = self.manager.cdn_url_for

    @property
    def google(self):
        return GoogleAuthService.from_environment(
            self.url_for('google_auth_callback'), test_mode=self.manager.testing
        )

    def authenticated_admin_from_request(self):
        """Returns an authenticated admin or begins the Google OAuth flow"""

        access_token = flask.session.get("admin_access_token")
        if access_token:
            admin = get_one(self._db, Admin, access_token=access_token)
            if admin and self.google.active_credentials(admin):
                return admin
        return INVALID_ADMIN_CREDENTIALS

    def authenticated_admin(self, admin_details):
        """Creates or updates an admin with the given details"""

        admin, ignore = get_one_or_create(
            self._db, Admin, email=admin_details['email']
        )
        admin.update_credentials(
            self._db, admin_details['access_token'], admin_details['credentials']
        )
        return admin

    def check_csrf_token(self):
        """Verifies that the provided CSRF token is valid."""
        token = self.get_csrf_token()
        if not token or token != flask.request.form.get("csrf_token"):
            return INVALID_CSRF_TOKEN
        return token

    def get_csrf_token(self):
        """Returns the CSRF token for the current session."""
        return flask.session.get("csrf_token")

class SignInController(AdminController):

    ERROR_RESPONSE_TEMPLATE = """<!DOCTYPE HTML>
<html lang="en">
<head><meta charset="utf8"></head>
</body>
<p><strong>%(status_code)d ERROR:</strong> %(message)s</p>
</body>
</html>"""

    def sign_in(self):
        """Redirects admin if they're signed in."""
        admin = self.authenticated_admin_from_request()

        if isinstance(admin, ProblemDetail):
            redirect_url = flask.request.args.get("redirect")
            return redirect(self.google.auth_uri(redirect_url), Response=Response)
        elif admin:
            return redirect(flask.request.args.get("redirect"), Response=Response)

    def redirect_after_sign_in(self):
        """Uses the Google OAuth client to determine admin details upon
        callback. Barring error, redirects to the provided redirect url.."""

        admin_details, redirect_url = self.google.callback(flask.request.args)
        if isinstance(admin_details, ProblemDetail):
            return self.error_response(admin_details)

        if not self.staff_email(admin_details['email']):
            return self.error_response(INVALID_ADMIN_CREDENTIALS)
        else:
            admin = self.authenticated_admin(admin_details)
            flask.session["admin_access_token"] = admin_details.get("access_token")
            flask.session["csrf_token"] = base64.b64encode(os.urandom(24))
            return redirect(redirect_url, Response=Response)
    
    def staff_email(self, email):
        """Checks the domain of an email address against the admin-authorized
        domain"""

        staff_domain = Configuration.policy(
            Configuration.ADMIN_AUTH_DOMAIN, required=True
        )
        domain = email[email.index('@')+1:]
        return domain.lower() == staff_domain.lower()

    def error_response(self, problem_detail):
        """Returns a problem detail as an HTML response"""
        html = self.ERROR_RESPONSE_TEMPLATE % dict(
            status_code=problem_detail.status_code,
            message=problem_detail.detail
        )
        return Response(html, problem_detail.status_code)

class WorkController(CirculationManagerController):

    def details(self, data_source, identifier):
        """Return an OPDS entry with detailed information for admins.
        
        This includes relevant links for editing the book.
        """

        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        work = pool.work

        annotator = AdminAnnotator(self.circulation)
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator)
        )
        
    def complaints(self, data_source, identifier):
        """Return detailed complaint information for admins."""
        
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        counter = self._count_complaints_for_licensepool(pool)
        response = dict({
            "book": { 
                "data_source": data_source,
                "identifier": identifier
            },
            "complaints": counter
        })
        
        return response

    def edit(self, data_source, identifier):
        """Edit a work's metadata."""

        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        work = pool.work
        changed = False

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        new_title = flask.request.form.get("title")
        if new_title and work.title != new_title:
            work.primary_edition.title = unicode(new_title)
            changed = True

        new_audience = flask.request.form.get("audience")
        if new_audience != work.audience:
            # Delete all previous staff classifications
            old_classifications = self._db.query(Classification).filter(
                Classification.identifier==work.primary_edition.primary_identifier,
                Classification.data_source==staff_data_source,
            )
            for c in old_classifications:
                if c.subject.type == Subject.FREEFORM_AUDIENCE:
                    self._db.delete(c)

            # Create a new classification with a high weight
            work.primary_edition.primary_identifier.classify(
                data_source=staff_data_source,
                subject_type=Subject.FREEFORM_AUDIENCE,
                subject_identifier=new_audience,
                weight=100000,
            )
            changed = True

        new_summary = flask.request.form.get("summary")
        if new_summary != work.summary_text:
            (link, is_new) =  work.primary_edition.primary_identifier.add_link(
                Hyperlink.DESCRIPTION, None, 
                staff_data_source, content=new_summary)
            work.set_summary(link.resource)
            changed = True

        if changed:
            # Even if the presentation doesn't visibly change, we want
            # to regenerate the OPDS entries and update the search
            # index for the work, because that might be the 'real'
            # problem the user is trying to fix.
            policy = PresentationCalculationPolicy(
                classify=True,
                regenerate_opds_entries=True,
                update_search_index=True,
            )
            work.calculate_presentation(policy=policy)
        return Response("", 200)

    def suppress(self, data_source, identifier):
        """Suppress the license pool associated with a book."""
        
        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            # Something went wrong.
            return pool
    
        pool.suppressed = True
        return Response("", 200)

    def unsuppress(self, data_source, identifier):
        """Unsuppress the license pool associated with a book."""
        
        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            # Something went wrong.
            return pool
    
        pool.suppressed = False
        return Response("", 200)

    def refresh_metadata(self, data_source, identifier, provider=None):
        """Refresh the metadata for a book from the content server"""
        if not provider:
            provider = MetadataWranglerCoverageProvider(self._db)

        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        try:
            record = provider.ensure_coverage(pool.identifier, force=True)
        except Exception:
            # The coverage provider may raise an HTTPIntegrationException.
            return REMOTE_INTEGRATION_FAILED

        if record.exception:
            # There was a coverage failure.
            if (isinstance(record.exception, int)
                and record.exception in [201, 202]):
                # A 201/202 error means it's never looked up this work before
                # so it's started the resolution process or looking for sources.
                return METADATA_REFRESH_PENDING
            # Otherwise, it just doesn't know anything.
            return METADATA_REFRESH_FAILURE

        return Response("", 200)

    def resolve_complaints(self, data_source, identifier):
        """Resolve all complaints for a particular license pool and complaint type."""

        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        work = pool.work
        resolved = False
        found = False

        type = flask.request.form.get("type")
        if type:
            for complaint in pool.complaints:
                if complaint.type == type:
                    found = True
                    if complaint.resolved == None:
                        complaint.resolve()
                        resolved = True

        if not found:
            return UNRECOGNIZED_COMPLAINT
        elif not resolved:
            return COMPLAINT_ALREADY_RESOLVED
        return Response("", 200)

    def _count_complaints_for_licensepool(self, pool):
        complaint_types = [complaint.type for complaint in pool.complaints if complaint.resolved == None]
        return Counter(complaint_types)

    
class FeedController(CirculationManagerController):

    def complaints(self):
        this_url = self.url_for('complaints')
        annotator = AdminAnnotator(self.circulation)
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        opds_feed = AdminFeed.complaints(
            _db=self._db, title="Complaints",
            url=this_url, annotator=annotator,
            pagination=pagination
        )
        return feed_response(opds_feed)    

    def suppressed(self):
        this_url = self.url_for('suppressed')
        annotator = AdminAnnotator(self.circulation)
        pagination = load_pagination_from_request()
        if isinstance(pagination, ProblemDetail):
            return pagination
        opds_feed = AdminFeed.suppressed(
            _db=self._db, title="Hidden Books",
            url=this_url, annotator=annotator,
            pagination=pagination
        )
        return feed_response(opds_feed)
