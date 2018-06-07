from nose.tools import set_trace
from functools import wraps
import flask
from flask import (
    Response,
    redirect,
    make_response
)
import os

from api.app import app
from api.config import Configuration

from core.util.problem_detail import ProblemDetail
from core.app_server import returns_problem_detail
from core.model import (
    ConfigurationSetting,
    Library,
)

from controller import setup_admin_controllers
from templates import (
    admin_sign_in_again as sign_in_again_template,
)
from api.routes import (
    has_library,
    library_route,
)

import csv, codecs, cStringIO
from StringIO import StringIO
import urllib
from datetime import timedelta

# An admin's session will expire after this amount of time and
# the admin will have to log in again.
app.permanent_session_lifetime = timedelta(hours=9)

@app.before_first_request
def setup_admin(_db=None):
    if getattr(app, 'manager', None) is not None:
        setup_admin_controllers(app.manager)
    _db = _db or app._db
    # The secret key is used for signing cookies for admin login
    app.secret_key = ConfigurationSetting.sitewide_secret(
        _db, Configuration.SECRET_KEY
    )

def allows_admin_auth_setup(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        setting_up = (app.manager.admin_sign_in_controller.admin_auth_providers == [])
        return f(*args, setting_up=setting_up, **kwargs)
    return decorated

def requires_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'setting_up' in kwargs:
            # If the function also requires a CSRF token,
            # setting_up needs to stay in the arguments for
            # the next decorator. Otherwise, it should be
            # removed before the route function.
            if f.func_dict.get("requires_csrf_token"):
                setting_up = kwargs.get('setting_up')
            else:
                setting_up = kwargs.pop('setting_up')
        else:
            setting_up = False

        if not setting_up:
            admin = app.manager.admin_sign_in_controller.authenticated_admin_from_request()
            if isinstance(admin, ProblemDetail):
                return app.manager.admin_sign_in_controller.error_response(admin)
            elif isinstance(admin, Response):
                return admin

        return f(*args, **kwargs)
    return decorated

def requires_csrf_token(f):
    f.func_dict["requires_csrf_token"] = True
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'setting_up' in kwargs:
            setting_up = kwargs.pop('setting_up')
        else:
            setting_up = False
        if not setting_up and flask.request.method in ["POST", "PUT", "DELETE"]:
            token = app.manager.admin_sign_in_controller.check_csrf_token()
            if isinstance(token, ProblemDetail):
                return token
        return f(*args, **kwargs)
    return decorated

def returns_json_or_response_or_problem_detail(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        v = f(*args, **kwargs)
        if isinstance(v, ProblemDetail):
            return v.response
        if isinstance(v, Response):
            return v
        return flask.jsonify(**v)
    return decorated

@app.route('/admin/GoogleAuth/callback')
@returns_problem_detail
def google_auth_callback():
    return app.manager.admin_sign_in_controller.redirect_after_google_sign_in()

@app.route("/admin/sign_in_with_password", methods=["POST"])
@returns_problem_detail
def password_auth():
    return app.manager.admin_sign_in_controller.password_sign_in()

@app.route('/admin/sign_in')
@returns_problem_detail
def admin_sign_in():
    return app.manager.admin_sign_in_controller.sign_in()

@app.route('/admin/sign_out')
@returns_problem_detail
@requires_admin
def admin_sign_out():
    return app.manager.admin_sign_in_controller.sign_out()

@app.route('/admin/change_password', methods=["POST"])
@returns_problem_detail
@requires_admin
def admin_change_password():
    return app.manager.admin_sign_in_controller.change_password()

@library_route('/admin/works/<identifier_type>/<path:identifier>', methods=['GET'])
@has_library
@returns_problem_detail
@requires_admin
def work_details(identifier_type, identifier):
    return app.manager.admin_work_controller.details(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/classifications', methods=['GET'])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
def work_classifications(identifier_type, identifier):
    return app.manager.admin_work_controller.classifications(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/preview_book_cover', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
def work_preview_book_cover(identifier_type, identifier):
    return app.manager.admin_work_controller.preview_book_cover(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/change_book_cover', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
def work_change_book_cover(identifier_type, identifier):
    return app.manager.admin_work_controller.change_book_cover(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/complaints', methods=['GET'])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
def work_complaints(identifier_type, identifier):
    return app.manager.admin_work_controller.complaints(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/lists', methods=['GET', 'POST'])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def work_custom_lists(identifier_type, identifier):
    return app.manager.admin_work_controller.custom_lists(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/edit', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def edit(identifier_type, identifier):
    return app.manager.admin_work_controller.edit(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/suppress', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def suppress(identifier_type, identifier):
    return app.manager.admin_work_controller.suppress(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/unsuppress', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def unsuppress(identifier_type, identifier):
    return app.manager.admin_work_controller.unsuppress(identifier_type, identifier)

@library_route('/works/<identifier_type>/<path:identifier>/refresh', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def refresh(identifier_type, identifier):
    return app.manager.admin_work_controller.refresh_metadata(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/resolve_complaints', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def resolve_complaints(identifier_type, identifier):
    return app.manager.admin_work_controller.resolve_complaints(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/edit_classifications', methods=['POST'])
@has_library
@returns_problem_detail
@requires_admin
@requires_csrf_token
def edit_classifications(identifier_type, identifier):
    return app.manager.admin_work_controller.edit_classifications(identifier_type, identifier)

@app.route('/admin/roles')
@returns_json_or_response_or_problem_detail
def roles():
    return app.manager.admin_work_controller.roles()

@app.route('/admin/languages')
@returns_json_or_response_or_problem_detail
def languages():
    return app.manager.admin_work_controller.languages()

@app.route('/admin/media')
@returns_json_or_response_or_problem_detail
def media():
    return app.manager.admin_work_controller.media()

@app.route('/admin/rights_status')
@returns_json_or_response_or_problem_detail
def rights_status():
    return app.manager.admin_work_controller.rights_status()

@library_route('/admin/complaints')
@has_library
@returns_problem_detail
@requires_admin
def complaints():
    return app.manager.admin_feed_controller.complaints()

@library_route('/admin/suppressed')
@has_library
@returns_problem_detail
@requires_admin
def suppressed():
    """Returns a feed of suppressed works."""
    return app.manager.admin_feed_controller.suppressed()

@app.route('/admin/genres')
@returns_json_or_response_or_problem_detail
@requires_admin
def genres():
    """Returns a JSON representation of complete genre tree."""
    return app.manager.admin_feed_controller.genres()

@app.route('/admin/bulk_circulation_events')
@returns_problem_detail
@requires_admin
def bulk_circulation_events():
    """Returns a CSV representation of all circulation events with optional
    start and end times."""
    data, date = app.manager.admin_dashboard_controller.bulk_circulation_events()
    if isinstance(data, ProblemDetail):
        return data

    class UnicodeWriter:
        """
        A CSV writer for Unicode data.
        """

        def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
            # Redirect output to a queue
            self.queue = StringIO()
            self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
            self.stream = f
            self.encoder = codecs.getincrementalencoder(encoding)()

        def writerow(self, row):
            self.writer.writerow(
                [s.encode("utf-8") if hasattr(s, "encode") else "" for s in row]
            )
            # Fetch UTF-8 output from the queue ...
            data = self.queue.getvalue()
            data = data.decode("utf-8")
            # ... and reencode it into the target encoding
            data = self.encoder.encode(data)
            # write to the target stream
            self.stream.write(data)
            # empty queue
            self.queue.truncate(0)

        def writerows(self, rows):
            for row in rows:
                self.writerow(row)

    output = StringIO()
    writer = UnicodeWriter(output)
    writer.writerows(data)
    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = "attachment; filename=circulation_events_" + date + ".csv"
    response.headers["Content-type"] = "text/csv"
    return response

@library_route('/admin/circulation_events')
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
def circulation_events():
    """Returns a JSON representation of the most recent circulation events."""
    return app.manager.admin_dashboard_controller.circulation_events()

@app.route('/admin/stats')
@returns_json_or_response_or_problem_detail
@requires_admin
def stats():
    return app.manager.admin_dashboard_controller.stats()

@app.route('/admin/libraries', methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def libraries():
    return app.manager.admin_settings_controller.libraries()

@app.route("/admin/library/<library_uuid>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def library(library_uuid):
    return app.manager.admin_settings_controller.library(library_uuid)

@app.route("/admin/collections", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def collections():
    return app.manager.admin_settings_controller.collections()

@app.route("/admin/collection/<collection_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def collection(collection_id):
    return app.manager.admin_settings_controller.collection(collection_id)

@app.route("/admin/collection_library_registrations", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def collection_library_registrations():
    return app.manager.admin_settings_controller.collection_library_registrations()

@app.route("/admin/admin_auth_services", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def admin_auth_services():
    return app.manager.admin_settings_controller.admin_auth_services()

@app.route("/admin/admin_auth_service/<protocol>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def admin_auth_service(protocol):
    return app.manager.admin_settings_controller.admin_auth_service(protocol)

@app.route("/admin/individual_admins", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@allows_admin_auth_setup
@requires_admin
@requires_csrf_token
def individual_admins():
    return app.manager.admin_settings_controller.individual_admins()

@app.route("/admin/individual_admin/<email>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def individual_admin(email):
    return app.manager.admin_settings_controller.individual_admin(email)

@app.route("/admin/patron_auth_services", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def patron_auth_services():
    return app.manager.admin_settings_controller.patron_auth_services()

@app.route("/admin/patron_auth_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def patron_auth_service(service_id):
    return app.manager.admin_settings_controller.patron_auth_service(service_id)

@app.route("/admin/metadata_services", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def metadata_services():
    return app.manager.admin_settings_controller.metadata_services()

@app.route("/admin/metadata_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def metadata_service(service_id):
    return app.manager.admin_settings_controller.metadata_service(service_id)

@app.route("/admin/analytics_services", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def analytics_services():
    return app.manager.admin_settings_controller.analytics_services()

@app.route("/admin/analytics_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def analytics_service(service_id):
    return app.manager.admin_settings_controller.analytics_service(service_id)

@app.route("/admin/cdn_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def cdn_services():
    return app.manager.admin_settings_controller.cdn_services()

@app.route("/admin/cdn_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def cdn_service(service_id):
    return app.manager.admin_settings_controller.cdn_service(service_id)

@app.route("/admin/search_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def search_services():
    return app.manager.admin_settings_controller.search_services()

@app.route("/admin/search_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def search_service(service_id):
    return app.manager.admin_settings_controller.search_service(service_id)

@app.route("/admin/storage_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def storage_services():
    return app.manager.admin_settings_controller.storage_services()

@app.route("/admin/storage_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def storage_service(service_id):
    return app.manager.admin_settings_controller.storage_service(service_id)

@app.route("/admin/discovery_services", methods=["GET", "POST"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def discovery_services():
    return app.manager.admin_settings_controller.discovery_services()

@app.route("/admin/discovery_service/<service_id>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def discovery_service(service_id):
    return app.manager.admin_settings_controller.discovery_service(service_id)

@app.route("/admin/sitewide_settings", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def sitewide_settings():
    return app.manager.admin_settings_controller.sitewide_settings()

@app.route("/admin/sitewide_setting/<key>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def sitewide_setting(key):
    return app.manager.admin_settings_controller.sitewide_setting(key)

@app.route("/admin/logging_services", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def logging_services():
    return app.manager.admin_settings_controller.logging_services()

@app.route("/admin/logging_service/<key>", methods=["DELETE"])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def logging_service(key):
    return app.manager.admin_settings_controller.logging_service(key)

@app.route("/admin/discovery_service_library_registrations", methods=['GET', 'POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def discovery_service_library_registrations():
    return app.manager.admin_settings_controller.discovery_service_library_registrations()

@library_route("/admin/custom_lists", methods=["GET", "POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_lists():
    return app.manager.admin_custom_lists_controller.custom_lists()

@library_route("/admin/custom_list/<list_id>", methods=["GET", "POST", "DELETE"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def custom_list(list_id):
    return app.manager.admin_custom_lists_controller.custom_list(list_id)

@library_route("/admin/lanes", methods=["GET", "POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lanes():
    return app.manager.admin_lanes_controller.lanes()

@library_route("/admin/lane/<lane_identifier>", methods=["DELETE"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lane(lane_identifier):
    return app.manager.admin_lanes_controller.lane(lane_identifier)

@library_route("/admin/lane/<lane_identifier>/show", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lane_show(lane_identifier):
    return app.manager.admin_lanes_controller.show_lane(lane_identifier)

@library_route("/admin/lane/<lane_identifier>/hide", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def lane_hide(lane_identifier):
    return app.manager.admin_lanes_controller.hide_lane(lane_identifier)

@library_route("/admin/lanes/reset", methods=["POST"])
@has_library
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def reset_lanes():
    return app.manager.admin_lanes_controller.reset()

@app.route('/admin/sitewide_registration', methods=['POST'])
@returns_json_or_response_or_problem_detail
@requires_admin
@requires_csrf_token
def sitewide_registration():
    return app.manager.admin_settings_controller.sitewide_registration()

@app.route('/admin/sign_in_again')
def admin_sign_in_again():
    """Allows an  admin with expired credentials to sign back in
    from a new browser tab so they won't lose changes.
    """
    admin = app.manager.admin_sign_in_controller.authenticated_admin_from_request()
    csrf_token = app.manager.admin_sign_in_controller.get_csrf_token()
    if isinstance(admin, ProblemDetail) or csrf_token is None or isinstance(csrf_token, ProblemDetail):
        redirect_url = flask.request.url
        return redirect(app.manager.url_for('admin_sign_in', redirect=redirect_url))
    return flask.render_template_string(sign_in_again_template)

@app.route('/admin/web/', strict_slashes=False)
@app.route('/admin/web/collection/<path:collection>/book/<path:book>')
@app.route('/admin/web/collection/<path:collection>')
@app.route('/admin/web/book/<path:book>')
@app.route('/admin/web/<path:etc>') # catchall for single-page URLs
def admin_view(collection=None, book=None, etc=None, **kwargs):
    return app.manager.admin_view_controller(collection, book, path=etc)

@app.route('/admin/', strict_slashes=False)
def admin_base(**kwargs):
    return redirect(app.manager.url_for('admin_view'))

@app.route('/admin/static/circulation-web.js')
@returns_problem_detail
def admin_js():
    directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "node_modules", "simplified-circulation-web", "dist")
    cache_timeout = ConfigurationSetting.sitewide(
        app._db, Configuration.STATIC_FILE_CACHE_TIME
    ).int_value
    return flask.send_from_directory(directory, "circulation-web.js", cache_timeout=cache_timeout)

@app.route('/admin/static/circulation-web.css')
@returns_problem_detail
def admin_css():
    directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "node_modules", "simplified-circulation-web", "dist")
    cache_timeout = ConfigurationSetting.sitewide(
        app._db, Configuration.STATIC_FILE_CACHE_TIME
    ).int_value
    return flask.send_from_directory(directory, "circulation-web.css", cache_timeout=cache_timeout)
