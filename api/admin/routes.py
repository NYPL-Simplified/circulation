from nose.tools import set_trace
from functools import wraps
import flask
from flask import (
    Response,
    redirect,
    make_response
)
import os

from api.app import app, _db
from api.config import Configuration

from core.util.problem_detail import ProblemDetail
from core.app_server import returns_problem_detail
from core.model import (
    ConfigurationSetting,
    Library,
)

from controller import setup_admin_controllers
from templates import (
    admin as admin_template,
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

# The secret key is used for signing cookies for admin login
app.secret_key = ConfigurationSetting.sitewide_secret(
    _db, Configuration.SECRET_KEY
)

# An admin's session will expire after this amount of time and
# the admin will have to log in again.
app.permanent_session_lifetime = timedelta(hours=9)

@app.before_first_request
def setup_admin():
    if getattr(app, 'manager', None) is not None:
        setup_admin_controllers(app.manager)

def allows_admin_auth_setup(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        setting_up = (app.manager.admin_sign_in_controller.auth == None)
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

@app.route('/admin/GoogleAuth/callback')
@returns_problem_detail
def google_auth_callback():
    return app.manager.admin_sign_in_controller.redirect_after_google_sign_in()

@app.route("/admin/sign_in_with_password", methods=["GET", "POST"])
@returns_problem_detail
def password_auth():
    return app.manager.admin_sign_in_controller.password_sign_in()

@app.route('/admin/sign_in')
@returns_problem_detail
def admin_sign_in():
    return app.manager.admin_sign_in_controller.sign_in()

@library_route('/admin/works/<identifier_type>/<path:identifier>', methods=['GET'])
@has_library
@returns_problem_detail
@requires_admin
def work_details(identifier_type, identifier):
    return app.manager.admin_work_controller.details(identifier_type, identifier)

@library_route('/admin/works/<identifier_type>/<path:identifier>/classifications', methods=['GET'])
@has_library
@returns_problem_detail
@requires_admin
def work_classifications(identifier_type, identifier):
    data = app.manager.admin_work_controller.classifications(identifier_type, identifier)
    if isinstance(data, ProblemDetail):
        return data
    return flask.jsonify(**data)

@library_route('/admin/works/<identifier_type>/<path:identifier>/complaints', methods=['GET'])
@has_library
@returns_problem_detail
@requires_admin
def work_complaints(identifier_type, identifier):
    data = app.manager.admin_work_controller.complaints(identifier_type, identifier)
    if isinstance(data, ProblemDetail):
        return data
    return flask.jsonify(**data)

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
@returns_problem_detail
@requires_admin
def genres():
    """Returns a JSON representation of complete genre tree."""
    data = app.manager.admin_feed_controller.genres()
    if isinstance(data, ProblemDetail):
        return data
    return flask.jsonify(**data)

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
@returns_problem_detail
@requires_admin
def circulation_events():
    """Returns a JSON representation of the most recent circulation events."""
    data = app.manager.admin_dashboard_controller.circulation_events()
    if isinstance(data, ProblemDetail):
        return data
    return flask.jsonify(**data)

@app.route('/admin/stats')
@returns_problem_detail
@requires_admin
def stats():
    data = app.manager.admin_dashboard_controller.stats()
    if isinstance(data, ProblemDetail):
        return data
    return flask.jsonify(**data)

@app.route('/admin/libraries', methods=['GET', 'POST'])
@returns_problem_detail
@requires_admin
@requires_csrf_token
def libraries():
    data = app.manager.admin_settings_controller.libraries()
    if isinstance(data, ProblemDetail):
        return data
    if isinstance(data, Response):
        return data
    return flask.jsonify(**data)

@app.route("/admin/collections", methods=['GET', 'POST'])
@returns_problem_detail
@requires_admin
@requires_csrf_token
def collections():
    data = app.manager.admin_settings_controller.collections()
    if isinstance(data, ProblemDetail):
        return data
    if isinstance(data, Response):
        return data
    return flask.jsonify(**data)

@app.route("/admin/admin_auth_services", methods=['GET', 'POST'])
@returns_problem_detail
@allows_admin_auth_setup
@requires_admin
@requires_csrf_token
def admin_auth_services():
    data = app.manager.admin_settings_controller.admin_auth_services()
    if isinstance(data, ProblemDetail):
        return data
    if isinstance(data, Response):
        return data
    return flask.jsonify(**data)

@app.route("/admin/individual_admins", methods=['GET', 'POST'])
@returns_problem_detail
@allows_admin_auth_setup
@requires_admin
@requires_csrf_token
def individual_admins():
    data = app.manager.admin_settings_controller.individual_admins()
    if isinstance(data, ProblemDetail):
        return data
    if isinstance(data, Response):
        return data
    return flask.jsonify(**data)

@app.route("/admin/patron_auth_services", methods=['GET', 'POST'])
@returns_problem_detail
@requires_admin
@requires_csrf_token
def patron_auth_services():
    data = app.manager.admin_settings_controller.patron_auth_services()
    if isinstance(data, ProblemDetail):
        return data
    if isinstance(data, Response):
        return data
    return flask.jsonify(**data)

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

@app.route('/admin/web')
@app.route('/admin/web/')
@app.route('/admin/web/collection/<path:collection>/book/<path:book>')
@app.route('/admin/web/collection/<path:collection>')
@app.route('/admin/web/book/<path:book>')
@app.route('/admin/web/<path:etc>') # catchall for single-page URLs
def admin_view(collection=None, book=None, **kwargs):
    setting_up = (app.manager.admin_sign_in_controller.auth == None)
    if not setting_up:
        admin = app.manager.admin_sign_in_controller.authenticated_admin_from_request()
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
            return redirect(app.manager.url_for('admin_sign_in', redirect=redirect_url))
        library = Library.instance(app.manager._db)
        home_url = app.manager.url_for('acquisition_groups', library_short_name=library.short_name)
    else:
        home_url = None

    csrf_token = flask.request.cookies.get("csrf_token") or app.manager.admin_sign_in_controller.generate_csrf_token()

    show_circ_events_download = (
        "core.local_analytics_provider" in (Configuration.policy("analytics") or [])
    )
    response = Response(flask.render_template_string(
        admin_template,
        csrf_token=csrf_token,
        home_url=home_url,
        show_circ_events_download=show_circ_events_download,
        setting_up=setting_up,
    ))

    # The CSRF token is in its own cookie instead of the session cookie,
    # because if your session expires and you log in again, you should
    # be able to submit a form you already had open. The CSRF token lasts
    # until the user closes the browser window.
    response.set_cookie("csrf_token", csrf_token, httponly=True)
    return response

@app.route('/admin')
@app.route('/admin/')
def admin_base(**kwargs):
    return redirect(app.manager.url_for('admin_view'))

@app.route('/admin/static/circulation-web.js')
@returns_problem_detail
def admin_js():
    directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "node_modules", "simplified-circulation-web", "dist")
    return flask.send_from_directory(directory, "circulation-web.js")

@app.route('/admin/static/circulation-web.css')
@returns_problem_detail
def admin_css():
    directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "node_modules", "simplified-circulation-web", "dist")
    return flask.send_from_directory(directory, "circulation-web.css")

