from nose.tools import set_trace
from functools import wraps
import flask
from flask import (
    Response,
    redirect
)
import os

from api.app import app
from config import Configuration

from core.util.problem_detail import ProblemDetail

from api.routes import returns_problem_detail

from controller import setup_admin_controllers
from templates import (
    admin as admin_template,
    admin_sign_in_again as sign_in_again_template,
)

# The secret key is used for signing cookies for admin login
app.secret_key = Configuration.get(Configuration.SECRET_KEY)

@app.before_first_request
def setup_admin():
    if getattr(app, 'manager', None) is not None:
        setup_admin_controllers(app.manager)

def requires_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        admin = app.manager.admin_sign_in_controller.authenticated_admin_from_request()
        if isinstance(admin, ProblemDetail):
            return app.manager.admin_sign_in_controller.error_response(admin)
        elif isinstance(admin, Response):
            return admin
        return f(*args, **kwargs)
    return decorated

def requires_csrf_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = app.manager.admin_sign_in_controller.check_csrf_token()
        if isinstance(token, ProblemDetail):
            return token
        return f(*args, **kwargs)
    return decorated

@app.route('/admin/GoogleAuth/callback')
@returns_problem_detail
def google_auth_callback():
    return app.manager.admin_sign_in_controller.redirect_after_sign_in()

@app.route('/admin/sign_in')
@returns_problem_detail
def admin_sign_in():
    return app.manager.admin_sign_in_controller.sign_in()

@app.route('/admin/works/<data_source>/<identifier>', methods=['GET'])
@returns_problem_detail
@requires_admin
def work_details(data_source, identifier):
    return app.manager.admin_work_controller.details(data_source, identifier)

@app.route('/admin/works/<data_source>/<identifier>/complaints', methods=['GET'])
@returns_problem_detail
@requires_admin
def work_complaints(data_source, identifier):
    data = app.manager.admin_work_controller.complaints(data_source, identifier)
    if isinstance(data, ProblemDetail):
        return data
    return flask.jsonify(**data)

@app.route('/admin/works/<data_source>/<identifier>/edit', methods=['POST'])
@returns_problem_detail
@requires_admin
def edit(data_source, identifier):
    return app.manager.admin_work_controller.edit(data_source, identifier)

@app.route('/admin/works/<data_source>/<identifier>/suppress', methods=['POST'])
@returns_problem_detail
@requires_csrf_token
@requires_admin
def suppress(data_source, identifier):
    return app.manager.admin_work_controller.suppress(data_source, identifier)

@app.route('/admin/works/<data_source>/<identifier>/unsuppress', methods=['POST'])
@returns_problem_detail
@requires_csrf_token
@requires_admin
def unsuppress(data_source, identifier):
    return app.manager.admin_work_controller.unsuppress(data_source, identifier)

@app.route('/works/<data_source>/<identifier>/refresh', methods=['POST'])
@returns_problem_detail
@requires_csrf_token
@requires_admin
def refresh(data_source, identifier):
    return app.manager.admin_work_controller.refresh_metadata(data_source, identifier)

@app.route('/admin/works/<data_source>/<identifier>/resolve_complaints', methods=['POST'])
@returns_problem_detail
@requires_admin
@requires_csrf_token
def resolve_complaints(data_source, identifier):
    return app.manager.admin_work_controller.resolve_complaints(data_source, identifier)

@app.route('/admin/complaints')
@returns_problem_detail
@requires_admin
def complaints():
    return app.manager.admin_feed_controller.complaints()

@app.route('/admin/suppressed')
@returns_problem_detail
@requires_admin
def suppressed():
    """Returns a feed of suppressed works."""
    return app.manager.admin_feed_controller.suppressed()

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

@app.route('/admin')
@app.route('/admin/')
def admin_view():
    admin = app.manager.admin_sign_in_controller.authenticated_admin_from_request()
    csrf_token = app.manager.admin_sign_in_controller.get_csrf_token()
    if isinstance(admin, ProblemDetail) or csrf_token is None or isinstance(csrf_token, ProblemDetail):
        redirect_url = flask.request.url
        return redirect(app.manager.url_for('admin_sign_in', redirect=redirect_url))
    return flask.render_template_string(admin_template,
        csrf_token=csrf_token,
        home_url=app.manager.url_for('acquisition_groups'))

@app.route('/admin/static/circulation-web.js')
@returns_problem_detail
@requires_admin
def admin_js():
    directory = os.path.join(os.path.dirname(__file__), "node_modules", "simplified-circulation-web", "dist")
    return flask.send_from_directory(directory, "circulation-web.js")

