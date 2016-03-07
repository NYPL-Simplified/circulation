from nose.tools import set_trace
from functools import wraps
import flask
from flask import Response
import os

from api.app import app
from config import Configuration

from core.util.problem_detail import ProblemDetail

from api.routes import returns_problem_detail

from controller import setup_admin_controllers
from templates import admin as admin_template


if getattr(app, 'manager', None) is not None:
    setup_admin_controllers(app.manager)

# The secret key is used for signing cookies for admin login
app.secret_key = Configuration.get(Configuration.SECRET_KEY)


def requires_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        admin = app.manager.admin_signin_controller.authenticated_admin_from_request()
        if isinstance(admin, ProblemDetail):
            return app.manager.admin_signin_controller.error_response(admin)
        elif isinstance(admin, Response):
            return admin
        return f(*args, **kwargs)
    return decorated

def requires_csrf_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = app.manager.admin_signin_controller.check_csrf_token()
        if isinstance(token, ProblemDetail):
            return token
        return f(*args, **kwargs)
    return decorated

@app.route('/admin/GoogleAuth/callback')
@returns_problem_detail
def google_auth_callback():
    return app.manager.admin_signin_controller.redirect_after_signin()

@app.route('/admin/signin')
@returns_problem_detail
def admin_signin():
    return app.manager.admin_signin_controller.signin()


@app.route('/admin/works/<data_source>/<identifier>/details', methods=['GET'])
@requires_admin
@returns_problem_detail
def work_details(data_source, identifier):
    return app.manager.admin_work_controller.details(data_source, identifier)

@app.route('/admin/works/<data_source>/<identifier>/suppress', methods=['POST'])
@requires_admin
@requires_csrf_token
@returns_problem_detail
def suppress(data_source, identifier):
    return app.manager.admin_work_controller.suppress(data_source, identifier)

@app.route('/admin/works/<data_source>/<identifier>/unsuppress', methods=['POST'])
@requires_admin
@requires_csrf_token
@returns_problem_detail
def unsuppress(data_source, identifier):
    return app.manager.admin_work_controller.unsuppress(data_source, identifier)


@app.route('/admin')
@app.route('/admin/')
def admin_view():
    csrf_token = app.manager.admin_signin_controller.get_csrf_token()
    if isinstance(csrf_token, ProblemDetail):
        csrf_token = None
    return flask.render_template_string(admin_template, csrf_token=csrf_token)

@app.route('/admin/static/circulation-web.js')
def admin_js():
    directory = os.path.join(os.path.dirname(__file__), "node_modules", "simplified-circulation-web", "lib")
    return flask.send_from_directory(directory, "index.js")

