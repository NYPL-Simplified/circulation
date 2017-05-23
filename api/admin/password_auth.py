from nose.tools import set_trace

from flask import url_for

from core.model import (
    Admin,
    Session,
)
from problem_details import *

class PasswordAuthService(object):

    def __init__(self, auth_service):
        self.auth_service = auth_service

    def auth_uri(self, redirect):
        return url_for('password_auth') + "?redirect=%s" % redirect

    def sign_in(self, request={}):
        _db = Session.object_session(self.auth_service)

        email = request.get("email")
        password = request.get("password")
        redirect_url = request.get("redirect")

        match = _db.query(Admin).filter(Admin.email==email).filter(Admin.password==password).count()

        if match:
            return dict(
                email=email,
            ), redirect_url

        return INVALID_ADMIN_CREDENTIALS, None

    def active_credentials(self, admin):
        # Admins who have a password are always active.
        return True
