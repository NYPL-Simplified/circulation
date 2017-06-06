from nose.tools import set_trace

from flask import url_for

from admin_authentication_provider import AdminAuthenticationProvider
from core.model import (
    Admin,
    Session,
)
from problem_details import *

class PasswordAdminAuthenticationProvider(AdminAuthenticationProvider):

    def auth_uri(self, redirect):
        return url_for('password_auth') + "?redirect=%s" % redirect

    def sign_in(self, _db, request={}):
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
