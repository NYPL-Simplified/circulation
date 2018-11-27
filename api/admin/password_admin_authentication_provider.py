from nose.tools import set_trace

from flask import url_for

from admin_authentication_provider import AdminAuthenticationProvider
from core.model import (
    Admin,
    Session,
)
from problem_details import *
from template_styles import *

class PasswordAdminAuthenticationProvider(AdminAuthenticationProvider):

    NAME = "Password Auth"

    TEMPLATE = """

<form action="%(password_sign_in_url)s" method="post">
<input type="hidden" name="redirect" value="%(redirect)s"/>
<label style="{}">Email <input type="text" name="email" style="{}" /></label>
<label style="{}">Password <input type="password" name="password" style="{}" /></label>
<button type="submit" style="{}">Sign In</button>
</form>""".format(label_style, input_style, label_style, input_style, button_style)

    def sign_in_template(self, redirect):
        password_sign_in_url = url_for("password_auth")
        return self.TEMPLATE % dict(redirect=redirect, password_sign_in_url=password_sign_in_url)

    def sign_in(self, _db, request={}):
        email = request.get("email")
        password = request.get("password")
        redirect_url = request.get("redirect")

        if email and password:
            match = Admin.authenticate(_db, email, password)
            if match:
                return dict(
                    email=email,
                    type=self.NAME,
                ), redirect_url

        return INVALID_ADMIN_CREDENTIALS, None

    def active_credentials(self, admin):
        # Admins who have a password are always active.
        return True
