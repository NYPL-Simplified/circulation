from . import SettingsController
import flask
from flask import Response
from flask_babel import lazy_gettext as _
import json
from nose.tools import set_trace
from core.model import (
    Admin,
    AdminRole,
    Library,
    get_one,
    get_one_or_create,
)
from core.util.problem_detail import ProblemDetail
from api.admin.exceptions import *
from api.admin.problem_details import *
from api.admin.validator import Validator

class IndividualAdminSettingsController(SettingsController):

    def process_individual_admins(self):
        if flask.request.method == 'GET':
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self):
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

    def process_post(self):

        email = flask.request.form.get("email")
        error = self.validate_form_fields(email)
        if error:
            return error

        # If there are no admins yet, anyone can create the first system admin.
        settingUp = (self._db.query(Admin).count() == 0)
        if settingUp and not flask.request.form.get("password"):
            return INCOMPLETE_CONFIGURATION.detailed(_("The password field cannot be blank."))

        admin, is_new = get_one_or_create(self._db, Admin, email=email)

        self.check_permissions(admin, settingUp)

        roles = flask.request.form.get("roles")
        if roles:
            roles = json.loads(roles)
        else:
            roles = []

        roles_error = self.handle_roles(admin, roles, settingUp)
        if roles_error:
            return roles_error

        password = flask.request.form.get("password")
        self.handle_password(password, admin, is_new, settingUp)

        return self.response(admin, is_new)

    def check_permissions(self, admin, settingUp):
        """Before going any further, check that the user actually has permission
         to create/edit this type of admin"""

         # For readability: the person who is submitting the form is referred to as "user"
         # rather than as something that could be confused with "admin" (the admin
         # which the user is submitting the form in order to create/edit.)

        if not settingUp:
            user = flask.request.admin

            # System admin has all permissions.
            if user.is_system_admin():
                return

            # If we've hit this point, then the user isn't a system admin.  If the
            # admin is a system admin, the user won't be able to do anything.
            if admin.is_system_admin():
                raise AdminNotAuthorized()

            # By this point, we know no one is a system admin.
            if user.is_sitewide_library_manager():
                return

            # The user isn't a system admin or a sitewide manager.
            if admin.is_sitewide_library_manager():
                raise AdminNotAuthorized()

    def validate_form_fields(self, email):
        """Check that 1) the user has entered something into the required email field,
        and 2) if so, the input is formatted as a valid email address."""
        if not email:
            return INCOMPLETE_CONFIGURATION
        email_error = self.validate_formats(email)
        if email_error:
            return email_error

    def validate_role_exists(self, role):
        if role.get("role") not in AdminRole.ROLES:
            return UNKNOWN_ROLE

    def look_up_library_for_role(self, role):
        """If the role is affiliated with a particular library, as opposed to being
        sitewide, find the library (and check that it actually exists)."""
        library = None
        library_short_name = role.get("library")
        if library_short_name:
            library = Library.lookup(self._db, library_short_name)
            if not library:
                return LIBRARY_NOT_FOUND.detailed(_("Library \"%(short_name)s\" does not exist.", short_name=library_short_name))
        return library

    def handle_roles(self, admin, roles, settingUp):
        """Compare the admin's existing set of roles against the roles submitted in the form, and,
        unless there's a problem with the roles or the permissions, modify the admin's roles accordingly"""

        # User = person submitting the form; admin = person who the form is about

        if settingUp:
            # There are no admins yet; the user and the new system admin are the same person.
            user = admin
        else:
            user = flask.request.admin

        old_roles = admin.roles
        old_roles_set = set((role.role, role.library) for role in old_roles)

        for role in roles:
            error = self.validate_role_exists(role)
            if error:
                return error

            library = self.look_up_library_for_role(role)
            if isinstance(library, ProblemDetail):
                return library

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
                if user and user.is_librarian(role.library):
                    # A librarian can see roles for the library, but only a library manager
                    # can delete them.
                    self.require_library_manager(role.library)
                    admin.remove_role(role.role, role.library)
                else:
                    # An admin who isn't a librarian for the library won't be able to see
                    # its roles, so might make requests that change other roles without
                    # including this library's roles. Leave the non-visible roles alone.
                    continue

    def handle_password(self, password, admin, is_new, settingUp):
        """Check that the user has permission to change this type of admin's password"""

        # User = person submitting the form; admin = person who the form is about
        if settingUp:
            # There are no admins yet; the user and the new system admin are the same person.
            user = admin
        else:
            user = flask.request.admin

        if password:
            # If the admin we're editing has a sitewide manager role, we've already verified
            # the current admin's role in check_permissions. Otherwise, an admin can only change that
            # admin's password if they are a library manager of one of that admin's
            # libraries, or if they are editing a new admin or an admin who has no
            # roles yet.
            # TODO: set up password reset emails instead.
            # NOTE: librarians can change their own passwords via SignInController.change_password(),
            # but not via this controller; this is because they don't have access to the
            # IndividualAdmins create/edit form.
            if not is_new and not admin.is_sitewide_library_manager():
                can_change_pw = False
                if not admin.roles:
                    can_change_pw = True
                if admin.is_sitewide_librarian():
                    # A manager of any library can change a sitewide librarian's password.
                    if user.is_sitewide_library_manager():
                        can_change_pw = True
                    else:
                        for role in user.roles:
                            if role.role == AdminRole.LIBRARY_MANAGER:
                                can_change_pw = True
                else:
                    for role in admin.roles:
                        if user.is_library_manager(role.library):
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

    def response(self, admin, is_new):
        if is_new:
            return Response(str(admin.email), 201)
        else:
            return Response(str(admin.email), 200)

    def process_delete(self, email):
        self.require_sitewide_library_manager()
        admin = get_one(self._db, Admin, email=email)
        if admin.is_system_admin():
            self.require_system_admin()
        if not admin:
            return MISSING_ADMIN
        self._db.delete(admin)
        return Response(str(_("Deleted")), 200)
