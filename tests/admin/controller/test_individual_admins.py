from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
from flask_babel import lazy_gettext as _
import json
from werkzeug import MultiDict
from api.admin.exceptions import *
from api.admin.problem_details import *
from core.model import (
    Admin,
    AdminRole,
    create,
    get_one,
)

from .test_controller import SettingsControllerTest

class TestIndividualAdmins(SettingsControllerTest):

    def test_individual_admins_get(self):
        for admin in self._db.query(Admin):
            self._db.delete(admin)

        # There are two admins that can sign in with passwords, with different roles.
        admin1, ignore = create(self._db, Admin, email="admin1@nypl.org")
        admin1.password = "pass1"
        admin1.add_role(AdminRole.SYSTEM_ADMIN)
        admin2, ignore = create(self._db, Admin, email="admin2@nypl.org")
        admin2.password = "pass2"
        admin2.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        admin2.add_role(AdminRole.SITEWIDE_LIBRARIAN)

        # These admins don't have passwords.
        admin3, ignore = create(self._db, Admin, email="admin3@nypl.org")
        admin3.add_role(AdminRole.LIBRARIAN, self._default_library)
        library2 = self._library()
        admin4, ignore = create(self._db, Admin, email="admin4@l2.org")
        admin4.add_role(AdminRole.LIBRARY_MANAGER, library2)
        admin5, ignore = create(self._db, Admin, email="admin5@l2.org")
        admin5.add_role(AdminRole.LIBRARIAN, library2)

        # TODO PYTHON3 - It's easier not to sort these dicts. Replace
        # eq_sorted with eq_.
        def eq_sorted(x, y):
            eq_(sorted(x), sorted(x))

        with self.request_context_with_admin("/", admin=admin1):
            # A system admin can see all other admins' roles.
            response = self.manager.admin_individual_admin_settings_controller.process_get()
            admins = response.get("individualAdmins")
            eq_sorted([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }, { "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }]},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins
            )

        with self.request_context_with_admin("/", admin=admin2):
            # A sitewide librarian or library manager can also see all admins' roles.
            response = self.manager.admin_individual_admin_settings_controller.process_get()
            admins = response.get("individualAdmins")
            eq_sorted([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }, { "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }]},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins
            )

        with self.request_context_with_admin("/", admin=admin3):
            # A librarian or library manager of a specific library can see all admins, but only
            # roles that affect their libraries.
            response = self.manager.admin_individual_admin_settings_controller.process_get()
            admins = response.get("individualAdmins")
            eq_sorted([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }, { "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }]},
                 {"email": "admin4@l2.org", "roles": []},
                 {"email": "admin5@l2.org", "roles": []}],
                admins
            )

        with self.request_context_with_admin("/", admin=admin4):
            response = self.manager.admin_individual_admin_settings_controller.process_get()
            admins = response.get("individualAdmins")
            eq_sorted([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": []},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins
            )

        with self.request_context_with_admin("/", admin=admin5):
            response = self.manager.admin_individual_admin_settings_controller.process_get()
            admins = response.get("individualAdmins")
            eq_sorted([{"email": "admin1@nypl.org", "roles": [{ "role": AdminRole.SYSTEM_ADMIN }]},
                 {"email": "admin2@nypl.org", "roles": [{ "role": AdminRole.SITEWIDE_LIBRARIAN }]},
                 {"email": "admin3@nypl.org", "roles": []},
                 {"email": "admin4@l2.org", "roles": [{ "role": AdminRole.LIBRARY_MANAGER, "library": library2.short_name }]},
                 {"email": "admin5@l2.org", "roles": [{ "role": AdminRole.LIBRARIAN, "library": library2.short_name }]}],
                admins
            )

    def test_individual_admins_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
               ("email", "test@library.org"),
               ("roles", json.dumps([{ "role": AdminRole.LIBRARIAN, "library": "notalibrary" }])),
            ])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(response.uri, LIBRARY_NOT_FOUND.uri)

        library = self._library()
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
               ("email", "test@library.org"),
               ("roles", json.dumps([{ "role": "notarole", "library": library.short_name }])),
            ])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(response.uri, UNKNOWN_ROLE.uri)

    def test_individual_admins_post_permissions(self):
        l1 = self._library()
        l2 = self._library()
        system, ignore = create(self._db, Admin, email="system@example.com")
        system.add_role(AdminRole.SYSTEM_ADMIN)
        sitewide_manager, ignore = create(self._db, Admin, email="sitewide_manager@example.com")
        sitewide_manager.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        sitewide_librarian, ignore = create(self._db, Admin, email="sitewide_librarian@example.com")
        sitewide_librarian.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        manager1, ignore = create(self._db, Admin, email="library_manager_l1@example.com")
        manager1.add_role(AdminRole.LIBRARY_MANAGER, l1)
        librarian1, ignore = create(self._db, Admin, email="librarian_l1@example.com")
        librarian1.add_role(AdminRole.LIBRARIAN, l1)
        l2 = self._library()
        manager2, ignore = create(self._db, Admin, email="library_manager_l2@example.com")
        manager2.add_role(AdminRole.LIBRARY_MANAGER, l2)
        librarian2, ignore = create(self._db, Admin, email="librarian_l2@example.com")
        librarian2.add_role(AdminRole.LIBRARIAN, l2)

        def test_changing_roles(admin_making_request, target_admin, roles=None, allowed=False):
            with self.request_context_with_admin("/", method="POST", admin=admin_making_request):
                flask.request.form = MultiDict([
                    ("email", target_admin.email),
                    ("roles", json.dumps(roles or [])),
                ])
                if allowed:
                    self.manager.admin_individual_admin_settings_controller.process_post()
                    self._db.rollback()
                else:
                    assert_raises(AdminNotAuthorized,
                                  self.manager.admin_individual_admin_settings_controller.process_post)

        # Various types of user trying to change a system admin's roles
        test_changing_roles(system, system, allowed=True)
        test_changing_roles(sitewide_manager, system)
        test_changing_roles(sitewide_librarian, system)
        test_changing_roles(manager1, system)
        test_changing_roles(librarian1, system)
        test_changing_roles(manager2, system)
        test_changing_roles(librarian2, system)

        # Various types of user trying to change a sitewide manager's roles
        test_changing_roles(system, sitewide_manager, allowed=True)
        test_changing_roles(sitewide_manager, sitewide_manager, allowed=True)
        test_changing_roles(sitewide_librarian, sitewide_manager)
        test_changing_roles(manager1, sitewide_manager)
        test_changing_roles(librarian1, sitewide_manager)
        test_changing_roles(manager2, sitewide_manager)
        test_changing_roles(librarian2, sitewide_manager)

        # Various types of user trying to change a sitewide librarian's roles
        test_changing_roles(system, sitewide_librarian, allowed=True)
        test_changing_roles(sitewide_manager, sitewide_librarian, allowed=True)
        test_changing_roles(sitewide_librarian, sitewide_librarian)
        test_changing_roles(manager1, sitewide_librarian)
        test_changing_roles(librarian1, sitewide_librarian)
        test_changing_roles(manager2, sitewide_librarian)
        test_changing_roles(librarian2, sitewide_librarian)

        test_changing_roles(manager1, manager1, allowed=True)
        test_changing_roles(manager1, sitewide_librarian,
                            roles=[{ "role": AdminRole.SITEWIDE_LIBRARIAN },
                                   { "role": AdminRole.LIBRARY_MANAGER, "library": l1.short_name }],
                            allowed=True)
        test_changing_roles(manager1, librarian1, allowed=True)
        test_changing_roles(manager2, librarian2,
                            roles=[{ "role": AdminRole.LIBRARIAN, "library": l1.short_name }])
        test_changing_roles(manager2, librarian1,
                            roles=[{ "role": AdminRole.LIBRARY_MANAGER, "library": l1.short_name }])

        test_changing_roles(sitewide_librarian, librarian1)

        test_changing_roles(sitewide_manager, sitewide_manager,
                            roles=[{ "role": AdminRole.SYSTEM_ADMIN }])
        test_changing_roles(sitewide_librarian, manager1,
                            roles=[{ "role": AdminRole.SITEWIDE_LIBRARY_MANAGER }])

        def test_changing_password(admin_making_request, target_admin, allowed=False):
            with self.request_context_with_admin("/", method="POST", admin=admin_making_request):
                flask.request.form = MultiDict([
                    ("email", target_admin.email),
                    ("password", "new password"),
                    ("roles", json.dumps([role.to_dict() for role in target_admin.roles])),
                ])
                if allowed:
                    self.manager.admin_individual_admin_settings_controller.process_post()
                    self._db.rollback()
                else:
                    assert_raises(AdminNotAuthorized,
                                  self.manager.admin_individual_admin_settings_controller.process_post)

        # Various types of user trying to change a system admin's password
        test_changing_password(system, system, allowed=True)
        test_changing_password(sitewide_manager, system)
        test_changing_password(sitewide_librarian, system)
        test_changing_password(manager1, system)
        test_changing_password(librarian1, system)
        test_changing_password(manager2, system)
        test_changing_password(librarian2, system)

        # Various types of user trying to change a sitewide manager's password
        test_changing_password(system, sitewide_manager, allowed=True)
        test_changing_password(sitewide_manager, sitewide_manager, allowed=True)
        test_changing_password(sitewide_librarian, sitewide_manager)
        test_changing_password(manager1, sitewide_manager)
        test_changing_password(librarian1, sitewide_manager)
        test_changing_password(manager2, sitewide_manager)
        test_changing_password(librarian2, sitewide_manager)

        # Various types of user trying to change a sitewide librarian's password
        test_changing_password(system, sitewide_librarian, allowed=True)
        test_changing_password(sitewide_manager, sitewide_librarian, allowed=True)
        test_changing_password(manager1, sitewide_librarian, allowed=True)
        test_changing_password(manager2, sitewide_librarian, allowed=True)
        test_changing_password(sitewide_librarian, sitewide_librarian)
        test_changing_password(librarian1, sitewide_librarian)
        test_changing_password(librarian2, sitewide_librarian)

        # Various types of user trying to change a manager's password
        # Manager 1
        test_changing_password(system, manager1, allowed=True)
        test_changing_password(sitewide_manager, manager1, allowed=True)
        test_changing_password(manager1, manager1, allowed=True)
        test_changing_password(sitewide_librarian, manager1)
        test_changing_password(manager2, manager1)
        test_changing_password(librarian2, manager1)
        # Manager 2
        test_changing_password(system, manager2, allowed=True)
        test_changing_password(sitewide_manager, manager2, allowed=True)
        test_changing_password(manager2, manager2, allowed=True)
        test_changing_password(sitewide_librarian, manager2)
        test_changing_password(manager1, manager2)
        test_changing_password(librarian1, manager2)

        # Various types of user trying to change a librarian's password
        # Librarian 1
        test_changing_password(system, librarian1, allowed=True)
        test_changing_password(sitewide_manager, librarian1, allowed=True)
        test_changing_password(manager1, librarian1, allowed=True)
        test_changing_password(sitewide_librarian, librarian1)
        test_changing_password(manager2, librarian1)
        test_changing_password(librarian2, librarian1)
        # Librarian 2
        test_changing_password(system, librarian2, allowed=True)
        test_changing_password(sitewide_manager, librarian2, allowed=True)
        test_changing_password(manager2, librarian2, allowed=True)
        test_changing_password(sitewide_librarian, librarian2)
        test_changing_password(manager1, librarian2)
        test_changing_password(librarian1, librarian2)

    def test_individual_admins_post_create(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "admin@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }])),
            ])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(response.status_code, 201)

        # The admin was created.
        admin_match = Admin.authenticate(self._db, "admin@nypl.org", "pass")
        eq_(admin_match.email, response.response[0].decode("utf8"))
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        eq_(AdminRole.LIBRARY_MANAGER, role.role)
        eq_(self._default_library, role.library)

        # The new admin is a library manager, so they can create librarians.
        with self.request_context_with_admin("/", method="POST", admin=admin_match):
            flask.request.form = MultiDict([
                ("email", "admin2@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.LIBRARIAN, "library": self._default_library.short_name }])),
            ])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(response.status_code, 201)

        admin_match = Admin.authenticate(self._db, "admin2@nypl.org", "pass")
        eq_(admin_match.email, response.response[0].decode("utf8"))
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        eq_(AdminRole.LIBRARIAN, role.role)
        eq_(self._default_library, role.library)

    def test_individual_admins_post_edit(self):
        # An admin exists.
        admin, ignore = create(
            self._db, Admin, email="admin@nypl.org",
        )
        admin.password = "password"
        admin.add_role(AdminRole.SYSTEM_ADMIN)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "admin@nypl.org"),
                ("password", "new password"),
                ("roles", json.dumps([{"role": AdminRole.SITEWIDE_LIBRARIAN},
                                      {"role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name}])),
            ])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(response.status_code, 200)

        eq_(admin.email, response.response[0].decode("utf8"))

        # The password was changed.
        old_password_match = Admin.authenticate(self._db, "admin@nypl.org", "password")
        eq_(None, old_password_match)

        new_password_match = Admin.authenticate(self._db, "admin@nypl.org", "new password")
        eq_(admin, new_password_match)

        # The roles were changed.
        eq_(False, admin.is_system_admin())
        [librarian_all, manager] = sorted(admin.roles, key=lambda x: x.role)
        eq_(AdminRole.SITEWIDE_LIBRARIAN, librarian_all.role)
        eq_(None, librarian_all.library)
        eq_(AdminRole.LIBRARY_MANAGER, manager.role)
        eq_(self._default_library, manager.library)

    def test_individual_admin_delete(self):
        librarian, ignore = create(
            self._db, Admin, email=self._str)
        librarian.password = "password"
        librarian.add_role(AdminRole.LIBRARIAN, self._default_library)

        sitewide_manager, ignore = create(
            self._db, Admin, email=self._str)
        sitewide_manager.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)

        system_admin, ignore = create(
            self._db, Admin, email=self._str)
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with self.request_context_with_admin("/", method="DELETE", admin=librarian):
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_individual_admin_settings_controller.process_delete,
                          librarian.email)

        with self.request_context_with_admin("/", method="DELETE", admin=sitewide_manager):
            response = self.manager.admin_individual_admin_settings_controller.process_delete(librarian.email)
            eq_(response.status_code, 200)

            assert_raises(AdminNotAuthorized,
                          self.manager.admin_individual_admin_settings_controller.process_delete,
                          system_admin.email)

        with self.request_context_with_admin("/", method="DELETE", admin=system_admin):
            response = self.manager.admin_individual_admin_settings_controller.process_delete(system_admin.email)
            eq_(response.status_code, 200)

        admin = get_one(self._db, Admin, id=librarian.id)
        eq_(None, admin)

        admin = get_one(self._db, Admin, id=system_admin.id)
        eq_(None, admin)

    def test_individual_admins_post_create_on_setup(self):
        for admin in self._db.query(Admin):
            self._db.delete(admin)

        # Creating an admin that's not a system admin will fail.
        with self.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "first_admin@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.LIBRARY_MANAGER, "library": self._default_library.short_name }])),
            ])
            assert_raises(AdminNotAuthorized, self.manager.admin_individual_admin_settings_controller.process_post)
            self._db.rollback()

        # The password is required.
        with self.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "first_admin@nypl.org"),
                ("roles", json.dumps([{ "role": AdminRole.SYSTEM_ADMIN }])),
            ])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(400, response.status_code)
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        # Creating a system admin with a password works.
        with self.test_request_context("/", method="POST"):
            flask.request.form = MultiDict([
                ("email", "first_admin@nypl.org"),
                ("password", "pass"),
                ("roles", json.dumps([{ "role": AdminRole.SYSTEM_ADMIN }])),
            ])
            response = self.manager.admin_individual_admin_settings_controller.process_post()
            eq_(201, response.status_code)

        # The admin was created.
        admin_match = Admin.authenticate(self._db, "first_admin@nypl.org", "pass")
        eq_(admin_match.email, response.response[0].decode("utf8"))
        assert admin_match
        assert admin_match.has_password("pass")

        [role] = admin_match.roles
        eq_(AdminRole.SYSTEM_ADMIN, role.role)
