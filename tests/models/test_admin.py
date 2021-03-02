# encoding: utf-8
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
from .. import DatabaseTest
from ...model import create
from ...model.admin import (
    Admin,
    AdminRole,
)

class TestAdmin(DatabaseTest):
    def setup(self):
        super(TestAdmin, self).setup()
        self.admin, ignore = create(self._db, Admin, email="admin@nypl.org")
        self.admin.password = "password"

    def test_password_hashed(self):
        assert_raises(NotImplementedError, lambda: self.admin.password)
        assert self.admin.password_hashed.startswith('$2a$')

    def test_with_password(self):
        self._db.delete(self.admin)
        eq_([], Admin.with_password(self._db).all())

        admin, ignore = create(self._db, Admin, email="admin@nypl.org")
        eq_([], Admin.with_password(self._db).all())

        admin.password = "password"
        eq_([admin], Admin.with_password(self._db).all())

        admin2, ignore = create(self._db, Admin, email="admin2@nypl.org")
        eq_([admin], Admin.with_password(self._db).all())

        admin2.password = "password2"
        eq_(set([admin, admin2]), set(Admin.with_password(self._db).all()))

    def test_with_email_spaces(self):
        admin_spaces, ignore = create(self._db, Admin, email="test@email.com ")
        eq_("test@email.com", admin_spaces.email)

    def test_has_password(self):
        eq_(True, self.admin.has_password("password"))
        eq_(False, self.admin.has_password("banana"))

    def test_authenticate(self):
        other_admin, ignore = create(self._db, Admin, email="other@nypl.org")
        other_admin.password = "banana"
        eq_(self.admin, Admin.authenticate(self._db, "admin@nypl.org", "password"))
        eq_(None, Admin.authenticate(self._db, "other@nypl.org", "password"))
        eq_(None, Admin.authenticate(self._db, "example@nypl.org", "password"))

    def test_roles(self):
        # The admin has no roles yet.
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(False, self.admin.is_librarian(self._default_library))

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        eq_(True, self.admin.is_system_admin())
        eq_(True, self.admin.is_sitewide_library_manager())
        eq_(True, self.admin.is_sitewide_librarian())
        eq_(True, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        eq_(False, self.admin.is_system_admin())
        eq_(True, self.admin.is_sitewide_library_manager())
        eq_(True, self.admin.is_sitewide_librarian())
        eq_(True, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(True, self.admin.is_sitewide_librarian())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(False, self.admin.is_sitewide_librarian())
        eq_(True, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(False, self.admin.is_sitewide_librarian())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_librarian(self._default_library))

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        eq_(False, self.admin.is_system_admin())
        eq_(False, self.admin.is_sitewide_library_manager())
        eq_(False, self.admin.is_sitewide_librarian())
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(False, self.admin.is_librarian(self._default_library))

        other_library = self._library()
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, other_library)
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_library_manager(other_library))
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(True, self.admin.is_library_manager(other_library))
        eq_(True, self.admin.is_librarian(self._default_library))
        eq_(True, self.admin.is_librarian(other_library))
        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, other_library)
        eq_(False, self.admin.is_library_manager(self._default_library))
        eq_(False, self.admin.is_library_manager(other_library))
        eq_(True, self.admin.is_librarian(self._default_library))
        eq_(True, self.admin.is_librarian(other_library))

    def test_can_see_collection(self):
        # This collection is only visible to system admins since it has no libraries.
        c1 = self._collection()

        # This collection is visible to libraries of its library.
        c2 = self._collection()
        c2.libraries += [self._default_library]

        # The admin has no roles yet.
        eq_(False, self.admin.can_see_collection(c1));
        eq_(False, self.admin.can_see_collection(c2));

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        eq_(True, self.admin.can_see_collection(c1))
        eq_(True, self.admin.can_see_collection(c2))

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        eq_(False, self.admin.can_see_collection(c1));
        eq_(True, self.admin.can_see_collection(c2));

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        eq_(False, self.admin.can_see_collection(c1));
        eq_(True, self.admin.can_see_collection(c2));

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        eq_(False, self.admin.can_see_collection(c1));
        eq_(True, self.admin.can_see_collection(c2));

        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)
        eq_(False, self.admin.can_see_collection(c1));
        eq_(True, self.admin.can_see_collection(c2));

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        eq_(False, self.admin.can_see_collection(c1));
        eq_(False, self.admin.can_see_collection(c2));
