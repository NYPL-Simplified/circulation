# encoding: utf-8
import pytest
from ...testing import DatabaseTest
from ...model import create
from ...model.admin import (
    Admin,
    AdminRole,
)

class TestAdmin(DatabaseTest):
    def setup_method(self):
        super(TestAdmin, self).setup_method()
        self.admin, ignore = create(self._db, Admin, email="admin@nypl.org")
        self.admin.password = "password"

    def test_password_hashed(self):
        pytest.raises(NotImplementedError, lambda: self.admin.password)
        assert self.admin.password_hashed.startswith('$2a$')

    def test_with_password(self):
        self._db.delete(self.admin)
        assert [] == Admin.with_password(self._db).all()

        admin, ignore = create(self._db, Admin, email="admin@nypl.org")
        assert [] == Admin.with_password(self._db).all()

        admin.password = "password"
        assert [admin] == Admin.with_password(self._db).all()

        admin2, ignore = create(self._db, Admin, email="admin2@nypl.org")
        assert [admin] == Admin.with_password(self._db).all()

        admin2.password = "password2"
        assert set([admin, admin2]) == set(Admin.with_password(self._db).all())

    def test_with_email_spaces(self):
        admin_spaces, ignore = create(self._db, Admin, email="test@email.com ")
        assert "test@email.com" == admin_spaces.email

    def test_has_password(self):
        assert True == self.admin.has_password("password")
        assert False == self.admin.has_password("banana")

    def test_authenticate(self):
        other_admin, ignore = create(self._db, Admin, email="other@nypl.org")
        other_admin.password = "banana"
        assert self.admin == Admin.authenticate(self._db, "admin@nypl.org", "password")
        assert None == Admin.authenticate(self._db, "other@nypl.org", "password")
        assert None == Admin.authenticate(self._db, "example@nypl.org", "password")

    def test_roles(self):
        # The admin has no roles yet.
        assert False == self.admin.is_system_admin()
        assert False == self.admin.is_library_manager(self._default_library)
        assert False == self.admin.is_librarian(self._default_library)

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        assert True == self.admin.is_system_admin()
        assert True == self.admin.is_sitewide_library_manager()
        assert True == self.admin.is_sitewide_librarian()
        assert True == self.admin.is_library_manager(self._default_library)
        assert True == self.admin.is_librarian(self._default_library)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        assert False == self.admin.is_system_admin()
        assert True == self.admin.is_sitewide_library_manager()
        assert True == self.admin.is_sitewide_librarian()
        assert True == self.admin.is_library_manager(self._default_library)
        assert True == self.admin.is_librarian(self._default_library)

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert False == self.admin.is_system_admin()
        assert False == self.admin.is_sitewide_library_manager()
        assert True == self.admin.is_sitewide_librarian()
        assert False == self.admin.is_library_manager(self._default_library)
        assert True == self.admin.is_librarian(self._default_library)

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        assert False == self.admin.is_system_admin()
        assert False == self.admin.is_sitewide_library_manager()
        assert False == self.admin.is_sitewide_librarian()
        assert True == self.admin.is_library_manager(self._default_library)
        assert True == self.admin.is_librarian(self._default_library)

        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)
        assert False == self.admin.is_system_admin()
        assert False == self.admin.is_sitewide_library_manager()
        assert False == self.admin.is_sitewide_librarian()
        assert False == self.admin.is_library_manager(self._default_library)
        assert True == self.admin.is_librarian(self._default_library)

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        assert False == self.admin.is_system_admin()
        assert False == self.admin.is_sitewide_library_manager()
        assert False == self.admin.is_sitewide_librarian()
        assert False == self.admin.is_library_manager(self._default_library)
        assert False == self.admin.is_librarian(self._default_library)

        other_library = self._library()
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, other_library)
        assert False == self.admin.is_library_manager(self._default_library)
        assert True == self.admin.is_library_manager(other_library)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert False == self.admin.is_library_manager(self._default_library)
        assert True == self.admin.is_library_manager(other_library)
        assert True == self.admin.is_librarian(self._default_library)
        assert True == self.admin.is_librarian(other_library)
        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, other_library)
        assert False == self.admin.is_library_manager(self._default_library)
        assert False == self.admin.is_library_manager(other_library)
        assert True == self.admin.is_librarian(self._default_library)
        assert True == self.admin.is_librarian(other_library)

    def test_can_see_collection(self):
        # This collection is only visible to system admins since it has no libraries.
        c1 = self._collection()

        # This collection is visible to libraries of its library.
        c2 = self._collection()
        c2.libraries += [self._default_library]

        # The admin has no roles yet.
        assert False == self.admin.can_see_collection(c1);
        assert False == self.admin.can_see_collection(c2);

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        assert True == self.admin.can_see_collection(c1)
        assert True == self.admin.can_see_collection(c2)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        assert False == self.admin.can_see_collection(c1);
        assert True == self.admin.can_see_collection(c2);

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        self.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert False == self.admin.can_see_collection(c1);
        assert True == self.admin.can_see_collection(c2);

        self.admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
        self.admin.add_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        assert False == self.admin.can_see_collection(c1);
        assert True == self.admin.can_see_collection(c2);

        self.admin.remove_role(AdminRole.LIBRARY_MANAGER, self._default_library)
        self.admin.add_role(AdminRole.LIBRARIAN, self._default_library)
        assert False == self.admin.can_see_collection(c1);
        assert True == self.admin.can_see_collection(c2);

        self.admin.remove_role(AdminRole.LIBRARIAN, self._default_library)
        assert False == self.admin.can_see_collection(c1);
        assert False == self.admin.can_see_collection(c2);
