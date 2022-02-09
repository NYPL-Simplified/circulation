# encoding: utf-8
import pytest
from ...model.admin import (
    Admin,
    AdminRole,
)


def test_password_hashed(db_session, create_admin_user):
    # GIVEN: An Admin user
    # WHEN:  The Admin password is set to "password"
    # THEN:  Check the hashed password starts with $2a$
    admin = create_admin_user(db_session)
    admin.password = "password"

    pytest.raises(NotImplementedError, lambda: admin.password)
    assert admin.password_hashed.startswith('$2a$')

def test_with_password(db_session, create_admin_user):
    # GIVEN: An Admin user
    # WHEN:  There is no password set
    # THEN:  The Admin.with_password query is empty
    assert [] == Admin.with_password(db_session).all()

    admin = create_admin_user(db_session)
    assert [] == Admin.with_password(db_session).all()

    # WHEN: A password is set
    # THEN: The Admin.with_password query contains users
    admin.password = "password"
    assert [admin] == Admin.with_password(db_session).all()

    admin2 = create_admin_user(db_session, email="admin2@nypl.org")
    assert [admin] == Admin.with_password(db_session).all()

    admin2.password = "password2"
    assert set([admin, admin2]) == set(Admin.with_password(db_session).all())

def test_with_email_spaces(db_session, create_admin_user):
    # GIVEN: An Admin user
    # WHEN:  Creating the Admin user, the email address contains spaces
    # THEN:  The email address should not contain spaces
    admin_spaces = create_admin_user(db_session, email="test@email.com ")
    assert "test@email.com" == admin_spaces.email

def test_has_password(db_session, create_admin_user):
    # GIVEN: An Admin user
    # WHEN:  Setting the password to "password"
    # THEN:  The password should be "password" and not "banana"
    admin = create_admin_user(db_session)
    admin.password = "password"
    assert True == admin.has_password("password")
    assert False == admin.has_password("banana")

def test_authenticate(db_session, create_admin_user):
    # GIVEN: Two Admin users with different passwords
    # WHEN:  Authenticating the Admin users with the password "password"
    # THEN:  The Admin user that has the password "password" is authenticated
    admin = create_admin_user(db_session, email="admin@nypl.org")
    admin.password = "password"
    other_admin = create_admin_user(db_session, email="other@nypl.org")
    other_admin.password = "banana"

    assert admin == Admin.authenticate(db_session, "admin@nypl.org", "password")
    assert None == Admin.authenticate(db_session, "other@nypl.org", "password")
    assert None == Admin.authenticate(db_session, "example@nypl.org", "password")

def test_roles(db_session, create_admin_user, create_library):
    # GIVEN: An Admin user and a Library
    # WHEN:  Changing the Admin user roles
    # THEN:  The Admin user should have appropriate access based on role
    admin = create_admin_user(db_session)
    library = create_library(db_session)
    assert False == admin.is_system_admin()
    assert False == admin.is_library_manager(library)
    assert False == admin.is_librarian(library)

    admin.add_role(AdminRole.SYSTEM_ADMIN)
    assert True == admin.is_system_admin()
    assert True == admin.is_sitewide_library_manager()
    assert True == admin.is_sitewide_librarian()
    assert True == admin.is_library_manager(library)
    assert True == admin.is_librarian(library)

    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
    assert False == admin.is_system_admin()
    assert True == admin.is_sitewide_library_manager()
    assert True == admin.is_sitewide_librarian()
    assert True == admin.is_library_manager(library)
    assert True == admin.is_librarian(library)

    admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
    admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
    assert False == admin.is_system_admin()
    assert False == admin.is_sitewide_library_manager()
    assert True == admin.is_sitewide_librarian()
    assert False == admin.is_library_manager(library)
    assert True == admin.is_librarian(library)

    admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
    admin.add_role(AdminRole.LIBRARY_MANAGER, library)
    assert False == admin.is_system_admin()
    assert False == admin.is_sitewide_library_manager()
    assert False == admin.is_sitewide_librarian()
    assert True == admin.is_library_manager(library)
    assert True == admin.is_librarian(library)

    admin.remove_role(AdminRole.LIBRARY_MANAGER, library)
    admin.add_role(AdminRole.LIBRARIAN, library)
    assert False == admin.is_system_admin()
    assert False == admin.is_sitewide_library_manager()
    assert False == admin.is_sitewide_librarian()
    assert False == admin.is_library_manager(library)
    assert True == admin.is_librarian(library)

    admin.remove_role(AdminRole.LIBRARIAN, library)
    assert False == admin.is_system_admin()
    assert False == admin.is_sitewide_library_manager()
    assert False == admin.is_sitewide_librarian()
    assert False == admin.is_library_manager(library)
    assert False == admin.is_librarian(library)

    other_library = create_library(db_session, name="other", short_name="other")
    admin.add_role(AdminRole.LIBRARY_MANAGER, other_library)
    assert False == admin.is_library_manager(library)
    assert True == admin.is_library_manager(other_library)

    admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
    assert False == admin.is_library_manager(library)
    assert True == admin.is_library_manager(other_library)
    assert True == admin.is_librarian(library)
    assert True == admin.is_librarian(other_library)

    admin.remove_role(AdminRole.LIBRARY_MANAGER, other_library)
    assert False == admin.is_library_manager(library)
    assert False == admin.is_library_manager(other_library)
    assert True == admin.is_librarian(library)
    assert True == admin.is_librarian(other_library)

def test_can_see_collection(db_session, create_admin_user, create_collection, create_library):
    # GIVEN: An Admin user, a Collection tied to a Library, and a stand-alone Collection
    # WHEN: Changing the Admin user roles
    # THEN: The Admin user should see the collection if the correct role is assigned
    admin = create_admin_user(db_session)
    c1 = create_collection(db_session, name="c1")
    c2 = create_collection(db_session, name="c2")
    library = create_library(db_session)
    c2.libraries += [library]

    assert False == admin.can_see_collection(c1)
    assert False == admin.can_see_collection(c2)

    admin.add_role(AdminRole.SYSTEM_ADMIN)
    assert True == admin.can_see_collection(c1)
    assert True == admin.can_see_collection(c2)

    admin.remove_role(AdminRole.SYSTEM_ADMIN)
    admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
    assert False == admin.can_see_collection(c1)
    assert True == admin.can_see_collection(c2)

    admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
    admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
    assert False == admin.can_see_collection(c1)
    assert True == admin.can_see_collection(c2)

    admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
    admin.add_role(AdminRole.LIBRARY_MANAGER, library)
    assert False == admin.can_see_collection(c1)
    assert True == admin.can_see_collection(c2)

    admin.remove_role(AdminRole.LIBRARY_MANAGER, library)
    admin.add_role(AdminRole.LIBRARIAN, library)
    assert False == admin.can_see_collection(c1)
    assert True == admin.can_see_collection(c2)

    admin.remove_role(AdminRole.LIBRARIAN, library)
    assert False == admin.can_see_collection(c1)
    assert False == admin.can_see_collection(c2)
