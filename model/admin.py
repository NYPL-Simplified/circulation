# encoding: utf-8
# Admin, AdminRole
from nose.tools import set_trace

from . import (
    Base,
    get_one,
    get_one_or_create
)
from hasfulltablecache import HasFullTableCache

import bcrypt
from sqlalchemy import (
    Column,
    ForeignKey,
    Index,
    Integer,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    relationship,
    validates
)
from sqlalchemy.orm.session import Session

class Admin(Base, HasFullTableCache):

    __tablename__ = 'admins'

    id = Column(Integer, primary_key=True)
    email = Column(Unicode, unique=True, nullable=False)

    # Admins who log in with OAuth will have a credential.
    credential = Column(Unicode)

    # Admins can also log in with a local password.
    password_hashed = Column(Unicode, index=True)

    # An Admin may have many roles.
    roles = relationship("AdminRole", backref="admin", cascade="all, delete-orphan")

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    def cache_key(self):
        return self.email

    def update_credentials(self, _db, credential=None):
        if credential:
            self.credential = credential
        _db.commit()

    @validates('email')
    def validate_email(self, key, address):
        # strip any whitespace from email address
        return address.strip()

    @hybrid_property
    def password(self):
        raise NotImplementedError("Password comparison is only with Admin.authenticate")

    @password.setter
    def password(self, value):
        self.password_hashed = unicode(bcrypt.hashpw(value, bcrypt.gensalt()))

    def has_password(self, password):
        return self.password_hashed == bcrypt.hashpw(password, self.password_hashed)

    @classmethod
    def authenticate(cls, _db, email, password):
        """Finds an authenticated Admin by email and password
        :return: Admin or None
        """
        def lookup_hook():
            return get_one(_db, Admin, email=unicode(email)), False

        match, ignore = Admin.by_cache_key(_db, unicode(email), lookup_hook)
        if match and not match.has_password(password):
            # Admin with this email was found, but password is invalid.
            match = None
        return match

    @classmethod
    def with_password(cls, _db):
        """Get Admins that have a password."""
        return _db.query(Admin).filter(Admin.password_hashed != None)

    def is_system_admin(self):
        _db = Session.object_session(self)
        def lookup_hook():
            return get_one(_db, AdminRole, admin=self, role=AdminRole.SYSTEM_ADMIN), False
        role, ignore = AdminRole.by_cache_key(_db, (self.id, None, AdminRole.SYSTEM_ADMIN), lookup_hook)
        if role:
            return True
        return False

    def is_sitewide_library_manager(self):
        _db = Session.object_session(self)
        if self.is_system_admin():
            return True
        def lookup_hook():
            return get_one(_db, AdminRole, admin=self, role=AdminRole.SITEWIDE_LIBRARY_MANAGER), False
        role, ignore = AdminRole.by_cache_key(_db, (self.id, None, AdminRole.SITEWIDE_LIBRARY_MANAGER), lookup_hook)
        if role:
            return True
        return False

    def is_sitewide_librarian(self):
        _db = Session.object_session(self)
        if self.is_sitewide_library_manager():
            return True
        def lookup_hook():
            return get_one(_db, AdminRole, admin=self, role=AdminRole.SITEWIDE_LIBRARIAN), False
        role, ignore = AdminRole.by_cache_key(_db, (self.id, None, AdminRole.SITEWIDE_LIBRARIAN), lookup_hook)
        if role:
            return True
        return False

    def is_library_manager(self, library):
        _db = Session.object_session(self)
        # First check if the admin is a manager of _all_ libraries.
        if self.is_sitewide_library_manager():
            return True
        # If not, they could stil be a manager of _this_ library.
        def lookup_hook():
            return get_one(_db, AdminRole, admin=self, library=library, role=AdminRole.LIBRARY_MANAGER), False
        role, ignore = AdminRole.by_cache_key(_db, (self.id, library.id, AdminRole.LIBRARY_MANAGER), lookup_hook)
        if role:
            return True
        return False

    def is_librarian(self, library):
        _db = Session.object_session(self)
        # If the admin is a library manager, they can do everything a librarian can do.
        if self.is_library_manager(library):
            return True
        # Check if the admin is a librarian for _all_ libraries.
        if self.is_sitewide_librarian():
            return True
        # If not, they might be a librarian of _this_ library.
        def lookup_hook():
            return get_one(_db, AdminRole, admin=self, library=library, role=AdminRole.LIBRARIAN), False
        role, ignore = AdminRole.by_cache_key(_db, (self.id, library.id, AdminRole.LIBRARIAN), lookup_hook)
        if role:
            return True
        return False

    def can_see_collection(self, collection):
        if self.is_system_admin():
            return True
        for library in collection.libraries:
            if self.is_librarian(library):
                return True
        return False

    def add_role(self, role, library=None):
        _db = Session.object_session(self)
        role, is_new = get_one_or_create(_db, AdminRole, admin=self, role=role, library=library)
        return role

    def remove_role(self, role, library=None):
        _db = Session.object_session(self)
        role = get_one(_db, AdminRole, admin=self, role=role, library=library)
        if role:
            _db.delete(role)

    def __repr__(self):
        return u"<Admin: email=%s>" % self.email

class AdminRole(Base, HasFullTableCache):

    __tablename__ = 'adminroles'

    id = Column(Integer, primary_key=True)
    admin_id = Column(Integer, ForeignKey("admins.id"), nullable=False, index=True)
    library_id = Column(Integer, ForeignKey("libraries.id"), nullable=True, index=True)
    role = Column(Unicode, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint('admin_id', 'library_id', 'role'),
    )

    SYSTEM_ADMIN = "system"
    SITEWIDE_LIBRARY_MANAGER = "manager-all"
    LIBRARY_MANAGER = "manager"
    SITEWIDE_LIBRARIAN = "librarian-all"
    LIBRARIAN = "librarian"

    ROLES = [SYSTEM_ADMIN, SITEWIDE_LIBRARY_MANAGER, LIBRARY_MANAGER, SITEWIDE_LIBRARIAN, LIBRARIAN]

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    def cache_key(self):
        return (self.admin_id, self.library_id, self.role)

    def to_dict(self):
        if self.library:
            return dict(role=self.role, library=self.library.short_name)
        return dict(role=self.role)

    def __repr__(self):
        return u"<AdminRole: role=%s library=%s admin=%s>" % (
            self.role, (self.library and self.library.short_name), self.admin.email)


Index("ix_adminroles_admin_id_library_id_role", AdminRole.admin_id, AdminRole.library_id, AdminRole.role)
