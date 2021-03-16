# encoding: utf-8
# DataSource


from . import (
    Base,
    get_one,
    get_one_or_create,
)
from constants import (
    DataSourceConstants,
    IdentifierConstants,
)
from hasfulltablecache import HasFullTableCache
from licensing import LicensePoolDeliveryMechanism

from collections import defaultdict
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.mutable import (
    MutableDict,
)
from sqlalchemy.orm import (
    backref,
    relationship,
)

class DataSource(Base, HasFullTableCache, DataSourceConstants):

    """A source for information about books, and possibly the books themselves."""

    __tablename__ = 'datasources'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)
    offers_licenses = Column(Boolean, default=False)
    primary_identifier_type = Column(String, index=True)
    extra = Column(MutableDict.as_mutable(JSON), default={})

    # One DataSource can have one IntegrationClient.
    integration_client_id = Column(
        Integer, ForeignKey('integrationclients.id'),
        unique=True, index=True, nullable=True)
    integration_client = relationship("IntegrationClient", backref=backref("data_source", uselist=False))

    # One DataSource can generate many Editions.
    editions = relationship("Edition", backref="data_source")

    # One DataSource can generate many CoverageRecords.
    coverage_records = relationship("CoverageRecord", backref="data_source")

    # One DataSource can generate many IDEquivalencies.
    id_equivalencies = relationship("Equivalency", backref="data_source")

    # One DataSource can grant access to many LicensePools.
    license_pools = relationship(
        "LicensePool", backref=backref("data_source", lazy='joined'))

    # One DataSource can provide many Hyperlinks.
    links = relationship("Hyperlink", backref="data_source")

    # One DataSource can provide many Resources.
    resources = relationship("Resource", backref="data_source")

    # One DataSource can generate many Measurements.
    measurements = relationship("Measurement", backref="data_source")

    # One DataSource can provide many Classifications.
    classifications = relationship("Classification", backref="data_source")

    # One DataSource can have many associated Credentials.
    credentials = relationship("Credential", backref="data_source")

    # One DataSource can generate many CustomLists.
    custom_lists = relationship("CustomList", backref="data_source")

    # One DataSource can have provide many LicensePoolDeliveryMechanisms.
    delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism", backref="data_source",
        foreign_keys=lambda: [LicensePoolDeliveryMechanism.data_source_id]
    )

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    def __repr__(self):
        return '<DataSource: name="%s">' % (self.name)

    def cache_key(self):
        return self.name

    @classmethod
    def lookup(cls, _db, name, autocreate=False, offers_licenses=False,
               primary_identifier_type=None):
        # Turn a deprecated name (e.g. "3M" into the current name
        # (e.g. "Bibliotheca").
        name = cls.DEPRECATED_NAMES.get(name, name)

        def lookup_hook():
            """There was no such DataSource in the cache. Look one up or
            create one.
            """
            if autocreate:
                data_source, is_new = get_one_or_create(
                    _db, DataSource, name=name,
                    create_method_kwargs=dict(
                        offers_licenses=offers_licenses,
                        primary_identifier_type=primary_identifier_type
                    )
                )
            else:
                data_source = get_one(_db, DataSource, name=name)
                is_new = False
            return data_source, is_new

        # Look up the DataSource in the full-table cache, falling back
        # to the database if necessary.
        obj, is_new = cls.by_cache_key(_db, name, lookup_hook)
        return obj

    URI_PREFIX = u"http://librarysimplified.org/terms/sources/"

    @classmethod
    def name_from_uri(cls, uri):
        """Turn a data source URI into a name suitable for passing
        into lookup().
        """
        if not uri.startswith(cls.URI_PREFIX):
            return None
        name = uri[len(cls.URI_PREFIX):]
        return urllib.unquote(name)

    @classmethod
    def from_uri(cls, _db, uri):
        return cls.lookup(_db, cls.name_from_uri(uri))

    @property
    def uri(self):
        return self.URI_PREFIX + urllib.quote(self.name)

    @classmethod
    def license_source_for(cls, _db, identifier):
        """Find the one DataSource that provides licenses for books identified
        by the given identifier.
        If there is no such DataSource, or there is more than one,
        raises an exception.
        """
        sources = cls.license_sources_for(_db, identifier)
        return sources.one()

    @classmethod
    def license_sources_for(cls, _db, identifier):
        """A query that locates all DataSources that provide licenses for
        books identified by the given identifier.
        """
        if isinstance(identifier, basestring):
            type = identifier
        else:
            type = identifier.type
        q =_db.query(DataSource).filter(DataSource.offers_licenses==True).filter(
            DataSource.primary_identifier_type==type)
        return q

    @classmethod
    def metadata_sources_for(cls, _db, identifier):
        """Finds the DataSources that provide metadata for books
        identified by the given identifier.
        """
        if isinstance(identifier, basestring):
            type = identifier
        else:
            type = identifier.type

        if not hasattr(cls, 'metadata_lookups_by_identifier_type'):
            # This should only happen during testing.
            list(DataSource.well_known_sources(_db))

        names = cls.metadata_lookups_by_identifier_type[type]
        return _db.query(DataSource).filter(DataSource.name.in_(names)).all()

    @classmethod
    def well_known_sources(cls, _db):
        """Make sure all the well-known sources exist in the database.
        """

        cls.metadata_lookups_by_identifier_type = defaultdict(list)

        for (name, offers_licenses, offers_metadata_lookup, primary_identifier_type, refresh_rate) in (
                (cls.GUTENBERG, True, False, IdentifierConstants.GUTENBERG_ID, None),
                (cls.RB_DIGITAL, True, True, IdentifierConstants.RB_DIGITAL_ID, None),
                (cls.OVERDRIVE, True, False, IdentifierConstants.OVERDRIVE_ID, 0),
                (cls.BIBLIOTHECA, True, False, IdentifierConstants.BIBLIOTHECA_ID, 60*60*6),
                (cls.ODILO, True, False, IdentifierConstants.ODILO_ID, 0),
                (cls.AXIS_360, True, False, IdentifierConstants.AXIS_360_ID, 0),
                (cls.OCLC, False, False, None, None),
                (cls.OCLC_LINKED_DATA, False, False, None, None),
                (cls.AMAZON, False, False, None, None),
                (cls.OPEN_LIBRARY, False, False, IdentifierConstants.OPEN_LIBRARY_ID, None),
                (cls.GUTENBERG_COVER_GENERATOR, False, False, IdentifierConstants.GUTENBERG_ID, None),
                (cls.GUTENBERG_EPUB_GENERATOR, False, False, IdentifierConstants.GUTENBERG_ID, None),
                (cls.WEB, True, False, IdentifierConstants.URI, None),
                (cls.VIAF, False, False, None, None),
                (cls.CONTENT_CAFE, True, True, IdentifierConstants.ISBN, None),
                (cls.MANUAL, False, False, None, None),
                (cls.NYT, False, False, IdentifierConstants.ISBN, None),
                (cls.LIBRARY_STAFF, False, False, None, None),
                (cls.METADATA_WRANGLER, False, False, None, None),
                (cls.PROJECT_GITENBERG, True, False, IdentifierConstants.GUTENBERG_ID, None),
                (cls.STANDARD_EBOOKS, True, False, IdentifierConstants.URI, None),
                (cls.UNGLUE_IT, True, False, IdentifierConstants.URI, None),
                (cls.ADOBE, False, False, None, None),
                (cls.PLYMPTON, True, False, IdentifierConstants.ISBN, None),
                (cls.ELIB, True, False, IdentifierConstants.ELIB_ID, None),
                (cls.OA_CONTENT_SERVER, True, False, None, None),
                (cls.NOVELIST, False, True, IdentifierConstants.NOVELIST_ID, None),
                (cls.PRESENTATION_EDITION, False, False, None, None),
                (cls.INTERNAL_PROCESSING, False, False, None, None),
                (cls.FEEDBOOKS, True, False, IdentifierConstants.URI, None),
                (cls.BIBBLIO, False, True, IdentifierConstants.BIBBLIO_CONTENT_ITEM_ID, None),
                (cls.ENKI, True, False, IdentifierConstants.ENKI_ID, None),
                (cls.PROQUEST, True, False, IdentifierConstants.PROQUEST_ID, None)
        ):

            obj = DataSource.lookup(
                _db, name, autocreate=True,
                offers_licenses=offers_licenses,
                primary_identifier_type = primary_identifier_type
            )

            if offers_metadata_lookup:
                l = cls.metadata_lookups_by_identifier_type[primary_identifier_type]
                l.append(obj.name)

            yield obj
