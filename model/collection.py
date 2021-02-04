# encoding: utf-8
# Collection, CollectionIdentifier, CollectionMissing
from abc import ABCMeta, abstractmethod

from sqlalchemy import (
    Column,
    exists,
    ForeignKey,
    func,
    Integer,
    Table,
    Unicode,
    UniqueConstraint,
    Boolean
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    backref,
    contains_eager,
    joinedload,
    mapper,
    relationship,
)
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import (
    and_,
    or_,
)

from configuration import (
    ConfigurationSetting,
    ExternalIntegration,
    BaseConfigurationStorage)
from constants import EditionConstants
from coverage import (
    CoverageRecord,
    WorkCoverageRecord,
)
from datasource import DataSource
from edition import Edition
from hasfulltablecache import HasFullTableCache
from identifier import Identifier
from integrationclient import IntegrationClient
from library import Library
from licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from work import Work
from . import (
    Base,
    create,
    get_one,
    get_one_or_create,
)
from ..util.string_helpers import (
    base64,
    native_string,
)


class Collection(Base, HasFullTableCache):

    """A Collection is a set of LicensePools obtained through some mechanism.
    """

    __tablename__ = 'collections'
    id = Column(Integer, primary_key=True)

    name = Column(Unicode, unique=True, nullable=False, index=True)

    DATA_SOURCE_NAME_SETTING = u'data_source'

    # For use in forms that edit Collections.
    EXTERNAL_ACCOUNT_ID_KEY = u'external_account_id'

    # How does the provider of this collection distinguish it from
    # other collections it provides? On the other side this is usually
    # called a "library ID".
    external_account_id = Column(Unicode, nullable=True)

    # How do we connect to the provider of this collection? Any url,
    # authentication information, or additional configuration goes
    # into the external integration, as does the 'protocol', which
    # designates the integration technique we will use to actually get
    # the metadata and licenses. Each Collection has a distinct
    # ExternalIntegration.
    external_integration_id = Column(
        Integer, ForeignKey('externalintegrations.id'), unique=True, index=True)

    # A Collection may specialize some other Collection. For instance,
    # an Overdrive Advantage collection is a specialization of an
    # ordinary Overdrive collection. It uses the same access key and
    # secret as the Overdrive collection, but it has a distinct
    # external_account_id.
    parent_id = Column(Integer, ForeignKey('collections.id'), index=True)

    # When deleting a collection, this flag is set to True so that the deletion
    # script can take care of deleting it in the background. This is
    # useful for deleting large collections which can timeout when deleting.
    marked_for_deletion = Column(Boolean, default=False)

    # A collection may have many child collections. For example,
    # An Overdrive collection may have many children corresponding
    # to Overdrive Advantage collections.
    children = relationship(
        "Collection", backref=backref("parent", remote_side = [id]),
        uselist=True
    )

    # A Collection can provide books to many Libraries.
    libraries = relationship(
        "Library", secondary=lambda: collections_libraries,
        backref="collections"
    )

    # A Collection can include many LicensePools.
    licensepools = relationship(
        "LicensePool", backref="collection",
        cascade="all, delete-orphan"
    )

    # A Collection can have many associated Credentials.
    credentials = relationship("Credential", backref="collection", cascade="delete")

    # A Collection can be monitored by many Monitors, each of which
    # will have its own Timestamp.
    timestamps = relationship("Timestamp", backref="collection")

    catalog = relationship(
        "Identifier", secondary=lambda: collections_identifiers,
        backref="collections"
    )

    # A Collection can be associated with multiple CoverageRecords
    # for Identifiers in its catalog.
    coverage_records = relationship(
        "CoverageRecord", backref="collection",
        cascade="all"
    )

    # A collection may be associated with one or more custom lists.
    # When a new license pool is added to the collection, it will
    # also be added to the list. Admins can remove items from the
    # the list and they won't be added back, so the list doesn't
    # necessarily match the collection.
    customlists = relationship(
        "CustomList", secondary=lambda: collections_customlists,
        backref="collections"
    )

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    # Most data sources offer different catalogs to different
    # libraries.  Data sources in this list offer the same catalog to
    # every library.
    GLOBAL_COLLECTION_DATA_SOURCES = [DataSource.ENKI]

    def __repr__(self):
        return native_string(
            u'<Collection "%s"/"%s" ID=%d>' % (
                self.name, self.protocol, self.id
            )
        )

    def cache_key(self):
        return (self.name, self.external_integration.protocol)

    @classmethod
    def by_name_and_protocol(cls, _db, name, protocol):
        """Find or create a Collection with the given name and the given
        protocol.

        This method uses the full-table cache if possible.

        :return: A 2-tuple (collection, is_new)
        """
        key = (name, protocol)
        def lookup_hook():
            return cls._by_name_and_protocol(_db, key)
        return cls.by_cache_key(_db, key, lookup_hook)

    @classmethod
    def _by_name_and_protocol(cls, _db, cache_key):
        """Find or create a Collection with the given name and the given
        protocol.

        We can't use get_one_or_create because the protocol is kept in
        a separate database object, (an ExternalIntegration).

        :return: A 2-tuple (collection, is_new)
        """
        name, protocol = cache_key

        qu = cls.by_protocol(_db, protocol)
        qu = qu.filter(Collection.name==name)
        try:
            collection = qu.one()
            is_new = False
        except NoResultFound, e:
            # Make a new Collection.
            collection, is_new = get_one_or_create(_db, Collection, name=name)
            if not is_new and collection.protocol != protocol:
                # The collection already exists, it just uses a different
                # protocol than the one we asked about.
                raise ValueError(
                    'Collection "%s" does not use protocol "%s".' % (
                        name, protocol
                    )
                )
            integration = collection.create_external_integration(
                protocol=protocol
            )
            collection.external_integration.protocol=protocol
        return collection, is_new

    @classmethod
    def by_protocol(cls, _db, protocol):
        """Query collections that get their licenses through the given protocol.

        Collections marked for deletion are not included.

        :param protocol: Protocol to use. If this is None, all
            Collections will be returned except those marked for deletion.
        """
        qu = _db.query(Collection)
        if protocol:
            qu = qu.join(
            ExternalIntegration,
            ExternalIntegration.id==Collection.external_integration_id).filter(
                ExternalIntegration.goal==ExternalIntegration.LICENSE_GOAL
            ).filter(ExternalIntegration.protocol==protocol).filter(
                Collection.marked_for_deletion==False
            )

        return qu

    @classmethod
    def by_datasource(cls, _db, data_source):
        """Query collections that are associated with the given DataSource.

        Collections marked for deletion are not included.
        """
        if isinstance(data_source, DataSource):
            data_source = data_source.name

        qu = _db.query(cls).join(ExternalIntegration,
                cls.external_integration_id==ExternalIntegration.id)\
            .join(ExternalIntegration.settings)\
            .filter(ConfigurationSetting.key==Collection.DATA_SOURCE_NAME_SETTING)\
            .filter(ConfigurationSetting.value==data_source).filter(
                Collection.marked_for_deletion==False
            )
        return qu

    @hybrid_property
    def protocol(self):
        """What protocol do we need to use to get licenses for this
        collection?
        """
        return self.external_integration.protocol

    @protocol.setter
    def protocol(self, new_protocol):
        """Modify the protocol in use by this Collection."""
        if self.parent and self.parent.protocol != new_protocol:
            raise ValueError(
                "Proposed new protocol (%s) contradicts parent collection's protocol (%s)." % (
                    new_protocol, self.parent.protocol
                )
            )
        self.external_integration.protocol = new_protocol
        for child in self.children:
            child.protocol = new_protocol

    @hybrid_property
    def primary_identifier_source(self):
        """ Identify if should try to use another identifier than <id> """
        return self.external_integration.primary_identifier_source

    @primary_identifier_source.setter
    def primary_identifier_source(self, new_primary_identifier_source):
        """ Modify the primary identifier source in use by this Collection."""
        self.external_integration.primary_identifier_source = new_primary_identifier_source

    # For collections that can control the duration of the loans they
    # create, the durations are stored in these settings and new loans are
    # expected to be created using these settings. For collections
    # where loan duration is negotiated out-of-bounds, all loans are
    # _assumed_ to have these durations unless we hear otherwise from
    # the server.
    AUDIOBOOK_LOAN_DURATION_KEY = 'audio_loan_duration'
    EBOOK_LOAN_DURATION_KEY = 'ebook_loan_duration'
    STANDARD_DEFAULT_LOAN_PERIOD = 21

    def default_loan_period(self, library, medium=EditionConstants.BOOK_MEDIUM):
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        return self.default_loan_period_setting(
            library, medium).int_value or self.STANDARD_DEFAULT_LOAN_PERIOD

    def default_loan_period_setting(self, library, medium=EditionConstants.BOOK_MEDIUM):
        """Until we hear otherwise from the license provider, we assume
        that someone who borrows a non-open-access item from this
        collection has it for this number of days.
        """
        _db = Session.object_session(library)
        if medium == EditionConstants.AUDIO_MEDIUM:
            key = self.AUDIOBOOK_LOAN_DURATION_KEY
        else:
            key = self.EBOOK_LOAN_DURATION_KEY
        if isinstance(library, Library):
            return (
                ConfigurationSetting.for_library_and_externalintegration(
                    _db, key, library, self.external_integration
                )
            )
        elif isinstance(library, IntegrationClient):
            return self.external_integration.setting(key)

    DEFAULT_RESERVATION_PERIOD_KEY = 'default_reservation_period'
    STANDARD_DEFAULT_RESERVATION_PERIOD = 3

    @hybrid_property
    def default_reservation_period(self):
        """Until we hear otherwise from the license provider, we assume
        that someone who puts an item on hold has this many days to
        check it out before it goes to the next person in line.
        """
        return (
            self.external_integration.setting(
                self.DEFAULT_RESERVATION_PERIOD_KEY,
            ).int_value or self.STANDARD_DEFAULT_RESERVATION_PERIOD
        )

    @default_reservation_period.setter
    def default_reservation_period(self, new_value):
        new_value = int(new_value)
        self.external_integration.setting(
            self.DEFAULT_RESERVATION_PERIOD_KEY).value = str(new_value)

    # When you import an OPDS feed, you may know the intended audience of the works (e.g. children or researchers),
    # even though the OPDS feed may not contain that information.
    # It should be possible to configure a collection with a default audience,
    # so that books imported from the OPDS feed end up with the right audience.
    DEFAULT_AUDIENCE_KEY = 'default_audience'

    @hybrid_property
    def default_audience(self):
        """Return the default audience set up for this collection.

        :return: Default audience
        :rtype: Optional[str]
        """
        setting = self.external_integration.setting(self.DEFAULT_AUDIENCE_KEY)

        return setting.value_or_default(None)

    @default_audience.setter
    def default_audience(self, new_value):
        """Set the default audience for this collection.

        :param new_value: New default audience
        :type new_value: Optional[str]
        """
        setting = self.external_integration.setting(self.DEFAULT_AUDIENCE_KEY)

        setting.value = str(new_value)

    def create_external_integration(self, protocol):
        """Create an ExternalIntegration for this Collection.

        To be used immediately after creating a new Collection,
        e.g. in by_name_and_protocol, from_metadata_identifier, and
        various test methods that create mock Collections.

        If an external integration already exists, return it instead
        of creating another one.

        :param protocol: The protocol known to be in use when getting
            licenses for this collection.
        """
        _db = Session.object_session(self)
        goal = ExternalIntegration.LICENSE_GOAL
        external_integration, is_new = get_one_or_create(
            _db, ExternalIntegration, id=self.external_integration_id,
            create_method_kwargs=dict(protocol=protocol, goal=goal)
        )
        if external_integration.protocol != protocol:
            raise ValueError(
                "Located ExternalIntegration, but its protocol (%s) does not match desired protocol (%s)." % (
                    external_integration.protocol, protocol
                )
            )
        self.external_integration_id = external_integration.id
        return external_integration

    @property
    def external_integration(self):
        """Find the external integration for this Collection, assuming
        it already exists.

        This is generally a safe assumption since by_name_and_protocol and
        from_metadata_identifier both create ExternalIntegrations for the
        Collections they create.
        """
        # We don't enforce this on the database level because it is
        # legitimate for a newly created Collection to have no
        # ExternalIntegration. But by the time it's being used for real,
        # it needs to have one.
        if not self.external_integration_id:
            raise ValueError(
                "No known external integration for collection %s" % self.name
            )
        return self._external_integration

    @property
    def unique_account_id(self):
        """Identifier that uniquely represents this Collection of works"""
        if (self.data_source
            and self.data_source.name in self.GLOBAL_COLLECTION_DATA_SOURCES
            and not self.parent):
            # Every top-level collection from this data source has the
            # same catalog. Treat them all as one collection named
            # after the data source.
            unique_account_id = self.data_source.name
        else:
            unique_account_id = self.external_account_id

        if not unique_account_id:
            raise ValueError("Unique account identifier not set")

        if self.parent:
            return self.parent.unique_account_id + '+' + unique_account_id
        return unique_account_id

    @hybrid_property
    def data_source(self):
        """Find the data source associated with this Collection.

        Bibliographic metadata obtained through the collection
        protocol is recorded as coming from this data source. A
        LicensePool inserted into this collection will be associated
        with this data source, unless its bibliographic metadata
        indicates some other data source.

        For most Collections, the integration protocol sets the data
        source.  For collections that use the OPDS import protocol,
        the data source is a Collection-specific setting.
        """
        data_source = None
        name = ExternalIntegration.DATA_SOURCE_FOR_LICENSE_PROTOCOL.get(
            self.protocol
        )
        if not name:
            name = self.external_integration.setting(
                Collection.DATA_SOURCE_NAME_SETTING
            ).value
        _db = Session.object_session(self)
        if name:
            data_source = DataSource.lookup(_db, name, autocreate=True)
        return data_source

    @data_source.setter
    def data_source(self, new_value):
        if isinstance(new_value, DataSource):
            new_value = new_value.name
        if self.protocol == new_value:
            return

        # Only set a DataSource for Collections that don't have an
        # implied source.
        if self.protocol not in ExternalIntegration.DATA_SOURCE_FOR_LICENSE_PROTOCOL:
            setting = self.external_integration.setting(
                Collection.DATA_SOURCE_NAME_SETTING
            )
            if new_value is not None:
                new_value = unicode(new_value)
            setting.value = new_value

    @property
    def parents(self):
        if self.parent_id:
            _db = Session.object_session(self)
            parent = Collection.by_id(_db, self.parent_id)
            yield parent
            for collection in parent.parents:
                yield collection

    @property
    def metadata_identifier(self):
        """Identifier based on collection details that uniquely represents
        this Collection on the metadata wrangler. This identifier is
        composed of the Collection protocol and account identifier.

        A circulation manager provides a Collection's metadata
        identifier as part of collection registration. The metadata
        wrangler creates a corresponding Collection on its side,
        *named after* the metadata identifier -- regardless of the name
        of that collection on the circulation manager side.
        """
        account_id = self.unique_account_id
        if self.protocol == ExternalIntegration.OPDS_IMPORT:
            # Remove ending / from OPDS url that could duplicate the collection
            # on the Metadata Wrangler.
            while account_id.endswith('/'):
                account_id = account_id[:-1]

        encode = base64.urlsafe_b64encode
        account_id = encode(account_id)
        protocol = encode(self.protocol)

        metadata_identifier = protocol + ':' + account_id
        return encode(metadata_identifier)

    def disassociate_library(self, library):
        """Disassociate a Library from this Collection and delete any relevant
        ConfigurationSettings.
        """
        if library is None or not library in self.libraries:
            # No-op.
            return

        self.libraries.remove(library)

        _db = Session.object_session(self)
        qu = _db.query(
            ConfigurationSetting
        ).filter(
            ConfigurationSetting.library==library
        ).filter(
            ConfigurationSetting.external_integration==self.external_integration
        )
        qu.delete()

    @classmethod
    def _decode_metadata_identifier(cls, metadata_identifier):
        """Invert the metadata_identifier property."""
        if not metadata_identifier:
            raise ValueError("No metadata identifier provided.")
        try:
            decode = base64.urlsafe_b64decode
            details = decode(metadata_identifier)
            encoded_details  = details.split(':', 1)
            [protocol, account_id] = [decode(d) for d in encoded_details]
        except (TypeError, ValueError) as e:
            raise ValueError(
                u"Metadata identifier '%s' is invalid: %s" % (
                    metadata_identifier, unicode(e)
                )
            )
        return protocol, account_id

    @classmethod
    def from_metadata_identifier(cls, _db, metadata_identifier, data_source=None):
        """Finds or creates a Collection on the metadata wrangler, based
        on its unique metadata_identifier.
        """

        # Decode the metadata identifier into a protocol and an
        # account ID. If the metadata identifier is invalid, this
        # will raise an exception.
        protocol, account_id = cls._decode_metadata_identifier(metadata_identifier)

        # Now that we know the metadata identifier is valid, try to
        # look up a collection named after it.
        collection = get_one(_db, Collection, name=metadata_identifier)
        is_new = False

        if not collection:
            # Create a collection named after the metadata
            # identifier. Give it an ExternalIntegration with the
            # corresponding protocol, and set its data source and
            # external_account_id.
            collection, is_new = create(
                _db, Collection, name=metadata_identifier
            )
            collection.create_external_integration(protocol)

        if protocol == ExternalIntegration.OPDS_IMPORT:
            # For OPDS Import collections only, we store the URL to
            # the OPDS feed (the "account ID") and the data source.
            collection.external_account_id = account_id
            if data_source and not isinstance(data_source, DataSource):
                data_source = DataSource.lookup(
                    _db, data_source, autocreate=True
                )
            collection.data_source = data_source

        return collection, is_new

    @property
    def pools_with_no_delivery_mechanisms(self):
        """Find all LicensePools in this Collection that have no delivery
        mechanisms whatsoever.

        :return: A query object.
        """
        _db = Session.object_session(self)
        qu = LicensePool.with_no_delivery_mechanisms(_db)
        return qu.filter(LicensePool.collection==self)

    def explain(self, include_secrets=False):
        """Create a series of human-readable strings to explain a collection's
        settings.

        :param include_secrets: For security reasons,
           sensitive settings such as passwords are not displayed by default.

        :return: A list of explanatory strings.
        """
        lines = []
        if self.name:
            lines.append('Name: "%s"' % self.name)
        if self.parent:
            lines.append('Parent: %s' % self.parent.name)
        integration = self.external_integration
        if integration.protocol:
            lines.append('Protocol: "%s"' % integration.protocol)
        for library in self.libraries:
            lines.append('Used by library: "%s"' % library.short_name)
        if self.external_account_id:
            lines.append('External account ID: "%s"' % self.external_account_id)
        for setting in sorted(integration.settings, key=lambda x: x.key):
            if (include_secrets or not setting.is_secret) and setting.value is not None:
                lines.append('Setting "%s": "%s"' % (setting.key, setting.value))
        return lines

    def catalog_identifier(self, identifier):
        """Inserts an identifier into a catalog"""
        self.catalog_identifiers([identifier])

    def catalog_identifiers(self, identifiers):
        """Inserts identifiers into the catalog"""
        if not identifiers:
            # Nothing to do.
            return

        _db = Session.object_session(identifiers[0])
        already_in_catalog = _db.query(Identifier).join(
            CollectionIdentifier
        ).filter(
            CollectionIdentifier.collection_id==self.id
        ).filter(
             Identifier.id.in_([x.id for x in identifiers])
        ).all()

        new_catalog_entries = [
            dict(collection_id=self.id, identifier_id=identifier.id)
            for identifier in identifiers
            if identifier not in already_in_catalog
        ]
        _db.bulk_insert_mappings(CollectionIdentifier, new_catalog_entries)
        _db.commit()

    def unresolved_catalog(self, _db, data_source_name, operation):
        """Returns a query with all identifiers in a Collection's catalog that
        have unsuccessfully attempted resolution. This method is used on the
        metadata wrangler.

        :return: a sqlalchemy.Query
        """
        coverage_source = DataSource.lookup(_db, data_source_name)
        is_not_resolved = and_(
            CoverageRecord.operation==operation,
            CoverageRecord.data_source_id==coverage_source.id,
            CoverageRecord.status!=CoverageRecord.SUCCESS,
        )

        query = _db.query(Identifier)\
            .outerjoin(Identifier.licensed_through)\
            .outerjoin(Identifier.coverage_records)\
            .outerjoin(LicensePool.work).outerjoin(Identifier.collections)\
            .filter(
                Collection.id==self.id, is_not_resolved, Work.id==None
            ).order_by(Identifier.id)

        return query

    def licensepools_with_works_updated_since(self, _db, timestamp):
        """Finds all LicensePools in a collection's catalog whose Works' OPDS
        entries have been updated since the timestamp. Used by the
        metadata wrangler.

        :param _db: A database connection,
        :param timestamp: A datetime.timestamp object

        :return: a Query that yields LicensePools. The Work and
           Identifier associated with each LicensePool have been
           pre-loaded, giving the caller all the information
           necessary to create full OPDS entries for the works.
        """
        opds_operation = WorkCoverageRecord.GENERATE_OPDS_OPERATION
        qu = _db.query(
            LicensePool
        ).join(
            LicensePool.work,
        ).join(
            LicensePool.identifier,
        ).join(
            Work.coverage_records,
        ).join(
            CollectionIdentifier,
            Identifier.id==CollectionIdentifier.identifier_id
        )
        qu = qu.filter(
            WorkCoverageRecord.operation==opds_operation,
            CollectionIdentifier.collection_id==self.id
        )
        qu = qu.options(
            contains_eager(LicensePool.work),
            contains_eager(LicensePool.identifier),
        )

        if timestamp:
            qu = qu.filter(
                WorkCoverageRecord.timestamp > timestamp
            )

        qu = qu.order_by(WorkCoverageRecord.timestamp)
        return qu

    def isbns_updated_since(self, _db, timestamp):
        """Finds all ISBNs in a collection's catalog that have been updated
           since the timestamp but don't have a Work to show for it. Used in
           the metadata wrangler.

           :return: a Query
        """
        isbns = _db.query(Identifier, func.max(CoverageRecord.timestamp).label('latest'))\
            .join(Identifier.collections)\
            .join(Identifier.coverage_records)\
            .outerjoin(Identifier.licensed_through)\
            .group_by(Identifier.id).order_by('latest')\
            .filter(
                Collection.id==self.id,
                LicensePool.work_id==None,
                CoverageRecord.status==CoverageRecord.SUCCESS,
            ).enable_eagerloads(False).options(joinedload(Identifier.coverage_records))

        if timestamp:
            isbns = isbns.filter(CoverageRecord.timestamp > timestamp)

        return isbns

    @classmethod
    def restrict_to_ready_deliverable_works(
        cls, query, collection_ids=None, show_suppressed=False,
            allow_holds=True,
    ):
        """Restrict a query to show only presentation-ready works present in
        an appropriate collection which the default client can
        fulfill.

        Note that this assumes the query has an active join against
        LicensePool and Edition.

        :param query: The query to restrict.

        :param show_suppressed: Include titles that have nothing but
            suppressed LicensePools.

        :param collection_ids: Only include titles in the given
            collections.

        :param allow_holds: If false, pools with no available copies
            will be hidden.
        """

        # Only find presentation-ready works.
        query = query.filter(Work.presentation_ready == True)

        # Only find books that have some kind of DeliveryMechanism.
        LPDM = LicensePoolDeliveryMechanism
        exists_clause = exists().where(
            and_(LicensePool.data_source_id==LPDM.data_source_id,
                LicensePool.identifier_id==LPDM.identifier_id)
        )
        query = query.filter(exists_clause)

        # Some sources of audiobooks may be excluded because the
        # server can't fulfill them or the expected client can't play
        # them.
        _db = query.session
        excluded = ConfigurationSetting.excluded_audio_data_sources(_db)
        if excluded:
            audio_excluded_ids = [
                DataSource.lookup(_db, x).id for x in excluded
            ]
            query = query.filter(
                or_(Edition.medium != EditionConstants.AUDIO_MEDIUM,
                    ~LicensePool.data_source_id.in_(audio_excluded_ids))
            )

        # Only find books with unsuppressed LicensePools.
        if not show_suppressed:
            query = query.filter(LicensePool.suppressed==False)

        # Only find books with available licenses or books from self-hosted collections using MirrorUploader
        query = query.filter(
            or_(
                LicensePool.licenses_owned > 0,
                LicensePool.open_access,
                LicensePool.unlimited_access,
                LicensePool.self_hosted
            )
        )

        # Only find books in an appropriate collection.
        if collection_ids is not None:
            query = query.filter(
                LicensePool.collection_id.in_(collection_ids)
            )

        # If we don't allow holds, hide any books with no available copies.
        if not allow_holds:
            query = query.filter(
                or_(
                    LicensePool.licenses_available > 0,
                    LicensePool.open_access,
                    LicensePool.self_hosted,
                    LicensePool.unlimited_access
                )
            )
        return query

    def delete(self, search_index=None):
        """Delete a collection.

        Collections can have hundreds of thousands of
        LicensePools. This deletes a collection gradually in a way
        that can be confined to the background and survive interruption.
        """
        if not self.marked_for_deletion:
            raise Exception(
                "Cannot delete %s: it is not marked for deletion." % self.name
            )

        _db = Session.object_session(self)

        # Disassociate all libraries from this collection.
        for library in self.libraries:
            self.disassociate_library(library)

        # Delete all the license pools. This should be the only part
        # of the application where LicensePools are permanently
        # deleted.
        for i, pool in enumerate(self.licensepools):
            work = pool.work
            _db.delete(pool)
            if not i % 100:
                _db.commit()
            if work and not work.license_pools:
                work.delete(search_index)

        # Delete the ExternalIntegration associated with this
        # Collection, assuming it wasn't deleted already.
        if self.external_integration:
            _db.delete(self.external_integration)

        # Now delete the Collection itself.
        _db.delete(self)
        _db.commit()


collections_libraries = Table(
    'collections_libraries', Base.metadata,
     Column(
         'collection_id', Integer, ForeignKey('collections.id'),
         index=True, nullable=False
     ),
     Column(
         'library_id', Integer, ForeignKey('libraries.id'),
         index=True, nullable=False
     ),
     UniqueConstraint('collection_id', 'library_id'),
 )


collections_identifiers = Table(
    'collections_identifiers', Base.metadata,
    Column(
        'collection_id', Integer, ForeignKey('collections.id'),
        index=True, nullable=False
    ),
    Column(
        'identifier_id', Integer, ForeignKey('identifiers.id'),
        index=True, nullable=False
    ),
    UniqueConstraint('collection_id', 'identifier_id'),
)

# Create an ORM model for the collections_identifiers join table
# so it can be used in a bulk_insert_mappings call.
class CollectionIdentifier(object):
    pass


class CollectionMissing(Exception):
    """An operation was attempted that can only happen within the context
    of a Collection, but there was no Collection available.
    """

mapper(
    CollectionIdentifier, collections_identifiers,
    primary_key=(
        collections_identifiers.columns.collection_id,
        collections_identifiers.columns.identifier_id
    )
)

collections_customlists = Table(
    'collections_customlists', Base.metadata,
    Column(
        'collection_id', Integer, ForeignKey('collections.id'),
        index=True, nullable=False,
    ),
    Column(
        'customlist_id', Integer, ForeignKey('customlists.id'),
        index=True, nullable=False,
    ),
    UniqueConstraint('collection_id', 'customlist_id'),
)


class HasExternalIntegrationPerCollection(object):
    """Interface allowing to get access to an external integration"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def collection_external_integration(self, collection):
        """Returns an external integration associated with the collection

        :param collection: Collection
        :type collection: core.model.Collection

        :return: External integration associated with the collection
        :rtype: core.model.configuration.ExternalIntegration
        """
        raise NotImplementedError()


class CollectionConfigurationStorage(BaseConfigurationStorage):
    """Serializes and deserializes values as library's configuration settings"""

    def __init__(self, external_integration_association, collection):
        """Initializes a new instance of ConfigurationStorage class

        :param external_integration_association: Association with an external integtation
        :type external_integration_association: HasExternalIntegrationPerCollection

        :param collection: Collection object
        :type collection: Collection
        """
        self._integration_owner = external_integration_association
        self._collection_id = collection.id

    def save(self, db, setting_name, value):
        """Save the value as as a new configuration setting

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param setting_name: Name of the library's configuration setting
        :type setting_name: string

        :param value: Value to be saved
        :type value: Any
        """
        collection = Collection.by_id(db, self._collection_id)
        integration = self._integration_owner.collection_external_integration(collection)
        ConfigurationSetting.for_externalintegration(
            setting_name,
            integration).value = value

    def load(self, db, setting_name):
        """Loads and returns the library's configuration setting

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param setting_name: Name of the library's configuration setting
        :type setting_name: string

        :return: Any
        """
        collection = Collection.by_id(db, self._collection_id)
        integration = self._integration_owner.collection_external_integration(collection)
        value = ConfigurationSetting.for_externalintegration(
            setting_name,
            integration).value

        return value
