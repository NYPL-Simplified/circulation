# encoding: utf-8
# BaseCoverageRecord, Timestamp, CoverageRecord, WorkCoverageRecord


from . import (
    Base,
    get_one,
    get_one_or_create,
)

import datetime
from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import (
    and_,
    or_,
    literal,
    literal_column,
)

class BaseCoverageRecord(object):
    """Contains useful constants used by both CoverageRecord and
    WorkCoverageRecord.
    """

    SUCCESS = 'success'
    TRANSIENT_FAILURE = 'transient failure'
    PERSISTENT_FAILURE = 'persistent failure'
    REGISTERED = 'registered'

    ALL_STATUSES = [REGISTERED, SUCCESS, TRANSIENT_FAILURE, PERSISTENT_FAILURE]

    # Count coverage as attempted if the record is not 'registered'.
    PREVIOUSLY_ATTEMPTED = [SUCCESS, TRANSIENT_FAILURE, PERSISTENT_FAILURE]

    # By default, count coverage as present if it ended in
    # success or in persistent failure. Do not count coverage
    # as present if it ended in transient failure.
    DEFAULT_COUNT_AS_COVERED = [SUCCESS, PERSISTENT_FAILURE]

    status_enum = Enum(SUCCESS, TRANSIENT_FAILURE, PERSISTENT_FAILURE,
                       REGISTERED, name='coverage_status')

    @classmethod
    def not_covered(cls, count_as_covered=None,
                    count_as_not_covered_if_covered_before=None):
        """Filter a query to find only items without coverage records.

        :param count_as_covered: A list of constants that indicate
            types of coverage records that should count as 'coverage'
            for purposes of this query.
        :param count_as_not_covered_if_covered_before: If a coverage record
            exists, but is older than the given date, do not count it as
            covered.
        :return: A clause that can be passed in to Query.filter().
        """
        if not count_as_covered:
            count_as_covered = cls.DEFAULT_COUNT_AS_COVERED
        elif isinstance(count_as_covered, (bytes, str)):
            count_as_covered = [count_as_covered]

        # If there is no coverage record, then of course the item is
        # not covered.
        missing = cls.id==None

        # If we're looking for specific coverage statuses, then a
        # record does not count if it has some other status.
        missing = or_(
            missing, ~cls.status.in_(count_as_covered)
        )

        # If the record's timestamp is before the cutoff time, we
        # don't count it as covered, regardless of which status it
        # has.
        if count_as_not_covered_if_covered_before:
            missing = or_(
                missing, cls.timestamp < count_as_not_covered_if_covered_before
            )

        return missing


class Timestamp(Base):
    """Tracks the activities of Monitors, CoverageProviders,
    and general scripts.
    """

    __tablename__ = 'timestamps'

    MONITOR_TYPE = "monitor"
    COVERAGE_PROVIDER_TYPE = "coverage_provider"
    SCRIPT_TYPE = "script"

    # A stand-in value used to indicate that a field in the timestamps
    # table should be explicitly set to None. Passing in None for most
    # fields will use default values.
    CLEAR_VALUE = object()

    service_type_enum = Enum(
        MONITOR_TYPE, COVERAGE_PROVIDER_TYPE, SCRIPT_TYPE,
        name="service_type",
    )

    # Unique ID
    id = Column(Integer, primary_key=True)

    # Name of the service.
    service = Column(String(255), index=True, nullable=False)

    # Type of the service -- monitor, coverage provider, or script.
    # If the service type does not fit into these categories, this field
    # can be left null.
    service_type = Column(service_type_enum, index=True, default=None)

    # The collection, if any, associated with this service -- some services
    # run separately on a number of collections.
    collection_id = Column(Integer, ForeignKey('collections.id'),
                           index=True, nullable=True)

    # The last time the service _started_ running.
    start = Column(DateTime, nullable=True)

    # The last time the service _finished_ running. In most cases this
    # is the 'timestamp' proper.
    finish = Column(DateTime)

    # A description of the things the service achieved during its last
    # run. Each service may decide for itself what counts as an
    # 'achievement'; this is just a way to distinguish services that
    # do a lot of things from services that do a few things, or to see
    # services that run to completion but don't actually do anything.
    achievements = Column(Unicode, nullable=True)

    # This column allows a service to keep one item of state between
    # runs. For example, a monitor that iterates over a database table
    # needs to keep track of the last database ID it processed.
    counter = Column(Integer, nullable=True)

    # The exception, if any, that stopped the service from running
    # during its previous run.
    exception = Column(Unicode, nullable=True)

    def __repr__(self):
        format = '%b %d, %Y at %H:%M'
        if self.finish:
            finish = self.finish.strftime(format)
        else:
            finish = None
        if self.start:
            start = self.start.strftime(format)
        else:
            start = None
        if self.collection:
            collection = self.collection.name
        else:
            collection = None

        message = "<Timestamp %s: collection=%s, start=%s finish=%s counter=%s>" % (
            self.service, collection, start, finish, self.counter
        )
        return message

    @classmethod
    def lookup(cls, _db, service, service_type, collection):
        return get_one(
            _db, Timestamp, service=service, service_type=service_type,
            collection=collection
        )

    @classmethod
    def value(cls, _db, service, service_type, collection):
        """Return the current value of the given Timestamp, if it exists.
        """
        stamp = cls.lookup(_db, service, service_type, collection)
        if not stamp:
            return None
        return stamp.finish

    @classmethod
    def stamp(
        cls, _db, service, service_type, collection=None, start=None,
        finish=None, achievements=None, counter=None, exception=None
    ):
        """Set a Timestamp, creating it if necessary.

        This should be called once a service has stopped running,
        whether or not it was able to complete its task.

        :param _db: A database connection.
        :param service: The name of the service associated with the Timestamp.

        :param service_type: The type of the service associated with
            the Timestamp. This must be one of the values in
            Timestmap.service_type_enum.
        :param collection: The Collection, if any, on which this service
            just ran.
        :param start: The time at which this service started running.
            Defaults to now.
        :param finish: The time at which this service stopped running.
            Defaults to now.
        :param achievements: A human-readable description of what the service
            did during its run.
        :param counter: An integer item of state that the service may use
            to track its progress between runs.
        :param exception: A stack trace for the exception, if any, which
            stopped the service from running.
        """
        if start is None and finish is None:
            start = finish = datetime.datetime.utcnow()
        elif start is None:
            start = finish
        elif finish is None:
            finish = start
        stamp, was_new = get_one_or_create(
            _db, Timestamp,
            service=service,
            service_type=service_type,
            collection=collection,
        )
        stamp.update(start, finish, achievements, counter, exception)

        # Committing immediately reduces the risk of contention.
        _db.commit()
        return stamp

    def update(self, start=None, finish=None, achievements=None,
               counter=None, exception=None):
        """Use a single method to update all the fields that aren't
        used to identify a Timestamp.
        """

        if start is not None:
            if start is self.CLEAR_VALUE:
                # In most cases, None is not a valid value for
                # Timestamp.start, but this can be overridden.
                start = None
            self.start = start
        if finish is not None:
            if finish is self.CLEAR_VALUE:
                # In most cases, None is not a valid value for
                # Timestamp.finish, but this can be overridden.
                finish = None
            self.finish = finish
        if achievements is not None:
            if achievements is self.CLEAR_VALUE:
                achievements = None
            self.achievements = achievements
        if counter is not None:
            if counter is self.CLEAR_VALUE:
                counter = None
            self.counter = counter

        # Unlike the other fields, None is the default value for
        # .exception, so passing in None to mean "use the default" and
        # None to mean "no exception" mean the same thing. But we'll
        # support CLEAR_VALUE anyway.
        if exception is self.CLEAR_VALUE:
            exception = None
        self.exception = exception

    def to_data(self):
        """Convert this Timestamp to an unfinalized TimestampData."""
        from ..metadata_layer import TimestampData
        return TimestampData(
            start=self.start, finish=self.finish,
            achievements=self.achievements, counter=self.counter
        )

    __table_args__ = (
        UniqueConstraint('service', 'collection_id'),
    )

class CoverageRecord(Base, BaseCoverageRecord):
    """A record of a Identifier being used as input into some process."""
    __tablename__ = 'coveragerecords'

    SET_EDITION_METADATA_OPERATION = 'set-edition-metadata'
    CHOOSE_COVER_OPERATION = 'choose-cover'
    REAP_OPERATION = 'reap'
    IMPORT_OPERATION = 'import'
    RESOLVE_IDENTIFIER_OPERATION = 'resolve-identifier'
    REPAIR_SORT_NAME_OPERATION = 'repair-sort-name'
    METADATA_UPLOAD_OPERATION = 'metadata-upload'

    id = Column(Integer, primary_key=True)
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)

    # If applicable, this is the ID of the data source that took the
    # Identifier as input.
    data_source_id = Column(
        Integer, ForeignKey('datasources.id')
    )
    operation = Column(String(255), default=None)

    timestamp = Column(DateTime, index=True)

    status = Column(BaseCoverageRecord.status_enum, index=True)
    exception = Column(Unicode, index=True)

    # If applicable, this is the ID of the collection for which
    # coverage has taken place. This is currently only applicable
    # for Metadata Wrangler coverage.
    collection_id = Column(
        Integer, ForeignKey('collections.id'), nullable=True
    )

    __table_args__ = (
        Index(
            'ix_identifier_id_data_source_id_operation',
            identifier_id, data_source_id, operation,
            unique=True, postgresql_where=collection_id.is_(None)),
        Index(
            'ix_identifier_id_data_source_id_operation_collection_id',
            identifier_id, data_source_id, operation, collection_id,
            unique=True
        ),
    )

    def __repr__(self):
        template = '<CoverageRecord: %(timestamp)s identifier=%(identifier_type)s/%(identifier)s data_source="%(data_source)s"%(operation)s status="%(status)s" %(exception)s>'
        return self.human_readable(template)

    def human_readable(self, template):
        """Interpolate data into a human-readable template."""
        if self.operation:
            operation = ' operation="%s"' % self.operation
        else:
            operation = ''
        if self.exception:
            exception = ' exception="%s"' % self.exception
        else:
            exception = ''
        return template % dict(
            timestamp=self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            identifier_type=self.identifier.type,
            identifier=self.identifier.identifier,
            data_source=self.data_source.name,
            operation=operation,
            status=self.status,
            exception=exception,
        )

    @classmethod
    def lookup(cls, edition_or_identifier, data_source, operation=None,
               collection=None):
        from .datasource import DataSource
        from .edition import Edition
        from .identifier import Identifier

        _db = Session.object_session(edition_or_identifier)
        if isinstance(edition_or_identifier, Identifier):
            identifier = edition_or_identifier
        elif isinstance(edition_or_identifier, Edition):
            identifier = edition_or_identifier.primary_identifier
        else:
            raise ValueError(
                "Cannot look up a coverage record for %r." % edition)

        if isinstance(data_source, (bytes, str)):
            data_source = DataSource.lookup(_db, data_source)

        return get_one(
            _db, CoverageRecord,
            identifier=identifier,
            data_source=data_source,
            operation=operation,
            collection=collection,
            on_multiple='interchangeable',
        )

    @classmethod
    def add_for(self, edition, data_source, operation=None, timestamp=None,
                status=BaseCoverageRecord.SUCCESS, collection=None):
        from .edition import Edition
        from .identifier import Identifier

        _db = Session.object_session(edition)
        if isinstance(edition, Identifier):
            identifier = edition
        elif isinstance(edition, Edition):
            identifier = edition.primary_identifier
        else:
            raise ValueError(
                "Cannot create a coverage record for %r." % edition)
        timestamp = timestamp or datetime.datetime.utcnow()
        coverage_record, is_new = get_one_or_create(
            _db, CoverageRecord,
            identifier=identifier,
            data_source=data_source,
            operation=operation,
            collection=collection,
            on_multiple='interchangeable'
        )
        coverage_record.status = status
        coverage_record.timestamp = timestamp
        return coverage_record, is_new

    @classmethod
    def bulk_add(cls, identifiers, data_source, operation=None, timestamp=None,
        status=BaseCoverageRecord.SUCCESS, exception=None, collection=None,
        force=False,
    ):
        """Create and update CoverageRecords so that every Identifier in
        `identifiers` has an identical record.
        """
        from .identifier import Identifier

        if not identifiers:
            # Nothing to do.
            return

        _db = Session.object_session(identifiers[0])
        timestamp = timestamp or datetime.datetime.utcnow()
        identifier_ids = [i.id for i in identifiers]

        equivalent_record = and_(
            cls.operation==operation,
            cls.data_source==data_source,
            cls.collection==collection,
        )

        updated_or_created_results = list()
        if force:
            # Make sure that works that previously had a
            # CoverageRecord for this operation have their timestamp
            # and status updated.
            update = cls.__table__.update().where(and_(
                cls.identifier_id.in_(identifier_ids),
                equivalent_record,
            )).values(
                dict(timestamp=timestamp, status=status, exception=exception)
            ).returning(cls.id, cls.identifier_id)
            updated_or_created_results = _db.execute(update).fetchall()

        already_covered = _db.query(cls.id, cls.identifier_id).filter(
            equivalent_record,
            cls.identifier_id.in_(identifier_ids),
        ).subquery()

        # Make sure that any identifiers that need a CoverageRecord get one.
        # The SELECT part of the INSERT...SELECT query.
        data_source_id = data_source.id
        collection_id = None
        if collection:
            collection_id = collection.id

        new_records = _db.query(
            Identifier.id.label('identifier_id'),
            literal(operation, type_=String(255)).label('operation'),
            literal(timestamp, type_=DateTime).label('timestamp'),
            literal(status, type_=BaseCoverageRecord.status_enum).label('status'),
            literal(exception, type_=Unicode).label('exception'),
            literal(data_source_id, type_=Integer).label('data_source_id'),
            literal(collection_id, type_=Integer).label('collection_id'),
        ).select_from(Identifier).outerjoin(
            already_covered, Identifier.id==already_covered.c.identifier_id,
        ).filter(already_covered.c.id==None)

        new_records = new_records.filter(Identifier.id.in_(identifier_ids))

        # The INSERT part.
        insert = cls.__table__.insert().from_select(
            [
                literal_column('identifier_id'),
                literal_column('operation'),
                literal_column('timestamp'),
                literal_column('status'),
                literal_column('exception'),
                literal_column('data_source_id'),
                literal_column('collection_id'),
            ],
            new_records
        ).returning(cls.id, cls.identifier_id)

        inserts = _db.execute(insert).fetchall()

        updated_or_created_results.extend(inserts)
        _db.commit()

        # Default return for the case when all of the identifiers were
        # ignored.
        new_records = list()
        ignored_identifiers = identifiers

        new_and_updated_record_ids = [r[0] for r in updated_or_created_results]
        impacted_identifier_ids = [r[1] for r in updated_or_created_results]

        if new_and_updated_record_ids:
            new_records = _db.query(cls).filter(cls.id.in_(
                new_and_updated_record_ids
            )).all()

        ignored_identifiers = [i for i in identifiers if i.id not in impacted_identifier_ids]

        return new_records, ignored_identifiers

Index("ix_coveragerecords_data_source_id_operation_identifier_id", CoverageRecord.data_source_id, CoverageRecord.operation, CoverageRecord.identifier_id)

class WorkCoverageRecord(Base, BaseCoverageRecord):
    """A record of some operation that was performed on a Work.
    This is similar to CoverageRecord, which operates on Identifiers,
    but since Work identifiers have no meaning outside of the database,
    we presume that all the operations involve internal work only,
    and as such there is no data_source_id.
    """
    __tablename__ = 'workcoveragerecords'

    CHOOSE_EDITION_OPERATION = 'choose-edition'
    CLASSIFY_OPERATION = 'classify'
    SUMMARY_OPERATION = 'summary'
    QUALITY_OPERATION = 'quality'
    GENERATE_OPDS_OPERATION = 'generate-opds'
    GENERATE_MARC_OPERATION = 'generate-marc'
    UPDATE_SEARCH_INDEX_OPERATION = 'update-search-index'

    id = Column(Integer, primary_key=True)
    work_id = Column(Integer, ForeignKey('works.id'), index=True)
    operation = Column(String(255), index=True, default=None)

    timestamp = Column(DateTime, index=True)

    status = Column(BaseCoverageRecord.status_enum, index=True)
    exception = Column(Unicode, index=True)

    __table_args__ = (
        UniqueConstraint('work_id', 'operation'),
    )

    def __repr__(self):
        if self.exception:
            exception = ' exception="%s"' % self.exception
        else:
            exception = ''
        template = '<WorkCoverageRecord: work_id=%s operation="%s" timestamp="%s"%s>'
        return template % (
            self.work_id, self.operation,
            self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            exception
        )

    @classmethod
    def lookup(self, work, operation):
        _db = Session.object_session(work)
        return get_one(
            _db, WorkCoverageRecord,
            work=work,
            operation=operation,
            on_multiple='interchangeable',
        )

    @classmethod
    def add_for(self, work, operation, timestamp=None,
                status=CoverageRecord.SUCCESS):
        _db = Session.object_session(work)
        timestamp = timestamp or datetime.datetime.utcnow()
        coverage_record, is_new = get_one_or_create(
            _db, WorkCoverageRecord,
            work=work,
            operation=operation,
            on_multiple='interchangeable'
        )
        coverage_record.status = status
        coverage_record.timestamp = timestamp
        return coverage_record, is_new

    @classmethod
    def bulk_add(self, works, operation, timestamp=None,
                 status=CoverageRecord.SUCCESS, exception=None):
        """Create and update WorkCoverageRecords so that every Work in
        `works` has an identical record.
        """
        from .work import Work

        if not works:
            # Nothing to do.
            return
        _db = Session.object_session(works[0])
        timestamp = timestamp or datetime.datetime.utcnow()
        work_ids = [w.id for w in works]

        # Make sure that works that previously had a
        # WorkCoverageRecord for this operation have their timestamp
        # and status updated.
        update = WorkCoverageRecord.__table__.update().where(
            and_(WorkCoverageRecord.work_id.in_(work_ids),
                 WorkCoverageRecord.operation==operation)
        ).values(dict(timestamp=timestamp, status=status, exception=exception))
        _db.execute(update)

        # Make sure that any works that are missing a
        # WorkCoverageRecord for this operation get one.

        # Works that already have a WorkCoverageRecord will be ignored
        # by the INSERT but handled by the UPDATE.
        already_covered = _db.query(WorkCoverageRecord.work_id).select_from(
            WorkCoverageRecord).filter(
                WorkCoverageRecord.work_id.in_(work_ids)
            ).filter(
                WorkCoverageRecord.operation==operation
            )

        # The SELECT part of the INSERT...SELECT query.
        new_records = _db.query(
            Work.id.label('work_id'),
            literal(operation, type_=String(255)).label('operation'),
            literal(timestamp, type_=DateTime).label('timestamp'),
            literal(status, type_=BaseCoverageRecord.status_enum).label('status')
        ).select_from(
            Work
        )
        new_records = new_records.filter(
            Work.id.in_(work_ids)
        ).filter(
            ~Work.id.in_(already_covered)
        )

        # The INSERT part.
        insert = WorkCoverageRecord.__table__.insert().from_select(
            [
                literal_column('work_id'),
                literal_column('operation'),
                literal_column('timestamp'),
                literal_column('status'),
            ],
            new_records
        )
        _db.execute(insert)

Index("ix_workcoveragerecords_operation_work_id", WorkCoverageRecord.operation, WorkCoverageRecord.work_id)
