# encoding: utf-8
# PolicyException LicensePool, LicensePoolDeliveryMechanism, DeliveryMechanism,
# RightsStatus
from nose.tools import set_trace

from . import (
    Base,
    create,
    flush,
    get_one,
    get_one_or_create,
)
from circulationevent import CirculationEvent
from complaint import Complaint
from constants import (
    DataSourceConstants,
    EditionConstants,
    LinkRelations,
    MediaTypes,
)
from hasfulltablecache import HasFullTableCache
from patron import (
    Patron,
    Loan,
    Hold,
)

import datetime
import logging
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    func,
    String,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func

class PolicyException(Exception):
    pass

class License(Base):
    """A single license for a work from a given source.

    TODO: This currently assumes all licenses for a pool have the same
    delivery mechanisms, which may not always be true.
    """

    __tablename__ = 'licenses'
    id = Column(Integer, primary_key=True)

    identifier = Column(Unicode)
    checkout_url = Column(Unicode)
    status_url = Column(Unicode)

    expires = Column(DateTime)
    remaining_checkouts = Column(Integer)
    concurrent_checkouts = Column(Integer)

    # A License belongs to one LicensePool.
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)

    # One License can have many Loans.
    loans = relationship(
        'Loan', backref='license', cascade='all, delete-orphan'
    )

    __table_args__ = (
        UniqueConstraint('identifier', 'license_pool_id'),
    )

    def loan_to(self, patron_or_client, **kwargs):
        loan, is_new = self.license_pool.loan_to(patron_or_client, **kwargs)
        loan.license = self
        return loan, is_new

    @property
    def is_perpetual(self):
        return (self.expires is None) and (self.remaining_checkouts is None)

    @property
    def is_time_limited(self):
        return self.expires is not None

    @property
    def is_loan_limited(self):
        return self.remaining_checkouts is not None

    @property
    def is_expired(self):
        now = datetime.datetime.utcnow()
        return ((self.expires and self.expires <= now) or
                (self.remaining_checkouts is not None and self.remaining_checkouts <= 0))

class LicensePool(Base):
    """A pool of undifferentiated licenses for a work from a given source.
    """
    __tablename__ = 'licensepools'
    id = Column(Integer, primary_key=True)

    # A LicensePool may be associated with a Work. (If it's not, no one
    # can check it out.)
    work_id = Column(Integer, ForeignKey('works.id'), index=True)

    # Each LicensePool is associated with one DataSource and one
    # Identifier.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)
    identifier_id = Column(Integer, ForeignKey('identifiers.id'), index=True)

    # Each LicensePool belongs to one Collection.
    collection_id = Column(Integer, ForeignKey('collections.id'),
                           index=True, nullable=False)

    # Each LicensePool has an Edition which contains the metadata used
    # to describe this book.
    presentation_edition_id = Column(Integer, ForeignKey('editions.id'), index=True)

    # If the source provides information about individual licenses, the
    # LicensePool may have many Licenses.
    licenses = relationship(
        'License', backref='license_pool', cascade='all, delete-orphan'
    )

    # One LicensePool can have many Loans.
    loans = relationship(
        'Loan', backref='license_pool', cascade='all, delete-orphan'
    )

    # One LicensePool can have many Holds.
    holds = relationship(
        'Hold', backref='license_pool', cascade='all, delete-orphan'
    )

    # One LicensePool can have many CirculationEvents
    circulation_events = relationship(
        "CirculationEvent", backref="license_pool",
        cascade='all, delete-orphan'
    )

    # One LicensePool can be associated with many Complaints.
    complaints = relationship(
        'Complaint', backref='license_pool', cascade='all, delete-orphan'
    )

    # The date this LicensePool was first created in our db
    # (the date we first discovered that ​we had that book in ​our collection).
    availability_time = Column(DateTime, index=True)

    # A LicensePool may be superceded by some other LicensePool
    # associated with the same Work. This may happen if it's an
    # open-access LicensePool and a better-quality version of the same
    # book is available from another Open-Access source.
    superceded = Column(Boolean, default=False)

    # A LicensePool that seemingly looks fine may be manually suppressed
    # to be temporarily or permanently removed from the collection.
    suppressed = Column(Boolean, default=False, index=True)

    # A textual description of a problem with this license pool
    # that caused us to suppress it.
    license_exception = Column(Unicode, index=True)

    open_access = Column(Boolean, index=True)
    last_checked = Column(DateTime, index=True)
    licenses_owned = Column(Integer, default=0, index=True)
    licenses_available = Column(Integer,default=0, index=True)
    licenses_reserved = Column(Integer,default=0)
    patrons_in_hold_queue = Column(Integer,default=0)

    # This lets us cache the work of figuring out the best open access
    # link for this LicensePool.
    _open_access_download_url = Column(Unicode, name="open_access_download_url")

    # A Collection can not have more than one LicensePool for a given
    # Identifier from a given DataSource.
    __table_args__ = (
        UniqueConstraint('identifier_id', 'data_source_id', 'collection_id'),
    )

    delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism",
        primaryjoin="and_(LicensePool.data_source_id==LicensePoolDeliveryMechanism.data_source_id, LicensePool.identifier_id==LicensePoolDeliveryMechanism.identifier_id)",
        foreign_keys=(data_source_id, identifier_id),
        uselist=True,
    )

    def __repr__(self):
        if self.identifier:
            identifier = "%s/%s" % (self.identifier.type,
                                    self.identifier.identifier)
        else:
            identifier = "unknown identifier"
        return "<LicensePool #%s for %s: owned=%d available=%d reserved=%d holds=%d>" % (
            self.id, identifier, self.licenses_owned, self.licenses_available,
            self.licenses_reserved, self.patrons_in_hold_queue
        )

    @classmethod
    def for_foreign_id(self, _db, data_source, foreign_id_type, foreign_id,
                       rights_status=None, collection=None, autocreate=True):
        """Find or create a LicensePool for the given foreign ID."""
        from collection import CollectionMissing
        from datasource import DataSource
        from identifier import Identifier
        if not collection:
            raise CollectionMissing()

        # Get the DataSource.
        if isinstance(data_source, basestring):
            data_source = DataSource.lookup(_db, data_source)

        # The type of the foreign ID must be the primary identifier
        # type for the data source.
        if (data_source.primary_identifier_type and
            foreign_id_type != data_source.primary_identifier_type
            and foreign_id_type != Identifier.DEPRECATED_NAMES.get(data_source.primary_identifier_type)
        ):
            raise ValueError(
                "License pools for data source '%s' are keyed to "
                "identifier type '%s' (not '%s', which was provided)" % (
                    data_source.name, data_source.primary_identifier_type,
                    foreign_id_type
                )
            )

        # Get the Identifier.
        identifier, ignore = Identifier.for_foreign_id(
            _db, foreign_id_type, foreign_id
            )

        kw = dict(data_source=data_source, identifier=identifier,
                  collection=collection)
        if rights_status:
            kw['rights_status'] = rights_status

        # Get the LicensePool that corresponds to the
        # DataSource/Identifier/Collection.
        if autocreate:
            license_pool, was_new = get_one_or_create(_db, LicensePool, **kw)
        else:
            license_pool = get_one(_db, LicensePool, **kw)
            was_new = False

        if was_new and not license_pool.availability_time:
            now = datetime.datetime.utcnow()
            license_pool.availability_time = now

        if was_new:
            # Set the LicensePool's initial values to indicate
            # that we don't actually know how many copies we own.
            license_pool.licenses_owned = 0
            license_pool.licenses_available = 0
            license_pool.licenses_reserved = 0
            license_pool.patrons_in_hold_queue = 0

        return license_pool, was_new

    @classmethod
    def with_no_work(cls, _db):
        """Find LicensePools that have no corresponding Work."""
        from work import Work
        return _db.query(LicensePool).outerjoin(Work).filter(
            Work.id==None).all()

    @classmethod
    def with_no_delivery_mechanisms(cls, _db):
        """Find LicensePools that have no delivery mechanisms.

        :return: A query object.
        """
        return _db.query(LicensePool).outerjoin(
            LicensePool.delivery_mechanisms).filter(
                LicensePoolDeliveryMechanism.id==None
            )

    @property
    def deliverable(self):
        """This LicensePool can actually be delivered to patrons.
        """
        return (
            (self.open_access or self.licenses_owned > 0)
            and any(
                [dm.delivery_mechanism.default_client_can_fulfill
                for dm in self.delivery_mechanisms]
            )
        )

    @classmethod
    def with_complaint(cls, library, resolved=False):
        """Return query for LicensePools that have at least one Complaint."""
        from collection import Collection
        from library import Library
        _db = Session.object_session(library)
        subquery = _db.query(
                LicensePool.id,
                func.count(LicensePool.id).label("complaint_count")
            ).select_from(LicensePool).join(
                LicensePool.collection).join(
                    Collection.libraries).filter(
                        Library.id==library.id
                    ).join(
                        LicensePool.complaints
                    ).group_by(
                        LicensePool.id
                    )

        if resolved == False:
            subquery = subquery.filter(Complaint.resolved == None)
        elif resolved == True:
            subquery = subquery.filter(Complaint.resolved != None)

        subquery = subquery.subquery()

        return _db.query(LicensePool).\
            join(subquery, LicensePool.id == subquery.c.id).\
            order_by(subquery.c.complaint_count.desc()).\
            add_columns(subquery.c.complaint_count)

    @property
    def open_access_source_priority(self):
        """What priority does this LicensePool's DataSource have in
        our list of open-access content sources?
        e.g. GITenberg books are prefered over Gutenberg books,
        because there's a defined process for fixing errors and they
        are more likely to have good cover art.
        """
        try:
            priority = DataSourceConstants.OPEN_ACCESS_SOURCE_PRIORITY.index(
                self.data_source.name
            )
        except ValueError, e:
            # The source of this download is not mentioned in our
            # priority list. Treat it as the lowest priority.
            priority = -1
        return priority

    def better_open_access_pool_than(self, champion):
        """ Is this open-access pool generally known for better-quality
        download files than the passed-in pool?
        """
        # A license pool with no identifier shouldn't happen, but it
        # definitely shouldn't be considered.
        if not self.identifier:
            return False

        # A non-open-access license pool is not eligible for consideration.
        if not self.open_access:
            return False

        # At this point we have a LicensePool that is at least
        # better than nothing.
        if not champion:
            return True

        # A suppressed license pool should never be used unless there is
        # no alternative.
        if self.suppressed:
            return False

        # If the previous champion is suppressed but we have a license pool
        # that's not, it's definitely better.
        if champion.suppressed:
            return True

        challenger_resource = self.best_open_access_link
        if not challenger_resource:
            # This LicensePool is supposedly open-access but we don't
            # actually know where the book is. It will be chosen only
            # if there is no alternative.
            return False

        champion_priority = champion.open_access_source_priority
        challenger_priority = self.open_access_source_priority

        if challenger_priority > champion_priority:
            return True

        if challenger_priority < champion_priority:
            return False

        if (self.data_source.name == DataSourceConstants.GUTENBERG
            and champion.data_source == self.data_source):
            # These two LicensePools are both from Gutenberg, and
            # normally this wouldn't matter, but higher Gutenberg
            # numbers beat lower Gutenberg numbers.
            champion_id = int(champion.identifier.identifier)
            challenger_id = int(self.identifier.identifier)

            if challenger_id > champion_id:
                logging.info(
                    "Gutenberg %d beats Gutenberg %d",
                    challenger_id, champion_id
                )
                return True
        return False

    def set_open_access_status(self):
        """Set .open_access based on whether there is currently
        an open-access LicensePoolDeliveryMechanism for this LicensePool.
        """
        old_status = self.open_access
        for dm in self.delivery_mechanisms:
            if dm.is_open_access:
                self.open_access = True
                break
        else:
            self.open_access = False

    def set_presentation_edition(self, equivalent_editions=None):
        """Create or update the presentation Edition for this LicensePool.
        The presentation Edition is made of metadata from all Editions
        associated with the LicensePool's identifier.
        :param equivalent_editions: An optional list of Edition objects
        that don't share this LicensePool's identifier but are associated
        with its equivalent identifiers in some way. This option is used
        to create Works on the Metadata Wrangler.
        :return: A boolean explaining whether any of the presentation
        information associated with this LicensePool actually changed.
        """
        from edition import Edition
        _db = Session.object_session(self)
        old_presentation_edition = self.presentation_edition
        changed = False

        editions = equivalent_editions
        if not editions:
            editions = self.identifier.primarily_identifies
        all_editions = list(Edition.sort_by_priority(editions, self.data_source))

        # Note: We can do a cleaner solution, if we refactor to not use metadata's
        # methods to update editions.  For now, we're choosing to go with the below approach.
        from ..metadata_layer import (
            Metadata,
            IdentifierData,
            ReplacementPolicy,
        )

        if len(all_editions) == 1:
            # There's only one edition associated with this
            # LicensePool. Use it as the presentation edition rather
            # than creating an identical composite.
            self.presentation_edition = all_editions[0]
        else:
            edition_identifier = IdentifierData(self.identifier.type, self.identifier.identifier)
            metadata = Metadata(data_source=DataSourceConstants.PRESENTATION_EDITION, primary_identifier=edition_identifier)

            for edition in all_editions:
                if (edition.data_source.name != DataSourceConstants.PRESENTATION_EDITION):
                    metadata.update(Metadata.from_edition(edition))

            # Note: Since this is a presentation edition it does not have a
            # license data source, even if one of the editions it was
            # created from does have a license data source.
            metadata._license_data_source = None
            metadata.license_data_source_obj = None
            edition, is_new = metadata.edition(_db)

            policy = ReplacementPolicy.from_metadata_source()
            self.presentation_edition, edition_core_changed = metadata.apply(
                edition, collection=self.collection, replace=policy
            )
            changed = changed or edition_core_changed

        presentation_changed = self.presentation_edition.calculate_presentation()
        changed = changed or presentation_changed

        # if the license pool is associated with a work, and the work currently has no presentation edition,
        # then do a courtesy call to the work, and tell it about the presentation edition.
        if self.work and not self.work.presentation_edition:
            self.work.set_presentation_edition(self.presentation_edition)

        return (
            self.presentation_edition != old_presentation_edition
            or changed
        )

    def add_link(self, rel, href, data_source, media_type=None,
                 content=None, content_path=None,
                 rights_status_uri=None, rights_explanation=None,
                 original_resource=None, transformation_settings=None,
                 ):
        """Add a link between this LicensePool and a Resource.

        :param rel: The relationship between this LicensePool and the resource
            on the other end of the link.
        :param href: The URI of the resource on the other end of the link.
        :param media_type: Media type of the representation associated
            with the resource.
        :param content: Content of the representation associated with the
            resource.
        :param content_path: Path (relative to DATA_DIRECTORY) of the
            representation associated with the resource.
        :param rights_status_uri: The URI of the RightsStatus for this resource.
        :param rights_explanation: A free text explanation of why the RightsStatus
            applies.
        :param original_resource: Another resource that this resource was derived from.
        :param transformation_settings: The settings used to transform the original
            resource into this resource.
        """
        return self.identifier.add_link(
            rel, href, data_source, media_type, content, content_path,
            rights_status_uri, rights_explanation, original_resource,
            transformation_settings)

    def needs_update(self):
        """Is it time to update the circulation info for this license pool?"""
        now = datetime.datetime.utcnow()
        if not self.last_checked:
            # This pool has never had its circulation info checked.
            return True
        maximum_stale_time = self.data_source.extra.get(
            'circulation_refresh_rate_seconds')
        if maximum_stale_time is None:
            # This pool never needs to have its circulation info checked.
            return False
        age = now - self.last_checked
        return age > maximum_stale_time

    def update_availability(
            self, new_licenses_owned, new_licenses_available,
            new_licenses_reserved, new_patrons_in_hold_queue,
            analytics=None, as_of=None):
        """Update the LicensePool with new availability information.
        Log the implied changes with the analytics provider.
        """
        changes_made = False
        _db = Session.object_session(self)
        if not as_of:
            as_of = datetime.datetime.utcnow()
        elif as_of == CirculationEvent.NO_DATE:
            # The caller explicitly does not want
            # LicensePool.last_checked to be updated.
            as_of = None

        old_licenses_owned = self.licenses_owned
        old_licenses_available = self.licenses_available
        old_licenses_reserved = self.licenses_reserved
        old_patrons_in_hold_queue = self.patrons_in_hold_queue

        for old_value, new_value, more_event, fewer_event in (
                [self.patrons_in_hold_queue,  new_patrons_in_hold_queue,
                 CirculationEvent.DISTRIBUTOR_HOLD_PLACE, CirculationEvent.DISTRIBUTOR_HOLD_RELEASE],
                [self.licenses_available, new_licenses_available,
                 CirculationEvent.DISTRIBUTOR_CHECKIN, CirculationEvent.DISTRIBUTOR_CHECKOUT],
                [self.licenses_reserved, new_licenses_reserved,
                 CirculationEvent.DISTRIBUTOR_AVAILABILITY_NOTIFY, None],
                [self.licenses_owned, new_licenses_owned,
                 CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
                 CirculationEvent.DISTRIBUTOR_LICENSE_REMOVE]):
            if new_value is None:
                continue
            if old_value == new_value:
                continue
            changes_made = True

            if old_value < new_value:
                event_name = more_event
            else:
                event_name = fewer_event

            if not event_name:
                continue

            self.collect_analytics_event(
                analytics, event_name, as_of, old_value, new_value
            )

        # Update the license pool with the latest information.
        any_data = False
        if new_licenses_owned is not None:
            self.licenses_owned = new_licenses_owned
            any_data = True
        if new_licenses_available is not None:
            self.licenses_available = new_licenses_available
            any_data = True
        if new_licenses_reserved is not None:
            self.licenses_reserved = new_licenses_reserved
            any_data = True
        if new_patrons_in_hold_queue is not None:
            self.patrons_in_hold_queue = new_patrons_in_hold_queue
            any_data = True

        if as_of and (any_data or changes_made):
            # Sometimes update_availability is called with no actual
            # numbers, but that's not the case this time. We got
            # numbers and they may have even changed our view of the
            # LicensePool.
            self.last_checked = as_of
            if self.work:
                self.work.last_update_time = as_of

        if changes_made:
            message, args = self.circulation_changelog(
                old_licenses_owned, old_licenses_available,
                old_licenses_reserved, old_patrons_in_hold_queue
            )
            logging.info(message, *args)

        return changes_made

    def collect_analytics_event(self, analytics, event_name, as_of,
                                old_value, new_value):
        if not analytics:
            return
        for library in self.collection.libraries:
            analytics.collect_event(
                library, self, event_name, as_of,
                old_value=old_value, new_value=new_value
            )

    def update_availability_from_delta(self, event_type, event_date, delta, analytics=None):
        """Call update_availability based on a single change seen in the
        distributor data, rather than a complete snapshot of
        distributor information as of a certain time.
        This information is unlikely to be completely accurate, but it
        should suffice until more accurate information can be
        obtained.
        No CirculationEvent is created until `update_availability` is
        called.
        Events must be processed in chronological order. Any event
        that happened than `LicensePool.last_checked` is ignored, and
        calling this method will update `LicensePool.last_checked` to
        the time of the event.
        :param event_type: A CirculationEvent constant representing the
        type of change that was seen.
        :param event_date: A datetime corresponding to when the
        change was seen.
        :param delta: The magnitude of the change that was seen.
        """
        ignore = False
        if event_date != CirculationEvent.NO_DATE and self.last_checked and event_date < self.last_checked:
            # This is an old event and its effect on availability has
            # already been taken into account.
            ignore = True

        elif self.last_checked and event_date == CirculationEvent.NO_DATE:
            # We have a history for this LicensePool and we don't know
            # where this event fits into that history. Ignore the
            # event.
            ignore = True

        if not ignore:
            (new_licenses_owned, new_licenses_available,
             new_licenses_reserved,
             new_patrons_in_hold_queue) = self._calculate_change_from_one_event(
                 event_type, delta
             )

            changes_made = self.update_availability(
                new_licenses_owned, new_licenses_available,
                new_licenses_reserved, new_patrons_in_hold_queue,
                analytics=analytics, as_of=event_date
            )
        if ignore or not changes_made:
            # Even if the event was ignored or didn't actually change
            # availability, we want to record receipt of the event
            # in the analytics.
            self.collect_analytics_event(
                analytics, event_type, event_date, 0, 0
            )

    def _calculate_change_from_one_event(self, type, delta):
        new_licenses_owned = self.licenses_owned
        new_licenses_available = self.licenses_available
        new_licenses_reserved = self.licenses_reserved
        new_patrons_in_hold_queue = self.patrons_in_hold_queue

        def deduct(value):
            # It's impossible for any of these numbers to be
            # negative.
            return max(value-delta, 0)

        CE = CirculationEvent
        added = False
        if type == CE.DISTRIBUTOR_HOLD_PLACE:
            new_patrons_in_hold_queue += delta
            if new_licenses_available:
                # If someone has put a book on hold, it must not be
                # immediately available.
                new_licenses_available = 0
        elif type == CE.DISTRIBUTOR_HOLD_RELEASE:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
        elif type == CE.DISTRIBUTOR_CHECKIN:
            if self.patrons_in_hold_queue == 0:
                new_licenses_available += delta
            else:
                # When there are patrons in the hold queue, checking
                # in a single book does not make new licenses
                # available.  Checking in more books than there are
                # patrons in the hold queue _does_ make books
                # available.  However, in neither case do patrons
                # leave the hold queue. That will happen in the near
                # future as DISTRIBUTOR_AVAILABILITY_NOTIFICATION events
                # are sent out.
                if delta > new_patrons_in_hold_queue:
                    new_licenses_available += (delta-new_patrons_in_hold_queue)
        elif type == CE.DISTRIBUTOR_CHECKOUT:
            if new_licenses_available == 0:
                # The only way to borrow books while there are no
                # licenses available is to borrow reserved copies.
                new_licenses_reserved = deduct(new_licenses_reserved)
            else:
                # We don't know whether this checkout came from
                # licenses available or from a lingering reserved
                # copy, but in most cases it came from licenses
                # available.
                new_licenses_available = deduct(new_licenses_available)
        elif type == CE.DISTRIBUTOR_LICENSE_ADD:
            new_licenses_owned += delta
            # Newly added licenses start out as available, unless there
            # are patrons in the holds queue.
            if new_patrons_in_hold_queue == 0:
                new_licenses_available += delta
        elif type == CE.DISTRIBUTOR_LICENSE_REMOVE:
            new_licenses_owned = deduct(new_licenses_owned)
            # We can't say whether or not the removed licenses should
            # be deducted from the list of available licenses, because they
            # might already be checked out.
        elif type == CE.DISTRIBUTOR_AVAILABILITY_NOTIFY:
            new_patrons_in_hold_queue = deduct(new_patrons_in_hold_queue)
            new_licenses_reserved += delta
        if new_licenses_owned < new_licenses_available:
            # It's impossible to have more licenses available than
            # owned. We don't know whether this means there are some
            # extra licenses we never heard about, or whether some
            # licenses expired without us being notified, but the
            # latter is more likely.
            new_licenses_available = new_licenses_owned

        return (new_licenses_owned, new_licenses_available,
                new_licenses_reserved, new_patrons_in_hold_queue)

    def circulation_changelog(self, old_licenses_owned, old_licenses_available,
                              old_licenses_reserved, old_patrons_in_hold_queue):
        """Generate a log message describing a change to the circulation.
        :return: a 2-tuple (message, args) suitable for passing into
        logging.info or a similar method
        """
        edition = self.presentation_edition
        message = u'CHANGED '
        args = []
        if self.identifier:
            identifier_template = '%s/%s'
            identifier_args = [self.identifier.type, self.identifier.identifier]
        else:
            identifier_template = '%s'
            identifier_args = [self.identifier]
        if edition:
            message += u'%s "%s" %s (' + identifier_template + ')'
            args.extend([edition.medium,
                         edition.title or "[NO TITLE]",
                         edition.author or "[NO AUTHOR]"]
                    )
            args.extend(identifier_args)
        else:
            message += identifier_template
            args.extend(identifier_args)

        def _part(message, args, string, old_value, new_value):
            if old_value != new_value:
                args.extend([string, old_value, new_value])
                message += ' %s: %s=>%s'
            return message, args

        message, args = _part(
            message, args, "OWN", old_licenses_owned, self.licenses_owned
        )

        message, args = _part(
            message, args, "AVAIL", old_licenses_available,
            self.licenses_available
        )

        message, args = _part(
            message, args, "RSRV", old_licenses_reserved,
            self.licenses_reserved
        )

        message, args =_part(
            message, args, "HOLD", old_patrons_in_hold_queue,
            self.patrons_in_hold_queue
        )
        return message, tuple(args)

    def loan_to(self, patron_or_client, start=None, end=None, fulfillment=None, external_identifier=None):
        _db = Session.object_session(patron_or_client)
        kwargs = dict(start=start or datetime.datetime.utcnow(),
                      end=end)
        if isinstance(patron_or_client, Patron):
            loan, is_new = get_one_or_create(
                _db, Loan, patron=patron_or_client, license_pool=self,
                create_method_kwargs=kwargs)
        else:
            # An IntegrationClient can have multiple loans, so this always creates
            # a new loan rather than returning an existing loan.
            loan, is_new = create(
                _db, Loan, integration_client=patron_or_client, license_pool=self,
                create_method_kwargs=kwargs)
        if fulfillment:
            loan.fulfillment = fulfillment
        if external_identifier:
            loan.external_identifier = external_identifier
        return loan, is_new

    def on_hold_to(self, patron_or_client, start=None, end=None, position=None, external_identifier=None):
        _db = Session.object_session(patron_or_client)
        if isinstance(patron_or_client, Patron) and not patron_or_client.library.allow_holds:
            raise PolicyException("Holds are disabled for this library.")
        start = start or datetime.datetime.utcnow()
        if isinstance(patron_or_client, Patron):
            hold, new = get_one_or_create(
                _db, Hold, patron=patron_or_client, license_pool=self)
        else:
            # An IntegrationClient can have multiple holds, so this always creates
            # a new hold rather than returning an existing loan.
            hold, new = create(
                _db, Hold, integration_client=patron_or_client, license_pool=self)
        hold.update(start, end, position)
        if external_identifier:
            hold.external_identifier = external_identifier
        return hold, new

    def best_available_license(self):
        """Determine the next license that should be lent out for this pool.

        Time-limited licenses and perpetual licenses are the best. It doesn't matter which
        is used first, unless a time-limited license would expire within the loan period, in
        which case it's better to loan the time-limited license so the perpetual one is still
        available. We can handle this by always loaning the time-limited one first, followed
        by perpetual. If there is more than one time-limited license, it's better to use the one
        expiring soonest.

        If no time-limited or perpetual licenses are available, the next best is a loan-limited
        license. We should choose the license with the most remaining loans, so that we'll
        maximize the number of concurrent checkouts available in the future.

        The worst option would be pay-per-use, but we don't yet support any distributors that
        offer that model.
        """
        best = None
        now = datetime.datetime.utcnow()

        for license in self.licenses:
            if license.is_expired:
                continue

            active_loan_count = len([l for l in license.loans if not l.end or l.end > now])
            if active_loan_count >= license.concurrent_checkouts:
                continue

            if (
                not best or
                (license.is_time_limited and not best.is_time_limited) or
                (license.is_time_limited and best.is_time_limited and license.expires < best.expires) or
                (license.is_perpetual and not best.is_time_limited) or
                (license.is_loan_limited and best.is_loan_limited and license.remaining_checkouts > best.remaining_checkouts)
                ):
                best = license

        return best

    @classmethod
    def consolidate_works(cls, _db, batch_size=10):
        """Assign a (possibly new) Work to every unassigned LicensePool."""
        from edition import Edition
        a = 0
        lps = cls.with_no_work(_db)
        logging.info(
            "Assigning Works to %d LicensePools with no Work.", len(lps)
        )
        for unassigned in lps:
            etext, new = unassigned.calculate_work()
            if not etext:
                # We could not create a work for this LicensePool,
                # most likely because it does not yet have any
                # associated Edition.
                continue
            a += 1
            logging.info("When consolidating works, created %r", etext)
            if a and not a % batch_size:
                _db.commit()
        _db.commit()


    def calculate_work(
        self, known_edition=None, exclude_search=False,
        even_if_no_title=False
    ):
        """Find or create a Work for this LicensePool.
        A pool that is not open-access will always have its own
        Work. Open-access LicensePools will be grouped together with
        other open-access LicensePools based on the permanent work ID
        of the LicensePool's presentation edition.
        :param even_if_no_title: Ordinarily this method will refuse to
        create a Work for a LicensePool whose Edition has no title.
        However, in components that don't present information directly
        to readers, it's sometimes useful to create a Work even if the
        title is unknown. In that case, pass in even_if_no_title=True
        and the Work will be created.
        TODO: I think known_edition is mostly useless. We should
        either remove it or replace it with a boolean that stops us
        from calling set_presentation_edition() and assumes we've
        already done that work.
        """
        from work import Work

        if not self.identifier:
            # A LicensePool with no Identifier should never have a Work.
            self.work = None
            return None, False

        if known_edition:
            presentation_edition = known_edition
        else:
            self.set_presentation_edition()
            presentation_edition = self.presentation_edition
        if presentation_edition:
            if self not in presentation_edition.is_presentation_for:
                raise ValueError(
                    "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!"
                )

        logging.info("Calculating work for %r", presentation_edition)
        if not presentation_edition:
            # We don't have any information about the identifier
            # associated with this LicensePool, so we can't create a work.
            logging.warn("NO EDITION for %s, cowardly refusing to create work.",
                     self.identifier)

            # If there was a work associated with this LicensePool,
            # it was by mistake. Remove it.
            self.work = None
            return None, False

        if not presentation_edition.title or not presentation_edition.author:
            presentation_edition.calculate_presentation()

        if not presentation_edition.title and not even_if_no_title:
            if presentation_edition.work:
                logging.warn(
                    "Edition %r has no title but has a Work assigned. This will not stand.", presentation_edition
                )
            else:
                logging.info("Edition %r has no title and it will not get a Work.", presentation_edition)
            self.work = None
            self.work_id = None
            return None, False

        presentation_edition.calculate_permanent_work_id()

        _db = Session.object_session(self)
        work = None
        is_new = False
        licensepools_changed = False
        if self.open_access and presentation_edition.permanent_work_id:
            # This is an open-access book. Use the Work for all
            # open-access books associated with this book's permanent
            # work ID.
            #
            # If the dataset is in an inconsistent state, calling
            # Work.open_access_for_permanent_work_id may result in works being
            # merged.
            work, is_new = Work.open_access_for_permanent_work_id(
                _db, presentation_edition.permanent_work_id,
                presentation_edition.medium, presentation_edition.language
            )

            # Run a sanity check to make sure every LicensePool
            # associated with this Work actually belongs there. This
            # may result in new Works being created.
            #
            # This could go into Work.for_permanent_work_id, but that
            # could conceivably lead to an infinite loop, or at least
            # a very long recursive call, so I've put it here.
            work.make_exclusive_open_access_for_permanent_work_id(
                presentation_edition.permanent_work_id,
                presentation_edition.medium,
                presentation_edition.language,
            )
            self.work = work
            licensepools_changed = True

        # All LicensePools with a given Identifier must share a work.
        existing_works = set([x.work for x in self.identifier.licensed_through])
        if len(existing_works) > 1:
            logging.warn(
                "LicensePools for %r have more than one Work between them. Removing them all and starting over.", self.identifier
            )
            for lp in self.identifier.licensed_through:
                lp.work = None
                if lp.presentation_edition:
                    lp.presentation_edition.work = None
        else:
            # There is a consensus Work for this Identifier.
            [self.work] = existing_works

        if self.work:
            # This pool is already associated with a Work. Use that
            # Work.
            work = self.work
        elif presentation_edition.work:
            # This pool's presentation edition is already associated with
            # a Work. Use that Work.
            work = presentation_edition.work
            self.work = work

        if work:
            # There is already a Work associated with this LicensePool,
            # but we need to run a sanity check because occasionally
            # LicensePools get mis-grouped due to bugs.
            #
            # A commercially-licensed book should have a Work to
            # itself. All other LicensePools need to be kicked out and
            # associated with some other work.
            #
            # This won't cause an infinite recursion because we're
            # setting pool.work to None before calling
            # pool.calculate_work(), and the recursive call only
            # happens if self.work is set.
            for pool in list(work.license_pools):
                if pool is self:
                    continue
                if not (self.open_access and pool.open_access):
                    pool.work = None
                    pool.calculate_work(
                        exclude_search=exclude_search,
                        even_if_no_title=even_if_no_title
                    )
                    licensepools_changed = True

        else:
            # There is no better choice than creating a brand new Work.
            is_new = True
            logging.info(
                "Creating a new work for %r" % presentation_edition.title
            )
            work = Work()
            _db = Session.object_session(self)
            _db.add(work)
            flush(_db)
            licensepools_changed = True

        # Associate this LicensePool and its Edition with the work we
        # chose or created.
        if not self in work.license_pools:
            work.license_pools.append(self)
            licensepools_changed = True

        # Recalculate the display information for the Work. Either the
        # associated LicensePools have changed, which may have caused
        # the Work's presentation Edition to change, or
        # the caller has reason to believe that the presentation Edition
        # is changing for some other reason.
        work.calculate_presentation(exclude_search=exclude_search)

        # Ensure that all LicensePools with this Identifier share
        # the same Work. (We may have wiped out their .work earlier
        # in this method.)
        for lp in self.identifier.licensed_through:
            lp.work = work

        if is_new:
            logging.info("Created a new work: %r", work)

        # All done!
        return work, is_new


    @property
    def open_access_links(self):
        """Yield all open-access Resources for this LicensePool."""
        from identifier import Identifier
        open_access = LinkRelations.OPEN_ACCESS_DOWNLOAD
        _db = Session.object_session(self)
        if not self.identifier:
            return
        q = Identifier.resources_for_identifier_ids(
            _db, [self.identifier.id], open_access
        )
        for resource in q:
            yield resource

    @property
    def open_access_download_url(self):
        """Alias for best_open_access_link.
        If _open_access_download_url is currently None, this will set
        to a good value if possible.
        """
        return self.best_open_access_link

    @property
    def best_open_access_link(self):
        """Find the best open-access link for this LicensePool.
        Cache it so that the next access will be faster.
        """
        if not self.open_access:
            return None
        if not self._open_access_download_url:
            url = None
            resource = self.best_open_access_resource
            if resource and resource.representation:
                url = resource.representation.public_url
            self._open_access_download_url = url
        return self._open_access_download_url

    @property
    def best_open_access_resource(self):
        """Determine the best open-access Resource currently provided by this
        LicensePool.
        """
        best = None
        best_priority = -1
        for resource in self.open_access_links:
            if not any(
                    [resource.representation and
                     resource.representation.media_type and
                     resource.representation.media_type.startswith(x)
                     for x in MediaTypes.SUPPORTED_BOOK_MEDIA_TYPES]):
                # This representation is not in a media type we
                # support. We can't serve it, so we won't consider it.
                continue

            data_source_priority = self.open_access_source_priority
            if not best or data_source_priority > best_priority:
                # Something is better than nothing.
                best = resource
                best_priority = data_source_priority
                continue

            if (best.data_source.name==DataSourceConstants.GUTENBERG
                and resource.data_source.name==DataSourceConstants.GUTENBERG
                and 'noimages' in best.representation.public_url
                and not 'noimages' in resource.representation.public_url):
                # A Project Gutenberg-ism: an epub without 'noimages'
                # in the filename is better than an epub with
                # 'noimages' in the filename.
                best = resource
                best_priority = data_source_priority
                continue

        return best

    @property
    def best_license_link(self):
        """Find the best available licensing link for the work associated
        with this LicensePool.
        # TODO: This needs work and may not be necessary anymore.
        """
        edition = self.edition
        if not edition:
            return self, None
        link = edition.best_open_access_link
        if link:
            return self, link

        # Either this work is not open-access, or there was no epub
        # link associated with it.
        work = self.work
        for pool in work.license_pools:
            edition = pool.edition
            link = edition.best_open_access_link
            if link:
                return pool, link
        return self, None

    def set_delivery_mechanism(self, *args, **kwargs):
        """Ensure that this LicensePool (and any other LicensePools for the same
        book) have a LicensePoolDeliveryMechanism for this media type,
        DRM scheme, rights status, and resource.
        """
        return LicensePoolDeliveryMechanism.set(
            self.data_source, self.identifier, *args, **kwargs
        )

Index("ix_licensepools_data_source_id_identifier_id_collection_id", LicensePool.collection_id, LicensePool.data_source_id, LicensePool.identifier_id, unique=True)

class LicensePoolDeliveryMechanism(Base):
    """A mechanism for delivering a specific book from a specific
    distributor.
    It's presumed that all LicensePools for a given DataSource and
    Identifier have the same set of LicensePoolDeliveryMechanisms.
    This is mostly an association class between DataSource, Identifier and
    DeliveryMechanism, but it also may incorporate a specific Resource
    (i.e. a static link to a downloadable file) which explains exactly
    where to go for delivery.
    """
    __tablename__ = 'licensepooldeliveries'

    id = Column(Integer, primary_key=True)

    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True, nullable=False
    )

    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True, nullable=False
    )

    delivery_mechanism_id = Column(
        Integer, ForeignKey('deliverymechanisms.id'),
        index=True,
        nullable=False
    )

    resource_id = Column(Integer, ForeignKey('resources.id'), nullable=True)

    # One LicensePoolDeliveryMechanism may fulfill many Loans.
    fulfills = relationship("Loan", backref="fulfillment")

    # One LicensePoolDeliveryMechanism may be associated with one RightsStatus.
    rightsstatus_id = Column(
        Integer, ForeignKey('rightsstatus.id'), index=True)

    @classmethod
    def set(cls, data_source, identifier, content_type, drm_scheme, rights_uri,
            resource=None, autocommit=True):
        """Register the fact that a distributor makes a title available in a
        certain format.

        :param data_source: A DataSource identifying the distributor.
        :param identifier: An Identifier identifying the title.
        :param content_type: The title is available in this media type.
        :param drm_scheme: Access to the title is confounded by this
            DRM scheme.
        :param rights_uri: A URI representing the public's rights to the
            title.
        :param resource: A Resource representing the book itself in
            a freely redistributable form.
        :param autocommit: Commit the database session immediately if
            anything changes in the database. If you're already inside
            a nested transaction, pass in False here to avoid
            committing prematurely, but understand that if a
            LicensePool's open-access status changes as a result of
            calling this method, the change may not be properly
            reflected in LicensePool.open_access.
        """
        _db = Session.object_session(data_source)
        delivery_mechanism, ignore = DeliveryMechanism.lookup(
            _db, content_type, drm_scheme
        )
        rights_status = RightsStatus.lookup(_db, rights_uri)
        lpdm, dirty = get_one_or_create(
            _db, LicensePoolDeliveryMechanism,
            identifier=identifier,
            data_source=data_source,
            delivery_mechanism=delivery_mechanism,
            resource=resource
        )
        if not lpdm.rights_status or rights_status.uri != RightsStatus.UNKNOWN:
            # We have better information available about the
            # rights status of this delivery mechanism.
            lpdm.rights_status = rights_status
            dirty = True

        if dirty:
            # TODO: We need to explicitly commit here so that
            # LicensePool.delivery_mechanisms gets updated. It would be
            # better if we didn't have to do this, but I haven't been able
            # to get LicensePool.delivery_mechanisms to notice that it's
            # out of date.
            if autocommit:
                _db.commit()

            # Creating or modifying a LPDM might change the open-access status
            # of all LicensePools for that DataSource/Identifier.
            for pool in lpdm.license_pools:
                pool.set_open_access_status()
        return lpdm

    @property
    def is_open_access(self):
        """Is this an open-access delivery mechanism?"""
        return (self.rights_status
                and self.rights_status.uri in RightsStatus.OPEN_ACCESS)

    def compatible_with(self, other):
        """Can a single loan be fulfilled with both this
        LicensePoolDeliveryMechanism and the given one?

        :param other: A LicensePoolDeliveryMechanism.
        """
        if not isinstance(other, LicensePoolDeliveryMechanism):
            return False

        if other.id==self.id:
            # They two LicensePoolDeliveryMechanisms are the same object.
            return True

        # The two LicensePoolDeliveryMechanisms must be different ways
        # of getting the same book from the same source.
        if other.identifier_id != self.identifier_id:
            return False
        if other.data_source_id != self.data_source_id:
            return False

        if other.delivery_mechanism_id == self.delivery_mechanism_id:
            # We have two LicensePoolDeliveryMechanisms for the same
            # underlying delivery mechanism. This can happen when an
            # open-access book gets its content mirrored to two
            # different places.
            return True

        # If the DeliveryMechanisms themselves are compatible, then the
        # LicensePoolDeliveryMechanisms are compatible.
        #
        # In practice, this means that either the two
        # DeliveryMechanisms are the same or that one of them is a
        # streaming mechanism.
        open_access_rules = self.is_open_access and other.is_open_access
        return (
            other.delivery_mechanism
            and self.delivery_mechanism.compatible_with(
                other.delivery_mechanism, open_access_rules
            )
        )

    def delete(self):
        """Delete a LicensePoolDeliveryMechanism."""
        _db = Session.object_session(self)
        pools = list(self.license_pools)
        _db.delete(self)

        # TODO: We need to explicitly commit here so that
        # LicensePool.delivery_mechanisms gets updated. It would be
        # better if we didn't have to do this, but I haven't been able
        # to get LicensePool.delivery_mechanisms to notice that it's
        # out of date.
        _db.commit()

        # The deletion of a LicensePoolDeliveryMechanism might affect
        # the open-access status of its associated LicensePools.
        for pool in pools:
            pool.set_open_access_status()

    def set_rights_status(self, uri):
        _db = Session.object_session(self)
        status = RightsStatus.lookup(_db, uri)
        self.rights_status = status
        # A change to a LicensePoolDeliveryMechanism's rights status
        # might affect the open-access status of its associated
        # LicensePools.
        for pool in self.license_pools:
            pool.set_open_access_status()
        return status

    @property
    def license_pools(self):
        """Find all LicensePools for this LicensePoolDeliveryMechanism.
        """
        _db = Session.object_session(self)
        return _db.query(LicensePool).filter(
            LicensePool.data_source==self.data_source).filter(
                LicensePool.identifier==self.identifier)

    def __repr__(self):
        return "<LicensePoolDeliveryMechanism: data_source=%s, identifier=%r, mechanism=%r>" % (self.data_source, self.identifier, self.delivery_mechanism)

    __table_args__ = (
        UniqueConstraint('data_source_id', 'identifier_id',
                         'delivery_mechanism_id', 'resource_id'),
    )

# The uniqueness constraint doesn't enforce uniqueness when one of the
# fields is null, and one of these fields -- resource_id -- is
# _usually_ null. So we also need a unique partial index to properly
# enforce the constraint.
Index(
    'ix_licensepooldeliveries_unique_when_no_resource',
    LicensePoolDeliveryMechanism.data_source_id,
    LicensePoolDeliveryMechanism.identifier_id,
    LicensePoolDeliveryMechanism.delivery_mechanism_id,
    unique=True,
    postgresql_where=(LicensePoolDeliveryMechanism.resource_id==None)
)


class DeliveryMechanism(Base, HasFullTableCache):
    """A technique for delivering a book to a patron.
    There are two parts to this: a DRM scheme and a content
    type. Either may be identified with a MIME media type
    (e.g. "application/vnd.adobe.adept+xml" or "application/epub+zip") or an
    informal name ("Kindle via Amazon").
    """
    KINDLE_CONTENT_TYPE = u"Kindle via Amazon"
    NOOK_CONTENT_TYPE = u"Nook via B&N"
    STREAMING_TEXT_CONTENT_TYPE = u"Streaming Text"
    STREAMING_AUDIO_CONTENT_TYPE = u"Streaming Audio"
    STREAMING_VIDEO_CONTENT_TYPE = u"Streaming Video"

    NO_DRM = None
    ADOBE_DRM = u"application/vnd.adobe.adept+xml"
    FINDAWAY_DRM = u"application/vnd.librarysimplified.findaway.license+json"
    KINDLE_DRM = u"Kindle DRM"
    NOOK_DRM = u"Nook DRM"
    STREAMING_DRM = u"Streaming"

    # This represents the DRM system used by the app called 'Overdrive'
    # and associated with the application/x-od-media media type.
    OVERDRIVE_DRM = u"Overdrive DRM"

    # This represents the DRM system used by the app called 'Libby' and
    # associated with the
    # application/vnd.overdrive.circulation.api+json media type and
    # its profiles.
    LIBBY_DRM = u"Libby DRM"

    BEARER_TOKEN = u"application/vnd.librarysimplified.bearer-token+json"
    FEEDBOOKS_AUDIOBOOK_DRM = u"http://www.feedbooks.com/audiobooks/access-restriction"

    FEEDBOOKS_AUDIOBOOK_PROFILE = ';profile="%s"' % FEEDBOOKS_AUDIOBOOK_DRM
    STREAMING_PROFILE = ';profile="http://librarysimplified.org/terms/profiles/streaming-media"'
    MEDIA_TYPES_FOR_STREAMING = {
        STREAMING_TEXT_CONTENT_TYPE: MediaTypes.TEXT_HTML_MEDIA_TYPE,
        STREAMING_AUDIO_CONTENT_TYPE: MediaTypes.TEXT_HTML_MEDIA_TYPE,
    }

    __tablename__ = 'deliverymechanisms'
    id = Column(Integer, primary_key=True)
    content_type = Column(String)
    drm_scheme = Column(String)

    # Can the Library Simplified client fulfill a book with this
    # content type and this DRM scheme?
    default_client_can_fulfill = Column(Boolean, default=False, index=True)

    # These are the media type/DRM scheme combos known to be supported
    # by the default Library Simplified client.
    #
    # This is primarily used when deciding which books can be imported
    # from an OPDS For Distributors collection.
    default_client_can_fulfill_lookup = set([
        # EPUB books
        (MediaTypes.EPUB_MEDIA_TYPE, NO_DRM),
        (MediaTypes.EPUB_MEDIA_TYPE, ADOBE_DRM),

        # PDF books
        (MediaTypes.PDF_MEDIA_TYPE, NO_DRM),

        # Various audiobook formats
        (None, FINDAWAY_DRM),
        (MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, NO_DRM),

        (MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE, LIBBY_DRM),
    ])

    # If the default client supports a given media type with no DRM,
    # we can infer that the client _also_ supports that media type via
    # bearer token exchange.
    for media_type, drm in list(default_client_can_fulfill_lookup):
        if media_type is not None and drm == NO_DRM:
            default_client_can_fulfill_lookup.add(
                (media_type, BEARER_TOKEN)
            )

    license_pool_delivery_mechanisms = relationship(
        "LicensePoolDeliveryMechanism",
        backref="delivery_mechanism",
    )

    _cache = HasFullTableCache.RESET
    _id_cache = HasFullTableCache.RESET

    __table_args__ = (
        UniqueConstraint('content_type', 'drm_scheme'),
    )

    @property
    def name(self):
        if self.drm_scheme is self.NO_DRM:
            drm_scheme = "DRM-free"
        else:
            drm_scheme = self.drm_scheme
        return "%s (%s)" % (self.content_type, drm_scheme)

    def cache_key(self):
        return (self.content_type, self.drm_scheme)

    def __repr__(self):

        if self.default_client_can_fulfill:
            fulfillable = "fulfillable"
        else:
            fulfillable = "not fulfillable"

        return "<Delivery mechanism: %s, %s)>" % (
            self.name, fulfillable
        )

    @classmethod
    def lookup(cls, _db, content_type, drm_scheme):
        def lookup_hook():
            return get_one_or_create(
                _db, DeliveryMechanism, content_type=content_type,
                drm_scheme=drm_scheme
            )
        return cls.by_cache_key(_db, (content_type, drm_scheme), lookup_hook)

    @property
    def implicit_medium(self):
        """What would be a good setting for EditionConstants.MEDIUM for an edition
        available through this DeliveryMechanism?
        """
        if self.content_type in (
                MediaTypes.EPUB_MEDIA_TYPE,
                MediaTypes.PDF_MEDIA_TYPE,
                "Kindle via Amazon",
                "Streaming Text"):
            return EditionConstants.BOOK_MEDIUM
        elif self.content_type in (
                "Streaming Video" or self.content_type.startswith('video/')
        ):
            return EditionConstants.VIDEO_MEDIUM
        else:
            return None

    @classmethod
    def is_media_type(cls, x):
        "Does this string look like a media type?"
        if x is None:
            return False

        return any(x.startswith(prefix) for prefix in
                   ['vnd.', 'application', 'text', 'video', 'audio', 'image'])

    @property
    def is_streaming(self):
        return self.content_type in self.MEDIA_TYPES_FOR_STREAMING.keys()

    @property
    def drm_scheme_media_type(self):
        """Return the media type for this delivery mechanism's
        DRM scheme, assuming it's represented that way.
        """
        if self.is_media_type(self.drm_scheme):
            return self.drm_scheme
        return None

    @property
    def content_type_media_type(self):
        """Return the media type for this delivery mechanism's
        content type, assuming it's represented as a media type.
        """
        if self.is_media_type(self.content_type):
            if self.drm_scheme == self.FEEDBOOKS_AUDIOBOOK_DRM:
                return self.content_type + self.FEEDBOOKS_AUDIOBOOK_PROFILE
            return self.content_type

        media_type_for_streaming = self.MEDIA_TYPES_FOR_STREAMING.get(self.content_type)
        if media_type_for_streaming:
            return media_type_for_streaming + self.STREAMING_PROFILE

        return None

    def compatible_with(self, other, open_access_rules=False):
        """Can a single loan be fulfilled with both this delivery mechanism
        and the given one?

        :param other: A DeliveryMechanism
        :param open_access: If this is True, the rules for open-access
            fulfillment will be applied. If not, the stricted rules
            for commercial fulfillment will be applied.
        """
        if not isinstance(other, DeliveryMechanism):
            return False

        if self.id == other.id:
            # The two DeliveryMechanisms are the same.
            return True

        # Streaming delivery mechanisms can be used even when a
        # license pool is locked into a non-streaming delivery
        # mechanism.
        if self.is_streaming or other.is_streaming:
            return True

        # For an open-access book, loans are not locked to delivery
        # mechanisms, so as long as neither delivery mechanism has
        # DRM, they're compatible.
        if (open_access_rules and self.drm_scheme==self.NO_DRM
            and other.drm_scheme==self.NO_DRM):
            return True

        # For non-open-access books, locking a license pool to a
        # non-streaming delivery mechanism prohibits the use of any
        # other non-streaming delivery mechanism.
        return False

# The uniqueness constraint doesn't enforce uniqueness when one of the
# fields is null, and one of these fields -- drm_scheme -- is
# frequently null. So we also need a unique partial index to properly
# enforce the constraint.
Index(
    'ix_deliverymechanisms_unique_when_no_drm',
    DeliveryMechanism.content_type,
    unique=True,
    postgresql_where=(DeliveryMechanism.drm_scheme==None)
)


class RightsStatus(Base):

    """The terms under which a book has been made available to the general
    public.
    This will normally be 'in copyright', or 'public domain', or a
    Creative Commons license.
    """

    # Currently in copyright.
    IN_COPYRIGHT = u"http://librarysimplified.org/terms/rights-status/in-copyright"

    # Public domain in the USA.
    PUBLIC_DOMAIN_USA = u"http://librarysimplified.org/terms/rights-status/public-domain-usa"

    # Public domain in some unknown territory
    PUBLIC_DOMAIN_UNKNOWN = u"http://librarysimplified.org/terms/rights-status/public-domain-unknown"

    # Creative Commons Public Domain Dedication (No rights reserved)
    CC0 = u"https://creativecommons.org/publicdomain/zero/1.0/"

    # Creative Commons Attribution (CC BY)
    CC_BY = u"http://creativecommons.org/licenses/by/4.0/"

    # Creative Commons Attribution-ShareAlike (CC BY-SA)
    CC_BY_SA = u"https://creativecommons.org/licenses/by-sa/4.0"

    # Creative Commons Attribution-NoDerivs (CC BY-ND)
    CC_BY_ND = u"https://creativecommons.org/licenses/by-nd/4.0"

    # Creative Commons Attribution-NonCommercial (CC BY-NC)
    CC_BY_NC = u"https://creativecommons.org/licenses/by-nc/4.0"

    # Creative Commons Attribution-NonCommercial-ShareAlike (CC BY-NC-SA)
    CC_BY_NC_SA = u"https://creativecommons.org/licenses/by-nc-sa/4.0"

    # Creative Commons Attribution-NonCommercial-NoDerivs (CC BY-NC-ND)
    CC_BY_NC_ND = u"https://creativecommons.org/licenses/by-nc-nd/4.0"

    # Open access download but no explicit license
    GENERIC_OPEN_ACCESS = u"http://librarysimplified.org/terms/rights-status/generic-open-access"

    # Unknown copyright status.
    UNKNOWN = u"http://librarysimplified.org/terms/rights-status/unknown"

    OPEN_ACCESS = [
        PUBLIC_DOMAIN_USA,
        CC0,
        CC_BY,
        CC_BY_SA,
        CC_BY_ND,
        CC_BY_NC,
        CC_BY_NC_SA,
        CC_BY_NC_ND,
        GENERIC_OPEN_ACCESS,
    ]

    # These open access rights allow derivative works to be created, but may
    # require attribution or prohibit commercial use.
    ALLOWS_DERIVATIVES = [
        PUBLIC_DOMAIN_USA,
        CC0,
        CC_BY,
        CC_BY_SA,
        CC_BY_NC,
        CC_BY_NC_SA,
    ]

    NAMES = {
        IN_COPYRIGHT: "In Copyright",
        PUBLIC_DOMAIN_USA: "Public domain in the USA",
        CC0: "Creative Commons Public Domain Dedication (CC0)",
        CC_BY: "Creative Commons Attribution (CC BY)",
        CC_BY_SA: "Creative Commons Attribution-ShareAlike (CC BY-SA)",
        CC_BY_ND: "Creative Commons Attribution-NoDerivs (CC BY-ND)",
        CC_BY_NC: "Creative Commons Attribution-NonCommercial (CC BY-NC)",
        CC_BY_NC_SA: "Creative Commons Attribution-NonCommercial-ShareAlike (CC BY-NC-SA)",
        CC_BY_NC_ND: "Creative Commons Attribution-NonCommercial-NoDerivs (CC BY-NC-ND)",
        GENERIC_OPEN_ACCESS: "Open access with no specific license",
        UNKNOWN: "Unknown",
    }

    DATA_SOURCE_DEFAULT_RIGHTS_STATUS = {
        DataSourceConstants.GUTENBERG: PUBLIC_DOMAIN_USA,
        DataSourceConstants.PLYMPTON: CC_BY_NC,
        # workaround for opds-imported license pools with 'content server' as data source
        DataSourceConstants.OA_CONTENT_SERVER : GENERIC_OPEN_ACCESS,

        DataSourceConstants.OVERDRIVE: IN_COPYRIGHT,
        DataSourceConstants.BIBLIOTHECA: IN_COPYRIGHT,
        DataSourceConstants.AXIS_360: IN_COPYRIGHT,
    }

    __tablename__ = 'rightsstatus'
    id = Column(Integer, primary_key=True)

    # A URI unique to the license. This may be a URL (e.g. Creative
    # Commons)
    uri = Column(String, index=True, unique=True)

    # Human-readable name of the license.
    name = Column(String, index=True)

    # One RightsStatus may apply to many LicensePoolDeliveryMechanisms.
    licensepooldeliverymechanisms = relationship("LicensePoolDeliveryMechanism", backref="rights_status")

    # One RightsStatus may apply to many Resources.
    resources = relationship("Resource", backref="rights_status")

    @classmethod
    def lookup(cls, _db, uri):
        if not uri in cls.NAMES.keys():
            uri = cls.UNKNOWN
        name = cls.NAMES.get(uri)
        create_method_kwargs = dict(name=name)
        status, ignore = get_one_or_create(
            _db, RightsStatus, uri=uri,
            create_method_kwargs=create_method_kwargs
        )
        return status

    @classmethod
    def rights_uri_from_string(cls, rights):
        rights = rights.lower()
        if rights == 'public domain in the usa.':
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights == 'public domain in the united states.':
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights == 'pd-us':
            return RightsStatus.PUBLIC_DOMAIN_USA
        elif rights.startswith('public domain'):
            return RightsStatus.PUBLIC_DOMAIN_UNKNOWN
        elif rights.startswith('copyrighted.'):
            return RightsStatus.IN_COPYRIGHT
        elif rights == 'cc0':
            return RightsStatus.CC0
        elif rights == 'cc by':
            return RightsStatus.CC_BY
        elif rights == 'cc by-sa':
            return RightsStatus.CC_BY_SA
        elif rights == 'cc by-nd':
            return RightsStatus.CC_BY_ND
        elif rights == 'cc by-nc':
            return RightsStatus.CC_BY_NC
        elif rights == 'cc by-nc-sa':
            return RightsStatus.CC_BY_NC_SA
        elif rights == 'cc by-nc-nd':
            return RightsStatus.CC_BY_NC_ND
        elif (rights in RightsStatus.OPEN_ACCESS
              or rights == RightsStatus.IN_COPYRIGHT):
            return rights
        else:
            return RightsStatus.UNKNOWN
