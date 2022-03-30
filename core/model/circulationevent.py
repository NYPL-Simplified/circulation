# encoding: utf-8
# CirculationEvent


import logging
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
)

from . import (
    Base,
    get_one_or_create,
)
from ..util.datetime_helpers import utc_now

class CirculationEvent(Base):

    """Changes to a license pool's circulation status.
    We log these so we can measure things like the velocity of
    individual books.
    """
    __tablename__ = 'circulationevents'

    # Used to explicitly tag an event as happening at an unknown time.
    NO_DATE = object()

    id = Column(Integer, primary_key=True)

    # One LicensePool can have many circulation events.
    license_pool_id = Column(
        Integer, ForeignKey('licensepools.id'), index=True)

    type = Column(String(32), index=True)
    start = Column(DateTime(timezone=True), index=True)
    end = Column(DateTime(timezone=True))
    old_value = Column(Integer)
    delta = Column(Integer)
    new_value = Column(Integer)

    # The Library associated with the event, if it happened in the
    # context of a particular Library and we know which one.
    library_id = Column(
        Integer, ForeignKey('libraries.id'),
        index=True, nullable=True
    )

    # The geographic location associated with the event. This string
    # may mean different things for different libraries. It might be a
    # measurement of latitude and longitude, or it might be taken from
    # a controlled vocabulary -- a list of library branch codes, for
    # instance.
    location = Column(Unicode, index=True)

    __table_args__ = (
        # Make it easy to list circulation events in descending
        # order. This is used in the admin interface to show recent
        # events.
        #
        # TODO: Maybe there should also be an index that takes
        # library_id into account, for per-library event lists.
        Index(
            "ix_circulationevents_start_desc_nullslast",
            start.desc().nullslast()
        ),

        # License pool ID + library ID + type + start must be unique.
        Index(
            "ix_circulationevents_license_pool_library_type_start",
            license_pool_id,
            library_id,
            type,
            start,
            unique=True
        ),

        # However, library_id may be null. If this is so, then license pool ID
        # + type + start must be unique.
        Index(
            "ix_circulationevents_license_pool_type_start",
            license_pool_id,
            type,
            start,
            unique=True,
            postgresql_where=(library_id==None)
        ),
    )

    # Constants for use in logging circulation events to JSON
    SOURCE = "source"
    TYPE = "event"

    # The names of the circulation events we recognize.
    # They may be sent to third-party analytics services
    # as well as used locally.

    # Events that happen in a circulation manager.
    NEW_PATRON = "circulation_manager_new_patron"
    CM_CHECKOUT = "circulation_manager_check_out"
    CM_CHECKIN = "circulation_manager_check_in"
    CM_HOLD_PLACE = "circulation_manager_hold_place"
    CM_HOLD_RELEASE = "circulation_manager_hold_release"
    CM_FULFILL = "circulation_manager_fulfill"

    # Events that we hear about from a distributor.
    DISTRIBUTOR_CHECKOUT = "distributor_check_out"
    DISTRIBUTOR_CHECKIN = "distributor_check_in"
    DISTRIBUTOR_HOLD_PLACE = "distributor_hold_place"
    DISTRIBUTOR_HOLD_RELEASE = "distributor_hold_release"
    DISTRIBUTOR_LICENSE_ADD = "distributor_license_add"
    DISTRIBUTOR_LICENSE_REMOVE = "distributor_license_remove"
    DISTRIBUTOR_AVAILABILITY_NOTIFY = "distributor_availability_notify"
    DISTRIBUTOR_TITLE_ADD = "distributor_title_add"
    DISTRIBUTOR_TITLE_REMOVE = "distributor_title_remove"

    # Events that we hear about from a client app.
    OPEN_BOOK = "open_book"

    CLIENT_EVENTS = [
        OPEN_BOOK,
    ]


    # The time format used when exporting to JSON.
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"

    @classmethod
    def log(cls, _db, license_pool, event_name, old_value, new_value,
            start=None, end=None, library=None, location=None):
        """Log a CirculationEvent to the database, assuming it
        hasn't already been recorded.
        """
        if new_value is None or old_value is None:
            delta = None
        else:
            delta = new_value - old_value
        if not start:
            start = utc_now()
        if not end:
            end = start
        event, was_new = get_one_or_create(
            _db, CirculationEvent, license_pool=license_pool,
            type=event_name, start=start, library=library,
            create_method_kwargs=dict(
                old_value=old_value,
                new_value=new_value,
                delta=delta,
                end=end,
                location=location
            )
        )
        if was_new:
            logging.info("EVENT %s %s=>%s", event_name, old_value, new_value)
        return event, was_new
