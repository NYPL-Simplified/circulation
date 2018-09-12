# encoding: utf-8
from nose.tools import set_trace
import datetime
import logging
import time
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from . import (
    Base,
    get_one_or_create,
)

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
    start = Column(DateTime, index=True)
    end = Column(DateTime)
    old_value = Column(Integer)
    delta = Column(Integer)
    new_value = Column(Integer)
    foreign_patron_id = Column(String)

    # A given license pool can only have one event of a given type for
    # a given patron at a given time.
    __table_args__ = (UniqueConstraint('license_pool_id', 'type', 'start',
                                       'foreign_patron_id'),)

    # Constants for use in logging circulation events to JSON
    SOURCE = u"source"
    TYPE = u"event"

    # The names of the circulation events we recognize.
    # They may be sent to third-party analytics services
    # as well as used locally.

    # Events that happen in a circulation manager.
    NEW_PATRON = u"circulation_manager_new_patron"
    CM_CHECKOUT = u"circulation_manager_check_out"
    CM_CHECKIN = u"circulation_manager_check_in"
    CM_HOLD_PLACE = u"circulation_manager_hold_place"
    CM_HOLD_RELEASE = u"circulation_manager_hold_release"
    CM_FULFILL = u"circulation_manager_fulfill"

    # Events that we hear about from a distributor.
    DISTRIBUTOR_CHECKOUT = u"distributor_check_out"
    DISTRIBUTOR_CHECKIN = u"distributor_check_in"
    DISTRIBUTOR_HOLD_PLACE = u"distributor_hold_place"
    DISTRIBUTOR_HOLD_RELEASE = u"distributor_hold_release"
    DISTRIBUTOR_LICENSE_ADD = u"distributor_license_add"
    DISTRIBUTOR_LICENSE_REMOVE = u"distributor_license_remove"
    DISTRIBUTOR_AVAILABILITY_NOTIFY = u"distributor_availability_notify"
    DISTRIBUTOR_TITLE_ADD = u"distributor_title_add"
    DISTRIBUTOR_TITLE_REMOVE = u"distributor_title_remove"

    # Events that we hear about from a client app.
    OPEN_BOOK = u"open_book"

    CLIENT_EVENTS = [
        OPEN_BOOK,
    ]


    # The time format used when exporting to JSON.
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+00:00"

    @classmethod
    def log(cls, _db, license_pool, event_name, old_value, new_value,
            start=None, end=None, foreign_patron_id=None):
        if new_value is None or old_value is None:
            delta = None
        else:
            delta = new_value - old_value
        if not start:
            start = datetime.datetime.utcnow()
        if not end:
            end = start
        logging.info("EVENT %s %s=>%s", event_name, old_value, new_value)
        event, was_new = get_one_or_create(
            _db, CirculationEvent, license_pool=license_pool,
            type=event_name, start=start, foreign_patron_id=foreign_patron_id,
            create_method_kwargs=dict(
                old_value=old_value,
                new_value=new_value,
                delta=delta,
                end=end)
            )
        return event, was_new


Index("ix_circulationevents_start_desc_nullslast", CirculationEvent.start.desc().nullslast())
