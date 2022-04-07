# encoding: utf-8
from dbm.ndbm import library
from isbnlib import editions
import pytest
import datetime
from sqlalchemy.exc import IntegrityError
from ...model import (
    create,
    get_one_or_create
)
from ...model.circulationevent import CirculationEvent
from ...model.datasource import DataSource
from ...model.identifier import Identifier
from ...model.licensing import LicensePool
from ...util.datetime_helpers import (
    datetime_utc,
    strptime_utc,
    utc_now,
)


class TestCirculationEvent:

    def _event_data(self, **kwargs):
        for k, default in (
                ("source", DataSource.OVERDRIVE),
                ("id_type", Identifier.OVERDRIVE_ID),
                ("start", utc_now()),
                ("type", CirculationEvent.DISTRIBUTOR_LICENSE_ADD),
        ):
            kwargs.setdefault(k, default)
        if 'old_value' in kwargs and 'new_value' in kwargs:
            kwargs['delta'] = kwargs['new_value'] - kwargs['old_value']
        return kwargs

    def _get_datetime(self, data, key):
        date = data.get(key, None)
        if not date:
            return None
        elif isinstance(date, datetime.date):
            return date
        else:
            return strptime_utc(date, CirculationEvent.TIME_FORMAT)

    def _get_int(self, data, key):
        value = data.get(key, None)
        if not value:
            return value
        else:
            return int(value)

    def from_dict(self, data, db_session):
        # Identify the source of the event.
        source_name = data['source']
        source = DataSource.lookup(db_session, source_name)

        # Identify which LicensePool the event is talking about.
        foreign_id = data['id']
        identifier_type = source.primary_identifier_type
        collection = data['collection']

        license_pool, was_new = LicensePool.for_foreign_id(
            db_session, source, identifier_type, foreign_id, collection=collection
        )

        # Finally, gather some information about the event itself.
        type = data.get("type")
        start = self._get_datetime(data, 'start')
        end = self._get_datetime(data, 'end')
        old_value = self._get_int(data, 'old_value')
        new_value = self._get_int(data, 'new_value')
        delta = self._get_int(data, 'delta')
        event, was_new = get_one_or_create(
            db_session, CirculationEvent, license_pool=license_pool,
            type=type, start=start,
            create_method_kwargs=dict(
                old_value=old_value,
                new_value=new_value,
                delta=delta,
                end=end)
            )
        return event, was_new

    def test_new_title(self, db_session, create_collection):
        """
        GIVEN: A dictionary
        WHEN:  Creating a CirculationEvent with the dictionary data
        THEN:  A CirculationEvent is succesfully created
        """
        # Here's a new title.
        collection = create_collection(db_session)
        data = self._event_data(
            source=DataSource.OVERDRIVE,
            id="{1-2-3}",
            type=CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
            collection=collection,
            old_value=0,
            delta=2,
            new_value=2,
        )

        # Turn it into an event and see what happens.
        event, _ = self.from_dict(data, db_session)

        # The event is associated with the correct data source.
        assert DataSource.OVERDRIVE == event.license_pool.data_source.name

        # The event identifies a work by its ID plus the data source's
        # primary identifier and its collection.
        assert Identifier.OVERDRIVE_ID == event.license_pool.identifier.type
        assert "{1-2-3}" == event.license_pool.identifier.identifier
        assert collection == event.license_pool.collection

        # The number of licenses has not been set to the new value.
        # The creator of a circulation event is responsible for also
        # updating the dataset.
        assert 0 == event.license_pool.licenses_owned

    def test_log(self, db_session, create_edition, create_library, create_licensepool):
        """
        GIVEN: Data to populate a CirculationEvent
        WHEN:  Logging a CirculationEvent
        THEN:  A CirculationEvent is correctly retrieved or created
        """
        # Basic test of CirculationEvent.log.
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        library = create_library(db_session)
        event_name = CirculationEvent.DISTRIBUTOR_CHECKOUT
        old_value = 10
        new_value = 8
        start = datetime_utc(2019, 1, 1)
        end = datetime_utc(2019, 1, 2)
        location = "Westgate Branch"

        m = CirculationEvent.log
        event, is_new = m(
            db_session, license_pool=pool, event_name=event_name,
            library=library, old_value=old_value, new_value=new_value,
            start=start, end=end, location=location
        )
        assert True == is_new
        assert pool == event.license_pool
        assert library == event.library
        assert -2 == event.delta  # calculated from old_value and new_value
        assert start == event.start
        assert end == event.end
        assert location == event.location

        # If log finds another event with the same license pool,
        # library, event name, and start date, that event is returned
        # unchanged.
        event, is_new = m(
            db_session, license_pool=pool, event_name=event_name,
            library=library, start=start,

            # These values will be ignored.
            old_value=500, new_value=200,
            end=utc_now(),
            location="another location"
        )
        assert False == is_new
        assert pool == event.license_pool
        assert library == event.library
        assert -2 == event.delta
        assert start == event.start
        assert end == event.end
        assert location == event.location

        # If no timestamp is provided, the current time is used. This
        # is the most common case, so basically a new event will be
        # created each time you call log().
        event, is_new = m(
            db_session, license_pool=pool, event_name=event_name,
            library=library, old_value=old_value, new_value=new_value,
            end=end, location=location
        )
        assert (utc_now() - event.start).total_seconds() < 2
        assert True == is_new
        assert pool == event.license_pool
        assert library == event.library
        assert -2 == event.delta
        assert end == event.end
        assert location == event.location

    def test_uniqueness_constraints_no_library(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: An Edition and LicensePool
        WHEN:  Creating a CirculationEvent with a duplicate timeestamps
        THEN:  An IntegrityError is raised 
        """
        # If library is null, then license_pool + type + start must be
        # unique.
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        now = utc_now()
        kwargs = dict(
            license_pool=pool, type=CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        )
        event = create(db_session, CirculationEvent, start=now, **kwargs)

        # Different timestamp -- no problem.
        now2 = utc_now()
        event2 = create(db_session, CirculationEvent, start=now2, **kwargs)
        assert event != event2

        # Reuse the timestamp and you get an IntegrityError which ruins the
        # entire transaction.
        pytest.raises(
            IntegrityError, create, db_session, CirculationEvent, start=now,
            **kwargs
        )

    def test_uniqueness_constraints_with_library(self, db_session, create_edition, create_library, create_licensepool):
        """
        GIVEN: An Edition, Library, and LicensePool
        WHEN:  Creating a CirculationEvent with a duplicate timestamp
        THEN:  An IntegrityError is raised
        """
        # If library is provided, then license_pool + library + type +
        # start must be unique.
        library = create_library(db_session)
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        now = utc_now()
        kwargs = dict(
            license_pool=pool,
            library=library,
            type=CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        )
        event = create(db_session, CirculationEvent, start=now, **kwargs)

        # Different timestamp -- no problem.
        now2 = utc_now()
        event2 = create(db_session, CirculationEvent, start=now2, **kwargs)
        assert event != event2

        # Reuse the timestamp and you get an IntegrityError which ruins the
        # entire transaction.
        pytest.raises(
            IntegrityError, create, db_session, CirculationEvent, start=now,
            **kwargs
        )
