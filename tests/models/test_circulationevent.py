# encoding: utf-8
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
import datetime
from sqlalchemy.exc import IntegrityError
from .. import DatabaseTest
from ...model import (
    create,
    get_one_or_create
)
from ...model.circulationevent import CirculationEvent
from ...model.datasource import DataSource
from ...model.identifier import Identifier
from ...model.licensing import LicensePool

class TestCirculationEvent(DatabaseTest):

    def _event_data(self, **kwargs):
        for k, default in (
                ("source", DataSource.OVERDRIVE),
                ("id_type", Identifier.OVERDRIVE_ID),
                ("start", datetime.datetime.utcnow()),
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
            return datetime.datetime.strptime(date, CirculationEvent.TIME_FORMAT)

    def _get_int(self, data, key):
        value = data.get(key, None)
        if not value:
            return value
        else:
            return int(value)

    def from_dict(self, data):
        _db = self._db

        # Identify the source of the event.
        source_name = data['source']
        source = DataSource.lookup(_db, source_name)

        # Identify which LicensePool the event is talking about.
        foreign_id = data['id']
        identifier_type = source.primary_identifier_type
        collection = data['collection']

        license_pool, was_new = LicensePool.for_foreign_id(
            _db, source, identifier_type, foreign_id, collection=collection
        )

        # Finally, gather some information about the event itself.
        type = data.get("type")
        start = self._get_datetime(data, 'start')
        end = self._get_datetime(data, 'end')
        old_value = self._get_int(data, 'old_value')
        new_value = self._get_int(data, 'new_value')
        delta = self._get_int(data, 'delta')
        event, was_new = get_one_or_create(
            _db, CirculationEvent, license_pool=license_pool,
            type=type, start=start,
            create_method_kwargs=dict(
                old_value=old_value,
                new_value=new_value,
                delta=delta,
                end=end)
            )
        return event, was_new

    def test_new_title(self):

        # Here's a new title.
        collection = self._collection()
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
        event, ignore = self.from_dict(data)

        # The event is associated with the correct data source.
        eq_(DataSource.OVERDRIVE, event.license_pool.data_source.name)

        # The event identifies a work by its ID plus the data source's
        # primary identifier and its collection.
        eq_(Identifier.OVERDRIVE_ID, event.license_pool.identifier.type)
        eq_("{1-2-3}", event.license_pool.identifier.identifier)
        eq_(collection, event.license_pool.collection)

        # The number of licenses has not been set to the new value.
        # The creator of a circulation event is responsible for also
        # updating the dataset.
        eq_(0, event.license_pool.licenses_owned)

    def test_uniqueness_constraints_no_library(self):
        # If library is null, then license_pool + type + start must be
        # unique.
        pool = self._licensepool(edition=None)
        now = datetime.datetime.utcnow()
        kwargs = dict(
            license_pool=pool, type=CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        )
        event = create(self._db, CirculationEvent, start=now, **kwargs)

        # Different timestamp -- no problem.
        now2 = datetime.datetime.utcnow()
        event2 = create(self._db, CirculationEvent, start=now2, **kwargs)
        assert event != event2

        # Reuse the timestamp and you get an IntegrityError which ruins the
        # entire transaction.
        assert_raises(
            IntegrityError, create, self._db, CirculationEvent, start=now,
            **kwargs
        )
        self._db.rollback()

    def test_uniqueness_constraints_with_library(self):
        # If library is provided, then license_pool + library + type +
        # start must be unique.
        pool = self._licensepool(edition=None)
        now = datetime.datetime.utcnow()
        kwargs = dict(
            license_pool=pool,
            library=self._default_library,
            type=CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        )
        event = create(self._db, CirculationEvent, start=now, **kwargs)

        # Different timestamp -- no problem.
        now2 = datetime.datetime.utcnow()
        event2 = create(self._db, CirculationEvent, start=now2, **kwargs)
        assert event != event2

        # Reuse the timestamp and you get an IntegrityError which ruins the
        # entire transaction.
        assert_raises(
            IntegrityError, create, self._db, CirculationEvent, start=now,
            **kwargs
        )
        self._db.rollback()
