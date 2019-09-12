from datetime import datetime, timedelta, date
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)
import csv

from . import DatabaseTest
from core.model import (
    get_one_or_create,
    CirculationEvent,
    Genre,
    WorkGenre,
)
from api.local_analytics_exporter import LocalAnalyticsExporter

class TestLocalAnalyticsExporter(DatabaseTest):
    """Tests the local analytics exporter."""

    def test_export(self):
        exporter = LocalAnalyticsExporter()

        w1 = self._work(with_open_access_download=True)
        w2 = self._work(with_open_access_download=True)
        [lp1] = w1.license_pools
        [lp2] = w2.license_pools
        edition1 = w1.presentation_edition
        edition1.publisher = "A publisher"
        edition1.imprint = "An imprint"
        edition2 = w2.presentation_edition
        identifier1 = w1.presentation_edition.primary_identifier
        identifier2 = w2.presentation_edition.primary_identifier
        genres = self._db.query(Genre).order_by(Genre.name).all()
        get_one_or_create(self._db, WorkGenre, work=w1, genre=genres[0], affinity=0.2)
        get_one_or_create(self._db, WorkGenre, work=w1, genre=genres[1], affinity=0.3)
        get_one_or_create(self._db, WorkGenre, work=w1, genre=genres[2], affinity=0.5)

        # We expect the genre with the highest affinity to be put first.
        ordered_genre_string = ",".join(
            [genres[2].name, genres[1].name, genres[0].name]
        )
        get_one_or_create(self._db, WorkGenre, work=w2, genre=genres[1], affinity=0.5)
        types = [
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            CirculationEvent.DISTRIBUTOR_CHECKOUT,
            CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
            CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            CirculationEvent.DISTRIBUTOR_TITLE_ADD
        ]
        num = len(types)
        time = datetime.now() - timedelta(minutes=len(types))
        for type in types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp1, type=type, start=time, end=time)
            time += timedelta(minutes=1)
        get_one_or_create(
            self._db, CirculationEvent,
            license_pool=lp2, type=types[3], start=time, end=time)

        today = date.today() - timedelta(days=1)
        output = exporter.export(self._db, today, time)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row
        eq_(num, len(rows))
        eq_(types, [row[1] for row in rows])
        eq_([identifier1.identifier]*num, [row[2] for row in rows])
        eq_([identifier1.type]*num, [row[3] for row in rows])
        eq_([edition1.title]*num, [row[4] for row in rows])
        eq_([edition1.author]*num, [row[5] for row in rows])
        eq_(["fiction"]*num, [row[6] for row in rows])
        eq_([w1.audience]*num, [row[7] for row in rows])
        eq_([edition1.publisher or '']*num, [row[8] for row in rows])
        eq_([edition1.imprint or '']*num, [row[9] for row in rows])
        eq_([edition1.language]*num, [row[10] for row in rows])
        eq_([w1.target_age_string or ""]*num, [row[11] for row in rows])
        eq_([ordered_genre_string]*num, [row[12] for row in rows])

        output = exporter.export(self._db, today, time + timedelta(minutes=1))
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row
        eq_(num + 1, len(rows))
        eq_(types + [types[3]], [row[1] for row in rows])
        eq_([identifier1.identifier]*num + [identifier2.identifier], [row[2] for row in rows])
        eq_([identifier1.type]*num + [identifier2.type], [row[3] for row in rows])
        eq_([edition1.title]*num + [edition2.title], [row[4] for row in rows])
        eq_([edition1.author]*num + [edition2.author], [row[5] for row in rows])
        eq_(["fiction"]*(num+1), [row[6] for row in rows])
        eq_([w1.audience]*num + [w2.audience], [row[7] for row in rows])
        eq_([edition1.publisher or '']*num + [edition2.publisher or ''], [row[8] for row in rows])
        eq_([edition1.imprint or '']*num + [edition2.imprint or ''], [row[9] for row in rows])
        eq_([edition1.language]*num + [edition2.language], [row[10] for row in rows])
        eq_([w1.target_age_string or ""]*num + [w2.target_age_string or ''], [row[11] for row in rows])
        eq_([ordered_genre_string]*num + [genres[1].name], [row[12] for row in rows])

        output = exporter.export(self._db, today, today)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row
        eq_(0, len(rows))

        # Add example events that will be used to report by location
        user_added_locations = "11377,10018,11378"

        # The CM_HOLD_PLACE event should not be returned since it's not in the
        # list of events to gather when there is a list of locations.
        new_types = [
            CirculationEvent.CM_FULFILL,
            CirculationEvent.CM_CHECKOUT,
            CirculationEvent.OPEN_BOOK,
            CirculationEvent.CM_HOLD_PLACE,
        ]

        # Only information from the first three events should be returned.
        num = len(new_types) - 1
        time = datetime.now() - timedelta(minutes=num)
        for type in new_types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp1, type=type, start=time, end=time)
            time += timedelta(minutes=1)

        output = exporter.export(self._db, today, time + timedelta(minutes=1), user_added_locations)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row

        # No location was associated with each event so none will be returned
        eq_(0, len(rows))

        for type in new_types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp1, type=type, start=time, end=time, location="10001")
            time += timedelta(minutes=1)

        output = exporter.export(self._db, today, time + timedelta(minutes=1), user_added_locations)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row

        # Some events have a location but not in the list of locations that was passed
        eq_(0, len(rows))

        for type in new_types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp1, type=type, start=time, end=time, location="11377")
            time += timedelta(minutes=1)

        output = exporter.export(self._db, today, time + timedelta(minutes=1), user_added_locations)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row

        # These events have a location that is in the list of acceptable
        # locations. The CM_HOLD_PLACE event is not in the list of event types
        # to gather information from, so it should not be returned even though
        # it has a location.
        eq_(num, len(rows))
        # The last event in new_types should not be returned
        eq_(new_types[:-1], [row[1] for row in rows])
        eq_([identifier1.identifier]*num, [row[2] for row in rows])
        eq_([identifier1.type]*num, [row[3] for row in rows])
        eq_([edition1.title]*num, [row[4] for row in rows])
        eq_([edition1.author]*num, [row[5] for row in rows])
        eq_(["fiction"]*num, [row[6] for row in rows])
        eq_([w1.audience]*num, [row[7] for row in rows])
        eq_([edition1.publisher or '']*num, [row[8] for row in rows])
        eq_([edition1.imprint or '']*num, [row[9] for row in rows])
        eq_([edition1.language]*num, [row[10] for row in rows])
        eq_([w1.target_age_string or ""]*num, [row[11] for row in rows])
        eq_([ordered_genre_string]*num, [row[12] for row in rows])
