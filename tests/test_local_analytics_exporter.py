from datetime import datetime, timedelta, date
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)
import csv

from core.testing import DatabaseTest
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
        location = "11377"

        # Create a bunch of circulation events of different types,
        # all with the same .location.
        for type in types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp1, type=type, start=time, end=time,
                location=location
            )
            time += timedelta(minutes=1)

        # Create a circulation event for a different book,
        # with no .location.
        get_one_or_create(
            self._db, CirculationEvent,
            license_pool=lp2, type=types[3], start=time, end=time
        )

        # Run a query that excludes the last event created.
        today = date.today() - timedelta(days=1)
        output = exporter.export(self._db, today, time)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row
        eq_(num, len(rows))

        # We've got one circulation event for each type.
        eq_(types, [row[1] for row in rows])

        # After the start date and event type, every row has the same
        # data. For the rest of this test we'll be using this block of
        # data to verify that circulation events for w1 look like we'd
        # expect.
        constant = [
            identifier1.identifier, identifier1.type,
            edition1.title, edition1.author, "fiction", w1.audience,
            edition1.publisher or '', edition1.imprint or '',
            edition1.language, w1.target_age_string or '',
            ordered_genre_string, location
        ]
        for row in rows:
            eq_(14, len(row))
            eq_(constant, row[2:])

        # Now run a query that includes the last event created.
        output = exporter.export(self._db, today, time + timedelta(minutes=1))
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row
        eq_(num + 1, len(rows))
        eq_(types + [types[3]], [row[1] for row in rows])

        # All but the last row is the same as in the previous report.
        all_but_last_row = rows[:-1]
        eq_(types, [row[1] for row in all_but_last_row])
        for row in all_but_last_row:
            eq_(14, len(row))
            eq_(constant, row[2:])

        # Now let's look at the last row. It's got metadata from a
        # different book, and notably, there is no location.
        no_location = ''
        eq_(
            [
                types[3], identifier2.identifier, identifier2.type,
                edition2.title, edition2.author, "fiction",
                w2.audience, edition2.publisher or '',
                edition2.imprint or '', edition2.language,
                w2.target_age_string or '', genres[1].name,
                no_location
            ],
            rows[-1][1:]
        )

        output = exporter.export(self._db, today, today)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row
        eq_(0, len(rows))

        # Gather events by library - these events have an associated library id
        # but it was not passed in the exporter
        library = self._library()
        library2 = self._library()
        time = datetime.now() - timedelta(minutes=num)
        for type in types:
            get_one_or_create(
                self._db, CirculationEvent,
                license_pool=lp1, type=type, start=time, end=time,
                library=library, location=location
            )
            time += timedelta(minutes=1)

        today = date.today() - timedelta(days=1)
        output = exporter.export(self._db, today, time)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row

        # There have been a total of 11 events so far. No library ID was passed
        # so all events are returned.
        eq_(11, len(rows))

        # Pass in the library ID.
        today = date.today() - timedelta(days=1)
        output = exporter.export(self._db, today, time, library=library)
        reader = csv.reader([row for row in output.split("\r\n") if row], dialect=csv.excel)
        rows = [row for row in reader][1::] # skip header row

        # There are five events with a library ID.
        eq_(num, len(rows))
        eq_(types, [row[1] for row in rows])
        for row in rows:
            eq_(14, len(row))
            eq_(constant, row[2:])

        # We are looking for events from a different library but there
        # should be no events associated with this library.
        time = datetime.now() - timedelta(minutes=num)
        today = date.today() - timedelta(days=1)
        output = exporter.export(self._db, today, time, library=library2)
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

        # After the start time and event type, the rest of the row is
        # the same content we've come to expect.
        for row in rows:
            eq_(14, len(row))
            eq_(constant, row[2:])
