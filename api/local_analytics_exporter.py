from nose.tools import set_trace
import logging
import csv
import codecs
from StringIO import StringIO

from sqlalchemy.sql import (
    func,
    select,
)
from sqlalchemy.sql.expression import (
    and_,
    case,
    literal_column,
    join,
)
from sqlalchemy.orm import (
    defer,
    lazyload
)

from core.model import (
    CirculationEvent,
    Edition,
    Genre,
    Identifier,
    LicensePool,
    Work,
    WorkGenre,
)

class LocalAnalyticsExporter(object):
    def export(self, _db, start, end):
        import time
        s = time.time()

        events_alias = select(
            [
                func.to_char(CirculationEvent.start, "YYYY-MM-DD HH24:MI:SS").label("start"),
                CirculationEvent.type.label("event_type"),
                Identifier.identifier,
                Identifier.type.label("identifier_type"),
                Edition.sort_title,
                Edition.sort_author,
                case(
                    [(Work.fiction==True, literal_column("'Fiction'"))],
                    else_=literal_column("'Nonfiction'")
                ).label("fiction"),
                Work.id.label("work_id"),
                Work.audience,
                Edition.publisher,
                Edition.imprint,
                Edition.language,
            ],
        ).select_from(
            join(
                CirculationEvent, LicensePool,
                CirculationEvent.license_pool_id==LicensePool.id
            ).join(
                Identifier,
                LicensePool.identifier_id==Identifier.id
            ).join(
                Work,
                Work.id==LicensePool.work_id
            ).join(
                Edition, Work.presentation_edition_id==Edition.id
            )
        ).where(
            and_(
                CirculationEvent.start >= start,
                CirculationEvent.start < end
            )
        ).order_by(
            CirculationEvent.start.asc()
        ).alias("events_alias")

        work_id_column = literal_column(
            events_alias.name + "." + events_alias.c.work_id.name
        )
        # This subquery gets the list of genres associated with a work
        # as a single comma-separated string.
        genres_alias = select(
            [Genre.name.label("genre_name")]
        ).select_from(
            join(
                WorkGenre, Genre,
                WorkGenre.genre_id==Genre.id
            )
        ).where(
            WorkGenre.work_id==work_id_column
        ).alias("genres_subquery")
        genres = select(
            [
                func.array_to_string(
                    func.array_agg(genres_alias.c.genre_name), ", "
                )
            ]
        ).select_from(genres_alias)

        target_age = Work.target_age_query(work_id_column).alias(
            "target_age_subquery"
        )
        target_age_string = select(
            [
                func.concat(target_age.c.lower, "-", target_age.c.upper),
            ]
        ).select_from(target_age)


        # Build the main query.
        events = events_alias.c
        query = select(
            [
                events.start,
                events.event_type,
                events.identifier,
                events.identifier_type,
                events.sort_title,
                events.sort_author,
                events.fiction,
                events.work_id,
                events.audience,
                events.publisher,
                events.imprint,
                events.language,
                target_age_string.label('target_age'),
                genres.label('genres'),
            ]
        ).select_from(
            events_alias
        )

        results = _db.execute(query)

        header = [
            "time", "event", "identifier", "identifier_type", "title", "author",
            "fiction", "audience", "publisher", "imprint", "language",
            "target_age", "genres"
        ]

        output = StringIO()
        writer = UnicodeWriter(output)
        writer.writerow(header)
        writer.writerows(results)

        v = output.getvalue()
        finish = time.time()
        return ""


class UnicodeWriter:
    """
    A CSV writer for Unicode data.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow(
            [s.encode("utf-8") if hasattr(s, "encode") else "" for s in row]
        )
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

