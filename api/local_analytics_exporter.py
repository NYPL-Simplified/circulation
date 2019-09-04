import csv, codecs, cStringIO
from StringIO import StringIO

from sqlalchemy.sql import func
from sqlalchemy.orm import lazyload

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

        query = _db.query(
                CirculationEvent, Identifier, Work, Edition
            ) \
            .join(LicensePool, LicensePool.id == CirculationEvent.license_pool_id) \
            .join(Identifier, Identifier.id == LicensePool.identifier_id) \
            .join(Work, Work.id == LicensePool.work_id) \
            .join(Edition, Edition.id == Work.presentation_edition_id) \
            .filter(CirculationEvent.start >= start) \
            .filter(CirculationEvent.start < end) \
            .order_by(CirculationEvent.start.asc())
        query = query \
            .options(lazyload(Identifier.licensed_through)) \
            .options(lazyload(Work.license_pools))
        results = query.all()

        work_ids = map(lambda result: result[2].id, results)

        subquery = _db \
            .query(WorkGenre.work_id, Genre.name) \
            .join(Genre) \
            .filter(WorkGenre.work_id.in_(work_ids)) \
            .order_by(WorkGenre.affinity.desc()) \
            .subquery()
        genre_query = _db \
            .query(subquery.c.work_id, func.string_agg(subquery.c.name, ",")) \
            .select_from(subquery) \
            .group_by(subquery.c.work_id)
        genres = dict(genre_query.all())

        header = [
            "time", "event", "identifier", "identifier_type", "title", "author",
            "fiction", "audience", "publisher", "language", "target_age", "genres"
        ]

        def result_to_row(result):
            (event, identifier, work, edition) = result
            return [
                str(event.start) or "",
                event.type,
                identifier.identifier,
                identifier.type,
                edition.title,
                edition.author,
                "fiction" if work.fiction else "nonfiction",
                work.audience,
                edition.publisher,
                edition.language,
                work.target_age_string,
                genres.get(work.id)
            ]

        data = [header] + map(result_to_row, results)

        output = StringIO()
        writer = UnicodeWriter(output)
        writer.writerows(data)
        return output.getvalue()


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

