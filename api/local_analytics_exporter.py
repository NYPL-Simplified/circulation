import logging
import unicodecsv as csv
from io import BytesIO

from sqlalchemy.sql import (
    func,
    select,
)
from sqlalchemy.sql.expression import (
    and_,
    case,
    literal_column,
    join,
    or_,
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
    """Export large numbers of analytics events in CSV format."""

    def export(self, _db, start, end, locations=None, library=None):

        # Get the results from the database.
        query = self.analytics_query(start, end, locations, library)
        results = _db.execute(query)

        # Write the CSV file to a BytesIO.
        header = [
            "time", "event", "identifier", "identifier_type", "title", "author",
            "fiction", "audience", "publisher", "imprint", "language",
            "target_age", "genres", "location"
        ]
        output = BytesIO()
        writer = csv.writer(output, encoding="utf-8")
        writer.writerow(header)
        writer.writerows(results)

        return output.getvalue()

    def analytics_query(self, start, end,  locations=None, library=None):
        """Build a database query that fetches rows of analytics data.

        This method uses low-level SQLAlchemy code to do all
        calculations and data conversations in the database. It's
        modeled after Work.to_search_documents, which generates a
        large JSON document entirely in the database.

        :return: An iterator of results, each of which can be written
            directly to a CSV file.
        """

        clauses = [
            CirculationEvent.start >= start,
            CirculationEvent.start < end,
        ]

        if locations:
            event_types = [
                CirculationEvent.CM_CHECKOUT,
                CirculationEvent.CM_FULFILL,
                CirculationEvent.OPEN_BOOK
            ]
            locations = locations.strip().split(",")

            clauses += [
                CirculationEvent.type.in_(event_types),
                CirculationEvent.location.in_(locations),
            ]

        if library:
            clauses += [
                CirculationEvent.library == library
            ]

        # Build the primary query. This is a query against the
        # CirculationEvent table and a few other tables joined against
        # it. This makes up the bulk of the data.
        events_alias = select(
            [
                func.to_char(
                    CirculationEvent.start, "YYYY-MM-DD HH24:MI:SS"
                ).label("start"),
                CirculationEvent.type.label("event_type"),
                Identifier.identifier,
                Identifier.type.label("identifier_type"),
                Edition.sort_title,
                Edition.sort_author,
                case(
                    [(Work.fiction==True, literal_column("'fiction'"))],
                    else_=literal_column("'nonfiction'")
                ).label("fiction"),
                Work.id.label("work_id"),
                Work.audience,
                Edition.publisher,
                Edition.imprint,
                Edition.language,
                CirculationEvent.location,
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
            and_(*clauses)
        ).order_by(
            CirculationEvent.start.asc()
        ).alias("events_alias")

        # A subquery can hook into the main query by referencing its
        # 'work_id' field in its WHERE clause.
        work_id_column = literal_column(
            events_alias.name + "." + events_alias.c.work_id.name
        )

        # This subquery gets the names of a Work's genres as a single
        # comma-separated string.
        #

        # This Alias selects some number of rows, each containing one
        # string column (Genre.name). Genres with higher affinities with
        # this work go first.
        genres_alias = select(
            [Genre.name.label("genre_name")]
        ).select_from(
            join(
                WorkGenre, Genre,
                WorkGenre.genre_id==Genre.id
            )
        ).where(
            WorkGenre.work_id==work_id_column
        ).order_by(
            WorkGenre.affinity.desc(), Genre.name
        ).alias("genres_subquery")

        # Use array_agg() to consolidate the rows into one row -- this
        # gives us a single value, an array of strings, for each
        # Work. Then use array_to_string to convert the array into a
        # single comma-separated string.
        genres = select(
            [
                func.array_to_string(
                    func.array_agg(genres_alias.c.genre_name), ","
                )
            ]
        ).select_from(genres_alias)

        # This subquery gets the a Work's target age as a single string.
        #

        # This Alias selects two fields: the lower and upper bounds of
        # the Work's target age. This reuses code originally written
        # for Work.to_search_documents().
        target_age = Work.target_age_query(work_id_column).alias(
            "target_age_subquery"
        )

        # Concatenate the lower and upper bounds with a dash in the
        # middle. If both lower and upper bound are empty, just give
        # the empty string. This simulates the behavior of
        # Work.target_age_string.
        target_age_string = select([
            case(
                [
                    (or_(target_age.c.lower != None,
                         target_age.c.upper != None),
                     func.concat(target_age.c.lower, "-", target_age.c.upper))
                ],
                else_=literal_column("''")
            )
        ]).select_from(target_age)


        # Build the main query out of the subqueries.
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
                events.audience,
                events.publisher,
                events.imprint,
                events.language,
                target_age_string.label('target_age'),
                genres.label('genres'),
                events.location,
            ]
        ).select_from(
            events_alias
        )
        return query
