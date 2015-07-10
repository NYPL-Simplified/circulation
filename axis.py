from nose.tools import set_trace
from datetime import datetime

from core.axis import (
    Axis360API as BaseAxis360API,
    BibliographicParser,
)

from core.monitor import Monitor

from core.model import (
    CirculationEvent,
    get_one_or_create,
    Contributor,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Subject,
)

class Axis360API(BaseAxis360API):

    def update_licensepool(self, data):
        pass

class Axis360CirculationMonitor(Monitor):

    """Maintain LicensePools for Axis 360 titles.
    """

    def __init__(self, _db, name="Axis 360 Circulation Monitor",
                 interval_seconds=60):
        super(Axis360CirculationMonitor, self).__init__(
            _db, name, interval_seconds=interval_seconds,
            default_start_time = datetime.utcnow() - Monitor.ONE_YEAR_AGO)

    def run(self):
        self.api = Axis360API(self._db)
        super(Axis360CirculationMonitor, self).run()

    def run_once(self, start, cutoff):
        _db = self._db
        added_books = 0
        print start
        availability = self.api.availability(start)
        status_code = availability.status_code
        content = availability.content
        print content
        if status_code != 200:
            raise Exception(
                "Got status code %d from API: %s" % (status_code, content))
        for bibliographic, circulation in BibliographicParser().process_all(
                content):
            self.process_book(bibliographic, circulation)

    def process_book(self, bibliographic, availability):
        [axis_id] = bibliographic[Identifier][Identifier.AXIS_360_ID]
        axis_id = axis_id[Identifier.identifier]

        license_pool, new_license_pool = LicensePool.for_foreign_id(
            self._db, self.api.source, Identifier.AXIS_360_ID, axis_id)

        # The Axis 360 identifier is exactly equivalent to each ISBN.
        any_new_isbn = False
        isbns = []
        for i in bibliographic[Identifier].get(Identifier.ISBN):
            isbn_id = i[Identifier.identifier]
            isbn, was_new = Identifier.for_foreign_id(
                self._db, Identifier.ISBN, isbn_id)
            isbns.append(isbn)
            any_new_isbn = any_new_isbn or was_new

        edition, new_edition = Edition.for_foreign_id(
            self._db, self.api.source, Identifier.AXIS_360_ID, axis_id)

        axis_id = license_pool.identifier

        if any_new_isbn or new_license_pool or new_edition:
            for isbn in isbns:
                axis_id.equivalent_to(self.api.source, isbn, strength=1)

        if new_license_pool or new_edition:
            # Add bibliographic information to the Edition.
            edition.title = bibliographic.get(Edition.title)
            edition.subtitle = bibliographic.get(Edition.subtitle)
            edition.series = bibliographic.get(Edition.series)
            edition.published = bibliographic.get(Edition.published)
            edition.publisher = bibliographic.get(Edition.publisher)
            edition.imprint = bibliographic.get(Edition.imprint)
            edition.language = bibliographic.get(Edition.language)

            # Contributors!
            contributors_by_role = bibliographic.get(Contributor, {})
            for role, contributors in contributors_by_role.items():
                for name in contributors:
                    edition.add_contributor(name, role)

            # Subjects!
            for subject in bibliographic.get(Subject, []):
                s_type = subject[Subject.type]
                s_identifier = subject[Subject.identifier]

                axis_id.classify(
                    self.api.source, s_type, s_identifier)

        # Update the license pool with new availability information
        new_licenses_owned = availability.get(LicensePool.licenses_owned, 0)
        new_licenses_available = availability.get(
            LicensePool.licenses_available, 0)
        new_licenses_reserved = 0
        new_patrons_in_hold_queue = availability.get(
            LicensePool.patrons_in_hold_queue, 0)

        last_checked = availability.get(
            LicensePool.last_checked, datetime.utcnow())

        # If this is our first time seeing this LicensePool, log its
        # occurance as a separate event
        if new_license_pool:
            event = get_one_or_create(
                self._db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=last_checked,
                    delta=1,
                    end=last_checked,
                )
            )

        license_pool.update_availability(
            new_licenses_owned, new_licenses_available, new_licenses_reserved,
            new_patrons_in_hold_queue, last_checked)

        return edition, license_pool
