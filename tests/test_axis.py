import datetime
from nose.tools import eq_, set_trace

from ..core.model import (
    Edition,
    Identifier,
    Subject,
    Contributor,
    LicensePool,
)

from ..axis import (
    Axis360CirculationMonitor,
    Axis360API,
)

from . import (
    DatabaseTest,
)


class TestCirculationMonitor(DatabaseTest):

    BIBLIOGRAPHIC_DATA = {
        Edition.publisher: u'Random House Inc',
        Edition.language: 'eng', 
        Edition.title: u'Faith of My Fathers : A Family Memoir', 
        Edition.imprint : u'Random House Inc2',
        Edition.published: datetime.datetime(2000, 3, 7, 0, 0),
        Identifier: { 
            Identifier.ISBN: [{Identifier.identifier: u'9780375504587'}],
            Identifier.AXIS_360_ID : [
                {Identifier.identifier: u'0003642860'}
            ],
        },
        Contributor: {
            Contributor.PRIMARY_AUTHOR_ROLE : [u'McCain, John'],
            Contributor.AUTHOR_ROLE : [u'Salter, Mark'], 
        },
        Subject: [
            {Subject.type : Subject.BISAC,
             Subject.identifier : u'BIOGRAPHY & AUTOBIOGRAPHY / Political'},
            {Subject.type : Subject.FREEFORM_AUDIENCE,
             Subject.identifier : u'Adult'},
        ],
    }

    AVAILABILITY_DATA = {
        LicensePool.licenses_owned: 9,
        LicensePool.licenses_available: 8,
        LicensePool.patrons_in_hold_queue: 0,
        LicensePool.last_checked: datetime.datetime(2015, 5, 20, 2, 9, 8),
    }

    def test_process_book(self):
        monitor = Axis360CirculationMonitor(self._db)
        monitor.api = Axis360API(self._db)
        edition, license_pool = monitor.process_book(
            self.BIBLIOGRAPHIC_DATA, self.AVAILABILITY_DATA)
        eq_(u'Faith of My Fathers : A Family Memoir', edition.title)
        eq_(u'eng', edition.language)
        eq_(u'Random House Inc', edition.publisher)
        eq_(u'Random House Inc2', edition.imprint)

        eq_(Identifier.AXIS_360_ID, edition.primary_identifier.type)
        eq_(u'0003642860', edition.primary_identifier.identifier)

        [isbn] = [x for x in edition.equivalent_identifiers()
                  if x is not edition.primary_identifier]
        eq_(Identifier.ISBN, isbn.type)
        eq_(u'9780375504587', isbn.identifier)

        eq_(["McCain, John", "Salter, Mark"], 
            sorted([x.name for x in edition.contributors]),
        )

        subs = sorted(
            (x.subject.type, x.subject.identifier)
            for x in edition.primary_identifier.classifications
        )
        eq_([(Subject.BISAC, u'BIOGRAPHY & AUTOBIOGRAPHY / Political'), 
             (Subject.FREEFORM_AUDIENCE, u'Adult')], subs)

        eq_(9, license_pool.licenses_owned)
        eq_(8, license_pool.licenses_available)
        eq_(0, license_pool.patrons_in_hold_queue)
        eq_(datetime.datetime(2015, 5, 20, 2, 9, 8), license_pool.last_checked)

        # Three circulation events were created, backdated to the
        # last_checked date of the license pool.
        events = license_pool.circulation_events
        eq_([u'title_add', u'check_in', u'license_add'], 
            [x.title for x in events])
        for e in events:
            eq_(e.start, license_pool.last_checked)

