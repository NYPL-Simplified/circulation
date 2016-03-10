import datetime
from nose.tools import (
    eq_, 
    assert_raises,
    set_trace,
)
import os

from core.model import (
    DataSource,
    Edition,
    Identifier,
    Subject,
    Contributor,
    LicensePool,
)

from core.metadata_layer import (
    Metadata,
    CirculationData,
    IdentifierData,
    ContributorData,
    SubjectData,
)

from api.axis import (
    Axis360CirculationMonitor,
    Axis360API,
    AvailabilityResponseParser,
    CheckoutResponseParser,
    HoldResponseParser,
    HoldReleaseResponseParser,
)

from . import (
    DatabaseTest,
)

from api.circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
)
from api.circulation_exceptions import *

class TestCirculationMonitor(DatabaseTest):

    BIBLIOGRAPHIC_DATA = Metadata(
        DataSource.AXIS_360,
        publisher=u'Random House Inc',
        language='eng', 
        title=u'Faith of My Fathers : A Family Memoir', 
        imprint=u'Random House Inc2',
        published=datetime.datetime(2000, 3, 7, 0, 0),
        primary_identifier=IdentifierData(
            type=Identifier.AXIS_360_ID,
            identifier=u'0003642860'
        ),
        identifiers = [
            IdentifierData(type=Identifier.ISBN, identifier=u'9780375504587')
        ],
        contributors = [
            ContributorData(sort_name=u"McCain, John", 
                            roles=[Contributor.PRIMARY_AUTHOR_ROLE]
                        ),
            ContributorData(sort_name=u"Salter, Mark", 
                            roles=[Contributor.AUTHOR_ROLE]
                        ),
        ],
        subjects = [
            SubjectData(type=Subject.BISAC,
                        identifier=u'BIOGRAPHY & AUTOBIOGRAPHY / Political'),
            SubjectData(type=Subject.FREEFORM_AUDIENCE,
                        identifier=u'Adult'),
        ],
    )

    AVAILABILITY_DATA = CirculationData(
        licenses_owned=9,
        licenses_available=8,
        licenses_reserved=0,
        patrons_in_hold_queue=0,
        last_checked=datetime.datetime(2015, 5, 20, 2, 9, 8),
    )

    def test_process_book(self):
        monitor = Axis360CirculationMonitor(self._db)
        monitor.api = None
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
            [x.type for x in events])
        for e in events:
            eq_(e.start, license_pool.last_checked)

class TestResponseParser(object):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "axis")

    @classmethod
    def sample_data(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data

class TestCheckoutResponseParser(TestResponseParser):

    def test_parse_checkout_success(self):
        data = self.sample_data("checkout_success.xml")
        parser = CheckoutResponseParser()
        parsed = parser.process_all(data)
        assert isinstance(parsed, LoanInfo)
        eq_(Identifier.AXIS_360_ID, parsed.identifier_type)
        eq_(datetime.datetime(2015, 8, 11, 18, 57, 42), 
            parsed.end_date)

        assert isinstance(parsed.fulfillment_info, FulfillmentInfo)
        eq_("http://axis360api.baker-taylor.com/Services/VendorAPI/GetAxisDownload/v2?blahblah", 
            parsed.fulfillment_info.content_link)

    def test_parse_already_checked_out(self):
        data = self.sample_data("already_checked_out.xml")
        parser = CheckoutResponseParser()
        assert_raises(AlreadyCheckedOut, parser.process_all, data)

    def test_parse_not_found_on_remote(self):
        data = self.sample_data("not_found_on_remote.xml")
        parser = CheckoutResponseParser()
        assert_raises(NotFoundOnRemote, parser.process_all, data)

class TestHoldResponseParser(TestResponseParser):

    def test_parse_hold_success(self):
        data = self.sample_data("place_hold_success.xml")
        parser = HoldResponseParser()
        parsed = parser.process_all(data)
        assert isinstance(parsed, HoldInfo)
        eq_(1, parsed.hold_position)

    def test_parse_already_on_hold(self):
        data = self.sample_data("already_on_hold.xml")
        parser = HoldResponseParser()
        assert_raises(AlreadyOnHold, parser.process_all, data)

class TestHoldReleaseResponseParser(TestResponseParser):

    def test_success(self):
        data = self.sample_data("release_hold_success.xml")
        parser = HoldReleaseResponseParser()
        eq_(True, parser.process_all(data))

    def test_failure(self):
        data = self.sample_data("release_hold_failure.xml")
        parser = HoldReleaseResponseParser()
        assert_raises(NotOnHold, parser.process_all, data)

class TestAvailabilityResponseParser(TestResponseParser):

    def test_parse_loan_and_hold(self):
        data = self.sample_data("availability_with_loan_and_hold.xml")
        parser = AvailabilityResponseParser()
        activity = list(parser.process_all(data))
        hold, loan, reserved = sorted(activity, key=lambda x: x.identifier)
        eq_(Identifier.AXIS_360_ID, hold.identifier_type)
        eq_("0012533119", hold.identifier)
        eq_(1, hold.hold_position)
        eq_(None, hold.end_date)

        eq_("0015176429", loan.identifier)
        eq_("http://fulfillment/", loan.fulfillment_info.content_link)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)

        eq_("1111111111", reserved.identifier)
        eq_(datetime.datetime(2015, 1, 1, 13, 11, 11), reserved.end_date)
        eq_(0, reserved.hold_position)
