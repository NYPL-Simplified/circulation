import datetime
import os
from nose.tools import (
    set_trace, eq_,
    assert_raises,
)

from ..threem import (
    ThreeMAPI,
    PatronCirculationParser,
    CheckoutResponseParser,
)

from . import (
    DatabaseTest,
)

from ..core.model import (
    Identifier,
    Loan,
    Hold,
)

class TestThreeMAPI(DatabaseTest):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "threem")

    @classmethod
    def sample_data(self, filename):
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data


class TestPatronCirculationParser(TestThreeMAPI):

    def test_parse(self):
        data = self.sample_data("checkouts.xml")
        loans, holds, reserves = PatronCirculationParser().process_all(data)
        eq_(2, len(loans))
        eq_(1, len(holds))
        eq_(1, len(reserves))
        l1, l2 = loans
        eq_("1ad589", l1[Identifier][Identifier.THREEM_ID])
        eq_("cgaxr9", l2[Identifier][Identifier.THREEM_ID])
        expect_loan_start = datetime.datetime(2015, 3, 20, 18, 50, 22)
        expect_loan_end = datetime.datetime(2015, 4, 10, 18, 50, 22)
        eq_(expect_loan_start, l1[Loan.start])
        eq_(expect_loan_end, l1[Loan.end])

        [h1] = holds
        eq_("d4o8r9", h1[Identifier][Identifier.THREEM_ID])
        expect_hold_start = datetime.datetime(2015, 3, 24, 15, 6, 56)
        expect_hold_end = datetime.datetime(2015, 3, 24, 15, 7, 51)
        eq_(expect_hold_start, h1[Hold.start])
        eq_(expect_hold_end, h1[Hold.end])
        eq_(4, h1[Hold.position])

        [r1] = reserves
        eq_("9wd8", r1[Identifier][Identifier.THREEM_ID])
        expect_hold_start = datetime.datetime(2015, 5, 25, 17, 5, 34)
        expect_hold_end = datetime.datetime(2015, 5, 27, 17, 5, 34)
        eq_(expect_hold_start, r1[Hold.start])
        eq_(expect_hold_end, r1[Hold.end])
        eq_(1, r1[Hold.position])

class TestCheckoutResponseParser(TestThreeMAPI):
    def test_parse(self):
        data = self.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_all(data)
        eq_(datetime.datetime(2015, 4, 16, 0, 32, 36), due_date)


class TestSyncBookshelf(TestThreeMAPI):
    
    def basic_test(self):
        data = self.sample_data("checkouts.xml")
        loans, holds, reserves = PatronCirculationParser().process_all(data)
        patron = self._patron()
        
        api = ThreeMAPI(self._db)
        api.sync_bookshelf(patron, loans, holds, reserves)

        # The patron should have two loans and two holds.
        l1, l2 = patron.loans
        h1, h2 = patron.holds

        eq_(datetime.datetime(2015, 3, 20, 18, 50, 22), l1.start)
        eq_(datetime.datetime(2015, 4, 10, 18, 50, 22), l1.end)

        eq_(datetime.datetime(2015, 3, 13, 13, 38, 19), l2.start)
        eq_(datetime.datetime(2015, 4, 3, 13, 38, 19), l2.end)

        # This hold has no end date because there's no decision to be
        # made. The patron is fourth in line.
        eq_(datetime.datetime(2015, 3, 24, 15, 6, 56), h1.start)
        eq_(None, h1.end)
        eq_(4, h1.position)

        # The hold has an end date. It's time for the patron to decide
        # whether or not to check out this book.
        eq_(datetime.datetime(2015, 5, 25, 17, 5, 34), h2.start)
        eq_(datetime.datetime(2015, 5, 27, 17, 5, 34), h2.end)
        eq_(0, h1.position)
