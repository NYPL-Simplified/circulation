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
        loans, holds = PatronCirculationParser().process_all(data)
        eq_(2, len(loans))
        eq_(2, len(holds))
        l1, l2 = loans
        eq_("1ad589", l1[Identifier][Identifier.THREEM_ID])
        eq_("cgaxr9", l2[Identifier][Identifier.THREEM_ID])
        expect_loan_start = datetime.datetime(2015, 3, 20, 18, 50, 22)
        expect_loan_end = datetime.datetime(2015, 4, 10, 18, 50, 22)
        eq_(expect_loan_start, l1[Loan.start])
        eq_(expect_loan_end, l1[Loan.end])

        # One of the holds is from 'holds' and one is from 'reserves',
        # so we'll check them both.
        h1, h2 = holds
        eq_("d4o8r9", h1[Identifier][Identifier.THREEM_ID])
        expect_hold_start = datetime.datetime(2015, 3, 24, 15, 6, 56)
        expect_hold_end = datetime.datetime(2015, 3, 24, 15, 7, 51)
        eq_(expect_hold_start, h1[Hold.start])
        eq_(expect_hold_end, h1[Hold.end])
        eq_(4, h1[Hold.position])

        eq_("9wd8", h2[Identifier][Identifier.THREEM_ID])
        expect_hold_start = datetime.datetime(2015, 5, 25, 17, 5, 34)
        expect_hold_end = datetime.datetime(2015, 5, 27, 17, 5, 34)
        eq_(expect_hold_start, h2[Hold.start])
        eq_(expect_hold_end, h2[Hold.end])
        eq_(1, h2[Hold.position])


class TestCheckoutResponseParser(TestThreeMAPI):
    def test_parse(self):
        data = self.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_all(data)
        eq_(datetime.datetime(2015, 4, 16, 0, 32, 36), due_date)
