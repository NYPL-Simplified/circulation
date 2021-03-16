# encoding: utf-8
from StringIO import StringIO
import datetime
import os
import sys
import site
import re
import tempfile

from ..model import (
    Contributor,
    DataSource,
    Work,
    Identifier,
    Edition,
    create,
    get_one,
    get_one_or_create,
)

from ..testing import (
    DatabaseTest,
    DummyHTTPClient,
)

from ..util.personal_names import (
    display_name_to_sort_name,
)
from ..mock_analytics_provider import MockAnalyticsProvider



class TestNameConversions(DatabaseTest):

    def test_display_name_to_sort_name(self):
        # Make sure the sort name algorithm processes the messy reality of contributor
        # names in a way we expect.

        m = display_name_to_sort_name

        # no input means don't do anything
        sort_name = m(None)
        assert None == sort_name

        def unchanged(x):
            # Verify that the input is already a sort name -- either
            # because it's in "Family, Given" format or for some other
            # reason.
            assert x == m(x)
        unchanged(u"Bitshifter, Bob")
        unchanged(u"Prince")
        unchanged(u"Pope Francis")
        unchanged(u"Heliodorus (bp. of Tricca.)")
        unchanged(u"谢新源 (Xie Xinyuan)")
        unchanged(u"Alfred, Lord Tennyson")
        unchanged(u"Bob, The Grand Duke of Awesomeness")

        sort_name = m(u"Bob Bitshifter")
        assert u"Bitshifter, Bob" == sort_name

        # foreign characters don't confuse the algorithm
        sort_name = m(u"Боб Битшифтер")
        assert u"Битшифтер, Боб" == sort_name

        sort_name = m(u"Bob Bitshifter, Jr.")
        assert u"Bitshifter, Bob Jr." == sort_name

        sort_name = m(u"Bob Bitshifter, III")
        assert u"Bitshifter, Bob III" == sort_name

        assert ("Beck, James M. (James Montgomery)" ==
            m("James M. (James Montgomery) Beck"))

        # all forms of PhD are recognized
        sort_name = m(u"John Doe, PhD")
        assert u"Doe, John PhD" == sort_name
        sort_name = m(u"John Doe, Ph.D.")
        assert u"Doe, John PhD" == sort_name
        sort_name = m(u"John Doe, Ph D")
        assert u"Doe, John PhD" == sort_name
        sort_name = m(u"John Doe, Ph. D.")
        assert u"Doe, John PhD" == sort_name
        sort_name = m(u"John Doe, PHD")
        assert u"Doe, John PhD" == sort_name

        sort_name = m(u"John Doe, M.D.")
        assert u"Doe, John MD" == sort_name

        # corporate names are unchanged
        unchanged(u"Church of Jesus Christ of Latter-day Saints")
        unchanged(u"(C) 2006 Vanguard")

        # NOTE: These results are not the best.
        assert "XVI, Pope Benedict" == m(u"Pope Benedict XVI")
        assert "Byron, Lord" == m(u"Lord Byron")

    def test_name_tidy(self):
        # remove improper comma
        sort_name = display_name_to_sort_name(u"Bitshifter, Bob,")
        assert u"Bitshifter, Bob" == sort_name

        # remove improper period
        sort_name = display_name_to_sort_name(u"Bitshifter, Bober.")
        assert u"Bitshifter, Bober" == sort_name

        # retain proper period
        sort_name = display_name_to_sort_name(u"Bitshifter, B.")
        assert u"Bitshifter, B." == sort_name





