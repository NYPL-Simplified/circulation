# encoding: utf-8
from io import StringIO
import datetime
import os
import sys
import site
import re
import tempfile

from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    assert_not_equal,
    eq_,
    set_trace,
)

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

from . import (
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
        eq_(None, sort_name)

        def unchanged(x):
            # Verify that the input is already a sort name -- either
            # because it's in "Family, Given" format or for some other
            # reason.
            eq_(x, m(x))
        unchanged("Bitshifter, Bob")
        unchanged("Prince")
        unchanged("Pope Francis")
        unchanged("Heliodorus (bp. of Tricca.)")
        unchanged("谢新源 (Xie Xinyuan)")
        unchanged("Alfred, Lord Tennyson")
        unchanged("Bob, The Grand Duke of Awesomeness")

        sort_name = m("Bob Bitshifter")
        eq_("Bitshifter, Bob", sort_name)

        # foreign characters don't confuse the algorithm
        sort_name = m("Боб Битшифтер")
        eq_("Битшифтер, Боб", sort_name)

        sort_name = m("Bob Bitshifter, Jr.")
        eq_("Bitshifter, Bob Jr.", sort_name)

        sort_name = m("Bob Bitshifter, III")
        eq_("Bitshifter, Bob III", sort_name)

        eq_("Beck, James M. (James Montgomery)",
            m("James M. (James Montgomery) Beck"))

        # all forms of PhD are recognized
        sort_name = m("John Doe, PhD")
        eq_("Doe, John PhD", sort_name)
        sort_name = m("John Doe, Ph.D.")
        eq_("Doe, John PhD", sort_name)
        sort_name = m("John Doe, Ph D")
        eq_("Doe, John PhD", sort_name)
        sort_name = m("John Doe, Ph. D.")
        eq_("Doe, John PhD", sort_name)
        sort_name = m("John Doe, PHD")
        eq_("Doe, John PhD", sort_name)

        sort_name = m("John Doe, M.D.")
        eq_("Doe, John MD", sort_name)

        # corporate names are unchanged
        unchanged("Church of Jesus Christ of Latter-day Saints")
        unchanged("(C) 2006 Vanguard")

        # NOTE: These results are not the best.
        eq_("XVI, Pope Benedict", m("Pope Benedict XVI"))
        eq_("Byron, Lord", m("Lord Byron"))

    def test_name_tidy(self):
        # remove improper comma
        sort_name = display_name_to_sort_name("Bitshifter, Bob,")
        eq_("Bitshifter, Bob", sort_name)

        # remove improper period
        sort_name = display_name_to_sort_name("Bitshifter, Bober.")
        eq_("Bitshifter, Bober", sort_name)

        # retain proper period
        sort_name = display_name_to_sort_name("Bitshifter, B.")
        eq_("Bitshifter, B.", sort_name)





