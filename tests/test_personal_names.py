# encoding: utf-8
from StringIO import StringIO
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
        unchanged(u"Bitshifter, Bob")
        unchanged(u"Prince")
        unchanged(u"Pope Francis")
        unchanged(u"Lord Byron")
        unchanged(u"Heliodorus (bp. of Tricca.)")
        unchanged(u"谢新源 (Xie Xinyuan)")

        sort_name = m(u"Bob Bitshifter")
        eq_(u"Bitshifter, Bob", sort_name)

        # foreign characters don't confuse the algorithm
        sort_name = m(u"Боб Битшифтер")
        eq_(u"Битшифтер, Боб", sort_name)

        sort_name = m(u"Bob Bitshifter, Jr.")
        eq_(u"Bitshifter, Bob Jr.", sort_name)

        sort_name = m(u"Bob Bitshifter, III")
        eq_(u"Bitshifter, Bob III", sort_name)

        eq_("Beck, James M. (James Montgomery)",
            m("James M. (James Montgomery) Beck"))

        # all forms of PhD are recognized
        sort_name = m(u"John Doe, PhD")
        eq_(u"Doe, John PhD", sort_name)
        sort_name = m(u"John Doe, Ph.D.")
        eq_(u"Doe, John PhD", sort_name)
        sort_name = m(u"John Doe, Ph D")
        eq_(u"Doe, John PhD", sort_name)
        sort_name = m(u"John Doe, Ph. D.")
        eq_(u"Doe, John PhD", sort_name)
        sort_name = m(u"John Doe, PHD")
        eq_(u"Doe, John PhD", sort_name)

        sort_name = m(u"John Doe, M.D.")
        eq_(u"Doe, John MD", sort_name)

        # corporate names are unchanged
        unchanged(u"Church of Jesus Christ of Latter-day Saints")
        unchanged(u"(C) 2006 Vanguard")

        # NOTE: These results are not the best.
        eq_("Pope XVI, Benedict", m(u"Pope Benedict XVI"))
        eq_("Lord Alfred, Tennyson", m(u"Alfred, Lord Tennyson"))
        eq_("Lord Lennox, William", m("Lord William Lennox"))
        sort_name = m(u"Bob, The Grand Duke of Awesomeness")
        eq_(u"The Grand Bob, Duke of Awesomeness", sort_name)

    def test_name_tidy(self):
        # remove improper comma
        sort_name = display_name_to_sort_name(u"Bitshifter, Bob,")
        eq_(u"Bitshifter, Bob", sort_name)

        # remove improper period
        sort_name = display_name_to_sort_name(u"Bitshifter, Bober.")
        eq_(u"Bitshifter, Bober", sort_name)

        # retain proper period
        sort_name = display_name_to_sort_name(u"Bitshifter, B.")
        eq_(u"Bitshifter, B.", sort_name)





