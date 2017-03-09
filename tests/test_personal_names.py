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

from model import (
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

from util.personal_names import (
    display_name_to_sort_name,
)
from mock_analytics_provider import MockAnalyticsProvider



class TestNameConversions(DatabaseTest):

    def test_display_name_to_sort_name(self):
        # Make sure the sort name algorithm processes the messy reality of contributor 
        # names in a way we expect.

        # no input means don't do anything
        sort_name = display_name_to_sort_name(None)
        eq_(None, sort_name)

        # already sort-ready input means don't do anything
        sort_name = display_name_to_sort_name(u"Bitshifter, Bob")
        eq_(u"Bitshifter, Bob", sort_name)

        sort_name = display_name_to_sort_name(u"Prince")
        eq_(u"Prince", sort_name)

        sort_name = display_name_to_sort_name(u"Bob Bitshifter")
        eq_(u"Bitshifter, Bob", sort_name)

        # foreign characters don't confuse the algorithm
        sort_name = display_name_to_sort_name(u"Боб Битшифтер")
        eq_(u"Битшифтер, Боб", sort_name)

        sort_name = display_name_to_sort_name(u"Bob Bitshifter, Jr.")
        eq_(u"Bitshifter, Bob Jr.", sort_name)

        sort_name = display_name_to_sort_name(u"Bob Bitshifter, III")
        eq_(u"Bitshifter, Bob III", sort_name)

        # already having a comma still gets good results
        sort_name = display_name_to_sort_name(u"Bob, The Grand Duke of Awesomeness")
        eq_(u"Bob, Duke of Awesomeness The Grand", sort_name)





