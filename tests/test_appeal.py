import os
import sys
import site
import re

from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

from ..model import (
    Work,
)

class TestAppealAssignment(DatabaseTest):

    def test_assign_appeals(self):
        work = self._work()
        work.assign_appeals(0.50, 0.25, 0.20, 0.05)
        eq_(0.50, work.appeal_character)
        eq_(0.25, work.appeal_language)
        eq_(0.20, work.appeal_setting)
        eq_(0.05, work.appeal_story)
        eq_(Work.CHARACTER_APPEAL, work.primary_appeal)
        eq_(Work.LANGUAGE_APPEAL, work.secondary_appeal)

        # Increase the cutoff point so that there is no secondary appeal.
        work.assign_appeals(0.50, 0.25, 0.20, 0.05, cutoff=0.30)
        eq_(0.50, work.appeal_character)
        eq_(0.25, work.appeal_language)
        eq_(0.20, work.appeal_setting)
        eq_(0.05, work.appeal_story)
        eq_(Work.CHARACTER_APPEAL, work.primary_appeal)
        eq_(Work.NO_APPEAL, work.secondary_appeal)
