from ...testing import (
    DatabaseTest,
)

from ...model import (
    Work,
)

class TestAppealAssignment(DatabaseTest):

    def test_assign_appeals(self):
        work = self._work()
        work.assign_appeals(0.50, 0.25, 0.20, 0.05)
        assert 0.50 == work.appeal_character
        assert 0.25 == work.appeal_language
        assert 0.20 == work.appeal_setting
        assert 0.05 == work.appeal_story
        assert Work.CHARACTER_APPEAL == work.primary_appeal
        assert Work.LANGUAGE_APPEAL == work.secondary_appeal

        # Increase the cutoff point so that there is no secondary appeal.
        work.assign_appeals(0.50, 0.25, 0.20, 0.05, cutoff=0.30)
        assert 0.50 == work.appeal_character
        assert 0.25 == work.appeal_language
        assert 0.20 == work.appeal_setting
        assert 0.05 == work.appeal_story
        assert Work.CHARACTER_APPEAL == work.primary_appeal
        assert Work.NO_APPEAL == work.secondary_appeal
