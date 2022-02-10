from ...model import Work


class TestAppealsAssignment:

    def test__assign_appeals(self, db_session, create_work):
        """
        GIVEN: A Work item
        WHEN:  Assigning appeals
        THEN:  Verify that the correct appeals have been assigned
        """
        work = create_work(db_session)
        work.assign_appeals(0.50, 0.25, 0.20, 0.05)
        assert 0.50 == work.appeal_character
        assert 0.25 == work.appeal_language
        assert 0.20 == work.appeal_setting
        assert 0.05 == work.appeal_story
        assert Work.CHARACTER_APPEAL == work.primary_appeal
        assert Work.LANGUAGE_APPEAL == work.secondary_appeal

        # WHEN: Increasing the cutoff point
        # THEN: There is no secondary appeal.
        work.assign_appeals(0.50, 0.25, 0.20, 0.05, cutoff=0.30)
        assert 0.50 == work.appeal_character
        assert 0.25 == work.appeal_language
        assert 0.20 == work.appeal_setting
        assert 0.05 == work.appeal_story
        assert Work.CHARACTER_APPEAL == work.primary_appeal
        assert Work.NO_APPEAL == work.secondary_appeal
