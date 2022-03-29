# encoding: utf-8
import pytest

from ...model.complaint import Complaint
from ...util.datetime_helpers import utc_now

COMPLAINT_TYPE = "http://librarysimplified.org/terms/problem/wrong-genre"


class TestComplaint:

    def test_for_license_pool(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: A Complaint
        WHEN:  The Complaint is registered
        THEN:  Ensure the complaint is lodged against the LicensePool
        """
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        work_complaint, _ = Complaint.register(
            pool, COMPLAINT_TYPE, "yes", "okay"
        )

        lp_type = COMPLAINT_TYPE.replace('wrong-genre', 'cannot-render')
        lp_complaint, _ = Complaint.register(
            pool, lp_type, "yes", "okay"
        )

        assert False == work_complaint.for_license_pool
        assert True == lp_complaint.for_license_pool

    def test_success(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: Two Complaints
        WHEN:  A complaint with the same source is registered
        THEN:  Ensure the second complaint is folded into the original complaint
        """
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        complaint, is_new = Complaint.register(
            pool, COMPLAINT_TYPE, "foo", "bar"
        )
        assert True == is_new
        assert COMPLAINT_TYPE == complaint.type
        assert "foo" == complaint.source
        assert "bar" == complaint.detail
        assert abs(utc_now() - complaint.timestamp).seconds < 3

        # A second complaint from the same source is folded into the
        # original complaint.
        complaint2, is_new = Complaint.register(
            pool, COMPLAINT_TYPE, "foo", "baz"
        )
        assert False == is_new
        assert complaint.id == complaint2.id
        assert "baz" == complaint.detail

        assert 1 == len(pool.complaints)

    def test_success_no_source(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: Two Complaints
        WHEN:  There is no source for either complaint
        THEN:  Ensure that the complaints are treated as two separate complaints
        """
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        complaint, is_new = Complaint.register(
            pool, COMPLAINT_TYPE, None, None
        )
        assert True == is_new
        assert COMPLAINT_TYPE == complaint.type
        assert None == complaint.source

        # A second identical complaint from no source is treated as a
        # separate complaint.
        complaint2, is_new = Complaint.register(
            pool, COMPLAINT_TYPE, None, None
        )
        assert True ==  is_new
        assert None == complaint.source
        assert complaint2.id != complaint.id

        assert 2 == len(pool.complaints)

    def test_failure_no_licensepool(self, db_session):
        """
        GIVEN: A Complaint
        WHEN:  The complaint is registered with no License Pool
        THEN:  Ensure a ValueError is raised
        """
        pytest.raises(
            ValueError, Complaint.register, None, COMPLAINT_TYPE, None, None
        )

    def test_unrecognized_type(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: A Complaint
        WHEN:  The complaint is registered with an invalid type
        THEN:  Ensure a ValueError is raised
        """
        complaint_type = COMPLAINT_TYPE.replace('wrong-genre', 'no-such-error')
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        pytest.raises(
            ValueError, Complaint.register, pool, complaint_type, None, None
        )

    def test_register_resolved(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: Two Complaints
        WHEN:  One complaint is resolved but the other is not
        THEN:  Ensure the complaints are not folded together
        """
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        complaint, is_new = Complaint.register(
            pool, COMPLAINT_TYPE, "foo", "bar", resolved=utc_now()
        )
        assert True == is_new
        assert COMPLAINT_TYPE == complaint.type
        assert "foo" == complaint.source
        assert "bar" == complaint.detail
        assert abs(utc_now() - complaint.timestamp).seconds < 3
        assert abs(utc_now() - complaint.resolved).seconds < 3

        # A second complaint from the same source is not folded into the same complaint.
        complaint2, is_new = Complaint.register(
            pool, COMPLAINT_TYPE, "foo", "baz"
        )
        assert True == is_new
        assert complaint2.id != complaint.id
        assert "baz" == complaint2.detail
        assert 2 == len(pool.complaints)

    def test_resolve(self, db_session, create_edition, create_licensepool):
        """
        GIVEN: A Complaint
        WHEN:  The complaint is resolved
        THEN:  Ensure that the complaint's resolved status is not None
        """
        edition = create_edition(db_session)
        pool = create_licensepool(db_session, edition=edition)
        complaint, _ = Complaint.register(
            pool, COMPLAINT_TYPE, "foo", "bar"
        )
        complaint.resolve()
        assert complaint.resolved != None
        assert abs(utc_now() - complaint.resolved).seconds < 3
