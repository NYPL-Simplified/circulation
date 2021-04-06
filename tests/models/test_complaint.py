# encoding: utf-8
import pytest

from ...testing import DatabaseTest
from ...model.complaint import Complaint
from ...util.datetime_helpers import utc_now

class TestComplaint(DatabaseTest):

    def setup_method(self):
        super(TestComplaint, self).setup_method()
        self.edition, self.pool = self._edition(with_license_pool=True)
        self.type = "http://librarysimplified.org/terms/problem/wrong-genre"

    def test_for_license_pool(self):
        work_complaint, is_new = Complaint.register(
            self.pool, self.type, "yes", "okay"
        )

        lp_type = self.type.replace('wrong-genre', 'cannot-render')
        lp_complaint, is_new = Complaint.register(
            self.pool, lp_type, "yes", "okay")

        assert False == work_complaint.for_license_pool
        assert True == lp_complaint.for_license_pool

    def test_success(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        assert True == is_new
        assert self.type == complaint.type
        assert "foo" == complaint.source
        assert "bar" == complaint.detail
        assert abs(utc_now() -complaint.timestamp).seconds < 3

        # A second complaint from the same source is folded into the
        # original complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        assert False == is_new
        assert complaint.id == complaint2.id
        assert "baz" == complaint.detail

        assert 1 == len(self.pool.complaints)

    def test_success_no_source(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        assert True == is_new
        assert self.type == complaint.type
        assert None == complaint.source

        # A second identical complaint from no source is treated as a
        # separate complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        assert True == is_new
        assert None == complaint.source
        assert complaint2.id != complaint.id

        assert 2 == len(self.pool.complaints)

    def test_failure_no_licensepool(self):
        pytest.raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )

    def test_unrecognized_type(self):
        type = "http://librarysimplified.org/terms/problem/no-such-error"
        pytest.raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )

    def test_register_resolved(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar", resolved=utc_now()
        )
        assert True == is_new
        assert self.type == complaint.type
        assert "foo" == complaint.source
        assert "bar" == complaint.detail
        assert abs(utc_now() -complaint.timestamp).seconds < 3
        assert abs(utc_now() -complaint.resolved).seconds < 3

        # A second complaint from the same source is not folded into the same complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        assert True == is_new
        assert complaint2.id != complaint.id
        assert "baz" == complaint2.detail
        assert 2 == len(self.pool.complaints)

    def test_resolve(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        complaint.resolve()
        assert complaint.resolved != None
        assert abs(utc_now() - complaint.resolved).seconds < 3
