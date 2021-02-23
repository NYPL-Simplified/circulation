# encoding: utf-8
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
import datetime
from .. import DatabaseTest
from ...model.complaint import Complaint

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

        eq_(False, work_complaint.for_license_pool)
        eq_(True, lp_complaint.for_license_pool)

    def test_success(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)
        assert abs(datetime.datetime.utcnow() -complaint.timestamp).seconds < 3

        # A second complaint from the same source is folded into the
        # original complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        eq_(False, is_new)
        eq_(complaint.id, complaint2.id)
        eq_("baz", complaint.detail)

        eq_(1, len(self.pool.complaints))

    def test_success_no_source(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_(None, complaint.source)

        # A second identical complaint from no source is treated as a
        # separate complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, None, None
        )
        eq_(True, is_new)
        eq_(None, complaint.source)
        assert complaint2.id != complaint.id

        eq_(2, len(self.pool.complaints))

    def test_failure_no_licensepool(self):
        assert_raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )

    def test_unrecognized_type(self):
        type = "http://librarysimplified.org/terms/problem/no-such-error"
        assert_raises(
            ValueError, Complaint.register, self.pool, type, None, None
        )

    def test_register_resolved(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar", resolved=datetime.datetime.utcnow()
        )
        eq_(True, is_new)
        eq_(self.type, complaint.type)
        eq_("foo", complaint.source)
        eq_("bar", complaint.detail)
        assert abs(datetime.datetime.utcnow() -complaint.timestamp).seconds < 3
        assert abs(datetime.datetime.utcnow() -complaint.resolved).seconds < 3

        # A second complaint from the same source is not folded into the same complaint.
        complaint2, is_new = Complaint.register(
            self.pool, self.type, "foo", "baz"
        )
        eq_(True, is_new)
        assert complaint2.id != complaint.id
        eq_("baz", complaint2.detail)
        eq_(2, len(self.pool.complaints))

    def test_resolve(self):
        complaint, is_new = Complaint.register(
            self.pool, self.type, "foo", "bar"
        )
        complaint.resolve()
        assert complaint.resolved != None
        assert abs(datetime.datetime.utcnow() - complaint.resolved).seconds < 3
