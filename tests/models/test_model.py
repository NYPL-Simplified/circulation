# encoding: utf-8
import pytest
import datetime
from psycopg2.extras import NumericRange
from sqlalchemy import not_
from sqlalchemy.orm.exc import MultipleResultsFound

from .. import DatabaseTest
from ... import classifier
from ...external_search import mock_search_index
from ...config import Configuration
from ...model import (
    DataSource,
    Edition,
    Genre,
    get_one,
    SessionManager,
    Timestamp,
    numericrange_to_tuple,
    tuple_to_numericrange,
)


class TestDatabaseInterface(DatabaseTest):

    def test_get_one(self):

        # When a matching object isn't found, None is returned.
        result = get_one(self._db, Edition)
        assert None == result

        # When a single item is found, it is returned.
        edition = self._edition()
        result = get_one(self._db, Edition)
        assert edition == result

        # When multiple items are found, an error is raised.
        other_edition = self._edition()
        pytest.raises(MultipleResultsFound, get_one, self._db, Edition)

        # Unless they're interchangeable.
        result = get_one(self._db, Edition, on_multiple='interchangeable')
        assert result in self._db.query(Edition)

        # Or specific attributes are passed that limit the results to one.
        result = get_one(
            self._db, Edition,
            title=other_edition.title,
            author=other_edition.author)
        assert other_edition == result

        # A particular constraint clause can also be passed in.
        titles = [ed.title for ed in (edition, other_edition)]
        constraint = not_(Edition.title.in_(titles))
        result = get_one(self._db, Edition, constraint=constraint)
        assert None == result

    def test_initialize_data_does_not_reset_timestamp(self):
        # initialize_data() has already been called, so the database is
        # initialized and the 'site configuration changed' Timestamp has
        # been set. Calling initialize_data() again won't change the
        # date on the timestamp.
        timestamp = get_one(self._db, Timestamp,
                            collection=None,
                            service=Configuration.SITE_CONFIGURATION_CHANGED)
        old_timestamp = timestamp.finish
        SessionManager.initialize_data(self._db)
        assert old_timestamp == timestamp.finish


class TestNumericRangeConversion(object):
    """Test the helper functions that convert between tuples and NumericRange
    objects.
    """

    def test_tuple_to_numericrange(self):
        f = tuple_to_numericrange
        assert None == f(None)

        one_to_ten = f((1,10))
        assert isinstance(one_to_ten, NumericRange)
        assert 1 == one_to_ten.lower
        assert 10 == one_to_ten.upper
        assert True == one_to_ten.upper_inc

        up_to_ten = f((None, 10))
        assert isinstance(up_to_ten, NumericRange)
        assert None == up_to_ten.lower
        assert 10 == up_to_ten.upper
        assert True == up_to_ten.upper_inc

        ten_and_up = f((10,None))
        assert isinstance(ten_and_up, NumericRange)
        assert 10 == ten_and_up.lower
        assert None == ten_and_up.upper
        assert False == ten_and_up.upper_inc

    def test_numericrange_to_tuple(self):
        m = numericrange_to_tuple
        two_to_six_inclusive = NumericRange(2,6, '[]')
        assert (2,6) == m(two_to_six_inclusive)
        two_to_six_exclusive = NumericRange(2,6, '()')
        assert (3,5) == m(two_to_six_exclusive)
