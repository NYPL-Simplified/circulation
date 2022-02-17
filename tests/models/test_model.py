# encoding: utf-8
import pytest
from psycopg2.extras import NumericRange
from sqlalchemy import not_
from sqlalchemy.orm.exc import MultipleResultsFound

from ...external_search import mock_search_index
from ...config import Configuration
from ...model import (
    Edition,
    get_one,
    SessionManager,
    Timestamp,
    numericrange_to_tuple,
    tuple_to_numericrange,
)


class TestDatabaseInterface:

    def test_get_one(self, db_session, create_edition):
        """
        GIVEN: A database query to retrieve a single row
        WHEN:  Querying the database
        THEN:  One result is returned or an exception is raised
        """

        # When a matching object isn't found, None is returned.
        result = get_one(db_session, Edition)
        assert None == result

        # When a single item is found, it is returned.
        edition = create_edition(db_session, title="Default Title")
        result = get_one(db_session, Edition)
        assert edition == result

        # When multiple items are found, an error is raised.
        other_edition = create_edition(db_session, title="Default Title")
        pytest.raises(MultipleResultsFound, get_one, db_session, Edition)

        # Unless they're interchangeable.
        result = get_one(db_session, Edition, on_multiple='interchangeable')
        assert result in db_session.query(Edition)

        # Or specific attributes are passed that limit the results to one.
        result = get_one(
            db_session, Edition,
            title=other_edition.title,
            author=other_edition.author)
        assert other_edition == result

        # A particular constraint clause can also be passed in.
        titles = [ed.title for ed in (edition, other_edition)]
        constraint = not_(Edition.title.in_(titles))
        result = get_one(db_session, Edition, constraint=constraint)
        assert None == result

    def test_initialize_data_does_not_reset_timestamp(self, db_session, initalize_data):
        """
        GIVEN: An initialized database with data
        WHEN:  Re-initializing data
        THEN:  The Timestamp is unchanged from the first initialization
        """
        # initialize_data() has already been called, so the database is
        # initialized and the 'site configuration changed' Timestamp has
        # been set. Calling initialize_data() again won't change the
        # date on the timestamp.
        timestamp = get_one(db_session, Timestamp,
                            collection=None,
                            service=Configuration.SITE_CONFIGURATION_CHANGED)
        old_timestamp = timestamp.finish
        SessionManager.initialize_data(db_session)
        assert old_timestamp == timestamp.finish


class TestNumericRangeConversion(object):
    """Test the helper functions that convert between tuples and NumericRange
    objects.
    """

    @pytest.mark.parametrize(
        'lower,upper,upper_inc',
        [
            (1, 10, True),
            (None, 10, True),
            (10, None, False),
        ],
        ids=[
            "one_to_ten",
            "up_to_ten",
            "ten_and_up"
        ]
    )
    def test_tuple_to_numericrange(self, lower, upper, upper_inc):
        """
        GIVEN: A lower and upper bound of numbers or None
        WHEN:  Converting a tuple to a NumericRange
        THEN:  The bounds are correctly set
        """
        f = tuple_to_numericrange
        assert None == f(None)

        range = f((lower, upper))
        assert isinstance(range, NumericRange)
        assert lower == range.lower
        assert upper == range.upper
        assert upper_inc == range.upper_inc

    @pytest.mark.parametrize(
        'lower,upper,clusivity,tuple_lower,tuple_upper',
        [
            (2, 6, '[]', 2, 6),
            (2, 6, '()', 3, 5)
        ],
        ids=[
            "two_to_six_inclusive",
            "two_to_six_exclusive"
        ]
    )
    def test_numericrange_to_tuple(self, lower, upper, clusivity, tuple_lower, tuple_upper):
        """
        GIVEN: A NumericRange
        WHEN:  Converting a NumeriRange to a tuple inclusively or exclusively
        THEN:  Correct tuple range is returned
        """
        m = numericrange_to_tuple
        range = NumericRange(lower, upper, clusivity)

        assert (tuple_lower, tuple_upper) == m(range)
