# encoding: utf-8
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
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

class TestSessionManager(DatabaseTest):

    def test_refresh_materialized_views(self):
        work = self._work(fiction=True, with_license_pool=True,
                          genre="Science Fiction")
        romance, ignore = Genre.lookup(self._db, "Romance")
        work.genres.append(romance)
        fiction = self._lane(display_name="Fiction", fiction=True)
        nonfiction = self._lane(display_name="Nonfiction", fiction=False)

        from ...model import MaterializedWorkWithGenre as mwg

        # There are no items in the materialized views.
        eq_([], self._db.query(mwg).all())

        # The lane sizes are wrong.
        fiction.size = 100
        nonfiction.size = 100

        class Mock(object):
            def count_works(self, filter):
                if filter.fiction == True:
                    return 51
                else:
                    return 22

        with mock_search_index(Mock()):
            SessionManager.refresh_materialized_views(self._db)

        # The work has been added to the materialized view. (It was
        # added twice because it's filed under two genres.)
        eq_([work.id, work.id], [x.works_id for x in self._db.query(mwg)])

        # Both lanes have had .size set to the value returned by
        # count_works() for the corresponding filter.
        #
        # (NOTE: there's no longer any connection between refreshing
        # the materialized view and updating the lane sizes -- they're
        # now two unrelated things that both need to happen
        # periodically.)
        eq_(51, fiction.size)
        eq_(22, nonfiction.size)

class TestDatabaseInterface(DatabaseTest):

    def test_get_one(self):

        # When a matching object isn't found, None is returned.
        result = get_one(self._db, Edition)
        eq_(None, result)

        # When a single item is found, it is returned.
        edition = self._edition()
        result = get_one(self._db, Edition)
        eq_(edition, result)

        # When multiple items are found, an error is raised.
        other_edition = self._edition()
        assert_raises(MultipleResultsFound, get_one, self._db, Edition)

        # Unless they're interchangeable.
        result = get_one(self._db, Edition, on_multiple='interchangeable')
        assert result in self._db.query(Edition)

        # Or specific attributes are passed that limit the results to one.
        result = get_one(
            self._db, Edition,
            title=other_edition.title,
            author=other_edition.author)
        eq_(other_edition, result)

        # A particular constraint clause can also be passed in.
        titles = [ed.title for ed in (edition, other_edition)]
        constraint = not_(Edition.title.in_(titles))
        result = get_one(self._db, Edition, constraint=constraint)
        eq_(None, result)

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
        eq_(old_timestamp, timestamp.finish)

class TestMaterializedViews(DatabaseTest):

    def test_license_pool_is_works_preferred_license_pool(self):
        """Verify that the license_pool_id stored in the materialized views
        identifies the LicensePool associated with the Work's
        presentation edition, not some other LicensePool.
        """
        # Create a Work with two LicensePools
        work = self._work(with_license_pool=True)
        [pool1] = work.license_pools
        edition2, pool2 = self._edition(with_license_pool=True)
        work.license_pools.append(pool1)
        eq_([pool1], work.presentation_edition.license_pools)
        work.presentation_ready = True
        work.simple_opds_entry = '<entry>'
        work.assign_genres_from_weights({classifier.Fantasy : 1})

        # Make sure the Work shows up in the materialized view.
        SessionManager.refresh_materialized_views(self._db)

        from ...model import MaterializedWorkWithGenre as mwgc
        [mwg] = self._db.query(mwgc).all()

        eq_(pool1.id, mwg.license_pool_id)

        # If we change the Work's preferred edition, we change the
        # license_pool_id that gets stored in the materialized views.
        work.set_presentation_edition(edition2)
        SessionManager.refresh_materialized_views(self._db)
        [mwg] = self._db.query(mwgc).all()

        eq_(pool2.id, mwg.license_pool_id)

    def test_license_data_source_is_stored_in_views(self):
        """Verify that the data_source_name stored in the materialized view
        is the DataSource associated with the LicensePool, not the
        DataSource associated with the presentation Edition.
        """

        # Create a Work whose LicensePool has three Editions: one from
        # Gutenberg (created by default), one from the admin interface
        # (created manually), and one generated by the presentation
        # edition generator, which synthesizes the other two.
        work = self._work(with_license_pool=True)

        [pool] = work.license_pools
        gutenberg_edition = pool.presentation_edition

        identifier = pool.identifier
        staff_edition = self._edition(
            data_source_name=DataSource.LIBRARY_STAFF,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier
        )
        staff_edition.title = u"staff chose this title"
        staff_edition.sort_title = u"staff chose this title"
        pool.set_presentation_edition()
        work.set_presentation_edition(pool.presentation_edition)

        # The presentation edition has the title taken from the admin
        # interface, but it was created by the presentation edition
        # generator.
        presentation_edition = pool.presentation_edition
        eq_("staff chose this title", presentation_edition.title)
        eq_(DataSource.PRESENTATION_EDITION,
            presentation_edition.data_source.name
        )

        # Make sure the Work will show up in the materialized view.
        work.presentation_ready = True
        work.simple_opds_entry = '<entry>'
        work.assign_genres_from_weights({classifier.Fantasy : 1})

        SessionManager.refresh_materialized_views(self._db)

        from ...model import MaterializedWorkWithGenre as mwgc
        [mwg] = self._db.query(mwgc).all()

        # We would expect the data source to be Gutenberg, since
        # that's the edition associated with the LicensePool, and not
        # the data source of the Work's presentation edition.
        eq_(pool.data_source.name, mwg.name)

        # However, we would expect the title of the work to come from
        # the presentation edition.
        eq_("staff chose this title", mwg.sort_title)

        # And since the data_source_id is the ID of the data source
        # associated with the license pool, we would expect it to be
        # the data source ID of the license pool.
        eq_(pool.data_source.id, mwg.data_source_id)

    def test_work_on_same_list_twice(self):
        # Here's the NYT best-seller list.
        cl, ignore = self._customlist(num_entries=0)

        # Here are two Editions containing data from the NYT
        # best-seller list.
        now = datetime.datetime.utcnow()
        earlier = now - datetime.timedelta(seconds=3600)
        edition1 = self._edition()
        entry1, ignore = cl.add_entry(edition1, first_appearance=earlier)

        edition2 = self._edition()
        entry2, ignore = cl.add_entry(edition2, first_appearance=now)

        # In a shocking turn of events, we've determined that the two
        # editions are slight title variants of the same work.
        romance, ignore = Genre.lookup(self._db, "Romance")
        work = self._work(with_license_pool=True, genre=romance)
        entry1.work = work
        entry2.work = work
        self._db.commit()

        # The materialized view can handle this revelation
        # and stores the two list entries in different rows.
        SessionManager.refresh_materialized_views(self._db)
        from ...model import MaterializedWorkWithGenre as mw
        [o1, o2] = self._db.query(mw).order_by(mw.list_edition_id)

        # Both MaterializedWorkWithGenre objects are on the same
        # list, associated with the same work, the same genre,
        # and the same presentation edition.
        for o in (o1, o2):
            eq_(cl.id, o.list_id)
            eq_(work.id, o.works_id)
            eq_(romance.id, o.genre_id)
            eq_(work.presentation_edition.id, o.editions_id)

        # But they are associated with different list editions.
        eq_(edition1.id, o1.list_edition_id)
        eq_(edition2.id, o2.list_edition_id)

class TestNumericRangeConversion(object):
    """Test the helper functions that convert between tuples and NumericRange
    objects.
    """

    def test_tuple_to_numericrange(self):
        f = tuple_to_numericrange
        eq_(None, f(None))

        one_to_ten = f((1,10))
        assert isinstance(one_to_ten, NumericRange)
        eq_(1, one_to_ten.lower)
        eq_(10, one_to_ten.upper)
        eq_(True, one_to_ten.upper_inc)

        up_to_ten = f((None, 10))
        assert isinstance(up_to_ten, NumericRange)
        eq_(None, up_to_ten.lower)
        eq_(10, up_to_ten.upper)
        eq_(True, up_to_ten.upper_inc)

        ten_and_up = f((10,None))
        assert isinstance(ten_and_up, NumericRange)
        eq_(10, ten_and_up.lower)
        eq_(None, ten_and_up.upper)
        eq_(False, ten_and_up.upper_inc)

    def test_numericrange_to_tuple(self):
        m = numericrange_to_tuple
        two_to_six_inclusive = NumericRange(2,6, '[]')
        eq_((2,6), m(two_to_six_inclusive))
        two_to_six_exclusive = NumericRange(2,6, '()')
        eq_((3,5), m(two_to_six_exclusive))
