import datetime

from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)

from psycopg2.extras import NumericRange

from . import (
    DatabaseTest,
)

import classifier
from classifier import (
    Classifier,
)

from lane import (
    Facets,
    Pagination,
    Lane,
    LaneList,
    UndefinedLane,
)

from config import (
    Configuration, 
    temp_config,
)

from model import (
    get_one_or_create,
    DataSource,
    Edition,
    Genre,
    Identifier,
    LicensePool,
    SessionManager,
    Work,
    WorkGenre,
)


class TestFacets(object):

    def test_facet_groups(self):

        facets = Facets(
            Facets.COLLECTION_MAIN, Facets.AVAILABLE_ALL, Facets.ORDER_TITLE
        )
        all_groups = list(facets.facet_groups)

        # By default, there are a 9 facet transitions: three groups of three.
        eq_(9, len(all_groups))

        # available=all, collection=main, and order=title are the selected
        # facets.
        selected = sorted([x[:2] for x in all_groups if x[-1] == True])
        eq_(
            [('available', 'all'), ('collection', 'main'), ('order', 'title')],
            selected
        )

        test_facet_policy = {
            "enabled" : {
                Facets.ORDER_FACET_GROUP_NAME : [
                    Facets.ORDER_WORK_ID, Facets.ORDER_TITLE
                ],
                Facets.COLLECTION_FACET_GROUP_NAME : [Facets.COLLECTION_FULL],
                Facets.AVAILABILITY_FACET_GROUP_NAME : [Facets.AVAILABLE_ALL],
            },
            "default" : {
                Facets.ORDER_FACET_GROUP_NAME : Facets.ORDER_TITLE,
                Facets.COLLECTION_FACET_GROUP_NAME : Facets.COLLECTION_FULL,
                Facets.AVAILABILITY_FACET_GROUP_NAME : Facets.AVAILABLE_ALL,
            }
        }
        with temp_config() as config:
            config['policies'] = {
                Configuration.FACET_POLICY : test_facet_policy
            }
            facets = Facets(None, None, Facets.ORDER_TITLE)
            all_groups = list(facets.facet_groups)

            # We have disabled almost all the facets, so the list of
            # facet transitions includes only two items.
            #
            # 'Sort by title' was selected, and it shows up as the selected
            # item in this facet group.
            expect = [['order', 'title', True], ['order', 'work_id', False]]
            eq_(expect, sorted([list(x[:2]) + [x[-1]] for x in all_groups]))

    def test_facets_can_be_enabled_at_initialization(self):
        enabled_facets = {
            Facets.ORDER_FACET_GROUP_NAME : [
                Facets.ORDER_TITLE, Facets.ORDER_AUTHOR,
            ],
            Facets.COLLECTION_FACET_GROUP_NAME : [Facets.COLLECTION_MAIN],
            Facets.AVAILABILITY_FACET_GROUP_NAME : [Facets.AVAILABLE_OPEN_ACCESS]
        }

        # Create a new Facets object with these facets enabled,
        # no matter the Configuration.
        facets = Facets(
            Facets.COLLECTION_MAIN, Facets.AVAILABLE_OPEN_ACCESS,
            Facets.ORDER_TITLE, enabled_facets=enabled_facets
        )
        all_groups = list(facets.facet_groups)
        expect = [['order', 'author', False], ['order', 'title', True]]
        eq_(expect, sorted([list(x[:2]) + [x[-1]] for x in all_groups]))

    def test_order_facet_to_database_field(self):
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        def fields(facet):
            return [
                Facets.order_facet_to_database_field(facet, w, e)
                for w, e in ((Work, Edition), (mw, mw), (mwg, mwg))
            ]

        # You can sort by title...
        eq_([Edition.sort_title, mw.sort_title, mwg.sort_title],
            fields(Facets.ORDER_TITLE))

        # ...by author...
        eq_([Edition.sort_author, mw.sort_author, mwg.sort_author],
            fields(Facets.ORDER_AUTHOR))

        # ...by work ID...
        eq_([Work.id, mw.works_id, mwg.works_id],
            fields(Facets.ORDER_WORK_ID))

        # ...by last update time...
        eq_([Work.last_update_time, mw.last_update_time, mwg.last_update_time],
            fields(Facets.ORDER_LAST_UPDATE))

        # ...by most recently added...
        eq_([LicensePool.availability_time, mw.availability_time, mwg.availability_time],
            fields(Facets.ORDER_ADDED_TO_COLLECTION))

        # ...or randomly.
        eq_([Work.random, mw.random, mwg.random],
            fields(Facets.ORDER_RANDOM))

    def test_order_by(self):
        from model import (
            MaterializedWork as mw,
            MaterializedWorkWithGenre as mwg,
        )

        def order(facet, work, edition, ascending=None):
            f = Facets(
                collection=Facets.COLLECTION_FULL, 
                availability=Facets.AVAILABLE_ALL,
                order=facet,
                order_ascending=ascending,
            )
            return f.order_by(work, edition)[0]

        def compare(a, b):
            assert(len(a) == len(b))
            for i in range(0, len(a)):
                assert(a[i].compare(b[i]))

        expect = [Edition.sort_author.asc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, Work, Edition, True)  
        compare(expect, actual)

        expect = [Edition.sort_author.desc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_AUTHOR, Work, Edition, False)  
        compare(expect, actual)

        expect = [mw.sort_title.asc(), mw.sort_author.asc(), mw.works_id.asc()]
        actual = order(Facets.ORDER_TITLE, mw, mw, True)
        compare(expect, actual)

        expect = [Work.last_update_time.asc(), Edition.sort_author.asc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_LAST_UPDATE, Work, Edition, True)
        compare(expect, actual)

        expect = [mw.random.asc(), mw.sort_author.asc(), mw.sort_title.asc(),
                  mw.works_id.asc()]
        actual = order(Facets.ORDER_RANDOM, mw, mw, True)
        compare(expect, actual)

        expect = [LicensePool.availability_time.desc(), Edition.sort_author.asc(), Edition.sort_title.asc(), Work.id.asc()]
        actual = order(Facets.ORDER_ADDED_TO_COLLECTION, Work, Edition, None)  
        compare(expect, actual)


class TestFacetsApply(DatabaseTest):

    def test_apply(self):
        # Set up works that are matched by different types of collections.

        # A high-quality open-access work.
        open_access_high = self._work(with_open_access_download=True)
        open_access_high.quality = 0.8
        open_access_high.random = 0.2
        
        # A low-quality open-access work.
        open_access_low = self._work(with_open_access_download=True)
        open_access_low.quality = 0.2
        open_access_low.random = 0.4

        # A high-quality licensed work which is not currently available.
        (licensed_e1, licensed_p1) = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)
        licensed_high = self._work(presentation_edition=licensed_e1)
        licensed_high.license_pools.append(licensed_p1)
        licensed_high.quality = 0.8
        licensed_p1.open_access = False
        licensed_p1.licenses_owned = 1
        licensed_p1.licenses_available = 0
        licensed_high.random = 0.3

        # A low-quality licensed work which is currently available.
        (licensed_e2, licensed_p2) = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)
        licensed_p2.open_access = False
        licensed_low = self._work(presentation_edition=licensed_e2)
        licensed_low.license_pools.append(licensed_p2)
        licensed_low.quality = 0.2
        licensed_p2.licenses_owned = 1
        licensed_p2.licenses_available = 1
        licensed_low.random = 0.1

        qu = self._db.query(Work).join(Work.presentation_edition).join(
            Work.license_pools
        )
        def facetify(collection=Facets.COLLECTION_FULL, 
                     available=Facets.AVAILABLE_ALL,
                     order=Facets.ORDER_TITLE
        ):
            f = Facets(collection, available, order)
            return f.apply(self._db, qu)

        # When holds are allowed, we can find all works by asking
        # for everything.
        with temp_config() as config:
            config['policies'] = {
                Configuration.HOLD_POLICY : Configuration.HOLD_POLICY_ALLOW
            }

            everything = facetify()
            eq_(4, everything.count())

        # If we disallow holds, we lose one book even when we ask for
        # everything.
        with temp_config() as config:
            config['policies'] = {
                Configuration.HOLD_POLICY : Configuration.HOLD_POLICY_HIDE
            }
            everything = facetify()
            eq_(3, everything.count())
            assert licensed_high not in everything

        with temp_config() as config:
            config['policies'] = {
                Configuration.HOLD_POLICY : Configuration.HOLD_POLICY_ALLOW
            }
            # Even when holds are allowed, if we restrict to books
            # currently available we lose the unavailable book.
            available_now = facetify(available=Facets.AVAILABLE_NOW)
            eq_(3, available_now.count())
            assert licensed_high not in available_now

            # If we restrict to open-access books we lose two books.
            open_access = facetify(available=Facets.AVAILABLE_OPEN_ACCESS)
            eq_(2, open_access.count())
            assert licensed_high not in open_access
            assert licensed_low not in open_access

            # If we restrict to the main collection we lose the low-quality
            # open-access book.
            main_collection = facetify(collection=Facets.COLLECTION_MAIN)
            eq_(3, main_collection.count())
            assert open_access_low not in main_collection

            # If we restrict to the featured collection we lose both
            # low-quality books.
            featured_collection = facetify(collection=Facets.COLLECTION_FEATURED)
            eq_(2, featured_collection.count())
            assert open_access_low not in featured_collection
            assert licensed_low not in featured_collection

            title_order = facetify(order=Facets.ORDER_TITLE)
            eq_([open_access_high, open_access_low, licensed_high, licensed_low],
                title_order.all())

            random_order = facetify(order=Facets.ORDER_RANDOM)
            eq_([licensed_low, open_access_high, licensed_high, open_access_low],
                random_order.all())


class TestLane(DatabaseTest):

    def test_depth(self):
        child = Lane(self._db, "sublane")
        parent = Lane(self._db, "parent", sublanes=[child])
        eq_(0, parent.depth)
        eq_(1, child.depth)

    def test_includes_language(self):
        english_lane = Lane(self._db, self._str, languages=['eng'])
        eq_(True, english_lane.includes_language('eng'))
        eq_(False, english_lane.includes_language('fre'))

        no_english_lane = Lane(self._db, self._str, exclude_languages=['eng'])
        eq_(False, no_english_lane.includes_language('eng'))
        eq_(True, no_english_lane.includes_language('fre'))

        all_language_lane = Lane(self._db, self._str)
        eq_(True, all_language_lane.includes_language('eng'))
        eq_(True, all_language_lane.includes_language('fre'))

    def test_set_customlist_ignored_when_no_list(self):

        class SetCustomListErrorLane(Lane):
            def set_customlist_information(self, *args, **kwargs):
                raise RuntimeError()

        # Because this lane has no list-related information, the
        # RuntimeError shouldn't pop up at all.
        lane = SetCustomListErrorLane(self._db, self._str)

        # The minute we put in some list information, it does!
        assert_raises(
            RuntimeError, SetCustomListErrorLane, self._db, self._str,
            list_data_source=DataSource.NYT
        )

        # It can be a DataSource, or a CustomList identifier. World == oyster.
        assert_raises(
            RuntimeError, SetCustomListErrorLane, self._db, self._str,
            list_identifier=u"Staff Picks"
        )


class TestLanes(DatabaseTest):

    def test_all_matching_genres(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        cooking, ig = Genre.lookup(self._db, classifier.Cooking)
        matches = Lane.all_matching_genres(self._db, [fantasy, cooking])
        names = sorted([x.name for x in matches])
        eq_(
            [u'Cooking', u'Epic Fantasy', u'Fantasy', u'Historical Fantasy', 
             u'Urban Fantasy'], 
            names
        )

    def test_nonexistent_list_raises_exception(self):
        assert_raises(
            UndefinedLane, Lane, self._db, 
            u"This Will Fail", list_identifier=u"No Such List"
        )

    def test_staff_picks_and_best_sellers_sublane(self):
        staff_picks, ignore = self._customlist(
            foreign_identifier=u"Staff Picks", name=u"Staff Picks!", 
            data_source_name=DataSource.LIBRARY_STAFF,
            num_entries=0
        )
        best_sellers, ignore = self._customlist(
            foreign_identifier=u"NYT Best Sellers", name=u"Best Sellers!", 
            data_source_name=DataSource.NYT,
            num_entries=0
        )
        lane = Lane(
            self._db, "Everything", 
            include_staff_picks=True, include_best_sellers=True
        )

        # A staff picks sublane and a best-sellers sublane have been
        # created for us.
        best, picks = lane.sublanes.lanes
        eq_("Best Sellers", best.display_name)
        eq_("Everything - Best Sellers", best.name)
        nyt = DataSource.lookup(self._db, DataSource.NYT)
        eq_(nyt.id, best.list_data_source_id)

        eq_("Staff Picks", picks.display_name)
        eq_("Everything - Staff Picks", picks.name)
        eq_([staff_picks.id], picks.list_ids)

    def test_custom_list_can_set_featured_works(self):
        my_list = self._customlist(num_entries=4)[0]

        featured_entries = my_list.entries[1:3]
        featured_works = list()
        for entry in featured_entries:
            featured_works.append(entry.edition.work)
            entry.featured = True

        other_works = [e.edition.work for e in my_list.entries if not e.featured]
        for work in other_works:
            # Make the other works feature-quality so they are in the running.
            work.quality = 1.0

        self._db.commit()
        SessionManager.refresh_materialized_views(self._db)

        lane = Lane(self._db, u'My Lane', list_identifier=my_list.foreign_identifier)

        result = lane.list_featured_works_query.all()
        eq_(sorted(featured_works), sorted(result))

        def _assert_featured_works(size, expected_works=None, expected_length=None,
                                   sampled_works=None):
            featured_works = None
            featured_materialized_works = None
            with temp_config() as config:
                config[Configuration.POLICIES] = {
                    Configuration.FEATURED_LANE_SIZE : size
                }
                featured_works = lane.featured_works(use_materialized_works=False)
                featured_materialized_works = lane.featured_works()

            expected_length = expected_length
            if expected_length == None:
                expected_length = size
            eq_(expected_length, len(featured_works))
            eq_(expected_length, len(featured_materialized_works))

            expected_works = expected_works or []
            for work in expected_works:
                assert work in featured_works
                # There's also a single MaterializedWork that matches the work.
                [materialized_work] = filter(
                    lambda mw: mw.works_id==work.id, featured_materialized_works
                )

                # Remove the confirmed works for the next test.
                featured_works.remove(work)
                featured_materialized_works.remove(materialized_work)

            sampled_works = sampled_works or []
            for work in featured_works:
                assert work in sampled_works
            for work in featured_materialized_works:
                [sampled_work] = filter(
                    lambda sample: sample.id==work.works_id, sampled_works
                )

        # If the number of featured works completely fills the lane,
        # we only get featured works back.
        _assert_featured_works(2, featured_works)

        # If the number of featured works doesn't fill the lane, a
        # random other work that does will be sampled from the lane's
        # works
        _assert_featured_works(3, featured_works, sampled_works=other_works)

        # If the number of featured works falls slightly below the featured
        # lane size, all the available books are returned, without the
        # CustomList features being duplicated.
        _assert_featured_works(
            5, featured_works, expected_length=4, sampled_works=other_works)

        # If the number of featured works falls far (>5) below the featured
        # lane size, nothing is returned.
        _assert_featured_works(10, expected_length=0)

    def test_gather_matching_genres(self):
        self.fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        self.urban_fantasy, ig = Genre.lookup(
            self._db, classifier.Urban_Fantasy
        )

        self.cooking, ig = Genre.lookup(self._db, classifier.Cooking)
        self.history, ig = Genre.lookup(self._db, classifier.History)

        # Fantasy contains three subgenres and is restricted to fiction.
        fantasy, default = Lane.gather_matching_genres(
            self._db, [self.fantasy], Lane.FICTION_DEFAULT_FOR_GENRE
        )
        eq_(4, len(fantasy))
        eq_(True, default)

        fantasy, default = Lane.gather_matching_genres(
            self._db, [self.fantasy], True
        )
        eq_(4, len(fantasy))
        eq_(True, default)

        fantasy, default = Lane.gather_matching_genres(
            self._db, [self.fantasy], True, [self.urban_fantasy]
        )
        eq_(3, len(fantasy))
        eq_(True, default)

        # If there are only exclude_genres available, then it and its
        # subgenres are ignored while every OTHER genre is set.
        genres, default = Lane.gather_matching_genres(
            self._db, [], True, [self.fantasy]
        )
        eq_(False, any([g for g in self.fantasy.self_and_subgenres if g in genres]))
        # According to known fiction status, that is.
        eq_(True, all([g.default_fiction==True for g in genres]))

        # Attempting to create a contradiction (like nonfiction fantasy)
        # will create a lane broad enough to actually contain books
        fantasy, default = Lane.gather_matching_genres(self._db, [self.fantasy], False)
        eq_(4, len(fantasy))
        eq_(Lane.BOTH_FICTION_AND_NONFICTION, default)

        # Fantasy and history have conflicting fiction defaults, so
        # although we can make a lane that contains both, we can't
        # have it use the default value.
        assert_raises(UndefinedLane, Lane.gather_matching_genres,
            self._db, [self.fantasy, self.history], Lane.FICTION_DEFAULT_FOR_GENRE
        )

    def test_subgenres_become_sublanes(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        lane = Lane(
            self._db, "YA Fantasy", genres=fantasy, 
            languages='eng',
            audiences=Lane.AUDIENCE_YOUNG_ADULT,
            age_range=[15,16],
            subgenre_behavior=Lane.IN_SUBLANES
        )
        sublanes = lane.sublanes.lanes
        names = sorted([x.name for x in sublanes])
        eq_(["Epic Fantasy", "Historical Fantasy", "Urban Fantasy"],
            names)

        # Sublanes inherit settings from their parent.
        assert all([x.languages==['eng'] for x in sublanes])
        assert all([x.age_range==[15, 16] for x in sublanes])
        assert all([x.audiences==set(['Young Adult']) for x in sublanes])

    def test_get_search_target(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        lane = Lane(
            self._db, "YA Fantasy", genres=fantasy, 
            languages='eng',
            audiences=Lane.AUDIENCE_YOUNG_ADULT,
            age_range=[15,16],
            subgenre_behavior=Lane.IN_SUBLANES
        )
        sublanes = lane.sublanes.lanes
        names = sorted([x.name for x in sublanes])
        eq_(["Epic Fantasy", "Historical Fantasy", "Urban Fantasy"],
            names)

        # To start with, none of the lanes are searchable.
        eq_(None, lane.search_target)
        eq_(None, sublanes[0].search_target)

        # If we make a lane searchable, suddenly there's a search target.
        lane.searchable = True
        eq_(lane, lane.search_target)

        # The searchable lane also becomes the search target for its
        # children.
        eq_(lane, sublanes[0].search_target)

    def test_custom_sublanes(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        urban_fantasy, ig = Genre.lookup(self._db, classifier.Urban_Fantasy)

        urban_fantasy_lane = Lane(
            self._db, "Urban Fantasy", genres=urban_fantasy)

        fantasy_lane = Lane(
            self._db, "Fantasy", fantasy, 
            genres=fantasy,
            subgenre_behavior=Lane.IN_SAME_LANE,
            sublanes=[urban_fantasy_lane]
        )
        eq_([urban_fantasy_lane], fantasy_lane.sublanes.lanes)

        # You can just give the name of a genre as a sublane and it
        # will work.
        fantasy_lane = Lane(
            self._db, "Fantasy", fantasy, 
            genres=fantasy,
            subgenre_behavior=Lane.IN_SAME_LANE,
            sublanes="Urban Fantasy"
        )
        eq_([["Urban Fantasy"]], [x.genre_names for x in fantasy_lane.sublanes.lanes])

    def test_custom_lanes_conflict_with_subgenre_sublanes(self):

        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        urban_fantasy, ig = Genre.lookup(self._db, classifier.Urban_Fantasy)

        urban_fantasy_lane = Lane(
            self._db, "Urban Fantasy", genres=urban_fantasy)

        assert_raises(UndefinedLane, Lane,
            self._db, "Fantasy", fantasy, 
            genres=fantasy,
            audiences=Lane.AUDIENCE_YOUNG_ADULT,
            subgenre_behavior=Lane.IN_SUBLANES,
            sublanes=[urban_fantasy_lane]
        )

    def test_lane_query_with_configured_opds(self):
        """The appropriate opds entry is deferred during querying.
        """
        original_setting = Configuration.DEFAULT_OPDS_FORMAT
        lane = Lane(self._db, "Everything")

        # Verbose config doesn't query simple OPDS entries.
        Configuration.DEFAULT_OPDS_FORMAT = "verbose_opds_entry"
        works_query_str = str(lane.works())
        mw_query_str = str(lane.materialized_works())
        
        assert "verbose_opds_entry" in works_query_str
        assert "verbose_opds_entry" in mw_query_str
        assert "works.simple_opds_entry" not in works_query_str
        assert "simple_opds_entry" not in mw_query_str

        # Simple config doesn't query verbose OPDS entries.
        Configuration.DEFAULT_OPDS_FORMAT = "simple_opds_entry"
        works_query_str = str(lane.works())
        mw_query_str = str(lane.materialized_works())

        assert "works.simple_opds_entry" in works_query_str
        assert "simple_opds_entry" in mw_query_str
        assert "verbose_opds_entry" not in works_query_str
        assert "verbose_opds_entry" not in mw_query_str

        Configuration.DEFAULT_OPDS_FORMAT = original_setting

    def test_visible_parent(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        urban_fantasy, ig = Genre.lookup(self._db, classifier.Urban_Fantasy)

        sublane = Lane(
            self._db, "Urban Fantasy", genres=urban_fantasy)

        invisible_parent = Lane(
            self._db, "Fantasy", invisible=True, genres=fantasy, 
            sublanes=[sublane], subgenre_behavior=Lane.IN_SAME_LANE)

        visible_grandparent = Lane(
            self._db, "English", sublanes=[invisible_parent],
            subgenre_behavior=Lane.IN_SAME_LANE)

        eq_(sublane.visible_parent(), visible_grandparent)
        eq_(invisible_parent.visible_parent(), visible_grandparent)
        eq_(visible_grandparent.visible_parent(), None)

    def test_visible_ancestors(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        urban_fantasy, ig = Genre.lookup(self._db, classifier.Urban_Fantasy)

        lane = Lane(
            self._db, "Urban Fantasy", genres=urban_fantasy)

        visible_parent = Lane(
            self._db, "Fantasy", genres=fantasy,
            sublanes=[lane], subgenre_behavior=Lane.IN_SAME_LANE)

        invisible_grandparent = Lane(
            self._db, "English", invisible=True, sublanes=[visible_parent],
            subgenre_behavior=Lane.IN_SAME_LANE)

        visible_ancestor = Lane(
            self._db, "Books With Words", sublanes=[invisible_grandparent],
            subgenre_behavior=Lane.IN_SAME_LANE)

        eq_(lane.visible_ancestors(), [visible_parent, visible_ancestor])

    def test_has_visible_sublane(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        urban_fantasy, ig = Genre.lookup(self._db, classifier.Urban_Fantasy)

        sublane = Lane(
            self._db, "Urban Fantasy", genres=urban_fantasy,
            subgenre_behavior=Lane.IN_SAME_LANE)

        invisible_parent = Lane(
            self._db, "Fantasy", invisible=True, genres=fantasy,
            sublanes=[sublane], subgenre_behavior=Lane.IN_SAME_LANE)

        visible_grandparent = Lane(
            self._db, "English", sublanes=[invisible_parent],
            subgenre_behavior=Lane.IN_SAME_LANE)

        eq_(False, visible_grandparent.has_visible_sublane())
        eq_(True, invisible_parent.has_visible_sublane())
        eq_(False, sublane.has_visible_sublane())

    def test_visible_sublanes(self):
        fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        urban_fantasy, ig = Genre.lookup(self._db, classifier.Urban_Fantasy)
        humorous, ig = Genre.lookup(self._db, classifier.Humorous_Fiction)

        visible_sublane = Lane(self._db, "Humorous Fiction", genres=humorous)

        visible_grandchild = Lane(
            self._db, "Urban Fantasy", genres=urban_fantasy)

        invisible_sublane = Lane(
            self._db, "Fantasy", invisible=True, genres=fantasy,
            sublanes=[visible_grandchild], subgenre_behavior=Lane.IN_SAME_LANE)

        lane = Lane(
            self._db, "English", sublanes=[visible_sublane, invisible_sublane],
            subgenre_behavior=Lane.IN_SAME_LANE)

        eq_(2, len(lane.visible_sublanes))
        assert visible_sublane in lane.visible_sublanes
        assert visible_grandchild in lane.visible_sublanes


class TestLanesQuery(DatabaseTest):

    def setup(self):
        super(TestLanesQuery, self).setup()

        # Look up the Fantasy genre and some of its subgenres.
        self.fantasy, ig = Genre.lookup(self._db, classifier.Fantasy)
        self.epic_fantasy, ig = Genre.lookup(self._db, classifier.Epic_Fantasy)
        self.urban_fantasy, ig = Genre.lookup(
            self._db, classifier.Urban_Fantasy
        )

        # Look up the History genre and some of its subgenres.
        self.history, ig = Genre.lookup(self._db, classifier.History)
        self.african_history, ig = Genre.lookup(
            self._db, classifier.African_History
        )

        self.adult_works = {}
        self.ya_works = {}
        self.childrens_works = {}

        for genre in (self.fantasy, self.epic_fantasy, self.urban_fantasy,
                      self.history, self.african_history):
            fiction = True
            if genre in (self.history, self.african_history):
                fiction = False

            # Create a number of books for each genre.
            adult_work = self._work(
                title="%s Adult" % genre.name, 
                audience=Lane.AUDIENCE_ADULT,
                fiction=fiction,
                with_license_pool=True,
                genre=genre,
            )
            self.adult_works[genre] = adult_work
            adult_work.simple_opds_entry = '<entry>'

            # Childrens and YA books need to be attached to a data
            # source other than Gutenberg, or they'll get filtered
            # out.
            ya_edition, lp = self._edition(
                title="%s YA" % genre.name,                 
                data_source_name=DataSource.OVERDRIVE,
                with_license_pool=True
            )
            ya_work = self._work(
                audience=Lane.AUDIENCE_YOUNG_ADULT,
                fiction=fiction,
                with_license_pool=True,
                presentation_edition=ya_edition,
                genre=genre,
            )
            self.ya_works[genre] = ya_work
            ya_work.simple_opds_entry = '<entry>'

            childrens_edition, lp = self._edition(
                title="%s Childrens" % genre.name,
                data_source_name=DataSource.OVERDRIVE, with_license_pool=True
            )
            childrens_work = self._work(
                audience=Lane.AUDIENCE_CHILDREN,
                fiction=fiction,
                with_license_pool=True,
                presentation_edition=childrens_edition,
                genre=genre,
            )
            if genre == self.epic_fantasy:
                childrens_work.target_age = NumericRange(7, 9, '[]')
            else:
                childrens_work.target_age = NumericRange(8, 10, '[]')
            self.childrens_works[genre] = childrens_work
            childrens_work.simple_opds_entry = '<entry>'

        # Create generic 'Adults Only' fiction and nonfiction books
        # that are not in any genre.
        self.nonfiction = self._work(
            title="Generic Nonfiction", fiction=False,
            audience=Lane.AUDIENCE_ADULTS_ONLY,
            with_license_pool=True
        )
        self.nonfiction.simple_opds_entry = '<entry>'
        self.fiction = self._work(
            title="Generic Fiction", fiction=True,
            audience=Lane.AUDIENCE_ADULTS_ONLY,
            with_license_pool=True
        )
        self.fiction.simple_opds_entry = '<entry>'

        # Create a work of music.
        self.music = self._work(
            title="Music", fiction=False,
            audience=Lane.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        self.music.presentation_edition.medium=Edition.MUSIC_MEDIUM
        self.music.simple_opds_entry = '<entry>'

        # Create a Spanish book.
        self.spanish = self._work(
            title="Spanish book", fiction=True,
            audience=Lane.AUDIENCE_ADULT,
            with_license_pool=True,
            language='spa'
        )
        self.spanish.simple_opds_entry = '<entry>'

        # Refresh the materialized views so that all these books are present
        # in them.
        SessionManager.refresh_materialized_views(self._db)

    def test_lanes(self):
        # I'm putting all these tests into one method because the
        # setup function is so very expensive.

        def _assert_expectations(lane, expected_count, predicate,
                              mw_predicate=None):
            """Ensure that a database query and a query of the
            materialized view give the same results.
            """
            mw_predicate = mw_predicate or predicate
            w = lane.works().all()
            mw = lane.materialized_works().all()
            eq_(len(w), expected_count)
            eq_(len(mw), expected_count)
            assert all([predicate(x) for x in w])
            assert all([mw_predicate(x) for x in mw])
            return w, mw

        # The 'everything' lane contains 18 works -- everything except
        # the music.
        lane = Lane(self._db, "Everything")
        w, mw = _assert_expectations(lane, 18, lambda x: True)

        # The 'Spanish' lane contains 1 book.
        lane = Lane(self._db, "Spanish", languages='spa')
        eq_(['spa'], lane.languages)
        w, mw = _assert_expectations(lane, 1, lambda x: True)
        eq_([self.spanish], w)

        # The 'everything except English' lane contains that same book.
        lane = Lane(self._db, "Not English", exclude_languages='eng')
        eq_(None, lane.languages)
        eq_(['eng'], lane.exclude_languages)
        w, mw = _assert_expectations(lane, 1, lambda x: True)
        eq_([self.spanish], w)

        # The 'music' lane contains 1 work of music
        lane = Lane(self._db, "Music", media=Edition.MUSIC_MEDIUM)
        w, mw = _assert_expectations(
            lane, 1, 
            lambda x: x.presentation_edition.medium==Edition.MUSIC_MEDIUM,
            lambda x: x.medium==Edition.MUSIC_MEDIUM
        )
        
        # The 'English fiction' lane contains ten fiction books.
        lane = Lane(self._db, "English Fiction", fiction=True, languages='eng')
        w, mw = _assert_expectations(
            lane, 10, lambda x: x.fiction
        )

        # The 'nonfiction' lane contains seven nonfiction books.
        # It does not contain the music.
        lane = Lane(self._db, "Nonfiction", fiction=False)
        w, mw = _assert_expectations(
            lane, 7, 
            lambda x: x.presentation_edition.medium==Edition.BOOK_MEDIUM and not x.fiction,
            lambda x: x.medium==Edition.BOOK_MEDIUM and not x.fiction
        )

        # The 'adults' lane contains five books for adults.
        lane = Lane(self._db, "Adult English",
                    audiences=Lane.AUDIENCE_ADULT, languages='eng')
        w, mw = _assert_expectations(
            lane, 5, lambda x: x.audience==Lane.AUDIENCE_ADULT
        )

        # This lane contains those five books plus two adults-only
        # books.
        audiences = [Lane.AUDIENCE_ADULT, Lane.AUDIENCE_ADULTS_ONLY]
        lane = Lane(self._db, "Adult + Adult Only",
                    audiences=audiences, languages='eng'
        )
        w, mw = _assert_expectations(
            lane, 7, lambda x: x.audience in audiences
        )
        eq_(2, len([x for x in w if x.audience==Lane.AUDIENCE_ADULTS_ONLY]))
        eq_(2, len([x for x in mw if x.audience==Lane.AUDIENCE_ADULTS_ONLY]))

        # The 'Young Adults' lane contains five books.
        lane = Lane(self._db, "Young Adults", 
                    audiences=Lane.AUDIENCE_YOUNG_ADULT)
        w, mw = _assert_expectations(
            lane, 5, lambda x: x.audience==Lane.AUDIENCE_YOUNG_ADULT
        )

        # There is one book suitable for seven-year-olds.
        lane = Lane(
            self._db, "If You're Seven", audiences=Lane.AUDIENCE_CHILDREN,
            age_range=7
        )
        w, mw = _assert_expectations(
            lane, 1, lambda x: x.audience==Lane.AUDIENCE_CHILDREN
        )

        # There are four books suitable for ages 10-12.
        lane = Lane(
            self._db, "10-12", audiences=Lane.AUDIENCE_CHILDREN,
            age_range=(10,12)
        )
        w, mw = _assert_expectations(
            lane, 4, lambda x: x.audience==Lane.AUDIENCE_CHILDREN
        )
       
        #
        # Now let's start messing with genres.
        #

        # Here's an 'adult fantasy' lane, in which the subgenres of Fantasy
        # are kept in the same place as generic Fantasy.
        lane = Lane(
            self._db, "Adult Fantasy",
            genres=[self.fantasy], 
            subgenre_behavior=Lane.IN_SAME_LANE,
            fiction=Lane.FICTION_DEFAULT_FOR_GENRE,
            audiences=Lane.AUDIENCE_ADULT,
        )
        # We get three books: Fantasy, Urban Fantasy, and Epic Fantasy.
        w, mw = _assert_expectations(
            lane, 3, lambda x: True
        )
        expect = [u'Epic Fantasy Adult', u'Fantasy Adult', u'Urban Fantasy Adult']
        eq_(expect, sorted([x.sort_title for x in w]))
        eq_(expect, sorted([x.sort_title for x in mw]))

        # Here's a 'YA fantasy' lane in which urban fantasy is explicitly
        # excluded (maybe because it has its own separate lane).
        lane = Lane(
            self._db, full_name="Adult Fantasy",
            genres=[self.fantasy], 
            exclude_genres=[self.urban_fantasy],
            subgenre_behavior=Lane.IN_SAME_LANE,
            fiction=Lane.FICTION_DEFAULT_FOR_GENRE,
            audiences=Lane.AUDIENCE_YOUNG_ADULT,
        )

        # Urban Fantasy does not show up in this lane's genres.
        eq_(
            ["Epic Fantasy", "Fantasy", "Historical Fantasy"], 
            sorted(lane.genre_names)
        )

        # We get two books: Fantasy and Epic Fantasy.
        w, mw = _assert_expectations(
            lane, 2, lambda x: True
        )
        expect = [u'Epic Fantasy YA', u'Fantasy YA']
        eq_(expect, sorted([x.sort_title for x in w]))
        eq_(sorted([x.id for x in w]), sorted([x.works_id for x in mw]))

        # Try a lane based on license source.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        lane = Lane(self._db, full_name="Overdrive Books",
                    license_source=overdrive)
        w, mw = _assert_expectations(
            lane, 10, lambda x: True
        )
        for i in mw:
            eq_(i.data_source_id, overdrive.id)
        for i in w:
            eq_(i.license_pools[0].data_source, overdrive)


        # Finally, test lanes based on lists. Create two lists, each
        # with one book.
        one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        one_year_ago = datetime.datetime.utcnow() - datetime.timedelta(days=365)

        fic_name = "Best Sellers - Fiction"
        best_seller_list_1, ignore = self._customlist(
            foreign_identifier=fic_name, name=fic_name,
            num_entries=0
        )
        best_seller_list_1.add_entry(
            self.fiction.presentation_edition, first_appearance=one_day_ago
        )
        
        nonfic_name = "Best Sellers - Nonfiction"
        best_seller_list_2, ignore = self._customlist(
            foreign_identifier=nonfic_name, name=nonfic_name, num_entries=0
        )
        best_seller_list_2.add_entry(
            self.nonfiction.presentation_edition, first_appearance=one_year_ago
        )

        # Create a lane for one specific list
        fiction_best_sellers = Lane(
            self._db, full_name="Fiction Best Sellers",
            list_identifier=fic_name
        )
        w, mw = _assert_expectations(
            fiction_best_sellers, 1, 
            lambda x: x.sort_title == self.fiction.sort_title
        )

        # Create a lane for all best-sellers.
        all_best_sellers = Lane(
            self._db, full_name="All Best Sellers",
            list_data_source=best_seller_list_1.data_source.name
        )
        w, mw = _assert_expectations(
            all_best_sellers, 2, 
            lambda x: x.sort_title in (
                self.fiction.sort_title, self.nonfiction.sort_title
            )
        )

        # Combine list membership with another criteria (nonfiction)
        all_nonfiction_best_sellers = Lane(
            self._db, full_name="All Nonfiction Best Sellers",
            fiction=False,
            list_data_source=best_seller_list_1.data_source.name
        )
        w, mw = _assert_expectations(
            all_nonfiction_best_sellers, 1, 
            lambda x: x.sort_title==self.nonfiction.sort_title
        )

        # Apply a cutoff date to a best-seller list,
        # excluding the work that was last seen a year ago.
        best_sellers_past_week = Lane(
            self._db, full_name="Best Sellers - The Past Week",
            list_data_source=best_seller_list_1.data_source.name,
            list_seen_in_previous_days=7
        )
        w, mw = _assert_expectations(
            best_sellers_past_week, 1, 
            lambda x: x.sort_title==self.fiction.sort_title
        )
  
    def test_from_description(self):
        """Create a LaneList from a simple description."""
        lanes = LaneList.from_description(
            self._db,
            None,
            [dict(
                full_name="Fiction",
                fiction=True,
                audiences=Classifier.AUDIENCE_ADULT,
            ),
             classifier.Fantasy,
             dict(
                 full_name="Young Adult",
                 fiction=Lane.BOTH_FICTION_AND_NONFICTION,
                 audiences=Classifier.AUDIENCE_YOUNG_ADULT,
             ),
         ]
        )

        fantasy_genre, ignore = Genre.lookup(self._db, classifier.Fantasy.name)
        urban_fantasy_genre, ignore = Genre.lookup(self._db, classifier.Urban_Fantasy.name)

        fiction = lanes.by_languages['']['Fiction']
        young_adult = lanes.by_languages['']['Young Adult']
        fantasy = lanes.by_languages['']['Fantasy'] 
        urban_fantasy = lanes.by_languages['']['Urban Fantasy'] 

        eq_(set([fantasy, fiction, young_adult]), set(lanes.lanes))

        eq_("Fiction", fiction.name)
        eq_(set([Classifier.AUDIENCE_ADULT]), fiction.audiences)
        eq_([], fiction.genre_ids)
        eq_(True, fiction.fiction)

        eq_("Fantasy", fantasy.name)
        eq_(set(), fantasy.audiences)
        expect = set(x.name for x in fantasy_genre.self_and_subgenres)
        eq_(expect, set(fantasy.genre_names))
        eq_(True, fantasy.fiction)

        eq_("Urban Fantasy", urban_fantasy.name)
        eq_(set(), urban_fantasy.audiences)
        eq_([urban_fantasy_genre.id], urban_fantasy.genre_ids)
        eq_(True, urban_fantasy.fiction)

        eq_("Young Adult", young_adult.name)
        eq_(set([Classifier.AUDIENCE_YOUNG_ADULT]), young_adult.audiences)
        eq_([], young_adult.genre_ids)
        eq_(Lane.BOTH_FICTION_AND_NONFICTION, young_adult.fiction)


class TestFilters(DatabaseTest):

    def test_only_show_ready_deliverable_works(self):
        # w1 has licenses but no available copies. It's available
        # unless site policy is to hide books like this.
        w1 = self._work(with_license_pool=True)
        w1.presentation_edition.title = 'I have no available copies'
        w1.license_pools[0].open_access = False
        w1.license_pools[0].licenses_owned = 10
        w1.license_pools[0].licenses_available = 0

        # w2 has no delivery mechanisms.
        w2 = self._work(with_license_pool=True, with_open_access_download=False)
        w2.presentation_edition.title = 'I have no delivery mechanisms'
        for dm in w2.license_pools[0].delivery_mechanisms:
            self._db.delete(dm)

        # w3 is not presentation ready.
        w3 = self._work(with_license_pool=True)
        w3.presentation_edition.title = "I'm not presentation ready"
        w3.presentation_ready = False

        # w4's only license pool is suppressed.
        w4 = self._work(with_open_access_download=True)
        w4.presentation_edition.title = "I am suppressed"
        w4.license_pools[0].suppressed = True

        # w5 has no licenses.
        w5 = self._work(with_license_pool=True)
        w5.presentation_edition.title = "I have no owned licenses."
        w5.license_pools[0].open_access = False
        w5.license_pools[0].licenses_owned = 0

        # w6 is an open-access book, so it's available even though
        # licenses_owned and licenses_available are zero.
        w6 = self._work(with_open_access_download=True)
        w6.presentation_edition.title = "I'm open-access."
        w6.license_pools[0].open_access = True
        w6.license_pools[0].licenses_owned = 0
        w6.license_pools[0].licenses_available = 0

        # w7 is not open-access. We own licenses for it, and there are
        # licenses available right now. It's available.
        w7 = self._work(with_license_pool=True)
        w7.presentation_edition.title = "I have available licenses."
        w7.license_pools[0].open_access = False
        w7.license_pools[0].licenses_owned = 9
        w7.license_pools[0].licenses_available = 5

        # w8 has a delivery mechanism that can't be rendered by the
        # default client.
        w8 = self._work(with_license_pool=True)
        w8.presentation_edition.title = "I have a weird delivery mechanism"
        [pool] = w8.license_pools
        for dm in pool.delivery_mechanisms:
            self._db.delete(dm)
        self._db.commit()
        pool.set_delivery_mechanism(
            "weird content type", "weird DRM scheme", "weird rights URI",
            None
        )
        
        # A normal query against Work/LicensePool finds all works.
        orig_q = self._db.query(Work).join(Work.license_pools)
        eq_(8, orig_q.count())

        # only_show_ready_deliverable_works filters out everything but
        # w1 (owned licenses), w6 (open-access), w7 (available
        # licenses), and w8 (available licenses, weird delivery mechanism).
        q = Lane.only_show_ready_deliverable_works(orig_q, Work)
        eq_(set([w1, w6, w7, w8]), set(q.all()))

        # If we decide to show suppressed works, w4 shows up as well.
        q = Lane.only_show_ready_deliverable_works(
            orig_q, Work, show_suppressed=True
        )
        eq_(set([w1, w4, w6, w7, w8]), set(q.all()))

        # Change site policy to hide books that can't be borrowed.
        with temp_config() as config:
            config['policies'] = {
                Configuration.HOLD_POLICY : Configuration.HOLD_POLICY_HIDE
            }

            # w1 no longer shows up, because although we own licenses, 
            #  no copies are available.
            # w4 is open-access but it's suppressed, so it still doesn't 
            #  show up.
            # w6 still shows up because it's an open-access work.
            # w7 and w8 show up because we own licenses and copies are
            #  available.
            q = Lane.only_show_ready_deliverable_works(orig_q, Work)
            eq_(set([w6, w7, w8]), set(q.all()))

    def test_lane_subclass_queries(self):
        """Subclasses of Lane can effectively retrieve all of a Work's
        LicensePools
        """
        class LaneSubclass(Lane):
            """A subclass of Lane that filters against a
            LicensePool-specific criteria
            """
            def apply_filters(self, qu, **kwargs):
                return qu.filter(DataSource.name==DataSource.GUTENBERG)

        # Create a work with two license_pools. One that fits the
        # LaneSubclass criteria and one that doesn't.
        w1 = self._work(with_open_access_download=True)
        _edition, additional_lp = self._edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
            with_open_access_download=True
        )
        additional_lp.work = w1
        self._db.commit()

        # When the work is queried, both of the LicensePools are
        # available in the database session, despite the filtering.
        subclass = LaneSubclass(self._db, "Lane Subclass")
        [subclass_work] = subclass.works().all()
        eq_(2, len(subclass_work.license_pools))


class TestPagination(DatabaseTest):

    def test_has_next_page(self):
        query = self._db.query(Work)
        pagination = Pagination(size=2)

        # When the query is empty, pagination doesn't have a next page.
        pagination.apply(query)
        eq_(False, pagination.has_next_page)

        # When there are more results in the query, it does.
        for num in range(3):
            # Create three works.
            self._work()
        pagination.apply(query)
        eq_(True, pagination.has_next_page)

        # When we reach the end of results, there's no next page.
        pagination.offset = 1
        eq_(False, pagination.has_next_page)

        # When the database is updated, pagination knows.
        for num in range(3):
            self._work()
        pagination.apply(query)
        eq_(True, pagination.has_next_page)

        # Even when the query ends at the same size as a page, all is well.
        pagination.offset = 4
        eq_(False, pagination.has_next_page)
