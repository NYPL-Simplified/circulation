import datetime

from nose.tools import (
    eq_,
    set_trace,
)

from . import (
    DatabaseTest,
)

import classifier

from model import (
    Classifier,
    DataSource,
    Edition,
    Genre,
    Lane,
    Work,
)

from feed import (
    WorkFeed,
    LaneFeed,
    CustomListFeed,
    SingleCustomListFeed,
)


class TestWorkFeed(DatabaseTest):

    def setup(self):
        super(TestWorkFeed, self).setup()
        self.fantasy_genre, ignore = Genre.lookup(
            self._db, classifier.Fantasy)
        self.fantasy_lane = Lane(
            self._db, self.fantasy_genre.name, 
            [self.fantasy_genre], True, Lane.FICTION_DEFAULT_FOR_GENRE,
            Classifier.AUDIENCE_ADULT)

    def test_setup(self):
        by_author = LaneFeed(self.fantasy_lane, "eng",
                             order_facet='author')

        eq_(["eng"], by_author.languages)
        eq_(self.fantasy_lane, by_author.lane)
        eq_([Edition.sort_author, Edition.sort_title, Work.id],
            by_author.order_by)

        by_title = LaneFeed(self.fantasy_lane, ["eng", "spa"],
                            order_facet='title')
        eq_(["eng", "spa"], by_title.languages)
        eq_([Edition.sort_title, Edition.sort_author, Work.id],
            by_title.order_by)

    def test_several_books_same_author(self):
        title = "The Title"
        author = "Author, The"
        language = "eng"
        genre = self.fantasy_genre
        lane = self.fantasy_lane
        audience = Classifier.AUDIENCE_ADULT

        # We've got three works with the same author but different
        # titles, plus one with a different author and title.
        w1 = self._work("Title B", author, genre, language, audience, 
                        with_license_pool=True)
        w2 = self._work("Title A", author, genre, language, audience, 
                        with_license_pool=True)
        w3 = self._work("Title C", author, genre, language, audience, 
                        with_license_pool=True)
        w4 = self._work("Title D", "Author, Another", genre, language, 
                        audience, with_license_pool=True)

        eq_("Another Author", w4.author)
        eq_("Author, Another", w4.sort_author)

        # Order them by title, and everything's fine.
        feed = LaneFeed(self.fantasy_lane, language, order_facet='title')
        eq_("title", feed.active_facet)
        eq_([w2, w1, w3, w4], feed.page_query(self._db, None, 10).all())
        eq_([w3, w4], feed.page_query(self._db, 2, 10).all())

        # Order them by author, and they're secondarily ordered by title.
        feed = LaneFeed(lane, language, order_facet='author')
        eq_("author", feed.active_facet)
        eq_([w4, w2, w1, w3], feed.page_query(self._db, None, 10).all())
        eq_([w3], feed.page_query(self._db, 3, 10).all())

        eq_([], feed.page_query(self._db, 4, 10).all())

    def test_several_books_different_authors(self):
        title = "The Title"
        language = "eng"
        genre = self.fantasy_genre
        lane = self.fantasy_lane
        audience = Classifier.AUDIENCE_ADULT
        
        # We've got three works with the same title but different
        # authors, plus one with a different author and title.
        w1 = self._work(title, "Author B", genre, language, audience,
                        with_license_pool=True)
        w2 = self._work(title, "Author A", genre, language, audience, 
                        with_license_pool=True)
        w3 = self._work(title, "Author C", genre, language, audience, 
                        with_license_pool=True)
        w4 = self._work("Different title", "Author D", genre, language, 
                        with_license_pool=True)

        # Order them by author, and everything's fine.
        feed = LaneFeed(lane, language, order_facet='author')
        eq_([w2, w1, w3, w4], feed.page_query(self._db, None, 10).all())
        eq_([w3, w4], feed.page_query(self._db, 2, 10).all())

        # Order them by title, and they're secondarily ordered by author.
        feed = LaneFeed(lane, language, order_facet='title')
        eq_([w4, w2, w1, w3], feed.page_query(self._db, None, 10).all())
        eq_([w3], feed.page_query(self._db, 3, 10).all())

        eq_([], feed.page_query(self._db, 4, 10).all())

    def test_several_books_same_author_and_title(self):
        
        title = "The Title"
        author = "Author, The"
        language = "eng"
        genre = self.fantasy_genre
        lane = self.fantasy_lane
        audience = Classifier.AUDIENCE_ADULT

        # We've got four works with the exact same title and author
        # string.
        w1, w2, w3, w4 = [
            self._work(title, author, genre, language, audience,
                       with_license_pool=True)
            for i in range(4)]

        # WorkFeed orders them by the ID of their Editions.
        feed = LaneFeed(lane, language, order_facet='author')
        query = feed.page_query(self._db, None, 10)
        eq_([w1, w2, w3, w4], query.all())

        # If we provide an offset, we only get the works after the
        # offset.
        query = feed.page_query(self._db, 2, 10)
        eq_([w3, w4], query.all())

        eq_([], feed.page_query(self._db, 4, 10).all())

    # def test_page_query_custom_filter(self):
    #     work = self._work()
    #     lane = self.fantasy_lane
    #     language = "eng"
    #     feed = LaneFeed(lane, language, order_facet='author')
    #     # Let's exclude the only work.
    #     q = feed.page_query(self._db, None, 10, Work.title != work.title)
        
    #     # The feed is empty.
    #     eq_([], q.all())

class TestCustomListFeed(DatabaseTest):

    def test_only_matching_work_ids_are_included(self):

        # Two works from Project Gutenberg.
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)

        # A custom list from the New York Times.
        custom_list, editions = self._customlist(
            num_entries=2, entries_exist_as_works=False)
        
        # One of the Gutenberg works has the same permanent work ID as
        # one of the editions on the NYT list.
        w1.primary_edition.permanent_work_id = editions[0].permanent_work_id

        # The other work has a totally different permanent work ID.
        w2.primary_edition.permanent_work_id = "totally different work id"

        # Now create a custom list feed containing books from all NYT
        # custom lists.
        feed = CustomListFeed(None, custom_list.data_source,
                              languages="eng")

        # At first, the list entries are not associated with any IDs
        # that have a licensepool. set_license_pool() does nothing.
        eq_([], feed.base_query(self._db).all())
        for entry in custom_list.entries:
            entry.set_license_pool()
        eq_([], feed.base_query(self._db).all())

        # But if we associate a list entry's primary identifier with
        # the primary identifier of one of our license pools,
        # set_license_pool() does something.
        first_list_entry = custom_list.entries[0]
        source = w1.license_pools[0].data_source
        first_list_entry.edition.primary_identifier.equivalent_to(
            source, w1.license_pools[0].identifier, 1)
        first_list_entry.set_license_pool()
        eq_(w1.license_pools[0], first_list_entry.license_pool)

        # Suddenly, one of the books in our collection is associated
        # with a list item.
        [match] = feed.base_query(self._db).all()
        eq_(w1, match)

    def test_feed_consolidates_multiple_lists(self):

        # Two works.
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)

        # Two custom lists.
        customlist1, [edition1] = self._customlist(num_entries=1)
        customlist2, [edition2] = self._customlist(num_entries=1)
        
        # Each work is on one list.
        customlist1.entries[0].license_pool = w1.license_pools[0]
        customlist2.entries[0].license_pool = w2.license_pools[0]
        w1.primary_edition.permanent_work_id = edition1.permanent_work_id
        w2.primary_edition.permanent_work_id = edition2.permanent_work_id

        # Now create a custom list feed with both lists.
        feed = CustomListFeed(
            None, customlist1.data_source, languages="eng")

        # Both works match.
        matches = set(feed.base_query(self._db).all())
        eq_(matches, set([w1, w2]))

    def test_all_custom_lists_from_data_source_feed(self):
        # Three works.
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        w3 = self._work(with_license_pool=True)

        # Three custom lists, two from NYT and one from Bibliocommons.
        customlist1, [edition1] = self._customlist(num_entries=1)
        customlist2, [edition2] = self._customlist(num_entries=1)
        customlist3, [edition3] = self._customlist(
            num_entries=1, data_source_name=DataSource.BIBLIOCOMMONS)

        # Each work is on one list.
        customlist1.entries[0].license_pool = w1.license_pools[0]
        customlist2.entries[0].license_pool = w2.license_pools[0]
        customlist3.entries[0].license_pool = w3.license_pools[0]

        # Let's ask for a complete feed of NYT lists.
        self._db.commit()
        nyt = DataSource.lookup(self._db, DataSource.NYT)
        feed = CustomListFeed(None, nyt, languages='eng')

        # The two works on the NYT list are in the feed. The work from
        # the Bibliocommons feed is not.
        qu = feed.base_query(self._db)
        eq_(set([w1, w2]), set(qu.all()))

    def test_feed_excludes_works_not_seen_on_list_recently(self):
        # One work.
        work = self._work(with_license_pool=True)

        # One custom list.
        customlist, [edition] = self._customlist(num_entries=1)

        work.primary_edition.permanent_work_id = edition.permanent_work_id
        customlist.entries[0].license_pool = work.license_pools[0]

        # Create a feed for works whose last appearance on the list
        # was no more than one day ago.
        feed = SingleCustomListFeed(
            customlist, languages=["eng"], list_duration_days=1)

        # The work shows up.
        eq_([work], feed.base_query(self._db).all())

        # ... But let's say the work was last seen on the list a week ago.
        [list_entry] = customlist.entries
        list_entry.most_recent_appearance = (
            datetime.datetime.utcnow() - datetime.timedelta(days=7))

        # Now it no longer shows up.
        eq_([], feed.base_query(self._db).all())
