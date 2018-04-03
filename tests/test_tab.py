from testing import DatabaseTest
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
from model import (
    Edition,
)
from tab import (
    Tab,
    EbooksTab,
    AudiobooksTab,
    MediumTab,
)

class TestTab(object):

    def test_defaults(self):
        ebooks, audiobooks = Tab.TABS
        eq_(EbooksTab, ebooks)
        eq_(AudiobooksTab, audiobooks)

        display = Tab.DISPLAY_TITLES
        eq_("Books", display[ebooks])
        eq_("Audiobooks", display[audiobooks])

        eq_(Edition.BOOK_MEDIUM, EbooksTab.INTERNAL_NAME)
        eq_(Edition.AUDIO_MEDIUM, AudiobooksTab.INTERNAL_NAME)

    def test_register(self):

        class Mock(object):
            pass

        args = [Mock, "Mock!"]

        assert_raises_regexp(
            ValueError, "must define INTERNAL_NAME", Tab.register, *args
        )

        # Test successful registration.
        Mock.INTERNAL_NAME = "a name"
        Tab.register(*args)
        assert Mock in Tab.TABS
        eq_("Mock!", Tab.DISPLAY_TITLES[Mock])
        assert Mock not in Tab.DEFAULT_ENABLED

        # Can't register twice.
        assert_raises_regexp(
            ValueError, "Duplicate tab internal name: a name",
            Tab.register, *args
        )
        
        Tab.unregister(Mock)

        # Test successful registration as a default-enabled tab.
        Tab.register(*args, default_enabled=True)
        assert Mock in Tab.DEFAULT_ENABLED


class TestMediumTab(DatabaseTest):

    def test_apply(self):
        # Create a video, and a tab that contains videos.
        work = self._work(with_license_pool=True)
        work.license_pools[0].presentation_edition.medium = Edition.VIDEO_MEDIUM
        self.add_to_materialized_view([work])

        class Videos(MediumTab):
            INTERNAL_NAME = Edition.VIDEO_MEDIUM

        from model import MaterializedWorkWithGenre
        qu = self._db.query(MaterializedWorkWithGenre)

        # The default tabs filter out the video.
        for tab in EbooksTab, AudiobooksTab:
            modified = tab.apply(qu)
            eq_([], modified.all())

        # But the video tab includes it.
        videos = Videos.apply(qu)
        eq_([work.id], [x.works_id for x in videos])


    def test_modified_search_arguments(self):

        class Mock(MediumTab):
            INTERNAL_NAME = object()

        kwargs = dict(media="something else", other_argument="unaffected")
        new_kwargs = Mock.modified_search_arguments(**kwargs)
        eq_(dict(media=[Mock.INTERNAL_NAME], other_argument="unaffected"),
            new_kwargs)
