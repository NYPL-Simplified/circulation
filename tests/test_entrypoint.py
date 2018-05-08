from testing import DatabaseTest
import json
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
from model import (
    Edition,
)
from entrypoint import (
    EntryPoint,
    EbooksEntryPoint,
    EverythingEntryPoint,
    AudiobooksEntryPoint,
    MediumEntryPoint,
)

class TestEntryPoint(DatabaseTest):

    def test_defaults(self):
        everything, ebooks, audiobooks = EntryPoint.ENTRY_POINTS
        eq_(EverythingEntryPoint, everything)
        eq_(EbooksEntryPoint, ebooks)
        eq_(AudiobooksEntryPoint, audiobooks)

        display = EntryPoint.DISPLAY_TITLES
        eq_("Books", display[ebooks])
        eq_("Audio", display[audiobooks])

        eq_(Edition.BOOK_MEDIUM, EbooksEntryPoint.INTERNAL_NAME)
        eq_(Edition.AUDIO_MEDIUM, AudiobooksEntryPoint.INTERNAL_NAME)

    def test_no_changes(self):
        # EntryPoint doesn't modify queries or searches.
        qu = self._db.query(Edition)
        eq_(qu, EntryPoint.apply(qu))
        args = dict(arg="value")
        eq_(args, EverythingEntryPoint.modified_search_arguments(**args))

    def test_register(self):

        class Mock(object):
            pass

        args = [Mock, "Mock!"]

        assert_raises_regexp(
            ValueError, "must define INTERNAL_NAME", EntryPoint.register, *args
        )

        # Test successful registration.
        Mock.INTERNAL_NAME = "a name"
        EntryPoint.register(*args)
        assert Mock in EntryPoint.ENTRY_POINTS
        eq_("Mock!", EntryPoint.DISPLAY_TITLES[Mock])
        assert Mock not in EntryPoint.DEFAULT_ENABLED

        # Can't register twice.
        assert_raises_regexp(
            ValueError, "Duplicate entry point internal name: a name",
            EntryPoint.register, *args
        )

        EntryPoint.unregister(Mock)

        # Test successful registration as a default-enabled entry point.
        EntryPoint.register(*args, default_enabled=True)
        assert Mock in EntryPoint.DEFAULT_ENABLED

        # Can't register two different entry points with the same
        # display name.
        class Mock2(object):
            INTERNAL_NAME = "mock2"

        assert_raises_regexp(
            ValueError, "Duplicate entry point display name: Mock!",
            EntryPoint.register, Mock2, "Mock!"
        )


class TestEverythingEntryPoint(DatabaseTest):

    def test_no_changes(self):
        # EverythingEntryPoint doesn't modify queries or searches
        # beyond the default behavior for any entry point.
        qu = self._db.query(Edition)
        eq_(qu, EntryPoint.apply(qu))
        args = dict(arg="value")
        eq_(args, EverythingEntryPoint.modified_search_arguments(**args))
        eq_("All", EverythingEntryPoint.INTERNAL_NAME)


class TestMediumEntryPoint(DatabaseTest):

    def test_apply(self):
        # Create a video, and a entry point that contains videos.
        work = self._work(with_license_pool=True)
        work.license_pools[0].presentation_edition.medium = Edition.VIDEO_MEDIUM
        self.add_to_materialized_view([work])

        class Videos(MediumEntryPoint):
            INTERNAL_NAME = Edition.VIDEO_MEDIUM

        from model import MaterializedWorkWithGenre
        qu = self._db.query(MaterializedWorkWithGenre)

        # The default entry points filter out the video.
        for entrypoint in EbooksEntryPoint, AudiobooksEntryPoint:
            modified = entrypoint.apply(qu)
            eq_([], modified.all())

        # But the video entry point includes it.
        videos = Videos.apply(qu)
        eq_([work.id], [x.works_id for x in videos])


    def test_modified_search_arguments(self):

        class Mock(MediumEntryPoint):
            INTERNAL_NAME = object()

        kwargs = dict(media="something else", other_argument="unaffected")
        new_kwargs = Mock.modified_search_arguments(**kwargs)
        eq_(dict(media=[Mock.INTERNAL_NAME], other_argument="unaffected"),
            new_kwargs)


class TestLibrary(DatabaseTest):
    """Test a Library's interaction with EntryPoints."""

    def test_enabled_entrypoints(self):
        l = self._default_library

        setting = l.setting(EntryPoint.ENABLED_SETTING)

        # When the value is not set, the default is used.
        eq_(EntryPoint.DEFAULT_ENABLED, list(l.entrypoints))
        setting.value = None
        eq_(EntryPoint.DEFAULT_ENABLED, list(l.entrypoints))

        # Names that don't correspond to registered entry points are
        # ignored. Names that do are looked up.
        setting.value = json.dumps(
            ["no such entry point", AudiobooksEntryPoint.INTERNAL_NAME]
        )
        eq_([AudiobooksEntryPoint], list(l.entrypoints))

        # An empty list is a valid value.
        setting.value = json.dumps([])
        eq_([], list(l.entrypoints))
