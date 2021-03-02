# encoding: utf-8
from nose.tools import (
    eq_,
    set_trace,
)
from ...classifier import *
from ...classifier.overdrive import OverdriveClassifier as Overdrive

class TestOverdriveClassifier(object):

    def test_lookup(self):
        eq_(Overdrive, Classifier.lookup(Classifier.OVERDRIVE))

    def test_scrub_identifier(self):
        scrub = Overdrive.scrub_identifier
        eq_("Foreign Language Study",
            scrub("Foreign Language Study - Italian"))
        eq_("Foreign Language Study",
            scrub("Foreign Language Study - Klingon"))
        eq_("Foreign Affairs", scrub("Foreign Affairs"))

    def test_target_age(self):
        def a(x, y):
            return Overdrive.target_age(x,y)
        eq_((0,4), a("Picture Book Nonfiction", None))
        eq_((5,8), a("Beginning Reader", None))
        eq_((12,17), a("Young Adult Fiction", None))
        eq_((None,None), a("Fiction", None))

    def test_audience(self):
        def a(identifier):
            return Overdrive.audience(identifier, None)
        eq_(Classifier.AUDIENCE_CHILDREN, a("Picture Books"))
        eq_(Classifier.AUDIENCE_CHILDREN, a("Beginning Reader"))
        eq_(Classifier.AUDIENCE_CHILDREN, a("Children's Video"))
        eq_(Classifier.AUDIENCE_CHILDREN, a("Juvenile Nonfiction"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, a("Young Adult Nonfiction"))
        eq_(Classifier.AUDIENCE_YOUNG_ADULT, a("Young Adult Video"))
        eq_(Classifier.AUDIENCE_ADULTS_ONLY, a("Erotic Literature"))
        eq_(Classifier.AUDIENCE_ADULT, a("Fiction"))
        eq_(Classifier.AUDIENCE_ADULT, a("Nonfiction"))
        eq_(None, a("Antiques"))

    def test_is_fiction(self):
        def f(identifier):
            return Overdrive.is_fiction(identifier, None)

        # Everything in FICTION is fiction.
        for yes in [
            "Fantasy",
            "Horror",
            "Literary Anthologies",
            "Mystery",
            "Romance",
            "Short Stories",
            "Suspense",
            "Thriller",
            "Western",
        ]:
            assert yes in Overdrive.FICTION
            eq_(True, f(yes))

        # Everything that includes the string 'Fiction' or
        # 'Literature' is fiction.
        for yes in [
            "Bad Literature",
            "Literature",
            "Picture Book Fiction",
            "Science Fiction & Fantasy",
            "Young Adult Fiction",
        ]:
            eq_(True, f(yes))

        # A few specific genres are neither fiction nor nonfiction.
        for neither in ["Drama", "Poetry", "Latin"]:
            eq_(None, f(neither))

        # Same for video and music.
        for video in Overdrive.VIDEO_GENRES:
            eq_(None, f(video))
        for music in Overdrive.MUSIC_GENRES:
            eq_(None, f(music))

        # Everything else is presumed to be nonfiction. Do some spot
        # checks.
        for no in [
            "Antiques",
            "Music",
            "Pets",
            "Nonfiction",
            "Science",
            "Young Adult Nonfiction",
        ]:
            eq_(False, f(no))

    def test_genre(self):
        """Check the fiction status and genre of every known Overdrive
        subject."""
        def g(x, fiction=None):
            genre = Overdrive.genre(x, None, fiction=fiction)
            if genre:
                genre = genre.name
            return genre

        # Video and music are not classified.
        for video in Overdrive.VIDEO_GENRES:
            eq_(None, g(video))

        for music in Overdrive.MUSIC_GENRES:
            eq_(None, g(music))

        eq_("Urban Fiction", g("African American Fiction"))
        eq_(None, g("African American Nonfiction"))
        eq_(None, g("Analysis"))
        eq_("Historical Fiction", g("Antiquarian"))
        eq_("Antiques & Collectibles", g("Antiques"))
        eq_("Architecture", g("Architecture"))
        eq_("Art", g("Art"))
        eq_(None, g("Beginning Reader"))
        eq_("Biography & Memoir", g("Biography & Autobiography"))
        eq_("Science", g("Biology"))
        eq_("Business", g("Business"))
        eq_("Business", g("Careers"))
        eq_("Science", g("Chemistry"))
        eq_("Women's Fiction", g("Chick Lit Fiction"))
        eq_("Parenting & Family", g("Child Development"))
        eq_(None, g("Children"))
        eq_("Religious Fiction", g("Christian Fiction"))
        eq_("Christianity", g("Christian Nonfiction"))
        eq_("Classics", g("Classic Literature"))
        eq_("Comics & Graphic Novels", g("Comic and Graphic Books"))
        eq_("Computers", g("Computer Technology"))
        eq_("Cooking", g("Cooking & Food"))
        eq_("Crafts & Hobbies", g("Crafts"))

        # This classification is not coherent; some of these titles
        # are True Crime and some are 'how to serve on a jury'.
        eq_(None, g("Crime"))

        eq_("Literary Criticism", g("Criticism"))
        eq_("Political Science", g("Current Events"))

        # This is used to classify both books and video.
        eq_("Drama", g("Drama"))

        eq_("Economics", g("Economics"))
        eq_("Education", g("Education"))
        eq_("Technology", g("Engineering"))
        eq_("Entertainment", g("Entertainment"))
        eq_("Erotica", g("Erotic Literature"))
        eq_(None, g("Essays"))
        eq_("Philosophy", g("Ethics"))
        eq_("Parenting & Family", g("Family & Relationships"))
        eq_("Fantasy", g("Fantasy"))
        eq_(None, g("Feminist"))
        eq_(None, g("Fiction"))
        eq_("Personal Finance & Investing", g("Finance"))
        eq_("Folklore", g("Folklore"))
        eq_(None, g("Foreign Language"))
        eq_("Foreign Language Study", g("Foreign Language Study"))
        eq_("Games", g("Games"))
        eq_("Gardening", g("Gardening"))

        # If we know a title is fiction, then 'Gay/Lesbian' maps to
        # LGBTQ Fiction. If we don't know that it's fiction, it doesn't
        # map to any particular genre.
        eq_(None, g("Gay/Lesbian"))
        eq_("LGBTQ Fiction", g("Gay/Lesbian", True))

        eq_("Social Sciences", g("Gender Studies"))
        eq_("Social Sciences", g("Genealogy"))
        eq_(None, g("Geography")) # This is all over the place.
        eq_("Reference & Study Aids", g("Grammar & Language Usage"))
        eq_("Health & Diet", g("Health & Fitness"))
        eq_("Historical Fiction", g("Historical Fiction"))
        eq_("History", g("History"))
        eq_("House & Home", g("Home Design & Décor"))
        eq_("Horror", g("Horror"))
        eq_(None, g("Human Rights"))
        eq_("Humorous Fiction", g("Humor (Fiction)"))
        eq_("Humorous Nonfiction", g("Humor (Nonfiction)"))
        eq_(None, g("Inspirational")) # Mix of Christian nonfiction and fiction
        eq_(None, g("Journalism"))
        eq_("Judaism", g("Judaica"))
        eq_(None, g("Juvenile Fiction"))
        eq_(None, g("Juvenile Literature"))
        eq_(None, g("Juvenile Nonfiction"))
        eq_("Literary Criticism", g("Language Arts"))
        eq_(None, g("Latin")) # A language, not a genre
        eq_("Law", g("Law"))
        eq_("Short Stories", g("Literary Anthologies"))
        eq_("Literary Criticism", g("Literary Criticism"))
        eq_(None, g("Literature"))
        eq_("Management & Leadership", g("Management"))
        eq_("Business", g("Marketing & Sales"))
        eq_("Mathematics", g("Mathematics"))
        eq_("Social Sciences", g("Media Studies"))
        eq_("Medical", g("Medical"))
        eq_("Military History", g("Military"))
        eq_(None, g("Multi-Cultural")) # All over the place
        eq_("Music", g("Music"))
        eq_("Mystery", g("Mystery"))
        eq_("Folklore", g("Mythology"))
        eq_("Nature", g("Nature"))
        eq_("Body, Mind & Spirit", g("New Age"))
        eq_(None, g("Non-English Fiction"))
        eq_(None, g("Non-English Nonfiction"))
        eq_(None, g("Nonfiction"))
        eq_("Travel", g("Outdoor Recreation"))
        eq_("Performing Arts", g("Performing Arts"))
        eq_("Pets", g("Pets"))
        eq_("Philosophy", g("Philosophy"))
        eq_("Photography", g("Photography"))
        eq_("Science", g("Physics"))
        eq_(None, g("Picture Book Fiction"))
        eq_(None, g("Picture Book Nonfiction"))
        eq_("Poetry", g("Poetry"))
        eq_("Political Science", g("Politics"))
        eq_(None, g("Professional"))
        eq_("Psychology", g("Psychiatry"))
        eq_("Psychology", g("Psychiatry & Psychology"))
        eq_("Psychology", g("Psychology"))
        eq_("Self-Help", g("Recovery"))
        eq_("Reference & Study Aids", g("Reference"))
        eq_("Religion & Spirituality", g("Religion & Spirituality"))
        eq_(None, g("Research"))
        eq_("Romance", g("Romance"))
        eq_(None, g("Scholarly"))
        eq_("Science", g("Science"))
        eq_("Science Fiction", g("Science Fiction"))
        eq_(None, g("Science Fiction & Fantasy"))
        eq_("Self-Help", g("Self Help"))
        eq_("Self-Help", g("Self-Improvement"))
        eq_("Short Stories", g("Short Stories"))
        eq_("Computers", g("Social Media"))
        eq_("Social Sciences", g("Social Studies"))
        eq_("Social Sciences", g("Sociology"))
        eq_("Music", g("Songbook"))
        eq_("Sports", g("Sports & Recreations"))
        eq_("Study Aids", g("Study Aids & Workbooks"))
        eq_("Suspense/Thriller", g("Suspense"))
        eq_("Technology", g("Technology"))
        eq_("Study Aids", g("Text Book"))
        eq_("Suspense/Thriller", g("Thriller"))
        eq_("Technology", g("Transportation"))
        eq_("Travel", g("Travel"))
        eq_("Travel", g("Travel Literature"))
        eq_("True Crime", g("True Crime"))
        eq_("Urban Fiction", g("Urban Fiction"))
        eq_("Westerns", g("Western"))
        eq_(None, g("Women's Studies"))
        eq_("Literary Criticism", g("Writing"))
        eq_(None, g("Young Adult Fiction"))
        eq_(None, g("Young Adult Literature"))
        eq_(None, g("Young Adult Nonfiction"))
