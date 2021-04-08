# encoding: utf-8
from ...classifier import *
from ...classifier.overdrive import OverdriveClassifier as Overdrive

class TestOverdriveClassifier(object):

    def test_lookup(self):
        assert Overdrive == Classifier.lookup(Classifier.OVERDRIVE)

    def test_scrub_identifier(self):
        scrub = Overdrive.scrub_identifier
        assert ("Foreign Language Study" ==
            scrub("Foreign Language Study - Italian"))
        assert ("Foreign Language Study" ==
            scrub("Foreign Language Study - Klingon"))
        assert "Foreign Affairs" == scrub("Foreign Affairs")

    def test_target_age(self):
        def a(x, y):
            return Overdrive.target_age(x,y)
        assert (0,4) == a("Picture Book Nonfiction", None)
        assert (5,8) == a("Beginning Reader", None)
        assert (12,17) == a("Young Adult Fiction", None)
        assert (None,None) == a("Fiction", None)

    def test_audience(self):
        def a(identifier):
            return Overdrive.audience(identifier, None)
        assert Classifier.AUDIENCE_CHILDREN == a("Picture Books")
        assert Classifier.AUDIENCE_CHILDREN == a("Beginning Reader")
        assert Classifier.AUDIENCE_CHILDREN == a("Children's Video")
        assert Classifier.AUDIENCE_CHILDREN == a("Juvenile Nonfiction")
        assert Classifier.AUDIENCE_YOUNG_ADULT == a("Young Adult Nonfiction")
        assert Classifier.AUDIENCE_YOUNG_ADULT == a("Young Adult Video")
        assert Classifier.AUDIENCE_ADULTS_ONLY == a("Erotic Literature")
        assert Classifier.AUDIENCE_ADULT == a("Fiction")
        assert Classifier.AUDIENCE_ADULT == a("Nonfiction")
        assert None == a("Antiques")

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
            assert True == f(yes)

        # Everything that includes the string 'Fiction' or
        # 'Literature' is fiction.
        for yes in [
            "Bad Literature",
            "Literature",
            "Picture Book Fiction",
            "Science Fiction & Fantasy",
            "Young Adult Fiction",
        ]:
            assert True == f(yes)

        # A few specific genres are neither fiction nor nonfiction.
        for neither in ["Drama", "Poetry", "Latin"]:
            assert None == f(neither)

        # Same for video and music.
        for video in Overdrive.VIDEO_GENRES:
            assert None == f(video)
        for music in Overdrive.MUSIC_GENRES:
            assert None == f(music)

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
            assert False == f(no)

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
            assert None == g(video)

        for music in Overdrive.MUSIC_GENRES:
            assert None == g(music)

        assert "Urban Fiction" == g("African American Fiction")
        assert None == g("African American Nonfiction")
        assert None == g("Analysis")
        assert "Historical Fiction" == g("Antiquarian")
        assert "Antiques & Collectibles" == g("Antiques")
        assert "Architecture" == g("Architecture")
        assert "Art" == g("Art")
        assert None == g("Beginning Reader")
        assert "Biography & Memoir" == g("Biography & Autobiography")
        assert "Science" == g("Biology")
        assert "Business" == g("Business")
        assert "Business" == g("Careers")
        assert "Science" == g("Chemistry")
        assert "Women's Fiction" == g("Chick Lit Fiction")
        assert "Parenting & Family" == g("Child Development")
        assert None == g("Children")
        assert "Religious Fiction" == g("Christian Fiction")
        assert "Christianity" == g("Christian Nonfiction")
        assert "Classics" == g("Classic Literature")
        assert "Comics & Graphic Novels" == g("Comic and Graphic Books")
        assert "Computers" == g("Computer Technology")
        assert "Cooking" == g("Cooking & Food")
        assert "Crafts & Hobbies" == g("Crafts")

        # This classification is not coherent; some of these titles
        # are True Crime and some are 'how to serve on a jury'.
        assert None == g("Crime")

        assert "Literary Criticism" == g("Criticism")
        assert "Political Science" == g("Current Events")

        # This is used to classify both books and video.
        assert "Drama" == g("Drama")

        assert "Economics" == g("Economics")
        assert "Education" == g("Education")
        assert "Technology" == g("Engineering")
        assert "Entertainment" == g("Entertainment")
        assert "Erotica" == g("Erotic Literature")
        assert None == g("Essays")
        assert "Philosophy" == g("Ethics")
        assert "Parenting & Family" == g("Family & Relationships")
        assert "Fantasy" == g("Fantasy")
        assert None == g("Feminist")
        assert None == g("Fiction")
        assert "Personal Finance & Investing" == g("Finance")
        assert "Folklore" == g("Folklore")
        assert None == g("Foreign Language")
        assert "Foreign Language Study" == g("Foreign Language Study")
        assert "Games" == g("Games")
        assert "Gardening" == g("Gardening")

        # If we know a title is fiction, then 'Gay/Lesbian' maps to
        # LGBTQ Fiction. If we don't know that it's fiction, it doesn't
        # map to any particular genre.
        assert None == g("Gay/Lesbian")
        assert "LGBTQ Fiction" == g("Gay/Lesbian", True)

        assert "Social Sciences" == g("Gender Studies")
        assert "Social Sciences" == g("Genealogy")
        assert None == g("Geography") # This is all over the place.
        assert "Reference & Study Aids" == g("Grammar & Language Usage")
        assert "Health & Diet" == g("Health & Fitness")
        assert "Historical Fiction" == g("Historical Fiction")
        assert "History" == g("History")
        assert "House & Home" == g("Home Design & Décor")
        assert "Horror" == g("Horror")
        assert None == g("Human Rights")
        assert "Humorous Fiction" == g("Humor (Fiction)")
        assert "Humorous Nonfiction" == g("Humor (Nonfiction)")
        assert None == g("Inspirational") # Mix of Christian nonfiction and fiction
        assert None == g("Journalism")
        assert "Judaism" == g("Judaica")
        assert None == g("Juvenile Fiction")
        assert None == g("Juvenile Literature")
        assert None == g("Juvenile Nonfiction")
        assert "Literary Criticism" == g("Language Arts")
        assert None == g("Latin") # A language, not a genre
        assert "Law" == g("Law")
        assert "Short Stories" == g("Literary Anthologies")
        assert "Literary Criticism" == g("Literary Criticism")
        assert None == g("Literature")
        assert "Management & Leadership" == g("Management")
        assert "Business" == g("Marketing & Sales")
        assert "Mathematics" == g("Mathematics")
        assert "Social Sciences" == g("Media Studies")
        assert "Medical" == g("Medical")
        assert "Military History" == g("Military")
        assert None == g("Multi-Cultural") # All over the place
        assert "Music" == g("Music")
        assert "Mystery" == g("Mystery")
        assert "Folklore" == g("Mythology")
        assert "Nature" == g("Nature")
        assert "Body, Mind & Spirit" == g("New Age")
        assert None == g("Non-English Fiction")
        assert None == g("Non-English Nonfiction")
        assert None == g("Nonfiction")
        assert "Travel" == g("Outdoor Recreation")
        assert "Performing Arts" == g("Performing Arts")
        assert "Pets" == g("Pets")
        assert "Philosophy" == g("Philosophy")
        assert "Photography" == g("Photography")
        assert "Science" == g("Physics")
        assert None == g("Picture Book Fiction")
        assert None == g("Picture Book Nonfiction")
        assert "Poetry" == g("Poetry")
        assert "Political Science" == g("Politics")
        assert None == g("Professional")
        assert "Psychology" == g("Psychiatry")
        assert "Psychology" == g("Psychiatry & Psychology")
        assert "Psychology" == g("Psychology")
        assert "Self-Help" == g("Recovery")
        assert "Reference & Study Aids" == g("Reference")
        assert "Religion & Spirituality" == g("Religion & Spirituality")
        assert None == g("Research")
        assert "Romance" == g("Romance")
        assert None == g("Scholarly")
        assert "Science" == g("Science")
        assert "Science Fiction" == g("Science Fiction")
        assert None == g("Science Fiction & Fantasy")
        assert "Self-Help" == g("Self Help")
        assert "Self-Help" == g("Self-Improvement")
        assert "Short Stories" == g("Short Stories")
        assert "Computers" == g("Social Media")
        assert "Social Sciences" == g("Social Studies")
        assert "Social Sciences" == g("Sociology")
        assert "Music" == g("Songbook")
        assert "Sports" == g("Sports & Recreations")
        assert "Study Aids" == g("Study Aids & Workbooks")
        assert "Suspense/Thriller" == g("Suspense")
        assert "Technology" == g("Technology")
        assert "Study Aids" == g("Text Book")
        assert "Suspense/Thriller" == g("Thriller")
        assert "Technology" == g("Transportation")
        assert "Travel" == g("Travel")
        assert "Travel" == g("Travel Literature")
        assert "True Crime" == g("True Crime")
        assert "Urban Fiction" == g("Urban Fiction")
        assert "Westerns" == g("Western")
        assert None == g("Women's Studies")
        assert "Literary Criticism" == g("Writing")
        assert None == g("Young Adult Fiction")
        assert None == g("Young Adult Literature")
        assert None == g("Young Adult Nonfiction")
