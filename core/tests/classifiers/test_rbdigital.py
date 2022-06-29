from ...classifier import (
    RBDigitalAudienceClassifier,
    RBDigitalSubjectClassifier,
    Classifier,
)

class MockSubject(object):
    def __init__(self, identifier, name):
        self.identifier = identifier
        self.name = name

class ClassifierTest(object):

    CLASSIFIER = None

    def _subject(self, identifier, name):
        subject = MockSubject(identifier, name)
        (subject.genre, subject.audience, subject.target_age,
         subject.fiction) = self.CLASSIFIER.classify(subject)
        return subject

    def genre_is(self, identifier, expect, name=None):
        subject = self._subject(identifier, name)
        if expect and subject.genre:
            assert expect == subject.genre.name
        else:
            assert expect == subject.genre

    def fiction_is(self, identifier, expect, name=None):
        subject = self._subject(identifier, name)
        assert expect == subject.fiction

    def audience_is(self, identifier, expect, name=None):
        subject = self._subject(identifier, name)
        assert expect == subject.audience

    def target_age_is(self, identifier, expect, name=None):
        subject = self._subject(identifier, name)
        assert expect == subject.target_age

class TestRBDigitalAudienceClassifier(ClassifierTest):

    CLASSIFIER = RBDigitalAudienceClassifier

    def test_target_age(self):
        self.target_age_is("Adult", (18, None))
        self.target_age_is("young adult", (14, 17))

        # RBdigital splits up childrens books into "beginning reader"
        # (birth-8 years) and "childrens" (9-13 years).
        self.target_age_is("beginning reader", (0, 8))
        self.target_age_is("childrens", (9, 13))


class TestRBDigitalSubjectClassifier(ClassifierTest):

    CLASSIFIER = RBDigitalSubjectClassifier

    def test_fiction(self):
        self.fiction_is("general-nonfiction", False)
        self.fiction_is("juvenile-nonfiction", False)
        self.fiction_is("humor", False)

        self.fiction_is("juvenile-fiction", True)
        self.fiction_is("humorous-fiction", True)
        self.fiction_is("general-fiction", True)

        # Most subjects will inherit fiction status from the genre
        # default.
        self.fiction_is("cooking", False)
        self.fiction_is("true-crime", False)
        self.fiction_is("romance", True)
        self.fiction_is("drama", True)

        # Some subjects have no fiction status because they
        # contain both fiction and nonfiction.
        self.fiction_is("african-american-interest", None)
        self.fiction_is("lgbt-interest", None)

    def test_audience(self):
        # We generally don't try to derive audience information from
        # subjects of this type because RBdigital always provides an
        # explicit audience.
        self.audience_is("erotica", Classifier.AUDIENCE_ADULTS_ONLY)
        self.audience_is("cooking", None)
        self.audience_is("juvenile-nonfiction", None)
        self.audience_is("juvenile-fiction", None)

    def test_target_age(self):
        # We don't even try to derive audience information from
        # subjects of this type, because RBdigital always provides an
        # explicit audience which is more useful for this purpose.
        self.target_age_is("erotica", None)
        self.target_age_is("cooking", None)
        self.target_age_is("juvenile-nonfiction", None)
        self.target_age_is("juvenile-fiction", None)

    def test_genre(self):
        # Some RBdigital subjects can't be assigned to genres at all.
        self.genre_is("general-nonfiction", None)
        self.genre_is("juvenile-fiction", None)
        self.genre_is("juvenile-nonfiction", None)
        self.genre_is("african-american-interest", None)

        # Fiction in some subjects can be assigned to a subgenre even though
        # nonfiction in the same subject cannot.
        self.genre_is("lgbt-interest", None)
        assert (
            self.CLASSIFIER.genre('lgbt interest', None, True).name ==
            "LGBTQ Fiction")

        # But most subjects can be assigned to a genre no matter what.
        self.genre_is("arts-entertainment", "Entertainment")
        self.genre_is("biography-autobiography-memoir", "Biography & Memoir")
        self.genre_is("business-economics", "Business")
        self.genre_is("classics", "Classics")
        self.genre_is("comics-graphic-novels", "Comics & Graphic Novels")
        self.genre_is("cooking", "Cooking")
        self.genre_is("drama", "Drama")
        self.genre_is("fantasy", "Fantasy")
        self.genre_is("foreign-language-study", "Foreign Language Study")
        self.genre_is("general-fiction", "Literary Fiction")
        self.genre_is("historical-fiction", "Historical Fiction")
        self.genre_is("history", "History")
        self.genre_is("home-garden", "Hobbies & Home")
        self.genre_is("horror", "Horror")
        self.genre_is("humor", "Humorous Nonfiction")
        self.genre_is("humorous-fiction", "Humorous Fiction")
        self.genre_is("inspirational-fiction", "Religious Fiction")
        self.genre_is("inspirational-nonfiction", "Religion & Spirituality")
        self.genre_is("language-arts", "Reference & Study Aids")
        self.genre_is("literary-fiction", "Literary Fiction")
        self.genre_is("mystery", "Mystery")
        self.genre_is("poetry", "Poetry")
        self.genre_is("politics-current-events", "Political Science")
        self.genre_is("religion", "Religion & Spirituality")
        self.genre_is("romance", "Romance")
        self.genre_is("science", "Science")
        self.genre_is("sci-fi", "Science Fiction")
        self.genre_is("self-help", "Self-Help")
        self.genre_is("short-stories", "Short Stories")
        self.genre_is("social-science", "Social Sciences")
        self.genre_is("sports", "Sports")
        self.genre_is("suspense-thriller", "Suspense/Thriller")
        self.genre_is("travel", "Travel")
        self.genre_is("true-crime", "True Crime")
        self.genre_is("urban-fiction", "Urban Fiction")
        self.genre_is("western", "Westerns")
        self.genre_is("womens-fiction", "Women's Fiction")
