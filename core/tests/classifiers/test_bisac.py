import re

import pytest
from ...classifier import (
    BISACClassifier,
    Classifier,
)
from ...classifier.bisac import (
    MatchingRule,
    RE,
    anything,
    fiction,
    juvenile,
    m,
    nonfiction,
    something,
    ya,
)

class TestMatchingRule(object):

    def test_registered_object_returned_on_match(self):
        o = object()
        rule = MatchingRule(o, "Fiction")
        assert o == rule.match("fiction")
        assert None == rule.match("nonfiction")

        # You can't create a MatchingRule that returns None on
        # match, since that's the value returned on non-match.
        pytest.raises(
            ValueError, MatchingRule, None, "Fiction"
        )

    def test_string_match(self):
        rule = MatchingRule(True, 'Fiction')
        assert True == rule.match("fiction", "westerns")
        assert None == rule.match("nonfiction", "westerns")
        assert None == rule.match("all books", "fiction")

    def test_regular_expression_match(self):
        rule = MatchingRule(True, RE('F.*O'))
        assert True == rule.match("food")
        assert True == rule.match("flapjacks and oatmeal")
        assert None == rule.match("good", "food")
        assert None == rule.match("fads")

    def test_special_tokens_must_be_first(self):
        # In general, special tokens can only appear in the first
        # slot of a ruleset.
        for special in (juvenile, fiction, nonfiction):
            pytest.raises(ValueError, MatchingRule, True, "first item", special)

        # This rule doesn't apply to the 'anything' token.
        MatchingRule(True, "first item", anything)

    def test_juvenile_match(self):
        rule = MatchingRule(True, juvenile, "western")
        assert True == rule.match("juvenile fiction", "western")
        assert None == rule.match("juvenile nonfiction", "western civilization")
        assert None == rule.match("juvenile nonfiction", "penguins")
        assert None == rule.match("young adult nonfiction", "western")
        assert None == rule.match("fiction", "western")

    def test_ya_match(self):
        rule = MatchingRule(True, ya, "western")
        assert True == rule.match("young adult fiction", "western")
        assert True == rule.match("young adult nonfiction", "western")
        assert None == rule.match("juvenile fiction", "western")
        assert None == rule.match("fiction", "western")

    def test_nonfiction_match(self):
        rule = MatchingRule(True, nonfiction, "art")
        assert True == rule.match("juvenile nonfiction", "art")
        assert True == rule.match("art")
        assert None == rule.match("juvenile fiction", "art")
        assert None == rule.match("fiction", "art")

    def test_fiction_match(self):
        rule = MatchingRule(True, fiction, "art")
        assert None == rule.match("juvenile nonfiction", "art")
        assert None == rule.match("art")
        assert True == rule.match("juvenile fiction", "art")
        assert True == rule.match("fiction", "art")

    def test_anything_match(self):
        # 'anything' can go up front.
        rule = MatchingRule(True, anything, 'Penguins')
        assert True == rule.match("juvenile fiction", "science fiction", "antarctica", "animals", "penguins")
        assert True == rule.match("fiction", "penguins")
        assert True == rule.match("nonfiction", "penguins")
        assert True == rule.match("penguins")
        assert None == rule.match("geese")

        # 'anything' can go in the middle, even after another special
        # match rule.
        rule = MatchingRule(True, fiction, anything, 'Penguins')
        assert True == rule.match("juvenile fiction", "science fiction", "antarctica", "animals", "penguins")
        assert True == rule.match("fiction", "penguins")
        assert None == rule.match("fiction", "geese")

        # It's redundant, but 'anything' can go last.
        rule = MatchingRule(True, anything, 'Penguins', anything)
        assert True == rule.match("juvenile fiction", "science fiction", "antarctica", "animals", "penguins")
        assert True == rule.match("fiction", "penguins", "more penguins")
        assert True == rule.match("penguins")
        assert None == rule.match("geese")

    def test_something_match(self):
        # 'something' can go anywhere.
        rule = MatchingRule(True, something, 'Penguins', something, something)

        assert True == rule.match("juvenile fiction", "penguins", "are", "great")
        assert True == rule.match("penguins", "penguins", "i said", "penguins")
        assert None == rule.match("penguins", "what?", "i said", "penguins")

        # unlike 'anything', 'something' must match a specific token.
        assert None == rule.match("penguins")
        assert None == rule.match("juvenile fiction", "penguins", "and seals")


class MockSubject(object):
    def __init__(self, identifier, name):
        self.identifier = identifier
        self.name = name


class TestBISACClassifier(object):

    def _subject(self, identifier, name):
        subject = MockSubject(identifier, name)
        subject.genre, subject.audience, subject.target_age, subject.fiction = BISACClassifier.classify(subject)
        return subject

    def genre_is(self, name, expect):
        subject = self._subject("", name)
        if expect and subject.genre:
            assert expect == subject.genre.name
        else:
            assert expect == subject.genre

    def test_every_rule_fires(self):
        """There's no point in having a rule that doesn't catch any real BISAC
        subjects. The presence of such a rule generally indicates a
        bug -- usually a typo, or a rule is completely 'shadowed' by another
        rule above it.
        """
        subjects = []
        for identifier, name in sorted(BISACClassifier.NAMES.items()):
            subjects.append(self._subject(identifier, name))
        for i in BISACClassifier.FICTION:
            if i.caught == []:
                raise Exception(
                    "Fiction rule %s didn't catch anything!" % i.ruleset
                )

        for i in BISACClassifier.GENRE:
            if i.caught == []:
                raise Exception(
                    "Genre rule %s didn't catch anything!" % i.ruleset
                )

        need_fiction = []
        need_audience = []
        for subject in subjects:
            if subject.fiction is None:
                need_fiction.append(subject)
            if subject.audience is None:
                need_audience.append(subject)

        # We determined fiction/nonfiction status for every BISAC
        # subject except for humor, drama, and poetry.
        for subject in need_fiction:
            assert any(subject.name.lower().startswith(x)
                       for x in ['humor', 'drama', 'poetry'])

        # We determined the target audience for every BISAC subject.
        assert [] == need_audience

        # At this point, you can also create a list of subjects that
        # were not classified in some way. There are currently about
        # 400 such subjects, most of them under Juvenile and Young
        # Adult.
        #
        # Not every subject has to be classified under a genre, but
        # if it's possible for one to be, it should be. This is the place
        # to check how well the current rules are operating.
        #
        # need_genre = sorted(x.name for x in subjects if x.genre is None)

    def test_genre_spot_checks(self):
        """Test some unusual cases with respect to how BISAC
        classifications are turned into genres.
        """
        genre_is = self.genre_is

        genre_is("Fiction / Science Fiction / Erotica", "Erotica")
        genre_is("Literary Criticism / Science Fiction", "Literary Criticism")
        genre_is("Fiction / Christian / Science Fiction", "Religious Fiction")
        genre_is("Fiction / Science Fiction / Short Stories", "Short Stories")
        genre_is("Fiction / Steampunk", "Steampunk")
        genre_is("Fiction / Science Fiction / Steampunk", "Steampunk")
        genre_is("Fiction / African American / Urban", "Urban Fiction")
        genre_is("Fiction / Urban", None)
        genre_is("History / Native American", "United States History")
        genre_is("History / Modern / 17th Century", "Renaissance & Early Modern History")
        genre_is("Biography & Autobiography / Composers & Musicians", "Music"),
        genre_is("Biography & Autobiography / Entertainment & Performing Arts", "Entertainment"),
        genre_is("Fiction / Christian", "Religious Fiction"),
        genre_is("Juvenile Nonfiction / Science & Nature / Fossils", "Nature")
        genre_is("Juvenile Nonfiction / Science & Nature / Physics", "Science")
        genre_is("Juvenile Nonfiction / Science & Nature / General", "Science")
        genre_is("Juvenile Fiction / Social Issues / General", "Life Strategies")
        genre_is("Juvenile Nonfiction / Social Issues / Pregnancy", "Life Strategies")
        genre_is("Juvenile Nonfiction / Religious / Christian / Social Issues", "Christianity")

        genre_is("Young Adult Fiction / Zombies", "Horror")
        genre_is("Young Adult Fiction / Superheroes", "Suspense/Thriller")
        genre_is("Young Adult Nonfiction / Social Topics", "Life Strategies")
        genre_is("Young Adult Fiction / Social Themes", None)

        genre_is("Young Adult Fiction / Poetry", "Poetry")
        genre_is("Poetry", "Poetry")

        # Grandfathered in from an older test to validate that the new
        # BISAC algorithm gives the same results as the old one.
        genre_is("JUVENILE FICTION / Dystopian", "Dystopian SF")
        genre_is("JUVENILE FICTION / Stories in Verse (see also Poetry)",
                 "Poetry")


    def test_deprecated_bisac_terms(self):
        """These BISAC terms have been deprecated. We classify them
        the same as the new terms.
        """
        self.genre_is("Psychology & Psychiatry / Jungian", "Psychology")
        self.genre_is("Mind & Spirit / Crystals, Man", "Body, Mind & Spirit")
        self.genre_is("Technology / Fire", "Technology")
        self.genre_is(
            "Young Adult Nonfiction / Social Situations / Junior Prom",
            "Life Strategies"
        )

    def test_non_bisac_classified_as_keywords(self):
        """Categories that are not official BISAC categories (and any official
        BISAC categories our rules didn't catch) are classified as
        though they were free-text keywords.
        """
        self.genre_is("Fiction / Unicorns", "Fantasy")

    def test_fiction_spot_checks(self):
        def fiction_is(name, expect):
            subject = self._subject("", name)
            assert expect == subject.fiction

        # Some easy tests.
        fiction_is("Fiction / Science Fiction", True)
        fiction_is("Antiques & Collectibles / Kitchenware", False)

        # Humor, drama and poetry do not have fiction classifications
        # unless the fiction classification comes from elsewhere in the
        # subject.
        fiction_is("Humor", None)
        fiction_is("Drama", None)
        fiction_is("Poetry", None)
        fiction_is("Young Adult Fiction / Poetry", True)

        fiction_is("Young Adult Nonfiction / Humor", False)
        fiction_is("Juvenile Fiction / Humorous Stories", True)

        # Literary collections in general are presumed to be
        # collections of short fiction, but letters and essays are
        # definitely nonfiction.
        fiction_is("Literary Collections / General", True)
        fiction_is("Literary Collections / Letters", False)
        fiction_is("Literary Collections / Essays", False)

        # Grandfathered in from an older test to validate that the new
        # BISAC algorithm gives the same results as the old one.
        fiction_is("FICTION / Classics", True)
        fiction_is("JUVENILE FICTION / Concepts / Date & Time", True)
        fiction_is("YOUNG ADULT FICTION / Lifestyles / Country Life", True)
        fiction_is("HISTORY / General", False)


    def test_audience_spot_checks(self):

        def audience_is(name, expect):
            subject = self._subject("", name)
            assert expect == subject.audience

        adult = Classifier.AUDIENCE_ADULT
        adults_only = Classifier.AUDIENCE_ADULTS_ONLY
        ya = Classifier.AUDIENCE_YOUNG_ADULT
        children = Classifier.AUDIENCE_CHILDREN

        audience_is("Fiction / Science Fiction", adult)
        audience_is("Fiction / Science Fiction / Erotica", adults_only)
        audience_is("Juvenile Fiction / Science Fiction", children)
        audience_is("Young Adult Fiction / Science Fiction / General", ya)

        # Grandfathered in from an older test to validate that the new
        # BISAC algorithm gives the same results as the old one.
        audience_is("FAMILY & RELATIONSHIPS / Love & Romance", adult)
        audience_is("JUVENILE FICTION / Action & Adventure / General", children)
        audience_is("YOUNG ADULT FICTION / Action & Adventure / General", ya)

    def test_target_age_spot_checks(self):

        def target_age_is(name, expect):
            subject = self._subject("", name)
            assert expect == subject.target_age

        # These are the only BISAC classifications with implied target
        # ages.
        for check in ('Fiction', 'Nonfiction'):
            target_age_is("Juvenile %s / Readers / Beginner" % check,
                          (0,4))
            target_age_is("Juvenile %s / Readers / Intermediate" % check,
                          (5,7))
            target_age_is("Juvenile %s / Readers / Chapter Books" % check,
                          (8,13))
            target_age_is(
                "Juvenile %s / Religious / Christian / Early Readers" % check,
                (5,7)
            )

        # In all other cases, the classifier will fall back to the
        # default for the target audience.
        target_age_is("Fiction / Science Fiction / Erotica", (18, None))
        target_age_is("Fiction / Science Fiction", (18, None))
        target_age_is("Juvenile Fiction / Science Fiction", (None, None))
        target_age_is("Young Adult Fiction / Science Fiction / General",
                      (14, 17))

    def test_feedbooks_bisac(self):
        """Feedbooks uses a system based on BISAC but with different
        identifiers, different names, and some additions. This is all
        handled transparently by the default BISAC classifier.
        """
        subject = self._subject("FBFIC022000", "Mystery & Detective")
        assert "Mystery" == subject.genre.name

        # This is not an official BISAC classification, so we'll
        # end up running it through the keyword classifier.
        subject = self._subject("FSHUM000000N", "Human Science")
        assert "Social Sciences" == subject.genre.name

    def test_scrub_identifier(self):
        # FeedBooks prefixes are removed.
        assert "abc" == BISACClassifier.scrub_identifier("FBabc")

        # Otherwise, the identifier is left alone.
        assert "abc" == BISACClassifier.scrub_identifier("abc")

        # If the identifier is recognized as an official BISAC identifier,
        # the canonical name is also returned. This will override
        # any other name associated with the subject for classification
        # purposes.
        assert (("FIC015000", "Fiction / Horror") ==
            BISACClassifier.scrub_identifier("FBFIC015000"))

    def test_scrub_name(self):
        """Sometimes a data provider sends BISAC names that contain extra or
        nonstandard characters. We store the data as it was provided to us,
        but when it's time to classify things, we normalize it.
        """
        def scrubbed(before, after):
            assert after == BISACClassifier.scrub_name(before)

        scrubbed("ART/Collections  Catalogs  Exhibitions/",
                 ["art", "collections, catalogs, exhibitions"])
        scrubbed("ARCHITECTURE|History|Contemporary|",
                 ["architecture", "history", "contemporary"])
        scrubbed("BIOGRAPHY & AUTOBIOGRAPHY / Editors, Journalists, Publishers",
                 ["biography & autobiography", "editors, journalists, publishers"])
        scrubbed("EDUCATION/Teaching Methods & Materials/Arts & Humanities */",
                 ["education", "teaching methods & materials",
                  "arts & humanities"])
        scrubbed("JUVENILE FICTION / Family / General (see also headings under Social Issues)",
                 ["juvenile fiction", "family", "general"])
