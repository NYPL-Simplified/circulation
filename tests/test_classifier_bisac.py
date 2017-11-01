import re
from nose.tools import (
    assert_raises,
    eq_,
    set_trace
)
from classifier.bisac import (
    nonfiction,
    fiction,
    juvenile,
    anything,
    MatchingRule,
    BISACClassifier,
    m,    
)
from classifier import Classifier

class TestMatchingRule(object):

    def test_registered_object_returned_on_match(self):
        o = object()
        rule = MatchingRule(o, "Fiction")
        eq_(o, rule.match("fiction"))
        eq_(None, rule.match("nonfiction"))

        # You can't create a MatchingRule that returns None on
        # match, since that's the value returned on non-match.
        assert_raises(
            ValueError, MatchingRule, None, "Fiction"
        )

    def test_string_match(self):        
        rule = MatchingRule(True, 'Fiction')
        eq_(True, rule.match("fiction", "westerns"))
        eq_(None, rule.match("nonfiction", "westerns"))
        eq_(None, rule.match("all books", "fiction"))

    def test_regular_expression_match(self):
        rule = MatchingRule(True, re.compile('F.*O'))
        eq_(True, rule.match("food"))
        eq_(True, rule.match("flapjacks and oatmeal"))
        eq_(None, rule.match("good", "food"))
        eq_(None, rule.match("fads"))

    def test_special_tokens_must_be_first(self):
        # In general, special tokens can only appear in the first
        # slot of a ruleset.
        for special in (juvenile, fiction, nonfiction):
            assert_raises(ValueError, MatchingRule, True, "first item", special)

        # This rule doesn't apply to the 'anything' token.
        MatchingRule(True, "first item", anything)

    def test_juvenile_match(self):
        rule = MatchingRule(True, juvenile, "western")
        eq_(True, rule.match("juvenile fiction", "western"))
        eq_(None, rule.match("juvenile nonfiction", "western civilization"))
        eq_(None, rule.match("juvenile nonfiction", "penguins"))
        eq_(None, rule.match("young adult nonfiction", "western"))
        eq_(None, rule.match("fiction", "western"))

    def test_ya_match(self):
        rule = MatchingRule(True, ya, "western")
        eq_(True, rule.match("young adult fiction", "western"))
        eq_(True, rule.match("juvenile fiction", "western"))
        eq_(None, rule.match("fiction", "western"))

    def test_nonfiction_match(self):
        rule = MatchingRule(True, nonfiction, "art")
        eq_(True, rule.match("juvenile nonfiction", "art"))
        eq_(True, rule.match("art"))
        eq_(None, rule.match("juvenile fiction", "art"))
        eq_(None, rule.match("fiction", "art"))

    def test_fiction_match(self):
        rule = MatchingRule(True, fiction, "art")
        eq_(None, rule.match("juvenile nonfiction", "art"))
        eq_(None, rule.match("art"))
        eq_(True, rule.match("juvenile fiction", "art"))
        eq_(True, rule.match("fiction", "art"))

    def test_anything_match(self):
        # 'anything' can go up front.
        rule = MatchingRule(True, anything, 'Penguins')
        eq_(True, rule.match("juvenile fiction", "science fiction", "antarctica", "animals", "penguins"))
        eq_(True, rule.match("fiction", "penguins"))
        eq_(True, rule.match("nonfiction", "penguins"))
        eq_(True, rule.match("penguins"))
        eq_(None, rule.match("geese"))

        # 'anything' can go in the middle, even after another special
        # match rule.
        rule = MatchingRule(True, fiction, anything, 'Penguins')
        eq_(True, rule.match("juvenile fiction", "science fiction", "antarctica", "animals", "penguins"))
        eq_(True, rule.match("fiction", "penguins"))
        eq_(None, rule.match("fiction", "geese"))

        # It's redundant, but 'anything' can go last.
        rule = MatchingRule(True, anything, 'Penguins', anything)
        eq_(True, rule.match("juvenile fiction", "science fiction", "antarctica", "animals", "penguins"))
        eq_(True, rule.match("fiction", "penguins", "more penguins"))
        eq_(True, rule.match("penguins"))
        eq_(None, rule.match("geese"))

    def test_something_match(self):
        # 'something' can go anywhere.
        rule = MatchingRule(True, something, 'Penguins', something, something)

        eq_(True, rule.match("juvenile fiction", "penguins", "are", "great"))
        eq_(True, rule.match("penguins", "penguins", "i said", "penguins"))
        eq_(None, rule.match("penguins", "what?", "i said", "penguins"))

        # unlike 'anything', 'something' must match a specific token.
        eq_(None, rule.match("penguins"))
        eq_(None, rule.match("juvenile fiction", "penguins", "and seals"))


class MockSubject(object):
    def __init__(self, identifier, name):
        self.identifier = identifier
        self.name = name


class TestBISACClassifier(object):        

    def _subject(self, identifier, name):
        subject = MockSubject(identifier, name)
        subject.genre, subject.audience, subject.target_age, subject.fiction = BISACClassifier.classify(subject)
        return subject

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
        # subject except the ones that start with 'humor'.
        for subject in need_fiction:
            assert subject.name.lower().startswith('humor')

        # We determined the target audience for every BISAC subject.
        eq_([], need_audience)

        # At this point, you can also create a list of subjects
        # that were not classified in some way. There are currently
        # about 240 such subjects, most of them under "Juvenile Fiction"
        # and "Juvenile Nonfiction".
        #
        # Not every subject has to be classified under a genre, but
        # if it's possible for one to be, it should be. This is the place
        # to check how well the current rules are operating.
        #
        #need_genre = sorted(x.name for x in subjects if x.genre is None)

    def test_genre_spot_checks(self):
        """Test some unusual cases with respect to how BISAC
        classifications are turned into genres.
        """
        def genre_is(name, genre):
            subject = self._subject("", name)
            if genre and subject.genre:
                eq_(genre, subject.genre.name)
            else:
                eq_(genre, subject.genre)

        genre_is("Fiction / Science Fiction / Erotica", "Erotica")
        genre_is("Literary Criticism / Science Fiction", "Literary Criticism")
        genre_is("Fiction / Christian / Science Fiction", "Religious Fiction")
        genre_is("Fiction / Science Fiction / Short Stories", "Short Stories")
        genre_is("Fiction / Steampunk", "Steampunk")
        genre_is("Fiction / Science Fiction / Steampunk", "Steampunk")
        genre_is("Fiction / African American / Urban", "Urban Fiction")
        genre_is("Fiction / Urban", None)
        genre_is("History / Modern / 17th Century", "Renaissance & Early Modern History")
        genre_is("Juvenile Nonfiction / Science & Nature / Fossils", "Nature")
        genre_is("Juvenile Nonfiction / Science & Nature / Physics", "Science")
        genre_is("Juvenile Nonfiction / Science & Nature / General", "Science")
        genre_is("Juvenile Nonfiction / Science & Nature", "Science")
        genre_is("Juvenile Fiction / Social Issues / General", "Life Strategies")
        genre_is("Juvenile Nonfiction / Social Issues / Pregnancy", "Life Strategies")
        genre_is("Juvenile Nonfiction / Social Issues / Pregnancy", "Life Strategies")
        genre_is("Juvenile Nonfiction / Religious / Christian / Social Issues", "Christianity")

        genre_is("Young Adult Fiction / Zombies", "Horror")
        genre_is("Young Adult Fiction / Superheroes", "Suspense & Thriller")
        genre_is("Young Adult Nonfiction / Social Topics", "Life Strategies")
        genre_is("Young Adult Fiction / Social Themes", None)
