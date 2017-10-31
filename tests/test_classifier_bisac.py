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


    def test_tough_case(self):
        rule = MatchingRule(True, nonfiction, "History", "Modern", re.compile('^1[678]th Century'))
        eq_(True, rule.match("history", "modern", "17th century"))

class MockSubject(object):
    def __init__(self, identifier, name):
        self.identifier = identifier
        self.name = name

class TestBISACClassifier(object):        

    def test_every_rule_fires(self):
        """There's no point in having a rule that doesn't catch any real BISAC
        subjects. The presence of such a rule generally indicates a
        bug -- usually a typo, or a rule is completely 'shadowed' by another
        rule above it.
        """
        subjects = []
        for identifier, name in sorted(BISACClassifier.NAMES.items()):
            subject = MockSubject(identifier, name)
            subjects.append(subject)
            subject.genre, subject.audience, subject.target_age, subject.fiction = BISACClassifier.classify(subject)

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

        # At this point, you can also create a list of subjects
        # that were not classified in some way. There are currently
        # about 240 such subjects, most of them under "Juvenile Fiction"
        # and "Juvenile Nonfiction".
        #
        # Not every subject has to be classified under a genre, but
        # if it's possible for one to be, it should be.
        #
        # need_genre = sorted(x.name for x in subjects if not x.genre)
        need_fiction = sorted(x.name for x in subjects if not x.fiction)
        need_audience = sorted(x.name for x in subjects if not x.audience)
        need_target_age = sorted(x.name for x in subjects if not x.target_age)

    def test_every_fiction_rule_fires(self):
        subjects = []
        for identifier, name in sorted(BISACClassifier.NAMES.items()):
            subject = MockSubject(identifier, name)
            subjects.append(subject)
            subject.genre, i1, i2, i3 = BISACClassifier.classify(subject)
    
        for i in BISACClassifier.GENRE:
            if i.caught == []:
                raise Exception(
                    "Rule %s didn't catch anything!" % i.ruleset
                )
