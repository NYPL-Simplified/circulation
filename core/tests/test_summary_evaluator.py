"""Test the code that evaluates the quality of summaries."""

from textblob import TextBlob
from textblob.exceptions import MissingCorpusError
from ..util.summary import SummaryEvaluator

class TestSummaryEvaluator(object):

    def _best(self, *summaries):
        e = SummaryEvaluator()
        for s in summaries:
            e.add(s)
        e.ready()
        return e.best_choice()[0]

    def test_four_sentences_is_better_than_three(self):
        s1 = "Hey, this is Sentence one. And now, here is Sentence two."
        s2 = "Sentence one. Sentence two. Sentence three. Sentence four."
        assert s2 == self._best(s1, s2)

    def test_four_sentences_is_better_than_five(self):
        s1 = "Sentence 1. Sentence 2. Sentence 3. Sentence 4. Sentence 5."
        s2 = "Sentence one. Sentence two. Sentence three.  Sentence four."
        assert s2 == self._best(s1, s2)

    def test_shorter_is_better(self):
        s1 = "A very long sentence."
        s2 = "Tiny sentence."
        assert s2 == self._best(s1, s2)

    def test_noun_phrase_coverage_is_important(self):

        s1 = "The story of Alice and the White Rabbit."
        s2 = "The story of Alice and the Mock Turtle."
        s3 = "Alice meets the Mock Turtle and the White Rabbit."
        # s3 is longer, and they're all one sentence, but s3 mentions
        # three noun phrases instead of two.
        assert s3 == self._best(s1, s2, s3)

    def test_non_english_is_penalized(self):
        """If description text appears not to be in English, it is rated down
        for its deviations from average English bigram distribution.
        """
        dutch = "Op haar nieuwe school leert de jarige Bella (ik-figuur) een mysterieuze jongen kennen op wie ze ogenblikkelijk verliefd wordt. Hij blijkt een groot geheim te hebben. Vanaf ca. jaar."

        evaluator = SummaryEvaluator()
        evaluator.add(dutch)
        evaluator.ready()

        dutch_no_language_penalty = evaluator.score(
            dutch, apply_language_penalty=False)

        dutch_language_penalty = evaluator.score(
            dutch, apply_language_penalty=True)

    def test_english_is_not_penalized(self):
        """If description text appears to be in English, it is not rated down
        for its deviations from average English bigram distribution.
        """

        english = "After the warrior cat Clans settle into their new homes, the harmony they once had disappears as the clans start fighting each other, until the day their common enemy the badger."

        evaluator = SummaryEvaluator()
        evaluator.add(english)
        evaluator.ready()

        english_no_language_penalty = evaluator.score(
            english, apply_language_penalty=False)

        english_language_penalty = evaluator.score(
            english, apply_language_penalty=True)
        assert english_language_penalty == english_no_language_penalty

    def test_missing_corpus_error_ignored(self):
        class AlwaysErrorBlob(TextBlob):
            @property
            def noun_phrases(self):
                raise MissingCorpusError()

        evaluator = SummaryEvaluator()
        assert evaluator._nltk_installed == True

        summary = "Yes, this is a summary."
        evaluator.add(summary, parser=AlwaysErrorBlob)
        evaluator.add("And another", parser=AlwaysErrorBlob)
        assert evaluator._nltk_installed == False
        assert 1 == evaluator.score(summary)
