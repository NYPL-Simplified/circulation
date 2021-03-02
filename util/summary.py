import logging
from textblob import TextBlob
from textblob.exceptions import MissingCorpusError
from collections import Counter
from nose.tools import set_trace
from . import (
    Bigrams,
    english_bigrams,
)
import re

class SummaryEvaluator(object):

    """Evaluate summaries of a book to find a usable summary.

    A usable summary will have good coverage of the popular noun
    phrases found across all summaries of the book, will have an
    approximate length of four sentences (this is customizable), and
    will not mention words that indicate it's a summary of a specific
    edition of the book.

    All else being equal, a shorter summary is better.

    A summary is penalized for apparently not being in English.
    """

    # These phrases are indicative of a description we can't use for
    # whatever reason.
    default_bad_phrases = set([
        "version of",
        "retelling of",
        "abridged",
        "retelling",
        "condensed",
        "adaptation of",
        "look for",
        "new edition",
        "excerpts",
        "version",
        "edition",
        "selections",
        "complete texts",
        "in one volume",
        "contains",
        "--container",
        "--original container",
        "playaway",
        "complete novels",
        "all rights reserved",
    ])

    bad_res = set([
        re.compile("the [^ ]+ Collection"),
        re.compile("Includes"),
        re.compile("This is"),
    ])

    _nltk_installed = True
    log = logging.getLogger("Summary Evaluator")

    def __init__(self, optimal_number_of_sentences=4,
                 noun_phrases_to_consider=10, bad_phrases=None):
        self.optimal_number_of_sentences=optimal_number_of_sentences
        self.summaries = []
        self.noun_phrases = Counter()
        self.blobs = dict()
        self.scores = dict()
        self.noun_phrases_to_consider = float(noun_phrases_to_consider)
        self.top_noun_phrases = None
        if bad_phrases is None:
            self.bad_phrases = self.default_bad_phrases
        else:
            self.bad_phrases = bad_phrases

    def add(self, summary, parser=None):
        parser_class = parser or TextBlob
        if isinstance(summary, bytes):
            summary = summary.decode("utf8")
        if summary in self.blobs:
            # We already evaluated this summary. Don't count it more than once
            return
        blob = parser_class(summary)
        self.blobs[summary] = blob
        self.summaries.append(summary)

        if self._nltk_installed:
            try:
                for phrase in blob.noun_phrases:
                    self.noun_phrases[phrase] = self.noun_phrases[phrase] + 1
            except MissingCorpusError as e:
                self._nltk_installed = False
                self.log.error("Summary cannot be evaluated: NLTK not installed %r" % e)

    def ready(self):
        """We are done adding to the corpus and ready to start evaluating."""
        self.top_noun_phrases = set([
            k for k, v in self.noun_phrases.most_common(
                int(self.noun_phrases_to_consider))])

    def best_choice(self):
        c = self.best_choices(1)
        if c:
            return c[0]
        else:
            return None, None

    def best_choices(self, n=3):
        """Choose the best `n` choices among the current summaries."""
        scores = Counter()
        for summary in self.summaries:
            scores[summary] = self.score(summary)
        return scores.most_common(n)

    def score(self, summary, apply_language_penalty=True):
        """Score a summary relative to our current view of the dataset."""
        if not self._nltk_installed:
            # Without NLTK, there's no need to evaluate the score.
            return 1

        if isinstance(summary, bytes):
            summary = summary.decode("utf8")
        if summary in self.scores:
            return self.scores[summary]
        score = 1
        blob = self.blobs[summary]

        top_noun_phrases_used = len(
            [p for p in self.top_noun_phrases if p in blob.noun_phrases])
        score = 1 * (top_noun_phrases_used/self.noun_phrases_to_consider)

        try:
            sentences = len(blob.sentences)
        except Exception as e:
            # Can't parse into sentences for whatever reason.
            # Make a really bad guess.
            sentences = summary.count(". ") + 1
        off_from_optimal = abs(sentences-self.optimal_number_of_sentences)
        if off_from_optimal == 1:
            off_from_optimal = 1.5
        if off_from_optimal:
            # This summary is too long or too short.
            score /= (off_from_optimal ** 1.5)

        bad_phrases = 0
        l = summary.lower()
        for i in self.bad_phrases:
            if i in l:
                bad_phrases += 1

        for i in self.bad_res:
            if i.search(summary):
                bad_phrases += 1

        if l.count(" -- ") > 3:
            bad_phrases += (l.count(" -- ") - 3)

        score *= (0.5 ** bad_phrases)

        if apply_language_penalty:
            language_difference = english_bigrams.difference_from(
                Bigrams.from_string(summary))
            if language_difference > 1:
                score *= (0.5 ** (language_difference-1))

        return score
