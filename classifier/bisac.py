# encoding: utf-8
import csv
import os
import re
import string
from . import *
from .keyword import KeywordBasedClassifier

class CustomMatchToken(object):
    """A custom token used in matching rules."""
    def matches(self, subject_token):
        """Does the given token match this one?"""
        raise NotImplementedError()

class Something(CustomMatchToken):
    """A CustomMatchToken that will match any single token."""
    def matches(self, subject_token):
        return True

class RE(CustomMatchToken):
    """A CustomMatchToken that performs a regular expression search."""
    def __init__(self, pattern):
        self.re = re.compile(pattern, re.I)

    def matches(self, subject_token):
        return self.re.search(subject_token)

class Interchangeable(CustomMatchToken):
    """A token that matches a list of strings."""
    def __init__(self, *choices):
        """All of these strings are interchangeable for matching purposes."""
        self.choices = set([Lowercased(x) for x in choices])

    def matches(self,subject_token):
        return Lowercased(subject_token) in self.choices

# Special tokens for use in matching rules.
something = Something()
fiction = Interchangeable("Juvenile Fiction", "Young Adult Fiction", "Fiction")
juvenile = Interchangeable("Juvenile Fiction", "Juvenile Nonfiction")
ya = Interchangeable("Young Adult Fiction", "Young Adult Nonfiction")

# These need special code because they can modify the token stack.
anything = object()
nonfiction = object()

# These are BISAC categories that changed their names. We want to treat both
# names as equivalent. In most cases, the name change is cosmetic.
body_mind_spirit = Interchangeable("Body, Mind & Spirit", "Mind & Spirit")
psychology = Interchangeable("Psychology", "Psychology & Psychiatry")
technology = Interchangeable("Technology & Engineering", "Technology")
social_topics = Interchangeable("Social Situations", "Social Topics")

# This name change is _not_ cosmetic. The category was split into
# two, and we're putting everything that was in the old category into
# one of the two.
literary_criticism = Interchangeable(
    "Literary Criticism", "Literary Criticism & Collections"
)

# If these variables are used in a rule, they must be the first token in
# that rule.
special_variables = { nonfiction : "nonfiction",
                      fiction : "fiction",
                      juvenile : "juvenile",
                      ya : "ya",}

class MatchingRule(object):
    """A rule that takes a list of subject parts and returns
    an appropriate classification.
    """

    def __init__(self, result, *ruleset):
        if result is None:
            raise ValueError(
                "MatchingRule returns None on a non-match, it can't also return None on a match."
            )

        self.result = result
        self.ruleset = []

        # Track the subjects that were 'caught' by this rule,
        # for debugging purposes.
        self.caught = []

        for i, rule in enumerate(ruleset):
            if i > 0 and rule in special_variables:
                raise ValueError(
                    "Special token '%s' must be the first in a ruleset."
                    % special_variables[rule]
                )

            if isinstance(rule, str):
                # It's a string. We do case-insensitive comparisons,
                # so lowercase it.
                self.ruleset.append(Lowercased(rule))
            else:
                # It's a special object. Add it to the ruleset as-is.
                self.ruleset.append(rule)

    def match(self, *subject):
        """If `subject` matches this ruleset, return the appropriate
        result. Otherwise, return None.
        """
        # Create parallel lists of the subject and the things it has to
        # match.
        must_match = list(self.ruleset)
        remaining_subject = list(subject)

        # Consume tokens from both lists until we've confirmed no
        # match or there is nothing left to match.
        match_so_far = True
        while match_so_far and must_match:
            match_so_far, must_match, remaining_subject = self._consume(
                must_match, remaining_subject
            )

        if match_so_far:
            # Everything that had to match, did.
            self.caught.append(subject)
            return self.result

        # Something that had to match, didn't.
        return None

    def _consume(self, rules, subject):
        """The first token (and possibly more) of the rules must match the
        first token (and possibly more) of the subject.

        All matched rule and subject tokens are consumed.

        :return: A 3-tuple (could_match, new_rules, new_subject)

        could_match is a boolean that is False if we now know that the
        subject does not match the rule, and True if it might still
        match the rule.

        new_rules contains the tokens in the ruleset that have yet to
        be activated.

        new_subject contains the tokens in the subject that have yet
        to be checked.
        """
        if not rules:
            # An empty ruleset matches everything.
            return True, rules, subject

        if not subject and rules != [anything]:
            # Apart from [anything], no non-empty ruleset matches an
            # empty subject.
            return False, rules, subject

        # Figure out which rule we'll be applying. We won't need it
        # again, so we can remove it from the ruleset.
        rule_token = rules.pop(0)
        if rule_token == anything:
            # This is the complicated one.

            if not rules:
                # If the final rule is 'anything', then that's redundant,
                # but we can declare success and stop.
                return True, rules, subject

            # At this point we know that 'anything' is followed by some
            # other rule token.
            next_rule = rules.pop(0)

            # We can consume as many subject tokens as necessary, but
            # eventually a subject token must match this subsequent
            # rule token.
            while subject:
                subject_token = subject.pop(0)
                submatch, ignore1, ignore2 = self._consume(
                    [next_rule], [subject_token]
                )
                if submatch:
                    # We had to remove some number of subject tokens,
                    # but we found one that matches the next rule.
                    return True, rules, subject
                else:
                    # That token didn't match, but maybe the next one will.
                    pass

            # We went through the entire remaining subject and didn't
            # find a match for the rule token that follows 'anything'.
            return False, rules, subject

        # We're comparing two individual tokens.
        subject_token = subject.pop(0)
        if isinstance(rule_token, CustomMatchToken):
            match = rule_token.matches(subject_token)
        elif rule_token == nonfiction:
            # This is too complex to be a CustomMatchToken because
            # we may be modifying the subject token list.
            match = subject_token not in (
                'juvenile fiction', 'young adult fiction', 'fiction'
            )
            if match and subject_token not in (
                    'juvenile nonfiction', 'young adult nonfiction'
            ):
                # The implicit top-level lane is 'nonfiction',
                # which means we popped a token like 'History' that
                # needs to go back on the stack.
                subject.insert(0, subject_token)
        else:
            # The strings must match exactly.
            match = rule_token == subject_token
        return match, rules, subject


def m(result, *ruleset):
    """Alias for the MatchingRule constructor with a short name."""
    return MatchingRule(result, *ruleset)


class BISACClassifier(Classifier):
    """Handle real, genuine, according-to-Hoyle BISAC classifications.

    Subclasses of this method can use the same basic classification logic
    to classify classifications that are based on BISAC but have cosmetic
    differences.

    First, a BISAC code is mapped to its human-readable name.

    Second, the name is split into parts (e.g. ["Fiction", "War &
    Military"]).

    To determine fiction status, audience, target age, or genre, the
    list of name parts is compared against each of a list of matching
    rules.
    """

    # Map identifiers to human-readable names.
    NAMES = dict(
        [i.strip() for i in l]
        for l in csv.reader(open(os.path.join(resource_dir, "bisac.csv")))
    )

    # Indicates that even though this rule doesn't match a subject, no
    # further rules in the same category should be run on it, because they
    # will lead to inaccurate information.
    stop = object()

    # If none of these rules match, a lane's fiction status depends on the
    # genre assigned to it.
    FICTION = [
        m(True, "Fiction"),
        m(True, "Juvenile Fiction"),
        m(False, "Juvenile Nonfiction"),
        m(True, "Young Adult Fiction"),
        m(False, "Young Adult Nonfiction"),
        m(False, anything, "Essays"),
        m(False, anything, "Letters"),
        m(True, "Literary Collections"),
        m(stop, "Humor"),
        m(stop, "Drama"),
        m(stop, "Poetry"),
        m(False, anything),
    ]

    # In BISAC, juvenile fiction and YA fiction are kept in separate
    # spaces. Nearly everything outside that space can be presumed to
    # have AUDIENCE_ADULT.
    AUDIENCE = [
        m(Classifier.AUDIENCE_CHILDREN, "Bibles", anything, "Children"),
        m(Classifier.AUDIENCE_CHILDREN, juvenile, anything),
        m(Classifier.AUDIENCE_YOUNG_ADULT, ya, anything),
        m(Classifier.AUDIENCE_YOUNG_ADULT, "Bibles", anything, "Youth & Teen"),
        m(Classifier.AUDIENCE_ADULTS_ONLY, anything, "Erotica"),
        m(Classifier.AUDIENCE_ADULTS_ONLY, "Humor", "Topic", "Adult"),
        m(Classifier.AUDIENCE_ADULT, anything),
    ]

    TARGET_AGE = [
        m((0,4), juvenile, anything, "Readers", "Beginner") ,
        m((5,7), juvenile, anything, "Readers", "Intermediate"),
        m((5,7), juvenile, anything, "Early Readers"),
        m((8,13), juvenile, anything, "Chapter Books")
    ]

    GENRE = [

        # Put all erotica in Erotica, to keep the other lanes at
        # "Adult" level or lower.
        m(Erotica, anything, 'Erotica'),

        # Put all non-erotica comics into the same bucket, regardless
        # of their content.
        m(Comics_Graphic_Novels, 'Comics & Graphic Novels'),
        m(Comics_Graphic_Novels, nonfiction, 'Comics & Graphic Novels'),
        m(Comics_Graphic_Novels, fiction, 'Comics & Graphic Novels'),

        # "Literary Criticism / Foo" implies Literary Criticism, not Foo.
	m(Literary_Criticism, anything, literary_criticism),

        # "Fiction / Christian / Foo" implies Religious Fiction
        # more strongly than it implies Foo.
        m(Religious_Fiction, fiction, anything, 'Christian'),

        # "Fiction / Foo / Short Stories" implies Short Stories more
        # strongly than it implies Foo. This assumes that a short
        # story collection within a genre will also be classified
        # separately under that genre. This could definitely be
        # improved but would require a Subject to map to multiple
        # Genres.
        m(Short_Stories, fiction, anything, RE('^Anthologies')),
        m(Short_Stories, fiction, anything, RE('^Short Stories')),
        m(Short_Stories, 'Literary Collections'),
        m(Short_Stories, fiction, anything, 'Collections & Anthologies'),

        # Classify top-level fiction categories into fiction genres.
        #
        # First, handle large overarching genres that have subgenres
        # and adjacent genres.
        #

        # Fantasy
        m(Epic_Fantasy, fiction, 'Fantasy', 'Epic'),
        m(Historical_Fantasy, fiction, 'Fantasy', 'Historical'),
        m(Urban_Fantasy, fiction, 'Fantasy', 'Urban'),
        m(Fantasy, fiction, 'Fantasy'),
        m(Fantasy, fiction, 'Romance', 'Fantasy'),
        m(Fantasy, fiction, 'Sagas'),

        # Mystery
        # n.b. no BISAC for Paranormal_Mystery
        m(Crime_Detective_Stories, fiction, 'Mystery & Detective', 'Private Investigators'),
        m(Crime_Detective_Stories, fiction, 'Crime'),
        m(Crime_Detective_Stories, fiction, 'Thrillers', 'Crime'),
        m(Hard_Boiled_Mystery, fiction, 'Mystery & Detective', 'Hard-Boiled'),
        m(Police_Procedural, fiction, 'Mystery & Detective', 'Police Procedural'),
        m(Cozy_Mystery, fiction, 'Mystery & Detective', 'Cozy'),
        m(Historical_Mystery, fiction, 'Mystery & Detective', 'Historical'),
        m(Women_Detectives, fiction, 'Mystery & Detective', 'Women Sleuths'),
        m(Mystery, fiction, anything, 'Mystery & Detective'),

        # Horror
        m(Ghost_Stories, fiction, 'Ghost'),
        m(Occult_Horror, fiction, 'Occult & Supernatural'),
        m(Gothic_Horror, fiction, 'Gothic'),
        m(Horror, fiction, 'Horror'),

        # Romance
        # n.b. no BISAC for Gothic Romance
        m(Contemporary_Romance, fiction, 'Romance', 'Contemporary'),
        m(Historical_Romance, fiction, 'Romance', 'Historical'),
        m(Paranormal_Romance, fiction, 'Romance', 'Paranormal'),
        m(Western_Romance, fiction, 'Romance', 'Western'),
        m(Romantic_Suspense, fiction, 'Romance', 'Suspense'),
        m(Romantic_SF, fiction, 'Romance', 'Time Travel'),
        m(Romantic_SF, fiction, 'Romance', 'Science Fiction'),
        m(Romance, fiction, 'Romance'),

        # Science fiction
        # n.b. no BISAC for Cyberpunk
        m(Dystopian_SF, fiction, 'Dystopian'),
        m(Space_Opera, fiction, 'Science Fiction', 'Space Opera'),
        m(Military_SF, fiction, 'Science Fiction', 'Military'),
        m(Alternative_History, fiction, 'Alternative History'),
        # Juvenile steampunk is classified directly beneath 'fiction'.
        m(Steampunk, fiction, anything, 'Steampunk'),
        m(Science_Fiction, fiction, 'Science Fiction'),

        # Thrillers
        # n.b. no BISAC for Supernatural_Thriller
        m(Historical_Thriller, fiction, 'Thrillers', 'Historical'),
        m(Espionage, fiction, 'Thrillers', 'Espionage'),
        m(Medical_Thriller, fiction, 'Thrillers', 'Medical'),
        m(Political_Thriller, fiction, 'Thrillers', 'Political'),
        m(Legal_Thriller, fiction, 'Thrillers', 'Legal'),
        m(Technothriller, fiction, 'Thrillers', 'Technological'),
        m(Military_Thriller, fiction, 'Thrillers', 'Military'),
        m(Suspense_Thriller, fiction, 'Thrillers'),

        # Then handle the less complicated genres of fiction.
        m(Adventure, fiction, 'Action & Adventure'),
        m(Adventure, fiction, 'Sea Stories'),
        m(Adventure, fiction, 'War & Military'),
        m(Classics, fiction, 'Classics'),
        m(Folklore, fiction, 'Fairy Tales, Folk Tales, Legends & Mythology'),
        m(Historical_Fiction, anything, 'Historical'),
        m(Humorous_Fiction, fiction, 'Humorous'),
        m(Humorous_Fiction, fiction, 'Satire'),
        m(Literary_Fiction, fiction, 'Literary'),
        m(LGBTQ_Fiction, fiction, 'Gay'),
        m(LGBTQ_Fiction, fiction, 'Lesbian'),
        m(LGBTQ_Fiction, fiction, 'Gay & Lesbian'),
        m(Religious_Fiction, fiction, 'Religious'),
        m(Religious_Fiction, fiction, 'Jewish'),
        m(Religious_Fiction, fiction, 'Visionary & Metaphysical'),
        m(Womens_Fiction, fiction, anything, 'Contemporary Women'),
        m(Westerns, fiction, 'Westerns'),

        # n.b. BISAC "Fiction / Urban" is distinct from "Fiction /
        # African-American / Urban", and does not map to any of our
        # genres.
        m(Urban_Fiction, fiction, 'African American', 'Urban'),

        # BISAC classifies these genres at the top level, which we
        # treat as 'nonfiction', but we classify them as fiction. It
        # doesn't matter because they're neither, really.
        m(Drama, nonfiction, 'Drama'),
	m(Poetry, nonfiction, 'Poetry'),

        # Now on to nonfiction.

        # Classify top-level nonfiction categories into fiction genres.
        #
        # First, handle large overarching genres that have subgenres
        # and adjacent genres.
        #

        # Art & Design
        m(Architecture, nonfiction, 'Architecture'),
        m(Art_Criticism_Theory, nonfiction, 'Art', 'Criticism & Theory'),
        m(Art_History, nonfiction, 'Art', 'History'),
        m(Fashion, nonfiction, 'Design', 'Fashion'),
        m(Design, nonfiction, 'Design'),
        m(Art_Design, nonfiction, 'Art'),
        m(Photography, nonfiction, 'Photography'),

        # Personal Finance & Business
        m(Business, nonfiction, 'Business & Economics', RE('^Business.*')),
        m(Business, nonfiction, 'Business & Economics', 'Accounting'),
        m(Economics, nonfiction, 'Business & Economics', 'Economics'),

        m(Economics, nonfiction, 'Business & Economics', 'Environmental Economics'),
        m(Economics, nonfiction, 'Business & Economics', RE('^Econo.*')),
        m(Management_Leadership, nonfiction, 'Business & Economics', 'Management'),
        m(Management_Leadership, nonfiction, 'Business & Economics', 'Management Science'),
        m(Management_Leadership, nonfiction, 'Business & Economics', 'Leadership'),
        m(Personal_Finance_Investing, nonfiction, 'Business & Economics', 'Personal Finance'),
        m(Personal_Finance_Investing, nonfiction, 'Business & Economics', 'Personal Success'),
        m(Personal_Finance_Investing, nonfiction, 'Business & Economics', 'Investments & Securities'),
        m(Real_Estate, nonfiction, 'Business & Economics', 'Real Estate'),
        m(Personal_Finance_Business, nonfiction, 'Business & Economics'),

        # Parenting & Family
        m(Parenting, nonfiction, 'Family & Relationships', 'Parenting'),
        m(Family_Relationships, nonfiction, 'Family & Relationships'),

        # Food & Health
        m(Bartending_Cocktails, nonfiction, 'Cooking', 'Beverages'),
        m(Health_Diet, nonfiction, 'Cooking', 'Health & Healing'),
        m(Health_Diet, nonfiction, 'Health & Fitness'),
        m(Vegetarian_Vegan, nonfiction, 'Cooking', 'Vegetarian & Vegan'),
        m(Cooking, nonfiction, 'Cooking'),

        # History
        m(African_History, nonfiction, 'History', 'Africa'),
        m(Ancient_History, nonfiction, 'History', 'Ancient'),
        m(Asian_History, nonfiction, 'History', 'Asia'),
        m(Civil_War_History, nonfiction, 'History', 'United States', RE('^Civil War')),
        m(European_History, nonfiction, 'History', 'Europe'),
        m(Latin_American_History, nonfiction, 'History', 'Latin America'),
        m(Medieval_History, nonfiction, 'History', 'Medieval'),
        m(Military_History, nonfiction, 'History', 'Military'),
        m(Renaissance_Early_Modern_History, nonfiction, 'History', 'Renaissance'),
        m(Renaissance_Early_Modern_History, nonfiction, 'History', 'Modern', RE('^1[678]th Century')),
        m(Modern_History, nonfiction, 'History', 'Modern'),
        m(United_States_History, nonfiction, 'History', 'Native American'),
        m(United_States_History, nonfiction, 'History', 'United States'),
        m(World_History, nonfiction, 'History', 'World'),
        m(World_History, nonfiction, 'History', 'Civilization'),
        m(History, nonfiction, 'History'),

        # Hobbies & Home
        m(Antiques_Collectibles, nonfiction, 'Antiques & Collectibles'),
        m(Crafts_Hobbies, nonfiction, 'Crafts & Hobbies'),
        m(Gardening, nonfiction, 'Gardening'),
        m(Games, nonfiction, 'Games'),
        m(House_Home, nonfiction, 'House & Home'),
        m(Pets, nonfiction, 'Pets'),

        # Entertainment
        m(Film_TV, nonfiction, 'Performing Arts', 'Film & Video'),
        m(Film_TV, nonfiction, 'Performing Arts', 'Television'),
        m(Music, nonfiction, 'Music'),
        m(Performing_Arts, nonfiction, 'Performing Arts'),

        # Reference & Study Aids
        m(Dictionaries, nonfiction, 'Reference', 'Dictionaries'),
        m(Foreign_Language_Study, nonfiction, 'Foreign Language Study'),
        m(Law, nonfiction, 'Law'),
        m(Study_Aids, nonfiction, 'Study Aids'),
        m(Reference_Study_Aids, nonfiction, 'Reference'),
        m(Reference_Study_Aids, nonfiction, 'Language Arts & Disciplines'),

        # Religion & Spirituality
        m(Body_Mind_Spirit, nonfiction, body_mind_spirit),
        m(Buddhism, nonfiction, 'Religion', 'Buddhism'),
        m(Christianity, nonfiction, 'Religion', RE('^Biblical')),
        m(Christianity, nonfiction, 'Religion', RE('^Christian')),
        m(Christianity, nonfiction, 'Bibles'),
        m(Hinduism, nonfiction, 'Religion', 'Hinduism'),
        m(Islam, nonfiction, 'Religion', 'Islam'),
        m(Judaism, nonfiction, 'Religion', 'Judaism'),
        m(Religion_Spirituality, nonfiction, 'Religion'),

        # Science & Technology
        m(Computers, nonfiction, 'Computers'),
        m(Mathematics, nonfiction, 'Mathematics'),
        m(Medical, nonfiction, 'Medical'),
        m(Nature, nonfiction, 'Nature'),
        m(Psychology, nonfiction, psychology),
	m(Political_Science, nonfiction, 'Social Science', 'Politics & Government'),
        m(Social_Sciences, nonfiction, 'Social Science'),
        m(Technology, nonfiction, technology),
        m(Technology, nonfiction, 'Transportation'),
        m(Science, nonfiction, 'Science'),

        # Then handle the less complicated genres of nonfiction.
        # n.b. no BISAC for Periodicals.
        # n.b. no BISAC for Humorous Nonfiction per se.
        m(Music, nonfiction, 'Biography & Autobiography', 'Composers & Musicians'),
        m(Entertainment, nonfiction, 'Biography & Autobiography', 'Entertainment & Performing Arts'),
	m(Biography_Memoir, nonfiction, 'Biography & Autobiography'),
        m(Education, nonfiction, "Education"),
	m(Philosophy, nonfiction, 'Philosophy'),
	m(Political_Science, nonfiction, 'Political Science'),
	m(Self_Help, nonfiction, 'Self-Help'),
	m(Sports, nonfiction, 'Sports & Recreation'),
	m(Travel, nonfiction, 'Travel'),
	m(True_Crime, nonfiction, 'True Crime'),

        # Handle cases where Juvenile/YA uses different terms than
        # would be used for the same books for adults.
        m(Business, nonfiction, 'Careers'),
        m(Christianity, nonfiction, "Religious", "Christian"),
        m(Cooking, nonfiction, "Cooking & Food"),
        m(Education, nonfiction, "School & Education"),
        m(Family_Relationships, nonfiction, "Family"),
        m(Fantasy, fiction, "Fantasy & Magic"),
        m(Ghost_Stories, fiction, 'Ghost Stories'),
        m(Fantasy, fiction, 'Magical Realism'),
        m(Fantasy, fiction, 'Mermaids'),
        m(Fashion, nonfiction, 'Fashion'),
        m(Folklore, fiction, "Fairy Tales & Folklore"),
        m(Folklore, fiction, "Legends, Myths, Fables"),
        m(Games, nonfiction, "Games & Activities"),
        m(Health_Diet, nonfiction, "Health & Daily Living"),
        m(Horror, fiction, "Horror & Ghost Stories"),
        m(Horror, fiction, "Monsters"),
        m(Horror, fiction, "Paranormal"),
        m(Horror, fiction, 'Paranormal, Occult & Supernatural'),
        m(Horror, fiction, 'Vampires'),
        m(Horror, fiction, 'Werewolves & Shifters'),
        m(Horror, fiction, 'Zombies'),
        m(Humorous_Fiction, fiction, "Humorous Stories"),
        m(Humorous_Nonfiction, "Young Adult Nonfiction", "Humor"),
        m(LGBTQ_Fiction, fiction, 'LGBT'),
        m(Law, nonfiction, "Law & Crime"),
        m(Mystery, fiction, "Mysteries & Detective Stories"),
        m(Nature, nonfiction, "Animals"),
        m(Personal_Finance_Investing, nonfiction, 'Personal Finance'),
        m(Poetry, fiction, "Nursery Rhymes"),
        m(Poetry, fiction, "Stories in Verse"),
        m(Poetry, fiction, 'Novels in Verse'),
        m(Poetry, fiction, 'Poetry'),
        m(Reference_Study_Aids, nonfiction, "Language Arts"),
        m(Romance, fiction, "Love & Romance"),
        m(Science_Fiction, fiction, "Robots"),
        m(Science_Fiction, fiction, "Time Travel"),
        m(Social_Sciences, nonfiction, "Media Studies"),
        m(Suspense_Thriller, fiction, 'Superheroes'),
        m(Suspense_Thriller, fiction, 'Thrillers & Suspense'),

        # Most of the subcategories of 'Science & Nature' go into Nature,
        # but these go into Science.
        m(Science, nonfiction, 'Science & Nature', 'Discoveries'),
        m(Science, nonfiction, 'Science & Nature', 'Experiments & Projects'),
        m(Science, nonfiction, 'Science & Nature', 'History of Science'),
        m(Science, nonfiction, 'Science & Nature', 'Physics'),
        m(Science, nonfiction, 'Science & Nature', 'Weights & Measures'),
        m(Science, nonfiction, 'Science & Nature', 'General'),

        # Any other subcategory of 'Science & Nature' goes under Nature
        m(Nature, nonfiction, 'Science & Nature', something),

        # Life Strategies is juvenile/YA-specific, and contains both
        # fiction and nonfiction. It's called "Social Issues" for
        # juvenile fiction/nonfiction, and "Social Topics" for YA
        # nonfiction. "Social Themes" in YA fiction is _not_
        # classified as Life Strategies.
        m(Life_Strategies, fiction, "social issues"),
        m(Life_Strategies, nonfiction, "social issues"),
        m(Life_Strategies, nonfiction, social_topics),
    ]

    @classmethod
    def is_fiction(cls, identifier, name):
        for ruleset in cls.FICTION:
            fiction = ruleset.match(*name)
            if fiction is cls.stop:
                return None
            if fiction is not None:
                return fiction
        keyword = "/".join(name)
        return KeywordBasedClassifier.is_fiction(identifier, keyword)

    @classmethod
    def audience(cls, identifier, name):
        for ruleset in cls.AUDIENCE:
            audience = ruleset.match(*name)
            if audience is cls.stop:
                return None
            if audience is not None:
                return audience
        keyword = "/".join(name)
        return KeywordBasedClassifier.audience(identifier, keyword)

    @classmethod
    def target_age(cls, identifier, name):
        for ruleset in cls.TARGET_AGE:
            target_age = ruleset.match(*name)
            if target_age is cls.stop:
                return None
            if target_age is not None:
                return target_age

        # If all else fails, try the keyword-based classifier.
        keyword = "/".join(name)
        return KeywordBasedClassifier.target_age(identifier, keyword)

    @classmethod
    def genre(cls, identifier, name, fiction, audience):
        for ruleset in cls.GENRE:
            genre = ruleset.match(*name)
            if genre is cls.stop:
                return None
            if genre is not None:
                return genre

        # If all else fails, try a keyword-based classifier.
        keyword = "/".join(name)
        return KeywordBasedClassifier.genre(
            identifier, keyword, fiction, audience
        )

    # A BISAC name copied from the BISAC website may end with this
    # human-readable note, which is not part of the official name.
    see_also = re.compile('\(see also .*')

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier
        if identifier.startswith('FB'):
            identifier = identifier[2:]
        if identifier in cls.NAMES:
            # We know the canonical name for this BISAC identifier,
            # and we are better equipped to classify the canonical
            # names, so use the canonical name in preference to
            # whatever name the distributor provided.
            return (identifier, cls.NAMES[identifier])
        return identifier

    @classmethod
    def scrub_name(cls, name):
        """Split the name into a list of lowercase keywords."""

        # All of our comparisons are case-insensitive.
        name = Lowercased(name)

        # Take corrective action to finame a number of common problems
        # seen in the wild.
        #

        # A comma may have been replaced with a space.
        name = name.replace("  ", ", ")

        # The name may be enclosed in an extra set of quotes.
        for quote in ("'\""):
            if name.startswith(quote):
                name = name[1:]
            if name.endswith(quote):
                name = name[:-1]

        # The name may end with an extraneous marker character or
        # (if it was copied from the BISAC website) an asterisk.
        for separator in '|/*':
            if name.endswith(separator):
                name = name[:-1]

        # A name copied from the BISAC website may end with a
        # human-readable cross-reference.
        name = cls.see_also.sub('', name)

        # The canonical separator character is a slash, but a pipe
        # has also been used.
        for separator in '|/':
            if separator in name:
                parts = [name.strip() for name in name.split(separator)
                         if name.strip()]
                break
        else:
            parts = [name]
        return parts

Classifier.classifiers[Classifier.BISAC] = BISACClassifier
