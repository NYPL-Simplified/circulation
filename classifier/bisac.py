# encoding: utf-8
import csv
import os
import re
import string
from . import *

# Special tokens used in matching rules.
nonfiction = object()
fiction = object()
juvenile = object()
anything = object()

special_variables = { nonfiction : "nonfiction",
                      fiction : "fiction",
                      juvenile : "juvenile"}

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

            if isinstance(rule, basestring):
                # It's a string. We do case-insensitive comparisons,
                # so lowercase it.
                self.ruleset.append(rule.lower())
            elif hasattr(rule, 'pattern'):
                # It's a regular expression. Recompile it to be
                # case-insensitive.
                self.ruleset.append(re.compile(rule.pattern, re.I))
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
                elif not subject:
                    # We are out of subject tokens, and we never found a
                    # candidate for the next rule token.
                    return False, rules, subject
                else:
                    # That token didn't match, but maybe the next one will.
                    pass

            # We went through the entire remaining subject and didn't
            # find a match for the rule token that follows 'anything'.
            return False, must_match, subject

        # We're comparing two individual tokens.
        subject_token = subject.pop(0)
        if rule_token == juvenile:
            match = subject_token in ('juvenile fiction', 'juvenile nonfiction')
        elif rule_token == nonfiction:
            match = subject_token not in ('juvenile fiction', 'fiction')
            if match and subject_token != 'juvenile nonfiction':
                # The implicit top-level lane is 'nonfiction', 
                # which means we popped a token like 'History' that
                # needs to go back on the stack.
                subject.insert(0, subject_token)
        elif rule_token == fiction:
            match = subject_token in ('juvenile fiction', 'fiction')
        elif isinstance(rule_token, basestring):
            # The strings must match exactly.
            match = rule_token == subject_token
        else:
            # The regular expression must match the subject.
            match = rule_token.search(subject_token)
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
        map(string.strip, x)
        for x in csv.reader(open(os.path.join(resource_dir, "bisac.csv")))
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
        m(stop, "Humor"),
        m(False, anything),
    ]

    # In BISAC, juvenile fiction is kept in a separate space. Nearly
    # everything outside that space can be presumed to have
    # AUDIENCE_ADULT.
    AUDIENCE = [
        m(Classifier.AUDIENCE_CHILDREN, juvenile, anything, "Readers"),
        m(Classifier.AUDIENCE_CHILDREN, juvenile, anything, "Early Readers"),
        m(Classifier.AUDIENCE_CHILDREN, "Bibles", anything, "Children"),
        m(Classifier.AUDIENCE_YOUNG_ADULT, juvenile),
        m(Classifier.AUDIENCE_YOUNG_ADULT, "Bibles", anything, "Youth & Teen"),
        m(Classifier.AUDIENCE_ADULTS_ONLY, anything, "Erotica"),
        m(Classifier.AUDIENCE_ADULTS_ONLY, "Humor", "Topic", "Adult"),
        m(Classifier.AUDIENCE_ADULT, re.compile(".*")),
    ]

    TARGET_AGE = [
        # TODO: need to verify the first two age ranges.
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
	m(Literary_Criticism, anything, 'Literary Criticism'),

        # "Fiction / Christian / Foo" implies Religious Fiction
        # more strongly than it implies Foo.
        m(Religious_Fiction, fiction, anything, 'Christian'),

        # "Fiction / Foo / Short Stories" implies Short Stories more
        # strongly than it implies Foo. This assumes that a short
        # story collection within a genre will also be classified
        # separately under that genre. This could definitely be
        # improved but would require a Subject to map to multiple
        # Genres.
        m(Short_Stories, fiction, anything, re.compile('^Anthologies')),
        m(Short_Stories, fiction, anything, re.compile('^Short Stories')),
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

        # BISAC classifies these genres as nonfiction but we classify
        # them as fiction. It doesn't matter because they're neither,
        # really.
        m(Drama, nonfiction, 'Drama'),
	m(Poetry, nonfiction, 'Poetry'),

        # Now on to nonfiction.

        # Classify top-level fiction categories into fiction genres.
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
        m(Business, nonfiction, 'Business & Economics', re.compile('^Business.*')),
        m(Business, nonfiction, 'Business & Economics', 'Accounting'),
        m(Economics, nonfiction, 'Business & Economics', 'Economics'),

        m(Economics, nonfiction, 'Business & Economics', 'Environmental Economics'),
        m(Economics, nonfiction, 'Business & Economics', re.compile('^Econo.*')),
        m(Management_Leadership, nonfiction, 'Business & Economics', 'Management'),
        m(Management_Leadership, nonfiction, 'Business & Economics', 'Management Science'),
        m(Management_Leadership, nonfiction, 'Business & Economics', 'Leadership'),
        m(Personal_Finance_Investing, nonfiction, 'Business & Economics', 'Personal Finance'),
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
        m(Civil_War_History, nonfiction, 'History', 'United States', re.compile('^Civil War')),
        m(European_History, nonfiction, 'History', 'Europe'),
        m(Latin_American_History, nonfiction, 'History', 'Latin America'),
        m(Medieval_History, nonfiction, 'History', 'Medieval'),
        m(Military_History, nonfiction, 'History', 'Military'),
        m(Renaissance_Early_Modern_History, nonfiction, 'History', 'Renaissance'),
        m(Renaissance_Early_Modern_History, nonfiction, 'History', 'Modern', re.compile('^1[678]th Century')),
        m(Modern_History, nonfiction, 'History', 'Modern'),
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
        # m(Reference_Study_Aids, nonfiction, 'Linguistics'),
        m(Reference_Study_Aids, nonfiction, 'Language Arts & Disciplines'),

        # Religion & Spirituality
        m(Body_Mind_Spirit, nonfiction, 'Body, Mind & Spirit'),
        m(Buddhism, nonfiction, 'Religion', 'Buddhism'),
        m(Christianity, nonfiction, 'Religion', re.compile('^Biblical')),
        m(Christianity, nonfiction, 'Religion', re.compile('^Christian')),
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
        m(Psychology, nonfiction, 'Psychology'),
	m(Political_Science, nonfiction, 'Social Science', 'Politics & Government'),
        m(Social_Sciences, nonfiction, 'Social Science'),
        m(Technology, nonfiction, 'Technology'),
        m(Technology, nonfiction, 'Technology & Engineering'),
        m(Technology, nonfiction, 'Transportation'),
        m(Science, nonfiction, 'Science'),

        # Then handle the less complicated genres of nonfiction.
        # n.b. no BISAC for Periodicals.
        # n.b. no BISAC for Humorous Nonfiction per se.
	m(Biography_Memoir, nonfiction, 'Biography & Autobiography'),
        m(Education, nonfiction, "Education"),
	m(Philosophy, nonfiction, 'Philosophy'),
	m(Political_Science, nonfiction, 'Political Science'),
	m(Self_Help, nonfiction, 'Self-Help'),
	m(Sports, nonfiction, 'Sports & Recreation'),
	m(Travel, nonfiction, 'Travel'),
	m(True_Crime, nonfiction, 'True Crime'),

        # Finally, handle cases where Juvenile Fiction/Nonfiction uses
        # different terms than would be used for the same books for
        # adults.
        m(Christianity, nonfiction, "Religious", "Christian"),
        m(Cooking, nonfiction, "Cooking & Food"),
        m(Education, nonfiction, "School & Education"),
        m(Family_Relationships, nonfiction, "Family"),
        m(Fantasy, fiction, "Fantasy & Magic"),
        m(Folklore, fiction, "Fairy Tales & Folklore"),
        m(Folklore, fiction, "Legends, Myths, Fables"),
        m(Games, nonfiction, "Games & Activities"),
        m(Health_Diet, nonfiction, "Health & Daily Living"),
        m(Horror, fiction, "Horror & Ghost Stories"),
        m(Horror, fiction, "Monsters"),
        m(Horror, fiction, "Paranormal"),
        m(Humorous_Fiction, fiction, "Humorous Stories"),
        m(Humorous_Nonfiction, nonfiction, "Humor"),
        m(Law, nonfiction, "Law & Crime"),
        m(Literary_Criticism, nonfiction, "Literary Criticism & Collections"),
        m(Mystery, fiction, "Mysteries & Detective Stories"),
        m(Nature, nonfiction, "Animals"),
        m(Poetry, fiction, "Nursery Rhymes"),
        m(Poetry, fiction, "Stories in Verse"),
        m(Reference_Study_Aids, nonfiction, "Language Arts"),
        m(Romance, fiction, "Love & Romance"),
        m(Science_Fiction, fiction, "Robots"),
        m(Social_Sciences, nonfiction, "Media Studies"),

        # Most of the subcategories of 'Science & Nature' go into Nature,
        # but these go into Science.
        m(Science, nonfiction, 'Science & Nature', 'Discoveries'),
        m(Science, nonfiction, 'Science & Nature', 'Experiments & Projects'),
        m(Science, nonfiction, 'Science & Nature', 'History of Science'),
        m(Science, nonfiction, 'Science & Nature', 'Physics'),
        m(Science, nonfiction, 'Science & Nature', 'Weights & Measures'),
        m(Science, nonfiction, 'Science & Nature', 'General'),
        m(Nature, nonfiction, 'Science & Nature'),

        # Life Strategies is juvenile-specific, and contains both fiction
        # and nonfiction.
        m(Life_Strategies, fiction, "social issues"),
        m(Life_Strategies, nonfiction, "social issues"),
    ]

    @classmethod
    def is_fiction(cls, identifier, name):
        for ruleset in cls.FICTION:
            fiction = ruleset.match(*name)
            if fiction is cls.stop:
                return None
            if fiction is not None:
                return fiction

    @classmethod
    def audience(cls, identifier, name):
        for ruleset in cls.AUDIENCE:
            audience = ruleset.match(*name)
            if audience is cls.stop:
                return None
            if audience is not None:
                return audience


    @classmethod
    def target_age(cls, identifier, name):
        for ruleset in cls.TARGET_AGE:
            target_age = ruleset.match(*name)
            if target_age is cls.stop:
                return None
            if target_age is not None:
                return target_age

    @classmethod
    def genre(cls, identifier, name, fiction, audience):
        for ruleset in cls.GENRE:
            genre = ruleset.match(*name)
            if genre is cls.stop:
                return None
            if genre is not None:
                return genre

    @classmethod
    def scrub_name(cls, name):
        """Split the name into a list of lowercase keywords."""
        return [x.strip().lower() for x in name.split('/')]


