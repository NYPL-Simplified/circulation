from . import *
from .keyword import KeywordBasedClassifier

class RBDigitalAudienceClassifier(FreeformAudienceClassifier):

    @classmethod
    def target_age(cls, identifier, name):
        # RBdigital uses 'beginning reader' to refer to books
        # suitable from birth to age 8, rather than (as is more common)
        # ages 5 to 8.
        #
        # Rather than covering the entire early lifespan of a child,
        # the normally vague 'childrens' is used here to cover the
        # time in between 'beginning reader' and 'young adult'
        if identifier == 'beginning reader':
            return cls.range_tuple(0,8)
        elif identifier == 'childrens':
            return cls.range_tuple(9, 13)
        return FreeformAudienceClassifier.target_age(identifier, name)

class RBDigitalSubjectClassifier(KeywordBasedClassifier):

    # We have subgenres for fiction in these categories but not for
    # nonfiction in these categories (thus the None mapping in
    # 'genres' below).
    fiction_genres = {
        'lgbt interest' : LGBTQ_Fiction,
    }

    genres = {
        # This isn't true in general but because RBdigital is heavy on
        # audio content, 'arts and entertainment' is more likely to be
        # entertainment (e.g. music) than arts.
        'arts entertainment' : Entertainment,
        'business economics' : Business,
        'comics graphic novels' : Comics_Graphic_Novels,
        'home garden' : Hobbies_Home,
        'humorous fiction' : Humorous_Fiction,
        # Determined by looking at a sample collection.
        'humor' : Humorous_Nonfiction,

        # We don't check this in KeywordBasedClassifier to avoid
        # confusion with e.g. 'western civ'.
        'western' : Westerns,

        # If we go to this point we know it's not fiction so we have
        # nothing to say about the genre -- it could be anything.
        'lgbt interest' : None,
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier.replace('-', ' ')

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None, **kwargs):
        if fiction and identifier in cls.fiction_genres:
            return cls.fiction_genres[identifier]
        if identifier in cls.genres:
            return cls.genres[identifier]
        return KeywordBasedClassifier.genre(
            identifier, name, fiction, audience, **kwargs
        )

    @classmethod
    def audience(cls, identifier, name):
        """The two subjects here that imply an audience, juvenile-fiction and
        juvenile-nonfiction, could be either Childrens or YA, and
        RBdigital always provides a more specific audience
        classification, so we don't try to derive audience information
        from an RBDigital Subject.
        """
        if identifier == 'erotica':
            return Classifier.AUDIENCE_ADULTS_ONLY
        return None

    @classmethod
    def target_age(cls, identifier, name):
        """RBdigital's audience classification is much more useful for this
        purpose, so we don't try to derive target age from an RBDigital
        Subject.
        """
        return None

    @classmethod
    def is_fiction(cls, identifier, name):
        # In general, the fiction status of a RBdigital subject
        # is the default fiction status of its genre.
        genre = cls.genre(identifier, name)
        if genre and genre.is_fiction is not None:
            return genre.is_fiction
        if identifier.endswith(' fiction'):
            return True
        if identifier.endswith(' nonfiction'):
            return False
        return None

Classifier.classifiers[Classifier.RBDIGITAL_AUDIENCE] = RBDigitalAudienceClassifier
Classifier.classifiers[Classifier.RBDIGITAL] = RBDigitalSubjectClassifier
