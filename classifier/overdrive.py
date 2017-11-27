# encoding: utf-8

from . import *

class OverdriveClassifier(Classifier):

    # Any classification that includes the string "Fiction" will be
    # counted as fiction. This is just the leftovers.
    FICTION = set([
        "Short Stories",
        "Fantasy",
        "Horror",
        "Mystery",
        "Romance",
        "Western",
        "Suspense",
        "Thriller",
        "Science Fiction & Fantasy",
        ])

    NONFICTION = set([
        "Biography & Autobiography",
        "Business",
        "Careers",
        "Computer Technology",
        "Cooking & Food",
        "Family & Relationships",
        "Finance",
        "Health & Fitness",
        "History",
        "Politics",
        "Psychology",
        "Reference",
        "Science",
        "Self Help",
        "Self-Improvement",
        "Sociology",
        "Sports & Recreations",
        "Technology"
        "Travel",
    ])

    GENRES = {
        Antiques_Collectibles : "Antiques",
        Architecture : "Architecture",
        Art : "Art",
        Biography_Memoir : "Biography & Autobiography",
        Business : ["Business", "Marketing & Sales", "Careers"],
        Christianity : "Christian Nonfiction",
        Computers : "Computer Technology",
        Classics : "Classic Literature",
        Cooking : "Cooking & Food",
        Crafts_Hobbies : "Crafts",
        Games : "Games",
        Drama : "Drama",
        Education : "Education",
        Erotica : "Erotic Literature",
        Fantasy : "Fantasy",
        Foreign_Language_Study : "Foreign Language Study",
        Gardening : "Gardening",
        Comics_Graphic_Novels : "Comic and Graphic Books",
        Health_Diet : "Health & Fitness",
        Historical_Fiction : "Historical Fiction",
        History : "History",
        Horror : "Horror",
        House_Home : u"Home Design & Décor",
        Humorous_Fiction : "Humor (Fiction)", 
        Humorous_Nonfiction : "Humor (Nonfiction)",
        Entertainment : "Entertainment",
        Judaism : "Judaica",
        Law : "Law",
        Literary_Criticism : [
            "Literary Criticism", "Criticism", "Literary Anthologies",
            "Language Arts"],
        Management_Leadership : "Management",
        Mathematics : "Mathematics",
        Medical : "Medical",
        Military_History : "Military",
        Music : "Music",
        Mystery : "Mystery",
        Nature : "Nature",
        Body_Mind_Spirit : "New Age",
        Parenting_Family : "Family & Relationships",
        Performing_Arts : "Performing Arts",
        Personal_Finance_Investing : "Finance",
        Pets : "Pets",
        Philosophy : ["Philosophy", "Ethics"],
        Photography : "Photography",
        Poetry : "Poetry",
        Political_Science : ["Politics", "Current Events"],
        Psychology : ["Psychology", "Psychiatry", "Psychiatry & Psychology"],
        Reference_Study_Aids : ["Reference", "Grammar & Language Usage"],
        Religious_Fiction : ["Christian Fiction"],
        Religion_Spirituality : "Religion & Spirituality",
        Romance : "Romance",
        Science : ["Science", "Physics", "Chemistry"],
        Science_Fiction : "Science Fiction",
        # Science_Fiction_Fantasy : "Science Fiction & Fantasy",
        Self_Help : ["Self-Improvement", "Self-Help", "Self Help"],
        Social_Sciences : ["Sociology", "Gender Studies"],
        Sports : "Sports & Recreations",
        Study_Aids : "Study Aids & Workbooks",
        Technology : ["Technology", "Engineering", "Transportation"],
        Suspense_Thriller : ["Suspense", "Thriller"],
        Travel : ["Travel", "Travel Literature"],
        True_Crime : "True Crime",
        Urban_Fiction: ["African American Fiction", "Urban Fiction"],
        Womens_Fiction: "Chick Lit Fiction",
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        if not identifier:
            return identifier
        if identifier.startswith('Foreign Language Study'):
            return 'Foreign Language Study'
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        if (identifier in cls.FICTION
            or "Fiction" in identifier
            or "Literature" in identifier):
            # "Literature" on Overdrive seems to be synonymous with fiction,
            # but not necessarily "Literary Fiction".
            return True
        if (identifier in cls.NONFICTION or 'Nonfiction' in identifier
            or 'Study' in identifier or 'Studies' in identifier):
            return False
        return None

    @classmethod
    def audience(cls, identifier, name):
        if ("Juvenile" in identifier or "Picture Book" in identifier
            or "Beginning Reader" in identifier or "Children's" in identifier):
            return cls.AUDIENCE_CHILDREN
        elif "Young Adult" in identifier:
            return cls.AUDIENCE_YOUNG_ADULT
        elif identifier in ('Fiction', 'Nonfiction'):
            return cls.AUDIENCE_ADULT
        elif identifier == 'Erotic Literature':
            return cls.AUDIENCE_ADULTS_ONLY
        return None

    @classmethod
    def target_age(cls, identifier, name):
        if identifier.startswith('Picture Book'):
            return cls.range_tuple(0, 4)
        elif identifier.startswith('Beginning Reader'):
            return cls.range_tuple(5,8)
        elif 'Young Adult' in identifier:
            # Internally we believe that 'Young Adult' means ages
            # 14-17, but after looking at a large number of Overdrive
            # books classified as 'Young Adult' we think that
            # Overdrive means something closer to 12-17.
            return cls.range_tuple(12, 17)
        return cls.range_tuple(None, None)

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for l, v in cls.GENRES.items():
            if identifier == v or (isinstance(v, list) and identifier in v):
                return l
        return None

Classifier.classifiers[Classifier.OVERDRIVE] = OverdriveClassifier
