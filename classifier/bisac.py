# encoding: utf-8
import csv
from . import (
    resource_dir,
    Classifier,
)

class BISACClassifier(Classifier):
    """Handle real, genuine, according-to-Hoyle BISAC classifications.
    
    Subclasses of this method can use the same basic classification logic
    to classify classifications that are based on BISAC but have cosmetic
    differences.

    First, a BISAC code is mapped to its human-readable name.

    Second, the name is split into parts (e.g. ["Fiction", "War &
    Military"]).

    To determine fiction status, audience, target age, and genre, the
    list of name parts is compared against lists of targets.
    """

    # Map identifiers to human-readable names.
    NAMES = dict(
        x for x in csv.reader(open(os.path.join(resource_dir, "bisac.csv")))
    )
    
    FICTION = [
        (True, "Fiction"),
        (True, "Juvenile Fiction"),
        (False, "Juvenile Nonfiction"),
        (False) # Default
    ])

    AUDIENCE = [
        (AUDIENCE_CHILDREN, juvenile, then, "Readers"),
        (AUDIENCE_CHILDREN, juvenile, then, "Early Readers"),
        (AUDIENCE_CHILDREN, "Bibles", then, "Children"),
        (AUDIENCE_YA, juvenile),
        (AUDIENCE_YA, "Bibles", then, "Youth & Teen"),
        (AUDIENCE_ADULTS_ONLY, any_kind_of, "Erotica")
        (AUDIENCE_ADULTS_ONLY, "Humor", "Topic", "Adult")
        (AUDIENCE_ADULT) # Default
    ]

    TARGET_AGE = [
        # Need to verify the first two.
        ((0,4), juvenile, then, "Readers", "Beginner") ,
        ((5,7), juvenile, then, "Readers", "Intermediate"),
        ((5,7), (juvenile, then, "Early Readers")),
        ((8,13), (juvenile, then, "Chapter Books"))
    ]

    GENRE = [
        # Literary Criticism goes here no matter what's being critiqued.
	(Literary_Criticism, nonfiction, 'Literary Criticism'),

        (Comics, 'Comics & Graphic Novels'), # Needs work

        (Classics, fiction, 'Classics'),
        (Erotica, fiction, 'Erotica'),
        (LGBT_Fiction, fiction, then, "Gay")
        (LGBT_Fiction, fiction, then, "Lesbian")

        # Beyond this point, we are classifying everything underneath
        # top-level nonfiction categories into those categories.

        (Antiques_Collectibles, nonfiction, 'Antiques & Collectibles'),
        (Art_Design, nonfiction, 'Architecture'),
        (Art_Design, nonfiction, 'Art'),
        (Christianity, nonfiction, 'Bibles'),
        (Biography, nonfiction, 'Biography & Autobiography'),
        (Business, nonfiction, 'Business & Economics'),

        (Cooking, nonfiction, 'Cooking'),
	(Computers, nonfiction, 'Computers'),
	(Crafts_Hobbies, nonfiction, 'Crafts & Hobbies'),
	(Art_Design, nonfiction, 'Design'),
	(Drama, nonfiction, 'Drama'),
	(Education, nonfiction, 'Education'),
	(Parenting_Family, nonfiction, 'Family & Relationships'),
	(Foreign_Language_Study, nonfiction, 'Foreign Language Study'),
	(Games, nonfiction, 'Games'),
	(Gardening, nonfiction, 'Gardening'),
	(Health_Diet, nonfiction, 'Health & Fitness'),
	(History, nonfiction, 'History'),
	(House_Home, nonfiction, 'House & Home'),
	(Humor, 'Humor'),
	(Law, nonfiction, 'Law'),
	(Literary_Criticism, nonfiction, 'Language Arts & Disciplines'),
	(???, 'Literary Collections'),
	(Mathematics, nonfiction, 'Mathematics'),
	(Medical, nonfiction, 'Medical'),
	(Music, nonfiction, 'Music'),
	(Nature, nonfiction, 'Nature'),
	(Body_Mind_Spirit, nonfiction, 'Body, Mind & Spirit'),
	(Performing_Arts, nonfiction, 'Performing Arts'),
	(Pets, nonfiction, 'Pets'),
	(Philosophy, nonfiction, 'Philosophy'),
	(Photography, nonfiction, 'Photography'),
	(Poetry, nonfiction, 'Poetry'),
	(Political_Science, nonfiction, 'Political Science'),
	(Psychology, nonfiction, 'Psychology'),
	(Reference, nonfiction, 'Reference'),
	(Religion, nonfiction, 'Religion'),
	(Science, nonfiction, 'Science'),
	(Self_Help, nonfiction, 'Self-Help'),
	(Social_Sciences, nonfiction, 'Social Science'),
	(Sports, nonfiction, 'Sports & Recreation'),
	(Study_Aids, nonfiction, 'Study Aids'),
	(Technology, nonfiction, 'Technology & Engineering'),
	(Technology, nonfiction, 'Transportation'),
	(True_Crime, nonfiction, 'True Crime'),
	(Travel, nonfiction, 'Travel')
    ]

    # "Comics & Graphic Novels / Manga / Science Fiction"
    # -> Graphic_Novels
    GENRES = {
        Graphic_Novels : ["Comics & Graphic Novels"],
        Romance: [fiction, "Romance"]
        Science_Fiction: [fiction, "Science Fiction"],
        
    }

    NONFICTION = set(
        [
        "JNF",            

        # Nonfiction comics are nonfiction even though comics in general are
        # fiction.
        "CGN004170", # "Comics & Graphic Novels / Manga / NonFiction"
        "CGN007000", # "Comics & Graphic Novels / NonFiction",
        "LIT", # Literary criticism is always nonfiction even if it's _about_ fiction.
        ]
        )

    # 'Humor' and 'Literary Collections' might be either fiction or
    # nonfiction.
    NEITHER = set(["HUM", "LCO"])

    @classmethod
    def scrub_name(cls, name):
        """Split the name into a list of keywords."""
        parts = [x.strip() for x in name.split('/')]
        if parts[-1] == 'General':
            parts = parts[:-1]

    
