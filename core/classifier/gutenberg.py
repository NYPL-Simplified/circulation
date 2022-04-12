# encoding: utf-8

from . import *

class GutenbergBookshelfClassifier(Classifier):

    # Any classification that includes the string "Fiction" will be
    # counted as fiction. This is just the leftovers.
    FICTION = set([
        "Bestsellers, American, 1895-1923",
        "Adventure",
        "Fantasy",
        "Horror",
        "Mystery",
        "Western",
        "Suspense",
        "Thriller",
        "Children's Anthologies",
        ])

    GENRES = {
        Adventure: [
            "Adventure",
            "Pirates, Buccaneers, Corsairs, etc.",
        ],
        # African_American : ["African American Writers"],
        Ancient_History: ["Classical Antiquity"],
        Architecture : [
            "Architecture",
            "The American Architect and Building News",
        ],
        Art : ["Art"],
        Biography_Memoir : [
            "Biographies",
            "Children's Biography",
        ],
        Christianity : ["Christianity"],
        Civil_War_History: "US Civil War",
        Classics : [
            "Best Books Ever Listings",
            "Harvard Classics",
        ],
        Cooking : [
            "Armour's Monthly Cook Book",
            "Cookery",
        ],
        Drama : [
            "One Act Plays",
            "Opera",
            "Plays",
        ],
        Erotica : "Erotic Fiction",
        Fantasy : "Fantasy",
        Foreign_Language_Study : [
            "Language Education",
        ],
        Gardening : [
            "Garden and Forest",
            "Horticulture",
        ],
        Historical_Fiction : "Historical Fiction",
        History : [
            "Children's History",
        ],
        Horror : ["Gothic Fiction", "Horror"],
        Humorous_Fiction : ["Humor"],
        Islam : "Islam",
        Judaism : "Judaism",
        Law : [
            "British Law",
            "Noteworthy Trials",
            "United States Law",
        ],
        Literary_Criticism : ["Bibliomania"],
        Mathematics : "Mathematics",
        Medical : [
            "Medicine",
            "The North American Medical and Surgical Journal",
            "Physiology",
        ],
        Military_History : [
            "American Revolutionary War",
            "World War I",
            "World War II",
            "Spanish American War",
            "Boer War",
            "Napoleonic",
        ],
        Modern_History: "Current History",
        Music : [
            "Music",
            "Child's Own Book of Great Musicians",
        ],
        Mystery : [
            "Crime Fiction",
            "Detective Fiction",
            "Mystery Fiction",
        ],
        Nature : [
            "Animal",
            "Animals-Wild",
            "Bird-Lore"
            "Birds, Illustrated by Color Photography",
        ],
        Periodicals : [
            "Ainslee's",
            "Prairie Farmer",
            "Blackwood's Edinburgh Magazine",
            "Barnavännen",
            "Buchanan's Journal of Man",
            "Bulletin de Lille",
            "Celtic Magazine",
            "Chambers's Edinburgh Journal",
            "Contemporary Reviews",
            "Continental Monthly",
            "De Aarde en haar Volken",
            "Dew Drops",
            "Donahoe's Magazine",
            "Golden Days for Boys and Girls",
            "Harper's New Monthly Magazine",
            "Harper's Young People",
            "Graham's Magazine",
            "Lippincott's Magazine",
            "L'Illustration",
            "McClure's Magazine",
            "Mrs Whittelsey's Magazine for Mothers and Daughters",
            "Northern Nut Growers Association",
            "Notes and Queries",
            "Our Young Folks",
            "The American Missionary",
            "The American Quarterly Review",
            "The Arena",
            "The Argosy",
            "The Atlantic Monthly",
            "The Baptist Magazine",
            "The Bay State Monthly",
            "The Botanical Magazine",
            "The Catholic World",
            "The Christian Foundation",
            "The Church of England Magazine",
            "The Contemporary Review",
            "The Economist",
            "The Esperantist",
            "The Girls Own Paper",
            "The Great Round World And What Is Going On In It",
            "The Idler",
            "The Illustrated War News",
            "The International Magazine of Literature, Art, and Science",
            "The Irish Ecclesiastical Record",
            "The Irish Penny Journal",
            "The Journal of Negro History",
            "The Knickerbocker",
            "The Mayflower",
            "The Menorah Journal",
            "The Mentor",
            "The Mirror of Literature, Amusement, and Instruction",
            "The Mirror of Taste, and Dramatic Censor",
            "The National Preacher",
            "The Aldine",
            "The Nursery",
            "St. Nicholas Magazine for Boys and Girls",
            "Punch",
            "Punchinello",
            "Scribner's Magazine",
            "The Scrap Book",
            "The Speaker",
            "The Stars and Stripes",
            "The Strand Magazine",
            "The Unpopular Review",
            "The Writer",
            "The Yellow Book",
            "Women's Travel Journals",
        ],
        Pets : ["Animals-Domestic"],
        Philosophy : ["Philosophy"],
        Photography : "Photography",
        Poetry : [
            "Poetry",
            "Poetry, A Magazine of Verse",
            "Children's Verse",
        ],
        Political_Science : [
            "Anarchism",
            "Politics",
        ],
        Psychology : ["Psychology"],
        Reference_Study_Aids : [
            "Reference",
            "CIA World Factbooks",
        ],
        Religion_Spirituality : [
            "Atheism",
            "Bahá'í Faith",
            "Hinduism",
            "Paganism",
            "Children's Religion",
        ],
        Science : [
            "Astronomy",
            "Biology",
            "Botany",
            "Chemistry",
            "Ecology",
            "Geology",
            "Journal of Entomology and Zoology",
            "Microbiology",
            "Microscopy",
            "Natural History",
            "Mycology",
            "Popular Science Monthly",
            "Physics",
            "Scientific American",
        ],
        Science_Fiction : [
            "Astounding Stories",
            "Precursors of Science Fiction",
            "The Galaxy",
            "Science Fiction",
        ],
        Social_Sciences : [
            "Anthropology",
            "Archaeology",
            "The American Journal of Archaeology",
            "Sociology",
        ],
        Suspense_Thriller : [
            "Suspense",
            "Thriller",
        ],
        Technology : [
            "Engineering",
            "Technology",
            "Transportation",
        ],
        Travel : "Travel",
        True_Crime : "Crime Nonfiction",
        Westerns : "Western",
    }

    @classmethod
    def scrub_identifier(cls, identifier):
        return identifier

    @classmethod
    def is_fiction(cls, identifier, name):
        if (identifier in cls.FICTION
            or "Fiction" in identifier or "Stories" in identifier):
            return True
        return None

    @classmethod
    def audience(cls, identifier, name):
        if ("Children's" in identifier):
            return cls.AUDIENCE_CHILDREN
        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for l, v in list(cls.GENRES.items()):
            if identifier == v or (isinstance(v, list) and identifier in v):
                return l
        return None

Classifier.classifiers[Classifier.GUTENBERG_BOOKSHELF] = GutenbergBookshelfClassifier
