from . import *

class BICClassifier(Classifier):
    # These prefixes came from from http://editeur.dyndns.org/bic_categories

    LEVEL_1_PREFIXES = {
        Art_Design: 'A',
        Biography_Memoir: 'B',
        Foreign_Language_Study: 'C',
        Literary_Criticism: 'D',
        Reference_Study_Aids: 'G',
        Social_Sciences: 'J',
        Personal_Finance_Business: 'K',
        Law: 'L',
        Medical: 'M',
        Science_Technology: 'P',
        Technology: 'T',
        Computers: 'U',
    }

    LEVEL_2_PREFIXES = {
        Art_History: 'AC',
        Photography: 'AJ',
        Design: 'AK',
        Architecture: 'AM',
        Film_TV: 'AP',
        Performing_Arts: 'AS',
        Music: 'AV',
        Poetry: 'DC',
        Drama: 'DD',
        Classics: 'FC',
        Mystery: 'FF',
        Suspense_Thriller: 'FH',
        Adventure: 'FJ',
        Horror: 'FK',
        Science_Fiction: 'FL',
        Fantasy: 'FM',
        Erotica: 'FP',
        Romance: 'FR',
        Historical_Fiction: 'FV',
        Religious_Fiction: 'FW',
        Comics_Graphic_Novels: 'FX',
        History: 'HB',
        Philosophy: 'HP',
        Religion_Spirituality: 'HR',
        Psychology: 'JM',
        Education: 'JN',
        Political_Science: 'JP',
        Economics: 'KC',
        Business: 'KJ',
        Mathematics: 'PB',
        Science: 'PD',
        Self_Help: 'VS',
        Body_Mind_Spirit: 'VX',
        Food_Health: 'WB',
        Antiques_Collectibles: 'WC',
        Crafts_Hobbies: 'WF',
        Humorous_Nonfiction: 'WH',
        House_Home: 'WK',
        Gardening: 'WM',
        Nature: 'WN',
        Sports: 'WS',
        Travel: 'WT',
    }

    LEVEL_3_PREFIXES = {
        Historical_Mystery: 'FFH',
        Espionage: 'FHD',
        Westerns: 'FJW',
        Space_Opera: 'FLS',
        Historical_Romance: 'FRH',
        Short_Stories: 'FYB',
        World_History: 'HBG',
        Military_History: 'HBW',
        Christianity: 'HRC',
        Buddhism: 'HRE',
        Hinduism: 'HRG',
        Islam: 'HRH',
        Judaism: 'HRJ',
        Fashion: 'WJF',
        Poetry: 'YDP',
        Adventure: 'YFC',
        Horror: 'YFD',
        Science_Fiction: 'YFG',
        Fantasy: 'YFH',
        Romance: 'YFM',
        Humorous_Fiction: 'YFQ',
        Historical_Fiction: 'YFT',
        Comics_Graphic_Novels: 'YFW',
        Art: 'YNA',
        Music: 'YNC',
        Performing_Arts: 'YND',
        Film_TV: 'YNF',
        History: 'YNH',
        Nature: 'YNN',
        Religion_Spirituality: 'YNR',
        Science_Technology: 'YNT',
        Humorous_Nonfiction: 'YNU',
        Sports: 'YNW',
    }

    LEVEL_4_PREFIXES = {
        European_History: 'HBJD',
        Asian_History: 'HBJF',
        African_History: 'HBJH',
        Ancient_History: 'HBLA',
        Modern_History: 'HBLL',
        Drama: 'YNDS',
        Comics_Graphic_Novels: 'YNUC',
    }

    PREFIX_LISTS = [LEVEL_4_PREFIXES, LEVEL_3_PREFIXES, LEVEL_2_PREFIXES, LEVEL_1_PREFIXES]

    @classmethod
    def is_fiction(cls, identifier, name):
        if identifier.startswith('f') or identifier.startswith('yf'):
            return True
        return False

    @classmethod
    def audience(cls, identifier, name):
        # BIC doesn't distinguish children's and YA.
        # Classify it as YA to be safe.
        if identifier.startswith("y"):
            return cls.AUDIENCE_YOUNG_ADULT
        return cls.AUDIENCE_ADULT

    @classmethod
    def genre(cls, identifier, name, fiction=None, audience=None):
        for prefixes in cls.PREFIX_LISTS:
            for l, v in list(prefixes.items()):
                if identifier.startswith(v.lower()):
                    return l
        return None

Classifier.classifiers[Classifier.BIC] = BICClassifier
