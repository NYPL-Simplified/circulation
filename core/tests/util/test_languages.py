# encoding: utf-8
"""Test language lookup capabilities."""
import pytest

from ...util.languages import (
    LanguageCodes,
    LanguageNames,
    LookupTable,
)


class TestLookupTable(object):

    def test_lookup(self):
        d = LookupTable()
        d['key'] = 'value'
        assert 'value' == d['key']
        assert None == d['missing']
        assert False == ('missing' in d)
        assert None == d['missing']


class TestLanguageCodes(object):

    def test_lookups(self):
        c = LanguageCodes

        assert "eng" == c.two_to_three['en']
        assert "en" == c.three_to_two['eng']
        assert ["English"] == c.english_names['en']
        assert ["English"] == c.english_names['eng']
        assert ["English"] == c.native_names['en']
        assert ["English"] == c.native_names['eng']

        assert "spa" == c.two_to_three['es']
        assert "es" == c.three_to_two['spa']
        assert ['Spanish', 'Castilian'] == c.english_names['es']
        assert ['Spanish', 'Castilian'] == c.english_names['spa']
        assert ["español", "castellano"] == c.native_names['es']
        assert ["español", "castellano"] == c.native_names['spa']

        assert "chi" == c.two_to_three['zh']
        assert "zh" == c.three_to_two['chi']
        assert ["Chinese"] == c.english_names['zh']
        assert ["Chinese"] == c.english_names['chi']
        # We don't have this translation yet.
        assert [] == c.native_names['zh']
        assert [] == c.native_names['chi']

        assert None == c.two_to_three['nosuchlanguage']
        assert None == c.three_to_two['nosuchlanguage']
        assert [] == c.english_names['nosuchlanguage']
        assert [] == c.native_names['nosuchlanguage']

    def test_locale(self):
        m = LanguageCodes.iso_639_2_for_locale
        assert "eng" == m("en-US")
        assert "eng" == m("en")
        assert "eng" == m("en-GB")
        assert None == m("nq-none")

    def test_string_to_alpha_3(self):
        m = LanguageCodes.string_to_alpha_3
        assert "eng" == m("en")
        assert "eng" == m("eng")
        assert "eng" == m("en-GB")
        assert "eng" == m("English")
        assert "eng" == m("ENGLISH")
        assert "ssa" == m("Nilo-Saharan languages")
        assert None == m("NO SUCH LANGUAGE")
        assert None == None

    def test_name_for_languageset(self):
        m = LanguageCodes.name_for_languageset

        assert "" == m([])
        assert "English" == m(["en"])
        assert "English" == m(["eng"])
        assert "español" == m(['es'])
        assert "English/español" == m(["eng", "spa"])
        assert "español/English" == m("spa,eng")
        assert "español/English/Chinese" == m(["spa","eng","chi"])
        pytest.raises(ValueError, m, ["eng, nxx"])


class TestLanguageNames(object):
    """Test our (very rough) ability to map from natural-language names
    of languages to ISO-639-2 language codes.
    """

    def test_name_to_codes(self):
        # Verify that the name_to_codes dictionary was populated
        # appropriately.
        d = LanguageNames.name_to_codes

        def coded(name, code):
            # In almost all cases, a human-readable language name maps to
            # a set containing a single ISO-639-2 language code.
            assert set([code]) == d[name]

        # English-language names work.
        coded("english", "eng")
        coded("french", "fre")
        coded("irish", "gle")
        coded("tokelau", "tkl")
        coded("persian", "per")

        # (Some) native-language names work
        coded("francais", "fre")
        coded("espanol", "spa")
        coded("castellano", "spa")
        for item in LanguageCodes.NATIVE_NAMES_RAW_DATA:
            coded(item['nativeName'].lower(),
                  LanguageCodes.two_to_three[item['code']])

        # Languages associated with a historical period are not mapped
        # to codes.
        assert set() == d['irish, old (to 900)']

        # This general rule would exclude Greek ("Greek, Modern
        # (1453-)") and Occitan ("Occitan (post 1500)"), so we added
        # them manually.
        coded('greek', 'gre')
        coded('occitan', 'oci')

        # Languages associated with a geographical area, such as "Luo
        # (Kenya and Tanzania)", can be looked up without that area.
        coded('luo', 'luo')

        # This causes a little problem for Tonga: there are two
        # unrelated languages called 'Tonga', and the geographic area
        # is the only way to distinguish them. For now, we map 'tonga'
        # to both ISO codes. (This is why name_to_codes is called that
        # rather than name_to_code.)
        assert set(['ton', 'tog']) == d['tonga']

        # Language families such as "Himacahli languages" can be
        # looked up without the " languages".
        coded('himachali', 'him')

        # Language groups such as "Bantu (Other)" can be looked up
        # without the "(Other)".
        coded('south american indian', 'sai')
        coded('bantu', 'bnt')

        # If a language is known by multiple English names, lookup on
        # any of those names will work.
        for i in "Blissymbols; Blissymbolics; Bliss".split(";"):
            coded(i.strip().lower(), 'zbl')

    def test_name_re(self):
        # Verify our ability to find language names inside text.
        def find(text, expect):
            match = LanguageNames.name_re.search(text)
            if not match:
                return match
            return match.groups()

        find("books in Italian", ["Italian"])
        find("Chinese Cooking", ["Chinese"])
        find("500 spanish verbs", ["spanish"])

        # Only the first language is returned.
        find("books in japanese or italian", ["japanese"])
        find("english-russian dictionary", ["english"])

        # The language name must be a standalone word.
        find("50,000 frenchmen can't be wrong", None)
        find("visiting Thailand", None)
