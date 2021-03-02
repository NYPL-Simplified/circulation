# encoding: utf-8
"""Test language lookup capabilities."""

from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)

from ...util.languages import (
    LanguageCodes,
    LanguageNames,
    LookupTable,
)


class TestLookupTable(object):

    def test_lookup(self):
        d = LookupTable()
        d['key'] = 'value'
        eq_('value', d['key'])
        eq_(None, d['missing'])
        eq_(False, 'missing' in d)
        eq_(None, d['missing'])


class TestLanguageCodes(object):

    def test_lookups(self):
        c = LanguageCodes
        eq_("eng", c.two_to_three['en'])
        eq_("en", c.three_to_two['eng'])
        eq_(["English"], c.english_names['en'])
        eq_(["English"], c.english_names['eng'])
        eq_(["English"], c.native_names['en'])
        eq_(["English"], c.native_names['eng'])

        eq_("spa", c.two_to_three['es'])
        eq_("es", c.three_to_two['spa'])
        eq_(['Spanish', 'Castilian'], c.english_names['es'])
        eq_(['Spanish', 'Castilian'], c.english_names['spa'])
        eq_(["español", "castellano"], c.native_names['es'])
        eq_(["español", "castellano"], c.native_names['spa'])

        eq_("chi", c.two_to_three['zh'])
        eq_("zh", c.three_to_two['chi'])
        eq_(["Chinese"], c.english_names['zh'])
        eq_(["Chinese"], c.english_names['chi'])
        # We don't have this translation yet.
        eq_([], c.native_names['zh'])
        eq_([], c.native_names['chi'])

        eq_(None, c.two_to_three['nosuchlanguage'])
        eq_(None, c.three_to_two['nosuchlanguage'])
        eq_([], c.english_names['nosuchlanguage'])
        eq_([], c.native_names['nosuchlanguage'])

    def test_locale(self):
        m = LanguageCodes.iso_639_2_for_locale
        eq_("eng", m("en-US"))
        eq_("eng", m("en"))
        eq_("eng", m("en-GB"))
        eq_(None, m("nq-none"))

    def test_string_to_alpha_3(self):
        m = LanguageCodes.string_to_alpha_3
        eq_("eng", m("en"))
        eq_("eng", m("eng"))
        eq_("eng", m("en-GB"))
        eq_("eng", m("English"))
        eq_("eng", m("ENGLISH"))
        eq_("ssa", m("Nilo-Saharan languages"))
        eq_(None, m("NO SUCH LANGUAGE"))
        eq_(None, None)

    def test_name_for_languageset(self):
        m = LanguageCodes.name_for_languageset
        eq_("", m([]))
        eq_("English", m(["en"]))
        eq_("English", m(["eng"]))
        eq_("español", m(['es']))
        eq_("English/español", m(["eng", "spa"]))
        eq_("español/English", m("spa,eng"))
        eq_("español/English/Chinese", m(["spa","eng","chi"]))
        assert_raises(ValueError, m, ["eng, nxx"])


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
            eq_(set([code]), d[name])

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
        eq_(set(), d['irish, old (to 900)'])

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
        eq_(set(['ton', 'tog']), d['tonga'])

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
