# encoding: utf-8
"Miscellaneous utilities"
from money import Money
from nose.tools import set_trace
from collections import (
    Counter,
    defaultdict,
)
import os
import re
import string
from sqlalchemy import distinct
from sqlalchemy.sql.functions import func

def batch(iterable, size=1):
    """Split up `iterable` into batches of size `size`."""

    l = len(iterable)
    for start in range(0, l, size):
        yield iterable[start:min(start+size, l)]

def fast_query_count(query):
    """Counts the results of a query without using super-slow subquery"""

    statement = query.enable_eagerloads(False).with_labels().statement
    distinct_columns = statement._distinct
    new_columns = [func.count()]
    if isinstance(distinct_columns, list):
        # When using distinct to select from the db, the distinct
        # columns need to be incorporated into the count itself.
        new_columns = [func.count(distinct(func.concat(*distinct_columns)))]

        # Then we can remove the distinct criteria from the statement
        # itself by setting it to its default value, False.
        statement._distinct = False
    count_q = statement.with_only_columns(new_columns).order_by(None)
    count = query.session.execute(count_q).scalar()

    if query._limit and query._limit < count:
        return query._limit

    return count

def slugify(text, length_limit=None):
    """Takes a string and turns it into a slug.

    :Example:

    >>> slugify('Some (???) Title Somewhere')
    some-title-somewhere
    >>> slugify('Sly & the Family Stone')
    sly-and-the-family-stone
    >>> slugify('Happy birthday!', length_limit=4)
    happ
    """
    slug = re.sub('[.!@#\'$,?\(\)]', '', text.lower())
    slug = re.sub('&', ' and ', slug)
    slug = re.sub(' {2,}', ' ', slug)

    slug = '-'.join(slug.split(' '))
    while '--' in slug:
        slug = re.sub('--', '-', slug)

    if length_limit:
        slug = slug[:length_limit]
    return unicode(slug)

class LanguageCodes(object):
    """Convert between ISO-639-2 and ISO-693-1 language codes.

    The data file comes from
    http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
    """

    two_to_three = defaultdict(lambda: None)
    three_to_two = defaultdict(lambda: None)
    english_names = defaultdict(list)
    english_names_to_three = defaultdict(lambda: None)
    native_names = defaultdict(list)

    RAW_DATA = u"""aar||aa|Afar|afar
abk||ab|Abkhazian|abkhaze
ace|||Achinese|aceh
ach|||Acoli|acoli
ada|||Adangme|adangme
ady|||Adyghe; Adygei|adyghé
afa|||Afro-Asiatic languages|afro-asiatiques, langues
afh|||Afrihili|afrihili
afr||af|Afrikaans|afrikaans
ain|||Ainu|aïnou
aka||ak|Akan|akan
akk|||Akkadian|akkadien
alb|sqi|sq|Albanian|albanais
ale|||Aleut|aléoute
alg|||Algonquian languages|algonquines, langues
alt|||Southern Altai|altai du Sud
amh||am|Amharic|amharique
ang|||English, Old (ca.450-1100)|anglo-saxon (ca.450-1100)
anp|||Angika|angika
apa|||Apache languages|apaches, langues
ara||ar|Arabic|arabe
arc|||Official Aramaic (700-300 BCE); Imperial Aramaic (700-300 BCE)|araméen d'empire (700-300 BCE)
arg||an|Aragonese|aragonais
arm|hye|hy|Armenian|arménien
arn|||Mapudungun; Mapuche|mapudungun; mapuche; mapuce
arp|||Arapaho|arapaho
art|||Artificial languages|artificielles, langues
arw|||Arawak|arawak
asm||as|Assamese|assamais
ast|||Asturian; Bable; Leonese; Asturleonese|asturien; bable; léonais; asturoléonais
ath|||Athapascan languages|athapascanes, langues
aus|||Australian languages|australiennes, langues
ava||av|Avaric|avar
ave||ae|Avestan|avestique
awa|||Awadhi|awadhi
aym||ay|Aymara|aymara
aze||az|Azerbaijani|azéri
bad|||Banda languages|banda, langues
bai|||Bamileke languages|bamiléké, langues
bak||ba|Bashkir|bachkir
bal|||Baluchi|baloutchi
bam||bm|Bambara|bambara
ban|||Balinese|balinais
baq|eus|eu|Basque|basque
bas|||Basa|basa
bat|||Baltic languages|baltes, langues
bej|||Beja; Bedawiyet|bedja
bel||be|Belarusian|biélorusse
bem|||Bemba|bemba
ben||bn|Bengali|bengali
ber|||Berber languages|berbères, langues
bho|||Bhojpuri|bhojpuri
bih||bh|Bihari languages|langues biharis
bik|||Bikol|bikol
bin|||Bini; Edo|bini; edo
bis||bi|Bislama|bichlamar
bla|||Siksika|blackfoot
bnt|||Bantu (Other)|bantoues, autres langues
bos||bs|Bosnian|bosniaque
bra|||Braj|braj
bre||br|Breton|breton
btk|||Batak languages|batak, langues
bua|||Buriat|bouriate
bug|||Buginese|bugi
bul||bg|Bulgarian|bulgare
bur|mya|my|Burmese|birman
byn|||Blin; Bilin|blin; bilen
cad|||Caddo|caddo
cai|||Central American Indian languages|amérindiennes de L'Amérique centrale, langues
car|||Galibi Carib|karib; galibi; carib
cat||ca|Catalan; Valencian|catalan; valencien
cau|||Caucasian languages|caucasiennes, langues
ceb|||Cebuano|cebuano
cel|||Celtic languages|celtiques, langues; celtes, langues
cha||ch|Chamorro|chamorro
chb|||Chibcha|chibcha
che||ce|Chechen|tchétchène
chg|||Chagatai|djaghataï
chi|zho|zh|Chinese|chinois
chk|||Chuukese|chuuk
chm|||Mari|mari
chn|||Chinook jargon|chinook, jargon
cho|||Choctaw|choctaw
chp|||Chipewyan; Dene Suline|chipewyan
chr|||Cherokee|cherokee
chu||cu|Church Slavic; Old Slavonic; Church Slavonic; Old Bulgarian; Old Church Slavonic|slavon d'église; vieux slave; slavon liturgique; vieux bulgare
chv||cv|Chuvash|tchouvache
chy|||Cheyenne|cheyenne
cmc|||Chamic languages|chames, langues
cop|||Coptic|copte
cor||kw|Cornish|cornique
cos||co|Corsican|corse
cpe|||Creoles and pidgins, English based|créoles et pidgins basés sur l'anglais
cpf|||Creoles and pidgins, French-based |créoles et pidgins basés sur le français
cpp|||Creoles and pidgins, Portuguese-based |créoles et pidgins basés sur le portugais
cre||cr|Cree|cree
crh|||Crimean Tatar; Crimean Turkish|tatar de Crimé
crp|||Creoles and pidgins |créoles et pidgins
csb|||Kashubian|kachoube
cus|||Cushitic languages|couchitiques, langues
cze|ces|cs|Czech|tchèque
dak|||Dakota|dakota
dan||da|Danish|danois
dar|||Dargwa|dargwa
day|||Land Dayak languages|dayak, langues
del|||Delaware|delaware
den|||Slave (Athapascan)|esclave (athapascan)
dgr|||Dogrib|dogrib
din|||Dinka|dinka
div||dv|Divehi; Dhivehi; Maldivian|maldivien
doi|||Dogri|dogri
dra|||Dravidian languages|dravidiennes, langues
dsb|||Lower Sorbian|bas-sorabe
dua|||Duala|douala
dum|||Dutch, Middle (ca.1050-1350)|néerlandais moyen (ca. 1050-1350)
dut|nld|nl|Dutch; Flemish|néerlandais; flamand
dyu|||Dyula|dioula
dzo||dz|Dzongkha|dzongkha
efi|||Efik|efik
egy|||Egyptian (Ancient)|égyptien
eka|||Ekajuk|ekajuk
elx|||Elamite|élamite
eng||en|English|anglais
enm|||English, Middle (1100-1500)|anglais moyen (1100-1500)
epo||eo|Esperanto|espéranto
est||et|Estonian|estonien
ewe||ee|Ewe|éwé
ewo|||Ewondo|éwondo
fan|||Fang|fang
fao||fo|Faroese|féroïen
fat|||Fanti|fanti
fij||fj|Fijian|fidjien
fil|||Filipino; Pilipino|filipino; pilipino
fin||fi|Finnish|finnois
fiu|||Finno-Ugrian languages|finno-ougriennes, langues
fon|||Fon|fon
fre|fra|fr|French|français
frm|||French, Middle (ca.1400-1600)|français moyen (1400-1600)
fro|||French, Old (842-ca.1400)|français ancien (842-ca.1400)
frr|||Northern Frisian|frison septentrional
frs|||Eastern Frisian|frison oriental
fry||fy|Western Frisian|frison occidental
ful||ff|Fulah|peul
fur|||Friulian|frioulan
gaa|||Ga|ga
gay|||Gayo|gayo
gba|||Gbaya|gbaya
gem|||Germanic languages|germaniques, langues
geo|kat|ka|Georgian|géorgien
ger|deu|de|German|allemand
gez|||Geez|guèze
gil|||Gilbertese|kiribati
gla||gd|Gaelic; Scottish Gaelic|gaélique; gaélique écossais
gle||ga|Irish|irlandais
glg||gl|Galician|galicien
glv||gv|Manx|manx; mannois
gmh|||German, Middle High (ca.1050-1500)|allemand, moyen haut (ca. 1050-1500)
goh|||German, Old High (ca.750-1050)|allemand, vieux haut (ca. 750-1050)
gon|||Gondi|gond
gor|||Gorontalo|gorontalo
got|||Gothic|gothique
grb|||Grebo|grebo
grc|||Greek, Ancient (to 1453)|grec ancien (jusqu'à 1453)
gre|ell|el|Greek, Modern (1453-)|grec moderne (après 1453)
grn||gn|Guarani|guarani
gsw|||Swiss German; Alemannic; Alsatian|suisse alémanique; alémanique; alsacien
guj||gu|Gujarati|goudjrati
gwi|||Gwich'in|gwich'in
hai|||Haida|haida
hat||ht|Haitian; Haitian Creole|haïtien; créole haïtien
hau||ha|Hausa|haoussa
haw|||Hawaiian|hawaïen
heb||he|Hebrew|hébreu
her||hz|Herero|herero
hil|||Hiligaynon|hiligaynon
him|||Himachali languages; Western Pahari languages|langues himachalis; langues paharis occidentales
hin||hi|Hindi|hindi
hit|||Hittite|hittite
hmn|||Hmong; Mong|hmong
hmo||ho|Hiri Motu|hiri motu
hrv||hr|Croatian|croate
hsb|||Upper Sorbian|haut-sorabe
hun||hu|Hungarian|hongrois
hup|||Hupa|hupa
iba|||Iban|iban
ibo||ig|Igbo|igbo
ice|isl|is|Icelandic|islandais
ido||io|Ido|ido
iii||ii|Sichuan Yi; Nuosu|yi de Sichuan
ijo|||Ijo languages|ijo, langues
iku||iu|Inuktitut|inuktitut
ile||ie|Interlingue; Occidental|interlingue
ilo|||Iloko|ilocano
ina||ia|Interlingua (International Auxiliary Language Association)|interlingua (langue auxiliaire internationale)
inc|||Indic languages|indo-aryennes, langues
ind||id|Indonesian|indonésien
ine|||Indo-European languages|indo-européennes, langues
inh|||Ingush|ingouche
ipk||ik|Inupiaq|inupiaq
ira|||Iranian languages|iraniennes, langues
iro|||Iroquoian languages|iroquoises, langues
ita||it|Italian|italien
jav||jv|Javanese|javanais
jbo|||Lojban|lojban
jpn||ja|Japanese|japonais
jpr|||Judeo-Persian|judéo-persan
jrb|||Judeo-Arabic|judéo-arabe
kaa|||Kara-Kalpak|karakalpak
kab|||Kabyle|kabyle
kac|||Kachin; Jingpho|kachin; jingpho
kal||kl|Kalaallisut; Greenlandic|groenlandais
kam|||Kamba|kamba
kan||kn|Kannada|kannada
kar|||Karen languages|karen, langues
kas||ks|Kashmiri|kashmiri
kau||kr|Kanuri|kanouri
kaw|||Kawi|kawi
kaz||kk|Kazakh|kazakh
kbd|||Kabardian|kabardien
kha|||Khasi|khasi
khi|||Khoisan languages|khoïsan, langues
khm||km|Central Khmer|khmer central
kho|||Khotanese; Sakan|khotanais; sakan
kik||ki|Kikuyu; Gikuyu|kikuyu
kin||rw|Kinyarwanda|rwanda
kir||ky|Kirghiz; Kyrgyz|kirghiz
kmb|||Kimbundu|kimbundu
kok|||Konkani|konkani
kom||kv|Komi|kom
kon||kg|Kongo|kongo
kor||ko|Korean|coréen
kos|||Kosraean|kosrae
kpe|||Kpelle|kpellé
krc|||Karachay-Balkar|karatchai balkar
krl|||Karelian|carélien
kro|||Kru languages|krou, langues
kru|||Kurukh|kurukh
kua||kj|Kuanyama; Kwanyama|kuanyama; kwanyama
kum|||Kumyk|koumyk
kur||ku|Kurdish|kurde
kut|||Kutenai|kutenai
lad|||Ladino|judéo-espagnol
lah|||Lahnda|lahnda
lam|||Lamba|lamba
lao||lo|Lao|lao
lat||la|Latin|latin
lav||lv|Latvian|letton
lez|||Lezghian|lezghien
lim||li|Limburgan; Limburger; Limburgish|limbourgeois
lin||ln|Lingala|lingala
lit||lt|Lithuanian|lituanien
lol|||Mongo|mongo
loz|||Lozi|lozi
ltz||lb|Luxembourgish; Letzeburgesch|luxembourgeois
lua|||Luba-Lulua|luba-lulua
lub||lu|Luba-Katanga|luba-katanga
lug||lg|Ganda|ganda
lui|||Luiseno|luiseno
lun|||Lunda|lunda
luo|||Luo (Kenya and Tanzania)|luo (Kenya et Tanzanie)
lus|||Lushai|lushai
mac|mkd|mk|Macedonian|macédonien
mad|||Madurese|madourais
mag|||Magahi|magahi
mah||mh|Marshallese|marshall
mai|||Maithili|maithili
mak|||Makasar|makassar
mal||ml|Malayalam|malayalam
man|||Mandingo|mandingue
mao|mri|mi|Maori|maori
map|||Austronesian languages|austronésiennes, langues
mar||mr|Marathi|marathe
mas|||Masai|massaï
may|msa|ms|Malay|malais
mdf|||Moksha|moksa
mdr|||Mandar|mandar
men|||Mende|mendé
mga|||Irish, Middle (900-1200)|irlandais moyen (900-1200)
mic|||Mi'kmaq; Micmac|mi'kmaq; micmac
min|||Minangkabau|minangkabau
mis|||Uncoded languages|langues non codées
mkh|||Mon-Khmer languages|môn-khmer, langues
mlg||mg|Malagasy|malgache
mlt||mt|Maltese|maltais
mnc|||Manchu|mandchou
mni|||Manipuri|manipuri
mno|||Manobo languages|manobo, langues
moh|||Mohawk|mohawk
mon||mn|Mongolian|mongol
mos|||Mossi|moré
mul|||Multiple languages|multilingue
mun|||Munda languages|mounda, langues
mus|||Creek|muskogee
mwl|||Mirandese|mirandais
mwr|||Marwari|marvari
myn|||Mayan languages|maya, langues
myv|||Erzya|erza
nah|||Nahuatl languages|nahuatl, langues
nai|||North American Indian languages|nord-amérindiennes, langues
nap|||Neapolitan|napolitain
nau||na|Nauru|nauruan
nav||nv|Navajo; Navaho|navaho
nbl||nr|Ndebele, South; South Ndebele|ndébélé du Sud
nde||nd|Ndebele, North; North Ndebele|ndébélé du Nord
ndo||ng|Ndonga|ndonga
nds|||Low German; Low Saxon; German, Low; Saxon, Low|bas allemand; bas saxon; allemand, bas; saxon, bas
nep||ne|Nepali|népalais
new|||Nepal Bhasa; Newari|nepal bhasa; newari
nia|||Nias|nias
nic|||Niger-Kordofanian languages|nigéro-kordofaniennes, langues
niu|||Niuean|niué
nno||nn|Norwegian Nynorsk; Nynorsk, Norwegian|norvégien nynorsk; nynorsk, norvégien
nob||nb|Bokmål, Norwegian; Norwegian Bokmål|norvégien bokmål
nog|||Nogai|nogaï; nogay
non|||Norse, Old|norrois, vieux
nor||no|Norwegian|norvégien
nqo|||N'Ko|n'ko
nso|||Pedi; Sepedi; Northern Sotho|pedi; sepedi; sotho du Nord
nub|||Nubian languages|nubiennes, langues
nwc|||Classical Newari; Old Newari; Classical Nepal Bhasa|newari classique
nya||ny|Chichewa; Chewa; Nyanja|chichewa; chewa; nyanja
nym|||Nyamwezi|nyamwezi
nyn|||Nyankole|nyankolé
nyo|||Nyoro|nyoro
nzi|||Nzima|nzema
oci||oc|Occitan (post 1500); Provençal|occitan (après 1500); provençal
oji||oj|Ojibwa|ojibwa
ori||or|Oriya|oriya
orm||om|Oromo|galla
osa|||Osage|osage
oss||os|Ossetian; Ossetic|ossète
ota|||Turkish, Ottoman (1500-1928)|turc ottoman (1500-1928)
oto|||Otomian languages|otomi, langues
paa|||Papuan languages|papoues, langues
pag|||Pangasinan|pangasinan
pal|||Pahlavi|pahlavi
pam|||Pampanga; Kapampangan|pampangan
pan||pa|Panjabi; Punjabi|pendjabi
pap|||Papiamento|papiamento
pau|||Palauan|palau
peo|||Persian, Old (ca.600-400 B.C.)|perse, vieux (ca. 600-400 av. J.-C.)
per|fas|fa|Persian|persan
phi|||Philippine languages|philippines, langues
phn|||Phoenician|phénicien
pli||pi|Pali|pali
pol||pl|Polish|polonais
pon|||Pohnpeian|pohnpei
por||pt|Portuguese|portugais
pra|||Prakrit languages|prâkrit, langues
pro|||Provençal, Old (to 1500)|provençal ancien (jusqu'à 1500)
pus||ps|Pushto; Pashto|pachto
qaa-qtz|||Reserved for local use|réservée à l'usage local
que||qu|Quechua|quechua
raj|||Rajasthani|rajasthani
rap|||Rapanui|rapanui
rar|||Rarotongan; Cook Islands Maori|rarotonga; maori des îles Cook
roa|||Romance languages|romanes, langues
roh||rm|Romansh|romanche
rom|||Romany|tsigane
rum|ron|ro|Romanian; Moldavian; Moldovan|roumain; moldave
run||rn|Rundi|rundi
rup|||Aromanian; Arumanian; Macedo-Romanian|aroumain; macédo-roumain
rus||ru|Russian|russe
sad|||Sandawe|sandawe
sag||sg|Sango|sango
sah|||Yakut|iakoute
sai|||South American Indian (Other)|indiennes d'Amérique du Sud, autres langues
sal|||Salishan languages|salishennes, langues
sam|||Samaritan Aramaic|samaritain
san||sa|Sanskrit|sanskrit
sas|||Sasak|sasak
sat|||Santali|santal
scn|||Sicilian|sicilien
sco|||Scots|écossais
sel|||Selkup|selkoupe
sem|||Semitic languages|sémitiques, langues
sga|||Irish, Old (to 900)|irlandais ancien (jusqu'à 900)
sgn|||Sign Languages|langues des signes
shn|||Shan|chan
sid|||Sidamo|sidamo
sin||si|Sinhala; Sinhalese|singhalais
sio|||Siouan languages|sioux, langues
sit|||Sino-Tibetan languages|sino-tibétaines, langues
sla|||Slavic languages|slaves, langues
slo|slk|sk|Slovak|slovaque
slv||sl|Slovenian|slovène
sma|||Southern Sami|sami du Sud
sme||se|Northern Sami|sami du Nord
smi|||Sami languages|sames, langues
smj|||Lule Sami|sami de Lule
smn|||Inari Sami|sami d'Inari
smo||sm|Samoan|samoan
sms|||Skolt Sami|sami skolt
sna||sn|Shona|shona
snd||sd|Sindhi|sindhi
snk|||Soninke|soninké
sog|||Sogdian|sogdien
som||so|Somali|somali
son|||Songhai languages|songhai, langues
sot||st|Sotho, Southern|sotho du Sud
spa||es|Spanish; Castilian|espagnol; castillan
srd||sc|Sardinian|sarde
srn|||Sranan Tongo|sranan tongo
srp||sr|Serbian|serbe
srr|||Serer|sérère
ssa|||Nilo-Saharan languages|nilo-sahariennes, langues
ssw||ss|Swati|swati
suk|||Sukuma|sukuma
sun||su|Sundanese|soundanais
sus|||Susu|soussou
sux|||Sumerian|sumérien
swa||sw|Swahili|swahili
swe||sv|Swedish|suédois
syc|||Classical Syriac|syriaque classique
syr|||Syriac|syriaque
tah||ty|Tahitian|tahitien
tai|||Tai languages|tai, langues
tam||ta|Tamil|tamoul
tat||tt|Tatar|tatar
tel||te|Telugu|télougou
tem|||Timne|temne
ter|||Tereno|tereno
tet|||Tetum|tetum
tgk||tg|Tajik|tadjik
tgl||tl|Tagalog|tagalog
tha||th|Thai|thaï
tib|bod|bo|Tibetan|tibétain
tig|||Tigre|tigré
tir||ti|Tigrinya|tigrigna
tiv|||Tiv|tiv
tkl|||Tokelau|tokelau
tlh|||Klingon; tlhIngan-Hol|klingon
tli|||Tlingit|tlingit
tmh|||Tamashek|tamacheq
tog|||Tonga (Nyasa)|tonga (Nyasa)
ton||to|Tonga (Tonga Islands)|tongan (Îles Tonga)
tpi|||Tok Pisin|tok pisin
tsi|||Tsimshian|tsimshian
tsn||tn|Tswana|tswana
tso||ts|Tsonga|tsonga
tuk||tk|Turkmen|turkmène
tum|||Tumbuka|tumbuka
tup|||Tupi languages|tupi, langues
tur||tr|Turkish|turc
tut|||Altaic languages|altaïques, langues
tvl|||Tuvalu|tuvalu
twi||tw|Twi|twi
tyv|||Tuvinian|touva
udm|||Udmurt|oudmourte
uga|||Ugaritic|ougaritique
uig||ug|Uighur; Uyghur|ouïgour
ukr||uk|Ukrainian|ukrainien
umb|||Umbundu|umbundu
und|||Undetermined|indéterminée
urd||ur|Urdu|ourdou
uzb||uz|Uzbek|ouszbek
vai|||Vai|vaï
ven||ve|Venda|venda
vie||vi|Vietnamese|vietnamien
vol||vo|Volapük|volapük
vot|||Votic|vote
wak|||Wakashan languages|wakashanes, langues
wal|||Walamo|walamo
war|||Waray|waray
was|||Washo|washo
wel|cym|cy|Welsh|gallois
wen|||Sorbian languages|sorabes, langues
wln||wa|Walloon|wallon
wol||wo|Wolof|wolof
xal|||Kalmyk; Oirat|kalmouk; oïrat
xho||xh|Xhosa|xhosa
yao|||Yao|yao
yap|||Yapese|yapois
yid||yi|Yiddish|yiddish
yor||yo|Yoruba|yoruba
ypk|||Yupik languages|yupik, langues
zap|||Zapotec|zapotèque
zbl|||Blissymbols; Blissymbolics; Bliss|symboles Bliss; Bliss
zen|||Zenaga|zenaga
zgh|||Standard Moroccan Tamazight|amazighe standard marocain
zha||za|Zhuang; Chuang|zhuang; chuang
znd|||Zande languages|zandé, langues
zul||zu|Zulu|zoulou
zun|||Zuni|zuni
zxx|||No linguistic content; Not applicable|pas de contenu linguistique; non applicable
zza|||Zaza; Dimili; Dimli; Kirdki; Kirmanjki; Zazaki|zaza; dimili; dimli; kirdki; kirmanjki; zazaki"""

    NATIVE_NAMES_RAW_DATA =  [
        {"code":"en","name":"English","nativeName":u"English"},
        {"code":"fr","name":"French","nativeName":u"français"},
        {"code":"de","name":"German","nativeName":u"Deutsch"},
        {"code":"el","name":"Greek, Modern","nativeName":u"Ελληνικά"},
        {"code":"hu","name":"Hungarian","nativeName":u"Magyar"},
        {"code":"it","name":"Italian","nativeName":u"Italiano"},
        {"code":"no","name":"Norwegian","nativeName":u"Norsk"},
        {"code":"pl","name":"Polish","nativeName":u"polski"},
        {"code":"pt","name":"Portuguese","nativeName":u"Português"},
        {"code":"ru","name":"Russian","nativeName":u"русский"},
        {"code":"es","name":"Spanish, Castilian","nativeName":u"español, castellano"},
        {"code":"sv","name":"Swedish","nativeName":u"svenska"},
    ]

    for i in RAW_DATA.split("\n"):
        (alpha_3, terminologic_code, alpha_2, names,
         french_names) = i.strip().split("|")
        names = [x.strip() for x in names.split(";")]
        if alpha_2:
            three_to_two[alpha_3] = alpha_2
            english_names[alpha_2] = names
            two_to_three[alpha_2] = alpha_3
        for name in names:
            english_names_to_three[name.lower()] = alpha_3
        english_names[alpha_3] = names

    for i in NATIVE_NAMES_RAW_DATA:
        alpha_2 = i['code']
        alpha_3 = two_to_three[alpha_2]
        names = i['nativeName']
        names = [x.strip() for x in names.split(",")]
        native_names[alpha_2] = names
        native_names[alpha_3] = names

    @classmethod
    def iso_639_2_for_locale(cls, locale):
        """Turn a locale code into an ISO-639-2 alpha-3 language code."""
        if '-' in locale:
            language, place = locale.lower().split("-",1)
        else:
            language = locale
        if cls.two_to_three[language]:
            return cls.two_to_three[language]
        elif cls.three_to_two[language]:
            # It's already ISO-639-2.
            return language
        return None

    @classmethod
    def string_to_alpha_3(cls, s):
        """Try really hard to convert a string to an ISO-639-2 alpha-3 language code."""
        if not s:
            return None
        s = s.lower()
        if s in cls.english_names_to_three:
            # It's the English name of a language.
            return cls.english_names_to_three[s]

        if "-" in s:
            s = s.split("-")[0]

        if s in cls.three_to_two:
            # It's already an alpha-3.
            return s
        elif s in cls.two_to_three:
            # It's an alpha-2.
            return cls.two_to_three[s]

        return None

    @classmethod
    def name_for_languageset(cls, languages):
        if isinstance(languages, basestring):
            languages = languages.split(",")
        all_names = []
        if not languages:
            return ""
        for l in languages:
            normalized = cls.string_to_alpha_3(l)
            native_names = cls.native_names.get(normalized, [])
            if native_names:
                all_names.append(native_names[0])
            else:
                names = cls.english_names.get(normalized, [])
                if not names:
                    raise ValueError("No native or English name for %s" % l)
                all_names.append(names[0])
        if len(all_names) == 1:
            return all_names[0]
        return "/".join(all_names)

def languages_from_accept(accept_languages):
    """Turn a list of (locale, quality) 2-tuples into a list of language codes."""
    seen = set([])
    languages = []
    for locale, quality in accept_languages:
        language = LanguageCodes.iso_639_2_for_locale(locale)
        if language and language not in seen:
            languages.append(language)
            seen.add(language)
    if not languages:
        languages = os.environ.get('DEFAULT_LANGUAGES', 'eng')
        languages = languages.split(',')
    return languages


class MetadataSimilarity(object):
    """Estimate how similar two bits of metadata are."""

    SEPARATOR = re.compile("\W")

    @classmethod
    def _wordbag(cls, s):
        return set(cls._wordlist(s))

    @classmethod
    def _wordlist(cls, s):
        return [x.strip().lower() for x in cls.SEPARATOR.split(s) if x.strip()]

    @classmethod
    def histogram(cls, strings, stopwords=None):
        """Create a histogram of word frequencies across the given list of
        strings.
        """
        histogram = Counter()
        words = 0
        for string in strings:
            for word in cls._wordlist(string):
                if not stopwords or word not in stopwords:
                    histogram[word] += 1
                    words += 1

        return cls.normalize_histogram(histogram, words)

    @classmethod
    def normalize_histogram(cls, histogram, total=None):
        if not total:
            total = sum(histogram.values())
        total = float(total)
        for k, v in histogram.items():
            histogram[k] = v/total
        return histogram

    @classmethod
    def histogram_distance(cls, strings_1, strings_2, stopwords=None):
        """Calculate the histogram distance between two sets of strings.

        The histogram distance is the sum of the word distance for
        every word that occurs in either histogram.

        If a word appears in one histogram but not the other, its word
        distance is its frequency of appearance. If a word appears in
        both histograms, its word distance is the absolute value of
        the difference between that word's frequency of appearance in
        histogram A, and its frequency of appearance in histogram B.

        If the strings use the same words at exactly the same
        frequency, the difference will be 0. If the strings use
        completely different words, the difference will be 1.

        """
        if not stopwords:
            stopwords = set(["the", "a", "an"])

        histogram_1 = cls.histogram(strings_1, stopwords=stopwords)
        histogram_2 = cls.histogram(strings_2, stopwords=stopwords)
        return cls.counter_distance(histogram_1, histogram_2)

    @classmethod
    def counter_distance(cls, counter1, counter2):
        differences = []
        # For every item that appears in histogram 1, compare its
        # frequency against the frequency of that item in histogram 2.
        for k, v in counter1.items():
            difference = abs(v - counter2.get(k, 0))
            differences.append(difference)

        # Add the frequency of every item that appears in histogram 2
        # titles but not in histogram 1.
        for k, v in counter2.items():
            if k not in counter1:
                differences.append(abs(v))

        return sum(differences) / 2


    @classmethod
    def most_common(cls, maximum_size, *items):
        """Return the most common item that's not longer than the max."""
        c = Counter()
        for i in items:
            if i and len(i) <= maximum_size:
                c[i] += 1

        common = c.most_common(1)
        if not common:
            return None
        return common[0][0]

    @classmethod
    def _wordbags_for_author(cls, author):
        bags = [cls._wordbag(author.sort_name)]
        for alias in author.aliases:
            bags.append(cls._wordbag(alias))
        return bags

    @classmethod
    def _matching_author_in(cls, to_match, authors):
        for author in authors:
            for name in author:
                if name in to_match:
                    return name
        return None

    @classmethod
    def _word_match_proportion(cls, s1, s2, stopwords):
        """What proportion of words do s1 and s2 share, considered as wordbags?"""
        b1 = cls._wordbag(s1) - stopwords
        b2 = cls._wordbag(s2) - stopwords
        return b1, b2, cls._proportion(b1, b2)

    @classmethod
    def _proportion(cls, s1, s2):
        if s1 == s2:
            return 1
        total = len(s1.union(s2))
        shared = len(s1.intersection(s2))
        if not total:
            return 0
        return shared/float(total)

    @classmethod
    def title_similarity(cls, title1, title2):
        if title1 == title2:
            return 1
        if title1 == None or title2 == None:
            return 0
        b1, b2, proportion = cls._word_match_proportion(
            title1, title2, set(['a', 'the', 'an']))
        if not b1.union(b2) in (b1, b2):
            # Penalize titles where one title is not a subset of the
            # other. "Tom Sawyer Abroad" will not face an extra
            # penalty vis-a-vis "Tom Sawyer", but it will face an
            # extra penalty vis-a-vis "Tom Sawyer, Detective".
            proportion *= 0.4
        return proportion

    @classmethod
    def author_similarity(cls, authors1, authors2):
        """What percentage of the total number of authors in the two sets
        are present in both sets?
        """
        return cls._proportion(set(authors1), set(authors2))

    @classmethod
    def author_name_similarity(cls, authors1, authors2):
        """What percentage of the total number of authors in the two sets
        are present in both sets?
        """
        return cls._proportion(
            set([x.sort_name for x in authors1]), set([x.sort_name for x in authors2]))

class TitleProcessor(object):

    title_stopwords = ['The ', 'A ', 'An ']

    @classmethod
    def sort_title_for(cls, title):
        if not title:
            return title
        for stopword in cls.title_stopwords:
            if title.startswith(stopword):
                title = title[len(stopword):] + ", " + stopword.strip()
                break
        return title

    @classmethod
    def extract_subtitle(cls, main_title, subtitled_title):
        """Extracts a subtitle given a shorter and longer title version

        :return: subtitle or None
        """
        if not subtitled_title:
            return None
        subtitle = subtitled_title.replace(main_title, '')
        while (subtitle and
                (subtitle[0] in string.whitespace+':.')):
            # Trim any leading whitespace or colons
            subtitle = subtitle[1:]
        if not subtitle:
            # The main title and the full title were the same.
            return None
        return subtitle


class Bigrams(object):

    all_letters = re.compile("^[a-z]+$")

    def __init__(self, bigrams):
        self.bigrams = bigrams
        self.proportional = Counter()
        total = float(sum(bigrams.values()))
        for bigram, quantity in self.bigrams.most_common():
            proportion = quantity/total
            if proportion < 0.001:
                break
            self.proportional[bigram] = proportion

    def difference_from(self, other_bigrams):
        total_difference = 0
        for bigram, proportion in self.proportional.items():
            other_proportion = other_bigrams.proportional[bigram]
            difference = abs(other_proportion - proportion)
            total_difference += difference
            # print "%s %.4f-%.4f = %.4f => %.4f" % (bigram, other_proportion, proportion, difference, total_difference)
        for bigram, proportion in other_bigrams.proportional.items():
            if bigram not in self.proportional:
                total_difference += proportion
                # print "%s MISSING %.4f => %.4f" % (bigram, proportion, total_difference)
        return total_difference

    @classmethod
    def from_text_files(cls, paths):
        bigrams = Counter()
        for path in paths:
            cls.process_data(open(path).read(), bigrams)
        return Bigrams(bigrams)

    @classmethod
    def from_string(cls, string):
        bigrams = Counter()
        cls.process_data(string, bigrams)
        return Bigrams(bigrams)

    @classmethod
    def process_data(cls, data, bigrams):
        for i in range(0, len(data)-1):
            bigram = data[i:i+2].strip()
            if len(bigram) == 2 and cls.all_letters.match(bigram):
                bigrams[bigram.lower()] += 1

english_bigram_frequencies = {
    "ab": 0.0021712725750437792,
    "ac": 0.005213707466347486,
    "ad": 0.004761174757224308,
    "ag": 0.002362898803662714,
    "ai": 0.004243783939953184,
    "ak": 0.0016317710390858545,
    "al": 0.009420640208489336,
    "am": 0.0022184421082422864,
    "an": 0.019261384072027876,
    "ap": 0.001748220824169669,
    "ar": 0.010173878691752996,
    "as": 0.009223117788220589,
    "at": 0.01276525492184598,
    "au": 0.0010539442574041427,
    "av": 0.0018941515675025501,
    "ay": 0.0026193831404295966,
    "ba": 0.001463729577066173,
    "be": 0.005828385445840531,
    "bl": 0.002477874540834075,
    "bo": 0.0026577083861533835,
    "br": 0.0010568923532290493,
    "bu": 0.002104940418983379,
    "by": 0.0013163247858208383,
    "ca": 0.004967541464967778,
    "ce": 0.006574253689541925,
    "ch": 0.004742012134362416,
    "ci": 0.002072511364909405,
    "ck": 0.0032694382698215223,
    "cl": 0.0010966916468652897,
    "co": 0.007743173684117428,
    "cr": 0.0021771687666935925,
    "ct": 0.0035804623793491783,
    "cu": 0.0016922070034964417,
    "da": 0.0015005807748775066,
    "de": 0.006702495857925366,
    "di": 0.003999091986485929,
    "do": 0.0031824694429867747,
    "ds": 0.0014740479124533464,
    "ea": 0.007121125465062116,
    "ec": 0.005023555285641005,
    "ed": 0.012741670155246725,
    "ee": 0.0045415416182687605,
    "ef": 0.0013325393128578251,
    "eg": 0.001064262592791316,
    "ei": 0.0018351896510044163,
    "el": 0.00604359644105872,
    "em": 0.003947500309550061,
    "en": 0.01328117169120465,
    "ep": 0.0017762277345062824,
    "er": 0.02339461441854706,
    "es": 0.010507013519967454,
    "et": 0.004740538086449962,
    "ev": 0.0024631340617095416,
    "ew": 0.001260310965147611,
    "ex": 0.0015035288707024132,
    "ey": 0.002464608109621995,
    "fa": 0.0013045324025212116,
    "fe": 0.002000283017199191,
    "ff": 0.0015919717454496141,
    "fi": 0.0024395492951102883,
    "fo": 0.004199562502579583,
    "fr": 0.0018233972677047896,
    "ft": 0.0012278819110736374,
    "fu": 0.0010524702094916894,
    "ga": 0.001748220824169669,
    "ge": 0.003719022883119793,
    "gh": 0.0031839434908992282,
    "gi": 0.0014755219603657997,
    "go": 0.0016332450869983078,
    "gr": 0.002012075400498818,
    "ha": 0.010978708851952524,
    "he": 0.03081791970566211,
    "hi": 0.007993761829234497,
    "ho": 0.00562496683392197,
    "hr": 0.0012308300068985443,
    "ht": 0.002037134215010525,
    "ia": 0.0017334803450451354,
    "ib": 0.001068684736528676,
    "ic": 0.007367291466441825,
    "id": 0.004277687041939611,
    "ie": 0.0031692030117746947,
    "if": 0.001578705314237534,
    "ig": 0.0029687324956810396,
    "il": 0.004321908479313212,
    "im": 0.0030615975141656004,
    "in": 0.02371595686346189,
    "io": 0.005216655562172393,
    "ir": 0.003085182280764854,
    "is": 0.008928308205729919,
    "it": 0.011687725897842583,
    "iv": 0.0021801168625184995,
    "ke": 0.004137652490256543,
    "ki": 0.0015521724518133737,
    "ks": 0.0010657366407037694,
    "la": 0.003962240788674595,
    "ld": 0.0029628363040312264,
    "le": 0.009557726664347498,
    "li": 0.006910336613581288,
    "ll": 0.007724011061255535,
    "lo": 0.0036350021521099523,
    "ls": 0.0010274113949799825,
    "ly": 0.0051871746039233255,
    "ma": 0.004444254456046839,
    "me": 0.00833868904074858,
    "mi": 0.0025766357509684496,
    "mo": 0.0027977429378364515,
    "mp": 0.0029304072499572527,
    "mu": 0.0011689199945755036,
    "my": 0.0012558888214102512,
    "na": 0.0025810578947058093,
    "nc": 0.002830171991910425,
    "nd": 0.014385233577632207,
    "ne": 0.007968703014722791,
    "ng": 0.01172162899982901,
    "ni": 0.003599625002211072,
    "no": 0.004734641894800148,
    "ns": 0.003651216679146939,
    "nt": 0.008982847978490693,
    "ny": 0.0015698610267628138,
    "oc": 0.0017187398659206019,
    "od": 0.0017718055907689223,
    "of": 0.009453069262563311,
    "oi": 0.0010229892512426224,
    "ok": 0.0014946845832276932,
    "ol": 0.0031087670473641076,
    "om": 0.006596364408228725,
    "on": 0.015450970218335977,
    "oo": 0.0033667254320434432,
    "op": 0.0026650786257156503,
    "or": 0.012544147734977978,
    "os": 0.0024248088159857547,
    "ot": 0.004404455162410599,
    "ou": 0.012628168465987818,
    "ov": 0.0014814181520156132,
    "ow": 0.004357285629212092,
    "pa": 0.0024248088159857547,
    "pe": 0.004336648958437745,
    "ph": 0.001596393889186974,
    "pi": 0.0014047676605680392,
    "pl": 0.0026621305298907437,
    "po": 0.0027137222068266105,
    "pp": 0.0012897919233966781,
    "pr": 0.003148566341000348,
    "pu": 0.002514725738645409,
    "qu": 0.0011851345216124904,
    "ra": 0.00642242675455923,
    "rc": 0.001068684736528676,
    "rd": 0.0022656116414407935,
    "re": 0.016922070034964418,
    "rg": 0.0016332450869983078,
    "ri": 0.00681010135553446,
    "rk": 0.0017261101054828686,
    "rl": 0.001046574017841876,
    "rm": 0.0016317710390858545,
    "rn": 0.001328117169120465,
    "ro": 0.007760862259066869,
    "rr": 0.0010878473593905697,
    "rs": 0.004743486182274869,
    "rt": 0.0029893691664553863,
    "ru": 0.0013177988337332916,
    "ry": 0.002701929823526984,
    "sa": 0.0028758674771964788,
    "sc": 0.0012691552526223311,
    "se": 0.008092523039368872,
    "sh": 0.0038148359974292606,
    "si": 0.004310116096013585,
    "so": 0.0036910159727831793,
    "sp": 0.0017718055907689223,
    "ss": 0.0034404278276661104,
    "st": 0.01015324202097865,
    "su": 0.0018631965613410298,
    "ta": 0.004728745703150335,
    "te": 0.013699801298341401,
    "th": 0.031493033649565745,
    "ti": 0.009472231885425203,
    "tl": 0.0016745184285470015,
    "to": 0.010639677832088254,
    "tr": 0.0039519224532874216,
    "ts": 0.0027933207940990913,
    "tt": 0.0022449749706664464,
    "tu": 0.0017644353512066555,
    "ty": 0.0020164975442361777,
    "ub": 0.0011114321259898233,
    "uc": 0.0013590721752819853,
    "ue": 0.0012529407255853443,
    "ug": 0.0015904976975371608,
    "ui": 0.0010952175989528364,
    "ul": 0.0035052859358140577,
    "um": 0.0012323040548109976,
    "un": 0.004690420457426548,
    "up": 0.0018956256154150034,
    "ur": 0.0044309880248347595,
    "us": 0.004758226661399402,
    "ut": 0.007224308818933851,
    "ve": 0.008085152799806605,
    "vi": 0.0024498676304974616,
    "wa": 0.00562938897765933,
    "we": 0.0034581164026155505,
    "wh": 0.0031721511075996013,
    "wi": 0.0038531612431530475,
    "wn": 0.0013413836003325452,
    "wo": 0.0029937913101927465,
    "ye": 0.0010288854428924358,
    "yo": 0.002843438423122505,
    "ys": 0.0013649683669317988
}
english_bigrams = Bigrams(Counter())
english_bigrams.proportional = Counter(english_bigram_frequencies)


class MoneyUtility(object):

    DEFAULT_CURRENCY = 'USD'

    @classmethod
    def parse(cls, amount):
        """Attempt to turn a string into a Money object."""
        currency = cls.DEFAULT_CURRENCY
        if not amount:
            amount = '0'
        if amount[0] == '$':
            currency = 'USD'
            amount = amount[1:]
        return Money(amount, currency)

