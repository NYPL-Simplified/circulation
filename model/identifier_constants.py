# encoding: utf-8
import re
class IdentifierConstants(object):
    # Common types of identifiers.
    OVERDRIVE_ID = u"Overdrive ID"
    ODILO_ID = u"Odilo ID"
    BIBLIOTHECA_ID = u"Bibliotheca ID"
    GUTENBERG_ID = u"Gutenberg ID"
    AXIS_360_ID = u"Axis 360 ID"
    ELIB_ID = u"eLiburutegia ID"
    ASIN = u"ASIN"
    ISBN = u"ISBN"
    NOVELIST_ID = u"NoveList ID"
    OCLC_WORK = u"OCLC Work ID"
    OCLC_NUMBER = u"OCLC Number"
    # RBdigital uses ISBNs for ebooks and eaudio, and its own ids for magazines
    RB_DIGITAL_ID = u"RBdigital ID"
    OPEN_LIBRARY_ID = u"OLID"
    BIBLIOCOMMONS_ID = u"Bibliocommons ID"
    URI = u"URI"
    DOI = u"DOI"
    UPC = u"UPC"
    BIBBLIO_CONTENT_ITEM_ID = u"Bibblio Content Item ID"
    ENKI_ID = u"Enki ID"

    DEPRECATED_NAMES = {
        u"3M ID" : BIBLIOTHECA_ID,
        u"OneClick ID" : RB_DIGITAL_ID,
    }
    THREEM_ID = BIBLIOTHECA_ID
    ONECLICK_ID = RB_DIGITAL_ID

    LICENSE_PROVIDING_IDENTIFIER_TYPES = [
        BIBLIOTHECA_ID, OVERDRIVE_ID, ODILO_ID, AXIS_360_ID,
        GUTENBERG_ID, ELIB_ID
    ]

    URN_SCHEME_PREFIX = "urn:librarysimplified.org/terms/id/"
    ISBN_URN_SCHEME_PREFIX = "urn:isbn:"
    GUTENBERG_URN_SCHEME_PREFIX = "http://www.gutenberg.org/ebooks/"
    GUTENBERG_URN_SCHEME_RE = re.compile(
        GUTENBERG_URN_SCHEME_PREFIX + "([0-9]+)")
    OTHER_URN_SCHEME_PREFIX = "urn:"

    IDEAL_COVER_ASPECT_RATIO = 2.0/3
    IDEAL_IMAGE_HEIGHT = 240
    IDEAL_IMAGE_WIDTH = 160
