# encoding: utf-8
class DataSourceConstants(object):
    GUTENBERG = u"Gutenberg"
    OVERDRIVE = u"Overdrive"
    ODILO = u"Odilo"
    PROJECT_GITENBERG = u"Project GITenberg"
    STANDARD_EBOOKS = u"Standard Ebooks"
    UNGLUE_IT = u"unglue.it"
    BIBLIOTHECA = u"Bibliotheca"
    OCLC = u"OCLC Classify"
    OCLC_LINKED_DATA = u"OCLC Linked Data"
    AMAZON = u"Amazon"
    XID = u"WorldCat xID"
    AXIS_360 = u"Axis 360"
    WEB = u"Web"
    OPEN_LIBRARY = u"Open Library"
    CONTENT_CAFE = u"Content Cafe"
    VIAF = u"VIAF"
    GUTENBERG_COVER_GENERATOR = u"Gutenberg Illustrated"
    GUTENBERG_EPUB_GENERATOR = u"Project Gutenberg EPUB Generator"
    METADATA_WRANGLER = u"Library Simplified metadata wrangler"
    MANUAL = u"Manual intervention"
    NOVELIST = u"NoveList Select"
    NYT = u"New York Times"
    NYPL_SHADOWCAT = u"NYPL Shadowcat"
    LIBRARY_STAFF = u"Library staff"
    ADOBE = u"Adobe DRM"
    PLYMPTON = u"Plympton"
    RB_DIGITAL = u"RBdigital"
    ELIB = u"eLiburutegia"
    OA_CONTENT_SERVER = u"Library Simplified Open Access Content Server"
    PRESENTATION_EDITION = u"Presentation edition generator"
    INTERNAL_PROCESSING = u"Library Simplified Internal Process"
    FEEDBOOKS = u"FeedBooks"
    BIBBLIO = u"Bibblio"
    ENKI = u"Enki"

    DEPRECATED_NAMES = {
        u"3M" : BIBLIOTHECA,
        u"OneClick" : RB_DIGITAL,
    }
    THREEM = BIBLIOTHECA
    ONECLICK = RB_DIGITAL

    # Some sources of open-access ebooks are better than others. This
    # list shows which sources we prefer, in ascending order of
    # priority. unglue.it is lowest priority because it tends to
    # aggregate books from other sources. We prefer books from their
    # original sources.
    OPEN_ACCESS_SOURCE_PRIORITY = [
        UNGLUE_IT,
        GUTENBERG,
        GUTENBERG_EPUB_GENERATOR,
        PROJECT_GITENBERG,
        ELIB,
        FEEDBOOKS,
        PLYMPTON,
        STANDARD_EBOOKS,
    ]

    # When we're generating the presentation edition for a
    # LicensePool, editions are processed based on their data source,
    # in the following order:
    #
    # [all other sources] < [source of the license pool] < [metadata
    # wrangler] < [library staff] < [manual intervention]
    #
    # This list keeps track of the high-priority portion of that
    # ordering.
    #
    # "LIBRARY_STAFF" comes from the Admin Interface.
    # "MANUAL" is not currently used, but will give the option of putting in
    # software engineer-created system overrides.
    PRESENTATION_EDITION_PRIORITY = [METADATA_WRANGLER, LIBRARY_STAFF, MANUAL]
