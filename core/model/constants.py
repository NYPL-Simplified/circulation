# encoding: utf-8
# DataSourceConstants, EditionConstants, IdentifierConstants, LinkRelations,
# MediaTypes

import re
from collections import OrderedDict

class DataSourceConstants(object):
    GUTENBERG = "Gutenberg"
    OVERDRIVE = "Overdrive"
    ODILO = "Odilo"
    PROJECT_GITENBERG = "Project GITenberg"
    STANDARD_EBOOKS = "Standard Ebooks"
    UNGLUE_IT = "unglue.it"
    BIBLIOTHECA = "Bibliotheca"
    OCLC = "OCLC Classify"
    OCLC_LINKED_DATA = "OCLC Linked Data"
    AMAZON = "Amazon"
    XID = "WorldCat xID"
    AXIS_360 = "Axis 360"
    WEB = "Web"
    OPEN_LIBRARY = "Open Library"
    CONTENT_CAFE = "Content Cafe"
    VIAF = "VIAF"
    GUTENBERG_COVER_GENERATOR = "Gutenberg Illustrated"
    GUTENBERG_EPUB_GENERATOR = "Project Gutenberg EPUB Generator"
    METADATA_WRANGLER = "Library Simplified metadata wrangler"
    MANUAL = "Manual intervention"
    NOVELIST = "NoveList Select"
    NYT = "New York Times"
    NYPL_SHADOWCAT = "NYPL Shadowcat"
    LIBRARY_STAFF = "Library staff"
    ADOBE = "Adobe DRM"
    PLYMPTON = "Plympton"
    RB_DIGITAL = "RBdigital"
    ELIB = "eLiburutegia"
    OA_CONTENT_SERVER = "Library Simplified Open Access Content Server"
    PRESENTATION_EDITION = "Presentation edition generator"
    INTERNAL_PROCESSING = "Library Simplified Internal Process"
    FEEDBOOKS = "FeedBooks"
    BIBBLIO = "Bibblio"
    ENKI = "Enki"
    LCP = "LCP"
    PROQUEST = "ProQuest"

    DEPRECATED_NAMES = {
        "3M" : BIBLIOTHECA,
        "OneClick" : RB_DIGITAL,
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
    # [all other sources] < [metadata wrangler] < [source of the license pool]
    # < [library staff] < [manual intervention]
    #
    # This list keeps track of the portion of that ordering that's
    # higher priority than the source of the license pool.
    #
    # "LIBRARY_STAFF" comes from the Admin Interface.
    # "MANUAL" is not currently used, but will give the option of putting in
    # software engineer-created system overrides.
    PRESENTATION_EDITION_PRIORITY = [LIBRARY_STAFF, MANUAL]

    # When we're finding the cover image for a book, images from these
    # sources are given priority, in the following order:
    #
    # [Open Library] < [Project Gutenberg] < [Gutenberg cover
    # generator] < [all other data sources] < [metadata wrangler] <
    # [the presentation edition priority sources]
    #
    # This list keeps track of the portion of that ordering that's
    # higher priority than the source of the license pool.
    COVER_IMAGE_PRIORITY = [METADATA_WRANGLER] + PRESENTATION_EDITION_PRIORITY

class EditionConstants(object):
    ALL_MEDIUM = object()
    BOOK_MEDIUM = "Book"
    PERIODICAL_MEDIUM = "Periodical"
    AUDIO_MEDIUM = "Audio"
    MUSIC_MEDIUM = "Music"
    VIDEO_MEDIUM = "Video"
    IMAGE_MEDIUM = "Image"
    COURSEWARE_MEDIUM = "Courseware"

    ELECTRONIC_FORMAT = "Electronic"
    CODEX_FORMAT = "Codex"

    # These are all media known to the system.
    KNOWN_MEDIA = (BOOK_MEDIUM, PERIODICAL_MEDIUM, AUDIO_MEDIUM, MUSIC_MEDIUM,
                   VIDEO_MEDIUM, IMAGE_MEDIUM, COURSEWARE_MEDIUM)

    # These are the media types currently fulfillable by the default
    # client.
    FULFILLABLE_MEDIA = [BOOK_MEDIUM, AUDIO_MEDIUM]

    medium_to_additional_type = {
        BOOK_MEDIUM : "http://schema.org/EBook",
        AUDIO_MEDIUM : "http://bib.schema.org/Audiobook",
        PERIODICAL_MEDIUM : "http://schema.org/PublicationIssue",
        MUSIC_MEDIUM :  "http://schema.org/MusicRecording",
        VIDEO_MEDIUM :  "http://schema.org/VideoObject",
        IMAGE_MEDIUM: "http://schema.org/ImageObject",
        COURSEWARE_MEDIUM: "http://schema.org/Course"
    }

    additional_type_to_medium = {}
    for k, v in list(medium_to_additional_type.items()):
        additional_type_to_medium[v] = k

    additional_type_to_medium["http://schema.org/Book"] = BOOK_MEDIUM

    # Map the medium constants to the strings used when generating
    # permanent work IDs.
    medium_for_permanent_work_id = {
        BOOK_MEDIUM : "book",
        AUDIO_MEDIUM : "book",
        MUSIC_MEDIUM : "music",
        PERIODICAL_MEDIUM : "book",
        VIDEO_MEDIUM: "movie",
        IMAGE_MEDIUM: "image",
        COURSEWARE_MEDIUM: "courseware"
    }


class IdentifierConstants(object):
    # Common types of identifiers.
    OVERDRIVE_ID = "Overdrive ID"
    ODILO_ID = "Odilo ID"
    BIBLIOTHECA_ID = "Bibliotheca ID"
    GUTENBERG_ID = "Gutenberg ID"
    AXIS_360_ID = "Axis 360 ID"
    ELIB_ID = "eLiburutegia ID"
    ASIN = "ASIN"
    ISBN = "ISBN"
    NOVELIST_ID = "NoveList ID"
    OCLC_WORK = "OCLC Work ID"
    OCLC_NUMBER = "OCLC Number"
    # RBdigital uses ISBNs for ebooks and eaudio, and its own ids for magazines
    RB_DIGITAL_ID = "RBdigital ID"
    OPEN_LIBRARY_ID = "OLID"
    BIBLIOCOMMONS_ID = "Bibliocommons ID"
    URI = "URI"
    DOI = "DOI"
    UPC = "UPC"
    BIBBLIO_CONTENT_ITEM_ID = "Bibblio Content Item ID"
    ENKI_ID = "Enki ID"
    SUDOC_CALL_NUMBER = "SuDoc Call Number"
    PROQUEST_ID = "ProQuest Doc ID"

    DEPRECATED_NAMES = {
        "3M ID" : BIBLIOTHECA_ID,
        "OneClick ID" : RB_DIGITAL_ID,
    }
    THREEM_ID = BIBLIOTHECA_ID
    ONECLICK_ID = RB_DIGITAL_ID

    LICENSE_PROVIDING_IDENTIFIER_TYPES = [
        BIBLIOTHECA_ID, OVERDRIVE_ID, ODILO_ID, AXIS_360_ID,
        GUTENBERG_ID, ELIB_ID, SUDOC_CALL_NUMBER,
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


class LinkRelations(object):
    # Some common link relations.
    CANONICAL = "canonical"
    GENERIC_OPDS_ACQUISITION = "http://opds-spec.org/acquisition"
    OPEN_ACCESS_DOWNLOAD = "http://opds-spec.org/acquisition/open-access"
    IMAGE = "http://opds-spec.org/image"
    THUMBNAIL_IMAGE = "http://opds-spec.org/image/thumbnail"
    SAMPLE = "http://opds-spec.org/acquisition/sample"
    ILLUSTRATION = "http://librarysimplified.org/terms/rel/illustration"
    REVIEW = "http://schema.org/Review"
    DESCRIPTION = "http://schema.org/description"
    SHORT_DESCRIPTION = "http://librarysimplified.org/terms/rel/short-description"
    AUTHOR = "http://schema.org/author"
    ALTERNATE = "alternate"

    # TODO: Is this the appropriate relation?
    DRM_ENCRYPTED_DOWNLOAD = "http://opds-spec.org/acquisition/"
    BORROW = "http://opds-spec.org/acquisition/borrow"

    CIRCULATION_ALLOWED = [OPEN_ACCESS_DOWNLOAD, DRM_ENCRYPTED_DOWNLOAD, BORROW, GENERIC_OPDS_ACQUISITION]
    METADATA_ALLOWED = [CANONICAL, IMAGE, THUMBNAIL_IMAGE, ILLUSTRATION, REVIEW,
        DESCRIPTION, SHORT_DESCRIPTION, AUTHOR, ALTERNATE, SAMPLE]
    MIRRORED = [OPEN_ACCESS_DOWNLOAD, GENERIC_OPDS_ACQUISITION, IMAGE, THUMBNAIL_IMAGE]
    SELF_HOSTED_BOOKS = list(set(CIRCULATION_ALLOWED) & set(MIRRORED))


class MediaTypes(object):
    EPUB_MEDIA_TYPE = "application/epub+zip"
    PDF_MEDIA_TYPE = "application/pdf"
    MOBI_MEDIA_TYPE = "application/x-mobipocket-ebook"
    AMAZON_KF8_MEDIA_TYPE = "application/x-mobi8-ebook"
    TEXT_XML_MEDIA_TYPE = "text/xml"
    TEXT_HTML_MEDIA_TYPE = "text/html"
    APPLICATION_XML_MEDIA_TYPE = "application/xml"
    JPEG_MEDIA_TYPE = "image/jpeg"
    PNG_MEDIA_TYPE = "image/png"
    GIF_MEDIA_TYPE = "image/gif"
    SVG_MEDIA_TYPE = "image/svg+xml"
    MP3_MEDIA_TYPE = "audio/mpeg"
    MP4_MEDIA_TYPE = "video/mp4"
    WMV_MEDIA_TYPE = "video/x-ms-wmv"
    SCORM_MEDIA_TYPE = "application/vnd.librarysimplified.scorm+zip"
    ZIP_MEDIA_TYPE = "application/zip"
    OCTET_STREAM_MEDIA_TYPE = "application/octet-stream"
    TEXT_PLAIN = "text/plain"
    AUDIOBOOK_MANIFEST_MEDIA_TYPE = "application/audiobook+json"
    AUDIOBOOK_PACKAGE_MEDIA_TYPE = "application/audiobook+zip"
    MARC_MEDIA_TYPE = "application/marc"

    # To distinguish internally between Overdrive's audiobook and
    # (hopefully future) ebook manifests, we invent values for the
    # 'profile' parameter.
    OVERDRIVE_MANIFEST_MEDIA_TYPE = "application/vnd.overdrive.circulation.api+json"
    OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE = OVERDRIVE_MANIFEST_MEDIA_TYPE + ";profile=audiobook"
    OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE = OVERDRIVE_MANIFEST_MEDIA_TYPE + ";profile=ebook"

    AUDIOBOOK_MEDIA_TYPES = [
        OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
        AUDIOBOOK_MANIFEST_MEDIA_TYPE,
        AUDIOBOOK_PACKAGE_MEDIA_TYPE
    ]

    BOOK_MEDIA_TYPES = [
        EPUB_MEDIA_TYPE,
        PDF_MEDIA_TYPE,
        MOBI_MEDIA_TYPE,
        MP3_MEDIA_TYPE,
        AMAZON_KF8_MEDIA_TYPE,
    ]

    # These media types are in the order we would prefer to use them.
    # e.g. all else being equal, we would prefer a PNG to a JPEG.
    IMAGE_MEDIA_TYPES = [
        PNG_MEDIA_TYPE,
        JPEG_MEDIA_TYPE,
        GIF_MEDIA_TYPE,
        SVG_MEDIA_TYPE,
    ]

    # If an open access book is imported and not any of these media types,
    # then it won't show up in an OPDS feed.
    SUPPORTED_BOOK_MEDIA_TYPES = [
        EPUB_MEDIA_TYPE,
        PDF_MEDIA_TYPE,
        AUDIOBOOK_MANIFEST_MEDIA_TYPE
    ]

    # Most of the time, if you believe a resource to be media type A,
    # but then you make a request and get media type B, then the
    # actual media type (B) takes precedence over what you thought it
    # was (A). These media types are the exceptions: they are so
    # generic that they don't tell you anything, so it's more useful
    # to stick with A.
    GENERIC_MEDIA_TYPES = [OCTET_STREAM_MEDIA_TYPE]

    FILE_EXTENSIONS = OrderedDict(
        [
            (EPUB_MEDIA_TYPE, "epub"),
            (MOBI_MEDIA_TYPE, "mobi"),
            (PDF_MEDIA_TYPE, "pdf"),
            (MP3_MEDIA_TYPE, "mp3"),
            (MP4_MEDIA_TYPE, "mp4"),
            (WMV_MEDIA_TYPE, "wmv"),
            (JPEG_MEDIA_TYPE, "jpg"),
            (PNG_MEDIA_TYPE, "png"),
            (SVG_MEDIA_TYPE, "svg"),
            (GIF_MEDIA_TYPE, "gif"),
            (ZIP_MEDIA_TYPE, "zip"),
            (TEXT_PLAIN, "txt"),
            (TEXT_HTML_MEDIA_TYPE, "html"),
            (APPLICATION_XML_MEDIA_TYPE, "xml"),
            (AUDIOBOOK_MANIFEST_MEDIA_TYPE, "audiobook-manifest"),
            (AUDIOBOOK_PACKAGE_MEDIA_TYPE, "audiobook"),
            (SCORM_MEDIA_TYPE, "zip")
        ]
    )

    COMMON_EBOOK_EXTENSIONS = ['.epub', '.pdf', '.audiobook']
    COMMON_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

    # Invert FILE_EXTENSIONS and add some extra guesses.
    MEDIA_TYPE_FOR_EXTENSION = {
        ".htm" : TEXT_HTML_MEDIA_TYPE,
        ".jpeg" : JPEG_MEDIA_TYPE,
    }

    for media_type, extension in list(FILE_EXTENSIONS.items()):
        extension = '.' + extension
        if extension not in MEDIA_TYPE_FOR_EXTENSION:
            # FILE_EXTENSIONS lists more common extensions first.  If
            # multiple media types have the same extension, the most
            # common media type will be used.
            MEDIA_TYPE_FOR_EXTENSION[extension] = media_type
