# encoding: utf-8
class MediaTypes(object):
    EPUB_MEDIA_TYPE = u"application/epub+zip"
    PDF_MEDIA_TYPE = u"application/pdf"
    MOBI_MEDIA_TYPE = u"application/x-mobipocket-ebook"
    AMAZON_KF8_MEDIA_TYPE = u"application/x-mobi8-ebook"
    TEXT_XML_MEDIA_TYPE = u"text/xml"
    TEXT_HTML_MEDIA_TYPE = u"text/html"
    APPLICATION_XML_MEDIA_TYPE = u"application/xml"
    JPEG_MEDIA_TYPE = u"image/jpeg"
    PNG_MEDIA_TYPE = u"image/png"
    GIF_MEDIA_TYPE = u"image/gif"
    SVG_MEDIA_TYPE = u"image/svg+xml"
    MP3_MEDIA_TYPE = u"audio/mpeg"
    MP4_MEDIA_TYPE = u"video/mp4"
    WMV_MEDIA_TYPE = u"video/x-ms-wmv"
    SCORM_MEDIA_TYPE = u"application/vnd.librarysimplified.scorm+zip"
    ZIP_MEDIA_TYPE = u"application/zip"
    OCTET_STREAM_MEDIA_TYPE = u"application/octet-stream"
    TEXT_PLAIN = u"text/plain"
    AUDIOBOOK_MANIFEST_MEDIA_TYPE = u"application/audiobook+json"

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

    SUPPORTED_BOOK_MEDIA_TYPES = [
        EPUB_MEDIA_TYPE
    ]

    # Most of the time, if you believe a resource to be media type A,
    # but then you make a request and get media type B, then the
    # actual media type (B) takes precedence over what you thought it
    # was (A). These media types are the exceptions: they are so
    # generic that they don't tell you anything, so it's more useful
    # to stick with A.
    GENERIC_MEDIA_TYPES = [OCTET_STREAM_MEDIA_TYPE]

    FILE_EXTENSIONS = {
        EPUB_MEDIA_TYPE: "epub",
        MOBI_MEDIA_TYPE: "mobi",
        PDF_MEDIA_TYPE: "pdf",
        MP3_MEDIA_TYPE: "mp3",
        MP4_MEDIA_TYPE: "mp4",
        WMV_MEDIA_TYPE: "wmv",
        JPEG_MEDIA_TYPE: "jpg",
        PNG_MEDIA_TYPE: "png",
        SVG_MEDIA_TYPE: "svg",
        GIF_MEDIA_TYPE: "gif",
        ZIP_MEDIA_TYPE: "zip",
        TEXT_PLAIN: "txt",
        TEXT_HTML_MEDIA_TYPE: "html",
        APPLICATION_XML_MEDIA_TYPE: "xml",
        AUDIOBOOK_MANIFEST_MEDIA_TYPE: "audiobook-manifest",
        SCORM_MEDIA_TYPE: "zip"
    }

    COMMON_EBOOK_EXTENSIONS = ['.epub', '.pdf']
    COMMON_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

    # Invert FILE_EXTENSIONS and add some extra guesses.
    MEDIA_TYPE_FOR_EXTENSION = {
        ".htm" : TEXT_HTML_MEDIA_TYPE,
        ".jpeg" : JPEG_MEDIA_TYPE,
    }
    for media_type, extension in FILE_EXTENSIONS.items():
        MEDIA_TYPE_FOR_EXTENSION['.' + extension] = media_type
