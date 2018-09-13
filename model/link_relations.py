# encoding: utf-8

class LinkRelations(object):
    # Some common link relations.
    CANONICAL = u"canonical"
    GENERIC_OPDS_ACQUISITION = u"http://opds-spec.org/acquisition"
    OPEN_ACCESS_DOWNLOAD = u"http://opds-spec.org/acquisition/open-access"
    IMAGE = u"http://opds-spec.org/image"
    THUMBNAIL_IMAGE = u"http://opds-spec.org/image/thumbnail"
    SAMPLE = u"http://opds-spec.org/acquisition/sample"
    ILLUSTRATION = u"http://librarysimplified.org/terms/rel/illustration"
    REVIEW = u"http://schema.org/Review"
    DESCRIPTION = u"http://schema.org/description"
    SHORT_DESCRIPTION = u"http://librarysimplified.org/terms/rel/short-description"
    AUTHOR = u"http://schema.org/author"
    ALTERNATE = u"alternate"

    # TODO: Is this the appropriate relation?
    DRM_ENCRYPTED_DOWNLOAD = u"http://opds-spec.org/acquisition/"
    BORROW = u"http://opds-spec.org/acquisition/borrow"

    CIRCULATION_ALLOWED = [OPEN_ACCESS_DOWNLOAD, DRM_ENCRYPTED_DOWNLOAD, BORROW, GENERIC_OPDS_ACQUISITION]
    METADATA_ALLOWED = [CANONICAL, IMAGE, THUMBNAIL_IMAGE, ILLUSTRATION, REVIEW,
        DESCRIPTION, SHORT_DESCRIPTION, AUTHOR, ALTERNATE, SAMPLE]
    MIRRORED = [OPEN_ACCESS_DOWNLOAD, IMAGE, THUMBNAIL_IMAGE]
