# encoding: utf-8
class EditionConstants(object):
    ALL_MEDIUM = object()
    BOOK_MEDIUM = u"Book"
    PERIODICAL_MEDIUM = u"Periodical"
    AUDIO_MEDIUM = u"Audio"
    MUSIC_MEDIUM = u"Music"
    VIDEO_MEDIUM = u"Video"
    IMAGE_MEDIUM = u"Image"
    COURSEWARE_MEDIUM = u"Courseware"

    ELECTRONIC_FORMAT = u"Electronic"
    CODEX_FORMAT = u"Codex"

    # These are the media types currently fulfillable by the default
    # client.
    FULFILLABLE_MEDIA = [BOOK_MEDIUM, AUDIO_MEDIUM]

    medium_to_additional_type = {
        BOOK_MEDIUM : u"http://schema.org/EBook",
        AUDIO_MEDIUM : u"http://bib.schema.org/Audiobook",
        PERIODICAL_MEDIUM : u"http://schema.org/PublicationIssue",
        MUSIC_MEDIUM :  u"http://schema.org/MusicRecording",
        VIDEO_MEDIUM :  u"http://schema.org/VideoObject",
        IMAGE_MEDIUM: u"http://schema.org/ImageObject",
        COURSEWARE_MEDIUM: u"http://schema.org/Course"
    }

    additional_type_to_medium = {}
    for k, v in medium_to_additional_type.items():
        additional_type_to_medium[v] = k
