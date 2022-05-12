import logging

from enum import Enum
from lxml import etree

from core.classifier import Classifier
from core.metadata_layer import (
    Metadata,
    IdentifierData,
    SubjectData,
    ContributorData,
    LinkData,
    CirculationData)
from core.model import (
    Classification,
    Identifier,
    Contributor,
    Hyperlink,
    Representation,
    Subject,
    LicensePool, EditionConstants)
from core.util.datetime_helpers import strptime_utc
from core.util.xmlparser import XMLParser


class UsageStatus(Enum):
    UNLIMITED = '01'
    LIMITED = '02'
    PROHIBITED = '03'


class UsageUnit(Enum):
    COPIES = '01'
    CHARACTERS = '02'
    WORDS = '03'
    PAGES = '04'
    PERCENTAGE = '05'
    DEVICES = '06'
    CONCURRENT_USERS = '07'
    PERCENTAGE_PER_TIME_PERIOD = '08'
    DAYS = '09'
    TIMES = '10'


class ONIXExtractor(object):
    """Transform an ONIX file into a list of Metadata objects."""

    # TODO: '20' indicates a semicolon-separated list of freeform tags,
    # which could also be useful.
    SUBJECT_TYPES = {
        '01': Classifier.DDC,
        '03': Classifier.LCC,
        '04': Classifier.LCSH,
        '10': Classifier.BISAC,
        '12': Classifier.BIC,
    }

    AUDIENCE_TYPES = {
        '01': Classifier.AUDIENCE_ADULT, # General/trade for adult audience
        '02': Classifier.AUDIENCE_CHILDREN, # (not for educational purpose)
        '03': Classifier.AUDIENCE_YOUNG_ADULT, # (not for educational purpose)
        '04': Classifier.AUDIENCE_CHILDREN, # Primary and secondary/elementary and high school
        '05': Classifier.AUDIENCE_ADULT, # College/higher education
        '06': Classifier.AUDIENCE_ADULT, # Professional and scholarly
        '07': Classifier.AUDIENCE_ADULT, # ESL
        '08': Classifier.AUDIENCE_ADULT, # Adult education
        '09': Classifier.AUDIENCE_ADULT, # Second language teaching other than English
    }

    CONTRIBUTOR_TYPES = {
        'A01': Contributor.AUTHOR_ROLE,
        'A02': Contributor.AUTHOR_ROLE, # 'With or as told to'
        'A03': Contributor.AUTHOR_ROLE, # Screenplay author
        'A04': Contributor.LYRICIST_ROLE, # Libretto author for an opera
        'A05': Contributor.LYRICIST_ROLE,
        'A06': Contributor.COMPOSER_ROLE,
        'A07': Contributor.ILLUSTRATOR_ROLE, # Visual artist who is the primary creator of the work
        'A08': Contributor.PHOTOGRAPHER_ROLE,
        'A09': Contributor.AUTHOR_ROLE, # 'Created by'
        'A10': Contributor.UNKNOWN_ROLE, # 'From an idea by'
        'A11': Contributor.DESIGNER_ROLE,
        'A12': Contributor.ILLUSTRATOR_ROLE,
        'A13': Contributor.PHOTOGRAPHER_ROLE,
        'A14': Contributor.AUTHOR_ROLE, # Author of the text for a work that is primarily photos or illustrations
        'A15': Contributor.INTRODUCTION_ROLE, # Preface author
        'A16': Contributor.UNKNOWN_ROLE, # Prologue author
        'A17': Contributor.UNKNOWN_ROLE, # Summary author
        'A18': Contributor.UNKNOWN_ROLE, # Supplement author
        'A19': Contributor.AFTERWORD_ROLE, # Afterword author
        'A20': Contributor.UNKNOWN_ROLE, # Author of notes or annotations
        'A21': Contributor.UNKNOWN_ROLE, # Author of commentary on main text
        'A22': Contributor.UNKNOWN_ROLE, # Epilogue author
        'A23': Contributor.FOREWORD_ROLE,
        'A24': Contributor.INTRODUCTION_ROLE,
        'A25': Contributor.UNKNOWN_ROLE, # Author/compiler of footnotes
        'A26': Contributor.UNKNOWN_ROLE, # Author of memoir accompanying main text
        'A27': Contributor.UNKNOWN_ROLE, # Person who carried out experiments reported in the text
        'A29': Contributor.INTRODUCTION_ROLE, # Author of introduction and notes
        'A30': Contributor.UNKNOWN_ROLE, # Writer of computer programs ancillary to the text
        'A31': Contributor.LYRICIST_ROLE, # 'Book and lyrics by'
        'A32': Contributor.CONTRIBUTOR_ROLE, # 'Contributions by'
        'A33': Contributor.UNKNOWN_ROLE, # Appendix author
        'A34': Contributor.UNKNOWN_ROLE, # Compiler of index
        'A35': Contributor.ARTIST_ROLE, # 'Drawings by'
        'A36': Contributor.ARTIST_ROLE, # Cover artist
        'A37': Contributor.UNKNOWN_ROLE, # Responsible for preliminary work on which the work is based
        'A38': Contributor.UNKNOWN_ROLE, # Author of the first edition who is not an author of the current edition
        'A39': Contributor.UNKNOWN_ROLE, # 'Maps by'
        'A40': Contributor.ARTIST_ROLE, # 'Inked or colored by'
        'A41': Contributor.UNKNOWN_ROLE, # 'Paper engineering by'
        'A42': Contributor.UNKNOWN_ROLE, # 'Continued by'
        'A43': Contributor.UNKNOWN_ROLE, # Interviewer
        'A44': Contributor.UNKNOWN_ROLE, # Interviewee
        'A45': Contributor.AUTHOR_ROLE, # Writer of dialogue, captions in a comic book
        'A46': Contributor.ARTIST_ROLE, # Inker
        'A47': Contributor.ARTIST_ROLE, # Colorist
        'A48': Contributor.ARTIST_ROLE, # Letterer
        'A51': Contributor.UNKNOWN_ROLE, # 'Research by'
        'A99': Contributor.UNKNOWN_ROLE, # 'Other primary creator'
        'B01': Contributor.EDITOR_ROLE,
        'B02': Contributor.EDITOR_ROLE, # 'Revised by'
        'B03': Contributor.UNKNOWN_ROLE, # 'Retold by'
        'B04': Contributor.UNKNOWN_ROLE, # 'Abridged by'
        'B05': Contributor.ADAPTER_ROLE,
        'B06': Contributor.TRANSLATOR_ROLE,
        'B07': Contributor.UNKNOWN_ROLE, # 'As told by'
        'B08': Contributor.TRANSLATOR_ROLE, # With commentary on the translation
        'B09': Contributor.EDITOR_ROLE, # Series editor
        'B10': Contributor.TRANSLATOR_ROLE, # 'Edited and translated by'
        'B11': Contributor.EDITOR_ROLE, # Editor-in-chief
        'B12': Contributor.EDITOR_ROLE, # Guest editor
        'B13': Contributor.EDITOR_ROLE, # Volume editor
        'B14': Contributor.EDITOR_ROLE, # Editorial board member
        'B15': Contributor.EDITOR_ROLE, # 'Editorial coordination by'
        'B16': Contributor.EDITOR_ROLE, # Managing editor
        'B17': Contributor.EDITOR_ROLE, # Founding editor of a serial publication
        'B18': Contributor.EDITOR_ROLE, # 'Prepared for publication by'
        'B19': Contributor.EDITOR_ROLE, # Associate editor
        'B20': Contributor.EDITOR_ROLE, # Consultant editor
        'B21': Contributor.EDITOR_ROLE, # General editor
        'B22': Contributor.UNKNOWN_ROLE, # 'Dramatized by'
        'B23': Contributor.EDITOR_ROLE, # 'General rapporteur'
        'B24': Contributor.EDITOR_ROLE, # Literary editor
        'B25': Contributor.COMPOSER_ROLE, # 'Arranged by (music)'
        'B26': Contributor.EDITOR_ROLE, # Technical editor
        'B27': Contributor.UNKNOWN_ROLE, # Thesis advisor
        'B28': Contributor.UNKNOWN_ROLE, # Thesis examiner
        'B29': Contributor.EDITOR_ROLE, # Scientific editor
        'B30': Contributor.UNKNOWN_ROLE, # Historical advisor
        'B31': Contributor.UNKNOWN_ROLE, # Editor of the first edition who is not an editor of the current edition
        'B99': Contributor.EDITOR_ROLE, # Other type of adaptation or editing
        'C01': Contributor.UNKNOWN_ROLE, # 'Compiled by'
        'C02': Contributor.UNKNOWN_ROLE, # 'Selected by'
        'C03': Contributor.UNKNOWN_ROLE, # 'Non-text material selected by'
        'C04': Contributor.UNKNOWN_ROLE, # 'Curated by'
        'C99': Contributor.UNKNOWN_ROLE, # Other type of compilation
        'D01': Contributor.PRODUCER_ROLE,
        'D02': Contributor.DIRECTOR_ROLE,
        'D03': Contributor.MUSICIAN_ROLE, # Conductor
        'D04': Contributor.UNKNOWN_ROLE, # Choreographer
        'D05': Contributor.DIRECTOR_ROLE, # Other type of direction
        'E01': Contributor.ACTOR_ROLE,
        'E02': Contributor.PERFORMER_ROLE, # Dancer
        'E03': Contributor.NARRATOR_ROLE, # 'Narrator'
        'E04': Contributor.UNKNOWN_ROLE, # Commentator
        'E05': Contributor.PERFORMER_ROLE, # Vocal soloist
        'E06': Contributor.PERFORMER_ROLE, # Instrumental soloist
        'E07': Contributor.NARRATOR_ROLE, # Reader of recorded text, as in an audiobook
        'E08': Contributor.PERFORMER_ROLE, # Name of a musical group in a performing role
        'E09': Contributor.PERFORMER_ROLE, # Speaker
        'E10': Contributor.UNKNOWN_ROLE, # Presenter
        'E99': Contributor.PERFORMER_ROLE, # Other type of performer
        'F01': Contributor.PHOTOGRAPHER_ROLE, # 'Filmed/photographed by'
        'F02': Contributor.EDITOR_ROLE, # 'Editor (film or video)'
        'F99': Contributor.UNKNOWN_ROLE, # Other type of recording
        'Z01': Contributor.UNKNOWN_ROLE, # 'Assisted by'
        'Z02': Contributor.UNKNOWN_ROLE, # 'Honored/dedicated to'
        'Z99': Contributor.UNKNOWN_ROLE, # Other creative responsibility
    }

    PRODUCT_CONTENT_TYPES = {
        '10': EditionConstants.BOOK_MEDIUM,  # Text (eye-readable)
        '01': EditionConstants.AUDIO_MEDIUM  # Audiobook
    }

    _logger = logging.getLogger(__name__)

    @classmethod
    def parse(cls, file, data_source_name, default_medium=None):
        metadata_records = []

        # TODO: ONIX has plain language 'reference names' and short tags that
        # may be used interchangably. This code currently only handles short tags,
        # and it's not comprehensive.

        parser = XMLParser()
        tree = etree.parse(file)
        root = tree.getroot()

        for record in root.findall('product'):
            title = parser.text_of_optional_subtag(record, 'descriptivedetail/titledetail/titleelement/b203')
            if not title:
                title_prefix = parser.text_of_optional_subtag(record, 'descriptivedetail/titledetail/titleelement/b030')
                title_without_prefix = parser.text_of_optional_subtag(record, 'descriptivedetail/titledetail/titleelement/b031')
                if title_prefix and title_without_prefix:
                    title = title_prefix + " " + title_without_prefix

            medium = parser.text_of_optional_subtag(record, 'b385')

            if not medium and default_medium:
                medium = default_medium
            else:
                medium = cls.PRODUCT_CONTENT_TYPES.get(medium, EditionConstants.BOOK_MEDIUM)

            subtitle = parser.text_of_optional_subtag(record, 'descriptivedetail/titledetail/titleelement/b029')
            language = parser.text_of_optional_subtag(record, 'descriptivedetail/language/b252') or "eng"
            publisher = parser.text_of_optional_subtag(record, 'publishingdetail/publisher/b081')
            imprint = parser.text_of_optional_subtag(record, 'publishingdetail/imprint/b079')
            if imprint == publisher:
                imprint = None

            publishing_date = parser.text_of_optional_subtag(record, 'publishingdetail/publishingdate/b306')
            issued = None
            if publishing_date:
                issued = strptime_utc(publishing_date, "%Y%m%d")

            identifier_tags = parser._xpath(record, 'productidentifier')
            identifiers = []
            primary_identifier = None
            for tag in identifier_tags:
                type = parser.text_of_subtag(tag, "b221")
                if type == '02' or type == '15':
                    primary_identifier = IdentifierData(Identifier.ISBN, parser.text_of_subtag(tag, 'b244'))
                    identifiers.append(primary_identifier)

            subject_tags = parser._xpath(record, 'descriptivedetail/subject')
            subjects = []

            weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT
            for tag in subject_tags:
                type = parser.text_of_subtag(tag, 'b067')
                if type in cls.SUBJECT_TYPES:
                    subjects.append(
                        SubjectData(
                            cls.SUBJECT_TYPES[type],
                            parser.text_of_subtag(tag, 'b069'),
                            weight=weight
                        )
                    )

            audience_tags = parser._xpath(record, 'descriptivedetail/audience/b204')
            audiences = []
            for tag in audience_tags:
                if tag.text in cls.AUDIENCE_TYPES:
                    subjects.append(
                        SubjectData(
                            Subject.FREEFORM_AUDIENCE,
                            cls.AUDIENCE_TYPES[tag.text],
                            weight=weight
                        )
                    )

            contributor_tags = parser._xpath(record, 'descriptivedetail/contributor')
            contributors = []
            for tag in contributor_tags:
                type = parser.text_of_subtag(tag, 'b035')
                if type in cls.CONTRIBUTOR_TYPES:
                    display_name = parser.text_of_subtag(tag, 'b036')
                    sort_name = parser.text_of_optional_subtag(tag, 'b037')
                    family_name = parser.text_of_optional_subtag(tag, 'b040')
                    bio = parser.text_of_optional_subtag(tag, 'b044')
                    contributors.append(ContributorData(sort_name=sort_name,
                                                        display_name=display_name,
                                                        family_name=family_name,
                                                        roles=[cls.CONTRIBUTOR_TYPES[type]],
                                                        biography=bio))

            collateral_tags = parser._xpath(record, 'collateraldetail/textcontent')
            links = []
            for tag in collateral_tags:
                type = parser.text_of_subtag(tag, 'x426')
                # TODO: '03' is the summary in the example I'm testing, but that
                # might not be generally true.
                if type == '03':
                    text = parser.text_of_subtag(tag, 'd104')
                    links.append(LinkData(rel=Hyperlink.DESCRIPTION,
                                          media_type=Representation.TEXT_HTML_MEDIA_TYPE,
                                          content=text))

            usage_constraint_tags = parser._xpath(record, 'descriptivedetail/epubusageconstraint')
            licenses_owned = LicensePool.UNLIMITED_ACCESS

            if usage_constraint_tags:
                cls._logger.debug('Found {0} EpubUsageConstraint tags'.format(len(usage_constraint_tags)))

            for usage_constraint_tag in usage_constraint_tags:
                usage_status = parser.text_of_subtag(usage_constraint_tag, 'x319')

                cls._logger.debug('EpubUsageStatus: {0}'.format(usage_status))

                if usage_status == UsageStatus.PROHIBITED.value:
                    raise Exception('The content is prohibited')
                elif usage_status == UsageStatus.LIMITED.value:
                    usage_limit_tags = parser._xpath(record, 'descriptivedetail/epubusageconstraint/epubusagelimit')

                    cls._logger.debug('Found {0} EpubUsageLimit tags'.format(len(usage_limit_tags)))

                    if not usage_limit_tags:
                        continue

                    [usage_limit_tag] = usage_limit_tags

                    usage_unit = parser.text_of_subtag(usage_limit_tag, 'x321')

                    cls._logger.debug('EpubUsageUnit: {0}'.format(usage_unit))

                    if usage_unit == UsageUnit.COPIES.value or usage_status == UsageUnit.CONCURRENT_USERS.value:
                        quantity_limit = parser.text_of_subtag(usage_limit_tag, 'x320')

                        cls._logger.debug('Quantity: {0}'.format(quantity_limit))

                        if licenses_owned == LicensePool.UNLIMITED_ACCESS:
                            licenses_owned = 0

                        licenses_owned += int(quantity_limit)

            metadata_records.append(Metadata(
                data_source=data_source_name,
                title=title,
                subtitle=subtitle,
                language=language,
                medium=medium,
                publisher=publisher,
                imprint=imprint,
                issued=issued,
                primary_identifier=primary_identifier,
                identifiers=identifiers,
                subjects=subjects,
                contributors=contributors,
                links=links,
                circulation=CirculationData(
                    data_source_name,
                    primary_identifier,
                    licenses_owned=licenses_owned,
                    licenses_available=licenses_owned,
                    licenses_reserved=0,
                    patrons_in_hold_queue=0
                )
            ))

        return metadata_records
