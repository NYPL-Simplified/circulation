
import datetime
from io import BytesIO
from flask_babel import lazy_gettext as _
import re

from pymarc import (
    Field,
    Record,
    MARCWriter
)
from .config import (
    Configuration,
    CannotLoadConfiguration,
)
from .lane import BaseFacets
from .external_search import (
    ExternalSearchIndex,
    SortKeyPagination,
)
from .model import (
    get_one,
    get_one_or_create,
    CachedMARCFile,
    Collection,
    ConfigurationSetting,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Identifier,
    Representation,
    Session,
    Work,
)
from .classifier import Classifier
from .mirror import MirrorUploader
from .s3 import S3Uploader
from .lane import Lane
from .util import LanguageCodes

class Annotator(object):
    """The Annotator knows how to add information about a Work to
    a MARC record."""

    marc_cache_field = Work.marc_record.name

    # From https://www.loc.gov/standards/valuelist/marctarget.html
    AUDIENCE_TERMS = {
        Classifier.AUDIENCE_CHILDREN: "Juvenile",
        Classifier.AUDIENCE_YOUNG_ADULT: "Adolescent",
        Classifier.AUDIENCE_ADULTS_ONLY: "Adult",
        Classifier.AUDIENCE_ADULT: "General",
    }

    # TODO: Add remaining formats. Maybe there's a better place to
    # store this so it's easier to keep up-to-date.
    # There doesn't seem to be any particular vocabulary for this.
    FORMAT_TERMS = {
        (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM): "EPUB eBook",
        (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM): "Adobe EPUB eBook",
        (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM): "PDF eBook",
        (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM): "Adobe PDF eBook",
    }

    def annotate_work_record(self, work, active_license_pool, edition,
                             identifier, record, integration=None, updated=None):
        """Add metadata from this work to a MARC record.

        :work: The Work whose record is being annotated.
        :active_license_pool: Of all the LicensePools associated with this
           Work, the client has expressed interest in this one.
        :edition: The Edition to use when associating bibliographic
           metadata with this entry.
        :identifier: Of all the Identifiers associated with this
           Work, the client has expressed interest in this one.
        :param record: A MARCRecord object to be annotated.
        """
        self.add_distributor(record, active_license_pool)
        self.add_formats(record, active_license_pool)

    @classmethod
    def leader(cls, work):
        # The record length is automatically updated once fields are added.
        initial_record_length = "00000"

        record_status = "n" # New record
        if getattr(work, cls.marc_cache_field):
            record_status = "c" # Corrected or revised

        # Distributors consistently seem to use type "a" - language material - for
        # ebooks, though there is also type "m" for computer files.
        record_type = "a"
        bibliographic_level = "m" # Monograph/item
        
        leader = initial_record_length + record_status + record_type + bibliographic_level
        # Additional information about the record that's always the same.
        leader += "  2200000   4500"
        return leader

    @classmethod
    def add_control_fields(cls, record, identifier, pool, edition):
        # Unique identifier for this record.
        record.add_field(
            Field(tag="001", data=identifier.urn))

        # Field 003 (MARC organization code) is library-specific, so it's added separately.

        record.add_field(
            Field(tag="005", data=datetime.datetime.now().strftime("%Y%m%d%H%M%S.0")))

        # Field 006: m = computer file, d = the file is a document
        record.add_field(
            Field(tag="006", data="m        d        "))

        # Field 007: more details about electronic resource
        # Since this depends on the pool, it might be better not to cache it.
        # But it's probably not a huge problem if it's outdated.
        # File formats: a=one format, m=multiple formats, u=unknown
        if len(pool.delivery_mechanisms) == 1:
            file_formats_code = "a"
        else:
            file_formats_code = "m"
        record.add_field(
            Field(tag="007", data="cr cn ---" + file_formats_code + "nuuu"))

        # Field 008 (fixed-length data elements):
        data = datetime.datetime.now().strftime("%y%m%d")
        publication_date = edition.issued or edition.published
        if publication_date:
            date_type = "s" # single known date
            # Not using strftime because some years are pre-1900.
            date_value = "%04i" % publication_date.year
        else:
            date_type = "n" # dates unknown
            date_value = "    "
        data += date_type + date_value
        data += "    "
        # TODO: Start tracking place of publication when available. Since we don't have
        # this yet, assume everything was published in the US.
        data += "xxu"
        data += "                 "
        language = "eng"
        if edition.language:
            language = LanguageCodes.string_to_alpha_3(edition.language)
        data += language
        data += "  "
        record.add_field(
            Field(tag="008", data=data))

    @classmethod
    def add_marc_organization_code(cls, record, marc_org):
        record.add_field(
            Field(tag="003", data=marc_org))

    @classmethod
    def add_isbn(cls, record, identifier):
        # Add the ISBN if we have one.
        isbn = None
        if identifier.type == Identifier.ISBN:
            isbn = identifier
        if not isbn:
            _db = Session.object_session(identifier)
            identifier_ids = identifier.equivalent_identifier_ids()[identifier.id]
            isbn = _db.query(Identifier).filter(
                Identifier.type==Identifier.ISBN).filter(
                Identifier.id.in_(identifier_ids)).order_by(
                Identifier.id).first()
        if isbn:
            record.add_field(
                Field(
                    tag="020",
                    indicators=[" "," "],
                    subfields=[
                        "a", isbn.identifier,
                    ]))

    @classmethod
    def add_title(cls, record, edition):
        # Non-filing characters are used to indicate when the beginning of a title
        # should not be used in sorting. This code tries to identify them by comparing
        # the title and the sort_title.
        non_filing_characters = 0
        if edition.title != edition.sort_title and ("," in edition.sort_title):
            stemmed = edition.sort_title[:edition.sort_title.rindex(",")]
            non_filing_characters = edition.title.index(stemmed)
        # MARC only supports up to 9 non-filing characters, but if we got more
        # something is probably wrong anyway.
        if non_filing_characters > 9:
            non_filing_characters = 0

        subfields = ["a", str(edition.title or "")]
        if edition.subtitle:
            subfields += ["b", str(edition.subtitle)]
        if edition.author:
            subfields += ["c", str(edition.author)]
        record.add_field(
            Field(
                tag="245",
                indicators=["0", non_filing_characters],
                subfields=subfields,
            ))

    @classmethod
    def add_contributors(cls, record, edition):
        """Create contributor fields for this edition.

        TODO: Use canonical names from LoC.
        """
        contibutor_fields = []

        # If there's one author, use the 100 field.
        if edition.sort_author and len(edition.contributions) == 1:
            record.add_field(
                Field(
                    tag="100",
                    indicators=["1"," "],
                    subfields=[
                        "a", str(edition.sort_author),
                    ]))

        if len(edition.contributions) > 1:
            for contribution in edition.contributions:
                contributor = contribution.contributor
                record.add_field(
                    Field(
                        tag="700",
                        indicators=["1", " "],
                        subfields=[
                            "a", str(contributor.sort_name),
                            "e", contribution.role,
                        ]))

    @classmethod
    def add_publisher(cls, record, edition):
        if edition.publisher:
            publication_date = edition.issued or edition.published
            year = ""
            if publication_date:
                year = str(publication_date.year)
            record.add_field(
                Field(
                    tag="264",
                    indicators=[" ", "1"],
                    subfields=[
                        "a", "[Place of publication not identified]",
                        "b", str(edition.publisher or ""),
                        "c", year,
                    ]))

    @classmethod
    def add_distributor(cls, record, pool):
        # Distributor
        record.add_field(
            Field(
                tag="264",
                indicators=[" ", "2"],
                subfields=[
                    "b", str(pool.data_source.name),
                ]))

    @classmethod
    def add_physical_description(cls, record, edition):
        # These 3xx fields are for a physical description of the item.
        if edition.medium == Edition.BOOK_MEDIUM:
            record.add_field(
                Field(
                    tag="300",
                    indicators=[" ", " "],
                    subfields=[
                        "a", "1 online resource",
                    ]))

            record.add_field(
                Field(
                    tag="336",
                    indicators=[" ", " "],
                    subfields=[
                        "a", "text",
                        "b", "txt",
                        "2", "rdacontent"
                    ]))
        elif edition.medium == Edition.AUDIO_MEDIUM:
            record.add_field(
                Field(
                    tag="300",
                    indicators=[" ", " "],
                    subfields=[
                        "a", "1 sound file",
                        "b", "digital",
                    ]))

            record.add_field(
                Field(
                    tag="336",
                    indicators=[" ", " "],
                    subfields=[
                        "a", "spoken word",
                        "b", "spw",
                        "2", "rdacontent"
                    ]))

        record.add_field(
            Field(
                tag="337",
                indicators=[" ", " "],
                subfields=[
                    "a", "computer",
                    "b", "c",
                    "2", "rdamedia"
                ]))

        record.add_field(
            Field(
                tag="338",
                indicators=[" ", " "],
                subfields=[
                    "a", "online resource",
                    "b", "cr",
                    "2", "rdacarrier",
                ]))


        file_type = None
        if edition.medium == Edition.BOOK_MEDIUM:
            file_type = "text file"
        elif edition.medium == Edition.AUDIO_MEDIUM:
            file_type = "audio file"
        if file_type:
            record.add_field(
                Field(
                    tag="347",
                    indicators=[" ", " "],
                    subfields=[
                        "a", file_type,
                        "2", "rda",
                    ]))

        # Form of work
        form = None
        if edition.medium == Edition.BOOK_MEDIUM:
            form = "eBook"
        elif edition.medium == Edition.AUDIO_MEDIUM:
            # This field doesn't seem to be used for audio.
            pass
        if form:
            record.add_field(
                Field(
                    tag="380",
                    indicators=[" ", " "],
                    subfields=[
                        "a", "eBook",
                        "2", "tlcgt",
                    ]))

    @classmethod
    def add_audience(cls, record, work):
        audience = cls.AUDIENCE_TERMS.get(work.audience, "General")
        record.add_field(
            Field(
                tag="385",
                indicators=[" ",  " "],
                subfields=[
                    "a", audience,
                    "2", "tlctarget",
                ]))

    @classmethod
    def add_series(cls, record, edition):
        if edition.series:
            subfields = ["a", str(edition.series)]
            if edition.series_position:
                subfields.extend(["v", str(edition.series_position)])
            record.add_field(
                Field(
                    tag="490",
                    indicators=["0", " "],
                    subfields=subfields,
                    ))

    @classmethod
    def add_system_details(cls, record):
        record.add_field(
            Field(
                tag="538",
                indicators=[" ", " "],
                subfields=[
                    "a", "Mode of access: World Wide Web."
                ]))

    @classmethod
    def add_formats(cls, record, pool):
        formats = []
        for lpdm in pool.delivery_mechanisms:
            format = None
            dm = lpdm.delivery_mechanism
            format = cls.FORMAT_TERMS.get((dm.content_type, dm.drm_scheme))
            if format:
                record.add_field(
                    Field(
                        tag="538",
                        indicators=[" "," "],
                        subfields=[
                            "a", format,
                        ]))


    @classmethod
    def add_summary(cls, record, work):
        summary = work.summary_text
        if summary:
            stripped = re.sub('<[^>]+?>', ' ', summary)
            record.add_field(
                Field(
                    tag="520",
                    indicators=[" ", " "],
                    subfields=[
                        "a", stripped.encode('ascii', 'ignore'),
                    ]))

    @classmethod
    def add_simplified_genres(cls, record, work):
        """Create subject fields for this work."""
        genres = []
        genres = work.genres

        for genre in genres:
            record.add_field(
                Field(
                    tag="650",
                    indicators=["0", "7"],
                    subfields=[
                        "a", genre.name,
                        "2", "Library Simplified",
                    ]))

    @classmethod
    def add_ebooks_subject(cls, record):
        # This is a general subject that can be added to all records.
        record.add_field(
            Field(
                tag="655",
                indicators=[" ", "0"],
                subfields=[
                    "a", "Electronic books.",
                ]))


class MARCExporterFacets(BaseFacets):
    """A faceting object used to configure the search engine so that
    it only works updated since a certain time.
    """

    def __init__(self, start_time):
        self.start_time = start_time

    def modify_search_filter(self, filter):
        filter.order = self.SORT_ORDER_TO_ELASTICSEARCH_FIELD_NAME[
            self.ORDER_LAST_UPDATE
        ]
        filter.order_ascending = True
        filter.updated_after = self.start_time


class MARCExporter(object):
    """Turn a work into a record for a MARC file."""

    NAME = ExternalIntegration.MARC_EXPORT

    DESCRIPTION = _("Export metadata into MARC files that can be imported into an ILS manually.")

    # This setting (in days) controls how often MARC files should be
    # automatically updated. Since the crontab in docker isn't easily
    # configurable, we can run a script daily but check this to decide
    # whether to do anything.
    UPDATE_FREQUENCY = "marc_update_frequency"
    DEFAULT_UPDATE_FREQUENCY = 30

    # MARC organization codes are assigned by the
    # Library of Congress and can be found here:
    # http://www.loc.gov/marc/organizations/org-search.php
    MARC_ORGANIZATION_CODE = "marc_organization_code"

    WEB_CLIENT_URL = 'marc_web_client_url'
    INCLUDE_SUMMARY = 'include_summary'
    INCLUDE_SIMPLIFIED_GENRES = 'include_simplified_genres'

    LIBRARY_SETTINGS = [
        { "key": UPDATE_FREQUENCY,
          "label": _("Update frequency (in days)"),
          "description": _("The circulation manager will wait this number of days between generating MARC files."),
          "type": "number",
          "default": DEFAULT_UPDATE_FREQUENCY,
        },
        { "key": MARC_ORGANIZATION_CODE,
          "label": _("The MARC organization code for this library (003 field)."),
          "description": _("MARC organization codes are assigned by the Library of Congress."),
        },
        {
          "key": WEB_CLIENT_URL,
          "label": _("The base URL for the web catalog for this library, for the 856 field."),
          "description": _("If using a library registry that provides a web catalog, this can be left blank."),
        },
        { "key": INCLUDE_SUMMARY,
          "label": _("Include summaries in MARC records (520 field)"),
          "type": "select",
          "options": [
              { "key": "false", "label": _("Do not include summaries") },
              { "key": "true", "label": _("Include summaries") },
          ],
          "default": "false",
        },
        { "key": INCLUDE_SIMPLIFIED_GENRES,
          "label": _("Include Library Simplified genres in MARC records (650 fields)"),
          "type": "select",
          "options": [
              { "key": "false", "label": _("Do not include Library Simplified genres") },
              { "key": "true", "label": _("Include Library Simplified genres") },
          ],
          "default": "false",
        },
    ]

    NO_MIRROR_INTEGRATION = "NO_MIRROR"
    DEFAULT_MIRROR_INTEGRATION = dict(
        key=NO_MIRROR_INTEGRATION,
        label=_("None - Do not mirror MARC files")
    )
    SETTING = {
        "key": "mirror_integration_id",
        "label": _("MARC Mirror"),
        "description": _("Storage protocol to use for uploading generated MARC files. The service must already be configured under 'Storage Services'."),
        "type": "select",
        "options" : [DEFAULT_MIRROR_INTEGRATION]
    }

    @classmethod
    def from_config(cls, library):
        _db = Session.object_session(library)
        integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL, library=library
        )
        if not integration:
            raise CannotLoadConfiguration(
                "No MARC export service is configured for this library"
            )
        return cls(_db, library, integration)

    def __init__(self, _db, library, integration):
        self._db = _db
        self.library = library
        self.integration = integration
        
    @classmethod
    def get_storage_settings(cls, _db):
        integrations = ExternalIntegration.for_goal(
            _db, ExternalIntegration.STORAGE_GOAL
        )
        cls.SETTING['options'] = [cls.DEFAULT_MIRROR_INTEGRATION]
        for integration in integrations:
            # Only add an integration to choose from if it has a 
            # MARC File Bucket field in its settings.
            configuration_settings = [s for s in integration.settings if s.key=="marc_bucket"]

            if configuration_settings:
                if configuration_settings[0].value:
                    cls.SETTING['options'].append(
                        dict(key=str(integration.id), label=integration.name)
                    )
        
        return cls.SETTING


    @classmethod
    def create_record(cls, work, annotator, force_create=False, integration=None):
        """Build a complete MARC record for a given work."""
        if callable(annotator):
            annotator = annotator()

        pool = work.active_license_pool()
        if not pool:
            return None

        edition = pool.presentation_edition
        identifier = pool.identifier

        _db = Session.object_session(work)

        record = None
        existing_record = getattr(work, annotator.marc_cache_field)
        if existing_record and not force_create:
            record = Record(data=existing_record.encode("utf-8"), force_utf8=True)

        if not record:
            record = Record(leader=annotator.leader(work), force_utf8=True)
            annotator.add_control_fields(record, identifier, pool, edition)
            annotator.add_isbn(record, identifier)

            # TODO: The 240 and 130 fields are for translated works, so they can be grouped even
            # though they have different titles. We do not group editions of the same work in
            # different languages, so we can't use those yet.

            annotator.add_title(record, edition)
            annotator.add_contributors(record, edition)
            annotator.add_publisher(record, edition)
            annotator.add_physical_description(record, edition)
            annotator.add_audience(record, work)
            annotator.add_series(record, edition)
            annotator.add_system_details(record)
            annotator.add_ebooks_subject(record)

            data = record.as_marc()
            setattr(work, annotator.marc_cache_field, data.decode("utf8"))

        # Add additional fields that should not be cached.
        annotator.annotate_work_record(work, pool, edition, identifier, record, integration)
        return record

    def records(self, lane, annotator, mirror_integration, start_time=None,
                force_refresh=False, mirror=None, search_engine=None,
                query_batch_size=500, upload_batch_size=7500,
    ):
        """
        Create and export a MARC file for the books in a lane.

        :param lane: The Lane to export books from.
        :param annotator: The Annotator to use when creating MARC records.
        :param mirror_integration: The mirror integration to use for MARC files.
        :param start_time: Only include records that were created or modified after this time.
        :param force_refresh: Create new records even when cached records are available.
        :param mirror: Optional mirror to use instead of loading one from configuration.
        :param query_batch_size: Number of works to retrieve with a single Elasticsearch query.
        :param upload_batch_size: Number of records to mirror at a time. This is different
          from query_batch_size because S3 enforces a minimum size of 5MB for all parts
          of a multipart upload except the last, but 5MB of records would be too many
          works for a single query.
        """

        # We mirror the content, if it's not empty. If it's empty, we create a CachedMARCFile
        # and Representation, but don't actually mirror it.
        if not mirror:
            storage_protocol = mirror_integration.protocol
            mirror = MirrorUploader.implementation(mirror_integration)
            if mirror.NAME != storage_protocol:
                raise Exception("Mirror integration does not match configured storage protocol")

        if not mirror:
            raise Exception("No mirror integration is configured")

        search_engine = search_engine or ExternalSearchIndex(self._db)

        # End time is before we start the query, because if any records are changed
        # during the processing we may not catch them, and they should be handled
        # again on the next run.
        end_time = datetime.datetime.utcnow()

        facets = MARCExporterFacets(start_time=start_time)
        pagination = SortKeyPagination(size=query_batch_size)

        url = mirror.marc_file_url(self.library, lane, end_time, start_time)
        representation, ignore = get_one_or_create(
            self._db, Representation, url=url,
            media_type=Representation.MARC_MEDIA_TYPE
        )

        with mirror.multipart_upload(representation, url) as upload:
            this_batch = BytesIO()
            this_batch_size = 0
            while pagination is not None:
                # Retrieve one 'page' of works from the search index.
                works = lane.works(
                    self._db, pagination=pagination, facets=facets,
                    search_engine=search_engine
                )
                for work in works:
                    # Create a record for each work and add it to the
                    # MARC file in progress.
                    record = self.create_record(
                        work, annotator, force_refresh, self.integration
                    )
                    if record:
                        this_batch.write(record.as_marc())
                this_batch_size += pagination.this_page_size
                if this_batch_size >= upload_batch_size:
                    # We've reached or exceeded the upload threshold.
                    # Upload one part of the multi-part document.
                    self._upload_batch(this_batch, upload)
                    this_batch = BytesIO()
                    this_batch_size = 0
                pagination = pagination.next_page

            # Upload the final part of the multi-document, if
            # necessary.
            self._upload_batch(this_batch, upload)

        representation.fetched_at = end_time
        if not representation.mirror_exception:
            cached, is_new = get_one_or_create(
                self._db, CachedMARCFile, library=self.library,
                lane=(lane if isinstance(lane, Lane) else None),
                start_time=start_time,
                create_method_kwargs=dict(representation=representation))
            if not is_new:
                cached.representation = representation
            cached.end_time = end_time

    def _upload_batch(self, output, upload):
        "Upload a batch of MARC records as one part of a multi-part upload."
        content = output.getvalue()
        if content:
            upload.upload_part(content)
        output.close()
