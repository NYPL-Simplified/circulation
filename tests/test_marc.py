from nose.tools import (
    eq_,
    set_trace,
    assert_raises,
)
import datetime
from pymarc import Record, MARCReader
from io import StringIO
import urllib.request, urllib.parse, urllib.error
from sqlalchemy.orm.session import Session

from . import DatabaseTest

from ..model import (
    CachedMARCFile,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Genre,
    Identifier,
    LicensePoolDeliveryMechanism,
    Representation,
    RightsStatus,
    Work,
    get_one,
)
from ..config import CannotLoadConfiguration
from ..external_search import (
    MockExternalSearchIndex,
    Filter,
)
from ..marc import (
  Annotator,
  MARCExporter,
  MARCExporterFacets,
)

from ..s3 import (
    MockS3Uploader,
    S3Uploader,
)
from ..lane import WorkList

class TestAnnotator(DatabaseTest):

    def test_annotate_work_record(self):
        # Verify that annotate_work_record adds the distributor and formats.
        class MockAnnotator(Annotator):
            add_distributor_called_with = None
            add_formats_called_with = None
            def add_distributor(self, record, pool):
                self.add_distributor_called_with = [record, pool]
            def add_formats(self, record, pool):
                self.add_formats_called_with = [record, pool]

        annotator = MockAnnotator()
        record = Record()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]

        annotator.annotate_work_record(work, pool, None, None, record)
        eq_([record, pool], annotator.add_distributor_called_with)
        eq_([record, pool], annotator.add_formats_called_with)

    def test_leader(self):
        work = self._work(with_license_pool=True)
        leader = Annotator.leader(work)
        eq_("00000nam  2200000   4500", leader)

        # If there's already a marc record cached, the record status changes.
        work.marc_record = "cached"
        leader = Annotator.leader(work)
        eq_("00000cam  2200000   4500", leader)

    def _check_control_field(self, record, tag, expected):
        [field] = record.get_fields(tag)
        eq_(expected, field.value())

    def _check_field(self, record, tag, expected_subfields, expected_indicators=None):
        if not expected_indicators:
            expected_indicators = [" ", " "]
        [field] = record.get_fields(tag)
        eq_(expected_indicators, field.indicators)
        for subfield, value in list(expected_subfields.items()):
            eq_(value, field.get_subfields(subfield)[0])

    def test_add_control_fields(self):
        # This edition has one format and was published before 1900.
        edition, pool = self._edition(with_license_pool=True)
        identifier = pool.identifier
        edition.issued = datetime.datetime(956, 1, 1)

        now = datetime.datetime.now()
        record = Record()

        Annotator.add_control_fields(record, identifier, pool, edition)
        self._check_control_field(record, "001", identifier.urn)
        assert now.strftime("%Y%m%d") in record.get_fields("005")[0].value()
        self._check_control_field(record, "006", "m        d        ")
        self._check_control_field(record, "007", "cr cn ---anuuu")
        self._check_control_field(
            record, "008",
            now.strftime("%y%m%d") + "s0956    xxu                 eng  ")

        # This French edition has two formats and was published in 2018.
        edition2, pool2 = self._edition(with_license_pool=True)
        identifier2 = pool2.identifier
        edition2.issued = datetime.datetime(2018, 2, 3)
        edition2.language = "fre"
        LicensePoolDeliveryMechanism.set(
            pool2.data_source, identifier2, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM, RightsStatus.IN_COPYRIGHT)

        record = Record()
        Annotator.add_control_fields(record, identifier2, pool2, edition2)
        self._check_control_field(record, "001", identifier2.urn)
        assert now.strftime("%Y%m%d") in record.get_fields("005")[0].value()
        self._check_control_field(record, "006", "m        d        ")
        self._check_control_field(record, "007", "cr cn ---mnuuu")
        self._check_control_field(
            record, "008",
            now.strftime("%y%m%d") + "s2018    xxu                 fre  ")

    def test_add_marc_organization_code(self):
        record = Record()
        Annotator.add_marc_organization_code(record, "US-MaBoDPL")
        self._check_control_field(record, "003", "US-MaBoDPL")

    def test_add_isbn(self):
        isbn = self._identifier(identifier_type=Identifier.ISBN)
        record = Record()
        Annotator.add_isbn(record, isbn)
        self._check_field(record, "020", {"a": isbn.identifier})

        # If the identifier isn't an ISBN, but has an equivalent that is, it still
        # works.
        equivalent = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.OCLC)
        equivalent.equivalent_to(data_source, isbn, 1)
        record = Record()
        Annotator.add_isbn(record, equivalent)
        self._check_field(record, "020", {"a": isbn.identifier})

        # If there is no ISBN, the field is left out.
        non_isbn = self._identifier()
        record = Record()
        Annotator.add_isbn(record, non_isbn)
        eq_([], record.get_fields("020"))

    def test_add_title(self):
        edition = self._edition()
        edition.title = "The Good Soldier"
        edition.sort_title = "Good Soldier, The"
        edition.subtitle = "A Tale of Passion"

        record = Record()
        Annotator.add_title(record, edition)
        [field] = record.get_fields("245")
        self._check_field(
            record, "245", {
                "a": edition.title,
                "b": edition.subtitle,
                "c": edition.author,
            }, ["0", "4"])

        # If there's no subtitle or no author, those subfields are left out.
        edition.subtitle = None
        edition.author = None

        record = Record()
        Annotator.add_title(record, edition)
        [field] = record.get_fields("245")
        self._check_field(
            record, "245", {
                "a": edition.title,
            }, ["0", "4"])
        eq_([], field.get_subfields("b"))
        eq_([], field.get_subfields("c"))

    def test_add_contributors(self):
        author = "a"
        author2 = "b"
        translator = "c"

        # Edition with one author gets a 100 field and no 700 fields.
        edition = self._edition(authors=[author])
        edition.sort_author = "sorted"

        record = Record()
        Annotator.add_contributors(record, edition)
        eq_([], record.get_fields("700"))
        self._check_field(record, "100", {"a": edition.sort_author}, ["1", " "])

        # Edition with two authors and a translator gets three 700 fields and no 100 fields.
        edition = self._edition(authors=[author, author2])
        edition.add_contributor(translator, Contributor.TRANSLATOR_ROLE)

        record = Record()
        Annotator.add_contributors(record, edition)
        eq_([], record.get_fields("100"))
        fields = record.get_fields("700")
        for field in fields:
            eq_(["1", " "], field.indicators)
        [author_field, author2_field, translator_field] = sorted(fields, key=lambda x: x.get_subfields("a")[0])
        eq_(author, author_field.get_subfields("a")[0])
        eq_(Contributor.PRIMARY_AUTHOR_ROLE, author_field.get_subfields("e")[0])
        eq_(author2, author2_field.get_subfields("a")[0])
        eq_(Contributor.AUTHOR_ROLE, author2_field.get_subfields("e")[0])
        eq_(translator, translator_field.get_subfields("a")[0])
        eq_(Contributor.TRANSLATOR_ROLE, translator_field.get_subfields("e")[0])

    def test_add_publisher(self):
        edition = self._edition()
        edition.publisher = self._str
        edition.issued = datetime.datetime(1894, 4, 5)

        record = Record()
        Annotator.add_publisher(record, edition)
        self._check_field(
            record, "264", {
                "a": "[Place of publication not identified]",
                "b": edition.publisher,
                "c": "1894",
            }, [" ", "1"])

        # If there's no publisher, the field is left out.
        record = Record()
        edition.publisher = None
        Annotator.add_publisher(record, edition)
        eq_([], record.get_fields("264"))

    def test_add_distributor(self):
        edition, pool = self._edition(with_license_pool=True)
        record = Record()
        Annotator.add_distributor(record, pool)
        self._check_field(record, "264", {"b": pool.data_source.name}, [" ", "2"])

    def test_add_physical_description(self):
        book = self._edition()
        book.medium = Edition.BOOK_MEDIUM
        audio = self._edition()
        audio.medium = Edition.AUDIO_MEDIUM

        record = Record()
        Annotator.add_physical_description(record, book)
        self._check_field(record, "300", {"a": "1 online resource"})
        self._check_field(record, "336", {
            "a": "text",
            "b": "txt",
            "2": "rdacontent",
        })
        self._check_field(record, "337", {
            "a": "computer",
            "b": "c",
            "2": "rdamedia",
        })
        self._check_field(record, "338", {
            "a": "online resource",
            "b": "cr",
            "2": "rdacarrier",
        })
        self._check_field(record, "347", {
            "a": "text file",
            "2": "rda",
        })
        self._check_field(record, "380", {
            "a": "eBook",
            "2": "tlcgt",
        })

        record = Record()
        Annotator.add_physical_description(record, audio)
        self._check_field(record, "300", {
            "a": "1 sound file",
            "b": "digital",
        })
        self._check_field(record, "336", {
            "a": "spoken word",
            "b": "spw",
            "2": "rdacontent",
        })
        self._check_field(record, "337", {
            "a": "computer",
            "b": "c",
            "2": "rdamedia",
        })
        self._check_field(record, "338", {
            "a": "online resource",
            "b": "cr",
            "2": "rdacarrier",
        })
        self._check_field(record, "347", {
            "a": "audio file",
            "2": "rda",
        })
        eq_([], record.get_fields("380"))

    def test_add_audience(self):
        for audience, term in list(Annotator.AUDIENCE_TERMS.items()):
            work = self._work(audience=audience)
            record = Record()
            Annotator.add_audience(record, work)
            self._check_field(record, "385", {
                "a": term,
                "2": "tlctarget",
            })

    def test_add_series(self):
        edition = self._edition()
        edition.series = self._str
        edition.series_position = 5
        record = Record()
        Annotator.add_series(record, edition)
        self._check_field(record, "490", {
            "a": edition.series,
            "v": str(edition.series_position),
        }, ["0", " "])

        # If there's no series position, the same field is used without
        # the v subfield.
        edition.series_position = None
        record = Record()
        Annotator.add_series(record, edition)
        self._check_field(record, "490", {
            "a": edition.series,
        }, ["0", " "])
        [field] = record.get_fields("490")
        eq_([], field.get_subfields("v"))

        # If there's no series, the field is left out.
        edition.series = None
        record = Record()
        Annotator.add_series(record, edition)
        eq_([], record.get_fields("490"))

    def test_add_system_details(self):
        record = Record()
        Annotator.add_system_details(record)
        self._check_field(record, "538", {"a": "Mode of access: World Wide Web."})

    def test_add_formats(self):
        edition, pool = self._edition(with_license_pool=True)
        epub_no_drm, ignore = DeliveryMechanism.lookup(
            self._db, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        pool.delivery_mechanisms[0].delivery_mechanism = epub_no_drm
        LicensePoolDeliveryMechanism.set(
            pool.data_source, pool.identifier, Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM, RightsStatus.IN_COPYRIGHT)

        record = Record()
        Annotator.add_formats(record, pool)
        fields = record.get_fields("538")
        eq_(2, len(fields))
        [pdf, epub] = sorted(fields, key=lambda x: x.get_subfields("a")[0])
        eq_("Adobe PDF eBook", pdf.get_subfields("a")[0])
        eq_([" ", " "], pdf.indicators)
        eq_("EPUB eBook", epub.get_subfields("a")[0])
        eq_([" ", " "], epub.indicators)

    def test_add_summary(self):
        work = self._work(with_license_pool=True)
        work.summary_text = "<p>Summary</p>"

        record = Record()
        Annotator.add_summary(record, work)
        self._check_field(record, "520", {"a": b" Summary "})

    def test_add_simplified_genres(self):
        work = self._work(with_license_pool=True)
        fantasy, ignore = Genre.lookup(self._db, "Fantasy", autocreate=True)
        romance, ignore = Genre.lookup(self._db, "Romance", autocreate=True)
        work.genres = [fantasy, romance]

        record = Record()
        Annotator.add_simplified_genres(record, work)
        fields = record.get_fields("650")
        [fantasy_field, romance_field] = sorted(fields, key=lambda x: x.get_subfields("a")[0])
        eq_(["0", "7"], fantasy_field.indicators)
        eq_("Fantasy", fantasy_field.get_subfields("a")[0])
        eq_("Library Simplified", fantasy_field.get_subfields("2")[0])
        eq_(["0", "7"], romance_field.indicators)
        eq_("Romance", romance_field.get_subfields("a")[0])
        eq_("Library Simplified", romance_field.get_subfields("2")[0])

    def test_add_ebooks_subject(self):
        record = Record()
        Annotator.add_ebooks_subject(record)
        self._check_field(record, "655", {"a": "Electronic books."}, [" ", "0"])

class TestMARCExporter(DatabaseTest):

    def _integration(self):
        return self._external_integration(
            ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL,
            libraries=[self._default_library])

    def test_from_config(self):
        assert_raises(CannotLoadConfiguration, MARCExporter.from_config, self._default_library)

        integration = self._integration()
        exporter = MARCExporter.from_config(self._default_library)
        eq_(integration, exporter.integration)
        eq_(self._default_library, exporter.library)

        other_library = self._library()
        assert_raises(CannotLoadConfiguration, MARCExporter.from_config, other_library)

    def test_create_record(self):
        work = self._work(with_license_pool=True, title="old title",
                          authors=["old author"], data_source_name=DataSource.OVERDRIVE)
        annotator = Annotator()

        # The record isn't cached yet, so a new record is created and cached.
        eq_(None, work.marc_record)
        record = MARCExporter.create_record(work, annotator)
        [title_field] = record.get_fields("245")
        eq_("old title", title_field.get_subfields("a")[0])
        [author_field] = record.get_fields("100")
        eq_("author, old", author_field.get_subfields("a")[0])
        [distributor_field] = record.get_fields("264")
        eq_(DataSource.OVERDRIVE, distributor_field.get_subfields("b")[0])
        cached = work.marc_record.encode("utf-8")
        assert b"old title" in cached
        assert b"author, old" in cached
        # The distributor isn't part of the cached record.
        assert DataSource.OVERDRIVE.encode("utf8") not in cached

        work.presentation_edition.title = "new title"
        work.presentation_edition.sort_author = "author, new"
        new_data_source = DataSource.lookup(self._db, DataSource.BIBLIOTHECA)
        work.license_pools[0].data_source = new_data_source

        # Now that the record is cached, creating a record will
        # use the cache. Distributor will be updated since it's
        # not part of the cached record.
        record = MARCExporter.create_record(work, annotator)
        [title_field] = record.get_fields("245")
        eq_("old title", title_field.get_subfields("a")[0])
        [author_field] = record.get_fields("100")
        eq_("author, old", author_field.get_subfields("a")[0])
        [distributor_field] = record.get_fields("264")
        eq_(DataSource.BIBLIOTHECA, distributor_field.get_subfields("b")[0])

        # But we can force an update to the cached record.
        record = MARCExporter.create_record(work, annotator, force_create=True)
        [title_field] = record.get_fields("245")
        eq_("new title", title_field.get_subfields("a")[0])
        [author_field] = record.get_fields("100")
        eq_("author, new", author_field.get_subfields("a")[0])
        [distributor_field] = record.get_fields("264")
        eq_(DataSource.BIBLIOTHECA, distributor_field.get_subfields("b")[0])
        cached = work.marc_record.encode("utf-8")
        assert b"old title" not in cached
        assert b"author, old" not in cached
        assert b"new title" in cached
        assert b"author, new" in cached

        # If we pass in an integration, it's passed along to the annotator.
        integration = self._integration()
        class MockAnnotator(Annotator):
            integration = None
            def annotate_work_record(self, work, pool, edition, identifier, record, integration):
                self.integration = integration

        annotator = MockAnnotator()
        record = MARCExporter.create_record(work, annotator, integration=integration)
        eq_(integration, annotator.integration)

    def test_create_record_roundtrip(self):
        # Create a marc record from a work with special characters
        # in both the title and author name and round-trip it to
        # the DB and back again to make sure we are creating records
        # we can understand.

        annotator = Annotator()

        # Creates a new record and saves it to the database
        work = self._work(
          title="Little Mimi\u2019s First Counting Lesson",
          authors=["Lagerlo\xf6f, Selma Ottiliana Lovisa,"],
          with_license_pool=True
        )
        record = MARCExporter.create_record(work, annotator)
        loaded_record = MARCExporter.create_record(work, annotator)
        eq_(record.as_marc(), loaded_record.as_marc())

        # Loads a existing record from the DB
        db = Session(self.connection)
        new_work = get_one(db, Work, id=work.id)
        new_record = MARCExporter.create_record(new_work, annotator)
        eq_(record.as_marc(), new_record.as_marc())

    def test_records(self):
        integration = self._integration()
        now = datetime.datetime.utcnow()
        exporter = MARCExporter.from_config(self._default_library)
        annotator = Annotator()
        lane = self._lane("Test Lane", genres=["Mystery"])
        w1 = self._work(genre="Mystery", with_open_access_download=True)
        w2 = self._work(genre="Mystery", with_open_access_download=True)

        search_engine = MockExternalSearchIndex()
        search_engine.bulk_update([w1, w2])

        # If there's a storage protocol but not corresponding storage integration,
        # it raises an exception.
        assert_raises(Exception, exporter.records, lane, annotator)

        # If there is a storage integration, the output file is mirrored.
        mirror_integration = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            username="username", password="password",
        )

        mirror = MockS3Uploader()

        exporter.records(lane, annotator, mirror_integration, mirror=mirror, query_batch_size=1, upload_batch_size=1, search_engine=search_engine)

        # The file was mirrored and a CachedMARCFile was created to track the mirrored file.
        eq_(1, len(mirror.uploaded))
        [cache] = self._db.query(CachedMARCFile).all()
        eq_(self._default_library, cache.library)
        eq_(lane, cache.lane)
        eq_(mirror.uploaded[0], cache.representation)
        eq_(None, cache.representation.content)
        eq_("https://test-marc-bucket.s3.amazonaws.com/%s/%s/%s.mrc" % (
            self._default_library.short_name,
            urllib.parse.quote(str(cache.representation.fetched_at)),
            urllib.parse.quote(lane.display_name)),
            mirror.uploaded[0].mirror_url)
        eq_(None, cache.start_time)
        assert cache.end_time > now

        # The content was uploaded in two parts.
        eq_(2, len(mirror.content[0]))
        complete_file = b"".join(mirror.content[0])
        records = list(MARCReader(complete_file))
        eq_(2, len(records))

        title_fields = [record.get_fields("245") for record in records]
        titles = [fields[0].get_subfields("a")[0] for fields in title_fields]
        eq_(set([w1.title, w2.title]), set(titles))

        assert w1.title.encode("utf8") in w1.marc_record
        assert w2.title.encode("utf8") in w2.marc_record

        self._db.delete(cache)

        # It also works with a WorkList instead of a Lane, in which case
        # there will be no lane in the CachedMARCFile.
        worklist = WorkList()
        worklist.initialize(self._default_library, display_name="All Books")

        mirror = MockS3Uploader()
        exporter.records(worklist, annotator, mirror_integration, mirror=mirror, query_batch_size=1, upload_batch_size=1, search_engine=search_engine)

        eq_(1, len(mirror.uploaded))
        [cache] = self._db.query(CachedMARCFile).all()
        eq_(self._default_library, cache.library)
        eq_(None, cache.lane)
        eq_(mirror.uploaded[0], cache.representation)
        eq_(None, cache.representation.content)
        eq_("https://test-marc-bucket.s3.amazonaws.com/%s/%s/%s.mrc" % (
            self._default_library.short_name,
            urllib.parse.quote(str(cache.representation.fetched_at)),
            urllib.parse.quote(worklist.display_name)),
            mirror.uploaded[0].mirror_url)
        eq_(None, cache.start_time)
        assert cache.end_time > now

        eq_(2, len(mirror.content[0]))
        complete_file = b"".join(mirror.content[0])
        records = list(MARCReader(complete_file))
        eq_(2, len(records))

        self._db.delete(cache)

        # If a start time is set, it's used in the mirror url.
        #
        # (Our mock search engine returns everthing in its 'index',
        # so this doesn't test that the start time is actually used to
        # find works -- that's in the search index tests and the
        # tests of MARCExporterFacets.)
        start_time = now - datetime.timedelta(days=3)

        mirror = MockS3Uploader()
        exporter.records(
            lane, annotator, mirror_integration, start_time=start_time,
            mirror=mirror, query_batch_size=2,
            upload_batch_size=2, search_engine=search_engine
        )
        [cache] = self._db.query(CachedMARCFile).all()
        eq_(self._default_library, cache.library)
        eq_(lane, cache.lane)
        eq_(mirror.uploaded[0], cache.representation)
        eq_(None, cache.representation.content)
        eq_("https://test-marc-bucket.s3.amazonaws.com/%s/%s-%s/%s.mrc" % (
            self._default_library.short_name, urllib.parse.quote(str(start_time)),
            urllib.parse.quote(str(cache.representation.fetched_at)),
            urllib.parse.quote(lane.display_name)),
            mirror.uploaded[0].mirror_url)
        eq_(start_time, cache.start_time)
        assert cache.end_time > now
        self._db.delete(cache)

        # If the search engine returns no contents for the lane,
        # nothing will be mirrored, but a CachedMARCFile is still
        # created to track that we checked for updates.
        empty_search_engine = MockExternalSearchIndex()

        mirror = MockS3Uploader()
        exporter.records(lane, annotator, mirror_integration,
                         mirror=mirror, search_engine=empty_search_engine)

        eq_([], mirror.content[0])
        [cache] = self._db.query(CachedMARCFile).all()
        eq_(cache.representation, mirror.uploaded[0])
        eq_(self._default_library, cache.library)
        eq_(lane, cache.lane)
        eq_(None, cache.representation.content)
        eq_(None, cache.start_time)
        assert cache.end_time > now

        self._db.delete(cache)


class TestMARCExporterFacets(object):
    def test_modify_search_filter(self):
        # A facet object.
        facets = MARCExporterFacets("some start time")

        # A filter about to be modified by the facet object.
        filter = Filter()
        filter.order_ascending = False

        facets.modify_search_filter(filter)

        # updated_after has been set and results are to be returned in
        # order of increasing last_update_time.
        eq_("last_update_time", filter.order)
        eq_(True, filter.order_ascending)
        eq_("some start time", filter.updated_after)

    def test_scoring_functions(self):
        # A no-op.
        facets = MARCExporterFacets("some start time")
        eq_([], facets.scoring_functions(object()))
