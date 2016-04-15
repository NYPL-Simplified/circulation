import datetime
from nose.tools import (
    set_trace,
    eq_,
)
from . import (
    DatabaseTest
)
from testing import (
    AlwaysSuccessfulCoverageProvider,
    DummyHTTPClient,
    NeverSuccessfulCoverageProvider,
    TransientFailureCoverageProvider,
)
from model import (
    Contributor,
    CoverageRecord,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    PresentationCalculationPolicy,
    Representation,
    Subject,
    Timestamp,
)
from metadata_layer import (
    Metadata,
    IdentifierData,
    ContributorData,
    LinkData,
    ReplacementPolicy,
    SubjectData,
)
from s3 import DummyS3Uploader
from coverage import (
    BibliographicCoverageProvider,
    CoverageProvider,
    CoverageFailure,
)

class TestCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestCoverageProvider, self).setup()
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        self.input_identifier_types = gutenberg.primary_identifier_type
        self.output_source = DataSource.lookup(self._db, DataSource.OCLC)
        self.edition = self._edition(gutenberg.name)
        self.identifier = self.edition.primary_identifier

    def test_ensure_coverage(self):

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, self.output_source
        )
        result = provider.ensure_coverage(self.edition)

        # There is now one CoverageRecord -- the one returned by
        # ensure_coverage().
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)
        eq_(None, record.exception)
        eq_(record, result)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_ensure_coverage_persistent_coverage_failure(self):

        provider = NeverSuccessfulCoverageProvider(
            "Never successful", self.input_identifier_types, self.output_source
        )
        record = provider.ensure_coverage(self.edition)

        assert isinstance(record, CoverageRecord)
        eq_(self.edition.primary_identifier, record.identifier)
        eq_("What did you expect?", record.exception)

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        eq_([], self._db.query(Timestamp).all())

    def test_items_that_need_coverage(self):
        cutoff_time = datetime.datetime(2016, 1, 1)
        record = CoverageRecord.add_for(
            self.edition, self.output_source, timestamp=cutoff_time
        )

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, 
            self.output_source, cutoff_time=cutoff_time
        )
        eq_([], provider.items_that_need_coverage.all())

        one_second_after = cutoff_time + datetime.timedelta(seconds=1)
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, 
            self.output_source, cutoff_time=one_second_after
        )
        eq_([self.edition.primary_identifier], 
            provider.items_that_need_coverage.all())

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, 
            self.output_source
        )
        eq_([], provider.items_that_need_coverage.all())

    def test_ensure_coverage_transient_coverage_failure(self):

        provider = TransientFailureCoverageProvider(
            "Transient failure", self.input_identifier_types, self.output_source
        )
        result = provider.ensure_coverage(self.edition)
        eq_(True, isinstance(result, CoverageFailure))

        # Because the error is transient we have no coverage record.
        eq_([], self._db.query(CoverageRecord).all())

        # Timestamp was not updated.
        eq_([], self._db.query(Timestamp).all())

    def test_run_on_identifiers(self):
        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, self.output_source
        )
        provider.workset_size = 6
        
        to_be_tested = [self._identifier() for i in range(6)]
        not_to_be_tested = [self._identifier() for i in range(6)]
        provider.run_on_identifiers(to_be_tested)
        for i in to_be_tested:
            assert i in provider.attempts
        for i in not_to_be_tested:
            assert i not in provider.attempts

    def test_always_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, self.output_source
        )
        provider.run()

        # There is now one CoverageRecord
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Always successful", timestamp.service)

    def test_run_once_and_update_timestamp(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = AlwaysSuccessfulCoverageProvider(
            "Always successful", self.input_identifier_types, self.output_source
        )
        new_offset = provider.run_once_and_update_timestamp(0)
        eq_(None, new_offset)

        # There is now one CoverageRecord
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)

        # The timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Always successful", timestamp.service)

    def test_never_successful(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = NeverSuccessfulCoverageProvider(
            "Never successful", self.input_identifier_types, self.output_source
        )
        provider.run()

        # We have a CoverageRecord that signifies failure.
        [record] = self._db.query(CoverageRecord).all()
        eq_(self.edition.primary_identifier, record.identifier)
        eq_(self.output_source, self.output_source)
        eq_("What did you expect?", record.exception)

        # But the coverage provider did run, and the timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Never successful", timestamp.service)

    def test_transient_failure(self):

        # We start with no CoverageRecords and no Timestamp.
        eq_([], self._db.query(CoverageRecord).all())
        eq_([], self._db.query(Timestamp).all())

        provider = TransientFailureCoverageProvider(
            "Transient failure", self.input_identifier_types, self.output_source
        )
        provider.run()

        # We have no CoverageRecord, since the error was transient.
        eq_([], self._db.query(CoverageRecord).all())

        # But the coverage provider did run, and the timestamp is now set.
        [timestamp] = self._db.query(Timestamp).all()
        eq_("Transient failure", timestamp.service)

    def test_set_metadata_incorporates_replacement_policy(self):
        """Make sure that if a ReplacementPolicy is passed in to
        set_metadata(), the policy's settings (and those of its
        .presentation_calculation_policy) are respected.
        """

        edition, pool = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier

        # All images and open-access content should be uploaded to
        # this 'mirror'.
        mirror = DummyS3Uploader()
        http = DummyHTTPClient()
        http.queue_response(
            200, content='I am an epub.',
            media_type=Representation.EPUB_MEDIA_TYPE,
        )

        class Tripwire(PresentationCalculationPolicy):
            # This class sets a variable if one of its properties is
            # accessed.
            def __init__(self, *args, **kwargs):
                self.tripped = False

            def __getattr__(self, name):
                self.tripped = True
                return True

        presentation_calculation_policy = Tripwire()

        metadata_replacement_policy = ReplacementPolicy(
            mirror=mirror,
            http_get=http.do_get,
            presentation_calculation_policy=presentation_calculation_policy
        )

        output_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        provider = CoverageProvider(
            "service", [identifier.type], output_source
        )

        # We've got a Metadata object that includes an open-access download.
        link = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="http://foo.com/")
        metadata = Metadata(output_source, links=[link])

        provider.set_metadata(
            identifier, metadata, 
            metadata_replacement_policy=metadata_replacement_policy
        )

        # The open-access download was 'downloaded' and 'mirrored'.
        [mirrored] = mirror.uploaded
        eq_("http://foo.com/", mirrored.url)
        assert mirrored.mirror_url.endswith(
            "/%s/%s.epub" % (identifier.identifier, edition.title)
        )
        eq_("I am an epub.", mirrored.content)

        # Our custom PresentationCalculationPolicy was used when
        # determining whether to recalculate the work's
        # presentation. We know this because the tripwire was
        # triggered.
        eq_(True, presentation_calculation_policy.tripped)


class TestBibliographicCoverageProvider(DatabaseTest):

    BIBLIOGRAPHIC_DATA = Metadata(
        DataSource.OVERDRIVE,
        publisher=u'Perfection Learning',
        language='eng',
        title=u'A Girl Named Disaster',
        published=datetime.datetime(1998, 3, 1, 0, 0),
        identifiers = [
            IdentifierData(
                    type=Identifier.OVERDRIVE_ID,
                    identifier=u'ba9b3419-b0bd-4ca7-a24f-26c4246b6b44'
                ),
            IdentifierData(type=Identifier.ISBN, identifier=u'9781402550805')
        ],
        contributors = [
            ContributorData(sort_name=u"Nancy Farmer",
                            roles=[Contributor.PRIMARY_AUTHOR_ROLE])
        ],
        subjects = [
            SubjectData(type=Subject.TOPIC,
                        identifier=u'Action & Adventure'),
            SubjectData(type=Subject.FREEFORM_AUDIENCE,
                        identifier=u'Young Adult'),
            SubjectData(type=Subject.PLACE, identifier=u'Africa')
        ],
    )

    def test_edition(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        provider.CAN_CREATE_LICENSE_POOLS = False
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # Returns a CoverageFailure if the identifier doesn't have a
        # license pool and none can be created.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Returns an Edition otherwise, creating it if necessary.
        edition, lp = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        eq_(edition, provider.edition(identifier))

        # The Edition will be created if necessary.
        lp.identifier.primarily_identifies = []
        e2 = provider.edition(identifier)
        assert edition != e2
        assert isinstance(e2, Edition)

    def test_work(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA
        provider.CAN_CREATE_LICENSE_POOLS = False

        # Returns a CoverageFailure if the identifier doesn't have a
        # license pool.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Returns a CoverageFailure if there's no work available.
        edition, lp = self._edition(with_license_pool=True)
        # Remove edition so that the work won't be calculated
        lp.identifier.primarily_identifies = []
        result = provider.work(lp.identifier)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)

        # Returns the work if it can be created or found.
        ed, lp = self._edition(with_license_pool=True)
        result = provider.work(lp.identifier)
        eq_(result, lp.work)

    def test_set_metadata(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        provider.CAN_CREATE_LICENSE_POOLS = False
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # If there is no LicensePool and it can't be autocreated, a
        # CoverageRecord results.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        edition, lp = self._edition(with_license_pool=True)

        # If no metadata is passed in, a CoverageRecord results.
        result = provider.set_metadata(edition.primary_identifier, None)
        assert isinstance(result, CoverageFailure)
        eq_("Did not receive metadata from input source", result.exception)

        # If no work can be created (in this case, because there's no title),
        # a CoverageFailure results.
        edition.title = None
        old_title = test_metadata.title
        test_metadata.title = None
        result = provider.set_metadata(edition.primary_identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        eq_("Work could not be calculated", result.exception)
        test_metadata.title = old_title        

        # Test success
        result = provider.set_metadata(edition.primary_identifier, test_metadata)
        eq_(result, edition.primary_identifier)

        # If there's an exception setting the metadata, a
        # CoverageRecord results. This call raises a ValueError
        # because the primary identifier & the edition's primary
        # identifier don't match.
        test_metadata.primary_identifier = self._identifier(
            identifier_type=Identifier.OVERDRIVE_ID
        )
        result = provider.set_metadata(lp.identifier, test_metadata)
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception

    def test_autocreate_licensepool(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)

        # If this constant is set to False, the coverage provider cannot
        # autocreate LicensePools for identifiers.
        provider.CAN_CREATE_LICENSE_POOLS = False
        eq_(None, provider.license_pool(identifier))

        # If it's set to True, the coverage provider can autocreate
        # LicensePools for identifiers.
        provider.CAN_CREATE_LICENSE_POOLS = True
        pool = provider.license_pool(identifier)
        eq_(pool.data_source, provider.output_source)
        eq_(pool.identifier, identifier)
       
    def test_set_presentation_ready(self):
        provider = BibliographicCoverageProvider(self._db, None,
                DataSource.OVERDRIVE)
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        test_metadata = self.BIBLIOGRAPHIC_DATA

        # If the work can't be found, it can't be made presentation ready.
        provider.CAN_CREATE_LICENSE_POOLS = False
        result = provider.set_presentation_ready(identifier)
        assert isinstance(result, CoverageFailure)
        eq_("No license pool available", result.exception)

        # Test success.
        ed, lp = self._edition(with_license_pool=True)
        result = provider.set_presentation_ready(ed.primary_identifier)
        eq_(result, ed.primary_identifier)

