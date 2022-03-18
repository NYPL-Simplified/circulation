# encoding: utf-8
import datetime
from ...testing import DatabaseTest
from ...model import (
    get_one_or_create,
    PresentationCalculationPolicy,
)
from ...model.constants import MediaTypes
from ...model.coverage import CoverageRecord
from ...model.contributor import Contributor
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier
from ...model.licensing import DeliveryMechanism
from ...model.resource import (
    Hyperlink,
    Representation,
)
from ...util.datetime_helpers import utc_now

class TestEdition(DatabaseTest):

    def test_medium_from_media_type(self):
        # Verify that we can guess a value for Edition.medium from a
        # media type.

        m = Edition.medium_from_media_type
        for audio_type in MediaTypes.AUDIOBOOK_MEDIA_TYPES:
            assert Edition.AUDIO_MEDIUM == m(audio_type)
            assert Edition.AUDIO_MEDIUM == m(audio_type + ";param=value")

        for book_type in MediaTypes.BOOK_MEDIA_TYPES:
            assert Edition.BOOK_MEDIUM == m(book_type)
            assert Edition.BOOK_MEDIUM == m(book_type + ";param=value")

        assert Edition.BOOK_MEDIUM == m(DeliveryMechanism.ADOBE_DRM)

    def test_license_pools(self):
        # Here are two collections that provide access to the same book.
        c1 = self._collection()
        c2 = self._collection()

        edition, lp1 = self._edition(with_license_pool=True)
        lp2 = self._licensepool(edition=edition, collection=c2)

        # Two LicensePools for the same work.
        assert lp1.identifier == lp2.identifier

        # Edition.license_pools contains both.
        assert set([lp1, lp2]) == set(edition.license_pools)

    def test_author_contributors(self):
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = self._str
        type = Identifier.GUTENBERG_ID

        edition, was_new = Edition.for_foreign_id(
            self._db, data_source, type, id
        )

        # We've listed the same person as primary author and author.
        [alice], ignore = Contributor.lookup(self._db, "Adder, Alice")
        edition.add_contributor(
            alice, [Contributor.AUTHOR_ROLE, Contributor.PRIMARY_AUTHOR_ROLE]
        )

        # We've listed a different person as illustrator.
        [bob], ignore = Contributor.lookup(self._db, "Bitshifter, Bob")
        edition.add_contributor(bob, [Contributor.ILLUSTRATOR_ROLE])

        # Both contributors show up in .contributors.
        assert set([alice, bob]) == edition.contributors

        # Only the author shows up in .author_contributors, and she
        # only shows up once.
        assert [alice] == edition.author_contributors

    def test_for_foreign_id(self):
        """Verify we can get a data source's view of a foreign id."""
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        id = "549"
        type = Identifier.GUTENBERG_ID

        record, was_new = Edition.for_foreign_id(
            self._db, data_source, type, id)
        assert data_source == record.data_source
        identifier = record.primary_identifier
        assert id == identifier.identifier
        assert type == identifier.type
        assert True == was_new
        assert [identifier] == record.equivalent_identifiers()

        # We can get the same work record by providing only the name
        # of the data source.
        record, was_new = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, type, id)
        assert data_source == record.data_source
        assert identifier == record.primary_identifier
        assert False == was_new

    def test_missing_coverage_from(self):
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        web = DataSource.lookup(self._db, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "1")

        g2, ignore = Edition.for_foreign_id(
            self._db, gutenberg, Identifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        c1 = self._coverage_record(g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        c2 = self._coverage_record(g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, ignore = Edition.for_foreign_id(
            self._db, web, Identifier.URI, "http://www.foo.com/")

        # missing_coverage_from picks up the Gutenberg record with no
        # coverage from OCLC. It doesn't pick up the other
        # Gutenberg record, and it doesn't pick up the web record.
        [in_gutenberg_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, gutenberg, oclc).all()

        assert g2 == in_gutenberg_but_not_in_oclc

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.
        [has_generic_coverage_only] = Edition.missing_coverage_from(
            self._db, gutenberg, oclc, "some operation").all()
        assert g1 == has_generic_coverage_only

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Edition.missing_coverage_from(
            self._db, web, oclc).all()
        assert w == in_web_but_not_in_oclc

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        assert [g1.id, g2.id] == sorted([x.id for x in Edition.missing_coverage_from(
            self._db, gutenberg, web)])

    def test_sort_by_priority(self):

        # Make editions created by the license source, the metadata
        # wrangler, and library staff.
        admin = self._edition(data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        od = self._edition(data_source_name=DataSource.OVERDRIVE, with_license_pool=False)
        mw = self._edition(data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)

        # Create an invalid edition with no data source. (This shouldn't
        # happen.)
        no_data_source = self._edition(with_license_pool=False)
        no_data_source.data_source = None

        def ids(l):
            return [x for x in l]

        # The invalid edition is the lowest priority. The admin
        # interface and metadata wrangler take precedence over any
        # other data sources.
        expect = [no_data_source, od, mw, admin]
        actual = Edition.sort_by_priority(expect)
        assert ids(expect) == ids(actual)

        # If you specify which data source is associated with the
        # license for the book, you will boost its priority above that
        # of the metadata wrangler.
        expect = [no_data_source, mw, od, admin]
        actual = Edition.sort_by_priority(expect, od.data_source)
        assert ids(expect) == ids(actual)

    def test_equivalent_identifiers(self):

        edition = self._edition()
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.OCLC)

        identifier.equivalent_to(data_source, edition.primary_identifier, 0.6)

        policy = PresentationCalculationPolicy(
            equivalent_identifier_threshold=0.5
        )
        assert (set([identifier, edition.primary_identifier]) ==
            set(edition.equivalent_identifiers(policy=policy)))

        policy.equivalent_identifier_threshold = 0.7
        assert (set([edition.primary_identifier]) ==
            set(edition.equivalent_identifiers(policy=policy)))

    def test_recursive_edition_equivalence(self):

        # Here's a Edition for a Project Gutenberg text.
        gutenberg, gutenberg_pool = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Original Gutenberg text")

        # Here's a Edition for an Open Library text.
        open_library, open_library_pool = self._edition(
            data_source_name=DataSource.OPEN_LIBRARY,
            identifier_type=Identifier.OPEN_LIBRARY_ID,
            identifier_id="W1111",
            with_open_access_download=True,
            title="Open Library record")

        # We've learned from OCLC Classify that the Gutenberg text is
        # equivalent to a certain OCLC Number. We've learned from OCLC
        # Linked Data that the Open Library text is equivalent to the
        # same OCLC Number.
        oclc_classify = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_linked_data = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)

        oclc_number, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "22")
        gutenberg.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        open_library.primary_identifier.equivalent_to(
            oclc_linked_data, oclc_number, 1)

        # Here's a Edition for a Recovering the Classics cover.
        web_source = DataSource.lookup(self._db, DataSource.WEB)
        recovering, ignore = Edition.for_foreign_id(
            self._db, web_source, Identifier.URI,
            "http://recoveringtheclassics.com/pride-and-prejudice.jpg")
        recovering.title = "Recovering the Classics cover"

        # We've manually associated that Edition's URI directly
        # with the Project Gutenberg text.
        manual = DataSource.lookup(self._db, DataSource.MANUAL)
        gutenberg.primary_identifier.equivalent_to(
            manual, recovering.primary_identifier, 1)

        # Finally, here's a completely unrelated Edition, which
        # will not be showing up.
        gutenberg2, gutenberg2_pool = self._edition(
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="2",
            with_open_access_download=True,
            title="Unrelated Gutenberg record.")

        # When we call equivalent_editions on the Project Gutenberg
        # Edition, we get three Editions: the Gutenberg record
        # itself, the Open Library record, and the Recovering the
        # Classics record.
        #
        # We get the Open Library record because it's associated with
        # the same OCLC Number as the Gutenberg record. We get the
        # Recovering the Classics record because it's associated
        # directly with the Gutenberg record.
        results = list(gutenberg.equivalent_editions())
        assert 3 == len(results)
        assert gutenberg in results
        assert open_library in results
        assert recovering in results

        # Here's a Work that incorporates one of the Gutenberg records.
        work = self._work()
        work.license_pools.extend([gutenberg2_pool])

        # Its set-of-all-editions contains only one record.
        assert 1 == work.all_editions().count()

        # If we add the other Gutenberg record to it, then its
        # set-of-all-editions is extended by that record, *plus*
        # all the Editions equivalent to that record.
        work.license_pools.extend([gutenberg_pool])
        assert 4 == work.all_editions().count()

    def test_calculate_presentation_title(self):
        wr = self._edition(title="The Foo")
        wr.calculate_presentation()
        assert "Foo, The" == wr.sort_title

        wr = self._edition(title="A Foo")
        wr.calculate_presentation()
        assert "Foo, A" == wr.sort_title

    def test_calculate_presentation_missing_author(self):
        wr = self._edition()
        self._db.delete(wr.contributions[0])
        self._db.commit()
        wr.calculate_presentation()
        assert "[Unknown]" == wr.sort_author
        assert "[Unknown]" == wr.author

    def test_calculate_presentation_author(self):
        bob, ignore = self._contributor(sort_name="Bitshifter, Bob")
        wr = self._edition(authors=bob.sort_name)
        wr.calculate_presentation()
        assert "Bob Bitshifter" == wr.author
        assert "Bitshifter, Bob" == wr.sort_author

        bob.display_name="Bob A. Bitshifter"
        wr.calculate_presentation()
        assert "Bob A. Bitshifter" == wr.author
        assert "Bitshifter, Bob" == wr.sort_author

        kelly, ignore = self._contributor(sort_name="Accumulator, Kelly")
        wr.add_contributor(kelly, Contributor.AUTHOR_ROLE)
        wr.calculate_presentation()
        assert "Kelly Accumulator, Bob A. Bitshifter" == wr.author
        assert "Accumulator, Kelly ; Bitshifter, Bob" == wr.sort_author

    def test_set_summary(self):
        e, pool = self._edition(with_license_pool=True)
        work = self._work(presentation_edition=e)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # Set the work's summmary.
        l1, new = pool.add_link(Hyperlink.DESCRIPTION, None, overdrive, "text/plain",
                      "F")
        work.set_summary(l1.resource)

        assert l1.resource == work.summary
        assert "F" == work.summary_text

        # Remove the summary.
        work.set_summary(None)

        assert None == work.summary
        assert "" == work.summary_text

    def test_calculate_evaluate_summary_quality_with_privileged_data_sources(self):
        e, pool = self._edition(with_license_pool=True)
        oclc = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # There's a perfunctory description from Overdrive.
        l1, new = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, overdrive, "text/plain",
                      "F")

        overdrive_resource = l1.resource

        # There's a much better description from OCLC Linked Data.
        l2, new = pool.add_link(Hyperlink.DESCRIPTION, None, oclc, "text/plain",
                      """Nothing about working with his former high school crush, Stephanie Stephens, is ideal. Still, if Aaron Caruthers intends to save his grandmother's bakery, he must. Good thing he has a lot of ideas he can't wait to implement. He never imagines Stephanie would have her own ideas for the business. Or that they would clash with his!""")
        oclc_resource = l2.resource

        # In a head-to-head evaluation, the OCLC Linked Data description wins.
        ids = [e.primary_identifier.id]
        champ1, resources = Identifier.evaluate_summary_quality(self._db, ids)

        assert set([overdrive_resource, oclc_resource]) == set(resources)
        assert oclc_resource == champ1

        # But if we say that Overdrive is the privileged data source, it wins
        # automatically. The other resource isn't even considered.
        champ2, resources2 = Identifier.evaluate_summary_quality(
            self._db, ids, [overdrive])
        assert overdrive_resource == champ2
        assert [overdrive_resource] == resources2

        # If we say that some other data source is privileged, and
        # there are no descriptions from that data source, a
        # head-to-head evaluation is performed, and OCLC Linked Data
        # wins.
        threem = DataSource.lookup(self._db, DataSource.THREEM)
        champ3, resources3 = Identifier.evaluate_summary_quality(
            self._db, ids, [threem])
        assert set([overdrive_resource, oclc_resource]) == set(resources3)
        assert oclc_resource == champ3

        # If there are two privileged data sources and there's no
        # description from the first, the second is used.
        champ4, resources4 = Identifier.evaluate_summary_quality(
            self._db, ids, [threem, overdrive])
        assert [overdrive_resource] == resources4
        assert overdrive_resource == champ4

        # Even an empty string wins if it's from the most privileged data source.
        # This is not a silly example.  The librarian may choose to set the description
        # to an empty string in the admin inteface, to override a bad overdrive/etc. description.
        staff = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        l3, new = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, staff, "text/plain", "")
        staff_resource = l3.resource

        champ5, resources5 = Identifier.evaluate_summary_quality(
            self._db, ids, [staff, overdrive])
        assert [staff_resource] == resources5
        assert staff_resource == champ5

    def test_calculate_presentation_cover(self):
        # Here's a cover image with a thumbnail.
        representation, ignore = get_one_or_create(self._db, Representation, url="http://cover")
        representation.media_type = Representation.JPEG_MEDIA_TYPE
        representation.mirrored_at = utc_now()
        representation.mirror_url = "http://mirror/cover"
        thumb, ignore = get_one_or_create(self._db, Representation, url="http://thumb")
        thumb.media_type = Representation.JPEG_MEDIA_TYPE
        thumb.mirrored_at = utc_now()
        thumb.mirror_url = "http://mirror/thumb"
        thumb.thumbnail_of_id = representation.id

        # Verify that a cover for the edition's primary identifier is used.
        e, pool = self._edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://cover", e.data_source)
        link.resource.representation = representation
        e.calculate_presentation()
        assert "http://mirror/cover" == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url

        # Verify that a cover will be used even if it's some
        # distance away along the identifier-equivalence line.
        e, pool = self._edition(with_license_pool=True)
        oclc_classify = DataSource.lookup(self._db, DataSource.OCLC)
        oclc_number, ignore = Identifier.for_foreign_id(
            self._db, Identifier.OCLC_NUMBER, "22")
        e.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        link, ignore = oclc_number.add_link(Hyperlink.IMAGE, "http://cover", oclc_classify)
        link.resource.representation = representation
        e.calculate_presentation()
        assert "http://mirror/cover" == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url

        # Verify that a nearby cover takes precedence over a
        # faraway cover.
        link, ignore = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://nearby-cover", e.data_source)
        nearby, ignore = get_one_or_create(self._db, Representation, url=link.resource.url)
        nearby.media_type = Representation.JPEG_MEDIA_TYPE
        nearby.mirrored_at = utc_now()
        nearby.mirror_url = "http://mirror/nearby-cover"
        link.resource.representation = nearby
        nearby_thumb, ignore = get_one_or_create(self._db, Representation, url="http://nearby-thumb")
        nearby_thumb.media_type = Representation.JPEG_MEDIA_TYPE
        nearby_thumb.mirrored_at = utc_now()
        nearby_thumb.mirror_url = "http://mirror/nearby-thumb"
        nearby_thumb.thumbnail_of_id = nearby.id
        e.calculate_presentation()
        assert "http://mirror/nearby-cover" == e.cover_full_url
        assert "http://mirror/nearby-thumb" == e.cover_thumbnail_url

        # Verify that a thumbnail is used even if there's
        # no full-sized cover.
        e, pool = self._edition(with_license_pool=True)
        link, ignore = e.primary_identifier.add_link(Hyperlink.THUMBNAIL_IMAGE, "http://thumb", e.data_source)
        link.resource.representation = thumb
        e.calculate_presentation()
        assert None == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url


    def test_calculate_presentation_registers_coverage_records(self):
        edition = self._edition()
        identifier = edition.primary_identifier

        # This Identifier has no CoverageRecords.
        assert [] == identifier.coverage_records

        # But once we calculate the Edition's presentation...
        edition.calculate_presentation()

        # Two CoverageRecords have been associated with this Identifier.
        records = identifier.coverage_records

        # One for setting the Edition metadata and one for choosing
        # the Edition's cover.
        expect = set([
            CoverageRecord.SET_EDITION_METADATA_OPERATION,
            CoverageRecord.CHOOSE_COVER_OPERATION]
        )
        assert expect == set([x.operation for x in records])

        # We know the records are associated with this specific
        # Edition, not just the Identifier, because each
        # CoverageRecord's DataSource is set to this Edition's
        # DataSource.
        assert (
            [edition.data_source, edition.data_source] ==
            [x.data_source for x in records])

    def test_no_permanent_work_id_for_edition_without_title_or_medium(self):
        # An edition with no title or medium is not assigned a permanent work
        # ID.
        edition = self._edition()
        assert None == edition.permanent_work_id

        edition.title = ''
        edition.calculate_permanent_work_id()
        assert None == edition.permanent_work_id

        edition.title = 'something'
        edition.calculate_permanent_work_id()
        assert None != edition.permanent_work_id

        edition.medium = None
        edition.calculate_permanent_work_id()
        assert None == edition.permanent_work_id

    def test_choose_cover_can_choose_full_image_and_thumbnail_separately(self):
        edition = self._edition()

        # This edition has a full-sized image and a thumbnail image,
        # but there is no evidence that they are the _same_ image.
        main_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.IMAGE, "http://main/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_image, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumbnail/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )

        # Nonetheless, Edition.choose_cover() will assign the
        # potentially unrelated images to the Edition, because there
        # is no better option.
        edition.choose_cover()
        assert main_image.resource.url == edition.cover_full_url
        assert thumbnail_image.resource.url == edition.cover_thumbnail_url

        # If there is a clear indication that one of the thumbnails
        # associated with the identifier is a thumbnail _of_ the
        # full-sized image...
        thumbnail_2, ignore = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumbnail2/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_2.resource.representation.thumbnail_of = main_image.resource.representation
        edition.choose_cover()

        # ...That thumbnail will be chosen in preference to the
        # possibly unrelated thumbnail.
        assert main_image.resource.url == edition.cover_full_url
        assert thumbnail_2.resource.url == edition.cover_thumbnail_url
