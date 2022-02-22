# encoding: utf-8
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


class TestEdition:

    def test_medium_from_media_type(self):
        """
        GIVEN: A media type (Audio and Book)
        WHEN:  Deriving a value for Edition.medium from the media type
        THEN:  A value for Edition.medium is returned
        """
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

    def test_license_pools(self, db_session, create_collection, create_edition, create_licensepool):
        """
        GIVEN: Two Collections that provide access to the same book
        WHEN:  Creating an Edition that has a LicensePool
               and creating a LicensePool associated with the Edition and a Collection
        THEN:  The LicensePools are for the same Work and the Edition contains both LicensePools
        """
        c1 = create_collection(db_session)
        c2 = create_collection(db_session)

        edition, lp1 = create_edition(db_session, with_license_pool=True)
        lp2 = create_licensepool(db_session, edition=edition, collection=c2)

        # Two LicensePools for the same work.
        assert lp1.identifier == lp2.identifier

        # Edition.license_pools contains both.
        assert set([lp1, lp2]) == set(edition.license_pools)

    def test_author_contributors(self, db_session):
        """
        GIVEN: An Edition with an author Contributor and illustrator Contributor
        WHEN:  Checking the Edition's contributors
        THEN:  Both Contributors show up in Edition.contributors
               and only the author Contributor shows up in Edition.author_contributors
        """
        data_source = DataSource.lookup(db_session, DataSource.GUTENBERG)
        id = "ID"
        type = Identifier.GUTENBERG_ID

        edition, _ = Edition.for_foreign_id(
            db_session, data_source, type, id
        )

        # We've listed the same person as primary author and author.
        [alice], _ = Contributor.lookup(db_session, "Adder, Alice")
        edition.add_contributor(
            alice, [Contributor.AUTHOR_ROLE, Contributor.PRIMARY_AUTHOR_ROLE]
        )

        # We've listed a different person as illustrator.
        [bob], _ = Contributor.lookup(db_session, "Bitshifter, Bob")
        edition.add_contributor(bob, [Contributor.ILLUSTRATOR_ROLE])

        # Both contributors show up in .contributors.
        assert set([alice, bob]) == edition.contributors

        # Only the author shows up in .author_contributors, and she
        # only shows up once.
        assert [alice] == edition.author_contributors

    def test_for_foreign_id(self, db_session):
        """
        GIVEN: An identifier
        WHEN:  Looking up an Edition for a foreign id
        THEN:  Correct Edition is returned
        """
        """Verify we can get a data source's view of a foreign id."""
        data_source = DataSource.lookup(db_session, DataSource.GUTENBERG)
        id = "549"
        type = Identifier.GUTENBERG_ID

        record, was_new = Edition.for_foreign_id(db_session, data_source, type, id)
        assert data_source == record.data_source
        identifier = record.primary_identifier
        assert id == identifier.identifier
        assert type == identifier.type
        assert was_new is True
        assert [identifier] == record.equivalent_identifiers()

        # We can get the same work record by providing only the name of the data source.
        record, was_new = Edition.for_foreign_id(db_session, DataSource.GUTENBERG, type, id)
        assert data_source == record.data_source
        assert identifier == record.primary_identifier
        assert was_new is False

    def test_missing_coverage_from(self, db_session, create_coverage_record, init_datasource_and_genres):
        """
        GIVEN: An Edition that has some CoverageRecords with varying associations
        WHEN:  Querying for missing coverages via Edition.missing_coverage_from(...)
        THEN:  Associated records are returned
        """
        gutenberg = DataSource.lookup(db_session, DataSource.GUTENBERG)
        oclc = DataSource.lookup(db_session, DataSource.OCLC)
        web = DataSource.lookup(db_session, DataSource.WEB)

        # Here are two Gutenberg records.
        g1, _ = Edition.for_foreign_id(
            db_session, gutenberg, Identifier.GUTENBERG_ID, "1")

        g2, _ = Edition.for_foreign_id(
            db_session, gutenberg, Identifier.GUTENBERG_ID, "2")

        # One of them has coverage from OCLC Classify
        create_coverage_record(db_session, g1, oclc)

        # The other has coverage from a specific operation on OCLC Classify
        create_coverage_record(db_session, g2, oclc, "some operation")

        # Here's a web record, just sitting there.
        w, _ = Edition.for_foreign_id(
            db_session, web, Identifier.URI, "http://www.example.com/")

        # missing_coverage_from picks up the Gutenberg record with no
        # coverage from OCLC. It doesn't pick up the other
        # Gutenberg record, and it doesn't pick up the web record.
        [in_gutenberg_but_not_in_oclc] = Edition.missing_coverage_from(
            db_session, gutenberg, oclc).all()

        assert g2 == in_gutenberg_but_not_in_oclc

        # If we ask about a specific operation, we get the Gutenberg
        # record that has coverage for that operation, but not the one
        # that has generic OCLC coverage.
        [has_generic_coverage_only] = Edition.missing_coverage_from(
            db_session, gutenberg, oclc, "some operation").all()
        assert g1 == has_generic_coverage_only

        # We don't put web sites into OCLC, so this will pick up the
        # web record (but not the Gutenberg record).
        [in_web_but_not_in_oclc] = Edition.missing_coverage_from(
            db_session, web, oclc).all()
        assert w == in_web_but_not_in_oclc

        # We don't use the web as a source of coverage, so this will
        # return both Gutenberg records (but not the web record).
        assert [g1.id, g2.id] == sorted([x.id for x in Edition.missing_coverage_from(
            db_session, gutenberg, web)])

    def test_sort_by_priority(self, db_session, create_edition):
        """
        GIVEN: Editions with varying data source
        WHEN:  Sorting by priority
        THEN:  Editions are sorted by priority
        """
        # Make editions created by the license source, the metadata wrangler, and library staff.
        admin = create_edition(db_session, data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False)
        od = create_edition(db_session, data_source_name=DataSource.OVERDRIVE, with_license_pool=False)
        mw = create_edition(db_session, data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False)

        # Create an invalid edition with no data source. (This shouldn't happen.)
        no_data_source = create_edition(db_session, with_license_pool=False)
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

    def test_equivalent_identifiers(self, db_session, create_edition, create_identifier):
        """
        GIVEN: An Edition and an Identifier
        WHEN:  Setting the equivalency of an Identifier consisting of an OCLC DataSource
               and a Gutenberg identifier higher than an identifier threshold for a
               PresentationCalculationPolicy
        THEN:  Edition returns the correct identifiers
        """
        edition = create_edition(db_session)
        identifier = create_identifier(db_session)
        data_source = DataSource.lookup(db_session, DataSource.OCLC)

        identifier.equivalent_to(data_source, edition.primary_identifier, 0.6)

        policy = PresentationCalculationPolicy(
            equivalent_identifier_threshold=0.5
        )
        assert (set([identifier, edition.primary_identifier]) ==
               set(edition.equivalent_identifiers(policy=policy)))

        policy.equivalent_identifier_threshold = 0.7
        assert (set([edition.primary_identifier]) ==
               set(edition.equivalent_identifiers(policy=policy)))

    def test_recursive_edition_equivalence(self, db_session, create_edition, create_work, init_datasource_and_genres):
        """
        GIVEN: Given a few Editions and a Project Gutenberg identifier that is equivalent to an OCLC Number
        WHEN:  Calling Edition.equivalent_editions
        THEN:  An Open Library Edition is included in the list of Editions
        """
        # Here's a Edition for a Project Gutenberg text.
        gutenberg, gutenberg_pool = create_edition(
            db_session,
            data_source_name=DataSource.GUTENBERG,
            identifier_type=Identifier.GUTENBERG_ID,
            identifier_id="1",
            with_open_access_download=True,
            title="Original Gutenberg text")

        # Here's a Edition for an Open Library text.
        open_library, _ = create_edition(
            db_session,
            data_source_name=DataSource.OPEN_LIBRARY,
            identifier_type=Identifier.OPEN_LIBRARY_ID,
            identifier_id="W1111",
            with_open_access_download=True,
            title="Open Library record")

        # We've learned from OCLC Classify that the Gutenberg text is
        # equivalent to a certain OCLC Number. We've learned from OCLC
        # Linked Data that the Open Library text is equivalent to the
        # same OCLC Number.
        oclc_classify = DataSource.lookup(db_session, DataSource.OCLC)
        oclc_linked_data = DataSource.lookup(db_session, DataSource.OCLC_LINKED_DATA)

        oclc_number, _ = Identifier.for_foreign_id(
            db_session, Identifier.OCLC_NUMBER, "22")
        gutenberg.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        open_library.primary_identifier.equivalent_to(
            oclc_linked_data, oclc_number, 1)

        # Here's a Edition for a Recovering the Classics cover.
        web_source = DataSource.lookup(db_session, DataSource.WEB)
        recovering, _ = Edition.for_foreign_id(
            db_session, web_source, Identifier.URI,
            "http://recoveringtheclassics.com/pride-and-prejudice.jpg")
        recovering.title = "Recovering the Classics cover"

        # We've manually associated that Edition's URI directly
        # with the Project Gutenberg text.
        manual = DataSource.lookup(db_session, DataSource.MANUAL)
        gutenberg.primary_identifier.equivalent_to(
            manual, recovering.primary_identifier, 1)

        # Finally, here's a completely unrelated Edition, which
        # will not be showing up.
        _, gutenberg2_pool = create_edition(
            db_session,
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
        work = create_work(db_session)
        work.license_pools.extend([gutenberg2_pool])

        # Its set-of-all-editions contains only one record.
        assert 1 == work.all_editions().count()

        # If we add the other Gutenberg record to it, then its
        # set-of-all-editions is extended by that record, *plus*
        # all the Editions equivalent to that record.
        work.license_pools.extend([gutenberg_pool])
        assert 4 == work.all_editions().count()

    def test_calculate_presentation_title(self, db_session, create_edition):
        """
        GIVEN: An Edition with a title
        WHEN:  Calculating the Edition's presentation edition
        THEN:  The Edition sort_title takes articles into account
        """
        wr = create_edition(db_session, title="The Foo")
        wr.calculate_presentation()
        assert "Foo, The" == wr.sort_title

        wr = create_edition(db_session, title="A Foo")
        wr.calculate_presentation()
        assert "Foo, A" == wr.sort_title

    def test_calculate_presentation_missing_author(self, db_session, create_edition):
        """
        GIVEN: An Edition
        WHEN:  Deleting the author
        THEN:  "[Unknown]" is returned as the author
        """
        wr = create_edition(db_session)
        db_session.delete(wr.contributions[0])
        db_session.commit()
        wr.calculate_presentation()
        assert "[Unknown]" == wr.sort_author
        assert "[Unknown]" == wr.author

    def test_calculate_presentation_author(self, db_session, create_contributor, create_edition):
        """
        GIVEN: An Edition with a Contributor as an author
        WHEN:  Calculating the presentation edition
               and adding an another Contributor as an author
        THEN:  The correct authors are returned
        """
        bob = create_contributor(db_session, sort_name="Bitshifter, Bob")
        wr = create_edition(db_session, authors=bob.sort_name)
        wr.calculate_presentation()
        assert "Bob Bitshifter" == wr.author
        assert "Bitshifter, Bob" == wr.sort_author

        bob.display_name = "Bob A. Bitshifter"
        wr.calculate_presentation()
        assert "Bob A. Bitshifter" == wr.author
        assert "Bitshifter, Bob" == wr.sort_author

        kelly = create_contributor(db_session, sort_name="Accumulator, Kelly")
        wr.add_contributor(kelly, Contributor.AUTHOR_ROLE)
        wr.calculate_presentation()
        assert "Kelly Accumulator, Bob A. Bitshifter" == wr.author
        assert "Accumulator, Kelly ; Bitshifter, Bob" == wr.sort_author

    def test_set_summary(self, db_session, create_edition, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work with a presentation Edition that has a LicensePool
        WHEN:  Setting the Work's summary
        THEN:  The summary is correctly set
        """
        e, pool = create_edition(db_session, with_license_pool=True)
        work = create_work(db_session, presentation_edition=e)
        overdrive = DataSource.lookup(db_session, DataSource.OVERDRIVE)

        # Set the work's summmary.
        l1, _ = pool.add_link(Hyperlink.DESCRIPTION, None, overdrive, "text/plain", "F")
        work.set_summary(l1.resource)

        assert l1.resource == work.summary
        assert "F" == work.summary_text

        # Remove the summary.
        work.set_summary(None)

        assert work.summary is None
        assert "" == work.summary_text

    def test_calculate_evaluate_summary_quality_with_privileged_data_sources(
            self, db_session, create_edition, init_datasource_and_genres):
        """
        GIVEN: An Edition with a LicensePool with varying linked descriptions
        WHEN:  Evaluating the summary quality with a privileged data source
        THEN:  The privileged data source wins
        """
        e, pool = create_edition(db_session, with_license_pool=True)
        oclc = DataSource.lookup(db_session, DataSource.OCLC_LINKED_DATA)
        overdrive = DataSource.lookup(db_session, DataSource.OVERDRIVE)

        # There's a perfunctory description from Overdrive.
        l1, _ = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, overdrive, "text/plain", "F")

        overdrive_resource = l1.resource

        # There's a much better description from OCLC Linked Data.
        l2, _ = pool.add_link(Hyperlink.DESCRIPTION, None, oclc, "text/plain",
                      """Nothing about working with his former high school crush, Stephanie Stephens, is ideal. Still, if Aaron Caruthers intends to save his grandmother's bakery, he must. Good thing he has a lot of ideas he can't wait to implement. He never imagines Stephanie would have her own ideas for the business. Or that they would clash with his!""")
        oclc_resource = l2.resource

        # In a head-to-head evaluation, the OCLC Linked Data description wins.
        ids = [e.primary_identifier.id]
        champ1, resources = Identifier.evaluate_summary_quality(db_session, ids)

        assert set([overdrive_resource, oclc_resource]) == set(resources)
        assert oclc_resource == champ1

        # But if we say that Overdrive is the privileged data source, it wins
        # automatically. The other resource isn't even considered.
        champ2, resources2 = Identifier.evaluate_summary_quality(
            db_session, ids, [overdrive])
        assert overdrive_resource == champ2
        assert [overdrive_resource] == resources2

        # If we say that some other data source is privileged, and
        # there are no descriptions from that data source, a
        # head-to-head evaluation is performed, and OCLC Linked Data
        # wins.
        threem = DataSource.lookup(db_session, DataSource.THREEM)
        champ3, resources3 = Identifier.evaluate_summary_quality(
            db_session, ids, [threem])
        assert set([overdrive_resource, oclc_resource]) == set(resources3)
        assert oclc_resource == champ3

        # If there are two privileged data sources and there's no
        # description from the first, the second is used.
        champ4, resources4 = Identifier.evaluate_summary_quality(
            db_session, ids, [threem, overdrive])
        assert [overdrive_resource] == resources4
        assert overdrive_resource == champ4

        # Even an empty string wins if it's from the most privileged data source.
        # This is not a silly example.  The librarian may choose to set the description
        # to an empty string in the admin inteface, to override a bad overdrive/etc. description.
        staff = DataSource.lookup(db_session, DataSource.LIBRARY_STAFF)
        l3, _ = pool.add_link(Hyperlink.SHORT_DESCRIPTION, None, staff, "text/plain", "")
        staff_resource = l3.resource

        champ5, resources5 = Identifier.evaluate_summary_quality(
            db_session, ids, [staff, overdrive])
        assert [staff_resource] == resources5
        assert staff_resource == champ5

    def test_calculate_presentation_cover(self, db_session, create_edition, init_datasource_and_genres):
        """
        GIVEN: An Edition with varying attributes for cover images and thumbnails
        WHEN:  Calculating the presentation
        THEN:  The presentation has the correct cover image url and thumbnail url
        """
        # Here's a cover image with a thumbnail.
        representation, _ = get_one_or_create(db_session, Representation, url="http://cover")
        representation.media_type = Representation.JPEG_MEDIA_TYPE
        representation.mirrored_at = utc_now()
        representation.mirror_url = "http://mirror/cover"
        thumb, _ = get_one_or_create(db_session, Representation, url="http://thumb")
        thumb.media_type = Representation.JPEG_MEDIA_TYPE
        thumb.mirrored_at = utc_now()
        thumb.mirror_url = "http://mirror/thumb"
        thumb.thumbnail_of_id = representation.id

        # Verify that a cover for the edition's primary identifier is used.
        e, _ = create_edition(db_session, with_license_pool=True)
        link, _ = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://cover", e.data_source)
        link.resource.representation = representation
        e.calculate_presentation()
        assert "http://mirror/cover" == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url

        # Verify that a cover will be used even if it's some
        # distance away along the identifier-equivalence line.
        e, _ = create_edition(db_session, with_license_pool=True)
        oclc_classify = DataSource.lookup(db_session, DataSource.OCLC)
        oclc_number, _ = Identifier.for_foreign_id(
            db_session, Identifier.OCLC_NUMBER, "22")
        e.primary_identifier.equivalent_to(
            oclc_classify, oclc_number, 1)
        link, _ = oclc_number.add_link(Hyperlink.IMAGE, "http://cover", oclc_classify)
        link.resource.representation = representation
        e.calculate_presentation()
        assert "http://mirror/cover" == e.cover_full_url
        assert "http://mirror/thumb" == e.cover_thumbnail_url

        # Verify that a nearby cover takes precedence over a
        # faraway cover.
        link, _ = e.primary_identifier.add_link(Hyperlink.IMAGE, "http://nearby-cover", e.data_source)
        nearby, _ = get_one_or_create(db_session, Representation, url=link.resource.url)
        nearby.media_type = Representation.JPEG_MEDIA_TYPE
        nearby.mirrored_at = utc_now()
        nearby.mirror_url = "http://mirror/nearby-cover"
        link.resource.representation = nearby
        nearby_thumb, _ = get_one_or_create(db_session, Representation, url="http://nearby-thumb")
        nearby_thumb.media_type = Representation.JPEG_MEDIA_TYPE
        nearby_thumb.mirrored_at = utc_now()
        nearby_thumb.mirror_url = "http://mirror/nearby-thumb"
        nearby_thumb.thumbnail_of_id = nearby.id
        e.calculate_presentation()
        assert "http://mirror/nearby-cover" == e.cover_full_url
        assert "http://mirror/nearby-thumb" == e.cover_thumbnail_url

        # Verify that a thumbnail is used even if there's
        # no full-sized cover.
        e, _ = create_edition(db_session, with_license_pool=True)
        link, _ = e.primary_identifier.add_link(Hyperlink.THUMBNAIL_IMAGE, "http://thumb", e.data_source)
        link.resource.representation = thumb
        e.calculate_presentation()
        assert e.cover_full_url is None
        assert "http://mirror/thumb" == e.cover_thumbnail_url

    def test_calculate_presentation_registers_coverage_records(self, db_session, create_edition):
        """
        GIVEN: An Edition
        WHEN:  Calculating the presentation
        THEN:  A CoverageRecord is created
        """
        edition = create_edition(db_session)
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

    def test_no_permanent_work_id_for_edition_without_title_or_medium(self, db_session, create_edition):
        """
        GIVEN: An Edition with no title or medium
        WHEN:  Calculating the permanent work ID
        THEN:  There is no permanent work ID assigned
        """
        edition = create_edition(db_session)
        assert edition.permanent_work_id is None

        edition.title = ''
        edition.calculate_permanent_work_id()
        assert edition.permanent_work_id is None

        edition.title = 'something'
        edition.calculate_permanent_work_id()
        assert edition.permanent_work_id is not None

        edition.medium = None
        edition.calculate_permanent_work_id()
        assert edition.permanent_work_id is None

    def test_choose_cover_can_choose_full_image_and_thumbnail_separately(self, db_session, create_edition):
        """
        GIVEN: An Edition
        WHEN:  Choosing the Edition's cover
        THEN:  The correct thumbnail is chosen
        """
        edition = create_edition(db_session)

        # This edition has a full-sized image and a thumbnail image,
        # but there is no evidence that they are the _same_ image.
        main_image, _ = edition.primary_identifier.add_link(
            Hyperlink.IMAGE, "http://main/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_image, _ = edition.primary_identifier.add_link(
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
        thumbnail_2, _ = edition.primary_identifier.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://thumbnail2/",
            edition.data_source, Representation.PNG_MEDIA_TYPE
        )
        thumbnail_2.resource.representation.thumbnail_of = main_image.resource.representation
        edition.choose_cover()

        # ...That thumbnail will be chosen in preference to the
        # possibly unrelated thumbnail.
        assert main_image.resource.url == edition.cover_full_url
        assert thumbnail_2.resource.url == edition.cover_thumbnail_url
