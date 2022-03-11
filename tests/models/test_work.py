# encoding: utf-8
import pytest
import datetime
from mock import MagicMock
import os
from psycopg2.extras import NumericRange
import random

from ...external_search import MockExternalSearchIndex
from ...classifier import (
    Classifier,
    Fantasy,
    Romance,
    Science_Fiction,
)
from ...model import (
    get_one_or_create,
    tuple_to_numericrange,
)
from ...model.coverage import WorkCoverageRecord
from ...model.classification import (
    Genre,
    Subject,
)
from ...model.complaint import Complaint
from ...model.contributor import Contributor
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier
from ...model.licensing import LicensePool
from ...model.resource import (
    Hyperlink,
    Representation,
    Resource,
)
from ...model.work import (
    Work,
    WorkGenre,
)
from ...util.datetime_helpers import from_timestamp
from ...util.datetime_helpers import datetime_utc, utc_now


class TestWork:

    def test_complaints(self, db_session, create_edition, create_work, create_licensepool, init_datasource_and_genres):
        """
        GIVEN: A LicensePool with a Work
        WHEN:  Registering a Complaint against the LicensePool
        THEN:  Complaints are associated with the Work
        """
        work = create_work(db_session, with_license_pool=True)

        [lp1] = work.license_pools
        lp2 = create_licensepool(
            db_session,
            edition=work.presentation_edition,
            data_source_name=DataSource.OVERDRIVE
        )
        lp2.work = work

        complaint_type = random.choice(list(Complaint.VALID_TYPES))
        complaint1, _ = Complaint.register(lp1, complaint_type, "blah", "blah")
        complaint2, _ = Complaint.register(lp2, complaint_type, "blah", "blah")

        # Create a complaint with no association with the work.
        _, lp3 = create_edition(db_session, with_license_pool=True)
        Complaint.register(lp3, complaint_type, "blah", "blah")

        # Only the first two complaints show up in work.complaints.
        assert sorted([complaint1.id, complaint2.id]) == sorted([x.id for x in work.complaints])

    def test_all_identifier_ids(self, db_session, create_identifier, create_edition,
                                create_licensepool, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work associated with two LicensePools, a DataSource,
               and an Identifier that equivalates the LicensePools to the DataSource
        WHEN:  Getting all the Identifier IDs for the Work
        THEN:  Associated Identifier IDs are returned
        """
        work = create_work(db_session, with_license_pool=True)
        lp = work.license_pools[0]
        identifier = create_identifier(db_session)
        data_source = DataSource.lookup(db_session, DataSource.OCLC)
        identifier.equivalent_to(data_source, lp.identifier, 1)

        # Make sure there aren't duplicates in the list, if an
        # identifier's equivalent to two of the primary identifiers.
        edition = create_edition(db_session)
        lp2 = create_licensepool(db_session, edition)
        work.license_pools.append(lp2)
        identifier.equivalent_to(data_source, lp2.identifier, 1)

        all_identifier_ids = work.all_identifier_ids()
        assert len(all_identifier_ids) == 3
        expect_all_ids = set(
            [lp.identifier.id, lp2.identifier.id, identifier.id]
        )

        assert expect_all_ids == all_identifier_ids

    def test_from_identifiers(self, db_session, create_identifier, create_work):
        """
        GIVEN: A Work associated with a LicensePool that has various Identifiers
        WHEN:  Getting Works from an Identifier
        THEN:  The correct Works are returned
        """
        # Prep a work to be identified and a work to be ignored.
        work = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        lp = work.license_pools[0]
        _ = create_work(db_session, with_license_pool=True, with_open_access_download=True)

        # No identifiers returns None.
        result = Work.from_identifiers(db_session, [])
        assert result is None

        # A work can be found according to its identifier.
        identifiers = [lp.identifier]
        result = Work.from_identifiers(db_session, identifiers).all()
        assert len(result) == 1
        assert result == [work]

        # When the work has an equivalent identifier.
        isbn = create_identifier(db_session, Identifier.ISBN)
        source = lp.data_source
        lp.identifier.equivalent_to(source, isbn, 1)

        # It can be found according to that equivalency.
        identifiers = [isbn]
        result = Work.from_identifiers(db_session, identifiers).all()
        assert len(result) == 1
        assert result == [work]

        # Unless the strength is too low.
        lp.identifier.equivalencies[0].strength = 0.8
        identifiers = [isbn]

        result = Work.from_identifiers(db_session, identifiers).all()
        assert result == []

        # Two+ of the same or equivalent identifiers lead to one result.
        identifiers = [lp.identifier, isbn, lp.identifier]
        result = Work.from_identifiers(db_session, identifiers).all()
        assert len(result) == 1
        assert result == [work]

        # It accepts a base query.
        qu = db_session.query(Work).join(LicensePool).join(Identifier).\
            filter(LicensePool.suppressed)
        identifiers = [lp.identifier]
        result = Work.from_identifiers(db_session, identifiers, base_query=qu).all()
        # Because the work's license_pool isn't suppressed, it isn't returned.
        assert result == []

    def test_calculate_presentation(self, db_session, create_edition,
                                    create_identifier, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work
        WHEN:  Making the Work ready to show to patrons
        THEN:  A presentation edition is calculated and appropriate WorkCoverageRecords are created
        """
        # Test that:
        # - work coverage records are made on work creation and primary edition selection.
        # - work's presentation information (author, title, etc. fields) does a proper job
        #   of combining fields from underlying editions.
        # - work's presentation information keeps in sync with work's presentation edition.
        # - there can be only one edition that thinks it's the presentation edition for this work.
        # - time stamps are stamped.
        # - higher-standard sources (library staff) can replace, but not delete, authors.
        # - works are made presentation-ready as soon as possible

        gutenberg_source = DataSource.GUTENBERG
        gitenberg_source = DataSource.PROJECT_GITENBERG

        [bob], _ = Contributor.lookup(db_session, "Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()

        edition1, pool1 = create_edition(
            db_session, gitenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition1.title = "The 1st Title"
        edition1.subtitle = "The 1st Subtitle"
        edition1.add_contributor(bob, Contributor.AUTHOR_ROLE)

        edition2, pool2 = create_edition(
            db_session,
            gitenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition2.title = "The 2nd Title"
        edition2.subtitle = "The 2nd Subtitle"
        edition2.add_contributor(bob, Contributor.AUTHOR_ROLE)
        [alice], _ = Contributor.lookup(db_session, "Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()
        edition2.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition3, pool3 = create_edition(
            db_session,
            gutenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition3.title = "The 2nd Title"
        edition3.subtitle = "The 2nd Subtitle"
        edition3.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition3.add_contributor(alice, Contributor.AUTHOR_ROLE)

        # Create three summaries.

        # This summary is associated with one of the work's
        # LicensePools, and it comes from a good source -- Library
        # Staff. It will be chosen even though it doesn't look great,
        # textually.
        library_staff = DataSource.lookup(db_session, DataSource.LIBRARY_STAFF)
        chosen_summary = "direct"
        pool1.identifier.add_link(Hyperlink.DESCRIPTION, None, library_staff, content=chosen_summary)

        # This summary is associated with one of the work's
        # LicensePools, but it comes from a less reliable source, so
        # it won't be chosen.
        less_reliable_summary_source = DataSource.lookup(db_session, DataSource.OCLC)
        pool2.identifier.add_link(
            Hyperlink.DESCRIPTION, None, less_reliable_summary_source,
            content="less reliable summary"
        )

        # This summary looks really nice, and it's associated with the
        # same source as the LicensePool, which is good, but it's not
        # directly associated with any of the LicensePools, so it
        # won't be chosen.
        related_identifier = create_identifier(db_session)
        pool3.identifier.equivalent_to(pool3.data_source, related_identifier, strength=1)
        related_identifier.add_link(
            Hyperlink.DESCRIPTION, None, pool3.data_source,
            content="This is an indirect summary. It's much longer, and looks more 'real', so you'd think it would be prefered, but it won't be."
        )

        work = create_work(db_session, presentation_edition=edition2)
        work.calculate_presentation_edition()
        work.calculate_opds_entries(verbose=False)

        # The work starts out with no description, even though its
        # presentation was calculated, because a description can only
        # come from an Identifier associated with a LicensePool, and
        # this Work has no LicensePools.
        assert work.summary is None

        # add in 3, 2, 1 order to make sure the selection of edition1 as presentation
        # in the second half of the test is based on business logic, not list order.
        for p in pool3, pool1:
            work.license_pools.append(p)

        # The author of the Work is the author of its primary work record.
        assert "Alice Adder, Bob Bitshifter" == work.author

        # This Work starts out with a single CoverageRecord reflecting
        # the work done to generate its initial OPDS entry, and then
        # it adds choose-edition as a primary edition is set. The
        # search index CoverageRecord is a marker for work that must
        # be done in the future, and is not tested here.
        [choose_edition, generate_opds, update_search_index] = sorted(work.coverage_records, key=lambda x: x.operation)
        assert (generate_opds.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION)
        assert (choose_edition.operation == WorkCoverageRecord.CHOOSE_EDITION_OPERATION)

        # pools aren't yet aware of each other
        assert pool1.superceded is False
        assert pool2.superceded is False
        assert pool3.superceded is False

        work.last_update_time = None
        work.presentation_ready = True
        index = MockExternalSearchIndex()

        work.calculate_presentation(search_index_client=index)

        # The author of the Work has not changed.
        assert "Alice Adder, Bob Bitshifter" == work.author

        # one and only one license pool should be un-superceded
        assert pool1.superceded is True
        assert pool2.superceded is False
        assert pool3.superceded is True

        # sanity check
        assert work.presentation_edition == pool2.presentation_edition
        assert work.presentation_edition == edition2

        # editions that aren't the presentation edition have no work
        assert edition1.work is None
        assert edition2.work == work
        assert edition3.work is None

        # The title of the Work is the title of its primary work record.
        assert "The 2nd Title" == work.title
        assert "The 2nd Subtitle" == work.subtitle

        # The author of the Work is the author of its primary work record.
        assert "Alice Adder, Bob Bitshifter" == work.author
        assert "Adder, Alice ; Bitshifter, Bob" == work.sort_author

        # The summary has now been chosen.
        assert chosen_summary == work.summary.representation.content.decode("utf-8")

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (utc_now() - work.last_update_time) < datetime.timedelta(seconds=2)

        # The index has not been updated.
        assert list(index.docs.items()) == []

        # The Work now has a complete set of WorkCoverageRecords
        # associated with it, reflecting all the operations that
        # occured as part of calculate_presentation().
        #
        # All the work has actually been done, except for the work of
        # updating the search index, which has been registered and
        # will be done later.
        records = work.coverage_records

        wcr = WorkCoverageRecord
        success = wcr.SUCCESS
        expect = set([
            (wcr.CHOOSE_EDITION_OPERATION, success),
            (wcr.CLASSIFY_OPERATION, success),
            (wcr.SUMMARY_OPERATION, success),
            (wcr.QUALITY_OPERATION, success),
            (wcr.GENERATE_OPDS_OPERATION, success),
            (wcr.GENERATE_MARC_OPERATION, success),
            (wcr.UPDATE_SEARCH_INDEX_OPERATION, wcr.REGISTERED),
        ])
        assert expect == set([(x.operation, x.status) for x in records])

        # Now mark the pool with the presentation edition as suppressed.
        # work.calculate_presentation() will call work.mark_licensepools_as_superceded(),
        # which will mark the suppressed pool as superceded and take its edition out of the running.
        # Make sure that work's presentation edition and work's author, etc.
        # fields are updated accordingly, and that the superceded pool's edition
        # knows it's no longer the champ.
        pool2.suppressed = True

        work.calculate_presentation(search_index_client=index)

        # The title of the Work is the title of its new primary work record.
        assert "The 1st Title" == work.title
        assert "The 1st Subtitle" == work.subtitle

        # author of composite edition is now just Bob
        assert "Bob Bitshifter" == work.author
        assert "Bitshifter, Bob" == work.sort_author

        # sanity check
        assert work.presentation_edition == pool1.presentation_edition
        assert work.presentation_edition == edition1

        # editions that aren't the presentation edition have no work
        assert edition1.work == work
        assert edition2.work is None
        assert edition3.work is None

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (utc_now() - work.last_update_time) < datetime.timedelta(seconds=2)

        # make a staff (admin interface) edition.  its fields should supercede all others below it
        # except when it has no contributors, and they do.
        pool2.suppressed = False

        staff_edition = create_edition(
            db_session, data_source_name=DataSource.LIBRARY_STAFF,
            with_license_pool=False, authors=[])
        staff_edition.title = "The Staff Title"
        staff_edition.primary_identifier = pool2.identifier
        # set edition's authorship to "nope", and make sure the lower-priority
        # editions' authors don't get clobbered
        staff_edition.contributions = []
        staff_edition.author = Edition.UNKNOWN_AUTHOR
        staff_edition.sort_author = Edition.UNKNOWN_AUTHOR

        work.calculate_presentation(search_index_client=index)

        # The title of the Work got superceded.
        assert work.title == "The Staff Title"

        # The author of the Work is still the author of edition2 and was not clobbered.
        assert work.author == "Alice Adder, Bob Bitshifter"
        assert work.sort_author == "Adder, Alice ; Bitshifter, Bob"

    def test_calculate_presentation_with_no_presentation_edition(self, db_session, create_work):
        """
        GIVEN: A Work with no presentation edtion
        WHEN:  Calculating the presentation edition
        THEN:  The Work is not presentation ready and a WorkCoverageRecord was created
        """
        # Calling calculate_presentation() on a work with no
        # presentation edition won't do anything, but at least it doesn't
        # crash.
        work = create_work(db_session)
        work.presentation_edition = None
        work.coverage_records = []
        db_session.commit()
        work.calculate_presentation()

        # The work is not presentation-ready.
        assert work.presentation_ready is False

        # Work was done to choose the presentation edition, but since no
        # presentation edition was found, no other work was done.
        [choose_edition] = work.coverage_records
        assert choose_edition.operation == WorkCoverageRecord.CHOOSE_EDITION_OPERATION

    def test_calculate_presentation_sets_presentation_ready_based_on_content(self, db_session, create_work):
        """
        GIVEN: A Work that has a presentation edition with no language set
        WHEN:  Determining if the Work is presentation ready
        THEN:  The Work is presentation ready when the presentation edition has a language set
        """
        # This work is incorrectly presentation-ready; its presentation
        # edition has no language.
        work = create_work(db_session, with_license_pool=True)
        edition = work.presentation_edition
        edition.language = None

        assert work.presentation_ready is True
        work.calculate_presentation()
        assert work.presentation_ready is False

        # Give it a language, and it becomes presentation-ready again.
        edition.language = "eng"
        work.calculate_presentation()
        assert work.presentation_ready is True

    def test_calculate_presentation_uses_default_audience_set_as_collection_setting(
            self, db_session, create_edition, create_work, default_library, init_datasource_and_genres):
        """
        GIVEN: A Work, an Edition, and a Collection that has a default audience
        WHEN:  Determining if a Work is ready to show to patrons
        THEN:  The Work's audience is the same audience as the Collection
        """
        default_audience = Classifier.AUDIENCE_ADULT
        [collection] = default_library.collections
        collection.default_audience = default_audience
        edition, _ = create_edition(
            db_session,
            DataSource.GUTENBERG,
            Identifier.GUTENBERG_ID,
            collection=collection,
            with_license_pool=True,
            with_open_access_download=True
        )
        work = create_work(db_session, presentation_edition=edition)
        work.last_update_time = None
        work.presentation_ready = True

        work.calculate_presentation()

        assert default_audience == work.audience

    def test__choose_summary(self, db_session, create_identifier, init_datasource_and_genres):
        """
        GIVEN: A Work and multiple Identifiers for summaries
        WHEN:  Choosing a summary for the presentation edition
        THEN:  The best summary is chosen accordingly
        """
        # Test the _choose_summary helper method, called by
        # calculate_presentation().

        class Mock(Work):
            def set_summary(self, summary):
                if isinstance(summary, Resource):
                    self.summary_text = summary.representation.unicode_content
                else:
                    self.summary_text = summary

        w = Mock()
        w.the_summary = "old summary"
        db_session.add(w)
        m = w._choose_summary

        # If no summaries are available, any old summary is cleared out.
        m([], [], [])
        assert w.summary_text is None

        # Create three summaries on two identifiers.
        source1 = DataSource.lookup(db_session, DataSource.OVERDRIVE)
        source2 = DataSource.lookup(db_session, DataSource.BIBLIOTHECA)

        i1 = create_identifier(db_session)
        l1, _ = i1.add_link(Hyperlink.DESCRIPTION, None, source1, content="ok summary")
        good_summary = "This summary is great! It's more than one sentence long and features some noun phrases."
        i1.add_link(Hyperlink.DESCRIPTION, None, source2, content=good_summary)

        i2 = create_identifier(db_session)
        i2.add_link(Hyperlink.DESCRIPTION, None, source2, content="not too bad")

        # Now we can test out the rules for choosing summaries.

        # In a choice between all three summaries, good_summary is
        # chosen based on textual characteristics.
        m([], [i1.id, i2.id], [])
        assert good_summary == w.summary_text

        m([i1.id, i2.id], [], [])
        assert good_summary == w.summary_text

        # If an identifier is associated directly with the work, its
        # summaries are considered first, and the other identifiers
        # are not considered at all.
        m([i2.id], [object(), i1.id], [])
        assert "not too bad" == w.summary_text

        # A summary that comes from a preferred data source will be
        # chosen over some other summary.
        m([i1.id, i2.id], [], [source1])
        assert "ok summary" == w.summary_text

        # But if there is no summary from a preferred data source, the
        # normal rules apply.
        source3 = DataSource.lookup(db_session, DataSource.AXIS_360)
        m([i1.id], [], [source3])
        assert good_summary == w.summary_text

        # LIBRARY_STAFF is always considered a good source of
        # descriptions.
        l1.data_source = DataSource.lookup(
            db_session, DataSource.LIBRARY_STAFF
        )
        m([i1.id, i2.id], [], [])
        assert l1.resource.representation.content.decode("utf-8") == w.summary_text

    def test_set_presentation_ready_based_on_content(self, db_session, create_work):
        """
        GIVEN: A Work
        WHEN:  Setting the Work as presentation ready based on its data
        THEN:  Work is either presentation ready or not
        """
        work = create_work(db_session, with_license_pool=True)

        search = MockExternalSearchIndex()
        # This is how the work will be represented in the dummy search index.
        _ = (search.works_index, MockExternalSearchIndex.work_document_type, work.id)

        presentation = work.presentation_edition
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is True

        # The work has not been added to the search index.
        assert list(search.docs.keys()) == []

        # But the work of adding it to the search engine has been
        # registered.
        def assert_record():
            # Verify the search index WorkCoverageRecord for this work
            # is in the REGISTERED state.
            [record] = [
                x for x in work.coverage_records
                if x.operation == WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
            ]
            assert WorkCoverageRecord.REGISTERED == record.status
        assert_record()

        # This work is presentation ready because it has a title.
        # Remove the title, and the work stops being presentation
        # ready.
        presentation.title = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is False

        # The search engine WorkCoverageRecord is still in the
        # REGISTERED state, but its meaning has changed -- the work
        # will now be _removed_ from the search index, rather than
        # updated.
        assert_record()

        # Restore the title, and everything is fixed.
        presentation.title = "foo"
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is True

        # Remove the medium, and the work stops being presentation ready.
        presentation.medium = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is False

        presentation.medium = Edition.BOOK_MEDIUM
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is True

        # Remove the language, and it stops being presentation ready.
        presentation.language = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is False

        presentation.language = 'eng'
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is True

        # Remove the fiction status, and the work is still
        # presentation ready. Fiction status used to make a difference, but
        # it no longer does.
        work.fiction = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        assert work.presentation_ready is True

    def test_assign_genres_from_weights(self, db_session, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work
        WHEN:  Assigning genres with weights
        THEN:  The Work has weighted genres
        """
        work = create_work(db_session)

        # This work was once classified under Fantasy and Romance.
        work.assign_genres_from_weights({Romance: 1000, Fantasy: 1000})
        db_session.commit()
        before = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        assert [('Fantasy', 0.5), ('Romance', 0.5)] == before

        # But now it's classified under Science Fiction and Romance.
        work.assign_genres_from_weights({Romance: 100, Science_Fiction: 300})
        db_session.commit()
        after = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        assert [('Romance', 0.25), ('Science Fiction', 0.75)] == after

    def test_classifications_with_genre(
            self, db_session, create_work, create_classification, create_subject, init_datasource_and_genres):
        """
        GIVEN: A Work, multiple Subjects, and multiple Classifications
        WHEN:  Getting a Work's classifications that have genres
        THEN:  Classifications are returned
        """
        work = create_work(db_session, with_open_access_download=True)
        identifier = work.presentation_edition.primary_identifier
        genres = db_session.query(Genre).all()
        subject1 = create_subject(db_session, type="type1", identifier="subject1")
        subject1.genre = genres[0]
        subject2 = create_subject(db_session, type="type2", identifier="subject2")
        subject2.genre = genres[1]
        subject3 = create_subject(db_session, type="type2", identifier="subject3")
        subject3.genre = None
        source = DataSource.lookup(db_session, DataSource.AXIS_360)
        classification1 = create_classification(
            db_session,
            identifier=identifier, subject=subject1,
            data_source=source, weight=1)
        classification2 = create_classification(
            db_session,
            identifier=identifier, subject=subject2,
            data_source=source, weight=2)
        _ = create_classification(
            db_session,
            identifier=identifier, subject=subject3,
            data_source=source, weight=2)

        results = work.classifications_with_genre().all()

        assert [classification2, classification1] == results

    def test_mark_licensepools_as_superceded(
            self, db_session, create_edition, create_licensepool, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work associated with varying LicensePools
        WHEN:  Marking LicensePools as superceded
        THEN:  All but the best pool are superceded
        """
        # A commercial LP that somehow got superceded will be
        # un-superceded.
        edition = create_edition(db_session)
        commercial = create_licensepool(
            db_session, edition, data_source_name=DataSource.OVERDRIVE
        )
        work, _ = commercial.calculate_work()
        commercial.superceded = True
        work.mark_licensepools_as_superceded()
        assert commercial.superceded is False

        # An open-access LP that was superceded will be un-superceded if
        # chosen.
        edition = create_edition(db_session)
        gutenberg = create_licensepool(
            db_session, edition, data_source_name=DataSource.GUTENBERG,
            open_access=True, with_open_access_download=True
        )
        work, _ = gutenberg.calculate_work()
        gutenberg.superceded = True
        work.mark_licensepools_as_superceded()
        assert gutenberg.superceded is False

        # Of two open-access LPs, the one from the higher-quality data
        # source will be un-superceded, and the one from the
        # lower-quality data source will be superceded.
        edition = create_edition(db_session)
        standard_ebooks = create_licensepool(
            db_session, edition, data_source_name=DataSource.STANDARD_EBOOKS,
            open_access=True, with_open_access_download=True
        )
        work.license_pools.append(standard_ebooks)
        gutenberg.superceded = False
        standard_ebooks.superceded = True
        work.mark_licensepools_as_superceded()
        assert gutenberg.superceded is True
        assert standard_ebooks.superceded is False

        # Of three open-access pools, 1 and only 1 will be chosen as non-superceded.
        gitenberg1 = create_licensepool(
            db_session, edition=create_edition(db_session), open_access=True,
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gitenberg2 = create_licensepool(
            db_session, edition=create_edition(db_session), open_access=True,
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gutenberg1 = create_licensepool(
            db_session, edition=create_edition(db_session), open_access=True,
            data_source_name=DataSource.GUTENBERG, with_open_access_download=True
        )

        work_multipool = create_work(db_session, presentation_edition=None)
        work_multipool.license_pools.append(gutenberg1)
        work_multipool.license_pools.append(gitenberg2)
        work_multipool.license_pools.append(gitenberg1)

        # pools aren't yet aware of each other
        assert gutenberg1.superceded is False
        assert gitenberg1.superceded is False
        assert gitenberg2.superceded is False

        # make pools figure out who's best
        work_multipool.mark_licensepools_as_superceded()

        assert gutenberg1.superceded is True
        # There's no way to choose between the two gitenberg pools,
        # so making sure only one has been chosen is enough.
        chosen_count = 0
        for chosen_pool in gutenberg1, gitenberg1, gitenberg2:
            if chosen_pool.superceded is False:
                chosen_count += 1
        assert chosen_count == 1

        # throw wrench in
        gitenberg1.suppressed = True

        # recalculate bests
        work_multipool.mark_licensepools_as_superceded()
        assert gutenberg1.superceded is True
        assert gitenberg1.superceded is True
        assert gitenberg2.superceded is False

        # A suppressed pool won't be superceded if it's the only pool for a work.
        only_pool = create_licensepool(
            db_session, edition=create_edition(db_session),
            open_access=True, with_open_access_download=True
        )
        work, _ = only_pool.calculate_work()
        only_pool.suppressed = True
        work.mark_licensepools_as_superceded()
        assert only_pool.superceded is False

    def test_work_remains_viable_on_pools_suppressed(self, db_session, get_sample_ecosystem):
        """
        GIVEN: A Work associated with multiple LicensePools
        WHEN:  The Work has all of its pools suppressed
        THEN:  The work's author, title, and subtitle still have the last best-known info in them
        """
        (work, pool_std_ebooks, pool_git, pool_gut,
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = get_sample_ecosystem(db_session)

        # make sure the setup is what we expect
        assert pool_std_ebooks.suppressed is False
        assert pool_git.suppressed is False
        assert pool_gut.suppressed is False

        # sanity check - we like standard ebooks and it got determined to be the best
        assert work.presentation_edition == pool_std_ebooks.presentation_edition
        assert work.presentation_edition == edition_std_ebooks

        # editions know who's the presentation edition
        assert edition_std_ebooks.work == work
        assert edition_git.work is None
        assert edition_gut.work is None

        # The title of the Work is the title of its presentation edition.
        assert work.title == "The Standard Ebooks Title"
        assert work.subtitle == "The Standard Ebooks Subtitle"

        # The author of the Work is the author of its presentation edition.
        assert "Alice Adder" == work.author
        assert "Adder, Alice" == work.sort_author

        # now suppress all of the license pools
        pool_std_ebooks.suppressed = True
        pool_git.suppressed = True
        pool_gut.suppressed = True

        # and let work know
        work.calculate_presentation()

        # standard ebooks was last viable pool, and it stayed as work's choice
        assert work.presentation_edition == pool_std_ebooks.presentation_edition
        assert work.presentation_edition == edition_std_ebooks

        # editions know who's the presentation edition
        assert edition_std_ebooks.work == work
        assert edition_git.work is None
        assert edition_gut.work is None

        # The title of the Work is still the title of its last viable presentation edition.
        assert work.title == "The Standard Ebooks Title"
        assert work.subtitle == "The Standard Ebooks Subtitle"

        # The author of the Work is still the author of its last viable presentation edition.
        assert work.author == "Alice Adder"
        assert work.sort_author == "Adder, Alice"

    def test_work_updates_info_on_pool_suppressed(self, db_session, get_sample_ecosystem):
        """
        GIVEN: A Work associated with multiple LicensePools
        WHEN:  The provider of the work's presentation edition gets suppressed
        THEN:  The Work will choose another child license pool's presentation edition as its presentation edition.
        """
        (work, pool_std_ebooks, pool_git, pool_gut,
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = get_sample_ecosystem(db_session)

        # make sure the setup is what we expect
        assert pool_std_ebooks.suppressed is False
        assert pool_git.suppressed is False
        assert pool_gut.suppressed is False

        # sanity check - we like standard ebooks and it got determined to be the best
        assert work.presentation_edition == pool_std_ebooks.presentation_edition
        assert work.presentation_edition == edition_std_ebooks

        # editions know who's the presentation edition
        assert edition_std_ebooks.work == work
        assert edition_git.work is None
        assert edition_gut.work is None

        # The title of the Work is the title of its presentation edition.
        assert work.title == "The Standard Ebooks Title"
        assert work.subtitle == "The Standard Ebooks Subtitle"

        # The author of the Work is the author of its presentation edition.
        assert work.author == "Alice Adder"
        assert work.sort_author == "Adder, Alice"

        # now suppress the primary license pool
        pool_std_ebooks.suppressed = True

        # and let work know
        work.calculate_presentation()

        # gitenberg is next best and it got determined to be the best
        assert work.presentation_edition == pool_git.presentation_edition
        assert work.presentation_edition == edition_git

        # editions know who's the presentation edition
        assert edition_std_ebooks.work is None
        assert edition_git.work is work
        assert edition_gut.work is None

        # The title of the Work is still the title of its last viable presentation edition.
        assert work.title == "The GItenberg Title"
        assert work.subtitle == "The GItenberg Subtitle"

        # The author of the Work is still the author of its last viable presentation edition.
        assert work.author == "Alice Adder, Bob Bitshifter"
        assert work.sort_author == "Adder, Alice ; Bitshifter, Bob"

    def test_different_language_means_different_work(self, db_session, create_edition):
        """
        GIVEN: Two open-access LicensePools for the same book in different languages
        WHEN:  Retrieving the Work for a LicensePool
        THEN:  The Works are treated as separate due to having different languages
        """
        title = 'Siddhartha'
        author = ['Herman Hesse']
        _, lp1 = create_edition(
            db_session, title=title, authors=author, language='eng',
            with_license_pool=True, with_open_access_download=True
        )
        w1 = lp1.calculate_work()
        _, lp2 = create_edition(
            db_session, title=title, authors=author, language='ger',
            with_license_pool=True, with_open_access_download=True
        )
        w2 = lp2.calculate_work()
        for pool in (lp1, lp2):
            assert pool.superceded is False
        assert w1 != w2

    def test_reject_covers(
            self, db_session, create_edition, create_representation, create_work,
            get_sample_cover_path, init_datasource_and_genres):
        """
        GIVEN: A cover Representation, a Work, an Identifier, and multiple Editions
        WHEN:  Rejecting covers
        THEN:  The Works and Editions associated with the cover no longer have a cover
        """
        edition, lp = create_edition(db_session, with_open_access_download=True)

        # Create a cover and thumbnail for the edition.
        sample_cover_path = get_sample_cover_path('test-book-cover.png')
        cover_href = 'http://cover.png'
        cover_link = lp.add_link(
            Hyperlink.IMAGE, cover_href, lp.data_source,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path, 'rb').read()
        )[0]

        thumbnail_href = 'http://thumbnail.png'
        thumbnail_rep = create_representation(
            db_session,
            url=thumbnail_href,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path, 'rb').read(),
            mirrored=True
        )

        cover_rep = cover_link.resource.representation
        cover_rep.mirror_url = cover_href
        cover_rep.mirrored_at = utc_now()
        cover_rep.thumbnails.append(thumbnail_rep)

        edition.set_cover(cover_link.resource)
        full_url = cover_link.resource.url
        thumbnail_url = thumbnail_rep.mirror_url

        # A Work created from this edition has cover details.
        work = create_work(db_session, presentation_edition=edition)
        assert work.cover_full_url and work.cover_thumbnail_url

        # A couple helper methods to make these tests more readable.
        def has_no_cover(work_or_edition):
            """Determines whether a Work or an Edition has a cover."""
            assert work_or_edition.cover_full_url is None
            assert work_or_edition.cover_thumbnail_url is None
            assert (cover_link.resource.voted_quality < 0) is True
            assert (cover_link.resource.votes_for_quality > 0) is True

            if isinstance(work_or_edition, Work):
                # It also removes the link from the cached OPDS entries.
                for url in [full_url, thumbnail_url]:
                    assert url not in work.simple_opds_entry
                    assert url not in work.verbose_opds_entry

            return True

        def reset_cover():
            """Makes the cover visible again for the main work object
            and confirms its visibility.
            """
            r = cover_link.resource
            r.votes_for_quality = r.voted_quality = 0
            r.update_quality()
            work.calculate_presentation(search_index_client=index)
            assert full_url == work.cover_full_url
            assert thumbnail_url == work.cover_thumbnail_url
            for url in [full_url, thumbnail_url]:
                assert url in work.simple_opds_entry
                assert url in work.verbose_opds_entry

        # Suppressing the cover removes the cover from the work.
        index = MockExternalSearchIndex()
        Work.reject_covers(db_session, [work], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # It also works with Identifiers.
        identifier = work.license_pools[0].identifier
        Work.reject_covers(db_session, [identifier], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # When other Works or Editions share a cover, they are also
        # updated during the suppression process.
        other_edition = create_edition(db_session)
        other_edition.set_cover(cover_link.resource)
        other_work_ed = create_edition(db_session)
        other_work_ed.set_cover(cover_link.resource)
        other_work = create_work(db_session, presentation_edition=other_work_ed)

        Work.reject_covers(db_session, [work], search_index_client=index)
        assert has_no_cover(other_edition)
        assert has_no_cover(other_work)

    def test_missing_coverage_from(self, db_session, create_work, create_work_coverage_record):
        """
        GIVEN: A Work
        WHEN:  Finding Works that don't have any WorkCoverageRecords
        THEN:  Returns Works that are missing coverage records
        """
        operation = 'the_operation'

        # Here's a work with a coverage record.
        work = create_work(db_session, with_license_pool=True)

        # It needs coverage.
        assert Work.missing_coverage_from(db_session, operation).all() == [work]

        # Let's give it coverage.
        record = create_work_coverage_record(db_session, work, operation)

        # It no longer needs coverage!
        assert Work.missing_coverage_from(db_session, operation).all() == []

        # But if we disqualify coverage records created before a
        # certain time, it might need coverage again.
        cutoff = record.timestamp + datetime.timedelta(seconds=1)

        assert Work.missing_coverage_from(db_session, operation, count_as_missing_before=cutoff).all() == [work]

    def test_top_genre(self, db_session, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work with genres of varying affinities
        WHEN:  Getting the Work's top genre
        THEN:  The Work's top genre by affinity score is returned
        """
        work = create_work(db_session)
        genres = db_session.query(Genre).all()

        # returns None when work has no genres
        assert work.top_genre() is None

        # returns only genre
        wg1, _ = get_one_or_create(db_session, WorkGenre, work=work, genre=genres[0], affinity=1)
        assert work.top_genre() == genres[0].name

        # returns top genre
        wg1.affinity = 0.2
        get_one_or_create(db_session, WorkGenre, work=work, genre=genres[1], affinity=0.8)
        assert work.top_genre() == genres[1].name

    def test_to_search_document(
            self, db_session, create_collection, create_customlist, create_edition, create_identifier,
            create_licensepool, create_work, default_library, init_datasource_and_genres):
        """
        GIVEN: A Work with a presentation edition that has a license pool associated with a collection
        WHEN:  Generating search documents
        THEN:  Returns a document with information about a Work and Edition
        """
        # Set up an edition and work.
        [collection1] = default_library.collections
        edition, pool1 = create_edition(db_session, authors=["Author 1", "Author 2"], 
                                        with_license_pool=True, collection=collection1)
        work = create_work(db_session, presentation_edition=edition)

        # Create a second Collection that has a different LicensePool for the same Work.
        collection2 = create_collection(db_session)
        default_library.collections.append(collection2)
        pool2 = create_licensepool(db_session, edition=edition, collection=collection2)
        pool2.work_id = work.id
        pool2.licenses_available = 0
        pool2.licenses_owned = 10
        work.license_pools.append(pool2)

        # Create a third Collection that's just hanging around, not doing anything.
        _ = create_collection(db_session)

        # These are the edition's authors.
        [contributor1] = [c.contributor for c in edition.contributions if c.role == Contributor.PRIMARY_AUTHOR_ROLE]
        contributor1.display_name = "Contributor Display Name"
        contributor1.family_name = "Contributor Full Name"
        contributor1.viaf = "Contributor VIAF"
        contributor1.lc = "Contributor LC"
        [contributor2] = [c.contributor for c in edition.contributions if c.role == Contributor.AUTHOR_ROLE]

        data_source = DataSource.lookup(db_session, DataSource.THREEM)

        # This identifier is strongly equivalent to the edition's.
        identifier1 = create_identifier(db_session, identifier_type=Identifier.ISBN)
        identifier1.equivalent_to(data_source, edition.primary_identifier, 0.9)

        # This identifier is equivalent to the other identifier, but the strength
        # is too weak for it to be used.
        identifier2 = create_identifier(db_session, identifier_type=Identifier.ISBN)
        identifier2.equivalent_to(data_source, identifier2, 0.1)

        # This identifier is equivalent to the _edition's_, but too weak to
        # be used.
        identifier3 = create_identifier(db_session, identifier_type=Identifier.ISBN)
        identifier3.equivalent_to(data_source, edition.primary_identifier, 0.1)

        # Add some classifications.

        # This classification has no subject name, so the search document will use the subject identifier.
        edition.primary_identifier.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 6)

        # This one has the same subject type and identifier, so their weights will be combined.
        identifier1.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 1)

        # Here's another classification with a different subject type.
        edition.primary_identifier.classify(data_source, Subject.OVERDRIVE, "Romance", None, 2)

        # This classification has a subject name, so the search document will use that instead of the identifier.
        identifier1.classify(data_source, Subject.FAST, "FAST", "Sea Stories", 7)

        # This classification will be left out because its subject type isn't useful for search.
        identifier1.classify(data_source, Subject.DDC, "DDC", None)

        # These classifications will be left out because their identifiers aren't sufficiently equivalent to the edition's.
        identifier2.classify(data_source, Subject.FAST, "Fast2", None)
        identifier3.classify(data_source, Subject.FAST, "Fast3", None)

        # Add some genres.
        genre1, _ = Genre.lookup(db_session, "Science Fiction")
        genre2, _ = Genre.lookup(db_session, "Romance")
        work.genres = [genre1, genre2]
        work.work_genres[0].affinity = 1

        # Add two custom lists. The work is featured on one list but
        # not the other.
        appeared_1 = datetime_utc(2010, 1, 1)
        appeared_2 = datetime_utc(2011, 1, 1)
        l1, _ = create_customlist(db_session, num_entries=0)
        l1.add_entry(work, featured=False, update_external_index=False, first_appearance=appeared_1)
        l2, _ = create_customlist(db_session, num_entries=0)
        l2.add_entry(work, featured=True, update_external_index=False, first_appearance=appeared_2)

        # Add the other fields used in the search document.
        work.target_age = NumericRange(7, 8, '[]')
        edition.subtitle = "Subtitle"
        edition.series = "Series"
        edition.series_position = 99
        edition.publisher = "Publisher"
        edition.imprint = "Imprint"
        work.fiction = False
        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        work.summary_text = "Summary Text"
        work.rating = 5
        work.popularity = 4
        work.last_update_time = utc_now()

        # Make sure all of this will show up in a database query.
        db_session.flush()

        def assert_time_match(python, postgres):
            """Compare a datetime object and a Postgres
            seconds-since-epoch as closely as possible.

            The Postgres numbers are generated by a database function,
            and have less precision than the datetime objects used to
            put the data in the database, but we can check that it's
            basically the same time.

            :param python: A datetime from the Python part of this test.
            :param postgres: A float from the Postgres part.
            """
            expect = (
                python - from_timestamp(0)
            ).total_seconds()
            assert int(expect) == int(postgres)

        search_doc = work.to_search_document()
        assert work.id == search_doc['_id']
        assert work.id == search_doc['work_id']
        assert work.title == search_doc['title']
        assert edition.subtitle == search_doc['subtitle']
        assert edition.series == search_doc['series']
        assert edition.series_position == search_doc['series_position']
        assert edition.language == search_doc['language']
        assert work.sort_title == search_doc['sort_title']
        assert work.author == search_doc['author']
        assert work.sort_author == search_doc['sort_author']
        assert edition.publisher == search_doc['publisher']
        assert edition.imprint == search_doc['imprint']
        assert edition.permanent_work_id == search_doc['permanent_work_id']
        assert "Nonfiction" == search_doc['fiction']
        assert "YoungAdult" == search_doc['audience']
        assert work.summary_text == search_doc['summary']
        assert work.quality == search_doc['quality']
        assert work.rating == search_doc['rating']
        assert work.popularity == search_doc['popularity']
        assert work.presentation_ready == search_doc['presentation_ready']
        assert_time_match(work.last_update_time, search_doc['last_update_time'])
        assert dict(lower=7, upper=8) == search_doc['target_age']

        # Each LicensePool for the Work is listed in
        # the 'licensepools' section.
        licensepools = search_doc['licensepools']
        assert len(licensepools) == 2
        assert set([x['licensepool_id'] for x in licensepools]) == set([x.id for x in work.license_pools])

        # Each item in the 'licensepools' section has a variety of useful information
        # about the corresponding LicensePool.
        for pool in work.license_pools:
            [match] = [x for x in licensepools if x['licensepool_id'] == pool.id]
            assert pool.open_access == match['open_access']
            assert pool.collection_id == match['collection_id']
            assert pool.suppressed == match['suppressed']
            assert pool.data_source_id == match['data_source_id']

            assert isinstance(match['available'], bool)
            assert (pool.licenses_available > 0) == match['available']
            assert isinstance(match['licensed'], bool)
            assert (pool.licenses_owned > 0) == match['licensed']

            # The work quality is stored in the main document, but
            # it's also stored in the license pool subdocument so that
            # we can apply a nested filter that includes quality +
            # information from the subdocument.
            assert work.quality == match['quality']

            assert_time_match(
                pool.availability_time, match['availability_time']
            )

            # The medium of the work's presentation edition is stored
            # in the main document, but it's also stored in the
            # license poolsubdocument, so that we can filter out
            # license pools that represent audiobooks from unsupported
            # sources.
            assert edition.medium == search_doc['medium']
            assert edition.medium == match['medium']

        # Each identifier that could, with high confidence, be
        # associated with the work, is in the 'identifiers' section.
        #
        # This includes each identifier associated with a LicensePool
        # for the work, and the ISBN associated with one of those
        # LicensePools through a high-confidence equivalency. It does
        # not include the low-confidence ISBN, or any of the
        # identifiers not tied to a LicensePool.
        expect = [
            dict(identifier=identifier1.identifier, type=identifier1.type),
            dict(identifier=pool1.identifier.identifier,
                 type=pool1.identifier.type),
        ]

        def s(x):
            # Sort an identifier dictionary by its identifier value.
            return sorted(x, key=lambda b: b['identifier'])
        assert s(expect) == s(search_doc['identifiers'])

        # Each custom list entry for the work is in the 'customlists' section.
        not_featured, featured = sorted(search_doc['customlists'], key=lambda x: x['featured'])
        assert_time_match(appeared_1, not_featured.pop('first_appearance'))
        assert dict(featured=False, list_id=l1.id) == not_featured
        assert_time_match(appeared_2, featured.pop('first_appearance'))
        assert dict(featured=True, list_id=l2.id) == featured

        contributors = search_doc['contributors']
        assert len(contributors) == 2

        [contributor1_doc] = [c for c in contributors if c['sort_name'] == contributor1.sort_name]
        [contributor2_doc] = [c for c in contributors if c['sort_name'] == contributor2.sort_name]

        assert contributor1.display_name == contributor1_doc['display_name']
        assert contributor2_doc['display_name'] is None

        assert contributor1.family_name == contributor1_doc['family_name']
        assert contributor2_doc['family_name'] is None

        assert contributor1.viaf == contributor1_doc['viaf']
        assert contributor2_doc['viaf'] is None

        assert contributor1.lc == contributor1_doc['lc']
        assert contributor2_doc['lc'] is None

        assert Contributor.PRIMARY_AUTHOR_ROLE == contributor1_doc['role']
        assert Contributor.AUTHOR_ROLE == contributor2_doc['role']

        classifications = search_doc['classifications']
        assert len(classifications) == 3
        [classification1_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.BISAC]]
        [classification2_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.OVERDRIVE]]
        [classification3_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.FAST]]
        assert classification1_doc['term'] == "FICTION Science Fiction Time Travel"
        assert classification1_doc['weight'] == float(6 + 1)/(6 + 1 + 2 + 7)
        assert classification2_doc['term'] == "Romance"
        assert classification2_doc['weight'] == float(2)/(6 + 1 + 2 + 7)
        assert classification3_doc['term'] == "Sea Stories"
        assert classification3_doc['weight'] == float(7)/(6 + 1 + 2 + 7)

        genres = search_doc['genres']
        assert len(genres) == 2
        [genre1_doc] = [g for g in genres if g['name'] == genre1.name]
        [genre2_doc] = [g for g in genres if g['name'] == genre2.name]
        assert genre1_doc['scheme'] == Subject.SIMPLIFIED_GENRE
        assert genre1_doc['term'] == genre1.id
        assert genre1_doc['weight'] == 1
        assert genre2_doc['scheme'] == Subject.SIMPLIFIED_GENRE
        assert genre2_doc['term'] == genre2.id
        assert genre2_doc['weight'] == 0

        target_age_doc = search_doc['target_age']
        assert target_age_doc['lower'] == work.target_age.lower
        assert target_age_doc['upper'] == work.target_age.upper

        # If a book stops being available through a collection
        # (because its LicensePool loses all its licenses or stops
        # being open access), it will no longer be listed
        # in its Work's search document.
        [pool] = collection1.licensepools
        pool.licenses_owned = 0
        db_session.commit()
        search_doc = work.to_search_document()
        assert [x['collection_id'] for x in search_doc['licensepools']] == [collection2.id]

        # If the book becomes available again, the collection will
        # start showing up again.
        pool.open_access = True
        db_session.commit()
        search_doc = work.to_search_document()
        assert set([x['collection_id'] for x in search_doc['licensepools']]) == set([collection1.id, collection2.id])

    def test_age_appropriate_for_patron(self, db_session, create_patron, create_work):
        """
        GIVEN: A Patron and a Work for a target audience and target age range
        WHEN:  Determining if the Work is age-appropriate for a Patron
        THEN:  Retruns True or False
        """
        work = create_work(db_session)
        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        work.target_age = tuple_to_numericrange((12, 15))
        patron = create_patron(db_session)

        # If no Patron is specified, the method always returns True.
        assert work.age_appropriate_for_patron(None) is True

        # Otherwise, this method is a simple passthrough for
        # Patron.work_is_age_appropriate.
        patron.work_is_age_appropriate = MagicMock(return_value="value")

        assert "value" == work.age_appropriate_for_patron(patron)
        patron.work_is_age_appropriate.assert_called_with(
            work.audience, work.target_age
        )

    def test_age_appropriate_for_patron_end_to_end(self, db_session, create_lane, create_patron, create_work):
        """
        GIVEN: A Patron and a Work
        WHEN:  Determining if the Work is age-appropriate for a Patron
        THEN:  Returns True or False
        """
        # A test of age_appropriate_for_patron without any mocks.
        # More detailed unit tests are in test_patron.py.
        #
        # Some end-to-end examples are useful because the
        # 'age-appropriate' logic is quite complicated, and because
        # target age ranges are sometimes passed around as tuples and
        # sometimes as NumericRange objects.
        patron = create_patron(db_session)
        patron.external_type = "a"

        # This Lane contains books at the old end of the "children"
        # range and the young end of the "young adult" range.
        lane = create_lane(db_session)
        lane.root_for_patron_type = ["a"]

        # A patron with this root lane can see children's and YA
        # titles in the age range 9-14.

        # NOTE: setting target_age sets .audiences to appropriate values,
        # so setting .audiences here is purely demonstrative.
        lane.audiences = [
            Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT
        ]
        lane.target_age = (9, 14)

        # This work is a YA title within the age range.
        work = create_work(db_session)
        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        work.target_age = tuple_to_numericrange((12, 15))
        assert work.age_appropriate_for_patron(patron) is True

        # Bump up the target age of the work, and it stops being
        # age-appropriate.
        work.target_age = tuple_to_numericrange((16, 17))
        assert work.age_appropriate_for_patron(patron) is False

        # Bump up the lane to match, and it's age-appropriate again.
        lane.target_age = (9, 16)
        assert work.age_appropriate_for_patron(patron) is True

        # Change the audience to AUDIENCE_ADULT, and the work stops being
        # age-appropriate.
        work.audience = Classifier.AUDIENCE_ADULT
        assert work.age_appropriate_for_patron(patron) is False

    def test_unlimited_access_books_are_available_by_default(
            self, db_session, create_edition, create_work, init_delivery_mechanism):
        """
        GIVEN: A Work with an Edition and LicensePool with unlimited access
        WHEN:  Searching the Work's search document
        THEN:  The unlimited access LicensePool is found in the search document
        """
        # Set up an edition and work.
        edition, pool = create_edition(db_session, authors=["Author 1", "Author 2"], with_license_pool=True)
        work = create_work(db_session, presentation_edition=edition)

        pool.open_access = False
        pool.self_hosted = False
        pool.unlimited_access = True

        # Make sure all of this will show up in a database query.
        db_session.flush()

        search_doc = work.to_search_document()

        # Each LicensePool for the Work is listed in
        # the 'licensepools' section.
        licensepools = search_doc['licensepools']
        assert len(licensepools) == 1
        assert licensepools[0]['open_access'] is False
        assert licensepools[0]['available'] is True

    def test_self_hosted_books_are_available_by_default(self, db_session, create_edition, create_work):
        """
        GIVEN: A Work with an Edition and a LicensePool that is self hosted
        WHEN:  Searching the Work's search document
        THEN:  The self hosted LicensePool is found in the search document
        """
        # Set up an edition and work.
        edition, pool = create_edition(db_session, authors=["Author 1", "Author 2"], with_license_pool=True)
        work = create_work(db_session, presentation_edition=edition)

        pool.licenses_owned = 0
        pool.licenses_available = 0
        pool.self_hosted = True

        # Make sure all of this will show up in a database query.
        db_session.flush()

        search_doc = work.to_search_document()

        # Each LicensePool for the Work is listed in
        # the 'licensepools' section.
        licensepools = search_doc['licensepools']
        assert len(licensepools) == 1
        assert licensepools[0]['open_access'] is False
        assert licensepools[0]['available'] is True

    @pytest.mark.parametrize(
        'lower,upper,bounds,expected_range',
        [
            (7, 8, '[]', '7-8'),
            (0, 8, '[]', '0-8'),
            (8, None, '[]', '8'),
            (None, 8, '[]', '8'),
            (7, 8, '[)', '7'),
            (0, 8, '[)', '0-7'),
            (7, 8, '(]', '8'),
            (0, 8, '(]', '1-8'),
            (7, 9, '()', '8'),
            (0, 8, '()', '1-7'),
            (None, None, '()', ''),
        ]
    )
    def test_target_age_string(self, db_session, create_work, lower, upper, bounds, expected_range):
        """
        GIVEN: Numerical range with bounds
        WHEN:  Getting the target age range as a string
        THEN:  Age range is returned as a string
        """
        work = create_work(db_session)
        work.target_age = NumericRange(lower, upper, bounds)

        assert work.target_age_string == expected_range

    def test_target_age_string_none(self, db_session, create_work):
        """
        GIVEN: A target age range of None
        WHEN:  Getting the target age range as a string
        THEN:  An empty string is returned
        """
        work = create_work(db_session)
        work.target_age = None

        assert work.target_age_string == ""

    def test_reindex_on_availability_change(self, db_session, create_collection, create_work):
        """
        GIVEN: A Work with a LicensePool
        WHEN:  Changing the LicensePool's availability
        THEN:  A WorkCoverageRecord is created indicating taht the Work needs to be re-indexed
        """
        def find_record(work):
            """Find the Work's 'update search index operation'
            WorkCoverageRecord.
            """
            records = [
                x for x in work.coverage_records
                if x.operation.startswith(
                        WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
                )
            ]
            if records:
                return records[0]
            return None
        registered = WorkCoverageRecord.REGISTERED
        success = WorkCoverageRecord.SUCCESS

        # A Work with no LicensePool isn't registered as needing
        # indexing. (It will be indexed anyway, but it's not registered
        # as needing it.)
        no_licensepool = create_work(db_session)
        assert find_record(no_licensepool) is None

        # A Work with a LicensePool starts off in a state where it
        # needs to be indexed.
        work = create_work(db_session, with_open_access_download=True)
        [pool] = work.license_pools
        record = find_record(work)
        assert record.status == registered

        # If it stops being open-access, it needs to be reindexed.
        record.status = success
        pool.open_access = False
        record = find_record(work)
        assert record.status == registered

        # If it becomes open-access again, it needs to be reindexed.
        record.status = success
        pool.open_access = True
        record = find_record(work)
        assert record.status == registered

        # If its last_update_time is changed, it needs to be
        # reindexed. (This happens whenever
        # LicensePool.update_availability is called, meaning that
        # patron transactions always trigger a reindex).
        record.status = success
        work.last_update_time = utc_now()
        assert record.status == registered

        # If its collection changes (which shouldn't happen), it needs
        # to be reindexed.
        record.status = success
        collection2 = create_collection(db_session)
        pool.collection_id = collection2.id
        assert record.status == registered

        # If a LicensePool is deleted (which also shouldn't happen),
        # its former Work needs to be reindexed.
        record.status = success
        db_session.delete(pool)
        work = db_session.query(Work).filter(Work.id == work.id).one()
        record = find_record(work)
        assert record.status == registered

        # If a LicensePool is moved in from another Work, _both_ Works
        # need to be reindexed.
        record.status = success
        another_work = create_work(db_session, with_license_pool=True)
        [another_pool] = another_work.license_pools
        work.license_pools.append(another_pool)
        assert another_work.license_pools == []

        for work in (work, another_work):
            record = find_record(work)
            assert record.status == registered

    def test_reset_coverage(self, db_session, create_work):
        """
        GIVEN: A Work
        WHEN:  Resetting coverage for a Work through various methods
        THEN:  Status indicates that some task needs to be performed again
        """
        WCR = WorkCoverageRecord
        work = create_work(db_session)
        work.presentation_ready = True
        index = MockExternalSearchIndex()

        # Calling _reset_coverage when there is no coverage creates
        # a new WorkCoverageRecord in the REGISTERED state
        operation = "an operation"
        record = work._reset_coverage(operation)
        assert record.status == WCR.REGISTERED

        # Calling _reset_coverage when the WorkCoverageRecord already
        # exists sets the state back to REGISTERED.
        record.state = WCR.SUCCESS
        work._reset_coverage(operation)
        assert record.status == WCR.REGISTERED

        # A number of methods with helpful names all call _reset_coverage
        # for some specific operation.
        def mock_reset_coverage(operation):
            work.coverage_reset_for = operation
        work._reset_coverage = mock_reset_coverage

        for method, operation in (
            (work.needs_full_presentation_recalculation, WCR.CLASSIFY_OPERATION),
            (work.needs_new_presentation_edition, WCR.CHOOSE_EDITION_OPERATION),
            (work.external_index_needs_updating, WCR.UPDATE_SEARCH_INDEX_OPERATION)
        ):
            method()
            assert work.coverage_reset_for == operation

        # The work was not added to the search index when we called
        # external_index_needs_updating. That happens later, when the
        # WorkCoverageRecord is processed.
        assert list(index.docs.values()) == []

    def test_for_unchecked_subjects(self, db_session, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work with a LicensePool with an Identifier that has unchecked Subjects
        WHEN:  Searching for Works that have an Identifier for these subjects
        THEN:  Works are returned that need to be reclassified
        """
        w1 = create_work(db_session, with_license_pool=True)
        w2 = create_work(db_session)
        identifier = w1.license_pools[0].identifier

        # Neither of these works is associated with any subjects, so
        # they're not associated with any unchecked subjects.
        qu = Work.for_unchecked_subjects(db_session)
        assert qu.all() == []

        # These Subjects haven't been checked, so the Work associated with
        # them shows up.
        ds = DataSource.lookup(db_session, DataSource.OVERDRIVE)
        classification = identifier.classify(ds, Subject.TAG, "some tag")
        classification2 = identifier.classify(ds, Subject.TAG, "another tag")
        assert qu.all() == [w1]

        # If one of them is checked, the Work still shows up.
        classification.subject.checked = True
        assert qu.all() == [w1]

        # Only when all Subjects are checked does the work stop showing up.
        classification2.subject.checked = True
        assert qu.all() == []

    def test_calculate_opds_entries(self, db_session, create_work):
        """
        GIVEN: A Work
        WHEN:  Calculating the Work's OPDS entries based on verbosity
        THEN:  The Work has simple and verbose entries
        """
        work = create_work(db_session)
        work.simple_opds_entry = None
        work.verbose_opds_entry = None

        work.calculate_opds_entries(verbose=False)
        simple_entry = work.simple_opds_entry
        assert simple_entry.startswith('<entry')
        assert work.verbose_opds_entry is None

        work.calculate_opds_entries(verbose=True)
        # The simple OPDS entry is the same length as before.
        # It's not necessarily _exactly_ the same because the
        # <updated> timestamp may be different.
        assert len(simple_entry) == len(work.simple_opds_entry)

        # The verbose OPDS entry is longer than the simple one.
        assert work.verbose_opds_entry.startswith('<entry')
        assert len(work.verbose_opds_entry) > len(simple_entry)

    def test_calculate_marc_record(self, db_session, create_work):
        """
        GIVEN: A Work
        WHEN:  Calculating the Work's MARC record
        THEN:  The Work has a MARC record
        """
        work = create_work(db_session, with_license_pool=True)
        work.marc_record = None

        work.calculate_marc_record()
        assert work.title in work.marc_record
        assert "online resource" in work.marc_record

    def test_active_licensepool_ignores_superceded_licensepools(self, db_session, create_edition, create_work):
        """
        GIVEN: A Work and two LicensePools
        WHEN:  Determining the Work's active license pool based on if the pool has been superseded
        THEN:  If a LicensePool hasn't been superseded then it is returned,
               if both have been superseded None are returned
        """
        work = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [pool1] = work.license_pools
        _, pool2 = create_edition(db_session, with_license_pool=True)
        work.license_pools.append(pool2)

        # Start off with neither LicensePool being open-access. pool1
        # will become open-access later on, which is why we created an
        # open-access download for it.
        pool1.open_access = False
        pool1.licenses_owned = 1

        pool2.open_access = False
        pool2.licenses_owned = 1

        # If there are multiple non-superceded non-open-access license
        # pools for a work, the active license pool is one of them,
        # though we don't really know or care which one.
        assert work.active_license_pool() is not None

        # Neither license pool is open-access, and pool1 is superceded.
        # The active license pool is pool2.
        pool1.superceded = True
        assert pool2 == work.active_license_pool()

        # pool2 is superceded and pool1 is not. The active licensepool
        # is pool1.
        pool1.superceded = False
        pool2.superceded = True
        assert pool1 == work.active_license_pool()

        # If both license pools are superceded, there is no active license
        # pool for the book.
        pool1.superceded = True
        assert work.active_license_pool() is None
        pool1.superceded = False
        pool2.superceded = False

        # If one license pool is open-access and the other is not, the
        # open-access pool wins.
        pool1.open_access = True
        assert pool1 == work.active_license_pool()
        pool1.open_access = False

        # pool2 is open-access but has no usable download. The other
        # pool wins.
        pool2.open_access = True
        assert pool1 == work.active_license_pool()
        pool2.open_access = False

        # If one license pool has no owned licenses and the other has
        # owned licenses, the one with licenses wins.
        pool1.licenses_owned = 0
        pool2.licenses_owned = 1
        assert pool2 == work.active_license_pool()
        pool1.licenses_owned = 1

        # If one license pool has a presentation edition that's missing
        # a title, and the other pool has a presentation edition with a title,
        # the one with a title wins.
        pool2.presentation_edition.title = None
        assert pool1 == work.active_license_pool()

    def test_delete_work(self, db_session, create_work):
        """
        GIVEN: A Work
        WHEN:  Deleting the Work
        THEN:  The Work is deleted from the database and search index
        """
        # Search mock
        class MockSearchIndex():
            removed = []

            def remove_work(self, work):
                self.removed.append(work)

        s = MockSearchIndex()
        work = create_work(db_session, with_license_pool=True)
        work.delete(search_index=s)

        assert db_session.query(Work).filter(Work.id == work.id).all() == []
        assert len(s.removed) == 1
        assert s.removed == [work]


class TestWorkConsolidation:

    def test_calculate_work_success(self, db_session, create_edition):
        """
        GIVEN: An Edition with a LicensePool
        WHEN:  Finding or creating a Work for this LicensePool
        THEN:  A Work is created that has the same presentation edition as the license pool
        """
        _, pool = create_edition(db_session, with_license_pool=True)
        work, new = pool.calculate_work()
        assert pool.presentation_edition == work.presentation_edition
        assert new is True

    def test_calculate_work_bails_out_if_no_title(self, db_session, create_edition, init_datasource_and_genres):
        """
        GIVEN: An Edition with a LicensePool
        WHEN:  Creating a Work from the LicensePool with no title
        THEN:  A Work is created with no title and no permanent work ID for its presentation edition
        """
        edition, pool = create_edition(db_session, with_license_pool=True)
        edition.title = None
        work, new = pool.calculate_work()
        assert work is None
        assert new is False

        # even_if_no_title means we don't need a title.
        work, new = pool.calculate_work(even_if_no_title=True)
        assert isinstance(work, Work)
        assert new is True
        assert work.title is None
        assert work.presentation_edition.permanent_work_id is None

    def test_calculate_work_even_if_no_author(self, db_session, create_edition):
        """
        GIVEN: An Edition, that has no authors, with a LicensePool
        WHEN:  Finding or creating a Work for this LicensePool
        THEN:  A Work is created
        """
        title = "Book"
        _, pool = create_edition(db_session, with_license_pool=True, authors=[], title=title)
        work, new = pool.calculate_work()
        assert work.title == title
        assert new is True

    def test_calculate_work_matches_based_on_permanent_work_id(
            self, db_session, create_edition, init_datasource_and_genres):
        """
        GIVEN: Two Editions that share a permanent work ID
        WHEN:  Creating a Work from each Edition
        THEN:  The Work is only created once and is the same for both Editions
        """
        # Here are two Editions with the same permanent work ID,
        # since they have the same title/author.
        edition1, _ = create_edition(db_session, with_license_pool=True)
        edition2, _ = create_edition(db_session, title=edition1.title, authors=edition1.author, with_license_pool=True)

        # For purposes of this test, let's pretend all these books are
        # open-access.
        for e in [edition1, edition2]:
            for license_pool in e.license_pools:
                license_pool.open_access = True

        # Calling calculate_work() on the first edition creates a Work.
        work1, created = edition1.license_pools[0].calculate_work()
        assert created is True

        # Calling calculate_work() on the second edition associated
        # the second edition's pool with the first work.
        work2, created = edition2.license_pools[0].calculate_work()
        assert created is False

        assert work2 == work1

        expect = edition1.license_pools + edition2.license_pools
        assert set(expect) == set(work1.license_pools)

    def test_calculate_work_for_licensepool_creates_new_work(self, db_session, create_edition):
        """
        GIVEN: Two Editions with their own LicensePools and a Work associated with an Edition
        WHEN:  Creating a Work from the LicensePool
        THEN:  A new Work is created
        """
        edition1, _ = create_edition(
            db_session,
            data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID,
            title="Title", authors=["Author"], with_license_pool=True)

        # This edition is unique to the existing work.
        preexisting_work = Work()
        preexisting_work.set_presentation_edition(edition1)

        # This edition is unique to the new LicensePool
        _, pool = create_edition(
            db_session,
            data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID,
            title="Title", authors=["Author"], with_license_pool=True)

        # Call calculate_work(), and a new Work is created.
        work, created = pool.calculate_work()
        assert created is True
        assert work != preexisting_work

    def test_calculate_work_does_nothing_unless_edition_has_title(
            self, db_session, create_collection, init_datasource_and_genres):
        """
        GIVEN: An Edition, a LicensePool associated with a Collection
        WHEN:  Creating a Work from the LicensePool
        THEN:  Work is not created unless Edition has a title
        """
        DataSource.lookup(db_session, DataSource.GUTENBERG, autocreate=True)
        collection = create_collection(db_session)
        edition, _ = Edition.for_foreign_id(
            db_session, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
        )
        pool, _ = LicensePool.for_foreign_id(
            db_session, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1", collection=collection
        )
        work, created = pool.calculate_work()
        assert work is None

        edition.title = "foo"
        work, created = pool.calculate_work()
        edition.calculate_presentation()
        assert created is True

        # The edition is the work's presentation edition.
        assert edition.work == work
        assert work.presentation_edition == edition
        assert work.title == "foo"
        assert work.author == "[Unknown]"

    def test_calculate_work_fails_when_presentation_edition_identifier_does_not_match_license_pool(
            self, db_session, create_edition, init_datasource_and_genres):
        """
        GIVEN: An Edition with no LicensePool, two Editions with their own LicensePools
        WHEN:  Calculating a Work from a LicensePool that has a different Identifier than presentation edition
        THEN:  A ValueError is raised
        """
        # Here's a LicensePool with an Edition.
        edition1, pool = create_edition(db_session, data_source_name=DataSource.GUTENBERG, with_license_pool=True)

        # Here's a second Edition that's talking about a different Identifier
        # altogether, and has no LicensePool.
        edition2 = create_edition(db_session)
        assert edition1.primary_identifier != edition2.primary_identifier

        # Here's a third Edition that's tied to a totally different
        # LicensePool.
        edition3, _ = create_edition(db_session, with_license_pool=True)
        assert edition1.primary_identifier != edition3.primary_identifier

        # When we calculate a Work for a LicensePool, we can pass in
        # any Edition as the presentation edition, so long as that
        # Edition's primary identifier matches the LicensePool's
        # identifier.
        pool.calculate_work(known_edition=edition1)

        # But we can't pass in an Edition that's the presentation
        # edition for a LicensePool with a totally different Identifier.
        for edition in (edition2, edition3):
            with pytest.raises(ValueError) as excinfo:
                pool.calculate_work(known_edition=edition)
            assert "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!" \
                in str(excinfo.value)

    def test_open_access_pools_grouped_together(self, db_session, create_edition):
        """
        GIVEN: Four Editions with LicensePools
        WHEN:  Creating Works from the LicensePools
        THEN:  The open-access LicensePools have the same Work
        """
        # We have four editions with exactly the same title and author.
        # Two of them are open-access, two are not.
        title = "The Only Title"
        author = "Single Author"
        ed1, open1 = create_edition(db_session, title=title, authors=author, with_license_pool=True)
        ed2, open2 = create_edition(db_session, title=title, authors=author, with_license_pool=True)
        open1.open_access = True
        open2.open_access = True
        ed3, restricted3 = create_edition(
            db_session, title=title, authors=author,
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True)
        ed4, restricted4 = create_edition(
            db_session, title=title, authors=author,
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True)

        restricted3.open_access = False
        restricted4.open_access = False

        # Every identifier is equivalent to every other identifier.
        s = DataSource.lookup(db_session, DataSource.OCLC_LINKED_DATA)
        ed1.primary_identifier.equivalent_to(s, ed2.primary_identifier, 1)
        ed1.primary_identifier.equivalent_to(s, ed3.primary_identifier, 1)
        ed1.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)
        ed2.primary_identifier.equivalent_to(s, ed3.primary_identifier, 1)
        ed2.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)
        ed3.primary_identifier.equivalent_to(s, ed4.primary_identifier, 1)

        open1.calculate_work()
        open2.calculate_work()
        restricted3.calculate_work()
        restricted4.calculate_work()

        assert open1.work is not None
        assert open2.work is not None
        assert restricted3.work is not None
        assert restricted4.work is not None

        # The two open-access pools are grouped together.
        assert open1.work == open2.work

        # Each restricted-access pool is completely isolated.
        assert restricted3.work != restricted4.work
        assert restricted3.work != open1.work

    def test_all_licensepools_with_same_identifier_get_same_work(
            self, db_session, create_collection, create_edition, init_datasource_and_genres):
        """
        GIVEN: Two LicensePools with the same Identifier but different Collections
        WHEN:  Creating Works from the LicensePools
        THEN:  THe LicensePools have the same Work
        """
        # Here are two LicensePools for the same Identifier and
        # DataSource, but different Collections.
        collection1 = create_collection(db_session)
        edition1, pool1 = create_edition(db_session, with_license_pool=True, collection=collection1)
        identifier = pool1.identifier
        collection2 = create_collection(db_session)

        edition2, pool2 = create_edition(
            db_session,
            with_license_pool=True,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
            collection=collection2
        )

        assert pool1.identifier == pool2.identifier
        assert pool1.data_source == pool2.data_source
        assert collection1 == pool1.collection
        assert collection2 == pool2.collection

        # The two LicensePools have the same Edition (since a given
        # DataSource has only one opinion about an Identifier's
        # bibliographic information).
        assert edition1 == edition2

        # Because the two LicensePools have the same Identifier, they
        # have the same Work.
        work1, is_new_1 = pool1.calculate_work()
        work2, is_new_2 = pool2.calculate_work()
        assert work1 == work2
        assert is_new_1 is True
        assert is_new_2 is False
        assert work1.presentation_edition == edition1

    def test_calculate_work_fixes_work_in_invalid_state(
            self, init_datasource_and_genres, init_delivery_mechanism, db_session, create_edition, create_work):
        """
        GIVEN: A Work with four varying LicensePools
        WHEN:  Calculating a Work for a LicensePool
        THEN:  The mismatched LicensePools create their own Work
        """
        # Here's a Work with a commercial edition of "abcd".
        work = create_work(db_session, with_license_pool=True)
        [abcd_commercial] = work.license_pools
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains a _second_
        # commercial edition of "abcd"...
        _, abcd_commercial_2 = create_edition(db_session, with_license_pool=True)
        abcd_commercial_2.open_access = False
        abcd_commercial_2.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(abcd_commercial_2)

        # ...as well as an open-access edition of "abcd".
        _, abcd_open_access = create_edition(
            db_session, with_license_pool=True, with_open_access_download=True
        )
        abcd_open_access.open_access = True
        abcd_open_access.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(abcd_open_access)

        # calculate_work() recalculates the permanent work ID of a
        # LicensePool's presentation edition, and obviously the real
        # value isn't "abcd" for any of these Editions. Mocking
        # calculate_permanent_work_id ensures that we run the code
        # under the assumption that all these Editions have the same
        # permanent work ID.
        def mock_pwid(debug=False):
            return "abcd"
        for lp in [abcd_commercial, abcd_commercial_2, abcd_open_access]:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # Anyway, we can fix the whole problem by calling
        # calculate_work() on one of the LicensePools.
        work_after, is_new = abcd_commercial.calculate_work()
        assert work_after == work
        assert is_new is False

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other two have been kicked out and
        # given their own works.
        assert abcd_commercial_2.work != work
        assert abcd_open_access.work != work

        # The commercial LicensePool has been given a Work of its own.
        assert [abcd_commercial_2] == abcd_commercial_2.work.license_pools

        # The open-access work has been given the Work that will be
        # used for all open-access LicensePools for that book going
        # forward.

        expect_open_access_work, _ = (
            Work.open_access_for_permanent_work_id(
                db_session, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        assert expect_open_access_work == abcd_open_access.work

        # Now we're going to restore the bad configuration, where all
        # three books have the same Work. This time we're going to
        # call calculate_work() on the open-access LicensePool, and
        # verify that we get similar results as when we call
        # calculate_work() on one of the commercial LicensePools.
        abcd_commercial_2.work = work
        abcd_open_access.work = work

        work_after, is_new = abcd_open_access.calculate_work()
        # Since we called calculate_work() on the open-access work, it
        # maintained control of the Work, and both commercial books
        # got assigned new Works.
        assert work_after == work
        assert is_new is False

        assert abcd_commercial.work != work
        assert abcd_commercial.work is not None
        assert abcd_commercial_2.work != work
        assert abcd_commercial_2.work is not None
        assert abcd_commercial.work != abcd_commercial_2.work

        # Finally, let's test that nothing happens if you call
        # calculate_work() on a self-consistent situation.
        open_access_work = abcd_open_access.work
        assert (open_access_work, False) == abcd_open_access.calculate_work()

        commercial_work = abcd_commercial.work
        assert (commercial_work, False) == abcd_commercial.calculate_work()

    def test_calculate_work_fixes_incorrectly_grouped_books(
            self, db_session, create_edition, create_work, init_delivery_mechanism):
        """
        GIVEN: A Work with multiple incorrect LicensePools
        WHEN:  Calculating the Work for a LicensePool
        THEN:  The mismatched LicensePools get their own Work
        """
        # Here's a Work with an open-access edition of "abcd".
        work = create_work(db_session, with_license_pool=True)
        [book] = work.license_pools
        book.open_access = True
        book.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains an
        # open-access _audiobook_ of "abcd".
        _, audiobook = create_edition(db_session, with_license_pool=True)
        audiobook.open_access = True
        audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM
        audiobook.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(audiobook)

        # And the Work _also_ contains an open-access book of "abcd"
        # in a different language.
        _, spanish = create_edition(db_session, with_license_pool=True)
        spanish.open_access = True
        spanish.presentation_edition.language = 'spa'
        spanish.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(spanish)

        def mock_pwid(debug=False):
            return "abcd"
        for lp in [book, audiobook, spanish]:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We can fix this by calling calculate_work() on one of the
        # LicensePools.
        work_after, is_new = book.calculate_work()
        assert work_after == work
        assert is_new is False

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other one has been kicked out and
        # given its own work.
        assert book.work == work
        assert audiobook.work != work

        # The audiobook LicensePool has been given a Work of its own.
        assert audiobook.work.license_pools == [audiobook]

        # The book has been given the Work that will be used for all
        # book-type LicensePools for that title going forward.
        expect_book_work, _ = (
            Work.open_access_for_permanent_work_id(
                db_session, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        assert book.work == expect_book_work

        # The audiobook has been given the Work that will be used for
        # all audiobook-type LicensePools for that title going
        # forward.
        expect_audiobook_work, _ = (
            Work.open_access_for_permanent_work_id(
                db_session, "abcd", Edition.AUDIO_MEDIUM, 'eng'
            )
        )
        assert audiobook.work == expect_audiobook_work

        # The Spanish book has been given the Work that will be used
        # for all Spanish LicensePools for that title going forward.
        expect_spanish_work, _ = (
            Work.open_access_for_permanent_work_id(
                db_session, "abcd", Edition.BOOK_MEDIUM, 'spa'
            )
        )
        assert spanish.work == expect_spanish_work
        assert expect_spanish_work.language == 'spa'

    def test_calculate_work_detaches_licensepool_with_no_title(
            self, db_session, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work associated with a LicensePool whose presentation edition has no title
        WHEN:  Retrieving the Work for the LicensePool
        THEN:  The Work loses all its LicensePools
        """
        # Here's a Work with an open-access edition of "abcd".
        work = create_work(db_session, with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # But the LicensePool's presentation edition has lost its title.
        book.presentation_edition.title = None

        # Calling calculate_work() on the LicensePool will detach the
        # book from its work, since a book with no title cannot have
        # an associated Work.
        work_after, _ = book.calculate_work()
        assert work_after is None
        assert work.license_pools == []

    def test_calculate_work_detaches_licensepool_with_no_pwid(self, db_session, create_edition, create_work):
        """
        GIVEN: A Work with two LicensePools, one of which has an Edition with no title or author
        WHEN:  Retrieving the Work for the LicensePool
        THEN:  The LicensePool with the bad Edition is removed from the Work's license pools
        """
        # Here's a Work with an open-access edition of "abcd".
        work = create_work(db_session, with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains an edition
        # with no title or author, and thus no permanent work ID.
        _, no_title = create_edition(db_session, with_license_pool=True)

        no_title.presentation_edition.title = None
        no_title.presentation_edition.author = None
        no_title.presentation_edition.permanent_work_id = None
        work.license_pools.append(no_title)

        # Calling calculate_work() on the functional LicensePool will
        # split off the bad one.
        book.calculate_work()
        assert work.license_pools == [book]
        assert no_title.work is None
        assert no_title.presentation_edition.work is None

        # calculate_work() on the bad LicensePool will split it off from
        # the good one.
        work.license_pools.append(no_title)
        work_after_2, _ = no_title.calculate_work()
        assert work_after_2 is None
        assert work.license_pools == [book]

        # The same thing happens if the bad LicensePool has no
        # presentation edition at all.
        work.license_pools.append(no_title)
        no_title.presentation_edition = None
        book.calculate_work()
        assert work.license_pools == [book]

        work.license_pools.append(no_title)
        no_title.calculate_work()
        assert work.license_pools == [book]

    def test_pwids(self, db_session, create_edition, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work associated with two LicensePools, one of which has an Edition
        WHEN:  Finding all permanent work IDs associated with the Work
        THEN:  All permanent work IDs associated with the Work are found
        """
        # Create a (bad) situation in which LicensePools associated
        # with two different PWIDs are associated with the same work.
        work = create_work(db_session, with_license_pool=True)
        [lp1] = work.license_pools
        assert work.pwids == set([lp1.presentation_edition.permanent_work_id])

        _, lp2 = create_edition(db_session, with_license_pool=True)
        work.license_pools.append(lp2)

        # Work.pwids finds both PWIDs.
        assert (
            work.pwids
            ==
            set([lp1.presentation_edition.permanent_work_id, lp2.presentation_edition.permanent_work_id])
        )

    def test_open_access_for_permanent_work_id_no_licensepools(self, db_session, create_work):
        """
        GIVEN: An open-access LicensePool
        WHEN:  Retrieving a Work that encompasses all open-access LicensePools for given
               permanent work ID, medium, and language
        THEN:  A corresponding Work is either retrieved or not
        """
        # There are no LicensePools, which short-circuilts
        # open_access_for_permanent_work_id.
        assert (
            Work.open_access_for_permanent_work_id(db_session, "No such permanent work ID", Edition.BOOK_MEDIUM, "eng")
            ==
            (None, False)
        )

        # Now it works.
        work = create_work(
            db_session, language="eng", with_license_pool=True,
            with_open_access_download=True
        )
        work.presentation_edition.permanent_work_id = "permid"
        assert (
             Work.open_access_for_permanent_work_id(db_session, "permid", Edition.BOOK_MEDIUM, "eng")
             ==
             (work, False)
        )

        # But the language, medium, and permanent ID must all match.
        assert (
            Work.open_access_for_permanent_work_id(db_session, "permid", Edition.BOOK_MEDIUM, "spa")
            ==
            (None, False)
        )

        assert (
            Work.open_access_for_permanent_work_id(db_session, "differentid", Edition.BOOK_MEDIUM, "eng")
            ==
            (None, False)
        )

        assert (
            Work.open_access_for_permanent_work_id(db_session, "differentid", Edition.AUDIO_MEDIUM, "eng")
            ==
            (None, False)
        )

    #@pytest.mark.skip(reason="Now this test fails like all the time... super frustrating.")
    def test_open_access_for_permanent_work_id(self, db_session, create_edition, create_work):
        """
        GIVEN: Three Works with LicensePools
        WHEN:  Retrieving a Work that encompasses all open-access LicensePools for given
               permanent work ID, medium, and language
        THEN:  The open-access license pools share the same Work while the commercial license pool's Work was split off
        """
        # Two different works full of open-access license pools.
        w1 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        w2 = create_work(db_session, with_license_pool=True, with_open_access_download=True)

        [lp1] = w1.license_pools
        [lp2] = w2.license_pools

        # Work #2 has two different license pools grouped together. Work #1 only has one.
        _, lp3 = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        w2.license_pools.append(lp3)

        # Due to an error, it turns out both Works are providing the exact same book.
        def mock_pwid(debug=False):
            return "abcd"
        for lp in [lp1, lp2, lp3]:
            lp.presentation_edition.permanent_work_id = "abcd"
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We've also got Work #3, which provides a commercial license for that book.
        w3 = create_work(db_session, with_license_pool=True)
        w3_pool = w3.license_pools[0]
        w3_pool.presentation_edition.permanent_work_id = "abcd"
        w3_pool.open_access = False

        # Work.open_access_for_permanent_work_id can resolve this problem.
        work, is_new = Work.open_access_for_permanent_work_id(db_session, "abcd", Edition.BOOK_MEDIUM, "eng")

        # Work #3 still exists and its license pool was not affected.
        assert db_session.query(Work).filter(Work.id == w3.id).all() == [w3]
        assert w3_pool.work == w3

        # But the other three license pools now have the same work.
        assert work == lp1.work
        assert work == lp2.work
        assert work == lp3.work

        # Because work #2 had two license pools, and work #1 only had
        # one, work #1 was merged into work #2, rather than the other
        # way around.
        assert work == w2
        assert is_new is False

        # Work #1 no longer exists.
        assert db_session.query(Work).filter(Work.id == w1.id).all() == []

        # Calling Work.open_access_for_permanent_work_id again returns the same result.
        Work.open_access_for_permanent_work_id(db_session, "abcd", Edition.BOOK_MEDIUM, "eng")
        assert Work.open_access_for_permanent_work_id(db_session, "abcd", Edition.BOOK_MEDIUM, "eng") == (w2, False)

    def test_open_access_for_permanent_work_id_can_create_work(
            self, db_session, create_edition, init_datasource_and_genres):
        """
        GIVEN: An open-access LicensePool
        WHEN:  Retrieving a Work that encompasses all open-access LicensePools for given
               permanent work ID, medium, and language
        THEN:  A Work is created
        """
        # Here's a LicensePool with no corresponding Work.
        edition, lp = create_edition(db_session, with_license_pool=True)
        lp.open_access = True
        edition.permanent_work_id = "abcd"

        # open_access_for_permanent_work_id creates the Work.
        work, is_new = Work.open_access_for_permanent_work_id(
            db_session, "abcd", Edition.BOOK_MEDIUM, edition.language
        )
        assert work.license_pools == [lp]
        assert is_new is True

    def test_potential_open_access_works_for_permanent_work_id(
            self, db_session, create_edition, init_datasource_and_genres):
        """
        GIVEN: Two Editions with LicensePools that are open-access
        WHEN:  Finding all Works that might be suitable for use as the canonical
               open-access Work for the given pwid, medium, and language
        THEN:  Returns the relevant LicensePools and a count of affected LicensePools by Work
        """
        # Here are two editions of the same book with the same PWID.
        title = 'Siddhartha'
        author = ['Herman Hesse']
        e1, lp1 = create_edition(
            db_session, data_source_name=DataSource.STANDARD_EBOOKS,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e1.permanent_work_id = "pwid"

        e2, lp2 = create_edition(
            db_session, data_source_name=DataSource.GUTENBERG,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e2.permanent_work_id = "pwid"

        w1 = Work()
        db_session.add(w1)
        for lp in [lp1, lp2]:
            w1.license_pools.append(lp)
            lp.open_access = True

        def m():
            return Work._potential_open_access_works_for_permanent_work_id(
                db_session, "pwid", Edition.BOOK_MEDIUM, "eng"
            )
        pools, counts = m()

        # Both LicensePools show up in the list of LicensePools that
        # should be grouped together, and both LicensePools are
        # associated with the same Work.
        poolset = set([lp1, lp2])
        assert pools == poolset
        assert counts == {w1: 2}

        # Since the work was just created, it has no presentation
        # edition and thus no language. If the presentation edition
        # were set, the result would be the same.
        w1.presentation_edition = e1
        pools, counts = m()
        assert pools == poolset
        assert counts == {w1: 2}

        # If the Work's presentation edition has information that
        # _conflicts_ with the information passed in to
        # _potential_open_access_works_for_permanent_work_id, the Work
        # does not show up in `counts`, indicating that a new Work
        # should to be created to hold those books.
        bad_pe = create_edition(db_session)
        bad_pe.permanent_work_id = 'pwid'
        w1.presentation_edition = bad_pe

        bad_pe.language = 'fin'
        pools, counts = m()
        assert pools == poolset
        assert counts == {}
        bad_pe.language = 'eng'

        bad_pe.medium = Edition.AUDIO_MEDIUM
        pools, counts = m()
        assert pools == poolset
        assert counts == {}
        bad_pe.medium = Edition.BOOK_MEDIUM

        bad_pe.permanent_work_id = "Some other ID"
        pools, counts = m()
        assert pools == poolset
        assert counts == {}
        bad_pe.permanent_work_id = "pwid"

        w1.presentation_edition = None

        # Now let's see what changes to a LicensePool will cause it
        # not to be eligible in the first place.
        def assert_lp1_missing():
            # A LicensePool that is not eligible will not show up in
            # the set and will not be counted towards the total of eligible
            # LicensePools for its Work.
            pools, counts = m()
            assert set([lp2]) == pools
            assert {w1: 1} == counts

        # It has to be open-access.
        lp1.open_access = False
        assert_lp1_missing()
        lp1.open_access = True

        # The presentation edition's permanent work ID must match
        # what's passed into the helper method.
        e1.permanent_work_id = "another pwid"
        assert_lp1_missing()
        e1.permanent_work_id = "pwid"

        # The medium must also match.
        e1.medium = Edition.AUDIO_MEDIUM
        assert_lp1_missing()
        e1.medium = Edition.BOOK_MEDIUM

        # The language must also match.
        e1.language = "another language"
        assert_lp1_missing()
        e1.language = 'eng'

        # Finally, let's see what happens when there are two Works where
        # there should be one.
        w2 = Work()
        db_session.add(w2)
        w2.license_pools.append(lp2)
        pools, counts = m()

        # This work is irrelevant and will not show up at all.
        w3 = Work()
        db_session.add(w3)

        # Both Works have one associated LicensePool, so they have
        # equal claim to being 'the' Work for this work
        # ID/language/medium. The calling code will have to sort it
        # out.
        assert pools == poolset
        assert counts == {w1: 1, w2: 1}

    def test_make_exclusive_open_access_for_permanent_work_id(self, db_session, create_edition, create_work):
        """
        GIVEN: A Work with open-access and commercial LicensePools, and
               a Work with an open-access LicensePool wrongly associated with the first Work
        WHEN:  Ensuring that every open-access LicensePool associated with a given Work has
               the given permanent work ID and medium
        THEN:  The commercial LicensePool gets its own Work
        """
        # Here's a work containing an open-access LicensePool for
        # literary work "abcd".
        work1 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id = "abcd"

        # Unfortunately, a commercial LicensePool for the literary
        # work "abcd" has gotten associated with the same work.
        _, abcd_commercial = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id = "abcd"
        abcd_commercial.work = work1

        # Here's another Work containing an open-access LicensePool
        # for literary work "efgh".
        work2 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [efgh_1] = work2.license_pools
        efgh_1.presentation_edition.permanent_work_id = "efgh"

        # Unfortunately, there's another open-access LicensePool for
        # "efgh", and it's incorrectly associated with the "abcd"
        # work.
        _, efgh_2 = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        efgh_2.presentation_edition.permanent_work_id = "efgh"
        efgh_2.work = work1

        # Let's fix these problems.
        work1.make_exclusive_open_access_for_permanent_work_id("abcd", Edition.BOOK_MEDIUM, "eng")

        # The open-access "abcd" book is now the only LicensePool
        # associated with work1.
        assert [abcd_oa] == work1.license_pools

        # Both open-access "efgh" books are now associated with work2.
        assert set([efgh_1, efgh_2]) == set(work2.license_pools)

        # A third work has been created for the commercial edition of "abcd".
        assert abcd_commercial.work not in (work1, work2)

    def test_make_exclusive_open_access_for_null_permanent_work_id(
            self, db_session, create_edition, create_work, init_datasource_and_genres):
        """
        GIVEN: A Work with two LicensePools whose presentation edition has no permanent work ID
        WHEN:  Ensuring that every open-access LicensePool associated with a given Work has
               the given permanent work ID and medium
        THEN:  The Work loses both LicensePools
        """
        # Here's a LicensePool that, due to a previous error, has
        # a null PWID in its presentation edition.
        work = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [null1] = work.license_pools
        null1.presentation_edition.title = None
        null1.presentation_edition.sort_author = None
        null1.presentation_edition.permanent_work_id = None

        # Here's another LicensePool associated with the same work and
        # with the same problem.
        edition, null2 = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        work.license_pools.append(null2)

        for pool in work.license_pools:
            pool.presentation_edition.title = None
            pool.presentation_edition.sort_author = None
            pool.presentation_edition.permanent_work_id = None

        work.make_exclusive_open_access_for_permanent_work_id(None, Edition.BOOK_MEDIUM, edition.language)

        # Since a LicensePool with no PWID cannot have an associated Work,
        # this Work now have no LicensePools at all.
        assert work.license_pools == []

        assert null1.work is None
        assert null2.work is None

    def test_merge_into_success(self, db_session, create_work):
        """
        GIVEN: Two Works with LicensePools that have the same permanent work ID
        WHEN:  Merging one Work into the other
        THEN:  The merged in Work is deleted along with it's WorkGenres and WorkCoverageRecords
               leaving the second Work intact
        """
        # Here's a work with an open-access LicensePool.
        work1 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id = "abcd"

        # Let's give it a WorkGenre and a WorkCoverageRecord.
        genre, _ = Genre.lookup(db_session, "Fantasy")
        get_one_or_create(db_session, WorkGenre, work=work1, genre=genre)
        WorkCoverageRecord.add_for(work1, "test")

        # Here's another work with an open-access LicensePool for the same book.
        work2 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.presentation_edition.permanent_work_id = "abcd"

        # Let's merge the first work into the second.
        work1.merge_into(work2)

        # The first work has been deleted, as have its WorkGenre and WorkCoverageRecord.
        assert db_session.query(Work).filter(Work.id == work1.id).all() == []
        assert db_session.query(WorkGenre).all() == []
        assert db_session.query(WorkCoverageRecord).filter(WorkCoverageRecord.work_id == work1.id).all() == []

    def test_open_access_for_permanent_work_id_fixes_mismatched_works_incidentally(
            self, db_session, create_edition, create_work, init_datasource_and_genres):
        """
        GIVEN: Two Work with multiple open-access LicensePools and an additional open-access
               LicensePool wrongly pointing to a totally separate Edition
        WHEN:  Retrieving a Work that encompasses all open-access LicensePools for given
               permanent work ID, medium, and language
        THEN:  The Works have their correct LicensePools sorted out
        """
        # Here's a work with two open-access LicensePools for the book "abcd".
        work1 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [abcd_1] = work1.license_pools
        _, abcd_2 = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        work1.license_pools.append(abcd_2)

        # Unfortunately, due to an earlier error, that work also
        # contains a _third_ open-access LicensePool, and this one
        # belongs to a totally separate book, "efgh".
        _, efgh = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        work1.license_pools.append(efgh)

        # Here's another work with an open-access LicensePool for the
        # book "abcd".
        work2 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [abcd_3] = work2.license_pools

        # Unfortunately, this work also contains an open-access Licensepool
        # for the totally separate book, 'ijkl".
        _, ijkl = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        work2.license_pools.append(ijkl)

        # Mock the permanent work IDs for all the presentation
        # editions in play.
        def mock_pwid_abcd(debug=False):
            return "abcd"

        def mock_pwid_efgh(debug=False):
            return "efgh"

        def mock_pwid_ijkl(debug=False):
            return "ijkl"

        for lp in abcd_1, abcd_2, abcd_3:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_abcd
            lp.presentation_edition.permanent_work_id = 'abcd'

        efgh.presentation_edition.calculate_permanent_work_id = mock_pwid_efgh
        efgh.presentation_edition.permanent_work_id = 'efgh'

        ijkl.presentation_edition.calculate_permanent_work_id = mock_pwid_ijkl
        ijkl.presentation_edition.permanent_work_id = 'ijkl'

        # Calling Work.open_access_for_permanent_work_id()
        # automatically kicks the 'efgh' and 'ijkl' LicensePools into
        # their own works, and merges the second 'abcd' work with the
        # first one. (The first work is chosen because it represents
        # two LicensePools for 'abcd', not just one.)
        abcd_work, _ = Work.open_access_for_permanent_work_id(db_session, "abcd", Edition.BOOK_MEDIUM, "eng")
        efgh_work, efgh_new = Work.open_access_for_permanent_work_id(db_session, "efgh", Edition.BOOK_MEDIUM, "eng")
        ijkl_work, ijkl_new = Work.open_access_for_permanent_work_id(db_session, "ijkl", Edition.BOOK_MEDIUM, "eng")

        # We've got three different works here. The 'abcd' work is the
        # old 'abcd' work that had three LicensePools--the other work
        # was merged into it.
        assert abcd_1.work == abcd_work
        assert efgh_work != abcd_work
        assert ijkl_work != abcd_work
        assert ijkl_work != efgh_work

        # The two 'new' works (for efgh and ijkl) are not counted as
        # new because they were created during the first call to
        # Work.open_access_for_permanent_work_id, when those
        # LicensePools were split out of Works where they didn't
        # belong.
        assert efgh_new is False
        assert ijkl_new is False

        assert ijkl_work.license_pools == [ijkl]
        assert efgh_work.license_pools == [efgh]
        assert len(abcd_work.license_pools) == 3

    def test_open_access_for_permanent_work_untangles_tangled_works(self, db_session, create_edition, create_work):
        """
        GIVEN: Two Works with open-access LicensePools pointing to different permanent work IDs
        WHEN:  Retrieving a Work that encompasses all open-access LicensePools for given
               permanent work ID, medium, and language
        THEN:  Relevant Works are consolidated with their respective LicensePools
        """
        # Here are two works for the books "abcd", "efgh".
        abcd_work = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [abcd_1] = abcd_work.license_pools

        efgh_work = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [efgh_1] = efgh_work.license_pools

        # Unfortunately, due to an earlier error, the 'abcd' work
        # contains a LicensePool for 'efgh', and the 'efgh' work contains
        # a LicensePool for 'abcd'.
        #
        # (This is pretty much impossible, but bear with me...)

        _, abcd_2 = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        efgh_work.license_pools.append(abcd_2)

        efgh_edition, efgh_2 = create_edition(db_session, with_license_pool=True, with_open_access_download=True)
        abcd_work.license_pools.append(efgh_2)

        # Both Works have a presentation edition that indicates the
        # permanent work ID is 'abcd'.
        abcd_work.presentation_edition = efgh_edition
        efgh_work.presentation_edition = efgh_edition

        def mock_pwid_abcd(debug=False):
            return "abcd"

        for lp in abcd_1, abcd_2:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_abcd
            lp.presentation_edition.permanent_work_id = 'abcd'

        def mock_pwid_efgh(debug=False):
            return "efgh"

        for lp in efgh_1, efgh_2:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid_efgh
            lp.presentation_edition.permanent_work_id = 'efgh'

        # Calling Work.open_access_for_permanent_work_id() creates a
        # new work that contains both 'abcd' LicensePools.
        abcd_new, is_new = Work.open_access_for_permanent_work_id(
            db_session, "abcd", Edition.BOOK_MEDIUM, "eng"
        )
        assert is_new is True
        assert set([abcd_1, abcd_2]) == set(abcd_new.license_pools)

        # The old abcd_work now contains only the 'efgh' LicensePool
        # that didn't fit.
        assert [efgh_2] == abcd_work.license_pools

        # We now have two works with 'efgh' LicensePools: abcd_work
        # and efgh_work. Calling
        # Work.open_access_for_permanent_work_id on 'efgh' will
        # consolidate the two LicensePools into one of the Works
        # (which one is nondeterministic).
        efgh_new, is_new = Work.open_access_for_permanent_work_id(
            db_session, "efgh", Edition.BOOK_MEDIUM, "eng"
        )
        assert is_new is False
        assert set([efgh_1, efgh_2]) == set(efgh_new.license_pools)
        assert efgh_new in (abcd_work, efgh_work)

        # The Work that was not chosen for consolidation now has no
        # LicensePools.
        if efgh_new is abcd_work:
            other = efgh_work
        else:
            other = abcd_work
        assert other.license_pools == []

    def test_merge_into_raises_exception_if_grouping_rules_violated(
            self, db_session, create_work, init_datasource_and_genres):
        """
        GIVEN: Two Works, one with an open-access LicensePool and one without open-access
        WHEN:  Merging the Work with the open-access LicensePool into the Work without the open-access LicensePool
        THEN:  A ValueError is raised outlining open-access limitations
        """
        # Here's a work with an open-access LicensePool.
        work1 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id = "abcd"

        # Here's another work with a commercial LicensePool for the
        # same book.
        work2 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.open_access = False
        lp2.presentation_edition.permanent_work_id = "abcd"

        # The works cannot be merged.
        with pytest.raises(ValueError) as excinfo:
            work1.merge_into(work2)
        assert "Refusing to merge {} into {} because it would put an open-access LicensePool into the same work as a non-open-access LicensePool.".format(work1, work2) \
               in str(excinfo.value)

    def test_merge_into_raises_exception_if_pwids_differ(self, db_session, create_work):
        """
        GIVEN: Two Works with open-access LicensePools that have presentation editions with disctinct permanent work IDs
        WHEN:  Merging one Work into the other Work
        THEN:  A ValueError is raised outlining permanent work ID mismatch
        """
        work1 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id = "abcd"

        work2 = create_work(db_session, with_license_pool=True, with_open_access_download=True)
        [efgh_oa] = work2.license_pools
        efgh_oa.presentation_edition.permanent_work_id = "efgh"

        with pytest.raises(ValueError) as excinfo:
            work1.merge_into(work2)
        assert "Refusing to merge {} into {} because permanent work IDs don't match: abcd vs. efgh".format(work1, work2) \
               in str(excinfo.value)

    def test_licensepool_without_identifier_gets_no_work(self, db_session, create_work):
        """
        GIVEN: A LicensePool with no Identifier
        WHEN:  Retrieving the Work for a LicensePool
        THEN:  No work is retrieved or created
        """
        work = create_work(db_session, with_license_pool=True)
        [lp] = work.license_pools
        lp.identifier = None

        # Even if the LicensePool had a work before, it gets removed.
        assert lp.calculate_work() == (None, False)
        assert lp.work is None

    def test_licensepool_without_presentation_edition_gets_no_work(
            self, db_session, create_work, init_datasource_and_genres):
        """
        GIVEN: A LicensePool with no presentation edition
        WHEN:  Retrieving the Work for a LicensePool
        THEN:  No Work is retrieved or created
        """
        work = create_work(db_session, with_license_pool=True)
        [lp] = work.license_pools

        # This LicensePool has no presentation edition and no way of getting one.
        lp.presentation_edition = None
        lp.identifier.primarily_identifies = []

        # Even if the LicensePool had a work before, it gets removed.
        assert lp.calculate_work() == (None, False)
        assert lp.work is None
