# encoding: utf-8
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
import datetime
from ...external_search import MockExternalSearchIndex
import os
from psycopg2.extras import NumericRange
import random
from .. import DatabaseTest
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


class TestWork(DatabaseTest):
    def test_complaints(self):
        work = self._work(with_license_pool=True)

        [lp1] = work.license_pools
        lp2 = self._licensepool(
            edition=work.presentation_edition,
            data_source_name=DataSource.OVERDRIVE
        )
        lp2.work = work

        complaint_type = random.choice(list(Complaint.VALID_TYPES))
        complaint1, ignore = Complaint.register(
            lp1, complaint_type, "blah", "blah"
        )
        complaint2, ignore = Complaint.register(
            lp2, complaint_type, "blah", "blah"
        )

        # Create a complaint with no association with the work.
        _edition, lp3 = self._edition(with_license_pool=True)
        complaint3, ignore = Complaint.register(
            lp3, complaint_type, "blah", "blah"
        )

        # Only the first two complaints show up in work.complaints.
        eq_(sorted([complaint1.id, complaint2.id]),
            sorted([x.id for x in work.complaints]))

    def test_all_identifier_ids(self):
        work = self._work(with_license_pool=True)
        lp = work.license_pools[0]
        identifier = self._identifier()
        data_source = DataSource.lookup(self._db, DataSource.OCLC)
        identifier.equivalent_to(data_source, lp.identifier, 1)

        # Make sure there aren't duplicates in the list, if an
        # identifier's equivalent to two of the primary identifiers.
        lp2 = self._licensepool(None)
        work.license_pools.append(lp2)
        identifier.equivalent_to(data_source, lp2.identifier, 1)

        all_identifier_ids = work.all_identifier_ids()
        eq_(3, len(all_identifier_ids))
        expect_all_ids = set(
            [lp.identifier.id, lp2.identifier.id, identifier.id]
        )

        eq_(expect_all_ids, all_identifier_ids)

    def test_from_identifiers(self):
        # Prep a work to be identified and a work to be ignored.
        work = self._work(with_license_pool=True, with_open_access_download=True)
        lp = work.license_pools[0]
        ignored_work = self._work(with_license_pool=True, with_open_access_download=True)

        # No identifiers returns None.
        result = Work.from_identifiers(self._db, [])
        eq_(None, result)

        # A work can be found according to its identifier.
        identifiers = [lp.identifier]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # When the work has an equivalent identifier.
        isbn = self._identifier(Identifier.ISBN)
        source = lp.data_source
        lp.identifier.equivalent_to(source, isbn, 1)

        # It can be found according to that equivalency.
        identifiers = [isbn]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # Unless the strength is too low.
        lp.identifier.equivalencies[0].strength = 0.8
        identifiers = [isbn]

        result = Work.from_identifiers(self._db, identifiers).all()
        eq_([], result)

        # Two+ of the same or equivalent identifiers lead to one result.
        identifiers = [lp.identifier, isbn, lp.identifier]
        result = Work.from_identifiers(self._db, identifiers).all()
        eq_(1, len(result))
        eq_([work], result)

        # It accepts a base query.
        qu = self._db.query(Work).join(LicensePool).join(Identifier).\
            filter(LicensePool.suppressed)
        identifiers = [lp.identifier]
        result = Work.from_identifiers(self._db, identifiers, base_query=qu).all()
        # Because the work's license_pool isn't suppressed, it isn't returned.
        eq_([], result)

    def test_calculate_presentation(self):
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

        [bob], ignore = Contributor.lookup(self._db, u"Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()

        edition1, pool1 = self._edition(gitenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition1.title = u"The 1st Title"
        edition1.subtitle = u"The 1st Subtitle"
        edition1.add_contributor(bob, Contributor.AUTHOR_ROLE)

        edition2, pool2 = self._edition(gitenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition2.title = u"The 2nd Title"
        edition2.subtitle = u"The 2nd Subtitle"
        edition2.add_contributor(bob, Contributor.AUTHOR_ROLE)
        [alice], ignore = Contributor.lookup(self._db, u"Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()
        edition2.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition3, pool3 = self._edition(gutenberg_source, Identifier.GUTENBERG_ID,
            with_license_pool=True, with_open_access_download=True, authors=[])
        edition3.title = u"The 2nd Title"
        edition3.subtitle = u"The 2nd Subtitle"
        edition3.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition3.add_contributor(alice, Contributor.AUTHOR_ROLE)

        # Create three summaries.

        # This summary is associated with one of the work's
        # LicensePools, and it comes from a good source -- Library
        # Staff. It will be chosen even though it doesn't look great,
        # textually.
        library_staff = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        chosen_summary = "direct"
        pool1.identifier.add_link(
            Hyperlink.DESCRIPTION, None, library_staff, content=chosen_summary
        )

        # This summary is associated with one of the work's
        # LicensePools, but it comes from a less reliable source, so
        # it won't be chosen.
        less_reliable_summary_source = DataSource.lookup(
            self._db, DataSource.OCLC
        )
        pool2.identifier.add_link(
            Hyperlink.DESCRIPTION, None, less_reliable_summary_source,
            content="less reliable summary"
        )

        # This summary looks really nice, and it's associated with the
        # same source as the LicensePool, which is good, but it's not
        # directly associated with any of the LicensePools, so it
        # won't be chosen.
        related_identifier = self._identifier()
        pool3.identifier.equivalent_to(
            pool3.data_source, related_identifier, strength=1
        )
        related_identifier.add_link(
            Hyperlink.DESCRIPTION, None, pool3.data_source,
            content="This is an indirect summary. It's much longer, and looks more 'real', so you'd think it would be prefered, but it won't be."
        )

        work = self._slow_work(presentation_edition=edition2)

        # The work starts out with no description, even though its
        # presentation was calculated, because a description can only
        # come from an Identifier associated with a LicensePool, and
        # this Work has no LicensePools.
        eq_(None, work.summary)

        # add in 3, 2, 1 order to make sure the selection of edition1 as presentation
        # in the second half of the test is based on business logic, not list order.
        for p in pool3, pool1:
            work.license_pools.append(p)

        # The author of the Work is the author of its primary work record.
        eq_("Alice Adder, Bob Bitshifter", work.author)

        # This Work starts out with a single CoverageRecord reflecting
        # the work done to generate its initial OPDS entry, and then
        # it adds choose-edition as a primary edition is set. The
        # search index CoverageRecord is a marker for work that must
        # be done in the future, and is not tested here.
        [choose_edition, generate_opds, update_search_index] = sorted(work.coverage_records, key=lambda x: x.operation)
        assert (generate_opds.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION)
        assert (choose_edition.operation == WorkCoverageRecord.CHOOSE_EDITION_OPERATION)

        # pools aren't yet aware of each other
        eq_(pool1.superceded, False)
        eq_(pool2.superceded, False)
        eq_(pool3.superceded, False)

        work.last_update_time = None
        work.presentation_ready = True
        index = MockExternalSearchIndex()

        work.calculate_presentation(search_index_client=index)

        # The author of the Work has not changed.
        eq_("Alice Adder, Bob Bitshifter", work.author)

        # one and only one license pool should be un-superceded
        eq_(pool1.superceded, True)
        eq_(pool2.superceded, False)
        eq_(pool3.superceded, True)

        # sanity check
        eq_(work.presentation_edition, pool2.presentation_edition)
        eq_(work.presentation_edition, edition2)

        # editions that aren't the presentation edition have no work
        eq_(edition1.work, None)
        eq_(edition2.work, work)
        eq_(edition3.work, None)

        # The title of the Work is the title of its primary work record.
        eq_("The 2nd Title", work.title)
        eq_("The 2nd Subtitle", work.subtitle)

        # The author of the Work is the author of its primary work record.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

        # The summary has now been chosen.
        eq_(chosen_summary, work.summary.representation.content)

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

        # The index has not been updated.
        eq_([], index.docs.items())

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
        eq_(expect, set([(x.operation, x.status) for x in records]))

        # Now mark the pool with the presentation edition as suppressed.
        # work.calculate_presentation() will call work.mark_licensepools_as_superceded(),
        # which will mark the suppressed pool as superceded and take its edition out of the running.
        # Make sure that work's presentation edition and work's author, etc.
        # fields are updated accordingly, and that the superceded pool's edition
        # knows it's no longer the champ.
        pool2.suppressed = True

        work.calculate_presentation(search_index_client=index)

        # The title of the Work is the title of its new primary work record.
        eq_("The 1st Title", work.title)
        eq_("The 1st Subtitle", work.subtitle)

        # author of composite edition is now just Bob
        eq_("Bob Bitshifter", work.author)
        eq_("Bitshifter, Bob", work.sort_author)

        # sanity check
        eq_(work.presentation_edition, pool1.presentation_edition)
        eq_(work.presentation_edition, edition1)

        # editions that aren't the presentation edition have no work
        eq_(edition1.work, work)
        eq_(edition2.work, None)
        eq_(edition3.work, None)

        # The last update time has been set.
        # Updating availability also modified work.last_update_time.
        assert (datetime.datetime.utcnow() - work.last_update_time) < datetime.timedelta(seconds=2)

        # make a staff (admin interface) edition.  its fields should supercede all others below it
        # except when it has no contributors, and they do.
        pool2.suppressed = False

        staff_edition = self._edition(data_source_name=DataSource.LIBRARY_STAFF,
            with_license_pool=False, authors=[])
        staff_edition.title = u"The Staff Title"
        staff_edition.primary_identifier = pool2.identifier
        # set edition's authorship to "nope", and make sure the lower-priority
        # editions' authors don't get clobbered
        staff_edition.contributions = []
        staff_edition.author = Edition.UNKNOWN_AUTHOR
        staff_edition.sort_author = Edition.UNKNOWN_AUTHOR

        work.calculate_presentation(search_index_client=index)

        # The title of the Work got superceded.
        eq_("The Staff Title", work.title)

        # The author of the Work is still the author of edition2 and was not clobbered.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

    def test_calculate_presentation_with_no_presentation_edition(self):
        # Calling calculate_presentation() on a work with no
        # presentation edition won't do anything, but at least it doesn't
        # crash.
        work = self._work()
        work.presentation_edition = None
        work.coverage_records = []
        self._db.commit()
        work.calculate_presentation()

        # The work is not presentation-ready.
        eq_(False, work.presentation_ready)

        # Work was done to choose the presentation edition, but since no
        # presentation edition was found, no other work was done.
        [choose_edition] = work.coverage_records
        eq_(WorkCoverageRecord.CHOOSE_EDITION_OPERATION,
            choose_edition.operation)

    def test_calculate_presentation_sets_presentation_ready_based_on_content(self):

        # This work is incorrectly presentation-ready; its presentation
        # edition has no language.
        work = self._work(with_license_pool=True)
        edition = work.presentation_edition
        edition.language = None

        eq_(True, work.presentation_ready)
        work.calculate_presentation()
        eq_(False, work.presentation_ready)

        # Give it a language, and it becomes presentation-ready again.
        edition.language = "eng"
        work.calculate_presentation()
        eq_(True, work.presentation_ready)

    def test__choose_summary(self):
        # Test the _choose_summary helper method, called by
        # calculate_presentation().

        class Mock(Work):
            def set_summary(self, summary):
                if isinstance(summary, Resource):
                    self.summary_text = summary.representation.content
                else:
                    self.summary_text = summary

        w = Mock()
        w.the_summary = "old summary"
        self._db.add(w)
        m = w._choose_summary

        # If no summaries are available, any old summary is cleared out.
        m([], [], [])
        eq_(None, w.summary_text)

        # Create three summaries on two identifiers.
        source1 = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        source2 = DataSource.lookup(self._db, DataSource.BIBLIOTHECA)

        i1 = self._identifier()
        l1, ignore = i1.add_link(
            Hyperlink.DESCRIPTION, None, source1,
            content="ok summary"
        )
        good_summary = "This summary is great! It's more than one sentence long and features some noun phrases."
        i1.add_link(
            Hyperlink.DESCRIPTION, None, source2,
            content=good_summary
        )

        i2 = self._identifier()
        i2.add_link(
            Hyperlink.DESCRIPTION, None, source2,
            content="not too bad"
        )

        # Now we can test out the rules for choosing summaries.

        # In a choice between all three summaries, good_summary is
        # chosen based on textual characteristics.
        m([], [i1.id, i2.id], [])
        eq_(good_summary, w.summary_text)

        m([i1.id, i2.id], [], [])
        eq_(good_summary, w.summary_text)

        # If an identifier is associated directly with the work, its
        # summaries are considered first, and the other identifiers
        # are not considered at all.
        m([i2.id], [object(), i1.id], [])
        eq_("not too bad", w.summary_text)

        # A summary that comes from a preferred data source will be
        # chosen over some other summary.
        m([i1.id, i2.id], [], [source1])
        eq_("ok summary", w.summary_text)

        # But if there is no summary from a preferred data source, the
        # normal rules apply.
        source3 = DataSource.lookup(self._db, DataSource.AXIS_360)
        m([i1.id], [], [source3])
        eq_(good_summary, w.summary_text)

        # LIBRARY_STAFF is always considered a good source of
        # descriptions.
        l1.data_source = DataSource.lookup(
            self._db, DataSource.LIBRARY_STAFF
        )
        m([i1.id, i2.id], [], [])
        eq_(l1.resource.representation.content, w.summary_text)

    def test_set_presentation_ready_based_on_content(self):

        work = self._work(with_license_pool=True)

        search = MockExternalSearchIndex()
        # This is how the work will be represented in the dummy search
        # index.
        index_key = (search.works_index,
                     MockExternalSearchIndex.work_document_type,
                     work.id)

        presentation = work.presentation_edition
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

        # The work has not been added to the search index.
        eq_([], search.docs.keys())

        # But the work of adding it to the search engine has been
        # registered.
        def assert_record():
            # Verify the search index WorkCoverageRecord for this work
            # is in the REGISTERED state.
            [record] = [
                x for x in work.coverage_records
                if x.operation==WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
            ]
            eq_(WorkCoverageRecord.REGISTERED, record.status)
        assert_record()

        # This work is presentation ready because it has a title.
        # Remove the title, and the work stops being presentation
        # ready.
        presentation.title = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(False, work.presentation_ready)

        # The search engine WorkCoverageRecord is still in the
        # REGISTERED state, but its meaning has changed -- the work
        # will now be _removed_ from the search index, rather than
        # updated.
        assert_record()

        # Restore the title, and everything is fixed.
        presentation.title = u"foo"
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

        # Remove the medium, and the work stops being presentation ready.
        presentation.medium = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(False, work.presentation_ready)

        presentation.medium = Edition.BOOK_MEDIUM
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

        # Remove the language, and it stops being presentation ready.
        presentation.language = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(False, work.presentation_ready)

        presentation.language = 'eng'
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

        # Remove the fiction status, and the work is still
        # presentation ready. Fiction status used to make a difference, but
        # it no longer does.
        work.fiction = None
        work.set_presentation_ready_based_on_content(search_index_client=search)
        eq_(True, work.presentation_ready)

    def test_assign_genres_from_weights(self):
        work = self._work()

        # This work was once classified under Fantasy and Romance.
        work.assign_genres_from_weights({Romance : 1000, Fantasy : 1000})
        self._db.commit()
        before = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        eq_([(u'Fantasy', 0.5), (u'Romance', 0.5)], before)

        # But now it's classified under Science Fiction and Romance.
        work.assign_genres_from_weights({Romance : 100, Science_Fiction : 300})
        self._db.commit()
        after = sorted((x.genre.name, x.affinity) for x in work.work_genres)
        eq_([(u'Romance', 0.25), (u'Science Fiction', 0.75)], after)

    def test_classifications_with_genre(self):
        work = self._work(with_open_access_download=True)
        identifier = work.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        subject1 = self._subject(type="type1", identifier="subject1")
        subject1.genre = genres[0]
        subject2 = self._subject(type="type2", identifier="subject2")
        subject2.genre = genres[1]
        subject3 = self._subject(type="type2", identifier="subject3")
        subject3.genre = None
        source = DataSource.lookup(self._db, DataSource.AXIS_360)
        classification1 = self._classification(
            identifier=identifier, subject=subject1,
            data_source=source, weight=1)
        classification2 = self._classification(
            identifier=identifier, subject=subject2,
            data_source=source, weight=2)
        classification3 = self._classification(
            identifier=identifier, subject=subject3,
            data_source=source, weight=2)

        results = work.classifications_with_genre().all()

        eq_([classification2, classification1], results)

    def test_mark_licensepools_as_superceded(self):
        # A commercial LP that somehow got superceded will be
        # un-superceded.
        commercial = self._licensepool(
            None, data_source_name=DataSource.OVERDRIVE
        )
        work, is_new = commercial.calculate_work()
        commercial.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(False, commercial.superceded)

        # An open-access LP that was superceded will be un-superceded if
        # chosen.
        gutenberg = self._licensepool(
            None, data_source_name=DataSource.GUTENBERG,
            open_access=True, with_open_access_download=True
        )
        work, is_new = gutenberg.calculate_work()
        gutenberg.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(False, gutenberg.superceded)

        # Of two open-access LPs, the one from the higher-quality data
        # source will be un-superceded, and the one from the
        # lower-quality data source will be superceded.
        standard_ebooks = self._licensepool(
            None, data_source_name=DataSource.STANDARD_EBOOKS,
            open_access=True, with_open_access_download=True
        )
        work.license_pools.append(standard_ebooks)
        gutenberg.superceded = False
        standard_ebooks.superceded = True
        work.mark_licensepools_as_superceded()
        eq_(True, gutenberg.superceded)
        eq_(False, standard_ebooks.superceded)

        # Of three open-access pools, 1 and only 1 will be chosen as non-superceded.
        gitenberg1 = self._licensepool(edition=None, open_access=True,
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gitenberg2 = self._licensepool(edition=None, open_access=True,
            data_source_name=DataSource.PROJECT_GITENBERG, with_open_access_download=True
        )

        gutenberg1 = self._licensepool(edition=None, open_access=True,
            data_source_name=DataSource.GUTENBERG, with_open_access_download=True
        )

        work_multipool = self._work(presentation_edition=None)
        work_multipool.license_pools.append(gutenberg1)
        work_multipool.license_pools.append(gitenberg2)
        work_multipool.license_pools.append(gitenberg1)

        # pools aren't yet aware of each other
        eq_(gutenberg1.superceded, False)
        eq_(gitenberg1.superceded, False)
        eq_(gitenberg2.superceded, False)

        # make pools figure out who's best
        work_multipool.mark_licensepools_as_superceded()

        eq_(gutenberg1.superceded, True)
        # There's no way to choose between the two gitenberg pools,
        # so making sure only one has been chosen is enough.
        chosen_count = 0
        for chosen_pool in gutenberg1, gitenberg1, gitenberg2:
            if chosen_pool.superceded is False:
                chosen_count += 1;
        eq_(chosen_count, 1)

        # throw wrench in
        gitenberg1.suppressed = True

        # recalculate bests
        work_multipool.mark_licensepools_as_superceded()
        eq_(gutenberg1.superceded, True)
        eq_(gitenberg1.superceded, True)
        eq_(gitenberg2.superceded, False)

        # A suppressed pool won't be superceded if it's the only pool for a work.
        only_pool = self._licensepool(
            None, open_access=True, with_open_access_download=True
        )
        work, ignore = only_pool.calculate_work()
        only_pool.suppressed = True
        work.mark_licensepools_as_superceded()
        eq_(False, only_pool.superceded)


    def test_work_remains_viable_on_pools_suppressed(self):
        """ If a work has all of its pools suppressed, the work's author, title,
        and subtitle still have the last best-known info in them.
        """
        (work, pool_std_ebooks, pool_git, pool_gut,
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = self._sample_ecosystem()

        # make sure the setup is what we expect
        eq_(pool_std_ebooks.suppressed, False)
        eq_(pool_git.suppressed, False)
        eq_(pool_gut.suppressed, False)

        # sanity check - we like standard ebooks and it got determined to be the best
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is the title of its presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is the author of its presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

        # now suppress all of the license pools
        pool_std_ebooks.suppressed = True
        pool_git.suppressed = True
        pool_gut.suppressed = True

        # and let work know
        work.calculate_presentation()

        # standard ebooks was last viable pool, and it stayed as work's choice
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is still the title of its last viable presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is still the author of its last viable presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

    def test_work_updates_info_on_pool_suppressed(self):
        """ If the provider of the work's presentation edition gets suppressed,
        the work will choose another child license pool's presentation edition as
        its presentation edition.
        """
        (work, pool_std_ebooks, pool_git, pool_gut,
            edition_std_ebooks, edition_git, edition_gut, alice, bob) = self._sample_ecosystem()

        # make sure the setup is what we expect
        eq_(pool_std_ebooks.suppressed, False)
        eq_(pool_git.suppressed, False)
        eq_(pool_gut.suppressed, False)

        # sanity check - we like standard ebooks and it got determined to be the best
        eq_(work.presentation_edition, pool_std_ebooks.presentation_edition)
        eq_(work.presentation_edition, edition_std_ebooks)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, work)
        eq_(edition_git.work, None)
        eq_(edition_gut.work, None)

        # The title of the Work is the title of its presentation edition.
        eq_("The Standard Ebooks Title", work.title)
        eq_("The Standard Ebooks Subtitle", work.subtitle)

        # The author of the Work is the author of its presentation edition.
        eq_("Alice Adder", work.author)
        eq_("Adder, Alice", work.sort_author)

        # now suppress the primary license pool
        pool_std_ebooks.suppressed = True

        # and let work know
        work.calculate_presentation()

        # gitenberg is next best and it got determined to be the best
        eq_(work.presentation_edition, pool_git.presentation_edition)
        eq_(work.presentation_edition, edition_git)

        # editions know who's the presentation edition
        eq_(edition_std_ebooks.work, None)
        eq_(edition_git.work, work)
        eq_(edition_gut.work, None)

        # The title of the Work is still the title of its last viable presentation edition.
        eq_("The GItenberg Title", work.title)
        eq_("The GItenberg Subtitle", work.subtitle)

        # The author of the Work is still the author of its last viable presentation edition.
        eq_("Alice Adder, Bob Bitshifter", work.author)
        eq_("Adder, Alice ; Bitshifter, Bob", work.sort_author)

    def test_different_language_means_different_work(self):
        """There are two open-access LicensePools for the same book in
        different languages. The author and title information is the
        same, so the books have the same permanent work ID, but since
        they are in different languages they become separate works.
        """
        title = 'Siddhartha'
        author = ['Herman Hesse']
        edition1, lp1 = self._edition(
            title=title, authors=author, language='eng', with_license_pool=True,
            with_open_access_download=True
        )
        w1 = lp1.calculate_work()
        edition2, lp2 = self._edition(
            title=title, authors=author, language='ger', with_license_pool=True,
            with_open_access_download=True
        )
        w2 = lp2.calculate_work()
        for l in (lp1, lp2):
            eq_(False, l.superceded)
        assert w1 != w2

    def test_reject_covers(self):
        edition, lp = self._edition(with_open_access_download=True)

        # Create a cover and thumbnail for the edition.
        current_folder = os.path.split(__file__)[0]
        base_path = os.path.dirname(current_folder)
        sample_cover_path = base_path + '/files/covers/test-book-cover.png'
        cover_href = 'http://cover.png'
        cover_link = lp.add_link(
            Hyperlink.IMAGE, cover_href, lp.data_source,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path, 'rb').read()
        )[0]

        thumbnail_href = 'http://thumbnail.png'
        thumbnail_rep = self._representation(
            url=thumbnail_href,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=open(sample_cover_path, 'rb').read(),
            mirrored=True
        )[0]

        cover_rep = cover_link.resource.representation
        cover_rep.mirror_url = cover_href
        cover_rep.mirrored_at = datetime.datetime.utcnow()
        cover_rep.thumbnails.append(thumbnail_rep)

        edition.set_cover(cover_link.resource)
        full_url = cover_link.resource.url
        thumbnail_url = thumbnail_rep.mirror_url

        # A Work created from this edition has cover details.
        work = self._work(presentation_edition=edition)
        assert work.cover_full_url and work.cover_thumbnail_url

        # A couple helper methods to make these tests more readable.
        def has_no_cover(work_or_edition):
            """Determines whether a Work or an Edition has a cover."""
            eq_(None, work_or_edition.cover_full_url)
            eq_(None, work_or_edition.cover_thumbnail_url)
            eq_(True, cover_link.resource.voted_quality < 0)
            eq_(True, cover_link.resource.votes_for_quality > 0)

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
            eq_(full_url, work.cover_full_url)
            eq_(thumbnail_url, work.cover_thumbnail_url)
            for url in [full_url, thumbnail_url]:
                assert url in work.simple_opds_entry
                assert url in work.verbose_opds_entry

        # Suppressing the cover removes the cover from the work.
        index = MockExternalSearchIndex()
        Work.reject_covers(self._db, [work], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # It also works with Identifiers.
        identifier = work.license_pools[0].identifier
        Work.reject_covers(self._db, [identifier], search_index_client=index)
        assert has_no_cover(work)
        reset_cover()

        # When other Works or Editions share a cover, they are also
        # updated during the suppression process.
        other_edition = self._edition()
        other_edition.set_cover(cover_link.resource)
        other_work_ed = self._edition()
        other_work_ed.set_cover(cover_link.resource)
        other_work = self._work(presentation_edition=other_work_ed)

        Work.reject_covers(self._db, [work], search_index_client=index)
        assert has_no_cover(other_edition)
        assert has_no_cover(other_work)

    def test_missing_coverage_from(self):
        operation = 'the_operation'

        # Here's a work with a coverage record.
        work = self._work(with_license_pool=True)

        # It needs coverage.
        eq_([work], Work.missing_coverage_from(self._db, operation).all())

        # Let's give it coverage.
        record = self._work_coverage_record(work, operation)

        # It no longer needs coverage!
        eq_([], Work.missing_coverage_from(self._db, operation).all())

        # But if we disqualify coverage records created before a
        # certain time, it might need coverage again.
        cutoff = record.timestamp + datetime.timedelta(seconds=1)

        eq_(
            [work], Work.missing_coverage_from(
                self._db, operation, count_as_missing_before=cutoff
            ).all()
        )

    def test_top_genre(self):
        work = self._work()
        identifier = work.presentation_edition.primary_identifier
        genres = self._db.query(Genre).all()
        source = DataSource.lookup(self._db, DataSource.AXIS_360)

        # returns None when work has no genres
        eq_(None, work.top_genre())

        # returns only genre
        wg1, is_new = get_one_or_create(
            self._db, WorkGenre, work=work, genre=genres[0], affinity=1
        )
        eq_(genres[0].name, work.top_genre())

        # returns top genre
        wg1.affinity = 0.2
        wg2, is_new = get_one_or_create(
            self._db, WorkGenre, work=work, genre=genres[1], affinity=0.8
        )
        eq_(genres[1].name, work.top_genre())

    def test_to_search_document(self):
        # Set up an edition and work.
        edition, pool1 = self._edition(authors=[self._str, self._str], with_license_pool=True)
        work = self._work(presentation_edition=edition)

        # Create a second Collection that has a different LicensePool
        # for the same Work.
        collection1 = self._default_collection
        collection2 = self._collection()
        self._default_library.collections.append(collection2)
        pool2 = self._licensepool(edition=edition, collection=collection2)
        pool2.work_id = work.id
        pool2.licenses_available = 0
        pool2.licenses_owned = 10
        work.license_pools.append(pool2)

        # Create a third Collection that's just hanging around, not
        # doing anything.
        collection3 = self._collection()

        # These are the edition's authors.
        [contributor1] = [c.contributor for c in edition.contributions if c.role == Contributor.PRIMARY_AUTHOR_ROLE]
        contributor1.display_name = self._str
        contributor1.family_name = self._str
        contributor1.viaf = self._str
        contributor1.lc = self._str
        [contributor2] = [c.contributor for c in edition.contributions if c.role == Contributor.AUTHOR_ROLE]

        data_source = DataSource.lookup(self._db, DataSource.THREEM)

        # This identifier is strongly equivalent to the edition's.
        identifier1 = self._identifier(identifier_type=Identifier.ISBN)
        identifier1.equivalent_to(data_source, edition.primary_identifier, 0.9)

        # This identifier is equivalent to the other identifier, but the strength
        # is too weak for it to be used.
        identifier2 = self._identifier(identifier_type=Identifier.ISBN)
        identifier2.equivalent_to(data_source, identifier2, 0.1)

        # This identifier is equivalent to the _edition's_, but too weak to
        # be used.
        identifier3 = self._identifier(identifier_type=Identifier.ISBN)
        identifier3.equivalent_to(data_source, edition.primary_identifier, 0.1)

        # Add some classifications.

        # This classification has no subject name, so the search document will use the subject identifier.
        edition.primary_identifier.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 6)

        # This one has the same subject type and identifier, so their weights will be combined.
        identifier1.classify(data_source, Subject.BISAC, "FICTION/Science Fiction/Time Travel", None, 1)

        # Here's another classification with a different subject type.
        edition.primary_identifier.classify(data_source, Subject.OVERDRIVE, "Romance", None, 2)

        # This classification has a subject name, so the search document will use that instead of the identifier.
        identifier1.classify(data_source, Subject.FAST, self._str, "Sea Stories", 7)

        # This classification will be left out because its subject type isn't useful for search.
        identifier1.classify(data_source, Subject.DDC, self._str, None)

        # These classifications will be left out because their identifiers aren't sufficiently equivalent to the edition's.
        identifier2.classify(data_source, Subject.FAST, self._str, None)
        identifier3.classify(data_source, Subject.FAST, self._str, None)

        # Add some genres.
        genre1, ignore = Genre.lookup(self._db, "Science Fiction")
        genre2, ignore = Genre.lookup(self._db, "Romance")
        work.genres = [genre1, genre2]
        work.work_genres[0].affinity = 1

        # Add two custom lists. The work is featured on one list but
        # not the other.
        appeared_1 = datetime.datetime(2010, 1, 1)
        appeared_2 = datetime.datetime(2011, 1, 1)
        l1, ignore = self._customlist(num_entries=0)
        l1.add_entry(work, featured=False, update_external_index=False,
                     first_appearance=appeared_1)
        l2, ignore = self._customlist(num_entries=0)
        l2.add_entry(work, featured=True, update_external_index=False,
                     first_appearance=appeared_2)

        # Add the other fields used in the search document.
        work.target_age = NumericRange(7, 8, '[]')
        edition.subtitle = self._str
        edition.series = self._str
        edition.series_position = 99
        edition.publisher = self._str
        edition.imprint = self._str
        work.fiction = False
        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        work.summary_text = self._str
        work.rating = 5
        work.popularity = 4
        work.last_update_time = datetime.datetime.utcnow()

        # Make sure all of this will show up in a database query.
        self._db.flush()

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
                python - datetime.datetime.utcfromtimestamp(0)
            ).total_seconds()
            eq_(int(expect), int(postgres))

        search_doc = work.to_search_document()
        eq_(work.id, search_doc['_id'])
        eq_(work.id, search_doc['work_id'])
        eq_(work.title, search_doc['title'])
        eq_(edition.subtitle, search_doc['subtitle'])
        eq_(edition.series, search_doc['series'])
        eq_(edition.series_position, search_doc['series_position'])
        eq_(edition.language, search_doc['language'])
        eq_(work.sort_title, search_doc['sort_title'])
        eq_(work.author, search_doc['author'])
        eq_(work.sort_author, search_doc['sort_author'])
        eq_(edition.publisher, search_doc['publisher'])
        eq_(edition.imprint, search_doc['imprint'])
        eq_(edition.permanent_work_id, search_doc['permanent_work_id'])
        eq_("Nonfiction", search_doc['fiction'])
        eq_("YoungAdult", search_doc['audience'])
        eq_(work.summary_text, search_doc['summary'])
        eq_(work.quality, search_doc['quality'])
        eq_(work.rating, search_doc['rating'])
        eq_(work.popularity, search_doc['popularity'])
        eq_(work.presentation_ready, search_doc['presentation_ready'])
        assert_time_match(work.last_update_time, search_doc['last_update_time'])
        eq_(dict(lower=7, upper=8), search_doc['target_age'])

        # Each LicensePool for the Work is listed in
        # the 'licensepools' section.
        licensepools = search_doc['licensepools']
        eq_(2, len(licensepools))
        eq_(set([x.id for x in work.license_pools]),
            set([x['licensepool_id'] for x in licensepools]))

        # Each item in the 'licensepools' section has a variety of useful information
        # about the corresponding LicensePool.
        for pool in work.license_pools:
            [match] = [x for x in licensepools if x['licensepool_id'] == pool.id]
            eq_(pool.open_access, match['open_access'])
            eq_(pool.collection_id, match['collection_id'])
            eq_(pool.suppressed, match['suppressed'])
            eq_(pool.data_source_id, match['data_source_id'])

            eq_(pool.licenses_available > 0, match['available'])
            eq_(pool.licenses_owned > 0, match['licensed'])

            # The work quality is stored in the main document, but
            # it's also stored in the license pool subdocument so that
            # we can apply a nested filter that includes quality +
            # information from the subdocument.
            eq_(work.quality, match['quality'])

            assert_time_match(
                pool.availability_time, match['availability_time']
            )

            # The medium of the work's presentation edition is stored
            # in the main document, but it's also stored in the
            # license poolsubdocument, so that we can filter out
            # license pools that represent audiobooks from unsupported
            # sources.
            eq_(edition.medium, search_doc['medium'])
            eq_(edition.medium, match['medium'])

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
            return sorted(x, key = lambda b: b['identifier'])
        eq_(s(expect), s(search_doc['identifiers']))

        # Each custom list entry for the work is in the 'customlists'
        # section.
        not_featured, featured = sorted(
            search_doc['customlists'], key = lambda x: x['featured']
        )
        assert_time_match(appeared_1, not_featured.pop('first_appearance'))
        eq_(dict(featured=False, list_id=l1.id), not_featured)
        assert_time_match(appeared_2, featured.pop('first_appearance'))
        eq_(dict(featured=True, list_id=l2.id), featured)

        contributors = search_doc['contributors']
        eq_(2, len(contributors))

        [contributor1_doc] = [c for c in contributors if c['sort_name'] == contributor1.sort_name]
        [contributor2_doc] = [c for c in contributors if c['sort_name'] == contributor2.sort_name]

        eq_(contributor1.display_name, contributor1_doc['display_name'])
        eq_(None, contributor2_doc['display_name'])

        eq_(contributor1.family_name, contributor1_doc['family_name'])
        eq_(None, contributor2_doc['family_name'])

        eq_(contributor1.viaf, contributor1_doc['viaf'])
        eq_(None, contributor2_doc['viaf'])

        eq_(contributor1.lc, contributor1_doc['lc'])
        eq_(None, contributor2_doc['lc'])

        eq_(Contributor.PRIMARY_AUTHOR_ROLE, contributor1_doc['role'])
        eq_(Contributor.AUTHOR_ROLE, contributor2_doc['role'])

        classifications = search_doc['classifications']
        eq_(3, len(classifications))
        [classification1_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.BISAC]]
        [classification2_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.OVERDRIVE]]
        [classification3_doc] = [c for c in classifications if c['scheme'] == Subject.uri_lookup[Subject.FAST]]
        eq_("FICTION Science Fiction Time Travel", classification1_doc['term'])
        eq_(float(6 + 1)/(6 + 1 + 2 + 7), classification1_doc['weight'])
        eq_("Romance", classification2_doc['term'])
        eq_(float(2)/(6 + 1 + 2 + 7), classification2_doc['weight'])
        eq_("Sea Stories", classification3_doc['term'])
        eq_(float(7)/(6 + 1 + 2 + 7), classification3_doc['weight'])

        genres = search_doc['genres']
        eq_(2, len(genres))
        [genre1_doc] = [g for g in genres if g['name'] == genre1.name]
        [genre2_doc] = [g for g in genres if g['name'] == genre2.name]
        eq_(Subject.SIMPLIFIED_GENRE, genre1_doc['scheme'])
        eq_(genre1.id, genre1_doc['term'])
        eq_(1, genre1_doc['weight'])
        eq_(Subject.SIMPLIFIED_GENRE, genre2_doc['scheme'])
        eq_(genre2.id, genre2_doc['term'])
        eq_(0, genre2_doc['weight'])

        target_age_doc = search_doc['target_age']
        eq_(work.target_age.lower, target_age_doc['lower'])
        eq_(work.target_age.upper, target_age_doc['upper'])

        # If a book stops being available through a collection
        # (because its LicensePool loses all its licenses or stops
        # being open access), it will no longer be listed
        # in its Work's search document.
        [pool] = collection1.licensepools
        pool.licenses_owned = 0
        self._db.commit()
        search_doc = work.to_search_document()
        eq_([collection2.id],
            [x['collection_id'] for x in search_doc['licensepools']])

        # If the book becomes available again, the collection will
        # start showing up again.
        pool.open_access = True
        self._db.commit()
        search_doc = work.to_search_document()
        eq_(set([collection1.id, collection2.id]),
            set([x['collection_id'] for x in search_doc['licensepools']]))

    def test_age_appropriate_for_patron(self):
        # The target audience and age of a patron's root lane controls
        # whether a given book is 'age-appropriate' for them.
        class MockLane(object):
            audiences = [Classifier.AUDIENCE_CHILDREN,
                         Classifier.AUDIENCE_YOUNG_ADULT]
            target_age = object()

        class MockPatron(object):
            root_lane = None

        class MockWork(Work):
            calls = []
            return_true_for = None
            def age_appropriate_for(self, audience, target_age):
                self.calls.append((audience, target_age))
                if audience == self.return_true_for:
                    return True
                return False

        patron = MockPatron()
        w = MockWork()

        # The patron has no root lane, so age_appropriate_for is not
        # even called.
        eq_(True, w.age_appropriate_for_patron(patron))
        eq_([], w.calls)

        # Give the patron a root lane.
        lane = MockLane()
        patron.root_lane = lane
        eq_(False, w.age_appropriate_for_patron(patron))

        # age_appropriate_for was called on each audience associated
        # with the root lane.
        c1, c2 = w.calls
        eq_((Classifier.AUDIENCE_CHILDREN, lane.target_age), c1)
        eq_((Classifier.AUDIENCE_YOUNG_ADULT, lane.target_age), c2)

        # age_appropriate_for_patron() will only return True if at
        # least one of the age_appropriate_for() calls returns
        # True.
        #
        # Simulate this by telling our mock Work to return True only
        # when passed a specific audience. Our Mock lane has two
        # audiences, one of which may or may not match.
        w.return_true_for = Classifier.AUDIENCE_CHILDREN
        eq_(True, w.age_appropriate_for_patron(patron))

        w.return_true_for = Classifier.AUDIENCE_YOUNG_ADULT
        eq_(True, w.age_appropriate_for_patron(patron))

        w.return_true_for = Classifier.AUDIENCE_ADULT
        eq_(False, w.age_appropriate_for_patron(patron))

    def test_age_appropriate_for_patron_end_to_end(self):
        # A test of age_appropriate_for_patron without mocks.
        patron = self._patron()
        patron.external_type = "a"

        # This Lane contains books at the old end of the "children"
        # range and the young end of the "young adult" range.
        lane = self._lane()
        lane.root_for_patron_type = ["a"]
        # NOTE: setting target_age sets .audiences to appropriate values,
        # so setting .audiences here is purely demonstrative.
        lane.audiences = [
            Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT
        ]
        lane.target_age = (9,14)

        work = self._work()
        work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        work.target_age = tuple_to_numericrange((12, 15))

        eq_(True, work.age_appropriate_for_patron(patron))

    def test_age_appropriate_for(self):
        # Check whether this work is age-appropriate for a certain audience.
        w = self._work()
        m = w.age_appropriate_for
        w.audience = object()

        ya = Classifier.AUDIENCE_YOUNG_ADULT
        children = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT

        # A reader with no particular audience can see everything.
        eq_(True, m(None, object()))

        # A patron associated with a non-juvenile audience, such as
        # AUDIENCE_ADULT, can see everything.
        for patron_audience in Classifier.AUDIENCES:
            if patron_audience in Classifier.AUDIENCES_JUVENILE:
                # Tested later.
                continue
            eq_(True, m(patron_audience, object()))

        # Everyone can see 'all-ages' books.
        w.audience = Classifier.AUDIENCE_ALL_AGES
        for patron_audience in Classifier.AUDIENCES:
            eq_(True, m(patron_audience, object()))

        # Children cannot see YA or adult books.
        for audience in (ya, adult):
            w.audience = audience
            eq_(False, m(children, None))

            # This is true even if the "child's" target age is set to
            # a value that would allow for this (as can happen when
            # the patron's root lane is set up to show both children's
            # and YA titles).
            eq_(False, m(children, (14,18)))

        # YA readers can see any children's title.
        w.audience = children
        eq_(True, m(ya, object()))

        # A YA reader is treated as an adult (with no reading
        # restrictions) if they have no associated age range, or their
        # age range includes ADULT_AGE_CUTOFF.
        w.audience = adult
        eq_(True, m(ya, None))
        eq_(True, m(ya, 18))
        eq_(True, m(ya, (14, 18)))

        # Otherwise, YA readers cannot see books for adults.
        eq_(False, m(ya, 16))
        eq_(False, m(ya, (14,17)))

        # Now let's consider the most complicated cases. First, a
        # child who wants to read a children's book.
        w.audience = children
        for patron_audience in Classifier.AUDIENCES_YOUNG_CHILDREN:
            # No target age -> it's fine (or at least we don't have
            # the information necessary to say it's not fine).
            w.target_age = None
            eq_(True, m(patron_audience, object()))

            w.target_age = (5, 7)
            # Old enough.
            for patron_age in (5,6,7,8,9):
                eq_(True, m(patron_audience, patron_age))
                eq_(True, m(patron_audience, (patron_age-1, patron_age)))
            # Not old enough.
            for patron_age in (2,3,4):
                eq_(False, m(patron_audience, patron_age))
                eq_(False, m(patron_audience, (patron_age-1, patron_age)))

        # Similarly, a YA reader who wants to read a YA book.
        w.audience = ya

        # No target age -> it's fine (or at least we don't have
        # the information necessary to say it's not fine).
        w.target_age = None
        eq_(True, m(ya, object()))

        w.target_age = (14, 16)
        # Old enough.
        for patron_age in range(14, 20):
            eq_(True, m(ya, patron_age))
            eq_(True, m(ya, (patron_age-1, patron_age)))

        # Not old enough.
        for patron_age in range(6,14):
            eq_(False, m(ya, patron_age))
            eq_(False, m(ya, (patron_age-1, patron_age)))

    def test_unlimited_access_books_are_available_by_default(self):
        # Set up an edition and work.
        edition, pool = self._edition(authors=[self._str, self._str], with_license_pool=True)
        work = self._work(presentation_edition=edition)

        pool.open_access = False
        pool.self_hosted = False
        pool.unlimited_access = True

        # Make sure all of this will show up in a database query.
        self._db.flush()

        search_doc = work.to_search_document()

        # Each LicensePool for the Work is listed in
        # the 'licensepools' section.
        licensepools = search_doc['licensepools']
        eq_(1, len(licensepools))
        eq_(licensepools[0]['open_access'], False)
        eq_(licensepools[0]['available'], True)

    def test_self_hosted_books_are_available_by_default(self):
        # Set up an edition and work.
        edition, pool = self._edition(authors=[self._str, self._str], with_license_pool=True)
        work = self._work(presentation_edition=edition)

        pool.licenses_owned = 0
        pool.licenses_available = 0
        pool.self_hosted = True

        # Make sure all of this will show up in a database query.
        self._db.flush()

        search_doc = work.to_search_document()

        # Each LicensePool for the Work is listed in
        # the 'licensepools' section.
        licensepools = search_doc['licensepools']
        eq_(1, len(licensepools))
        eq_(licensepools[0]['open_access'], False)
        eq_(licensepools[0]['available'], True)

    def test_target_age_string(self):
        work = self._work()
        work.target_age = NumericRange(7, 8, '[]')
        eq_("7-8", work.target_age_string)

        work.target_age = NumericRange(0, 8, '[]')
        eq_("0-8", work.target_age_string)

        work.target_age = NumericRange(8, None, '[]')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(None, 8, '[]')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(7, 8, '[)')
        eq_("7", work.target_age_string)

        work.target_age = NumericRange(0, 8, '[)')
        eq_("0-7", work.target_age_string)

        work.target_age = NumericRange(7, 8, '(]')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(0, 8, '(]')
        eq_("1-8", work.target_age_string)

        work.target_age = NumericRange(7, 9, '()')
        eq_("8", work.target_age_string)

        work.target_age = NumericRange(0, 8, '()')
        eq_("1-7", work.target_age_string)

        work.target_age = NumericRange(None, None, '()')
        eq_("", work.target_age_string)

        work.target_age = None
        eq_("", work.target_age_string)

    def test_reindex_on_availability_change(self):
        # A change in a LicensePool's availability creates a
        # WorkCoverageRecord indicating that the work needs to be
        # re-indexed.
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
        no_licensepool = self._work()
        eq_(None, find_record(no_licensepool))

        # A Work with a LicensePool starts off in a state where it
        # needs to be indexed.
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        record = find_record(work)
        eq_(registered, record.status)

        # If it stops being open-access, it needs to be reindexed.
        record.status = success
        pool.open_access = False
        record = find_record(work)
        eq_(registered, record.status)

        # If it becomes open-access again, it needs to be reindexed.
        record.status = success
        pool.open_access = True
        record = find_record(work)
        eq_(registered, record.status)

        # If its last_update_time is changed, it needs to be
        # reindexed. (This happens whenever
        # LicensePool.update_availability is called, meaning that
        # patron transactions always trigger a reindex).
        record.status = success
        work.last_update_time = datetime.datetime.utcnow()
        eq_(registered, record.status)

        # If its collection changes (which shouldn't happen), it needs
        # to be reindexed.
        record.status = success
        collection2 = self._collection()
        pool.collection_id = collection2.id
        eq_(registered, record.status)

        # If a LicensePool is deleted (which also shouldn't happen),
        # its former Work needs to be reindexed.
        record.status = success
        self._db.delete(pool)
        work = self._db.query(Work).filter(Work.id==work.id).one()
        record = find_record(work)
        eq_(registered, record.status)

        # If a LicensePool is moved in from another Work, _both_ Works
        # need to be reindexed.
        record.status = success
        another_work = self._work(with_license_pool=True)
        [another_pool] = another_work.license_pools
        work.license_pools.append(another_pool)
        eq_([], another_work.license_pools)

        for work in (work, another_work):
            record = find_record(work)
            eq_(registered, record.status)

    def test_reset_coverage(self):
        # Test the methods that reset coverage for works, indicating
        # that some task needs to be performed again.
        WCR = WorkCoverageRecord
        work = self._work()
        work.presentation_ready = True
        index = MockExternalSearchIndex()

        # Calling _reset_coverage when there is no coverage creates
        # a new WorkCoverageRecord in the REGISTERED state
        operation = "an operation"
        record = work._reset_coverage(operation)
        eq_(WCR.REGISTERED, record.status)

        # Calling _reset_coverage when the WorkCoverageRecord already
        # exists sets the state back to REGISTERED.
        record.state = WCR.SUCCESS
        work._reset_coverage(operation)
        eq_(WCR.REGISTERED, record.status)

        # A number of methods with helpful names all call _reset_coverage
        # for some specific operation.
        def mock_reset_coverage(operation):
            work.coverage_reset_for = operation
        work._reset_coverage = mock_reset_coverage

        for method, operation in (
            (work.needs_full_presentation_recalculation,
             WCR.CLASSIFY_OPERATION),
            (work.needs_new_presentation_edition,
             WCR.CHOOSE_EDITION_OPERATION),
            (work.external_index_needs_updating,
             WCR.UPDATE_SEARCH_INDEX_OPERATION)
        ):
            method()
            eq_(operation, work.coverage_reset_for)

        # The work was not added to the search index when we called
        # external_index_needs_updating. That happens later, when the
        # WorkCoverageRecord is processed.
        eq_([], index.docs.values())

    def test_for_unchecked_subjects(self):

        w1 = self._work(with_license_pool=True)
        w2 = self._work()
        identifier = w1.license_pools[0].identifier

        # Neither of these works is associated with any subjects, so
        # they're not associated with any unchecked subjects.
        qu = Work.for_unchecked_subjects(self._db)
        eq_([], qu.all())

        # These Subjects haven't been checked, so the Work associated with
        # them shows up.
        ds = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        classification = identifier.classify(ds, Subject.TAG, "some tag")
        classification2 = identifier.classify(ds, Subject.TAG, "another tag")
        eq_([w1], qu.all())

        # If one of them is checked, the Work still shows up.
        classification.subject.checked = True
        eq_([w1], qu.all())

        # Only when all Subjects are checked does the work stop showing up.
        classification2.subject.checked = True
        eq_([], qu.all())

    def test_calculate_opds_entries(self):
        """Verify that calculate_opds_entries sets both simple and verbose
        entries.
        """
        work = self._work()
        work.simple_opds_entry = None
        work.verbose_opds_entry = None

        work.calculate_opds_entries(verbose=False)
        simple_entry = work.simple_opds_entry
        assert simple_entry.startswith('<entry')
        eq_(None, work.verbose_opds_entry)

        work.calculate_opds_entries(verbose=True)
        # The simple OPDS entry is the same length as before.
        # It's not necessarily _exactly_ the same because the
        # <updated> timestamp may be different.
        eq_(len(simple_entry), len(work.simple_opds_entry))

        # The verbose OPDS entry is longer than the simple one.
        assert work.verbose_opds_entry.startswith('<entry')
        assert len(work.verbose_opds_entry) > len(simple_entry)

    def test_calculate_marc_record(self):
        work = self._work(with_license_pool=True)
        work.marc_record = None

        work.calculate_marc_record()
        assert work.title.encode("utf8") in work.marc_record
        assert b"online resource" in work.marc_record

    def test_active_licensepool_ignores_superceded_licensepools(self):
        work = self._work(with_license_pool=True,
                          with_open_access_download=True)
        [pool1] = work.license_pools
        edition, pool2 = self._edition(with_license_pool=True)
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
        eq_(pool2, work.active_license_pool())

        # pool2 is superceded and pool1 is not. The active licensepool
        # is pool1.
        pool1.superceded = False
        pool2.superceded = True
        eq_(pool1, work.active_license_pool())

        # If both license pools are superceded, there is no active license
        # pool for the book.
        pool1.superceded = True
        eq_(None, work.active_license_pool())
        pool1.superceded = False
        pool2.superceded = False

        # If one license pool is open-access and the other is not, the
        # open-access pool wins.
        pool1.open_access = True
        eq_(pool1, work.active_license_pool())
        pool1.open_access = False

        # pool2 is open-access but has no usable download. The other
        # pool wins.
        pool2.open_access = True
        eq_(pool1, work.active_license_pool())
        pool2.open_access = False

        # If one license pool has no owned licenses and the other has
        # owned licenses, the one with licenses wins.
        pool1.licenses_owned = 0
        pool2.licenses_owned = 1
        eq_(pool2, work.active_license_pool())
        pool1.licenses_owned = 1

        # If one license pool has a presentation edition that's missing
        # a title, and the other pool has a presentation edition with a title,
        # the one with a title wins.
        pool2.presentation_edition.title = None
        eq_(pool1, work.active_license_pool())

    def test_delete_work(self):
        # Search mock
        class MockSearchIndex():
            removed = []
            def remove_work(self, work):
                self.removed.append(work)

        s = MockSearchIndex();
        work = self._work(with_license_pool=True)
        work.delete(search_index=s)

        eq_([], self._db.query(Work).filter(Work.id==work.id).all())
        eq_(1, len(s.removed))
        eq_(s.removed, [work])

class TestWorkConsolidation(DatabaseTest):

    def test_calculate_work_success(self):
        e, p = self._edition(with_license_pool=True)
        work, new = p.calculate_work()
        eq_(p.presentation_edition, work.presentation_edition)
        eq_(True, new)

    def test_calculate_work_bails_out_if_no_title(self):
        e, p = self._edition(with_license_pool=True)
        e.title=None
        work, new = p.calculate_work()
        eq_(None, work)
        eq_(False, new)

        # even_if_no_title means we don't need a title.
        work, new = p.calculate_work(even_if_no_title=True)
        assert isinstance(work, Work)
        eq_(True, new)
        eq_(None, work.title)
        eq_(None, work.presentation_edition.permanent_work_id)

    def test_calculate_work_even_if_no_author(self):
        title = "Book"
        e, p = self._edition(with_license_pool=True, authors=[], title=title)
        work, new = p.calculate_work()
        eq_(title, work.title)
        eq_(True, new)

    def test_calculate_work_matches_based_on_permanent_work_id(self):
        # Here are two Editions with the same permanent work ID,
        # since they have the same title/author.
        edition1, ignore = self._edition(with_license_pool=True)
        edition2, ignore = self._edition(
            title=edition1.title, authors=edition1.author,
            with_license_pool=True
        )

        # For purposes of this test, let's pretend all these books are
        # open-access.
        for e in [edition1, edition2]:
            for license_pool in e.license_pools:
                license_pool.open_access = True

        # Calling calculate_work() on the first edition creates a Work.
        work1, created = edition1.license_pools[0].calculate_work()
        eq_(created, True)

        # Calling calculate_work() on the second edition associated
        # the second edition's pool with the first work.
        work2, created = edition2.license_pools[0].calculate_work()
        eq_(created, False)

        eq_(work1, work2)

        expect = edition1.license_pools + edition2.license_pools
        eq_(set(expect), set(work1.license_pools))


    def test_calculate_work_for_licensepool_creates_new_work(self):
        edition1, ignore = self._edition(data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID,
            title=self._str, authors=[self._str], with_license_pool=True)

        # This edition is unique to the existing work.
        preexisting_work = Work()
        preexisting_work.set_presentation_edition(edition1)

        # This edition is unique to the new LicensePool
        edition2, pool = self._edition(data_source_name=DataSource.GUTENBERG, identifier_type=Identifier.GUTENBERG_ID,
            title=self._str, authors=[self._str], with_license_pool=True)

        # Call calculate_work(), and a new Work is created.
        work, created = pool.calculate_work()
        eq_(True, created)
        assert work != preexisting_work

    def test_calculate_work_does_nothing_unless_edition_has_title(self):
        collection=self._collection()
        edition, ignore = Edition.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
        )
        pool, ignore = LicensePool.for_foreign_id(
            self._db, DataSource.GUTENBERG, Identifier.GUTENBERG_ID, "1",
            collection=collection
        )
        work, created = pool.calculate_work()
        eq_(None, work)

        edition.title = u"foo"
        work, created = pool.calculate_work()
        edition.calculate_presentation()
        eq_(True, created)
        #
        # # The edition is the work's presentation edition.
        eq_(work, edition.work)
        eq_(edition, work.presentation_edition)
        eq_(u"foo", work.title)
        eq_(u"[Unknown]", work.author)

    def test_calculate_work_fails_when_presentation_edition_identifier_does_not_match_license_pool(self):

        # Here's a LicensePool with an Edition.
        edition1, pool = self._edition(
            data_source_name=DataSource.GUTENBERG, with_license_pool=True
        )

        # Here's a second Edition that's talking about a different Identifier
        # altogether, and has no LicensePool.
        edition2 = self._edition()
        assert edition1.primary_identifier != edition2.primary_identifier

        # Here's a third Edition that's tied to a totally different
        # LicensePool.
        edition3, pool2 = self._edition(with_license_pool=True)
        assert edition1.primary_identifier != edition3.primary_identifier

        # When we calculate a Work for a LicensePool, we can pass in
        # any Edition as the presentation edition, so long as that
        # Edition's primary identifier matches the LicensePool's
        # identifier.
        work, is_new = pool.calculate_work(known_edition=edition1)

        # But we can't pass in an Edition that's the presentation
        # edition for a LicensePool with a totally different Identifier.
        for edition in (edition2, edition3):
            assert_raises_regexp(
                ValueError,
                "Alleged presentation edition is not the presentation edition for the license pool for which work is being calculated!",
                pool.calculate_work,
                known_edition=edition
            )

    def test_open_access_pools_grouped_together(self):

        # We have four editions with exactly the same title and author.
        # Two of them are open-access, two are not.
        title = "The Only Title"
        author = "Single Author"
        ed1, open1 = self._edition(title=title, authors=author, with_license_pool=True)
        ed2, open2 = self._edition(title=title, authors=author, with_license_pool=True)
        open1.open_access = True
        open2.open_access = True
        ed3, restricted3 = self._edition(
            title=title, authors=author, data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)
        ed4, restricted4 = self._edition(
            title=title, authors=author, data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True)

        restricted3.open_access = False
        restricted4.open_access = False

        # Every identifier is equivalent to every other identifier.
        s = DataSource.lookup(self._db, DataSource.OCLC_LINKED_DATA)
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

        assert open1.work != None
        assert open2.work != None
        assert restricted3.work != None
        assert restricted4.work != None

        # The two open-access pools are grouped together.
        eq_(open1.work, open2.work)

        # Each restricted-access pool is completely isolated.
        assert restricted3.work != restricted4.work
        assert restricted3.work != open1.work

    def test_all_licensepools_with_same_identifier_get_same_work(self):

        # Here are two LicensePools for the same Identifier and
        # DataSource, but different Collections.
        edition1, pool1 = self._edition(with_license_pool=True)
        identifier = pool1.identifier
        collection2 = self._collection()

        edition2, pool2 = self._edition(
            with_license_pool=True,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
            collection=collection2
        )

        eq_(pool1.identifier, pool2.identifier)
        eq_(pool1.data_source, pool2.data_source)
        eq_(self._default_collection, pool1.collection)
        eq_(collection2, pool2.collection)

        # The two LicensePools have the same Edition (since a given
        # DataSource has only one opinion about an Identifier's
        # bibliographic information).
        eq_(edition1, edition2)

        # Because the two LicensePools have the same Identifier, they
        # have the same Work.
        work1, is_new_1 = pool1.calculate_work()
        work2, is_new_2 = pool2.calculate_work()
        eq_(work1, work2)
        eq_(True, is_new_1)
        eq_(False, is_new_2)
        eq_(edition1, work1.presentation_edition)

    def test_calculate_work_fixes_work_in_invalid_state(self):
        # Here's a Work with a commercial edition of "abcd".
        work = self._work(with_license_pool=True)
        [abcd_commercial] = work.license_pools
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains a _second_
        # commercial edition of "abcd"...
        edition, abcd_commercial_2 = self._edition(with_license_pool=True)
        abcd_commercial_2.open_access = False
        abcd_commercial_2.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(abcd_commercial_2)

        # ...as well as an open-access edition of "abcd".
        edition, abcd_open_access = self._edition(
            with_license_pool=True, with_open_access_download=True
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
        eq_(work_after, work)
        eq_(False, is_new)

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other two have been kicked out and
        # given their own works.
        assert abcd_commercial_2.work != work
        assert abcd_open_access.work != work

        # The commercial LicensePool has been given a Work of its own.
        eq_([abcd_commercial_2], abcd_commercial_2.work.license_pools)

        # The open-access work has been given the Work that will be
        # used for all open-access LicensePools for that book going
        # forward.

        expect_open_access_work, open_access_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        eq_(expect_open_access_work, abcd_open_access.work)

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
        eq_(work, work_after)
        eq_(False, is_new)

        assert abcd_commercial.work != work
        assert abcd_commercial.work != None
        assert abcd_commercial_2.work != work
        assert abcd_commercial_2.work != None
        assert abcd_commercial.work != abcd_commercial_2.work

        # Finally, let's test that nothing happens if you call
        # calculate_work() on a self-consistent situation.
        open_access_work = abcd_open_access.work
        eq_((open_access_work, False), abcd_open_access.calculate_work())

        commercial_work = abcd_commercial.work
        eq_((commercial_work, False), abcd_commercial.calculate_work())

    def test_calculate_work_fixes_incorrectly_grouped_books(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.open_access = True
        book.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains an
        # open-access _audiobook_ of "abcd".
        edition, audiobook = self._edition(with_license_pool=True)
        audiobook.open_access = True
        audiobook.presentation_edition.medium=Edition.AUDIO_MEDIUM
        audiobook.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(audiobook)

        # And the Work _also_ contains an open-access book of "abcd"
        # in a different language.
        edition, spanish = self._edition(with_license_pool=True)
        spanish.open_access = True
        spanish.presentation_edition.language='spa'
        spanish.presentation_edition.permanent_work_id = "abcd"
        work.license_pools.append(spanish)

        def mock_pwid(debug=False):
            return "abcd"
        for lp in [book, audiobook, spanish]:
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We can fix this by calling calculate_work() on one of the
        # LicensePools.
        work_after, is_new = book.calculate_work()
        eq_(work_after, work)
        eq_(False, is_new)

        # The LicensePool we called calculate_work() on gets to stay
        # in the Work, but the other one has been kicked out and
        # given its own work.
        eq_(book.work, work)
        assert audiobook.work != work

        # The audiobook LicensePool has been given a Work of its own.
        eq_([audiobook], audiobook.work.license_pools)

        # The book has been given the Work that will be used for all
        # book-type LicensePools for that title going forward.
        expect_book_work, book_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'eng'
            )
        )
        eq_(expect_book_work, book.work)

        # The audiobook has been given the Work that will be used for
        # all audiobook-type LicensePools for that title going
        # forward.
        expect_audiobook_work, audiobook_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.AUDIO_MEDIUM, 'eng'
            )
        )
        eq_(expect_audiobook_work, audiobook.work)

        # The Spanish book has been given the Work that will be used
        # for all Spanish LicensePools for that title going forward.
        expect_spanish_work, spanish_work_is_new = (
            Work.open_access_for_permanent_work_id(
                self._db, "abcd", Edition.BOOK_MEDIUM, 'spa'
            )
        )
        eq_(expect_spanish_work, spanish.work)
        eq_('spa', expect_spanish_work.language)


    def test_calculate_work_detaches_licensepool_with_no_title(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # But the LicensePool's presentation edition has lost its
        # title.
        book.presentation_edition.title = None

        # Calling calculate_work() on the LicensePool will detach the
        # book from its work, since a book with no title cannot have
        # an associated Work.
        work_after, is_new = book.calculate_work()
        eq_(None, work_after)
        eq_([], work.license_pools)

    def test_calculate_work_detaches_licensepool_with_no_pwid(self):
        # Here's a Work with an open-access edition of "abcd".
        work = self._work(with_license_pool=True)
        [book] = work.license_pools
        book.presentation_edition.permanent_work_id = "abcd"

        # Due to a earlier error, the Work also contains an edition
        # with no title or author, and thus no permanent work ID.
        edition, no_title = self._edition(with_license_pool=True)

        no_title.presentation_edition.title=None
        no_title.presentation_edition.author=None
        no_title.presentation_edition.permanent_work_id = None
        work.license_pools.append(no_title)

        # Calling calculate_work() on the functional LicensePool will
        # split off the bad one.
        work_after, is_new = book.calculate_work()
        eq_([book], work.license_pools)
        eq_(None, no_title.work)
        eq_(None, no_title.presentation_edition.work)

        # calculate_work() on the bad LicensePool will split it off from
        # the good one.
        work.license_pools.append(no_title)
        work_after_2, is_new = no_title.calculate_work()
        eq_(None, work_after_2)
        eq_([book], work.license_pools)

        # The same thing happens if the bad LicensePool has no
        # presentation edition at all.
        work.license_pools.append(no_title)
        no_title.presentation_edition = None
        work_after, is_new = book.calculate_work()
        eq_([book], work.license_pools)

        work.license_pools.append(no_title)
        work_after, is_new = no_title.calculate_work()
        eq_([book], work.license_pools)


    def test_pwids(self):
        """Test the property that finds all permanent work IDs
        associated with a Work.
        """
        # Create a (bad) situation in which LicensePools associated
        # with two different PWIDs are associated with the same work.
        work = self._work(with_license_pool=True)
        [lp1] = work.license_pools
        eq_(set([lp1.presentation_edition.permanent_work_id]),
            work.pwids)
        edition, lp2 = self._edition(with_license_pool=True)
        work.license_pools.append(lp2)

        # Work.pwids finds both PWIDs.
        eq_(set([lp1.presentation_edition.permanent_work_id,
                 lp2.presentation_edition.permanent_work_id]),
            work.pwids)

    def test_open_access_for_permanent_work_id_no_licensepools(self):
        # There are no LicensePools, which short-circuilts
        # open_access_for_permanent_work_id.
        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "No such permanent work ID", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        # Now it works.
        w = self._work(
            language="eng", with_license_pool=True,
            with_open_access_download=True
        )
        w.presentation_edition.permanent_work_id = "permid"
        eq_(
            (w, False), Work.open_access_for_permanent_work_id(
                self._db, "permid", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        # But the language, medium, and permanent ID must all match.
        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "permid", Edition.BOOK_MEDIUM,
                "spa"
            )
        )

        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "differentid", Edition.BOOK_MEDIUM,
                "eng"
            )
        )

        eq_(
            (None, False), Work.open_access_for_permanent_work_id(
                self._db, "differentid", Edition.AUDIO_MEDIUM,
                "eng"
            )
        )

    def test_open_access_for_permanent_work_id(self):
        # Two different works full of open-access license pools.
        w1 = self._work(with_license_pool=True, with_open_access_download=True)

        w2 = self._work(with_license_pool=True, with_open_access_download=True)

        [lp1] = w1.license_pools
        [lp2] = w2.license_pools

        # Work #2 has two different license pools grouped
        # together. Work #1 only has one.
        edition, lp3 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        w2.license_pools.append(lp3)

        # Due to an error, it turns out both Works are providing the
        # exact same book.
        def mock_pwid(debug=False):
            return "abcd"
        for lp in [lp1, lp2, lp3]:
            lp.presentation_edition.permanent_work_id="abcd"
            lp.presentation_edition.calculate_permanent_work_id = mock_pwid

        # We've also got Work #3, which provides a commercial license
        # for that book.
        w3 = self._work(with_license_pool=True)
        w3_pool = w3.license_pools[0]
        w3_pool.presentation_edition.permanent_work_id="abcd"
        w3_pool.open_access = False

        # Work.open_access_for_permanent_work_id can resolve this problem.
        work, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )

        # Work #3 still exists and its license pool was not affected.
        eq_([w3], self._db.query(Work).filter(Work.id==w3.id).all())
        eq_(w3, w3_pool.work)

        # But the other three license pools now have the same work.
        eq_(work, lp1.work)
        eq_(work, lp2.work)
        eq_(work, lp3.work)

        # Because work #2 had two license pools, and work #1 only had
        # one, work #1 was merged into work #2, rather than the other
        # way around.
        eq_(w2, work)
        eq_(False, is_new)

        # Work #1 no longer exists.
        eq_([], self._db.query(Work).filter(Work.id==w1.id).all())

        # Calling Work.open_access_for_permanent_work_id again returns the same
        # result.
        _db = self._db
        Work.open_access_for_permanent_work_id(_db, "abcd", Edition.BOOK_MEDIUM, "eng")
        eq_((w2, False), Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        ))

    def test_open_access_for_permanent_work_id_can_create_work(self):

        # Here's a LicensePool with no corresponding Work.
        edition, lp = self._edition(with_license_pool=True)
        lp.open_access = True
        edition.permanent_work_id="abcd"

        # open_access_for_permanent_work_id creates the Work.
        work, is_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, edition.language
        )
        eq_([lp], work.license_pools)
        eq_(True, is_new)

    def test_potential_open_access_works_for_permanent_work_id(self):
        # Test of the _potential_open_access_works_for_permanent_work_id
        # helper method.

        # Here are two editions of the same book with the same PWID.
        title = 'Siddhartha'
        author = ['Herman Hesse']
        e1, lp1 = self._edition(
            data_source_name=DataSource.STANDARD_EBOOKS,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e1.permanent_work_id = "pwid"

        e2, lp2 = self._edition(
            data_source_name=DataSource.GUTENBERG,
            title=title, authors=author, language='eng', with_license_pool=True,
        )
        e2.permanent_work_id = "pwid"

        w1 = Work()
        self._db.add(w1)
        for lp in [lp1, lp2]:
            w1.license_pools.append(lp)
            lp.open_access = True

        def m():
            return Work._potential_open_access_works_for_permanent_work_id(
                self._db, "pwid", Edition.BOOK_MEDIUM, "eng"
            )
        pools, counts = m()

        # Both LicensePools show up in the list of LicensePools that
        # should be grouped together, and both LicensePools are
        # associated with the same Work.
        poolset = set([lp1, lp2])
        eq_(poolset, pools)
        eq_({w1 : 2}, counts)

        # Since the work was just created, it has no presentation
        # edition and thus no language. If the presentation edition
        # were set, the result would be the same.
        w1.presentation_edition = e1
        pools, counts = m()
        eq_(poolset, pools)
        eq_({w1 : 2}, counts)

        # If the Work's presentation edition has information that
        # _conflicts_ with the information passed in to
        # _potential_open_access_works_for_permanent_work_id, the Work
        # does not show up in `counts`, indicating that a new Work
        # should to be created to hold those books.
        bad_pe = self._edition()
        bad_pe.permanent_work_id='pwid'
        w1.presentation_edition = bad_pe

        bad_pe.language = 'fin'
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.language = 'eng'

        bad_pe.medium = Edition.AUDIO_MEDIUM
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.medium = Edition.BOOK_MEDIUM

        bad_pe.permanent_work_id = "Some other ID"
        pools, counts = m()
        eq_(poolset, pools)
        eq_({}, counts)
        bad_pe.permanent_work_id = "pwid"

        w1.presentation_edition = None

        # Now let's see what changes to a LicensePool will cause it
        # not to be eligible in the first place.
        def assert_lp1_missing():
            # A LicensePool that is not eligible will not show up in
            # the set and will not be counted towards the total of eligible
            # LicensePools for its Work.
            pools, counts = m()
            eq_(set([lp2]), pools)
            eq_({w1 : 1}, counts)

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
        self._db.add(w2)
        w2.license_pools.append(lp2)
        pools, counts = m()

        # This work is irrelevant and will not show up at all.
        w3 = Work()
        self._db.add(w3)

        # Both Works have one associated LicensePool, so they have
        # equal claim to being 'the' Work for this work
        # ID/language/medium. The calling code will have to sort it
        # out.
        eq_(poolset, pools)
        eq_({w1: 1, w2: 1}, counts)

    def test_make_exclusive_open_access_for_permanent_work_id(self):
        # Here's a work containing an open-access LicensePool for
        # literary work "abcd".
        work1 = self._work(with_license_pool=True,
                          with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id="abcd"

        # Unfortunately, a commercial LicensePool for the literary
        # work "abcd" has gotten associated with the same work.
        edition, abcd_commercial = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        abcd_commercial.open_access = False
        abcd_commercial.presentation_edition.permanent_work_id="abcd"
        abcd_commercial.work = work1

        # Here's another Work containing an open-access LicensePool
        # for literary work "efgh".
        work2 = self._work(with_license_pool=True,
                          with_open_access_download=True)
        [efgh_1] = work2.license_pools
        efgh_1.presentation_edition.permanent_work_id="efgh"

        # Unfortunately, there's another open-access LicensePool for
        # "efgh", and it's incorrectly associated with the "abcd"
        # work.
        edition, efgh_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        efgh_2.presentation_edition.permanent_work_id = "efgh"
        efgh_2.work = work1

        # Let's fix these problems.
        work1.make_exclusive_open_access_for_permanent_work_id(
            "abcd", Edition.BOOK_MEDIUM, "eng",
        )

        # The open-access "abcd" book is now the only LicensePool
        # associated with work1.
        eq_([abcd_oa], work1.license_pools)

        # Both open-access "efgh" books are now associated with work2.
        eq_(set([efgh_1, efgh_2]), set(work2.license_pools))

        # A third work has been created for the commercial edition of "abcd".
        assert abcd_commercial.work not in (work1, work2)

    def test_make_exclusive_open_access_for_null_permanent_work_id(self):
        # Here's a LicensePool that, due to a previous error, has
        # a null PWID in its presentation edition.
        work = self._work(with_license_pool=True,
                          with_open_access_download=True)
        [null1] = work.license_pools
        null1.presentation_edition.title = None
        null1.presentation_edition.sort_author = None
        null1.presentation_edition.permanent_work_id = None

        # Here's another LicensePool associated with the same work and
        # with the same problem.
        edition, null2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(null2)

        for pool in work.license_pools:
            pool.presentation_edition.title = None
            pool.presentation_edition.sort_author = None
            pool.presentation_edition.permanent_work_id = None

        work.make_exclusive_open_access_for_permanent_work_id(
            None, Edition.BOOK_MEDIUM, edition.language
        )

        # Since a LicensePool with no PWID cannot have an associated Work,
        # this Work now have no LicensePools at all.
        eq_([], work.license_pools)

        eq_(None, null1.work)
        eq_(None, null2.work)

    def test_merge_into_success(self):
        # Here's a work with an open-access LicensePool.
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id="abcd"

        # Let's give it a WorkGenre and a WorkCoverageRecord.
        genre, ignore = Genre.lookup(self._db, "Fantasy")
        wg, wg_is_new = get_one_or_create(
            self._db, WorkGenre, work=work1, genre=genre
        )
        wcr, wcr_is_new = WorkCoverageRecord.add_for(work1, "test")

        # Here's another work with an open-access LicensePool for the
        # same book.
        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.presentation_edition.permanent_work_id="abcd"

        # Let's merge the first work into the second.
        work1.merge_into(work2)

        # The first work has been deleted, as have its WorkGenre and
        # WorkCoverageRecord.
        eq_([], self._db.query(Work).filter(Work.id==work1.id).all())
        eq_([], self._db.query(WorkGenre).all())
        eq_([], self._db.query(WorkCoverageRecord).filter(
            WorkCoverageRecord.work_id==work1.id).all()
        )

    def test_open_access_for_permanent_work_id_fixes_mismatched_works_incidentally(self):

        # Here's a work with two open-access LicensePools for the book "abcd".
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [abcd_1] = work1.license_pools
        edition, abcd_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work1.license_pools.append(abcd_2)

        # Unfortunately, due to an earlier error, that work also
        # contains a _third_ open-access LicensePool, and this one
        # belongs to a totally separate book, "efgh".
        edition, efgh = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        work1.license_pools.append(efgh)

        # Here's another work with an open-access LicensePool for the
        # book "abcd".
        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [abcd_3] = work2.license_pools

        # Unfortunately, this work also contains an open-access Licensepool
        # for the totally separate book, 'ijkl".
        edition, ijkl = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
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
        abcd_work, abcd_new = Work.open_access_for_permanent_work_id(
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )
        efgh_work, efgh_new = Work.open_access_for_permanent_work_id(
            self._db, "efgh", Edition.BOOK_MEDIUM, "eng"
        )
        ijkl_work, ijkl_new = Work.open_access_for_permanent_work_id(
            self._db, "ijkl", Edition.BOOK_MEDIUM, "eng"
        )

        # We've got three different works here. The 'abcd' work is the
        # old 'abcd' work that had three LicensePools--the other work
        # was merged into it.
        eq_(abcd_1.work, abcd_work)
        assert efgh_work != abcd_work
        assert ijkl_work != abcd_work
        assert ijkl_work != efgh_work

        # The two 'new' works (for efgh and ijkl) are not counted as
        # new because they were created during the first call to
        # Work.open_access_for_permanent_work_id, when those
        # LicensePools were split out of Works where they didn't
        # belong.
        eq_(False, efgh_new)
        eq_(False, ijkl_new)

        eq_([ijkl], ijkl_work.license_pools)
        eq_([efgh], efgh_work.license_pools)
        eq_(3, len(abcd_work.license_pools))

    def test_open_access_for_permanent_work_untangles_tangled_works(self):

        # Here are three works for the books "abcd", "efgh", and "ijkl".
        abcd_work = self._work(with_license_pool=True,
                               with_open_access_download=True)
        [abcd_1] = abcd_work.license_pools

        efgh_work = self._work(with_license_pool=True,
                               with_open_access_download=True)
        [efgh_1] = efgh_work.license_pools

        # Unfortunately, due to an earlier error, the 'abcd' work
        # contains a LicensePool for 'efgh', and the 'efgh' work contains
        # a LicensePool for 'abcd'.
        #
        # (This is pretty much impossible, but bear with me...)

        abcd_edition, abcd_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
        efgh_work.license_pools.append(abcd_2)

        efgh_edition, efgh_2 = self._edition(
            with_license_pool=True, with_open_access_download=True
        )
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
            self._db, "abcd", Edition.BOOK_MEDIUM, "eng"
        )
        eq_(True, is_new)
        eq_(set([abcd_1, abcd_2]), set(abcd_new.license_pools))

        # The old abcd_work now contains only the 'efgh' LicensePool
        # that didn't fit.
        eq_([efgh_2], abcd_work.license_pools)

        # We now have two works with 'efgh' LicensePools: abcd_work
        # and efgh_work. Calling
        # Work.open_access_for_permanent_work_id on 'efgh' will
        # consolidate the two LicensePools into one of the Works
        # (which one is nondeterministic).
        efgh_new, is_new = Work.open_access_for_permanent_work_id(
            self._db, "efgh", Edition.BOOK_MEDIUM, "eng"
        )
        eq_(False, is_new)
        eq_(set([efgh_1, efgh_2]), set(efgh_new.license_pools))
        assert efgh_new in (abcd_work, efgh_work)

        # The Work that was not chosen for consolidation now has no
        # LicensePools.
        if efgh_new is abcd_work:
            other = efgh_work
        else:
            other = abcd_work
        eq_([], other.license_pools)

    def test_merge_into_raises_exception_if_grouping_rules_violated(self):
        # Here's a work with an open-access LicensePool.
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp1] = work1.license_pools
        lp1.presentation_edition.permanent_work_id="abcd"

        # Here's another work with a commercial LicensePool for the
        # same book.
        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [lp2] = work2.license_pools
        lp2.open_access = False
        lp2.presentation_edition.permanent_work_id="abcd"

        # The works cannot be merged.
        assert_raises_regexp(
            ValueError,
            "Refusing to merge .* into .* because it would put an open-access LicensePool into the same work as a non-open-access LicensePool.",
            work1.merge_into, work2,
        )

    def test_merge_into_raises_exception_if_pwids_differ(self):
        work1 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [abcd_oa] = work1.license_pools
        abcd_oa.presentation_edition.permanent_work_id="abcd"

        work2 = self._work(with_license_pool=True,
                           with_open_access_download=True)
        [efgh_oa] = work2.license_pools
        efgh_oa.presentation_edition.permanent_work_id="efgh"

        assert_raises_regexp(
            ValueError,
            "Refusing to merge .* into .* because permanent work IDs don't match: abcd vs. efgh",
            work1.merge_into,
            work2
        )

    def test_licensepool_without_identifier_gets_no_work(self):
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools
        lp.identifier = None

        # Even if the LicensePool had a work before, it gets removed.
        eq_((None, False), lp.calculate_work())
        eq_(None, lp.work)

    def test_licensepool_without_presentation_edition_gets_no_work(self):
        work = self._work(with_license_pool=True)
        [lp] = work.license_pools

        # This LicensePool has no presentation edition and no way of
        # getting one.
        lp.presentation_edition = None
        lp.identifier.primarily_identifies = []

        # Even if the LicensePool had a work before, it gets removed.
        eq_((None, False), lp.calculate_work())
        eq_(None, lp.work)
