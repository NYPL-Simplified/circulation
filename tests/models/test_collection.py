# encoding: utf-8
from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    eq_,
    set_trace,
)
import base64
import datetime
import json
from .. import DatabaseTest
from ...config import Configuration
from ...model import (
    create,
    get_one_or_create,
)
from ...model.coverage import (
    CoverageRecord,
    WorkCoverageRecord,
)
from ...model.collection import Collection
from ...model.configuration import (
    ConfigurationSetting,
    ExternalIntegration,
)
from ...model.customlist import CustomList
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.hasfulltablecache import HasFullTableCache
from ...model.identifier import Identifier
from ...model.work import MaterializedWorkWithGenre as work_model

class TestCollection(DatabaseTest):

    def setup(self):
        super(TestCollection, self).setup()
        self.collection = self._collection(
            name="test collection", protocol=ExternalIntegration.OVERDRIVE
        )

    def test_by_name_and_protocol(self):
        name = "A name"
        protocol = ExternalIntegration.OVERDRIVE
        key = (name, protocol)

        # Cache is empty.
        eq_(HasFullTableCache.RESET, Collection._cache)

        collection1, is_new = Collection.by_name_and_protocol(
            self._db, name, ExternalIntegration.OVERDRIVE
        )
        eq_(True, is_new)

        # Cache was populated and then reset because we created a new
        # Collection.
        eq_(HasFullTableCache.RESET, Collection._cache)

        collection2, is_new = Collection.by_name_and_protocol(
            self._db, name, ExternalIntegration.OVERDRIVE
        )
        eq_(collection1, collection2)
        eq_(False, is_new)

        # This time the cache was not reset after being populated.
        eq_(collection1, Collection._cache[key])

        # You'll get an exception if you look up an existing name
        # but the protocol doesn't match.
        assert_raises_regexp(
            ValueError,
            'Collection "A name" does not use protocol "Bibliotheca".',
            Collection.by_name_and_protocol,
            self._db, name, ExternalIntegration.BIBLIOTHECA
        )

    def test_by_protocol(self):
        """Verify the ability to find all collections that implement
        a certain protocol.
        """
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA
        c1 = self._collection(self._str, protocol=overdrive)
        c1.parent = self.collection
        c2 = self._collection(self._str, protocol=bibliotheca)
        eq_(set([self.collection, c1]),
            set(Collection.by_protocol(self._db, overdrive).all()))
        eq_(([c2]),
            Collection.by_protocol(self._db, bibliotheca).all())
        eq_(set([self.collection, c1, c2]),
            set(Collection.by_protocol(self._db, None).all()))

    def test_by_datasource(self):
        """Collections can be found by their associated DataSource"""
        c1 = self._collection(data_source_name=DataSource.GUTENBERG)
        c2 = self._collection(data_source_name=DataSource.OVERDRIVE)

        # Using the DataSource name
        eq_(set([c1]),
            set(Collection.by_datasource(self._db, DataSource.GUTENBERG).all()))

        # Using the DataSource itself
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        eq_(set([c2]),
            set(Collection.by_datasource(self._db, overdrive).all()))

    def test_parents(self):
        # Collections can return all their parents recursively.
        c1 = self._collection()
        eq_([], list(c1.parents))

        c2 = self._collection()
        c2.parent_id = c1.id
        eq_([c1], list(c2.parents))

        c3 = self._collection()
        c3.parent_id = c2.id
        eq_([c2, c1], list(c3.parents))

    def test_create_external_integration(self):
        # A newly created Collection has no associated ExternalIntegration.
        collection, ignore = get_one_or_create(
            self._db, Collection, name=self._str
        )
        eq_(None, collection.external_integration_id)
        assert_raises_regexp(
            ValueError,
            "No known external integration for collection",
            getattr, collection, 'external_integration'
        )

        # We can create one with create_external_integration().
        overdrive = ExternalIntegration.OVERDRIVE
        integration = collection.create_external_integration(protocol=overdrive)
        eq_(integration.id, collection.external_integration_id)
        eq_(overdrive, integration.protocol)

        # If we call create_external_integration() again we get the same
        # ExternalIntegration as before.
        integration2 = collection.create_external_integration(protocol=overdrive)
        eq_(integration, integration2)


        # If we try to initialize an ExternalIntegration with a different
        # protocol, we get an error.
        assert_raises_regexp(
            ValueError,
            "Located ExternalIntegration, but its protocol \(Overdrive\) does not match desired protocol \(blah\).",
            collection.create_external_integration,
            protocol="blah"
        )

    def test_change_protocol(self):
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA

        # Create a parent and a child collection, both with
        # protocol=Overdrive.
        child = self._collection(self._str, protocol=overdrive)
        child.parent = self.collection

        # We can't change the child's protocol to a value that contradicts
        # the parent's protocol.
        child.protocol = overdrive
        def set_child_protocol():
            child.protocol = bibliotheca
        assert_raises_regexp(
            ValueError,
            "Proposed new protocol \(Bibliotheca\) contradicts parent collection's protocol \(Overdrive\).",
            set_child_protocol
        )

        # If we change the parent's protocol, the children are
        # automatically updated.
        self.collection.protocol = bibliotheca
        eq_(bibliotheca, child.protocol)

    def test_data_source(self):
        opds = self._collection()
        bibliotheca = self._collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # The rote data_source is returned for the obvious collection.
        eq_(DataSource.BIBLIOTHECA, bibliotheca.data_source.name)

        # The less obvious OPDS collection doesn't have a DataSource.
        eq_(None, opds.data_source)

        # Trying to change the Bibliotheca collection's data_source does nothing.
        bibliotheca.data_source = DataSource.AXIS_360
        eq_(DataSource.BIBLIOTHECA, bibliotheca.data_source.name)

        # Trying to change the opds collection's data_source is fine.
        opds.data_source = DataSource.PLYMPTON
        eq_(DataSource.PLYMPTON, opds.data_source.name)

        # Resetting it to something else is fine.
        opds.data_source = DataSource.OA_CONTENT_SERVER
        eq_(DataSource.OA_CONTENT_SERVER, opds.data_source.name)

        # Resetting it to None is fine.
        opds.data_source = None
        eq_(None, opds.data_source)

    def test_default_loan_period(self):
        library = self._default_library
        library.collections.append(self.collection)

        ebook = Edition.BOOK_MEDIUM
        audio = Edition.AUDIO_MEDIUM

        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(library, ebook)
        )

        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(library, audio)
        )

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(library, ebook).value = 604
        eq_(604, self.collection.default_loan_period(library))
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(library, audio)
        )

        self.collection.default_loan_period_setting(library, audio).value = 606
        eq_(606, self.collection.default_loan_period(library, audio))

        # Given an integration client rather than a library, use
        # a sitewide integration setting rather than a library-specific
        # setting.
        client = self._integration_client()

        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(client, ebook)
        )

        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(client, audio)
        )

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(client, ebook).value = 347
        eq_(347, self.collection.default_loan_period(client))
        eq_(
            Collection.STANDARD_DEFAULT_LOAN_PERIOD,
            self.collection.default_loan_period(client, audio)
        )

        self.collection.default_loan_period_setting(client, audio).value = 349
        eq_(349, self.collection.default_loan_period(client, audio))

        # The same value is used for other clients.
        client2 = self._integration_client()
        eq_(347, self.collection.default_loan_period(client))
        eq_(349, self.collection.default_loan_period(client, audio))

    def test_default_reservation_period(self):
        library = self._default_library
        # The default when no value is set.
        eq_(
            Collection.STANDARD_DEFAULT_RESERVATION_PERIOD,
            self.collection.default_reservation_period
        )

        # Set a value, and it's used.
        self.collection.default_reservation_period = 601
        eq_(601, self.collection.default_reservation_period)

        # The underlying value is controlled by a ConfigurationSetting.
        self.collection.external_integration.setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY
        ).value = 954
        eq_(954, self.collection.default_reservation_period)

    def test_pools_with_no_delivery_mechanisms(self):
        # Collection.pools_with_no_delivery_mechanisms returns a query
        # that finds all LicensePools in the Collection which are
        # missing delivery mechanisms.
        collection1 = self._default_collection
        collection2 = self._collection()
        pool1 = self._licensepool(None, collection=collection1)
        pool2 = self._licensepool(None, collection=collection2)

        # At first, the query matches nothing, because
        # all LicensePools have delivery mechanisms.
        qu = collection1.pools_with_no_delivery_mechanisms
        eq_([], qu.all())

        # Let's delete all the delivery mechanisms.
        for pool in (pool1, pool2):
            [self._db.delete(x) for x in pool.delivery_mechanisms]

        # Now the query matches LicensePools if they are in the
        # appropriate collection.
        eq_([pool1], qu.all())
        eq_([pool2], collection2.pools_with_no_delivery_mechanisms.all())

    def test_explain(self):
        """Test that Collection.explain gives all relevant information
        about a Collection.
        """
        library = self._default_library
        library.name="The only library"
        library.short_name = "only one"
        library.collections.append(self.collection)

        self.collection.external_account_id = "id"
        self.collection.external_integration.url = "url"
        self.collection.external_integration.username = "username"
        self.collection.external_integration.password = "password"
        setting = self.collection.external_integration.set_setting("setting", "value")

        data = self.collection.explain()
        eq_(['Name: "test collection"',
             'Protocol: "Overdrive"',
             'Used by library: "only one"',
             'External account ID: "id"',
             'Setting "setting": "value"',
             'Setting "url": "url"',
             'Setting "username": "username"',
        ],
            data
        )

        with_password = self.collection.explain(include_secrets=True)
        assert 'Setting "password": "password"' in with_password

        # If the collection is the child of another collection,
        # its parent is mentioned.
        child = Collection(
            name="Child", parent=self.collection, external_account_id="id2"
        )
        child.create_external_integration(
            protocol=ExternalIntegration.OVERDRIVE
        )
        data = child.explain()
        eq_(['Name: "Child"',
             'Parent: test collection',
             'Protocol: "Overdrive"',
             'External account ID: "id2"'],
            data
        )

    def test_metadata_identifier(self):
        # If the collection doesn't have its unique identifier, an error
        # is raised.
        assert_raises(ValueError, getattr, self.collection, 'metadata_identifier')

        def build_expected(protocol, unique_id):
            encoded = [base64.b64encode(unicode(value), '-_')
                       for value in [protocol, unique_id]]
            return base64.b64encode(':'.join(encoded), '-_')

        # With a unique identifier, we get back the expected identifier.
        self.collection.external_account_id = 'id'
        expected = build_expected(ExternalIntegration.OVERDRIVE, 'id')
        eq_(expected, self.collection.metadata_identifier)

        # If there's a parent, its unique id is incorporated into the result.
        child = self._collection(
            name="Child", protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=self._url
        )
        child.parent = self.collection
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, 'id+%s' % child.external_account_id)
        eq_(expected, child.metadata_identifier)

        # If it's an OPDS_IMPORT collection with a url external_account_id,
        # closing '/' marks are removed.
        opds = self._collection(
            name='OPDS', protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id=(self._url+'/')
        )
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, opds.external_account_id[:-1])
        eq_(expected, opds.metadata_identifier)

    def test_from_metadata_identifier(self):
        # If a mirrored collection doesn't exist, it is created.
        self.collection.external_account_id = 'id'
        mirror_collection, is_new = Collection.from_metadata_identifier(
            self._db, self.collection.metadata_identifier
        )
        eq_(True, is_new)
        eq_(self.collection.metadata_identifier, mirror_collection.name)
        eq_(self.collection.protocol, mirror_collection.protocol)
        # Because this isn't an OPDS collection, no account details are held.
        eq_(None, mirror_collection.external_account_id)

        # If the mirrored collection already exists, it is returned.
        collection = self._collection(external_account_id=self._url)
        mirror_collection = create(
            self._db, Collection,
            name=collection.metadata_identifier,
        )[0]
        mirror_collection.create_external_integration(collection.protocol)
        # Confirm that there's no external_account_id and no DataSource.
        eq_(None, mirror_collection.external_account_id)
        eq_(None, mirror_collection.data_source)

        source = DataSource.lookup(self._db, DataSource.OA_CONTENT_SERVER)
        result, is_new = Collection.from_metadata_identifier(
            self._db, collection.metadata_identifier, data_source=source
        )
        eq_(False, is_new)
        eq_(mirror_collection, result)
        # The external_account_id and data_source have been set now.
        eq_(collection.external_account_id, mirror_collection.external_account_id)
        eq_(source, mirror_collection.data_source)

    def test_catalog_identifier(self):
        """#catalog_identifier associates an identifier with the catalog"""
        identifier = self._identifier()
        self.collection.catalog_identifier(identifier)

        eq_(1, len(self.collection.catalog))
        eq_(identifier, self.collection.catalog[0])

    def test_catalog_identifiers(self):
        """#catalog_identifier associates multiple identifiers with a catalog"""
        i1 = self._identifier()
        i2 = self._identifier()
        i3 = self._identifier()

        # One of the identifiers is already in the catalog.
        self.collection.catalog_identifier(i3)

        self.collection.catalog_identifiers([i1, i2, i3])

        # Now all three identifiers are in the catalog.
        assert sorted([i1, i2, i3]) == sorted(self.collection.catalog)

    def test_unresolved_catalog(self):
        # A regular schmegular identifier: untouched, pure.
        pure_id = self._identifier()

        # A 'resolved' identifier that doesn't have a work yet.
        # (This isn't supposed to happen, but jic.)
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        operation = 'test-thyself'
        resolved_id = self._identifier()
        self._coverage_record(
            resolved_id, source, operation=operation,
            status=CoverageRecord.SUCCESS
        )

        # An unresolved identifier--we tried to resolve it, but
        # it all fell apart.
        unresolved_id = self._identifier()
        self._coverage_record(
            unresolved_id, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE
        )

        # An identifier with a Work already.
        id_with_work = self._work().presentation_edition.primary_identifier


        self.collection.catalog_identifiers([
            pure_id, resolved_id, unresolved_id, id_with_work
        ])

        result = self.collection.unresolved_catalog(
            self._db, source.name, operation
        )

        # Only the failing identifier is in the query.
        eq_([unresolved_id], result.all())

    def test_works_updated_since(self):
        w1 = self._work(with_license_pool=True)
        w2 = self._work(with_license_pool=True)
        w3 = self._work(with_license_pool=True)

        # An empty catalog returns nothing.
        timestamp = datetime.datetime.utcnow()
        eq_([], self.collection.works_updated_since(self._db, timestamp).all())

        self.collection.catalog_identifier(w1.license_pools[0].identifier)
        self.collection.catalog_identifier(w2.license_pools[0].identifier)

        # This Work is catalogued in another catalog and will never show up.
        collection2 = self._collection()
        in_other_catalog = self._work(
            with_license_pool=True, collection=collection2
        )
        collection2.catalog_identifier(
            in_other_catalog.license_pools[0].identifier
        )

        # When no timestamp is passed, all works in the catalog are returned.
        # in order of their WorkCoverageRecord timestamp.
        t1, t2 = self.collection.works_updated_since(self._db, None).all()
        eq_(w1, t1[0])
        eq_(w2, t2[0])

        # The return value is a sequence of 5-tuples, each containing
        # (Work, LicensePool, Identifier, WorkCoverageRecord,
        # CollectionIdentifier). This gives the caller all the information
        # necessary to understand the path by which a given Work belongs to
        # a given Collection.
        _w1, lp1, i1 = t1
        [pool] = w1.license_pools
        eq_(pool, lp1)
        eq_(pool.identifier, i1)

        # When a timestamp is passed, only works that have been updated
        # since then will be returned
        [w1_coverage_record] = [
            c for c in w1.coverage_records
            if c.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION
        ]
        w1_coverage_record.timestamp = datetime.datetime.utcnow()
        eq_([w1], [x[0] for x in self.collection.works_updated_since(self._db, timestamp)])

    def test_isbns_updated_since(self):
        i1 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i2 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i3 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)
        i4 = self._identifier(identifier_type=Identifier.ISBN, foreign_id=self._isbn)

        timestamp = datetime.datetime.utcnow()

        # An empty catalog returns nothing..
        eq_([], self.collection.isbns_updated_since(self._db, None).all())

        # Give the ISBNs some coverage.
        content_cafe = DataSource.lookup(self._db, DataSource.CONTENT_CAFE)
        for isbn in [i2, i3, i1]:
            self._coverage_record(isbn, content_cafe)

        # Give one ISBN more than one coverage record.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        i1_oclc_record = self._coverage_record(i1, oclc)

        def assert_isbns(expected, result_query):
            results = [r[0] for r in result_query]
            eq_(expected, results)

        # When no timestamp is given, all ISBNs in the catalog are returned,
        # in order of their CoverageRecord timestamp.
        self.collection.catalog_identifiers([i1, i2])
        updated_isbns = self.collection.isbns_updated_since(self._db, None).all()
        assert_isbns([i2, i1], updated_isbns)

        # That CoverageRecord timestamp is also returned.
        i1_timestamp = updated_isbns[1][1]
        assert isinstance(i1_timestamp, datetime.datetime)
        eq_(i1_oclc_record.timestamp, i1_timestamp)

        # When a timestamp is passed, only works that have been updated since
        # then will be returned.
        timestamp = datetime.datetime.utcnow()
        i1.coverage_records[0].timestamp = datetime.datetime.utcnow()
        updated_isbns = self.collection.isbns_updated_since(self._db, timestamp)
        assert_isbns([i1], updated_isbns)

        # Prepare an ISBN associated with a Work.
        work = self._work(with_license_pool=True)
        work.license_pools[0].identifier = i2
        i2.coverage_records[0].timestamp = datetime.datetime.utcnow()

        # ISBNs that have a Work will be ignored.
        updated_isbns = self.collection.isbns_updated_since(self._db, timestamp)
        assert_isbns([i1], updated_isbns)

    def test_custom_lists(self):
        # A Collection can be associated with one or more CustomLists.
        list1, ignore = get_one_or_create(self._db, CustomList, name=self._str)
        list2, ignore = get_one_or_create(self._db, CustomList, name=self._str)
        self.collection.customlists = [list1, list2]
        eq_(0, len(list1.entries))
        eq_(0, len(list2.entries))

        # When a new pool is added to the collection and its presentation edition is
        # calculated for the first time, it's automatically added to the lists.
        work = self._work(collection=self.collection, with_license_pool=True)
        eq_(1, len(list1.entries))
        eq_(1, len(list2.entries))
        eq_(work, list1.entries[0].work)
        eq_(work, list2.entries[0].work)

        # Now remove it from one of the lists. If its presentation edition changes
        # again or its pool changes works, it's not added back.
        self._db.delete(list1.entries[0])
        self._db.commit()
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))

        pool = work.license_pools[0]
        identifier = pool.identifier
        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        staff_edition, ignore = Edition.for_foreign_id(
            self._db, staff_data_source,
            identifier.type, identifier.identifier)

        staff_edition.title = self._str
        work.calculate_presentation()
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))

        new_work = self._work(collection=self.collection)
        pool.work = new_work
        eq_(0, len(list1.entries))
        eq_(1, len(list2.entries))

    def test_restrict_to_ready_deliverable_works(self):
        """A partial test of restrict_to_ready_deliverable_works.
        This only covers the bit that excludes audiobooks that come
        from certain data sources. The other parts are tested
        indirectly in lane.py, but could use a more explicit test
        here.
        """
        # Create two audiobooks and one ebook.
        overdrive_audiobook = self._work(
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True,
            title="Overdrive Audiobook"
        )
        overdrive_audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM
        overdrive_ebook = self._work(
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True,
            title="Overdrive Ebook",
        )
        rbdigital_audiobook = self._work(
            data_source_name=DataSource.RB_DIGITAL, with_license_pool=True,
            title="RBDigital Audiobook"
        )
        rbdigital_audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM
        # Add them to the materialized view.
        self.add_to_materialized_view(
            [overdrive_audiobook, overdrive_ebook, rbdigital_audiobook]
        )
        def expect(qu, works):
            """Modify the query `qu` by calling
            restrict_to_ready_deliverable_works(), then verify that
            the query returns the works expected by `works`.
            """
            restricted_query = Collection.restrict_to_ready_deliverable_works(
                qu, work_model
            )
            expect_ids = [x.id for x in works]
            actual_ids = [x.works_id for x in restricted_query]
            eq_(set(expect_ids), set(actual_ids))
        # Here's the setting which controls which data sources should
        # have their audiobooks excluded.
        setting = ConfigurationSetting.sitewide(
            self._db, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        )
        qu = self._db.query(work_model).join(work_model.license_pool)
        # When its value is set to the empty list, every work shows
        # up.
        setting.value = json.dumps([])
        expect(qu, [overdrive_ebook, overdrive_audiobook, rbdigital_audiobook])
        # Putting a data source in the list excludes its audiobooks, but
        # not its ebooks.
        setting.value = json.dumps([DataSource.OVERDRIVE])
        expect(qu, [overdrive_ebook, rbdigital_audiobook])
        setting.value = json.dumps(
            [DataSource.OVERDRIVE, DataSource.RB_DIGITAL]
        )
        expect(qu, [overdrive_ebook])


class TestCollectionForMetadataWrangler(DatabaseTest):

    """Tests that requirements to the metadata wrangler's use of Collection
    are being met by continued development on the Collection class.

    If any of these tests are failing, development will be required on the
    metadata wrangler to meet the needs of the new Collection class.
    """

    def test_only_name_is_required(self):
        """Test that only name is a required field on
        the Collection class.
        """
        collection = create(
            self._db, Collection, name='banana'
        )[0]
        eq_(True, isinstance(collection, Collection))
