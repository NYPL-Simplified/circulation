# encoding: utf-8
import pytest
from mock import create_autospec, MagicMock
import datetime
import json

from ...config import Configuration
from ...model import (
    create,
    get_one_or_create,
)
from ...model.coverage import (
    CoverageRecord,
    WorkCoverageRecord,
)
from ...model.circulationevent import CirculationEvent
from ...model.collection import Collection, HasExternalIntegrationPerCollection, CollectionConfigurationStorage
from ...model.complaint import Complaint
from ...model.configuration import (
    ConfigurationSetting,
    ExternalIntegration,
)
from ...model.customlist import CustomList
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.hasfulltablecache import HasFullTableCache
from ...model.identifier import Identifier
from ...model.licensing import (
    Hold,
    Loan,
    License,
    LicensePool,
)
from ...model.work import Work
from ...util.string_helpers import base64
from ...util.datetime_helpers import utc_now


class TestCollection:

    @pytest.fixture(autouse=True)
    def setup_method(self, db_session, create_collection):
        self.collection = create_collection(db_session,
            name="test collection", protocol=ExternalIntegration.OVERDRIVE)

    def test_by_name_and_protocol(self, db_session):
        """
        GIVEN: An ExternalIntegration protocol and a name for a Collection
        WHEN:  Querying a Collection by name and protocol
        THEN:  The correct Collection is returned
        """
        name = "A name"
        protocol = ExternalIntegration.OVERDRIVE
        key = (name, protocol)

        # Cache is empty.
        assert HasFullTableCache.RESET == Collection._cache

        collection1, is_new = Collection.by_name_and_protocol(
            db_session, name, ExternalIntegration.OVERDRIVE
        )
        assert True == is_new

        # Cache was populated and then reset because we created a new
        # Collection.
        assert HasFullTableCache.RESET == Collection._cache

        collection2, is_new = Collection.by_name_and_protocol(
            db_session, name, ExternalIntegration.OVERDRIVE
        )
        assert collection1 == collection2
        assert False == is_new

        # This time the cache was not reset after being populated.
        assert collection1 == Collection._cache[key]

        # You'll get an exception if you look up an existing name
        # but the protocol doesn't match.
        with pytest.raises(ValueError) as excinfo:
            Collection.by_name_and_protocol(db_session, name, ExternalIntegration.BIBLIOTHECA)
        assert 'Collection "A name" does not use protocol "Bibliotheca".' in str(excinfo.value)

    def test_by_protocol(self, db_session, create_collection):
        """
        GIVEN: A collection created with an ExternalIntegration protocol
        WHEN:  Querying Collection by protocol
        THEN:  Returns the correct Collection(s)
        """
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA
        c1 = create_collection(db_session, protocol=overdrive)
        c1.parent = self.collection
        c2 = create_collection(db_session, protocol=bibliotheca)

        assert (set([self.collection, c1]) ==
            set(Collection.by_protocol(db_session, overdrive).all()))
        assert (([c2]) ==
            Collection.by_protocol(db_session, bibliotheca).all())
        assert (set([self.collection, c1, c2]) ==
            set(Collection.by_protocol(db_session, None).all()))

        # A collection marked for deletion is filtered out.
        c1.marked_for_deletion = True
        assert ([self.collection] ==
            Collection.by_protocol(db_session, overdrive).all())

    def test_by_datasource(self, db_session, create_collection):
        """
        GIVEN: A Collection associated with a DataSource
        WHEN:  Querying Collection by data source
        THEN:  Returns the correct Collection(s)
        """
        c1 = create_collection(db_session, data_source_name=DataSource.GUTENBERG)
        c2 = create_collection(db_session, data_source_name=DataSource.OVERDRIVE)

        # Using the DataSource name
        assert (set([c1]) ==
            set(Collection.by_datasource(db_session, DataSource.GUTENBERG).all()))

        # Using the DataSource itself
        overdrive = DataSource.lookup(db_session, DataSource.OVERDRIVE)
        assert (set([c2]) ==
            set(Collection.by_datasource(db_session, overdrive).all()))

        # A collection marked for deletion is filtered out.
        c2.marked_for_deletion = True
        assert 0 == Collection.by_datasource(db_session, overdrive).count()

    def test_parents(self, db_session, create_collection):
        """
        GIVEN: Three Collections
        WHEN:  Setting a Collection as a parent
        THEN:  Collections return all their parents recursively 
        """
        c1 = create_collection(db_session)
        assert [] == list(c1.parents)

        c2 = create_collection(db_session)
        c2.parent_id = c1.id
        assert [c1] == list(c2.parents)

        c3 = create_collection(db_session)
        c3.parent_id = c2.id
        assert [c2, c1] == list(c3.parents)

    def test_create_external_integration(self, db_session):
        """
        GIVEN: A Collection and ExternalIntegration
        WHEN:  Creating Collections with and without ExternalIntegrations
        THEN:  The correct ExternalIntegration is created or returned
        """
        # A newly created Collection has no associated ExternalIntegration.
        collection, _ = get_one_or_create(
            db_session, Collection, name="Test Collection"
        )
        assert None == collection.external_integration_id
        with pytest.raises(ValueError) as excinfo:
            getattr(collection, 'external_integration')
        assert "No known external integration for collection" in str(excinfo.value)

        # We can create one with create_external_integration().
        overdrive = ExternalIntegration.OVERDRIVE
        integration = collection.create_external_integration(protocol=overdrive)
        assert integration.id == collection.external_integration_id
        assert overdrive == integration.protocol

        # If we call create_external_integration() again we get the same
        # ExternalIntegration as before.
        integration2 = collection.create_external_integration(protocol=overdrive)
        assert integration == integration2

        # If we try to initialize an ExternalIntegration with a different
        # protocol, we get an error.
        with pytest.raises(ValueError) as excinfo:
            collection.create_external_integration(protocol = "blah")
        assert "Located ExternalIntegration, but its protocol (Overdrive) does not match desired protocol (blah)." \
            in str(excinfo.value)

    def test_unique_account_id(self, db_session, create_collection):
        """
        GIVEN: A Collection associated with a DataSource
        WHEN:  Assigning a Collection to a parent Collection
        THEN:  unique_account_id is a combination of the parent Collection and child Collection
        """
        # Most collections work like this:
        overdrive = create_collection(
            db_session,
            external_account_id="od1", data_source_name=DataSource.OVERDRIVE
        )
        od_child = create_collection(
            db_session,
            external_account_id="odchild", data_source_name=DataSource.OVERDRIVE
        )
        od_child.parent = overdrive

        # The unique account ID of a primary collection is the
        # external account ID.
        assert "od1" == overdrive.unique_account_id

        # For children of those collections, the unique account ID is scoped
        # to the parent collection.
        assert "od1+odchild" == od_child.unique_account_id

        # Enki works a little differently. Enki collections don't have
        # an external account ID, because all Enki collections are
        # identical.
        enki = create_collection(db_session, data_source_name=DataSource.ENKI)

        # So the unique account ID is the name of the data source.
        assert DataSource.ENKI == enki.unique_account_id

        # A (currently hypothetical) library-specific subcollection of
        # the global Enki collection must have an external_account_id,
        # and its name is scoped to the parent collection as usual.
        enki_child = create_collection(
            db_session,
            external_account_id="enkichild", data_source_name=DataSource.ENKI
        )
        enki_child.parent = enki
        assert DataSource.ENKI + "+enkichild" == enki_child.unique_account_id

    def test_change_protocol(self, db_session, create_collection):
        """
        GIVEN: A Collection associated with an ExternalIntegration protocol and a parent Collection
        WHEN:  Changing the child Collection's protocol to contradict the parent's protocol
        THEN:  A ValueError is raised
        """
        overdrive = ExternalIntegration.OVERDRIVE
        bibliotheca = ExternalIntegration.BIBLIOTHECA

        # Create a parent and a child collection, both with
        # protocol=Overdrive.
        child = create_collection(db_session, protocol=overdrive)
        child.parent = self.collection

        # We can't change the child's protocol to a value that contradicts
        # the parent's protocol.
        child.protocol = overdrive
        def set_child_protocol():
            child.protocol = bibliotheca
        with pytest.raises(ValueError) as excinfo:
            set_child_protocol()
        assert "Proposed new protocol (Bibliotheca) contradicts parent collection's protocol (Overdrive)." in str(excinfo.value)

        # If we change the parent's protocol, the children are
        # automatically updated.
        self.collection.protocol = bibliotheca
        assert bibliotheca == child.protocol

    def test_data_source(self, db_session, create_collection):
        """
        GIVEN: Two Collections, one associated with an ExternalIntegration protocol
        WHEN:  Altering the Collection's data_source
        THEN:  The Collection data_source can be updated if it wasn't initially instantiated
        """
        opds = create_collection(db_session)
        bibliotheca = create_collection(db_session, protocol=ExternalIntegration.BIBLIOTHECA)

        # The rote data_source is returned for the obvious collection.
        assert DataSource.BIBLIOTHECA == bibliotheca.data_source.name

        # The less obvious OPDS collection doesn't have a DataSource.
        assert None == opds.data_source

        # Trying to change the Bibliotheca collection's data_source does nothing.
        bibliotheca.data_source = DataSource.AXIS_360
        assert DataSource.BIBLIOTHECA == bibliotheca.data_source.name

        # Trying to change the opds collection's data_source is fine.
        opds.data_source = DataSource.PLYMPTON
        assert DataSource.PLYMPTON == opds.data_source.name

        # Resetting it to something else is fine.
        opds.data_source = DataSource.OA_CONTENT_SERVER
        assert DataSource.OA_CONTENT_SERVER == opds.data_source.name

        # Resetting it to None is fine.
        opds.data_source = None
        assert None == opds.data_source

    def test_default_loan_period(self, db_session, create_library, create_integration_client):
        """
        GIVEN: A Collection associated with a Library
        WHEN:  Checking the default loan period by Edition type
        THEN:  Returns the correct loan period
        """
        library = create_library(db_session)
        library.collections.append(self.collection)

        ebook = Edition.BOOK_MEDIUM
        audio = Edition.AUDIO_MEDIUM

        # The default when no value is set.
        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD ==
            self.collection.default_loan_period(library, ebook))

        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD ==
            self.collection.default_loan_period(library, audio))

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(library, ebook).value = 604
        assert 604 == self.collection.default_loan_period(library)
        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD ==
            self.collection.default_loan_period(library, audio))

        self.collection.default_loan_period_setting(library, audio).value = 606
        assert 606 == self.collection.default_loan_period(library, audio)

        # Given an integration client rather than a library, use
        # a sitewide integration setting rather than a library-specific
        # setting.
        client = create_integration_client(db_session)

        # The default when no value is set.
        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD ==
            self.collection.default_loan_period(client, ebook))

        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD ==
            self.collection.default_loan_period(client, audio))

        # Set a value, and it's used.
        self.collection.default_loan_period_setting(client, ebook).value = 347
        assert 347 == self.collection.default_loan_period(client)
        assert (
            Collection.STANDARD_DEFAULT_LOAN_PERIOD ==
            self.collection.default_loan_period(client, audio))

        self.collection.default_loan_period_setting(client, audio).value = 349
        assert 349 == self.collection.default_loan_period(client, audio)

        # The same value is used for other clients.
        client2 = create_integration_client(db_session)
        assert 347 == self.collection.default_loan_period(client)
        assert 349 == self.collection.default_loan_period(client, audio)

    def test_default_reservation_period(self, db_session, create_library):
        """
        GIVEN: A Collection
        WHEN:  Checking the default reservation period
        THEN:  Returns the correct default reservation period
        """
        # The default when no value is set.
        assert (
            Collection.STANDARD_DEFAULT_RESERVATION_PERIOD ==
            self.collection.default_reservation_period)

        # Set a value, and it's used.
        self.collection.default_reservation_period = 601
        assert 601 == self.collection.default_reservation_period

        # The underlying value is controlled by a ConfigurationSetting.
        self.collection.external_integration.setting(
            Collection.DEFAULT_RESERVATION_PERIOD_KEY
        ).value = 954
        assert 954 == self.collection.default_reservation_period

    def test_pools_with_no_delivery_mechanisms(self, db_session, create_collection, create_edition, create_licensepool):
        """
        GIVEN: Two LicensePool associated with a Collection
        WHEN:  Deleting the LicensePool delivery mechanism
        THEN:  Returns the correct LicensePool for collections with no delivery mechanism
        """
        # Collection.pools_with_no_delivery_mechanisms returns a query
        # that finds all LicensePools in the Collection which are
        # missing delivery mechanisms.
        collection1 = create_collection(db_session)
        collection2 = create_collection(db_session)
        edition = create_edition(db_session)
        pool1 = create_licensepool(db_session, edition=edition, collection=collection1)
        pool2 = create_licensepool(db_session, edition=edition, collection=collection2)

        # At first, the query matches nothing, because
        # all LicensePools have delivery mechanisms.
        qu = collection1.pools_with_no_delivery_mechanisms
        assert [] == qu.all()

        # Let's delete all the delivery mechanisms.
        for pool in (pool1, pool2):
            [db_session.delete(x) for x in pool.delivery_mechanisms]

        # Now the query matches LicensePools if they are in the
        # appropriate collection.
        assert [pool1] == qu.all()
        assert [pool2] == collection2.pools_with_no_delivery_mechanisms.all()

    def test_explain(self, db_session, create_library):
        """
        GIVEN: A Collection
        WHEN:  Calling Collection.explain
        THEN:  Returns all relevant information about a Collection
        """
        library = create_library(db_session)
        library.name="The only library"
        library.short_name = "only one"
        library.collections.append(self.collection)

        self.collection.external_account_id = "id"
        self.collection.external_integration.url = "url"
        self.collection.external_integration.username = "username"
        self.collection.external_integration.password = "password"
        setting = self.collection.external_integration.set_setting("setting", "value")

        data = self.collection.explain()
        assert (['Name: "test collection"',
             'Protocol: "Overdrive"',
             'Used by library: "only one"',
             'External account ID: "id"',
             'Setting "setting": "value"',
             'Setting "url": "url"',
             'Setting "username": "username"',
        ] ==
            data)

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
        assert (['Name: "Child"',
             'Parent: test collection',
             'Protocol: "Overdrive"',
             'External account ID: "id2"'] ==
            data)

    def test_metadata_identifier(self, db_session, create_collection):
        """
        GIVEN: A Collection
        WHEN:  Checking the Collection's metadata identifier
        THEN:  Returns the correct metadata identifier
        """
        # If the collection doesn't have its unique identifier, an error
        # is raised.
        pytest.raises(ValueError, getattr, self.collection, 'metadata_identifier')

        def build_expected(protocol, unique_id):
            encode = base64.urlsafe_b64encode
            encoded = [
                encode(value)
                for value in [protocol, unique_id]
            ]
            joined = ':'.join(encoded)
            return encode(joined)

        # With a unique identifier, we get back the expected identifier.
        self.collection.external_account_id = 'id'
        expected = build_expected(ExternalIntegration.OVERDRIVE, 'id')
        assert expected == self.collection.metadata_identifier

        # If there's a parent, its unique id is incorporated into the result.
        child = create_collection(
            db_session,
            name="Child", protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id="http://foo.com"
        )
        child.parent = self.collection
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, 'id+%s' % child.external_account_id)
        assert expected == child.metadata_identifier

        # If it's an OPDS_IMPORT collection with a url external_account_id,
        # closing '/' marks are removed.
        opds = create_collection(
            db_session,
            name='OPDS', protocol=ExternalIntegration.OPDS_IMPORT,
            external_account_id="http://foo.com/"
        )
        expected = build_expected(ExternalIntegration.OPDS_IMPORT, opds.external_account_id[:-1])
        assert expected == opds.metadata_identifier

    def test_from_metadata_identifier(self, db_session, create_collection):
        """
        GIVEN: A Collection
        WHEN:  Querying for Collections via from_metdata_identifier
        THEN:  Creates or returns a Collection
        """

        data_source = "New data source"

        # A ValueError results if we try to look up using an invalid
        # identifier.
        with pytest.raises(ValueError) as excinfo:
            Collection.from_metadata_identifier(db_session, "not a real identifier", data_source = data_source)
        assert "Metadata identifier 'not a real identifier' is invalid: Incorrect padding" in str(excinfo.value)

        # Of if we pass in the empty string.
        with pytest.raises(ValueError) as excinfo:
            Collection.from_metadata_identifier(db_session, "", data_source = data_source)
        assert "No metadata identifier provided" in str(excinfo.value)

        # No new data source was created.
        def new_data_source():
            return DataSource.lookup(db_session, data_source)
        assert None == new_data_source()

        # If a mirrored collection doesn't exist, it is created.
        self.collection.external_account_id = 'id'
        mirror_collection, is_new = Collection.from_metadata_identifier(
            db_session, self.collection.metadata_identifier,
            data_source=data_source
        )
        assert True == is_new
        assert self.collection.metadata_identifier == mirror_collection.name
        assert self.collection.protocol == mirror_collection.protocol

        # Because this isn't an OPDS collection, the external account
        # ID is not stored, the data source is the default source for
        # the protocol, and no new data source was created.
        assert None == mirror_collection.external_account_id
        assert DataSource.OVERDRIVE == mirror_collection.data_source.name
        assert None == new_data_source()

        # If the mirrored collection already exists, it is returned.
        collection = create_collection(db_session, external_account_id="http://foo.com")
        mirror_collection = create(
            db_session, Collection,
            name=collection.metadata_identifier
        )[0]
        mirror_collection.create_external_integration(collection.protocol)

        # Confirm that there's no external_account_id and no DataSource.
        # TODO I don't understand why we don't store this information,
        # even if only to keep it in an easy-to-read form.
        assert None == mirror_collection.external_account_id
        assert None == mirror_collection.data_source
        assert None == new_data_source()

        # Now try a lookup of an OPDS Import-type collection.
        result, is_new = Collection.from_metadata_identifier(
            db_session, collection.metadata_identifier, data_source=data_source
        )
        assert False == is_new
        assert mirror_collection == result
        # The external_account_id and data_source have been set now.
        assert collection.external_account_id == mirror_collection.external_account_id

        # A new DataSource object has been created.
        source = new_data_source()
        assert "New data source" == source.name
        assert source == mirror_collection.data_source

    def test_catalog_identifier(self, db_session, create_identifier):
        """
        GIVEN: An Identifier and Collection
        WHEN:  Associating the Identifier with the Collection
        THEN:  The Identifier is inserted into the catalog
        """
        identifier = create_identifier(db_session)
        self.collection.catalog_identifier(identifier)

        assert 1 == len(self.collection.catalog)
        assert identifier == self.collection.catalog[0]

    def test_catalog_identifiers(self, db_session, create_identifier):
        """
        GIVEN: Three Identifiers and a Collection
        WHEN:  Associating the Identifiers with the Collection
        THEN:  The Identifiers are inserted into the catalog
        """
        i1 = create_identifier(db_session)
        i2 = create_identifier(db_session)
        i3 = create_identifier(db_session)

        # One of the identifiers is already in the catalog.
        self.collection.catalog_identifier(i3)

        self.collection.catalog_identifiers([i1, i2, i3])

        # Now all three identifiers are in the catalog.
        assert sorted([i1, i2, i3]) == sorted(self.collection.catalog)

    def test_unresolved_catalog(self, db_session, create_coverage_record, create_identifier, create_work):
        """
        GIVEN: An unresolved Identifier
        WHEN:  Associating the unresolved Identifier with a Collection
        THEN:  The unresolved Identifier is found in the Collection.unresolved_catalog query
        """
        # A regular schmegular identifier: untouched, pure.
        pure_id = create_identifier(db_session)

        # A 'resolved' identifier that doesn't have a work yet.
        # (This isn't supposed to happen, but jic.)
        source = DataSource.lookup(db_session, DataSource.GUTENBERG)
        operation = 'test-thyself'
        resolved_id = create_identifier(db_session)
        # Need to create a coverage record fixture
        create_coverage_record(
            db_session, resolved_id, source, operation=operation, status=CoverageRecord.SUCCESS
        )

        # An unresolved identifier--we tried to resolve it, but
        # it all fell apart.
        unresolved_id = create_identifier(db_session)
        create_coverage_record(
            db_session,
            unresolved_id, source, operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE
        )

        # An identifier with a Work already.
        work = create_work(db_session)
        id_with_work = work.presentation_edition.primary_identifier

        self.collection.catalog_identifiers([
            pure_id, resolved_id, unresolved_id, id_with_work
        ])

        result = self.collection.unresolved_catalog(
            db_session, source.name, operation
        )

        # Only the failing identifier is in the query.
        assert [unresolved_id] == result.all()

    def test_disassociate_library(self, db_session, create_collection, create_library):
        """
        GIVEN: A Collection associated with a Library
        WHEN:  Disassociating the Library from the Collection
        THEN:  The Collection is no longer associated with the Library
        """
        # Here's a Collection.
        collection = create_collection(db_session)
        default_library = create_library(db_session, name="default library", short_name="default")
        integration = collection.create_external_integration(
            ExternalIntegration.OPDS_IMPORT
        )
        integration.goal = ExternalIntegration.LICENSE_GOAL
        if collection not in default_library.collections:
            default_library.collections.append(collection)

        assert default_library in collection.libraries
        other_library = create_library(db_session, name="other library", short_name="other")
        collection.libraries.append(other_library)

        # It has an ExternalIntegration, which has some settings.
        integration = collection.external_integration
        setting1 = integration.set_setting("integration setting", "value2")
        setting2 = ConfigurationSetting.for_library_and_externalintegration(
            db_session, "default_library+integration setting",
            default_library, integration,
        )
        setting2.value = "value2"
        setting3 = ConfigurationSetting.for_library_and_externalintegration(
            db_session, "other_library+integration setting",
            other_library, integration,
        )
        setting3.value = "value3"

        # Now, disassociate one of the libraries from the collection.
        collection.disassociate_library(default_library)

        # It's gone.
        assert default_library not in collection.libraries
        assert collection not in default_library.collections

        # Furthermore, ConfigurationSettings that configure that
        # Library's relationship to this Collection's
        # ExternalIntegration have been deleted.
        all_settings = db_session.query(ConfigurationSetting).all()
        assert setting2 not in all_settings

        # The other library is unaffected.
        assert other_library in collection.libraries
        assert collection in other_library.collections
        assert setting3 in all_settings

        # As is the library-independent configuration of this Collection's
        # ExternalIntegration.
        assert setting1 in all_settings

        # Calling disassociate_library again is a no-op.
        collection.disassociate_library(default_library)
        assert default_library not in collection.libraries

        # If you somehow manage to call disassociate_library on a Collection
        # that has no associated ExternalIntegration, an exception is raised.
        collection.external_integration_id = None
        with pytest.raises(ValueError) as excinfo:
            collection.disassociate_library(other_library)
        assert "No known external integration for collection" in str(excinfo.value)

    def test_licensepools_with_works_updated_since(self, db_session, create_collection, create_work):
        """
        GIVEN: A Collection with Works associated with a LicensePool
        WHEN:  Checking for LicensePools with Works through the Collection
        THEN:  Returns the correct LicensePool and Work
        """
        m = self.collection.licensepools_with_works_updated_since

        # Verify our ability to find LicensePools with works whose
        # OPDS entries were updated since a given time.
        w1 = create_work(db_session, with_license_pool=True)
        w2 = create_work(db_session, with_license_pool=True)
        w3 = create_work(db_session, with_license_pool=True)

        # An empty catalog returns nothing.
        timestamp = utc_now()
        assert [] == m(db_session, timestamp).all()

        self.collection.catalog_identifier(w1.license_pools[0].identifier)
        self.collection.catalog_identifier(w2.license_pools[0].identifier)

        # This Work is catalogued in another catalog and will never show up.
        collection2 = create_collection(db_session)
        in_other_catalog = create_work(
            db_session, with_license_pool=True, collection=collection2
        )
        collection2.catalog_identifier(
            in_other_catalog.license_pools[0].identifier
        )

        # When no timestamp is passed, all LicensePools in the catalog
        # are returned, in order of the WorkCoverageRecord
        # timestamp on the associated Work.
        lp1, lp2 = m(db_session, None).all()
        assert w1 == lp1.work
        assert w2 == lp2.work

        # When a timestamp is passed, only LicensePools whose works
        # have been updated since then will be returned.
        [w1_coverage_record] = [
            c for c in w1.coverage_records
            if c.operation == WorkCoverageRecord.GENERATE_OPDS_OPERATION
        ]
        w1_coverage_record.timestamp = utc_now()
        assert (
            [w1] == [x.work for x in m(db_session, timestamp)])

    def test_isbns_updated_since(self, db_session, create_coverage_record, create_identifier, create_work):
        """
        GIVEN: ISBN Identifiers with an ISBN that are associated with a CoverageRecord
        WHEN:  Updating a Collection that has these as catalog identifiers
        THEN:  Returns the correct Identifiers within a last updated time period
        """
        isbns = [
            "9780674368279", "0636920028468", "9781936460236", "9780316075978"
        ]

        i1 = create_identifier(db_session, identifier_type=Identifier.ISBN, foreign_id=isbns.pop())
        i2 = create_identifier(db_session, identifier_type=Identifier.ISBN, foreign_id=isbns.pop())
        i3 = create_identifier(db_session, identifier_type=Identifier.ISBN, foreign_id=isbns.pop())
        i4 = create_identifier(db_session, identifier_type=Identifier.ISBN, foreign_id=isbns.pop())

        timestamp = utc_now()

        # An empty catalog returns nothing..
        assert [] == self.collection.isbns_updated_since(db_session, None).all()

        # Give the ISBNs some coverage.
        content_cafe = DataSource.lookup(db_session, DataSource.CONTENT_CAFE)
        for isbn in [i2, i3, i1]:
            create_coverage_record(db_session, isbn, content_cafe)

        # Give one ISBN more than one coverage record.
        oclc = DataSource.lookup(db_session, DataSource.OCLC)
        i1_oclc_record = create_coverage_record(db_session, i1, oclc)

        def assert_isbns(expected, result_query):
            results = [r[0] for r in result_query]
            assert expected == results

        # When no timestamp is given, all ISBNs in the catalog are returned,
        # in order of their CoverageRecord timestamp.
        self.collection.catalog_identifiers([i1, i2])
        updated_isbns = self.collection.isbns_updated_since(db_session, None).all()
        assert_isbns([i2, i1], updated_isbns)

        # That CoverageRecord timestamp is also returned.
        i1_timestamp = updated_isbns[1][1]
        assert isinstance(i1_timestamp, datetime.datetime)
        assert i1_oclc_record.timestamp == i1_timestamp

        # When a timestamp is passed, only works that have been updated since
        # then will be returned.
        timestamp = utc_now()
        i1.coverage_records[0].timestamp = utc_now()
        updated_isbns = self.collection.isbns_updated_since(db_session, timestamp)
        assert_isbns([i1], updated_isbns)

        # Prepare an ISBN associated with a Work.
        work = create_work(db_session, with_license_pool=True)
        work.license_pools[0].identifier = i2
        i2.coverage_records[0].timestamp = utc_now()

        # ISBNs that have a Work will be ignored.
        updated_isbns = self.collection.isbns_updated_since(db_session, timestamp)
        assert_isbns([i1], updated_isbns)

    def test_custom_lists(self, db_session, create_work):
        """
        GIVEN: CustomLists associated with a Collection that has Works
        WHEN:  Deleting a Work from a CustomList's entries
        THEN:  The Work is not in the CustomList entries
        """
        # A Collection can be associated with one or more CustomLists.
        list1, _ = get_one_or_create(db_session, CustomList, name="list 1")
        list2, _ = get_one_or_create(db_session, CustomList, name="list 2")
        self.collection.customlists = [list1, list2]
        assert 0 == len(list1.entries)
        assert 0 == len(list2.entries)

        # When a new pool is added to the collection and its presentation edition is
        # calculated for the first time, it's automatically added to the lists.
        work = create_work(db_session, collection=self.collection, with_license_pool=True)
        assert 1 == len(list1.entries)
        assert 1 == len(list2.entries)
        assert work == list1.entries[0].work
        assert work == list2.entries[0].work

        # Now remove it from one of the lists. If its presentation edition changes
        # again or its pool changes works, it's not added back.
        db_session.delete(list1.entries[0])
        db_session.commit()
        assert 0 == len(list1.entries)
        assert 1 == len(list2.entries)

        pool = work.license_pools[0]
        identifier = pool.identifier
        staff_data_source = DataSource.lookup(db_session, DataSource.LIBRARY_STAFF)
        staff_edition, _ = Edition.for_foreign_id(
            db_session, staff_data_source,
            identifier.type, identifier.identifier)

        staff_edition.title = "Staff Edition Title"
        work.calculate_presentation()
        assert 0 == len(list1.entries)
        assert 1 == len(list2.entries)

        new_work = create_work(db_session, collection=self.collection)
        pool.work = new_work
        assert 0 == len(list1.entries)
        assert 1 == len(list2.entries)

    def test_restrict_to_ready_deliverable_works(self, db_session, create_work):
        """
        GIVEN: A number of Works with some that are AudioBooks
        WHEN: Querying the Work table
        THEN: Returns Works based on filter values
        """
        """A partial test of restrict_to_ready_deliverable_works.

        This test covers the following cases:
        1. The bit that excludes audiobooks from certain data sources.
        2. Makes sure that self-hosted books and books with unlimited access are not get filtered out that come.

        The other cases are tested indirectly in lane.py, but could use a more explicit test here.
        """
        # Create two audiobooks and one ebook.
        overdrive_audiobook = create_work(
            db_session,
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True,
            title="Overdrive Audiobook"
        )
        overdrive_audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM
        overdrive_ebook = create_work(
            db_session,
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True,
            title="Overdrive Ebook",
        )
        rbdigital_audiobook = create_work(
            db_session,
            data_source_name=DataSource.RB_DIGITAL, with_license_pool=True,
            title="RBDigital Audiobook"
        )
        rbdigital_audiobook.presentation_edition.medium = Edition.AUDIO_MEDIUM

        DataSource.lookup(db_session, DataSource.LCP, autocreate=True)
        self_hosted_lcp_book = create_work(
            db_session,
            data_source_name=DataSource.LCP,
            title="Self-hosted LCP book",
            with_license_pool=True,
            self_hosted=True
        )
        unlimited_access_book = create_work(
            db_session,
            data_source_name=DataSource.LCP,
            title="Self-hosted LCP book",
            with_license_pool=True,
            unlimited_access=True
        )

        def expect(qu, works):
            """Modify the query `qu` by calling
            restrict_to_ready_deliverable_works(), then verify that
            the query returns the works expected by `works`.
            """
            restricted_query = Collection.restrict_to_ready_deliverable_works(
                qu
            )
            expect_ids = [x.id for x in works]
            actual_ids = [x.id for x in restricted_query]
            assert set(expect_ids) == set(actual_ids)
        # Here's the setting which controls which data sources should
        # have their audiobooks excluded.
        setting = ConfigurationSetting.sitewide(
            db_session, Configuration.EXCLUDED_AUDIO_DATA_SOURCES
        )
        qu = db_session.query(Work).join(Work.license_pools).join(
            Work.presentation_edition
        )
        # When its value is set to the empty list, every work shows
        # up.
        setting.value = json.dumps([])
        expect(
            qu, [overdrive_ebook, overdrive_audiobook, rbdigital_audiobook, self_hosted_lcp_book, unlimited_access_book]
        )
        # Putting a data source in the list excludes its audiobooks, but
        # not its ebooks.
        setting.value = json.dumps([DataSource.OVERDRIVE])
        expect(
            qu,
            [overdrive_ebook, rbdigital_audiobook, self_hosted_lcp_book, unlimited_access_book]
        )
        setting.value = json.dumps(
            [DataSource.OVERDRIVE, DataSource.RB_DIGITAL]
        )
        expect(
            qu, [overdrive_ebook, self_hosted_lcp_book, unlimited_access_book]
        )

    def test_delete(self, db_session, create_collection, create_edition, create_library, 
                    create_license, create_licensepool, create_patron, create_work):
        """
        GIVEN: A Collection that a Patron has loans/holds with a Work through a LicensePool
        WHEN:  The Collection is deleted
        THEN:  Deletion cascades to all relevant related database objects.
        """
        # This collection is doomed.
        library = create_library(db_session)
        collection = create_collection(db_session, name="Doomed Collection")
        integration = collection.create_external_integration(ExternalIntegration.OPDS_IMPORT)
        if collection not in library.collections:
            library.collections.append(collection)

        # It's associated with a library.
        assert library in collection.libraries

        # It has an ExternalIntegration, which has some settings.
        integration = collection.external_integration
        setting1 = integration.set_setting("integration setting", "value2")
        setting2 = ConfigurationSetting.for_library_and_externalintegration(
            db_session, "library+integration setting",
            library, integration,
        )
        setting2.value = "value2"

        # It's got a Work that has a LicensePool, which has a License,
        # which has a loan.
        work = create_work(db_session, title="First Work", with_license_pool=True, collection=collection)
        [pool] = work.license_pools
        license = create_license(db_session, pool)
        patron = create_patron(db_session)
        license.loan_to(patron)

        # The LicensePool also has a hold.
        patron2 = create_patron(db_session)
        pool.on_hold_to(patron2)

        # And a Complaint.
        Complaint.register(
            pool, list(Complaint.VALID_TYPES)[0],
            source=None, detail=None
        )

        # And a CirculationEvent.
        CirculationEvent.log(
            db_session, pool, CirculationEvent.DISTRIBUTOR_TITLE_ADD, 0, 1
        )

        # There's a second Work which has _two_ LicensePools from two
        # different Collections -- the one we're about to delete and
        # another Collection.
        collection2 = create_collection(db_session, name="Second Collection")
        work2 = create_work(db_session, title="Second Work", with_license_pool=True, collection=collection)
        edition = create_edition(db_session)
        pool2 = create_licensepool(db_session, edition, collection=collection2)
        work2.license_pools.append(pool2)

        # Finally, here's a mock ExternalSearchIndex so we can track when
        # Works are removed from the search index.
        class MockExternalSearchIndex(object):
            removed = []
            def remove_work(self, work):
                self.removed.append(work)
        index = MockExternalSearchIndex()

        # delete() will not work on a collection that's not marked for
        # deletion.
        with pytest.raises(Exception) as excinfo:
            collection.delete()
        assert "Cannot delete %s: it is not marked for deletion." % collection.name in str(excinfo.value)

        # Delete the collection.
        collection.marked_for_deletion = True
        collection.delete(search_index=index)

        # Normally this gets done through collection.delete(), but due to the
        # enumeration the work's license pool's deletion isn't committed so it
        # needs to be done here.
        work.delete(index)

        # It's gone.
        assert collection not in db_session.query(Collection).all()

        # The default library now has no collections.
        assert [] == library.collections

        # The deletion of the Collection's sole LicensePool has
        # cascaded to Loan, Hold, Complaint, License, and
        # CirculationEvent.
        assert [] == patron.loans
        assert [] == patron2.holds
        for cls in (Loan, Hold, Complaint, License, CirculationEvent):
            assert [] == db_session.query(cls).all()

        # n.b. Annotations are associated with Identifier, not
        # LicensePool, so they can and should survive the deletion of
        # the Collection in which they were originally created.

        # The first Work has been deleted, since it lost all of its
        # LicensePools.
        assert [work2] == db_session.query(Work).all()

        # The second Work is still around, and it still has the other
        # LicensePool.
        assert [pool2] == work2.license_pools

        # Our search index was told to remove the first work (which no longer
        # has any LicensePools), but not the second.
        assert [work] == index.removed

        # The ExternalIntegration and its settings have been deleted.
        assert integration not in db_session.query(ExternalIntegration).all()
        settings = db_session.query(ConfigurationSetting).all()
        for s in (setting1, setting2):
            assert s not in settings

        # If no search_index is passed into delete() (the default behavior),
        # we try to instantiate the normal ExternalSearchIndex object. Since
        # no search index is configured, this will raise an exception -- but
        # delete() will catch the exception and carry out the delete,
        # without trying to delete any Works from the search index.
        collection2.marked_for_deletion = True
        collection2.delete()

        # We've now deleted every LicensePool created for this test.
        assert 0 == db_session.query(LicensePool).count()
        assert [] == work2.license_pools


class TestCollectionForMetadataWrangler:

    """Tests that requirements to the metadata wrangler's use of Collection
    are being met by continued development on the Collection class.

    If any of these tests are failing, development will be required on the
    metadata wrangler to meet the needs of the new Collection class.
    """

    def test_only_name_is_required(self, db_session):
        """
        GIVEN: Only a name
        WHEN:  Creating a Collection with just the name 
        THEN:  The Collection is created without raising any errors
        """
        """Test that only name is a required field on
        the Collection class.
        """
        collection = create(
            db_session, Collection, name='banana'
        )[0]
        assert True == isinstance(collection, Collection)


class TestCollectionConfigurationStorage:
    def test_load(self, db_session, create_collection):
        """
        GIVEN: A Collection stored with CollectionConfigurationStorage
        WHEN:  Saving and loading the storage
        THEN:  Data loaded from storage is the same data that was saved 
        """
        # Arrange
        lcp_collection = create_collection(db_session, name="Test Collection", protocol=DataSource.LCP)
        external_integration = lcp_collection.external_integration
        external_integration_association = create_autospec(spec=HasExternalIntegrationPerCollection)
        external_integration_association.collection_external_integration = MagicMock(return_value=external_integration)
        storage = CollectionConfigurationStorage(external_integration_association, lcp_collection)
        setting_name = 'Test'
        expected_result = 'Test'

        # Act
        storage.save(db_session, setting_name, expected_result)
        result = storage.load(db_session, setting_name)

        assert result == expected_result
