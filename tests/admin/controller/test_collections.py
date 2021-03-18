import json

import flask
import pytest

from werkzeug.datastructures import MultiDict

from api.admin.exceptions import *
from core.model import (
    Admin,
    AdminRole,
    Collection,
    ConfigurationSetting,
    create,
    ExternalIntegration,
    get_one,
    Library,
)
from core.model.configuration import ExternalIntegrationLink
from core.s3 import S3UploaderConfiguration
from core.selftest import HasSelfTests
from .test_controller import SettingsControllerTest


class TestCollectionSettings(SettingsControllerTest):
    def test_collections_get_with_no_collections(self):
        # Delete any existing collections created by the test setup.
        for collection in self._db.query(Collection):
            self._db.delete(collection)

        with self.request_context_with_admin("/"):
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.get("collections") == []

            names = [p.get("name") for p in response.get("protocols")]
            assert ExternalIntegration.OVERDRIVE in names
            assert ExternalIntegration.OPDS_IMPORT in names

    def test_collections_get_collection_protocols(self):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results

        l1 = self._default_library
        [c1] = l1.collections

        # When there is no storage integration configured,
        # the protocols will not offer a 'mirror_integration_id'
        # setting for covers or books.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_collection_settings_controller.process_collections()
            protocols = response.get('protocols')
            for protocol in protocols:
                assert all([not s.get('key').endswith('mirror_integration_id')
                            for s in protocol['settings'] if s])

        # When storage integrations are configured, each protocol will
        # offer a 'mirror_integration_id' setting for covers and books.
        storage1 = self._external_integration(
            name="integration 1",
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: 'covers',
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: 'open-access-books',
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: 'protected-access-books'
            }
        )
        storage2 = self._external_integration(
            name="integration 2",
            protocol="Some other protocol",
            goal=ExternalIntegration.STORAGE_GOAL,
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: 'covers',
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: 'open-access-books',
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: 'protected-access-books'
            }
        )

        with self.request_context_with_admin("/"):
            controller = self.manager.admin_collection_settings_controller
            response = controller.process_collections()
            protocols = response.get('protocols')
            for protocol in protocols:
                mirror_settings = [x for x in protocol['settings']
                             if x.get('key').endswith('mirror_integration_id')]

                covers_mirror = mirror_settings[0]
                open_access_books_mirror = mirror_settings[1]
                protected_access_books_mirror = mirror_settings[2]
                assert "Covers Mirror" == covers_mirror['label']
                assert "Open Access Books Mirror" == open_access_books_mirror['label']
                assert "Protected Access Books Mirror" == protected_access_books_mirror['label']
                covers_mirror_option = covers_mirror['options']
                open_books_mirror_option = open_access_books_mirror['options']
                protected_books_mirror_option = protected_access_books_mirror['options']

                # The first option is to disable mirroring on this
                # collection altogether.
                no_mirror_covers = covers_mirror_option[0]
                no_mirror_open_books = open_books_mirror_option[0]
                no_mirror_protected_books = protected_books_mirror_option[0]
                assert controller.NO_MIRROR_INTEGRATION == no_mirror_covers['key']
                assert controller.NO_MIRROR_INTEGRATION == no_mirror_open_books['key']
                assert controller.NO_MIRROR_INTEGRATION == no_mirror_protected_books['key']

                # The other options are to use one of the storage
                # integrations to do the mirroring.
                use_covers_mirror = [(x['key'], x['label'])
                               for x in covers_mirror_option[1:]]
                use_open_books_mirror = [(x['key'], x['label'])
                               for x in open_books_mirror_option[1:]]
                use_protected_books_mirror = [(x['key'], x['label'])
                               for x in protected_books_mirror_option[1:]]

                # Expect to have two separate mirrors
                expect_covers = [(str(integration.id), integration.name)
                          for integration in (storage1, storage2)]
                assert expect_covers == use_covers_mirror
                expect_open_books = [(str(integration.id), integration.name)
                          for integration in (storage1, storage2)]
                assert expect_open_books == use_open_books_mirror
                expect_protected_books = [(str(integration.id), integration.name)
                          for integration in (storage1, storage2)]
                assert expect_protected_books == use_protected_books_mirror

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_collections_get_collections_with_multiple_collections(self):

        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results

        [c1] = self._default_library.collections

        c2 = self._collection(
            name="Collection 2", protocol=ExternalIntegration.OVERDRIVE,
        )
        c2_storage = self._external_integration(
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )
        c2_external_integration_link = self._external_integration_link(
            integration=c2.external_integration,
            other_integration=c2_storage, purpose=ExternalIntegrationLink.COVERS
        )

        c2.external_account_id = "1234"
        c2.external_integration.password = "b"
        c2.external_integration.username = "user"
        c2.external_integration.setting('website_id').value = '100'

        c3 = self._collection(
            name="Collection 3", protocol=ExternalIntegration.OVERDRIVE,
        )
        c3.external_account_id = "5678"
        c3.parent = c2

        l1 = self._library(short_name="L1")
        c3.libraries += [l1, self._default_library]
        c3.external_integration.libraries += [l1]
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, "ebook_loan_duration", l1, c3.external_integration).value = "14"

        l1_librarian, ignore = create(self._db, Admin, email="admin@l1.org")
        l1_librarian.add_role(AdminRole.LIBRARIAN, l1)

        with self.request_context_with_admin("/"):
            controller = self.manager.admin_collection_settings_controller
            response = controller.process_collections()
            # The system admin can see all collections.
            coll2, coll3, coll1 = sorted(
                response.get("collections"), key = lambda c: c.get('name')
            )
            assert c1.id == coll1.get("id")
            assert c2.id == coll2.get("id")
            assert c3.id == coll3.get("id")

            assert c1.name == coll1.get("name")
            assert c2.name == coll2.get("name")
            assert c3.name == coll3.get("name")

            assert c1.protocol == coll1.get("protocol")
            assert c2.protocol == coll2.get("protocol")
            assert c3.protocol == coll3.get("protocol")

            assert self.self_test_results == coll1.get("self_test_results")
            assert self.self_test_results == coll2.get("self_test_results")
            assert self.self_test_results == coll3.get("self_test_results")

            settings1 = coll1.get("settings", {})
            settings2 = coll2.get("settings", {})
            settings3 = coll3.get("settings", {})

            assert (controller.NO_MIRROR_INTEGRATION ==
                settings1.get("covers_mirror_integration_id"))
            assert (controller.NO_MIRROR_INTEGRATION ==
                settings1.get("books_mirror_integration_id"))
            # Only added an integration for S3 storage for covers.
            assert str(c2_storage.id) == settings2.get("covers_mirror_integration_id")
            assert controller.NO_MIRROR_INTEGRATION == settings2.get("books_mirror_integration_id")
            assert (controller.NO_MIRROR_INTEGRATION ==
                settings3.get("covers_mirror_integration_id"))
            assert (controller.NO_MIRROR_INTEGRATION ==
                settings3.get("books_mirror_integration_id"))

            assert c1.external_account_id == settings1.get("external_account_id")
            assert c2.external_account_id == settings2.get("external_account_id")
            assert c3.external_account_id == settings3.get("external_account_id")

            assert c1.external_integration.password == settings1.get("password")
            assert c2.external_integration.password == settings2.get("password")

            assert c2.id == coll3.get("parent_id")

            coll3_libraries = coll3.get("libraries")
            assert 2 == len(coll3_libraries)
            coll3_l1, coll3_default = sorted(coll3_libraries, key=lambda x: x.get("short_name"))
            assert "L1" == coll3_l1.get("short_name")
            assert "14" == coll3_l1.get("ebook_loan_duration")
            assert self._default_library.short_name == coll3_default.get("short_name")

        with self.request_context_with_admin("/", admin=l1_librarian):
            # A librarian only sees collections associated with their library.
            response = controller.process_collections()
            [coll3] = response.get("collections")
            assert c3.id == coll3.get("id")

            coll3_libraries = coll3.get("libraries")
            assert 1 == len(coll3_libraries)
            assert "L1" == coll3_libraries[0].get("short_name")
            assert "14" == coll3_libraries[0].get("ebook_loan_duration")

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_collections_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Overdrive"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == MISSING_COLLECTION_NAME

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123456789"),
                ("name", "collection"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == MISSING_COLLECTION

        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.OVERDRIVE
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 1"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == COLLECTION_NAME_ALREADY_IN_USE

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", "Overdrive"),
            ])
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_collection_settings_controller.process_collections)

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == CANNOT_CHANGE_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 2"),
                ("protocol", "Bibliotheca"),
                ("parent_id", "1234"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == PROTOCOL_DOES_NOT_SUPPORT_PARENTS

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 2"),
                ("protocol", "Overdrive"),
                ("parent_id", "1234"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == MISSING_PARENT

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "OPDS Import"),
                ("external_account_id", "http://url.test"),
                ("data_source", "test"),
                ("libraries", json.dumps([{"short_name": "nosuchlibrary"}])),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.uri == NO_SUCH_LIBRARY.uri

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "OPDS Import"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Overdrive"),
                ("external_account_id", "1234"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Bibliotheca"),
                ("external_account_id", "1234"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Axis 360"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", ExternalIntegration.RB_DIGITAL),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_collections_post_create(self):
        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )
        l2, ignore = create(
            self._db, Library, name="Library 2", short_name="L2",
        )
        l3, ignore = create(
            self._db, Library, name="Library 3", short_name="L3",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "New Collection"),
                ("protocol", "Overdrive"),
                ("libraries", json.dumps([
                    {"short_name": "L1", "ils_name": "l1_ils"},
                    {"short_name":"L2", "ils_name": "l2_ils"}
                ])),
                ("external_account_id", "acctid"),
                ("username", "username"),
                ("password", "password"),
                ("website_id", "1234"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 201

        # The collection was created and configured properly.
        collection = get_one(self._db, Collection, name="New Collection")
        assert collection.id == int(response.response[0])
        assert "New Collection" == collection.name
        assert "acctid" == collection.external_account_id
        assert "username" == collection.external_integration.username
        assert "password" == collection.external_integration.password

        # Two libraries now have access to the collection.
        assert [collection] == l1.collections
        assert [collection] == l2.collections
        assert [] == l3.collections

        # Additional settings were set on the collection.
        setting = collection.external_integration.setting("website_id")
        assert "website_id" == setting.key
        assert "1234" == setting.value

        assert "l1_ils" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value
        assert "l2_ils" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l2, collection.external_integration).value

        # This collection will be a child of the first collection.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Child Collection"),
                ("protocol", "Overdrive"),
                ("parent_id", collection.id),
                ("libraries", json.dumps([{"short_name": "L3", "ils_name": "l3_ils"}])),
                ("external_account_id", "child-acctid"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 201

        # The collection was created and configured properly.
        child = get_one(self._db, Collection, name="Child Collection")
        assert child.id == int(response.response[0])
        assert "Child Collection" == child.name
        assert "child-acctid" == child.external_account_id

        # The settings that are inherited from the parent weren't set.
        assert None == child.external_integration.username
        assert None == child.external_integration.password
        setting = child.external_integration.setting("website_id")
        assert None == setting.value

        # One library has access to the collection.
        assert [child] == l3.collections

        assert "l3_ils" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l3, child.external_integration).value

    def test_collections_post_edit(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.OVERDRIVE
        )

        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.OVERDRIVE),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("website_id", "1234"),
                ("libraries", json.dumps([{"short_name": "L1", "ils_name": "the_ils"}])),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

        # The collection has been changed.
        assert "user2" == collection.external_integration.username

        # A library now has access to the collection.
        assert [collection] == l1.collections

        # Additional settings were set on the collection.
        setting = collection.external_integration.setting("website_id")
        assert "website_id" == setting.key
        assert "1234" == setting.value

        assert "the_ils" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.OVERDRIVE),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("website_id", "1234"),
                ("libraries", json.dumps([])),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

        # The collection is the same.
        assert "user2" == collection.external_integration.username
        assert ExternalIntegration.OVERDRIVE == collection.protocol

        # But the library has been removed.
        assert [] == l1.collections

        # All ConfigurationSettings for that library and collection
        # have been deleted.
        qu = self._db.query(ConfigurationSetting).filter(
            ConfigurationSetting.library==l1
        ).filter(
            ConfigurationSetting.external_integration==collection.external_integration
        )
        assert 0 == qu.count()

        parent = self._collection(
            name="Parent",
            protocol=ExternalIntegration.OVERDRIVE
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.OVERDRIVE),
                ("parent_id", parent.id),
                ("external_account_id", "1234"),
                ("libraries", json.dumps([])),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

        # The collection now has a parent.
        assert parent == collection.parent

    def _base_collections_post_request(self, collection):
        """A template for POST requests to the collections controller."""
        return [
            ("id", collection.id),
            ("name", "Collection 1"),
            ("protocol", ExternalIntegration.RB_DIGITAL),
            ("external_account_id", "1234"),
            ("username", "user2"),
            ("password", "password"),
            ("url", "http://rb/"),
        ]

    def test_collections_post_edit_mirror_integration(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.RB_DIGITAL
        )

        # There is a storage integration not associated with the collection.
        storage = self._external_integration(
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )

        # It's possible to associate the storage integration with the
        # collection for either a books or covers mirror.
        base_request = self._base_collections_post_request(collection)
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("books_mirror_integration_id", storage.id)]
            )
            flask.request.form = request
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 200

            # There is an external integration link to associate the collection's
            # external integration with the storage integration for a books mirror.
            external_integration_link = get_one(
                self._db, ExternalIntegrationLink,
                external_integration_id=collection.external_integration.id
            )
            assert storage.id == external_integration_link.other_integration_id

        # It's possible to unset the mirror integration.
        controller = self.manager.admin_collection_settings_controller
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("books_mirror_integration_id",
                                 str(controller.NO_MIRROR_INTEGRATION))]
            )
            flask.request.form = request
            response = controller.process_collections()
            assert response.status_code == 200
            external_integration_link = get_one(
                self._db, ExternalIntegrationLink,
                external_integration_id=collection.external_integration.id
            )
            assert None == external_integration_link

        # Providing a nonexistent integration ID gives an error.
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("books_mirror_integration_id", -200)]
            )
            flask.request.form = request
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == MISSING_SERVICE

    def test_cannot_set_non_storage_integration_as_mirror_integration(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.RB_DIGITAL
        )

        # There is a storage integration not associated with the collection,
        # which makes it possible to associate storage integrations
        # with collections through the collections controller.
        storage = self._external_integration(
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )

        # Trying to set a non-storage integration (such as the
        # integration associated with the collection's licenses) as
        # the collection's mirror integration gives an error.
        base_request = self._base_collections_post_request(collection)
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [
                    ("books_mirror_integration_id", collection.external_integration.id)
                ]
            )
            flask.request.form = request
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response == INTEGRATION_GOAL_CONFLICT

    def test_collections_post_edit_library_specific_configuration(self):
        # The collection exists.
        collection = self._collection(
            name="Collection 1",
            protocol=ExternalIntegration.RB_DIGITAL
        )

        l1, ignore = create(
            self._db, Library, name="Library 1", short_name="L1",
        )

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.RB_DIGITAL),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("url", "http://rb/"),
                ("libraries", json.dumps([
                    {
                        "short_name": "L1",
                        "ebook_loan_duration": "14",
                        "audio_loan_duration": "12"
                    }
                ])
                ),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 200

        # Additional settings were set on the collection+library.
        assert "14" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ebook_loan_duration", l1, collection.external_integration).value
        assert "12" == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "audio_loan_duration", l1, collection.external_integration).value

        # Remove the connection between collection and library.
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", ExternalIntegration.RB_DIGITAL),
                ("external_account_id", "1234"),
                ("username", "user2"),
                ("password", "password"),
                ("url", "http://rb/"),
                ("libraries", json.dumps([])),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            assert response.status_code == 200

        assert collection.id == int(response.response[0])

        # The settings associated with the collection+library were removed
        # when the connection between collection and library was deleted.
        assert None == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ebook_loan_duration", l1, collection.external_integration).value
        assert None == ConfigurationSetting.for_library_and_externalintegration(
                self._db, "audio_loan_duration", l1, collection.external_integration).value
        assert [] == collection.libraries

    def test_collection_delete(self):
        collection = self._collection()
        assert False == collection.marked_for_deletion

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(AdminNotAuthorized,
                          self.manager.admin_collection_settings_controller.process_delete,
                          collection.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_collection_settings_controller.process_delete(collection.id)
            assert response.status_code == 200

        # The collection should still be available because it is not immediately deleted.
        # The collection will be deleted in the background by a script, but it is
        # now marked for deletion
        fetchedCollection = get_one(self._db, Collection, id=collection.id)
        assert collection == fetchedCollection
        assert True == fetchedCollection.marked_for_deletion

    def test_collection_delete_cant_delete_parent(self):
        parent = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        child = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        child.parent = parent

        with self.request_context_with_admin("/", method="DELETE"):
            response = self.manager.admin_collection_settings_controller.process_delete(parent.id)
            assert CANNOT_DELETE_COLLECTION_WITH_CHILDREN == response
