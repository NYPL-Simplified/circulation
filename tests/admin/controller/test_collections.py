from nose.tools import (
    set_trace,
    eq_,
    assert_raises
)
import flask
import json
from api.admin.problem_details import *
from api.admin.exceptions import *
from core.selftest import HasSelfTests
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
from werkzeug import MultiDict

from test_controller import SettingsControllerTest

class TestCollectionSettings(SettingsControllerTest):
    def test_collections_get_with_no_collections(self):
        # Delete any existing collections created by the test setup.
        for collection in self._db.query(Collection):
            self._db.delete(collection)

        with self.request_context_with_admin("/"):
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.get("collections"), [])

            names = [p.get("name") for p in response.get("protocols")]
            assert ExternalIntegration.OVERDRIVE in names
            assert ExternalIntegration.OPDS_IMPORT in names

    def test_collections_get_collection_protocols(self):
        old_prior_test_results = HasSelfTests.prior_test_results
        HasSelfTests.prior_test_results = self.mock_prior_test_results

        [c1] = self._default_library.collections

        # When there is no storage integration configured,
        # the protocols will not offer a 'mirror_integration_id'
        # setting.
        with self.request_context_with_admin("/"):
            response = self.manager.admin_collection_settings_controller.process_collections()
            protocols = response.get('protocols')
            for protocol in protocols:
                assert all([s.get('key') != 'mirror_integration_id'
                            for s in protocol['settings']])

        # When storage integrations are configured, each protocol will
        # offer a 'mirror_integration_id' setting.
        storage1 = self._external_integration(
            name="integration 1",
            protocol=ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL
        )
        storage2 = self._external_integration(
            name="integration 2",
            protocol="Some other protocol",
            goal=ExternalIntegration.STORAGE_GOAL
        )

        with self.request_context_with_admin("/"):
            controller = self.manager.admin_collection_settings_controller
            response = controller.process_collections()
            protocols = response.get('protocols')
            for protocol in protocols:
                [setting] = [x for x in protocol['settings']
                             if x.get('key') == 'mirror_integration_id']
                eq_("Mirror", setting['label'])
                options = setting['options']

                # The first option is to disable mirroring on this
                # collection altogether.
                no_mirror = options[0]
                eq_(controller.NO_MIRROR_INTEGRATION, no_mirror['key'])

                # The other options are to use one of the storage
                # integrations to do the mirroring.
                use_mirrors = [(x['key'], x['label'])
                               for x in options[1:]]
                expect = [(integration.id, integration.name)
                          for integration in (storage1, storage2)]
                eq_(expect, use_mirrors)

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
        c2.external_account_id = "1234"
        c2.external_integration.password = "b"
        c2.external_integration.username = "user"
        c2.external_integration.setting('website_id').value = '100'
        c2.mirror_integration_id=c2_storage.id

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
            eq_(c1.id, coll1.get("id"))
            eq_(c2.id, coll2.get("id"))
            eq_(c3.id, coll3.get("id"))

            eq_(c1.name, coll1.get("name"))
            eq_(c2.name, coll2.get("name"))
            eq_(c3.name, coll3.get("name"))

            eq_(c1.protocol, coll1.get("protocol"))
            eq_(c2.protocol, coll2.get("protocol"))
            eq_(c3.protocol, coll3.get("protocol"))

            eq_(self.self_test_results, coll1.get("self_test_results"))
            eq_(self.self_test_results, coll2.get("self_test_results"))
            eq_(self.self_test_results, coll3.get("self_test_results"))

            settings1 = coll1.get("settings", {})
            settings2 = coll2.get("settings", {})
            settings3 = coll3.get("settings", {})

            eq_(controller.NO_MIRROR_INTEGRATION,
                settings1.get("mirror_integration_id"))
            eq_(c2_storage.id, settings2.get("mirror_integration_id"))
            eq_(controller.NO_MIRROR_INTEGRATION,
                settings3.get("mirror_integration_id"))

            eq_(c1.external_account_id, settings1.get("external_account_id"))
            eq_(c2.external_account_id, settings2.get("external_account_id"))
            eq_(c3.external_account_id, settings3.get("external_account_id"))

            eq_(c1.external_integration.password, settings1.get("password"))
            eq_(c2.external_integration.password, settings2.get("password"))

            eq_(c2.id, coll3.get("parent_id"))

            coll3_libraries = coll3.get("libraries")
            eq_(2, len(coll3_libraries))
            coll3_l1, coll3_default = sorted(coll3_libraries, key=lambda x: x.get("short_name"))
            eq_("L1", coll3_l1.get("short_name"))
            eq_("14", coll3_l1.get("ebook_loan_duration"))
            eq_(self._default_library.short_name, coll3_default.get("short_name"))

        with self.request_context_with_admin("/", admin=l1_librarian):
            # A librarian only sees collections associated with their library.
            response = controller.process_collections()
            [coll3] = response.get("collections")
            eq_(c3.id, coll3.get("id"))

            coll3_libraries = coll3.get("libraries")
            eq_(1, len(coll3_libraries))
            eq_("L1", coll3_libraries[0].get("short_name"))
            eq_("14", coll3_libraries[0].get("ebook_loan_duration"))

        HasSelfTests.prior_test_results = old_prior_test_results

    def test_collections_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("protocol", "Overdrive"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, MISSING_COLLECTION_NAME)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, NO_PROTOCOL_FOR_NEW_SERVICE)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "Unknown"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, UNKNOWN_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", "123456789"),
                ("name", "collection"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, MISSING_COLLECTION)

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
            eq_(response, COLLECTION_NAME_ALREADY_IN_USE)

        self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", "Overdrive"),
            ])
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_collection_settings_controller.process_collections)

        self.admin.add_role(AdminRole.SYSTEM_ADMIN)
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("id", collection.id),
                ("name", "Collection 1"),
                ("protocol", "Bibliotheca"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, CANNOT_CHANGE_PROTOCOL)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 2"),
                ("protocol", "Bibliotheca"),
                ("parent_id", "1234"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, PROTOCOL_DOES_NOT_SUPPORT_PARENTS)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "Collection 2"),
                ("protocol", "Overdrive"),
                ("parent_id", "1234"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, MISSING_PARENT)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection"),
                ("protocol", "OPDS Import"),
                ("external_account_id", "http://url.test"),
                ("data_source", "test"),
                ("libraries", json.dumps([{"short_name": "nosuchlibrary"}])),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.uri, NO_SUCH_LIBRARY.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "OPDS Import"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Overdrive"),
                ("external_account_id", "1234"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Bibliotheca"),
                ("external_account_id", "1234"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", "Axis 360"),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([
                ("name", "collection1"),
                ("protocol", ExternalIntegration.RB_DIGITAL),
                ("username", "user"),
                ("password", "password"),
            ])
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.uri, INCOMPLETE_CONFIGURATION.uri)

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
            eq_(response.status_code, 201)

        # The collection was created and configured properly.
        collection = get_one(self._db, Collection, name="New Collection")
        eq_(collection.id, int(response.response[0]))
        eq_("New Collection", collection.name)
        eq_("acctid", collection.external_account_id)
        eq_("username", collection.external_integration.username)
        eq_("password", collection.external_integration.password)

        # Two libraries now have access to the collection.
        eq_([collection], l1.collections)
        eq_([collection], l2.collections)
        eq_([], l3.collections)

        # Additional settings were set on the collection.
        setting = collection.external_integration.setting("website_id")
        eq_("website_id", setting.key)
        eq_("1234", setting.value)

        eq_("l1_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value)
        eq_("l2_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l2, collection.external_integration).value)

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
            eq_(response.status_code, 201)

        # The collection was created and configured properly.
        child = get_one(self._db, Collection, name="Child Collection")
        eq_(child.id, int(response.response[0]))
        eq_("Child Collection", child.name)
        eq_("child-acctid", child.external_account_id)

        # The settings that are inherited from the parent weren't set.
        eq_(None, child.external_integration.username)
        eq_(None, child.external_integration.password)
        setting = child.external_integration.setting("website_id")
        eq_(None, setting.value)

        # One library has access to the collection.
        eq_([child], l3.collections)

        eq_("l3_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l3, child.external_integration).value)

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
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The collection has been changed.
        eq_("user2", collection.external_integration.username)

        # A library now has access to the collection.
        eq_([collection], l1.collections)

        # Additional settings were set on the collection.
        setting = collection.external_integration.setting("website_id")
        eq_("website_id", setting.key)
        eq_("1234", setting.value)

        eq_("the_ils", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value)

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
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The collection is the same.
        eq_("user2", collection.external_integration.username)
        eq_(ExternalIntegration.OVERDRIVE, collection.protocol)

        # But the library has been removed.
        eq_([], l1.collections)

        eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ils_name", l1, collection.external_integration).value)

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
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The collection now has a parent.
        eq_(parent, collection.parent)

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
        eq_(None, collection.mirror_integration_id)

        # It's possible to associate the storage integration with the
        # collection.
        base_request = self._base_collections_post_request(collection)
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("mirror_integration_id", storage.id)]
            )
            flask.request.form = request
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response.status_code, 200)
            eq_(storage.id, collection.mirror_integration_id)

        # It's possible to unset the mirror integration ID.
        controller = self.manager.admin_collection_settings_controller
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("mirror_integration_id",
                                 str(controller.NO_MIRROR_INTEGRATION))]
            )
            flask.request.form = request
            response = controller.process_collections()
            eq_(response.status_code, 200)
            eq_(None, collection.mirror_integration_id)

        # Providing a nonexistent integration ID gives an error.
        with self.request_context_with_admin("/", method="POST"):
            request = MultiDict(
                base_request + [("mirror_integration_id", -200)]
            )
            flask.request.form = request
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, MISSING_SERVICE)

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
                    ("mirror_integration_id", collection.external_integration.id)
                ]
            )
            flask.request.form = request
            response = self.manager.admin_collection_settings_controller.process_collections()
            eq_(response, INTEGRATION_GOAL_CONFLICT)

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
            eq_(response.status_code, 200)

        # Additional settings were set on the collection+library.
        eq_("14", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ebook_loan_duration", l1, collection.external_integration).value)
        eq_("12", ConfigurationSetting.for_library_and_externalintegration(
                self._db, "audio_loan_duration", l1, collection.external_integration).value)

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
            eq_(response.status_code, 200)

        eq_(collection.id, int(response.response[0]))

        # The settings associated with the collection+library were removed
        # when the connection between collection and library was deleted.
        eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                self._db, "ebook_loan_duration", l1, collection.external_integration).value)
        eq_(None, ConfigurationSetting.for_library_and_externalintegration(
                self._db, "audio_loan_duration", l1, collection.external_integration).value)
        eq_([], collection.libraries)

    def test_collection_delete(self):
        collection = self._collection()
        eq_(False, collection.marked_for_deletion)

        with self.request_context_with_admin("/", method="DELETE"):
            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            assert_raises(AdminNotAuthorized,
                          self.manager.admin_collection_settings_controller.process_delete,
                          collection.id)

            self.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = self.manager.admin_collection_settings_controller.process_delete(collection.id)
            eq_(response.status_code, 200)

        # The collection should still be available because it is not immediately deleted.
        # The collection will be deleted in the background by a script, but it is
        # now marked for deletion
        fetchedCollection = get_one(self._db, Collection, id=collection.id)
        eq_(collection, fetchedCollection)
        eq_(True, fetchedCollection.marked_for_deletion)

    def test_collection_delete_cant_delete_parent(self):
        parent = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        child = self._collection(protocol=ExternalIntegration.OVERDRIVE)
        child.parent = parent

        with self.request_context_with_admin("/", method="DELETE"):
            response = self.manager.admin_collection_settings_controller.process_delete(parent.id)
            eq_(CANNOT_DELETE_COLLECTION_WITH_CHILDREN, response)
