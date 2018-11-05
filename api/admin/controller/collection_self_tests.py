from nose.tools import set_trace
import flask
from flask import Response
from flask_babel import lazy_gettext as _
from api.admin.problem_details import *
from core.opds_import import (OPDSImporter, OPDSImportMonitor)
from core.model import (
    Collection
)
from core.selftest import HasSelfTests
from . import SettingsController

class CollectionSelfTestsController(SettingsController):

    def __init__(self, manager):
        super(CollectionSelfTestsController, self).__init__(manager)
        self.protocols = self._get_collection_protocols(self.PROVIDER_APIS)

    def process_collection_self_tests(self, identifier):
        if not identifier:
            return MISSING_COLLECTION_IDENTIFIER
        if flask.request.method == 'GET':
            return self.process_get(identifier)
        else:
            return self.process_post(identifier)

    def process_get(self, identifier):
        collection = self.look_up_collection_by_id(identifier)
        collection_info = self.get_collection_info(collection)
        return dict(collection=collection_info)

    def look_up_collection_by_id(self, identifier):
        """Find the collection to display self test results or run self tests for"""
        return self._db.query(Collection).filter(Collection.id==int(identifier))[0]

    def get_collection_info(self, collection):
        """Compile information about this collection, including the results from the last time, if ever,
        that the self tests were run."""

        protocolClass = None
        collection_info = dict(
            id=collection.id,
            name=collection.name,
            protocol=collection.protocol,
            parent_id=collection.parent_id,
            settings=dict(external_account_id=collection.external_account_id),
        )

        protocolClass = self.find_protocol_class(collection.protocol)
        collection_info["self_test_results"] = self._get_prior_test_results(collection, protocolClass)
        return collection_info

    def find_protocol_class(self, collectionProtocol):
        """Figure out which protocol is providing books to this collection"""

        if collectionProtocol in [p.get("name") for p in self.protocols]:
            protocolClassFound = [p for p in self.PROVIDER_APIS if p.NAME == collectionProtocol]
            if len(protocolClassFound) == 1:
                return protocolClassFound[0]

    def process_post(self, identifier):
        collection = self.look_up_collection_by_id(identifier)
        collectionProtocol = collection.protocol or None
        protocolClass = self.find_protocol_class(collectionProtocol) or None

        if protocolClass:
            value = None
            if (collectionProtocol == OPDSImportMonitor.PROTOCOL):
                protocolClass = OPDSImportMonitor
                value, results = protocolClass.run_self_tests(self._db, protocolClass, self._db, collection, OPDSImporter)
            elif issubclass(protocolClass, HasSelfTests):
                value, results = protocolClass.run_self_tests(self._db, protocolClass, self._db, collection)

            if (value):
                return Response(_("Successfully ran new self tests"), 200)

        return FAILED_TO_RUN_SELF_TESTS
