import logging

import flask
from flask import Response

from api.admin.problem_details import MISSING_COLLECTION
from api.controller import CirculationManagerController
from api.lcp.factory import LCPServerFactory
from core.lcp.credential import LCPCredentialFactory
from core.model import Session, ExternalIntegration, Collection
from core.util.problem_detail import ProblemDetail


class LCPController(CirculationManagerController):
    """Contains API endpoints related to LCP workflow"""

    def __init__(self, manager):
        """Initializes a new instance of LCPController class

        :param manager: CirculationManager object
        :type manager: CirculationManager
        """
        super(LCPController, self).__init__(manager)

        self._logger = logging.getLogger(__name__)
        self._credential_factory = LCPCredentialFactory()
        self._lcp_server_factory = LCPServerFactory()

    def _get_patron(self):
        """Returns a patron associated with the request (if any)

        :return: Patron associated with the request (if any)
        :rtype: core.model.patron.Patron
        """
        self._logger.info('Started fetching an authenticated patron associated with the request')

        patron = self.authenticated_patron_from_request()

        self._logger.info('Finished fetching an authenticated patron associated with the request: {0}'.format(patron))

        return patron

    def _get_lcp_passphrase(self, patron):
        """Returns a patron's LCP passphrase

        :return: Patron's LCP passphrase
        :rtype: string
        """
        db = Session.object_session(patron)

        self._logger.info('Started fetching a patron\'s LCP passphrase')

        lcp_passphrase = self._credential_factory.get_patron_passphrase(db, patron)

        self._logger.info('Finished fetching a patron\'s LCP passphrase: {0}'.format(lcp_passphrase))

        return lcp_passphrase

    def _get_lcp_collection(self, patron, collection_name):
        """Returns an LCP collection for a specified library
        NOTE: We assume that there is only ONE LCP collection per library

        :param patron: Patron object
        :type patron: core.model.patron.Patron

        :param collection_name: Name of the collection
        :type collection_name: string

        :return: LCP collection
        :rtype: core.model.collection.Collection
        """
        db = Session.object_session(patron)
        lcp_collection, _ = Collection.by_name_and_protocol(db, collection_name, ExternalIntegration.LCP)

        if not lcp_collection or lcp_collection not in patron.library.collections:
            return MISSING_COLLECTION

        return lcp_collection

    def get_lcp_passphrase(self):
        """Returns an LCP passphrase for the authenticated patron

        :return: Flask response containing the LCP passphrase for the authenticated patron
        :rtype: Response
        """
        self._logger.info('Started fetching a patron\'s LCP passphrase')

        patron = self._get_patron()
        lcp_passphrase = self._get_lcp_passphrase(patron)

        self._logger.info('Finished fetching a patron\'s LCP passphrase: {0}'.format(lcp_passphrase))

        response = flask.jsonify({
            'passphrase': lcp_passphrase
        })

        return response

    def get_lcp_license(self, collection_name, license_id):
        """Returns an LCP license with the specified ID

        :param collection_name: Name of the collection
        :type collection_name: string

        :param license_id: License ID
        :type license_id: string

        :return: Flask response containing the LCP license with the specified ID
        :rtype: string
        """
        self._logger.info('Started fetching license # {0}'.format(license_id))

        patron = self._get_patron()
        lcp_collection = self._get_lcp_collection(patron, collection_name)

        if isinstance(lcp_collection, ProblemDetail):
            return lcp_collection

        lcp_api = self.circulation.api_for_collection.get(lcp_collection.id)
        lcp_server = self._lcp_server_factory.create(lcp_api)

        db = Session.object_session(patron)
        lcp_license = lcp_server.get_license(db, license_id, patron)

        self._logger.info('Finished fetching license # {0}: {1}'.format(license_id, lcp_license))

        return flask.jsonify(lcp_license)
