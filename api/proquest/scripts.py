import logging

from api.proquest.client import ProQuestAPIClientFactory
from api.proquest.importer import ProQuestOPDS2ImportMonitor
from core.scripts import OPDSImportScript


class ProQuestOPDS2ImportScript(OPDSImportScript):
    """Runs a ProQuestOPDS2ImportMonitor."""

    def __init__(self, *args, **kwargs):
        super(ProQuestOPDS2ImportScript, self).__init__(*args, **kwargs)

        self._logger = logging.getLogger(__name__)

    @classmethod
    def arg_parser(cls):
        parser = OPDSImportScript.arg_parser()
        parser.add_argument(
            "--process-removals",
            help="Remove from the Circulation Manager's catalog items that are no longer present in the ProQuest feed",
            dest="process_removals",
            action="store_true",
        )

        return parser

    def run_monitor(self, collection, force=None):
        """Run the monitor for the specified collection.

        :param collection: Collection object
        :type collection: core.model.collection.Collection

        :param force: Boolean value indicating whether the import process should be run from scratch
        :type force: bool
        """
        if not issubclass(self.monitor_class, ProQuestOPDS2ImportMonitor):
            raise ValueError()

        self._logger.info(
            "Started running ProQuestOPDS2ImportScript for collection {0}".format(
                collection
            )
        )

        parsed = self.parse_command_line(self._db)
        client_factory = ProQuestAPIClientFactory()
        monitor = self.monitor_class(
            client_factory,
            self._db,
            collection,
            import_class=self.importer_class,
            force_reimport=force,
            process_removals=parsed.process_removals,
        )

        monitor.run()

        self._logger.info(
            "Finished running ProQuestOPDS2ImportScript for collection {0}".format(
                collection
            )
        )
