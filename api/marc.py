from nose.tools import set_trace
from pymarc import Field
import urllib

from core.marc import (
    Annotator,
    MARCExporter,
)
from core.model import (
    ConfigurationSetting,
    Session,
)

class LibraryAnnotator(Annotator):
    def __init__(self, library):
        super(LibraryAnnotator, self).__init__()
        self.library = library

    def value(self, key, integration):
        _db = Session.object_session(integration)
        return ConfigurationSetting.for_library_and_externalintegration(
            _db, key, self.library, integration).value


    def annotate_work_record(self, work, active_license_pool, edition,
                             identifier, record, integration=None, updated=None):
        super(LibraryAnnotator, self).annotate_work_record(
            work, active_license_pool, edition, identifier, record, integration, updated)

        if integration:
            marc_org = self.value(MARCExporter.MARC_ORGANIZATION_CODE, integration)
            include_summary = (self.value(MARCExporter.INCLUDE_SUMMARY, integration) == "true")
            include_genres = (self.value(MARCExporter.INCLUDE_SIMPLIFIED_GENRES, integration) == "true")

            if marc_org:
                self.add_marc_organization_code(record, marc_org)

            if include_summary:
                self.add_summary(record, work)

            if include_genres:
                self.add_simplified_genres(record, work)

        self.add_web_client_urls(record, self.library, identifier, integration)

    def add_web_client_urls(self, record, library, identifier, integration=None):
        _db = Session.object_session(library)
        settings = []

        if integration:
            marc_setting = self.value(MARCExporter.WEB_CLIENT_URL, integration)
            if marc_setting:
                settings.append(marc_setting)

        from api.registry import Registration
        settings += [s.value for s in _db.query(
            ConfigurationSetting
        ).filter(
            ConfigurationSetting.key==Registration.LIBRARY_REGISTRATION_WEB_CLIENT,
            ConfigurationSetting.library_id==library.id
        ) if s.value]

        for setting in settings:
            record.add_field(
                Field(
                    tag="856",
                    indicators=["4", "0"],
                    subfields=[
                        "u", setting + "/book/" + urllib.quote(identifier.type + "/" + identifier.identifier, safe='')
                    ]))


