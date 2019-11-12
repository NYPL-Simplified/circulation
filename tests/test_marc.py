from nose.tools import (
    eq_,
    set_trace,
)
from pymarc import Record

from . import DatabaseTest
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
)

from api.marc import LibraryAnnotator
from core.marc import MARCExporter
from api.registry import Registration

class TestLibraryAnnotator(DatabaseTest):

    def test_annotate_work_record(self):
        # Mock class to verify that the correct methods
        # are called by annotate_work_record.
        class MockAnnotator(LibraryAnnotator):
            called_with = dict()
            def add_marc_organization_code(self, record, marc_org):
                self.called_with['add_marc_organization_code'] = [record, marc_org]

            def add_summary(self, record, work):
                self.called_with['add_summary'] = [record, work]

            def add_simplified_genres(self, record, work):
                self.called_with['add_simplified_genres'] = [record, work]

            def add_web_client_urls(self, record, library, identifier, integration):
                self.called_with['add_web_client_urls'] = [record, library, identifier, integration]

            # Also check that the parent class annotate_work_record is called.
            def add_distributor(self, record, pool):
                self.called_with['add_distributor'] = [record, pool]

            def add_formats(self, record, pool):
                self.called_with['add_formats'] = [record, pool]

        annotator = MockAnnotator(self._default_library)
        record = Record()
        work = self._work(with_license_pool=True)
        pool = work.license_pools[0]
        edition = pool.presentation_edition
        identifier = pool.identifier

        integration = self._external_integration(
            ExternalIntegration.MARC_EXPORT, ExternalIntegration.CATALOG_GOAL,
            libraries=[self._default_library])

        annotator.annotate_work_record(work, pool, edition, identifier, record, integration)

        # If there are no settings, the only methods called will be add_web_client_urls
        # and the parent class methods.
        assert 'add_marc_organization_code' not in annotator.called_with
        assert 'add_summary' not in annotator.called_with
        assert 'add_simplified_genres' not in annotator.called_with
        eq_([record, self._default_library, identifier, integration], annotator.called_with.get('add_web_client_urls'))
        eq_([record, pool], annotator.called_with.get('add_distributor'))
        eq_([record, pool], annotator.called_with.get('add_formats'))

        # If settings are false, the methods still won't be called.
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.INCLUDE_SUMMARY,
            self._default_library, integration).value = "false"

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.INCLUDE_SIMPLIFIED_GENRES,
            self._default_library, integration).value = "false"

        annotator = MockAnnotator(self._default_library)
        annotator.annotate_work_record(work, pool, edition, identifier, record, integration)

        assert 'add_marc_organization_code' not in annotator.called_with
        assert 'add_summary' not in annotator.called_with
        assert 'add_simplified_genres' not in annotator.called_with
        eq_([record, self._default_library, identifier, integration], annotator.called_with.get('add_web_client_urls'))
        eq_([record, pool], annotator.called_with.get('add_distributor'))
        eq_([record, pool], annotator.called_with.get('add_formats'))

        # Once the include settings are true and the marc organization code is set,
        # all methods are called.
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.INCLUDE_SUMMARY,
            self._default_library, integration).value = "true"

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.INCLUDE_SIMPLIFIED_GENRES,
            self._default_library, integration).value = "true"

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.MARC_ORGANIZATION_CODE,
            self._default_library, integration).value = "marc org"

        annotator = MockAnnotator(self._default_library)
        annotator.annotate_work_record(work, pool, edition, identifier, record, integration)

        eq_([record, "marc org"], annotator.called_with.get("add_marc_organization_code"))
        eq_([record, work], annotator.called_with.get("add_summary"))
        eq_([record, work], annotator.called_with.get("add_simplified_genres"))
        eq_([record, self._default_library, identifier, integration], annotator.called_with.get('add_web_client_urls'))
        eq_([record, pool], annotator.called_with.get('add_distributor'))
        eq_([record, pool], annotator.called_with.get('add_formats'))

    def test_add_web_client_urls(self):
        # Web client URLs can come from either the MARC export integration or
        # a library registry integration.

        annotator = LibraryAnnotator(self._default_library)

        # If no web catalog URLs are set for the library, nothing will be changed.
        record = Record()
        identifier = self._identifier(foreign_id="identifier")
        annotator.add_web_client_urls(record, self._default_library, identifier)
        eq_([], record.get_fields("856"))

        # Add a URL from a library registry.
        registry = self._external_integration(
            ExternalIntegration.OPDS_REGISTRATION, ExternalIntegration.DISCOVERY_GOAL,
            libraries=[self._default_library])
        ConfigurationSetting.for_library_and_externalintegration(
            self._db, Registration.LIBRARY_REGISTRATION_WEB_CLIENT,
            self._default_library, registry).value = "http://web_catalog"

        record = Record()
        annotator.add_web_client_urls(record, self._default_library, identifier)
        [field] = record.get_fields("856")
        eq_(["4", "0"], field.indicators)
        eq_("http://web_catalog/book/Gutenberg%20ID%2Fidentifier",
            field.get_subfields("u")[0])

        # Add a manually configured URL on a MARC export integration.
        integration = self._external_integration(
            ExternalIntegration.MARC_EXPORT, ExternalIntegration.CATALOG_GOAL,
            libraries=[self._default_library])

        ConfigurationSetting.for_library_and_externalintegration(
            self._db, MARCExporter.WEB_CLIENT_URL,
            self._default_library, integration).value = "http://another_web_catalog"

        record = Record()
        annotator.add_web_client_urls(record, self._default_library, identifier, integration)
        [field1, field2] = record.get_fields("856")
        eq_(["4", "0"], field1.indicators)
        eq_("http://another_web_catalog/book/Gutenberg%20ID%2Fidentifier",
            field1.get_subfields("u")[0])

        eq_(["4", "0"], field2.indicators)
        eq_("http://web_catalog/book/Gutenberg%20ID%2Fidentifier",
            field2.get_subfields("u")[0])

        

