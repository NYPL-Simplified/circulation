from nose.tools import set_trace

from api.opds import CirculationManagerAnnotator
from core.model import BaseMaterializedWork


class AdminAnnotator(CirculationManagerAnnotator):

    def __init__(self, circulation, test_mode=False):
        super(AdminAnnotator, self).__init__(circulation, None, test_mode=test_mode)

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry):

        super(AdminAnnotator, self).annotate_work_entry(work, active_license_pool, edition, identifier, feed, entry)

        if isinstance(work, BaseMaterializedWork):
            identifier_identifier = work.identifier
            data_source_name = work.name
        else:
            identifier_identifier = identifier.identifier
            data_source_name = active_license_pool.data_source.name


        if active_license_pool.suppressed:
            feed.add_link_to_entry(
                entry,
                rel="http://librarysimplified.org/terms/rel/restore",
                href=self.url_for(
                    "unsuppress", data_source=data_source_name,
                    identifier=identifier_identifier, _external=True)
            )
        else:
            feed.add_link_to_entry(
                entry,
                rel="http://librarysimplified.org/terms/rel/hide",
                href=self.url_for(
                    "suppress", data_source=data_source_name,
                    identifier=identifier_identifier, _external=True)
            )
            
