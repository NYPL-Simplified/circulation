#!/usr/bin/env python3
"""Look up and set the delivery mechanism for all 3M books."""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.monitor import IdentifierSweepMonitor         # noqa: E402
from core.model import Identifier                       # noqa: E402
from core.opds_import import SimplifiedOPDSLookup       # noqa: E402,F401
from api.axis import Axis360API, BibliographicParser    # noqa: E402
from core.scripts import RunMonitorScript               # noqa: E402


class SetDeliveryMechanismMonitor(IdentifierSweepMonitor):

    def __init__(self, _db, interval_seconds=None):
        super(SetDeliveryMechanismMonitor, self).__init__(
            _db, "20150929 migration - Set delivery mechanism for Axis books",
            interval_seconds, batch_size=10)
        self.api = Axis360API(_db)

    def identifier_query(self):
        return self._db.query(Identifier).filter(
            Identifier.type==Identifier.AXIS_360_ID         # noqa: E225
        )

    def process_identifier(self, identifier):
        availability = self.api.availability(title_ids=[identifier.identifier])
        status_code = availability.status_code              # noqa: F841
        content = availability.content
        result = list(BibliographicParser().process_all(content))
        if len(result) == 1:
            [(metadata, circulation)] = result
            license_pool = identifier.licensed_through
            for format in metadata.formats:
                print("%s: %s - %s" % (identifier.identifier, format.content_type, format.drm_scheme))
                mech = license_pool.set_delivery_mechanism(     # noqa: F841
                    format.content_type,
                    format.drm_scheme,
                    format.link
                )
        else:
            print("Book not in collection: %s" % identifier.identifier)


RunMonitorScript(SetDeliveryMechanismMonitor).run()
