#!/usr/bin/env python
"""Look up and set the delivery mechanism for all 3M books."""
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from core.monitor import IdentifierSweepMonitor
from core.model import (
    Identifier,
    Representation,
    DeliveryMechanism,
)
from core.opds_import import SimplifiedOPDSLookup
from core.scripts import RunMonitorScript

class SetDeliveryMechanismMonitor(IdentifierSweepMonitor):

    def __init__(self, _db, interval_seconds=None):
        super(SetDeliveryMechanismMonitor, self).__init__(
            _db, "20150929 migration - Set delivery mechanism for Gutenberg books",
            interval_seconds, batch_size=10)

    def identifier_query(self):
        return self._db.query(Identifier).filter(
            Identifier.type==Identifier.GUTENBERG_ID
        )

    def process_identifier(self, identifier):

        license_pool = identifier.licensed_through
        if not license_pool:
            print "No license pool for %s!" % identifier.identifier
            return
        edition = license_pool.edition
        if edition:
            best = edition.best_open_access_link
            if best:
                print edition.id, edition.title, best.url
                edition.license_pool.set_delivery_mechanism(
                    Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM,
                    best
                )
            else:
                print "Edition but no link for %s/%s!" % (
                    identifier.identifier, edition.title)
        else:
            print "No edition for %s!" % identifier.identifier

RunMonitorScript(SetDeliveryMechanismMonitor).run()
