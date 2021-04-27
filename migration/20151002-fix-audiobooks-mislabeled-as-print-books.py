#!/usr/bin/env python
"""Fix audiobooks mislabeled as print books."""

import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from monitor import IdentifierSweepMonitor
from model import (
    Identifier,
    LicensePool,
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
    Edition,
)
from scripts import RunMonitorScript
from overdrive import OverdriveAPI, OverdriveRepresentationExtractor
from threem import ThreeMAPI

class SetDeliveryMechanismMonitor(IdentifierSweepMonitor):

    def __init__(self, _db, interval_seconds=None):
        super(SetDeliveryMechanismMonitor, self).__init__(
            _db, "20151002 migration - Correct medium of mislabeled audiobooks",
            interval_seconds, batch_size=100)
        self.overdrive = OverdriveAPI(_db)
        self.threem = ThreeMAPI(_db)

    types = [Identifier.THREEM_ID, Identifier.OVERDRIVE_ID,
             Identifier.AXIS_360_ID]

    content_types = ["application/epub+zip", "application/pdf",
                     "Kindle via Amazon", "Streaming Text"]

    def identifier_query(self):
        qu = self._db.query(Identifier).join(
                Identifier.licensed_through).join(
                    LicensePool.delivery_mechanisms).join(
                        LicensePoolDeliveryMechanism.delivery_mechanism).filter(
                    Identifier.type.in_(self.types)).filter(
                        ~DeliveryMechanism.content_type.in_(self.content_types)
                    )
        return qu

    def process_identifier(self, identifier):
        # What is the correct medium?
        correct_medium = None
        lp = identifier.licensed_through
        for lpdm in lp.delivery_mechanisms:
            correct_medium = lpdm.delivery_mechanism.implicit_medium
            if correct_medium:
                break
        if not correct_medium and identifier.type==Identifier.OVERDRIVE_ID:
            content = self.overdrive.metadata_lookup(identifier)
            metadata = OverdriveRepresentationExtractor.book_info_to_metadata(content)
            correct_medium = metadata.medium

        if not correct_medium and identifier.type==Identifier.THREEM_ID:
            metadata = self.threem.bibliographic_lookup(identifier)
            correct_medium = metadata.medium

        if not correct_medium:
            set_trace()

        if lp.edition.medium != correct_medium:
            print(("%s is actually %s, not %s" % (lp.edition.title, correct_medium, lp.edition.medium)))
            lp.edition.medium = correct_medium or Edition.BOOK_MEDIUM

RunMonitorScript(SetDeliveryMechanismMonitor).run()
