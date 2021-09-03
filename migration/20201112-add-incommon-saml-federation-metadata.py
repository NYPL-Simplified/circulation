#!/usr/bin/env python3
"""Adds InCommon SAML federation metadata to `samlfederations` table."""

import os
import sys

from contextlib2 import closing

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import production_session                       # noqa: E402
from api.saml.metadata.federations import incommon              # noqa: E402
from api.saml.metadata.federations.model import SAMLFederation  # noqa: E402


with closing(production_session()) as db:
    incommon_federation = db.query(SAMLFederation).filter(
        SAMLFederation.type == incommon.FEDERATION_TYPE).one_or_none()

    if not incommon_federation:
        incommon_federation = SAMLFederation(
            incommon.FEDERATION_TYPE,
            incommon.IDP_METADATA_SERVICE_URL,
            incommon.CERTIFICATE
        )

        db.add(incommon_federation)
        db.commit()
