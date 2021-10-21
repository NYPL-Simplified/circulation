#!/usr/bin/env python3
"""Migrate patron restriction to library identifier."""
import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (        # noqa: E402
    Library,
    ExternalIntegration,
    ConfigurationSetting,
    production_session,
)
from api.authenticator import AuthenticationProvider        # noqa: E402

try:
    _db = production_session()
    for library in _db.query(Library):
        for integration in library.integrations:
            if integration.goal == ExternalIntegration.PATRON_AUTH_GOAL:
                # Get old patron restriction.
                patron_restriction = ConfigurationSetting.for_library_and_externalintegration(
                    _db, 'patron_identifier_restriction', library, integration)

                # Get new settings.
                library_identifier_field = ConfigurationSetting.for_library_and_externalintegration(
                    _db, AuthenticationProvider.LIBRARY_IDENTIFIER_FIELD, library, integration)
                library_identifier_restriction_type = ConfigurationSetting.for_library_and_externalintegration(
                    _db, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE, library, integration)
                library_identifier_restriction = ConfigurationSetting.for_library_and_externalintegration(
                    _db, AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION, library, integration)

                # Set new settings.
                if not patron_restriction.value:
                    library_identifier_restriction_type.value = AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_NONE     # noqa: E501
                elif patron_restriction.value.startswith("^"):
                    library_identifier_restriction_type.value = AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_REGEX    # noqa: E501
                    library_identifier_field.value = 'barcode'
                    library_identifier_restriction.value = patron_restriction.value
                else:
                    library_identifier_restriction_type.value = AuthenticationProvider.LIBRARY_IDENTIFIER_RESTRICTION_TYPE_PREFIX   # noqa: E501
                    library_identifier_field.value = 'barcode'
                    library_identifier_restriction.value = patron_restriction.value

                # Old patron restriction no longer needed.
                _db.delete(patron_restriction)
finally:
    _db.commit()
    _db.close()
