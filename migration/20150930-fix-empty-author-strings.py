#!/usr/bin/env python3
"""Try to fix the contributors for books that currently have none."""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (        # noqa: E402,F401
    production_session,
    DataSource,
    Work,
    Edition,
)
from api.overdrive import (     # noqa: E402
    OverdriveAPI,
    OverdriveRepresentationExtractor
)
from threem import ThreeMAPI    # noqa: E402
from core.opds_import import SimplifiedOPDSLookup   # noqa: E402

lookup = SimplifiedOPDSLookup("http://metadata.alpha.librarysimplified.org/")

_db = production_session()
overdrive = OverdriveAPI(_db)
threem = ThreeMAPI(_db)

q = _db.query(Edition).join(Edition.data_source).filter(DataSource.name.in_([DataSource.OVERDRIVE])).filter(Edition.author=='')  # noqa: E501,E225
print("Fixing %s books." % q.count())
for edition in q:
    if edition.data_source.name == DataSource.OVERDRIVE:
        data = overdrive.metadata_lookup(edition.primary_identifier)
        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(data)
    else:
        metadata = threem.bibliographic_lookup(edition.primary_identifier)
    metadata.update_contributions(_db, edition, metadata_client=lookup,
                                  replace_contributions=True)
    if edition.work:
        edition.work.calculate_presentation()
    else:
        edition.calculate_presentation()

    for c in edition.contributions:
        print("%s = %s (%s)" % (c.role, c.contributor.display_name, c.contributor.name))

    print(edition.author, edition.sort_author)

    _db.commit()
