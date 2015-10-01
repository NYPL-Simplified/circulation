#!/usr/bin/env python
"""Try to fix the contributors for books that currently have none.
"""

from pdb import set_trace
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from core.model import (
    production_session,
    DataSource,
    Work,
    Edition,
)
from overdrive import (
    OverdriveAPI,
    OverdriveRepresentationExtractor
)
from threem import ThreeMAPI
from core.opds_import import SimplifiedOPDSLookup
lookup = SimplifiedOPDSLookup("http://metadata.alpha.librarysimplified.org/")

_db = production_session()
overdrive = OverdriveAPI(_db)
threem = ThreeMAPI(_db)

q = _db.query(Edition).join(Edition.data_source).filter(DataSource.name.in_([DataSource.OVERDRIVE])).filter(Edition.author=='')
print "Fixing %s books." % q.count()
for edition in q:
    if edition.data_source.name==DataSource.OVERDRIVE:
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
        print "%s = %s (%s)" % (
            c.role, c.contributor.display_name, c.contributor.name
        )
    print edition.author, edition.sort_author
    _db.commit()
