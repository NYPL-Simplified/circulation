#!/usr/bin/env python
"""Fix work descriptions that were originally UTF-8 but were incorrectly
encoded as Windows-1252.
"""
import os
import sys
import logging
from pdb import set_trace
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))

import time

from core.external_search import ExternalSearchIndex
from core.model import (
    production_session,
    Work,
)

_db = production_session()
client = ExternalSearchIndex()
base = _db.query(Work).filter(Work.summary_text != None).order_by(Work.id)
results = True
offset = 0
print("Looking at %d works." % base.count())
while results:
    fixed = 0
    qu = base.offset(offset).limit(1000)
    results = qu.all()
    for work in results:
        possibly_bad = work.summary_text
        try:
            windows_1252_from_unicode = work.summary_text.encode("windows-1252")
            # If we get to this point, the Unicode summary can be
            # encoded as Windows-1252.
            try:
                final = windows_1252_from_unicode.decode("utf8")
                # If we get to this point, it's UTF-8 that was incorrectly
                # encoded as Windows-1252.
            except UnicodeDecodeError as e:
                # It was Windows-1252 all along.
                final = windows_1252_from_unicode.decode("windows-1252")
        except UnicodeEncodeError as e:
            # This description can't be encoded as Windows-1252, an
            # indication that it was originally UTF-8 and is not
            # subject to this problem.
            final = possibly_bad

        if possibly_bad != final:
            work.summary_text = final
            print("%s\n =>\n %s" % (possibly_bad.encode("utf8"), final.encode("utf8")))
            work.calculate_opds_entries()
            work.update_external_index(client)
            print()
            fixed += 1
        pass
    offset += 1000
    print("At %s, %s/%s needed fixing." % (offset, fixed, len(results)))
    _db.commit()
