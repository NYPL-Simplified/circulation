#!/usr/bin/env python
"""Fix Editions that list the same contributor as both 'Primary Author'
and 'Author', and Editions that list the same contributor in an
'Unknown' role plus some more specific role.
"""
import os
import sys
import logging
from pdb import set_trace
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..", "..")
sys.path.append(os.path.abspath(package_dir))

import time

from sqlalchemy.orm import (
    aliased,
)
from sqlalchemy.sql.expression import (
    and_,
    or_
)
from core.model import (
    Contribution,
    Contributor,
    Edition,
    production_session
)


def dedupe(edition):
    print("Deduping edition %s (%s)" % (edition.id, edition.title))
    primary_author = [x for x in edition.contributions if x.role==Contributor.PRIMARY_AUTHOR_ROLE]
    seen = set()
    contributors_with_roles = set()
    unresolved_mysteries = {}
    resolved_mysteries = set()

    if primary_author:
        primary_author_contribution = primary_author[0]
        print(" Primary author: %s" % primary_author_contribution.contributor.name)
        seen.add((primary_author_contribution.contributor, Contributor.AUTHOR_ROLE))
        contributors_with_roles.add(primary_author_contribution.contributor)

    for contribution in list(edition.contributions):
        contributor = contribution.contributor
        role = contribution.role
        if role == Contributor.PRIMARY_AUTHOR_ROLE:
            # Already handled.
            continue
        key = (contributor, role)
        if key in seen:
            print(" Removing duplicate %s %s" % (role, contributor.name))
            _db.delete(contribution)
            continue
        seen.add(key)
        if role == 'Unknown':
            if contributor in contributors_with_roles:
                print(" Found unknown role for %s, but mystery already resolved." % contributor.name)
                _db.delete(contribution)
            else:
                print(" The role of %s is a mystery." % contributor.name)
                unresolved_mysteries[contributor] = contribution
        else:
            print(" Found %s %s" % (role, contributor.name))
            contributors_with_roles.add(contributor)
            if contributor in unresolved_mysteries:
                print(" Deleting now-resolved mystery.")
                now_resolved = unresolved_mysteries[contributor]
                resolved_mysteries.add(now_resolved)
                del unresolved_mysteries[contributor]
                _db.delete(now_resolved)

_db = production_session()
contribution2 = aliased(Contribution)


# Find Editions where one Contributor is listed both in an 'Unknown' role
# and some other role. Also find editions where one Contributor is listed
# twice in author roles.
unknown_role_or_duplicate_author_role = or_(
    and_(Contribution.role==Contributor.UNKNOWN_ROLE,
         contribution2.role != Contributor.UNKNOWN_ROLE),
    and_(
        Contribution.role.in_(Contributor.AUTHOR_ROLES),
        contribution2.role.in_(Contributor.AUTHOR_ROLES),
    )
)

qu = _db.query(Edition).join(Edition.contributions).join(
    contribution2, contribution2.edition_id==Edition.id).filter(
        contribution2.id != Contribution.id).filter(
            contribution2.contributor_id==Contribution.contributor_id
        ).filter(
            unknown_role_or_duplicate_author_role
        )

print("Fixing %s Editions." % qu.count())
qu = qu.limit(1000)
results = True
while results:
    a = time.time()
    results = qu.all()
    for ed in qu:
        #for contribution in ed.contributions:
        #    print contribution.contributor, contribution.role
        dedupe(ed)
    _db.commit()
    b = time.time()
    print("Batch processed in %.2f sec" % (b-a))
