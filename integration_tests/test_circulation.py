#!/usr/bin/env python
import random
import sys
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    get_one_or_create,
    production_session,
    DataSource,
    Identifier,

    LicensePool,
    Patron,
    )
from threem import ThreeMAPI
from overdrive import OverdriveAPI

from circulation import CirculationAPI
from circulation_exceptions import *

barcode, pin, borrow_urn, hold_urn = sys.argv[1:5]
email = os.environ['OVERDRIVE_NOTIFICATION_EMAIL_ADDRESS']

_db = production_session()
patron, ignore = get_one_or_create(
    _db, Patron, authorization_identifier=barcode)

borrow_pool = Identifier.parse_urn(_db, borrow_urn, True)[0].licensed_through
hold_pool = Identifier.parse_urn(_db, hold_urn, True)[0].licensed_through

threem = ThreeMAPI(_db)
overdrive = OverdriveAPI(_db)
circulation = CirculationAPI(_db, overdrive=overdrive, threem=threem)

licensepool = borrow_pool
print "Attempting to borrow", licensepool.work
print "Initial revoke loan"
print circulation.revoke_loan(patron, pin, licensepool)
print "Fulfill with no loan"
try:
    circulation.fulfill(patron, pin, licensepool)
except NoActiveLoan, e:
    print " Exception as expected."
print "Borrow"
print circulation.borrow(patron, pin, licensepool, email)
print "Borrow again!"
print circulation.borrow(patron, pin, licensepool, email)
print "Fulfill with loan"
print circulation.fulfill(patron, pin, licensepool)
print "Revoke loan"
print circulation.revoke_loan(patron, pin, licensepool)
print "Revoke already revoked loan"
print circulation.revoke_loan(patron, pin, licensepool)

licensepool = hold_pool
print "Attempting to place hold on", licensepool.work
print "Initial release hold"
print "", circulation.release_hold(patron, pin, licensepool)
print "Creating hold."
print "", circulation.borrow(patron, pin, licensepool, email)
print "Creating hold again!"
try:
    print circulation.borrow(patron, pin, licensepool, email)
except CannotLoan, e:
    print " Exception as expected."
print "Attempt to fulfill hold."
try:
    print circulation.fulfill(patron, pin, licensepool)
except NoActiveLoan, e:
    print " Exception as expected"
print "Release hold."
print circulation.release_hold(patron, pin, licensepool)
print "Release nonexistent hold."
print circulation.release_hold(patron, pin, licensepool)

