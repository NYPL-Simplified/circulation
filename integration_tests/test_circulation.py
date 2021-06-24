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
from axis import Axis360API

from circulation import CirculationAPI
from circulation_exceptions import *

barcode, pin, borrow_urn, hold_urn = sys.argv[1:5]
email = os.environ.get('DEFAULT_NOTIFICATION_EMAIL_ADDRESS', 'test@librarysimplified.org')

_db = production_session()
patron, ignore = get_one_or_create(
    _db, Patron, authorization_identifier=barcode)

borrow_identifier = Identifier.parse_urn(_db, borrow_urn, True)[0]
hold_identifier = Identifier.parse_urn(_db, hold_urn, True)[0]
borrow_pool = borrow_identifier.licensed_through
hold_pool = hold_identifier.licensed_through

if any(x.type == Identifier.THREEM_ID for x in [borrow_identifier, hold_identifier]):
    threem = ThreeMAPI(_db)
else:
    threem = None

if any(x.type == Identifier.OVERDRIVE_ID for x in [borrow_identifier, hold_identifier]):
    overdrive = OverdriveAPI(_db)
else:
    overdrive = None

if any(x.type == Identifier.AXIS_360_ID for x in [borrow_identifier, hold_identifier]):
    axis = Axis360API(_db)
else:
    axis = None

circulation = CirculationAPI(_db, overdrive=overdrive, threem=threem,
                             axis=axis)

activity = circulation.patron_activity(patron, pin)
print('-' * 80)
for i in activity:
    print(i)
print('-' * 80)

licensepool = borrow_pool
mechanism = licensepool.delivery_mechanisms[0]
try:
    circulation.fulfill(patron, pin, licensepool, mechanism)
except NoActiveLoan as e:
    print(" No active loan...")
circulation.borrow(patron, pin, licensepool, mechanism, email)
print("Attempting to borrow", licensepool.work)
print("Initial revoke loan")
print(circulation.revoke_loan(patron, pin, licensepool))
print("Fulfill with no loan")
try:
    circulation.fulfill(patron, pin, licensepool, mechanism)
except NoActiveLoan as e:
    print(" Exception as expected.")
print("Borrow")
print(circulation.borrow(patron, pin, licensepool, mechanism, email))
print("Borrow again!")
print(circulation.borrow(patron, pin, licensepool, mechanism, email))
print("Fulfill with loan")
print(circulation.fulfill(patron, pin, licensepool, mechanism))


licensepool = hold_pool
print("Attempting to place hold on", licensepool.work)
print("Initial release hold")
print("", circulation.release_hold(patron, pin, licensepool))
print("Creating hold.")
print("", circulation.borrow(patron, pin, licensepool, mechanism, email))
print("Creating hold again!")
try:
    print(circulation.borrow(patron, pin, licensepool, mechanism, email))
except CannotLoan as e:
    print(" Exception as expected.")
print("Attempt to fulfill hold.")
try:
    print(circulation.fulfill(patron, pin, licensepool, mechanism))
except NoActiveLoan as e:
    print(" Exception as expected")

activity = circulation.patron_activity(patron, pin)
print('-' * 80)
for i in activity:
    print(i)
print('-' * 80)

print("Revoke loan")
print(circulation.revoke_loan(patron, pin, licensepool))
print("Revoke already revoked loan")
print(circulation.revoke_loan(patron, pin, licensepool))

print("Release hold.")
print(circulation.release_hold(patron, pin, licensepool))
print("Release nonexistent hold.")
print(circulation.release_hold(patron, pin, licensepool))

