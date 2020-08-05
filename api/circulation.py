import datetime
import logging
import sys
import time
from threading import Thread

import flask
from flask_babel import lazy_gettext as _

from circulation_exceptions import *
from config import Configuration
from core.cdn import cdnify
from core.config import CannotLoadConfiguration
from core.mirror import MirrorUploader
from core.model import (
    get_one,
    CirculationEvent,
    Collection,
    ConfigurationSetting,
    DeliveryMechanism,
    ExternalIntegration,
    Library,
    LicensePoolDeliveryMechanism,
    LicensePool,
    Loan,
    Hold,
    Patron,
    RightsStatus,
    Session,
    ExternalIntegrationLink)
from util.patron import PatronUtility


class CirculationInfo(object):

    def __init__(self, collection, data_source_name, identifier_type,
                 identifier):
        """A loan, hold, or whatever.

        :param collection: The Collection that gives us the right to
        borrow this title, or the numeric database ID of the
        same. This does not have to be specified in the constructor --
        the code that instantiates CirculationInfo may not have
        access to a database connection -- but it needs to be present
        by the time the LoanInfo is connected to a LicensePool.

        :param data_source_name: The name of the data source that provides
            the LicencePool.
        :param identifier_type: The type of the Identifier associated
            with the LicensePool.
        :param identifier: The string identifying the LicensePool.

        """
        if isinstance(collection, int):
            self.collection_id = collection
        else:
            self.collection_id = collection.id
        self.data_source_name = data_source_name
        self.identifier_type = identifier_type
        self.identifier = identifier

    def collection(self, _db):
        """Find the Collection to which this object belongs."""
        return Collection.by_id(_db, self.collection_id)

    def license_pool(self, _db):
        """Find the LicensePool model object corresponding to this object."""
        collection = self.collection(_db)
        pool, is_new = LicensePool.for_foreign_id(
            _db, self.data_source_name, self.identifier_type, self.identifier,
            collection=collection
        )
        return pool

    def fd(self, d):
        # Stupid method to format a date
        if not d:
            return None
        else:
            return datetime.datetime.strftime(d, "%Y/%m/%d %H:%M:%S")


class DeliveryMechanismInfo(CirculationInfo):
    """A record of a technique that must be (but is not, currently, being)
    used to fulfill a certain loan.

    Although this class is similar to `FormatInfo` in
    core/metadata.py, usage here is strictly limited to recording
    which `LicensePoolDeliveryMechanism` a specific loan is currently
    locked to.

    If, in the course of investigating a patron's loans, you discover
    general facts about a LicensePool's availability or formats, that
    information needs to be stored in a `CirculationData` and applied to
    the LicensePool separately.
    """
    def __init__(self, content_type, drm_scheme,
                 rights_uri=RightsStatus.IN_COPYRIGHT, resource=None):
        """Constructor.

        :param content_type: Once the loan is fulfilled, the resulting document
            will be of this media type.
        :param drm_scheme: Fulfilling the loan will require negotiating this DRM
            scheme.
        :param rights_uri: Once the loan is fulfilled, the resulting
            document will be made available under this license or
            copyright regime.
        :param resource: The loan can be fulfilled by directly serving the
            content in the given `Resource`.
        """
        self.content_type = content_type
        self.drm_scheme = drm_scheme
        self.rights_uri = rights_uri
        self.resource = resource

    def apply(self, loan, autocommit=True):
        """Set an appropriate LicensePoolDeliveryMechanism on the given
        `Loan`, creating a DeliveryMechanism if necessary.

        :param loan: A Loan object.
        :param autocommit: Set this to false if you are in the middle
            of a nested transaction.
        :return: A LicensePoolDeliveryMechanism if one could be set on the
            given Loan; None otherwise.
        """
        _db = Session.object_session(loan)

        # Create or update the DeliveryMechanism.
        delivery_mechanism, is_new = DeliveryMechanism.lookup(
            _db, self.content_type,
            self.drm_scheme
        )

        if (loan.fulfillment
            and loan.fulfillment.delivery_mechanism == delivery_mechanism):
            # The work has already been done. Do nothing.
            return

        # At this point we know we need to update the local delivery
        # mechanism.
        pool = loan.license_pool
        if not pool:
            # This shouldn't happen, but bail out if it does.
            return None

        # Look up the LicensePoolDeliveryMechanism for the way the
        # server says this book is available, creating the object if
        # necessary.
        #
        # We set autocommit=False because we're probably in the middle
        # of a nested transaction.
        lpdm = LicensePoolDeliveryMechanism.set(
            pool.data_source, pool.identifier, self.content_type,
            self.drm_scheme, self.rights_uri, self.resource,
            autocommit=autocommit
        )
        loan.fulfillment = lpdm
        return lpdm


class FulfillmentInfo(CirculationInfo):
    """A record of a technique that can be used *right now* to fulfill
    a loan.
    """

    def __init__(self, collection, data_source_name, identifier_type,
                 identifier, content_link, content_type, content,
                 content_expires):
        """Constructor.

        One and only one of `content_link` and `content` should be
        provided.

        :param collection: A Collection object explaining which Collection
            the loan is found in.
        :param identifier_type: A possible value for Identifier.type indicating
            a type of identifier such as ISBN.
        :param identifier: A possible value for Identifier.identifier containing
            the identifier used to designate the item beinf fulfilled.
        :param content_link: A "next step" URL towards fulfilling the
            work. This may be a link to an ACSM file, a
            streaming-content web application, a direct download, etc.
        :param content_type: Final media type of the content, once acquired.
            E.g. EPUB_MEDIA_TYPE or
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE
        :param content: "Next step" content to be served. This may be
            the actual content of the item on loan (in which case its
            is of the type mentioned in `content_type`) or an
            intermediate document such as an ACSM file or audiobook
            manifest (in which case its media type will differ from
            `content_type`).
        :param content_expires: A time after which the "next step"
            link or content will no longer be usable.
        """
        super(FulfillmentInfo, self).__init__(
            collection, data_source_name, identifier_type, identifier
        )
        self.content_link = content_link
        self.content_type = content_type
        self.content = content
        self.content_expires = content_expires

    def __repr__(self):
        if self.content:
            blength = len(self.content)
        else:
            blength = 0
        return "<FulfillmentInfo: content_link: %r, content_type: %r, content: %d bytes, expires: %r>" % (
            self.content_link, self.content_type, blength,
            self.fd(self.content_expires))

    @property
    def as_response(self):
        """Bypass the normal process of creating a Flask Response.

        :return: A Response object, or None if you're okay with the
           normal process.
        """
        return None


class APIAwareFulfillmentInfo(FulfillmentInfo):
    """This that acts like FulfillmentInfo but is prepared to make an API
    request on demand to get data, rather than having all the data
    ready right now.

    This class is useful in situations where generating a full
    FulfillmentInfo object would be costly. We only want to incur that
    cost when the patron wants to fulfill this title and is not just
    looking at their loans.
    """
    def __init__(self, api, data_source_name, identifier_type, identifier, key):
        """Constructor.

        :param api: An object that knows how to make API requests.
        :param data_source_name: The name of the data source that's
           offering to fulfill a book.
        :param identifier: The Identifier of the book being fulfilled.
        :param key: Any special data, such as a license key, which must
           be used to fulfill the book.
        """
        self.api = api
        self.key = key
        self.collection_id = api.collection.id
        self.data_source_name = data_source_name
        self.identifier_type = identifier_type
        self.identifier = identifier

        self._fetched = False
        self._content_link = None
        self._content_type = None
        self._content = None
        self._content_expires = None

    def fetch(self):
        """It's time to tell the API that we want to fulfill this book."""
        if self._fetched:
            # We already sent the API request..
            return
        self.do_fetch()
        self._fetched = True

    def do_fetch(self):
        """Actually make the API request.

        When implemented, this method must set values for some or all
        of _content_link, _content_type, _content, and
        _content_expires.
        """
        raise NotImplementedError()

    @property
    def content_link(self):
        self.fetch()
        return self._content_link

    @property
    def content_type(self):
        self.fetch()
        return self._content_type

    @property
    def content(self):
        self.fetch()
        return self._content

    @property
    def content_expires(self):
        self.fetch()
        return self._content_expires



class LoanInfo(CirculationInfo):
    """A record of a loan."""

    def __init__(self, collection, data_source_name, identifier_type,
                 identifier, start_date, end_date,
                 fulfillment_info=None, external_identifier=None,
                 locked_to=None):
        """Constructor.

        :param start_date: A datetime reflecting when the patron borrowed the book.
        :param end_date: A datetime reflecting when the checked-out book is due.
        :param fulfillment_info: A FulfillmentInfo object representing an
            active attempt to fulfill the loan.
        :param locked_to: A DeliveryMechanismInfo object representing the
            delivery mechanism to which this loan is 'locked'.
        """
        super(LoanInfo, self).__init__(
            collection, data_source_name, identifier_type, identifier
        )
        self.start_date = start_date
        self.end_date = end_date
        self.fulfillment_info = fulfillment_info
        self.locked_to = locked_to
        self.external_identifier = external_identifier

    def __repr__(self):
        if self.fulfillment_info:
            fulfillment = " Fulfilled by: " + repr(self.fulfillment_info)
        else:
            fulfillment = ""
        f = "%Y/%m/%d"
        return "<LoanInfo for %s/%s, start=%s end=%s>%s" % (
            self.identifier_type, self.identifier,
            self.fd(self.start_date), self.fd(self.end_date),
            fulfillment
        )


class HoldInfo(CirculationInfo):
    """A record of a hold.

    :param identifier_type: Ex. Identifier.RBDIGITAL_ID.
    :param identifier: Expected to be the unicode string of the isbn, etc.
    :param start_date: When the patron made the reservation.
    :param end_date: When reserved book is expected to become available.
        Expected to be passed in date, not unicode format.
    :param hold_position:  Patron's place in the hold line. When not available,
        default to be passed is None, which is equivalent to "first in line".
    """

    def __init__(self, collection, data_source_name, identifier_type,
                 identifier, start_date, end_date, hold_position,
                 external_identifier=None):
        super(HoldInfo, self).__init__(
            collection, data_source_name, identifier_type, identifier
        )
        self.start_date = start_date
        self.end_date = end_date
        self.hold_position = hold_position
        self.external_identifier = external_identifier

    def __repr__(self):
        return "<HoldInfo for %s/%s, start=%s end=%s, position=%s>" % (
            self.identifier_type, self.identifier,
            self.fd(self.start_date), self.fd(self.end_date),
            self.hold_position
        )


class CirculationAPI(object):
    """Implement basic circulation logic and abstract away the details
    between different circulation APIs behind generic operations like
    'borrow'.
    """

    def __init__(self, _db, library, analytics=None, api_map=None):
        """Constructor.

        :param _db: A database session (probably a scoped session, which is
            why we can't derive it from `library`).

        :param library: A Library object representing the library
          whose circulation we're concerned with.

        :param analytics: An Analytics object for tracking
          circulation events.

        :param api_map: A dictionary mapping Collection protocols to
           API classes that should be instantiated to deal with these
           protocols. The default map will work fine unless you're a
           unit test.

           Since instantiating these API classes may result in API
           calls, we only instantiate one CirculationAPI per library,
           and keep them around as long as possible.
        """
        self._db = _db
        self.library_id = library.id
        self.analytics = analytics
        self.initialization_exceptions = dict()
        api_map = api_map or self.default_api_map

        # Each of the Library's relevant Collections is going to be
        # associated with an API object.
        self.api_for_collection = {}

        # When we get our view of a patron's loans and holds, we need
        # to include loans whose license pools are in one of the
        # Collections we manage. We don't need to care about loans
        # from any other Collections.
        self.collection_ids_for_sync = []

        self.log = logging.getLogger("Circulation API")
        for collection in library.collections:
            if collection.protocol in api_map:
                api = None
                try:
                    api = api_map[collection.protocol](_db, collection)
                except CannotLoadConfiguration, e:
                    self.log.error(
                        "Error loading configuration for %s: %s",
                        collection.name, e.message
                    )
                    self.initialization_exceptions[collection.id] = e
                if api:
                    self.api_for_collection[collection.id] = api
                    self.collection_ids_for_sync.append(collection.id)

    @property
    def library(self):
        return Library.by_id(self._db, self.library_id)

    @property
    def default_api_map(self):
        """When you see a Collection that implements protocol X, instantiate
        API class Y to handle that collection.
        """
        from overdrive import OverdriveAPI
        from odilo import OdiloAPI
        from bibliotheca import BibliothecaAPI
        from axis import Axis360API
        from rbdigital import RBDigitalAPI
        from enki import EnkiAPI
        from opds_for_distributors import OPDSForDistributorsAPI
        from odl import ODLAPI, SharedODLAPI
        return {
            ExternalIntegration.OVERDRIVE : OverdriveAPI,
            ExternalIntegration.ODILO : OdiloAPI,
            ExternalIntegration.BIBLIOTHECA : BibliothecaAPI,
            ExternalIntegration.AXIS_360 : Axis360API,
            ExternalIntegration.ONE_CLICK : RBDigitalAPI,
            EnkiAPI.ENKI_EXTERNAL : EnkiAPI,
            OPDSForDistributorsAPI.NAME: OPDSForDistributorsAPI,
            ODLAPI.NAME: ODLAPI,
            SharedODLAPI.NAME: SharedODLAPI,
        }

    def api_for_license_pool(self, licensepool):
        """Find the API to use for the given license pool."""
        return self.api_for_collection.get(licensepool.collection.id)

    def can_revoke_hold(self, licensepool, hold):
        """Some circulation providers allow you to cancel a hold
        when the book is reserved to you. Others only allow you to cancel
        a hold while you're in the hold queue.
        """
        if hold.position is None or hold.position > 0:
            return True
        api = self.api_for_license_pool(licensepool)
        if api.CAN_REVOKE_HOLD_WHEN_RESERVED:
            return True
        return False

    def _try_to_sign_fulfillment_link(self, licensepool, fulfillment):
        """Tries to sign the fulfilment URL (only works in the case when the collection has mirrors set up)

        :param licensepool: License pool
        :type licensepool: LicensePool

        :param fulfillment: Fulfillment info
        :type fulfillment: FulfillmentInfo

        :return: Fulfillment info with a possibly signed URL
        :rtype: FulfillmentInfo
        """
        mirror_types = [ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS]
        mirror = next(iter([
            MirrorUploader.for_collection(licensepool.collection, mirror_type)
            for mirror_type in mirror_types
        ]))

        if mirror:
            signed_url = mirror.sign_url(fulfillment.content_link)

            self.log.info(
                'Fulfilment link {0} has been signed and translated into {1}'.format(
                    fulfillment.content_link, signed_url)
            )

            fulfillment.content_link = signed_url

        return fulfillment

    def _collect_event(self, patron, licensepool, name,
                       include_neighborhood=False):
        """Collect an analytics event.

        :param patron: The Patron associated with the event. If this
            is not specified, the current request's authenticated
            patron will be used.
        :param licensepool: The LicensePool associated with the event.
        :param name: The name of the event.
        :param include_neighborhood: If this is True, _and_ the
            current request's authenticated patron is the same as the
            patron in `patron`, _and_ the authenticated patron has
            associated neighborhood information obtained from the ILS,
            then that neighborhood information (but not the patron's
            identity) will be associated with the circulation event.
        """
        if not self.analytics:
            return

        # It would be really useful to know which patron caused this
        # this event -- this will help us get a library and
        # potentially a neighborhood.
        if flask.request:
            request_patron = getattr(flask.request, 'patron', None)
        else:
            request_patron = None
        patron = patron or request_patron

        # We need to figure out which library is associated with
        # this circulation event.
        if patron:
            # The library of the patron who caused the event.
            library = patron.library
        elif flask.request:
            # The library associated with the current request.
            library = flask.request.library
        else:
            # The library associated with the CirculationAPI itself.
            library = self.library

        neighborhood = None
        if (include_neighborhood and flask.request
            and request_patron and request_patron == patron):
            neighborhood = getattr(request_patron, 'neighborhood', None)
        return self.analytics.collect_event(
            library, licensepool, name, neighborhood=neighborhood
        )

    def _collect_checkout_event(self, patron, licensepool):
        """A simple wrapper around _collect_event for handling checkouts.

        This is called in two different places -- one when loaning
        licensed books and one when 'loaning' open-access books.
        """
        return self._collect_event(
            patron, licensepool, CirculationEvent.CM_CHECKOUT,
            include_neighborhood=True
        )

    def borrow(self, patron, pin, licensepool, delivery_mechanism,
               hold_notification_email=None):
        """Either borrow a book or put it on hold. Don't worry about fulfilling
        the loan yet.

        :return: A 3-tuple (`Loan`, `Hold`, `is_new`). Either `Loan`
            or `Hold` must be None, but not both.
        """
        # Short-circuit the request if the patron lacks borrowing
        # privileges. This can happen for a few different reasons --
        # fines, blocks, expired card, etc.
        PatronUtility.assert_borrowing_privileges(patron)

        now = datetime.datetime.utcnow()
        if licensepool.open_access or licensepool.self_hosted:
            # We can 'loan' open-access content ourselves just by
            # putting a row in the database.
            now = datetime.datetime.utcnow()
            __transaction = self._db.begin_nested()
            loan, is_new = licensepool.loan_to(patron, start=now, end=None)
            __transaction.commit()
            self._collect_checkout_event(patron, licensepool)
            return loan, None, is_new

        # Okay, it's not an open-access book. This means we need to go
        # to an external service to get the book.

        api = self.api_for_license_pool(licensepool)
        if not api:
            # If there's no API for the pool, the pool is probably associated
            # with a collection that this library doesn't have access to.
            raise NoLicenses()

        must_set_delivery_mechanism = (
            api.SET_DELIVERY_MECHANISM_AT == BaseCirculationAPI.BORROW_STEP)

        if must_set_delivery_mechanism and not delivery_mechanism:
            raise DeliveryMechanismMissing()

        content_link = content_expires = None

        internal_format = api.internal_format(delivery_mechanism)

        # Do we (think we) already have this book out on loan?
        existing_loan = get_one(
             self._db, Loan, patron=patron, license_pool=licensepool,
             on_multiple='interchangeable'
        )

        loan_info = None
        hold_info = None
        if existing_loan:
            # Sync with the API to see if the loan still exists.  If
            # it does, we still want to perform a 'checkout' operation
            # on the API, because that's how loans are renewed, but
            # certain error conditions (like NoAvailableCopies) mean
            # something different if you already have a confirmed
            # active loan.

            # TODO: This would be a great place to pass in only the
            # single API that needs to be synced.
            self.sync_bookshelf(patron, pin, force=True)
            existing_loan = get_one(
                self._db, Loan, patron=patron, license_pool=licensepool,
                on_multiple='interchangeable'
            )

        new_loan = False

        # Enforce any library-specific limits on loans or holds.
        self.enforce_limits(patron, licensepool)

        # Since that didn't raise an exception, we know that the
        # patron is able to get a loan or a hold. There are race
        # conditions that will allow someone to get a hold in excess
        # of their hold limit (because we thought they were getting a
        # loan but someone else checked out the book right before we
        # got to it) but they're rare and not serious.

        # We try to check out the book even if we believe it's not
        # available -- someone else may have checked it in since we
        # last looked.
        try:
            loan_info = api.checkout(
                patron, pin, licensepool, internal_format
            )

            if isinstance(loan_info, HoldInfo):
                # If the API couldn't give us a loan, it may have given us
                # a hold instead of raising an exception.
                hold_info = loan_info
                loan_info = None
            else:
                # We asked the API to create a loan and it gave us a
                # LoanInfo object, rather than raising an exception like
                # AlreadyCheckedOut.
                #
                # For record-keeping purposes we're going to treat this as
                # a newly transacted loan, although it's possible that the
                # API does something unusual like return LoanInfo instead
                # of raising AlreadyCheckedOut.
                new_loan = True
        except AlreadyCheckedOut:
            # This is good, but we didn't get the real loan info.
            # Just fake it.
            identifier = licensepool.identifier
            loan_info = LoanInfo(
                licensepool.collection,
                licensepool.data_source,
                identifier.type,
                identifier.identifier,
                start_date=None,
                end_date=now + datetime.timedelta(hours=1)
            )
            if existing_loan:
                loan_info.external_identifier=existing_loan.external_identifier
        except AlreadyOnHold:
            # We're trying to check out a book that we already have on hold.
            hold_info = HoldInfo(
                licensepool.collection, licensepool.data_source,
                licensepool.identifier.type, licensepool.identifier.identifier,
                None, None, None
            )
        except NoAvailableCopies:
            if existing_loan:
                # The patron tried to renew a loan but there are
                # people waiting in line for them to return the book,
                # so renewals are not allowed.
                raise CannotRenew(
                    _("You cannot renew a loan if other patrons have the work on hold.")
                )
            else:
                # That's fine, we'll just (try to) place a hold.
                #
                # Since the patron incorrectly believed there were
                # copies available, update availability information
                # immediately.
                api.update_availability(licensepool)
        except NoLicenses, e:
            # Since the patron incorrectly believed there were
            # licenses available, update availability information
            # immediately.
            api.update_availability(licensepool)
            raise e

        if loan_info:
            # We successfuly secured a loan.  Now create it in our
            # database.
            __transaction = self._db.begin_nested()
            loan, new_loan_record = licensepool.loan_to(
                patron, start=loan_info.start_date or now,
                end=loan_info.end_date,
                external_identifier=loan_info.external_identifier)

            if must_set_delivery_mechanism:
                loan.fulfillment = delivery_mechanism
            existing_hold = get_one(
                self._db, Hold, patron=patron, license_pool=licensepool,
                on_multiple='interchangeable'
            )
            if existing_hold:
                # The book was on hold, and now we have a loan.
                # Delete the record of the hold.
                self._db.delete(existing_hold)
            __transaction.commit()

            if loan and new_loan:
                # Send out an analytics event to record the fact that
                # a loan was initiated through the circulation
                # manager.
                self._collect_checkout_event(patron, licensepool)
            return loan, None, new_loan_record

        # At this point we know that we neither successfully
        # transacted a loan, nor discovered a preexisting loan.

        # Checking out a book didn't work, so let's try putting
        # the book on hold.
        if not hold_info:
            try:
                hold_info = api.place_hold(
                    patron, pin, licensepool,
                    hold_notification_email
                )
            except AlreadyOnHold, e:
                hold_info = HoldInfo(
                    licensepool.collection, licensepool.data_source,
                    licensepool.identifier.type, licensepool.identifier.identifier,
                    None, None, None
                )

        # It's pretty rare that we'd go from having a loan for a book
        # to needing to put it on hold, but we do check for that case.
        __transaction = self._db.begin_nested()
        hold, is_new = licensepool.on_hold_to(
            patron,
            hold_info.start_date or now,
            hold_info.end_date,
            hold_info.hold_position,
            hold_info.external_identifier,
        )

        if hold and is_new:
            # Send out an analytics event to record the fact that
            # a hold was initiated through the circulation
            # manager.
            self._collect_event(
                patron, licensepool, CirculationEvent.CM_HOLD_PLACE
            )

        if existing_loan:
            self._db.delete(existing_loan)
        __transaction.commit()
        return None, hold, is_new

    def enforce_limits(self, patron, pool):
        """Enforce library-specific patron loan and hold limits.

        :param patron: A Patron.
        :param pool: A LicensePool the patron is trying to access. As
           a side effect, this method may update `pool` with the latest
           availability information from the remote API.
        :raises PatronLoanLimitReached: If `pool` is currently
            available but the patron is at their loan limit.
        :raises PatronHoldLimitReached: If `pool` is currently
            unavailable and the patron is at their hold limit.
        """
        at_loan_limit = self.patron_at_loan_limit(patron)
        at_hold_limit = self.patron_at_hold_limit(patron)

        if not at_loan_limit and not at_hold_limit:
            # This patron can take out either a loan or a hold, so the
            # limits don't apply.
            return

        if at_loan_limit and at_hold_limit:
            # This patron can neither take out a loan or place a hold.
            # Raise PatronLoanLimitReached for the most understandable
            # error message.
            raise PatronLoanLimitReached(library=patron.library)

        # At this point it's important that we get up-to-date
        # availability information about this LicensePool, to reduce
        # the risk that (e.g.) we apply the loan limit to a book that
        # would be placed on hold instead.
        api = self.api_for_license_pool(pool)
        api.update_availability(pool)

        currently_available = pool.licenses_available > 0
        if currently_available and at_loan_limit:
             raise PatronLoanLimitReached(library=patron.library)
        if not currently_available and at_hold_limit:
            raise PatronHoldLimitReached(library=patron.library)

    def patron_at_loan_limit(self, patron):
        """Is the given patron at their loan limit?

        This doesn't belong in Patron because the loan limit is not core functionality.
        Of course, Patron itself isn't really core functionality...

        :param patron: A Patron.
        """
        loan_limit = patron.library.setting(Configuration.LOAN_LIMIT).int_value
        if loan_limit is None:
            return False

        # Open-access loans, and loans of indefinite duration, don't count towards the loan limit
        # because they don't block anyone else.
        non_open_access_loans_with_end_date = [
            loan for loan in patron.loans
            if loan.license_pool and loan.license_pool.open_access == False and loan.end
        ]
        return loan_limit and len(non_open_access_loans_with_end_date) >= loan_limit

    def patron_at_hold_limit(self, patron):
        """Is the given patron at their hold limit?

        This doesn't belong in Patron because the hold limit is not core functionality.
        Of course, Patron itself isn't really core functionality...

        :param patron: A Patron.
        """
        hold_limit = patron.library.setting(Configuration.HOLD_LIMIT).int_value
        if hold_limit is None:
            return False
        return hold_limit and len(patron.holds) >= hold_limit

    def can_fulfill_without_loan(self, patron, pool, lpdm):
        """Can we deliver the given book in the given format to the given
        patron, even though the patron has no active loan for that
        book?

        In general this is not possible, but there are some
        exceptions, managed in subclasses of BaseCirculationAPI.

        :param patron: A Patron. This is probably None, indicating
            that someone is trying to fulfill a book without identifying
            themselves.

        :param delivery_mechanism: The LicensePoolDeliveryMechanism
            representing a format for a specific title.
        """
        if not lpdm or not pool:
            return False
        if pool.open_access:
            return True
        api = self.api_for_license_pool(pool)
        if not api:
            return False
        return api.can_fulfill_without_loan(patron, pool, lpdm)

    def fulfill(self, patron, pin, licensepool, delivery_mechanism, part=None, fulfill_part_url=None, sync_on_failure=True):
        """Fulfil a book that a patron has previously checked out.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
            explaining how the patron wants the book to be delivered. If
            the book has previously been delivered through some other
            mechanism, this parameter is ignored and the previously used
            mechanism takes precedence.

        :param part: A vendor-specific identifier indicating that the
            patron wants to fulfill one specific part of the book
            (e.g. one chapter of an audiobook), not the whole thing.

        :param fulfill_part_url: A function that takes one argument (a
            vendor-specific part identifier) and returns the URL to use
            when fulfilling that part.

        :return: A FulfillmentInfo object.

        """
        fulfillment = None
        loan = get_one(
            self._db, Loan, patron=patron, license_pool=licensepool,
            on_multiple='interchangeable'
        )
        if not loan and not self.can_fulfill_without_loan(
            patron, licensepool, delivery_mechanism
        ):
            if sync_on_failure:
                # Sync and try again.
                # TODO: Pass in only the single collection or LicensePool
                # that needs to be synced.
                self.sync_bookshelf(patron, pin, force=True)
                return self.fulfill(
                    patron, pin, licensepool=licensepool,
                    delivery_mechanism=delivery_mechanism,
                    part=part, fulfill_part_url=fulfill_part_url,
                    sync_on_failure=False
                )
            else:
                raise NoActiveLoan(_("Cannot find your active loan for this work."))
        if loan and loan.fulfillment is not None and not loan.fulfillment.compatible_with(delivery_mechanism):
            raise DeliveryMechanismConflict(
                _("You already fulfilled this loan as %(loan_delivery_mechanism)s, you can't also do it as %(requested_delivery_mechanism)s",
                  loan_delivery_mechanism=loan.fulfillment.delivery_mechanism.name,
                  requested_delivery_mechanism=delivery_mechanism.delivery_mechanism.name)
            )

        if licensepool.open_access or licensepool.self_hosted:
            # We ignore the vendor-specific arguments when doing
            # open-access fulfillment, because we just don't support
            # partial fulfillment of open-access content.
            fulfillment = self.fulfill_open_access(
                licensepool, delivery_mechanism.delivery_mechanism,
            )

            if licensepool.self_hosted:
                fulfillment = self._try_to_sign_fulfillment_link(licensepool, fulfillment)
        else:
            api = self.api_for_license_pool(licensepool)
            internal_format = api.internal_format(delivery_mechanism)

            # Here we _do_ pass in the vendor-specific arguments, but
            # we pass them in as keyword arguments, to minimize the
            # impact on implementation signatures. Most vendor APIs
            # will ignore one or more of these arguments.
            fulfillment = api.fulfill(
                patron, pin, licensepool, internal_format=internal_format,
                part=part, fulfill_part_url=fulfill_part_url
            )
            if not fulfillment or not (
                fulfillment.content_link or fulfillment.content
            ):
                raise NoAcceptableFormat()

        # Send out an analytics event to record the fact that
        # a fulfillment was initiated through the circulation
        # manager.
        self._collect_event(
            patron, licensepool, CirculationEvent.CM_FULFILL,
            include_neighborhood=True
        )

        # Make sure the delivery mechanism we just used is associated
        # with the loan, if any.
        if loan and loan.fulfillment is None and not delivery_mechanism.delivery_mechanism.is_streaming:
            __transaction = self._db.begin_nested()
            loan.fulfillment = delivery_mechanism
            __transaction.commit()

        return fulfillment

    def fulfill_open_access(self, licensepool, delivery_mechanism):
        """Fulfill an open-access LicensePool through the requested
        DeliveryMechanism.

        :param licensepool: The title to be fulfilled.
        :param delivery_mechanism: A DeliveryMechanism.
        """
        if isinstance(delivery_mechanism, LicensePoolDeliveryMechanism):
            self.log.warn("LicensePoolDeliveryMechanism passed into fulfill_open_access, should be DeliveryMechanism.")
            delivery_mechanism = delivery_mechanism.delivery_mechanism
        fulfillment = None
        for lpdm in licensepool.delivery_mechanisms:
            if not (lpdm.resource and lpdm.resource.representation
                    and lpdm.resource.representation.url):
                # This LicensePoolDeliveryMechanism can't actually
                # be used for fulfillment.
                continue
            if lpdm.delivery_mechanism == delivery_mechanism:
                # We found it! This is how the patron wants
                # the book to be delivered.
                fulfillment = lpdm
                break

        if not fulfillment:
            # There is just no way to fulfill this loan the way the
            # patron wants.
            raise FormatNotAvailable()

        rep = fulfillment.resource.representation
        if rep:
            content_link = cdnify(rep.public_url)
        else:
            content_link = cdnify(fulfillment.resource.url)
        media_type = rep.media_type
        return FulfillmentInfo(
            licensepool.collection, licensepool.data_source,
            identifier_type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier,
            content_link=content_link, content_type=media_type, content=None,
            content_expires=None,
        )

    def revoke_loan(self, patron, pin, licensepool):
        """Revoke a patron's loan for a book."""
        loan = get_one(
            self._db, Loan, patron=patron, license_pool=licensepool,
            on_multiple='interchangeable'
        )
        if loan:
            if not licensepool.open_access:
                api = self.api_for_license_pool(licensepool)
                try:
                    api.checkin(patron, pin, licensepool)
                except NotCheckedOut, e:
                    # The book wasn't checked out in the first
                    # place. Everything's fine.
                    pass

            __transaction = self._db.begin_nested()
            logging.info("In revoke_loan(), deleting loan #%d" % loan.id)
            self._db.delete(loan)
            patron.last_loan_activity_sync = None
            __transaction.commit()

            # Send out an analytics event to record the fact that
            # a loan was revoked through the circulation
            # manager.
            self._collect_event(
                patron, licensepool, CirculationEvent.CM_CHECKIN
            )

        # Any other CannotReturn exception will be propagated upwards
        # at this point.
        return True

    def release_hold(self, patron, pin, licensepool):
        """Remove a patron's hold on a book."""
        hold = get_one(
            self._db, Hold, patron=patron, license_pool=licensepool,
            on_multiple='interchangeable'
        )
        if not licensepool.open_access:
            api = self.api_for_license_pool(licensepool)
            try:
                api.release_hold(patron, pin, licensepool)
            except NotOnHold, e:
                # The book wasn't on hold in the first place. Everything's
                # fine.
                pass
        # Any other CannotReleaseHold exception will be propagated
        # upwards at this point
        if hold:
            __transaction = self._db.begin_nested()
            self._db.delete(hold)
            patron.last_loan_activity_sync = None
            __transaction.commit()

            # Send out an analytics event to record the fact that
            # a hold was revoked through the circulation
            # manager.
            self._collect_event(
                patron, licensepool, CirculationEvent.CM_HOLD_RELEASE,
            )

        return True

    def patron_activity(self, patron, pin):
        """Return a record of the patron's current activity
        vis-a-vis all relevant external loan sources.

        We check each source in a separate thread for speed.

        :return: A 2-tuple (loans, holds) containing `HoldInfo` and
            `LoanInfo` objects.
        """
        log = self.log
        class PatronActivityThread(Thread):
            def __init__(self, api, patron, pin):
                self.api = api
                self.patron = patron
                self.pin = pin
                self.activity = None
                self.exception = None
                self.trace = None
                super(PatronActivityThread, self).__init__()

            def run(self):
                before = time.time()
                try:
                    self.activity = self.api.patron_activity(
                        self.patron, self.pin)
                except Exception, e:
                    self.exception = e
                    self.trace = sys.exc_info()
                after = time.time()
                log.debug(
                    "Synced %s in %.2f sec", self.api.__class__.__name__,
                    after-before
                )

        threads = []
        before = time.time()
        for api in self.api_for_collection.values():
            thread = PatronActivityThread(api, patron, pin)
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        loans = []
        holds = []
        complete = True
        for thread in threads:
            if thread.exception:
                # Something went wrong, so we don't have a complete
                # picture of the patron's loans.
                complete = False
                self.log.error(
                    "%s errored out: %s", thread.api.__class__.__name__,
                    thread.exception,
                    exc_info=thread.trace
                )
            if thread.activity:
                for i in thread.activity:
                    l = None
                    if isinstance(i, LoanInfo):
                        l = loans
                    elif isinstance(i, HoldInfo):
                        l = holds
                    else:
                        self.log.warn(
                            "value %r from patron_activity is neither a loan nor a hold.",
                            i
                        )
                    if l is not None:
                        l.append(i)
        after = time.time()
        self.log.debug("Full sync took %.2f sec", after-before)
        return loans, holds, complete

    def local_loans(self, patron):
        return self._db.query(Loan).join(Loan.license_pool).filter(
            LicensePool.collection_id.in_(self.collection_ids_for_sync)
        ).filter(
            Loan.patron==patron
        )

    def local_holds(self, patron):
        return self._db.query(Hold).join(Hold.license_pool).filter(
            LicensePool.collection_id.in_(self.collection_ids_for_sync)
        ).filter(
            Hold.patron==patron
        )

    def sync_bookshelf(self, patron, pin, force=False):
        """Sync our internal model of a patron's bookshelf with any external
        vendors that provide books to the patron's library.

        :param patron: A Patron.
        :param pin: The password authenticating the patron; used by some vendors
           that perform a cross-check against the library ILS.
        :param force: If this is True, the method will call out to external
           vendors even if it looks like the system has up-to-date information
           about the patron.
        """
        # Get our internal view of the patron's current state.
        local_loans = self.local_loans(patron)
        local_holds = self.local_holds(patron)

        if patron.last_loan_activity_sync and not force:
            # Our local data is considered fresh, so we can return it
            # without calling out to the vendor APIs.
            return local_loans, local_holds

        # Update the external view of the patron's current state.
        remote_loans, remote_holds, complete = self.patron_activity(patron, pin)
        __transaction = self._db.begin_nested()

        now = datetime.datetime.utcnow()
        local_loans_by_identifier = {}
        local_holds_by_identifier = {}
        for l in local_loans:
            if not l.license_pool:
                self.log.error("Active loan with no license pool!")
                continue
            i = l.license_pool.identifier
            if not i:
                self.log.error(
                    "Active loan on license pool %s, which has no identifier!",
                    l.license_pool
                )
                continue
            key = (i.type, i.identifier)
            local_loans_by_identifier[key] = l
        for h in local_holds:
            if not h.license_pool:
                self.log.error("Active hold with no license pool!")
                continue
            i = h.license_pool.identifier
            if not i:
                self.log.error(
                    "Active hold on license pool %r, which has no identifier!",
                    h.license_pool
                )
                continue
            key = (i.type, i.identifier)
            local_holds_by_identifier[key] = h

        active_loans = []
        active_holds = []
        for loan in remote_loans:
            # This is a remote loan. Find or create the corresponding
            # local loan.
            pool = loan.license_pool(self._db)
            start = loan.start_date
            end = loan.end_date
            key = (loan.identifier_type, loan.identifier)
            if key in local_loans_by_identifier:
                # We already have the Loan object, we don't need to look
                # it up again.
                local_loan = local_loans_by_identifier[key]

                # But maybe the remote's opinions as to the loan's
                # start or end date have changed.
                if start:
                    local_loan.start = start
                if end:
                    local_loan.end = end
            else:
                local_loan, new = pool.loan_to(patron, start, end)

            if loan.locked_to:
                # The loan source is letting us know that the loan is
                # locked to a specific delivery mechanism. Even if
                # this is the first we've heard of this loan,
                # it may have been created in another app or through
                # a library-website integration.
                loan.locked_to.apply(local_loan, autocommit=False)
            active_loans.append(local_loan)

            # Check the local loan off the list we're keeping so we
            # don't delete it later.
            key = (loan.identifier_type, loan.identifier)
            if key in local_loans_by_identifier:
                del local_loans_by_identifier[key]

        for hold in remote_holds:
            # This is a remote hold. Find or create the corresponding
            # local hold.
            pool = hold.license_pool(self._db)
            start = hold.start_date
            end = hold.end_date
            position = hold.hold_position
            key = (hold.identifier_type, hold.identifier)
            if key in local_holds_by_identifier:
                # We already have the Hold object, we don't need to look
                # it up again.
                local_hold = local_holds_by_identifier[key]

                # But maybe the remote's opinions as to the hold's
                # start or end date have changed.
                local_hold.update(start, end, position)
            else:
                local_hold, new = pool.on_hold_to(patron, start, end, position)
            active_holds.append(local_hold)

            # Check the local hold off the list we're keeping so that
            # we don't delete it later.
            if key in local_holds_by_identifier:
                del local_holds_by_identifier[key]

        # We only want to delete local loans and holds if we were able to
        # successfully sync with all the providers. If there was an error,
        # the provider might still know about a loan or hold that we don't
        # have in the remote lists.
        if complete:
            # Every loan remaining in loans_by_identifier is a hold that
            # the provider doesn't know about. This usually means it's expired
            # and we should get rid of it, but it's possible the patron is
            # borrowing a book and syncing their bookshelf at the same time,
            # and the local loan was created after we got the remote loans.
            # If the loan's start date is less than a minute ago, we'll keep it.
            for loan in local_loans_by_identifier.values():
                if loan.license_pool.collection_id in self.collection_ids_for_sync:
                    one_minute_ago = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
                    if loan.start < one_minute_ago:
                        logging.info("In sync_bookshelf for patron %s, deleting loan %d (patron %s)" % (patron.authorization_identifier, loan.id, loan.patron.authorization_identifier))
                        self._db.delete(loan)
                    else:
                        logging.info("In sync_bookshelf for patron %s, found local loan %d created in the past minute that wasn't in remote loans" % (patron.authorization_identifier, loan.id))

            # Every hold remaining in holds_by_identifier is a hold that
            # the provider doesn't know about, which means it's expired
            # and we should get rid of it.
            for hold in local_holds_by_identifier.values():
                if hold.license_pool.collection_id in self.collection_ids_for_sync:
                    self._db.delete(hold)

        __transaction.commit()
        return active_loans, active_holds


class BaseCirculationAPI(object):
    """Encapsulates logic common to all circulation APIs."""

    # Add to LIBRARY_SETTINGS if your circulation API is for a
    # distributor which includes ebooks and allows clients to specify
    # their own loan lengths.
    EBOOK_LOAN_DURATION_SETTING = {
        "key" : Collection.EBOOK_LOAN_DURATION_KEY,
        "label": _("Ebook Loan Duration (in Days)"),
        "default": Collection.STANDARD_DEFAULT_LOAN_PERIOD,
        "type": "number",
        "description": _("When a patron uses SimplyE to borrow an ebook from this collection, SimplyE will ask for a loan that lasts this number of days. This must be equal to or less than the maximum loan duration negotiated with the distributor.")
    }

    # Add to LIBRARY_SETTINGS if your circulation API is for a
    # distributor which includes audiobooks and allows clients to
    # specify their own loan lengths.
    AUDIOBOOK_LOAN_DURATION_SETTING = {
        "key" : Collection.AUDIOBOOK_LOAN_DURATION_KEY,
        "label": _("Audiobook Loan Duration (in Days)"),
        "default": Collection.STANDARD_DEFAULT_LOAN_PERIOD,
        "type": "number",
        "description": _("When a patron uses SimplyE to borrow an audiobook from this collection, SimplyE will ask for a loan that lasts this number of days. This must be equal to or less than the maximum loan duration negotiated with the distributor.")
    }

    # Add to LIBRARY_SETTINGS if your circulation API is for a
    # distributor with a default loan period negotiated out-of-band,
    # such that the circulation manager cannot _specify_ the length of
    # a loan.
    DEFAULT_LOAN_DURATION_SETTING = {
        "key": Collection.EBOOK_LOAN_DURATION_KEY,
        "label": _("Default Loan Period (in Days)"),
        "default": Collection.STANDARD_DEFAULT_LOAN_PERIOD,
        "type": "number",
        "description": _("Until it hears otherwise from the distributor, this server will assume that any given loan for this library from this collection will last this number of days. This number is usually a negotiated value between the library and the distributor. This only affects estimates&mdash;it cannot affect the actual length of loans.")
    }

    # These collection-specific settings should be inherited by all
    # distributors.
    SETTINGS = []

    # These library- and collection-specific settings should be
    # inherited by all distributors.
    LIBRARY_SETTINGS = []

    BORROW_STEP = 'borrow'
    FULFILL_STEP = 'fulfill'

    # In 3M only, when a book is in the 'reserved' state the patron
    # cannot revoke their hold on the book.
    CAN_REVOKE_HOLD_WHEN_RESERVED = True

    # If the client must set a delivery mechanism at the point of
    # checkout (Axis 360), set this to BORROW_STEP. If the client may
    # wait til the point of fulfillment to set a delivery mechanism
    # (Overdrive), set this to FULFILL_STEP. If there is no choice of
    # delivery mechanisms (3M), set this to None.
    SET_DELIVERY_MECHANISM_AT = FULFILL_STEP

    # Different APIs have different internal names for delivery
    # mechanisms. This is a mapping of (content_type, drm_type)
    # 2-tuples to those internal names.
    #
    # For instance, the combination ("application/epub+zip",
    # "vnd.adobe/adept+xml") is called "ePub" in Axis 360 and 3M, but
    # is called "ebook-epub-adobe" in Overdrive.
    delivery_mechanism_to_internal_format = {}

    def internal_format(self, delivery_mechanism):
        """Look up the internal format for this delivery mechanism or
        raise an exception.

        :param delivery_mechanism: A LicensePoolDeliveryMechanism
        """
        if not delivery_mechanism:
            return None
        d = delivery_mechanism.delivery_mechanism
        key = (d.content_type, d.drm_scheme)
        internal_format = self.delivery_mechanism_to_internal_format.get(key)
        if not internal_format:
            raise DeliveryMechanismError(
                _("Could not map Simplified delivery mechanism %(mechanism_name)s to internal delivery mechanism!", mechanism_name=d.name)
            )
        return internal_format

    @classmethod
    def default_notification_email_address(self, library_or_patron, pin):
        """What email address should be used to notify this library's
        patrons of changes?

        :param library_or_patron: A Library or a Patron.
        """
        if isinstance(library_or_patron, Patron):
            library_or_patron = library_or_patron.library
        return ConfigurationSetting.for_library(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
            library_or_patron
        ).value

    @classmethod
    def _library_authenticator(self, library):
        """Create a LibraryAuthenticator for the given library."""
        from authenticator import LibraryAuthenticator
        _db = Session.object_session(library)
        return LibraryAuthenticator.from_config(_db, library)

    def patron_email_address(self, patron, library_authenticator=None):
        """Look up the email address that the given Patron shared
        with their library.

        We do not store this information, but some API integrations
        need it, so we give the ability to look it up as needed.

        :param patron: A Patron.
        :return: The patron's email address. None if the patron never
            shared their email address with their library, or if the
            authentication technique will not share that information
            with us.
        """
        # LibraryAuthenticator knows about all authentication techniques
        # used to identify patrons of this library.
        if not library_authenticator:
            library_authenticator = self._library_authenticator(patron.library)
        authorization_identifier = patron.authorization_identifier

        # remote_patron_lookup will try to get information about the
        # patron through each authentication technique in turn.
        # As soon as one of these techniques gives us an email
        # address, we're done.
        email_address = None
        for authenticator in library_authenticator.providers:
            try:
                patrondata = authenticator.remote_patron_lookup(patron)
            except NotImplementedError, e:
                continue
            if patrondata and patrondata.email_address:
                email_address = patrondata.email_address
        return email_address

    def checkin(self, patron, pin, licensepool):
        """  Return a book early.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's alleged password.
        :param licensepool: Contains lending info as well as link to parent Identifier.
        """
        pass


    def checkout(self, patron, pin, licensepool, internal_format):
        """Check out a book on behalf of a patron.

        :param patron: a Patron object for the patron who wants to check out the book.
        :param pin: The patron's alleged password.
        :param licensepool: Contains lending info as well as link to parent Identifier.
        :param internal_format: Represents the patron's desired book format.

        :return: a LoanInfo object.
        """
        raise NotImplementedError()

    def can_fulfill_without_loan(self, patron, pool, lpdm):
        """In general, you can't fulfill a book without a loan."""
        return False

    def fulfill(self, patron, pin, licensepool, internal_format=None,
                part=None, fulfill_part_url=None):
        """Get the actual resource file to the patron.

        Implementations are encouraged to define ``**kwargs`` as a container
        for vendor-specific arguments, so that they don't have to change
        as new arguments are added.

        :param internal_format: A vendor-specific name indicating
            the format requested by the patron.

        :param part: A vendor-specific identifier indicating that the
            patron wants to fulfill one specific part of the book
            (e.g. one chapter of an audiobook), not the whole thing.

        :param fulfill_part_url: A function that takes one argument (a
            vendor-specific part identifier) and returns the URL to use
            when fulfilling that part.

        :return: a FulfillmentInfo object.
        """
        raise NotImplementedError()


    def patron_activity(self, patron, pin):
        """ Return a patron's current checkouts and holds.
        """
        raise NotImplementedError()


    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldInfo object
        """
        raise NotImplementedError()


    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.

        :raises CannotReleaseHold: If there is an error communicating
            with the provider, or the provider refuses to release the hold for
            any reason.
        """
        raise NotImplementedError()

    def update_availability(self, licensepool):
        """Update availability information for a book.
        """
        pass
