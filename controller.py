import flask
from flask import (
    Response,
    redirect,
)

from config import Configuration
from core.app_server import (
    entry_response,
    feed_response,
)

from core.opensearch import OpenSearchDocument
from core.util.problem_detail import ProblemDetail

from core.lane import (
    Facets, 
    Pagination,
)
from core.opds import (
    OPDSFeed,
)
from core.model import (
    Complaint,
    DataSource,
    Identifier,
    LicensePoolDeliveryMechanism,
)

from problem_details import *

class CirculationManagerController(object):

    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db
        self.circulation = self.manager.circulation
        self.url_for = self.manager.url_for
        self.cdn_url_for = self.manager.cdn_url_for

    def authenticated_patron(self, barcode, pin):
        """Look up the patron authenticated by the given barcode/pin.

        If there's a problem, return a 2-tuple (URI, title) for use in a
        Problem Detail Document.

        If there's no problem, return a Patron object.
        """
        patron = self.manager.auth.authenticated_patron(
            self._db, barcode, pin
        )
        if not patron:
            return INVALID_CREDENTIALS

        # Okay, we know who they are and their PIN is valid. But maybe the
        # account has expired?
        if not patron.authorization_is_active:
            return EXPIRED_CREDENTIALS

        # No, apparently we're fine.
        return patron

    def authenticate(self, uri, title):
        """Sends a 401 response that demands basic auth."""
        data = self.manager.opds_authentication_document
        headers= { 'WWW-Authenticate' : 'Basic realm="Library card"',
                   'Content-Type' : OPDSAuthenticationDocument.MEDIA_TYPE }
        return Response(data, 401, headers)

    def load_lane(self, language_key, name):
        """Turn user input into a Lane object."""
        if language_key is None and name is None:
            # The top-level lane.
            return self.manager

        name = name.replace("__", "/")
        if not language_key in languages:
            return NO_SUCH_NAME_PROBLEM.detail(
                "Unrecognized language key: %s" % language_key
            )

        lanes = self.manager.sublanes.by_languages[language_key]
        if name not in lanes:
            return NO_SUCH_LANE_PROBLEM.detail(
                "No such lane: %s" % name
            )
        return lanes[name]

    def load_facets_from_request(self, request):
        """Figure out which Facets object this request is asking for."""
        arg = flask.request.args.get
        order = arg('order', Facets.DEFAULT_ORDER_FACET)
        return self.load_facets(order)

    def load_pagination_from_request(self):
        """Figure out which Facets object this request is asking for."""
        arg = flask.request.args.get
        size = arg('size', Pagination.DEFAULT_SIZE)
        offset = arg('after', 0)
        return load_pagination(size, offset)

    @classmethod
    def load_facets(self, order):
        """Turn user input into a Facets object."""
        if not order in Facets.ORDER_FACETS:
            return INVALID_INPUT.detail(
                "I don't know how to order a feed by '%s'" % order,
                400
            )
        return Facets(order=order)

    @classmethod
    def load_pagination(self, size, offset):
        """Turn user input into a Pagination object."""
        try:
            size = int(size)
        except ValueError:
            return INVALID_INPUT.detail("Invalid size: %s" % size)
        size = min(size, 100)
        if offset:
            try:
                offset = int(offset)
            except ValueError:
                return INVALID_INPUT.detail("Invalid offset: %s" % offset)
        return Pagination(offset, size)

    def load_licensepool(self, data_source, identifier):
        """Turn user input into a LicensePool object."""
        if isinstance(data_source, DataSource):
            source = data_source
        else:
            source = DataSource.lookup(self._db, data_source)
        if source is None:
            return INVALID_INPUT.detail("No such data source: %s" % data_source)

        if isinstance(identifier, Identifier):
            id_obj = identifier
        else:
            identifier_type = source.primary_identifier_type
            id_obj, ignore = Identifier.for_foreign_id(
                self._db, identifier_type, identifier, autocreate=False)
        if not id_obj:
            return NO_LICENSES_PROBLEM.detail("I've never heard of this work.")
        pool = id_obj.licensed_through
        return pool

    @classmethod
    def load_licensepooldelivery(cls, pool, mechanism_id):
        """Turn user input into a LicensePoolDeliveryMechanism object.""" 
        mechanism = get_one(
            self._db, LicensePoolDeliveryMechanism, license_pool=pool,
            delivery_mechanism_id=mechanism_id
        )
        return mechanism or BAD_DELIVERY_MECHANISM

    @classmethod
    def apply_borrowing_policy(cls, patron, license_pool):
        if not patron.can_borrow(license_pool.work, self.manager.policy):
            return FORBIDDEN_BY_POLICY_PROBLEM.detail(
                "Library policy prohibits us from lending you this book.",
                status_code=451
            )

        if (license_pool.licenses_available == 0 and
            Configuration.hold_policy() !=
            Configuration.HOLD_POLICY_ALLOW
        ):
            return FORBIDDEN_BY_POLICY_PROBLEM.detail(
                "Library policy prohibits the placement of holds.",
                status_code=403
            )        
        return None

    @classmethod
    def add_configuration_links(cls, feed):
        for rel, value in (
                ("terms-of-service", Configuration.terms_of_service_url()),
                ("privacy-policy", Configuration.privacy_policy_url()),
                ("copyright", Configuration.acknowledgements_url()),
        ):
            if value:
                d = dict(href=value, type="text/html", rel=rel)
                if isinstance(feed, OPDSFeed):
                    feed.add_link(**d)
                else:
                    # This is an ElementTree object.
                    link = E.link(**d)
                    feed.append(link)


class IndexController(CirculationManagerController):
    """Redirect the patron to the appropriate feed."""

    def __call__(self):
        # The simple case: the app is equally open to all clients.
        policy = Configuration.root_lane_policy()
        if not policy:
            return redirect(self.cdn_url_for('acquisition_groups'))

        # The more complex case. We must authorize the patron, check
        # their type, and redirect them to an appropriate feed.
        return appropriate_index_for_patron_type()

    @requires_auth
    def authenticated_patron_root_lane(self):
        patron = flask.request.patron
        policy = Configuration.root_lane_policy()
        return policy.get(patron.external_type)

    @requires_auth
    def appropriate_index_for_patron_type():
        root_lane = authenticated_patron_root_lane()
        return redirect(
            self.cdn_url_for(
                'acquisition_groups', 
                languages=root_lane.language_key,
                lane=lane.name
            )
        )


class OPDSFeedController(CirculationManagerController):

    def groups(self, languages, lane_name):
        """Build or retrieve a grouped acquisition feed."""
        lane = CirculationManager.load_lane(languages, lane_name)
        if isinstance(lane, ProblemDetail):
            return lane

        if lane:
            title = lane.groups_title()
        else:
            title = 'All Books'

        annotator = CirculationManagerAnnotator(self.circulation, lane)
        feed = AcquisitionFeed.groups(_db, title, url, lane, annotator)
        return feed_response(feed)

    def feed(self, languages, lane_name):
        """Build or retrieve a paginated acquisition feed."""
        lane = self.load_lane(languages, lane_name)
        if isinstance(lane, ProblemDetail):
            return lane
        url = self.cdn_url_for(
            "feed", languages=languages, lane_name=lane_name
        )
        if lane:
            title = lane.feed_title()
        else:
            title = 'All Books'
        annotator = CirculationManagerAnnotator(self.circulation, lane)
        feed = AcquisitionFeed.page(
            _db, title, url, lane, annotator=annotator,
            facets=facets, pagination=pagination
        )
        return feed_response(feed)

    def search(self, languages, lane_name, query):
        lane = self.load_lane(languages, lane_name)
        this_url = self.url_for(
            'lane_search', languages=languages, lane_name=lane_name,
        )
        if not query:
            # Send the search form
            return OpenSearchDocument.for_lane(lane, this_url)
        # Run a search.    
        results = lane.search(languages, query, self.manager.search, 30)
        info = OpenSearchDocument.search_info(lane)
        annotator = CirculationManagerAnnotator(self.circulation, lane)
        opds_feed = AcquisitionFeed(
            self._db, info['name'], 
            this_url + "?q=" + urllib.quote(query.encode("utf8")),
            results, opds_feed
        )
        return feed_response(opds_feed)

class LoanController(CirculationManagerController):

    def sync():
        if flask.request.method=='HEAD':
            return Response()

        patron = flask.request.patron

        # First synchronize our local list of loans and holds with all
        # third-party loan providers.
        if patron.authorization_identifier and len(patron.authorization_identifier) >= 7:
            # TODO: Barcodes less than 7 digits are dummy code that allow
            # the creation of arbitrary test accounts that are limited to
            # public domain books. We cannot ask Overdrive or 3M about
            # these barcodes.
            header = flask.request.authorization
            try:
                self.circulation.sync_bookshelf(patron, header.password)
            except Exception, e:
                # If anything goes wrong, omit the sync step and just
                # display the current active loans, as we understand them.
                self.manager.log.error("ERROR DURING SYNC: %r", e, exc_info=e)

        # Then make the feed.
        feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            self.circulation, patron)
        return feed_response(feed, cache_for=None)

    def borrow(self, data_source, identifier, mechanism_id=None):
        """Create a new loan or hold for a book.

        Return an OPDS Acquisition feed that includes a link of rel
        "http://opds-spec.org/acquisition", which can be used to fetch the
        book or the license file.
        """

        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            # Something went wrong.
            return pool

        # Find the delivery mechanism they asked for, if any.
        mechanism = None
        if mechanism_id:
            mechanism = self.load_licensepooldelivery(pool, mechanism_id)
            if isinstance(mechanism, Response):
                return mechanism

        if not pool:
            # I've never heard of this book.
            return NO_LICENSES_PROBLEM.detail(
                "I don't understand which work you're asking about."
            )

        patron = flask.request.patron
        problem_doc = _apply_borrowing_policy(patron, pool)
        if problem_doc:
            # As a matter of policy, the patron is not allowed to check
            # this book out.
            return problem_doc

        pin = flask.request.authorization.password
        problem_doc = None

        try:
            loan, hold, is_new = self.circulation.borrow(
                patron, pin, pool, mechanism, self.manager.hold_notification_email_address)
        except NoOpenAccessDownload, e:
            problem_doc = NO_LICENSES.detail(
                "Sorry, couldn't find an open-access download link.", 
                status_code=404
            )
        except PatronAuthorizationFailedException, e:
            problem_doc = INVALID_CREDENTIALS
        except PatronLoanLimitReached, e:
            problem_doc = LOAN_LIMIT_REACHED.detail(str(e))
        except DeliveryMechanismError, e:
            return BAD_DELIVERY_MECHANISM.detail(
                str(e), status_code=e.status_code
            )
        except CannotLoan, e:
            problem_doc = CHECKOUT_FAILED.detail(str(e))
        except CannotHold, e:
            problem_doc = HOLD_FAILED.detail(str(e))
        except CannotRenew, e:
            problem_doc = RENEW_FAILED.detail(str(e))
        except CirculationException, e:
            # Generic circulation error.
            problem_doc = CHECKOUT_FAILED.detail(str(e))

        if problem_doc:
            return problem_doc

        # At this point we have either a loan or a hold. If a loan, serve
        # a feed that tells the patron how to fulfill the loan. If a hold,
        # serve a feed that talks about the hold.
        if loan:
            feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(
                self.circulation, loan)
        elif hold:
            feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
                self.circulation, hold)
        else:
            # This should never happen -- we should have sent a more specific
            # error earlier.
            return HOLD_FAILED_PROBLEM
        self.add_configuration_links(feed)
        if isinstance(feed, OPDSFeed):
            content = unicode(feed)
        else:
            content = etree.tostring(feed)
        if is_new:
            status_code = 201
        else:
            status_code = 200
        headers = { "Content-Type" : OPDSFeed.ACQUISITION_FEED_TYPE }
        return Response(content, status_code, headers)

    def fulfill(self, data_source, identifier, mechanism_id=None):
        """Fulfill a book that has already been checked out.

        If successful, this will serve the patron a downloadable copy of
        the book, or a DRM license file which can be used to get the
        book). Alternatively, it may serve an HTTP redirect that sends the
        patron to a copy of the book or a license file.
        """
        patron = flask.request.patron
        header = flask.request.authorization
        pin = header.password
    
        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
    
        # Find the LicensePoolDeliveryMechanism they asked for.
        mechanism = None
        if mechanism_id:
            mechanism = self.load_licensepooldelivery(pool, mechanism_id)
            if isinstance(mechanism, ProblemDetail):
                return mechanism
    
        if not mechanism:
            # See if the loan already has a mechanism set. We can use that.
            loan = get_one(self._db, Loan, patron=patron, license_pool=pool)
            if loan and loan.fulfillment:
                mechanism =  loan.fulfillment
            else:
                return BAD_DELIVERY_MECHANISM.detail(
                    "You must specify a delivery mechanism to fulfill this loan."
                )
    
        try:
            fulfillment = self.circulation.fulfill(patron, pin, pool, mechanism)
        except NoActiveLoan, e:
            return NO_ACTIVE_LOAN.detail( 
                    "Can't fulfill request because you have no active loan for this work.",
                    status_code=e.status_code
            )
        except CannotFulfill, e:
            return CANNOT_FULFILL.detail(
                str(e), status_code=e.status_code
            )
        except DeliveryMechanismError, e:
            return BAD_DELIVERY_MECHANISM.detail(
                str(e), status_code=e.status_code
            )
    
        headers = dict()
        if fulfillment.content_link:
            status_code = 302
            headers["Location"] = fulfillment.content_link
        else:
            status_code = 200
        if fulfillment.content_type:
            headers['Content-Type'] = fulfillment.content_type
        return Response(fulfillment.content, status_code, headers)
    

    def revoke(self, data_source, identifier):
        patron = flask.request.patron
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, Response):
            return pool
        loan = get_one(self._db, Loan, patron=patron, license_pool=pool)
        if loan:
            hold = None
        else:
            hold = get_one(self._db, Hold, patron=patron, license_pool=pool)

        if not loan and not hold:
            if not pool.work:
                title = 'this book'
            else:
                title = '"%s"' % pool.work.title
            return NO_ACTIVE_LOAN_OR_HOLD.detail(
                'You have no active loan or hold for %s.' % title,
                status_code=404
            )

        pin = flask.request.authorization.password
        if loan:
            try:
                self.circulation.revoke_loan(patron, pin, pool)
            except RemoteRefusedReturn, e:
                title = "Loan deleted locally but remote refused. Loan is likely to show up again on next sync."
                return COULD_NOT_MIRROR_TO_REMOTE.detail(title, status_code=500)
            except CannotReturn, e:
                title = "Loan deleted locally but remote failed: %s" % str(e)
                return COULD_NOT_MIRROR_TO_REMOTE.detail(title, 500)
        elif hold:
            if not self.circulation.can_revoke_hold(pool, hold):
                title = "Cannot release a hold once it enters reserved state."
                return CANNOT_RELEASE_HOLD.detail(title, 400)
            try:
                self.circulation.release_hold(patron, pin, pool)
            except CannotReleaseHold, e:
                title = "Hold released locally but remote failed: %s" % str(e)
                return CANNOT_RELEASE_HOLD.detail(title, 500)

        work = pool.work
        annotator = CirculationManagerAnnotator(self.circulation, None)
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator)
        )

    def detail(self, data_source, identifier):
        if flask.request.method=='DELETE':
            return self.revoke_loan_or_hold(data_source, identifier)

        patron = flask.request.patron
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, ProblemDetail):
            return pool
        loan = get_one(self._db, Loan, patron=patron, license_pool=pool)
        if loan:
            hold = None
        else:
            hold = get_one(self._db, Hold, patron=patron, license_pool=pool)

        if not loan and not hold:
            return NO_ACTIVE_LOAN_OR_HOLD.detail( 
                'You have no active loan or hold for "%s".' % pool.work.title,
                status_code=404
            )

        if flask.request.method=='GET':
            if loan:
                feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(
                    self.circulation, loan)
            else:
                feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
                    self.circulation, hold)
            feed = unicode(feed)
            return feed_response(feed, None)

class WorkController(CirculationManagerController):

    def permalink(self, data_source, identifier):
        """Serve an entry for a single book.

        This does not include any loan or hold-specific information for
        the authenticated patron.

        This is different from the /works lookup protocol, in that it
        returns a single entry while the /works lookup protocol returns a
        feed containing any number of entries.
        """
        pool = self.load_licensepool(data_source, identifier)
        work = pool.work
        annotator = CirculationManagerAnnotator(self.circulation, None)
        return entry_response(
            AcquisitionFeed.single_entry(self._db, work, annotator)
        )

    def report(self, data_source, identifier):
        """Report a problem with a book."""
    
        # Turn source + identifier into a LicensePool
        pool = self.load_licensepool(data_source, identifier)
        if isinstance(pool, Response):
            # Something went wrong.
            return pool
    
        if flask.request.method == 'GET':
            # Return a list of valid URIs to use as the type of a problem detail
            # document.
            data = "\n".join(Complaint.VALID_TYPES)
            return Response(data, 200, {"Content-Type" : "text/uri-list"})
    
        data = flask.request.data
        controller = ComplaintController()
        return controller.register(pool, data)
    


class ServiceStatusController(CirculationManagerController):

    def __call__(self):
        conf = Configuration.authentication_policy()
        username = conf[Configuration.AUTHENTICATION_TEST_USERNAME]
        password = conf[Configuration.AUTHENTICATION_TEST_PASSWORD]

        template = """<!DOCTYPE HTML>
<html lang="en" class="">
<head>
<meta charset="utf8">
</head>
<body>
<ul>
%(statuses)s
</ul>
</body>
</html>
"""
        timings = dict()

        patrons = []
        def _add_timing(k, x):
            try:
                a = time.time()
                x()
                b = time.time()
                result = b-a
            except Exception, e:
                result = e
            if isinstance(result, float):
                timing = "SUCCESS: %.2fsec" % result
            else:
                timing = "FAILURE: %s" % result
            timings[k] = timing

        def do_patron():
            patron = self.conf.auth.authenticated_patron(self.conf.db, username, password)
            patrons.append(patron)
            if patron:
                return patron
            else:
                raise ValueError("Could not authenticate test patron!")

        _add_timing('Patron authentication', do_patron)

        patron = patrons[0]
        def do_overdrive():
            if not self.conf.overdrive:
                raise ValueError("Overdrive not configured")
            return self.conf.overdrive.patron_activity(patron, password)
        _add_timing('Overdrive patron account', do_overdrive)

        def do_threem():
            if not self.conf.threem:
                raise ValueError("3M not configured")
            return self.conf.threem.patron_activity(patron, password)
        _add_timing('3M patron account', do_threem)

        def do_axis():
            if not self.conf.axis:
                raise ValueError("Axis not configured")
            return self.conf.axis.patron_activity(patron, password)
        _add_timing('Axis patron account', do_axis)

        statuses = []
        for k, v in sorted(timings.items()):
            statuses.append(" <li><b>%s</b>: %s</li>" % (k, v))

        doc = template % dict(statuses="\n".join(statuses))
        return make_response(doc, 200, {"Content-Type": "text/html"})


# TODO: the feeds generated for lanes need to have search links, like so.
# This should probably go into the Annotator.
    # # Add a 'search' link.
    # search_link = dict(
    #     rel="search",
    #     type="application/opensearchdescription+xml",
    #     href=self.url_for('lane_search', lane_name=lane.name, _external=True))
    # opds_feed.add_link(**search_link)
    # add_configuration_links(opds_feed)
    # return (200,
    #         {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
    #         unicode(opds_feed),
    #     )
