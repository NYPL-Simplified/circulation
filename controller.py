# TODO: the feeds generated for lanes need to have search links, like so.
    # # Add a 'search' link.
    # search_link = dict(
    #     rel="search",
    #     type="application/opensearchdescription+xml",
    #     href=url_for('lane_search', lane_name=lane.name, _external=True))
    # opds_feed.add_link(**search_link)
    # add_configuration_links(opds_feed)
    # return (200,
    #         {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, 
    #         unicode(opds_feed),
    #     )



from config import Configuration
import flask
import cdn_url_for

class CirculationManagerController(object):

    def __init__(self, setup):
        self.setup = setup
        self._db = self.setup.db

    def authenticated_patron(self, barcode, pin):
        """Look up the patron authenticated by the given barcode/pin.

        If there's a problem, return a 2-tuple (URI, title) for use in a
        Problem Detail Document.

        If there's no problem, return a Patron object.
        """
        patron = self.setup.auth.authenticated_patron(
            self._db, barcode, pin
        )
        if not patron:
            return (INVALID_CREDENTIALS_PROBLEM,
                    INVALID_CREDENTIALS_TITLE)

        # Okay, we know who they are and their PIN is valid. But maybe the
        # account has expired?
        if not patron.authorization_is_active:
            return (EXPIRED_CREDENTIALS_PROBLEM,
                    EXPIRED_CREDENTIALS_TITLE)

        # No, apparently we're fine.
        return patron

    def authenticate(self, uri, title):
        """Sends a 401 response that demands basic auth."""
        data = self.setup.opds_authentication_document
        headers= { 'WWW-Authenticate' : 'Basic realm="Library card"',
                   'Content-Type' : OPDSAuthenticationDocument.MEDIA_TYPE }
        return Response(data, 401, headers)

    def load_lane(self, language, name):
        if name is None:
            lane = self.setup
        else:
            lane_name = lane_name.replace("__", "/")
            if name in self.setup.sublanes.by_name:
                lane = self.setup.sublanes.by_name[lane_name]
            else:
                return problem(NO_SUCH_LANE_PROBLEM, "No such lane: %s" % lane_name, 404)
        return lane

    def load_facets(self, request):
        arg = flask.request.args.get
        order_facet = arg('order', 'author')
        if not order_facet in ('title', 'author'):
            return problem(
                None,
                "I don't know how to order a feed by '%s'" % order_facet,
                400)


        pass

    def load_pagination(self, request):
        arg = flask.request.args.get
        size = arg('size', '50')
        try:
            size = int(size)
        except ValueError:
            return problem(None, "Invalid size: %s" % size, 400)
        size = min(size, 100)

        offset = arg('after', None)
        if offset:
            try:
                offset = int(offset)
            except ValueError:
                return problem(None, "Invalid offset: %s" % offset, 400)

        pass

    def load_licensepool(self, data_source, identifier):
        if isinstance(data_source, DataSource):
            source = data_source
        else:
            source = DataSource.lookup(self._db, data_source)
        if source is None:
            return problem(None, "No such data source: %s" % data_source, 404)

        if isinstance(identifier, Identifier):
            id_obj = identifier
        else:
            identifier_type = source.primary_identifier_type
            id_obj, ignore = Identifier.for_foreign_id(
                Conf.db, identifier_type, identifier, autocreate=False)
        if not id_obj:
            # TODO
            return problem(
                NO_LICENSES_PROBLEM, "I never heard of such a book.", 404)
        pool = id_obj.licensed_through
        return pool

    @classmethod
    def load_licensepooldelivery(cls, pool, mechanism_id):
        mechanism = get_one(
            Conf.db, LicensePoolDeliveryMechanism, license_pool=pool,
            delivery_mechanism_id=mechanism_id
        )

        if not mechanism:
            return problem(
                BAD_DELIVERY_MECHANISM_PROBLEM, 
                "Unsupported delivery mechanism for this book.",
                400
            )
        return mechanism

    @classmethod
    def apply_borrowing_policy(cls, patron, license_pool):
        if not patron.can_borrow(license_pool.work, Conf.policy):
            return problem(
                FORBIDDEN_BY_POLICY_PROBLEM, 
                "Library policy prohibits us from lending you this book.",
                451
            )

        if (license_pool.licenses_available == 0 and
            Configuration.hold_policy() !=
            Configuration.HOLD_POLICY_ALLOW
        ):
            return problem(
                FORBIDDEN_BY_POLICY_PROBLEM, 
                "Library policy prohibits the placement of holds.",
                403
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
            return redirect(cdn_url_for('acquisition_groups'))

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
        return redirect(cdn_url_for('acquisition_groups', lane_name=root_lane))


class OPDSFeedController(CirculationManagerController):

    def groups(self, languages, lane_name):
        """Build or retrieve a grouped acquisition feed."""
        lane = CirculationManager.load_lane(languages, lane_name)
        if isinstance(lane, Response):
            return lane

        annotator = CirculationManagerAnnotator(self.setup.circulation, lane)
        feed = AcquisitionFeed.groups(_db, title, url, lane, annotator)
        return feed_response(feed)

    def feed(self, languages, lane_name):
        """Build or retrieve a paginated acquisition feed."""
        lane = self.load_lane(languages, lane_name)
        if isinstance(lane, Response):
            return lane

        annotator = CirculationManagerAnnotator(Conf.circulation, lane)
        feed = AcquisitionFeed.page(
            _db, title, url, lane, annotator=annotator,
            facets=facets, pagination=pagination
        )
        return feed_response(feed)

    def search(self, languages, lane_name, query):
        lane = self.load_lane(languages, lane_name)
        this_url = url_for('lane_search', lane_name=lane_name, _external=True)
        if not query:
            # Send the search form
            return OpenSearchDocument.for_lane(lane, this_url)
        # Run a search.    
        results = lane.search(languages, query, Conf.search, 30)
        info = OpenSearchDocument.search_info(lane)
        annotator =CirculationManagerAnnotator(self.setup.circulation, lane)
        opds_feed = AcquisitionFeed(
            self._db, info['name'], 
            this_url + "?q=" + urllib.quote(query.encode("utf8")),
            results, opds_feed
        )
        return feed_response(opds_feed)

class LoanController(object):

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
                Conf.circulation.sync_bookshelf(patron, header.password)
            except Exception, e:
                # If anything goes wrong, omit the sync step and just
                # display the current active loans, as we understand them.
                Conf.log.error("ERROR DURING SYNC: %r", e, exc_info=e)

        # Then make the feed.
        feed = CirculationManagerLoanAndHoldAnnotator.active_loans_for(
            Conf.circulation, patron)
        return feed_response(feed, cache_for=None)

    def revoke(self, data_source, identifier):
        patron = flask.request.patron
        pool = _load_licensepool(data_source, identifier)
        if isinstance(pool, Response):
            return pool
        loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)
        if loan:
            hold = None
        else:
            hold = get_one(Conf.db, Hold, patron=patron, license_pool=pool)

        if not loan and not hold:
            if not pool.work:
                title = 'this book'
            else:
                title = '"%s"' % pool.work.title
            return problem(
                NO_ACTIVE_LOAN_OR_HOLD_PROBLEM, 
                'You have no active loan or hold for %s.' % title,
                404)

        pin = flask.request.authorization.password
        if loan:
            try:
                Conf.circulation.revoke_loan(patron, pin, pool)
            except RemoteRefusedReturn, e:
                uri = COULD_NOT_MIRROR_TO_REMOTE
                title = "Loan deleted locally but remote refused. Loan is likely to show up again on next sync."
                return problem(uri, title, 500)
            except CannotReturn, e:
                title = "Loan deleted locally but remote failed: %s" % str(e)
                return problem(uri, title, 500)
        elif hold:
            if not Conf.circulation.can_revoke_hold(pool, hold):
                title = "Cannot release a hold once it enters reserved state."
                return problem(CANNOT_RELEASE_HOLD_PROBLEM, title, 400)
            try:
                Conf.circulation.release_hold(patron, pin, pool)
            except CannotReleaseHold, e:
                title = "Hold released locally but remote failed: %s" % str(e)
                return problem(CANNOT_RELEASE_HOLD_PROBLEM, title, 500)

        work = pool.work
        annotator = CirculationManagerAnnotator(Conf.circulation, None)
        return entry_response(
            AcquisitionFeed.single_entry(Conf.db, work, annotator)
        )

    def detail(self, data_source, identifier):
        if flask.request.method=='DELETE':
            return self.revoke_loan_or_hold(data_source, identifier)

        patron = flask.request.patron
        pool = _load_licensepool(data_source, identifier)
        if isinstance(pool, Response):
            return pool
        loan = get_one(Conf.db, Loan, patron=patron, license_pool=pool)
        if loan:
            hold = None
        else:
            hold = get_one(Conf.db, Hold, patron=patron, license_pool=pool)

        if not loan and not hold:
            return problem(
                NO_ACTIVE_LOAN_OR_HOLD_PROBLEM, 
                'You have no active loan or hold for "%s".' % pool.work.title,
                404)

        if flask.request.method=='GET':
            if loan:
                feed = CirculationManagerLoanAndHoldAnnotator.single_loan_feed(
                    Conf.circulation, loan)
            else:
                feed = CirculationManagerLoanAndHoldAnnotator.single_hold_feed(
                    Conf.circulation, hold)
            feed = unicode(feed)
            return feed_response(feed, None)



class ServiceStatusController(object):

    def __init__(self, conf):
        self.conf = conf

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
