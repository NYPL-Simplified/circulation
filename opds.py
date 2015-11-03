from nose.tools import set_trace
from flask import url_for
from lxml import etree
from collections import defaultdict

from config import Configuration
from core.opds import (
    Annotator,
    AcquisitionFeed,
    E,
    OPDSFeed,
    opds_ns,
)
from core.model import (
    Session,
    BaseMaterializedWork,
)
from core.app_server import cdn_url_for

class CirculationManagerAnnotator(Annotator):

    def __init__(self, circulation, lane, active_loans_by_work={}, active_holds_by_work={}, facet_view='feed'):
        self.circulation = circulation
        self.lane = lane
        self.active_loans_by_work = active_loans_by_work
        self.active_holds_by_work = active_holds_by_work
        self.lanes_by_work = defaultdict(list)
        self.facet_view=facet_view

    def facet_url(self, order):
        if self.lane:
            lane_name = self.lane.name
        else:
            lane_name = None
        return cdn_url_for(
            self.facet_view, lane_name=lane_name, order=order, _external=True)

    def permalink_for(self, work, license_pool, identifier):
        return url_for('work', data_source=license_pool.data_source.name,
                       identifier=identifier.identifier, _external=True)

    @classmethod
    def featured_feed_url(cls, lane, order=None, cdn=True):
        if cdn:
            m = cdn_url_for
        else:
            m = url_for
        return m('feed', lane_name=lane.name, order=order, _external=True)

    @classmethod
    def navigation_feed_url(self, lane):
        if not lane:
            lane_name = None
        else:
            lane_name = lane.name
        return cdn_url_for('navigation_feed', lane_name=lane_name, _external=True)

    def active_licensepool_for(self, work):
        loan = (self.active_loans_by_work.get(work) or
                self.active_holds_by_work.get(work))
        if loan:
            # The active license pool is the one associated with
            # the loan/hold.
            return loan.license_pool
        else:

            # There is no active loan. Use the default logic for
            # determining the active license pool.
            return super(
                CirculationManagerAnnotator, self).active_licensepool_for(work)

    def group_uri(self, work, license_pool, identifier):
        if not work in self.lanes_by_work:
            return None, ""

        lanes = self.lanes_by_work[work]
        if not lanes:
            # I don't think this should ever happen?
            lane_name = None
            url = cdn_url_for('acquisition_groups', lane_name=None, _external=True)
            title = "All Books"
            return url, title

        lane = lanes[0]
        self.lanes_by_work[work] = lanes[1:]
        lane_name = None
        show_feed = False
        if isinstance(lane, tuple):
            lane, lane_name = lane
        elif isinstance(lane, dict):
            show_feed = lane.get('link_to_list_feed', show_feed)
            lane_name = lane.get('label', lane_name)
            lane = lane['lane']
        lane_name = lane_name or lane.display_name

        if isinstance(lane, basestring):
            return lane, lane_name

        # If the lane has sublanes, the URL identifying the group will
        # take the user to another set of groups for the
        # sublanes. Otherwise it will take the user to a list of the
        # books in the lane by author.
        if lane.sublanes and not show_feed:
            url = cdn_url_for('acquisition_groups', lane_name=lane.url_name, _external=True)
        else:
            url = cdn_url_for('feed', lane_name=lane.url_name, order='author', _external=True)
        return url, lane_name

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry):
        active_loan = self.active_loans_by_work.get(work)
        active_hold = self.active_holds_by_work.get(work)

        if isinstance(work, BaseMaterializedWork):
            identifier_identifier = work.identifier
            data_source_name = work.name
        else:
            identifier_identifier = active_license_pool.identifier.identifier
            data_source_name = active_license_pool.data_source.name

        # First, add a permalink.
        feed.add_link_to_entry(
            entry, 
            rel='alternate',
            href=url_for(
                'permalink', data_source=data_source_name,
                identifier=identifier_identifier, _external=True)
        )

        # Add a link for reporting problems.
        # First, add a permalink.
        feed.add_link_to_entry(
            entry, 
            rel='issues',
            href=url_for(
                'report', data_source=data_source_name,
                identifier=identifier_identifier, _external=True)
        )

        # Now we need to generate a <link> tag for every delivery mechanism
        # that has well-defined media types.

        link_tags = self.acquisition_links(
            active_license_pool, active_loan, active_hold, feed,
            data_source_name, identifier_identifier
        )
        for tag in link_tags:
            entry.append(tag)

    def acquisition_links(self, active_license_pool, active_loan, active_hold,
                          feed, data_source_name, identifier_identifier):
        """Generate a number of <link> tags that enumerate all acquisition methods."""

        links = []

        can_borrow = False
        can_fulfill = False
        can_revoke = False
        can_hold = (
            Configuration.hold_policy() == 
            Configuration.HOLD_POLICY_ALLOW
        )

        if active_loan:
            can_fulfill = True
            can_revoke = True
        elif active_hold:
            # We display the borrow link even if the patron can't
            # borrow the book right this minute.
            can_borrow = True

            can_revoke = (
                not self.circulation or 
                self.circulation.can_revoke_hold(
                    active_license_pool, active_hold)
            )
        else:
            # The patron has no existing relationship with this
            # work. Give them the opportunity to check out the work
            # or put it on hold.
            can_borrow = True

        # If there is something to be revoked for this book,
        # add a link to revoke it.
        if can_revoke:
            url = url_for(
                'revoke_loan_or_hold', data_source=data_source_name,
                identifier=identifier_identifier, _external=True)

            kw = dict(href=url, rel=OPDSFeed.REVOKE_LOAN_REL)
            revoke_link_tag = E._makeelement("link", **kw)
            links.append(revoke_link_tag)

        # Add next-step information for every useful delivery
        # mechanism.
        if can_borrow:
            # Borrowing a book gives you an OPDS entry that gives you
            # fulfillment links.
            borrow_url = url_for(
                "borrow", data_source=data_source_name,
                identifier=identifier_identifier, _external=True)
            rel = OPDSFeed.BORROW_REL
            borrow_link = AcquisitionFeed.link(
                rel=rel, href=borrow_url, type=OPDSFeed.ENTRY_TYPE
            )

            # Generate the licensing tags that tell you whether the book
            # is available.
            license_tags = feed.license_tags(
                active_license_pool, active_loan, active_hold
            )
            for t in license_tags:
                borrow_link.append(t)

            # Later in this function, we will describe each delivery
            # mechanism by appending an <indirectAcquisition> tag to
            # this link.
        else:
            borrow_link = None
        fulfill_links = []
        open_access_links = []

        for lpdm in active_license_pool.delivery_mechanisms:
            # If the default client can't process this delivery mechanism,
            # ignore it (for now)
            if not lpdm.delivery_mechanism.default_client_can_fulfill:
                continue

            # If this is an open-access delivery mechanism,
            # add an open-access link.
            if lpdm.resource and active_license_pool.open_access:
                kw = dict(rel=OPDSFeed.OPEN_ACCESS_REL, 
                          href=lpdm.resource.url)
                rep = lpdm.resource.representation
                if rep and rep.media_type:
                    kw['type'] = rep.media_type
                open_access_link_tag = AcquisitionFeed.link(**kw)
                open_access_links.append(open_access_link_tag)

            # If we end up creating an indirect acquisition tag, these
            # are the formats mentioned in the indirect acquisition.
            format_types = feed.format_types(
                lpdm.delivery_mechanism
            )

            # If we can borrow this book, add this delivery mechanism
            # to the borrow link as an indirect acquisition.
            if can_borrow and format_types:
                indirect_acquisition = feed.indirect_acquisition(format_types)
                borrow_link.append(indirect_acquisition)
            
            if can_fulfill and format_types:
                # If the loan has a distribution mechanism set, we
                # will only show the fulfillment link for that
                # mechanism.
                if (active_loan and active_loan.fulfillment
                    and active_loan.fulfillment != lpdm.delivery_mechanism):
                    continue

                # Create a new fulfillment link.
                fulfill_url = url_for(
                    "fulfill", data_source=data_source_name,
                    identifier=identifier_identifier, 
                    mechanism_id=lpdm.delivery_mechanism.id,
                    _external=True
                )
                rel=OPDSFeed.ACQUISITION_REL
                link_tag = AcquisitionFeed.acquisition_link(
                    rel=rel, href=fulfill_url,
                    types=format_types
                )
                fulfill_links.append(link_tag)

        # Open-access links and fulfillment links are always
        # available.
        for l in links:
            data = E._makeelement(
                "{%s}availability" % opds_ns, status="available"
            )
            l.append(data)

        if borrow_link is not None:
            links += [borrow_link]
        links.extend(open_access_links)
        links.extend(fulfill_links)
        
        return links

    def summary(self, work):
        """Return an HTML summary of this work."""
        if work.summary_text:
            summary = work.summary_text
            if work.summary:
                qualities.append(("Summary quality", work.summary.quality))
        elif work.summary:
            work.summary_text = work.summary.content
            summary = work.summary_text
        else:
            summary = ""
        # summary += "<ul>"
        # for name, value in qualities:
        #     if isinstance(value, basestring):
        #         summary += "<li>%s: %s</li>" % (name, value)
        #     else:
        #         summary += "<li>%s: %.1f</li>" % (name, value)
        # summary += "<li>License Source: %s</li>" % active_license_pool.data_source.name
        # summary += "</ul>"
        return summary


class CirculationManagerLoanAndHoldAnnotator(CirculationManagerAnnotator):

    # def permalink_for(self, work, license_pool, identifier):
    #     ds = license_pool.data_source.name
    #     return url_for(
    #         'loan_or_hold_detail', data_source=ds,
    #         identifier=identifier.identifier, _external=True)

    @classmethod
    def active_loans_for(cls, circulation, patron):
        db = Session.object_session(patron)
        url = url_for('active_loans', _external=True)
        active_loans_by_work = {}
        for loan in patron.loans:
            if loan.license_pool.work:
                active_loans_by_work[loan.license_pool.work] = loan
        active_holds_by_work = {}
        for hold in patron.holds:
            if hold.license_pool.work:
                active_holds_by_work[hold.license_pool.work] = hold
        annotator = cls(circulation, None, active_loans_by_work, active_holds_by_work)
        works = patron.works_on_loan_or_on_hold()
        return AcquisitionFeed(db, "Active loans and holds", url, works, annotator)

    @classmethod
    def single_loan_feed(cls, circulation, loan):
        db = Session.object_session(loan)
        work = loan.license_pool.work
        url = url_for(
            'loan_or_hold_detail', data_source=loan.license_pool.data_source.name,
            identifier=loan.license_pool.identifier.identifier, _external=True)
        active_loans_by_work = { work : loan }
        annotator = cls(circulation, None, active_loans_by_work, {})
        if not work:
            return AcquisitionFeed(
                db, "Active loan for unknown work", url, [], annotator)
        return AcquisitionFeed.single_entry(db, work, annotator)

    @classmethod
    def single_hold_feed(cls, circulation, hold):
        db = Session.object_session(hold)
        work = hold.license_pool.work
        url = url_for(
            'loan_or_hold_detail', data_source=hold.license_pool.data_source,
            identifier=hold.license_pool.identifier, _external=True)
        active_holds_by_work = { work : hold }
        annotator = cls(circulation, None, {}, active_holds_by_work)
        return AcquisitionFeed.single_entry(db, work, annotator)
