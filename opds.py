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
                    active_license_pool, active_hold))
        else:
            # The patron has no existing relationship with this
            # work. Give them the opportunity to check out the work
            # or put it on hold.
            can_borrow = True

        available = (
            active_license_pool.open_access 
            or active_license_pool.licenses_available > 0
        )

        license_tags = []
        if active_license_pool.open_access:
            open_access_url = open_access_media_type = None
            if isinstance(work, BaseMaterializedWork):
                open_access_url = work.open_access_download_url
                # TODO: This is a bad heuristic.
                if open_access_url and open_access_url.endswith('.epub'):
                    open_access_media_type = OPDSFeed.EPUB_MEDIA_TYPE
            else:
                best_pool, best_link = active_license_pool.best_license_link
                if best_link:
                    representation = best_link.representation
                    if representation.mirror_url:
                        open_access_url = representation.mirror_url
                    open_access_media_type = representation.media_type

            if open_access_url:
                kw = dict(rel=OPDSFeed.OPEN_ACCESS_REL, 
                          href=open_access_url)
                if open_access_media_type:
                    kw['type'] = open_access_media_type
                feed.add_link_to_entry(entry, **kw)
            license_tags = []
        else:
            license_tags = feed.license_tags(
                active_license_pool, active_loan, active_hold
            )

        if can_fulfill:
            fulfill_url = url_for(
                "fulfill", data_source=data_source_name,
                identifier=identifier_identifier, _external=True)
            feed.add_link_to_entry(
                entry, children=license_tags,
                rel=OPDSFeed.ACQUISITION_REL,
                href=fulfill_url
            )
            license_tags = []

        if can_borrow:
            borrow_url = url_for(
                "borrow", data_source=data_source_name,
                identifier=identifier_identifier, _external=True)
            feed.add_link_to_entry(
                entry,
                children=license_tags,
                rel=OPDSFeed.BORROW_REL,
                href=borrow_url
            )
            license_tags = []

        if can_revoke:
            url = url_for(
                'revoke_loan_or_hold', data_source=data_source_name,
                identifier=identifier_identifier, _external=True)

            feed.add_link_to_entry(
                entry, 
                rel=OPDSFeed.REVOKE_LOAN_REL,
                href=url
            )

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

