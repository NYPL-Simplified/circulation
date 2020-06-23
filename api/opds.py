import datetime
import urllib
import copy
import logging
from nose.tools import set_trace
from flask import url_for
from lxml import etree
from collections import defaultdict
import uuid

from sqlalchemy.orm import lazyload

from core.cdn import cdnify
from core.classifier import Classifier
from core.entrypoint import (
    EverythingEntryPoint,
)
from core.external_search import WorkSearchResult
from core.opds import (
    Annotator,
    AcquisitionFeed,
    UnfulfillableWork,
)
from core.util.flask_util import OPDSFeedResponse
from core.util.opds_writer import (
    OPDSFeed,
)
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    Credential,
    CustomList,
    DataSource,
    DeliveryMechanism,
    Hold,
    Identifier,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Loan,
    Patron,
    Session,
    Work,
    Edition,
)
from core.lane import (
    Lane,
    WorkList,
)
from api.lanes import (
    DynamicLane,
    CrawlableCustomListBasedLane,
    CrawlableCollectionBasedLane,
)
from core.app_server import cdn_url_for

from adobe_vendor_id import AuthdataUtility
from annotations import AnnotationWriter
from circulation import BaseCirculationAPI
from config import (
    CannotLoadConfiguration,
    Configuration,
)
from novelist import NoveListAPI
from core.analytics import Analytics

class CirculationManagerAnnotator(Annotator):

    def __init__(self, lane,
                 active_loans_by_work={}, active_holds_by_work={},
                 active_fulfillments_by_work={}, hidden_content_types=[],
                 test_mode=False):
        if lane:
            logger_name = "Circulation Manager Annotator for %s" % lane.display_name
        else:
            logger_name = "Circulation Manager Annotator"
        self.log = logging.getLogger(logger_name)
        self.lane = lane
        self.active_loans_by_work = active_loans_by_work
        self.active_holds_by_work = active_holds_by_work
        self.active_fulfillments_by_work = active_fulfillments_by_work
        self.hidden_content_types = hidden_content_types
        self.test_mode = test_mode

    def _lane_identifier(self, lane):
        if isinstance(lane, Lane):
            return lane.id
        return None

    def top_level_title(self):
        return ""

    def default_lane_url(self):
        return self.feed_url(None)

    def lane_url(self, lane):
        return self.feed_url(lane)

    def url_for(self, *args, **kwargs):
        if self.test_mode:
            new_kwargs = {}
            for k, v in kwargs.items():
                if not k.startswith('_'):
                    new_kwargs[k] = v
            return self.test_url_for(False, *args, **new_kwargs)
        else:
            return url_for(*args, **kwargs)

    def cdn_url_for(self, *args, **kwargs):
        if self.test_mode:
            return self.test_url_for(True, *args, **kwargs)
        else:
            return cdn_url_for(*args, **kwargs)

    def test_url_for(self, cdn=False, *args, **kwargs):
        # Generate a plausible-looking URL that doesn't depend on Flask
        # being set up.
        if cdn:
            host = 'cdn'
        else:
            host = 'host'
        url = ("http://%s/" % host) + "/".join(args)
        connector = '?'
        for k, v in sorted(kwargs.items()):
            if v is None:
                v = ''
            v = urllib.quote(str(v))
            k = urllib.quote(str(k))
            url += connector + "%s=%s" % (k, v)
            connector = '&'
        return url

    def facet_url(self, facets):
        return self.feed_url(self.lane, facets=facets, default_route=self.facet_view)

    def feed_url(self, lane, facets=None, pagination=None, default_route='feed', extra_kwargs=None):
        if (isinstance(lane, WorkList) and
            hasattr(lane, 'url_arguments')):
            route, kwargs = lane.url_arguments
        else:
            route = default_route
            lane_identifier = self._lane_identifier(lane)
            kwargs = dict(lane_identifier=lane_identifier)
        if facets != None:
            kwargs.update(dict(facets.items()))
        if pagination != None:
            kwargs.update(dict(pagination.items()))
        if extra_kwargs:
            kwargs.update(extra_kwargs)
        return self.cdn_url_for(route, _external=True, **kwargs)

    def navigation_url(self, lane):
        return self.cdn_url_for(
            "navigation_feed", lane_identifier=self._lane_identifier(lane),
            library_short_name=lane.library.short_name, _external=True)

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

    def visible_delivery_mechanisms(self, licensepool):
        """Filter the given `licensepool`'s LicensePoolDeliveryMechanisms
        to those with content types that are not hidden.
        """
        hidden = self.hidden_content_types
        for lpdm in licensepool.delivery_mechanisms:
            mechanism = lpdm.delivery_mechanism
            if not mechanism:
                # This shouldn't happen, but just in case.
                continue
            if mechanism.content_type in hidden:
                continue
            yield lpdm

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry, updated=None):
        # If ElasticSearch included a more accurate last_update_time,
        # use it instead of Work.last_update_time
        updated = work.last_update_time
        if isinstance(work, WorkSearchResult):
            # Elasticsearch puts this field in a list, but we've set it up
            # so there will be at most one value.
            last_updates = getattr(work._hit, 'last_update', [])
            if last_updates:
                # last_update is seconds-since epoch; convert to UTC datetime.
                updated = datetime.datetime.utcfromtimestamp(last_updates[0])

                # There's a chance that work.last_updated has been
                # modified but the change hasn't made it to the search
                # engine yet. Even then, we stick with the search
                # engine value, because a sorted list is more
                # important to the import process than an up-to-date
                # 'last update' value.

        super(CirculationManagerAnnotator, self).annotate_work_entry(
            work, active_license_pool, edition, identifier, feed, entry, updated
        )
        active_loan = self.active_loans_by_work.get(work)
        active_hold = self.active_holds_by_work.get(work)
        active_fulfillment = self.active_fulfillments_by_work.get(work)

        # Now we need to generate a <link> tag for every delivery mechanism
        # that has well-defined media types.
        link_tags = self.acquisition_links(
            active_license_pool, active_loan, active_hold, active_fulfillment,
            feed, identifier
        )
        for tag in link_tags:
            entry.append(tag)

    def acquisition_links(
            self, active_license_pool, active_loan, active_hold,
            active_fulfillment, feed, identifier, can_hold=True,
            can_revoke_hold=True, set_mechanism_at_borrow=False,
            direct_fulfillment_delivery_mechanisms=[]
    ):
        """Generate a number of <link> tags that enumerate all acquisition
        methods.

        :param direct_fulfillment_delivery_mechanisms: A way to
            fulfill each LicensePoolDeliveryMechanism in this list will be
            presented as a link with
            rel="http://opds-spec.org/acquisition/open-access", indicating
            that it can be downloaded with no intermediate steps such as
            authentication.
        """
        can_borrow = False
        can_fulfill = False
        can_revoke = False

        if active_loan:
            can_fulfill = True
            can_revoke = True
        elif active_hold:
            # We display the borrow link even if the patron can't
            # borrow the book right this minute.
            can_borrow = True

            can_revoke = can_revoke_hold
        elif active_fulfillment:
            can_fulfill = True
            can_revoke = True
        else:
            # The patron has no existing relationship with this
            # work. Give them the opportunity to check out the work
            # or put it on hold.
            can_borrow = True

        # If there is something to be revoked for this book,
        # add a link to revoke it.
        revoke_links = []
        if can_revoke:
            revoke_links.append(self.revoke_link(active_license_pool, active_loan, active_hold))

        # Add next-step information for every useful delivery
        # mechanism.
        borrow_links = []
        if can_borrow:
            # Borrowing a book gives you an OPDS entry that gives you
            # fulfillment links for every visible delivery mechanism.
            visible_mechanisms = self.visible_delivery_mechanisms(
                active_license_pool
            )
            if set_mechanism_at_borrow and active_license_pool:
                # The ebook distributor requires that the delivery
                # mechanism be set at the point of checkout. This means
                # a separate borrow link for each mechanism.
                for mechanism in visible_mechanisms:
                    borrow_links.append(
                        self.borrow_link(
                            active_license_pool,
                            mechanism, [mechanism],
                            active_hold
                        )
                    )
            elif active_license_pool:
                # The ebook distributor does not require that the
                # delivery mechanism be set at the point of
                # checkout. This means a single borrow link with
                # indirectAcquisition tags for every visible delivery
                # mechanism. If a delivery mechanism must be set, it
                # will be set at the point of fulfillment.
                borrow_links.append(
                    self.borrow_link(
                        active_license_pool,
                        None, visible_mechanisms,
                        active_hold
                    )
                )

            # Generate the licensing tags that tell you whether the book
            # is available.
            for link in borrow_links:
                if link is not None:
                    for t in feed.license_tags(
                        active_license_pool, active_loan, active_hold
                    ):
                        link.append(t)

        # Add links for fulfilling an active loan.
        fulfill_links = []
        if can_fulfill:
            if active_fulfillment:
                # We're making an entry for a specific fulfill link.
                type = active_fulfillment.content_type
                url = active_fulfillment.content_link
                rel = OPDSFeed.ACQUISITION_REL
                link_tag = AcquisitionFeed.acquisition_link(
                    rel=rel, href=url, types=[type])
                fulfill_links.append(link_tag)

            elif active_loan and active_loan.fulfillment:
                # The delivery mechanism for this loan has been
                # set. There is one link for the delivery mechanism
                # that was locked in, and links for any streaming
                # delivery mechanisms.
                #
                # Since the delivery mechanism has already been locked in,
                # we choose not to use visible_delivery_mechanisms --
                # they already chose it and they're stuck with it.
                for lpdm in active_license_pool.delivery_mechanisms:
                    if (lpdm is active_loan.fulfillment 
                        or lpdm.delivery_mechanism.is_streaming
                        or lpdm.delivery_mechanism.always_available):
                        fulfill_links.append(
                            self.fulfill_link(
                                active_license_pool,
                                active_loan,
                                lpdm.delivery_mechanism
                            )
                        )
            else:
                # The delivery mechanism for this loan has not been
                # set. There is one fulfill link for every visible
                # delivery mechanism.
                for lpdm in self.visible_delivery_mechanisms(
                        active_license_pool
                ):
                    fulfill_links.append(
                        self.fulfill_link(
                            active_license_pool,
                            active_loan,
                            lpdm.delivery_mechanism
                        )
                    )

        open_access_links = []
        for lpdm in direct_fulfillment_delivery_mechanisms:
            # These links use the OPDS 'open-access' link relation not
            # because they are open access in the licensing sense, but
            # because they are ways to download the book "without any
            # requirement, which includes payment and registration."
            #
            # To avoid confusion, we explicitly add a dc:rights
            # statement to each link explaining what the rights are to
            # this title.
            direct_fulfill = self.fulfill_link(
                active_license_pool,
                active_loan,
                lpdm.delivery_mechanism,
                rel=OPDSFeed.OPEN_ACCESS_REL
            )
            direct_fulfill.attrib.update(self.rights_attributes(lpdm))
            open_access_links.append(direct_fulfill)

        # If this is an open-access book, add an open-access link for
        # every delivery mechanism with an associated resource.
        if active_license_pool and active_license_pool.open_access:
            for lpdm in active_license_pool.delivery_mechanisms:
                if lpdm.resource:
                    open_access_links.append(self.open_access_link(active_license_pool, lpdm))

        return [x for x in borrow_links + fulfill_links + open_access_links + revoke_links
                if x is not None]

    def revoke_link(self, active_license_pool, active_loan, active_hold):
        return None

    def borrow_link(self, active_license_pool,
                    borrow_mechanism, fulfillment_mechanisms, active_hold=None):
        return None

    def fulfill_link(self, license_pool, active_loan, delivery_mechanism,
                     rel=OPDSFeed.ACQUISITION_REL):
        return None

    def open_access_link(self, pool, lpdm):
        _db = Session.object_session(lpdm)
        url = cdnify(lpdm.resource.url)
        kw = dict(rel=OPDSFeed.OPEN_ACCESS_REL, type='')

        # Start off assuming that the URL associated with the
        # LicensePoolDeliveryMechanism's Resource is the URL we should
        # send for download purposes. This will be the case unless we
        # previously mirrored that URL somewhere else.
        href = lpdm.resource.url

        rep = lpdm.resource.representation
        if rep:
            if rep.media_type:
                kw['type'] = rep.media_type
            href = rep.public_url
        kw['href'] = cdnify(href)
        link_tag = AcquisitionFeed.link(**kw)
        link_tag.attrib.update(self.rights_attributes(lpdm))
        always_available = OPDSFeed.makeelement(
            "{%s}availability" % OPDSFeed.OPDS_NS, status="available"
        )
        link_tag.append(always_available)
        return link_tag

    def rights_attributes(self, lpdm):
        """Create a dictionary of tag attributes that explain the
        rights status of a LicensePoolDeliveryMechanism.

        If nothing is known, the dictionary will be empty.
        """
        if not lpdm or not lpdm.rights_status or not lpdm.rights_status.uri:
            return {}
        rights_attr = "{%s}rights" % OPDSFeed.DCTERMS_NS
        return {rights_attr : lpdm.rights_status.uri }

    @classmethod
    def _single_entry_response(
        cls, _db, work, annotator, url, feed_class=AcquisitionFeed, **response_kwargs
    ):
        """Helper method to create an OPDSEntryResponse for a single OPDS entry.

        :param _db: A database connection.
        :param work: A Work
        :param annotator: An Annotator
        :param url: The URL of the feed to be served. Used only if there's
            a problem with the Work.
        :param feed_class: A replacement for AcquisitionFeed, for use in tests.
        :param response_kwargs: A set of extra keyword arguments to
            be passed into the OPDSEntryResponse constructor.

        :return: An OPDSEntryResponse if everything goes well; otherwise an OPDSFeedResponse
            containing an error message.
        """
        if not work:
            return feed_class(
                _db, title="Unknown work", url=url, works=[],
                annotator=annotator
            ).as_error_response()

        # This method is generally used for reporting the results of
        # authenticated transactions such as borrowing and hold
        # placement.
        #
        # This means the document contains up-to-date information
        # specific to the authenticated client. The client should
        # cache this document for a while, but no one else should
        # cache it.
        response_kwargs.setdefault('max_age', 30*60)
        response_kwargs.setdefault('private', True)
        return feed_class.single_entry(_db, work, annotator, **response_kwargs)


class LibraryAnnotator(CirculationManagerAnnotator):

    TERMS_OF_SERVICE = Configuration.TERMS_OF_SERVICE
    PRIVACY_POLICY = Configuration.PRIVACY_POLICY
    COPYRIGHT = Configuration.COPYRIGHT
    ABOUT = Configuration.ABOUT
    LICENSE = Configuration.LICENSE
    REGISTER = Configuration.REGISTER

    CONFIGURATION_LINKS = [
        TERMS_OF_SERVICE,
        PRIVACY_POLICY,
        COPYRIGHT,
        ABOUT,
        LICENSE,
    ]

    HELP_LINKS = [
        Configuration.HELP_EMAIL,
        Configuration.HELP_WEB,
        Configuration.HELP_URI,
    ]

    def __init__(self, circulation, lane, library, patron=None,
                 active_loans_by_work={}, active_holds_by_work={},
                 active_fulfillments_by_work={},
                 facet_view='feed',
                 test_mode=False,
                 top_level_title="All Books",
                 library_identifies_patrons = True,
                 facets=None
    ):
        """Constructor.

        :param library_identifies_patrons: A boolean indicating
          whether or not this library can distinguish between its
          patrons. A library might not authenticate patrons at
          all, or it might distinguish patrons from non-patrons in a
          way that does not allow it to keep track of individuals.

          If this is false, links that imply the library can
          distinguish between patrons will not be included. Depending
          on the configured collections, some extra links may be
          added, for direct acquisition of titles that would normally
          require a loan.
        """
        super(LibraryAnnotator, self).__init__(
            lane, active_loans_by_work=active_loans_by_work,
            active_holds_by_work=active_holds_by_work,
            active_fulfillments_by_work=active_fulfillments_by_work,
            hidden_content_types=self._hidden_content_types(library),
            test_mode=test_mode
        )
        self.circulation = circulation
        self.library = library
        self.patron = patron
        self.lanes_by_work = defaultdict(list)
        self.facet_view = facet_view
        self._adobe_id_tags = {}
        self._top_level_title = top_level_title
        self.identifies_patrons = library_identifies_patrons
        self.facets = facets or None

    @classmethod
    def _hidden_content_types(self, library):
        """Find all content types which this library should not be
        presenting to patrons.

        This is stored as a per-library setting.
        """
        if not library:
            # This shouldn't happen, but we shouldn't crash if it does.
            return []
        setting = library.setting(Configuration.HIDDEN_CONTENT_TYPES)
        if not setting or not setting.value:
            return []
        try:
            hidden_types = setting.json_value
        except ValueError:
            hidden_types = setting.value
        hidden_types = hidden_types or []
        if isinstance(hidden_types, basestring):
            hidden_types = [hidden_types]
        elif not isinstance(hidden_types, list):
            hidden_types = list(hidden_types)
        return hidden_types

    def top_level_title(self):
        return self._top_level_title

    def permalink_for(self, work, license_pool, identifier):
        url = self.url_for(
            'permalink',
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            library_short_name=self.library.short_name,
            _external=True
        )
        return url, OPDSFeed.ENTRY_TYPE

    def groups_url(self, lane, facets=None):
        lane_identifier = self._lane_identifier(lane)
        if facets:
            kwargs = dict(facets.items())
        else:
            kwargs = {}

        return self.cdn_url_for(
            "acquisition_groups",
            lane_identifier=lane_identifier,
            library_short_name=self.library.short_name,
            _external=True,
            **kwargs
        )

    def default_lane_url(self, facets=None):
        return self.groups_url(None, facets=facets)

    def feed_url(self, lane, facets=None, pagination=None, default_route='feed'):
        extra_kwargs = dict()
        if self.library:
            extra_kwargs['library_short_name']=self.library.short_name
        return super(LibraryAnnotator, self).feed_url(lane, facets, pagination, default_route, extra_kwargs)

    def search_url(self, lane, query, pagination, facets=None):
        lane_identifier = self._lane_identifier(lane)
        kwargs = dict(q=query)
        if facets:
            kwargs.update(dict(facets.items()))
        if pagination:
            kwargs.update(dict(pagination.items()))
        return self.url_for(
            "lane_search", lane_identifier=lane_identifier,
            library_short_name=self.library.short_name,
            _external=True, **kwargs)

    def group_uri(self, work, license_pool, identifier):
        if not work in self.lanes_by_work:
            return None, ""

        lanes = self.lanes_by_work[work]
        if not lanes:
            # I don't think this should ever happen?
            lane_name = None
            url = self.cdn_url_for('acquisition_groups', lane_identifier=None,
                                   library_short_name=self.library.short_name, _external=True)
            title = "All Books"
            return url, title

        lane = lanes[0]
        self.lanes_by_work[work] = lanes[1:]
        lane_name = ''
        show_feed = False

        if isinstance(lane, dict):
            show_feed = lane.get('link_to_list_feed', show_feed)
            title = lane.get('label', lane_name)
            lane = lane['lane']

        if isinstance(lane, basestring):
            return lane, lane_name

        if hasattr(lane, 'display_name') and not title:
            title = lane.display_name

        if show_feed:
            return self.feed_url(lane, self.facets), title

        return self.lane_url(lane, self.facets), title

    def lane_url(self, lane, facets=None):
        # If the lane has sublanes, the URL identifying the group will
        # take the user to another set of groups for the
        # sublanes. Otherwise it will take the user to a list of the
        # books in the lane by author.

        if lane and isinstance(lane, Lane) and lane.sublanes:
            url = self.groups_url(lane, facets=facets)
        elif lane and (
            isinstance(lane, Lane)
            or isinstance(lane, DynamicLane)
            ):
            url = self.feed_url(lane, facets)
        else:
            # This lane isn't part of our lane hierarchy. It's probably
            # a WorkList created to represent the top-level. Use the top-level
            # url for it.
            url = self.default_lane_url(facets=facets)
        return url

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry):
        # Add a link for reporting problems.
        feed.add_link_to_entry(
            entry,
            rel='issues',
            href=self.url_for(
                'report',
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                library_short_name=self.library.short_name,
                _external=True
            )
        )

        super(LibraryAnnotator, self).annotate_work_entry(
            work, active_license_pool, edition, identifier, feed, entry
        )

        # Add a link to each author tag.
        self.add_author_links(work, feed, entry)

        # And a series, if there is one.
        if work.series:
            self.add_series_link(work, feed, entry)

        if NoveListAPI.is_configured(self.library):
            # If NoveList Select is configured, there might be
            # recommendations, too.
            feed.add_link_to_entry(
                entry,
                rel='recommendations',
                type=OPDSFeed.ACQUISITION_FEED_TYPE,
                title='Recommended Works',
                href=self.url_for(
                    'recommendations',
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    library_short_name=self.library.short_name,
                    _external=True
                )
            )

        # Add a link for related books if available.
        if self.related_books_available(work, self.library):
            feed.add_link_to_entry(
                entry,
                rel='related',
                type=OPDSFeed.ACQUISITION_FEED_TYPE,
                title='Recommended Works',
                href=self.url_for(
                    'related_books',
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    library_short_name=self.library.short_name,
                    _external=True
                )
            )

        # Add a link to get a patron's annotations for this book.
        if self.identifies_patrons:
            feed.add_link_to_entry(
                entry,
                rel="http://www.w3.org/ns/oa#annotationService",
                type=AnnotationWriter.CONTENT_TYPE,
                href=self.url_for(
                    'annotations_for_work',
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    library_short_name=self.library.short_name,
                    _external=True
                )
            )

        if Analytics.is_configured(self.library):
            feed.add_link_to_entry(
                entry,
                rel="http://librarysimplified.org/terms/rel/analytics/open-book",
                href=self.url_for(
                    'track_analytics_event',
                    identifier_type=identifier.type,
                    identifier=identifier.identifier,
                    event_type=CirculationEvent.OPEN_BOOK,
                    library_short_name=self.library.short_name,
                    _external=True
                )
            )

    @classmethod
    def related_books_available(cls, work, library):
        """:return: bool asserting whether related books might exist for a particular Work
        """
        contributions = work.sort_author and work.sort_author != Edition.UNKNOWN_AUTHOR

        return (contributions
                or work.series
                or NoveListAPI.is_configured(library))

    def language_and_audience_key_from_work(self, work):
        language_key = work.language

        audiences = None
        if work.audience == Classifier.AUDIENCE_CHILDREN:
            audiences = [Classifier.AUDIENCE_CHILDREN]
        elif work.audience == Classifier.AUDIENCE_YOUNG_ADULT:
            audiences = Classifier.AUDIENCES_JUVENILE
        elif work.audience == Classifier.AUDIENCE_ALL_AGES:
            audiences = [Classifier.AUDIENCE_CHILDREN,
                Classifier.AUDIENCE_ALL_AGES]
        elif work.audience in Classifier.AUDIENCES_ADULT:
            audiences = list(Classifier.AUDIENCES_NO_RESEARCH)
        elif work.audience == Classifier.AUDIENCE_RESEARCH:
            audiences = list(Classifier.AUDIENCES)
        else:
            audiences = []

        audience_key=None
        if audiences:
            audience_strings = [urllib.quote_plus(a) for a in sorted(audiences)]
            audience_key = u','.join(audience_strings)

        return language_key, audience_key

    def add_author_links(self, work, feed, entry):
        """Find all the <author> tags and add a link
        to each one that points to the author's other works.
        """
        author_tag = '{%s}author' % OPDSFeed.ATOM_NS
        author_entries = entry.findall(author_tag)

        languages, audiences = self.language_and_audience_key_from_work(work)
        for author_entry in author_entries:
            name_tag = '{%s}name' % OPDSFeed.ATOM_NS

            # A database ID would be better than a name, but the
            # <author> tag was created as part of the work's cached
            # OPDS entry, and as a rule we don't put database IDs into
            # the cached OPDS entry.
            #
            # So we take the content of the <author> tag, use it in
            # the link, and -- only if the user decides to fetch this feed
            # -- we do a little extra work to turn this name back into
            # one or more contributors.
            #
            # TODO: If we reliably had VIAF IDs for our contributors,
            # we could stick them in the <author> tags and get the
            # best of both worlds.
            contributor_name = author_entry.find(name_tag).text
            if not contributor_name:
                continue

            feed.add_link_to_entry(
                author_entry,
                rel='contributor',
                type=OPDSFeed.ACQUISITION_FEED_TYPE,
                title=contributor_name,
                href=self.url_for(
                    'contributor',
                    contributor_name=contributor_name,
                    languages=languages,
                    audiences=audiences,
                    library_short_name=self.library.short_name,
                    _external=True
                )
            )

    def add_series_link(self, work, feed, entry):
        series_tag = OPDSFeed.schema_('Series')
        series_entry = entry.find(series_tag)

        if series_entry is None:
            # There is no <series> tag, and thus nothing to annotate.
            # This probably indicates an out-of-date OPDS entry.
            work_id = work.id
            work_title = work.title
            self.log.error(
                'add_series_link() called on work %s ("%s"), which has no <schema:Series> tag in its OPDS entry.',
                work_id, work_title
            )
            return

        series_name = work.series
        languages, audiences = self.language_and_audience_key_from_work(work)
        href = self.url_for(
            'series',
            series_name=series_name,
            languages=languages,
            audiences=audiences,
            library_short_name=self.library.short_name,
            _external=True,
        )
        feed.add_link_to_entry(
            series_entry,
            rel='series',
            type=OPDSFeed.ACQUISITION_FEED_TYPE,
            title=series_name,
            href=href
        )

    def annotate_feed(self, feed, lane):
        if self.patron:
            # A patron is authenticated.
            self.add_patron(feed)
        else:
            # No patron is authenticated. Show them how to
            # authenticate (or that authentication is not supported).
            self.add_authentication_document_link(feed)

        # Add a 'search' link if the lane is searchable.
        if lane and lane.search_target:
            search_facet_kwargs = {}
            if self.facets != None:
                if self.facets.entrypoint_is_default:
                    # The currently selected entry point is a default.
                    # Rather than using it, we want the 'default' behavior
                    # for search, which is to search everything.
                    search_facets = self.facets.navigate(
                        entrypoint=EverythingEntryPoint
                    )
                else:
                    search_facets = self.facets
                search_facet_kwargs.update(dict(search_facets.items()))


            lane_identifier = self._lane_identifier(lane)
            search_url = self.url_for(
                'lane_search', lane_identifier=lane_identifier,
                library_short_name=self.library.short_name,
                _external=True, **search_facet_kwargs
            )
            search_link = dict(
                rel="search",
                type="application/opensearchdescription+xml",
                href=search_url
            )
            feed.add_link_to_feed(feed.feed, **search_link)

        if self.identifies_patrons:
            # Since this library authenticates patrons it can offer
            # a bookshelf and an annotation service.
            shelf_link = dict(
                rel="http://opds-spec.org/shelf",
                type=OPDSFeed.ACQUISITION_FEED_TYPE,
                href=self.url_for('active_loans', library_short_name=self.library.short_name, _external=True))
            feed.add_link_to_feed(feed.feed, **shelf_link)

            annotations_link = dict(
                rel="http://www.w3.org/ns/oa#annotationService",
                type=AnnotationWriter.CONTENT_TYPE,
                href=self.url_for('annotations', library_short_name=self.library.short_name, _external=True))
            feed.add_link_to_feed(feed.feed, **annotations_link)

        if lane and lane.uses_customlists:
            name = None
            if hasattr(lane, "customlists") and len(lane.customlists) == 1:
                name = lane.customlists[0].name
            else:
                _db = Session.object_session(self.library)
                customlist = lane.get_customlists(_db)
                if customlist:
                    name = customlist[0].name

            if name:
                crawlable_url = self.url_for(
                    "crawlable_list_feed", list_name=name,
                    library_short_name=self.library.short_name,
                    _external=True
                )
                crawlable_link = dict(
                    rel="http://opds-spec.org/crawlable",
                    type=OPDSFeed.ACQUISITION_FEED_TYPE,
                    href=crawlable_url,
                )
                feed.add_link_to_feed(feed.feed, **crawlable_link)

        self.add_configuration_links(feed)

    def add_configuration_links(self, feed):
        _db = Session.object_session(self.library)

        def _add_link(l):
            if isinstance(feed, OPDSFeed):
                feed.add_link_to_feed(feed.feed, **l)
            else:
                # This is an ElementTree object.
                link = OPDSFeed.link(**l)
                feed.append(link)

        for rel in self.CONFIGURATION_LINKS:
            setting = ConfigurationSetting.for_library(rel, self.library)
            if setting.value:
                d = dict(href=setting.value, type="text/html", rel=rel)
                _add_link(d)

        navigation_urls = ConfigurationSetting.for_library(
            Configuration.WEB_HEADER_LINKS, self.library).json_value
        if navigation_urls:
            navigation_labels = ConfigurationSetting.for_library(
                Configuration.WEB_HEADER_LABELS, self.library).json_value
            for (url, label) in zip(navigation_urls, navigation_labels):
                d = dict(href=url, title=label, type="text/html", rel="related", role="navigation")
                _add_link(d)

        for type, value in Configuration.help_uris(self.library):
            d = dict(href=value, rel="help")
            if type:
                d['type'] = type
            _add_link(d)

    def acquisition_links(
            self, active_license_pool, active_loan, active_hold,
            active_fulfillment, feed, identifier,
            direct_fulfillment_delivery_mechanisms=None, mock_api=None
    ):
        """Generate one or more <link> tags that can be used to borrow,
        reserve, or fulfill a book, depending on the state of the book
        and the current patron.

        :param active_license_pool: The LicensePool for which we're trying to
           generate <link> tags.
        :param active_loan: A Loan object representing the current patron's
           existing loan for this title, if any.
        :param active_hold: A Hold object representing the current patron's
           existing hold on this title, if any.
        :param active_fulfillment: A LicensePoolDeliveryMechanism object
           representing the mechanism, if any, which the patron has chosen
           to fulfill this work.
        :param feed: The OPDSFeed that will eventually contain these <link>
           tags.
        :param identifier: The Identifier of the title for which we're
           trying to generate <link> tags.
        :param direct_fulfillment_delivery_mechanisms: A list of
           LicensePoolDeliveryMechanisms for the given LicensePool
           that should have fulfillment-type <link> tags generated for
           them, even if this method wouldn't normally think that
           makes sense.
        :param mock_api: A mock object to stand in for the API to the
           vendor who provided this LicensePool. If this is not provided, a
           live API for that vendor will be used.
        """
        direct_fulfillment_delivery_mechanisms = direct_fulfillment_delivery_mechanisms or []
        api = mock_api
        if not api and self.circulation and active_license_pool:
            api = self.circulation.api_for_license_pool(active_license_pool)
        if api:
            set_mechanism_at_borrow = (
                api.SET_DELIVERY_MECHANISM_AT == BaseCirculationAPI.BORROW_STEP)
            if (active_license_pool and not self.identifies_patrons
                and not active_loan):
                for lpdm in active_license_pool.delivery_mechanisms:
                    if api.can_fulfill_without_loan(
                            None, active_license_pool, lpdm
                    ):
                        # This title can be fulfilled without an
                        # active loan, so we're going to add an acquisition
                        # link that goes directly to the fulfillment step
                        # without the 'borrow' step.
                        direct_fulfillment_delivery_mechanisms.append(lpdm)
        else:
            # This is most likely an open-access book. Just put one
            # borrow link and figure out the rest later.
            set_mechanism_at_borrow = False

        return super(LibraryAnnotator, self).acquisition_links(
            active_license_pool, active_loan, active_hold, active_fulfillment,
            feed, identifier, can_hold=self.library.allow_holds,
            can_revoke_hold=(active_hold and (not self.circulation or self.circulation.can_revoke_hold(active_license_pool, active_hold))),
            set_mechanism_at_borrow=set_mechanism_at_borrow,
            direct_fulfillment_delivery_mechanisms=direct_fulfillment_delivery_mechanisms
        )

    def revoke_link(self, active_license_pool, active_loan, active_hold):
        if not self.identifies_patrons:
            return
        url = self.url_for(
            'revoke_loan_or_hold',
            license_pool_id=active_license_pool.id,
            library_short_name=self.library.short_name,
            _external=True)
        kw = dict(href=url, rel=OPDSFeed.REVOKE_LOAN_REL)
        revoke_link_tag = OPDSFeed.makeelement("link", **kw)
        return revoke_link_tag

    def borrow_link(self, active_license_pool,
                    borrow_mechanism, fulfillment_mechanisms, active_hold=None):
        if not self.identifies_patrons:
            return
        identifier = active_license_pool.identifier
        if borrow_mechanism:
            # Following this link will both borrow the book and set
            # its delivery mechanism.
            mechanism_id = borrow_mechanism.delivery_mechanism.id
        else:
            # Following this link will borrow the book but not set
            # its delivery mechanism.
            mechanism_id = None
        borrow_url = self.url_for(
            "borrow",
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            mechanism_id=mechanism_id,
            library_short_name=self.library.short_name,
            _external=True)
        rel = OPDSFeed.BORROW_REL
        borrow_link = AcquisitionFeed.link(
            rel=rel, href=borrow_url, type=OPDSFeed.ENTRY_TYPE
        )

        indirect_acquisitions = []
        for lpdm in fulfillment_mechanisms:
            # We have information about one or more delivery
            # mechanisms that will be available at the point of
            # fulfillment. To the extent possible, put information
            # about these mechanisms into the <link> tag as
            # <opds:indirectAcquisition> tags.

            # These are the formats mentioned in the indirect
            # acquisition.
            format_types = AcquisitionFeed.format_types(lpdm.delivery_mechanism)

            # If we can borrow this book, add this delivery mechanism
            # to the borrow link as an <opds:indirectAcquisition>.
            if format_types:
                indirect_acquisition = AcquisitionFeed.indirect_acquisition(
                    format_types
                )
                indirect_acquisitions.append(indirect_acquisition)

        if not indirect_acquisitions:
            # If there's no way to actually get the book, cancel the creation
            # of an OPDS entry altogether.
            raise UnfulfillableWork()

        borrow_link.extend(indirect_acquisitions)
        return borrow_link

    def fulfill_link(self, license_pool, active_loan, delivery_mechanism,
                     rel=OPDSFeed.ACQUISITION_REL):
        """Create a new fulfillment link.

        This link may include tags from the OPDS Extensions for DRM.
        """
        if not self.identifies_patrons and rel != OPDSFeed.OPEN_ACCESS_REL:
            return
        if isinstance(delivery_mechanism, LicensePoolDeliveryMechanism):
            logging.warn("LicensePoolDeliveryMechanism passed into fulfill_link instead of DeliveryMechanism!")
            delivery_mechanism = delivery_mechanism.delivery_mechanism
        format_types = AcquisitionFeed.format_types(delivery_mechanism)
        if not format_types:
            return None

        fulfill_url = self.url_for(
            "fulfill",
            license_pool_id=license_pool.id,
            mechanism_id=delivery_mechanism.id,
            library_short_name=self.library.short_name,
            _external=True
        )

        link_tag = AcquisitionFeed.acquisition_link(
            rel=rel, href=fulfill_url,
            types=format_types
        )

        children = AcquisitionFeed.license_tags(license_pool, active_loan, None)
        link_tag.extend(children)

        children = self.drm_device_registration_tags(
            license_pool, active_loan, delivery_mechanism
        )
        link_tag.extend(children)
        return link_tag

    def open_access_link(self, pool, lpdm):
        link_tag = super(LibraryAnnotator, self).open_access_link(pool, lpdm)
        fulfill_url = self.url_for(
            "fulfill",
            license_pool_id=pool.id,
            mechanism_id=lpdm.delivery_mechanism.id,
            library_short_name=self.library.short_name,
            _external=True
        )
        link_tag.attrib.update(dict(href=fulfill_url))
        return link_tag

    def drm_device_registration_tags(self, license_pool, active_loan,
                                     delivery_mechanism):
        """Construct OPDS Extensions for DRM tags that explain how to
        register a device with the DRM server that manages this loan.
        :param delivery_mechanism: A DeliveryMechanism
        """
        if not active_loan or not delivery_mechanism or not self.identifies_patrons:
            return []

        if delivery_mechanism.drm_scheme == DeliveryMechanism.ADOBE_DRM:
            # Get an identifier for the patron that will be registered
            # with the DRM server.
            _db = Session.object_session(active_loan)
            patron = active_loan.patron

            # Generate a <drm:licensor> tag that can feed into the
            # Vendor ID service.
            return self.adobe_id_tags(patron)
        return []

    def adobe_id_tags(self, patron_identifier):
        """Construct tags using the DRM Extensions for OPDS standard that
        explain how to get an Adobe ID for this patron, and how to
        manage their list of device IDs.
        :param delivery_mechanism: A DeliveryMechanism
        :return: If Adobe Vendor ID delegation is configured, a list
        containing a <drm:licensor> tag. If not, an empty list.
        """
        # CirculationManagerAnnotators are created per request.
        # Within the context of a single request, we can cache the
        # tags that explain how the patron can get an Adobe ID, and
        # reuse them across <entry> tags. This saves a little time,
        # makes tests more reliable, and stops us from providing a
        # different Short Client Token for every <entry> tag.
        if isinstance(patron_identifier, Patron):
            cache_key = patron_identifier.id
        else:
            cache_key = patron_identifier
        cached = self._adobe_id_tags.get(cache_key)
        if cached is None:
            cached = []
            authdata = None
            try:
                authdata = AuthdataUtility.from_config(self.library)
            except CannotLoadConfiguration as e:
                logging.error("Cannot load Short Client Token configuration; outgoing OPDS entries will not have DRM autodiscovery support", exc_info=e)
                return []
            if authdata:
                vendor_id, token = authdata.short_client_token_for_patron(patron_identifier)
                drm_licensor = OPDSFeed.makeelement("{%s}licensor" % OPDSFeed.DRM_NS)
                vendor_attr = "{%s}vendor" % OPDSFeed.DRM_NS
                drm_licensor.attrib[vendor_attr] = vendor_id
                patron_key = OPDSFeed.makeelement("{%s}clientToken" % OPDSFeed.DRM_NS)
                patron_key.text = token
                drm_licensor.append(patron_key)

                # Add the link to the DRM Device Management Protocol
                # endpoint. See:
                # https://github.com/NYPL-Simplified/Simplified/wiki/DRM-Device-Management
                device_list_link = OPDSFeed.makeelement("link")
                device_list_link.attrib['rel'] = 'http://librarysimplified.org/terms/drm/rel/devices'
                device_list_link.attrib['href'] = self.url_for(
                    "adobe_drm_devices", library_short_name=self.library.short_name, _external=True
                )
                drm_licensor.append(device_list_link)
                cached = [drm_licensor]

            self._adobe_id_tags[cache_key] = cached
        else:
            cached = copy.deepcopy(cached)
        return cached

    def add_patron(self, feed_obj):
        if not self.identifies_patrons:
            return
        patron_details = {}
        if self.patron.username:
            patron_details["{%s}username" % OPDSFeed.SIMPLIFIED_NS] = self.patron.username
        if self.patron.authorization_identifier:
            patron_details["{%s}authorizationIdentifier" % OPDSFeed.SIMPLIFIED_NS] = self.patron.authorization_identifier

        patron_tag = OPDSFeed.makeelement("{%s}patron" % OPDSFeed.SIMPLIFIED_NS, patron_details)
        feed_obj.feed.append(patron_tag)

    def add_authentication_document_link(self, feed_obj):
        """Create a <link> tag that points to the circulation
        manager's Authentication for OPDS document
        for the current library.
        """
        # Even if self.identifies_patrons is false, we include this link,
        # because this document is the one that explains there is no
        # patron authentication at this library.
        feed_obj.add_link_to_feed(
            feed_obj.feed,
            rel='http://opds-spec.org/auth/document',
            href=self.url_for(
                'authentication_document',
                library_short_name=self.library.short_name, _external=True
            )
        )


class SharedCollectionAnnotator(CirculationManagerAnnotator):

    def __init__(self, collection, lane,
                 active_loans_by_work={}, active_holds_by_work={},
                 active_fulfillments_by_work={},
                 test_mode=False,
    ):
        super(SharedCollectionAnnotator, self).__init__(lane, active_loans_by_work=active_loans_by_work,
                                                        active_holds_by_work=active_holds_by_work,
                                                        active_fulfillments_by_work=active_fulfillments_by_work,
                                                        test_mode=test_mode)
        self.collection = collection

    def top_level_title(self):
        return self.collection.name

    def default_lane_url(self):
        return self.feed_url(None, default_route='crawlable_collection_feed')

    def lane_url(self, lane):
        return self.feed_url(lane, default_route='crawlable_collection_feed')

    def feed_url(self, lane, facets=None, pagination=None, default_route='feed'):
        extra_kwargs = dict(collection_name=self.collection.name)
        return super(SharedCollectionAnnotator, self).feed_url(lane, facets, pagination, default_route, extra_kwargs)

    def acquisition_links(self, active_license_pool, active_loan, active_hold, active_fulfillment,
                          feed, identifier):
        """Generate a number of <link> tags that enumerate all acquisition methods."""
        links = super(SharedCollectionAnnotator, self).acquisition_links(
            active_license_pool, active_loan, active_hold, active_fulfillment, feed, identifier)

        info_links = []
        if active_loan:
            url = self.url_for(
                'shared_collection_loan_info',
                collection_name=self.collection.name,
                loan_id=active_loan.id,
                _external=True)
            kw = dict(href=url, rel='self')
            info_link_tag = OPDSFeed.makeelement("link", **kw)
            info_links.append(info_link_tag)

        if active_hold and active_hold:
            url = self.url_for(
                'shared_collection_hold_info',
                collection_name=self.collection.name,
                hold_id=active_hold.id,
                _external=True)
            kw = dict(href=url, rel='self')
            info_link_tag = OPDSFeed.makeelement("link", **kw)
            info_links.append(info_link_tag)
        return links + info_links

    def revoke_link(self, active_license_pool, active_loan, active_hold):
        url = None
        if active_loan:
            url = self.url_for(
                'shared_collection_revoke_loan',
                collection_name=self.collection.name,
                loan_id=active_loan.id,
                _external=True)
        elif active_hold:
            url = self.url_for(
                'shared_collection_revoke_hold',
                collection_name=self.collection.name,
                hold_id=active_hold.id,
                _external=True)

        if url:
            kw = dict(href=url, rel=OPDSFeed.REVOKE_LOAN_REL)
            revoke_link_tag = OPDSFeed.makeelement("link", **kw)
            return revoke_link_tag

    def borrow_link(self, active_license_pool,
                    borrow_mechanism, fulfillment_mechanisms, active_hold=None):
        if active_license_pool.open_access:
            # No need to borrow from a shared collection when the book
            # already has an open access link.
            return None

        identifier = active_license_pool.identifier
        if active_hold:
            borrow_url = self.url_for(
                "shared_collection_borrow",
                collection_name=self.collection.name,
                hold_id=active_hold.id,
                _external=True,
            )
        else:
            borrow_url = self.url_for(
                "shared_collection_borrow",
                collection_name=self.collection.name,
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                _external=True,
            )
        rel = OPDSFeed.BORROW_REL
        borrow_link = AcquisitionFeed.link(
            rel=rel, href=borrow_url, type=OPDSFeed.ENTRY_TYPE
        )

        indirect_acquisitions = []
        for lpdm in fulfillment_mechanisms:
            # We have information about one or more delivery
            # mechanisms that will be available at the point of
            # fulfillment. To the extent possible, put information
            # about these mechanisms into the <link> tag as
            # <opds:indirectAcquisition> tags.

            # These are the formats mentioned in the indirect
            # acquisition.
            format_types = AcquisitionFeed.format_types(lpdm.delivery_mechanism)

            # If we can borrow this book, add this delivery mechanism
            # to the borrow link as an <opds:indirectAcquisition>.
            if format_types:
                indirect_acquisition = AcquisitionFeed.indirect_acquisition(
                    format_types
                )
                indirect_acquisitions.append(indirect_acquisition)

        if not indirect_acquisitions:
            # If there's no way to actually get the book, cancel the creation
            # of an OPDS entry altogether.
            raise UnfulfillableWork()

        borrow_link.extend(indirect_acquisitions)
        return borrow_link

    def fulfill_link(self, license_pool, active_loan, delivery_mechanism,
                     rel=OPDSFeed.ACQUISITION_REL):
        """Create a new fulfillment link."""
        if isinstance(delivery_mechanism, LicensePoolDeliveryMechanism):
            logging.warn("LicensePoolDeliveryMechanism passed into fulfill_link instead of DeliveryMechanism!")
            delivery_mechanism = delivery_mechanism.delivery_mechanism
        format_types = AcquisitionFeed.format_types(delivery_mechanism)
        if not format_types:
            return None

        fulfill_url = self.url_for(
            "shared_collection_fulfill",
            collection_name=license_pool.collection.name,
            loan_id=active_loan.id,
            mechanism_id=delivery_mechanism.id,
            _external=True
        )
        link_tag = AcquisitionFeed.acquisition_link(
            rel=rel, href=fulfill_url,
            types=format_types
        )

        children = AcquisitionFeed.license_tags(license_pool, active_loan, None)
        link_tag.extend(children)
        return link_tag

class LibraryLoanAndHoldAnnotator(LibraryAnnotator):

    @classmethod
    def active_loans_for(
            cls, circulation, patron, test_mode=False, **response_kwargs
    ):
        db = Session.object_session(patron)
        active_loans_by_work = {}
        for loan in patron.loans:
            work = loan.work
            if work:
                active_loans_by_work[work] = loan
        active_holds_by_work = {}
        for hold in patron.holds:
            work = hold.work
            if work:
                active_holds_by_work[work] = hold

        annotator = cls(
            circulation, None, patron.library, patron, active_loans_by_work, active_holds_by_work,
            test_mode=test_mode
        )
        url = annotator.url_for('active_loans', library_short_name=patron.library.short_name, _external=True)
        works = patron.works_on_loan_or_on_hold()

        feed_obj = AcquisitionFeed(db, "Active loans and holds", url, works, annotator)
        annotator.annotate_feed(feed_obj, None)
        return feed_obj.as_response(max_age=60*30, private=True)

    @classmethod
    def single_item_feed(cls, circulation, item, fulfillment=None, test_mode=False,
                         feed_class=AcquisitionFeed, **response_kwargs):
        """Construct a response containing a single OPDS entry representing an active loan
        or hold.

        :param circulation: A CirculationAPI
        :param item: A Loan or Hold -- perhaps one that was just created or looked up.
        :param fulfillment: A FulfillmentInfo representing the format in which an active loan
            should be fulfilled.
        :param test_mode: Passed along to the constructor for this annotator class.
        :param feed_class: A drop-in replacement for AcquisitionFeed, for use in tests.
        :param response_kwargs: Extra keyword arguments to be passed into the OPDSEntryResponse
            constructor.

        :return: An OPDSEntryResponse
        """
        _db = Session.object_session(item)
        license_pool = item.license_pool
        work = license_pool.work or license_pool.presentation_edition.work
        library = item.library

        active_loans_by_work = {}
        active_holds_by_work = {}
        active_fulfillments_by_work = {}
        if isinstance(item, Loan):
            d = active_loans_by_work
        elif isinstance(item, Hold):
            d = active_holds_by_work
        d[work] = item

        if fulfillment:
            active_fulfillments_by_work[work] = fulfillment

        annotator = cls(
            circulation, None, library,
            active_loans_by_work=active_loans_by_work,
            active_holds_by_work=active_holds_by_work,
            active_fulfillments_by_work=active_fulfillments_by_work,
            test_mode=test_mode
        )
        identifier = license_pool.identifier
        url = annotator.url_for(
            'loan_or_hold_detail',
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            library_short_name=library.short_name,
            _external=True
        )
        return annotator._single_entry_response(
            _db, work, annotator, url, feed_class, **response_kwargs
        )

    def drm_device_registration_feed_tags(self, patron):
        """Return tags that provide information on DRM device deregistration
        independent of any particular loan. These tags will go under
        the <feed> tag.

        This allows us to deregister an Adobe ID, in preparation for
        logout, even if there is no active loan that requires one.
        """
        tags = copy.deepcopy(self.adobe_id_tags(patron))
        attr = '{%s}scheme' % OPDSFeed.DRM_NS
        for tag in tags:
            tag.attrib[attr] = "http://librarysimplified.org/terms/drm/scheme/ACS"
        return tags

    @property
    def user_profile_management_protocol_link(self):
        """Create a <link> tag that points to the circulation
        manager's User Profile Management Protocol endpoint
        for the current patron.
        """
        link = OPDSFeed.makeelement("link")
        link.attrib['rel'] = 'http://librarysimplified.org/terms/rel/user-profile'
        link.attrib['href'] = self.url_for(
            'patron_profile', library_short_name=self.library.short_name, _external=True
        )
        return link

    def annotate_feed(self, feed, lane):
        """Annotate the feed with top-level DRM device registration tags
        and a link to the User Profile Management Protocol endpoint.
        """
        super(LibraryLoanAndHoldAnnotator, self).annotate_feed(
            feed, lane
        )
        if self.patron:
            tags = self.drm_device_registration_feed_tags(self.patron)
            tags.append(self.user_profile_management_protocol_link)
            for tag in tags:
                feed.feed.append(tag)

class SharedCollectionLoanAndHoldAnnotator(SharedCollectionAnnotator):

    @classmethod
    def single_item_feed(cls, collection, item, fulfillment=None, test_mode=False,
                         feed_class=AcquisitionFeed, **response_kwargs):
        """Create an OPDS entry representing a single loan or hold.

        TODO: This and LibraryLoanAndHoldAnnotator.single_item_feed
        can potentially be refactored. The main obstacle is different
        routes and arguments for 'loan info' and 'hold info'.

        :return: An OPDSEntryResponse

        """
        _db = Session.object_session(item)
        license_pool = item.license_pool
        work = license_pool.work or license_pool.presentation_edition.work
        identifier = license_pool.identifier

        active_loans_by_work = {}
        active_holds_by_work = {}
        active_fulfillments_by_work = {}
        if fulfillment:
            active_fulfillments_by_work[work] = fulfillment
        if isinstance(item, Loan):
            d = active_loans_by_work
            route = 'shared_collection_loan_info'
            route_kwargs = dict(loan_id=item.id)
        elif isinstance(item, Hold):
            d = active_holds_by_work
            route = 'shared_collection_hold_info'
            route_kwargs = dict(hold_id=item.id)
        d[work] = item
        annotator = cls(
            collection, None,
            active_loans_by_work=active_loans_by_work,
            active_holds_by_work=active_holds_by_work,
            active_fulfillments_by_work=active_fulfillments_by_work,
            test_mode=test_mode
        )
        url = annotator.url_for(
            route,
            collection_name=collection.name,
            _external=True,
            **route_kwargs
        )
        return annotator._single_entry_response(
            _db, work, annotator, url, feed_class, **response_kwargs
        )
