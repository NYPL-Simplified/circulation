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
from core.opds import (
    Annotator,
    AcquisitionFeed,
    UnfulfillableWork,
)
from core.util.opds_writer import (    
    OPDSFeed,
)
from core.model import (
    ConfigurationSetting,
    Credential,
    DataSource,
    DeliveryMechanism,
    Identifier,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Patron,
    Session,
    BaseMaterializedWork,
    Work,
    Edition,
)
from core.lane import (
    Lane,
    WorkList,
)
from api.lanes import DynamicLane
from core.app_server import cdn_url_for

from adobe_vendor_id import AuthdataUtility
from annotations import AnnotationWriter
from circulation import BaseCirculationAPI
from config import Configuration
from novelist import NoveListAPI

class CirculationManagerAnnotator(Annotator):

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
                 top_level_title="All Books"
    ):
        if lane:
            logger_name = "Circulation Manager Annotator for %s" % lane.display_name
        else:
            logger_name = "Circulation Manager Annotator"
        self.log = logging.getLogger(logger_name)
        self.circulation = circulation
        self.lane = lane
        self.library = library
        self.patron = patron
        self.active_loans_by_work = active_loans_by_work
        self.active_holds_by_work = active_holds_by_work
        self.active_fulfillments_by_work = active_fulfillments_by_work
        self.lanes_by_work = defaultdict(list)
        self.facet_view = facet_view
        self.test_mode = test_mode
        self._adobe_id_tags = {}
        self._top_level_title = top_level_title

    def top_level_title(self):
        return self._top_level_title

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

    def _lane_identifier(self, lane):
        if isinstance(lane, Lane):
            return lane.id
        return None

    def facet_url(self, facets):
        return self.feed_url(self.lane, facets=facets, default_route=self.facet_view)

    def permalink_for(self, work, license_pool, identifier):
        return self.url_for(
            'permalink',
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            library_short_name=self.library.short_name,
            _external=True
        )

    def groups_url(self, lane):
        lane_identifier = self._lane_identifier(lane)
        return self.cdn_url_for(
            "acquisition_groups",
            lane_identifier=lane_identifier,
            library_short_name=self.library.short_name,
            _external=True)

    def default_lane_url(self):
        return self.groups_url(None)

    def feed_url(self, lane, facets=None, pagination=None, default_route='feed'):
        if (isinstance(lane, WorkList) and
            hasattr(lane, 'url_arguments')):
            route, kwargs = lane.url_arguments
        else:
            route = default_route
            lane_identifier = self._lane_identifier(lane)
            kwargs = dict(lane_identifier=lane_identifier)
        kwargs['library_short_name'] = self.library.short_name
        if facets != None:
            kwargs.update(dict(facets.items()))
        if pagination != None:
            kwargs.update(dict(pagination.items()))
        return self.cdn_url_for(route, _external=True, **kwargs)

    def search_url(self, lane, query, pagination):
        lane_identifier = self._lane_identifier(lane)
        kwargs = dict(q=query)
        kwargs.update(dict(pagination.items()))
        return self.url_for(
            "lane_search", lane_identifier=lane_identifier,
            library_short_name=self.library.short_name,
            _external=True, **kwargs)

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
            return self.feed_url(lane), title

        return self.lane_url(lane), title

    def lane_url(self, lane):
        # If the lane has sublanes, the URL identifying the group will
        # take the user to another set of groups for the
        # sublanes. Otherwise it will take the user to a list of the
        # books in the lane by author.

        if lane and isinstance(lane, Lane) and lane.sublanes:
            url = self.groups_url(lane)
        elif lane and (
            isinstance(lane, Lane)
            or isinstance(lane, DynamicLane)
            ):
            url = self.feed_url(lane)
        else:
            # This lane isn't part of our lane hierarchy. It's probably
            # a WorkList created to represent the top-level. Use the top-level
            # url for it.
            url = self.default_lane_url()
        return url

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry):
        Annotator.annotate_work_entry(
            work, active_license_pool, edition, identifier, feed, entry
        )
        active_loan = self.active_loans_by_work.get(work)
        active_hold = self.active_holds_by_work.get(work)
        active_fulfillment = self.active_fulfillments_by_work.get(work)

        # First, add a permalink.
        feed.add_link_to_entry(
            entry, 
            rel='alternate',
            type=OPDSFeed.ENTRY_TYPE,
            href=self.permalink_for(
                work, active_license_pool, identifier
            )
        )

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

        # Now we need to generate a <link> tag for every delivery mechanism
        # that has well-defined media types.
        link_tags = self.acquisition_links(
            active_license_pool, active_loan, active_hold, active_fulfillment,
            feed, identifier
        )
        for tag in link_tags:
            entry.append(tag)

        # Add a link for each author.
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

    @classmethod
    def related_books_available(cls, work, library):
        """:return: bool asserting whether related books might exist for
        a particular Work
        """
        if isinstance(work, Work):
            edition = work.presentation_edition
        else:
            # This is a MaterializedWork*, so we're gonna grab the
            # edition where we can.
            edition = work.license_pool.presentation_edition

        contributions = edition.contributions
        series = edition.series
        return contributions or series or NoveListAPI.is_configured(library)

    def language_and_audience_key_from_work(self, work):
        language_key = work.language

        if work.audience == Classifier.AUDIENCE_CHILDREN:
            audiences = [Classifier.AUDIENCE_CHILDREN]
        if work.audience == Classifier.AUDIENCE_YOUNG_ADULT:
            audiences = Classifier.AUDIENCES_JUVENILE
        if work.audience in Classifier.AUDIENCES_ADULT:
            audiences = list(Classifier.AUDIENCES)

        audience_key=None
        if audiences:
            audience_strings = [urllib.quote_plus(a) for a in sorted(audiences)]
            audience_key = u','.join(audience_strings)

        return language_key, audience_key

    def add_author_links(self, work, feed, entry):
        author_tag = '{%s}author' % OPDSFeed.ATOM_NS
        author_entries = entry.findall(author_tag)

        languages, audiences = self.language_and_audience_key_from_work(work)
        for author_entry in author_entries:
            name_tag = '{%s}name' % OPDSFeed.ATOM_NS
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
            if isinstance(work, Work):
                work_id = work.id
                work_title = work.title
            else:
                work_id = work.works_id
                work_title = work.sort_title
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
            # authenticate.
            self.add_authentication_document_link(feed)
        
        # Add a 'search' link if the lane is searchable.
        if lane and lane.search_target:
            lane_identifier = self._lane_identifier(lane)
            search_url = self.url_for(
                'lane_search', lane_identifier=lane_identifier,
                library_short_name=self.library.short_name,
                _external=True
            )
            search_link = dict(
                rel="search",
                type="application/opensearchdescription+xml",
                href=search_url
            )
            feed.add_link_to_feed(feed.feed, **search_link)

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

        for type, value in Configuration.help_uris(self.library):
            d = dict(href=value, rel="help")
            if type:
                d['type'] = type
            _add_link(d)
        
                    
    def acquisition_links(self, active_license_pool, active_loan, active_hold, active_fulfillment,
                          feed, identifier):
        """Generate a number of <link> tags that enumerate all acquisition methods."""

        can_borrow = False
        can_fulfill = False
        can_revoke = False
        can_hold = self.library.allow_holds

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
            url = self.url_for(
                'revoke_loan_or_hold',
                license_pool_id=active_license_pool.id,
                library_short_name=self.library.short_name,
                _external=True)

            kw = dict(href=url, rel=OPDSFeed.REVOKE_LOAN_REL)
            revoke_link_tag = OPDSFeed.makeelement("link", **kw)
            revoke_links.append(revoke_link_tag)

        # Add next-step information for every useful delivery
        # mechanism.
        borrow_links = []
        api = None
        if self.circulation:
            api = self.circulation.api_for_license_pool(active_license_pool)
        if api:
            set_mechanism_at_borrow = (
                api.SET_DELIVERY_MECHANISM_AT == BaseCirculationAPI.BORROW_STEP)
        else:
            # This is most likely an open-access book. Just put one
            # borrow link and figure out the rest later.
            set_mechanism_at_borrow = False
        if can_borrow:
            # Borrowing a book gives you an OPDS entry that gives you
            # fulfillment links.
            if set_mechanism_at_borrow:
                # The ebook distributor requires that the delivery
                # mechanism be set at the point of checkout. This means
                # a separate borrow link for each mechanism.
                for mechanism in active_license_pool.delivery_mechanisms:
                    borrow_links.append(
                        self.borrow_link(
                            identifier,
                            mechanism, [mechanism]
                        )
                    )
            else:
                # The ebook distributor does not require that the
                # delivery mechanism be set at the point of
                # checkout. This means a single borrow link with
                # indirectAcquisition tags for every delivery
                # mechanism. If a delivery mechanism must be set, it
                # will be set at the point of fulfillment.
                borrow_links.append(
                    self.borrow_link(
                        identifier,
                        None, active_license_pool.delivery_mechanisms
                    )
                )

            # Generate the licensing tags that tell you whether the book
            # is available.
            license_tags = feed.license_tags(
                active_license_pool, active_loan, active_hold
            )
            for link in borrow_links:
                for t in license_tags:
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

            elif active_loan.fulfillment:
                # The delivery mechanism for this loan has been
                # set. There is one link for the delivery mechanism
                # that was locked in, and links for any streaming
                # delivery mechanisms.
                for lpdm in active_license_pool.delivery_mechanisms:
                    if lpdm is active_loan.fulfillment or lpdm.delivery_mechanism.is_streaming:
                        fulfill_links.append(
                            self.fulfill_link(
                                active_license_pool,
                                active_loan,
                                lpdm.delivery_mechanism
                            )
                        )
            else:
                # The delivery mechanism for this loan has not been
                # set. There is one fulfill link for every delivery
                # mechanism.
                for lpdm in active_license_pool.delivery_mechanisms:
                    fulfill_links.append(
                        self.fulfill_link(
                            active_license_pool,
                            active_loan,
                            lpdm.delivery_mechanism
                        )
                    )
                                               
        # If this is an open-access book, add an open-access link for
        # every delivery mechanism with an associated resource.
        open_access_links = []
        if active_license_pool.open_access:
            for lpdm in active_license_pool.delivery_mechanisms:
                if lpdm.resource:
                    open_access_links.append(self.open_access_link(lpdm))

        return [x for x in borrow_links + fulfill_links + open_access_links + revoke_links
                if x is not None]

    def borrow_link(self, identifier,
                    borrow_mechanism, fulfillment_mechanisms):
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
            raise UnfulfillableWork()

        borrow_link.extend(indirect_acquisitions)
        return borrow_link

    def fulfill_link(self, license_pool, active_loan, delivery_mechanism):
        """Create a new fulfillment link.

        This link may include tags from the OPDS Extensions for DRM.
        """
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
        rel=OPDSFeed.ACQUISITION_REL
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

    @classmethod
    def _adobe_patron_identifier(self, patron):
        _db = Session.object_session(patron)
        internal = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)

        def refresh(credential):
            credential.credential = str(uuid.uuid1())
        patron_identifier = Credential.lookup(
            _db, internal, AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER, patron,
            refresher_method=refresh, allow_persistent_token=True
        )
        return patron_identifier.credential
    
    def drm_device_registration_tags(self, license_pool, active_loan,
                                     delivery_mechanism):
        """Construct OPDS Extensions for DRM tags that explain how to 
        register a device with the DRM server that manages this loan.

        :param delivery_mechanism: A DeliveryMechanism
        """        
        if not active_loan or not delivery_mechanism:
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
            if isinstance(patron_identifier, Patron):
                # Find the patron's identifier for Adobe ID purposes.
                patron_identifier = self._adobe_patron_identifier(
                    patron_identifier
                )
            cached = []
            authdata = AuthdataUtility.from_config(self.library)
            if authdata:
                # TODO: We would like to call encode() here, and have
                # the client use a JWT as authdata, but we can't,
                # because there's no way to use authdata to deactivate
                # a device. So we've used this alternate technique
                # that's much smaller than a JWT and can be smuggled
                # into username/password.
                vendor_id, jwt = authdata.encode_short_client_token(patron_identifier)

                drm_licensor = OPDSFeed.makeelement("{%s}licensor" % OPDSFeed.DRM_NS)
                vendor_attr = "{%s}vendor" % OPDSFeed.DRM_NS
                drm_licensor.attrib[vendor_attr] = vendor_id
                patron_key = OPDSFeed.makeelement("{%s}clientToken" % OPDSFeed.DRM_NS)
                patron_key.text = jwt
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
        
    def open_access_link(self, lpdm):
        _db = Session.object_session(self.library)
        url = cdnify(lpdm.resource.url)
        kw = dict(rel=OPDSFeed.OPEN_ACCESS_REL, href=url)
        rep = lpdm.resource.representation
        if rep and rep.media_type:
            kw['type'] = rep.media_type
        link_tag = AcquisitionFeed.link(**kw)
        always_available = OPDSFeed.makeelement(
            "{%s}availability" % OPDSFeed.OPDS_NS, status="available"
        )
        link_tag.append(always_available)
        return link_tag

    def add_patron(self, feed_obj):
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
        feed_obj.add_link_to_feed(
            feed_obj.feed,
            rel='http://opds-spec.org/auth/document',
            href=self.url_for(
                'authentication_document', 
                library_short_name=self.library.short_name, _external=True
            )
        )


class CirculationManagerLoanAndHoldAnnotator(CirculationManagerAnnotator):

    @classmethod
    def active_loans_for(cls, circulation, patron, test_mode=False):
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
        return feed_obj
    
    @classmethod
    def single_loan_feed(cls, circulation, loan, test_mode=False):
        db = Session.object_session(loan)
        work = loan.license_pool.work or loan.license_pool.presentation_edition.work
        annotator = cls(circulation, None, loan.patron.library,
                        active_loans_by_work={work:loan}, 
                        active_holds_by_work={}, 
                        test_mode=test_mode)
        identifier = loan.license_pool.identifier
        url = annotator.url_for(
            'loan_or_hold_detail',
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            library_short_name=loan.patron.library.short_name,
            _external=True
        )
        if not work:
            return AcquisitionFeed(
                db, "Active loan for unknown work", url, [], annotator)
        return AcquisitionFeed.single_entry(db, work, annotator)

    @classmethod
    def single_hold_feed(cls, circulation, hold, test_mode=False):
        db = Session.object_session(hold)
        work = hold.license_pool.work or hold.license_pool.presentation_edition.work
        annotator = cls(circulation, None, hold.patron.library,
                        active_loans_by_work={}, 
                        active_holds_by_work={work:hold}, 
                        test_mode=test_mode)
        return AcquisitionFeed.single_entry(db, work, annotator)

    @classmethod
    def single_fulfillment_feed(cls, circulation, loan, fulfillment, test_mode=False):
        db = Session.object_session(loan)
        work = loan.license_pool.work or loan.license_pool.presentation_edition.work
        annotator = cls(circulation, None, loan.patron.library,
                        active_loans_by_work={}, 
                        active_holds_by_work={}, 
                        active_fulfillments_by_work={work:fulfillment},
                        test_mode=test_mode)
        identifier = loan.license_pool.identifier
        url = annotator.url_for(
            'loan_or_hold_detail',
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            library_short_name=loan.patron.library.short_name,
            _external=True
        )
        if not work:
            return AcquisitionFeed(
                db, "Active loan for unknown work", url, [], annotator)
        return AcquisitionFeed.single_entry(db, work, annotator)

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
        super(CirculationManagerLoanAndHoldAnnotator, self).annotate_feed(
            feed, lane
        )
        if self.patron:
            tags = self.drm_device_registration_feed_tags(self.patron)
            tags.append(self.user_profile_management_protocol_link)
            for tag in tags:
                feed.feed.append(tag)
