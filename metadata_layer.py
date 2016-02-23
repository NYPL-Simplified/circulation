"""An abstract way of representing incoming metadata and applying it
to Identifiers and Editions.

This acts as an intermediary between the third-party integrations
(which have this information in idiosyncratic formats) and the
model. Doing a third-party integration should be as simple as putting
the information into this format.
"""

from collections import defaultdict
from sqlalchemy.orm.session import Session
from nose.tools import set_trace
from dateutil.parser import parse
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm.exc import (
    NoResultFound,
)
import csv
import datetime
import logging
from util import LanguageCodes
from model import (
    get_one,
    get_one_or_create,
    CirculationEvent,
    Contributor,
    CoverageRecord,
    DataSource,
    DeliveryMechanism,
    Edition,
    Hyperlink,
    Identifier,
    LicensePool,
    Subject,
    Hyperlink,
    RightsStatus,
    Representation,
)

class SubjectData(object):
    def __init__(self, type, identifier, name=None, weight=1):
        self.type = type
        self.identifier = identifier
        self.name = name
        self.weight=weight

    def __repr__(self):
        return '<SubjectData type="%s" identifier="%s" name="%s" weight=%d>' % (
            self.type, self.identifier, self.name, self.weight
        )


class ContributorData(object):
    def __init__(self, sort_name=None, display_name=None, 
                 family_name=None, wikipedia_name=None, roles=None,
                 lc=None, viaf=None, biography=None, aliases=None):
        self.sort_name = sort_name
        self.display_name = display_name
        self.family_name = family_name
        self.wikipedia_name = wikipedia_name
        roles = roles or Contributor.AUTHOR_ROLE
        if not isinstance(roles, list):
            roles = [roles]
        self.roles = roles
        self.lc = lc
        self.viaf = viaf
        self.biography = biography
        self.aliases = aliases

    def __repr__(self):
        return '<ContributorData sort="%s" display="%s" family="%s" wiki="%s" roles=%r lc=%s viaf=%s>' % (self.sort_name, self.display_name, self.family_name, self.wikipedia_name, self.roles, self.lc, self.viaf)

    def find_sort_name(self, _db, identifiers, metadata_client):
        """Try as hard as possible to find this person's sort name.
        """
        log = logging.getLogger("Abstract metadata layer")
        if self.sort_name:
            # log.debug(
            #     "%s already has a sort name: %s", 
            #     self.display_name,
            #     self.sort_name
            # )
            return True

        if not self.display_name:
            raise ValueError(
                "Cannot find sort name for a contributor with no display name!"
            )

        # Is there a contributor already in the database with this
        # exact sort name? If so, use their display name.
        sort_name = self.display_name_to_sort_name(_db, self.display_name)
        if sort_name:
            self.sort_name = sort_name
            return True

        # Time to break out the big guns. Ask the metadata wrangler
        # if it can find a sort name for this display name.
        sort_name = self.display_name_to_sort_name_through_canonicalizer(
            _db, identifiers, metadata_client
        )
        self.sort_name = sort_name
        return (self.sort_name is not None)

    @classmethod
    def display_name_to_sort_name(self, _db, display_name):
        """Find the sort name for this book's author, assuming it's easy.

        'Easy' means we already have an established sort name for a
        Contributor with this exact display name.
        
        If it's not easy, this will be taken care of later with a call to
        the metadata wrangler's author canonicalization service.

        If we have a copy of this book in our collection (the only
        time an external list item is relevant), this will probably be
        easy.
        """
        contributors = _db.query(Contributor).filter(
            Contributor.display_name==display_name).filter(
                Contributor.name != None).all()
        if contributors:
            log = logging.getLogger("Abstract metadata layer")
            log.debug(
                "Determined that sort name of %s is %s based on previously existing contributor", 
                display_name,
                contributors[0].name
            )
            return contributors[0].name
        return None

    def _display_name_to_sort_name(
            self, _db, metadata_client, identifier_obj
    ):
        response = metadata_client.canonicalize_author_name(
            identifier_obj, self.display_name)
        sort_name = None
        log = logging.getLogger("Abstract metadata layer")
        if (response.status_code == 200 
            and response.headers['Content-Type'].startswith('text/plain')):
            sort_name = response.content.decode("utf8")
            log.info(
                "Canonicalizer found sort name for %r: %s => %s",
                identifier_obj, 
                self.display_name,
                sort_name
            )
        else:
            log.warn(
                "Canonicalizer could not find sort name for %r/%s",
                identifier_obj,
                self.display_name
            )
        return sort_name

    def display_name_to_sort_name_through_canonicalizer(
            self, _db, identifiers, metadata_client):
        sort_name = None
        for identifier in identifiers:
            if identifier.type != Identifier.ISBN:
                continue
            identifier_obj, ignore = identifier.load(_db)
            sort_name = self._display_name_to_sort_name(
                _db, metadata_client, identifier_obj
            )
            if sort_name:
                break

        if not sort_name:
            sort_name = self._display_name_to_sort_name(
                _db, metadata_client, None
            )
        return sort_name        


class IdentifierData(object):
    def __init__(self, type, identifier, weight=1):
        self.type = type
        self.identifier = identifier
        self.weight = 1

    def __repr__(self):
        return '<IdentifierData type="%s" identifier="%s" weight="%s">' % (
            self.type, self.identifier, self.weight
        )

    def load(self, _db):
        return Identifier.for_foreign_id(
            _db, self.type, self.identifier
        )

class LinkData(object):
    def __init__(self, rel, href=None, media_type=None, content=None,
                 thumbnail=None, rights_uri=None):
        if not rel:
            raise ValueError("rel is required")

        if not href and not content:
            raise ValueError("Either href or content is required")
        self.rel = rel
        self.href = href
        self.media_type = media_type
        self.content = content
        self.thumbnail = thumbnail
        # This handles content sources like unglue.it that have rights for each link
        # rather than each edition.
        self.rights_uri = rights_uri

    def __repr__(self):
        if self.content:
            content = ", %d bytes content" % len(self.content)
        else:
            content = ''
        if self.thumbnail:
            thumbnail = ', has thumbnail'
        else:
            thumbnail = ''
        return '<LinkData: rel="%s" href="%s" media_type=%r%s%s>' % (
            self.rel, self.href, self.media_type, thumbnail,
            content
        )

class MeasurementData(object):
    def __init__(self, 
                 quantity_measured,
                 value,
                 weight=1,
                 taken_at=None):
        if not quantity_measured:
            raise ValueError("quantity_measured is required.")
        if value is None:
            raise ValueError("measurement value is required.")
        self.quantity_measured = quantity_measured
        if not isinstance(value, float) and not isinstance(value, int):
            value = float(value)
        self.value = value
        self.weight = weight
        self.taken_at = taken_at or datetime.datetime.utcnow()

    def __repr__(self):
        return '<MeasurementData quantity="%s" value=%f weight=%d taken=%s>' % (
            self.quantity_measured, self.value, self.weight, self.taken_at
        )

class FormatData(object):
    def __init__(self, content_type, drm_scheme, link=None):
        self.content_type = content_type
        self.drm_scheme = drm_scheme
        if link and not isinstance(link, LinkData):
            raise TypeError(
                "Expected LinkData object, got %s" % type(link)
            )
        self.link = link

class CirculationData(object):
    def __init__(
            self, licenses_owned, 
            licenses_available, 
            licenses_reserved,
            patrons_in_hold_queue,
            first_appearance=None,
            last_checked=None,
    ):
        self.licenses_owned = licenses_owned
        self.licenses_available = licenses_available
        self.licenses_reserved = licenses_reserved
        self.patrons_in_hold_queue = patrons_in_hold_queue
        self.first_appearance = first_appearance
        self.last_checked = last_checked or datetime.datetime.utcnow()
        self.log = logging.getLogger(
            "Abstract metadata layer - Circulation data"
        )


    def update(self, license_pool, license_pool_is_new):
        _db = Session.object_session(license_pool)
        if license_pool_is_new:
            # This is our first time seeing this LicensePool. Log its
            # occurance as a separate event.
            event = get_one_or_create(
                _db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=self.last_checked,
                    delta=1,
                    end=self.last_checked,
                )
            )
            # TODO: Also put this in the log.

        changed = (license_pool.licenses_owned != self.licenses_owned or
                   license_pool.licenses_available != self.licenses_available or
                   license_pool.patrons_in_hold_queue != self.patrons_in_hold_queue or
                   license_pool.licenses_reserved != self.licenses_reserved)

        if changed:
            edition = license_pool.edition
            if edition:
                self.log.info(
                    'CHANGED %s "%s" %s (%s) OWN: %s=>%s AVAIL: %s=>%s HOLD: %s=>%s',
                    edition.medium, 
                    edition.title or "[NO TITLE]",
                    edition.author or "", 
                    edition.primary_identifier.identifier,
                    license_pool.licenses_owned, self.licenses_owned,
                    license_pool.licenses_available, self.licenses_available,
                    license_pool.patrons_in_hold_queue, self.patrons_in_hold_queue
                )
            else:
                self.log.info(
                    'CHANGED %r OWN: %s=>%s AVAIL: %s=>%s HOLD: %s=>%s',
                    license_pool.identifier, 
                    license_pool.licenses_owned, self.licenses_owned,
                    license_pool.licenses_available, self.licenses_available,
                    license_pool.patrons_in_hold_queue, self.patrons_in_hold_queue
                )


        # Update availabily information. This may result in the issuance
        # of additional events.
        license_pool.update_availability(
            self.licenses_owned,
            self.licenses_available,
            self.licenses_reserved,
            self.patrons_in_hold_queue,
            self.last_checked
        )
        return changed

class Metadata(object):

    """A (potentially partial) set of metadata for a published work."""

    log = logging.getLogger("Abstract metadata layer")

    def __init__(
            self, 
            data_source,
            license_data_source=None,
            title=None,
            subtitle=None,
            sort_title=None,
            language=None,
            medium=Edition.BOOK_MEDIUM,
            series=None,
            publisher=None,
            imprint=None,
            issued=None,
            published=None,            
            primary_identifier=None,
            identifiers=None,
            subjects=None,
            contributors=None,
            measurements=None,
            links=None,
            formats=None,
            rights_uri=None,
            last_update_time=None,
            circulation=None,
    ):
        # data_source is where the data comes from.
        self._data_source = data_source
        if isinstance(self._data_source, DataSource):
            self.data_source_obj = self._data_source
        else:
            self.data_source_obj = None

        # license_data_source is where our ability to actually access the
        # book comes from.
        self._license_data_source = license_data_source
        if isinstance(self._license_data_source, DataSource):
            self.license_data_source_obj = self._license_data_source
        else:
            self.license_data_source_obj = None

        self.title = title
        self.sort_title = sort_title
        self.subtitle = subtitle
        if language:
            language = LanguageCodes.string_to_alpha_3(language)
        self.language = language
        self.medium = medium
        self.series = series
        self.publisher = publisher
        self.imprint = imprint
        self.issued = issued
        self.published = published

        self.primary_identifier=primary_identifier
        self.identifiers = identifiers or []
        self.permanent_work_id = None
        if (self.primary_identifier 
            and self.primary_identifier not in self.identifiers):
            self.identifiers.append(self.primary_identifier)
        self.subjects = subjects or []
        self.contributors = contributors or []
        self.links = links or []
        self.measurements = measurements or []
        self.formats = formats or []
        self.rights_uri = rights_uri
        self.circulation = circulation

        self.last_update_time = last_update_time
        for link in self.links:
            # If a link has a rights_uri, make that the overall rights_uri. 
            # If there are multiple links with a rights_uri, they should be
            # split into separate metadata objects.
            if link.rights_uri:
                self.rights_uri = link.rights_uri

            # An open-access link or open-access rights implies a FormatData object.
            open_access_link = (link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD
                                and link.href)
            open_access_rights_link = (link.media_type in Representation.BOOK_MEDIA_TYPES 
                                       and link.href
                                       and self.rights_uri in RightsStatus.OPEN_ACCESS)
            
            if open_access_link or open_access_rights_link:
                self.formats.append(
                    FormatData(
                        content_type=link.media_type,
                        drm_scheme=DeliveryMechanism.NO_DRM,
                        link=link
                    )
            )

    @property
    def has_open_access_link(self):
        """Does this Metadata object have an associated open-access link?"""
        return any(
            [x for x in self.links 
             if x.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD and x.href]
        )

    @classmethod
    def from_edition(self, edition):
        """Create a basic Metadata object for the given Edition.

        This doesn't contain everything but it contains enough
        information to run guess_license_pools.
        """
        contributors = []
        for contribution in edition.contributions:
            c = contribution.contributor
            contributors.append(
                ContributorData(sort_name=c.name,
                                display_name=c.display_name,
                                roles=[contribution.role])
            )
        else:
            if edition.sort_author:
                contributors.append(
                    ContributorData(sort_name=edition.sort_author,
                                    display_name=edition.author,
                                    roles=[Contributor.PRIMARY_AUTHOR_ROLE])
                )
        i = edition.primary_identifier
        primary_identifier = IdentifierData(
            type=i.type, identifier=i.identifier, weight=1)

        return Metadata(
            data_source=edition.data_source.name,
            title=edition.title, 
            subtitle=edition.subtitle,
            sort_title=edition.sort_title,
            primary_identifier=primary_identifier,
            contributors=contributors
        )

    def normalize_contributors(self, metadata_client):
        """Make sure that all contributors without a .sort_name get one."""
        for contributor in contributors:
            if not contributor.sort_name:
                contributor.normalize(metadata_client)

    @property
    def primary_author(self):
        primary_author = None
        for tier in Contributor.author_contributor_tiers():
            for c in self.contributors:
                for role in tier:
                    if role in c.roles:
                        primary_author = c
                        break
                if primary_author:
                    break
            if primary_author:
                break
        return primary_author

    def calculate_permanent_work_id(self, _db, metadata_client):
        """Try to calculate a permanent work ID from this metadata.

        This may require asking a metadata wrangler to turn a display name
        into a sort name--thus the `metadata_client` argument.
        """
        primary_author = self.primary_author

        if not primary_author:
            return None, None

        if not primary_author.sort_name and metadata_client:
            primary_author.find_sort_name(
                _db, self.identifiers, metadata_client
            )

        sort_author = primary_author.sort_name
        pwid = Edition.calculate_permanent_work_id_for_title_and_author(
            self.title, sort_author, "book")
        self.permanent_work_id=pwid
        return pwid

    def associate_with_identifiers_based_on_permanent_work_id(
            self, _db):
        """Try to associate this object's primary identifier with
        the primary identifiers of Editions in the database which share
        a permanent work ID.
        """
        if (not self.primary_identifier or not self.permanent_work_id):
            # We don't have the information necessary to carry out this
            # task.
            return

        primary_identifier_obj, ignore = self.primary_identifier.load(_db)

        # Try to find the primary identifiers of other Editions with
        # the same permanent work ID, representing books already in
        # our collection.
        qu = _db.query(Identifier).join(
            Identifier.primarily_identifies).filter(
                Edition.permanent_work_id==self.permanent_work_id).filter(
                    Identifier.type.in_(
                        Identifier.LICENSE_PROVIDING_IDENTIFIER_TYPES
                    )
                )
        identifiers_same_work_id = qu.all()
        for same_work_id in identifiers_same_work_id:
            if same_work_id != self.primary_identifier:
                self.log.info(
                    "Discovered that %r is equivalent to %r because of matching permanent work ID %s",
                    same_work_id, primary_identifier_obj, self.permanent_work_id
                )
                primary_identifier_obj.equivalent_to(
                    self.data_source(_db), same_work_id, 0.85)

    def data_source(self, _db):
        if not self.data_source_obj:
            if not self._data_source:
                raise ValueError("No data source specified!")
            self.data_source_obj = DataSource.lookup(_db, self._data_source)
        if not self.data_source_obj:
            raise ValueError("Data source %s not found!" % self._data_source)
        return self.data_source_obj

    def license_data_source(self, _db):
        if not self.license_data_source_obj:
            if self._license_data_source:
                obj = DataSource.lookup(_db, self._license_data_source)
                if not obj:
                    raise ValueError("Data source %s not found!" % self._license_data_source)
                if not obj.offers_licenses:
                    raise ValueError("Data source %s does not offer licenses and cannot be used as a license data source." % self._license_data_Source)
            else:
                obj = None
            self.license_data_source_obj = obj
        return self.license_data_source_obj

    def edition(self, _db, create_if_not_exists=True):
        if not self.primary_identifier:
            raise ValueError(
                "Cannot find edition: metadata has no primary identifier."
            )

        data_source = self.license_data_source(_db) or self.data_source(_db)
        return Edition.for_foreign_id(
            _db, data_source, self.primary_identifier.type, 
            self.primary_identifier.identifier, 
            create_if_not_exists=create_if_not_exists
        )

    def set_default_rights_uri(self, data_source):
        if self.rights_uri == None and data_source:
            # We haven't been able to determine rights from the metadata, so use the default rights
            # for the data source if any.
            default = RightsStatus.DATA_SOURCE_DEFAULT_RIGHTS_STATUS.get(data_source.name, None)
            if default:
                self.rights_uri = default

        for format in self.formats:
            if format.link:
                link = format.link
                if self.rights_uri in (None, RightsStatus.UNKNOWN) and link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
                    # We haven't determined rights from the metadata or the data source, but there's an
                    # open access download link, so we'll consider it generic open access.
                    self.rights_uri = RightsStatus.GENERIC_OPEN_ACCESS

        if self.rights_uri == None:
            # We still haven't determined rights, so it's unknown.
            self.rights_uri = RightsStatus.UNKNOWN


    def license_pool(self, _db):
        if not self.primary_identifier:
            raise ValueError(
                "Cannot find license pool: metadata has no primary identifier."
            )

        license_pool = None
        is_new = False

        identifier_obj, ignore = self.primary_identifier.load(_db)

        metadata_data_source = self.data_source(_db) 
        license_data_source = self.license_data_source(_db)

        self.set_default_rights_uri(metadata_data_source)
        if license_data_source:
            can_create_new_pool = True
            check_for_licenses_from = [license_data_source]
        else:
            check_for_licenses_from = DataSource.license_sources_for(
                _db, identifier_obj
            ).all()
            if len(check_for_licenses_from) == 1:
                # Since there is only one source for this kind of book,
                # we can create a new license pool if necessary.
                can_create_new_pool = True
                self.license_data_source_obj = check_for_licenses_from[0]
            elif metadata_data_source in check_for_licenses_from:
                # We can assume that the license comes from the same
                # source as the metadata.
                self.license_data_source_obj = metadata_data_source
                can_create_new_pool = True
            else:
                # We might be able to find an existing license pool
                # for this book, but we won't be able to create a new
                # one, because we don't know who's responsible for the
                # book.
                can_create_new_pool = False

        license_data_source = self.license_data_source(_db)
        for potential_data_source in check_for_licenses_from:
            license_pool = get_one(
                _db, LicensePool, data_source=potential_data_source,
                identifier=identifier_obj
            )
            if license_pool:
                break

        if not license_pool and can_create_new_pool:
            rights_status = get_one(_db, RightsStatus, uri=self.rights_uri)
            license_pool, is_new = LicensePool.for_foreign_id(
                _db, self.license_data_source_obj,
                self.primary_identifier.type, 
                self.primary_identifier.identifier,
                rights_status=rights_status,
            )
            if self.has_open_access_link:
                license_pool.open_access = True
            if self.rights_uri:
                license_pool.set_rights_status(self.rights_uri)
        return license_pool, is_new

    def consolidate_identifiers(self):
        by_weight = defaultdict(list)
        for i in self.identifiers:
            by_weight[(i.type, i.identifier)].append(i.weight)
        new_identifiers = []
        import numpy
        for (type, identifier), weights in by_weight.items():
            new_identifiers.append(
                IdentifierData(type=type, identifier=identifier,
                               weight=numpy.median(weights))
            )
        self.identifiers = new_identifiers

    def guess_license_pools(self, _db, metadata_client):
        """Try to find existing license pools for this Metadata."""
        potentials = {}
        for contributor in self.contributors:
            if not any(
                    x in contributor.roles for x in 
                    (Contributor.AUTHOR_ROLE, 
                     Contributor.PRIMARY_AUTHOR_ROLE)
            ):
                continue
            contributor.find_sort_name(_db, self.identifiers, metadata_client)
            confidence = 0

            base = _db.query(Edition).filter(
                Edition.title.ilike(self.title)).filter(
                    Edition.medium==Edition.BOOK_MEDIUM)
            success = False

            # A match based on work ID is the most reliable.
            pwid = self.calculate_permanent_work_id(_db, metadata_client)
            clause = and_(Edition.data_source_id==LicensePool.data_source_id, Edition.primary_identifier_id==LicensePool.identifier_id)
            qu = base.filter(Edition.permanent_work_id==pwid).join(LicensePool, clause)
            success = self._run_query(qu, potentials, 0.95)
            if not success and contributor.sort_name:
                qu = base.filter(Edition.sort_author==contributor.sort_name)
                success = self._run_query(qu, potentials, 0.9)
            if not success and contributor.display_name:
                qu = base.filter(Edition.author==contributor.display_name)
                success = self._run_query(qu, potentials, 0.8)
            if not success:
                # Look for the book by an unknown author (our mistake)
                qu = base.filter(Edition.author==Edition.UNKNOWN_AUTHOR)
                success = self._run_query(qu, potentials, 0.45)
            if not success:
                # See if there is any book with this title at all.
                success = self._run_query(base, potentials, 0.3)
        return potentials

    def _run_query(self, qu, potentials, confidence):
        success = False
        for i in qu:
            lp = i.license_pool
            if lp and lp.deliverable and potentials.get(lp, 0) < confidence:
                potentials[lp] = confidence
                success = True
        return success

    def apply(
            self, edition, 
            metadata_client=None,
            replace_identifiers=False,
            replace_subjects=False, 
            replace_contributions=False,
            replace_links=False,
            replace_formats=False,
            replace_rights=False,
            force=False,
    ):
        """Apply this metadata to the given edition."""
        _db = Session.object_session(edition)

        # We were given an Edition, so either this metadata's
        # primary_identifier must be missing or it must match the
        # Edition's primary identifier.
        if self.primary_identifier:
            if (self.primary_identifier.type != edition.primary_identifier.type
                or self.primary_identifier.identifier != edition.primary_identifier.identifier):
                raise ValueError(
                    "Metadata's primary identifier (%s/%s) does not match edition's primary identifier (%r)" % (
                        self.primary_identifier.type,
                        self.primary_identifier.identifier,
                        edition.primary_identifier,
                    )
                )

        # Check whether we should do any work at all.
        data_source = self.data_source(_db)
        if self.last_update_time and not force:
            coverage_record = CoverageRecord.lookup(edition, data_source)
            if coverage_record:
                check_date = coverage_record.date
                if not isinstance(check_date, datetime.date):
                    check_date = check_date.date()
                last_date = self.last_update_time
                if isinstance(last_date, datetime.datetime):
                    last_date = last_date.date()
                if check_date >= last_date:
                    # The metadata has not changed since last time. Do nothing.
                    return

        if metadata_client and not self.permanent_work_id:
            self.calculate_permanent_work_id(_db, metadata_client)

        identifier = edition.primary_identifier
        pool = identifier.licensed_through
        self.log.info(
            "APPLYING METADATA TO EDITION: %s",  self.title
        )
        if self.title:
            edition.title = self.title
        if self.subtitle:
            edition.subtitle = self.subtitle
        if self.language:
            edition.language = self.language
        if self.medium:
            edition.medium = self.medium
        if self.series:
            edition.series = self.series
        if self.publisher:
            edition.publisher = self.publisher
        if self.imprint:
            edition.imprint = self.imprint
        if self.issued:
            edition.issued = self.issued
        if self.published:
            edition.published = self.published
        if self.permanent_work_id:
            edition.permanent_work_id = self.permanent_work_id

        # Create equivalencies between all given identifiers and
        # the edition's primary identifier.

        self.update_contributions(_db, edition, metadata_client, 
                                  replace_contributions)

        # TODO: remove equivalencies when replace_identifiers is True.

        if self.identifiers is not None:
            for identifier_data in self.identifiers:
                new_identifier, ignore = Identifier.for_foreign_id(
                    _db, identifier_data.type, identifier_data.identifier)
                identifier.equivalent_to(
                    data_source, new_identifier, identifier_data.weight)

        if replace_subjects and self.subjects is not None:
            # Remove any old Subjects from this data source -- we're
            # about to add a new set.
            surviving_classifications = []
            dirty = False
            for classification in identifier.classifications:
                if classification.data_source == data_source:
                    _db.delete(classification)
                    dirty = True
                else:
                    surviving_classifications.append(classification)
            if dirty:
                identifier.classifications = surviving_classifications

        # Apply all specified subjects to the identifier.
        if self.subjects:
            for subject in self.subjects:
                identifier.classify(
                    data_source, subject.type, subject.identifier, 
                    subject.name, weight=subject.weight)

        # Associate all links with the primary identifier.
        if replace_links and self.links is not None:
            surviving_hyperlinks = []
            dirty = False
            for hyperlink in identifier.links:
                if hyperlink.data_source == data_source:
                    _db.delete(hyperlink)
                    dirty = True
                else:
                    surviving_hyperlinks.append(hyperlink)
            if dirty:
                identifier.links = surviving_hyperlinks

        for link in self.links:
            link_obj, ignore = identifier.add_link(
                rel=link.rel, href=link.href, data_source=data_source, 
                license_pool=pool, media_type=link.media_type,
                content=link.content
            )
            thumbnail = link.thumbnail
            if thumbnail:
                if thumbnail.href == link.href:
                    # The image serves as its own thumbnail. This is a
                    # hacky way to represent this in the database.
                    if link_obj.resource.representation:
                        link_obj.resource.representation.image_height = Edition.MAX_THUMBNAIL_HEIGHT
                else:
                    # The thumbnail and image are different.
                    thumbnail_obj, ignore = identifier.add_link(
                        rel=thumbnail.rel, href=thumbnail.href, 
                        data_source=data_source, 
                        license_pool=pool, media_type=thumbnail.media_type,
                        content=thumbnail.content
                    )
                    if thumbnail_obj.resource.representation:
                        thumbnail_obj.resource.representation.thumbnail_of = link_obj.resource.representation

        if pool and replace_formats:
            for lpdm in pool.delivery_mechanisms:
                _db.delete(lpdm)
            pool.delivery_mechanisms = []

        self.set_default_rights_uri(data_source)

        for format in self.formats:
            if format.link:
                link = format.link
                if not format.content_type:
                    format.content_type = link.media_type
                link_obj, ignore = identifier.add_link(
                    rel=link.rel, href=link.href, data_source=data_source, 
                    license_pool=pool, media_type=link.media_type,
                    content=link.content
                )
                resource = link_obj.resource
            else:
                resource = None
            if pool:
                pool.set_delivery_mechanism(
                    format.content_type, format.drm_scheme, resource
                )

        if pool and replace_rights:
            pool.set_rights_status(self.rights_uri)

        # Apply all measurements to the primary identifier
        for measurement in self.measurements:
            identifier.add_measurement(
                data_source, measurement.quantity_measured,
                measurement.value, measurement.weight,
                measurement.taken_at
            )

        # Make sure the work we just did shows up.
        if edition.work:
            edition.work.calculate_presentation()
        else:
            edition.calculate_presentation()

        if not edition.sort_author:
            # This may be a situation like the NYT best-seller list where
            # we know the display name of the author but weren't able
            # to normalize that name.
            primary_author = self.primary_author
            if primary_author:
                self.log.info(
                    "In the absence of Contributor objects, setting Edition author name to %s/%s",
                    primary_author.sort_name,
                    primary_author.display_name
                )
                edition.sort_author = primary_author.sort_name
                edition.display_author = primary_author.display_name

        # Finally, update the coverage record for this edition
        # and data source.
        CoverageRecord.add_for(edition, data_source, self.last_update_time)
        return edition

    def update_contributions(self, _db, edition, metadata_client=None, 
                             replace_contributions=False):
        if replace_contributions and self.contributors is not None:
            dirty = False
            # Remove any old Contributions from this data source --
            # we're about to add a new set
            surviving_contributions = []
            for contribution in edition.contributions:
                _db.delete(contribution)
                dirty = True
            edition.contributions = surviving_contributions

        for contributor_data in self.contributors:
            contributor_data.find_sort_name(
                _db, self.identifiers, metadata_client
            )
            if (contributor_data.sort_name
                or contributor_data.lc 
                or contributor_data.viaf):
                contributor = edition.add_contributor(
                    name=contributor_data.sort_name, 
                    roles=contributor_data.roles,
                    lc=contributor_data.lc, 
                    viaf=contributor_data.viaf
                )
                if contributor_data.display_name:
                    contributor.display_name = contributor_data.display_name
                if contributor_data.biography:
                    contributor.biography = contributor_data.biography
                if contributor_data.aliases:
                    contributor.aliases = contributor_data.aliases
            else:
                self.log.info(
                    "Not registering %s because no sort name, LC, or VIAF",
                    contributor_data.display_name
                )
        

class CSVFormatError(csv.Error):
    pass

class CSVMetadataImporter(object):

    """Turn a CSV file into a list of Metadata objects."""

    log = logging.getLogger("CSV metadata importer")

    IDENTIFIER_PRECEDENCE = [
        Identifier.AXIS_360_ID,
        Identifier.OVERDRIVE_ID,
        Identifier.THREEM_ID,
        Identifier.ISBN
    ]

    DEFAULT_IDENTIFIER_FIELD_NAMES = {
        Identifier.OVERDRIVE_ID : ("overdrive id", 0.75),
        Identifier.THREEM_ID : ("3m id", 0.75),
        Identifier.AXIS_360_ID : ("axis 360 id", 0.75),
        Identifier.ISBN : ("isbn", 0.75),
    }
   
    DEFAULT_SUBJECT_FIELD_NAMES = {
        'tags': (Subject.TAG, 100),
        'age' : (Subject.AGE_RANGE, 100),
        'audience' : (Subject.FREEFORM_AUDIENCE, 100),
    }

    def __init__(
            self, 
            data_source_name, 
            title_field='title',
            language_field='language',
            default_language='eng',
            medium_field='medium',
            default_medium=Edition.BOOK_MEDIUM,
            series_field='series',
            publisher_field='publisher',
            imprint_field='imprint',
            issued_field='issued',
            published_field=['published', 'publication year'],
            identifier_fields=DEFAULT_IDENTIFIER_FIELD_NAMES,
            subject_fields=DEFAULT_SUBJECT_FIELD_NAMES,
            sort_author_field='file author as',
            display_author_field=['author', 'display author as']
    ):
        self.data_source_name = data_source_name
        self.title_field = title_field
        self.language_field=language_field
        self.default_language=default_language
        self.medium_field = medium_field
        self.default_medium = default_medium
        self.series_field = series_field
        self.publisher_field = publisher_field
        self.imprint_field = imprint_field
        self.issued_field = issued_field
        self.published_field = published_field
        self.identifier_fields = identifier_fields
        self.subject_fields = subject_fields
        self.sort_author_field = sort_author_field
        self.display_author_field = display_author_field

    def to_metadata(self, dictreader):
        """Turn the CSV file in `dictreader` into a sequence of Metadata.

        :yield: A sequence of Metadata objects.
        """
        fields = dictreader.fieldnames

        # Make sure this CSV file has some way of identifying books.
        found_identifier_field = False
        possibilities = []
        for field_name, weight in self.identifier_fields.values():
            possibilities.append(field_name)
            if field_name in fields:
                found_identifier_field = True
                break
        if not found_identifier_field:
            raise CSVFormatError(
                "Could not find a primary identifier field. Possibilities: %s. Actualities: %s." %
                (", ".join(possibilities),
                 ", ".join(fields))
            )

        for row in dictreader:
            yield self.row_to_metadata(row)

    def row_to_metadata(self, row):
        title = self._field(row, self.title_field)
        language = self._field(row, self.language_field, self.default_language)
        medium = self._field(row, self.medium_field, self.default_medium)
        if medium not in Edition.medium_to_additional_type.keys():
            self.log.warn("Ignored unrecognized medium %s" % medium)
            medium = Edition.BOOK_MEDIUM
        series = self._field(row, self.series_field)
        publisher = self._field(row, self.publisher_field)
        imprint = self._field(row, self.imprint_field)
        issued = self._date_field(row, self.issued_field)
        published = self._date_field(row, self.published_field)

        primary_identifier = None
        identifiers = []
        # TODO: This is annoying and could use some work.
        for identifier_type in self.IDENTIFIER_PRECEDENCE:
            correct_type = False
            for target_type, v in self.identifier_fields.items():
                if isinstance(v, tuple):
                    field_name, weight = v
                else:
                    field_name = v
                    weight = 1
                if target_type == identifier_type:
                    correct_type = True
                    break
            if not correct_type:
                continue

            if field_name in row:
                value = self._field(row, field_name)
                if value:
                    identifier = IdentifierData(
                        identifier_type, value, weight=weight
                    )
                    identifiers.append(identifier)
                    if not primary_identifier:
                        primary_identifier = identifier

        subjects = []
        for (field_name, (subject_type, weight)) in self.subject_fields.items():
            values = self.list_field(row, field_name)
            for value in values:
                subjects.append(
                    SubjectData(
                        type=subject_type,
                        identifier=value,
                        weight=weight
                    )
                )
        
        contributors = []
        sort_author = self._field(row, self.sort_author_field)
        display_author = self._field(row, self.display_author_field)
        if sort_author or display_author:
            contributors.append(
                ContributorData(
                    sort_name=sort_author, display_name=display_author, 
                    roles=[Contributor.AUTHOR_ROLE]
                )
            )
        
        metadata = Metadata(
            data_source=self.data_source_name,
            title=title,
            language=language,
            medium=medium,
            series=series,
            publisher=publisher,
            imprint=imprint,
            issued=issued,
            published=published,
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors
        )
        metadata.csv_row = row
        return metadata

    @property
    def identifier_field_names(self):
        """All potential field names that would identify an identifier."""
        for identifier_type in self.IDENTIFIER_PRECEDENCE:
            field_names = self.identifier_fields.get(identifier_type, [])
            if isinstance(field_names, basestring):
                field_names = [field_names]
            for field_name in field_names:
                yield field_name

    def list_field(self, row, names):
        """Parse a string into a list by splitting on commas."""
        value = self._field(row, names)
        if not value:
            return []
        return [item.strip() for item in value.split(",")]

    def _field(self, row, names, default=None):
        """Get a value from one of the given fields and ensure it comes in as
        Unicode.
        """
        if isinstance(names, basestring):
            return self.__field(row, names, default)
        if not names:
            return default
        for name in names:
            v = self.__field(row, name)
            if v:
                return v
        else:
            return default

    def __field(self, row, name, default=None):
        """Get a value from the given field and ensure it comes in as
        Unicode.
        """
        value = row.get(name, default)
        if isinstance(value, basestring):
            value = value.decode("utf8")
        return value

    def _date_field(self, row, field_name):
        """Attempt to parse a field as a date."""
        date = None
        value = self._field(row, field_name)
        if value:
            try:
                value = parse(value)
            except ValueError:
                self.log.warn('Could not parse date "%s"' % value)
                value = None
        return value
