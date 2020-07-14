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
from sqlalchemy.sql.expression import and_, or_
from sqlalchemy.orm.exc import (
    NoResultFound,
)
from sqlalchemy.orm import aliased
import csv
import datetime
import logging
import re

from pymarc import MARCReader

from classifier import Classifier
from util import LanguageCodes
from util.http import RemoteIntegrationException
from util.personal_names import name_tidy
from util.median import median
from model import (
    get_one,
    get_one_or_create,
    CirculationEvent,
    Classification,
    Collection,
    Contributor,
    CoverageRecord,
    DataSource,
    DeliveryMechanism,
    Edition,
    Equivalency,
    Hyperlink,
    Identifier,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
    LinkRelations,
    Subject,
    Hyperlink,
    PresentationCalculationPolicy,
    RightsStatus,
    Representation,
    Resource,
    Timestamp,
    Work,
)
from model.configuration import ExternalIntegrationLink
from classifier import NO_VALUE, NO_NUMBER
from analytics import Analytics
from util.personal_names import display_name_to_sort_name

class ReplacementPolicy(object):
    """How serious should we be about overwriting old metadata with
    this new metadata?
    """
    def __init__(
            self,
            identifiers=False,
            subjects=False,
            contributions=False,
            links=False,
            formats=False,
            rights=False,
            link_content=False,
            mirrors=None,
            content_modifier=None,
            analytics=None,
            http_get=None,
            even_if_not_apparently_updated=False,
            presentation_calculation_policy=None
    ):
        self.identifiers = identifiers
        self.subjects = subjects
        self.contributions = contributions
        self.links = links
        self.rights = rights
        self.formats = formats
        self.link_content = link_content
        self.even_if_not_apparently_updated = even_if_not_apparently_updated
        self.mirrors = mirrors
        self.content_modifier = content_modifier
        self.analytics = analytics
        self.http_get = http_get
        self.presentation_calculation_policy = (
            presentation_calculation_policy or
            PresentationCalculationPolicy()
        )

    @classmethod
    def from_license_source(cls, _db, **args):
        """When gathering data from the license source, overwrite all old data
        from this source with new data from the same source. Also
        overwrite an old rights status with an updated status and update
        the list of available formats. Log availability changes to the
        configured analytics services.
        """
        return cls(
            identifiers=True,
            subjects=True,
            contributions=True,
            links=True,
            rights=True,
            formats=True,
            analytics=Analytics(_db),
            **args
        )

    @classmethod
    def from_metadata_source(cls, **args):
        """When gathering data from a metadata source, overwrite all old data
        from this source, but do not overwrite the rights status or
        the available formats. License sources are the authority on rights
        and formats, and metadata sources have no say in the matter.
        """
        return cls(
            identifiers=True,
            subjects=True,
            contributions=True,
            links=True,
            rights=False,
            formats=False,
            **args
        )

    @classmethod
    def append_only(cls, **args):
        """Don't overwrite any information, just append it.

        This should probably never be used.
        """
        return cls(
            identifiers=False,
            subjects=False,
            contributions=False,
            links=False,
            rights=False,
            formats=False,
            **args
        )

class SubjectData(object):
    def __init__(self, type, identifier, name=None, weight=1):
        self.type = type

        # Because subjects are sometimes evaluated according to keyword
        # matching, it's important that any leading or trailing white
        # space is removed during import.
        self.identifier = identifier
        if identifier:
            self.identifier = identifier.strip()

        self.name = name
        if name:
            self.name = name.strip()

        self.weight = weight

    @property
    def key(self):
        return self.type, self.identifier, self.name, self.weight

    def __repr__(self):
        return '<SubjectData type="%s" identifier="%s" name="%s" weight=%d>' % (
            self.type, self.identifier, self.name, self.weight
        )


class ContributorData(object):

    def __init__(self, sort_name=None, display_name=None,
                 family_name=None, wikipedia_name=None, roles=None,
                 lc=None, viaf=None, biography=None, aliases=None, extra=None):
        self.sort_name = sort_name
        self.display_name = display_name
        self.family_name = family_name
        self.wikipedia_name = wikipedia_name
        if roles is None:
            roles = Contributor.AUTHOR_ROLE
        if not isinstance(roles, list):
            roles = [roles]
        self.roles = roles
        self.lc = lc
        self.viaf = viaf
        self.biography = biography
        self.aliases = aliases or []
        # extra is a dictionary of stuff like birthdates
        self.extra = extra or dict()
        # TODO:  consider if it's time for ContributorData to connect back to Contributions


    def __repr__(self):
        return '<ContributorData sort="%s" display="%s" family="%s" wiki="%s" roles=%r lc=%s viaf=%s>' % (self.sort_name, self.display_name, self.family_name, self.wikipedia_name, self.roles, self.lc, self.viaf)


    @classmethod
    def from_contribution(cls, contribution):
        """Create a ContributorData object from a data-model Contribution
        object.
        """
        c = contribution.contributor
        return cls(
            sort_name=c.sort_name,
            display_name=c.display_name,
            family_name=c.family_name,
            wikipedia_name=c.wikipedia_name,
            lc=c.lc,
            viaf=c.viaf,
            biography=c.biography,
            aliases=c.aliases,
            roles=[contribution.role]
        )

    @classmethod
    def lookup(cls, _db, sort_name=None, display_name=None, lc=None,
               viaf=None):
        """Create a (potentially synthetic) ContributorData based on
        the best available information in the database.

        :return: A ContributorData.
        """
        clauses = []
        if sort_name:
            clauses.append(Contributor.sort_name==sort_name)
        if display_name:
            clauses.append(Contributor.display_name==display_name)
        if lc:
            clauses.append(Contributor.lc==lc)
        if viaf:
            clauses.append(Contributor.viaf==viaf)

        if not clauses:
            raise ValueError("No Contributor information provided!")

        or_clause = or_(*clauses)
        contributors = _db.query(Contributor).filter(or_clause).all()
        if len(contributors) == 0:
            # We have no idea who this person is.
            return None

        # We found at least one matching Contributor. Let's try to
        # build a composite ContributorData for the person.
        values_by_field = defaultdict(set)

        # If all the people we found share (e.g.) a VIAF field, then
        # we can use that as a clue when doing a search -- anyone with
        # that VIAF number is probably this person, even if their display
        # name doesn't match.
        for c in contributors:
            if c.sort_name:
                values_by_field['sort_name'].add(c.sort_name)
            if c.display_name:
                values_by_field['display_name'].add(c.display_name)
            if c.lc:
                values_by_field['lc'].add(c.lc)
            if c.viaf:
                values_by_field['viaf'].add(c.viaf)

        # Use any passed-in values as default values for the
        # ContributorData. Below, missing values may be filled in and
        # inaccurate values may be replaced.
        kwargs = dict(
            sort_name=sort_name,
            display_name=display_name,
            lc=lc,
            viaf=viaf
        )
        for k, values in values_by_field.items():
            if len(values) == 1:
                # All the Contributors we found have the same
                # value for this field. We can use it.
                kwargs[k] = list(values)[0]

        return ContributorData(roles=[], **kwargs)

    def apply(self, destination, replace=None):
        """ Update the passed-in Contributor-type object with this
        ContributorData's information.

        :param: destination -- the Contributor or ContributorData object to
            write this ContributorData object's metadata to.
        :param: replace -- Replacement policy (not currently used).

        :return: the possibly changed Contributor object and a flag of whether it's been changed.
        """
        log = logging.getLogger("Abstract metadata layer")
        log.debug("Applying %r (%s) into %r (%s)", self, self.viaf, destination, destination.viaf)

        made_changes = False

        if self.sort_name and self.sort_name != destination.sort_name:
            destination.sort_name = self.sort_name
            made_changes = True

        existing_aliases = set(destination.aliases)
        new_aliases = list(destination.aliases)
        for name in [self.sort_name] + self.aliases:
            if name != destination.sort_name and name not in existing_aliases:
                new_aliases.append(name)
                made_changes = True
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases
            made_changes = True

        for k, v in self.extra.items():
            if not k in destination.extra:
                destination.extra[k] = v

        if self.lc and self.lc != destination.lc:
            destination.lc = self.lc
            made_changes = True
        if self.viaf and self.viaf != destination.viaf:
            destination.viaf = self.viaf
            made_changes = True
        if (self.family_name and
            self.family_name != destination.family_name):
            destination.family_name = self.family_name
            made_changes = True
        if (self.display_name and
            self.display_name != destination.display_name):
            destination.display_name = self.display_name
            made_changes = True
        if (self.wikipedia_name and
            self.wikipedia_name != destination.wikipedia_name):
            destination.wikipedia_name = self.wikipedia_name
            made_changes = True

        if (self.biography and
            self.biography != destination.biography):
            destination.biography = self.biography
            made_changes = True

        # TODO:  Contributor.merge_into also looks at
        # contributions.  Could maybe extract contributions from roles,
        # but not sure it'd be useful.

        return destination, made_changes


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
        # If not, take our best guess based on the display name.
        sort_name = self.display_name_to_sort_name_from_existing_contributor(
            _db, self.display_name)
        if sort_name:
            self.sort_name = sort_name
            return True

        # Time to break out the big guns. Ask the metadata wrangler
        # if it can find a sort name for this display name.
        if metadata_client:
            try:
                sort_name = self.display_name_to_sort_name_through_canonicalizer(
                    _db, identifiers, metadata_client
                )
            except RemoteIntegrationException, e:
                # There was some kind of problem with the metadata
                # wrangler. Act as though no metadata wrangler had
                # been provided.
                log = logging.getLogger("Abstract metadata layer")
                log.error(
                    "Metadata client exception while determining sort name for %s",
                    self.display_name, exc_info=e
                )
            if sort_name:
                self.sort_name = sort_name
                return True

        # If there's still no sort name, take our best guess based
        # on the display name.
        self.sort_name = display_name_to_sort_name(self.display_name)

        return (self.sort_name is not None)

    @classmethod
    def display_name_to_sort_name_from_existing_contributor(self, _db, display_name):
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
                Contributor.sort_name != None).all()
        if contributors:
            log = logging.getLogger("Abstract metadata layer")
            log.debug(
                "Determined that sort name of %s is %s based on previously existing contributor",
                display_name,
                contributors[0].sort_name
            )
            return contributors[0].sort_name
        return None

    def _display_name_to_sort_name(
            self, _db, metadata_client, identifier_obj
    ):
        response = metadata_client.canonicalize_author_name(
            identifier_obj, self.display_name)
        sort_name = None

        if isinstance(response, basestring):
            sort_name = response
        else:
            log = logging.getLogger("Abstract metadata layer")
            if (response.status_code == 200
                and response.headers['Content-Type'].startswith('text/plain')):
                sort_name = response.content
                log.info(
                    "Canonicalizer found sort name for %r: %s => %s",
                    identifier_obj, self.display_name, sort_name
                )
            else:
                log.warn(
                    "Canonicalizer could not find sort name for %r/%s",
                    identifier_obj, self.display_name
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
        self.weight = weight
        self.identifier = identifier

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
                 thumbnail=None, rights_uri=None, rights_explanation=None,
                 original=None, transformation_settings=None):
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
        # rather than each edition, and rights for cover images.
        self.rights_uri = rights_uri
        self.rights_explanation = rights_explanation
        # If this LinkData is a derivative, it may also contain the original link
        # and the settings used to transform the original into the derivative.
        self.original = original
        self.transformation_settings = transformation_settings or {}

    @property
    def guessed_media_type(self):
        """If the media type of a link is unknown, take a guess."""
        if self.media_type:
            # We know.
            return self.media_type

        if self.href:
            # Take a guess.
            return Representation.guess_media_type(self.href)

        # No idea.
        # TODO: We might be able to take a further guess based on the
        # content and the link relation.
        return None

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

    def mirror_type(self):
        """Returns the type of mirror that should be used for the link.
        """
        if self.rel in [Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE]:
            return ExternalIntegrationLink.COVERS

        if self.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
            return ExternalIntegrationLink.OPEN_ACCESS_BOOKS
        elif self.rel == Hyperlink.GENERIC_OPDS_ACQUISITION:
            return ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS


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
    def __init__(self, content_type, drm_scheme, link=None, rights_uri=None):
        self.content_type = content_type
        self.drm_scheme = drm_scheme
        if link and not isinstance(link, LinkData):
            raise TypeError(
                "Expected LinkData object, got %s" % type(link)
            )
        self.link = link
        self.rights_uri = rights_uri
        if ((not self.rights_uri) and self.link and self.link.rights_uri):
            self.rights_uri = self.link.rights_uri


class LicenseData(object):
    def __init__(self, identifier, checkout_url, status_url, expires=None, remaining_checkouts=None,
                 concurrent_checkouts=None):
        self.identifier = identifier
        self.checkout_url = checkout_url
        self.status_url = status_url
        self.expires = expires
        self.remaining_checkouts = remaining_checkouts
        self.concurrent_checkouts = concurrent_checkouts


class TimestampData(object):

    CLEAR_VALUE = Timestamp.CLEAR_VALUE

    def __init__(self, start=None, finish=None, achievements=None,
                 counter=None, exception=None):
        """A constructor intended to be used by a service to customize its
        eventual Timestamp.

        service, service_type, and collection cannot be set through
        this constructor, because they are generally not under the
        control of the code that runs the service. They are set
        afterwards, in finalize().

        :param start: The time that the service should be considered to
           have started running.
        :param finish: The time that the service should be considered
           to have stopped running.
        :param achievements: A string describing what was achieved by the
           service.
        :param counter: A single integer item of state representing the point
           at which the service left off.
        :param exception: A traceback representing an exception that stopped
           the progress of the service.
        """

        # These are set by finalize().
        self.service = None
        self.service_type = None
        self.collection_id = None

        self.start = start
        self.finish = finish
        self.achievements = achievements
        self.counter = counter
        self.exception = exception

    @property
    def is_failure(self):
        """Does this TimestampData represent an unrecoverable failure?"""
        return self.exception not in (None, self.CLEAR_VALUE)

    @property
    def is_complete(self):
        """Does this TimestampData represent an operation that has
        completed?

        An operation is completed if it has failed, or if the time of its
        completion is known.
        """
        return self.is_failure or self.finish not in (None, self.CLEAR_VALUE)

    def finalize(self, service, service_type, collection, start=None,
                 finish=None, achievements=None, counter=None,
                 exception=None):
        """Finalize any values that were not set during the constructor.

        This is intended to be run by the code that originally ran the
        service.

        The given values for `start`, `finish`, `achievements`,
        `counter`, and `exception` will be used only if the service
        did not specify its own values for those fields.
        """
        self.service = service
        self.service_type = service_type
        if collection is None:
            self.collection_id = None
        else:
            self.collection_id = collection.id
        if self.start is None:
            self.start = start
        if self.finish is None:
            if finish is None:
                finish = datetime.datetime.utcnow()
            self.finish = finish
        if self.start is None:
            self.start = self.finish
        if self.counter is None:
            self.counter = counter
        if self.exception is None:
            self.exception = exception

    def collection(self, _db):
        return get_one(_db, Collection, id=self.collection_id)

    def apply(self, _db):
        if any(x is None for x in [self.service, self.service_type]):
            raise ValueError(
                "Not enough information to write TimestampData to the database."
            )

        return Timestamp.stamp(
            _db, self.service, self.service_type, self.collection(_db),
            self.start, self.finish, self.achievements, self.counter,
            self.exception
        )


class MetaToModelUtility(object):
    """
    Contains functionality common to both CirculationData and Metadata.
    """

    log = logging.getLogger("Abstract metadata layer - mirror code")

    def mirror_link(self, model_object, data_source, link, link_obj, policy):
        """Retrieve a copy of the given link and make sure it gets
        mirrored. If it's a full-size image, create a thumbnail and
        mirror that too.

        The model_object can be either a pool or an edition.
        """
        if link_obj.rel not in Hyperlink.MIRRORED:
            # we only host locally open-source epubs and cover images
            if link.href:
                # The log message only makes sense if the resource is
                # hosted elsewhere.
                self.log.info("Not mirroring %s: rel=%s", link.href, link_obj.rel)
            return

        if (link.rights_uri
            and link.rights_uri == RightsStatus.IN_COPYRIGHT):
            self.log.info(
                "Not mirroring %s: rights status=%s" % (
                    link.href, link.rights_uri
                )
            )
            return

        mirror_type = link.mirror_type()

        if mirror_type in policy.mirrors:
            mirror = policy.mirrors[mirror_type]
            if not mirror:
                return
        else:
            self.log.info(
                "No mirror uploader with key %s found" % mirror_type
            )
            return

        http_get = policy.http_get

        _db = Session.object_session(link_obj)
        original_url = link.href

        self.log.info("About to mirror %s" % original_url)
        pools = []
        edition = None
        title = None
        identifier = None
        if model_object:
            if isinstance(model_object, LicensePool):
                pools = [model_object]
                identifier = model_object.identifier

                if (identifier and identifier.primarily_identifies and identifier.primarily_identifies[0]):
                    edition = identifier.primarily_identifies[0]
            elif isinstance(model_object, Edition):
                pools = model_object.license_pools
                identifier = model_object.primary_identifier
                edition = model_object
        if edition and edition.title:
            title = edition.title
        else:
            title = getattr(self, 'title', None) or None

        if ((not identifier) or (link_obj.identifier and identifier != link_obj.identifier)):
            # insanity found
            self.log.warn("Tried to mirror a link with an invalid identifier %r" % identifier)
            return

        max_age = None
        if policy.link_content:
            # We want to fetch the representation again, even if we
            # already have a recent usable copy. If we fetch it and it
            # hasn't changed, we'll keep using the one we have.
            max_age = 0

        # This will fetch a representation of the original and
        # store it in the database.
        representation, is_new = Representation.get(
            _db, link.href, do_get=http_get,
            presumed_media_type=link.media_type,
            max_age=max_age,
        )

        # Make sure the (potentially newly-fetched) representation is
        # associated with the resource.
        link_obj.resource.representation = representation

        # If we couldn't fetch this representation, don't mirror it,
        # and if this was an open/protected access link, then suppress the associated
        # license pool until someone fixes it manually.
        # The license pool to suppress will be either the passed-in model_object (if it's of type pool),
        # or the license pool associated with the passed-in model object (if it's of type edition).
        if representation.fetch_exception:
            if pools and link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
                for pool in pools:
                    pool.suppressed = True
                    pool.license_exception = "Fetch exception: %s" % representation.fetch_exception
                    self.log.error(pool.license_exception)
            return

        # If we fetched the representation and it hasn't changed,
        # the previously mirrored version is fine. Don't mirror it
        # again.
        if representation.status_code == 304 and representation.mirror_url:
            self.log.info(
                "Representation has not changed, assuming mirror at %s is up to date.", representation.mirror_url
            )
            return

        if representation.status_code // 100 not in (2,3):
            self.log.info(
                "Representation %s gave %s status code, not mirroring.",
                representation.url, representation.status_code
            )
            return

        if policy.content_modifier:
            policy.content_modifier(representation)

        # The metadata may have some idea about the media type for this
        # LinkObject, but it could be wrong. If the representation we
        # actually just saw is a mirrorable media type, that takes
        # precedence. If we were expecting this link to be mirrorable
        # but we actually saw something that's not, assume our original
        # metadata was right and the server told us the wrong media type.
        if representation.media_type and representation.mirrorable_media_type:
            link.media_type = representation.media_type

        if not representation.mirrorable_media_type:
            if link.media_type:
                self.log.info("Saw unsupported media type for %s: %s. Assuming original media type %s is correct",
                              representation.url, representation.media_type, link.media_type)
                representation.media_type = link.media_type
            else:
                self.log.info("Not mirroring %s: unsupported media type %s",
                              representation.url, representation.media_type)
                return

        # Determine the best URL to use when mirroring this
        # representation.
        if link.media_type in Representation.BOOK_MEDIA_TYPES:
            url_title = title or identifier.identifier
            extension = representation.extension()
            mirror_url = mirror.book_url(
                identifier,
                data_source=data_source,
                title=url_title,
                extension=extension,
                open_access=link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD
            )
        else:
            filename = representation.default_filename(
                link_obj, representation.media_type
            )
            mirror_url = mirror.cover_image_url(
                data_source, identifier, filename
            )

        # Mirror it.
        mirror.mirror_one(representation, mirror_url)

        # If we couldn't mirror an open/protected access link representation, suppress
        # the license pool until someone fixes it manually.
        if representation.mirror_exception:
            if pools and link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD:
                for pool in pools:
                    pool.suppressed = True
                    pool.license_exception = "Mirror exception: %s" % representation.mirror_exception
                    self.log.error(pool.license_exception)

        if link_obj.rel == Hyperlink.IMAGE:
            # Create and mirror a thumbnail.
            thumbnail_filename = representation.default_filename(
                link_obj, Representation.PNG_MEDIA_TYPE
            )
            thumbnail_url = mirror.cover_image_url(
                data_source, identifier, thumbnail_filename,
                Edition.MAX_THUMBNAIL_HEIGHT
            )
            thumbnail, is_new = representation.scale(
                max_height=Edition.MAX_THUMBNAIL_HEIGHT,
                max_width=Edition.MAX_THUMBNAIL_WIDTH,
                destination_url=thumbnail_url,
                destination_media_type=Representation.PNG_MEDIA_TYPE,
                force=True
            )
            if is_new:
                # A thumbnail was created distinct from the original
                # image. Mirror it as well.
                mirror.mirror_one(thumbnail, thumbnail_url)

        if link_obj.rel in Hyperlink.SELF_HOSTED_BOOKS:
            # If we mirrored book content successfully, remove it from
            # the database to save space. We do keep images in case we
            # ever need to resize them or mirror them elsewhere.
            if representation.mirrored_at and not representation.mirror_exception:
                representation.content = None


class CirculationData(MetaToModelUtility):
    """Information about actual copies of a book that can be delivered to
    patrons.

    As distinct from Metadata, which is a container for information
    about a book.

    Basically,
        Metadata : Edition :: CirculationData : Licensepool
    """

    log = logging.getLogger(
        "Abstract metadata layer - Circulation data"
    )

    def __init__(
            self,
            data_source,
            primary_identifier,
            licenses_owned=None,
            licenses_available=None,
            licenses_reserved=None,
            patrons_in_hold_queue=None,
            formats=None,
            default_rights_uri=None,
            links=None,
            licenses=None,
            last_checked=None,
    ):
        """Constructor.

        :param data_source: The authority providing the lending licenses.
            This may be a DataSource object or the name of the data source.
        :param primary_identifier: An Identifier or IdentifierData representing
            how the lending authority distinguishes this book from others.
        """
        self._data_source = data_source

        if isinstance(self._data_source, DataSource):
            self.data_source_obj = self._data_source
            self.data_source_name = self.data_source_obj.name
        else:
            self.data_source_obj = None
            self.data_source_name = data_source
        if isinstance(primary_identifier, Identifier):
            self.primary_identifier_obj = primary_identifier
            self._primary_identifier = IdentifierData(
                primary_identifier.type, primary_identifier.identifier
            )
        else:
            self.primary_identifier_obj = None
            self._primary_identifier = primary_identifier
        self.licenses_owned = licenses_owned
        self.licenses_available = licenses_available
        self.licenses_reserved = licenses_reserved
        self.patrons_in_hold_queue = patrons_in_hold_queue

        # If no 'last checked' data was provided, assume the data was
        # just gathered.
        self.last_checked = last_checked or datetime.datetime.utcnow()

        # format contains pdf/epub, drm, link
        self.formats = formats or []

        self.default_rights_uri = None
        self.set_default_rights_uri(data_source_name=self.data_source_name, default_rights_uri=default_rights_uri)

        self.__links = None
        self.links = links

        # Information about individual terms for each license in a pool.
        self.licenses = licenses or []

    @property
    def links(self):
        return self.__links

    @links.setter
    def links(self, arg_links):
        """ If got passed all links, undiscriminately, filter out to only those relevant to
            pools (the rights-related links).
        """
        # start by deleting any old links
        self.__links = []

        if not arg_links:
            return

        for link in arg_links:
            if link.rel in Hyperlink.CIRCULATION_ALLOWED:
                # TODO:  what about Hyperlink.SAMPLE?
                # only accept the types of links relevant to pools
                self.__links.append(link)

                # An open-access link or open-access rights implies a FormatData object.
                open_access_link = (link.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD and link.href)
                # try to deduce if the link is open-access, even if it doesn't explicitly say it is
                rights_uri =  link.rights_uri or self.default_rights_uri
                open_access_rights_link = (
                    link.media_type in Representation.BOOK_MEDIA_TYPES
                    and link.href
                    and rights_uri in RightsStatus.OPEN_ACCESS
                )

                if open_access_link or open_access_rights_link:
                    if (open_access_link
                        and rights_uri != RightsStatus.IN_COPYRIGHT
                        and not rights_uri in RightsStatus.OPEN_ACCESS):
                        # We don't know exactly what's going on here but
                        # the link said it was an open-access book
                        # and the rights URI doesn't contradict it,
                        # so treat it as a generic open-access book.
                        rights_uri = RightsStatus.GENERIC_OPEN_ACCESS
                    format_found = False
                    for format in self.formats:
                        if format and format.link and format.link.href == link.href:
                            if not format.rights_uri:
                                format.rights_uri = rights_uri
                            format_found = True
                            break
                    if not format_found:
                        self.formats.append(
                            FormatData(
                                content_type=link.media_type,
                                drm_scheme=DeliveryMechanism.NO_DRM,
                                link=link,
                                rights_uri=rights_uri,
                            )
                        )


    def __repr__(self):
        description_string = '<CirculationData primary_identifier=%(primary_identifier)r| licenses_owned=%(licenses_owned)s|'
        description_string += ' licenses_available=%(licenses_available)s| default_rights_uri=%(default_rights_uri)s|'
        description_string += ' links=%(links)r| formats=%(formats)r| data_source=%(data_source)s|>'


        description_data = {'licenses_owned':self.licenses_owned}
        if self._primary_identifier:
            description_data['primary_identifier'] = self._primary_identifier
        else:
            description_data['primary_identifier'] = self.primary_identifier_obj
        description_data['licenses_available'] = self.licenses_available
        description_data['default_rights_uri'] = self.default_rights_uri
        description_data['links'] = self.links
        description_data['formats'] = self.formats
        description_data['data_source'] = self.data_source_name

        return description_string % description_data

    def data_source(self, _db):
        """Find the DataSource associated with this circulation information."""
        if not self.data_source_obj:
            if self._data_source:
                obj = DataSource.lookup(_db, self._data_source)
                if not obj:
                    raise ValueError("Data source %s not found!" % self._data_source)
            else:
                obj = None
            self.data_source_obj = obj
        return self.data_source_obj

    def primary_identifier(self, _db):
        """Find the Identifier associated with this circulation information."""
        if not self.primary_identifier_obj:
            if self._primary_identifier:
                obj, ignore = self._primary_identifier.load(_db)
            else:
                obj = None
            self.primary_identifier_obj = obj
        return self.primary_identifier_obj

    def license_pool(self, _db, collection, analytics=None):
        """Find or create a LicensePool object for this CirculationData.

        :param collection: The LicensePool object will be associated with
            the given Collection.

        :param analytics: If the LicensePool is newly created, the event
            will be tracked with this.
        """
        if not collection:
            raise ValueError(
                "Cannot find license pool: no collection provided."
            )
        identifier = self.primary_identifier(_db)
        if not identifier:
            raise ValueError(
                "Cannot find license pool: CirculationData has no primary identifier."
            )

        data_source_obj = self.data_source(_db)
        license_pool, is_new = LicensePool.for_foreign_id(
            _db, data_source=data_source_obj,
            foreign_id_type=identifier.type,
            foreign_id=identifier.identifier,
            collection=collection
        )

        if is_new:
            license_pool.open_access = self.has_open_access_link
            license_pool.availability_time = self.last_checked
            # This is our first time seeing this LicensePool. Log its
            # occurrence as a separate analytics event.
            if analytics:
                for library in collection.libraries:
                    analytics.collect_event(
                        library, license_pool,
                        CirculationEvent.DISTRIBUTOR_TITLE_ADD,
                        self.last_checked,
                        old_value=0, new_value=1,
                    )
            license_pool.last_checked = self.last_checked

        return license_pool, is_new


    @property
    def has_open_access_link(self):
        """Does this Circulation object have an associated open-access link?"""
        return any(
            [x for x in self.links
             if x.rel == Hyperlink.OPEN_ACCESS_DOWNLOAD
             and x.href
             and x.rights_uri != RightsStatus.IN_COPYRIGHT
            ]
        )


    def set_default_rights_uri(self, data_source_name, default_rights_uri=None):
        if default_rights_uri:
            self.default_rights_uri = default_rights_uri

        elif data_source_name:
            # We didn't get rights passed in, so use the default rights for the data source if any.
            default = RightsStatus.DATA_SOURCE_DEFAULT_RIGHTS_STATUS.get(data_source_name, None)
            if default:
                self.default_rights_uri = default

        if not self.default_rights_uri:
            # We still haven't determined rights, so it's unknown.
            self.default_rights_uri = RightsStatus.UNKNOWN

    def apply(self, _db, collection, replace=None):
        """Update the title with this CirculationData's information.

        :param collection: A Collection representing actual copies of
            this title. Availability information (e.g. number of copies)
            will be associated with a LicensePool in this Collection. If
            this is not present, only delivery information (e.g. format
            information and open-access downloads) will be processed.

        """
        # Immediately raise an exception if there is information that
        # can only be stored in a LicensePool, but we have no
        # Collection to tell us which LicensePool to use. This is
        # indicative of an error in programming.
        if not collection and (self.licenses_owned is not None
                               or self.licenses_available is not None
                               or self.licenses_reserved is not None
                               or self.patrons_in_hold_queue is not None):
            raise ValueError(
                "Cannot store circulation information because no "
                "Collection was provided."
            )

        made_changes = False
        if replace is None:
            replace = ReplacementPolicy()

        analytics = replace.analytics or Analytics(_db)

        pool = None
        if collection:
            pool, ignore = self.license_pool(_db, collection, analytics)

        data_source = self.data_source(_db)
        identifier = self.primary_identifier(_db)
        # First, make sure all links in self.links are mirrored (if necessary)
        # and associated with the book's identifier.

        # TODO: be able to handle the case where the URL to a link changes or
        # a link disappears.
        link_objects = {}
        for link in self.links:
            if link.rel in Hyperlink.CIRCULATION_ALLOWED:
                link_obj, ignore = identifier.add_link(
                    rel=link.rel, href=link.href, data_source=data_source,
                    media_type=link.media_type, content=link.content
                )
                link_objects[link] = link_obj

        for link in self.links:
            if link.rel in Hyperlink.CIRCULATION_ALLOWED:
                link_obj = link_objects[link]
                if replace.mirrors:
                    # We need to mirror this resource.
                    self.mirror_link(pool, data_source, link, link_obj, replace)

        # Next, make sure the DeliveryMechanisms associated
        # with the book reflect the formats in self.formats.
        old_lpdms = new_lpdms = []
        if pool:
            old_lpdms = list(pool.delivery_mechanisms)

        # Before setting and unsetting delivery mechanisms, which may
        # change the open-access status of the work, see what it the
        # status currently is.
        pools = identifier.licensed_through
        old_open_access = any(pool.open_access for pool in pools)

        for format in self.formats:
            if format and format.link:
                link = format.link
                if not format.content_type:
                    format.content_type = link.media_type
                link_obj = link_objects[format.link]
                resource = link_obj.resource
            else:
                resource = None
            # This can cause a non-open-access LicensePool to go open-access.
            lpdm = LicensePoolDeliveryMechanism.set(
                data_source, identifier, format.content_type,
                format.drm_scheme,
                format.rights_uri or self.default_rights_uri,
                resource
            )
            new_lpdms.append(lpdm)

        if replace.formats:
            # If any preexisting LicensePoolDeliveryMechanisms were
            # not mentioned in self.formats, remove the corresponding
            # LicensePoolDeliveryMechanisms.
            for lpdm in old_lpdms:
                if lpdm not in new_lpdms:
                    for loan in lpdm.fulfills:
                        self.log.info("Loan %i is associated with a format that is no longer available. Deleting its delivery mechanism." % loan.id)
                        loan.fulfillment = None
                    # This can cause an open-access LicensePool to go
                    # non-open-access.
                    lpdm.delete()

        new_open_access = any(pool.open_access for pool in pools)
        open_access_status_changed = (old_open_access != new_open_access)

        old_licenses = new_licenses = []
        if pool:
            old_licenses = list(pool.licenses or [])

            for license in self.licenses:
                license_obj, ignore = get_one_or_create(
                    _db, License, identifier=license.identifier,
                    license_pool_id=pool.id,
                )
                license_obj.checkout_url = license.checkout_url
                license_obj.status_url = license.status_url
                license_obj.expires = license.expires
                license_obj.remaining_checkouts = license.remaining_checkouts
                license_obj.concurrent_checkouts = license.concurrent_checkouts
                new_licenses.append(license_obj)

        for license in old_licenses:
            if license not in new_licenses and license.loans:
                # TODO: For ODL, I don't think this will happen, but
                # it seems right not to delete the license if it does.
                # We have the status URL we can use to check on the license,
                # and if it is removed from the ODL feed when there are
                # still loans we'll need it.
                # But if we track individual licenses for other protocols,
                # we may need to handle this differently.
                self.log.warn("License %i is no longer available but still has loans." % license.id)

        # Finally, if we have data for a specific Collection's license
        # for this book, find its LicensePool and update it.
        changed_availability = False
        if pool and self._availability_needs_update(pool):
            # Update availabily information. This may result in
            # the issuance of additional circulation events.
            changed_availability = pool.update_availability(
                new_licenses_owned=self.licenses_owned,
                new_licenses_available=self.licenses_available,
                new_licenses_reserved=self.licenses_reserved,
                new_patrons_in_hold_queue=self.patrons_in_hold_queue,
                analytics=analytics,
                as_of=self.last_checked
            )

        # If this is the first time we've seen this pool, or we never
        # made a Work for it, make one now.
        work_changed = False
        if pool and not pool.work:
            work, work_changed = pool.calculate_work()
            if work:
                work.set_presentation_ready()
                work_changed = True

        made_changes = (made_changes or changed_availability
                        or open_access_status_changed
                        or work_changed)

        return pool, made_changes

    def _availability_needs_update(self, pool):
        """Does this CirculationData represent information more recent than
        what we have for the given LicensePool?
        """
        if not self.last_checked:
            # Assume that our data represents the state of affairs
            # right now.
            return True
        if not pool.last_checked:
            # It looks like the LicensePool has never been checked.
            return True
        return self.last_checked >= pool.last_checked


class Metadata(MetaToModelUtility):

    """A (potentially partial) set of metadata for a published work."""

    log = logging.getLogger("Abstract metadata layer")

    BASIC_EDITION_FIELDS = [
        'title', 'sort_title', 'subtitle', 'language', 'medium',
        'series', 'series_position', 'publisher', 'imprint',
        'issued', 'published'
    ]

    def __init__(
            self,
            data_source,
            title=None,
            subtitle=None,
            sort_title=None,
            language=None,
            medium=None,
            series=None,
            series_position=None,
            publisher=None,
            imprint=None,
            issued=None,
            published=None,
            primary_identifier=None,
            identifiers=None,
            recommendations=None,
            subjects=None,
            contributors=None,
            measurements=None,
            links=None,
            data_source_last_updated=None,
            # Note: brought back to keep callers of bibliographic extraction process_one() methods simple.
            circulation=None,
    ):
        # data_source is where the data comes from (e.g. overdrive, metadata wrangler, admin interface),
        # and not necessarily where the associated Identifier's LicencePool's lending licenses are coming from.
        self._data_source = data_source
        if isinstance(self._data_source, DataSource):
            self.data_source_obj = self._data_source
        else:
            self.data_source_obj = None

        self.title = title
        self.sort_title = sort_title
        self.subtitle = subtitle
        if language:
            language = LanguageCodes.string_to_alpha_3(language)
        self.language = language
        # medium is book/audio/video, etc.
        self.medium = medium
        self.series = series
        self.series_position = series_position
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
        self.recommendations = recommendations or []
        self.subjects = subjects or []
        self.contributors = contributors or []
        self.measurements = measurements or []

        self.circulation = circulation

        # renamed last_update_time to data_source_last_updated
        self.data_source_last_updated = data_source_last_updated

        self.__links = None
        self.links = links

    @property
    def links(self):
        return self.__links

    @links.setter
    def links(self, arg_links):
        """ If got passed all links, undiscriminately, filter out to only those relevant to
            editions (the image/cover/etc links).
        """
        # start by deleting any old links
        self.__links = []

        if not arg_links:
            return

        for link in arg_links:
            if link.rel in Hyperlink.METADATA_ALLOWED:
                # only accept the types of links relevant to editions
                self.__links.append(link)


    @classmethod
    def from_edition(cls, edition):
        """Create a basic Metadata object for the given Edition.

        This doesn't contain everything but it contains enough
        information to run guess_license_pools.
        """
        kwargs = dict()
        for field in cls.BASIC_EDITION_FIELDS:
            kwargs[field] = getattr(edition, field)

        contributors = []
        for contribution in edition.contributions:
            contributor = ContributorData.from_contribution(contribution)
            contributors.append(contributor)

        if not edition.contributions:
            # This should only happen for low-quality data sources such as
            # the NYT best-seller API.
            if edition.sort_author and edition.sort_author != Edition.UNKNOWN_AUTHOR:
                contributors.append(
                    ContributorData(sort_name=edition.sort_author,
                                    display_name=edition.author,
                                    roles=[Contributor.PRIMARY_AUTHOR_ROLE])
                )

        i = edition.primary_identifier
        primary_identifier = IdentifierData(
            type=i.type, identifier=i.identifier, weight=1
        )

        links = []
        for link in i.links:
            link_data = LinkData(link.rel, link.resource.url)
            links.append(link_data)

        return Metadata(
            data_source=edition.data_source.name,
            primary_identifier=primary_identifier,
            contributors=contributors,
            links=links,
            **kwargs
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


    def update(self, metadata):
        """Update this Metadata object with values from the given Metadata
        object.

        TODO: We might want to take a policy object as an argument.
        """

        fields = self.BASIC_EDITION_FIELDS
        for field in fields:
            new_value = getattr(metadata, field)
            if new_value != None and new_value != '':
                setattr(self, field, new_value)

        new_value = getattr(metadata, 'contributors')
        if new_value and isinstance(new_value, list):
            old_value = getattr(self, 'contributors')
            # if we already have a better value, don't override it with a "missing info" placeholder value
            if not (old_value and new_value[0].sort_name == Edition.UNKNOWN_AUTHOR):
                setattr(self, 'contributors', new_value)


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

        if not self.medium:
            # We don't know the medium of this item, and we only want
            # to associate it with other items of the same type.
            return

        primary_identifier_obj, ignore = self.primary_identifier.load(_db)

        # Try to find the primary identifiers of other Editions with
        # the same permanent work ID and the same medium, representing
        # books already in our collection.
        qu = _db.query(Identifier).join(
            Identifier.primarily_identifies).filter(
                Edition.permanent_work_id==self.permanent_work_id).filter(
                    Identifier.type.in_(
                        Identifier.LICENSE_PROVIDING_IDENTIFIER_TYPES
                    )
                ).filter(
                    Edition.medium==self.medium
                )
        identifiers_same_work_id = qu.all()
        for same_work_id in identifiers_same_work_id:
            if (same_work_id.type != self.primary_identifier.type
                or same_work_id.identifier != self.primary_identifier.identifier):
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

    def edition(self, _db, create_if_not_exists=True):
        """ Find or create the edition described by this Metadata object.
        """
        if not self.primary_identifier:
            raise ValueError(
                "Cannot find edition: metadata has no primary identifier."
            )

        data_source = self.data_source(_db)

        return Edition.for_foreign_id(
            _db, data_source, self.primary_identifier.type,
            self.primary_identifier.identifier,
            create_if_not_exists=create_if_not_exists
        )


    def consolidate_identifiers(self):
        by_weight = defaultdict(list)
        for i in self.identifiers:
            by_weight[(i.type, i.identifier)].append(i.weight)
        new_identifiers = []
        for (type, identifier), weights in by_weight.items():
            new_identifiers.append(
                IdentifierData(type=type, identifier=identifier,
                               weight=median(weights))
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
            pools = i.license_pools
            for lp in pools:
                if lp and lp.deliverable and potentials.get(lp, 0) < confidence:
                    potentials[lp] = confidence
                    success = True
        return success

    REL_REQUIRES_NEW_PRESENTATION_EDITION = [
        LinkRelations.IMAGE, LinkRelations.THUMBNAIL_IMAGE
    ]
    REL_REQUIRES_FULL_RECALCULATION = [LinkRelations.DESCRIPTION]

    # TODO: We need to change all calls to apply() to use a ReplacementPolicy
    # instead of passing in individual `replace` arguments. Once that's done,
    # we can get rid of the `replace` arguments.
    def apply(self, edition, collection, metadata_client=None, replace=None,
              replace_identifiers=False,
              replace_subjects=False,
              replace_contributions=False,
              replace_links=False,
              replace_formats=False,
              replace_rights=False,
              force=False,
    ):
        """Apply this metadata to the given edition.

        :return: (edition, made_core_changes), where edition is the newly-updated object, and made_core_changes
            answers the question: were any edition core fields harmed in the making of this update?
            So, if title changed, return True.
            New: If contributors changed, this is now considered a core change,
            so work.simple_opds_feed refresh can be triggered.
        """
        _db = Session.object_session(edition)

        # If summary, subjects, or measurements change, then any Work
        # associated with this edition will need a full presentation
        # recalculation.
        work_requires_full_recalculation = False

        # If any other data changes, then any Work associated with
        # this edition will need to have its presentation edition
        # regenerated, but we can do it on the cheap.
        work_requires_new_presentation_edition = False

        if replace is None:
            replace = ReplacementPolicy(
                identifiers=replace_identifiers,
                subjects=replace_subjects,
                contributions=replace_contributions,
                links=replace_links,
                formats=replace_formats,
                rights=replace_rights,
                even_if_not_apparently_updated=force
            )

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

        if self.data_source_last_updated and not replace.even_if_not_apparently_updated:
            coverage_record = CoverageRecord.lookup(edition, data_source)
            if coverage_record:
                check_time = coverage_record.timestamp
                last_time = self.data_source_last_updated
                if check_time >= last_time:
                    # The metadata has not changed since last time. Do nothing.
                    return edition, False

        if metadata_client and not self.permanent_work_id:
            self.calculate_permanent_work_id(_db, metadata_client)

        identifier = edition.primary_identifier

        self.log.info(
            "APPLYING METADATA TO EDITION: %s",  self.title
        )
        fields = self.BASIC_EDITION_FIELDS+['permanent_work_id']
        for field in fields:
            old_edition_value = getattr(edition, field)
            new_metadata_value = getattr(self, field)
            if new_metadata_value != None and new_metadata_value != '' and (new_metadata_value != old_edition_value):
                if new_metadata_value in [NO_VALUE, NO_NUMBER]:
                    new_metadata_value = None
                setattr(edition, field, new_metadata_value)
                work_requires_new_presentation_edition = True

        # Create equivalencies between all given identifiers and
        # the edition's primary identifier.
        contributors_changed = self.update_contributions(_db, edition,
                                  metadata_client, replace.contributions)
        if contributors_changed:
            work_requires_new_presentation_edition = True

        # TODO: remove equivalencies when replace.identifiers is True.
        if self.identifiers is not None:
            for identifier_data in self.identifiers:
                if not identifier_data.identifier:
                    continue
                if (identifier_data.identifier==identifier.identifier and
                    identifier_data.type==identifier.type):
                    # These are the same identifier.
                    continue
                new_identifier, ignore = Identifier.for_foreign_id(
                    _db, identifier_data.type, identifier_data.identifier)
                identifier.equivalent_to(
                    data_source, new_identifier, identifier_data.weight)

        new_subjects = {}
        if self.subjects:
            new_subjects = dict(
                (subject.key, subject)
                for subject in self.subjects
            )
        if replace.subjects:
            # Remove any old Subjects from this data source, unless they
            # are also in the list of new subjects.
            surviving_classifications = []

            def _key(classification):
                s = classification.subject
                return s.type, s.identifier, s.name, classification.weight

            for classification in identifier.classifications:
                if classification.data_source == data_source:
                    key = _key(classification)
                    if not key in new_subjects:
                        # The data source has stopped claiming that
                        # this classification should exist.
                        _db.delete(classification)
                        work_requires_full_recalculation = True
                    else:
                        # The data source maintains that this
                        # classification is a good idea. We don't have
                        # to do anything.
                        del new_subjects[key]
                        surviving_classifications.append(classification)
                else:
                    # This classification comes from some other data
                    # source.  Don't mess with it.
                    surviving_classifications.append(classification)
            identifier.classifications = surviving_classifications

        # Apply all new subjects to the identifier.
        for subject in new_subjects.values():
            identifier.classify(
                data_source, subject.type, subject.identifier,
                subject.name, weight=subject.weight)
            work_requires_full_recalculation = True

        # Associate all links with the primary identifier.
        if replace.links and self.links is not None:
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

        link_objects = {}

        for link in self.links:
            if link.rel in Hyperlink.METADATA_ALLOWED:
                original_resource = None
                if link.original:
                    rights_status = RightsStatus.lookup(_db, link.original.rights_uri)
                    original_resource, ignore = get_one_or_create(
                        _db, Resource, url=link.original.href,
                    )
                    if not original_resource.data_source:
                        original_resource.data_source = data_source
                    original_resource.rights_status = rights_status
                    original_resource.rights_explanation = link.original.rights_explanation
                    if link.original.content:
                        original_resource.set_fetched_content(
                            link.original.guessed_media_type,
                            link.original.content, None)

                link_obj, ignore = identifier.add_link(
                    rel=link.rel, href=link.href, data_source=data_source,
                    media_type=link.guessed_media_type,
                    content=link.content, rights_status_uri=link.rights_uri,
                    rights_explanation=link.rights_explanation,
                    original_resource=original_resource,
                    transformation_settings=link.transformation_settings,
                )
                if link.rel in self.REL_REQUIRES_NEW_PRESENTATION_EDITION:
                    work_requires_new_presentation_edition = True
                elif link.rel in self.REL_REQUIRES_FULL_RECALCULATION:
                    work_requires_full_recalculation = True

            link_objects[link] = link_obj
            if link.thumbnail:
                if link.thumbnail.rel == Hyperlink.THUMBNAIL_IMAGE:
                    thumbnail = link.thumbnail
                    thumbnail_obj, ignore = identifier.add_link(
                        rel=thumbnail.rel, href=thumbnail.href,
                        data_source=data_source,
                        media_type=thumbnail.guessed_media_type,
                        content=thumbnail.content
                    )
                    work_requires_new_presentation_edition = True
                    if (thumbnail_obj.resource
                        and thumbnail_obj.resource.representation):
                        thumbnail_obj.resource.representation.thumbnail_of = (
                            link_obj.resource.representation
                        )
                    else:
                        self.log.error(
                            "Thumbnail link %r cannot be marked as a thumbnail of %r because it has no Representation, probably due to a missing media type." % (
                                link.thumbnail, link
                            )
                        )
                else:
                    self.log.error(
                        "Thumbnail link %r does not have the thumbnail link relation! Not acceptable as a thumbnail of %r." % (
                            link.thumbnail, link
                        )
                    )
                    link.thumbnail = None

        # Apply all measurements to the primary identifier
        for measurement in self.measurements:
            work_requires_full_recalculation = True
            identifier.add_measurement(
                data_source, measurement.quantity_measured,
                measurement.value, measurement.weight,
                measurement.taken_at
            )

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
                work_requires_new_presentation_edition = True

        # The Metadata object may include a CirculationData object which
        # contains information about availability such as open-access
        # links. Make sure
        # that that Collection has a LicensePool for this book and that
        # its information is up-to-date.
        if self.circulation:
            self.circulation.apply(_db, collection, replace)

        # obtains a presentation_edition for the title, which will later be used to get a mirror link.
        has_image = any([link.rel == Hyperlink.IMAGE for link in self.links])
        for link in self.links:
            link_obj = link_objects[link]

            if link_obj.rel == Hyperlink.THUMBNAIL_IMAGE and has_image:
                # This is a thumbnail but we also have a full-sized image link,
                # so we don't need to separately mirror the thumbnail.
                continue

            if replace.mirrors:
                # We need to mirror this resource. If it's an image, a
                # thumbnail may be provided as a side effect.
                self.mirror_link(edition, data_source, link, link_obj, replace)
            elif link.thumbnail:
                # We don't need to mirror this image, but we do need
                # to make sure that its thumbnail exists locally and
                # is associated with the original image.
                self.make_thumbnail(data_source, link, link_obj)

        # Make sure the work we just did shows up. This needs to happen after mirroring
        # so mirror urls are available.
        made_changes = edition.calculate_presentation(
            policy=replace.presentation_calculation_policy
        )
        if made_changes:
            work_requires_new_presentation_edition = True

        # The metadata wrangler doesn't need information from these data sources.
        # We don't need to send it information it originally provided, and
        # Overdrive makes metadata accessible to everyone without buying licenses
        # for the book, so the metadata wrangler can obtain it directly from
        # Overdrive.
        # TODO: Remove Bibliotheca and Axis 360 from this list.
        METADATA_UPLOAD_BLACKLIST = [
            DataSource.METADATA_WRANGLER,
            DataSource.OVERDRIVE,
            DataSource.BIBLIOTHECA,
            DataSource.AXIS_360,
        ]
        if work_requires_new_presentation_edition and (not data_source.integration_client) and (data_source.name not in METADATA_UPLOAD_BLACKLIST):
            # Create a transient failure CoverageRecord for this edition
            # so it will be processed by the MetadataUploadCoverageProvider.
            internal_processing = DataSource.lookup(_db, DataSource.INTERNAL_PROCESSING)

            # If there's already a CoverageRecord, don't change it to transient failure.
            # TODO: Once the metadata wrangler can handle it, we'd like to re-sync the
            # metadata every time there's a change. For now,
            cr = CoverageRecord.lookup(edition, internal_processing,
                                       operation=CoverageRecord.METADATA_UPLOAD_OPERATION)
            if not cr:
                CoverageRecord.add_for(edition, internal_processing,
                                       operation=CoverageRecord.METADATA_UPLOAD_OPERATION,
                                       status=CoverageRecord.TRANSIENT_FAILURE)

        # Update the coverage record for this edition and data
        # source. We omit the collection information, even if we know
        # which collection this is, because we only changed metadata.
        CoverageRecord.add_for(
            edition, data_source, timestamp=self.data_source_last_updated,
            collection=None
        )

        if work_requires_full_recalculation or work_requires_new_presentation_edition:
            # If there is a Work associated with the Edition's primary
            # identifier, mark it for recalculation.

            # Any LicensePool will do here, since all LicensePools for
            # a given Identifier have the same Work.
            pool = get_one(
                _db, LicensePool, identifier=edition.primary_identifier,
                on_multiple='interchangeable'
            )
            if pool and pool.work:
                work = pool.work
                if work_requires_full_recalculation:
                    work.needs_full_presentation_recalculation()
                else:
                    work.needs_new_presentation_edition()

        return edition, work_requires_new_presentation_edition


    def make_thumbnail(self, data_source, link, link_obj):
        """Make sure a Hyperlink representing an image is connected
        to its thumbnail.
        """
        thumbnail = link.thumbnail
        if not thumbnail:
            return None

        if thumbnail.href == link.href:
            # The image serves as its own thumbnail. This is a
            # hacky way to represent this in the database.
            if link_obj.resource.representation:
                link_obj.resource.representation.image_height = Edition.MAX_THUMBNAIL_HEIGHT
            return link_obj

        # The thumbnail and image are different. Make sure there's a
        # separate link to the thumbnail.
        thumbnail_obj, ignore = link_obj.identifier.add_link(
            rel=thumbnail.rel, href=thumbnail.href,
            data_source=data_source,
            media_type=thumbnail.media_type,
            content=thumbnail.content
        )
        # And make sure the thumbnail knows it's a thumbnail of the main
        # image.
        if thumbnail_obj.resource.representation:
            thumbnail_obj.resource.representation.thumbnail_of = link_obj.resource.representation
        return thumbnail_obj


    def update_contributions(self, _db, edition, metadata_client=None,
                             replace=True):
        contributors_changed = False
        old_contributors = []
        new_contributors = []

        if not replace and self.contributors:
            # we've chosen to append new contributors, which exist
            # this means the edition's contributor list will, indeed, change
            contributors_changed = True

        if replace and self.contributors:
            # Remove any old Contributions from this data source --
            # we're about to add a new set
            surviving_contributions = []
            for contribution in edition.contributions:
                old_contributors.append(contribution.contributor.id)
                _db.delete(contribution)
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
                new_contributors.append(contributor.id)
                if contributor_data.display_name:
                    contributor.display_name = contributor_data.display_name
                if contributor_data.biography:
                    contributor.biography = contributor_data.biography
                if contributor_data.aliases:
                    contributor.aliases = contributor_data.aliases
                if contributor_data.lc:
                    contributor.lc = contributor_data.lc
                if contributor_data.viaf:
                    contributor.viaf = contributor_data.viaf
                if contributor_data.wikipedia_name:
                    contributor.wikipedia_name = contributor_data.wikipedia_name
            else:
                self.log.info(
                    "Not registering %s because no sort name, LC, or VIAF",
                    contributor_data.display_name
                )

        if sorted(old_contributors) != sorted(new_contributors):
            contributors_changed = True

        return contributors_changed


    def filter_recommendations(self, _db):
        """Filters out recommended identifiers that don't exist in the db.
        Any IdentifierData objects will be replaced with Identifiers.
        """

        by_type = defaultdict(list)
        for identifier in self.recommendations:
            by_type[identifier.type].append(identifier.identifier)

        self.recommendations = []
        for type, identifiers in by_type.items():
            existing_identifiers = _db.query(Identifier).\
                filter(Identifier.type==type).\
                filter(Identifier.identifier.in_(identifiers))
            self.recommendations += existing_identifiers.all()

        if self.primary_identifier in self.recommendations:
            self.recommendations.remove(identifier_data)


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

    # When classifications are imported from a CSV file, we treat 
    # them as though they came from a trusted distributor.
    DEFAULT_SUBJECT_FIELD_NAMES = {
        'tags': (Subject.TAG, Classification.TRUSTED_DISTRIBUTOR_WEIGHT),
        'age' : (Subject.AGE_RANGE, Classification.TRUSTED_DISTRIBUTOR_WEIGHT),
        'audience' : (Subject.FREEFORM_AUDIENCE,
                      Classification.TRUSTED_DISTRIBUTOR_WEIGHT),
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
                "Could not find a primary identifier field. Possibilities: %r. Actualities: %r." %
                (possibilities, fields)
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
        if isinstance(value, bytes):
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


class MARCExtractor(object):

    """Transform a MARC file into a list of Metadata objects.

    This is not totally general, but it's a good start.
    """

    # Common things found in a MARC record after the name of the author
    # which we sould like to remove.
    END_OF_AUTHOR_NAME_RES = [
        re.compile(",\s+[0-9]+-"), # Birth year
        re.compile(",\s+active "),
        re.compile(",\s+graf,"),
        re.compile(",\s+author."),
    ]

    @classmethod
    def name_cleanup(cls, name):
        # Turn 'Dante Alighieri,   1265-1321, author.'
        # into 'Dante Alighieri'.
        for regex in cls.END_OF_AUTHOR_NAME_RES:
            match = regex.search(name)
            if match:
                name = name[:match.start()]
                break
        name = name_tidy(name)
        return name

    @classmethod
    def parse_year(self, value):
        """Handle a publication year that may not be in the right format."""
        for format in ("%Y", "%Y."):
            try:
                return datetime.datetime.strptime(value, format)
            except ValueError:
                continue
        return None

    @classmethod
    def parse(cls, file, data_source_name):
        reader = MARCReader(file)
        metadata_records = []

        for record in reader:
            title = record.title()
            if title.endswith(' /'):
                title = title[:-len(' /')]
            issued_year = cls.parse_year(record.pubyear())
            publisher = record.publisher()
            if publisher.endswith(','):
                publisher = publisher[:-1]

            links = []
            summary = record.notes()[0]['a']

            if summary:
                summary_link = LinkData(
                    rel=Hyperlink.DESCRIPTION,
                    media_type=Representation.TEXT_PLAIN,
                    content=summary,
                )
                links.append(summary_link)

            isbn = record['020']['a'].split(" ")[0]
            primary_identifier = IdentifierData(
                Identifier.ISBN, isbn
            )

            subjects = [SubjectData(
                Classifier.FAST,
                subject['a'],
            ) for subject in record.subjects()]

            author = record.author()
            if author:
                author = cls.name_cleanup(author)
                author_names = [author]
            else:
                author_names = ['Anonymous']
            contributors = [
                ContributorData(
                    sort_name=author,
                    roles=[Contributor.AUTHOR_ROLE],
                )
                for author in author_names
            ]

            metadata_records.append(Metadata(
                data_source=data_source_name,
                title=title,
                language='eng',
                medium=Edition.BOOK_MEDIUM,
                publisher=publisher,
                issued=issued_year,
                primary_identifier=primary_identifier,
                subjects=subjects,
                contributors=contributors,
                links=links
            ))
        return metadata_records
