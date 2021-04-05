# encoding: utf-8
# Resource, ResourceTransformation, Hyperlink, Representation


from . import (
    Base,
    get_one,
    get_one_or_create,
)
from ..config import Configuration
from .constants import (
    DataSourceConstants,
    IdentifierConstants,
    LinkRelations,
    MediaTypes,
)
from .edition import Edition
from .licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from ..util.http import HTTP

from io import BytesIO
import datetime
import pytz
import json
import logging
from hashlib import md5
import os
from PIL import Image
import re
import requests
from sqlalchemy import (
    Binary,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import (
    backref,
    relationship,
)
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import or_
import time
import traceback
from urllib.parse import urlparse, urlsplit, quote

class Resource(Base):
    """An external resource that may be mirrored locally.
    E.g: a cover image, an epub, a description.
    """

    __tablename__ = 'resources'

    # How many votes is the initial quality estimate worth?
    ESTIMATED_QUALITY_WEIGHT = 5

    # The point at which a generic geometric image is better
    # than a lousy cover we got from the Internet.
    MINIMUM_IMAGE_QUALITY = 0.25

    id = Column(Integer, primary_key=True)

    # A URI that uniquely identifies this resource. Most of the time
    # this will be an HTTP URL, which is why we're calling it 'url',
    # but it may also be a made-up URI.
    url = Column(Unicode, index=True)

    # Many Editions may choose this resource (as opposed to other
    # resources linked to them with rel="image") as their cover image.
    cover_editions = relationship("Edition", backref="cover", foreign_keys=[Edition.cover_id])

    # Many Works may use this resource (as opposed to other resources
    # linked to them with rel="description") as their summary.
    from .work import Work
    summary_works = relationship("Work", backref="summary", foreign_keys=[Work.summary_id])

    # Many LicensePools (but probably one at most) may use this
    # resource in a delivery mechanism.
    licensepooldeliverymechanisms = relationship(
        "LicensePoolDeliveryMechanism", backref="resource",
        foreign_keys=[LicensePoolDeliveryMechanism.resource_id]
    )

    links = relationship("Hyperlink", backref="resource")

    # The DataSource that is the controlling authority for this Resource.
    data_source_id = Column(Integer, ForeignKey('datasources.id'), index=True)

    # An archived Representation of this Resource.
    representation_id = Column(
        Integer, ForeignKey('representations.id'), index=True)

    # The rights status of this Resource.
    rights_status_id = Column(Integer, ForeignKey('rightsstatus.id'))

    # An optional explanation of the rights status.
    rights_explanation = Column(Unicode)

    # A Resource may be transformed into many derivatives.
    transformations = relationship(
        'ResourceTransformation',
        primaryjoin="ResourceTransformation.original_id==Resource.id",
        foreign_keys=id,
        lazy="joined",
        backref=backref('original', uselist=False),
        uselist=True,
    )

    # A derivative resource may have one original.
    derived_through = relationship(
        'ResourceTransformation',
        primaryjoin="ResourceTransformation.derivative_id==Resource.id",
        foreign_keys=id,
        backref=backref('derivative', uselist=False),
        lazy="joined",
        uselist=False,
    )

    # A calculated value for the quality of this resource, based on an
    # algorithmic treatment of its content.
    estimated_quality = Column(Float)

    # The average of human-entered values for the quality of this
    # resource.
    voted_quality = Column(Float, default=float(0))

    # How many votes contributed to the voted_quality value. This lets
    # us scale new votes proportionately while keeping only two pieces
    # of information.
    votes_for_quality = Column(Integer, default=0)

    # A combination of the calculated quality value and the
    # human-entered quality value.
    quality = Column(Float, index=True)

    # URL must be unique.
    __table_args__ = (
        UniqueConstraint('url'),
    )

    @property
    def final_url(self):
        """URL to the final, mirrored version of this resource, suitable
        for serving to the client.
        :return: A URL, or None if the resource has no mirrored
        representation.
        """
        if not self.representation:
            return None
        if not self.representation.mirror_url:
            return None
        return self.representation.mirror_url

    def as_delivery_mechanism_for(self, licensepool):
        """If this Resource is used in a LicensePoolDeliveryMechanism for the
        given LicensePool, return that LicensePoolDeliveryMechanism.
        """
        for lpdm in licensepool.delivery_mechanisms:
            if lpdm.resource == self:
                return lpdm

    def set_fetched_content(self, media_type, content, content_path):
        """Simulate a successful HTTP request for a representation
        of this resource.
        This is used when the content of the representation is obtained
        through some other means.
        """
        _db = Session.object_session(self)

        if not (content or content_path):
            raise ValueError(
                "One of content and content_path must be specified.")
        if content and content_path:
            raise ValueError(
                "Only one of content and content_path may be specified.")
        representation, is_new = get_one_or_create(
            _db, Representation, url=self.url, media_type=media_type)
        self.representation = representation
        representation.set_fetched_content(content, content_path)

    def set_estimated_quality(self, estimated_quality):
        """Update the estimated quality."""
        self.estimated_quality = estimated_quality
        self.update_quality()

    def add_quality_votes(self, quality, weight=1):
        """Record someone's vote as to the quality of this resource."""
        self.voted_quality = self.voted_quality or 0
        self.votes_for_quality = self.votes_for_quality or 0

        total_quality = self.voted_quality * self.votes_for_quality
        total_quality += (quality * weight)
        self.votes_for_quality += weight
        self.voted_quality = total_quality / float(self.votes_for_quality)
        self.update_quality()

    def reject(self):
        """Reject a Resource by making its voted_quality negative.
        If the Resource is a cover, this rejection will render it unusable to
        all Editions and Identifiers. Even if the cover is later `approved`
        a rejection impacts the overall weight of the `vote_quality`.
        """
        if not self.voted_quality:
            self.add_quality_votes(-1)
            return

        if self.voted_quality < 0:
            # This Resource has already been rejected.
            return

        # Humans have voted positively on this Resource, and now it's
        # being rejected regardless.
        logging.warn("Rejecting Resource with positive votes: %r", self)

        # Make the voted_quality negative without impacting the weight
        # of existing votes so the value can be restored relatively
        # painlessly if necessary.
        self.voted_quality = -self.voted_quality

        # However, because `votes_for_quality` is incremented, a
        # rejection will impact the weight of all `voted_quality` votes
        # even if the Resource is later approved.
        self.votes_for_quality += 1
        self.update_quality()

    def approve(self):
        """Approve a rejected Resource by making its human-generated
        voted_quality positive while taking its rejection into account.
        """
        if self.voted_quality < 0:
            # This Resource has been rejected. Reset its value to be
            # positive.
            if self.voted_quality == -1 and self.votes_for_quality == 1:
                # We're undoing a single rejection.
                self.voted_quality = 0
            else:
                # An existing positive voted_quality was made negative.
                self.voted_quality = abs(self.voted_quality)
            self.votes_for_quality += 1
            self.update_quality()
            return

        self.add_quality_votes(1)

    def update_quality(self):
        """Combine computer-generated `estimated_quality` with
        human-generated `voted_quality` to form overall `quality`.
        """
        estimated_weight = self.ESTIMATED_QUALITY_WEIGHT
        votes_for_quality = self.votes_for_quality or 0
        total_weight = estimated_weight + votes_for_quality

        voted_quality = (self.voted_quality or 0) * votes_for_quality
        total_quality = (((self.estimated_quality or 0) * self.ESTIMATED_QUALITY_WEIGHT) +
                         voted_quality)

        if voted_quality < 0 and total_quality > 0:
            # If `voted_quality` is negative, the Resource has been
            # rejected by a human and should no longer be available.
            #
            # This human-generated negativity must be passed to the final
            # Resource.quality value.
            total_quality = -(total_quality)
        self.quality = total_quality / float(total_weight)

    @classmethod
    def image_type_priority(cls, media_type):
        """Where does the given image media type rank on our list of
        preferences?
        :return: A lower number is better. None means it's not an
        image type or we don't care about it at all.
        """
        if media_type in Representation.IMAGE_MEDIA_TYPES:
            return Representation.IMAGE_MEDIA_TYPES.index(media_type)
        return None

    @classmethod
    def best_covers_among(cls, resources):

        """Choose the best covers from a list of Resources."""
        champions = []
        champion_key = None

        for r in resources:
            rep = r.representation
            if not rep:
                # A Resource with no Representation is not usable, period
                continue
            media_priority = cls.image_type_priority(rep.media_type)
            if media_priority is None:
                media_priority = float('inf')

            # This method will set the quality if it hasn't been set before.
            r.quality_as_thumbnail_image
            # Now we can use it.
            quality = r.quality
            if not quality >= cls.MINIMUM_IMAGE_QUALITY:
                # A Resource below the minimum quality threshold is not
                # usable, period.
                continue

            # In order, our criteria are: whether we
            # mirrored the representation (which means we directly
            # control it), image quality, and media type suitability.
            #
            # We invert media type suitability because it's given to us
            # as a priority (where smaller is better), but we want to compare
            # it as a quantity (where larger is better).
            compare_key = (rep.mirror_url is not None, quality, -media_priority)
            if not champion_key or (compare_key > champion_key):
                # A new champion.
                champions = [r]
                champion_key = compare_key
            elif compare_key == champion_key:
                # This image is equally good as the existing champion.
                champions.append(r)

        return champions

    @property
    def quality_as_thumbnail_image(self):
        """Determine this image's suitability for use as a thumbnail image.
        """
        rep = self.representation
        if not rep:
            return 0

        quality = 1
        # If the size of the image is known, that might affect
        # the quality.
        quality = quality * rep.thumbnail_size_quality_penalty

        # Scale the estimated quality by the source of the image.
        source_name = self.data_source.name
        if source_name==DataSourceConstants.GUTENBERG_COVER_GENERATOR:
            quality = quality * 0.60
        elif source_name==DataSourceConstants.GUTENBERG:
            quality = quality * 0.50
        elif source_name==DataSourceConstants.OPEN_LIBRARY:
            quality = quality * 0.25
        elif source_name in DataSourceConstants.COVER_IMAGE_PRIORITY:
            # Covers from the data sources listed in
            # COVER_IMAGE_PRIORITY (e.g. the metadata wrangler
            # and the administrative interface) are given priority
            # over all others, relative to their position in
            # COVER_IMAGE_PRIORITY.
            i = DataSourceConstants.COVER_IMAGE_PRIORITY.index(source_name)
            quality = quality * (i+2)
        self.set_estimated_quality(quality)
        return quality

    def add_derivative(self, derivative_resource, settings=None):
        _db = Session.object_session(self)

        transformation, ignore = get_one_or_create(
            _db, ResourceTransformation, derivative_id=derivative_resource.id)
        transformation.original_id = self.id
        transformation.settings = settings or {}
        return transformation

class ResourceTransformation(Base):
    """A record that a resource is a derivative of another resource,
    and the settings that were used to transform the original into it.
    """

    __tablename__ = 'resourcetransformations'

    # The derivative resource. A resource can only be derived from one other resource.
    derivative_id = Column(
        Integer, ForeignKey('resources.id'), index=True, primary_key=True)

    # The original resource that was transformed into the derivative.
    original_id = Column(
        Integer, ForeignKey('resources.id'), index=True)

    # The settings used for the transformation.
    settings = Column(MutableDict.as_mutable(JSON), default={})

class Hyperlink(Base, LinkRelations):
    """A link between an Identifier and a Resource."""

    __tablename__ = 'hyperlinks'

    id = Column(Integer, primary_key=True)

    # A Hyperlink is always associated with some Identifier.
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True, nullable=False)

    # The DataSource through which this link was discovered.
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True, nullable=False)

    # The link relation between the Identifier and the Resource.
    rel = Column(Unicode, index=True, nullable=False)

    # The Resource on the other end of the link.
    resource_id = Column(
        Integer, ForeignKey('resources.id'), index=True, nullable=False)

    @classmethod
    def unmirrored(cls, collection):
        """Find all Hyperlinks associated with an item in the
        given Collection that could be mirrored but aren't.
        TODO: We don't cover the case where an image was mirrored but no
        thumbnail was created of it. (We do cover the case where the thumbnail
        was created but not mirrored.)
        """
        from .identifier import Identifier
        _db = Session.object_session(collection)
        qu = _db.query(Hyperlink).join(
            Hyperlink.identifier
        ).join(
            Identifier.licensed_through
        ).outerjoin(
            Hyperlink.resource
        ).outerjoin(
            Resource.representation
        )
        qu = qu.filter(LicensePool.collection_id==collection.id)
        qu = qu.filter(Hyperlink.rel.in_(Hyperlink.MIRRORED))
        qu = qu.filter(Hyperlink.data_source==collection.data_source)
        qu = qu.filter(
            or_(
                Representation.id==None,
                Representation.mirror_url==None,
            )
        )
        # Without this ordering, the query does a table scan looking for
        # items that match. With the ordering, they're all at the front.
        qu = qu.order_by(Representation.mirror_url.asc().nullsfirst(),
                         Representation.id.asc().nullsfirst())
        return qu

    @classmethod
    def generic_uri(cls, data_source, identifier, rel, content=None):
        """Create a generic URI for the other end of this hyperlink.
        This is useful for resources that are obtained through means
        other than fetching a single URL via HTTP. It lets us get a
        URI that's most likely unique, so we can create a Resource
        object without violating the uniqueness constraint.
        If the output of this method isn't unique in your situation
        (because the data source provides more than one link with a
        given link relation for a given identifier), you'll need some
        other way of coming up with generic URIs.
        """
        l = [identifier.urn, quote(data_source.name), quote(rel)]
        if content:
            m = md5()
            if isinstance(content, str):
                content = content.encode("utf8")
            m.update(content)
            l.append(m.hexdigest())
        return ":".join(l)

    @classmethod
    def _default_filename(self, rel):
        if rel == self.OPEN_ACCESS_DOWNLOAD:
            return 'content'
        elif rel == self.IMAGE:
            return 'cover'
        elif rel == self.THUMBNAIL_IMAGE:
            return 'cover-thumbnail'

    @property
    def default_filename(self):
        return self._default_filename(self.rel)


class Representation(Base, MediaTypes):
    """A cached document obtained from (and possibly mirrored to) the Web
    at large.
    Sometimes this is a DataSource's representation of a specific
    book.
    Sometimes it's associated with a database Resource (which has a
    well-defined relationship to one specific book).
    Sometimes it's just a web page that we need a cached local copy
    of.
    """

    __tablename__ = 'representations'
    id = Column(Integer, primary_key=True)

    # URL from which the representation was fetched.
    url = Column(Unicode, index=True)

    # The media type of the representation.
    media_type = Column(Unicode)

    resource = relationship("Resource", backref="representation", uselist=False)

    ### Records of things we tried to do with this representation.

    # When the representation was last fetched from `url`.
    fetched_at = Column(DateTime(timezone=True), index=True)

    # A textual description of the error encountered the last time
    # we tried to fetch the representation
    fetch_exception = Column(Unicode, index=True)

    # A URL under our control to which this representation has been
    # mirrored.
    mirror_url = Column(Unicode, index=True)

    # When the representation was last pushed to `mirror_url`.
    mirrored_at = Column(DateTime(timezone=True), index=True)

    # An exception that happened while pushing this representation
    # to `mirror_url.
    mirror_exception = Column(Unicode, index=True)

    # If this image is a scaled-down version of some other image,
    # `scaled_at` is the time it was last generated.
    scaled_at = Column(DateTime(timezone=True), index=True)

    # If this image is a scaled-down version of some other image,
    # this is the exception that happened the last time we tried
    # to scale it down.
    scale_exception = Column(Unicode, index=True)

    ### End records of things we tried to do with this representation.

    # An image Representation may be a thumbnail version of another
    # Representation.
    thumbnail_of_id = Column(
        Integer, ForeignKey('representations.id'), index=True)

    thumbnails = relationship(
        "Representation",
        backref=backref("thumbnail_of", remote_side = [id]),
        lazy="joined", post_update=True)

    # The HTTP status code from the last fetch.
    status_code = Column(Integer)

    # A textual representation of the HTTP headers sent along with the
    # representation.
    headers = Column(Unicode)

    # The Location header from the last representation.
    location = Column(Unicode)

    # The Last-Modified header from the last representation.
    last_modified = Column(Unicode)

    # The Etag header from the last representation.
    etag = Column(Unicode)

    # The size of the representation, in bytes.
    file_size = Column(Integer)

    # If this representation is an image, the height of the image.
    image_height = Column(Integer, index=True)

    # If this representation is an image, the width of the image.
    image_width = Column(Integer, index=True)

    # The content of the representation itself.
    content = Column(Binary)

    # Instead of being stored in the database, the content of the
    # representation may be stored on a local file relative to the
    # data root.
    local_content_path = Column(Unicode)

    # A Representation may be a CachedMARCFile.
    marc_file = relationship(
        "CachedMARCFile", backref="representation",
        cascade="all, delete-orphan",
    )

    # At any given time, we will have a single representation for a
    # given URL and media type.
    __table_args__ = (
        UniqueConstraint('url', 'media_type'),
    )

    # A User-Agent to use when acting like a web browser.
    # BROWSER_USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36 (Simplified)"
    BROWSER_USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:37.0) Gecko/20100101 Firefox/37.0"

    @property
    def age(self):
        if not self.fetched_at:
            return 1000000
        return (datetime.datetime.now(tz=pytz.UTC) - self.fetched_at).total_seconds()

    @property
    def has_content(self):
        if self.content and self.status_code == 200 and self.fetch_exception is None:
            return True
        if self.local_content_path and os.path.exists(self.local_content_path) and self.fetch_exception is None:
            return True
        return False

    @property
    def public_url(self):
        """Find the best URL to publish when referencing this Representation
        in a public space.
        :return: a bytestring
        """
        url = None
        if self.mirror_url:
            url = self.mirror_url
        elif self.url:
            url = self.url
        elif self.resource:
            # This really shouldn't happen.
            url = self.resource.url
        return url

    @property
    def is_usable(self):
        """Returns True if the Representation has some data or received
        a status code that's not in the 5xx series.
        """
        if not self.fetch_exception and (
            self.content or self.local_path or self.status_code
            and self.status_code // 100 != 5
        ):
            return True
        return False

    @classmethod
    def is_media_type(cls, s):
        """Return true if the given string looks like a media type."""
        if not s:
            return False
        s = s.lower()
        return any(s.startswith(x) for x in [
                   'application/',
                   'audio/',
                   'example/',
                   'image/',
                   'message/',
                   'model/',
                   'multipart/',
                   'text/',
                   'video/'
        ])

    @classmethod
    def guess_url_media_type_from_path(cls, url):
        """Guess a likely media type from the URL's path component."""
        if not url:
            return None
        path = urlparse(url).path
        return cls.guess_media_type(path)

    @classmethod
    def guess_media_type(cls, filename):
        """Guess a likely media type from a filename."""
        if not filename:
            return None
        filename = filename.lower()
        for extension, media_type in list(cls.MEDIA_TYPE_FOR_EXTENSION.items()):
            if filename.endswith(extension):
                return media_type
        return None

    def is_fresher_than(self, max_age):
        # Convert a max_age timedelta to a number of seconds.
        if isinstance(max_age, datetime.timedelta):
            max_age = max_age.total_seconds()

        if not self.is_usable:
            return False
        return (max_age is None or max_age > self.age)

    @classmethod
    def get(cls, _db, url, do_get=None, extra_request_headers=None,
            accept=None, max_age=None, pause_before=0, allow_redirects=True,
            presumed_media_type=None, debug=True, response_reviewer=None,
            exception_handler=None, url_normalizer=None):
        """Retrieve a representation from the cache if possible.
        If not possible, retrieve it from the web and store it in the
        cache.

        :param _db: A database connection.

        :param url: The URL to use as the target of any HTTP request.

        :param do_get: A function that takes arguments (url, headers)
           and retrieves a representation over the network.

        :param accept: A value for the Accept HTTP header.

        :param extra_request_headers: Any additional HTTP headers to
           include with the request.

        :param max_age: A timedelta object representing the maximum
           time to consider a cached representation fresh. (We ignore the
           caching directives from web servers because they're usually
           far too conservative for our purposes.)

        :param pause_before: A number of seconds to pause before sending
            the HTTP request. This is for use in situations where
            HTTP requests are subject to throttling.

        :param allow_redirects: Not currently used. (TODO: this seems like
            a problem!)

        :param presumed_media_type: If the response does not contain a
            Content-Type header, or if the specified Content-Type is
            too generic to use, the representation will be presumed to be
            of this media type.

        :param debug: If True, progress reports on the HTTP request will
           be logged.

        :param response_reviewer: A function that takes a 3-tuple
           (status_code, headers, content) and raises an exception if
           the response should not be treated as cacheable.

        :param exception_handler: A function that takes a 3-tuple
            (Representation, Exception, traceback) and handles
            an exceptional condition that occured during the HTTP request.

        :param url_normalizer: A function that takes the URL to be used in
            the HTTP request, and returns the URL to use when storing
            the corresponding Representation in the database. This can be
            used to strip irrelevant or sensitive information from
            URLs to increase the chances of a cache hit.

        :return: A 2-tuple (representation, obtained_from_cache)

        """
        representation = None
        do_get = do_get or cls.simple_http_get

        exception_handler = exception_handler or cls.record_exception

        # TODO: We allow representations of the same URL in different
        # media types, but we don't have a good solution here for
        # doing content negotiation (letting the caller ask for a
        # specific set of media types and matching against what we
        # have cached). Fortunately this isn't an issue with any of
        # the data sources we currently use, so for now we can treat
        # different representations of a URL as interchangeable.

        if url_normalizer:
            normalized_url = url_normalizer(url)
        else:
            normalized_url = url

        a = dict(url=normalized_url)
        if accept:
            a['media_type'] = accept
        representation = get_one(_db, Representation, 'interchangeable', **a)

        usable_representation = fresh_representation = False
        if representation:
            # Do we already have a usable representation?
            usable_representation = representation.is_usable

            # Assuming we have a usable representation, is it fresh?
            fresh_representation = representation.is_fresher_than(max_age)

        if debug is True:
            debug_level = logging.DEBUG
        elif debug is False:
            debug_level = None
        else:
            debug_level = debug

        if fresh_representation:
            if debug_level is not None:
                logging.info("Cached %s", url)
            return representation, True

        # We have a representation that is either not fresh or not usable.
        # We must make an HTTP request.
        if debug_level is not None:
            logging.log(debug_level, "Fetching %s", url)
        headers = {}
        if extra_request_headers:
            headers.update(extra_request_headers)
        if accept:
            headers['Accept'] = accept

        if usable_representation:
            # We have a representation but it's not fresh. We will
            # be making a conditional HTTP request to see if there's
            # a new version.
            if representation.last_modified:
                headers['If-Modified-Since'] = representation.last_modified
            if representation.etag:
                headers['If-None-Match'] = representation.etag

        fetched_at = datetime.datetime.now(tz=pytz.UTC)
        if pause_before:
            time.sleep(pause_before)
        media_type = None
        fetch_exception = None
        exception_traceback = None
        try:
            status_code, headers, content = do_get(url, headers)
            if response_reviewer:
                # An optional function passed to raise errors if the
                # post response isn't worth caching.
                response_reviewer((status_code, headers, content))
            exception = None
            media_type = cls._best_media_type(url, headers, presumed_media_type)
            if isinstance(content, str):
                content = content.encode("utf8")
        except Exception as e:
            # This indicates there was a problem with making the HTTP
            # request, not that the HTTP request returned an error
            # condition.
            fetch_exception = e
            logging.error("Error making HTTP request to %s", url, exc_info=fetch_exception)
            exception_traceback = traceback.format_exc()

            status_code = None
            headers = None
            content = None
            media_type = None

        # At this point we can create/fetch a Representation object if
        # we don't have one already, or if the URL or media type we
        # actually got from the server differs from what we thought
        # we had.
        if (not usable_representation
            or media_type != representation.media_type
            or normalized_url != representation.url):
            representation, is_new = get_one_or_create(
                _db, Representation, url=normalized_url,
                media_type=str(media_type)
            )

        if fetch_exception:
            exception_handler(
                representation, fetch_exception, exception_traceback
            )
        representation.fetched_at = fetched_at

        if status_code == 304:
            # The representation hasn't changed since we last checked.
            # Set its fetched_at property and return the cached
            # version as though it were new.
            representation.fetched_at = fetched_at
            representation.status_code = status_code
            return representation, False

        if status_code:
            status_code_series = status_code // 100
        else:
            status_code_series = None

        if status_code_series in (2,3) or status_code in (404, 410):
            # We have a new, good representation. Update the
            # Representation object and return it as fresh.
            representation.status_code = status_code
            representation.content = content
            representation.media_type = media_type

            for header, field in (
                    ('etag', 'etag'),
                    ('last-modified', 'last_modified'),
                    ('location', 'location')):
                if header in headers:
                    value = headers[header]
                else:
                    value = None
                setattr(representation, field, value)

            representation.headers = cls.headers_to_string(headers)
            representation.update_image_size()
            return representation, False

        # Okay, things didn't go so well.
        date_string = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
        representation.fetch_exception = representation.fetch_exception or (
            "Most recent fetch attempt (at %s) got status code %s" % (
                date_string, status_code))
        if usable_representation:
            # If we have a usable (but stale) representation, we'd
            # rather return the cached data than destroy the information.
            return representation, True

        # We didn't have a usable representation before, and we still don't.
        # At this point we're just logging an error.
        representation.status_code = status_code
        representation.headers = cls.headers_to_string(headers)
        representation.content = content
        return representation, False

    @classmethod
    def _best_media_type(cls, url, headers, default):
        """Determine the most likely media type for the given HTTP headers.
        Almost all the time, this is the value of the content-type
        header, if present. However, if the content-type header has a
        really generic value like "application/octet-stream" (as often
        happens with binary files hosted on Github), we'll privilege
        the default value. If there's no default value, we'll try to
        derive one from the URL extension.
        """
        default = default or cls.guess_url_media_type_from_path(url)
        if not headers or not 'content-type' in headers:
            return default
        headers_type = headers['content-type'].lower()
        clean = cls._clean_media_type(headers_type)
        if clean in Representation.GENERIC_MEDIA_TYPES and default:
            return default
        return headers_type

    @classmethod
    def reraise_exception(cls, representation, exception, traceback):
        """Deal with a fetch exception by re-raising it."""
        raise exception

    @classmethod
    def record_exception(cls, representation, exception, traceback):
        """Deal with a fetch exception by recording it
        and moving on.
        """
        representation.fetch_exception = traceback

    @classmethod
    def post(cls, _db, url, data, max_age=None, response_reviewer=None,
             **kwargs):
        """Finds or creates POST request as a Representation"""

        original_do_get = kwargs.pop('do_get', cls.simple_http_post)

        def do_post(url, headers, **kwargs):
            kwargs.update({'data' : data})
            return original_do_get(url, headers, **kwargs)

        return cls.get(
            _db, url, do_get=do_post, max_age=max_age,
            response_reviewer=response_reviewer, **kwargs
        )

    @property
    def mirrorable_media_type(self):
        """Does this Representation look like the kind of thing we
        create mirrors of?
        Basically, images and books.
        """
        return any(
            self.media_type in x for x in
            (Representation.BOOK_MEDIA_TYPES,
             Representation.IMAGE_MEDIA_TYPES)
        )

    def update_image_size(self):
        """Make sure .image_height and .image_width are up to date.
        Clears .image_height and .image_width if the representation
        is not an image.
        """
        if self.media_type and self.media_type.startswith('image/'):
            image = self.as_image()
            if image:
                self.image_width, self.image_height = image.size
                return
        self.image_width = self.image_height = None

    @classmethod
    def normalize_content_path(cls, content_path, base=None):
        if not content_path:
            return None
        base = base or Configuration.data_directory()
        if content_path.startswith(base):
            content_path = content_path[len(base):]
            if content_path.startswith('/'):
                content_path = content_path[1:]
        return content_path

    @property
    def unicode_content(self):
        """Attempt to convert the content into Unicode.
        If all attempts fail, we will return None rather than raise an exception.
        """
        content = None
        for encoding in ('utf-8', 'windows-1252'):
            try:
                content = self.content.decode(encoding)
                break
            except UnicodeDecodeError as e:
                pass
        return content

    def set_fetched_content(self, content, content_path=None):
        """Simulate a successful HTTP request for this representation.
        This is used when the content of the representation is obtained
        through some other means.
        """
        if isinstance(content, str):
            content = content.encode("utf8")
        self.content = content

        self.local_content_path = self.normalize_content_path(content_path)
        self.status_code = 200
        self.fetched_at = datetime.datetime.now(tz=pytz.UTC)
        self.fetch_exception = None
        self.update_image_size()

    def set_as_mirrored(self, mirror_url):
        """Record the fact that the representation has been mirrored
        to the given URL.
        This should only be called upon successful completion of the
        mirror operation.
        """
        self.mirror_url = mirror_url
        self.mirrored_at = datetime.datetime.now(tz=pytz.UTC)
        self.mirror_exception = None

    @classmethod
    def headers_to_string(cls, d):
        if d is None:
            return None
        return json.dumps(dict(d))

    @classmethod
    def simple_http_get(cls, url, headers, **kwargs):
        """The most simple HTTP-based GET."""
        if not 'allow_redirects' in kwargs:
            kwargs['allow_redirects'] = True
        response = HTTP.get_with_timeout(url, headers=headers, **kwargs)
        return response.status_code, response.headers, response.content

    @classmethod
    def simple_http_post(cls, url, headers, **kwargs):
        """The most simple HTTP-based POST."""
        data = kwargs.get('data')
        if 'data' in kwargs:
            del kwargs['data']
        response = HTTP.post_with_timeout(url, data, headers=headers, **kwargs)
        return response.status_code, response.headers, response.content

    @classmethod
    def http_get_no_timeout(cls, url, headers, **kwargs):
        return Representation.simple_http_get(url, headers, timeout=None, **kwargs)

    @classmethod
    def http_get_no_redirect(cls, url, headers, **kwargs):
        """HTTP-based GET with no redirects."""
        return cls.simple_http_get(url, headers, allow_redirects=False, **kwargs)

    @classmethod
    def browser_http_get(cls, url, headers, **kwargs):
        """GET the representation that would be displayed to a web browser.
        """
        headers = dict(headers)
        headers['User-Agent'] = cls.BROWSER_USER_AGENT
        return cls.simple_http_get(url, headers, **kwargs)

    @classmethod
    def cautious_http_get(cls, url, headers, **kwargs):
        """Examine the URL we're about to GET, possibly going so far as to
        perform a HEAD request, to avoid making a request (or
        following a redirect) to a site known to cause problems.
        The motivating case is that unglue.it contains gutenberg.org
        links that appear to be direct links to EPUBs, but 1) they're
        not direct links to EPUBs, and 2) automated requests to
        gutenberg.org quickly result in IP bans. So we don't make those
        requests.
        """
        do_not_access = kwargs.pop(
            'do_not_access', cls.AVOID_WHEN_CAUTIOUS_DOMAINS
        )
        check_for_redirect = kwargs.pop(
            'check_for_redirect', cls.EXERCISE_CAUTION_DOMAINS
        )
        do_get = kwargs.pop('do_get', cls.simple_http_get)
        head_client = kwargs.pop('cautious_head_client', requests.head)

        if cls.get_would_be_useful(
                url, headers, do_not_access, check_for_redirect,
                head_client
        ):
            # Go ahead and make the GET request.
            return do_get(url, headers, **kwargs)
        else:
            logging.info(
                "Declining to make non-useful HTTP request to %s", url
            )
            # 417 Expectation Failed - "... if the server is a proxy,
            # the server has unambiguous evidence that the request
            # could not be met by the next-hop server."
            #
            # Not quite accurate, but I think it's the closest match
            # to "the HTTP client decided to not even make your
            # request".
            return (
                417,
                {"content-type" :
                 "application/vnd.librarysimplified-did-not-make-request"},
                "Cautiously decided not to make a GET request to %s" % url
            )

    # Sites known to host both free books and redirects to a domain in
    # AVOID_WHEN_CAUTIOUS_DOMAINS.
    EXERCISE_CAUTION_DOMAINS = ['unglue.it']

    # Sites that cause problems for us if we make automated
    # HTTP requests to them while trying to find free books.
    AVOID_WHEN_CAUTIOUS_DOMAINS = ['gutenberg.org', 'books.google.com']

    @classmethod
    def get_would_be_useful(
            cls, url, headers, do_not_access=None, check_for_redirect=None,
            head_client=None
    ):
        """Determine whether making a GET request to a given URL is likely to
        have a useful result.

        :param URL: URL under consideration.
        :param headers: Headers that would be sent with the GET request.
        :param do_not_access: Domains to which GET requests are not useful.
        :param check_for_redirect: Domains to which we should make a HEAD
            request, in case they redirect to a `do_not_access` domain.
        :param head_client: Function for making the HEAD request, if
            one becomes necessary. Should return requests.Response or a mock.
        """
        do_not_access = do_not_access or cls.AVOID_WHEN_CAUTIOUS_DOMAINS
        check_for_redirect = check_for_redirect or cls.EXERCISE_CAUTION_DOMAINS
        head_client = head_client or requests.head

        def has_domain(domain, check_against):
            """Is the given `domain` in `check_against`,
            or maybe a subdomain of one of the domains in `check_against`?
            """
            return any(domain == x or domain.endswith('.' + x)
                       for x in check_against)

        netloc = urlparse(url).netloc
        if has_domain(netloc, do_not_access):
            # The link points directly to a domain we don't want to
            # access.
            return False

        if not has_domain(netloc, check_for_redirect):
            # We trust this domain not to redirect to a domain we don't
            # want to access.
            return True

        # We might be fine, or we might get redirected to a domain we
        # don't want to access. Make a HEAD request to see what
        # happens.
        head_response = head_client(url, headers=headers)
        if head_response.status_code // 100 != 3:
            # It's not a redirect. Go ahead and make the GET request.
            return True

        # Yes, it's a redirect. Does it redirect to a
        # domain we don't want to access?
        location = head_response.headers.get('location', '')
        netloc = urlparse(location).netloc
        return not has_domain(netloc, do_not_access)

    @property
    def is_image(self):
        return self.media_type and self.media_type.startswith("image/")

    @property
    def local_path(self):
        """Return the full local path to the representation on disk."""
        if not self.local_content_path:
            return None
        return os.path.join(Configuration.data_directory(),
                            self.local_content_path)

    @property
    def clean_media_type(self):
        """The most basic version of this representation's media type.
        No profiles or anything.
        """
        return self._clean_media_type(self.media_type)

    @property
    def url_extension(self):
        """The file extension in this representation's original url."""

        url_path = urlparse(self.url).path

        # Known extensions can be followed by a version number (.epub3)
        # or an additional extension (.epub.noimages)
        known_extensions = "|".join(list(self.FILE_EXTENSIONS.values()))
        known_extension_re = re.compile("\.(%s)\d?\.?[\w\d]*$" % known_extensions, re.I)

        known_match = known_extension_re.search(url_path)

        if known_match:
            return known_match.group()

        else:
            any_extension_re = re.compile("\.[\w\d]*$", re.I)

            any_match = any_extension_re.search(url_path)

            if any_match:
                return any_match.group()
        return None

    def extension(self, destination_type=None):
        """Try to come up with a good file extension for this representation."""
        if destination_type:
            return self._extension(destination_type)

        # We'd like to use url_extension because it has some extra
        # features for preserving information present in the original
        # URL. But if we're going to be changing the media type of the
        # resource when mirroring it, the original URL is irrelevant
        # and we need to use an extension associated with the
        # outward-facing media type.
        internal = self.clean_media_type
        external = self._clean_media_type(self.external_media_type)
        if internal != external:
            # External media type overrides any information that might
            # be present in the URL.
            return self._extension(external)

        # If there is information in the URL, use it.
        extension = self.url_extension
        if extension:
            return extension

        # Take a guess based on the internal media type.
        return self._extension(internal)

    @classmethod
    def _clean_media_type(cls, media_type):
        if not media_type:
            return media_type
        if ';' in media_type:
            media_type = media_type[:media_type.index(';')].strip()
        return media_type

    @classmethod
    def _extension(cls, media_type):
        value = cls.FILE_EXTENSIONS.get(media_type, '')
        if not value:
            return value
        return '.' + value

    def default_filename(self, link=None, destination_type=None):
        """Try to come up with a good filename for this representation."""

        scheme, netloc, path, query, fragment = urlsplit(self.url)
        path_parts = path.split("/")
        filename = None
        if path_parts:
            filename = path_parts[-1]

        if not filename and link:
            filename = link.default_filename
        if not filename:
            # This is the absolute last-ditch filename solution, and
            # it's basically only used when we try to mirror the root
            # URL of a domain.
            filename = 'resource'

        default_extension = self.extension()
        extension = self.extension(destination_type)
        if default_extension and default_extension != extension and filename.endswith(default_extension):
            filename = filename[:-len(default_extension)] + extension
        elif extension and not filename.endswith(extension):
            filename += extension
        return filename

    @property
    def external_media_type(self):
        return self.media_type

    def external_content(self):
        """Return a filehandle to the representation's contents, as they
        should be mirrored externally, and the media type to be used
        when mirroring.
        """
        return self.content_fh()

    def content_fh(self):
        """Return an open filehandle to the representation's contents.
        This works whether the representation is kept in the database
        or in a file on disk.
        """
        if self.content:
            if not isinstance(self.content, bytes):
                self.content = self.content.encode("utf-8")
            return BytesIO(self.content)
        elif self.local_path:
            if not os.path.exists(self.local_path):
                raise ValueError("%s does not exist." % self.local_path)
            return open(self.local_path, 'rb')
        return None

    def as_image(self):
        """Load this Representation's contents as a PIL image."""
        if not self.is_image:
            raise ValueError(
                "Cannot load non-image representation as image: type %s."
                % self.media_type)
        if not self.content and not self.local_path:
            raise ValueError("Image representation has no content.")

        fh = self.content_fh()
        if not fh or self.clean_media_type == self.SVG_MEDIA_TYPE:
            return None
        return Image.open(fh)

    pil_format_for_media_type = {
        "image/gif": "gif",
        "image/png": "png",
        "image/jpeg": "jpeg",
    }

    def scale(self, max_height, max_width,
              destination_url, destination_media_type, force=False):
        """Return a Representation that's a scaled-down version of this
        Representation, creating it if necessary.
        :param destination_url: The URL the scaled-down resource will
        (eventually) be uploaded to.
        :return: A 2-tuple (Representation, is_new)
        """
        _db = Session.object_session(self)

        if not destination_media_type in self.pil_format_for_media_type:
            raise ValueError("Unsupported destination media type: %s" % destination_media_type)

        pil_format = self.pil_format_for_media_type[destination_media_type]

        # Make sure we actually have an image to scale.
        image = None
        try:
            image = self.as_image()
        except Exception as e:
            self.scale_exception = traceback.format_exc()
            self.scaled_at = None
            # This most likely indicates an error during the fetch
            # phrase.
            self.fetch_exception = "Error found while scaling: %s" % (
                self.scale_exception)
            logging.error("Error found while scaling %r", self, exc_info=e)

        if not image:
            return self, False

        # Now that we've loaded the image, take the opportunity to set
        # the image size of the original representation.
        self.image_width, self.image_height = image.size

        # If the image is already a thumbnail-size bitmap, don't bother.
        if (self.clean_media_type != Representation.SVG_MEDIA_TYPE
            and self.image_height <= max_height
            and self.image_width <= max_width):
            self.thumbnails = []
            return self, False

        # Do we already have a representation for the given URL?
        thumbnail, is_new = get_one_or_create(
            _db, Representation, url=destination_url,
            media_type=destination_media_type
        )
        if thumbnail not in self.thumbnails:
            thumbnail.thumbnail_of = self

        if not is_new and not force:
            # We found a preexisting thumbnail and we're allowed to
            # use it.
            return thumbnail, is_new

        # At this point we have a parent Representation (self), we
        # have a Representation that will contain a thumbnail
        # (thumbnail), and we know we need to actually thumbnail the
        # parent into the thumbnail.
        #
        # Because the representation of this image is being
        # changed, it will need to be mirrored later on.
        now = datetime.datetime.now(tz=pytz.UTC)
        thumbnail.mirrored_at = None
        thumbnail.mirror_exception = None

        args = [(max_width, max_height), Image.LANCZOS]

        try:
            image.thumbnail(*args)
        except IOError as e:
            # I'm not sure why, but sometimes just trying
            # it again works.
            original_exception = traceback.format_exc()
            try:
                image.thumbnail(*args)
            except IOError as e:
                self.scale_exception = original_exception
                self.scaled_at = None
                return self, False

        # Save the thumbnail image to the database under
        # thumbnail.content.
        output = BytesIO()
        if image.mode != 'RGB':
            image = image.convert('RGB')
        try:
            image.save(output, pil_format)
        except Exception as e:
            self.scale_exception = traceback.format_exc()
            self.scaled_at = None
            # This most likely indicates a problem during the fetch phase,
            # Set fetch_exception so we'll retry the fetch.
            self.fetch_exception = "Error found while scaling: %s" % (self.scale_exception)
            return self, False
        thumbnail.content = output.getvalue()
        thumbnail.image_width, thumbnail.image_height = image.size
        output.close()
        thumbnail.scale_exception = None
        thumbnail.scaled_at = now
        return thumbnail, True

    @property
    def thumbnail_size_quality_penalty(self):
        return self._thumbnail_size_quality_penalty(
            self.image_width, self.image_height
        )

    @classmethod
    def _thumbnail_size_quality_penalty(cls, width, height):
        """Measure a cover image's deviation from the ideal aspect ratio, and
        by its deviation (in the "too small" direction only) from the
        ideal thumbnail resolution.
        """

        quotient = 1

        if not width or not height:
            # In the absence of any information, assume the cover is
            # just dandy.
            #
            # This is obviously less than ideal, but this code is used
            # pretty rarely now that we no longer have hundreds of
            # covers competing for the privilege of representing a
            # public domain book, so I'm not too concerned about it.
            #
            # Look at it this way: this escape hatch only causes a
            # problem if we compare an image whose size we know
            # against an image whose size we don't know.
            #
            # In the circulation manager, we never know what size an
            # image is, and we must always trust that the cover
            # (e.g. Overdrive and the metadata wrangler) give us
            # "thumbnail" images that are approximately the right
            # size. So we always use this escape hatch.
            #
            # In the metadata wrangler and content server, we always
            # have access to the covers themselves, so we always have
            # size information and we never use this escape hatch.
            return quotient

        # Penalize an image for deviation from the ideal aspect ratio.
        aspect_ratio = width / float(height)
        ideal = IdentifierConstants.IDEAL_COVER_ASPECT_RATIO
        if aspect_ratio > ideal:
            deviation = ideal / aspect_ratio
        else:
            deviation = aspect_ratio/ideal
        if deviation != 1:
            quotient *= deviation

        # Penalize an image for not being wide enough.
        width_shortfall = (
            float(width - IdentifierConstants.IDEAL_IMAGE_WIDTH) / IdentifierConstants.IDEAL_IMAGE_WIDTH)
        if width_shortfall < 0:
            quotient *= (1+width_shortfall)

        # Penalize an image for not being tall enough.
        height_shortfall = (
            float(height - IdentifierConstants.IDEAL_IMAGE_HEIGHT) / IdentifierConstants.IDEAL_IMAGE_HEIGHT)
        if height_shortfall < 0:
            quotient *= (1+height_shortfall)
        return quotient

    @property
    def best_thumbnail(self):
        """Find the best thumbnail among all the thumbnails associated with
        this Representation.
        Basically, we prefer a thumbnail that has been mirrored.
        """
        champion = None
        for thumbnail in self.thumbnails:
            if thumbnail.mirror_url:
                champion = thumbnail
                break
            elif not champion:
                champion = thumbnail
        return champion
