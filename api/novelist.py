import json
import logging
import urllib
from collections import Counter
from nose.tools import set_trace
from flask_babel import lazy_gettext as _

from core.config import Configuration
from core.coverage import (
    CoverageFailure,
    IdentifierCoverageProvider,
)
from core.metadata_layer import (
    ContributorData,
    IdentifierData,
    LinkData,
    MeasurementData,
    Metadata,
    SubjectData,
)
from core.model import (
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Measurement,
    Representation,
    Session,
    Subject,
    get_one,
    Equivalency,
    LicensePool,
    Collection,
    Edition,
    Contributor,
    Contribution,
)
from core.util import TitleProcessor
from sqlalchemy.sql import (
    select,
    join,
    and_,
    or_,
)
from sqlalchemy.orm import aliased
from core.util.http import HTTP

class NoveListAPI(object):

    PROTOCOL = ExternalIntegration.NOVELIST
    NAME = _("Novelist API")

    # Hardcoded authentication key used as a Header for calling the NoveList
    # Collections API. It identifies the client, and lets NoveList know that
    # SimplyE is making the requests.
    AUTHORIZED_IDENTIFIER = u"62521fa1-bdbb-4939-84aa-aee2a52c8d59"

    SETTINGS = [
        { "key": ExternalIntegration.USERNAME, "label": _("Profile"), "required": True },
        { "key": ExternalIntegration.PASSWORD, "label": _("Password"), "required": True },
    ]

    # Different libraries may have different NoveList integrations
    # on the same circulation manager.
    SITEWIDE = False

    IS_CONFIGURED = None
    _configuration_library_id = None

    log = logging.getLogger("NoveList API")
    version = "2.2"

    NO_ISBN_EQUIVALENCY = "No clear ISBN equivalency: %r"

    # While the NoveList API doesn't require parameters to be passed via URL,
    # the Representation object needs a unique URL to return the proper data
    # from the database.
    QUERY_ENDPOINT = (
        "https://novselect.ebscohost.com/Data/ContentByQuery?"
        "ISBN=%(ISBN)s&ClientIdentifier=%(ClientIdentifier)s&version=%(version)s"
    )
    COLLECTION_DATA_API = "http://www.noveListcollectiondata.com/api/collections"
    AUTH_PARAMS = "&profile=%(profile)s&password=%(password)s"
    MAX_REPRESENTATION_AGE = 7*24*60*60      # one week

    currentQueryIdentifier = None

    medium_to_book_format_type_values = {
        Edition.BOOK_MEDIUM : u"EBook",
        Edition.AUDIO_MEDIUM : u"Audiobook",
    }

    @classmethod
    def from_config(cls, library):
        profile, password = cls.values(library)
        if not (profile and password):
            raise ValueError("No NoveList client configured.")

        _db = Session.object_session(library)
        return cls(_db, profile, password)

    @classmethod
    def values(cls, library):
        _db = Session.object_session(library)

        integration = ExternalIntegration.lookup(
            _db, ExternalIntegration.NOVELIST,
            ExternalIntegration.METADATA_GOAL, library=library
        )

        if not integration:
            return (None, None)

        profile = integration.username
        password = integration.password
        return (profile, password)

    @classmethod
    def is_configured(cls, library):
        if (cls.IS_CONFIGURED is None or
            library.id != cls._configuration_library_id
        ):
            profile, password = cls.values(library)
            cls.IS_CONFIGURED = bool(profile and password)
            cls._configuration_library_id = library.id
        return cls.IS_CONFIGURED

    def __init__(self, _db, profile, password):
        self._db = _db
        self.profile = profile
        self.password = password

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.NOVELIST)

    def lookup_equivalent_isbns(self, identifier):
        """Finds NoveList data for all ISBNs equivalent to an identifier.

        :return: Metadata object or None
        """
        lookup_metadata = []
        license_sources = DataSource.license_sources_for(self._db, identifier)

        # Find strong ISBN equivalents.
        isbns = list()
        for license_source in license_sources:
            isbns += [eq.output for eq in identifier.equivalencies if (
                eq.data_source==license_source and
                eq.strength==1 and
                eq.output.type==Identifier.ISBN
            )]

        if not isbns:
            self.log.warn(
                ("Identifiers without an ISBN equivalent can't"
                "be looked up with NoveList: %r"), identifier
            )
            return None

        # Look up metadata for all equivalent ISBNs.
        lookup_metadata = list()
        for isbn in isbns:
            metadata = self.lookup(isbn)
            if metadata:
                lookup_metadata.append(metadata)

        if not lookup_metadata:
            self.log.warn(
                ("No NoveList metadata found for Identifiers without an ISBN"
                "equivalent can't be looked up with NoveList: %r"), identifier
            )
            return None

        best_metadata, confidence = self.choose_best_metadata(
            lookup_metadata, identifier
        )
        if best_metadata:
            if round(confidence, 2) < 0.5:
                self.log.warn(self.NO_ISBN_EQUIVALENCY, identifier)
                return None
            return metadata

    @classmethod
    def _confirm_same_identifier(self, metadata_objects):
        """Ensures that all metadata objects have the same NoveList ID"""

        novelist_ids = set([
            metadata.primary_identifier.identifier
            for metadata in metadata_objects
        ])
        return len(novelist_ids)==1

    def choose_best_metadata(self, metadata_objects, identifier):
        """Chooses the most likely book metadata from a list of Metadata objects

        Given several Metadata objects with different NoveList IDs, this
        method returns the metadata of the ID with the highest representation
        and a float representing confidence in the result.
        """
        confidence = 1.0
        if self._confirm_same_identifier(metadata_objects):
            # Metadata with the same NoveList ID will be identical. Take one.
            return metadata_objects[0], confidence

        # One or more of the equivalents did not return the same NoveList work
        self.log.warn("%r has inaccurate ISBN equivalents", identifier)
        counter = Counter()
        for metadata in metadata_objects:
            counter[metadata.primary_identifier] += 1

        [(target_identifier, most_amount),
        (ignore, secondmost)] = counter.most_common(2)
        if most_amount==secondmost:
            # The counts are the same, and neither can be trusted.
            self.log.warn(self.NO_ISBN_EQUIVALENCY, identifier)
            return None, None
        confidence = most_amount / float(len(metadata_objects))
        target_metadata = filter(
            lambda m: m.primary_identifier==target_identifier, metadata_objects
        )
        return target_metadata[0], confidence

    def lookup(self, identifier):
        """Requests NoveList metadata for a particular identifier

        :return: Metadata object or None
        """
        client_identifier = identifier.urn
        if identifier.type != Identifier.ISBN:
            return self.lookup_equivalent_isbns(identifier)

        params = dict(
            ClientIdentifier=client_identifier, ISBN=identifier.identifier,
            version=self.version, profile=self.profile, password=self.password
        )
        scrubbed_url = unicode(self.scrubbed_url(params))

        representation = self.cached_representation(scrubbed_url)
        if not representation:
            self.log.info("No cached NoveList request available.")

            url = self.build_query_url(params)
            self.log.debug("NoveList lookup: %s",  url)
            representation, from_cache = Representation.post(
                self._db, unicode(url), '', max_age=self.MAX_REPRESENTATION_AGE,
                response_reviewer=self.review_response
            )

            # Remove credential information from the Representation URL. This
            # avoids holding those details in an unexpected part of the database
            # and lets multiple libraries to use the same cached representation.
            representation.url = scrubbed_url

        return self.lookup_info_to_metadata(representation)

    @classmethod
    def review_response(cls, response):
        """Performs NoveList-specific error review of the request response"""
        status_code, headers, content = response
        if status_code == 403:
            raise Exception("Invalid NoveList credentials")
        if content.startswith('"Missing'):
            raise Exception("Invalid NoveList parameters: %s" % content)
        return response

    @classmethod
    def scrubbed_url(cls, params):
        """Removes authentication details from cached Representation.url"""
        return cls.build_query_url(params, include_auth=False)

    @classmethod
    def _scrub_subtitle(cls, subtitle):
        """Removes common NoveList subtitle annoyances"""
        if subtitle:
            subtitle = subtitle.replace('[electronic resource]', '')
            # Then get rid of any leading whitespace or punctuation.
            subtitle = TitleProcessor.extract_subtitle('', subtitle)
        return subtitle

    @classmethod
    def build_query_url(cls, params, include_auth=True):
        """Builds a unique and url-encoded query endpoint"""
        url = cls.QUERY_ENDPOINT
        if include_auth:
            url += cls.AUTH_PARAMS

        urlencoded_params = dict()
        for name, value in params.items():
            urlencoded_params[name] = urllib.quote(value)
        return url % urlencoded_params

    def cached_representation(self, scrubbed_url):
        """Attempts to find a usable cached Representation for a given URL"""
        representation = get_one(
            self._db, Representation, 'interchangeable', url=scrubbed_url
        )

        if not representation:
            return None
        if not representation.is_fresher_than(self.MAX_REPRESENTATION_AGE):
            # The Representation is nonexistent or stale. Delete it, so it
            # can be replaced.
            self._db.delete(representation)
            return None
        return representation

    def lookup_info_to_metadata(self, lookup_representation):
        """Transforms a NoveList JSON representation into a Metadata object"""

        if not lookup_representation.content:
            return None

        lookup_info = json.loads(lookup_representation.content)
        book_info = lookup_info['TitleInfo']
        if book_info:
            novelist_identifier = book_info.get('ui')
        if not book_info or not novelist_identifier:
            # NoveList didn't know the ISBN.
            return None

        primary_identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.NOVELIST_ID, novelist_identifier
        )
        metadata = Metadata(self.source, primary_identifier=primary_identifier)

        # Get the equivalent ISBN identifiers.
        metadata.identifiers += self._extract_isbns(book_info)

        author = book_info.get('author')
        if author:
            metadata.contributors.append(ContributorData(sort_name=author))

        description = book_info.get('description')
        if description:
            metadata.links.append(LinkData(
                rel=Hyperlink.DESCRIPTION, content=description,
                media_type=Representation.TEXT_PLAIN
            ))

        audience_level = book_info.get('audience_level')
        if audience_level:
            metadata.subjects.append(SubjectData(
                Subject.FREEFORM_AUDIENCE, audience_level
            ))

        novelist_rating = book_info.get('rating')
        if novelist_rating:
            metadata.measurements.append(MeasurementData(
                Measurement.RATING, novelist_rating
            ))

        # Extract feature content if it is available.
        series_info = None
        appeals_info = None
        lexile_info = None
        goodreads_info = None
        recommendations_info = None
        feature_content = lookup_info.get('FeatureContent')
        if feature_content:
            series_info = feature_content.get('SeriesInfo')
            appeals_info = feature_content.get('Appeals')
            lexile_info = feature_content.get('LexileInfo')
            goodreads_info = feature_content.get('GoodReads')
            recommendations_info = feature_content.get('SimilarTitles')

        metadata, title_key = self.get_series_information(
            metadata, series_info, book_info
        )
        metadata.title = book_info.get(title_key)
        subtitle = TitleProcessor.extract_subtitle(
            metadata.title, book_info.get('full_title')
        )
        metadata.subtitle = self._scrub_subtitle(subtitle)

        if appeals_info:
            extracted_genres = False
            for appeal in appeals_info:
                genres = appeal.get('genres')
                if genres:
                    for genre in genres:
                        metadata.subjects.append(SubjectData(
                            Subject.TAG, genre['Name']
                        ))
                        extracted_genres = True
                if extracted_genres:
                    break

        if lexile_info:
            metadata.subjects.append(SubjectData(
                Subject.LEXILE_SCORE, lexile_info['Lexile']
            ))

        if goodreads_info:
            metadata.measurements.append(MeasurementData(
                Measurement.RATING, goodreads_info['average_rating']
            ))

        metadata = self.get_recommendations(metadata, recommendations_info)

        # If nothing interesting comes from the API, ignore it.
        if not (metadata.measurements or metadata.series_position or
            metadata.series or metadata.subjects or metadata.links or
            metadata.subtitle or metadata.recommendations
        ):
            metadata = None
        return metadata

    def get_series_information(self, metadata, series_info, book_info):
        """Returns metadata object with series info and optimal title key"""

        title_key = 'main_title'
        if series_info:
            metadata.series = series_info['full_title']
            series_titles = series_info.get('series_titles')
            if series_titles:
                matching_series_volume = [volume for volume in series_titles
                        if volume.get('full_title')==book_info.get('full_title')]
                if not matching_series_volume:
                    # If there's no full_title match, try the main_title.
                    matching_series_volume = [volume for volume in series_titles
                        if volume.get('main_title')==book_info.get('main_title')]
                if len(matching_series_volume) > 1:
                    # This probably won't happen, but if it does, it will be
                    # difficult to debug without an error.
                    raise ValueError("Multiple matching volumes found.")
                series_position = matching_series_volume[0].get('volume')
                if series_position:
                    if series_position.endswith('.'):
                        series_position = series_position[:-1]
                    metadata.series_position = int(series_position)

                # Sometimes all of the volumes in a series have the same
                # main_title so using the full_title is preferred.
                main_titles = [volume.get(title_key) for volume in series_titles]
                if len(main_titles) > 1 and len(set(main_titles))==1:
                    title_key = 'full_title'

        return metadata, title_key

    def _extract_isbns(self, book_info):
        isbns = []

        synonymous_ids = book_info.get('manifestations')
        for synonymous_id in synonymous_ids:
            isbn = synonymous_id.get('ISBN')
            if isbn:
                isbn_data = IdentifierData(Identifier.ISBN, isbn)
                isbns.append(isbn_data)

        return isbns

    def get_recommendations(self, metadata, recommendations_info):
        if not recommendations_info:
            return metadata

        related_books = recommendations_info.get('titles')
        related_books = filter(lambda b: b.get('is_held_locally'), related_books)
        if related_books:
            for book_info in related_books:
                metadata.recommendations += self._extract_isbns(book_info)
        return metadata

    def get_items_from_query(self, library):
        """Gets identifiers and its related title, medium, and authors from the
        database.
        Keeps track of the current 'ISBN' identifier and current item object that
        is being processed. If the next ISBN being processed is new, the existing one
        gets added to the list of items. If the ISBN is the same, then we append
        the Author property since there are multiple contributors.
        """
        collectionList = []
        for c in library.collections:
            collectionList.append(c.id)

        LEFT_OUTER_JOIN = True
        i1 = aliased(Identifier)
        i2 = aliased(Identifier)
        roles = list(Contributor.AUTHOR_ROLES)
        # TODO: We should handle the Narrator role properly, by
        # setting the 'narrator' field in the NoveList API document.
        # roles.append(Contributor.NARRATOR_ROLE)

        isbnQuery = select(
            [i1.identifier, i1.type, i2.identifier,
            Edition.title, Edition.medium,
            Contribution.role, Contributor.sort_name],
        ).select_from(
            join(LicensePool, i1, i1.id==LicensePool.identifier_id)
            .join(Equivalency, i1.id==Equivalency.input_id, LEFT_OUTER_JOIN)
            .join(i2, Equivalency.output_id==i2.id, LEFT_OUTER_JOIN)
            .join(
                Edition,
                or_(Edition.primary_identifier_id==i1.id, Edition.primary_identifier_id==i2.id)
            )
            .join(Contribution, Edition.id==Contribution.edition_id)
            .join(Contributor, Contribution.contributor_id==Contributor.id)
        ).where(
            and_(
                LicensePool.collection_id.in_(collectionList),
                or_(i1.type=="ISBN", i2.type=="ISBN"),
                or_(Contribution.role.in_(roles))
            )
        ).order_by(i1.identifier, i2.identifier)

        result = self._db.execute(isbnQuery)

        items = []
        newItem = None
        existingItem = None
        currentIdentifier = None
        for item in result:
            if newItem:
                existingItem = newItem
            (currentIdentifier, existingItem, newItem, addItem) = (
                self.create_item_object(item, currentIdentifier, existingItem)
            )

            if addItem and existingItem:
                # The Role property isn't needed in the actual request.
                del existingItem['role']
                items.append(existingItem)

        # For the case when there's only one item in `result`
        if newItem:
            del newItem['role']
            items.append(newItem)

        return items

    def create_item_object(self, object, currentIdentifier, existingItem):
        """Returns a new item if the current identifier that was processed
        is not the same as the new object's ISBN being processed. If the new
        object's ISBN matches the current identifier, the previous object's
        Author property is updated.
        """
        if not object:
            return (None, None, None, False)

        if (object[1] == Identifier.ISBN):
            isbn = object[0]
        elif object[2] is not None:
            isbn = object[2]
        else:
            # We cannot find an ISBN for this work -- probably due to
            # a data error.
            return (None, None, None, False)

        role = object[5]
        author = object[6] if role in Contributor.AUTHOR_ROLES else ""

        # If we encounter an existing ISBN and its role is "Primary Author",
        # then that value overrides the existing Author property.
        #
        # TODO: add 'narrator' field when we encounter a Narrator role.
        if isbn == currentIdentifier and existingItem:
            if role == Contributor.PRIMARY_AUTHOR_ROLE:
                existingItem['author'] = author
                existingItem['role'] = role
            return (currentIdentifier, existingItem, None, False)
        else:
            # If we encounter a new ISBN, we take whatever author value is
            # initially given to us.
            title = object[3]
            mediaType = self.medium_to_book_format_type_values.get(object[4], "")
            newItem = dict(
                isbn=isbn,
                title=title,
                mediaType=mediaType,
                author=author,
                role=role,
            )
            return (isbn, existingItem, newItem, True)

    def put_items_novelist(self, library):
        items = self.get_items_from_query(library)

        content = None
        if items:
            data=json.dumps(self.make_novelist_data_object(items))
            response = self.put(
                self.COLLECTION_DATA_API,
                {
                    "AuthorizedIdentifier": self.AUTHORIZED_IDENTIFIER,
                    "Content-Type": "application/json; charset=utf-8"
                },
                data=data
            )
            if (response.status_code == 200):
                content = json.loads(response.content)
                logging.info(
                    "Success from NoveList: %r", response.content
                )
            else:
                logging.error("Data sent was: %r", data)
                logging.error(
                    "Error %s from NoveList: %r", response.status_code,
                    response.content
                )

        return content

    def make_novelist_data_object(self, items):
        return {
            "customer": "%s:%s" % (self.profile, self.password),
            "records": items,
        }

    def put(self, url, headers, **kwargs):
        data = kwargs.get('data')
        if 'data' in kwargs:
            del kwargs['data']

        response = HTTP.put_with_timeout(url, data, headers=headers, **kwargs)
        return response


class MockNoveListAPI(NoveListAPI):

    def __init__(self, _db, *args, **kwargs):
        self._db = _db
        self.responses = []

    def setup(self, *args):
        self.responses = self.responses + list(args)

    def lookup(self, identifier):
        response = self.responses[0]
        self.responses = self.responses[1:]
        return response


class NoveListCoverageProvider(IdentifierCoverageProvider):

    SERVICE_NAME = "NoveList CoverageProvider"
    DATA_SOURCE_NAME = DataSource.NOVELIST
    DEFAULT_BATCH_SIZE = 25
    INPUT_IDENTIFIER_TYPES = [Identifier.ISBN]

    def process_item(self, identifier):
        metadata = self.api.lookup(identifier)
        if not metadata:
            # Either NoveList didn't recognize the identifier or
            # no interesting data came of this. Consider it covered.
            return identifier

        # Set identifier equivalent to its NoveList ID.
        identifier.equivalent_to(
            self.data_source, metadata.primary_identifier,
            strength=1
        )
        # Create an edition with the NoveList metadata & NoveList identifier.
        # This will capture equivalent ISBNs and less appealing metadata in
        # its unfettered state on the NoveList identifier alone.
        edition, ignore = metadata.edition(self._db)
        metadata.apply(edition, collection=None)

        if edition.series or edition.series_position:
            metadata.primary_identifier = identifier
            # Series data from NoveList is appealing, but we need to avoid
            # creating any potentially-inaccurate ISBN equivalencies on the
            # license source identifier.
            #
            # So just remove the identifiers from the metadata object entirely.
            metadata.identifiers = []
            # Before creating an edition for the original identifier.
            novelist_edition, ignore = metadata.edition(self._db)
            metadata.apply(novelist_edition, collection=None)

        return identifier
