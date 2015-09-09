"""An abstract way of representing incoming metadata and applying it
to Identifiers and Editions.

This acts as an intermediary between the third-party integrations
(which have this information in idiosyncratic formats) and the
model. Doing a third-party integration should be as simple as putting
the information into this format.
"""

from sqlalchemy.orm.session import Session
from nose.tools import set_trace

import csv
import datetime
import logging
from util import LanguageCodes
from model import (
    get_one_or_create,
    CirculationEvent,
    Contributor,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Subject,
)

class SubjectData(object):
    def __init__(self, type, identifier, name=None, weight=1):
        self.type = type
        self.identifier = identifier
        self.name = name
        self.weight=weight

class ContributorData(object):
    def __init__(self, sort_name=None, display_name=None, roles=None,
                 lc=None, viaf=None):
        self.sort_name = sort_name
        self.display_name = display_name
        roles = roles or AUTHOR_ROLE
        if not isinstance(roles, list):
            roles = [roles]
        self.roles = roles
        self.lc = lc
        self.viaf = viaf

   def find_sort_name(cls, _db, identifiers, metadata_client):
        """Try as hard as possible to find the canonical name for the
        given author.
        """
        if self.sort_name:
            return True

        if not self.display_name:
            raise ValueError(
                "Cannot find sort name for a contributor with no display name!"
            )

        # Is there a contributor already in the database with this
        # exact sort name? If so, use their display name.
        sort_name = cls.sort_name_for_display_name(_db, self.display_name)
        if sort_name:
            self.sort_name = sort_name
            return True

        # Time to break out the big guns. Ask the metadata wrangler
        # if it can find a sort name for this display name.
        sort_name = cls.display_name_to_sort_name_through_canonicalizer(
            _db, identifiers, self.display_name, metadata_client
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
            return contributors[0].name
        return None

    @classmethod
    def display_name_to_sort_name_through_canonicalizer(
            cls, _db, identifiers, metadata_client):
        for identifier in identifiers:
            if identifier.type != Identifier.ISBN_TYPE:
                continue
            response = metadata_client.canonicalize_author_name(
                identifier.identifier, self.display_name)
            sort_name = None
            log = logging.getLogger("Abstract metadata layer")
            if (response.status_code == 200 
                and response.headers['Content-Type'].startswith('text/plain')):
                sort_name = response.content.decode("utf8")
                log.info(
                    "Canonicalizer found sort name for %s: %s => %s",
                    identifier, 
                    self.display_name,
                    sort_name
                )
            else:
                log.warn(
                    "Canonicalizer could not find sort name for %r/%s",
                    identifier,
                    self.display_name
                )
        return sort_name        


class IdentifierData(object):
    def __init__(self, type, identifier, weight=1):
        self.type = type
        self.identifier = identifier
        self.weight = 1

    def load(self, _db):
        return Identifier.for_foreign_identifier(
            _db, self.type, self.identifier
        )

class CirculationData(object):
    def __init__(
            self, licenses_owned, 
            licenses_available, 
            licenses_reserved,
            patrons_in_hold_queue,
            last_checked=None
    ):
        self.licenses_owned = licenses_owned
        self.licenses_available = licenses_available
        self.licenses_reserved = licenses_reserved
        self.patrons_in_hold_queue = patrons_in_hold_queue
        self.last_checked = last_checked or datetime.datetime.utcnow()

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

        # Update availabily information. This may result in the issuance
        # of additional events.
        license_pool.update_availability(
            self.licenses_owned,
            self.licenses_available,
            self.licenses_reserved,
            self.patrons_in_hold_queue,
            self.last_checked
        )

class Metadata(object):

    """A (potentially partial) set of metadata for a published work."""

    log = logging.getLogger("Abstract metadata layer")

    def __init__(
            self, 
            data_source,
            title=None,
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
    ):
        self._data_source = data_source
        if isinstance(self._data_source, DataSource):
            self.data_source_obj = self._data_source
        else:
            self.data_source_obj = None
        self.title = title
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
        self.identifiers = identifiers
        self.permanent_work_id = None
        if self.primary_identifier not in self.identifiers:
            self.identifiers.append(self.primary_identifier)
        self.subjects = subjects
        self.contributors = contributors

    def normalize_contributors(self, metadata_client):
        """Make sure that all contributors without a .sort_name get one."""
        for contributor in contributors:
            if not contributor.sort_name:
                contributor.normalize(metadata_client)

    def calculate_permanent_work_id(self, metadata_client):
        """Try to calculate a permanent work ID from this metadata.

        This may require asking a metadata wrangler to turn a display name
        into a sort name--thus the `metadata_client` argument.
        """
        primary_author = None
        for tier in Contributor.author_contributor_tiers:
            for c in self.contributors:
                for role in tier:
                    if role in c.roles:
                        primary_author = c
                        break
                if primary_author:
                    break
            if primary_author:
                break

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

    def associate_with_identifiers_based_on_permanent_work_id(
            self, _db):
        """Try to associate this object's primary identifier with
        the primary identifiers of Editions in the database which share
        a permanent work ID.
        """
        if (not self.primary_identifier or not self.permanent_work_id):
            # We don't have the information necessary to carry out this
            # method.
            return

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
                    same_work_id, self.primary_identifier, permanent_work_id
                )
                self.primary_identifier.equivalent_to(
                    self.data_source(_db), same_work_id, 0.85)

    def data_source(self, _db):
        if not self.data_source_obj:
            self.data_source_obj = DataSource.lookup(_db, self._data_source)
        return self.data_source_obj

    def edition(self, _db, create_if_not_exists=True):
        return Edition.for_foreign_id(
            _db, self.data_source(_db), self.primary_identifier.type, 
            self.primary_identifier.identifier, 
            create_if_not_exists=create_if_not_exists
        )        

    def license_pool(self, _db):
        return LicensePool.for_foreign_id(
            _db, self.data_source(_db), self.primary_identifier.type, 
            self.primary_identifier.identifier
        )

    def apply(
            self, edition, 
            metadata_client=None
            replace_identifiers=False,
            replace_subjects=False, 
            replace_contributions=False,
    ):
        """Apply this metadata to the given edition."""

        if metadata_client and not self.permanent_work_id:
            self.calculate_permanent_work_id(metadata_client)

        _db = Session.object_session(edition)
        __transaction = _db.begin_nested()

        identifier = edition.primary_identifier
        self.log.info(
            "APPLYING METADATA TO EDITION: %s",  self.title
        )
        if self.title:
            edition.title = self.title
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
        data_source = self.data_source(_db)

        # TODO: remove equivalencies when replace_identifiers is True.

        primary_identifier, ignore = Identifier.for_foreign_id(
            _db, self.primary_identifier.type, 
            self.primary_identifier.identifier
        )

        if self.identifiers is not None:
            for identifier_data in self.identifiers:
                identifier, ignore = Identifier.for_foreign_id(
                    _db, identifier_data.type, identifier_data.identifier)
                primary_identifier.equivalent_to(
                    data_source, identifier, identifier_data.weight)

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
                __transaction.flush()

        # Apply all specified subjects to the identifier.
        for subject in self.subjects:
            identifier.classify(
                data_source, subject.type, subject.identifier, 
                subject.name, weight=subject.weight)

        if replace_contributions:
            dirty = False
            if self.contributors is not None:
                # Remove any old Contributions from this data source --
                # we're about to add a new set
                surviving_contributions = []
                for contribution in edition.contributions:
                    _db.delete(contribution)
                    dirty = True
                edition.contributions = surviving_contributions
            if dirty:
                __transaction.flush()
            for contributor_data in self.contributors:
                contributor = edition.add_contributor(
                    name=contributor_data.sort_name, 
                    roles=contributor_data.roles,
                    lc=contributor_data.lc, 
                    viaf=contributor_data.viaf
                )
                if contributor_data.display_name:
                    contributor.display_name = display_name

        # Make sure the work we just did shows up.
        if edition.work:
            edition.work.calculate_presentation()
        else:
            edition.calculate_presentation()


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
        Identifier.OVERDRIVE_ID : "overdrive id",
        Identifier.THREEM_ID : "3m id",
        Identifier.AXIS_360_ID : "axis 360 id",
        Identifier.ISBN : "isbn"
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
            identifier_field=DEFAULT_IDENTIFIER_FIELD_NAMES,
            subject_field=DEFAULT_SUBJECT_FIELD_NAMES,
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
        self.identifier_field = identifier_field
        self.subject_field = subject_field
        self.sort_author_field = sort_author_field
        self.display_author_field = display_author_field

    def to_metadata(self, dictreader):
        """Turn the CSV file in `dictreader` into a sequence of Metadata.

        :yield: A sequence of Metadata objects.
        """
        fields = dictreader.fieldnames

        # Make sure this CSV file has some way of identifying books.
        found_identifier_field = False
        for field_name in self.identifier_field.values():
            if field_name in fields:
                found_identifier_field = True
                break
        if not found_identifier_field:
            possibilities = ", ".join(self.identifier_field.keys())
            raise CSVFormatError(
                "Could not find a primary identifier field. Possibilities: %s." %
                possibilities
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
        for identifier_type in self.IDENTIFIER_PRECEDENCE:
            field_name = self.identifier_field.get(identifier_type)
            if not field_name:
                continue
            if field_name in row:
                value = self._field(row, field_name)
                if value:
                    identifier = IdentifierData(
                        identifier_type, value
                    )
                    identifiers.append(identifier)
                    if not primary_identifier:
                        primary_identifier = identifier

        subjects = []
        for (field_name, (subject_type, weight)) in self.subject_field.items():
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
            field_names = self.identifier_field.get(identifier_type, [])
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
