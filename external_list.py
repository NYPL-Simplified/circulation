# encoding: utf-8
from nose.tools import set_trace
import datetime
from dateutil.parser import parse
import csv
import os
from sqlalchemy.orm.session import Session

from opds_import import SimplifiedOPDSLookup
import logging
from config import Configuration
from model import (
    get_one,
    get_one_or_create,
    CustomList,
    CustomListEntry,
    Contributor,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Subject,
    Work,
)
from util import LanguageCodes

class TitleFromExternalList(object):

    """This class helps you convert data from external lists into Simplified
    Edition and CustomListEntry objects.
    """

    def __init__(self, data_source_name, title, display_author, 
                 primary_identifier,
                 published_date, first_appearance,
                 most_recent_appearance, publisher, description,
                 language='eng',
                 identifiers=[]):
        self.log = logging.getLogger("Title from external list")
        self.title = title
        self.display_author = display_author
        self.data_source_name = data_source_name
        self.first_appearance = first_appearance or most_recent_appearance
        self.most_recent_appearance = most_recent_appearance
        self.published_date = published_date
        self.identifiers = identifiers
        self.primary_identifier = primary_identifier
        self.publisher = publisher
        self.description = description
        if not self.primary_identifier and self.identifiers:
            self.primary_identifier = self.identifiers[0]
        if (self.primary_identifier 
            and not self.primary_identifier in self.identifiers):
            self.identifiers.append(self.primary_identifier)
        self.language = language

    def _load_identifier(self, _db, t):
        # Turn a 2-tuple (type, identifier) into an Identifier object.
        if t is None:
            return t
        if isinstance(t, Identifier):
            return t
        identifier_type, identifier_identifier = t
        identifier, ignore = Identifier.for_foreign_id(
            _db, identifier_type, identifier_identifier)
        return identifier

    def find_permanent_work_id(self, _db, metadata_client):
        """Try to calculate a permanent work ID for a title
        even though no Edition has been created for the title yet.
        """
        if not self.title or not self.display_author:
            return None
        sort_author = self.find_sort_name(
            _db, self.primary_identifier, self.display_author, metadata_client)
        if not sort_author:
            return None, None
        pwid = Edition.calculate_permanent_work_id_for_title_and_author(
            self.title, sort_author, "book")
        return sort_author, pwid        
        

    def to_edition(self, _db, metadata_client, overwrite_old_data=False):
        """Create or update a Simplified Edition object for this title.
        """
        self.log.info("Converting %s/%s to an Edition object.", 
                      self.title, self.display_author)

        self.primary_identifier = self._load_identifier(
            _db, self.primary_identifier)
        self.log.info("Primary identifier: %r", self.primary_identifier)
        data_source = DataSource.lookup(_db, self.data_source_name)

        edition = None
        sort_author = None
        permanent_work_id = None
        if self.primary_identifier:
            # First thing to do is to try to find an existing edition
            # from this data source, from a previous attempt to import
            # the external list. This will let us skip most of the
            # work.
            edition = get_one(_db, Edition,
                              data_source=data_source,
                              primary_identifier=self.primary_identifier)
            if edition:
                self.log.info("Found existing edition: %r", edition)
                sort_author = edition.sort_author
                permanent_work_id = edition.permanent_work_id
        if not edition:
            # See if we can find a local identifier by doing a lookup
            # by permanent work ID.
            sort_author, permanent_work_id = self.find_permanent_work_id(
                _db, metadata_client
            )

        if not edition and not self.identifiers and not permanent_work_id:
            # There's just no way to associate this item
            # with anything in our collection. Do nothing.
            return None

        if self.identifiers:
            # We were told that this edition has a certain number of
            # identifiers. The creator of the list believes all these
            # identifiers are equivalent, but may be wrong.
            loaded_identifiers = []
            for i in self.identifiers:
                loaded = self._load_identifier(_db, i)
                loaded_identifiers.append(loaded)
                if loaded == self.primary_identifier:
                    continue
                self.primary_identifier.equivalent_to(
                    data_source, loaded, 0.75)
            self.identifiers = loaded_identifiers


        # Try to find additional identifiers--the primary identifiers
        # of other Editions with the same permanent work ID,
        # representing books already in our collection.
        qu = _db.query(Identifier).join(
            Identifier.primarily_identifies).filter(
                Edition.permanent_work_id==permanent_work_id).filter(
                    Identifier.type.in_(
                        [Identifier.THREEM_ID, 
                         Identifier.AXIS_360_ID,
                         Identifier.OVERDRIVE_ID]
                    )
                )
        identifiers_same_work_id = qu.all()
        if self.primary_identifier:
            # We had a primary identifier already, but now we have some
            # more identifiers.
            self.identifiers = list(set(self.identifiers + identifiers_same_work_id))
        elif not edition:
            # The list creator didn't provide a primary identifier, so
            # finding the book already in our collection is pretty much
            # our only hope.
            if identifiers_same_work_id:
                self.primary_identifier = identifiers_same_work_id[0]
                self.identifiers = identifiers_same_work_id
                self.log.info(
                    "No identifier was provided for %s/%s, but calculating a premanent work ID (%s) helped us find books already in our collection: %r",
                    self.title, sort_author, permanent_work_id, identifiers_same_work_id
                )
            else:
                self.log.info(
                    "Calculating permanent work ID (%s) for %s/%s didn't help us find any copies of the book in this collection.", 
                    permanent_work_id, self.title, sort_author
                )

        if self.primary_identifier:
            for same_work_id in identifiers_same_work_id:
                if same_work_id != self.primary_identifier:
                    self.primary_identifier.equivalent_to(
                        data_source, same_work_id, 0.85)
        elif not edition:
            # Without a primary identifier we can't create an Edition.
            return None

        if not edition:
            edition, was_new = Edition.for_foreign_id(
                _db, data_source, self.primary_identifier.type,
                self.primary_identifier.identifier)
        if edition.title != self.title:
            edition.title = self.title
            edition.permanent_work_id = None
        edition.publisher = self.publisher
        edition.medium = Edition.BOOK_MEDIUM
        edition.language = self.language

        if self.published_date:
            edition.published = self.published_date

        if edition.author != self.display_author:
            edition.permanent_work_id = None
            edition.author = self.display_author
        if not edition.sort_author:
            edition.sort_author = self.find_sort_name(
                _db, self.primary_identifier, self.display_author, 
                metadata_client)
        if edition.sort_author:
            edition.calculate_permanent_work_id()

        dirty = False
        if edition.permanent_work_id:
            # As a way of correcting errors, wipe out any previous
            # equivalencies created for this edition's primary
            # identifier by this data source.
            for eq in edition.primary_identifier.equivalencies:
                if eq.data_source == edition.data_source:
                    _db.delete(eq)
                    dirty = True

            # Find other editions with the same permanent work ID and
            # tie their primary identifiers to this list's primary
            # identifier.
            other_editions = _db.query(Edition).filter(
                Edition.permanent_work_id==edition.permanent_work_id).filter(
                    Edition.id != edition.id)
            for other_edition in other_editions:
                if (edition.primary_identifier 
                    != other_edition.primary_identifier):
                    edition.primary_identifier.equivalent_to(
                        data_source,
                        other_edition.primary_identifier, 0.75)
                    

        # Set or update the description.
        if overwrite_old_data:
            for h in self.primary_identifier.links:
                if (h.data_source==edition.data_source
                    and h.rel==Hyperlink.DESCRIPTION):
                    _db.delete(h.resource.representation)
                    _db.delete(h.resource)
                    _db.delete(h)
                    dirty = True

        if self.description:
            description, is_new = self.primary_identifier.add_link(
                Hyperlink.DESCRIPTION, None, data_source, media_type='text/plain', 
                content=self.description)
            dirty = dirty or is_new

        if dirty:
            _db.commit()

        return edition

    def to_custom_list_entry(self, custom_list, metadata_client,
                             overwrite_old_data=False):
        _db = Session.object_session(custom_list)
        edition = self.to_edition(_db, metadata_client, overwrite_old_data)

        list_entry, is_new = get_one_or_create(
            _db, CustomListEntry, edition=edition, customlist=custom_list
        )

        title = self.title.encode("utf8")
        if (not list_entry.first_appearance 
            or list_entry.first_appearance > self.first_appearance):
            if list_entry.first_appearance:
                self.log.info(
                    "I thought %s first showed up at %s, but then I saw it earlier, at %s!",
                    title, list_entry.first_appearance, self.first_appearance
                )
            list_entry.first_appearance = self.first_appearance

        if (not list_entry.most_recent_appearance 
            or list_entry.most_recent_appearance < self.most_recent_appearance):
            if list_entry.most_recent_appearance:
                self.log.info(
                    "I thought %s most recently showed up at %s, but then I saw it later, at %s!",
                    title, list_entry.most_recent_appearance, 
                    self.most_recent_appearance
                )
            list_entry.most_recent_appearance = self.most_recent_appearance
            
        list_entry.annotation = self.description

        return list_entry, is_new

    @classmethod
    def find_sort_name(cls, _db, identifier, display_name, metadata_client):
        """Try as hard as possible to find the canonical name for the
        given author.
        """
        canonical_name = cls.display_name_to_sort_name(
            _db, display_name)
        if canonical_name:
            return canonical_name

        # Time to break out the big guns.
        return cls.display_name_to_sort_name_through_canonicalizer(
            _db, identifier, display_name, metadata_client
        )

    @classmethod
    def display_name_to_sort_name_through_canonicalizer(
            cls, _db, identifier, display_name, metadata_client):
        response = metadata_client.canonicalize_author_name(
            identifier, display_name)
        canonical_name = None
        log = logging.getLogger("Title from external list")
        if (response.status_code == 200 
            and response.headers['Content-Type'].startswith('text/plain')):
            canonical_name = response.content.decode("utf8")
            log.info(
                "Canonicalizer found sort name for %s: %s => %s",
                identifier, 
                display_name,
                canonical_name
            )
        else:
            log.warn(
                "Canonicalizer could not find sort name for %r/%s",
                identifier,
                display_name
            )
        return canonical_name        

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

        # Maybe there's an Edition (e.g. from another list) that has a
        # sort name for this author?
        #
        # TODO: I commented this out because the data was not reliable.
        #
        #editions = _db.query(Edition).filter(
        #    Edition.author==self.display_author).filter(
        #        Edition.sort_author != None).all()
        #if editions:
        #    return editions[0].sort_author

        return None


class CSVFormatError(csv.Error):
    pass

class CustomListFromCSV(object):

    DEFAULT_IDENTIFIER_FIELDS = {
        "overdrive id" : Identifier.OVERDRIVE_ID,
        "3m id" : Identifier.THREEM_ID,
        "axis 360 id" : Identifier.AXIS_360_ID,
    }
    
    DEFAULT_TAG_FIELDS = {
        'tags': Subject.TAG,
    }

    def __init__(self, data_source_name, list_name, metadata_client=None,
                 classification_weight=100,
                 overwrite_old_data=True,
                 first_appearance_field='timestamp',
                 title_field='title',
                 author_field='author',
                 identifier_fields=DEFAULT_IDENTIFIER_FIELDS,
                 language_field='language',
                 publication_date_field='publication year',
                 tag_fields=DEFAULT_TAG_FIELDS,
                 audience_fields=['Age', 'Age range [children]'],
                 annotation_field='text',
                 annotation_author_name_field='name',
                 annotation_author_affiliation_field='location',
                 default_language='eng',
    ):
        self.data_source_name = data_source_name
        self.foreign_identifier = list_name
        self.list_name = list_name
        self.overwrite_old_data = overwrite_old_data
        if not metadata_client:
            metadata_url = Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION,
                required=True
            )
            metadata_client = SimplifiedOPDSLookup(metadata_url)
        self.metadata_client = metadata_client
        self.classification_weight = classification_weight

        # Set the field names
        self.title_field = title_field
        self.author_field = author_field
        self.identifier_fields = identifier_fields
        self.annotation_field=annotation_field
        self.publication_date_field=publication_date_field
        if not tag_fields:
            tag_fields = {}
        elif isinstance(tag_fields, basestring):
            tag_fields = {tag_fields : Subject.TAG}
        self.tag_fields = tag_fields
        if not audience_fields:
            audience_fields = []
        elif isinstance(audience_fields, basestring):
            audience_fields = [audience_fields]
        self.audience_fields=list(audience_fields)
        self.language_field=language_field
        self.default_language=default_language
        self.annotation_author_name_field = annotation_author_name_field
        self.annotation_author_affiliation_field = annotation_author_affiliation_field
        self.first_appearance_field = first_appearance_field


    def to_list(self, x):
        if not x:
            return []
        return [item.strip() for item in x.split(",")]

    def to_customlist(self, _db, dictreader, writer):
        """Turn the CSV file in `dictreader` into a CustomList.

        Outputs an annotated, corrected version of the data.
        """
        data_source = DataSource.lookup(_db, self.data_source_name)
        now = datetime.datetime.utcnow()
        custom_list, was_new = get_one_or_create(
            _db, 
            CustomList,
            data_source=data_source,
            foreign_identifier=self.foreign_identifier,
            create_method_kwargs = dict(
                created=now,
            )
        )
        custom_list.updated = now
        missing_fields = []
        fields = dictreader.fieldnames
        for i in (self.title_field, self.author_field):
            if i not in fields:
                missing_fields.append(i)

        if missing_fields:
            raise CSVFormatError("Could not find required field(s): %s." %
                                 ", ".join(missing_fields))

        found_identifier_field = False
        identifier_field_names = sorted(self.identifier_fields.keys())
        for identifier_field_name in identifier_field_names:
            if identifier_field_name in fields:
                found_identifier_field = True

        if not found_identifier_field:
            raise CSVFormatError(
                "Could not find any of the required identifier fields: %s." %
                ", ".join(identifier_field_names)
            )


        header_row = [
            "Title", "Author", "Primary Identifier", "All Identifiers",
            "Annotation", "Internal work ID", "Sort author", 
            "Import status",
        ]
        writer.writerow([])
        for row in dictreader:
            status, warnings, list_item = self.row_to_list_item(
                custom_list, data_source, now, row)
            status = warnings + [status]
            if list_item:
                e = list_item.edition
                new_row = [
                    e.title, e.author, e.primary_identifier.identifier,
                    ", ".join(repr(x) for x in e.equivalent_identifiers()),
                    list_item.annotation, e.permanent_work_id, e.sort_author,
                    "\n".join(status)
                ]
            else:
                new_row = [
                    row.get(self.title_field), row.get(self.author_field),
                    "", "", "", "", "", 
                    "\n".join(status),
                ]
                pass
            writer.writerow([self._out(x) for x in new_row])

    def row_to_list_item(self, custom_list, data_source, now, row):
        """Import a row of a CSV file to an item in the given CustomList."""
        _db = Session.object_session(data_source)
        title, warnings = self.row_to_title(
            data_source, now, row)

        list_item, was_new = title.to_custom_list_entry(
            custom_list, self.metadata_client, self.overwrite_old_data)
        e = list_item.edition

        if not e:
            # We couldn't create an Edition, probably because we
            # couldn't find a useful Identifier.
            return "Could not create edition", [], None
        q = _db.query(Work).join(Work.primary_edition).filter(
            Edition.permanent_work_id==e.permanent_work_id)
        if q.count() > 0:
            status = "Found matching work in collection."
        else:
            status = "No matching work found."


        # Set or update classifications.
        identifier = e.primary_identifier
        if self.overwrite_old_data:
            for cl in identifier.classifications:
                if cl.data_source==e.data_source:
                    _db.delete(cl)
            identifier.classifications = []
            _db.commit()

        for f, schema in self.tag_fields.items():
            tags = self.to_list(self._field(row, f, ""))
            for tag in tags:
                identifier.classify(
                    e.data_source, schema, tag,
                    self.classification_weight)

        for f in self.audience_fields:
            audiences = self.to_list(self._field(row, f))
            for audience in audiences:
                identifier.classify(
                    e.data_source, Subject.FREEFORM_AUDIENCE, 
                    audience, self.classification_weight)
        return status, warnings, list_item

    def _out(self, x):
        if x is None:
            return ''
        if isinstance(x, unicode):
            return x.encode('utf8')
        elif isinstance(x, basestring):
            return x
        else:
            return repr(x)

    def _field(self, row, name, default=None):
        "Get a value from a field and ensure it comes in as Unicode."
        value = row.get(name, default)
        if isinstance(value, basestring):
            return value.decode("utf8")
        return value

    def annotation_citation(self, row):
        annotation_author = self._field(row, self.annotation_author_name_field)
        annotation_author_affiliation = self._field(
            row, self.annotation_author_affiliation_field)
        if annotation_author_affiliation == annotation_author:
            annotation_author_affiliation = None
        annotation_extra = ''
        if annotation_author:
            annotation_extra = annotation_author
            if annotation_author_affiliation:
                annotation_extra += ', ' + annotation_author_affiliation
        if annotation_extra:
            return u' —' + annotation_extra
        return None

    def row_to_title(self, data_source, now, row):
        warnings = []
        t = self._field(row, self.title_field)
        author = self._field(row, self.author_field)
        annotation = self._field(row, self.annotation_field)
        identifiers = []

        for field_name, identifier_type in sorted(
                self.identifier_fields.items()):
            identifier = self._field(row, field_name)
            if identifier:
                identifiers.append((identifier_type, identifier))

        annotation_citation = self.annotation_citation(row)
        if annotation_citation:
            annotation = annotation + annotation_citation

        published_date = None
        if self.publication_date_field in row:
            raw = row[self.publication_date_field]
            try:
                published_date = parse(raw)
            except ValueError:
                warnings.append('Could not parse publication date "%s"' % raw)
        first_appearance = now
        if self.first_appearance_field in row:
            raw = row[self.first_appearance_field]
            try:
                first_appearance = parse(raw)
            except ValueError:
                warnings.append('Could not parse first appearance "%s"' % raw)
                
        language = self.default_language
        if self.language_field in row:
            language = self._field(row, self.language_field)
            language = LanguageCodes.string_to_alpha_3(language)
        title = TitleFromExternalList(
            data_source_name=data_source.name,
            title=t,
            display_author=author,
            primary_identifier=None,
            published_date=published_date,
            first_appearance=first_appearance,
            most_recent_appearance=now,
            publisher=None,
            description=annotation, 
            language=language,
            identifiers=identifiers,
        )
        return title, warnings
