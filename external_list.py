# encoding: utf-8
from nose.tools import set_trace
import datetime
from dateutil.parser import parse
import csv
import os
from sqlalchemy.orm.session import Session

from opds_import import SimplifiedOPDSLookup
from model import (
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

    def __init__(self, data_source_name, title, display_author, primary_isbn,
                 published_date, first_appearance,
                 most_recent_appearance, publisher, description,
                 language='eng',
                 isbns=[]):
        self.title = title
        self.display_author = display_author
        self.data_source_name = data_source_name
        self.first_appearance = first_appearance
        self.most_recent_appearance = most_recent_appearance
        self.published_date = published_date
        self.isbns = isbns
        self.primary_isbn = primary_isbn
        self.publisher = publisher
        self.description = description
        if not self.primary_isbn in self.isbns:
            self.isbns.append(self.primary_isbn)
        self.language = language

        if not self.primary_isbn:
            raise ValueError("Book has no identifier")

    def to_edition(self, _db, metadata_client, overwrite_old_data=False):
        """Create or update a Simplified Edition object for this title.
        """
        identifier = self.primary_isbn
        if not identifier:
            return None
        self.primary_identifier, ignore = Identifier.from_asin(_db, identifier)

        data_source = DataSource.lookup(_db, self.data_source_name)
        edition, was_new = Edition.for_foreign_id(
            _db, data_source, self.primary_identifier.type,
            self.primary_identifier.identifier)

        if edition.title != self.title:
            edition.title = self.title
            edition.permanent_work_id = None
        edition.publisher = self.publisher
        edition.medium = Edition.BOOK_MEDIUM
        edition.language = self.language

        for i in self.isbns:
            if i == identifier:
                # We already did this one.
                continue
            other_identifier, ignore = Identifier.from_asin(_db, i)
            edition.primary_identifier.equivalent_to(
                data_source, other_identifier, 1)

        if self.published_date:
            edition.published = self.published_date

        if edition.author != self.display_author:
            edition.permanent_work_id = None
            edition.author = self.display_author
        if not edition.sort_author:
            edition.sort_author = self.find_sort_name(_db)
            if edition.sort_author:
                "IT WAS EASY TO FIND %s!" % edition.sort_author
        # If find_sort_name returned a sort_name, we can calculate a
        # permanent work ID for this Edition, and be done with it.
        #
        # Otherwise, we'll have to ask the metadata wrangler to find
        # the canonicalized author name for this book.
        if edition.sort_author:
            edition.calculate_permanent_work_id()
        else:
            response = metadata_client.canonicalize_author_name(
                self.primary_identifier, self.display_author)
            #a = u"Trying to canonicalize %s, %s" % (
            #    self.primary_identifier.identifier, self.display_author)
            #print a.encode("utf8")
            if (response.status_code == 200 
                and response.headers['Content-Type'].startswith('text/plain')):
                edition.sort_author = response.content.decode("utf8")
                # print "CANONICALIZER TO THE RESCUE: %s" % edition.sort_author
                edition.calculate_permanent_work_id()
            else:
                # print "CANONICALIZER FAILED ME."
                pass


        # Set or update the description.
        if overwrite_old_data:
            for h in self.primary_identifier.links:
                if (h.data_source==edition.data_source
                    and h.rel==Hyperlink.DESCRIPTION):
                    _db.delete(h.resource.representation)
                    _db.delete(h.resource)
                    _db.delete(h)

            _db.commit()

        if self.description:
            description, is_new = self.primary_identifier.add_link(
                Hyperlink.DESCRIPTION, None, data_source, media_type='text/plain', 
                content=self.description)

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
                print "I thought %s first showed up at %s, but then I saw it earlier, at %s!" % (title, list_entry.first_appearance, self.first_appearance)
            list_entry.first_appearance = self.first_appearance

        if (not list_entry.most_recent_appearance 
            or list_entry.most_recent_appearance < self.most_recent_appearance):
            if list_entry.most_recent_appearance:
                print "I thought %s most recently showed up at %s, but then I saw it later, at %s!" % (title, list_entry.most_recent_appearance, self.most_recent_appearance)
            list_entry.most_recent_appearance = self.most_recent_appearance
            
        list_entry.annotation = self.description

        return list_entry, is_new

    def find_sort_name(self, _db):
        return self.display_name_to_sort_name(_db, self.display_author)

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

    def __init__(self, data_source_name, list_name, metadata_client=None,
                 classification_weight=100,
                 overwrite_old_data=True,
                 first_appearance_field='Timestamp',
                 title_field='Title',
                 author_field='Author',
                 isbn_field='ISBN',
                 language_field='Language',
                 publication_date_field='Publication Year',
                 tag_fields=['Genre / Collection area'],
                 audience_fields=['Age', 'Age range [children]'],
                 annotation_field='Annotation',
                 annotation_author_name_field='Name',
                 annotation_author_affiliation_field='Location',
                 default_language='eng',
    ):
        self.data_source_name = data_source_name
        self.foreign_identifier = list_name
        self.list_name = list_name
        self.overwrite_old_data = overwrite_old_data
        if not metadata_client:
            metadata_url = os.environ['METADATA_WEB_APP_URL']
            metadata_client = SimplifiedOPDSLookup(metadata_url)
        self.metadata_client = metadata_client
        self.classification_weight = classification_weight

        # Set the field names
        self.title_field = title_field
        self.author_field = author_field
        self.isbn_field = isbn_field
        self.annotation_field=annotation_field
        self.publication_date_field=publication_date_field
        if not tag_fields:
            tag_fields = []
        elif isinstance(tag_fields, basestring):
            tag_fields = [tag_fields]
        self.tag_fields=list(tag_fields)
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
        for i in (self.title_field, self.author_field, self.isbn_field):
            if i not in fields:
                missing_fields.append(i)

        if missing_fields:
            raise CSVFormatError("Could not find required field(s): %s." %
                                 ", ".join(missing_fields))
        writer.writerow(["Title", "Author", "ISBN", "Annotation", "Internal work ID", "Sort author", "Import status"])
        for row in dictreader:
            status, warnings, list_item = self.row_to_list_item(
                custom_list, data_source, now, row)
            e = list_item.edition

            status = warnings + [status]

            new_row = [e.title, e.author, e.primary_identifier.identifier,
                       list_item.annotation, e.permanent_work_id, e.sort_author,
                       "\n".join(status)]
            writer.writerow([self._out(x) for x in new_row])

    def row_to_list_item(self, custom_list, data_source, now, row):
        """Import a row of a CSV file to an item in the given CustomList."""
        _db = Session.object_session(data_source)
        title, warnings = self.row_to_title(
            data_source, now, row)

        list_item, was_new = title.to_custom_list_entry(
            custom_list, self.metadata_client, self.overwrite_old_data)
        e = list_item.edition

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

        for f in self.tag_fields:
            tags = self.to_list(self._field(row, f, ""))
            for tag in tags:
                identifier.classify(
                    e.data_source, Subject.TAG, tag,
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
        return x

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
        isbn = self._field(row, self.isbn_field)
        annotation = self._field(row, self.annotation_field)

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
            data_source.name, t, author, isbn, published_date,
            first_appearance, now, None, annotation, language)
        return title, warnings
