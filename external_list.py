# encoding: utf-8
from nose.tools import set_trace
import datetime
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
    Work,
)

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

    def to_edition(self, _db, metadata_client):
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
        if self.description:
            description, is_new = self.primary_identifier.add_link(
                Hyperlink.DESCRIPTION, None, data_source)
            description.resource.set_fetched_content(
                "text/plain", self.description or '', None)

        return edition

    def to_custom_list_entry(self, custom_list, metadata_client):
        _db = Session.object_session(custom_list)        
        edition = self.to_edition(_db, metadata_client)

        list_entry, is_new = get_one_or_create(
            _db, CustomListEntry, edition=edition, customlist=custom_list
        )

        if (not list_entry.first_appearance 
            or list_entry.first_appearance > self.first_appearance):
            if list_entry.first_appearance:
                print "I thought %s first showed up at %s, but then I saw it earlier, at %s!" % (self.title, list_entry.first_appearance, self.first_appearance)
            list_entry.first_appearance = self.first_appearance

        if (not list_entry.most_recent_appearance 
            or list_entry.most_recent_appearance < self.most_recent_appearance):
            if list_entry.most_recent_appearance:
                print "I thought %s most recently showed up at %s, but then I saw it later, at %s!" % (self.title, list_entry.most_recent_appearance, self.most_recent_appearance)
            list_entry.most_recent_appearance = self.most_recent_appearance
            
        list_entry.annotation = self.description

        return list_entry, is_new

    def find_sort_name(self, _db):
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
            Contributor.display_name==self.display_author).filter(
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

class CustomListFromCSV(object):

    ANNOTATION_FIELD = 'Annotation'
    PUBLICATION_YEAR_FIELD = 'Publication Year'
    TAG_FIELDS = ['Genre / Collection area']
    AUDIENCE_FIELDS = ['Age', 'Age range [children]']

    def __init__(self, data_source_name, list_name, metadata_client=None,
                 classification_weight=100):
        self.data_source_name = data_source_name
        self.foreign_identifier = list_name
        self.reader = reader
        self.list_name = list_name
        if not metadata_client:
            metadata_url = os.environ['METADATA_WEB_APP_URL']
            metadata_client = SimplifiedOPDSLookup(metadata_url)
        self.metadata_client = metadata_client
        self.classification_weight = classification_weight

    def to_list(self, x):
        return [item.strip() for item in ",".split(x)]

    def to_customlist(self, _db, dictreader, writer):
        """Turn the CSV file in `dictreader` into a CustomList.

        Outputs an annotated, corrected version of the data.
        """
        data_source = DataSource.lookup(_db, self.data_source_name)
        now = datetime.datetime.utcnow()
        l, was_new = get_one_or_create(
            _db, 
            CustomList,
            data_source=data_source,
            foreign_identifier=self.foreign_identifier,
            create_method_kwargs = dict(
                created=now,
            )
        )
        l.updated = now
        missing_fields = []
        fields = dictreader.fieldnames
        for i in ('Title', 'Author', 'ISBN'):
            if i not in fields:
                missing_fields.append(i)

        if missing_fields:
            raise ValueError("Could not find required field(s): %s." %
                             ", ".join(missing_fields))
        writer.writerow(["Title", "Author", "ISBN", "Annotation", "Work ID", "Sort author", "Import status"])
        for row in dictreader:
            list_item, was_new = self.to_custom_list_entry(
                l, now, row)
            e = list_item.edition

            q = _db.query(Work).join(Work.primary_edition).filter(
                Edition.permanent_work_id==e.permanent_work_id)
            if q.count() > 0:
                status = "Found matching work in collection."
            else:
                status = "No matching work found."

            new_row = [e.title, e.author, e.primary_identifier.identifier,
                       list_item.annotation, e.permanent_work_id, e.sort_author,
                       status]

            identifier = e.primary_identifier
            for f in self.TAG_FIELDS:
                tags = self.to_list(self.row.get(f, ""))
                for tag in tags:
                    identifier.classify(
                        e.data_source, Subject.TAG, tag,
                        self.classification_weight)

            for f in self.AUDIENCE_FIELDS:
                audiences = self.to_list(self.row.get(f))
                for audience in audiences:
                    tag.classify(e.data_source, Subject.FREEFORM_AUDIENCE, 
                                 audience, self.classification_weight)

            if status == "Found matching work in collection.":
                print new_row
            writer.writerow([(x or u'').encode("utf8") for x in new_row])

    def _v(self, x):
        if isinstance(x,basestring):
            return x.decode("utf8")
        return x

    def annotation_extra(self, row):
        annotation_author = self._v(row.get('Name'))
        annotation_author_location = self._v(row.get('Location'))
        if annotation_author_location == annotation_author:
            annotation_author_location = None
        annotation_extra = ''
        if annotation_author:
            annotation_extra = annotation_author
            if annotation_author_location:
                annotation_extra += ', %s' % annotation_author_location
        if annotation_extra:
            return u' —' + annotation_extra
        return None

    def to_custom_list_entry(self, custom_list, now, row):
        t = self._v(row['Title'])
        author = self._v(row['Author'])
        isbn = row['ISBN']
        annotation = self._v(row.get(self.ANNOTATION_FIELD, None))

        annotation_extra = self.annotation_extra(row)
        if annotation_extra:
            annotation = annotation + annotation_extra

        if self.PUBLICATION_YEAR_FIELD in row:
            raw = row[self.PUBLICATION_YEAR_FIELD]
            try:
                published_date = datetime.datetime.strptime(raw, "%Y")
            except ValueError:
                print "WARNING: Could not parse publication year %s" % raw
                published_date = None
        else:
            published_date = None
        if 'Timestamp' in row:
            raw = row['Timestamp']
            first_appearance = datetime.datetime.strptime(raw, "%m/%d/%Y %H:%M:%S")
        else:
            first_appearance = now
        if 'Language' in row:
            # TODO: Try to turn into 3-letter format.
            language = _f(row['Language'])
        else:
            language = 'eng'
        title = TitleFromExternalList(
            custom_list.data_source.name, t, author, isbn, published_date,
            first_appearance, now, None, annotation, language)
        return title.to_custom_list_entry(custom_list, self.metadata_client)

# if __name__ == '__main__':
#     from model import production_session
#     _db = production_session()
#     reader = csv.DictReader(sys.stdin)
#     l = CustomListFromCSV(DataSource.LIBRARIANS, "Staff picks")
#     writer = csv.writer(sys.stdout)
#     l.to_customlist(_db, reader, writer)
