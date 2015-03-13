from sqlalchemy.orm.session import Session

from model import (
    get_one_or_create,
    CustomListEntry,
    Contributor,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
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
            a = u"Trying to canonicalize %s, %s" % (
                self.primary_identifier.identifier, self.display_author)
            print a.encode("utf8")
            if (response.status_code == 200 
                and response.headers['Content-Type'].startswith('text/plain')):
                edition.sort_author = response.content.decode("utf8")
                print "CANONICALIZER TO THE RESCUE: %s" % edition.sort_author
                edition.calculate_permanent_work_id()
            else:
                print "CANONICALIZER FAILED ME."


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
        editions = _db.query(Edition).filter(
            Edition.author==self.display_author).filter(
                Edition.sort_author != None).all()
        if editions:
            return editions[0].author

        return None

