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
from metadata import CSVMetadataImporter
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

class CustomListFromCSV(CSVMetadataImporter):
    """Create a CustomList, with entries, from a CSV file."""

    def __init__(self, data_source_name, list_name, metadata_client=None,
                 overwrite_old_data=False,
                 annotation_field='text',
                 annotation_author_name_field='name',
                 annotation_author_affiliation_field='location',
                 first_appearance_field='timestamp',
                 **kwargs
             ):
        super(CustomListFromCSV, self).__init__(**kwargs)
        self.data_source_name = data_source_name
        self.foreign_identifier = list_name
        self.list_name = list_name
        self.overwrite_old_data=overwrite_old_data

        if not metadata_client:
            metadata_url = Configuration.integration_url(
                Configuration.METADATA_WRANGLER_INTEGRATION,
                required=True
            )
            metadata_client = SimplifiedOPDSLookup(metadata_url)
        self.metadata_client = metadata_client

        self.annotation_field = annotation_field
        self.annotation_author_name_field = annotation_author_name_field
        self.annotation_author_affiliation_field = annotation_author_affiliation_field
        self.first_appearance_field = first_appearance_field

    def to_customlist(self, _db, dictreader):
        """Turn the CSV file in `dictreader` into a CustomList.
        """
        data_source = DataSource.lookup(_db, self.data_source_name)
        now = datetime.datetime.utcnow()

        # Find or create the CustomList object itself.
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

        # Turn the rows of the CSV file into a sequence of Metadata
        # objects, then turn each Metadata into a CustomListEntry object.
        for metadata in self.to_metadata(_db, dictreader):
            self.metadata_to_list_entry(
                custom_list, data_source, now, metadata)

    def metadata_to_list_entry(self, custom_list, data_source, now, row):
        """Convert a Metadata object to a CustomListEntry."""
        _db = Session.object_session(data_source)
        metadata = self.row_to_metadata(row)

        title_from_external_list = self.row_to_title(now, row)
        list_entry, was_new = title_from_external_list.to_custom_list_entry(
            custom_list, self.metadata_client, self.overwrite_old_data)
        e = list_entry.edition

        if not e:
            # We couldn't create an Edition, probably because we
            # couldn't find a useful Identifier.
            self.log.info("Could not create edition for %s", metadata.title)
        else:
            q = _db.query(Work).join(Work.primary_edition).filter(
                Edition.permanent_work_id==e.permanent_work_id)
            if q.count() > 0:
                self.log.info("Found matching work in collection for %s", 
                              metadata.title
                )
            else:
                self.log.info("No matching work found for %s", 
                              metadata.title
                )
        return list_entry

    def row_to_title(self, now, row):
        """Convert a row of the CSV file to a TitleFromExternalList object."""
        metadata = self.row_to_metadata(row)
        first_appearance = self._date_field(row, self.first_appearance_field)
        annotation = self._field(row, self.annotation_field)
        annotation_citation = self.annotation_citation(row)
        if annotation_citation:
            annotation = annotation + annotation_citation

        return TitleFromExternalList(
            metadata=metadata,
            first_appearance=first_appearance,
            most_recent_appearance=now,
            annotation=annotation
        )

    def annotation_citation(self, row):
        """Extract a citation for an annotation from a row of a CSV file."""
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


class TitleFromExternalList(object):

    """This class helps you convert data from external lists into Simplified
    Edition and CustomListEntry objects.
    """

    def __init__(self, metadata, first_appearance, most_recent_appearance, 
                 annotation):
        self.log = logging.getLogger("Title from external list")
        self.metadata = metadata
        self.first_appearance = first_appearance or most_recent_appearance
        self.most_recent_appearance = (
            most_recent_appearance or datetime.datetime.now()
        )
        self.annotation = annotation

    def to_custom_list_entry(self, custom_list, metadata_client,
                             overwrite_old_data=False):
        """Turn this object into a CustomListEntry with associated Edition."""
        _db = Session.object_session(custom_list)
        edition = self.to_edition(_db, metadata_client, overwrite_old_data)

        list_entry, is_new = get_one_or_create(
            _db, CustomListEntry, edition=edition, customlist=custom_list
        )

        if (not list_entry.first_appearance 
            or list_entry.first_appearance > self.first_appearance):
            if list_entry.first_appearance:
                self.log.info(
                    "I thought %s first showed up at %s, but then I saw it earlier, at %s!",
                    self.title, list_entry.first_appearance, 
                    self.first_appearance
                )
            list_entry.first_appearance = self.first_appearance

        if (not list_entry.most_recent_appearance 
            or list_entry.most_recent_appearance < self.most_recent_appearance):
            if list_entry.most_recent_appearance:
                self.log.info(
                    "I thought %s most recently showed up at %s, but then I saw it later, at %s!",
                    self.title, list_entry.most_recent_appearance, 
                    self.most_recent_appearance
                )
            list_entry.most_recent_appearance = self.most_recent_appearance
            
        list_entry.annotation = self.annotation

        return list_entry, is_new

    def to_edition(self, _db, metadata_client, overwrite_old_data=False):
        """Create or update an Edition object for this list item.

        We have two goals here:

        1. Make sure there is an Edition representing the list's view
        of the data.

        2. If at all possible, connect the Edition's primary
        identifier to other identifiers in the system, identifiers
        which may have associated LicensePools. This can happen in two
        ways:

        2a. The Edition's primary identifier, or other identifiers
        associated with the Edition, may be directly associated with
        LicensePools. This can happen if a book's list entry includes
        (e.g.) an Overdrive ID.

        2b. The Edition's permanent work ID may identify it as the
        same work as other Editions in the system. In that case this
        Edition's primary identifier may be associated with the other
        Editions' primary identifiers. (p=0.85)
        """
        self.log.info("Converting %s to an Edition object.", 
                      self.metadata.title)

        # Make sure the Metadata object's view of the book is present
        # as an Edition. This will also associate all its identifiers
        # with its primary identifier, and calculate the permanent work
        # ID if possible.
        edition, is_new = self.metadata.edition(_db)
        self.metadata.apply(
            edition=edition, 
            metadata_client=metdata_client,
            replace_identifiers=overwrite_old_data,
            replace_subjects=overwrite_old_data, 
            replace_contributions=overwrite_old_data
        )

        self.metadata.associate_with_identifiers_based_on_permanent_work_id(_db)
        return edition
