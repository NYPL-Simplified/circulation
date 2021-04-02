# encoding: utf-8
from collections import defaultdict
from pdb import set_trace
import datetime
from dateutil.parser import parse
import csv
import os
from sqlalchemy import or_
from sqlalchemy.orm.session import Session

from .opds_import import SimplifiedOPDSLookup
import logging
from .config import Configuration
from .metadata_layer import (
    CSVMetadataImporter,
    ReplacementPolicy,
)
from .model import (
    get_one,
    get_one_or_create,
    Classification,
    CustomList,
    CustomListEntry,
    DataSource,
    Edition,
    Identifier,
    Subject,
    Work,
)
from .util import LanguageCodes

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
        super(CustomListFromCSV, self).__init__(data_source_name, **kwargs)
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

        TODO: Keep track of the list's current members. If any item
        was on the list but is no longer on the list, set its
        last_appeared date to its most recent appearance.
        """
        data_source = DataSource.lookup(_db, self.data_source_name)
        now = datetime.datetime.now(tz=datetime.timezone.utc)

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
        for metadata in self.to_metadata(dictreader):
            entry = self.metadata_to_list_entry(
                custom_list, data_source, now, metadata)

    def metadata_to_list_entry(self, custom_list, data_source, now, metadata):
        """Convert a Metadata object to a CustomListEntry."""
        _db = Session.object_session(data_source)

        title_from_external_list = self.metadata_to_title(now, metadata)
        list_entry, was_new = title_from_external_list.to_custom_list_entry(
            custom_list, self.metadata_client, self.overwrite_old_data)
        e = list_entry.edition

        if not e:
            # We couldn't create an Edition, probably because we
            # couldn't find a useful Identifier.
            self.log.info("Could not create edition for %s", metadata.title)
        else:
            q = _db.query(Work).join(Work.presentation_edition).filter(
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

    def metadata_to_title(self, now, metadata):
        """Convert a Metadata object to a TitleFromExternalList object."""
        row = metadata.csv_row
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
            return ' —' + annotation_extra
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
            most_recent_appearance or datetime.datetime.now(tz=datetime.timezone.utc)
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
                    self.metadata.title, list_entry.first_appearance,
                    self.first_appearance
                )
            list_entry.first_appearance = self.first_appearance

        if (not list_entry.most_recent_appearance
            or list_entry.most_recent_appearance < self.most_recent_appearance):
            if list_entry.most_recent_appearance:
                self.log.info(
                    "I thought %s most recently showed up at %s, but then I saw it later, at %s!",
                    self.metadata.title, list_entry.most_recent_appearance,
                    self.most_recent_appearance
                )
            list_entry.most_recent_appearance = self.most_recent_appearance

        list_entry.annotation = self.annotation

        list_entry.set_work(self.metadata, metadata_client)
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
        try:
            edition, is_new = self.metadata.edition(_db)
        except ValueError as e:
            self.log.info(
                "Ignoring %s, no corresponding edition.", self.metadata.title
            )
            return None
        if overwrite_old_data:
            policy = ReplacementPolicy.from_metadata_source(
                even_if_not_apparently_updated=True
            )
        else:
            policy = ReplacementPolicy.append_only(
                even_if_not_apparently_updated=True
            )
        self.metadata.apply(
            edition=edition,
            collection=None,
            metadata_client=metadata_client,
            replace=policy,
        )
        self.metadata.associate_with_identifiers_based_on_permanent_work_id(_db)
        return edition


class MembershipManager(object):
    """Manage the membership of a custom list based on some criteria."""

    def __init__(self, custom_list, log=None):
        self.log = log or logging.getLogger(
            "Membership manager for %s" % custom_list.name
        )
        self._db = Session.object_session(custom_list)
        self.custom_list = custom_list

    def update(self, update_time=None):
        update_time = update_time or datetime.datetime.now(tz=datetime.timezone.utc)

        # Map each Edition currently in this list to the corresponding
        # CustomListEntry.
        current_membership = defaultdict(list)
        for entry in self.custom_list.entries:
            if not entry.edition:
                continue
            current_membership[entry.edition].append(entry)

        # Find the new membership of the list.
        for new_edition in self.new_membership:
            if new_edition in current_membership:
                # This entry was in the list before, and is still in
                # the list. Update its .most_recent_appearance.
                self.log.debug("Maintaining %s" % new_edition.title)
                entry_list = current_membership[new_edition]
                for entry in entry_list:
                    entry.most_recent_appearance = update_time
                del current_membership[new_edition]
            else:
                # This is a new list entry.
                self.log.debug("Adding %s" % new_edition.title)
                self.custom_list.add_entry(
                    work_or_edition=new_edition, first_appearance=update_time
                )

        # Anything still left in current_membership used to be in the
        # list but is no longer. Remove these entries from the list.
        for entry_list in list(current_membership.values()):
            for entry in entry_list:
                self.log.debug("Deleting %s" % entry.edition.title)
                self._db.delete(entry)

    @property
    def new_membership(self):
        """Iterate over the new membership of the list.

        :yield: a sequence of Edition objects
        """
        raise NotImplementedError()


class ClassificationBasedMembershipManager(MembershipManager):
    """Manage a custom list containing all Editions whose primary
    Identifier is classified under one of the given subject fragments.
    """
    def __init__(self, custom_list, subject_fragments):
        super(ClassificationBasedMembershipManager, self).__init__(custom_list)
        self.subject_fragments = subject_fragments

    @property
    def new_membership(self):
        """Iterate over the new membership of the list.

        :yield: a sequence of Edition objects
        """
        subject_clause = None
        for i in self.subject_fragments:
            c = Subject.identifier.ilike('%' + i + '%')
            if subject_clause is None:
                subject_clause = c
            else:
                subject_clause = or_(subject_clause, c)
        qu = self._db.query(Edition).distinct(Edition.id).join(
            Edition.primary_identifier
        ).join(
            Identifier.classifications
        ).join(
            Classification.subject
        )
        qu = qu.filter(subject_clause)
        return qu

