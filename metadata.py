"""An abstract way of representing incoming metadata and applying it
to Identifiers and Editions.

This acts as an intermediary between the third-party integrations
(which have this information in idiosyncratic formats) and the
model. Doing a third-party integration should be as simple as putting
the information into this format.
"""

from sqlalchemy.orm.session import Session

import datetime
from util import LanguageCodes
from model import (
    CirculationEvent,
    Edition,
    LicensePool,
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

class IdentifierData(object):
    def __init__(self, type, identifier, weight=1):
        self.type = type
        self.identifier = identifier
        self.weight = 1

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
        self.patrons_in_hold_queue = patrons_in_hold_queue
        self.last_checked = last_checked or datetime.datetime.utcnow()

    def update(self, licensepool, licensepool_is_new):
        _db = Session.object_session(licensepool)
        if licensepool_is_new:
            # This is our first time seeing this LicensePool. Log its
            # occurance as a separate event.
            event = get_one_or_create(
                self._db, CirculationEvent,
                type=CirculationEvent.TITLE_ADD,
                license_pool=license_pool,
                create_method_kwargs=dict(
                    start=availability.last_checked,
                    delta=1,
                    end=availability.last_checked,
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


        set_trace()

class Metadata(object):

    """A (potentially partial) set of metadata for a published work."""

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
        self.title = title
        if language:
            language = LanguageCodes.string_to_alpha_three.get(language, None)
        self.language = language
        self.medium = medium
        self.series = series
        self.publisher = publisher
        self.imprint = imprint
        self.issued = issued
        self.published = published

        self.primary_identifier=None
        self.identifiers = identifiers
        self.subjects = subjects
        self.contributors = contributors

    def edition(self, _db):
        return Edition.for_foreign_id(
            _db, self.primary_identifier.type, 
            self.primary_identifier.identifier
        )        

    def license_pool(self, _db):
        return LicensePool.for_foreign_id(
            _db, self.primary_identifier.type, 
            self.primary_identifier.identifier
        )

        license_pool, new_license_pool = LicensePool.for_foreign_id(
            self._db, self.api.source, Identifier.AXIS_360_ID, axis_id)
        edition, new_edition = Edition.for_foreign_id(
            self._db, self.api.source, Identifier.AXIS_360_ID, axis_id)


    def apply(
            self, edition, replace_subjects=False, 
            replace_contributions=False
    ):
        """Apply this metadata to the given edition."""

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
        if self.publishers:
            edition.publisher = self.publisher
        if self.imprint:
            edition.imprint = self.imprint
        if self.issued:
            edition.issued = self.issued
        if self.published:
            edition.published = self.published

        # Create equivalencies between all given identifiers and
        # the edition's primary identifier.
        if self.identifiers is not None:
            for identifier_data in self.identifiers:
                identifier = Identifier.for_foreign_id(
                    _db, identifier_data.type, identifier_data.identifier)
                self.primary_identifier.equivalent_to(
                    self.data_source, identifier, weight)

        if replace_subjects and self.subjects is not None:
            # Remove any old Subjects from this data source -- we're
            # about to add a new set.
            surviving_classifications = []
            dirty = False
            for classification in identifier.classifications:
                if classification.data_source == self.data_source:
                    self._db.delete(classification)
                    dirty = True
                else:
                    surviving_classifications.append(classification)
            if dirty:
                identifier.classifications = surviving_classifications
                __transaction.flush()

        # Apply all specified subjects to the identifier.
        for subject in self.subjects:
            identifier.classify(
                self.data_source, subject.type, subject.identifier, 
                subject.name, weight=subject.weight)

        if replace_contributions:
            dirty = False
            if self.contributions is not None:
                # Remove any old Contributions from this data source --
                # we're about to add a new set
                surviving_contributions = []
                for contribution in edition.contributions:
                    self._db.delete(contribution)
                    dirty = True
                edition.contributions = surviving_contributions
            if dirty:
                __transaction.flush()
            for contributor_data in self.contributors:
                contributor = edition.add_contributor(
                    name=contributor_data.sort_name, contributor_data.roles,
                    lc=contributor_data.lc, viaf=contributor_data.viaf)
                if contributor_data.display_name:
                    contributor.display_name = display_name

        # Make sure the work we just did shows up.
        if edition.work:
            edition.work.calculate_presentation()
        else:
            edition.calculate_presentation()
