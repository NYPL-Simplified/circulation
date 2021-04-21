import flask
from flask import Response
from flask_babel import lazy_gettext as _
from . import AdminCirculationManagerController
from collections import Counter
from core.opds import AcquisitionFeed
from api.admin.opds import AdminAnnotator, AdminFeed
from api.admin.problem_details import *
from api.config import (
    Configuration,
    CannotLoadConfiguration
)
from api.metadata_wrangler import MetadataWranglerCollectionRegistrar
from api.admin.validator import Validator
from core.app_server import (
    load_pagination_from_request,
)
from core.classifier import (
    genres,
    SimplifiedGenreClassifier,
    NO_NUMBER,
    NO_VALUE
)
from core.mirror import MirrorUploader
from core.util.problem_detail import ProblemDetail
from core.util import LanguageCodes
from core.metadata_layer import (
    Metadata,
    LinkData,
    ReplacementPolicy,
)
from core.lane import (Lane, WorkList)
from core.model import (
    create,
    get_one,
    get_one_or_create,
    Classification,
    Collection,
    Complaint,
    Contributor,
    CustomList,
    DataSource,
    Edition,
    Genre,
    Hyperlink,
    Measurement,
    PresentationCalculationPolicy,
    Representation,
    RightsStatus,
    Subject,
    Work
)
from core.model.configuration import ExternalIntegrationLink
from core.util.datetime_helpers import (
    strptime_utc,
    utc_now,
)
import base64
import json
import os
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import textwrap
import urllib.request, urllib.parse, urllib.error

class WorkController(AdminCirculationManagerController):

    STAFF_WEIGHT = 1000

    def details(self, identifier_type, identifier):
        """Return an OPDS entry with detailed information for admins.

        This includes relevant links for editing the book.

        :return: An OPDSEntryResponse
        """
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        annotator = AdminAnnotator(self.circulation, flask.request.library)

        # single_entry returns an OPDSEntryResponse that will not be
        # cached, which is perfect. We want the admin interface
        # to update immediately when an admin makes a change.
        return AcquisitionFeed.single_entry(self._db, work, annotator)

    def complaints(self, identifier_type, identifier):
        """Return detailed complaint information for admins."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        counter = self._count_complaints_for_work(work)
        response = dict({
            "book": {
                "identifier_type": identifier_type,
                "identifier": identifier
            },
            "complaints": counter
        })

        return response

    def roles(self):
        """Return a mapping from MARC codes to contributor roles."""
        # TODO: The admin interface only allows a subset of the roles
        # listed in model.py since it uses the OPDS representation of
        # the data, and some of the roles map to the same MARC code.
        CODES = Contributor.MARC_ROLE_CODES
        marc_to_role = dict()
        for role in [
            Contributor.ACTOR_ROLE,
            Contributor.ADAPTER_ROLE,
            Contributor.AFTERWORD_ROLE,
            Contributor.ARTIST_ROLE,
            Contributor.ASSOCIATED_ROLE,
            Contributor.AUTHOR_ROLE,
            Contributor.COMPILER_ROLE,
            Contributor.COMPOSER_ROLE,
            Contributor.CONTRIBUTOR_ROLE,
            Contributor.COPYRIGHT_HOLDER_ROLE,
            Contributor.DESIGNER_ROLE,
            Contributor.DIRECTOR_ROLE,
            Contributor.EDITOR_ROLE,
            Contributor.ENGINEER_ROLE,
            Contributor.FOREWORD_ROLE,
            Contributor.ILLUSTRATOR_ROLE,
            Contributor.INTRODUCTION_ROLE,
            Contributor.LYRICIST_ROLE,
            Contributor.MUSICIAN_ROLE,
            Contributor.NARRATOR_ROLE,
            Contributor.PERFORMER_ROLE,
            Contributor.PHOTOGRAPHER_ROLE,
            Contributor.PRODUCER_ROLE,
            Contributor.TRANSCRIBER_ROLE,
            Contributor.TRANSLATOR_ROLE,
            ]:
            marc_to_role[CODES[role]] = role
        return marc_to_role

    def languages(self):
        """Return the supported language codes and their English names."""
        return LanguageCodes.english_names

    def media(self):
        """Return the supported media types for a work and their schema.org values."""
        return Edition.additional_type_to_medium

    def rights_status(self):
        """Return the supported rights status values with their names and whether
        they are open access."""
        return {uri: dict(name=name,
                          open_access=(uri in RightsStatus.OPEN_ACCESS),
                          allows_derivatives=(uri in RightsStatus.ALLOWS_DERIVATIVES))
                for uri, name in list(RightsStatus.NAMES.items())}

    def edit(self, identifier_type, identifier):
        """Edit a work's metadata."""
        self.require_librarian(flask.request.library)

        # TODO: It would be nice to use the metadata layer for this, but
        # this code handles empty values differently than other metadata
        # sources. When a staff member deletes a value, that indicates
        # they think it should be empty. This needs to be indicated in the
        # db so that it can overrule other data sources that set a value,
        # unlike other sources which set empty fields to None.

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        changed = False

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        primary_identifier = work.presentation_edition.primary_identifier
        staff_edition, is_new = get_one_or_create(
            self._db, Edition,
            primary_identifier_id=primary_identifier.id,
            data_source_id=staff_data_source.id
        )
        self._db.expire(primary_identifier)

        new_title = flask.request.form.get("title")
        if new_title and work.title != new_title:
            staff_edition.title = str(new_title)
            changed = True

        new_subtitle = flask.request.form.get("subtitle")
        if work.subtitle != new_subtitle:
            if work.subtitle and not new_subtitle:
                new_subtitle = NO_VALUE
            staff_edition.subtitle = str(new_subtitle)
            changed = True

        # The form data includes roles and names for contributors in the same order.
        new_contributor_roles = flask.request.form.getlist("contributor-role")
        new_contributor_names = [str(n) for n in flask.request.form.getlist("contributor-name")]
        # The first author in the form is considered the primary author, even
        # though there's no separate MARC code for that.
        for i, role in enumerate(new_contributor_roles):
            if role == Contributor.AUTHOR_ROLE:
                new_contributor_roles[i] = Contributor.PRIMARY_AUTHOR_ROLE
                break
        roles_and_names = list(zip(new_contributor_roles, new_contributor_names))

        # Remove any contributions that weren't in the form, and remove contributions
        # that already exist from the list so they won't be added again.
        deleted_contributions = False
        for contribution in staff_edition.contributions:
            if (contribution.role, contribution.contributor.display_name) not in roles_and_names:
                self._db.delete(contribution)
                deleted_contributions = True
                changed = True
            else:
                roles_and_names.remove((contribution.role, contribution.contributor.display_name))
        if deleted_contributions:
            # Ensure the staff edition's contributions are up-to-date when
            # calculating the presentation edition later.
            self._db.refresh(staff_edition)

        # Any remaining roles and names are new contributions.
        for role, name in roles_and_names:
            # There may be one extra role at the end from the input for
            # adding a contributor, in which case it will have no
            # corresponding name and can be ignored.
            if name:
                if role not in list(Contributor.MARC_ROLE_CODES.keys()):
                    self._db.rollback()
                    return UNKNOWN_ROLE.detailed(
                        _("Role %(role)s is not one of the known contributor roles.",
                          role=role))
                contributor = staff_edition.add_contributor(name=name, roles=[role])
                contributor.display_name = name
                changed = True

        new_series = flask.request.form.get("series")
        if work.series != new_series:
            if work.series and not new_series:
                new_series = NO_VALUE
            staff_edition.series = str(new_series)
            changed = True

        new_series_position = flask.request.form.get("series_position")
        if new_series_position != None and new_series_position != '':
            try:
                new_series_position = int(new_series_position)
            except ValueError:
                self._db.rollback()
                return INVALID_SERIES_POSITION
        else:
            new_series_position = None
        if work.series_position != new_series_position:
            if work.series_position and new_series_position == None:
                new_series_position = NO_NUMBER
            staff_edition.series_position = new_series_position
            changed = True

        new_medium = flask.request.form.get("medium")
        if new_medium:
            if new_medium not in list(Edition.medium_to_additional_type.keys()):
                self._db.rollback()
                return UNKNOWN_MEDIUM.detailed(
                    _("Medium %(medium)s is not one of the known media.",
                      medium=new_medium))
            staff_edition.medium = new_medium
            changed = True

        new_language = flask.request.form.get("language")
        if new_language != None and new_language != '':
            new_language = LanguageCodes.string_to_alpha_3(new_language)
            if not new_language:
                self._db.rollback()
                return UNKNOWN_LANGUAGE
        else:
            new_language = None
        if new_language != staff_edition.language:
            staff_edition.language = new_language
            changed = True

        new_publisher = flask.request.form.get("publisher")
        if new_publisher != staff_edition.publisher:
            if staff_edition.publisher and not new_publisher:
                new_publisher = NO_VALUE
            staff_edition.publisher = str(new_publisher)
            changed = True

        new_imprint = flask.request.form.get("imprint")
        if new_imprint != staff_edition.imprint:
            if staff_edition.imprint and not new_imprint:
                new_imprint = NO_VALUE
            staff_edition.imprint = str(new_imprint)
            changed = True

        new_issued = flask.request.form.get("issued")
        if new_issued != None and new_issued != '':
            try:
                new_issued = strptime_utc(new_issued, '%Y-%m-%d')
            except ValueError:
                self._db.rollback()
                return INVALID_DATE_FORMAT
        else:
            new_issued = None
        if new_issued != staff_edition.issued:
            staff_edition.issued = new_issued
            changed = True

        # TODO: This lets library staff add a 1-5 rating, which is used in the
        # quality calculation. However, this doesn't work well if there are any
        # other measurements that contribute to the quality. The form will show
        # the calculated quality rather than the staff rating, which will be
        # confusing. It might also be useful to make it more clear how this
        # relates to the quality threshold in the library settings.
        changed_rating = False
        new_rating = flask.request.form.get("rating")
        if new_rating != None and new_rating != '':
            try:
                new_rating = float(new_rating)
            except ValueError:
                self._db.rollback()
                return INVALID_RATING
            scale = Measurement.RATING_SCALES[DataSource.LIBRARY_STAFF]
            if new_rating < scale[0] or new_rating > scale[1]:
                self._db.rollback()
                return INVALID_RATING.detailed(
                    _("The rating must be a number between %(low)s and %(high)s.",
                      low=scale[0], high=scale[1]))
            if (new_rating - scale[0]) / (scale[1] - scale[0]) != work.quality:
                primary_identifier.add_measurement(staff_data_source, Measurement.RATING, new_rating, weight=WorkController.STAFF_WEIGHT)
                changed = True
                changed_rating = True

        changed_summary = False
        new_summary = flask.request.form.get("summary") or ""
        if new_summary != work.summary_text:
            old_summary = None
            if work.summary and work.summary.data_source == staff_data_source:
                old_summary = work.summary

            work.presentation_edition.primary_identifier.add_link(
                Hyperlink.DESCRIPTION, None,
                staff_data_source, content=new_summary)

            # Delete previous staff summary
            if old_summary:
                for link in old_summary.links:
                    self._db.delete(link)
                self._db.delete(old_summary)

            changed = True
            changed_summary = True

        if changed:
            # Even if the presentation doesn't visibly change, we want
            # to regenerate the OPDS entries and update the search
            # index for the work, because that might be the 'real'
            # problem the user is trying to fix.
            policy = PresentationCalculationPolicy(
                classify=True,
                regenerate_opds_entries=True,
                regenerate_marc_record=True,
                update_search_index=True,
                calculate_quality=changed_rating,
                choose_summary=changed_summary,
            )
            work.calculate_presentation(policy=policy)

        return Response("", 200)

    def suppress(self, identifier_type, identifier):
        """Suppress the license pool associated with a book."""
        self.require_librarian(flask.request.library)

        # Turn source + identifier into a LicensePool
        pools = self.load_licensepools(flask.request.library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            # Something went wrong.
            return pools

        # Assume that the Work is being suppressed from the catalog, and
        # not just the LicensePool.
        # TODO: Suppress individual LicensePools when it's not that deep.
        for pool in pools:
            pool.suppressed = True
        return Response("", 200)

    def unsuppress(self, identifier_type, identifier):
        """Unsuppress all license pools associated with a book.

        TODO: This will need to be revisited when we distinguish
        between complaints about a work and complaints about a
        LicensePoool.
        """
        self.require_librarian(flask.request.library)

        # Turn source + identifier into a group of LicensePools
        pools = self.load_licensepools(flask.request.library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            # Something went wrong.
            return pools

        # Unsuppress each pool.
        for pool in pools:
            pool.suppressed = False
        return Response("", 200)

    def refresh_metadata(self, identifier_type, identifier, provider=None):
        """Refresh the metadata for a book from the content server"""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        if not provider and work.license_pools:
            try:
                provider = MetadataWranglerCollectionRegistrar(work.license_pools[0].collection)
            except CannotLoadConfiguration:
                return METADATA_REFRESH_FAILURE

        identifier = work.presentation_edition.primary_identifier
        try:
            record = provider.ensure_coverage(identifier, force=True)
        except Exception:
            # The coverage provider may raise an HTTPIntegrationException.
            return REMOTE_INTEGRATION_FAILED

        if record.exception:
            # There was a coverage failure.
            if (str(record.exception).startswith("201") or
                str(record.exception).startswith("202")):
                # A 201/202 error means it's never looked up this work before
                # so it's started the resolution process or looking for sources.
                return METADATA_REFRESH_PENDING
            # Otherwise, it just doesn't know anything.
            return METADATA_REFRESH_FAILURE

        return Response("", 200)

    def resolve_complaints(self, identifier_type, identifier):
        """Resolve all complaints for a particular license pool and complaint type."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        resolved = False
        found = False

        requested_type = flask.request.form.get("type")
        if requested_type:
            for complaint in work.complaints:
                if complaint.type == requested_type:
                    found = True
                    if complaint.resolved == None:
                        complaint.resolve()
                        resolved = True

        if not found:
            return UNRECOGNIZED_COMPLAINT
        elif not resolved:
            return COMPLAINT_ALREADY_RESOLVED
        return Response("", 200)

    def classifications(self, identifier_type, identifier):
        """Return list of this work's classifications."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        identifier_id = work.presentation_edition.primary_identifier.id
        results = self._db \
            .query(Classification) \
            .join(Subject) \
            .join(DataSource) \
            .filter(Classification.identifier_id == identifier_id) \
            .order_by(Classification.weight.desc()) \
            .all()

        data = []
        for result in results:
            data.append(dict({
                "type": result.subject.type,
                "name": result.subject.identifier,
                "source": result.data_source.name,
                "weight": result.weight
            }))

        return dict({
            "book": {
                "identifier_type": identifier_type,
                "identifier": identifier
            },
            "classifications": data
        })

    def edit_classifications(self, identifier_type, identifier):
        """Edit a work's audience, target age, fiction status, and genres."""
        self.require_librarian(flask.request.library)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        # Previous staff classifications
        primary_identifier = work.presentation_edition.primary_identifier
        old_classifications = self._db \
            .query(Classification) \
            .join(Subject) \
            .filter(
                Classification.identifier == primary_identifier,
                Classification.data_source == staff_data_source
            )
        old_genre_classifications = old_classifications \
            .filter(Subject.genre_id != None)
        old_staff_genres = [
            c.subject.genre.name
            for c in old_genre_classifications
            if c.subject.genre
        ]
        old_computed_genres = [
            work_genre.genre.name
            for work_genre in work.work_genres
        ]

        # New genres should be compared to previously computed genres
        new_genres = flask.request.form.getlist("genres")
        genres_changed = sorted(new_genres) != sorted(old_computed_genres)

        # Update audience
        new_audience = flask.request.form.get("audience")
        if new_audience != work.audience:
            # Delete all previous staff audience classifications
            for c in old_classifications:
                if c.subject.type == Subject.FREEFORM_AUDIENCE:
                    self._db.delete(c)

            # Create a new classification with a high weight
            primary_identifier.classify(
                data_source=staff_data_source,
                subject_type=Subject.FREEFORM_AUDIENCE,
                subject_identifier=new_audience,
                weight=WorkController.STAFF_WEIGHT,
            )

        # Update target age if present
        new_target_age_min = flask.request.form.get("target_age_min")
        new_target_age_min = int(new_target_age_min) if new_target_age_min else None
        new_target_age_max = flask.request.form.get("target_age_max")
        new_target_age_max = int(new_target_age_max) if new_target_age_max else None
        if new_target_age_max is not None and new_target_age_min is not None and \
            new_target_age_max < new_target_age_min:
            return INVALID_EDIT.detailed(_("Minimum target age must be less than maximum target age."))

        if work.target_age:
            old_target_age_min = work.target_age.lower
            old_target_age_max = work.target_age.upper
        else:
            old_target_age_min = None
            old_target_age_max = None
        if new_target_age_min != old_target_age_min or new_target_age_max != old_target_age_max:
            # Delete all previous staff target age classifications
            for c in old_classifications:
                if c.subject.type == Subject.AGE_RANGE:
                    self._db.delete(c)

            # Create a new classification with a high weight - higher than audience
            if new_target_age_min and new_target_age_max:
                age_range_identifier = "%s-%s" % (new_target_age_min, new_target_age_max)
                primary_identifier.classify(
                    data_source=staff_data_source,
                    subject_type=Subject.AGE_RANGE,
                    subject_identifier=age_range_identifier,
                    weight=WorkController.STAFF_WEIGHT * 100,
                )

        # Update fiction status
        # If fiction status hasn't changed but genres have changed,
        # we still want to ensure that there's a staff classification
        new_fiction = True if flask.request.form.get("fiction") == "fiction" else False
        if new_fiction != work.fiction or genres_changed:
            # Delete previous staff fiction classifications
            for c in old_classifications:
                if c.subject.type == Subject.SIMPLIFIED_FICTION_STATUS:
                    self._db.delete(c)

            # Create a new classification with a high weight (higher than genre)
            fiction_term = "Fiction" if new_fiction else "Nonfiction"
            classification = primary_identifier.classify(
                data_source=staff_data_source,
                subject_type=Subject.SIMPLIFIED_FICTION_STATUS,
                subject_identifier=fiction_term,
                weight=WorkController.STAFF_WEIGHT,
            )
            classification.subject.fiction = new_fiction

        # Update genres
        # make sure all new genres are legit
        for name in new_genres:
            genre, is_new = Genre.lookup(self._db, name)
            if not isinstance(genre, Genre):
                return GENRE_NOT_FOUND
            if genres[name].is_fiction is not None and genres[name].is_fiction != new_fiction:
                return INCOMPATIBLE_GENRE
            if name == "Erotica" and new_audience != "Adults Only":
                return EROTICA_FOR_ADULTS_ONLY

        if genres_changed:
            # delete existing staff classifications for genres that aren't being kept
            for c in old_genre_classifications:
                if c.subject.genre.name not in new_genres:
                    self._db.delete(c)

            # add new staff classifications for new genres
            for genre in new_genres:
                if genre not in old_staff_genres:
                    classification = primary_identifier.classify(
                        data_source=staff_data_source,
                        subject_type=Subject.SIMPLIFIED_GENRE,
                        subject_identifier=genre,
                        weight=WorkController.STAFF_WEIGHT
                    )

            # add NONE genre classification if we aren't keeping any genres
            if len(new_genres) == 0:
                primary_identifier.classify(
                    data_source=staff_data_source,
                    subject_type=Subject.SIMPLIFIED_GENRE,
                    subject_identifier=SimplifiedGenreClassifier.NONE,
                    weight=WorkController.STAFF_WEIGHT
                )
            else:
                # otherwise delete existing NONE genre classification
                none_classifications = self._db \
                    .query(Classification) \
                    .join(Subject) \
                    .filter(
                        Classification.identifier == primary_identifier,
                        Subject.identifier == SimplifiedGenreClassifier.NONE
                    ) \
                    .all()
                for c in none_classifications:
                    self._db.delete(c)

        # Update presentation
        policy = PresentationCalculationPolicy(
            classify=True,
            regenerate_opds_entries=True,
            regenerate_marc_record=True,
            update_search_index=True
        )
        work.calculate_presentation(policy=policy)

        return Response("", 200)

    MINIMUM_COVER_WIDTH = 600
    MINIMUM_COVER_HEIGHT = 900
    TOP = 'top'
    CENTER = 'center'
    BOTTOM = 'bottom'
    TITLE_POSITIONS = [TOP, CENTER, BOTTOM]

    def _validate_cover_image(self, image):
        image_width, image_height = image.size
        if image_width < self.MINIMUM_COVER_WIDTH or image_height < self.MINIMUM_COVER_HEIGHT:
           return INVALID_IMAGE.detailed(_("Cover image must be at least %(width)spx in width and %(height)spx in height.",
                                                 width=self.MINIMUM_COVER_WIDTH, height=self.MINIMUM_COVER_HEIGHT))
        return True

    def _process_cover_image(self, work, image, title_position):
        title = work.presentation_edition.title
        author = work.presentation_edition.author
        if author == Edition.UNKNOWN_AUTHOR:
            author = ""

        if title_position in self.TITLE_POSITIONS:
            # Convert image to 'RGB' mode if it's not already, so drawing on it works.
            if image.mode != 'RGB':
                image = image.convert("RGB")

            draw = ImageDraw.Draw(image)
            image_width, image_height = image.size

            admin_dir = os.path.dirname(os.path.split(__file__)[0])
            package_dir = os.path.join(admin_dir, "../..")
            bold_font_path = os.path.join(package_dir, "resources/OpenSans-Bold.ttf")
            regular_font_path = os.path.join(package_dir, "resources/OpenSans-Regular.ttf")
            font_size = image_width // 20
            bold_font = ImageFont.truetype(bold_font_path, font_size)
            regular_font = ImageFont.truetype(regular_font_path, font_size)

            padding = image_width / 40

            max_line_width = 0
            bold_char_width = bold_font.getsize("n")[0]
            bold_char_count = image_width / bold_char_width
            regular_char_width = regular_font.getsize("n")[0]
            regular_char_count = image_width / regular_char_width
            title_lines = textwrap.wrap(title, bold_char_count)
            author_lines = textwrap.wrap(author, regular_char_count)
            for lines, font in [(title_lines, bold_font), (author_lines, regular_font)]:
                for line in lines:
                    line_width, ignore = font.getsize(line)
                    if line_width > max_line_width:
                        max_line_width = line_width

            ascent, descent = bold_font.getmetrics()
            line_height = ascent + descent

            total_text_height = line_height * (len(title_lines) + len(author_lines))
            rectangle_height = total_text_height + line_height

            rectangle_width = max_line_width + 2 * padding

            start_x = (image_width - rectangle_width) / 2
            if title_position == self.BOTTOM:
                start_y = image_height - rectangle_height - image_height / 14
            elif title_position == self.CENTER:
                start_y = (image_height - rectangle_height) / 2
            else:
                start_y = image_height / 14

            draw.rectangle([(start_x, start_y),
                            (start_x + rectangle_width, start_y + rectangle_height)],
                           fill=(255,255,255,255))

            current_y = start_y + line_height / 2
            for lines, font in [(title_lines, bold_font), (author_lines, regular_font)]:
                for line in lines:
                    line_width, ignore = font.getsize(line)
                    draw.text((start_x + (rectangle_width - line_width) / 2, current_y),
                              line, font=font, fill=(0,0,0,255))
                    current_y += line_height

            del draw

        return image

    def preview_book_cover(self, identifier_type, identifier):
        """Return a preview of the submitted cover image information."""
        self.require_librarian(flask.request.library)
        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        image = self.generate_cover_image(work, identifier_type, identifier, True)
        if isinstance(image, ProblemDetail):
            return image

        buffer = BytesIO()
        image.save(buffer, format="PNG")
        b64 = base64.b64encode(buffer.getvalue())
        value = "data:image/png;base64,%s" % b64

        return Response(value, 200)

    def generate_cover_image(self, work, identifier_type, identifier, preview=False):
        image_file = flask.request.files.get("cover_file")
        image_url = flask.request.form.get("cover_url")
        if not image_file and not image_url:
            return INVALID_IMAGE.detailed(_("Image file or image URL is required."))
        elif image_url and not Validator()._is_url(image_url, []):
            return INVALID_URL.detailed(_('"%(url)s" is not a valid URL.', url=image_url))

        title_position = flask.request.form.get("title_position")
        if image_url and not image_file:
            image_file = BytesIO(urllib.request.urlopen(image_url).read())

        image = Image.open(image_file)
        result = self._validate_cover_image(image)
        if isinstance(result, ProblemDetail):
            return result

        if preview:
            image = self._title_position(work, image)

        return image

    def _title_position(self, work, image):
        title_position = flask.request.form.get("title_position")
        if title_position and title_position in self.TITLE_POSITIONS:
            return self._process_cover_image(work, image, title_position)
        return image

    def _original_cover_info(self, image, work, data_source, rights_uri, rights_explanation):
        original, derivation_settings, cover_href = None, None, None
        cover_rights_explanation = rights_explanation
        title_position = flask.request.form.get("title_position")
        cover_url = flask.request.form.get("cover_url")
        if title_position in self.TITLE_POSITIONS:
            original_href = cover_url
            original_buffer = BytesIO()
            image.save(original_buffer, format="PNG")
            original_content = original_buffer.getvalue()
            if not original_href:
                original_href = Hyperlink.generic_uri(data_source, work.presentation_edition.primary_identifier, Hyperlink.IMAGE, content=original_content)

            image = self._process_cover_image(work, image, title_position)

            original_rights_explanation = None
            if rights_uri != RightsStatus.IN_COPYRIGHT:
                original_rights_explanation = rights_explanation
            original = LinkData(
                Hyperlink.IMAGE, original_href, rights_uri=rights_uri,
                rights_explanation=original_rights_explanation, content=original_content,
            )
            derivation_settings = dict(title_position=title_position)
            if rights_uri in RightsStatus.ALLOWS_DERIVATIVES:
                cover_rights_explanation = "The original image license allows derivatives."
        else:
            cover_href = cover_url

        return original, derivation_settings, cover_href, cover_rights_explanation

    def _get_collection_from_pools(self, identifier_type, identifier):
        pools = self.load_licensepools(flask.request.library, identifier_type, identifier)
        if isinstance(pools, ProblemDetail):
            return pools
        if not pools:
            return NO_LICENSES
        collection = pools[0].collection
        return collection

    def change_book_cover(self, identifier_type, identifier, mirrors=None):
        """Save a new book cover based on the submitted form."""
        self.require_librarian(flask.request.library)

        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        work = self.load_work(flask.request.library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        rights_uri = flask.request.form.get("rights_status")
        rights_explanation = flask.request.form.get("rights_explanation")

        if not rights_uri:
            return INVALID_IMAGE.detailed(_("You must specify the image's license."))

        collection = self._get_collection_from_pools(identifier_type, identifier)
        if isinstance(collection, ProblemDetail):
            return collection

        # Look for an appropriate mirror to store this cover image. Since the
        # mirror should be used for covers, we don't need a mirror for books.
        mirrors = mirrors or dict(
            covers_mirror=MirrorUploader.for_collection(collection, ExternalIntegrationLink.COVERS),
            books_mirror=None
        )
        if not mirrors.get(ExternalIntegrationLink.COVERS):
            return INVALID_CONFIGURATION_OPTION.detailed(_("Could not find a storage integration for uploading the cover."))

        image = self.generate_cover_image(work, identifier_type, identifier)
        if isinstance(image, ProblemDetail):
            return image

        original, derivation_settings, cover_href, cover_rights_explanation = self._original_cover_info(image, work, data_source, rights_uri, rights_explanation)

        buffer = BytesIO()
        image.save(buffer, format="PNG")
        content = buffer.getvalue()

        if not cover_href:
            cover_href = Hyperlink.generic_uri(data_source, work.presentation_edition.primary_identifier, Hyperlink.IMAGE, content=content)

        cover_data = LinkData(
            Hyperlink.IMAGE, href=cover_href,
            media_type=Representation.PNG_MEDIA_TYPE,
            content=content, rights_uri=rights_uri,
            rights_explanation=cover_rights_explanation,
            original=original, transformation_settings=derivation_settings,
        )

        presentation_policy = PresentationCalculationPolicy(
            choose_edition=False,
            set_edition_metadata=False,
            classify=False,
            choose_summary=False,
            calculate_quality=False,
            choose_cover=True,
            regenerate_opds_entries=True,
            regenerate_marc_record=True,
            update_search_index=False,
        )

        replacement_policy = ReplacementPolicy(
            links=True,
            # link_content is false because we already have the content.
            # We don't want the metadata layer to try to fetch it again.
            link_content=False,
            mirrors=mirrors,
            presentation_calculation_policy=presentation_policy,
        )

        metadata = Metadata(data_source, links=[cover_data])
        metadata.apply(work.presentation_edition,
                       collection,
                       replace=replacement_policy)

        # metadata.apply only updates the edition, so we also need
        # to update the work.
        work.calculate_presentation(policy=presentation_policy)

        return Response(_("Success"), 200)

    def _count_complaints_for_work(self, work):
        complaint_types = [complaint.type for complaint in work.complaints if not complaint.resolved]
        return Counter(complaint_types)

    def custom_lists(self, identifier_type, identifier):
        self.require_librarian(flask.request.library)

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        staff_data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)

        if flask.request.method == "GET":
            lists = []
            for entry in work.custom_list_entries:
                list = entry.customlist
                lists.append(dict(id=list.id, name=list.name))
            return dict(custom_lists=lists)

        if flask.request.method == "POST":
            lists = flask.request.form.get("lists")
            if lists:
                lists = json.loads(lists)
            else:
                lists = []

            affected_lanes = set()

            # Remove entries for lists that were not in the submitted form.
            submitted_ids = [l.get("id") for l in lists if l.get("id")]
            for entry in work.custom_list_entries:
                if entry.list_id not in submitted_ids:
                    list = entry.customlist
                    list.remove_entry(work)
                    for lane in Lane.affected_by_customlist(list):
                        affected_lanes.add(lane)

            # Add entries for any new lists.
            for list_info in lists:
                id = list_info.get("id")
                name = list_info.get("name")

                if id:
                    is_new = False
                    list = get_one(self._db, CustomList, id=int(id), name=name, library=library, data_source=staff_data_source)
                    if not list:
                        self._db.rollback()
                        return MISSING_CUSTOM_LIST.detailed(_("Could not find list \"%(list_name)s\"", list_name=name))
                else:
                    list, is_new = create(self._db, CustomList, name=name, data_source=staff_data_source, library=library)
                    list.created = utc_now()
                entry, was_new = list.add_entry(work, featured=True)
                if was_new:
                    for lane in Lane.affected_by_customlist(list):
                        affected_lanes.add(lane)

            # If any list changes affected lanes, update their sizes.
            # NOTE: This may not make a difference until the
            # works are actually re-indexed.
            for lane in affected_lanes:
                lane.update_size(self._db, self.search_engine)

            return Response(str(_("Success")), 200)
