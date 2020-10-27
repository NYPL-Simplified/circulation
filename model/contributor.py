# encoding: utf-8
# Contributor, Contribution
from nose.tools import set_trace

from . import (
    Base,
    flush,
    get_one_or_create,
)

import logging
import re
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    JSON,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import (
    relationship,
    synonym,
)
from sqlalchemy.orm.session import Session
from ..util.personal_names import display_name_to_sort_name
from ..util.string_helpers import native_string

class Contributor(Base):
    """Someone (usually human) who contributes to books."""

    __tablename__ = 'contributors'
    id = Column(Integer, primary_key=True)

    # Standard identifiers for this contributor.
    lc = Column(Unicode, index=True)
    viaf = Column(Unicode, index=True)

    # This is the name by which this person is known in the original
    # catalog. It is sortable, e.g. "Twain, Mark".
    _sort_name = Column('sort_name', Unicode, index=True)
    aliases = Column(ARRAY(Unicode), default=[])

    # This is the name we will display publicly. Ideally it will be
    # the name most familiar to readers.
    display_name = Column(Unicode, index=True)

    # This is a short version of the contributor's name, displayed in
    # situations where the full name is too long. For corporate contributors
    # this value will be None.
    family_name = Column(Unicode, index=True)

    # This is the name used for this contributor on Wikipedia. This
    # gives us an entry point to Wikipedia, Wikidata, etc.
    wikipedia_name = Column(Unicode, index=True)

    # This is a short biography for this contributor, probably
    # provided by a publisher.
    biography = Column(Unicode)

    extra = Column(MutableDict.as_mutable(JSON), default={})

    contributions = relationship("Contribution", backref="contributor")

    # Types of roles
    AUTHOR_ROLE = u"Author"
    PRIMARY_AUTHOR_ROLE = u"Primary Author"
    EDITOR_ROLE = u"Editor"
    ARTIST_ROLE = u"Artist"
    PHOTOGRAPHER_ROLE = u"Photographer"
    TRANSLATOR_ROLE = u"Translator"
    ILLUSTRATOR_ROLE = u"Illustrator"
    LETTERER_ROLE = u"Letterer"
    PENCILER_ROLE = u"Penciler"
    COLORIST_ROLE = u"Colorist"
    INKER_ROLE = u"Inker"
    INTRODUCTION_ROLE = u"Introduction Author"
    FOREWORD_ROLE = u"Foreword Author"
    AFTERWORD_ROLE = u"Afterword Author"
    COLOPHON_ROLE = u"Colophon Author"
    UNKNOWN_ROLE = u'Unknown'
    DIRECTOR_ROLE = u'Director'
    PRODUCER_ROLE = u'Producer'
    EXECUTIVE_PRODUCER_ROLE = u'Executive Producer'
    ACTOR_ROLE = u'Actor'
    LYRICIST_ROLE = u'Lyricist'
    CONTRIBUTOR_ROLE = u'Contributor'
    COMPOSER_ROLE = u'Composer'
    NARRATOR_ROLE = u'Narrator'
    COMPILER_ROLE = u'Compiler'
    ADAPTER_ROLE = u'Adapter'
    PERFORMER_ROLE = u'Performer'
    MUSICIAN_ROLE = u'Musician'
    ASSOCIATED_ROLE = u'Associated name'
    COLLABORATOR_ROLE = u'Collaborator'
    ENGINEER_ROLE = u'Engineer'
    COPYRIGHT_HOLDER_ROLE = u'Copyright holder'
    TRANSCRIBER_ROLE = u'Transcriber'
    DESIGNER_ROLE = u'Designer'
    AUTHOR_ROLES = set([PRIMARY_AUTHOR_ROLE, AUTHOR_ROLE])

    # Map our recognized roles to MARC relators.
    # https://www.loc.gov/marc/relators/relaterm.html
    #
    # This is used when crediting contributors in OPDS feeds.
    MARC_ROLE_CODES = {
        ACTOR_ROLE : 'act',
        ADAPTER_ROLE : 'adp',
        AFTERWORD_ROLE : 'aft',
        ARTIST_ROLE : 'art',
        ASSOCIATED_ROLE : 'asn',
        AUTHOR_ROLE : 'aut',            # Joint author: USE Author
        COLLABORATOR_ROLE : 'ctb',      # USE Contributor
        COLOPHON_ROLE : 'aft',          # Author of afterword, colophon, etc.
        COMPILER_ROLE : 'com',
        COMPOSER_ROLE : 'cmp',
        CONTRIBUTOR_ROLE : 'ctb',
        COPYRIGHT_HOLDER_ROLE : 'cph',
        DESIGNER_ROLE : 'dsr',
        DIRECTOR_ROLE : 'drt',
        EDITOR_ROLE : 'edt',
        ENGINEER_ROLE : 'eng',
        EXECUTIVE_PRODUCER_ROLE : 'pro',
        FOREWORD_ROLE : 'wpr',          # Writer of preface
        ILLUSTRATOR_ROLE : 'ill',
        INTRODUCTION_ROLE : 'win',
        LYRICIST_ROLE : 'lyr',
        MUSICIAN_ROLE : 'mus',
        NARRATOR_ROLE : 'nrt',
        PERFORMER_ROLE : 'prf',
        PHOTOGRAPHER_ROLE : 'pht',
        PRIMARY_AUTHOR_ROLE : 'aut',
        PRODUCER_ROLE : 'pro',
        TRANSCRIBER_ROLE : 'trc',
        TRANSLATOR_ROLE : 'trl',
        LETTERER_ROLE : 'contributor',
        PENCILER_ROLE : 'contributor',
        COLORIST_ROLE : 'contributor',
        INKER_ROLE : 'contributor',
        UNKNOWN_ROLE : 'asn',
    }

    # People from these roles can be put into the 'author' slot if no
    # author proper is given.
    AUTHOR_SUBSTITUTE_ROLES = [
        EDITOR_ROLE, COMPILER_ROLE, COMPOSER_ROLE, DIRECTOR_ROLE,
        CONTRIBUTOR_ROLE, TRANSLATOR_ROLE, ADAPTER_ROLE, PHOTOGRAPHER_ROLE,
        ARTIST_ROLE, LYRICIST_ROLE, COPYRIGHT_HOLDER_ROLE
    ]

    PERFORMER_ROLES = [ACTOR_ROLE, PERFORMER_ROLE, NARRATOR_ROLE, MUSICIAN_ROLE]

    # Extra fields
    BIRTH_DATE = 'birthDate'
    DEATH_DATE = 'deathDate'

    def __repr__(self):
        extra = ""
        if self.lc:
            extra += " lc=%s" % self.lc
        if self.viaf:
            extra += " viaf=%s" % self.viaf
        return native_string(
            u"Contributor %d (%s)" % (self.id, self.sort_name)
        )

    @classmethod
    def author_contributor_tiers(cls):
        yield [cls.PRIMARY_AUTHOR_ROLE]
        yield cls.AUTHOR_ROLES
        yield cls.AUTHOR_SUBSTITUTE_ROLES
        yield cls.PERFORMER_ROLES

    @classmethod
    def lookup(cls, _db, sort_name=None, viaf=None, lc=None, aliases=None,
               extra=None, create_new=True, name=None):
        """Find or create a record (or list of records) for the given Contributor.
        :return: A tuple of found Contributor (or None), and a boolean flag
        indicating if new Contributor database object has beed created.
        """

        new = False
        contributors = []

        # TODO: Stop using 'name' attribute, everywhere.
        sort_name = sort_name or name
        extra = extra or dict()

        create_method_kwargs = {
            Contributor.sort_name.name : sort_name,
            Contributor.aliases.name : aliases,
            Contributor.extra.name : extra
        }

        if not sort_name and not lc and not viaf:
            raise ValueError(
                "Cannot look up a Contributor without any identifying "
                "information whatsoever!")

        if sort_name and not lc and not viaf:
            # We will not create a Contributor based solely on a name
            # unless there is no existing Contributor with that name.
            #
            # If there *are* contributors with that name, we will
            # return all of them.
            #
            # We currently do not check aliases when doing name lookups.
            q = _db.query(Contributor).filter(Contributor.sort_name==sort_name)
            contributors = q.all()
            if contributors:
                return contributors, new
            else:
                try:
                    contributor = Contributor(**create_method_kwargs)
                    _db.add(contributor)
                    flush(_db)
                    contributors = [contributor]
                    new = True
                except IntegrityError:
                    _db.rollback()
                    contributors = q.all()
                    new = False
        else:
            # We are perfecly happy to create a Contributor based solely
            # on lc or viaf.
            query = dict()
            if lc:
                query[Contributor.lc.name] = lc
            if viaf:
                query[Contributor.viaf.name] = viaf

            if create_new:
                contributor, new = get_one_or_create(
                    _db, Contributor, create_method_kwargs=create_method_kwargs,
                    on_multiple='interchangeable',
                    **query
                )
                if contributor:
                    contributors = [contributor]
            else:
                contributor = get_one(_db, Contributor, **query)
                if contributor:
                    contributors = [contributor]

        return contributors, new


    @property
    def sort_name(self):
        return self._sort_name

    @sort_name.setter
    def sort_name(self, new_sort_name):
        """ See if the passed-in value is in the prescribed Last, First format.
        If it is, great, set the self._sprt_name to the new value.

        If new value is not in correct format, then
        attempt to re-format the value to look like: "Last, First Middle, Dr./Jr./etc.".

        Note: If for any reason you need to force the sort_name to an improper value,
        set it like so:  contributor._sort_name="Foo Bar", and you'll avoid further processing.

        Note: For now, have decided to not automatically update any edition.sort_author
        that might have contributions by this Contributor.
        """

        if not new_sort_name:
            self._sort_name = None
            return

        # simplistic test of format, but catches the most frequent problem
        # where display-style names are put into sort name metadata by third parties.
        if new_sort_name.find(",") == -1:
            # auto-magically fix syntax
            self._sort_name = display_name_to_sort_name(new_sort_name)
            return

        self._sort_name = new_sort_name

    # tell SQLAlchemy to use the sort_name setter for ort_name, not _sort_name, after all.
    sort_name = synonym('_sort_name', descriptor=sort_name)


    def merge_into(self, destination):
        """Two Contributor records should be the same.

        Merge this one into the other one.

        For now, this should only be used when the exact same record
        comes in through two sources. It should not be used when two
        Contributors turn out to represent different names for the
        same human being, e.g. married names or (especially) pen
        names. Just because we haven't thought that situation through
        well enough.
        """
        if self == destination:
            # They're already the same.
            return
        logging.info(
            u"MERGING %r (%s) into %r (%s)",
            self,
            self.viaf,
            destination,
            destination.viaf
        )

        # make sure we're not losing any names we know for the contributor
        existing_aliases = set(destination.aliases)
        new_aliases = list(destination.aliases)
        for name in [self.sort_name] + self.aliases:
            if name != destination.sort_name and name not in existing_aliases:
                new_aliases.append(name)
        if new_aliases != destination.aliases:
            destination.aliases = new_aliases

        if not destination.family_name:
            destination.family_name = self.family_name
        if not destination.display_name:
            destination.display_name = self.display_name
        # keep sort_name if one of the contributor objects has it.
        if not destination.sort_name:
            destination.sort_name = self.sort_name
        if not destination.wikipedia_name:
            destination.wikipedia_name = self.wikipedia_name

        # merge non-name-related properties
        for k, v in self.extra.items():
            if not k in destination.extra:
                destination.extra[k] = v
        if not destination.lc:
            destination.lc = self.lc
        if not destination.viaf:
            destination.viaf = self.viaf
        if not destination.biography:
            destination.biography = self.biography

        _db = Session.object_session(self)
        for contribution in self.contributions:
            # Is the new contributor already associated with this
            # Edition in the given role (in which case we delete
            # the old contribution) or not (in which case we switch the
            # contributor ID)?
            existing_record = _db.query(Contribution).filter(
                Contribution.contributor_id==destination.id,
                Contribution.edition_id==contribution.edition.id,
                Contribution.role==contribution.role)
            if existing_record.count():
                _db.delete(contribution)
            else:
                contribution.contributor_id = destination.id

        _db.commit()
        _db.delete(self)
        _db.commit()

    # Regular expressions used by default_names().
    PARENTHETICAL = re.compile("\([^)]*\)")
    ALPHABETIC = re.compile("[a-zA-z]")
    NUMBERS = re.compile("[0-9]")

    DATE_RES = [re.compile("\(?" + x + "\)?") for x in
                "[0-9?]+-",
                "[0-9]+st cent",
                "[0-9]+nd cent",
                "[0-9]+th cent",
                "\bcirca",
                ]


    def default_names(self, default_display_name=None):
        """Attempt to derive a family name ("Twain") and a display name ("Mark
        Twain") from a catalog name ("Twain, Mark").

        This is full of pitfalls, which is why we prefer to use data
        from VIAF. But when there is no data from VIAF, the output of
        this algorithm is better than the input in pretty much every
        case.
        """
        return self._default_names(self.sort_name, default_display_name)

    @classmethod
    def _default_names(cls, name, default_display_name=None):
        name = name or ""
        original_name = name
        """Split out from default_names to make it easy to test."""
        display_name = default_display_name
        # "Little, Brown &amp; Co." => "Little, Brown & Co."
        name = name.replace("&amp;", "&")

        # "Philadelphia Broad Street Church (Philadelphia, Pa.)"
        #  => "Philadelphia Broad Street Church"
        name = cls.PARENTHETICAL.sub("", name)
        name = name.strip()

        if ', ' in name:
            # This is probably a personal name.
            parts = name.split(", ")
            if len(parts) > 2:
                # The most likely scenario is that the final part
                # of the name is a date or a set of dates. If this
                # seems true, just delete that part.
                if (cls.NUMBERS.search(parts[-1])
                    or not cls.ALPHABETIC.search(parts[-1])):
                    parts = parts[:-1]
            # The final part of the name may have a date or a set
            # of dates at the end. If so, remove it from that string.
            final = parts[-1]
            for date_re in cls.DATE_RES:
                m = date_re.search(final)
                if m:
                    new_part = final[:m.start()].strip()
                    if new_part:
                        parts[-1] = new_part
                    else:
                        del parts[-1]
                    break

            family_name = parts[0]
            p = parts[-1].lower()
            if (p in ('llc', 'inc', 'inc.')
                or p.endswith("company") or p.endswith(" co.")
                or p.endswith(" co")):
                # No, this is a corporate name that contains a comma.
                # It can't be split on the comma, so don't bother.
                family_name = None
                display_name = display_name or name
            if not display_name:
                # The fateful moment. Swap the second string and the
                # first string.
                if len(parts) == 1:
                    display_name = parts[0]
                    family_name = display_name
                else:
                    display_name = parts[1] + " " + parts[0]
                if len(parts) > 2:
                    # There's a leftover bit.
                    if parts[2] in ('Mrs.', 'Mrs', 'Sir'):
                        # "Jones, Bob, Mrs."
                        #  => "Mrs. Bob Jones"
                        display_name = parts[2] + " " + display_name
                    else:
                        # "Jones, Bob, Jr."
                        #  => "Bob Jones, Jr."
                        display_name += ", " + " ".join(parts[2:])
        else:
            # Since there's no comma, this is probably a corporate name.
            family_name = None
            display_name = name

        return family_name, display_name

class Contribution(Base):
    """A contribution made by a Contributor to a Edition."""
    __tablename__ = 'contributions'
    id = Column(Integer, primary_key=True)
    edition_id = Column(Integer, ForeignKey('editions.id'), index=True,
                           nullable=False)
    contributor_id = Column(Integer, ForeignKey('contributors.id'), index=True,
                            nullable=False)
    role = Column(Unicode, index=True, nullable=False)
    __table_args__ = (
        UniqueConstraint('edition_id', 'contributor_id', 'role'),
    )

