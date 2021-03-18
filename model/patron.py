# encoding: utf-8
# LoanAndHoldMixin, Patron, Loan, Hold, Annotation, PatronProfileStorage


from . import (
    Base,
    get_one_or_create,
    numericrange_to_tuple
)
from credential import Credential
import datetime
import logging
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Unicode,
    UniqueConstraint,
)
from psycopg2.extras import NumericRange
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session
from ..classifier import Classifier
from ..user_profile import ProfileStorage
import uuid

class LoanAndHoldMixin(object):

    @property
    def work(self):
        """Try to find the corresponding work for this Loan/Hold."""
        license_pool = self.license_pool
        if not license_pool:
            return None
        if license_pool.work:
            return license_pool.work
        if license_pool.presentation_edition and license_pool.presentation_edition.work:
            return license_pool.presentation_edition.work
        return None

    @property
    def library(self):
        """Try to find the corresponding library for this Loan/Hold."""
        if self.patron:
            return self.patron.library
        # If this Loan/Hold belongs to a external patron, there may be no library.
        return None

class Patron(Base):

    __tablename__ = 'patrons'
    id = Column(Integer, primary_key=True)

    # Each patron is the patron _of_ one particular library.  An
    # individual human being may patronize multiple libraries, but
    # they will have a different patron account at each one.
    library_id = Column(
        Integer, ForeignKey('libraries.id'), index=True,
        nullable=False
    )

    # The patron's permanent unique identifier in an external library
    # system, probably never seen by the patron.
    #
    # This is not stored as a ForeignIdentifier because it corresponds
    # to the patron's identifier in the library responsible for the
    # Simplified instance, not a third party.
    external_identifier = Column(Unicode)

    # The patron's account type, as reckoned by an external library
    # system. Different account types may be subject to different
    # library policies.
    #
    # Depending on library policy it may be possible to automatically
    # derive the patron's account type from their authorization
    # identifier.
    external_type = Column(Unicode, index=True)

    # An identifier used by the patron that gives them the authority
    # to borrow books. This identifier may change over time.
    authorization_identifier = Column(Unicode)

    # An identifier used by the patron that authenticates them,
    # but does not give them the authority to borrow books. i.e. their
    # website username.
    username = Column(Unicode)

    # The last time this record was synced up with an external library
    # system such as an ILS.
    last_external_sync = Column(DateTime)

    # The last time this record was synced with the corresponding
    # records managed by the vendors who provide the library with
    # ebooks.
    _last_loan_activity_sync = Column(
        DateTime, default=None, name="last_loan_activity_sync"
    )

    # The time, if any, at which the user's authorization to borrow
    # books expires.
    authorization_expires = Column(Date, index=True)

    # Outstanding fines the user has, if any.
    fines = Column(Unicode)

    # If the patron's borrowing privileges have been blocked, this
    # field contains the library's reason for the block. If this field
    # is None, the patron's borrowing privileges have not been
    # blocked.
    #
    # Although we currently don't do anything with specific values for
    # this field, the expectation is that values will be taken from a
    # small controlled vocabulary (e.g. "banned", "incorrect personal
    # information", "unknown"), rather than freeform strings entered
    # by librarians.
    #
    # Common reasons for blocks are kept in circulation's PatronData
    # class.
    block_reason = Column(String(255), default=None)

    # Whether or not the patron wants their annotations synchronized
    # across devices (which requires storing those annotations on a
    # library server).
    _synchronize_annotations = Column(Boolean, default=None,
                                      name="synchronize_annotations")

    # If the circulation manager is set up to associate a patron's
    # neighborhood with circulation events, and it would be
    # prohibitively expensive to fetch a patron's neighborhood from
    # the ILS on every relevant request, the ILS may choose to cache
    # the information here.
    #
    # Periodically, patrons with an old last_external_sync (i.e. who
    # haven't used the circulation manager in a while) will have their
    # cached_neighborhood scrubbed from the database. This is the
    # responsibility of PatronNeighborhoodScrubber.
    #
    # This field is called cached_neighborhood for two reasons. First,
    # the name makes it clear that this is a performance cache, not a
    # permanent data store like authorization_identifier. Second, the
    # neighborhood of the authenticated patron (however obtained) is
    # stored in flask.request.patron.neighborhood. Giving the database
    # field a different name guarantees that a patron's neighborhood
    # is never _unintentionally_ written to the database.  It has to
    # be an explicit decision of the ILS integration code.
    cached_neighborhood = Column(Unicode, default=None, index=True)

    loans = relationship('Loan', backref='patron', cascade='delete')
    holds = relationship('Hold', backref='patron', cascade='delete')

    annotations = relationship('Annotation', backref='patron', order_by="desc(Annotation.timestamp)", cascade='delete')

    # One Patron can have many associated Credentials.
    credentials = relationship("Credential", backref="patron", cascade="delete")

    __table_args__ = (
        UniqueConstraint('library_id', 'username'),
        UniqueConstraint('library_id', 'authorization_identifier'),
        UniqueConstraint('library_id', 'external_identifier'),
    )

    # A patron with borrowing privileges should have their local
    # metadata synced with their ILS record at intervals no greater
    # than this time.
    MAX_SYNC_TIME = datetime.timedelta(hours=12)

    def __repr__(self):
        def date(d):
            """Format an object that might be a datetime as a date.

            This keeps a patron representation short.
            """
            if d is None:
                return None
            if isinstance(d, datetime.datetime):
                return d.date()
            return d
        return '<Patron authentication_identifier=%s expires=%s sync=%s>' % (
            self.authorization_identifier, date(self.authorization_expires),
            date(self.last_external_sync)
        )

    def identifier_to_remote_service(self, remote_data_source, generator=None):
        """Find or randomly create an identifier to use when identifying
        this patron to a remote service.
        :param remote_data_source: A DataSource object (or name of a
        DataSource) corresponding to the remote service.
        """
        _db = Session.object_session(self)
        def refresh(credential):
            if generator and callable(generator):
                identifier = generator()
            else:
                identifier = str(uuid.uuid1())
            credential.credential = identifier
        credential = Credential.lookup(
            _db, remote_data_source, Credential.IDENTIFIER_TO_REMOTE_SERVICE,
            self, refresh, allow_persistent_token=True
        )
        return credential.credential

    def works_on_loan(self):
        db = Session.object_session(self)
        loans = db.query(Loan).filter(Loan.patron==self)
        return [loan.work for loan in self.loans if loan.work]

    def works_on_loan_or_on_hold(self):
        db = Session.object_session(self)
        results = set()
        holds = [hold.work for hold in self.holds if hold.work]
        loans = self.works_on_loan()
        return set(holds + loans)

    @property
    def loan_activity_max_age(self):
        """In the absence of any other information, how long should loan
        activity be considered 'fresh' for this patron?

        We reset Patron.last_loan_activity_sync immediately if we hear
        about a change to a patron's loans or holds. This handles
        cases where patron activity happens where we can't see it,
        e.g. on a vendor website or mobile app.

        TODO: This is currently a constant, but in the future it could become
        a per-library setting.
        """
        return 15 * 60

    @hybrid_property
    def last_loan_activity_sync(self):
        """When was the last time we asked the vendors about
        this patron's loan activity?

        :return: A datetime, or None if we know our loan data is
            stale.
        """
        value = self._last_loan_activity_sync
        if not value:
            return value

        # We have an answer, but it may be so old that we should clear
        # it out.
        now = datetime.datetime.utcnow()
        expires = value + datetime.timedelta(
            seconds=self.loan_activity_max_age
        )
        if now > expires:
            # The value has expired. Clear it out.
            value = None
            self._last_loan_activity_sync = value
        return value

    @last_loan_activity_sync.setter
    def last_loan_activity_sync(self, value):
        self._last_loan_activity_sync = value

    @hybrid_property
    def synchronize_annotations(self):
        return self._synchronize_annotations

    @synchronize_annotations.setter
    def synchronize_annotations(self, value):
        """When a patron says they don't want their annotations to be stored
        on a library server, delete all their annotations.
        """
        if value is None:
            # A patron cannot decide to go back to the state where
            # they hadn't made a decision.
            raise ValueError(
                "synchronize_annotations cannot be unset once set."
            )
        if value is False:
            _db = Session.object_session(self)
            qu = _db.query(Annotation).filter(Annotation.patron==self)
            for annotation in qu:
                _db.delete(annotation)
        self._synchronize_annotations = value

    @property
    def root_lane(self):
        """Find the Lane, if any, to be used as the Patron's root lane.

        A patron with a root Lane can only access that Lane and the
        Lanes beneath it. In addition, a patron with a root lane
        cannot conduct a transaction on a book intended for an older
        audience than the one defined by their root lane.
        """

        # Two ways of improving performance by short-circuiting this
        # logic.
        if not self.external_type:
            return None
        if not self.library.has_root_lanes:
            return None

        _db = Session.object_session(self)
        from ..lane import Lane
        qu = _db.query(Lane).filter(
            Lane.library==self.library
        ).filter(
            Lane.root_for_patron_type.any(self.external_type)
        ).order_by(Lane.id)
        lanes = qu.all()
        if len(lanes) < 1:
            # The most common situation -- this patron has no special
            # root lane.
            return None
        if len(lanes) > 1:
            # Multiple root lanes for a patron indicates a
            # configuration problem, but we shouldn't make the patron
            # pay the price -- just pick the first one.
            logging.error(
                "Multiple root lanes found for patron type %s.",
                self.external_type
            )
        return lanes[0]

    def work_is_age_appropriate(self, work_audience, work_target_age):
        """Is the given audience and target age an age-appropriate match for this Patron?

        NOTE: What "age-appropriate" means depends on some policy questions
        that have not been answered and may be library-specific. For
        now, it is determined by comparing audience and target age to that of the
        Patron's root lane.

        This is designed for use when reasoning about works in
        general. If you have a specific Work in mind, use
        `Work.age_appropriate_for_patron`.

        :param work_audience: One of the audience constants from
           Classifier, representing the general reading audience to
           which a putative work belongs.

        :param work_target_age: A number or 2-tuple representing the target age
           or age range of a putative work.

        :return: A boolean

        """
        root = self.root_lane
        if not root:
            # The patron has no root lane. They can interact with any
            # title.
            return True

        # The patron can interact with a title if any of the audiences
        # in their root lane (in conjunction with the root lane's target_age)
        # are a match for the title's audience and target age.
        return any(
            self.age_appropriate_match(
                work_audience, work_target_age,
                audience, root.target_age
            )
            for audience in root.audiences
        )

    @classmethod
    def age_appropriate_match(
        cls, work_audience, work_target_age,
        reader_audience, reader_age
    ):
        """Match the audience and target age of a work with that of a reader,
        and see whether they are an age-appropriate match.

        NOTE: What "age-appropriate" means depends on some policy
        questions that have not been answered and may be
        library-specific. For now, non-children's books are
        age-inappropriate for young children, and children's books are
        age-inappropriate for children too young to be in the book's
        target age range.

        :param reader_audience: One of the audience constants from
           Classifier, representing the general reading audience to
           which the reader belongs.

        :param reader_age: A number or 2-tuple representing the age or
           age range of the reader.
        """
        if reader_audience is None:
            # A patron with no particular audience restrictions
            # can see everything.
            #
            # This is by far the most common case, so we don't set up
            # logging until after running it.
            return True

        log = logging.getLogger("Age-appropriate match calculator")
        log.debug(
            "Matching work %s/%s to reader %s/%s" % (
                work_audience, work_target_age,
                reader_audience, reader_age
            )
        )

        if reader_audience not in Classifier.AUDIENCES_JUVENILE:
            log.debug("A non-juvenile patron can see everything.")
            return True

        if work_audience == Classifier.AUDIENCE_ALL_AGES:
            log.debug("An all-ages book is always age appropriate.")
            return True

        # At this point we know that the patron is a juvenile.

        def ensure_tuple(x):
            # Convert a potential NumericRange into a tuple.
            if isinstance(x, NumericRange):
                x = numericrange_to_tuple(x)
            return x

        reader_age = ensure_tuple(reader_age)
        if isinstance(reader_age, tuple):
            # A range was passed in rather than a specific age. Assume
            # the reader is at the top edge of the range.
            ignore, reader_age = reader_age

        work_target_age = ensure_tuple(work_target_age)
        if isinstance(work_target_age, tuple):
            # Pick the _bottom_ edge of a work's target age range --
            # the work is appropriate for anyone _at least_ that old.
            work_target_age, ignore = work_target_age

        # A YA reader is treated as an adult (with no reading
        # restrictions) if they have no associated age range, or their
        # age range includes ADULT_AGE_CUTOFF.
        if (reader_audience == Classifier.AUDIENCE_YOUNG_ADULT
            and (reader_age is None
                 or reader_age >= Classifier.ADULT_AGE_CUTOFF)):
            log.debug("YA reader to be treated as an adult.")
            return True

        # There are no other situations where a juvenile reader can access
        # non-juvenile titles.
        if work_audience not in Classifier.AUDIENCES_JUVENILE:
            log.debug("Juvenile reader cannot access non-juvenile title.")
            return False

        # At this point we know we have a juvenile reader and a
        # juvenile book.

        if (reader_audience == Classifier.AUDIENCE_YOUNG_ADULT
            and work_audience in (Classifier.AUDIENCES_YOUNG_CHILDREN)):
            log.debug("YA reader can access any children's title.")
            return True

        if (reader_audience in (Classifier.AUDIENCES_YOUNG_CHILDREN)
            and work_audience == Classifier.AUDIENCE_YOUNG_ADULT):
            log.debug("Child reader cannot access any YA title.")
            return False

        # At this point we either have a YA patron with a YA book, or
        # a child patron with a children's book. It comes down to a
        # question of the reader's age vs. the work's target age.

        if work_target_age is None:
            # This is a generic children's or YA book with no
            # particular target age. Assume it's age appropriate.
            log.debug(
                "Juvenile book with no target age is presumed age-appropriate."
            )
            return True

        if reader_age is None:
            # We have no idea how old the patron is, so any work with
            # the appropriate audience is considered age-appropriate.
            log.debug(
                "Audience matches, and no specific patron age information available: presuming age-appropriate."
            )
            return True

        if reader_age < work_target_age:
            # The audience for this book matches the patron's
            # audience, but the book has a target age that is too high
            # for the reader.
            log.debug(
                "Audience matches, but work's target age is too high for reader."
            )
            return False

        log.debug("Both audience and target age match; it's age-appropriate.")
        return True


Index("ix_patron_library_id_external_identifier", Patron.library_id, Patron.external_identifier)
Index("ix_patron_library_id_authorization_identifier", Patron.library_id, Patron.authorization_identifier)
Index("ix_patron_library_id_username", Patron.library_id, Patron.username)

class Loan(Base, LoanAndHoldMixin):
    __tablename__ = 'loans'
    id = Column(Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    integration_client_id = Column(Integer, ForeignKey('integrationclients.id'), index=True)

    # A Loan is always associated with a LicensePool.
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)

    # It may also be associated with an individual License if the source
    # provides information about individual licenses.
    license_id = Column(Integer, ForeignKey('licenses.id'), index=True, nullable=True)

    fulfillment_id = Column(Integer, ForeignKey('licensepooldeliveries.id'))
    start = Column(DateTime, index=True)
    end = Column(DateTime, index=True)
    # Some distributors (e.g. Feedbooks) may have an identifier that can
    # be used to check the status of a specific Loan.
    external_identifier = Column(Unicode, unique=True, nullable=True)

    __table_args__ = (
        UniqueConstraint('patron_id', 'license_pool_id'),
    )

    def until(self, default_loan_period):
        """Give or estimate the time at which the loan will end."""
        if self.end:
            return self.end
        if default_loan_period is None:
            # This loan will last forever.
            return None
        start = self.start or datetime.datetime.utcnow()
        return start + default_loan_period

class Hold(Base, LoanAndHoldMixin):
    """A patron is in line to check out a book.
    """
    __tablename__ = 'holds'
    id = Column(Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    integration_client_id = Column(Integer, ForeignKey('integrationclients.id'), index=True)
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'), index=True)
    start = Column(DateTime, index=True)
    end = Column(DateTime, index=True)
    position = Column(Integer, index=True)
    external_identifier = Column(Unicode, unique=True, nullable=True)

    @classmethod
    def _calculate_until(
            self, start, queue_position, total_licenses, default_loan_period,
            default_reservation_period):
        """Helper method for `Hold.until` that can be tested independently.
        We have to wait for the available licenses to cycle a
        certain number of times before we get a turn.
        Example: 4 licenses, queue position 21
        After 1 cycle: queue position 17
              2      : queue position 13
              3      : queue position 9
              4      : queue position 5
              5      : queue position 1
              6      : available
        The worst-case cycle time is the loan period plus the reservation
        period.
        """
        if queue_position == 0:
            # The book is currently reserved to this patron--they need
            # to hurry up and check it out.
            return start + default_reservation_period

        if total_licenses == 0:
            # The book will never be available
            return None

        # If you are at the very front of the queue, the worst case
        # time to get the book is is the time it takes for the person
        # in front of you to get a reservation notification, borrow
        # the book at the last minute, and keep the book for the
        # maximum allowable time.
        cycle_period = (default_reservation_period + default_loan_period)

        # This will happen at least once.
        cycles = 1

        if queue_position <= total_licenses:
            # But then the book will be available to you.
            pass
        else:
            # This will happen more than once. After the first cycle,
            # other people will be notified that it's their turn,
            # they'll wait a while, get a reservation, and then keep
            # the book for a while, and so on.
            cycles += queue_position // total_licenses
            if (total_licenses > 1 and queue_position % total_licenses == 0):
                cycles -= 1
        return start + (cycle_period * cycles)


    def until(self, default_loan_period, default_reservation_period):
        """Give or estimate the time at which the book will be available
        to this patron.
        This is a *very* rough estimate that should be treated more or
        less as a worst case. (Though it could be even worse than
        this--the library's license might expire and then you'll
        _never_ get the book.)
        """
        if self.end and self.end > datetime.datetime.utcnow():
            # The license source provided their own estimate, and it's
            # not obviously wrong, so use it.
            return self.end

        if default_loan_period is None or default_reservation_period is None:
            # This hold has no definite end date, because there's no known
            # upper bound on how long someone in front of you can keep the
            # book.
            return None

        start = datetime.datetime.utcnow()
        licenses_available = self.license_pool.licenses_owned
        position = self.position
        if position is None:
            # We don't know where in line we are. Assume we're at the
            # end.
            position = self.license_pool.patrons_in_hold_queue
        return self._calculate_until(
            start, position, licenses_available,
            default_loan_period, default_reservation_period)

    def update(self, start, end, position):
        """When the book becomes available, position will be 0 and end will be
        set to the time at which point the patron will lose their place in
        line.
        Otherwise, end is irrelevant and is set to None.
        """
        if start is not None:
            self.start = start
        if end is not None:
            self.end = end
        if position is not None:
            self.position = position

    __table_args__ = (
        UniqueConstraint('patron_id', 'license_pool_id'),
    )

class Annotation(Base):
    # The Web Annotation Data Model defines a basic set of motivations.
    # https://www.w3.org/TR/annotation-model/#motivation-and-purpose
    OA_NAMESPACE = u"http://www.w3.org/ns/oa#"

    # We need to define some terms of our own.
    LS_NAMESPACE = u"http://librarysimplified.org/terms/annotation/"

    IDLING = LS_NAMESPACE + u'idling'
    BOOKMARKING = OA_NAMESPACE + u'bookmarking'

    MOTIVATIONS = [
        IDLING,
        BOOKMARKING,
    ]

    __tablename__ = 'annotations'
    id = Column(Integer, primary_key=True)
    patron_id = Column(Integer, ForeignKey('patrons.id'), index=True)
    identifier_id = Column(Integer, ForeignKey('identifiers.id'), index=True)
    motivation = Column(Unicode, index=True)
    timestamp = Column(DateTime, index=True)
    active = Column(Boolean, default=True)
    content = Column(Unicode)
    target = Column(Unicode)

    @classmethod
    def get_one_or_create(self, _db, patron, *args, **kwargs):
        """Find or create an Annotation, but only if the patron has
        annotation sync turned on.
        """
        if not patron.synchronize_annotations:
            raise ValueError(
                "Patron has opted out of synchronizing annotations."
            )

        return get_one_or_create(
            _db, Annotation, patron=patron, *args, **kwargs
        )

    def set_inactive(self):
        self.active = False
        self.content = None
        self.timestamp = datetime.datetime.utcnow()

class PatronProfileStorage(ProfileStorage):
    """Interface between a Patron object and the User Profile Management
    Protocol.
    """

    def __init__(self, patron, url_for=None):
        """Set up a storage interface for a specific Patron.
        :param patron: We are accessing the profile for this patron.
        """
        self.patron = patron
        self.url_for = url_for

    @property
    def writable_setting_names(self):
        """Return the subset of settings that are considered writable."""
        return set([self.SYNCHRONIZE_ANNOTATIONS])

    @property
    def profile_document(self):
        """Create a Profile document representing the patron's current
        status.
        """
        doc = dict()
        patron = self.patron
        doc[self.AUTHORIZATION_IDENTIFIER] = patron.authorization_identifier
        if patron.authorization_expires:
            doc[self.AUTHORIZATION_EXPIRES] = (
                patron.authorization_expires.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        settings = {
            self.SYNCHRONIZE_ANNOTATIONS :
            patron.synchronize_annotations
        }
        doc[self.SETTINGS_KEY] = settings
        return doc

    def update(self, settable, full):
        """Bring the Patron's status up-to-date with the given document.
        Right now this means making sure Patron.synchronize_annotations
        is up to date.
        """
        key = self.SYNCHRONIZE_ANNOTATIONS
        if key in settable:
            self.patron.synchronize_annotations = settable[key]
