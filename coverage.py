from nose.tools import set_trace
import datetime
import logging

from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func

from model import (
    get_one,
    get_one_or_create,
    BaseCoverageRecord,
    Collection,
    CollectionMissing,
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    Timestamp,
    Work,
    WorkCoverageRecord,
)
from metadata_layer import (
    ReplacementPolicy
)

import log # This sets the appropriate log format.

class CoverageFailure(object):
    """Object representing the failure to provide coverage."""

    def __init__(self, obj, exception, data_source=None, transient=True):
        self.obj = obj
        self.data_source = data_source
        self.exception = exception
        self.transient = transient

    def __repr__(self):
        if self.data_source:
            data_source = self.data_source.name
        else:
            data_source = None
        return "<CoverageFailure: obj=%r data_source=%r transient=%r exception=%r>" % (
            self.obj, data_source, self.transient, self.exception
        )

    def to_coverage_record(self, operation=None):
        """Convert this failure into a CoverageRecord."""
        if not self.data_source:
            raise Exception(
                "Cannot convert coverage failure to CoverageRecord because it has no output source."
            )

        record, ignore = CoverageRecord.add_for(
            self.obj, self.data_source, operation=operation
        )
        record.exception = self.exception
        if self.transient:
            record.status = CoverageRecord.TRANSIENT_FAILURE
        else:
            record.status = CoverageRecord.PERSISTENT_FAILURE
        return record

    def to_work_coverage_record(self, operation):
        """Convert this failure into a WorkCoverageRecord."""
        record, ignore = WorkCoverageRecord.add_for(
            self.obj, operation=operation
        )
        record.exception = self.exception
        if self.transient:
            record.status = CoverageRecord.TRANSIENT_FAILURE
        else:
            record.status = CoverageRecord.PERSISTENT_FAILURE
        return record


class BaseCoverageProvider(object):

    """Run certain objects through an algorithm. If the algorithm returns
    success, add a CoverageRecord for that object, so the object
    doesn't need to be processed again. If the algorithm returns a
    CoverageFailure, that failure may itself be memorialized as a
    CoverageRecord.

    Instead of instantiating this class directly, subclass one of its
    subclasses: either IdentifierCoverageProvider or
    WorkCoverageProvider.

    In IdentifierCoverageProvider the 'objects' are Identifier objects
    and the coverage records are CoverageRecord objects. In
    WorkCoverageProvider the 'objects' are Work objects and the
    coverage records are WorkCoverageRecord objects.
    """

    # In your subclass, set this to the name of the service,
    # e.g. "Overdrive Bibliographic Coverage Provider".
    SERVICE_NAME = None

    # In your subclass, you _may_ set this to a string that distinguishes
    # two different CoverageProviders from the same data source.
    OPERATION = None
    
    # The database session will be committed each time the
    # BaseCoverageProvider has (attempted to) provide coverage to this
    # number of Identifiers. You may change this in your subclass.
    # It's also possible to change it by passing in a value for
    # `batch_size` in the constructor, but generally nobody bothers
    # doing this.
    DEFAULT_BATCH_SIZE = 100
    
    def __init__(self, _db, service_name, operation, batch_size=None, 
                 cutoff_time=None):
        """Constructor.

        :param service_name: The name of the service that is providing
        coverage. Used in log messages and Timestamp objects.

        :param operation: An optional operation being performed by the
        service. This lets one service perform multiple operations.

        :batch_size: The maximum number of objects that will be processed
        at once.

        :param cutoff_time: Coverage records created before this time
        will be treated as though they did not exist.
        """
        self._db = _db
        if operation:
            service_name += ' (%s)' % operation
        self.service_name = service_name
        self.operation = operation
        if not batch_size or batch_size < 0:
            batch_size = self.DEFAULT_BATCH_SIZE
        self.batch_size = batch_size
        self.cutoff_time = cutoff_time
        self.collection = None
        
    @property
    def log(self):
        if not hasattr(self, '_log'):
            self._log = logging.getLogger(self.service_name)
        return self._log        
   
    def run(self):
        self.run_once_and_update_timestamp()

    def run_once_and_update_timestamp(self):
        # First cover items that have never had a coverage attempt
        # before.
        offset = 0
        while offset is not None:
            offset = self.run_once(
                offset, count_as_covered=BaseCoverageRecord.ALL_STATUSES
            )

        # Next, cover items that failed with a transient failure
        # on a previous attempt.
        offset = 0
        while offset is not None:
            offset = self.run_once(
                offset, 
                count_as_covered=BaseCoverageRecord.DEFAULT_COUNT_AS_COVERED
            )
        
        Timestamp.stamp(self._db, self.service_name, self.collection)
        self._db.commit()

    def run_once(self, offset, count_as_covered=None):
        count_as_covered = count_as_covered or BaseCoverageRecord.DEFAULT_COUNT_AS_COVERED
        # Make it clear which class of items we're covering on this
        # run.
        count_as_covered_message = '(counting %s as covered)' % (', '.join(count_as_covered))

        qu = self.items_that_need_coverage(count_as_covered=count_as_covered)
        self.log.info("%d items need coverage%s", qu.count(), 
                      count_as_covered_message)
        batch = qu.limit(self.batch_size).offset(offset)

        if not batch.count():
            # The batch is empty. We're done.
            return None
        (successes, transient_failures, persistent_failures), results = (
            self.process_batch_and_handle_results(batch)
        )

        if BaseCoverageRecord.SUCCESS not in count_as_covered:
            # If any successes happened in this batch, increase the
            # offset to ignore them, or they will just show up again
            # the next time we run this batch.
            offset += successes

        if BaseCoverageRecord.TRANSIENT_FAILURE not in count_as_covered:
            # If any transient failures happened in this batch,
            # increase the offset to ignore them, or they will
            # just show up again the next time we run this batch.
            offset += transient_failures

        if BaseCoverageRecord.PERSISTENT_FAILURE not in count_as_covered:
            # If any persistent failures happened in this batch,
            # increase the offset to ignore them, or they will
            # just show up again the next time we run this batch.
            offset += persistent_failures
        
        return offset

    def process_batch_and_handle_results(self, batch):
        """:return: A 2-tuple (counts, records). 

        `counts` is a 3-tuple (successes, transient failures,
        persistent_failures).

        `records` is a mixed list of CoverageRecord objects (for
        successes and persistent failures) and CoverageFailure objects
        (for transient failures).
        """

        # Batch is a query that may not be ordered, so it may return
        # different results when executed multiple times. Converting to
        # a list ensures that all subsequent code will run on the same items.
        batch = list(batch)

        offset_increment = 0
        results = self.process_batch(batch)
        successes = 0
        transient_failures = 0
        persistent_failures = 0
        num_ignored = 0
        records = []

        unhandled_items = set(batch)
        for item in results:
            if isinstance(item, CoverageFailure):
                if item.obj in unhandled_items:
                    unhandled_items.remove(item.obj)
                record = self.record_failure_as_coverage_record(item)
                if item.transient:
                    self.log.warn(
                        "Transient failure covering %r: %s", 
                        item.obj, item.exception
                    )

                    record.status = BaseCoverageRecord.TRANSIENT_FAILURE
                    transient_failures += 1
                else:
                    self.log.error(
                        "Persistent failure covering %r: %s", 
                        item.obj, item.exception
                    )
                    record.status = BaseCoverageRecord.PERSISTENT_FAILURE
                    persistent_failures += 1
            else:
                # Count this as a success and add a CoverageRecord for
                # it. It won't show up anymore, on this run or
                # subsequent runs.
                if item in unhandled_items:
                    unhandled_items.remove(item)
                successes += 1
                record, ignore = self.add_coverage_record_for(item)
                record.status = BaseCoverageRecord.SUCCESS
                record.exception = None
            records.append(record)

        # Perhaps some records were ignored--they neither succeeded nor
        # failed. Treat them as transient failures.
        for item in unhandled_items:
            self.log.warn(
                "%r was ignored by a coverage provider that was supposed to cover it.", item
            )
            failure = self.failure_for_ignored_item(item)
            record = self.record_failure_as_coverage_record(failure)
            record.status = BaseCoverageRecord.TRANSIENT_FAILURE
            records.append(record)
            num_ignored += 1

        self.log.info(
            "Batch processed with %d successes, %d transient failures, %d persistent failures, %d ignored.",
            successes, transient_failures, persistent_failures, num_ignored
        )

        # Finalize this batch before moving on to the next one.
        self.finalize_batch()

        # For all purposes outside this method, treat an ignored identifier
        # as a transient failure.
        transient_failures += num_ignored
        
        return (successes, transient_failures, persistent_failures), records

    def process_batch(self, batch):
        """Do what it takes to give CoverageRecords to a batch of
        items.

        :return: A mixed list of CoverageRecords and CoverageFailures.
        """
        results = []
        for item in batch:
            result = self.process_item(item)
            if not isinstance(result, CoverageFailure):
                self.handle_success(item)
            results.append(result)
        return results

    def handle_success(self, item):
        """Do something special to mark the successful coverage of the
        given item.
        """
        pass

    def should_update(self, coverage_record):
        """Should we do the work to update the given CoverageRecord?"""
        if coverage_record is None:
            # An easy decision -- there is no existing CoverageRecord,
            # so we need to do the work.
            return True

        if self.cutoff_time is None:
            # An easy decision -- without a cutoff_time, once we
            # create a CoverageRecord we never update it.
            return False

        # We update a CoverageRecord if it was last updated before
        # cutoff_time.
        return coverage_record.timestamp < self.cutoff_time

    def finalize_batch(self):
        """Do whatever is necessary to complete this batch before moving on to
        the next one.
        
        e.g. committing the database session or uploading a bunch of
        assets to S3.
        """
        self._db.commit()

    #
    # Subclasses must implement these virtual methods.
    #

    def items_that_need_coverage(self, identifiers, **kwargs):
        """Create a database query returning only those items that
        need coverage.

        :param subset: A list of Identifier objects. If present, return
        only items that need coverage *and* are associated with one
        of these identifiers.

        Implemented in CoverageProvider and WorkCoverageProvider.
        """
        raise NotImplementedError()

    def add_coverage_record_for(self, item):
        """Add a coverage record for the given item.

        Implemented in CoverageProvider and WorkCoverageProvider.
        """
        raise NotImplementedError()
        
    def record_failure_as_coverage_record(self, failure):
        """Convert the given CoverageFailure to a coverage record.

        Implemented in CoverageProvider and WorkCoverageProvider.
        """
        raise NotImplementedError()

    def failure_for_ignored_item(self, work):
        """Create a CoverageFailure recording the coverage provider's
        failure to even try to process an item.

        Implemented in CoverageProvider and WorkCoverageProvider.
        """
        raise NotImplementedError()

    def process_item(self, item):
        """Do the work necessary to give coverage to one specific item.

        Since this is where the actual work happens, this is not
        implemented in CoverageProvider or WorkCoverageProvider, and
        must be handled in a subclass.
        """
        raise NotImplementedError()


class IdentifierCoverageProvider(BaseCoverageProvider):

    """Run Identifiers of certain types (ISBN, Overdrive, OCLC Number,
    etc.) through an algorithm associated with a certain DataSource.

    This class is designed to be subclassed rather than instantiated
    directly. Subclasses should define SERVICE_NAME, OPERATION
    (optional), DATA_SOURCE_NAME, and
    INPUT_IDENTIFIER_TYPES. SERVICE_NAME and OPERATION are described
    in BaseCoverageProvider; the rest are described in appropriate
    comments in this class.
    """
    # In your subclass, set this to the name of the data source you
    # consult when providing coverage, e.g. DataSource.OVERDRIVE.
    DATA_SOURCE_NAME = None

    # In your subclass, set this to a single identifier type, or a list
    # of identifier types. The CoverageProvider will attempt to give
    # coverage to every Identifier of this type.
    #
    # Setting this to None will attempt to give coverage to every single
    # Identifier in the system, which is probably not what you want.
    NO_SPECIFIED_TYPES = object()
    INPUT_IDENTIFIER_TYPES = NO_SPECIFIED_TYPES
    
    def __init__(self, _db, collection=None, input_identifiers=None,
                 **kwargs):
        """Constructor.

        :param collection: Optional. If information comes in from a
           third party about a license pool associated with an
           Identifier, the LicensePool that belongs to this Collection
           will be used to contain that data. You may pass in None for
           this value, but that means that no circulation information
           (such as the formats in which a book is available) will be
           stored as a result of running this CoverageProvider. Only
           bibliographic information will be stored.
        :param input_identifiers: Optional. This CoverageProvider is
           requested to provide coverage for these specific
           Identifiers.
        """
        super(CoverageProvider, self).__init__(
            _db, service_name=self.SERVICE_NAME, operation=operation, **kwargs
        )

        self.collection = collection
        self.input_identifiers = input_identifiers

        # Get this information immediately so that an error happens immediately
        # if INPUT_IDENTIFIER_TYPES is not set properly.
        self.input_identifier_types = self._input_identifier_types()

    @classmethod
    def _input_identfier_types(cls):
        """Create a normalized value for `input_identifier_types`
        based on the INPUT_IDENTIFIER_TYPES class variable.
        """
        value = cls.INPUT_IDENTIFIER_TYPES

        # Nip in the bud a situation where someone subclassed this
        # class without thinking about a value for
        # INPUT_IDENTIFIER_TYPES.
        if value is self.NO_SPECIFIED_TYPES:
            raise ValueError(
                "%s must define INPUT_IDENTIFIER_TYPES, even if the value is None." % (cls.__name__)
            )

        if not value:
            # We will be processing every single type of identifier in
            # the system. This (hopefully) means that the identifiers
            # are restricted in some other way, such as being licensed
            # to a specific Collection.
            return None
        elif not isinstance(value, list):
            # We will be processing every identifier of a given type.
            return [value]
        else:
            # We will be processing every identify whose type belongs to
            # a list of types.
            return value

    @property
    def data_source(self):
        """Look up the DataSource object corresponding to the
        service we're running this data through.

        Out of an excess of caution, we look up the DataSource every
        time, rather than storing it, in case a CoverageProvider is
        ever used in an environment where the database session is
        scoped (e.g. the circulation manager).
        """
        return DataSource.lookup(self._db, self.DATA_SOURCE_NAME)
        
    def run_on_specific_identifiers(self, identifiers):
        """Split a specific set of Identifiers into batches and process one
        batch at a time.

        This is for use by IdentifierInputScript.

        :return: The same (counts, records) 2-tuple as
            process_batch_and_handle_results.
        """
        index = 0
        successes = 0
        transient_failures = 0
        persistent_failures = 0
        records = []

        # Of all the items that need coverage, find the intersection
        # with the given list of items.
        need_coverage = self.items_that_need_coverage(identifiers).all()

        # Treat any items with up-to-date coverage records as
        # automatic successes.
        #
        # NOTE: We won't actually be returning those coverage records
        # in `records`, since items_that_need_coverage() filters them
        # out, but nobody who calls this method really needs those
        # records.
        automatic_successes = len(identifiers) - len(need_coverage)
        successes += automatic_successes
        self.log.info("%d automatic successes.", successes)

        # Iterate over any items that were not automatic
        # successes.
        while index < len(need_coverage):
            batch = need_coverage[index:index+self.batch_size]
            (s, t, p), r = self.process_batch_and_handle_results(batch)
            successes += s
            transient_failures += t
            persistent_failures += p
            records += r
            index += self.batch_size
        return (successes, transient_failures, persistent_failures), records

    def ensure_coverage(self, item, force=False):
        """Ensure coverage for one specific item.

        :param item: This should always be an Identifier, but this
        code will also work if it's an Edition. (The Edition's
        .primary_identifier will be covered.)
        :param force: Run the coverage code even if an existing
           coverage record for this item was created after
           `self.cutoff_time`.
        :return: Either a coverage record or a CoverageFailure.
        """
        if isinstance(item, Identifier):
            identifier = item
        else:
            identifier = item.primary_identifier
        coverage_record = get_one(
            self._db, CoverageRecord,
            identifier=identifier,
            data_source=self.data_source,
            operation=self.operation,
            on_multiple='interchangeable',
        )
        if not force and not self.should_update(coverage_record):
            return coverage_record

        counts, records = self.process_batch_and_handle_results(
            [identifier]
        )
        if records:
            coverage_record = records[0]
        else:
            coverage_record = None
        return coverage_record
    
    def edition(self, identifier):
        """Finds or creates an Edition representing this coverage provider's
        view of a given Identifier.
        """
        edition, ignore = Edition.for_foreign_id(
            self._db, self.data_source, identifier.type,
            identifier.identifier
        )
        return edition

    def set_metadata(self, identifier, metadata, 
                     metadata_replacement_policy=None):
        """Finds or creates the Edition for an Identifier, updates it
        with the given metadata.

        :return: The Identifier (if successful) or an appropriate
        CoverageFailure (if not).
        """
        metadata_replacement_policy = metadata_replacement_policy or (
            ReplacementPolicy.from_metadata_source()
        )

        edition = self.edition(identifier)
        if isinstance(edition, CoverageFailure):
            return edition

        if not metadata:
            e = "Did not receive metadata from input source"
            return CoverageFailure(
                identifier, e, data_source=self.data_source, transient=True
            )

        try:
            metadata.apply(
                edition, collection=self.collection,
                replace=metadata_replacement_policy,
            )
        except Exception as e:
            self.log.warn(
                "Error applying metadata to edition %d: %s",
                edition.id, e, exc_info=e
            )
            return CoverageFailure(
                identifier, repr(e), data_source=self.data_source,
                transient=True
            )

        return identifier

    #
    # Implementation of BaseCoverageProvider virtual methods.
    #

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Find all items lacking coverage from this CoverageProvider.

        Items should be Identifiers, though Editions should also work.

        By default, all identifiers of the `INPUT_IDENTIFIER_TYPES` which
        don't already have coverage are chosen.

        :param identifiers: The batch of identifier objects to test
            for coverage. identifiers and self.input_identifiers can
            intersect -- if this provider was created for the purpose
            of running specific Identifiers, and within those
            Identifiers you want to batch, you can use both
            parameters.
        """
        qu = Identifier.missing_coverage_from(
            self._db, self.input_identifier_types, self.data_source,
            count_as_missing_before=self.cutoff_time, operation=self.operation,
            identifiers=self.input_identifiers, **kwargs
        )

        if identifiers:
            qu = qu.filter(Identifier.id.in_([x.id for x in identifiers]))

        return qu

    def add_coverage_record_for(self, item):
        """Record this CoverageProvider's coverage for the given
        Edition/Identifier, as a CoverageRecord.
        """
        return CoverageRecord.add_for(
            item, data_source=self.data_source, operation=self.operation
        )

    def record_failure_as_coverage_record(self, failure):
        """Turn a CoverageFailure into a CoverageRecord object."""
        return failure.to_coverage_record(operation=self.operation)

    def failure_for_ignored_item(self, item):
        """Create a CoverageFailure recording the CoverageProvider's
        failure to even try to process an item.
        """
        return CoverageFailure(
            item, "Was ignored by CoverageProvider.", 
            data_source=self.data_source, transient=True
        )


class CollectionCoverageProvider(IdentifierCoverageProvider):
    """A CoverageProvider that covers all the Identifiers currently
    licensed to a given Collection.

    You should subclass this CoverageProvider if you want to create
    Works (as opposed to operating on existing Works) or update the
    circulation information for LicensePools. You can't use it to
    create new LicensePools, since it only operates on Identifiers
    that already have a LicencePool in the given Collection.

    If a book shows up in multiple Collections, the first Collection
    to process it takes care of it for the others. Any books that were
    processed through their membership in another Collection will be
    left alone.

    For this reason it's important that subclasses of this
    CoverageProvider only deal with bibliographic information, and
    never circulation information. (Circulation information includes
    formatting information and links to open-access downloads.)

    In addition to defining the class variables defined by
    CoverageProvider, you must define the class variable PROTOCOL when
    subclassing this class.
    """
    # By default, this type of CoverageProvider will provide coverage to
    # all Identifiers in the given Collection, regardless of their type.
    INPUT_IDENTIFIER_TYPES = None
    
    DEFAULT_BATCH_SIZE = 10

    # Set this to the name of the protocol managed by this type of
    # CoverageProvider.
    PROTOCOL = None
    
    def __init__(self, collection, **kwargs):
        _db = Session.object_session(collection)
        if not collection:
            raise collectionMissing(
                "CollectionCoverageProvider must be instantiated with "
                "a Collection."
            )
        self.collection = collection
        super(CollectionCoverageProvider, self).__init__(
            _db, collection, **kwargs
        )

    @classmethod
    def all(cls, _db, **kwargs):
        """Yield a sequence of CollectionCoverageProvider instances, one for
        every Collection that implements cls.PROTOCOL.

        CollectionCoverageProviders will be yielded in a random order.

        :param kwargs: Keyword arguments passed into the constructor for
        CollectionCoverageProvider (or, more likely, one of its subclasses).
        """
        if not cls.PROTOCOL:
            raise ValueError("%s must define PROTOCOL." % cls.__name__)
        collections = _db.query(Collection).filter(
            Collection.protocol==cls.PROTOCOL).order_by(func.random())
        for collection in collections:
            yield cls(_db, collection=collection, **kwargs)
        
    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Find all Identifiers associated with this Collection but lacking
        coverage through this CoverageProvider.
        """
        qu = super(CollectionCoverageProvider, self).items_that_need_coverage(
            identifiers, **kwargs
        )
        qu = qu.join(Identifier.licensed_through).filter(
            LicencePool.collection_id==self.collection_id
        )
        return qu
        
    def license_pool(self, identifier):
        """Finds this Collection's LicensePool for the given Identifier,
        creating one if necessary.
        """
        license_pools = [p for p in identifier.licensed_through
                         if self.collection==p.collection]
            
        if license_pools:
            # A given Collection may have at most one LicensePool for
            # a given identifier.
            return license_pools[0]

        # This Collection has no LicensePool for the given Identifier.
        # Create one.
        #
        # Under normal circumstances, this will never happen, because
        # CollectionCoverageProvider only operates on Identifiers that
        # already have a LicensePool in this Collection.
        license_pool, ignore = LicensePool.for_foreign_id(
            self._db, self.data_source, identifier.type, 
            identifier.identifier, collection=self.collection
        )
        return license_pool

    def work(self, identifier):
        """Finds or creates a Work for this Identifier as licensed through
        this Collection.

        If the given Identifier already has a Work associated with it,
        that Work will always be used, since an Identifier can only have one
        Work associated with it.
        
        However, if there is no current Work, a Work will only be
        created if the given Identifier already has a LicensePool in
        the Collection associated with this CoverageProvider. This method
        will not create new LicensePools.

        :return: A Work, if possible. Otherwise, a CoverageFailure explaining
        why no Work could be created.
        """
        work = identifier.work
        if work:
            # There is already a Work associated with this Identifier.
            # Return it.
            return identifier.work

        # There is no Work associated with this Identifier. This means
        # we need to create one. Since we can only create a Work from
        # a LicensePool, beyond this point the CoverageProvider
        # needs to have a Collection associated with it.
        error = None
        pool = None
        pool, ignore = LicensePool.for_foreign_id(
            self._db, self.data_source, identifier.type, 
            identifier.identifier, collection=self.collection,
            autocreate=False
        )
            
        if pool:
            work, created = pool.calculate_work(even_if_no_author=True)
            if not work:
                error = "Work could not be calculated"
        else:
            error = "Cannot locate LicensePool"
                
        if error:
            return CoverageFailure(
                identifier, error, data_source=self.data_source,
                transient=True
            )
        return work
    
    def set_metadata_and_circulation_data(
            self, identifier, metadata, circulationdata, 
            metadata_replacement_policy=None, 
            circulationdata_replacement_policy=None, 
    ):
        """Makes sure that the given Identifier has a Work, Edition (in the
        context of this Collection), and LicensePool (ditto), and that
        all the information is up to date.

        :return: The Identifier (if successful) or an appropriate
        CoverageFailure (if not).
        """

        if not metadata and not circulationdata:
            e = "Received neither metadata nor circulation data from input source"
            return CoverageFailure(
                identifier, e, data_source=self.data_source, transient=True
            )

        if metadata:
            result = self.set_metadata(
                identifier, metadata, metadata_replacement_policy
            )
            if isinstance(result, CoverageFailure):
                return result

        if circulationdata:
            result = self._set_circulationdata(
                identifier, circulationdata, circulationdata_replacement_policy
            )
            if isinstance(result, CoverageFailure):
                return result

        # By this point the Identifier should have an appropriate
        # Edition and LicensePool. We should now be able to make a
        # Work.
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work

        return identifier

    def _set_circulationdata(self, identifier, circulationdata, 
                     circulationdata_replacement_policy=None
    ):
        """Finds or creates a LicensePool for an Identifier, updates it
        with the given circulationdata, then creates a Work for the book.

        :return: The Identifier (if successful) or an appropriate
        CoverageFailure (if not).
        """
        circulationdata_replacement_policy = circulationdata_replacement_policy or (
            ReplacementPolicy.from_license_source()
        )

        pool = self.license_pool(identifier)
        if isinstance(pool, CoverageFailure):
            return pool

        if not circulationdata:
            e = "Did not receive circulationdata from input source"
            return CoverageFailure(identifier, e, data_source=self.data_source, transient=True)

        try:
            circulationdata.apply(
                pool, replace=circulationdata_replacement_policy,
            )
        except Exception as e:
            self.log.warn(
                "Error applying circulationdata to pool %d: %s",
                pool.id, e, exc_info=e
            )
            return CoverageFailure(identifier, repr(e), data_source=self.data_source, transient=True)

        return identifier

    def set_presentation_ready(self, identifier):
        """Set a Work presentation-ready."""
        work = self.work(identifier)
        if isinstance(work, CoverageFailure):
            return work
        work.set_presentation_ready()
        return identifier


class BibliographicCoverageProvider(CollectionCoverageProvider):
    """Fill in bibliographic metadata for all books in a Collection.

    e.g. ensures that we get Overdrive coverage for all Overdrive IDs
    in a collection.

    TODO: The current BibliographicCoverageProviders deal with
    circulation information, which is now a no-no. I'm not going to
    address the issue in this branch.
    """
    def __init__(self, collection, metadata_replacement_policy=None, **kwargs):
        self.metadata_replacement_policy = (
            metadata_replacement_policy
            or ReplacementPolicy.from_metadata_source()
        )

        super(BibliographicCoverageProvider, self).__init__(
            collection, **kwargs
        )

    def handle_success(self, identifier):
        """Once a book has bibliographic coverage, it can be given a
        work and made presentation ready.
        """
        self.set_presentation_ready(identifier)


class WorkCoverageProvider(BaseCoverageProvider):

    #
    # Implementation of BaseCoverageProvider virtual methods.
    #

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        """Find all Works lacking coverage from this CoverageProvider.

        By default, all Works which don't already have coverage are
        chosen.

        :param: Only Works connected with one of the given identifiers
        are chosen.
        """
        qu = Work.missing_coverage_from(
            self._db, operation=self.operation, 
            count_as_missing_before=self.cutoff_time,
            **kwargs
        )
        if identifiers:
            ids = [x.id for x in identifiers]
            qu = qu.join(Work.license_pools).filter(
                LicensePool.identifier_id.in_(ids)
            )
        return qu

    def failure_for_ignored_item(self, work):
        """Create a CoverageFailure recording the WorkCoverageProvider's
        failure to even try to process a Work.
        """
        return CoverageFailure(
            work, "Was ignored by WorkCoverageProvider.", transient=True
        )

    def add_coverage_record_for(self, work):
        """Record this CoverageProvider's coverage for the given
        Edition/Identifier, as a WorkCoverageRecord.
        """
        return WorkCoverageRecord.add_for(
            work, operation=self.operation
        )

    def record_failure_as_coverage_record(self, failure):
        """Turn a CoverageFailure into a WorkCoverageRecord object."""
        return failure.to_work_coverage_record(operation=self.operation)
