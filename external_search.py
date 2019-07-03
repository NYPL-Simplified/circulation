from collections import defaultdict
import contextlib
import datetime
from nose.tools import set_trace
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as elasticsearch_bulk
from elasticsearch.exceptions import (
    RequestError,
    ElasticsearchException,
)
from elasticsearch_dsl import (
    Index,
    Search,
    Q,
)
from elasticsearch_dsl.query import (
    Bool,
    Query as BaseQuery
)

from flask_babel import lazy_gettext as _
from config import (
    Configuration,
    CannotLoadConfiguration,
)
from classifier import (
    KeywordBasedClassifier,
    GradeLevelClassifier,
    AgeClassifier,
)
from facets import FacetConstants
from model import (
    numericrange_to_tuple,
    Collection,
    Contributor,
    ConfigurationSetting,
    DataSource,
    Edition,
    ExternalIntegration,
    Library,
    Work,
    WorkCoverageRecord,
)
from lane import Pagination
from monitor import WorkSweepMonitor
from coverage import (
    CoverageFailure,
    WorkPresentationProvider,
)
from problem_details import INVALID_INPUT
from selftest import (
    HasSelfTests,
    SelfTestResult,
)
from util.problem_detail import ProblemDetail

import os
import logging
import re
import time

@contextlib.contextmanager
def mock_search_index(mock=None):
    """Temporarily mock the ExternalSearchIndex implementation
    returned by the load() class method.
    """
    try:
        ExternalSearchIndex.MOCK_IMPLEMENTATION = mock
        yield mock
    finally:
        ExternalSearchIndex.MOCK_IMPLEMENTATION = None


class ExternalSearchIndex(HasSelfTests):

    NAME = ExternalIntegration.ELASTICSEARCH

    # A test may temporarily set this to a mock of this class.
    # While that's true, load() will return the mock instead of
    # instantiating new ExternalSearchIndex objects.
    MOCK_IMPLEMENTATION = None

    WORKS_INDEX_PREFIX_KEY = u'works_index_prefix'
    DEFAULT_WORKS_INDEX_PREFIX = u'circulation-works'

    TEST_SEARCH_TERM_KEY = u'test_search_term'
    DEFAULT_TEST_SEARCH_TERM = u'test'

    work_document_type = 'work-type'
    __client = None

    CURRENT_ALIAS_SUFFIX = 'current'
    VERSION_RE = re.compile('-v([0-9]+)$')

    SETTINGS = [
        { "key": ExternalIntegration.URL, "label": _("URL"), "required": True, "format": "url" },
        { "key": WORKS_INDEX_PREFIX_KEY, "label": _("Index prefix"),
          "default": DEFAULT_WORKS_INDEX_PREFIX,
          "required": True,
          "description": _("Any Elasticsearch indexes needed for this application will be created with this unique prefix. In most cases, the default will work fine. You may need to change this if you have multiple application servers using a single Elasticsearch server.")
        },
        { "key": TEST_SEARCH_TERM_KEY,
          "label": _("Test search term"),
          "default": DEFAULT_TEST_SEARCH_TERM,
          "description": _("Self tests will use this value as the search term.")
        }
    ]

    SITEWIDE = True

    @classmethod
    def reset(cls):
        """Resets the __client object to None so a new configuration
        can be applied during object initialization.

        This method is only intended for use in testing.
        """
        cls.__client = None

    @classmethod
    def search_integration(cls, _db):
        """Look up the ExternalIntegration for ElasticSearch."""
        return ExternalIntegration.lookup(
            _db, ExternalIntegration.ELASTICSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL
        )

    @classmethod
    def works_prefixed(cls, _db, value):
        """Prefix the given value with the prefix to use when generating index
        and alias names.

        :return: A string "{prefix}-{value}", or None if no prefix is
        configured.
        """
        integration = cls.search_integration(_db)
        if not integration:
            return None
        setting = integration.setting(cls.WORKS_INDEX_PREFIX_KEY)
        prefix = setting.value_or_default(cls.DEFAULT_WORKS_INDEX_PREFIX)
        return prefix + '-' + value

    def works_index_name(self, _db):
        """Look up the name of the search index.

        It's possible, but unlikely, that the search index alias will
        point to some other index. But if there were no indexes, and a
        new one needed to be created, this would be the name of that
        index.
        """
        return self.works_prefixed(_db, self.mapping.version_name())

    @classmethod
    def works_alias_name(cls, _db):
        """Look up the name of the search index alias."""
        return cls.works_prefixed(_db, cls.CURRENT_ALIAS_SUFFIX)

    @classmethod
    def load(cls, _db, *args, **kwargs):
        """Load a generic implementation."""
        if cls.MOCK_IMPLEMENTATION:
            return cls.MOCK_IMPLEMENTATION
        return cls(_db, *args, **kwargs)

    def __init__(self, _db, url=None, works_index=None, test_search_term=None,
                 in_testing=False, mapping=None):
        """Constructor

        :param in_testing: Set this to true if you don't want an
        Elasticsearch client to be created, e.g. because you're
        running a unit test of the constructor.


        :param mapping: A custom Mapping object, for use in unit tests. By
        default, the most recent mapping will be used.
        """
        self.log = logging.getLogger("External search index")
        self.works_index = None
        self.works_alias = None
        integration = None

        self.mapping = mapping or Mapping.latest()

        if not _db:
            raise CannotLoadConfiguration(
                "Cannot load Elasticsearch configuration without a database.",
            )
        if not url or not works_index:
            integration = self.search_integration(_db)
            if not integration:
                raise CannotLoadConfiguration(
                    "No Elasticsearch integration configured."
                )
            url = url or integration.url
            if not works_index:
                works_index = self.works_index_name(_db)
            test_search_term = integration.setting(
                self.TEST_SEARCH_TERM_KEY).value
        if not url:
            raise CannotLoadConfiguration(
                "No URL configured to Elasticsearch server."
            )
        self.test_search_term = (
            test_search_term or self.DEFAULT_TEST_SEARCH_TERM
        )
        if not in_testing:
            if not ExternalSearchIndex.__client:
                use_ssl = url.startswith('https://')
                self.log.info(
                    "Connecting to index %s in Elasticsearch cluster at %s",
                    works_index, url
                )
                ExternalSearchIndex.__client = Elasticsearch(
                    url, use_ssl=use_ssl, timeout=20, maxsize=25
                )

            self.indices = self.__client.indices
            self.index = self.__client.index
            self.delete = self.__client.delete
            self.exists = self.__client.exists
            self.put_script = self.__client.put_script

        # Sets self.works_index and self.works_alias values.
        # Document upload runs against the works_index.
        # Search queries run against works_alias.
        if works_index and integration and not in_testing:
            try:
                self.set_works_index_and_alias(_db)
            except RequestError, e:
                # This is almost certainly a problem with our code,
                # not a communications error.
                raise e
            except ElasticsearchException, e:
                raise CannotLoadConfiguration(
                    "Exception communicating with Elasticsearch server: %s" %
                    repr(e)
                )

        self.search = Search(using=self.__client, index=self.works_alias)

        def bulk(docs, **kwargs):
            return elasticsearch_bulk(self.__client, docs, **kwargs)
        self.bulk = bulk

    def set_works_index_and_alias(self, _db):
        """Finds or creates the works_index and works_alias based on
        the current configuration.
        """
        # The index name to use is the one known to be right for this
        # version.
        self.works_index = self.__client.works_index = self.works_index_name(_db)
        if not self.indices.exists(self.works_index):
            # That index doesn't actually exist. Set it up.
            self.setup_index()

        # Make sure the alias points to the most recent index.
        self.setup_current_alias(_db)

        # Make sure the stored scripts for the latest mapping exist.
        self.set_stored_scripts()

    def setup_current_alias(self, _db):
        """Finds or creates the works_alias as named by the current site
        settings.

        If the resulting alias exists and is affixed to a different
        index or if it can't be generated for any reason, the alias will
        not be created or moved. Instead, the search client will use the
        the works_index directly for search queries.
        """
        alias_name = self.works_alias_name(_db)
        alias_is_set = self.indices.exists_alias(name=alias_name)

        def _use_as_works_alias(name):
            self.works_alias = self.__client.works_alias = name

        if alias_is_set:
            # The alias exists on the Elasticsearch server, so it must
            # point _somewhere.
            exists_on_works_index = self.indices.exists_alias(
                index=self.works_index, name=alias_name
            )
            if exists_on_works_index:
                # It points to the index we were expecting it to point to.
                # Use it.
                _use_as_works_alias(alias_name)
            else:
                # The alias exists but it points somewhere we didn't
                # expect. Rather than changing how the alias works and
                # then using the alias, use the index directly instead
                # of going through the alias.
                _use_as_works_alias(self.works_index)
            return

        # Create the alias and search against it.
        response = self.indices.put_alias(
            index=self.works_index, name=alias_name
        )
        if not response.get('acknowledged'):
            self.log.error("Alias '%s' could not be created", alias_name)
            # Work against the index instead of an alias.
            _use_as_works_alias(self.works_index)
            return
        _use_as_works_alias(alias_name)

    def setup_index(self, new_index=None, **index_settings):
        """Create the search index with appropriate mapping.

        This will destroy the search index, and all works will need
        to be indexed again. In production, don't use this on an
        existing index. Use it to create a new index, then change the
        alias to point to the new index.
        """
        index_name = new_index or self.works_index
        if self.indices.exists(index_name):
            self.indices.delete(index_name)

        self.log.info("Creating index %s", index_name)
        body = self.mapping.body()
        body.setdefault('settings', {}).update(index_settings)
        index = self.indices.create(index=index_name, body=body)

    def set_stored_scripts(self):
        for name, definition in self.mapping.stored_scripts():
            # Make sure the name of the script is scoped and versioned.
            if not name.startswith("simplified."):
                name = self.mapping.script_name(name)

            # If only the source code was provided, configure it as a
            # Painless script.
            if isinstance(definition, basestring):
                definition = dict(script=dict(lang="painless", source=definition))

            # Put it in the database.
            self.put_script(name, definition)

    def transfer_current_alias(self, _db, new_index):
        """Force -current alias onto a new index"""
        if not self.indices.exists(index=new_index):
            raise ValueError(
                "Index '%s' does not exist on this client." % new_index)

        current_base_name = self.base_index_name(self.works_index)
        new_base_name = self.base_index_name(new_index)

        if new_base_name != current_base_name:
            raise ValueError(
                ("Index '%s' is not in series with current index '%s'. "
                 "Confirm the base name (without version number) of both indices"
                 "is the same.") % (new_index, self.works_index))

        self.works_index = self.__client.works_index = new_index
        alias_name = self.works_alias_name(_db)

        exists = self.indices.exists_alias(name=alias_name)
        if not exists:
            # The alias doesn't already exist. Set it.
            self.setup_current_alias(_db)
            return

        # We know the alias already exists. Before we set it to point
        # to self.works_index, we may need to remove it from some
        # other indices.
        other_indices = self.indices.get_alias(name=alias_name).keys()

        if self.works_index in other_indices:
            # If the alias already points to the works index,
            # that's fine -- we want to see if it points to any
            # _other_ indices.
            other_indices.remove(self.works_index)

        if other_indices:
            # The alias exists on one or more other indices.  Remove
            # the alias altogether, then put it back on the works
            # index.
            self.indices.delete_alias(index='_all', name=alias_name)
            self.indices.put_alias(
                index=self.works_index, name=alias_name
            )

        self.works_alias = self.__client.works_alias = alias_name

    def base_index_name(self, index_or_alias):
        """Removes version or current suffix from base index name"""

        current_re = re.compile(self.CURRENT_ALIAS_SUFFIX+'$')
        base_works_index = re.sub(current_re, '', index_or_alias)
        base_works_index = re.sub(self.VERSION_RE, '', base_works_index)

        return base_works_index

    def create_search_doc(self, query_string, filter, pagination,
                          debug):

        query = Query(query_string, filter)
        query_without_filter = Query(query_string)
        search = query.build(self.search, pagination)
        if debug:
            search = search.extra(explain=True)

        fields = None
        if debug:
            # Don't restrict the fields at all -- get everything.
            # This makes it easy to investigate everything about the
            # results we do get.
            fields = ['*']
        else:
            # All we absolutely need is the work ID, which is a
            # key into the database, plus the values of any script fields,
            # which represent data not available through the database.
            fields = ["work_id"]
            if filter:
                fields += filter.script_fields.keys()

        # Change the Search object so it only retrieves the fields
        # we're asking for.
        if fields:
            search = search.source(fields)

        return search

    def query_works(self, query_string, filter=None, pagination=None,
                    debug=False):
        """Run a search query.

        :param query_string: The string to search for.
        :param filter: A Filter object, used to filter out works that
            would otherwise match the query string.
        :param pagination: A Pagination object, used to get a subset
            of the search results.
        :param debug: If this is True, debugging information will
            be gathered and logged. The search query will ask
            ElasticSearch for all available fields, not just the
            fields known to be used by the feed generation code.  This
            all comes at a slight performance cost.
        :return: A list of Hit objects containing information about
            the search results, including the values of any script fields
            calculated by ElasticSearch during the search process.
        """
        if not self.works_alias:
            return []

        if not pagination:
            pagination = Pagination.default()

        search = self.create_search_doc(query_string, filter=filter, pagination=pagination, debug=debug)
        start = pagination.offset
        stop = start + pagination.size

        function_scores = filter.scoring_functions if filter else None
        if function_scores:
            function_score = Q(
                'function_score',
                query=dict(match_all=dict()),
                functions=function_scores,
                score_mode="sum"
            )
            search = search.query(function_score)
        a = time.time()

        # NOTE: This is the code that actually executes the ElasticSearch
        # request.
        results = search[start:stop]
        if debug:
            b = time.time()
            self.log.info("Elasticsearch query completed in %.2fsec", b-a)
            for i, result in enumerate(results):
                self.log.debug(
                    '%02d "%s" (%s) work=%s score=%.3f shard=%s',
                    i, result.sort_title, result.sort_author, result.meta['id'],
                    result.meta['score'] or 0, result.meta['shard']
                )

        # Convert the Search object into a list of hits.
        results = [x for x in results]

        # Tell the Pagination object about this page -- this may help
        # it set up to generate a link to the next page.
        pagination.page_loaded(results)

        return results

    def count_works(self, filter):
        """Instead of retrieving works that match `filter`, count the total."""
        qu = self.create_search_doc(
            query_string=None, filter=filter, pagination=None, debug=False
        )
        return qu.count()

    def bulk_update(self, works, retry_on_batch_failure=True):
        """Upload a batch of works to the search index at once."""

        if not works:
            # There's nothing to do. Don't bother making any requests
            # to the search index.
            return [], []

        time1 = time.time()
        needs_add = []
        successes = []
        for work in works:
            needs_add.append(work)

        # Add/update any works that need adding/updating.
        docs = Work.to_search_documents(needs_add)

        for doc in docs:
            doc["_index"] = self.works_index
            doc["_type"] = self.work_document_type
        time2 = time.time()

        success_count, errors = self.bulk(
            docs,
            raise_on_error=False,
            raise_on_exception=False,
        )

        # If the entire update failed, try it one more time before
        # giving up on the batch.
        if len(errors) == len(docs):
            if retry_on_batch_failure:
                self.log.info("Elasticsearch bulk update timed out, trying again.")
                return self.bulk_update(needs_add, retry_on_batch_failure=False)
            else:
                docs = []

        time3 = time.time()
        self.log.info("Created %i search documents in %.2f seconds" % (len(docs), time2 - time1))
        self.log.info("Uploaded %i search documents in  %.2f seconds" % (len(docs), time3 - time2))

        doc_ids = [d['_id'] for d in docs]

        # We weren't able to create search documents for these works, maybe
        # because they don't have presentation editions yet.
        def get_error_id(error):
            return error.get('data', {}).get('_id', None) or error.get('index', {}).get('_id', None)
        error_ids = [get_error_id(error) for error in errors]

        missing_works = [
            work for work in works
            if work.id not in doc_ids and work.id not in error_ids
            and work not in successes
        ]

        successes.extend(
            [work for work in works
             if work.id in doc_ids and work.id not in error_ids]
        )

        failures = []
        for missing in missing_works:
            failures.append((work, "Work not indexed"))

        for error in errors:

            error_id = get_error_id(error)
            work = None
            works_with_error = [work for work in works if work.id == error_id]
            if works_with_error:
                work = works_with_error[0]

            exception = error.get('exception', None)
            error_message = error.get('error', None)
            if not error_message:
                error_message = error.get('index', {}).get('error', None)

            failures.append((work, error_message))

        self.log.info("Successfully indexed %i documents, failed to index %i." % (success_count, len(failures)))

        return successes, failures

    def remove_work(self, work):
        """Remove the search document for `work` from the search index.
        """
        args = dict(index=self.works_index, doc_type=self.work_document_type,
                    id=work.id)
        if self.exists(**args):
            self.delete(**args)

    def _run_self_tests(self, _db, in_testing=False):
        # Helper methods for setting up the self-tests:

        def _search():
            return self.create_search_doc(
                self.test_search_term, filter=None,
                pagination=None, debug=True
            )

        def _works():
            return self.query_works(
                self.test_search_term, filter=None, pagination=None,
                debug=False
            )

        # The self-tests:

        def _search_for_term():
            titles = [("%s (%s)" %(x.title, x.author)) for x in _works()]
            return titles

        yield self.run_test(
            ("Search results for '%s':" %(self.test_search_term)),
            _search_for_term
        )

        def _get_raw_doc():
            search = _search()
            if in_testing:
                if not len(search):
                    return str(search)
                search = search[0]
            return json.dumps(search.to_dict(), indent=1)

        yield self.run_test(
            ("Search document for '%s':" %(self.test_search_term)),
            _get_raw_doc
        )

        def _get_raw_results():
            return [json.dumps(x.to_dict(), indent=1) for x in _works()]

        yield self.run_test(
            ("Raw search results for '%s':" %(self.test_search_term)),
            _get_raw_results
        )

        def _count_docs():
            # The mock methods used in testing return a list, so we have to call len() rather than count().
            if in_testing:
                return str(len(_works()))

            return str(_works().count())

        yield self.run_test(
            ("Total number of search results for '%s':" %(self.test_search_term)),
            _count_docs
        )

        def _total_count():
            # The mock methods used in testing return a list, so we have to call len() rather than count().
            if in_testing:
                return str(len(self.search))

            return str(self.search.count())

        yield self.run_test(
            "Total number of documents in this search index:",
            _total_count
        )

        def _collections():
            result = {}

            collections = _db.query(Collection)
            for collection in collections:
                filter = Filter(collections=[collection])
                search = self.query_works(
                    "", filter=filter, pagination=None,
                    debug=True
                )
                if in_testing:
                    result[collection.name] = len(search)
                else:
                    result[collection.name] = search.count()

            return json.dumps(result, indent=1)

        yield self.run_test(
            "Total number of documents per collection:",
            _collections
        )


class Mapping(object):
    """A class that defines the mapping for a particular version of the search index."""

    VERSIONS = []

    VERSION_NAME = None

    # These methods are called on the Mapping class itself.
    @classmethod
    def latest(cls):
        """Intantiate the the Mapping class currently in use."""
        return cls.VERSIONS[-1]()

    @classmethod
    def register(cls, subclass):
        """Add a Mapping subclass to the list of versions."""
        cls.VERSIONS.append(subclass)

    # These class methods are called on a subclass of Mapping and will raise NotImplementedError
    # if called on Mapping.

    @classmethod
    def version_name(cls):
        """Return the name of this Mapping subclass."""
        version = cls.VERSION_NAME
        if not version:
            raise NotImplementedError("VERSION_NAME not defined")
        if not version.startswith('v'):
            version = 'v%s' % version
        return version

    def create(self, search_client, base_index_name):
        """Ensure that an index exists in `search_client` for this Mapping subclass.

        :return: True or False, indicating whether the index was created new.
        """
        versioned_index = base_index_name+'-'+self.version_name()
        if search_client.indices.exists(index=versioned_index):
            return False
        else:
            search_client.setup_index(new_index=versioned_index)
            return True

    def script_name(self, base_name):
        """Scope a script name with "simplified" (to avoid confusion with
        other applications on the ElasticSearch server), and the
        version number (to avoid confusion with other versions that
        implement the same script differently).
        """
        return "simplified.%s_%s" % (base_name, self.version_name())

    def __init__(self):
        self.filters = {}
        self.char_filters = {}
        self.normalizers = {}
        self.analyzers = {}
        self.properties = {}

    def body(self):
        """Generate the body of the mapping document for this version of the mapping."""
        settings = dict(
            analysis=dict(
                filter=self.filters,
                char_filter=self.char_filters,
                normalizer=self.normalizers,
                analyzer=self.analyzers
            )
        )

        work_mapping = dict(properties=self.properties)
        mappings = { 
            ExternalSearchIndex.work_document_type : work_mapping
        }
        return dict(settings=settings, mappings=mappings)


class MappingV4(Mapping):
    """Version four of the mapping, the first one to support only Elasticsearch 6."""

    VERSION_NAME = "v4"

    @classmethod
    def map_fields(cls, fields, field_description, mapping=None):
        mapping = mapping or {"properties": {}}
        for field in fields:
            mapping["properties"][field] = field_description
        return mapping

    @classmethod
    def map_fields_by_type(cls, fields_by_type, mapping=None):
        """Create a mapping for a number of fields of different types.

        :param fields_by_type: A dictionary mapping Elasticsearch types
            to field names.
        """
        for type, fields in fields_by_type.items():
            description={
                "type": type,
                # TODO: In some cases we can get away with setting
                # index: False here, which would presumably lead
                # to a smaller index and faster updates. However,
                # it might hurt performance of searches. When this
                # code is more mature we can do a side-by-side
                # comparison.
                "index": True,
                "store": False,
            }
            if type == 'icu_collation_keyword':
                # An icu_collation_keyword field can't have a custom
                # analyzer or normalizer, and we need a character filter
                # to exclude certain punctuation characters when determining
                # search order. A text field with the en_sortable_analyzer
                # approximates the (much simpler) icu_collation_keyword type
                # but also lets us have the character filter.
                description['type'] = 'text'
                description['analyzer'] = 'en_sortable_analyzer'
                description['fielddata'] = True
            elif type == 'filterable_text':
                description['type'] = 'text'
                description['analyzer'] = "en_analyzer"
                description['fields'] =  cls.FILTERABLE_TEXT_FIELDS
            mapping = cls.map_fields(
                fields=fields,
                mapping=mapping,
                field_description=description,
            )
        return mapping

    # Use regular expressions to normalized values in sortable fields.
    # These regexes are applied in order; that way "H. G. Wells"
    # becomes "H G Wells" becomes "HG Wells".
    CHAR_FILTERS = {}
    AUTHOR_CHAR_FILTER_NAMES = []
    for name, pattern, replacement in [
        # The special author name "[Unknown]" should sort after everything
        # else. REPLACEMENT CHARACTER is the final valid Unicode character.
        ("unknown_author", "\[Unknown\]", u"\N{REPLACEMENT CHARACTER}"),

        # Works by a given primary author should be secondarily sorted
        # by title, not by the other contributors.
        ("primary_author_only", "\s+;.*", ""),

        # Remove parentheticals (e.g. the full name of someone who
        # goes by initials).
        ("strip_parentheticals", "\s+\([^)]+\)", ""),

        # Remove periods from consideration.
        ("strip_periods", "\.", ""),

        # Collapse spaces for people whose sort names end with initials.
        ("collapse_three_initials", " ([A-Z]) ([A-Z]) ([A-Z])$", " $1$2$3"),
        ("collapse_two_initials", " ([A-Z]) ([A-Z])$", " $1$2"),
    ]:
        normalizer = dict(type="pattern_replace",
                          pattern=pattern,
                          replacement=replacement)
        CHAR_FILTERS[name] = normalizer
        AUTHOR_CHAR_FILTER_NAMES.append(name)

    # We want to index most text fields twice: once using the standard
    # analyzer and once using a minimal analyzer for near-exact
    # matches.
    BASIC_STRING_FIELDS = {
        "minimal": {
            "type": "text",
            "analyzer": "en_minimal_analyzer"},
        "standard": {
            "type": "text",
            "analyzer": "standard"
        }
    }

    # Some fields, such as series and contributor name, we want to
    # index as text fields (for use in searching) _and_ as keyword
    # fields (for use in filtering). For the keyword field, only
    # the most basic normalization is applied.
    FILTERABLE_TEXT_FIELDS = dict(BASIC_STRING_FIELDS)
    FILTERABLE_TEXT_FIELDS["keyword"] = {
        "type": "keyword",
        "index": True,
        "store": False,
        "normalizer": "filterable_string",
    }

    @classmethod
    def body(cls):
        """The first search body designed for ElasticSearch 6.

        This body has bibliographic information in the core document,
        primarily used for matching search requests. It also has
        nested documents, which are used for filtering and ranking
        Works when generating other types of feeds:

        * licensepools -- the Work has these LicensePools (includes current
          availability as a boolean, but not detailed availability information)
        * customlists -- the Work is on these CustomLists
        * contributors -- these Contributors worked on the Work
        """
        settings = {
            "analysis": {
                "filter": {
                    "en_stop_filter": {
                        "type": "stop",
                        "stopwords": ["_english_"]
                    },
                    "en_stem_filter": {
                        "type": "stemmer",
                        "name": "english"
                    },
                    "en_stem_minimal_filter": {
                        "type": "stemmer",
                        "name": "english"
                    },
                    # Use the ICU collation algorithm on textual fields we
                    # intend to use as a sort order.
                    "en_sortable_filter": {
                        "type": "icu_collation",
                        "language": "en",
                        "country": "US"
                    }
                },
                "char_filter" : cls.CHAR_FILTERS,

                # This normalizer is used on freeform strings that
                # will be used as tokens in filters. This way we can,
                # e.g. ignore capitalization when considering whether
                # two books belong to the same series or whether two
                # author names are the same.
                "normalizer": {
                    "filterable_string": {
                        "type": "custom",
                        "filter": ["lowercase", "asciifolding"]
                    }
                },

                "analyzer" : {
                    "en_analyzer": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding", "en_stop_filter", "en_stem_filter"]
                    },
                    "en_minimal_analyzer": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding", "en_stop_filter", "en_stem_minimal_filter"]
                    },
                    # An analyzer for textual fields we intend to sort on.
                    "en_sortable_analyzer": {
                        "tokenizer": "keyword",
                        "filter": [ "en_sortable_filter" ],
                    },
                    # A special analyzer for sort author names,
                    # including some special character filters for
                    # normalizing names.
                    "en_sort_author_analyzer": {
                        "tokenizer": "keyword",
                        "char_filter": cls.AUTHOR_CHAR_FILTER_NAMES,
                        "filter": [ "en_sortable_filter" ],
                    }
                }
            }
        }

        # For most string fields, we want to apply the standard
        # analyzer (for most searches) and the minimal analyzer (to
        # privilege near-exact matches).
        mapping = cls.map_fields(
            fields=["title", "subtitle", "summary", "classifications.term"],
            field_description={
                "type": "text",
                "analyzer": "en_analyzer",
                "fields": cls.BASIC_STRING_FIELDS
            }
        )

        # These fields are used for sorting and filtering search
        # results, but (apart from 'series') not for handling search
        # queries.
        fields_by_type = {
            'filterable_text': ['series'],
            'boolean': ['presentation_ready'],
            'icu_collation_keyword': ['sort_author', 'sort_title'],
            'integer': ['series_position', 'work_id'],
            'long': ['last_update_time'],
            'float': ['random'],
        }
        mapping = cls.map_fields_by_type(fields_by_type, mapping)

        # Sort author gets a different analyzer designed especially
        # to normalize author names.
        sort_author_description = {
            "type": "text",
            "index": True,
            "store": False,
            "fielddata": True,
            'analyzer': 'en_sort_author_analyzer'
        }
        mapping = cls.map_fields(
            fields=['sort_author'],
            mapping=mapping,
            field_description=sort_author_description,
        )

        # Build separate mappings for the nested data types.

        # Contributors
        contributor_fields_by_type = {
            'filterable_text' : ['sort_name', 'display_name', 'family_name'],
            'keyword': ['role', 'lc', 'viaf'],
        }
        contributor_definition = cls.map_fields_by_type(
            contributor_fields_by_type
        )

        # License pools.
        licensepool_fields_by_type = {
            'integer': ['collection_id', 'data_source_id'],
            'long': ['availability_time'],
            'boolean': ['available', 'open_access', 'suppressed', 'licensed'],
            'keyword': ['medium'],
        }
        licensepool_definition = cls.map_fields_by_type(
            licensepool_fields_by_type
        )

        # Custom list memberships.
        customlist_fields_by_type = {
            'integer': ['list_id'],
            'long':  ['first_appearance'],
            'boolean': ['featured'],
        }
        customlist_definition = cls.map_fields_by_type(
            customlist_fields_by_type
        )

        # Add the nested data type mappings to the main mapping.
        for type_name, type_definition in [
            ('licensepools', licensepool_definition),
            ('customlists', customlist_definition),
            ('contributors', contributor_definition),
        ]:
            type_definition['type'] = 'nested'
            mapping = cls.map_fields(
                fields=[type_name],
                field_description=type_definition,
                mapping=mapping,
            )
        # Finally, name the mapping to connect it to the 'works'
        # document type.
        mappings = { ExternalSearchIndex.work_document_type : mapping }
        return dict(settings=settings, mappings=mappings)

    @classmethod
    def stored_scripts(cls):
        """This version defines a single stored script, "work_last_update",
        defined below.
        """
        yield "work_last_update", cls.WORK_LAST_UPDATE_SCRIPT

    # Definition of the work_last_update_script.
    WORK_LAST_UPDATE_SCRIPT = """
double champion = -1;
// Start off by looking at the work's last update time.
for (candidate in doc['last_update_time']) {
    if (champion == -1 || candidate > champion) { champion = candidate; }
}
if (params.collection_ids != null && params.collection_ids.length > 0) {
    // Iterate over all licensepools looking for a pool in a collection
    // relevant to this filter. When one is found, check its
    // availability time to see if it's later than the last update time.
    for (licensepool in params._source.licensepools) {
        if (!params.collection_ids.contains(licensepool['collection_id'])) { continue; }
        double candidate = licensepool['availability_time'];
        if (champion == -1 || candidate > champion) { champion = candidate; }
    }
}
if (params.list_ids != null && params.list_ids.length > 0) {

    // Iterate over all customlists looking for a list relevant to
    // this filter. When one is found, check the previous work's first
    // appearance on that list to see if it's later than the last
    // update time.
    for (customlist in params._source.customlists) {
        if (!params.list_ids.contains(customlist['list_id'])) { continue; }
        double candidate = customlist['first_appearance'];
        if (champion == -1 || candidate > champion) { champion = candidate; }
    }
}

return champion;
"""

# Register the V4 mapping with the Mapping object.
Mapping.register(MappingV4)


class SearchBase(object):

    @classmethod
    def _match_range(cls, field, operation, value):
        """Match a ranged value for a field, using an operation other than
        equality.

        e.g. _match_range("field.name", "gte", 5) will match
        any value for field.name greater than 5.
        """
        match = {field : {operation: value}}
        return dict(range=match)


class Query(SearchBase):
    """An attempt to find something in the search index."""

    # When we run a simple query string search, we are matching the
    # query string against these fields.
    SIMPLE_QUERY_STRING_FIELDS = [
        # These fields have been stemmed.
        'title^4',
        "series^4",
        'subtitle^3',
        'summary^2',
        "classifications.term^2",

        # These fields only use the standard analyzer and are closer to the
        # original text.
        'author^6',
        'publisher',
        'imprint'
    ]

    # When we look for a close match against title, author, or series,
    # we apply minimal stemming (or no stemming, in the case of the
    # author), because we're handling the case where the user typed
    # something in exactly as is.
    #
    # TODO: If we're really serious about 'minimal stemming',
    # we should use title.standard and series.standard instead of
    # .minimal. Using .minimal gets rid of plurals and stop words.
    MINIMAL_STEMMING_QUERY_FIELDS = [
        'title.minimal', 'author', 'series.minimal'
    ]

    # When we run a fuzzy query string search, we are matching the
    # query string against these fields. It's more important that we
    # use fields that have undergone minimal stemming because the part
    # of the word that was stemmed may be the part that is misspelled
    FUZZY_QUERY_STRING_FIELDS = [
        'title.minimal^4',
        'series.minimal^4',
        "subtitle.minimal^3",
        "summary.minimal^2",
        'author^4',
        'publisher',
        'imprint'
    ]

    # These words will fuzzy-match other common words that aren't relevant,
    # so if they're present and correctly spelled we shouldn't use a
    # fuzzy query.
    FUZZY_CONFOUNDERS = [
        "baseball", "basketball", # These fuzzy match each other

        "soccer", # Fuzzy matches "saucer", "docker", "sorcery"

        "football", "softball", "software", "postwar",

        "tennis",

        "hamlet", "harlem", "amulet", "tablet",

        "biology", "ecology", "zoology", "geology",

        "joke", "jokes" # "jake"

        "cat", "cats",
        "car", "cars",
        "war", "wars",

        "away", "stay",
    ]

    # If this regular expression matches a query, we will not run
    # a fuzzy match against that query, because it's likely to be
    # counterproductive.
    #
    # TODO: Instead of this, avoid the fuzzy query or weigh it much
    # lower if there don't appear to be any misspelled words in the
    # query string. Or switch to a suggester.
    FUZZY_CIRCUIT_BREAKER = re.compile(
        r'\b(%s)\b' % "|".join(FUZZY_CONFOUNDERS), re.I
    )

    def __init__(self, query_string, filter=None):
        """Store a query string and filter.

        :param query_string: A user typed this string into a search box.
        :param filter: A Filter object representing the circumstances
            of the search -- for example, maybe we are searching within
            a specific lane.
        """
        self.query_string = query_string
        self.filter = filter

    def build(self, elasticsearch, pagination=None):
        """Make an Elasticsearch-DSL Search object out of this query.

        :param elasticsearch: An Elasticsearch-DSL Search object. This
        object is ready to run a search against an Elasticsearch server,
        but it doesn't represent any particular Elasticsearch query.

        :return: An Elasticsearch-DSL Search object that's prepared
            to run this specific query.
        """
        query = self.query()
        nested_filters = defaultdict(list)

        # Convert the resulting Filter into two objects -- one
        # describing the base filter and one describing the nested
        # filters.
        if self.filter:
            base_filter, nested_filters = self.filter.build()
        else:
            base_filter = None
            nested_filters = defaultdict(list)

        # Combine the query's base Filter with the universal base
        # filter -- works must be presentation-ready, etc.
        universal_base_filter = Filter.universal_base_filter()
        if universal_base_filter:
            query_filter = Filter._chain_filters(
                base_filter, universal_base_filter
            )
        else:
            query_filter = base_filter
        if query_filter:
            query = Q("bool", must=query, filter=query_filter)

        # We now have an Elasticsearch-DSL Query object (which isn't
        # tied to a specific server). Turn it into a Search object
        # (which is).
        search = elasticsearch.query(query)

        # Now update the 'nested filters' dictionary with the
        # universal nested filters -- no suppressed license pools,
        # etc.
        universal_nested_filters = Filter.universal_nested_filters() or {}
        for key, values in universal_nested_filters.items():
            nested_filters[key].extend(values)

        # Now we can convert any nested filters (universal or
        # otherwise) into nested queries.
        for path, subfilters in nested_filters.items():
            for subfilter in subfilters:
                # This ensures that the filter logic is executed in
                # filter context rather than query context.
                subquery = Bool(filter=subfilter)
                search = search.filter(
                    name_or_query='nested', path=path, query=subquery
                )

        if self.filter:
            # Apply any necessary sort order.
            order_fields = self.filter.sort_order
            if order_fields:
                search = search.sort(*order_fields)

            # Add any necessary script fields.
            script_fields = self.filter.script_fields
            if script_fields:
                search = search.script_fields(**script_fields)
        # Apply any necessary query restrictions imposed by the
        # Pagination object. This may happen through modification or
        # by returning an entirely new Search object.
        if pagination:
            result = pagination.modify_search_query(search)
            if result is not None:
                search = result

        # All done!
        return search

    def query(self):
        """Build an Elasticsearch Query object for this query string.
        """
        query_string = self.query_string

        if query_string is None:
            # There is no query string.
            return Q("match_all")

        # The search query will create a dis_max query, which tests a
        # number of hypotheses about what the query string might
        # 'really' mean. For each book, the highest-rated hypothesis
        # will be assumed to be true, and the highest-rated titles
        # overall will become the search results.
        hypotheses = []

        # Here are the hypotheses:

        # The query string might appear in one of the standard
        # searchable fields.
        simple = self.simple_query_string_query(query_string)
        self._hypothesize(hypotheses, simple)

        # The query string might be a close match against title,
        # author, or series.
        self._hypothesize(
            hypotheses,
            self.minimal_stemming_query(query_string),
            100
        )

        # The query string might be an exact match for title or
        # author. Such a match would be boosted quite a lot.
        self._hypothesize(
            hypotheses,
            self._match_phrase("title.standard", query_string), 200
        )
        self._hypothesize(
            hypotheses,
            self._match_phrase("author", query_string), 50
        )

        # The query string might be a fuzzy match against one of the
        # standard searchable fields.
        fuzzy = self.fuzzy_string_query(query_string)
        self._hypothesize(hypotheses, fuzzy, 1)

        # The query string might contain some specific field matches
        # (e.g. a genre name or target age), with the remainder being
        # the "real" query string.
        with_field_matches = self._parsed_query_matches(query_string)
        self._hypothesize(
            hypotheses, with_field_matches, 200, all_must_match=True
        )

        # For a given book, whichever one of these hypotheses gives
        # the highest score should be used.
        qu = self._combine_hypotheses(hypotheses)
        return qu

    @classmethod
    def _hypothesize(cls, hypotheses, query, boost=1.5, **kwargs):
        """Add a hypothesis to the ones to be tested for each book.

        :param hypotheses: If a new hypothesis is generated, it will be
        added to this list.

        :param query: A Query object (or list of Query objects) to be
        used as the basis for this hypothesis. If there's nothing here,
        no new hypothesis will be generated.

        :param boost: Boost the overall weight of this hypothesis
        relative to other hypotheses being tested. The default of 1.5
        allows most 'ordinary' hypotheses to rank higher than the
        fuzzy-search hypothesis.

        :param kwargs: Keyword arguments for the _boost method.
        """
        if query:
            query = cls._boost(boost, query, **kwargs)
        if query:
            hypotheses.append(query)
        return hypotheses

    @classmethod
    def _combine_hypotheses(cls, hypotheses):
        """Build an Elasticsearch Query object that tests a number
        of hypotheses at once.
        """
        return Q("dis_max", queries=hypotheses)

    @classmethod
    def _boost(cls, boost, queries, all_must_match=False):
        """Boost a query by a certain amount relative to its neighbors in a
        dis_max query.

        :param boost: Numeric value to boost search results that
           match `queries`.
        :param queries: One or more Query objects. If more than one query
           is provided, a new Bool-type query will be created with
           the given boost. If this is a single Bool-type query, its
           boost will be modified and no new query will be created.
        :param all_must_match: If this is False (the default), then only
           one of the `queries` must match for a search result to get
           the boost. If this is True, then all `queries` must match,
           or the boost will not apply.
        """
        if isinstance(queries, Bool):
            # This is already a boolean query; we just need to change
            # the boost.
            queries._params['boost'] = boost
            return queries

        if isinstance(queries, BaseQuery):
            if boost == 1:
                # We already have a Query and we don't actually need
                # to boost it. Leave it alone to simplify the final
                # query.
                return queries
            else:
                queries = [queries]

        if all_must_match or len(queries) == 1:
            # Every one of the subqueries in `queries` must match.
            # (If there's only one subquery, this simplifies the
            # final query slightly.)
            kwargs = dict(must=queries)
        else:
            # At least one of the queries in `queries` must match.
            kwargs = dict(should=queries, minimum_should_match=1)

        return Q("bool", boost=float(boost), **kwargs)

    @classmethod
    def simple_query_string_query(cls, query_string, fields=None):
        fields = fields or cls.SIMPLE_QUERY_STRING_FIELDS
        q = Q("simple_query_string", query=query_string, fields=fields)
        return q

    @classmethod
    def fuzzy_string_query(cls, query_string):
        # If the query string contains any of the strings known to counfound
        # fuzzy search, don't do the fuzzy search.
        if not query_string:
            return None
        if cls.FUZZY_CIRCUIT_BREAKER.search(query_string):
            return None

        fuzzy = Q(
            "multi_match",                        # Match any or all fields
            query=query_string,
            fields=cls.FUZZY_QUERY_STRING_FIELDS, # Look in these fields
            type="best_fields",                   # Score based on best match
            fuzziness="AUTO",          # More typos allowed in longer strings
            prefix_length=1,           # People don't usually typo first letter
        )
        return fuzzy

    @classmethod
    def _match(cls, field, query_string):
        """A clause that matches the query string against a specific field in the search document.
        """
        return Q("match", **{field: query_string})

    @classmethod
    def _match_phrase(cls, field, query_string):
        """A clause that matches the query string against a specific field in the search document.

        The words in the query_string must match the words in the field,
        in order. E.g. "fiction science" will not match "Science Fiction".
        """
        return Q("match_phrase", **{field: query_string})

    @classmethod
    def minimal_stemming_query(
            cls, query_string,
            fields=MINIMAL_STEMMING_QUERY_FIELDS
    ):
        """A clause that tries for a close match of the query string
        against any of a number of fields.
        """
        return [cls._match_phrase(field, query_string) for field in fields]

    @classmethod
    def make_target_age_query(cls, target_age, boost=1):
        """Create an Elasticsearch query object for a boolean query that
        matches works whose target ages overlap (partially or
        entirely) the given age range.

        :param target_age: A 2-tuple (lower limit, upper limit)
        :param boost: A value for the boost parameter
        """
        (lower, upper) = target_age[0], target_age[1]
        # There must be _some_ overlap with the provided range.
        must = [
            cls._match_range("target_age.upper", "gte", lower),
            cls._match_range("target_age.lower", "lte", upper)
        ]

        # Results with ranges contained within the query range are
        # better.
        # e.g. for query 4-6, a result with 5-6 beats 6-7
        should = [
            cls._match_range("target_age.upper", "lte", upper),
            cls._match_range("target_age.lower", "gte", lower),
        ]
        return Q("bool", must=must, should=should, boost=float(boost))

    @classmethod
    def _parsed_query_matches(cls, query_string):
        """Deal with a query string that contains information that should be
        exactly matched against a controlled vocabulary
        (e.g. "nonfiction" or "grade 5") along with information that
        is more search-like (such as a title or author).

        The match information is pulled out of the query string and
        used to make a series of match_phrase queries. The rest of the
        information is used in a simple query that matches basic
        fields.
        """
        return QueryParser(query_string).match_queries


class QueryParser(object):
    """Attempt to parse filter information out of a query string.

    This class is where we make sense of queries like the following:

      asteroids nonfiction
      grade 5 dogs
      young adult romance
      divorce age 10 and up

    These queries contain information that can best be thought of in
    terms of a filter against specific fields ("nonfiction", "grade
    5", "romance"). Books either match these criteria or they don't.

    These queries may also contain information that can be thought of
    in terms of a search ("asteroids", "dogs") -- books may match
    these criteria to a greater or lesser extent.
    """

    # The unparseable portion of a query is matched against these
    # fields.
    SIMPLE_QUERY_STRING_FIELDS = [
        "author^4", "subtitle^3", "summary^5", "title^1", "series^1"
    ]

    def __init__(self, query_string, query_class=Query):
        """Parse the query string and create a list of clauses
        that will boost certain types of books.

        Use .query to get an Elasticsearch Query object.

        :param query_class: Pass in a mock of Query here during testing
        to generate 'query' objects that are easier for you to test.
        """
        self.original_query_string = query_string.strip()
        self.query_class = query_class

        # We start with no match queries.
        self.match_queries = []

        # We handle genre first so that, e.g. 'Science Fiction' doesn't
        # get chomped up by the search for 'fiction'.

        # Handle the 'romance' part of 'young adult romance'
        genre, genre_match = KeywordBasedClassifier.genre_match(query_string)
        if genre:
            query_string = self.add_match_query(
                genre.name, 'genres.name', query_string, genre_match
            )

        # Handle the 'young adult' part of 'young adult romance'
        audience, audience_match = KeywordBasedClassifier.audience_match(
            query_string
        )
        if audience:
            query_string = self.add_match_query(
                audience.replace(" ", ""), 'audience', query_string,
                audience_match
            )

        # Handle the 'nonfiction' part of 'asteroids nonfiction'
        fiction = None
        if re.compile(r"\bnonfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Nonfiction"
        elif re.compile(r"\bfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Fiction"
        query_string = self.add_match_query(
            fiction, 'fiction', query_string, fiction
        )

        # Handle the 'grade 5' part of 'grade 5 dogs'
        age_from_grade, grade_match = GradeLevelClassifier.target_age_match(
            query_string
        )
        if age_from_grade and age_from_grade[0] == None:
            age_from_grade = None
        query_string = self.add_target_age_query(
            age_from_grade, query_string, grade_match
        )

        # Handle the 'age 10 and up' part of 'divorce age 10 and up'
        age, age_match = AgeClassifier.target_age_match(query_string)
        if age and age[0] == None:
            age = None
        query_string = self.add_target_age_query(age, query_string, age_match)

        self.final_query_string = query_string.strip()

        if len(query_string.strip()) == 0:
            # Someone who searched for 'young adult romance' ended up
            # with an empty query string -- they matched an audience
            # and a genre, and now there's nothing else to match.
            return

        # Someone who searched for 'asteroids nonfiction' ended up
        # with a query string of 'asteroids'. Their query string
        # has a field match component and a query-type component.
        #
        # What is likely to be in this query-type component?
        #
        # In theory, it could be anything that would go into a
        # regular query. So would be a really cool place to
        # call Query() recursively.
        #
        # However, someone who does this kind of search is
        # probably not looking for a specific book. They might be
        # looking for an author (eg, 'science fiction iain
        # banks'). But they're most likely searching for a _type_
        # of book, which means a match against summary or subject
        # ('asteroids') would be the most useful.
        if (self.final_query_string
            and self.final_query_string != self.original_query_string):
            match_rest_of_query = self.query_class.simple_query_string_query(
                self.final_query_string,
                self.SIMPLE_QUERY_STRING_FIELDS
            )
            self.match_queries.append(match_rest_of_query)

    def add_match_query(self, query, field, query_string, matched_portion):
        """Create a match query that finds documents whose value for `field`
        matches `query`.

        Add it to `self.match_queries`, and remove the relevant portion
        of `query_string` so it doesn't get reused.
        """
        if not query:
            # This is not a relevant part of the query string.
            return query_string
        match_query = self.query_class._match(field, query)
        self.match_queries.append(match_query)
        return self._without_match(query_string, matched_portion)

    def add_target_age_query(self, query, query_string, matched_portion):
        """Create a query that finds documents whose value for `target_age`
        matches `query`.

        Add it to `match_queries`, and remove the relevant portion
        of `query_string` so it doesn't get reused.
        """
        if not query:
            # This is not a relevant part of the query string.
            return query_string
        match_query = self.query_class.make_target_age_query(query, 40)
        self.match_queries.append(match_query)
        return self._without_match(query_string, matched_portion)

    @classmethod
    def _without_match(cls, query_string, match):
        """Take the portion of a query string that matched a controlled
        vocabulary, and remove it from the query string, so it
        doesn't get reused later.
        """
        # If the match was "children" and the query string was
        # "children's", we want to remove the "'s" as well as
        # the match. We want to remove everything up to the
        # next word boundary that's not an apostrophe or a
        # dash.
        word_boundary_pattern = r"\b%s[\w'\-]*\b"

        return re.compile(
            word_boundary_pattern % match.strip(), re.IGNORECASE
        ).sub("", query_string)


class Filter(SearchBase):
    """A filter for search results.

    This covers every reason you might want to not exclude a search
    result that would otherise match the query string -- wrong media,
    wrong language, not available in the patron's library, etc.

    This also covers every way you might want to order the search
    results: either by relevance to the search query (the default), or
    by a specific field (e.g. author) as described by a Facets object.

    It also covers additional calculated values you might need when
    presenting the search results.
    """

    # When search results include known script fields, we need to
    # wrap the works we would be returning in WorkSearchResults so
    # the useful information from the search engine isn't lost.
    KNOWN_SCRIPT_FIELDS = ['last_update']

    # In general, someone looking for things "by this person" is
    # probably looking for one of these roles.
    AUTHOR_MATCH_ROLES = list(Contributor.AUTHOR_ROLES) + [
        Contributor.NARRATOR_ROLE, Contributor.EDITOR_ROLE,
        Contributor.DIRECTOR_ROLE, Contributor.ACTOR_ROLE
    ]

    @classmethod
    def from_worklist(cls, _db, worklist, facets):
        """Create a Filter that finds only works that belong in the given
        WorkList and EntryPoint.

        :param worklist: A WorkList
        :param facets: A SearchFacets object.
        """
        library = worklist.get_library(_db)

        # For most configuration settings there is a single value --
        # either defined on the WorkList or defined by its parent.
        inherit_one = worklist.inherited_value
        media = inherit_one('media')
        languages = inherit_one('languages')
        fiction = inherit_one('fiction')
        audiences = inherit_one('audiences')
        target_age = inherit_one('target_age')
        collections = inherit_one('collection_ids') or library

        license_datasource_id = inherit_one('license_datasource_id')

        # For genre IDs and CustomList IDs, we might get a separate
        # set of restrictions from every item in the WorkList hierarchy.
        # _All_ restrictions must be met for a work to match the filter.
        inherit_some = worklist.inherited_values
        genre_id_restrictions = inherit_some('genre_ids')
        customlist_id_restrictions = inherit_some('customlist_ids')

        # See if there are any excluded audiobook sources on this
        # site.
        excluded = (
            ConfigurationSetting.excluded_audio_data_sources(_db)
        )
        excluded_audiobook_data_sources = [
            DataSource.lookup(_db, x) for x in excluded
        ]
        if library is None:
            allow_holds = True
        else:
            allow_holds = library.allow_holds
        return cls(
            collections, media, languages, fiction, audiences,
            target_age, genre_id_restrictions, customlist_id_restrictions,
            facets,
            excluded_audiobook_data_sources=excluded_audiobook_data_sources,
            allow_holds=allow_holds, license_datasource=license_datasource_id
        )

    def __init__(self, collections=None, media=None, languages=None,
                 fiction=None, audiences=None, target_age=None,
                 genre_restriction_sets=None, customlist_restriction_sets=None,
                 facets=None, script_fields=None, **kwargs
    ):
        """These minor arguments were made into unnamed keyword arguments to
        avoid cluttering the method signature:

        :param excluded_audiobook_data_sources: A list of DataSources that
        provide audiobooks known to be unsupported on this system.
        Such audiobooks will always be excluded from results.

        :param allow_holds: If this is False, books with no available
        copies will be excluded from results.

        :param series: If this is set to a string, only books in a matching
        series will be included.

        :param author: If this is set to a Contributor or
        ContributorData, then only books where this person had an
        authorship role will be included.

        :param license_datasource: If this is set to a DataSource,
        only books with LicensePools from that DataSource will be
        included.

        :param updated_after: If this is set to a datetime, only books
        whose Work records (~bibliographic metadata) have been updated since
        that time will be included in results.
        """

        if isinstance(collections, Library):
            # Find all works in this Library's collections.
            collections = collections.collections
        self.collection_ids = self._filter_ids(collections)

        self.media = media
        self.languages = languages
        self.fiction = fiction
        self.audiences = audiences

        if target_age:
            if isinstance(target_age, int):
                self.target_age = (target_age, target_age)
            elif isinstance(target_age, tuple) and len(target_age) == 2:
                self.target_age = target_age
            else:
                # It's a SQLAlchemy range object. Convert it to a tuple.
                self.target_age = numericrange_to_tuple(target_age)
        else:
            self.target_age = None

        # Filter the lists of database IDs to make sure we aren't
        # storing any database objects.
        if genre_restriction_sets:
            self.genre_restriction_sets = [
                self._filter_ids(x) for x in genre_restriction_sets
            ]
        else:
            self.genre_restriction_sets = []
        if customlist_restriction_sets:
            self.customlist_restriction_sets = [
                self._filter_ids(x) for x in customlist_restriction_sets
            ]
        else:
            self.customlist_restriction_sets = []

        # Pull less-important values out of the keyword arguments.
        excluded_audiobook_data_sources = kwargs.pop(
            'excluded_audiobook_data_sources', []
        )
        self.excluded_audiobook_data_sources = self._filter_ids(
            excluded_audiobook_data_sources
        )
        self.allow_holds = kwargs.pop('allow_holds', True)

        self.updated_after = kwargs.pop('updated_after', None)

        self.series = kwargs.pop('series', None)

        self.author = kwargs.pop('author', None)

        license_datasources = kwargs.pop('license_datasource', None)
        self.license_datasources = self._filter_ids(license_datasources)

        # At this point there should be no keyword arguments -- you can't pass
        # whatever you want into this method.
        if kwargs:
            raise ValueError("Unknown keyword arguments: %r" % kwargs)

        # Establish default values for additional restrictions that may be
        # imposed by the Facets object.
        self.minimum_featured_quality = 0
        self.availability = None
        self.subcollection = None
        self.order = None
        self.order_ascending = False

        self.script_fields = script_fields or dict()

        # Give the Facets object a chance to modify any or all of this
        # information.
        if facets:
            facets.modify_search_filter(self)
            self.scoring_functions = facets.scoring_functions(self)
        else:
            self.scoring_functions = []

    def build(self, _chain_filters=None):
        """Convert this object to an Elasticsearch Filter object.

        :return: A 2-tuple (filter, nested_filters). Filters on fields
           within nested documents (such as
           'licensepools.collection_id') must be applied as subqueries
           to the query that will eventually be created from this
           filter. `nested_filters` is a dictionary that maps a path
           to a list of filters to apply to that path.

        :param _chain_filters: Mock function to use instead of
            Filter._chain_filters
        """

        # Since a Filter object can be modified after it's created, we
        # need to scrub all the inputs, whether or not they were
        # scrubbed in the constructor.
        scrub_list = self._scrub_list
        filter_ids = self._filter_ids

        chain = _chain_filters or self._chain_filters

        f = None
        nested_filters = defaultdict(list)
        collection_ids = filter_ids(self.collection_ids)
        if collection_ids:
            collection_match = Q(
                'terms', **{'licensepools.collection_id' : collection_ids}
            )
            nested_filters['licensepools'].append(collection_match)

        license_datasources = filter_ids(self.license_datasources)
        if license_datasources:
            datasource_match = Q(
                'terms', **{'licensepools.data_source_id' : license_datasources}
            )
            nested_filters['licensepools'].append(datasource_match)

        if self.author is not None:
            nested_filters['contributors'].append(self.author_filter)

        if self.media:
            f = chain(f, Q('terms', medium=scrub_list(self.media)))

        if self.languages:
            f = chain(f, Q('terms', language=scrub_list(self.languages)))

        if self.fiction is not None:
            if self.fiction:
                value = 'fiction'
            else:
                value = 'nonfiction'
            f = chain(f, Q('term', fiction=value))

        if self.series:
            f = chain(f, Q('term', **{"series.keyword": self.series}))

        if self.audiences:
            f = chain(f, Q('terms', audience=scrub_list(self.audiences)))

        target_age_filter = self.target_age_filter
        if target_age_filter:
            f = chain(f, self.target_age_filter)

        for genre_ids in self.genre_restriction_sets:
            f = chain(f, Q('terms', **{'genres.term' : filter_ids(genre_ids)}))

        for customlist_ids in self.customlist_restriction_sets:
            ids = filter_ids(customlist_ids)
            nested_filters['customlists'].append(
                Q('terms', **{'customlists.list_id' : ids})
            )

        open_access = Q('term', **{'licensepools.open_access' : True})
        if self.availability==FacetConstants.AVAILABLE_NOW:
            # Only open-access books and books with currently available
            # copies should be displayed.
            available = Q('term', **{'licensepools.available' : True})
            nested_filters['licensepools'].append(
                Q('bool', should=[open_access, available])
            )
        elif self.availability==FacetConstants.AVAILABLE_OPEN_ACCESS:
            # Only open-access books should be displayed.
            nested_filters['licensepools'].append(open_access)

        if self.subcollection==FacetConstants.COLLECTION_MAIN:
            # Exclude open-access books with a quality of less than
            # 0.3.
            not_open_access = Q('term', **{'licensepools.open_access' : False})
            decent_quality = self._match_range('licensepools.quality', 'gte', 0.3)
            nested_filters['licensepools'].append(
                Q('bool', should=[not_open_access, decent_quality])
            )
        elif self.subcollection==FacetConstants.COLLECTION_FEATURED:
            # Exclude books with a quality of less than the library's
            # minimum featured quality.
            range_query = self._match_range(
                'quality', 'gte', self.minimum_featured_quality
            )
            f = chain(f, Q('bool', must=range_query))

        # Some sources of audiobooks may be excluded because the
        # server can't fulfill them or the anticipated client can't
        # play them.
        excluded = self.excluded_audiobook_data_sources
        if excluded:
            audio = Q('term', **{'licensepools.medium': Edition.AUDIO_MEDIUM})
            excluded_audio_source = Q(
                'terms', **{'licensepools.data_source_id' : excluded}
            )
            excluded_audio = Bool(must=[audio, excluded_audio_source])
            not_excluded_audio = Bool(must_not=excluded_audio)
            nested_filters['licensepools'].append(not_excluded_audio)

        # If holds are not allowed, only license pools that are
        # currently available should be considered.
        if not self.allow_holds:
            licenses_available = Q('term', **{'licensepools.available' : True})
            currently_available = Bool(should=[licenses_available, open_access])
            nested_filters['licensepools'].append(currently_available)

        # Perhaps only books whose bibliographic metadata was updated
        # recently should be included.
        if self.updated_after:
            # 'last update_time' is indexed as a number of seconds, but
            # .last_update is probably a datetime. Convert it here.
            updated_after = self.updated_after
            if isinstance(updated_after, datetime.datetime):
                updated_after = (
                    updated_after - datetime.datetime.utcfromtimestamp(0)
                ).total_seconds()
            last_update_time_query = self._match_range(
                'last_update_time', 'gte', updated_after
            )
            f = chain(f, Q('bool', must=last_update_time_query))

        return f, nested_filters

    @classmethod
    def universal_base_filter(cls, _chain_filters=None):
        """Build a set of restrictions on the main search document that are
        always applied, even in the absence of other filters.

        :param _chain_filters: Mock function to use instead of
            Filter._chain_filters

        :return: A Filter object.

        """

        _chain_filters = _chain_filters or cls._chain_filters

        base_filter = None

        # We only want to show works that are presentation-ready.
        base_filter = _chain_filters(
            base_filter, Q('term', **{"presentation_ready":True})
        )

        return base_filter

    @classmethod
    def universal_nested_filters(cls):
        """Build a set of restrictions on subdocuments that are
        always applied, even in the absence of other filters.
        """
        nested_filters = defaultdict(list)

        # TODO: It would be great to be able to filter out
        # LicensePools that have no delivery mechanisms. That's the
        # only part of Collection.restrict_to_ready_deliverable_works
        # not already implemented in this class.

        # We don't want to consider license pools that have been
        # suppressed, or of which there are currently no licensed
        # copies. This might lead to a Work being filtered out
        # entirely.
        #
        # It's easier to stay consistent by indexing all Works and
        # filtering them out later, than to do it by adding and
        # removing works from the index.
        not_suppressed = Q('term', **{'licensepools.suppressed' : False})
        nested_filters['licensepools'].append(not_suppressed)

        owns_licenses = Q('term', **{'licensepools.licensed' : True})
        open_access = Q('term', **{'licensepools.open_access' : True})
        currently_owned = Q('bool', should=[owns_licenses, open_access])
        nested_filters['licensepools'].append(currently_owned)

        return nested_filters

    @property
    def sort_order(self):
        """Create a description, for use in an Elasticsearch document,
        explaining how search results should be ordered.

        :return: A list of dictionaries, each dictionary mapping a
        field name to an explanation of how to sort that
        field. Usually the explanation is a simple string, either
        'asc' or 'desc'.
        """
        if not self.order:
            return []

        # These sort order fields are inserted as necessary between
        # the primary sort order field and the tiebreaker field (work
        # ID). This makes it more likely that the sort order makes
        # sense to a human, by putting off the opaque tiebreaker for
        # as long as possible. For example, a feed sorted by author
        # will be secondarily sorted by title and work ID, not just by
        # work ID.
        default_sort_order = ['sort_author', 'sort_title', 'work_id']

        order_field_keys = self.order
        if not isinstance(order_field_keys, list):
            order_field_keys = [order_field_keys]
        order_fields = [
            self._make_order_field(key) for key in order_field_keys
        ]

        # Apply any parts of the default sort order not yet covered,
        # concluding (in most cases) with work_id, the tiebreaker field.
        for x in default_sort_order:
            if x not in order_field_keys:
                order_fields.append({x: "asc"})
        return order_fields

    @property
    def asc(self):
        "Convert order_ascending to Elasticsearch-speak."
        if self.order_ascending is False:
            return "desc"
        else:
            return "asc"

    def _make_order_field(self, key):
        if key == 'last_update_time':
            # Sorting by last_update_time may be very simple or very
            # complex, depending on whether or not the filter
            # involves collection or list membership.
            if self.collection_ids or self.customlist_restriction_sets:
                # The complex case -- use a helper method.
                return self._last_update_time_order_by
            else:
                # The simple case, handled below.
                pass

        if '.' not in key:
            # A simple case.
            return { key : self.asc }

        # At this point we're sorting by a nested field.
        nested = None
        if key == 'licensepools.availability_time':
            # We're sorting works by the time they became
            # available to a library. This means we only want to
            # consider the availability times of license pools
            # found in one of the library's collections.
            collection_ids = self._filter_ids(self.collection_ids)
            if collection_ids:
                nested = dict(
                    path="licensepools",
                    filter=dict(
                        terms={
                            "licensepools.collection_id": collection_ids
                        }
                    ),
                )

            # If a book shows up in multiple collections, we're only
            # interested in the collection that had it the earliest.
            mode = 'min'
        else:
            raise ValueError(
                "I don't know how to sort by %s." % key
            )
        sort_description = dict(order=self.asc, mode=mode)
        if nested:
            sort_description['nested']=nested
        return { key : sort_description }

    @property
    def last_update_time_script_field(self):
        """Return the configuration for a script field that calculates the
        'last update' time of a work. An 'update' happens when the
        work's metadata is changed, when it's added to a collection
        used by this Filter, or when it's added to one of the lists
        used by this Filter.
        """
        # First, set up the parameters we're going to pass into the
        # script -- a list of custom list IDs relevant to this filter,
        # and a list of collection IDs relevant to this filter.
        collection_ids = self._filter_ids(self.collection_ids)

        # The different restriction sets don't matter here. The filter
        # part of the query ensures that we only match works present
        # on one list in every restriction set. Here, we need to find
        # the latest time a work was added to _any_ relevant list.
        all_list_ids = set()
        for restriction in self.customlist_restriction_sets:
            all_list_ids.update(self._filter_ids(restriction))
        nested = dict(
            path="customlists",
            filter=dict(
                terms={"customlists.list_id": list(all_list_ids)}
            )
        )
        params = dict(
            collection_ids=collection_ids,
            list_ids=list(all_list_ids)
        )
        return dict(
            script=dict(
                stored="simplified.work_last_update",
                params=params
            )
        )

    @property
    def _last_update_time_order_by(self):

        """We're sorting works by the time of their 'last update'.

        Add the 'last update' field to the dictionary of script fields
        (so we can use the result afterwards), and define it a second
        time as the script to use for a sort value.
        """
        field = self.last_update_time_script_field
        if not 'last_update' in self.script_fields:
            self.script_fields['last_update'] = field
        return dict(
            _script=dict(
                type="number",
                script=field['script'],
                order=self.asc,
            ),
        )

    @property
    def target_age_filter(self):
        """Helper method to generate the target age subfilter.

        It's complicated because it has to handle cases where the upper
        or lower bound on target age is missing (indicating there is no
        upper or lower bound).
        """
        if not self.target_age:
            return None
        lower, upper = self.target_age
        if lower is None and upper is None:
            return None
        def does_not_exist(field):
            """A filter that matches if there is no value for `field`."""
            return Q('bool', must_not=[Q('exists', field=field)])

        def or_does_not_exist(clause, field):
            """Either the given `clause` matches or the given field
            does not exist.
            """
            return Q('bool', should=[clause, does_not_exist(field)],
                     minimum_should_match=1)

        clauses = []

        if upper is not None:
            lower_does_not_exist = does_not_exist("target_age.lower")
            lower_in_range = self._match_range("target_age.lower", "lte", upper)
            lower_match = or_does_not_exist(lower_in_range, "target_age.lower")
            clauses.append(lower_match)

        if lower is not None:
            upper_does_not_exist = does_not_exist("target_age.upper")
            upper_in_range = self._match_range("target_age.upper", "gte", lower)
            upper_match = or_does_not_exist(upper_in_range, "target_age.upper")
            clauses.append(upper_match)

        if not clauses:
            # Neither upper nor lower age must match.
            return None

        if len(clauses) == 1:
            # Upper or lower age must match, but not both.
            return clauses[0]

        # Both upper and lower age must match.
        return Q('bool', must=clauses)

    @property
    def author_filter(self):
        """Build a filter that matches a 'contributors' subdocument only
        if it represents an author-level contribution by self.author.
        """
        if not self.author:
            return None
        authorship_role = Q(
            'terms', **{'contributors.role' : self.AUTHOR_MATCH_ROLES}
        )
        clauses = []
        for field, value in [
            ('sort_name.keyword', self.author.sort_name),
            ('display_name.keyword', self.author.display_name),
            ('viaf', self.author.viaf),
            ('lc', self.author.lc)
        ]:
            if not value or value == Edition.UNKNOWN_AUTHOR:
                continue
            clauses.append(
                Q('term', **{'contributors.%s' % field : value})
            )

        same_person = Q('bool', should=clauses, minimum_should_match=1)
        return Q('bool', must=[authorship_role, same_person])


    @classmethod
    def _scrub(cls, s):
        """Modify a string for use in a filter match.

        e.g. "Young Adult" becomes "youngadult"

        :param s: The string to modify.
        """
        if not s:
            return s
        return s.lower().replace(" ", "")

    @classmethod
    def _scrub_list(cls, s):
        """The same as _scrub, except it always outputs
        a list of items.
        """
        if s is None:
            return []
        if isinstance(s, basestring):
            s = [s]
        return [cls._scrub(x) for x in s]

    @classmethod
    def _filter_ids(cls, ids):
        """Process a list of database objects, provided either as their
        IDs or as the objects themselves.

        :return: A list of IDs, or None if nothing was provided.
        """
        # Generally None means 'no restriction', while an empty list
        # means 'one of the values in this empty list' -- in other
        # words, they are opposites.
        if ids is None:
            return None

        processed = []

        if not isinstance(ids, list) and not isinstance(ids, set):
            ids = [ids]

        for id in ids:
            if not isinstance(id, int):
                # Turn a database object into an ID.
                id = id.id
            processed.append(id)
        return processed

    @classmethod
    def _chain_filters(cls, existing, new):
        """Either chain two filters together or start a new chain."""
        if existing:
            # We're combining two filters.
            new = existing & new
        else:
            # There was no previous filter -- the 'new' one is it.
            pass
        return new


class SortKeyPagination(Pagination):
    """An Elasticsearch-specific implementation of Pagination that
    paginates search results by tracking where in a sorted list the
    previous page left off, rather than using a numeric index into the
    list.
    """

    def __init__(self, last_item_on_previous_page=None,
                 size=Pagination.DEFAULT_SIZE):
        self.size = size
        self.last_item_on_previous_page = last_item_on_previous_page

        # These variables are set by page_loaded(), after the query
        # is run.
        self.page_has_loaded = False
        self.last_item_on_this_page = None
        self.this_page_size = None

    @classmethod
    def from_request(cls, get_arg, default_size=None):
        """Instantiate a SortKeyPagination object from a Flask request."""
        size = cls.size_from_request(get_arg, default_size)
        if isinstance(size, ProblemDetail):
            return size
        pagination_key = get_arg('key', None)
        if pagination_key:
            try:
                pagination_key = json.loads(pagination_key)
            except ValueError, e:
                return INVALID_INPUT.detailed(
                    _("Invalid page key: %(key)s", key=pagination_key)
                )
        return cls(pagination_key, size)

    def items(self):
        """Yield the URL arguments necessary to convey the current page
        state.
        """
        pagination_key = self.pagination_key
        if pagination_key:
            yield("key", self.pagination_key)
        yield("size", self.size)

    @property
    def pagination_key(self):
        """Create the pagination key for this page."""
        if not self.last_item_on_previous_page:
            return None
        return json.dumps(self.last_item_on_previous_page)

    @property
    def offset(self):
        # This object never uses the traditional offset system; offset
        # is determined relative to the last item on the previous
        # page.
        return 0

    @property
    def total_size(self):
        # Although we technically know the total size after the first
        # page of results has been obtained, we don't use this feature
        # in pagination, so act like we don't.
        return None

    def modify_database_query(self, qu):
        raise NotImplementedError(
            "SortKeyPagination does not work with database queries."
        )

    def modify_search_query(self, search):
        """Modify the given Search object so that it starts
        picking up items immediately after the previous page.

        :param search: An elasticsearch-dsl Search object.
        """
        if self.last_item_on_previous_page:
            search = search.update_from_dict(
                dict(search_after=self.last_item_on_previous_page)
            )
        return search

    @property
    def previous_page(self):
        # TODO: We can get the previous page by flipping the sort
        # order and asking for the _next_ page of the reversed list,
        # using the sort keys of the _first_ item as the search_after.
        # But this is really confusing, it requires more context than
        # SortKeyPagination currently has, and this feature isn't
        # necessary for our current implementation.
        return None

    @property
    def next_page(self):
        """If possible, create a new SortKeyPagination representing the
        next page of results.
        """
        if self.this_page_size == 0:
            # This page is empty; there is no next page.
            return None
        if not self.last_item_on_this_page:
            # This probably means load_page wasn't called. At any
            # rate, we can't say anything about the next page.
            return None
        return SortKeyPagination(self.last_item_on_this_page, self.size)

    def page_loaded(self, page):
        """An actual page of results has been fetched. Keep any internal state
        that would be useful to know when reasoning about earlier or
        later pages.

        Specifically, keep track of the sort value of the last item on
        this page, so that self.next_page will create a
        SortKeyPagination object capable of generating the subsequent
        page.

        :param page: A list of elasticsearch-dsl Hit objects.
        """
        super(SortKeyPagination, self).page_loaded(page)
        if page:
            last_item = page[-1]
            values = list(last_item.meta.sort)
        else:
            # There's nothing on this page, so there's no next page
            # either.
            values = None
        self.last_item_on_this_page = values


class WorkSearchResult(object):
    """Wraps a Work object to give extra information obtained from 
    ElasticSearch.

    This object acts just like a Work (though isinstance(x, Work) will
    fail), with one exception: you can access the raw ElasticSearch Hit
    result as ._hit.

    This is useful when a Work needs to be 'tagged' with information
    obtained through Elasticsearch, such as its 'last modified' date
    the context of a specific lane.
    """
    def __init__(self, work, hit):
        self._work = work
        self._hit = hit

    def __getattr__(self, k):
        return getattr(self._work, k)


class MockExternalSearchIndex(ExternalSearchIndex):

    work_document_type = 'work-type'

    def __init__(self, url=None):
        self.url = url
        self.docs = {}
        self.works_index = "works"
        self.works_alias = "works-current"
        self.log = logging.getLogger("Mock external search index")
        self.queries = []
        self.search = self.docs.keys()
        self.test_search_term = "a search term"

    def _key(self, index, doc_type, id):
        return (index, doc_type, id)

    def index(self, index, doc_type, id, body):
        self.docs[self._key(index, doc_type, id)] = body
        self.search = self.docs.keys()

    def delete(self, index, doc_type, id):
        key = self._key(index, doc_type, id)
        if key in self.docs:
            del self.docs[key]

    def exists(self, index, doc_type, id):
        return self._key(index, doc_type, id) in self.docs

    def create_search_doc(self, query_string, filter=None, pagination=None, debug=False):
        return self.docs.values()

    def query_works(self, query_string, filter, pagination, debug=False, search=None):
        self.queries.append((query_string, filter, pagination, debug))
        # During a test we always sort works by the order in which the
        # work was created.

        def sort_key(x):
            # This needs to work with either a MockSearchResult or a
            # dictionary representing a raw search result.
            if isinstance(x, MockSearchResult):
                return x.work_id
            else:
                return x['_id']
        docs = sorted(self.docs.values(), key=sort_key)
        if pagination:
            start_at = 0
            if isinstance(pagination, SortKeyPagination):
                # Figure out where the previous page ended by looking
                # for the corresponding work ID.
                if pagination.last_item_on_previous_page:
                    look_for = pagination.last_item_on_previous_page[-1]
                    for i, x in enumerate(docs):
                        if x['_id'] == look_for:
                            start_at = i + 1
                            break
            else:
                start_at = pagination.offset
            stop = start_at + pagination.size
            docs = docs[start_at:stop]

        results = []
        for x in docs:
            if isinstance(x, MockSearchResult):
                results.append(x)
            else:
                results.append(
                    MockSearchResult(x["title"], x["author"], {}, x['_id'])
                )

        if pagination:
            pagination.page_loaded(results)
        return results

    def count_works(self, filter):
        return len(self.docs)

    def bulk(self, docs, **kwargs):
        for doc in docs:
            self.index(doc['_index'], doc['_type'], doc['_id'], doc)
        return len(docs), []

class MockMeta(dict):
    """Mock the .meta object associated with an Elasticsearch search
    result.  This is necessary to get SortKeyPagination to work with
    MockExternalSearchIndex.
    """
    @property
    def sort(self):
        return self['_sort']

class MockSearchResult(object):

    def __init__(self, title, author, meta, id):
        self.title = title
        self.author = author
        meta["id"] = id
        meta["_sort"] = [title, author, id]
        self.meta = MockMeta(meta)
        self.work_id = id

    def __contains__(self, k):
        return False

    def to_dict(self):
        return {
            "title": self.title,
            "author": self.author,
            "id": self.meta["id"],
            "meta": self.meta,
        }

class SearchIndexMonitor(WorkSweepMonitor):
    """Make sure the search index is up-to-date for every work.

    This operates on all Works, not just the ones with registered
    WorkCoverageRecords indicating that work needs to be done.
    """
    SERVICE_NAME = "Search index update"
    DEFAULT_BATCH_SIZE = 500

    def __init__(self, _db, collection, index_name=None, index_client=None,
                 **kwargs):
        super(SearchIndexMonitor, self).__init__(_db, collection, **kwargs)

        if index_client:
            # This would only happen during a test.
            self.search_index_client = index_client
        else:
            self.search_index_client = ExternalSearchIndex(
                _db, works_index=index_name
            )

        index_name = self.search_index_client.works_index
        # We got a generic service name. Replace it with a more
        # specific one.
        self.service_name = "Search index update (%s)" % index_name

    def process_batch(self, offset):
        """Update the search index for a set of Works."""
        batch = self.fetch_batch(offset).all()
        if batch:
            successes, failures = self.search_index_client.bulk_update(batch)

            for work, message in failures:
                self.log.error(
                    "Failed to update search index for %s: %s", work, message
                )
            WorkCoverageRecord.bulk_add(
                successes, WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
            )
            # Start work on the next batch.
            return batch[-1].id, len(batch)
        else:
            # We're done.
            return 0, 0


class SearchIndexCoverageProvider(WorkPresentationProvider):
    """Make sure all Works have up-to-date representation in the
    search index.
    """

    SERVICE_NAME = 'Search index coverage provider'

    DEFAULT_BATCH_SIZE = 500

    OPERATION = WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION

    def __init__(self, *args, **kwargs):
        search_index_client = kwargs.pop('search_index_client', None)
        super(SearchIndexCoverageProvider, self).__init__(*args, **kwargs)
        self.search_index_client = (
            search_index_client or ExternalSearchIndex(self._db)
        )

    def process_batch(self, works):
        """
        :return: a mixed list of Works and CoverageFailure objects.
        """
        successes, failures = self.search_index_client.bulk_update(works)

        records = list(successes)
        for (work, error) in failures:
            records.append(CoverageFailure(work, error))

        return records
