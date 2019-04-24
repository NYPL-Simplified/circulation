from nose.tools import set_trace
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as elasticsearch_bulk
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch_dsl import (
    Index,
    Search,
    Q,
)
try:
    from elasticsearch_dsl import F
    MAJOR_VERSION = 1
except ImportError, e:
    from elasticsearch_dsl import Q as F, Nested
    MAJOR_VERSION = 6
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
    ExternalIntegration,
    Library,
    Work,
    WorkCoverageRecord,
)
from monitor import WorkSweepMonitor
from coverage import (
    CoverageFailure,
    WorkPresentationProvider,
)
from selftest import (
    HasSelfTests,
    SelfTestResult,
)
import os
import logging
import re
import time

class ExternalSearchIndex(HasSelfTests):

    NAME = ExternalIntegration.ELASTICSEARCH

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

    @classmethod
    def works_index_name(cls, _db):
        """Look up the name of the search index.

        It's possible, but unlikely, that the search index alias will
        point to some other index. But if there were no indexes, and a
        new one needed to be created, this would be the name of that
        index.
        """
        return cls.works_prefixed(_db, ExternalSearchIndexVersions.latest())

    @classmethod
    def works_alias_name(cls, _db):
        """Look up the name of the search index alias."""
        return cls.works_prefixed(_db, cls.CURRENT_ALIAS_SUFFIX)

    def __init__(self, _db, url=None, works_index=None, test_search_term=None,
                 in_testing=False):
        """Constructor

        :param in_testing: Set this to true if you don't want an
        Elasticsearch client to be created, e.g. because you're
        running a unit test of the constructor.
        """
        self.log = logging.getLogger("External search index")
        self.works_index = None
        self.works_alias = None
        integration = None

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

        # Sets self.works_index and self.works_alias values.
        # Document upload runs against the works_index.
        # Search queries run against works_alias.
        if works_index and integration:
            try:
                self.set_works_index_and_alias(_db)
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
        body = ExternalSearchIndexVersions.latest_body()
        body.setdefault('settings', {}).update(index_settings)
        index = self.indices.create(index=index_name, body=body)

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

    def create_search_doc(self, query_string, filter,
                    debug, return_raw_results):

        query = Query(query_string, filter)
        query_without_filter = Query(query_string)
        search = self.search.query(query.build())
        if filter:
            search = filter.specify_sort_order(search)
        if debug:
            search = search.extra(explain=True)

        fields = None
        if debug or return_raw_results:
            # Don't restrict the fields at all -- get everything.
            # This makes it easy to investigate everything about the
            # results we do get.
            fields = None
        else:
            # All we absolutely need is the document ID, which is a
            # key into the database.
            fields = ["_id"]

        # Change the Search object so it only retrieves the fields
        # we're asking for.
        if fields:
            if MAJOR_VERSION == 1:
                search = search.fields(fields)
            else:
                search = search.source(fields)

        return search

    def query_works(self, query_string, filter=None, pagination=None,
                    debug=False, return_raw_results=False):
        """Run a search query.

        :param query_string: The string to search for.
        :param filter: A Filter object, used to filter out works that
            would otherwise match the query string.
        :param pagination: A Pagination object, used to get a subset
            of the search results.
        :param debug: If this is True, some debugging information will
            be gathered (at a slight performance cost) and logged.
        :param return_raw_results: If this is False (the default)
            the return value will be a list of work IDs. If this is
            true, the full result documents will be returned.
        :return: A list of Work IDs that match the query string, or
            (if return_raw_results is True) a list of dictionaries
            representing search results.

        """
        if not self.works_alias:
            return []

        search = self.create_search_doc(query_string, filter=filter, debug=debug, return_raw_results=return_raw_results)
        if not pagination:
            from lane import Pagination
            pagination = Pagination.default()
        start = pagination.offset
        stop = start + pagination.size

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
                    i, result.title, result.author, result.meta['id'],
                    result.meta['score'] or 0, result.meta['shard']
                )
        if return_raw_results:
            return results
        return [int(result.meta['id']) for result in results]

    def bulk_update(self, works, retry_on_batch_failure=True):
        """Upload a batch of works to the search index at once."""

        time1 = time.time()
        needs_add = []
        successes = []
        for work in works:
            if work.presentation_ready:
                needs_add.append(work)
            else:
                # Works are removed one at a time, which shouldn't
                # pose a performance problem because works almost never
                # stop being presentation ready.
                self.remove_work(work)
                successes.append(work)

        # Add any works that need adding.
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
        #
        # Removed works were already removed, so no need to try them again.
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
                debug=True, return_raw_results=True
            )

        def _works():
            return self.query_works(
                self.test_search_term, filter=None, pagination=None,
                debug=False, return_raw_results=True
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
                    debug=True, return_raw_results=True
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

class ExternalSearchIndexVersions(object):

    VERSIONS = ['v2', 'v3', 'v4']

    @classmethod
    def latest(cls):
        version_re = re.compile('v(\d+)')
        versions = [int(re.match(version_re, v).groups()[0]) for v in cls.VERSIONS]
        latest = sorted(versions)[-1]
        return 'v%d' % latest

    @classmethod
    def latest_body(cls):
        version_method = cls.latest() + '_body'
        return getattr(cls, version_method)()

    @classmethod
    def map_fields(cls, fields, field_description, mapping=None):
        mapping = mapping or {"properties": {}}
        for field in fields:
            mapping["properties"][field] = field_description
        return mapping

    @classmethod
    def map_fields_by_type(cls, fields_by_type, mapping=None):
        """Create a mapping for a number of fields of different types.

        It's assumed these fields will be used in filtering and
        sorting, not searching -- thus their values are stored but not
        indexed. (TODO: ???)

        :param fields_by_type: A dictionary mapping Elasticsearch types
            to field names.
        """
        for type, fields in fields_by_type.items():
            mapping = cls.map_fields(
                fields=fields,
                field_description={
                    "type": type,
                    "index": False,
                    "store": True,
                },
                mapping=mapping
            )
        return mapping

    @classmethod
    def v4_body(cls):
        """The v4 body adds raw versions of a work's sort_title and
        sort_author, so that lists can be sorted by those fields rather
        than by relevance.
        """
        if MAJOR_VERSION == 1:
            string_type = 'string'
        else:
            string_type = 'text'

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
                }
            }
        }

        # For most string fields, we want to apply the standard analyzer (for most searches)
        # and the minimal analyzer (to privilege near-exact matches).
        basic_string_fields = {
            "minimal": {
                "type": string_type,
                "analyzer": "en_minimal_analyzer"},
            "standard": {
                "type": string_type,
                "analyzer": "standard"
            }
        }
        mapping = cls.map_fields(
            fields=["title", "series", "subtitle", "summary", "classifications.term"],
            field_description={
                "type": string_type,
                "analyzer": "en_analyzer",
                "fields": basic_string_fields
            }
        )

        # Series must be analyzed (so it can be used in searches) but also present as a keyword
        # (so it can be used in a filter when listing books from a specific series).
        basic_string_plus_keyword = dict(basic_string_fields)
        basic_string_plus_keyword["keyword"] = {
            "type": "keyword",
            "index": False,
            "store": True,
        }
        mapping = cls.map_fields(
            fields=["series"],
            field_description={
                "type": string_type,
                "analyzer": "en_analyzer",
                "fields": basic_string_plus_keyword,
            },
            mapping=mapping
        )

        # These fields are used for sorting and filtering search
        # results, but not for handling search queries.
        #
        # When we list books by a specific author, we're applying a filter on sort_author,
        # not author.
        fields_by_type = {
            'keyword': ['sort_author', 'sort_title'],
            'date': ['last_update_time'],
            'integer': ['series_position'],
            'float': ['random'],
        }
        mapping = cls.map_fields_by_type(fields_by_type, mapping)

        # Build separate mappings for the nested data types --
        # currently just licensepools.
        licensepool_fields_by_type = {
            'date': ['availability_time'],
            'boolean': ['availability', 'open_access'],
        }
        licensepool_definition = cls.map_fields_by_type(
            licensepool_fields_by_type
        )

        # Add the nested data type mappings to the main mapping.
        for type_name, type_definition in [
            ('licensepool', licensepool_definition),
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
    def v3_body(cls):
        """The v3 body is the same as the v2 except for the inclusion of the
        '.standard' version of fields, analyzed using the standard
        analyzer for near-exact matches.
        """
        if MAJOR_VERSION == 1:
            string_type = 'string'
        else:
            string_type = 'text'

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
                }
            }
        }

        mapping = cls.map_fields(
            fields=["title", "series", "subtitle", "summary", "classifications.term"],
            field_description={
                "type": string_type,
                "analyzer": "en_analyzer",
                "fields": {
                    "minimal": {
                        "type": string_type,
                        "analyzer": "en_minimal_analyzer"},
                    "standard": {
                        "type": string_type,
                        "analyzer": "standard"
                    }
                }}
        )
        mappings = { ExternalSearchIndex.work_document_type : mapping }

        return dict(settings=settings, mappings=mappings)

    @classmethod
    def v2_body(cls):

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
                }
            }
        }

        mapping = cls.map_fields(
            fields=["title", "series", "subtitle", "summary", "classifications.term"],
            field_description={
                "type": "string",
                "analyzer": "en_analyzer",
                "fields": {
                    "minimal": {
                        "type": "string",
                        "analyzer": "en_minimal_analyzer"}}}
        )
        mappings = { ExternalSearchIndex.work_document_type : mapping }

        return dict(settings=settings, mappings=mappings)

    @classmethod
    def create_new_version(cls, search_client, base_index_name, version=None):
        """Creates an index for a new version

        :return: True or False, indicating whether the index was created new.
        """
        if not version:
            version = cls.latest()
        if not version.startswith('v'):
            version = 'v%s' % version

        versioned_index = base_index_name+'-'+version
        if search_client.indices.exists(index=versioned_index):
            return False
        else:
            search_client.setup_index(new_index=versioned_index)
            return True

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

    def build(self):
        """Make an Elasticsearch-DSL query object out of this query."""
        query = self.query()

        # Add the filter, if there is one.
        if self.filter:
            built_filter = self.filter.build()
            if built_filter:
                if MAJOR_VERSION == 1:
                    query = Q("filtered", query=query, filter=built_filter)
                else:
                    query = Q("bool", must=query, filter=built_filter)

        # There you go!
        return query

    def query(self):
        """Build an Elasticsearch Query object for this query string.
        """
        query_string = self.query_string

        if not query_string:
            # There is no 'query' part of this Query -- we will return
            # all records that match the filter.
            return self.match_all_query()

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
    def match_all_query(cls):
        return Q("match_all")

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
    """

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

        # For genre IDs and CustomList IDs, we might get a separate
        # set of restrictions from every item in the WorkList hierarchy.
        # _All_ restrictions must be met for a work to match the filter.
        inherit_some = worklist.inherited_values
        genre_id_restrictions = inherit_some('genre_ids')
        customlist_id_restrictions = inherit_some('customlist_ids')
        return cls(
            library, media, languages, fiction, audiences,
            target_age, genre_id_restrictions, customlist_id_restrictions,
            facets
        )

    def __init__(self, collections=None, media=None, languages=None,
                 fiction=None, audiences=None, target_age=None,
                 genre_restriction_sets=None, customlist_restriction_sets=None,
                 facets=None, order=None, order_ascending=False
    ):

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

        self.order = order
        self.order_ascending = order

        # Give the Facets object a chance to modify any or all of this
        # information.
        if facets:
            facets.modify_search_filter(self)

    def build(self, _chain_filters=None):
        """Convert this object to an Elasticsearch Filter object.

        :param _chain_filters: Mock function to use instead of
        Filter._chain_filters
        """

        # Since a Filter object can be modified after it's created, we
        # need to scrub all the inputs, whether or not they were
        # scrubbed in the constructor.
        scrub_list = self._scrub_list
        filter_ids = self._filter_ids

        chain = _chain_filters or self._chain_filters

        # If necessary, the contents of this dictionary will be used
        # at the end of this method to turn this into a nested filter.
        nested_filters = dict()
        def chain_nested(path, f):
            nested_filters[path] = chain(nested_filters.get(path), f)

        f = None

        collection_ids = filter_ids(self.collection_ids)
        if collection_ids:
            f = chain(f, self.collection_id_filter)

        if self.media:
            f = chain(f, F('terms', medium=scrub_list(self.media)))

        if self.languages:
            f = chain(f, F('terms', language=scrub_list(self.languages)))

        if self.fiction is not None:
            if self.fiction:
                value = 'fiction'
            else:
                value = 'nonfiction'
            f = chain(f, F('term', fiction=value))

        if self.audiences:
            f = chain(f, F('terms', audience=scrub_list(self.audiences)))

        target_age_filter = self.target_age_filter
        if target_age_filter:
            f = chain(f, self.target_age_filter)

        for genre_ids in self.genre_restriction_sets:
            f = chain(f, F('terms', **{'genres.term' : filter_ids(genre_ids)}))

        for customlist_ids in self.customlist_restriction_sets:
            ids = filter_ids(customlist_ids)
            f = chain(f, F('terms', **{'customlists.list_id' : ids}))

        if nested_filters:
            nested = None
            set_trace()
            for path, nested_filter in nested_filters.items():
                nested = Q('nested', path=path, filter=nested_filter)
            if nested:
                f = chain(f, nested)                
        return f

    @property
    def collection_id_filter(self):
        """Helper method to generate a filter on licensepools.collection_id.

        This is used both in building the initial filter and in
        applying a filter during sorting.
        """
        ids = self._filter_ids(self.collection_ids)
        if not ids:
            return None
        return F('terms', **{'licensepools.collection_id' : ids})

    def specify_sort_order(self, search):
        if not self.order:
            return search

        order_field = self.order
        if '.' in order_field:
            # We're sorting by a nested field.
            if self.order_ascending is False:
                order = "desc"
            else:
                order = "asc"
            nested_filter=None
            mode = 'min'
            if order_field == 'licensepools.availability_time':
                collection_ids = self._filter_ids(self.collection_ids)
                if collection_ids:
                    nested_filter = dict(
                        terms={
                            "licensepools.collection_id": self.collection_ids
                        }
                    )
            order_description = dict(order=order, mode=mode)
            if nested_filter:
                order_description['nested_filter'] = nested_filter
            order = { order_field : order_description }
        else:
            order = order_field
            if self.order_ascending is False:
                order = '-' + order
        return search.sort(order)

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
            return F('bool', must_not=[F('exists', field=field)])

        def or_does_not_exist(clause, field):
            """Either the given `clause` matches or the given field
            does not exist.
            """
            if MAJOR_VERSION==1:
                return F('or', [clause, does_not_exist(field)])
            else:
                return F('bool', should=[clause, does_not_exist(field)],
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
        return F('bool', must=clauses)

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

    @classmethod
    def _chain_nested(cls, main, subfilter):
        """Either chain together or start a new chain."""
        if not isinstance(main, Nested):
            # This is a regular query that's about to become a nested query.
            main = Nested(main)
        if existing:
            # We're combining two filters.
            new = existing & new
        else:
            # There was no previous filter -- the 'new' one is it.
            pass
        return new


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

    def create_search_doc(self, query_string, filter=None, debug=False, return_raw_results=False):
        return self.docs.values()

    def query_works(self, query_string, filter, pagination, debug=False, return_raw_results=False, search=None):
        self.queries.append((query_string, filter, pagination, debug, return_raw_results))
        if return_raw_results:
            doc_ids = self.docs.values()
        else:
            doc_ids = sorted([dict(_id=key[2]) for key in self.docs.keys()])

        if pagination:
            start = pagination.offset
            stop = start + pagination.size
            doc_ids = doc_ids[start:stop]

        if return_raw_results:
            return doc_ids
        else:
            return [x['_id'] for x in doc_ids]

    def bulk(self, docs, **kwargs):
        for doc in docs:
            self.index(doc['_index'], doc['_type'], doc['_id'], doc)
        return len(docs), []

class MockSearchResult(object):
    def __init__(self, title, author, meta, id):
        self.title = title
        self.author = author
        meta["id"] = id
        self.meta = meta

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
