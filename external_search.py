from nose.tools import set_trace
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as elasticsearch_bulk
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
from model import (
    ExternalIntegration,
    Work,
    WorkCoverageRecord,
)
from monitor import WorkSweepMonitor
from coverage import (
    CoverageFailure,
    WorkCoverageProvider,
)
import os
import logging
import re
import time

class ExternalSearchIndex(object):

    NAME = ExternalIntegration.ELASTICSEARCH

    WORKS_INDEX_PREFIX_KEY = u'works_index_prefix'

    DEFAULT_WORKS_INDEX_PREFIX = u'circulation-works'

    work_document_type = 'work-type'
    __client = None

    CURRENT_ALIAS_SUFFIX = 'current'
    VERSION_RE = re.compile('-v([0-9]+)$')

    SETTINGS = [
        { "key": ExternalIntegration.URL, "label": _("URL") },
        { "key": WORKS_INDEX_PREFIX_KEY, "label": _("Index prefix"),
          "default": DEFAULT_WORKS_INDEX_PREFIX,
          "description": _("Any Elasticsearch indexes needed for this application will be created with this unique prefix. In most cases, the default will work fine. You may need to change this if you have multiple application servers using a single Elasticsearch server.")
        },
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

    def __init__(self, _db, url=None, works_index=None):

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
        if not url:
            raise CannotLoadConfiguration(
                "No URL configured to Elasticsearch server."
            )

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
        self.search = self.__client.search
        self.index = self.__client.index
        self.delete = self.__client.delete
        self.exists = self.__client.exists

        # Sets self.works_index and self.works_alias values.
        # Document upload runs against the works_index.
        # Search queries run against works_alias.
        if works_index and integration:
            self.set_works_index_and_alias(_db)

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
        exists = self.indices.exists_alias(name=alias_name)

        def _set_works_alias(name):
            self.works_alias = self.__client.works_alias = name

        if exists:
            exists_on_works_index = self.indices.exists_alias(
                index=self.works_index, name=alias_name
            )
            if exists_on_works_index:
                _set_works_alias(alias_name)
            else:
                # The current alias is already set on a different index.
                # Don't overwrite it. Instead, just use the given index.
                _set_works_alias(self.works_index)
            return

        # Create the alias and search against it.
        response = self.indices.put_alias(
            index=self.works_index, name=alias_name
        )
        if not response.get('acknowledged'):
            self.log.error("Alias '%s' could not be created", alias_name)
            # Work against the index instead of an alias.
            _set_works_alias(self.works_index)
            return
        _set_works_alias(alias_name)

    def setup_index(self, new_index=None):
        """Create the search index with appropriate mapping.

        This will destroy the search index, and all works will need
        to be indexed again. In production, don't use this on an
        existing index. Use it to create a new index, then change the
        alias to point to the new index.
        """
        index = new_index or self.works_index

        if self.indices.exists(index):
            self.indices.delete(index)

        self.log.info("Creating index %s", index)
        body = ExternalSearchIndexVersions.latest_body()
        self.indices.create(index=index, body=body)

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
            self.setup_current_alias(_db)
            return

        exists_on_works_index = self.indices.get_alias(
            index=self.works_index, name=alias_name
        )
        if not exists_on_works_index:
            # The alias exists on one or more other indices.
            # Remove it from them.
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

    def query_works(self, library, query_string, media, languages, fiction, audiences,
                    target_age, in_any_of_these_genres=[], on_any_of_these_lists=None, fields=None, size=30, offset=0):
        if not self.works_alias:
            return []

        if library is None:
            # TODO: We're searching the entire index and filtering out
            # books that aren't available to the patron's current
            # library in a later step. To avoid search disruption,
            # this is the behavior we'll use until we're able to
            # migrate all existing search indexes to the new document
            # format.
            collection_ids = None
        else:
            collection_ids = [x.id for x in library.collections]

        filter = self.make_filter(
            collection_ids, media, languages, fiction,
            audiences, target_age, in_any_of_these_genres,
            on_any_of_these_lists
        )
        q = dict(
            filtered=dict(
                query=self.make_query(query_string),
                filter=filter,
            ),
        )
        body = dict(query=q)
        search_args = dict(
            index=self.works_alias,
            body=dict(query=q),
            from_=offset,
            size=size,
        )
        if fields is not None:
            search_args['fields'] = fields
        # search_args['explain'] = True
        # print "Args looks like: %r" % search_args
        results = self.search(**search_args)
        # print "Results: %r" % results
        return results

    def make_query(self, query_string):

        def make_query_string_query(query_string, fields):
            return {
                'simple_query_string': {
                    'query': query_string,
                    'fields': fields,
                }
            }

        def make_phrase_query(query_string, fields, boost=100):
            field_queries = []
            for field in fields:
                field_query = {
                  'match_phrase': {
                    field: query_string
                  }
                }
                field_queries.append(field_query)
            return {
                'bool': {
                  'should': field_queries,
                  'minimum_should_match': 1,
                  'boost': boost,
                }
              }

        def make_fuzzy_query(query_string, fields):
            return {
                'multi_match': {
                    'query': query_string,
                    'fields': fields,
                    'type': 'best_fields',
                    'fuzziness': 'AUTO'
                }
            }

        def make_match_query(query_string, field):
            query = {'match': {}}
            query['match'][field] = query_string
            return query

        def make_target_age_query(target_age):
            (lower, upper) = target_age[0], target_age[1]
            return {
                "bool" : {
                    # There must be some overlap with the range in the query
                    "must": [
                       {"range": {"target_age.upper": {"gte": lower}}},
                       {"range": {"target_age.lower": {"lte": upper}}},
                     ],
                    # Results with ranges closer to the query are better
                    # e.g. for query 4-6, a result with 5-6 beats 6-7
                    "should": [
                       {"range": {"target_age.upper": {"lte": upper}}},
                       {"range": {"target_age.lower": {"gte": lower}}},
                     ],
                    "boost": 40
                }
            }


        stemmed_query_string_fields = [
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

        fuzzy_fields = [
            # Only minimal stemming should be used with fuzzy queries.
            'title.minimal^4',
            'series.minimal^4',
            "subtitle.minimal^3",
            "summary.minimal^2",

            'author^4',
            'publisher',
            'imprint'
        ]

        # These words will fuzzy match other common words that aren't relevant,
        # so if they're present and correctly spelled we shouldn't use a
        # fuzzy query.
        fuzzy_blacklist = [
            "baseball", "basketball", # These fuzzy match each other

            "soccer", # Fuzzy matches "saucer", "docker", "sorcery"

            "football", "softball", "software", "postwar",

            "hamlet", "harlem", "amulet", "tablet",

            "biology", "ecology", "zoology", "geology",

            "joke", "jokes" # "jake"

            "cat", "cats",
            "car", "cars",
            "war", "wars",

            "away", "stay",
        ]
        fuzzy_blacklist_re = re.compile(r'\b(%s)\b' % "|".join(fuzzy_blacklist), re.I)

        # Find results that match the full query string in one of the main
        # fields.

        # Query string operators like "AND", "OR", "-", and quotation marks will
        # work in the query string queries, but not the fuzzy query.
        match_full_query_stemmed = make_query_string_query(query_string, stemmed_query_string_fields)
        must_match_options = [match_full_query_stemmed]

        match_phrase = make_phrase_query(query_string, ['title.minimal', 'author', 'series.minimal'])
        must_match_options.append(match_phrase)

        # An exact title or author match outweighs a match that is split
        # across fields.
        match_title = make_phrase_query(query_string, ['title.standard'], 200)
        must_match_options.append(match_title)
        match_author = make_phrase_query(query_string, ['author.standard'], 200)
        must_match_options.append(match_author)

        if not fuzzy_blacklist_re.search(query_string):
            fuzzy_query = make_fuzzy_query(query_string, fuzzy_fields)
            must_match_options.append(fuzzy_query)

        # If fiction or genre is in the query, results can match the fiction or
        # genre value and the remaining words in the query string, instead of the
        # full query.

        fiction = None
        if re.compile(r"\bnonfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Nonfiction"
        elif re.compile(r"\bfiction\b", re.IGNORECASE).search(query_string):
            fiction = "Fiction"

        # Get the genre and the words in the query that matched it, if any
        genre, genre_match = KeywordBasedClassifier.genre_match(query_string)

        # Get the audience and the words in the query that matched it, if any
        audience, audience_match = KeywordBasedClassifier.audience_match(query_string)

        # Get the grade level and the words in the query that matched it, if any
        age_from_grade, grade_match = GradeLevelClassifier.target_age_match(query_string)
        if age_from_grade and age_from_grade[0] == None:
            age_from_grade = None

        # Get the age range and the words in the query that matched it, if any
        age, age_match = AgeClassifier.target_age_match(query_string)
        if age and age[0] == None:
            age = None

        if fiction or genre or audience or age_from_grade or age:
            remaining_string = query_string
            classification_queries = []

            def without_match(original_string, match):
                # If the match was "children" and the query string was "children's",
                # we want to remove the "'s" as well as the match. We want to remove
                # everything up to the next word boundary that's not an apostrophe
                # or a dash.
                word_boundary_pattern = r"\b%s[\w'\-]*\b"

                return re.compile(word_boundary_pattern % match.strip(), re.IGNORECASE).sub("", original_string)

            if genre:
                match_genre = make_match_query(genre.name, 'genres.name')
                classification_queries.append(match_genre)
                remaining_string = without_match(remaining_string, genre_match)

            if audience:
                match_audience = make_match_query(audience.replace(" ", ""), 'audience')
                classification_queries.append(match_audience)
                remaining_string = without_match(remaining_string, audience_match)

            if fiction:
                match_fiction = make_match_query(fiction, 'fiction')
                classification_queries.append(match_fiction)
                remaining_string = without_match(remaining_string, fiction)

            if age_from_grade:
                match_age_from_grade = make_target_age_query(age_from_grade)
                classification_queries.append(match_age_from_grade)
                remaining_string = without_match(remaining_string, grade_match)

            if age:
                match_age = make_target_age_query(age)
                classification_queries.append(match_age)
                remaining_string = without_match(remaining_string, age_match)

            if len(remaining_string.strip()) > 0:
                # Someone who searches by genre is probably not looking for a specific book,
                # but they might be looking for an author (eg, "science fiction iain banks").
                # However, it's possible that they're searching for a subject that's not
                # mentioned in the summary (eg, a person's name in a biography). So title
                # is a possible match, but is less important than author, subtitle, and summary.
                match_rest_of_query = make_query_string_query(remaining_string, ["author^4", "subtitle^3", "summary^5", "title^1", "series^1"])
                classification_queries.append(match_rest_of_query)

            # If classification queries and the remaining string all match, the result will
            # have a higher score than results that match the full query in one of the
            # main fields.
            match_classification_and_rest_of_query = {
                'bool': {
                    'must': classification_queries,
                    'boost': 200.0
                }
            }

            must_match_options.append(match_classification_and_rest_of_query)

        # Results must match either the full query or the genre/fiction query.
        # dis_max uses the highest score from the matching queries, rather than
        # summing the scores.
        return {
            'dis_max': {
                'queries': must_match_options,
            }
        }

    def make_filter(self, collection_ids, media, languages, fiction, audiences, target_age, genres, customlist_ids):
        def _f(s):
            if not s:
                return s
            return s.lower().replace(" ", "")

        clauses = []
        if collection_ids is not None:
            # Either the collection ID field must be completely
            # missing (as it will be in older indexes) or it must
            # include one of the collection IDs we're looking for.
            collection_id_matches = dict(
                terms=dict(collection_id=list(collection_ids))
            )
            no_collection_id = dict(
                bool=dict(must_not=dict(exists=dict(field="collection_id")))
            )
            clauses.append({'or': [collection_id_matches, no_collection_id]})
        if languages:
            clauses.append(dict(terms=dict(language=list(languages))))
        if genres:
            genres = [x for x in genres]
            if isinstance(genres[0], int):
                # We were given genre IDs. Leave them alone.
                genre_ids = genres
            else:
                # We were given genre objects. This should
                # no longer happen but we'll handle it.
                genre_ids = [genre.id for genre in genres]
            clauses.append(dict(terms={"genres.term": genre_ids}))
        if customlist_ids is not None:
            clauses.append(dict(terms={"list_id": customlist_ids}))
        if media:
            media = [_f(medium) for medium in media]
            clauses.append(dict(terms=dict(medium=media)))
        if fiction == True:
            clauses.append(dict(term=dict(fiction="fiction")))
        elif fiction == False:
            clauses.append(dict(term=dict(fiction="nonfiction")))
        if audiences:
            if isinstance(audiences, list) or isinstance(audiences, set):
                audiences = [_f(aud) for aud in audiences]
                clauses.append(dict(terms=dict(audience=audiences)))
        if target_age:
            if isinstance(target_age, tuple) and len(target_age) == 2:
                lower, upper = target_age
            else:
                lower = target_age.lower
                upper = target_age.upper

            age_clause = {
                "and": [
                    {
                        "or" : [
                            {"range": {"target_age.upper": {"gte": lower}}},
                            {
                                "bool": {
                                    "must_not" : {
                                        "exists": {"field" : "target_age.upper"}
                                    }
                                }
                            }
                        ]
                    },
                    {
                        "or" : [
                            {"range": {"target_age.lower": {"lte": upper}}},
                            {
                                "bool": {
                                    "must_not" : {
                                        "exists": {"field" : "target_age.lower"}
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
            clauses.append(age_clause)
        if len(clauses) > 0:
            return {'and': clauses}
        else:
            return {}

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

class ExternalSearchIndexVersions(object):

    VERSIONS = ['v2', 'v3']

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
    def map_fields(cls, fields, field_description):
        mapping = {"properties": {}}
        for field in fields:
            mapping["properties"][field] = field_description
        return mapping

    @classmethod
    def v3_body(cls):
        """The v3 body is the same as the v2 except for the inclusion of the
        '.standard' version of fields, analyzed using the standard
        analyzer for near-exact matches.
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
                        "analyzer": "en_minimal_analyzer"},
                    "standard": {
                        "type": "string",
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


class DummyExternalSearchIndex(ExternalSearchIndex):

    work_document_type = 'work-type'

    def __init__(self, url=None):
        self.url = url
        self.docs = {}
        self.works_index = "works"
        self.works_alias = "works-current"
        self.log = logging.getLogger("Dummy external search index")
        self.queries = []

    def _key(self, index, doc_type, id):
        return (index, doc_type, id)

    def index(self, index, doc_type, id, body):
        self.docs[self._key(index, doc_type, id)] = body

    def delete(self, index, doc_type, id):
        key = self._key(index, doc_type, id)
        if key in self.docs:
            del self.docs[key]

    def exists(self, index, doc_type, id):
        return self._key(index, doc_type, id) in self.docs

    def query_works(self, *args, **kwargs):
        self.queries.append((args, kwargs))
        doc_ids = sorted([dict(_id=key[2]) for key in self.docs.keys()])
        if 'offset' in kwargs and 'size' in kwargs:
            offset = kwargs['offset']
            size = kwargs['size']
            doc_ids = doc_ids[offset: offset + size]
        return { "hits" : { "hits" : doc_ids }}

    def bulk(self, docs, **kwargs):
        for doc in docs:
            self.index(doc['_index'], doc['_type'], doc['_id'], doc)
        return len(docs), []


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
            return batch[-1].id
        else:
            # We're done.
            return 0


class SearchIndexCoverageProvider(WorkCoverageProvider):
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
