-- Remove all WorkCoverageRecords pertaining to the search index. This
-- will force a complete reindex on the next run of bin/search_index_refresh.
delete from workcoveragerecords where operation='update-search-index';
