create materialized view mv_works_editions_datasources_identifiers
as
 SELECT 
    distinct works.id AS works_id,
    editions.id AS editions_id,
    licensepools.data_source_id,
    licensepools.identifier_id,
    editions.sort_title,
    editions.permanent_work_id,
    editions.sort_author,
    editions.medium,
    editions.language,
    editions.cover_full_url,
    editions.cover_thumbnail_url,
    editions.series,
    editions.series_position,
    datasources.name,
    identifiers.type,
    identifiers.identifier,
    works.audience,
    works.target_age,
    works.fiction,
    works.quality,
    works.rating,
    works.popularity,
    works.random,
    works.last_update_time,
    works.simple_opds_entry,
    works.verbose_opds_entry,
    licensepools.id AS license_pool_id,
    licensepools.open_access_download_url,
    licensepools.availability_time,
    licensepools.collection_id

   FROM works
     JOIN editions ON editions.id = works.presentation_edition_id
     JOIN licensepools ON editions.id = licensepools.presentation_edition_id
     JOIN datasources ON licensepools.data_source_id = datasources.id
     JOIN identifiers on licensepools.identifier_id = identifiers.id
  WHERE works.presentation_ready = true
    AND works.simple_opds_entry IS NOT NULL
  
  ORDER BY editions.sort_title, editions.sort_author, licensepools.availability_time
  WITH NO DATA;

-- Create a unique index so that searches can look up books by work ID.

create unique index mv_works_editions_work_id on mv_works_editions_datasources_identifiers (works_id);

-- Create an index on everything, sorted by descending availability time, so that sync feeds are fast.

create index mv_works_editions_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, works_id);

-- Similarly, an index on everything, sorted by descending update time.

create index mv_works_editions_by_modification on mv_works_editions_datasources_identifiers (last_update_time DESC, sort_author, sort_title, works_id);

-- We need three versions of each index:
--- One that orders by sort_author, sort_title, and works_id
--- One that orders by sort_title, sort_author, and works_id
--- One that orders by availability_time (descending!), sort_title, sort_author, and works_id

-- English adult fiction

create index mv_works_editions_english_adult_fiction_by_author on mv_works_editions_datasources_identifiers (sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language = 'eng';

create index mv_works_editions_english_adult_fiction_by_title on mv_works_editions_datasources_identifiers (sort_title, sort_author, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language = 'eng';

create index mv_works_editions_english_adult_fiction_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language = 'eng';

-- English adult nonfiction

create index mv_works_editions_english_adult_nonfiction_by_author on mv_works_editions_datasources_identifiers (sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language = 'eng';

create index mv_works_editions_english_adult_nonfiction_by_title on mv_works_editions_datasources_identifiers (sort_title, sort_author, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language = 'eng';

create index mv_works_editions_english_adult_nonfiction_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language = 'eng';

-- Nonenglish adult fiction
--- These are also ordered by language

create index mv_works_editions_nonenglish_adult_fiction_by_author on mv_works_editions_datasources_identifiers (sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language <> 'eng';

create index mv_works_editions_nonenglish_adult_fiction_by_title on mv_works_editions_datasources_identifiers (sort_title, sort_author, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language <> 'eng';

create index mv_works_editions_nonenglish_adult_fiction_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language <> 'eng';

-- Nonenglish adult nonfiction
--- These are also ordered by language

create index mv_works_editions_nonenglish_adult_nonfiction_by_author on mv_works_editions_datasources_identifiers (sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language <> 'eng';

create index mv_works_editions_nonenglish_adult_nonfiction_by_title on mv_works_editions_datasources_identifiers (sort_title, sort_author, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language <> 'eng';

create index mv_works_editions_nonenglish_adult_nonfiction_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language <> 'eng';

-- YA/Children's fiction, regardless of language

create index mv_works_editions_ya_fiction_by_author on mv_works_editions_datasources_identifiers (sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = true;

create index mv_works_editions_ya_fiction_by_title on mv_works_editions_datasources_identifiers (sort_title, sort_author, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = true;

create index mv_works_editions_ya_fiction_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = true;

-- YA/Children's nonfiction, regardless of language

create index mv_works_editions_ya_nonfiction_by_author on mv_works_editions_datasources_identifiers (sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = false;

create index mv_works_editions_ya_nonfiction_by_title on mv_works_editions_datasources_identifiers (sort_title, sort_author, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = false;

create index mv_works_editions_ya_nonfiction_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = false;

-- The materialized view will be refreshed as part of initialization.
