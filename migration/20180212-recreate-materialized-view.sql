drop materialized view mv_works_for_lanes;

-- Create the materialized view with no data.
create materialized view mv_works_for_lanes
as
 SELECT 
    works.id AS works_id,
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
    workgenres.id AS workgenres_id,
    workgenres.genre_id,
    workgenres.affinity,
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
    licensepools.collection_id,
    customlistentries.list_id,
    customlistentries.edition_id as list_edition_id,
    customlistentries.first_appearance

   FROM works
     JOIN editions ON editions.id = works.presentation_edition_id
     JOIN licensepools ON editions.id = licensepools.presentation_edition_id
     JOIN datasources ON licensepools.data_source_id = datasources.id
     JOIN identifiers on licensepools.identifier_id = identifiers.id
     LEFT JOIN customlistentries on works.id = customlistentries.work_id
     LEFT JOIN workgenres ON works.id = workgenres.work_id
  WHERE works.presentation_ready = true
    AND works.simple_opds_entry IS NOT NULL

  ORDER BY (editions.sort_title, editions.sort_author, licensepools.availability_time)
  WITH NO DATA;

-- First create an index that allows work/genre lookup. It's unique and incorporates license_pool_id so that the materialized view can be refreshed CONCURRENTLY.
-- NOTE: All fields mentioned here also need to be part of the primary key
-- for the model object defined in model.py.
create unique index mv_works_for_lanes_unique on mv_works_for_lanes (works_id, genre_id, list_id, list_edition_id, license_pool_id);

-- Create an index on everything, sorted by descending availability time, so that sync feeds are fast.

create index mv_works_for_lanes_by_availability on mv_works_for_lanes (availability_time DESC, sort_author, sort_title, works_id);

-- Create an index on everything, sorted by the maximum of entry first appearance, license pool availability time, and work update time, so that crawlable feeds are fast.

-- MIGRATION NOTE: We don't create mv_works_for_lanes_by_recently_updated here
-- because it will be created in 20180307-replace-recently-updated-index.

-- Create indexes that are helpful in running the query to find featured works.

create index mv_works_for_lanes_by_random_and_genre on mv_works_for_lanes (random, language, genre_id);

create index mv_works_for_lanes_by_random_audience_target_age on mv_works_for_lanes (random, language, audience, target_age);

create index mv_works_for_lanes_by_random_fiction_audience_target_age on mv_works_for_lanes (random, language, fiction, audience, target_age);

-- Similarly, an index on everything, sorted by descending update time.

create index mv_works_for_lanes_by_modification on mv_works_for_lanes (last_update_time DESC, sort_author, sort_title, works_id);

-- We need three versions of each index:
--- One that orders by sort_author, sort_title, and works_id
--- One that orders by sort_title, sort_author, and works_id
--- One that orders by availability_time (descending!), sort_title, sort_author, and works_id

-- English adult fiction

create index mv_works_for_lanes_english_adult_fiction_by_author on mv_works_for_lanes (sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language = 'eng';

create index mv_works_for_lanes_english_adult_fiction_by_title on mv_works_for_lanes (sort_title, sort_author, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language = 'eng';

create index mv_works_for_lanes_english_adult_fiction_by_availability on mv_works_for_lanes (availability_time DESC, sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language = 'eng';

-- English adult nonfiction

create index mv_works_for_lanes_english_adult_nonfiction_by_author on mv_works_for_lanes (sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language = 'eng';

create index mv_works_for_lanes_english_adult_nonfiction_by_title on mv_works_for_lanes (sort_title, sort_author, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language = 'eng';

create index mv_works_for_lanes_english_adult_nonfiction_by_availability on mv_works_for_lanes (availability_time DESC, sort_author, sort_title, works_id) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language = 'eng';

-- Nonenglish adult fiction
--- These are also ordered by language

create index mv_works_for_lanes_nonenglish_adult_fiction_by_author on mv_works_for_lanes (sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language <> 'eng';

create index mv_works_for_lanes_nonenglish_adult_fiction_by_title on mv_works_for_lanes (sort_title, sort_author, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language <> 'eng';

create index mv_works_for_lanes_nonenglish_adult_fiction_by_availability on mv_works_for_lanes (availability_time DESC, sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = true AND language <> 'eng';

-- Nonenglish adult nonfiction
--- These are also ordered by language

create index mv_works_for_lanes_nonenglish_adult_nonfiction_by_author on mv_works_for_lanes (sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language <> 'eng';

create index mv_works_for_lanes_nonenglish_adult_nonfiction_by_title on mv_works_for_lanes (sort_title, sort_author, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language <> 'eng';

create index mv_works_for_lanes_nonenglish_adult_nonfiction_by_availability on mv_works_for_lanes (availability_time DESC, sort_author, sort_title, works_id, language) WHERE audience in ('Adult', 'Adults Only') AND fiction = false AND language <> 'eng';

-- YA/Children's fiction, regardless of language

create index mv_works_for_lanes_ya_fiction_by_author on mv_works_for_lanes (sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = true;

create index mv_works_for_lanes_ya_fiction_by_title on mv_works_for_lanes (sort_title, sort_author, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = true;

create index mv_works_for_lanes_ya_fiction_by_availability on mv_works_for_lanes (availability_time DESC, sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = true;

-- YA/Children's nonfiction, regardless of language

create index mv_works_for_lanes_ya_nonfiction_by_author on mv_works_for_lanes (sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = false;

create index mv_works_for_lanes_ya_nonfiction_by_title on mv_works_for_lanes (sort_title, sort_author, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = false;

create index mv_works_for_lanes_ya_nonfiction_by_availability on mv_works_for_lanes (availability_time DESC, sort_author, sort_title, language, works_id) WHERE audience in ('Children', 'Young Adult') AND fiction = false;

-- Refresh the new materialized view.
refresh materialized view mv_works_for_lanes;

