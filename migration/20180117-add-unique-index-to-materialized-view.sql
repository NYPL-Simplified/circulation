drop index if exists mv_works_for_lanes_work_id_genre_id;
create index mv_works_for_lanes_unique on mv_works_for_lanes (works_id, genre_id, license_pool_id);

-- This materialized view will be deleted soon, but until it is deleted,
-- it needs to have a unique index, and work+genre ID isn't necessarily
-- unique.
drop index if exists mv_works_editions_work_id;
create unique index mv_works_editions_work_id on mv_works_editions_datasources_identifiers (works_id, license_pool_id);
