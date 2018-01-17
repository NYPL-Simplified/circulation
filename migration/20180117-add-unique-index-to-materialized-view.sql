drop index if exists mv_works_for_lanes_work_id_genre_id;
create index mv_works_for_lanes_unique on mv_works_for_lanes (works_id, genre_id, license_pool_id);
