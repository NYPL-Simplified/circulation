-- Create an index on everything, sorted by descending availability time, so that sync feeds are fast.

create index mv_works_editions_by_availability on mv_works_editions_datasources_identifiers (availability_time DESC, sort_author, sort_title, works_id);

-- Similarly, an index on everything, sorted by descending update time.

create index mv_works_editions_by_modification on mv_works_editions_datasources_identifiers (last_update_time DESC, sort_author, sort_title, works_id);

-- Create an index on everything, sorted by descending availability time, so that sync feeds are fast.

create index mv_works_genres_by_availability on mv_works_editions_workgenres_datasources_identifiers (availability_time DESC, sort_author, sort_title, works_id);

-- Similarly, an index on everything, sorted by descending update time.

create index mv_works_genres_by_modification on mv_works_editions_workgenres_datasources_identifiers (last_update_time DESC, sort_author, sort_title, works_id);
