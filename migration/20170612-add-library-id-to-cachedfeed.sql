-- Remove the existing index from cachedfeeds
drop index if exists ix_cachedfeeds_lane_name_type_facets_pagination;

-- Add library_id as a foreign key
alter table cachedfeeds add column library_id integer;
alter table cachedfeeds add constraint cachedfeeds_library_id_fkey
    foreign key (library_id) references libraries(id);

create index "ix_cachedfeeds_library_id_lane_name_type_facets_pagination"
    on cachedfeeds (library_id, lane_name, type, facets, pagination);

-- Set library_id to the default library for all existing cachedfeeds.
update cachedfeeds set library_id = (select id from libraries limit 1);
