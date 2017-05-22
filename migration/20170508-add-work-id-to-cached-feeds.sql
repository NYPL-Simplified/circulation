alter table cachedfeeds add column work_id integer;
create index ix_cachedfeeds_work_id on cachedfeeds using btree (work_id);
alter table cachedfeeds
    ADD CONSTRAINT cachedfeeds_work_id_fkey
    FOREIGN KEY (work_id) REFERENCES works(id);
delete from cachedfeeds where license_pool_id is not null;
alter table cachedfeeds drop column if exists license_pool_id;
