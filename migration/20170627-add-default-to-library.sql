-- Add libraries.is_default
alter table libraries add column is_default boolean default false;

create index "ix_libraries_default" on libraries (is_default);

-- The first library in the system is set as the default.
update libraries set is_default = False;
update libraries set is_default = True where id in (select min(id) from libraries);
