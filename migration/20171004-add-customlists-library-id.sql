alter table customlists add column library_id integer;

alter table customlists
    add constraint customlists_library_id_fkey
    foreign key (library_id)
    references libraries(id);

create index "ix_customlists_library_id" ON customlists (library_id);