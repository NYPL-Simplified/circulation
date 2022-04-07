alter table customlistentries add column work_id integer;

alter table customlistentries
    add constraint customlistentries_work_id_fkey
    foreign key (work_id)
    references works(id);

update customlistentries set work_id = subquery.work_id from (
    select cle.id as entry_id, works.id as work_id
    from works
    join licensepools lp on lp.work_id=works.id
    join customlistentries cle on cle.license_pool_id = lp.id
    where lp.work_id is not null
)
as subquery where customlistentries.id=subquery.entry_id;
