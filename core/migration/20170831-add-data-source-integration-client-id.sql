alter table datasources add column integration_client_id integer unique;
alter table datasources add constraint datasources_integration_client_id_fkey
    foreign key (integration_client_id) references integrationclients(id);

create index "ix_datasources_integration_client"
    on datasources (integration_client_id);